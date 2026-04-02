/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/compaction/ob_tenant_ls_merge_scheduler.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tablet_refresh_dag.h"
#include "storage/compaction/ob_tenant_ls_merge_checker.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_verify_ckm_dag.h"
#include "storage/shared_storage/prewarm/ob_ss_micro_cache_prewarm_service.h"
#include "storage/compaction/ob_merge_ctx_func.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/compaction/ob_update_skip_major_tablet_dag.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/compaction/ob_ss_schedule_tablet_func.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace share;
using namespace storage;
namespace compaction
{

/**
 * -------------------------------------ObTenantLSMergeLoopTaskMgr-------------------------------------
 */
ObTenantLSMergeLoopTaskMgr::ObTenantLSMergeLoopTaskMgr()
  : merge_loop_tg_id_(-1),
    schedule_interval_(ObTenantLSMergeScheduler::DEFAULT_MERGE_SCHEDULE_INTERVAL),
    merge_loop_task_()
{
}

int ObTenantLSMergeLoopTaskMgr::start()
{
  int ret = OB_SUCCESS;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("This task should only start for shared storage mode", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MediumLoop, merge_loop_tg_id_))) {
    LOG_WARN("failed to create medium loop thread", K(ret));
  } else if (OB_FAIL(TG_START(merge_loop_tg_id_))) {
    LOG_WARN("failed to start medium merge scan thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(merge_loop_tg_id_, merge_loop_task_, schedule_interval_, true/*repeat*/))) {
    LOG_WARN("Fail to schedule medium merge scan task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(merge_loop_tg_id_, merge_verify_task_, schedule_interval_, true/*repeat*/))) {
    LOG_WARN("Fail to schedule medium merge verify task", K(ret));
  }
  return ret;
}

void ObTenantLSMergeLoopTaskMgr::stop()
{
  STOP_THREAD(merge_loop_tg_id_);
}

void ObTenantLSMergeLoopTaskMgr::wait()
{
  WAIT_THREAD(merge_loop_tg_id_);
}

void ObTenantLSMergeLoopTaskMgr::destroy()
{
  DESTROY_THREAD(merge_loop_tg_id_);
}

void ObTenantLSMergeLoopTaskMgr::refresh_schedule_interval(
    const int64_t new_schedule_interval)
{
  if (new_schedule_interval != schedule_interval_) {
    schedule_interval_ = new_schedule_interval;
  }
}

int ObTenantLSMergeLoopTaskMgr::reload_tenant_config(
    const int64_t new_schedule_interval)
{
  int ret = OB_SUCCESS;
  const int64_t old_schedule_interval = schedule_interval_;

  if (old_schedule_interval == new_schedule_interval) {
    // do nothing
  } else if (OB_FAIL(restart_schedule_timer_task(new_schedule_interval, merge_loop_tg_id_, merge_loop_task_))) {
    LOG_WARN("failed to restart ls merge loop task", KR(ret), K(new_schedule_interval), K(old_schedule_interval));
  } else if (OB_FAIL(restart_schedule_timer_task(new_schedule_interval, merge_loop_tg_id_, merge_verify_task_))) {
    LOG_WARN("failed to restart ls merge verify task", KR(ret), K(new_schedule_interval), K(old_schedule_interval));
  } else {
    refresh_schedule_interval(new_schedule_interval);
    LOG_INFO("[SS_MERGE] reload schedule interval", K(ret), K(new_schedule_interval), K(old_schedule_interval), K_(schedule_interval));
  }
  return ret;
}

void ObTenantLSMergeLoopTaskMgr::LSMergeLoopTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);

  if (!GCTX.is_shared_storage_mode() || !ObTenantLSMergeScheduler::could_start_loop_task()) {
    // cannot schedule ls merge loop task
  } else {
    bool need_refresh_compaction_obj = true;

    if (MTL(ObTenantLSMergeScheduler *)->is_compacting()) {
      // should absolutely refresh compaction obj
    } else if (MTL_LS_OBJ_MGR.exist_compacting_obj()) {
      // tenant merge finished, but still exists compacting obj, should refresh
      if (REACH_THREAD_TIME_INTERVAL(1_min)) {
        LOG_INFO("[SS_MERGE] tenant merge finished, but still exist compacting obj, should refresh");
      }
    } else {
      need_refresh_compaction_obj = false;
      if (REACH_THREAD_TIME_INTERVAL(1_min)) {
        LOG_INFO("[SS_MERGE] tenant no need to refresh compaction obj");
      }
    }

    if (need_refresh_compaction_obj && OB_TMP_FAIL(MTL(ObTenantCompactionObjMgr *)->refresh())) {
      LOG_WARN_RET(tmp_ret, "failed to refresh compaction obj");
    }

    if (OB_TMP_FAIL(MTL(ObTenantLSMergeScheduler *)->schedule_ls_merge())) {
      LOG_WARN_RET(tmp_ret, "failed to schedule ls merge");
    }

#ifdef ERRSIM
    bool schedule_error_tablet = true;
#else
    bool schedule_error_tablet = GCONF._enable_compaction_diagnose && REACH_THREAD_TIME_INTERVAL(10_min);
#endif
    if (schedule_error_tablet && OB_TMP_FAIL(MTL(ObTenantLSMergeScheduler *)->schedule_error_tablet_verify_ckm())) {
      LOG_WARN_RET(tmp_ret, "failed to schedule error tablet verify ckm");
    }
    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("[SS_MERGE] LSMergeLoopTask", K(cost_ts), K(tmp_ret));
  }
}

void ObTenantLSMergeLoopTaskMgr::LSMergeVerifyTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);

  if (!GCTX.is_shared_storage_mode() || !ObTenantLSMergeScheduler::could_start_loop_task()) {
    // cannot schedule ls merge verify task
  } else {
    if (OB_TMP_FAIL(MTL(ObTenantLSMergeChecker *)->process())) {
      LOG_WARN_RET(tmp_ret, "failed to check ls merge");
    }

    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("[SS_MERGE] LSMergeVerifyTask", K(cost_ts), K(tmp_ret));
  }
}

/**
 * --------------------------------------ObTenantLSMergeScheduler--------------------------------------
 */
ObTenantLSMergeScheduler::TenantScheduleCfg::TenantScheduleCfg()
  : schedule_interval_(DEFAULT_MERGE_SCHEDULE_INTERVAL),
    schedule_batch_size_(DEFAULT_TABLET_BATCH_CNT),
    delay_overwrite_interval_(DEFAULT_DELAY_OVERWRITE_INTERVAL)
{
}

void ObTenantLSMergeScheduler::TenantScheduleCfg::refresh()
{
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      schedule_interval_ = tenant_config->ob_compaction_schedule_interval;
      schedule_batch_size_ = tenant_config->compaction_schedule_tablet_batch_cnt;
      delay_overwrite_interval_ = tenant_config->_ss_new_leader_overwrite_delay;
    }
  }
}

ObTenantLSMergeScheduler::ObTenantLSMergeScheduler()
  : ObBasicMergeScheduler(),
    is_inited_(false),
    schedule_cfg_(),
    ls_tablet_iter_(),
    time_guard_(),
    task_mgr_(),
    validate_scheduler_()
{
}

ObTenantLSMergeScheduler::~ObTenantLSMergeScheduler()
{
  destroy();
}

int ObTenantLSMergeScheduler::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantLSMergeScheduler has inited", K(ret));
  } else if (!GCTX.is_shared_storage_mode()) {
    // do nothing
  } else if (OB_FAIL(validate_scheduler_.init())) {
    LOG_WARN("failed to init validate scheduler", K(ret));
  } else {
    schedule_cfg_.refresh();
    task_mgr_.refresh_schedule_interval(schedule_cfg_.schedule_interval_);
    is_inited_ = true;
  }
  return ret;
}

void ObTenantLSMergeScheduler::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    ObBasicMergeScheduler::reset();
    time_guard_.reuse();
    ls_tablet_iter_.reset();
    task_mgr_.destroy();
    validate_scheduler_.destroy();
    LOG_INFO("ObTenantLSMergeScheduler destroy");
  }
}

int ObTenantLSMergeScheduler::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantLSMergeScheduler not inited", K(ret));
  } else if (OB_FAIL(task_mgr_.start())) {
    LOG_WARN("failed to start task mgr", K(ret));
  }
  return ret;
}

void ObTenantLSMergeScheduler::stop()
{
  is_stop_ = true;
  task_mgr_.stop();
}

void ObTenantLSMergeScheduler::wait()
{
  task_mgr_.wait();
}

int ObTenantLSMergeScheduler::reload_tenant_config()
{
  schedule_cfg_.refresh();
  return task_mgr_.reload_tenant_config(schedule_cfg_.schedule_interval_);
}

void ObTenantLSMergeScheduler::skip_iter_cur_ls()
{
  ls_tablet_iter_.skip_cur_ls();
}

int ObTenantLSMergeScheduler::refresh_tenant_status()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantLSMergeScheduler has not been inited", K(ret));
  } else {
    IGNORE_RETURN tenant_status_.init_or_refresh();
  }
  return ret;
}

int ObTenantLSMergeScheduler::schedule_merge(
    const int64_t broadcast_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantLSMergeScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(broadcast_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(broadcast_version), K(ret));
  } else if (broadcast_version <= get_frozen_version()) {
    LOG_INFO("not update broadcast version", K(broadcast_version), K(frozen_version_));
  } else if (FALSE_IT(MTL(ObSSMicroCachePrewarmService *)->reset_major_prewarm_percent())) { // reset before start merge
  } else if (OB_FAIL(MTL_LS_OBJ_MGR.start_merge(broadcast_version))) {
    LOG_WARN("failed to start new major merge", K(ret), K(broadcast_version));
  } else {
    validate_scheduler_.reuse();
    update_frozen_version_and_merge_progress(broadcast_version);
    LOG_INFO("[SS_MERGE] succ to launch a new round of merge for all ls", K(broadcast_version));
  }
  return ret;
}

int ObTenantLSMergeScheduler::schedule_ls_merge()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t merge_version = get_frozen_version();
  LOG_INFO("[SS_MERGE] start to schedule ls merge", K(merge_version));
  time_guard_.reuse();

  if (!tenant_status_.is_inited() && OB_FAIL(tenant_status_.init_or_refresh())) {
    LOG_WARN("failed to init tenant_status", KR(ret), K_(tenant_status));
  } else if (INIT_COMPACTION_SCN >= merge_version) {
    // In share-storage merge, adaptive compaction is disabled, just do nothing when there is no tenant major task
  } else if (!could_major_merge_start()) {
    LOG_INFO("ls merge has been suspended", K(ret), K(merge_version));
  } else if (OB_FAIL(ls_tablet_iter_.build_iter(merge_version))) {
    LOG_WARN("failed to init ls iterator", K(ret), K(merge_version));
  } else {
    ObSSScheduleTabletFunc func(merge_version);
    bool skip_schedule = false;
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_SHARED_STORAGE_IGNORE_REPLICA) ret;
      if (OB_FAIL(ret)) {
        if (GCTX.get_server_id() == -ret) {
          skip_schedule = true;
        }
        ret = OB_SUCCESS;
        LOG_INFO("ERRSIM EN_SHARED_STORAGE_IGNORE_REPLICA", K(ret));
      }
    }
#endif
    while (OB_SUCC(ret) && !skip_schedule) {
      ObLSHandle ls_handle;
      if (OB_FAIL(ls_tablet_iter_.get_next_ls(ls_handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get ls", K(ret), K(ls_tablet_iter_), K(ls_handle));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_TMP_FAIL(func.switch_ls(ls_handle))) {
        if (OB_STATE_NOT_MATCH == tmp_ret) {
          LOG_INFO("ls cannot schedule merge", KR(tmp_ret), K(merge_version), K(func));
        } else {
          LOG_WARN("failed to switch ls", KR(tmp_ret), K(func));
        }
        skip_iter_cur_ls();
      } else {
        (void) schedule_prepare_medium_info(merge_version, *ls_handle.get_ls(), func);
        (void) schedule_ls_tablets_merge(merge_version, *ls_handle.get_ls(), func);
      }
      LOG_INFO("[SS_MERGE] finish schedule merge for cur ls", K(ret), K(merge_version), K(func));
    } // end while
    try_finish_merge_progress(merge_version);
  }
  LOG_INFO("[SS_MERGE] finish schedule merge for all ls", K(ret), K(merge_version), K_(time_guard));
  return ret;
}

int ObTenantLSMergeScheduler::get_tablet_merge_reason(
    const int64_t merge_version,
    const ObLSID &ls_id,
    ObTablet &tablet,
    ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason)
{
  int ret = OB_SUCCESS;
  // TODO(@DanLing) maybe a bettter way to read medium info
  ObMediumCompactionInfo *medium_info = nullptr;
  ObArenaAllocator tmp_allocator("MediumLoop", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(ObTabletMediumInfoReader::get_medium_info_with_merge_version(merge_version,
                                                                           tablet,
                                                                           tmp_allocator,
                                                                           medium_info))) {
    LOG_WARN("fail to get medium info with merge version", K(ret), K(merge_version), K(tablet));
  } else {
    merge_reason = (ObAdaptiveMergePolicy::AdaptiveMergeReason) medium_info->medium_merge_reason_;
  }
  return ret;
}

int ObTenantLSMergeScheduler::schedule_ls_tablets_merge(
    const int64_t merge_version,
    ObLS &ls,
    ObSSScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  ObBasicObjHandle<ObLSObj> ls_obj_hdl;
  const ObLSID &ls_id = ls.get_ls_id();
  ObLSBroadcastInfo &broadcast_info = func.get_broadcast_info();
  ObSEArray<ObTabletID, DEFAULT_ARRAY_CNT> tablet_ids;
  ObUpdateSkipMajorParam skip_param(ls_id, merge_version);

  if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_id, ls_obj_hdl))) {
    LOG_WARN("failed to get ls obj handle", K(ret), K(func));
  } else if (OB_FAIL(ls_obj_hdl.get_obj()->cur_svr_ls_compaction_status_.get_schedule_tablet(ls_obj_hdl.get_obj()->ls_compaction_list_,
                                                                                             broadcast_info,
                                                                                             ls,
                                                                                             tablet_ids,
                                                                                             skip_param.tablet_info_array_))) {
    LOG_WARN("failed to get schedule tablets", K(ret), K(ls_id), K(broadcast_info));
  } else if (COMPACTION_STATE_REPLICA_VERIFY == broadcast_info.state_) {
    ObVerifyCkmParam ckm_param(ls_id, get_frozen_version());
    if (OB_FAIL(ckm_param.tablet_info_array_.assign(tablet_ids))) {
      LOG_WARN("failed to add tablet to ckm param", K(ret), K(ls_id), K(merge_version));
    } else {
      (void) ObScheduleDagFunc::schedule_verify_ckm_dag(ckm_param);
      if (OB_UNLIKELY(!skip_param.tablet_info_array_.empty())) { // tmp warning, remove later
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "no inc tablets should be empty", K(skip_param));
      }
    }
    skip_iter_cur_ls();
  } else {
    if (!skip_param.tablet_info_array_.empty()) {
      (void) ObScheduleDagFunc::schedule_update_skip_major_tablet_dag(skip_param);
    }

    for (int64_t idx = 0; idx < tablet_ids.count(); ++idx) {
      const ObTabletID &tablet_id = tablet_ids.at(idx);
      ObTabletHandle tablet_handle;

      if (OB_FAIL(ls.get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 0/*timeout_us*/, storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("failed to get tablet handle", K(ret), K(tablet_id));
        }
      } else if (OB_FAIL(func.schedule_tablet(*tablet_handle.get_obj()))) {
        LOG_WARN("failed to schedule tablet", KR(ret), K(tablet_id));
      }
    } // for
  }
  return ret;
}

int ObTenantLSMergeScheduler::schedule_prepare_medium_info(
    const int64_t merge_version,
    ObLS &ls,
    ObSSScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  ObBasicObjHandle<ObLSObj> ls_obj_hdl;
  ObLSBroadcastInfo broadcast_info;
  const ObLSID &ls_id = ls.get_ls_id();
  ObSSCompactionTimeGuard tmp_time_guard;
  ObSEArray<ObTabletID, DEFAULT_ARRAY_CNT> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(MTL_ID(), "SchdTbltIDs"));

  if (!func.get_ls_status().is_leader_) {
    // only ls leader can submit clog, do nothing
  } else if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_id, ls_obj_hdl))) {
    LOG_WARN("failed to get ls obj handle", K(ret), K(ls_id));
  } else if (FALSE_IT(ls_obj_hdl.get_obj()->get_broadcast_info(broadcast_info))) {
  } else if (!broadcast_info.is_leader_) {
    // ls is no leader anymore, do nothing
  } else if (broadcast_info.state_ > ObLSCompactionState::COMPACTION_STATE_COMPACT) {
    LOG_DEBUG("ls state have passed compact state, all medium clog should be submitted", KR(ret), K(ls_id), K(broadcast_info));
  } else if (OB_FAIL(ls_obj_hdl.get_obj()->cur_svr_ls_compaction_status_.get_unsubmitted_clog_tablet(merge_version, tablet_ids))) {
    LOG_WARN("failed to get prepare medium tablet", KR(ret), K(ls_id));
  } else if (tablet_ids.empty()) {
    // no tablet need to schedule
    if (REACH_THREAD_TIME_INTERVAL(30_s)) {
      LOG_INFO("[SS_MERGE] cur ls has no tablet to prepare medium info", K(ret), K(merge_version), K(ls_id), K(broadcast_info));
    }
  } else {
    tmp_time_guard.click(ObSSCompactionTimeGuard::GET_SCHEDULE_TABLET);
    LOG_INFO("[SS_MERGE] get need prepare medium", KR(ret), K(merge_version), K(ls_id), "tablet_cnt", tablet_ids.count());

    for (int64_t idx = 0; idx < tablet_ids.count(); ++idx) { // ignore the ret code
      const ObTabletID &tablet_id = tablet_ids.at(idx);
      ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE;
      bool submit_clog_flag = false;
      ObTabletCompactionState tablet_state;
      ObTabletHandle tablet_handle;

      if (OB_FAIL(ls.get_tablet_svr()->get_tablet(tablet_id,
                                                  tablet_handle,
                                                  0/*timeout*/,
                                                  storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("failed to get tablet", K(ret), K(merge_version), K(ls_id), K(tablet_id));
        } // tablet state will be removed in ObSvrLSCompactionStatusObj::loop_map_to_get_tablets
      } else if (tablet_handle.get_obj()->get_last_major_snapshot_version() >= merge_version) {
        // do nothing, tablet will be filtered in ObSvrLSCompactionStatusObj::loop_map_to_get_tablets
      } else if (OB_FAIL(prepare_tablet_medium_info(merge_version, ls, tablet_handle, merge_reason, submit_clog_flag))) {
        LOG_WARN("failed to prepare tablet medium info", K(ret), K(merge_version), K(tablet_id), K(ls_id));
      } else if (!submit_clog_flag) {
        // do nothing
      } else if (FALSE_IT(++func.get_schedule_tablet_cnt().submit_clog_cnt_)) {
      } else if (ObAdaptiveMergePolicy::DURING_DDL == merge_reason) {
        if (OB_FAIL(ls_obj_hdl.get_obj()->ls_compaction_list_.add_skip_tablet(merge_version, tablet_id))) {
          LOG_WARN("failed to add skip tablet info to list obj", K(ret), K(merge_version), K(tablet_id));
        } else {
          tablet_state.set_skip_scn(merge_version);
          (void) ls_obj_hdl.get_obj()->cur_svr_ls_compaction_status_.update_tablet_state(tablet_id, tablet_state);
        }
      } else {
        tablet_state.compaction_scn_ = merge_version;
        tablet_state.clog_submitted_ = true;
        (void) ls_obj_hdl.get_obj()->cur_svr_ls_compaction_status_.update_tablet_state(tablet_id, tablet_state);
      }
    }
  }
  tmp_time_guard.click(ObSSCompactionTimeGuard::PREPARE_CLOG);
  func.add_time_guard(tmp_time_guard);
  return ret;
}

int ObTenantLSMergeScheduler::prepare_tablet_medium_info(
    const int64_t merge_version,
    ObLS &ls,
    ObTabletHandle &tablet_hdl,
    ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    bool &submit_clog_flag)
{
  int ret = OB_SUCCESS;
  submit_clog_flag = false;
  const ObTabletID &tablet_id = tablet_hdl.get_obj()->get_tablet_id();
  const int64_t last_major_snapshot = tablet_hdl.get_obj()->get_last_major_snapshot_version();
  const int64_t inner_merged_scn = MERGE_SCHEDULER_PTR->get_inner_table_merged_scn();

  ObArenaAllocator tmp_allocator("MediumLoop", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const compaction::ObMediumCompactionInfoList *medium_list = nullptr;
  int64_t max_sync_version = 0;

  if (OB_UNLIKELY(last_major_snapshot >= merge_version)) {
    // do nothing
  } else if (OB_FAIL(tablet_hdl.get_obj()->read_medium_info_list(tmp_allocator, medium_list))) {
    LOG_WARN("failed to load medium info list", K(ret), K(tablet_id));
  } else if (OB_FAIL(medium_list->get_max_sync_medium_scn(max_sync_version))) {
    LOG_WARN("failed to get max sync medium scn", K(ret), KPC(medium_list));
  } else if (max_sync_version >= merge_version) {
    submit_clog_flag = true;
    if (OB_FAIL(medium_list->get_specific_medium_reason(merge_version, merge_reason))) {
      LOG_WARN("failed to get specific medium reason", K(ret), K(merge_version), K(tablet_id), KPC(medium_list));
    }
  } else {
    bool need_schedule_refresh_dag = false;
    if (!medium_list->could_schedule_next_round(last_major_snapshot)) {
      LOG_TRACE("can't schedule next round", K(tablet_id), K(last_major_snapshot), K(inner_merged_scn), KPC(medium_list), K(medium_list->need_check_finish()));
      if (max_sync_version <= inner_merged_scn) {
        need_schedule_refresh_dag = true; // inner table shows finish, need refresh
      }
    } else if (last_major_snapshot < inner_merged_scn) {
      int64_t latest_upload_major_scn = 0;
      if (OB_FAIL(ObMergeCtxFunc::get_latest_upload_major_scn(tablet_id, latest_upload_major_scn))) {
        LOG_WARN("failed to get tablet version list", KR(ret), K(tablet_id));
      } else if (latest_upload_major_scn > last_major_snapshot) {
        need_schedule_refresh_dag = true;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (need_schedule_refresh_dag) { // TODO(@DanLing) Copying SST can be avoided for skip merge tablet, no need this logic, we can opt it later.
#ifdef ERRSIM
      SERVER_EVENT_SYNC_ADD("ss_merge_errsim", "leader_refresh_major",
                            "merge_version", inner_merged_scn,
                            "ls_id", ls.get_ls_id(), "tablet_id", tablet_id);
#endif
      LOG_INFO("found unfinished medium info, schedule refresh dag first", K(tablet_id), K(max_sync_version),
              K(last_major_snapshot), K(inner_merged_scn));
      ObTabletsRefreshSSTableParam param(ls.get_ls_id(), MERGE_SCHEDULER_PTR->get_inner_table_merged_scn());
      param.tablet_id_ = tablet_id;
      (void) compaction::ObScheduleDagFunc::schedule_tablet_refresh_dag(param);
    } else {
      ObScheduleTabletCnt not_used_tablet_cnt;
      ObMediumCompactionScheduleFunc func(ls,
                                          tablet_hdl,
                                          ls.get_ls_wrs_handler()->get_ls_weak_read_ts(),
                                          *medium_list,
                                          &not_used_tablet_cnt /*placeholder*/);
      if (OB_FAIL(func.prepare_ls_major_merge_info(merge_version, merge_reason, submit_clog_flag))) {
        LOG_WARN("failed to prepare ls major merge info", K(ret), K(merge_version), K(tablet_id), K(func));
      }
    }
  }
  return ret;
}


/********************************* ObErrorTabletValidateScheduler **********************************/

int ObErrorTabletValidateScheduler::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_id_set_.create(DEFAULT_BUCKET_CNT, lib::ObMemAttr(MTL_ID(), "ErrorTblSet") ))) {
    LOG_WARN("failed to create error tablet set", K(ret));
  }
  return ret;
}

int ObErrorTabletValidateScheduler::schedule_error_tablet_verify_ckm(const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("[SS_MERGE] schedule_error_tablet_verify_ckm", K(compaction_scn));
  ObSEArray<ObTabletLSPair, 16> error_tablets;
  error_tablets.set_attr(ObMemAttr(MTL_ID(), "CkmErrTablets"));
  if (error_tablets.count() > 0) { // return error_tablets is order by ls_id
    ObLSHandle ls_handle;
    ObLSID skip_ls_id;
    ObTabletHandle tablet_handle;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < error_tablets.count(); ++idx) {
      const ObTabletLSPair &pair = error_tablets.at(idx);
      const ObLSID &ls_id = pair.get_ls_id();
      const ObTabletID &tablet_id = pair.get_tablet_id();
      if (skip_ls_id.is_valid() && ls_id == skip_ls_id) {
        continue;
      } else if (OB_HASH_NOT_EXIST != (tmp_ret = tablet_id_set_.exist_refactored(tablet_id))) {
        if (OB_HASH_EXIST != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("failed to check exist from set", KR(ret), K(idx), K(tablet_id));
        }
      } else if (!ls_handle.is_valid() || ls_handle.get_ls()->get_ls_id() != ls_id) {
        if (OB_TMP_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))
            || ls_handle.get_ls()->is_offline()
            || ls_handle.get_ls()->is_stopped()) {
          if (OB_LS_NOT_EXIST != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("failed to get log stream", KR(tmp_ret), K(ls_id));
          }
          LOG_INFO("skip ls validate", KR(tmp_ret), K(ls_id), K(ls_handle));
          skip_ls_id = ls_id;
          continue;
        }
      }
      ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type;
      if (OB_FAIL(ret) || !ls_handle.is_valid()) {
      } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
                tablet_id,
                tablet_handle,
                0/*timeout_us*/,
                storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(ObTenantTabletScheduler::get_co_merge_type_for_compaction(compaction_scn, *tablet_handle.get_obj(), co_major_merge_type))) {
        LOG_WARN("fail to get co merge type from medium info", K(ret), K(compaction_scn), K(tablet_handle));
      } else if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_merge_dag(ls_id, 
                                                                         *tablet_handle.get_obj(), 
                                                                         MEDIUM_MERGE, 
                                                                         compaction_scn, 
                                                                         EXEC_MODE_VALIDATE,
                                                                         nullptr, /*dag_net_id*/
                                                                         co_major_merge_type))) {
        if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
          LOG_WARN("failed to schedule validate dag", KR(tmp_ret), K(ls_id), K(tablet_id));
        }
      } else if (OB_FAIL(tablet_id_set_.set_refactored(tablet_id))) { // TODO mark when validate finish?
        LOG_WARN("failed to mark in hashset", KR(ret), K(idx), K(tablet_id));
      } else {
        LOG_INFO("success to scheudle validate dag", KR(ret), K(ls_id), K(tablet_id));
      }
    } // end of for
  }
  return ret;
}

} // compaction
} // oceanbase
