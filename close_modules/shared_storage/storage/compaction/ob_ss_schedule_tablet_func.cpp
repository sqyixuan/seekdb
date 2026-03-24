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
#include "storage/compaction/ob_ss_schedule_tablet_func.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tablet_refresh_dag.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/compaction/ob_merge_ctx_func.h"
namespace oceanbase
{
using namespace storage;
namespace compaction
{
/********************************************ObSSTabletStatusCache impl******************************************/
int ObSSTabletStatusCache::init(
    ObLS &ls,
    const int64_t merge_version,
    const ObTablet &tablet,
    const bool is_skip_merge_tenant,
    const ObLSBroadcastInfo &broadcast_info,
    ObTabletCompactionState &tablet_state) // tablet_state used to update in mgr
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else {
    const ObLSID &ls_id = ls.get_ls_id();
    const ObTabletID &tablet_id = tablet.get_tablet_id();
    if (OB_FAIL(inner_init_state(merge_version, tablet, is_skip_merge_tenant))) {
      LOG_WARN("failed to init state", KR(ret), K(merge_version), K(ls_id), K(tablet_id));
    } else if (tablet_merge_finish_) {
      tablet_state.set_refreshed_scn(merge_version);
    } else if (can_merge()) {
      const ObLSCompactionState ls_state = broadcast_info.state_;
      if (COMPACTION_STATE_REPLICA_VERIFY == ls_state
        || COMPACTION_STATE_INDEX_VERIFY == ls_state
        || COMPACTION_STATE_RS_VERIFY == ls_state
        || COMPACTION_STATE_MAX == ls_state) {
        execute_state_ = INVALID_LS_STATE;
      } else if (COMPACTION_STATE_COMPACT == ls_state || COMPACTION_STATE_CALC_CKM == ls_state) {
        if (OB_FAIL(check_major_ckm_info(merge_version, tablet, broadcast_info, tablet_state))) {
          LOG_WARN("failed to check major ckm info", KR(ret), K(merge_version), K(ls_id), K(tablet_id),
            K(broadcast_info));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // for ss, schedule new round is depended on status of ls_obj, not used now
      is_inited_ = true;
      if (!can_merge()) {
        LOG_DEBUG("success to init tablet status", KR(ret), KPC(this), K(broadcast_info));
      }
    } else {
      inner_destroy();
    }
  }
  return ret;
}

int ObSSTabletStatusCache::check_major_ckm_info(
    const int64_t merge_version,
    const ObTablet &tablet,
    const ObLSBroadcastInfo &broadcast_info,
    ObTabletCompactionState &tablet_state)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;

  if (OB_FAIL(tablet.fetch_table_store(wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(tablet_id), K(broadcast_info));
  } else {
    const ObMajorChecksumInfo &ckm_info = wrapper.get_member()->get_major_ckm_info();
    const ObExecMode exec_mode = ckm_info.get_exec_mode();
    const int64_t ckm_compaction_scn = ckm_info.get_compaction_scn();

    if (exec_mode >= broadcast_info.get_exec_mode() && ckm_compaction_scn >= merge_version) {
      tablet_merge_finish_ = true;
      FLOG_INFO("tablet has stored major checkum info, just update tablet state", K(merge_version), K(tablet_id), K(ckm_info));
      if (is_output_exec_mode(exec_mode)) {
        tablet_state.set_output_scn(ckm_compaction_scn);
      } else if (is_calc_ckm_exec_mode(exec_mode)) {
        tablet_state.set_calc_ckm_scn(ckm_compaction_scn);
      }
    }
  }
  return ret;
}

/********************************************ObSSScheduleTabletFunc impl******************************************/
int ObSSScheduleTabletFunc::switch_ls(ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBasicScheduleTabletFunc::switch_ls(ls_handle))) {
    LOG_WARN("failed to switch ls", KR(ret), K(ls_handle));
  } else if (OB_FAIL(refresh_broadcast_info())) {
    LOG_WARN("failed to refresh_broadcast_info", KR(ret), K(ls_handle));
  }
  return ret;
}

int ObSSScheduleTabletFunc::refresh_broadcast_info()
{
  int ret = OB_SUCCESS;
  ObBasicObjHandle<ObLSObj> ls_obj_hdl;
  if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_status_.ls_id_, ls_obj_hdl))) {
    LOG_WARN("failed to get ls obj handle", K(ret), K_(ls_status));
  } else {
    ls_obj_hdl.get_obj()->get_broadcast_info(broadcast_info_);
  }
  return ret;
}

// when call schedule_tablet, ls status have been checked
int ObSSScheduleTabletFunc::schedule_tablet(
  ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_status_.ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  bool need_diagnose = false;
  ObTabletCompactionState tablet_state;
  time_guard_.click(ObCompactionScheduleTimeGuard::GET_TABLET);
  tablet_cnt_.loop_tablet_cnt_++;
  if (OB_UNLIKELY(!ls_status_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls status", KR(ret), K_(ls_status));
  } else if (OB_FAIL(tablet_status_.init(ls_status_.get_ls(), merge_version_, tablet, is_skip_merge_tenant_, broadcast_info_, tablet_state))) {
    need_diagnose = true;
    LOG_WARN("failed to init tablet status", KR(ret), K(ls_id), K(tablet_id));
  } else if (FALSE_IT(time_guard_.click(ObCompactionScheduleTimeGuard::INIT_TABLET_STATUS))) {
  } else if (tablet_status_.tablet_merge_finish()) {
    tablet_cnt_.finish_cnt_++;
    if (OB_FAIL(try_update_tablet_state(tablet, tablet_state))) {
      LOG_WARN("failed to update tablet state on cur svr", K(ret), K_(ls_status), K(tablet_id));
    }
  } else if (!tablet_status_.can_merge()) {
    need_diagnose = tablet_status_.need_diagnose();
  } else if (ls_could_schedule_merge_
      && OB_TMP_FAIL(schedule_tablet_execute(tablet))) {
    need_diagnose = true;
    LOG_WARN("failed to schedule tablet execute", KR(tmp_ret), K(ls_id), K(tablet_id));
  }
  if (need_diagnose
      && OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(ls_id, tablet_id,
                          share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE))) {
    LOG_WARN("failed to add diagnose tablet", K(tmp_ret), K(ls_id), K(tablet_id));
  }
  schedule_freeze_dag(false/*force*/);
  tablet_status_.destroy();
  return ret;
}

int ObSSScheduleTabletFunc::schedule_tablet_execute(
    const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  bool can_merge = false;
  if (OB_FAIL(check_with_schedule_scn(tablet, merge_version_, tablet_status_, can_merge))) {
    LOG_WARN("failed to check with schedule scn", KR(ret), K_(merge_version), K_(ls_status), K(tablet_id));
  } else if (can_merge && OB_TMP_FAIL(create_dag_for_different_state(tablet))) {
    if (OB_EAGAIN != tmp_ret && OB_SIZE_OVERFLOW != tmp_ret) {
      LOG_WARN("failed to create dag", KR(ret), K_(merge_version), K_(ls_status), K(tablet_id));
    }
  }
  time_guard_.click(ObCompactionScheduleTimeGuard::SCHEDULE_TABLET_EXECUTE);
  return ret;
}

int ObSSScheduleTabletFunc::create_dag_for_different_state(
  const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_status_.ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  if (COMPACTION_STATE_COMPACT == broadcast_info_.state_) {
    ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type;
    if (OB_FAIL(ObTenantTabletScheduler::get_co_merge_type_for_compaction(merge_version_, tablet, co_major_merge_type))) {
      LOG_WARN("fail to get co merge type from medium info", K(ret), K_(merge_version), K(tablet));
    } else if (OB_FAIL(ObTenantTabletScheduler::schedule_merge_dag(ls_id, 
                                                                   tablet, 
                                                                   MEDIUM_MERGE, 
                                                                   merge_version_, 
                                                                   broadcast_info_.get_exec_mode(),
                                                                   nullptr /*dag_net_id*/,
                                                                   co_major_merge_type))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("failed to schedule medium merge dag", K(ret), K_(ls_status), K(tablet_id));
      }
    } else {
      LOG_DEBUG("success to schedule medium merge dag", K(ret), K_(merge_version), K_(ls_status), K(tablet_id), K_(broadcast_info));
      ++tablet_cnt_.schedule_dag_cnt_;
    }
  } else if (COMPACTION_STATE_IDLE == broadcast_info_.state_
      || COMPACTION_STATE_REFRESH == broadcast_info_.state_) {
    ObTabletsRefreshSSTableParam refresh_param(ls_id, merge_version_);
    refresh_param.tablet_id_ = tablet_id;
    if (OB_FAIL(ObScheduleDagFunc::schedule_tablet_refresh_dag(refresh_param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to schedule tablet merge dag", K(ret), K_(ls_status), K(tablet_id));
      }
    } else {
      ++tablet_cnt_.schedule_dag_cnt_;
    }
  }
  return ret;
}

int ObSSScheduleTabletFunc::try_update_tablet_state(
  const ObTablet &tablet,
  const ObTabletCompactionState &tablet_state)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;

  if (OB_UNLIKELY(!tablet_state.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet state", KR(ret), K(tablet_state));
  } else if (OB_FAIL(ObMergeCtxFunc::update_tablet_state(ls_status_.ls_id_, tablet_id, tablet_state))) {
    LOG_WARN("failed to update tablet state", K(ret), K(tablet_id), K(tablet_state));
    ADD_SUSPECT_INFO(
            MAJOR_MERGE, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE, ls_status_.ls_id_,
            tablet_id, ObSuspectInfoType::SUSPECT_UPDATE_TALBET_STATE_FAILED,
            tablet_state.compaction_scn_, (int64_t)tablet_state.verified_,
            (int64_t)tablet_state.refreshed_);
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
