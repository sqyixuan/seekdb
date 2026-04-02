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
#include "storage/compaction/ob_update_skip_major_tablet_dag.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_merge_ctx_func.h"
#include "storage/compaction/ob_tablet_refresh_dag.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_schedule_status_cache.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "close_modules/shared_storage/storage/compaction/ob_tenant_compaction_obj_mgr.h"

namespace oceanbase
{
namespace compaction
{
ERRSIM_POINT_DEF(EN_UPDATE_SKIP_MAJOR_DAG_FAILED);
ObUpdateSkipMajorTabletTask::ObUpdateSkipMajorTabletTask()
  : ObBatchExecTask(ObITask::TASK_TYPE_UPDATE_SKIP_MAJOR_TABLET),
    update_cnt_()
{
}

int ObUpdateSkipMajorTabletTask::inner_process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  const ObUpdateSkipMajorParam &param = base_dag_->get_param();
  ObLSHandle ls_handle;
  ObBasicObjHandle<ObLSObj> ls_obj_hdl;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(param));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param.ls_id_));
  } else if (OB_UNLIKELY(!ObLSStatusCache::check_weak_read_ts_ready(param.compaction_scn_, *ls_handle.get_ls()))) {
    ret = OB_EAGAIN;
    LOG_WARN("ls weak read ts not ready, cannot schedule merge", K(ret), K(param));
  } else if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(param.ls_id_, ls_obj_hdl))) {
    LOG_WARN("failed to get ls obj handle", K(ret), K(param.ls_id_));
  } else {
    ObArenaAllocator allocator;
    int64_t succ_cnt = 0;
    const int64_t end_idx = MIN(param.tablet_info_array_.count(), get_end_idx());
    int64_t batch_finished_data_size = 0;

    for (int64_t idx = get_start_idx(); OB_SUCC(ret) && idx < end_idx; ++idx) {
      if (OB_TMP_FAIL(update_skip_major_tablet_meta(allocator,
                                                    *ls_handle.get_ls(),
                                                    *ls_obj_hdl.get_obj(),
                                                    param.compaction_scn_,
                                                    param.tablet_info_array_.at(idx),
                                                    batch_finished_data_size))) {
        LOG_WARN("failed to update tablet meta", KR(tmp_ret), "tablet_id", param.tablet_info_array_.at(idx));
      } else if (FALSE_IT(++succ_cnt)) {
      } else if (OB_FAIL(share::dag_yield())) {
        LOG_WARN("failed to dag yield", KR(ret));
      }
    }

    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("finish update skip major tablet", K(cost_ts), K(ret), "tablet_cnt", end_idx - get_start_idx(),
      K_(update_cnt), K(cost_ts));

    if (OB_SUCC(ret)) {
      (void) MTL(ObTenantCompactionProgressMgr *)->update_unfinish_tablet(param.compaction_scn_, succ_cnt, batch_finished_data_size);
    }
  }
  return ret;
}

int ObUpdateSkipMajorTabletTask::update_skip_major_tablet_meta(
    ObArenaAllocator &allocator,
    ObLS &ls,
    ObLSObj &ls_obj,
    const int64_t compaction_scn,
    const ObTabletID &tablet_id,
    int64_t &batch_finished_data_size)
{
  int ret = OB_SUCCESS;
  allocator.reuse();
  ObTabletHandle tablet_handle;
  const ObTabletTableStore *table_store = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableHandleV2 new_sstable_handle;
  ObSSTable *last_major = NULL;
  ObSSTable *new_major = NULL;
  int64_t old_root_macro_seq = 0;
  bool need_refresh_major_first = false;
  int64_t major_macro_block_count = 0;

  if (OB_FAIL(ls.get_tablet_svr()->get_tablet(tablet_id,
                                              tablet_handle,
                                              0/*timeout_us*/,
                                              storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // update tablet state later
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id), K(compaction_scn));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(tablet_id), K(compaction_scn));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(tablet_id), K(compaction_scn));
  } else if (OB_ISNULL(last_major = static_cast<ObSSTable *>(table_store->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable is unexpected null", K(ret), K(tablet_id), K(compaction_scn), KPC(table_store));
  } else if (OB_UNLIKELY(last_major->get_snapshot_version() >= compaction_scn)) {
    LOG_INFO("last major have larger compaction scn, no need to update tablet meta", KR(ret), KPC(last_major), K(compaction_scn));
  } else if (FALSE_IT(major_macro_block_count = last_major->get_total_macro_block_count())) {
  } else if (OB_FAIL(try_refresh_major_sstable(ls.get_ls_id(), tablet_id, last_major->get_snapshot_version(), need_refresh_major_first))) {
    LOG_WARN("failed to try refresh major sstable", K(ret));
  } else if (need_refresh_major_first) {
    // do nothing
  } else if (OB_FAIL(load_sstable_and_deep_copy(allocator, *last_major, new_sstable_handle))) {
    LOG_WARN("failed to load sstable and deep copy", K(ret), K(tablet_id), K(compaction_scn), KPC(last_major));
  } else if (OB_FAIL(new_sstable_handle.get_sstable(new_major))) {
    LOG_WARN("failed to get sstable from sstable handle", K(ret), K(tablet_id), K(compaction_scn), K(new_sstable_handle));
  } else if (new_major->is_co_sstable() && OB_FAIL(generate_new_co_sstable(compaction_scn, *static_cast<ObCOSSTableV2 *>(new_major)))) {
    LOG_WARN("failed to generate new co sstable", KR(ret), KPC(new_major));
  } else if (OB_FAIL(update_sstable(compaction_scn, *new_major, old_root_macro_seq))) {
    LOG_WARN("failed to update sstable", KR(ret), K(compaction_scn), KPC(new_major));
  } else if (OB_FAIL(update_tablet(allocator, old_root_macro_seq, ls, *tablet_handle.get_obj(), *new_major))) {
    LOG_WARN("failed to update tablet", K(ret), K(tablet_id), K(compaction_scn));
  }

  ObTabletCompactionState tablet_state;
  tablet_state.set_skip_scn(compaction_scn);
  if (OB_FAIL(ret) || need_refresh_major_first) {
    // do nothing
  } else if (OB_FAIL(ls_obj.cur_svr_ls_compaction_status_.update_tablet_state(tablet_id, tablet_state))) {
    LOG_WARN("failed to update tablet state", K(ret), K(tablet_id), K(compaction_scn));
  }
  if (OB_NOT_NULL(new_major)) {
    new_major->~ObSSTable();
    new_major = NULL;
  }

  if (OB_SUCC(ret)) {
    ++cnt_.success_cnt_;
    batch_finished_data_size += major_macro_block_count * DEFAULT_MACRO_BLOCK_SIZE;
  } else {
    ++cnt_.failure_cnt_;
  }
  return ret;
}

int ObUpdateSkipMajorTabletTask::try_refresh_major_sstable(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t last_major_snapshot_version,
    bool &need_refresh_major_first)
{
  int ret = OB_SUCCESS;
  need_refresh_major_first = false;
  int64_t latest_upload_major_scn = 0;
  ObTabletsRefreshSSTableParam param;
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;

  if (last_major_snapshot_version >= MERGE_SCHEDULER_PTR->get_inner_table_merged_scn()) {
    // no need to schedule refresh dag
  } else if (OB_FAIL(ObMergeCtxFunc::get_latest_upload_major_scn(tablet_id, latest_upload_major_scn))) {
    // should read meta list to decide refreshed_scn
    LOG_WARN("failed to decide refreshed scn by meta list obj", KR(ret), K(tablet_id), K(last_major_snapshot_version));
  } else if (latest_upload_major_scn <= last_major_snapshot_version) {
    // no need to schedule refresh dag
  } else if (FALSE_IT(need_refresh_major_first = true)) {
  } else if (FALSE_IT(param.compaction_scn_ = latest_upload_major_scn)) {
  } else if (OB_FAIL(ObScheduleDagFunc::schedule_tablet_refresh_dag(param, true/*is_emergency*/))) {
    LOG_WARN("failed to schedule tablet refresh dag", K(ret), K(param));
  }
  return ret;
}

int ObUpdateSkipMajorTabletTask::load_sstable_and_deep_copy(
  ObArenaAllocator &allocator,
  ObSSTable &last_major,
  ObTableHandleV2 &new_sstable_handle)
{
  int ret = OB_SUCCESS;
  ObStorageMetaHandle sstable_handle;
  ObMetaDiskAddr addr;
  addr.set_mem_addr(0, sizeof(ObSSTable));
  ObSSTable *orig_sstable = nullptr;
  ObSSTable *copied_sstable = nullptr;
  if (last_major.is_loaded()) {
    orig_sstable = &last_major;
  } else if (OB_FAIL(ObCacheSSTableHelper::load_sstable(
                 last_major.get_addr(), last_major.is_co_sstable(),
                 sstable_handle))) {
    LOG_WARN("failed to load sstable", K(ret), K(last_major));
  } else if (OB_FAIL(sstable_handle.get_sstable(orig_sstable))) {
    LOG_WARN("failed to get sstable from sstable handle", K(ret), K(sstable_handle));
  }
  if (FAILEDx(orig_sstable->copy_from_old_sstable(*orig_sstable, allocator, copied_sstable))) {
    LOG_WARN("failed to copy from old sstable", K(ret), KPC(orig_sstable), KP(copied_sstable));
  } else if (OB_FAIL(copied_sstable->set_addr(addr))) {
    LOG_WARN("failed to set sstable addr", K(ret), K(addr), KPC(copied_sstable));
  } else if (OB_FAIL(new_sstable_handle.set_sstable(copied_sstable, &allocator))) {
    LOG_WARN("failed to set sstable", K(ret), K(addr), KPC(copied_sstable));
  }
  return ret;
}

int ObUpdateSkipMajorTabletTask::update_tablet(
    common::ObArenaAllocator &allocator,
    const int64_t macro_seq,
    ObLS &ls,
    const ObTablet &old_tablet,
    const ObSSTable &new_major)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = old_tablet.get_tablet_meta().tablet_id_;
  const int64_t major_snapshot = new_major.get_key().get_snapshot_version();
  ObUpdateTableStoreParam update_param(major_snapshot,
                                       ObVersionRange::MIN_VERSION,
                                       nullptr /*storage_schema*/,
                                       ls.get_rebuild_seq(),
                                       &new_major,
                                       false /*allow_dup_major*/);
  ObStorageSchema *storage_schema = nullptr;
  bool exist_on_ss = false;

  /* step1. upload to shared storage */
  if (OB_FAIL(old_tablet.load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K(old_tablet));
  } else if (FALSE_IT(update_param.storage_schema_ = storage_schema)) {
  } else if (FALSE_IT(update_param.ddl_info_.update_with_major_flag_ = true)) {
    // download only used for update major
  } else if (OB_FAIL(update_param.init_with_compaction_info(
      ObCompactionTableStoreParam(MAJOR_MERGE, SCN::min_scn(), true/*need_report*/, false/*has_truncate_info*/)))) {
    LOG_WARN("failed to init with compaction info", KR(ret));
  } else if (OB_FAIL(ObMergeCtxFunc::check_major_tablet_exist(tablet_id, major_snapshot, ls.get_ls_epoch(), exist_on_ss))) {
    LOG_WARN("failed to check major tablet exist", KR(ret), K(tablet_id), K(major_snapshot));
  } else if (!exist_on_ss && OB_FAIL(ls.upload_major_compaction_tablet_meta(tablet_id, update_param, macro_seq))) {
    LOG_WARN("failed to upload compaction tablet meta", KR(ret), K(macro_seq));
  } else if (OB_FAIL(MTL(ObTenantStorageMetaService *)->update_shared_tablet_meta_list(tablet_id, major_snapshot))) {
    if (OB_ENTRY_EXIST == ret) {
      ++update_cnt_.ss_cnt_;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to update shared tablet meta list", KR(ret), K(tablet_id), K(major_snapshot));
    }
  } else {
    ++update_cnt_.ss_cnt_;
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_UPDATE_SKIP_MAJOR_DAG_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "ERRSIM EN_UPDATE_SKIP_MAJOR_DAG_FAILED", K(ret));
    }
  }
#endif

  /* step2. update local tablet */
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle unused_new_tablet_handle;
  if (FAILEDx(ls.update_tablet_table_store(tablet_id, update_param, unused_new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", K(ret), K(update_param), K(unused_new_tablet_handle));
  } else if (OB_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id, true/*need_diagnose*/))) {
    LOG_WARN("failed to submit tablet update task to report", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls.get_tablet_svr()->update_tablet_report_status(tablet_id))) {
    LOG_WARN("failed to update tablet report status", K(ret), K(ls_id), K(tablet_id));
  } else {
    ++update_cnt_.local_cnt_;
    LOG_INFO("success to update skip major tablet", KR(ret), K(ls_id), K(tablet_id), K(major_snapshot), K(exist_on_ss));
  }
  return ret;
}

int ObUpdateSkipMajorTabletTask::generate_new_co_sstable(
  const int64_t compaction_scn,
  ObCOSSTableV2 &new_major)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSSTableWrapper, 16> table_wrappers;
  int64_t root_macro_seq = 0;
  if (new_major.is_cgs_empty_co_table()) {
    // do nothing
  } else if (OB_FAIL(new_major.get_all_tables(table_wrappers))) {
    LOG_WARN("failed to get all tables", KR(ret), K(new_major));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < table_wrappers.count(); ++idx) {
      ObSSTable *sstable = table_wrappers.at(idx).get_sstable();
      if (OB_UNLIKELY(NULL == sstable || !sstable->is_column_store_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected sstable", KR(ret), KPC(sstable));
      } else if (sstable->get_column_group_id() != new_major.get_column_group_id() // new major will be update outside
          && OB_FAIL(update_sstable(compaction_scn, *sstable, root_macro_seq))) {
        LOG_WARN("failed to update sstable", KR(ret), K(compaction_scn), KPC(sstable));
      }
    } // end of for
  }
  return ret;
}

int ObUpdateSkipMajorTabletTask::update_sstable(
    const int64_t compaction_scn,
    blocksstable::ObSSTable &new_major,
    int64_t &old_root_macro_seq)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(new_major.get_meta(meta_handle))) {
    LOG_WARN("failed to get meta", KR(ret), K(new_major));
  } else {
    old_root_macro_seq = meta_handle.get_sstable_meta().get_basic_meta().root_macro_seq_;
    const int64_t new_root_macro_seq = old_root_macro_seq + MACRO_STEP_SIZE;
    if (OB_FAIL(new_major.modify_snapshot_and_seq(compaction_scn, new_root_macro_seq))) {
      LOG_WARN("failed to modify snapshot and seq", KR(ret), K(compaction_scn), K(new_root_macro_seq));
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
