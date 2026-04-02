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
#include "storage/compaction/ob_merge_ctx_func.h"
#include "storage/compaction/ob_major_task_checkpoint_mgr.h"
#include "storage/compaction/ob_refresh_tablet_util.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "share/ob_tablet_replica_checksum_operator.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_id_obj.h"
#include "storage/compaction/ob_medium_list_checker.h"
#include "meta_store/ob_shared_storage_obj_meta.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
using namespace blocksstable;
namespace compaction
{
int ObMergeCtxFunc::get_old_root_macro_seq(
  const ObTablesHandleArray &tables_handle,
  int64_t &root_macro_seq)
{
  int ret = OB_SUCCESS;
  root_macro_seq = 0;
  ObSSTableMetaHandle meta_handle;
  const ObSSTable *sstable = nullptr;
  if (OB_UNLIKELY(tables_handle.empty() || nullptr == tables_handle.get_table(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables are invalid", KR(ret), K(tables_handle));
  } else if (FALSE_IT(sstable = static_cast<const ObSSTable *>(tables_handle.get_table(0)))) {
  } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
    LOG_WARN("get meta handle fail", K(ret), KPC(sstable));
  } else if (FALSE_IT(root_macro_seq = meta_handle.get_sstable_meta().get_basic_meta().root_macro_seq_)) {
  } else if (OB_UNLIKELY(meta_handle.get_sstable_meta().get_basic_meta().table_shared_flag_.is_shared_sstable()
      && 0 == root_macro_seq
      && meta_handle.get_sstable_meta().get_basic_meta().get_total_macro_block_count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexepcted root macro seq", KR(ret), K(root_macro_seq),
      "total_macro_cnt", meta_handle.get_sstable_meta().get_basic_meta().get_total_macro_block_count());
  }
  return ret;
}

int ObMergeCtxFunc::init_major_task_checkpoint_mgr(
    const ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t parallel_cnt,
    const int64_t cg_cnt,
    const ObTablesHandleArray &tables_handle,
    const ObExecMode exec_mode,
    ObLS &ls,
    ObIAllocator &allocator,
    ObMajorTaskCheckpointMgr &task_ckp_mgr)
{
  int ret = OB_SUCCESS;
  int64_t root_macro_seq = 0;
  bool exist = false;
  if (is_output_exec_mode(exec_mode)
      && OB_FAIL(check_major_tablet_exist(tablet_id, compaction_scn, ls.get_ls_epoch(), exist))) {
    LOG_WARN("failed to check major tablet exist", KR(ret), K(tablet_id), K(compaction_scn),
      "ls_epoch", ls.get_ls_epoch());
  } else if (exist) {
    if (OB_FAIL(ObRefreshTabletUtil::download_major_ckm_info(
        tablet_id, compaction_scn, NULL /*new_micro_info*/, ls))) {
      LOG_WARN("failed to download tablet meta", KR(ret), K(tablet_id));
    } else {
      // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
      LOG_INFO("sstable merge finished, just download major and write major ckm info to report", KR(ret),
        K(tablet_id), K(compaction_scn));
      ret = OB_NO_NEED_MERGE;
    }
  } else if (OB_FAIL(get_old_root_macro_seq(tables_handle, root_macro_seq))) {
    LOG_WARN("failed to get root macro seq", KR(ret));
  } else if (OB_FAIL(task_ckp_mgr.init(
                 tablet_id, compaction_scn,
                 parallel_cnt, cg_cnt, root_macro_seq, exec_mode, allocator))) {
    LOG_WARN("failed to init task ckp mgr", KR(ret));
  } else if (is_output_exec_mode(exec_mode)) {
    // only exec replica needs to write tablet id object
    ObTabletIDObj tablet_id_obj(tablet_id, compaction_scn, root_macro_seq, parallel_cnt, cg_cnt);
    if (OB_FAIL(ObMergeCtxFunc::write_obj_with_retry(tablet_id_obj, true/*force_write*/))) {
      LOG_WARN("failed to write obj", KR(ret), K(tablet_id_obj));
    }
  }
  return ret;
}

int ObMergeCtxFunc::validate_sstable_ckm_info(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const blocksstable::ObMajorChecksumInfo &major_ckm_info,
    const ObTabletReplicaChecksumItem &replica_ckm)
{
  int ret = OB_SUCCESS;
  const ObColumnCkmStruct *col_ckm = NULL;
  SMART_VAR(ObTabletCkmErrorInfo, error_info) {
    #define FILL_ERROR_INFO(...) \
      ADD_COMPACTION_INFO_PARAM(error_info.check_error_info_, sizeof(error_info.check_error_info_), __VA_ARGS__)
    if (OB_UNLIKELY(major_ckm_info.get_row_count() != replica_ckm.row_count_)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("checksum error! unequal row count", KR(ret), K(major_ckm_info), K(replica_ckm));
      FILL_ERROR_INFO("info", "unequal row count", "local_row_cnt", major_ckm_info.get_row_count(),
        "inner_table_row_cnt", replica_ckm.row_count_);
    } else if (OB_UNLIKELY(major_ckm_info.get_data_checksum() != replica_ckm.data_checksum_)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("checksum error! unequal data checksum", KR(ret), K(major_ckm_info), K(replica_ckm));
      FILL_ERROR_INFO("info", "unequal data_checksum", "local_data_checksum", major_ckm_info.get_data_checksum(),
        "inner_table_data_checksum", replica_ckm.data_checksum_);
    } else if (FALSE_IT(col_ckm = &major_ckm_info.get_column_checksum_struct())) {
    } else if (OB_UNLIKELY(!col_ckm->is_valid() || col_ckm->is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column checksum is invalid or empty", KR(ret), KPC(col_ckm));
    } else if (OB_UNLIKELY(col_ckm->count_ != replica_ckm.column_meta_.column_checksums_.count())) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("checksum error! unequal column checksum count", KR(ret), K(major_ckm_info), K(replica_ckm));
      FILL_ERROR_INFO("info", "unequal column checksum count",
        "local_col_checksum_cnt", col_ckm->count_,
        "inner_table_col_checksum_cnt", replica_ckm.column_meta_.column_checksums_.count());
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < col_ckm->count_; ++idx) {
        if (OB_UNLIKELY(col_ckm->column_checksums_[idx] != replica_ckm.column_meta_.column_checksums_.at(idx))) {
          ret = OB_CHECKSUM_ERROR;
          LOG_ERROR("checksum error! unequal column checksum", KR(ret), K(idx),
            "local_cal_col_ckm", col_ckm->column_checksums_[idx],
            "inner_table_col_ckm", replica_ckm.column_meta_.column_checksums_.at(idx),
            K(major_ckm_info), K(replica_ckm));
          FILL_ERROR_INFO("info", "unequal column checksum count",
            "local_cal_col_ckm", col_ckm->column_checksums_[idx],
            "inner_table_col_ckm", replica_ckm.column_meta_.column_checksums_.at(idx));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("success to validate checksum info", KR(ret), K(ls_id), K(tablet_id), "compaction_scn", major_ckm_info.get_compaction_scn());
    } else if (OB_CHECKSUM_ERROR == ret) {
      int tmp_ret = OB_SUCCESS;
      error_info.set_info(MTL_ID(), ls_id, tablet_id, major_ckm_info.get_compaction_scn());
    }
    #undef FILL_ERROR_INFO
  }
  return ret;
}

int ObMergeCtxFunc::check_major_tablet_exist(
  const ObTabletID &tablet_id,
  const int64_t compaction_scn,
  const int64_t ls_epoch,
  bool &is_exist)
{
  is_exist = false;
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  MacroBlockId object_id;
  opt.set_ss_share_tablet_meta_object_opt(tablet_id.id(), compaction_scn);
  if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, object_id))) {
    LOG_WARN("failed to get object id", KR(ret), K(opt));
  } else if (OB_FAIL(ObObjectManager::ss_is_exist_object(object_id, ls_epoch, is_exist))) {
    LOG_WARN("failed to check object exist", KR(ret), K(opt), K(object_id));
  }
  return ret;
}

int ObMergeCtxFunc::validate_sstable_ckm_info(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const blocksstable::ObMajorChecksumInfo &major_ckm_info,
    const share::ObTabletReplicaChecksumItem &item)
{
  int ret = OB_SUCCESS;
  #define PRINT_WARN_LOG(str) LOG_WARN_RET(ret, str, K(ls_id), K(tablet_id), K(compaction_scn), K(major_ckm_info), K(item))
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || compaction_scn <= 0
      || major_ckm_info.is_empty() || !item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PRINT_WARN_LOG("invalid argument");
  } else if (OB_UNLIKELY(major_ckm_info.get_compaction_scn() != compaction_scn
      || item.compaction_scn_.get_val_for_tx() != compaction_scn
      || item.tablet_id_ != tablet_id
      || item.ls_id_ != ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    PRINT_WARN_LOG("checksum info not match");
  } else if (OB_FAIL(validate_sstable_ckm_info(ls_id, tablet_id, major_ckm_info, item))) {
    PRINT_WARN_LOG("failed to validate ckm");
  }
  #undef PRINT_WARN_LOG
  return ret;
}

int ObMergeCtxFunc::update_tablet_state(
  const share::ObLSID &ls_id,
  const common::ObTabletID &tablet_id,
  const ObTabletCompactionState &tmp_state,
  const ObNewMicroInfo *input_new_micro_info)
{
  int ret = OB_SUCCESS;
  ObBasicObjHandle<ObLSObj> ls_obj_hdl;
  if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_id, ls_obj_hdl))) {
    LOG_WARN("failed to get ls obj handle", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls_obj_hdl.get_obj()->cur_svr_ls_compaction_status_.update_tablet_state(
      tablet_id, tmp_state, input_new_micro_info))) {
    LOG_WARN("failed to update tablet state on cur server", KR(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObMergeCtxFunc::write_obj_with_retry(
    ObCompactionObjInterface &obj,
    const bool force_write)
{
  int ret = OB_SUCCESS;
  ObCompactionObjBuffer obj_buf;
  int64_t retry_times = 0;
  if (OB_FAIL(!obj.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(obj));
  } else if (OB_FAIL(obj_buf.init())) {
    LOG_WARN("failed to init obj buf", KR(ret));
  }
  while (OB_SUCC(ret) && (force_write || (retry_times < UPDATE_INNER_TABLE_MAX_RETRY_TIMES))) {
    if (OB_FAIL(obj.write_object(obj_buf))) {
      LOG_WARN("failed to write obj", KR(ret), K(obj));
    } else {
      LOG_INFO("success to write obj", KR(ret), K(obj), K(retry_times));
      break;
    }
    ++retry_times;
    ret = OB_SUCCESS;
    USLEEP(200_ms);
  } // end of while
  return ret;
}

int ObMergeCtxFunc::check_medium_info(
    storage::ObLS &ls,
    const ObMediumCompactionInfo &next_medium_info,
    const int64_t last_major_snapshot,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  if (OB_FAIL(ObMediumListChecker::check_next_schedule_medium_for_ss(
          next_medium_info, last_major_snapshot))) {
    if (next_medium_info.last_medium_snapshot_ > last_major_snapshot) {
      // means have major not refreshed
      const ObDownloadTabletMetaParam download_tablet_meta_param(
                                      next_medium_info.last_medium_snapshot_/*snapshot_version*/,
                                      false/*allow_dup_major*/,
                                      false/*init_major_ckm_info*/,
                                      true/*need_prewarm*/);
      if (OB_FAIL(ObRefreshTabletUtil::download_major_compaction_tablet_meta(
              ls, tablet_id, download_tablet_meta_param))) {
        LOG_WARN("failed to download tablet meta", K(ret), K(ls_id), K(tablet_id), K(download_tablet_meta_param));
      } else {
        ret = OB_NO_NEED_MERGE;
        // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
        LOG_INFO("refresh old major sstable to new tablet, schedule dag later", KR(ret), K(ls_id), K(tablet_id),
          "refresh_scn", next_medium_info.last_medium_snapshot_);
#ifdef ERRSIM
        SERVER_EVENT_SYNC_ADD("ss_merge_errsim", "follower_refresh_major",
                              "refresh_scn", next_medium_info.last_medium_snapshot_,
                              "ls_id", ls.get_ls_id(), "tablet_id", tablet_id);
#endif
      }
    } else {
      LOG_WARN("failed to check medium info", KR(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObMergeCtxFunc::get_latest_upload_major_scn(
  const ObTabletID &tablet_id,
  int64_t &latest_upload_major_scn)
{
  int ret = OB_SUCCESS;
  latest_upload_major_scn = 0;

  ObGCTabletMetaInfoList tablet_meta_version_list;
  if (OB_FAIL(MTL(ObTenantStorageMetaService*)->get_gc_tablet_scn_arr(
    tablet_id, blocksstable::ObStorageObjectType::SHARED_MAJOR_META_LIST, tablet_meta_version_list))) {
    LOG_WARN("failed to get tablet version list", KR(ret), K(tablet_id));
  } else {
    const ObIArray<ObGCTabletMetaInfo> &array = tablet_meta_version_list.tablet_version_arr_;
    if (array.count() > 0) {
      latest_upload_major_scn = array.at(array.count() - 1).scn_.get_val_for_tx();
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
