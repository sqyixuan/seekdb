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

#define USING_LOG_PREFIX STORAGE

#include "ob_direct_load_mgr.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_refresh_tablet_util.h"
#include "close_modules/shared_storage/share/compaction/ob_shared_storage_compaction_util.h"
#endif
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;


int ObTabletFullDirectLoadMgrV2::update(
    ObTabletDirectLoadMgr *lob_tablet_mgr,
    const ObTabletDirectLoadInsertParam &build_param)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObArenaAllocator arena_allocator("TDL_UPDATE_SS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  ObTabletHandle tablet_handle; // get_tablet_handle should be used after updated
  if (OB_UNLIKELY(!build_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(build_param));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(build_param.common_param_.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(build_param));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               build_param.common_param_.tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(build_param));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(arena_allocator, storage_schema))) {
    LOG_WARN("load storage schema failed", K(ret));
  } else if (FALSE_IT(start_scn_ = tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_)) {
  } else if (nullptr != lob_tablet_mgr) {
    // has lob
    ObTabletDirectLoadInsertParam lob_param;
    ObSchemaGetterGuard schema_guard;
    ObTabletBindingMdsUserData ddl_data;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(lob_param.assign(build_param))) {
      LOG_WARN("assign lob parameter failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
      LOG_WARN("get ddl data failed", K(ret));
    } else if (OB_FALSE_IT(lob_param.common_param_.tablet_id_ = ddl_data.lob_meta_tablet_id_)) {
    } else if (build_param.is_replay_) {
      // no need to update table id.
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
      MTL_ID(), schema_guard, lob_param.runtime_only_param_.schema_version_))) {
      LOG_WARN("get tenant schema failed", K(ret), K(MTL_ID()), K(lob_param));
    } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(),
              lob_param.runtime_only_param_.table_id_, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(lob_param));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(lob_param));
    } else {
      lob_param.runtime_only_param_.table_id_ = table_schema->get_aux_lob_meta_tid();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(lob_mgr_handle_.set_obj(lob_tablet_mgr))) {
      LOG_WARN("set lob direct load mgr failed", K(ret), K(lob_param));
    } else if (OB_FAIL(lob_mgr_handle_.get_obj()->update(nullptr, lob_param))) {
      LOG_WARN("init lob failed", K(ret), K(lob_param));
    } else {
      LOG_INFO("[SHARED STORAGE]set lob mgr handle", K(lob_param));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTabletDirectLoadMgr::update(nullptr, build_param))) {
      LOG_WARN("init failed", K(ret), K(build_param));
    } else if (OB_FAIL(slice_group_.init(build_param.runtime_only_param_.task_cnt_))) {
      LOG_WARN("fail to init slice group", K(ret), K(build_param.runtime_only_param_.task_cnt_));
    } else {
      sqc_build_ctx_.reset_slice_ctx_on_demand();
      table_key_.reset();
      table_key_.tablet_id_ = build_param.common_param_.tablet_id_;
      bool is_column_group_store = false;
      if (OB_FAIL(ObCODDLUtil::need_column_group_store(*storage_schema, is_column_group_store))) {
        LOG_WARN("fail to get schema is column group store", K(ret));
      } else if (is_column_group_store) {
        table_key_.table_type_ = ObITable::COLUMN_ORIENTED_SSTABLE;
        int64_t base_cg_idx = -1;
        if (OB_FAIL(ObCODDLUtil::get_base_cg_idx(storage_schema, base_cg_idx))) {
          LOG_WARN("get base cg idx failed", K(ret));
        } else {
          table_key_.column_group_idx_ = static_cast<uint16_t>(base_cg_idx);
        }
      } else {
        table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
      }
      table_key_.version_range_.snapshot_version_ = build_param.common_param_.read_snapshot_;
    }
  }

  if (OB_SUCC(ret)) {
    task_cnt_ = build_param.runtime_only_param_.task_cnt_;
    cg_cnt_ = storage_schema->get_column_group_count();
    is_no_logging_  = build_param.common_param_.is_no_logging_;
    if (OB_FAIL(ObDDLRedoLogWriter::write_gc_flag(tablet_handle, table_key_, task_cnt_, cg_cnt_))) {
      LOG_WARN("failed to write gc flag", K(ret));
    }
  }
  ObTabletObjLoadHelper::free(arena_allocator, storage_schema);
  LOG_INFO("[SHARED STORAGE]init tablet direct load mgr finished", K(ret), K(build_param), K(lbt()), KPC(this));
  return ret;
}

int ObTabletFullDirectLoadMgrV2::open(const int64_t current_execution_id, share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  uint32_t lock_tid = 0;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletFullDirectLoadMgr *lob_tablet_mgr = nullptr;
  start_scn.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid() || !sqc_build_ctx_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_FAIL(wrlock(TRY_LOCK_TIMEOUT, lock_tid))) {
    LOG_WARN("failed to wrlock", K(ret), KPC(this));
  } else if (lob_mgr_handle_.is_valid()
    && OB_ISNULL(lob_tablet_mgr = lob_mgr_handle_.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls service should not be null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id_, tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", K(ret), K(tablet_handle));
  } else {
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    ObDDLKvMgrHandle lob_kv_mgr_handle;
    ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
    if (OB_FAIL(direct_load_mgr_handle.set_obj(this))) {
      LOG_WARN("set handle failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
      LOG_WARN("create ddl kv mgr failed", K(ret));
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->add_idempotence_checker())) {
      LOG_WARN("add idempotence checker failed", K(ret));
    } else if (nullptr != lob_tablet_mgr) {
      ObTabletHandle lob_tablet_handle;
      if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, lob_tablet_mgr->get_tablet_id(), lob_tablet_handle))) {
        LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), KPC(lob_tablet_mgr));
      } else if (OB_FAIL(lob_tablet_handle.get_obj()->get_ddl_kv_mgr(lob_kv_mgr_handle, true/*try_create*/))) {
        LOG_WARN("create ddl kv mgr failed", K(ret));
      } else if (OB_FAIL(lob_kv_mgr_handle.get_obj()->add_idempotence_checker())) {
        LOG_WARN("add idempotence checker failed", K(ret));
      }
    }
  }
  if (lock_tid != 0) {
    unlock(lock_tid);
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::close(const int64_t current_execution_id, bool need_report_checksum)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_TABLET_FULL_DIRECT_LOAD_MGR_CLOSE);
  if (lob_mgr_handle_.is_valid()) {
    // create lob major
    ObTabletFullDirectLoadMgrV2 *lob_mgr = static_cast<ObTabletFullDirectLoadMgrV2 *>(lob_mgr_handle_.get_obj());
    /* sometimes empty lob sstable may exist
     * set a defensive action when don't execute open sstable slice for lob direct load mgr
     */
    if (OB_FAIL(lob_mgr->prepare_schema_item_on_demand(lob_mgr->sqc_build_ctx_.build_param_.runtime_only_param_.table_id_, 
                                                       lob_mgr->sqc_build_ctx_.build_param_.runtime_only_param_.parallel_))) {
      LOG_WARN("failed to prepare schema item on demand", K(ret));
    } else if (OB_FAIL(lob_mgr->close(current_execution_id, false /*checkusum_report*/))) {
      LOG_WARN("close lob mgr failed", K(ret), KPC(lob_mgr));
    }
  }

  if (OB_SUCC(ret)) {
    ObTablet *tablet = nullptr;
    ObStorageSchema *storage_schema = nullptr;
    ObArenaAllocator arena_allocator("TDL_SSTable_SS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    bool is_column_group_store = false;
    ObTableHandleV2 sstable_handle;
    ObSSTable *ddl_major_sstable = nullptr;
    ObSSTableMetaHandle sst_meta_hdl;
    ObTabletHandle tablet_handle;
    if (OB_FAIL(get_tablet_handle(tablet_handle))) {
    } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_FAIL(tablet->load_storage_schema(arena_allocator, storage_schema))) {
      LOG_WARN("load storage schema failed", K(ret), K(tablet_id_));
    } else if (FALSE_IT(storage_schema->schema_version_ = sqc_build_ctx_.build_param_.runtime_only_param_.schema_version_)) {
    } else if (OB_FAIL(ObCODDLUtil::need_column_group_store(*storage_schema, is_column_group_store))) {
      LOG_WARN("fail to check is column group store", K(ret));
    } else if (!is_column_group_store) {
      if (OB_FAIL(create_ddl_ro_sstable(*tablet,
                                        storage_schema,
                                        arena_allocator,
                                        sstable_handle))) {
        LOG_WARN("fail to create ddl ro sstable", K(ret), K(tablet_id_), KPC(sqc_build_ctx_.index_builder_));
      } else {
        LOG_INFO("[SHARED STORAGE]create ddl ro sstable success", K(tablet_id_));
      }
    } else {
      if (OB_FAIL(create_ddl_co_sstable(*tablet,
                                        storage_schema,
                                        arena_allocator,
                                        sstable_handle))) {
        LOG_WARN("fail to create ddl co sstable", K(ret));
      } else {
        LOG_INFO("[SHARED STORAGE]create ddl co sstable success", K(tablet_id_));
      }
    }

    // update table_store
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sstable_handle.get_sstable(ddl_major_sstable))) {
      LOG_WARN("fail to get sstable", K(ret), K(sstable_handle));
    } else if (OB_FAIL(update_table_store(ddl_major_sstable,
                                          storage_schema,
                                          table_key_,
                                          *tablet))) {
      LOG_WARN("fail to update table_store", K(ret), K(table_key_), KPC(ddl_major_sstable));
    }

    // sync max lob id
    if (OB_FAIL(ret)) {
    } else if (!lob_mgr_handle_.is_valid()) {
    } else {
      const ObTabletID lob_meta_tablet_id = lob_mgr_handle_.get_obj()->get_tablet_id();
      const int64_t last_lob_id = static_cast<ObTabletFullDirectLoadMgrV2 *>(lob_mgr_handle_.get_obj())->get_last_lob_id();
      if (OB_FAIL(ObDDLUtil::set_tablet_autoinc_seq(ls_id_, lob_meta_tablet_id, last_lob_id))) {
        LOG_WARN("set lob tablet autoinc seq failed", K(ret), K(ls_id_), K(lob_meta_tablet_id), K(last_lob_id));
      }
    }

    // report checksum
    if (OB_FAIL(ret) || !need_report_checksum) {
    } else if (OB_FAIL(ddl_major_sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else {
      const int64_t *column_checksums = sst_meta_hdl.get_sstable_meta().get_col_checksum();
      int64_t column_count = sst_meta_hdl.get_sstable_meta().get_col_checksum_cnt();
      ObArray<int64_t> co_column_checksums;
      co_column_checksums.set_attr(ObMemAttr(MTL_ID(), "TblDL_Ccc"));
      if (OB_FAIL(ObCODDLUtil::get_co_column_checksums_if_need(tablet_handle, ddl_major_sstable, co_column_checksums))) {
        LOG_WARN("get column checksum from co sstable failed", K(ret));
      } else {
        for (int64_t retry_cnt = 10;  OB_SUCC(ret) && retry_cnt > 0; retry_cnt--) { // overwrite ret
          if (OB_FAIL(ObTabletDDLUtil::report_ddl_checksum(
                  ls_id_,
                  tablet_id_,
                  sqc_build_ctx_.build_param_.runtime_only_param_.table_id_,
                  current_execution_id,
                  sqc_build_ctx_.build_param_.runtime_only_param_.task_id_,
                  co_column_checksums.empty() ? column_checksums : co_column_checksums.get_data(),
                  co_column_checksums.empty() ? column_count : co_column_checksums.count(),
                  data_format_version_))) {
            LOG_WARN("report ddl column checksum failed", K(ret), K(ls_id_), K(tablet_id_), K(current_execution_id), K(sqc_build_ctx_));
          } else {
            break;
          }
          ob_usleep(100L * 1000L);
        }
      }
    }
    if (OB_NOT_NULL(storage_schema)) {
      ObTabletObjLoadHelper::free(arena_allocator, storage_schema);
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::open_sstable_slice(
    const bool is_data_tablet_process_for_lob,
    const blocksstable::ObMacroDataSeq &start_seq,
    const ObDirectLoadSliceInfo &slice_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!start_seq.is_valid() || !slice_info.is_valid() || !sqc_build_ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tablet_id_), K(start_seq), K(slice_info), K(sqc_build_ctx_));
  } else if (is_data_tablet_process_for_lob) {
    if (OB_UNLIKELY(!lob_mgr_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), KPC(this));
    } else if (OB_FAIL(lob_mgr_handle_.get_obj()->open_sstable_slice(
        false, start_seq, slice_info))) {
      LOG_WARN("open sstable slice for lob failed", K(ret), KPC(this));
    }
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_FAIL(prepare_schema_item_on_demand(sqc_build_ctx_.build_param_.runtime_only_param_.table_id_,
				  		   sqc_build_ctx_.build_param_.runtime_only_param_.parallel_))) {
    LOG_WARN("prepare table schema item on demand", K(ret), K(sqc_build_ctx_.build_param_));
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_ISNULL(slice_writer = OB_NEWx(ObDirectLoadSliceWriter, (&sqc_build_ctx_.slice_writer_allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadSliceWriter", KR(ret));
    } else if (OB_FAIL(slice_writer->init(this, start_seq, slice_info.slice_idx_, slice_info.merge_slice_idx_))) {
      LOG_WARN("init sstable slice writer failed", K(ret), KPC(this));
    } else if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.set_refactored(slice_key, slice_writer))) {
      LOG_WARN("set refactored failed", K(ret), K(slice_info), KPC(this));
    } else {
      LOG_INFO("[SHARED STORAGE]add a slice writer", KP(slice_writer), K(slice_info), K(sqc_build_ctx_.slice_mgr_map_.size()), K(tablet_id_));
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(slice_writer)) {
        slice_writer->~ObDirectLoadSliceWriter();
        sqc_build_ctx_.slice_writer_allocator_.free(slice_writer);
        slice_writer = nullptr;
      }
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::fill_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    const SCN &start_scn,
    ObIStoreRowIterator *iter,
    int64_t &affected_rows,
    ObInsertMonitor *insert_monitor)
{ 
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !sqc_build_ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info), K(sqc_build_ctx_));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      LOG_WARN("get refactored failed", K(ret), K(slice_info));
    } else if (OB_ISNULL(slice_writer) || OB_UNLIKELY(!ATOMIC_LOAD(&is_schema_item_ready_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info), K(is_schema_item_ready_));
    } else if (OB_FAIL(slice_writer->fill_sstable_slice(start_scn, sqc_build_ctx_.build_param_.runtime_only_param_.table_id_, tablet_id_,
        sqc_build_ctx_.storage_schema_, iter, schema_item_, direct_load_type_, column_items_, dir_id_,
	sqc_build_ctx_.build_param_.runtime_only_param_.parallel_, slice_info.context_id_, affected_rows, insert_monitor))) {
      LOG_WARN("fill sstable slice failed", K(ret), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
    // cleanup when failed.
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_TMP_FAIL(sqc_build_ctx_.slice_mgr_map_.erase_refactored(slice_key, &slice_writer))) {
      LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
    } else {
      LOG_INFO("[SHARED STORAGE]erase a slice writer", KP(slice_writer), "slice_id", slice_info.slice_id_, K(sqc_build_ctx_.slice_mgr_map_.size()));
      slice_writer->~ObDirectLoadSliceWriter();
      sqc_build_ctx_.slice_writer_allocator_.free(slice_writer);
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::fill_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    const SCN &start_scn,
    const ObBatchDatumRows &datum_rows,
    ObInsertMonitor *insert_monitor)
{ 
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !sqc_build_ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info), K(sqc_build_ctx_));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      LOG_WARN("get refactored failed", K(ret), K(slice_info));
    } else if (OB_ISNULL(slice_writer) || OB_UNLIKELY(!ATOMIC_LOAD(&is_schema_item_ready_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info), K(is_schema_item_ready_));
    } else if (OB_FAIL(slice_writer->fill_sstable_slice(start_scn, sqc_build_ctx_.build_param_.runtime_only_param_.table_id_, tablet_id_,
        sqc_build_ctx_.storage_schema_, datum_rows, schema_item_, direct_load_type_, column_items_, dir_id_,
	sqc_build_ctx_.build_param_.runtime_only_param_.parallel_, slice_info.context_id_, insert_monitor))) {
      LOG_WARN("fill sstable slice failed", K(ret), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
    // cleanup when failed.
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_TMP_FAIL(sqc_build_ctx_.slice_mgr_map_.erase_refactored(slice_key, &slice_writer))) {
      LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
    } else {
      LOG_INFO("[SHARED STORAGE]erase a slice writer", KP(slice_writer), "slice_id", slice_info.slice_id_, K(sqc_build_ctx_.slice_mgr_map_.size()));
      slice_writer->~ObDirectLoadSliceWriter();
      sqc_build_ctx_.slice_writer_allocator_.free(slice_writer);
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::fill_lob_sstable_slice(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info,
    const SCN &start_scn,
    share::ObTabletCacheInterval &pk_interval,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  share::SCN commit_scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !sqc_build_ctx_.is_valid() ||
      !lob_mgr_handle_.is_valid() || !lob_mgr_handle_.get_obj()->get_sqc_build_ctx().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info), "lob_direct_load_mgr is valid", lob_mgr_handle_.is_valid(), KPC(this));
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    const int64_t trans_version = is_full_direct_load(direct_load_type_) ? table_key_.get_snapshot_version() : INT64_MAX;
    ObBatchSliceWriteInfo info(tablet_id_, ls_id_, trans_version, direct_load_type_, sqc_build_ctx_.build_param_.runtime_only_param_.trans_id_,
        sqc_build_ctx_.build_param_.runtime_only_param_.seq_no_, slice_info.src_tenant_id_, sqc_build_ctx_.build_param_.runtime_only_param_.tx_desc_);
   ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
   if (OB_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      LOG_WARN("get refactored failed", K(ret), K(slice_info), K(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.size()));
    } else if (OB_ISNULL(slice_writer) || OB_UNLIKELY(!lob_mgr_handle_.get_obj()->is_schema_item_ready())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info), K(lob_mgr_handle_.get_obj()->is_schema_item_ready()));
    } else if (OB_FAIL(slice_writer->fill_lob_sstable_slice(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().build_param_.runtime_only_param_.table_id_, allocator, sqc_build_ctx_.allocator_, 
          start_scn, info, pk_interval, lob_column_idxs_, lob_col_types_, schema_item_, datum_row))) {
        LOG_WARN("fail to fill batch sstable slice", K(ret), K(start_scn), K(tablet_id_), K(pk_interval));
    }
  }

  if (OB_FAIL(ret) && lob_mgr_handle_.is_valid()) {
    // cleanup when failed.
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_TMP_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.erase_refactored(slice_key, &slice_writer))) {
      LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
    } else {
      LOG_INFO("[SHARED STORAGE]erase a slice writer", KP(slice_writer), "slice_id", slice_info.slice_id_, K(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.size()));
      slice_writer->~ObDirectLoadSliceWriter();
      lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_writer_allocator_.free(slice_writer);
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::fill_lob_sstable_slice(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info,
    const SCN &start_scn,
    share::ObTabletCacheInterval &pk_interval,
    blocksstable::ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  share::SCN commit_scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !sqc_build_ctx_.is_valid() ||
      !lob_mgr_handle_.is_valid() || !lob_mgr_handle_.get_obj()->get_sqc_build_ctx().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info), "lob_direct_load_mgr is valid", lob_mgr_handle_.is_valid(), KPC(this));
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    const int64_t trans_version = is_full_direct_load(direct_load_type_) ? table_key_.get_snapshot_version() : INT64_MAX;
    ObBatchSliceWriteInfo info(tablet_id_, ls_id_, trans_version, direct_load_type_, sqc_build_ctx_.build_param_.runtime_only_param_.trans_id_,
        sqc_build_ctx_.build_param_.runtime_only_param_.seq_no_, slice_info.src_tenant_id_, sqc_build_ctx_.build_param_.runtime_only_param_.tx_desc_);
   ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
   if (OB_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      LOG_WARN("get refactored failed", K(ret), K(slice_info), K(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.size()));
    } else if (OB_ISNULL(slice_writer) || OB_UNLIKELY(!lob_mgr_handle_.get_obj()->is_schema_item_ready())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info), K(lob_mgr_handle_.get_obj()->is_schema_item_ready()));
    } else if (OB_FAIL(slice_writer->fill_lob_sstable_slice(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().build_param_.runtime_only_param_.table_id_, allocator, sqc_build_ctx_.allocator_, 
          start_scn, info, pk_interval, lob_column_idxs_, lob_col_types_, schema_item_, datum_rows))) {
        LOG_WARN("fail to fill batch sstable slice", K(ret), K(start_scn), K(tablet_id_), K(pk_interval));
    }
  }

  if (OB_FAIL(ret) && lob_mgr_handle_.is_valid()) {
    // cleanup when failed.
    int tmp_ret = OB_SUCCESS;
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_TMP_FAIL(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.erase_refactored(slice_key, &slice_writer))) {
      LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
    } else {
      LOG_INFO("[SHARED STORAGE]erase a slice writer", KP(slice_writer), "slice_id", slice_info.slice_id_, K(lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_mgr_map_.size()));
      slice_writer->~ObDirectLoadSliceWriter();
      lob_mgr_handle_.get_obj()->get_sqc_build_ctx().slice_writer_allocator_.free(slice_writer);
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::update_max_macro_seq(const int64_t macro_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // no need check input macro_seq
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    last_data_seq_ = max(last_data_seq_, macro_seq);
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::fill_batch_slice_cg(
    const int64_t context_id,
    const ObArray<int64_t> &slice_id_array,
    const ObStorageSchema *storage_schema,
    const share::SCN &start_scn,
    int64_t &last_seq,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  last_seq = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage schema", K(ret), KP(storage_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < slice_id_array.count(); ++i) {
      ObDirectLoadSliceWriter *slice_writer = nullptr;
      ObTabletDirectLoadBuildCtx::SliceKey slice_key(context_id, slice_id_array.at(i));
      if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
        ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
        LOG_WARN("get refactored failed", K(ret), K(slice_id_array), K(i));
      } else if (OB_ISNULL(slice_writer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), K(slice_id_array), K(i));
      } else if (OB_FAIL(slice_writer->fill_column_group(storage_schema, start_scn, insert_monitor))) {
        LOG_WARN("slice writer fill column group failed", K(ret));
      } else {
        last_seq = slice_writer->get_next_block_start_seq();
      }
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::close_sstable_slice(
    const bool is_data_tablet_process_for_lob,
    const ObDirectLoadSliceInfo &slice_info,
    const share::SCN &start_scn,
    const int64_t execution_id,
    ObInsertMonitor *insert_monitor,
    blocksstable::ObMacroDataSeq &next_seq)
{
  int ret = OB_SUCCESS;
  next_seq.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !sqc_build_ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info), K(sqc_build_ctx_));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (is_data_tablet_process_for_lob) {
    if (OB_UNLIKELY(!lob_mgr_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info));
    } else {
      ObTabletFullDirectLoadMgrV2 *lob_mgr = static_cast<ObTabletFullDirectLoadMgrV2 *>(lob_mgr_handle_.get_obj());
      if (OB_FAIL(lob_mgr->close_sstable_slice(false, slice_info, start_scn, execution_id, nullptr/*insert_monitor*/, next_seq))) {
        LOG_WARN("close lob sstable slice failed", K(ret), K(slice_info), K(start_scn), K(execution_id));
      }
    }
  } else {
    ObDirectLoadSliceWriter *slice_writer = nullptr;
    int64_t last_seq = 0;

    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    if (OB_FAIL(sqc_build_ctx_.slice_mgr_map_.get_refactored(slice_key, slice_writer))) {
      ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
      LOG_WARN("get refactored failed", K(ret), K(slice_info));
    } else if (OB_ISNULL(slice_writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(slice_info));
    } else if (OB_FAIL(slice_writer->close())) {
      LOG_WARN("close failed", K(ret), K(slice_info));
    } else if (OB_FALSE_IT(next_seq = slice_writer->get_next_block_start_seq())) {
    } else if (OB_FAIL(update_max_macro_seq(last_seq))) {
      LOG_WARN("update max macro seq failed", K(ret), K(last_seq));
    } else if (!slice_info.is_lob_slice_ && is_ddl_direct_load(direct_load_type_)) {
      ObTabletDirectLoadBatchSliceKey cur_key(tablet_id_);
      ObArray<int64_t> slice_id_array;
      if (OB_FAIL(slice_group_.record_slice_id(cur_key, slice_info.slice_id_))) {
        LOG_WARN("fail to put new slice id", K(ret), K(slice_info.slice_id_), K(cur_key));
      } else if (slice_info.is_task_finish_) {
        int64_t task_finish_count = ATOMIC_AAF(&sqc_build_ctx_.task_finish_count_, 1);
        LOG_INFO("[SHARED STORAGE]inc task finish count", K(tablet_id_), K(task_finish_count), K(sqc_build_ctx_.task_total_cnt_));
        ObTablet *tablet = nullptr;
        ObTabletHandle tablet_handle;
        ObStorageSchema *storage_schema = nullptr;
        ObArenaAllocator arena_allocator("DDL_RESCAN_SS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        bool is_column_group_store = false;
        if (OB_FAIL(get_tablet_handle(tablet_handle))) {
          LOG_WARN("failed to get tablet handle", K(ret));
        } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
        } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet is null", K(ret), K(ls_id_), K(tablet_id_));
        } else if (OB_FAIL(tablet->load_storage_schema(arena_allocator, storage_schema))) {
          LOG_WARN("load storage schema failed", K(ret), K(tablet_id_));
        } else if (OB_FAIL(ObCODDLUtil::need_column_group_store(*storage_schema, is_column_group_store))) {
          LOG_WARN("fail to check is column group store", K(ret));
        } else if (!is_column_group_store) {
          if (task_finish_count >= sqc_build_ctx_.task_total_cnt_) {
            if (OB_FAIL(close(execution_id))) {
              LOG_WARN("close sstable slice failed", K(ret));
            }
          }
        } else if (OB_FAIL(slice_group_.get_slice_array(cur_key, slice_id_array))) {
          LOG_WARN("fail to get slice array", K(ret), K(cur_key));
        } else {
          last_seq = -1;
          if (task_finish_count < sqc_build_ctx_.task_total_cnt_) {
            if (OB_FAIL(wait_notify(slice_writer))) {
              LOG_WARN("wait notify failed", K(ret));
            } else if (OB_FAIL(fill_batch_slice_cg(slice_info.context_id_, slice_id_array, storage_schema, start_scn, last_seq, insert_monitor))) {
              LOG_WARN("fail to fill batch slice cg", K(ret));
            }
          } else {
            if (OB_FAIL(calc_range(slice_info.context_id_, 0))) {
              LOG_WARN("calc range failed", K(ret));
            } else if (OB_FAIL(notify_all())) {
              LOG_WARN("notify all failed", K(ret));
            } else if (OB_FAIL(fill_batch_slice_cg(slice_info.context_id_, slice_id_array, storage_schema, start_scn, last_seq, insert_monitor))) {
              LOG_WARN("fail to fill batch slice cg", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            int64_t fill_cg_finish_count = ATOMIC_AAF(&sqc_build_ctx_.fill_column_group_finish_count_, 1);
            LOG_INFO("[SHARED STORAGE]inc fill cg finish count", K(tablet_id_), K(fill_cg_finish_count), K(sqc_build_ctx_.task_total_cnt_));
            if (OB_FAIL(update_max_macro_seq(last_seq))) {
              LOG_WARN("update max macro seq failed", K(ret), K(last_seq));
            } else if (fill_cg_finish_count >= sqc_build_ctx_.task_total_cnt_) {
              if (OB_FAIL(close(execution_id))) {
                LOG_WARN("close sstable slice failed", K(ret));
              }
            }
          }
        }
        ObTabletObjLoadHelper::free(arena_allocator, storage_schema);
      }
    }

    if (!slice_info.is_lob_slice_ && is_ddl_direct_load(direct_load_type_)) {
      if (!slice_info.is_task_finish_) {
        // do nothing, wait until all slices finish
      } else {
        ObArray<int64_t> cur_slice_array;
        ObTabletDirectLoadBatchSliceKey key(tablet_id_);
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(slice_group_.get_slice_array(key, cur_slice_array))) {
          LOG_WARN("fail to get slice array", K(ret), K(key));  
        } else {
          for (int64_t i = 0; i < cur_slice_array.count(); ++i) {
            int64_t slice_id = cur_slice_array.at(i);
            ObDirectLoadSliceWriter *cur_writer = nullptr;
    	      ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_id);
            if (OB_TMP_FAIL(sqc_build_ctx_.slice_mgr_map_.erase_refactored(slice_key, &cur_writer))) {
              LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
            } else {
              LOG_INFO("[SHARED STORAGE]erase a slice writer", K(ret), KP(cur_writer), K(sqc_build_ctx_.slice_mgr_map_.size()));
              cur_writer->~ObDirectLoadSliceWriter();
              sqc_build_ctx_.slice_writer_allocator_.free(cur_writer);
              cur_writer = nullptr;
            }
          }
          cur_slice_array.reset();
          if (OB_TMP_FAIL(slice_group_.remove_slice_array(key))) {
            LOG_WARN("fail to remove slice array", K(ret), K(tmp_ret), K(key));
          }
        }
      }
    } else if (OB_NOT_NULL(slice_writer)) {
      if (OB_SUCC(ret) && is_data_direct_load(direct_load_type_) && slice_writer->need_column_store()) {
        // for direct_load with column_store, ignore, free after rescan
      } else {
        // for direct_load with row_store or fill fail
        int tmp_ret = OB_SUCCESS;
    	ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
        if (OB_TMP_FAIL(sqc_build_ctx_.slice_mgr_map_.erase_refactored(slice_key))) {
          LOG_ERROR("erase failed", K(ret), K(tmp_ret), K(slice_info));
        } else {
          LOG_INFO("[SHARED STORAGE]erase a slice writer", K(ret), KP(slice_writer), K(sqc_build_ctx_.slice_mgr_map_.size()));
          slice_writer->~ObDirectLoadSliceWriter();
          sqc_build_ctx_.slice_writer_allocator_.free(slice_writer);
          slice_writer = nullptr;
        }
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::wait_notify(const ObDirectLoadSliceWriter *slice_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(slice_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(slice_writer));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else {
        ObThreadCondGuard guard(cond_);
        if (OB_FAIL(guard.get_ret())) {
          LOG_ERROR("guard condition failed", K(ret));
        } else {
          if (slice_writer->get_row_offset() >= 0) {
            // row offset already set
            break;
          } else {
            const int64_t wait_interval_ms = 100L;
            if (OB_FAIL(cond_.wait(wait_interval_ms))) {
              if (OB_TIMEOUT != ret) {
                LOG_WARN("wait thread condition failed", K(ret));
              } else {
                ret = OB_SUCCESS;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::update_max_lob_id(const int64_t lob_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    last_lob_id_ = MAX(last_lob_id_, lob_id);
  }
  return ret;
}

int64_t ObTabletFullDirectLoadMgrV2::calc_root_macro_seq()
{
  return MAX(last_meta_seq_, last_data_seq_) + compaction::MACRO_STEP_SIZE;
}


void ObTabletFullDirectLoadMgrV2::update_last_meta_seq(const ObITable::TableKey &table_key, const int64_t new_seq)
{
  if (0 == table_key.get_column_group_id() && new_seq > last_meta_seq_) {
    last_meta_seq_ = new_seq;
  }
}

int ObTabletFullDirectLoadMgrV2::build_ddl_sstable_res(ObSSTableIndexBuilder &sstable_index_builder, ObITable::TableKey &table_key, blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  int64_t slice_cnt = ATOMIC_LOAD(&total_slice_cnt_);
  if (OB_UNLIKELY(!sstable_index_builder.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("warn mode", K(ret));
  } else if (sstable_index_builder.is_closed()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already closed", K(ret), K(table_key), K(lbt()));
  } else if (slice_cnt < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid total slice cnt", K(ret), K(tablet_id_));
  } else {
    // In single-threaded construction of the index tree, macro seq starts from `task_cnt * 2 ^ 25`.
    ObMacroDataSeq tmp_seq(slice_cnt * compaction::MACRO_STEP_SIZE);
    tmp_seq.set_index_block();
    share::ObPreWarmerParam pre_warm_param;
    storage::ObDDLRedoLogWriterCallback flush_callback;
    if (OB_FAIL(pre_warm_param.init(ls_id_, tablet_id_))) {
      LOG_WARN("failed to init pre warm param", KR(ret));
    } else if (OB_FAIL(flush_callback.init(ls_id_, tablet_id_, 
                                          DDL_MB_INDEX_TYPE, 
                                          table_key, get_ddl_task_id(),
                                          get_start_scn(), data_format_version_, 
                                          task_cnt_, cg_cnt_, DIRECT_LOAD_LOAD_DATA_V2, 0 /*row id offset*/))) {
      // The row_id_offset of cg index_macro_block has no effect, so set 0 to pass the defensive check.
      LOG_WARN("fail to init redo log writer callback", K(ret), K(table_key));
    } else if (OB_FAIL(sstable_index_builder.close_with_macro_seq(
                   res, tmp_seq.macro_data_seq_,
                   OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                   0 /*nested_offset*/, pre_warm_param, &flush_callback))) {
      LOG_WARN("fail to close", K(ret), K(sstable_index_builder), K(tmp_seq), K(slice_cnt));
    } else {
      update_last_meta_seq(table_key, tmp_seq.macro_data_seq_);
      res.root_macro_seq_ = calc_root_macro_seq();
      LOG_INFO("[SHARED STORAGE]build ddl sstable res success", K(last_meta_seq_), K(last_data_seq_), K(res), K(table_key));
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::create_ddl_ro_sstable(ObTablet &tablet,
                                                       const ObStorageSchema *storage_schema,
                                                       common::ObArenaAllocator &allocator,
                                                       ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  sstable_handle.reset();
  if (OB_ISNULL(sqc_build_ctx_.index_builder_) || OB_UNLIKELY(!sqc_build_ctx_.index_builder_->is_inited() || OB_ISNULL(storage_schema) || !table_key_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(sqc_build_ctx_.index_builder_), KP(storage_schema), K(table_key_));
  } else if (table_key_.table_type_ != ObITable::MAJOR_SSTABLE) {      
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cg idx", K(ret), K(table_key_));
  } else {
    SMART_VARS_2((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, param)) {
      if (OB_FAIL(build_ddl_sstable_res(*sqc_build_ctx_.index_builder_, table_key_, res))) {
        LOG_WARN("fail to get merge res", K(ret), KPC(sqc_build_ctx_.index_builder_), K(res));
      } else {
        const int64_t create_schema_version_on_tablet = tablet.get_tablet_meta().create_schema_version_;
        if (OB_FAIL(param.init_for_ss_ddl(res, table_key_, *storage_schema, create_schema_version_on_tablet))) {
          LOG_WARN("fail to init param for ddl", K(ret), K(create_schema_version_on_tablet),
            KPC(sqc_build_ctx_.index_builder_), K(table_key_), KPC(storage_schema));
        } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(param, allocator, sstable_handle))) {
          LOG_WARN("create sstable failed", K(ret), K(param));
        }

        if (OB_SUCC(ret)) {
          LOG_INFO("[SHARED STORAGE]create ddl sstable success ro", K(table_key_), K(sstable_handle),
              "create_schema_version", create_schema_version_on_tablet);
        }
      }
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::create_ddl_co_sstable(ObTablet &tablet,
                                                       const ObStorageSchema *storage_schema,
                                                       common::ObArenaAllocator &allocator,
                                                       ObTableHandleV2 &co_sstable_handle)
{
  int ret = OB_SUCCESS;
  co_sstable_handle.reset();
  ObTablesHandleArray cg_sstable_handles;
  if (OB_UNLIKELY(OB_ISNULL(storage_schema) || !table_key_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_schema), K(table_key_));
  } else {
    const int64_t create_schema_version_on_tablet = tablet.get_tablet_meta().create_schema_version_;
    const int64_t base_cg_idx = table_key_.get_column_group_id(); //co
    int max_written_link_block_cnt = 0;
    if (OB_FAIL(ret)) {
      // error occurred
    } else if (table_key_.table_type_ != ObITable::COLUMN_ORIENTED_SSTABLE) {      
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cg idx", K(ret), K(base_cg_idx), K(table_key_));
    } else {
      // create co sstable
      bool is_empty_co_table = true;
      if (OB_FAIL(create_ddl_cg_sstable(base_cg_idx, create_schema_version_on_tablet, storage_schema, allocator, table_key_, co_sstable_handle))) {
        LOG_WARN("create base cg sstable failed", K(ret), K(base_cg_idx));
      } else {
        is_empty_co_table = static_cast<ObCOSSTableV2 *>(co_sstable_handle.get_table())->is_cgs_empty_co_table();
        LOG_INFO("[SHARED STORAGE]create ddl sstable success co", K(table_key_), K(co_sstable_handle), K(base_cg_idx), K(is_empty_co_table),
            "create_schema_version", create_schema_version_on_tablet);
      }

      // create cg sstables
      if (OB_FAIL(ret) || is_empty_co_table) {
      } else {
        ObITable::TableKey cg_table_key = table_key_;
        cg_table_key.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
        for (int64_t i = 0; OB_SUCC(ret) && i < storage_schema->get_column_group_count(); ++i) {
          const int64_t cur_cg_idx = i;
          if (cur_cg_idx == base_cg_idx) {
            // do nothing
          } else {
            ObTableHandleV2 cur_table_handle;
            cg_table_key.column_group_idx_ = cur_cg_idx;
            if (OB_FAIL(create_ddl_cg_sstable(cur_cg_idx,
                                                     create_schema_version_on_tablet,
                                                     storage_schema,
                                                     allocator,
                                                     cg_table_key,
                                                     cur_table_handle))) {
              LOG_WARN("fail to create cg sstable", K(ret), K(cur_cg_idx), K(cg_table_key));
            } else if (OB_FAIL(cg_sstable_handles.add_table(cur_table_handle))) {
              LOG_WARN("push back compacted cg sstable failed", K(ret), K(i), KP(cur_table_handle.get_table()));
            }
          }
        }

        // assemble the cg sstables into co sstable
        if (OB_SUCC(ret)) {
          ObArray<ObITable *> cg_sstables;
          if (OB_FAIL(cg_sstable_handles.get_tables(cg_sstables))) {
            LOG_WARN("get cg sstables failed", K(ret));
          } else if (OB_FAIL(static_cast<ObCOSSTableV2 *>(co_sstable_handle.get_table())->fill_cg_sstables(cg_sstables))) {
            LOG_WARN("fill cg sstables failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::create_ddl_cg_sstable(const int64_t cg_idx,
                                                       const int64_t create_schema_version_on_tablet,
                                                       const ObStorageSchema *storage_schema,
                                                       common::ObArenaAllocator &allocator,
                                                       ObITable::TableKey &cg_table_key,
                                                       ObTableHandleV2 &cg_sstable_handle)
{
  int ret = OB_SUCCESS;
  cg_sstable_handle.reset();
  ObArray<ObMacroMetaTempStore *> sorted_meta_stores;
  if (OB_UNLIKELY(OB_ISNULL(storage_schema) || !cg_table_key.is_valid() || cg_idx < 0 || cg_idx >= storage_schema->get_column_group_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_schema), K(cg_table_key), K(cg_idx));
  } else if (OB_FAIL(macro_meta_store_mgr_.get_sorted_macro_meta_stores(cg_idx, sorted_meta_stores))) {
    LOG_WARN("get sorted macro meta store array failed", K(ret), K(cg_idx));
  } else {
    HEAP_VAR(ObSSTableIndexBuilder, sstable_index_builder, true /*use buffer*/) {
    SMART_VARS_3((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, param), (ObWholeDataStoreDesc, data_desc)) {
      ObIndexBlockRebuilder index_block_rebuilder;
      storage::ObDDLRedoLogWriterCallback clustered_index_flush_callback;
      ObMacroSeqParam macro_seq_param;
      macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      macro_seq_param.start_ = 0; // for clustered micro index, build in one thread
      const ObStorageColumnGroupSchema &cg_schema = storage_schema->get_column_groups().at(cg_idx);
      if (OB_FAIL(data_desc.init(true/*is_ddl*/,
                                 *storage_schema,
                                 ls_id_,
                                 tablet_id_,
                                 compaction::ObMergeType::MAJOR_MERGE,
                                 table_key_.get_snapshot_version(),
                                 data_format_version_,
                                 get_micro_index_clustered(),
                                 get_tablet_transfer_seq(),
                                 SCN::min_scn(),
                                 &cg_schema,
                                 cg_idx))) {
        LOG_WARN("init data store desc failed", K(ret), K(tablet_id_));
      } else {
        data_desc.get_static_desc().exec_mode_ = compaction::EXEC_MODE_OUTPUT;
        data_desc.get_static_desc().schema_version_ = sqc_build_ctx_.build_param_.runtime_only_param_.schema_version_;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sstable_index_builder.init(data_desc.get_desc(), ObSSTableIndexBuilder::DISABLE/*small SSTable op*/))) {
        LOG_WARN("init sstable index builder failed", K(ret), K(data_desc));
      } else if (get_micro_index_clustered() && OB_FAIL(clustered_index_flush_callback.init(ls_id_, tablet_id_, DDL_MB_INDEX_TYPE,
              cg_table_key, get_ddl_task_id(), get_start_scn(), data_format_version_,
              task_cnt_, cg_cnt_, DIRECT_LOAD_LOAD_DATA_V2, 0 /*row id offset*/))) {
      } else if (OB_FAIL(index_block_rebuilder.init(sstable_index_builder, macro_seq_param, 0/*task_idx*/, cg_table_key,
              get_micro_index_clustered() ? &clustered_index_flush_callback : nullptr))) {
        LOG_WARN("fail to alloc index builder", K(ret));
      } else {
        ObMacroMetaTempStoreIter macro_meta_iter;
        ObDataMacroBlockMeta macro_meta;
        ObMicroBlockData leaf_index_block;
        for (int64_t i = 0; OB_SUCC(ret) && i < sorted_meta_stores.count(); ++i) {
          ObMacroMetaTempStore *cur_macro_meta_store = sorted_meta_stores.at(i);
          macro_meta_iter.reset();
          macro_meta.reset();
          leaf_index_block.reset();
          if (OB_ISNULL(cur_macro_meta_store)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("current macro meta store is null", K(ret));
          } else if (OB_FAIL(macro_meta_iter.init(*cur_macro_meta_store))) {
            LOG_WARN("init macro meta store iterator failed", K(ret));
          } else {
            while (OB_SUCC(ret)) {
              if (OB_FAIL(macro_meta_iter.get_next(macro_meta, leaf_index_block))) {
                if (OB_ITER_END != ret) {
                  LOG_WARN("get next macro meta failed", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  break;
                }
              } else if (OB_FAIL(index_block_rebuilder.append_macro_row(macro_meta, leaf_index_block.get_buf(), leaf_index_block.get_buf_size()))) {
                LOG_WARN("append macro row failed", K(ret), K(macro_meta), K(leaf_index_block));
              } else {
                LOG_TRACE("rebuilder append macro row", K(ret), K(cg_table_key), K(macro_meta));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(index_block_rebuilder.close())) {
          LOG_WARN("close index block rebuilder failed", K(ret));
        } else if (OB_FAIL(build_ddl_sstable_res(sstable_index_builder, cg_table_key, res))) {
          LOG_WARN("fail to get merge res", K(ret), K(sstable_index_builder), K(res));
        } else if (OB_FAIL(param.init_for_ss_ddl(res, cg_table_key, *storage_schema, create_schema_version_on_tablet))) {
          LOG_WARN("fail to init param for ddl", K(ret), K(create_schema_version_on_tablet),
            K(sstable_index_builder), K(cg_table_key), KPC(storage_schema));
        } else if (cg_table_key.is_co_sstable()) {
          if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator, cg_sstable_handle))) {
            LOG_WARN("create sstable failed", K(ret), K(param));
          }
        } else {
          if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(param, allocator, cg_sstable_handle))) {
            LOG_WARN("create sstable failed", K(ret), K(param));
          }
        }

        if (OB_SUCC(ret)) {
          LOG_INFO("[SHARED STORAGE]create ddl sstable success cg", K(cg_table_key), K(cg_sstable_handle), K(cg_idx),
              "create_schema_version", create_schema_version_on_tablet);
        }
      }
    }// SMART_VARS_2
    }// HEAP_VAR
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::update_table_store(
  const blocksstable::ObSSTable *sstable,
  const ObStorageSchema *storage_schema,
  ObITable::TableKey &table_key,
  ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  share::SCN unused_commit_scn = SCN::min_scn();
  int64_t start_meta_macro_seq = last_meta_seq_;
  if (OB_UNLIKELY(OB_ISNULL(storage_schema) || OB_ISNULL(sstable))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(storage_schema), KP(sstable));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else {
    const int64_t rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    const int64_t snapshot_version = table_key_.get_snapshot_version();
    const int64_t multi_version_start = table_key_.get_snapshot_version();
    ObUpdateTableStoreParam table_store_param(snapshot_version,
                                              multi_version_start,
                                              storage_schema,
                                              rebuild_seq,
                                              sstable);
    if (OB_FAIL(table_store_param.init_with_compaction_info(
            ObCompactionTableStoreParam(compaction::MEDIUM_MERGE,
                                        share::SCN::min_scn(),
                                        true /*need_report*/,
                                        false /*has_truncate_info*/)))) {
      /*DDL does not have verification between replicas,
        So using medium merge to force verification between replicas*/
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else {
      table_store_param.ddl_info_.update_with_major_flag_ = true;
      table_store_param.ddl_info_.keep_old_ddl_sstable_ = false; //major_sstable no need
      table_store_param.ddl_info_.data_format_version_ = data_format_version_;
      table_store_param.ddl_info_.ddl_commit_scn_ = unused_commit_scn;
      table_store_param.ddl_info_.ddl_checkpoint_scn_ = unused_commit_scn;
      LOG_INFO("[SHARED STORAGE]build ddl update_table_store_param", K(table_store_param), K(ret));
    }
    storage::ObDDLRedoLogWriter ddl_clog_writer;
    storage::ObDDLRedoLogWriterCallback ddl_redo_cb;
    storage::ObDDLFinishLogWriterCallback ddl_finish_cb;
    if (FAILEDx(ddl_clog_writer.init(ls_id_, tablet_id_))) {
      LOG_WARN("fail to init ddl clog writer", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_FAIL(ddl_redo_cb.init(ls_id_, tablet_id_,
                                        is_no_logging_ ? DDL_MB_SS_EMPTY_DATA_TYPE : DDL_MB_SSTABLE_META_TYPE, 
                                        table_key_, 
                                        sqc_build_ctx_.build_param_.runtime_only_param_.task_id_,
                                        tablet.get_tablet_meta().ddl_start_scn_, 
                                        data_format_version_,
                                        task_cnt_, cg_cnt_,
                                        DIRECT_LOAD_LOAD_DATA_V2,
                                        0 /* row offset, used for skip co sstable ckeck*/,
                                        true /* need delay write util wait*/))) {
    } else if (OB_FAIL(ddl_finish_cb.init(ls_id_,
                                          table_key_,
                                          sqc_build_ctx_.build_param_.runtime_only_param_.task_id_,
                                          data_format_version_,
                                          &ddl_clog_writer))) {
      LOG_WARN("fail to init ddl finish callback", K(ret), K(ls_id_), K(table_key));
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(table_store_param.ddl_info_.ddl_redo_callback_ = &ddl_redo_cb)) {
    } else if (FALSE_IT(table_store_param.ddl_info_.ddl_finish_callback_ = &ddl_finish_cb)) {
    } else if (OB_FAIL(ls_handle.get_ls()->upload_major_compaction_tablet_meta(
        tablet_id_,
        table_store_param,
        start_meta_macro_seq))) {
      LOG_WARN("failed to upload compaction tablet meta", K(ret), K(start_meta_macro_seq), K(last_meta_seq_));
    } else if (OB_FAIL(ddl_finish_cb.wait())) {
      LOG_WARN("failed to wait ddl finsih log", K(ret));
    } else if (OB_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id_, tablet_id_))) {
      LOG_WARN("fail to submit tablet update task", K(ret), K(ls_id_), K(tablet_id_));
    } else {
      FLOG_INFO("[SHARED STORAGE]update table store success", K(ret), K(ls_id_), K(tablet_id_), K(start_meta_macro_seq), K(last_data_seq_), K(last_meta_seq_));
    }
  }
  return ret;
}

struct DestroySliceWriterSSFn
{
public:
  DestroySliceWriterSSFn(ObIAllocator *allocator) :allocator_(allocator) {}
  int operator () (hash::HashMapPair<ObTabletDirectLoadBuildCtx::SliceKey, ObDirectLoadSliceWriter *> &entry) {
    int ret = OB_SUCCESS;
    if (nullptr != allocator_) {
      if (nullptr != entry.second) {
        LOG_INFO("[SHARED STORAGE]erase a slice writer", K(&entry.second), K(entry.first));
        entry.second->~ObDirectLoadSliceWriter();
        allocator_->free(entry.second);
        entry.second = nullptr;
      }
    }
    return ret;
  }

private:
  ObIAllocator *allocator_;
};

int ObTabletFullDirectLoadMgrV2::fill_column_group(const int64_t thread_cnt, const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(thread_cnt <= 0 || thread_id < 0 || thread_id > thread_cnt - 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret), K(thread_cnt), K(thread_id));
  } else if (sqc_build_ctx_.sorted_slice_writers_.count() == 0 || thread_id > sqc_build_ctx_.sorted_slices_idx_.count() - 1) {
    //ignore
    FLOG_INFO("[DIRECT_LOAD_FILL_CG] idle thread", K(sqc_build_ctx_.sorted_slice_writers_.count()), K(thread_id), K(sqc_build_ctx_.sorted_slices_idx_.count()));
  } else if (sqc_build_ctx_.sorted_slice_writers_.count() != sqc_build_ctx_.slice_mgr_map_.size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong slice writer num", K(ret), K(sqc_build_ctx_.sorted_slice_writers_.count()), K(sqc_build_ctx_.slice_mgr_map_.size()), K(common::lbt()));
  } else {
    const int64_t start_idx = sqc_build_ctx_.sorted_slices_idx_.at(thread_id).start_idx_;
    const int64_t last_idx = sqc_build_ctx_.sorted_slices_idx_.at(thread_id).last_idx_;

    ObArenaAllocator arena_allocator("DIRECT_RESCAN", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTablet *tablet = nullptr;
    ObStorageSchema *storage_schema = nullptr;
    int64_t fill_cg_finish_count = -1;
    int64_t row_cnt = 0;
    ObTabletHandle tablet_handle;
    if (OB_FAIL(get_tablet_handle(tablet_handle))) {
      LOG_WARN("failed to get tablet handle", K(ret));
    } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_FAIL(tablet->load_storage_schema(arena_allocator, storage_schema))) {
      LOG_WARN("load storage schema failed", K(ret), K(tablet_id_));
    } else if (OB_UNLIKELY(nullptr == storage_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage_schema", K(ret), KP(storage_schema));
    } else {
      const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
      FLOG_INFO("[DIRECT_LOAD_FILL_CG] start fill cg",
          "tablet_id", tablet_id_,
          "cg_cnt", cg_schemas.count(),
          "slice_cnt", sqc_build_ctx_.sorted_slice_writers_.count(),
          K(thread_cnt), K(thread_id), K(start_idx), K(last_idx));

      ObCOSliceWriter *cur_writer = nullptr;
      int64_t last_seq = 0;
      if (OB_ISNULL(cur_writer = OB_NEWx(ObCOSliceWriter, &arena_allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for co writer failed", K(ret));
      } else if (OB_FAIL(fill_aggregated_column_group(thread_id, start_idx, last_idx, storage_schema, cur_writer, fill_cg_finish_count, row_cnt, last_seq))) {
        LOG_WARN("fail to fill aggregated cg", K(ret), KPC(cur_writer), K(thread_id), K(start_idx), K(last_idx));
      } else if (OB_FAIL(update_max_macro_seq(last_seq))) {
        LOG_WARN("update max macro seq failed", K(ret), K(last_seq));
      }
      // free writer anyhow
      if (OB_NOT_NULL(cur_writer)) {
        cur_writer->~ObCOSliceWriter();
        arena_allocator.free(cur_writer);
        cur_writer = nullptr;
      }
      ObTabletObjLoadHelper::free(arena_allocator, storage_schema); //arena cannot free
      arena_allocator.reset();

      // after finish all slice, free slice_writer
      if (OB_SUCC(ret)) {
        if (fill_cg_finish_count == sqc_build_ctx_.sorted_slice_writers_.count()) {
          sqc_build_ctx_.sorted_slice_writers_.reset();
          FLOG_INFO("tablet_direct_mgr finish fill column group", K(sqc_build_ctx_.slice_mgr_map_.size()), K(this), K(fill_cg_finish_count));
          if (!sqc_build_ctx_.slice_mgr_map_.empty()) {
            DestroySliceWriterSSFn destroy_map_fn(&sqc_build_ctx_.slice_writer_allocator_);
            int tmp_ret = sqc_build_ctx_.slice_mgr_map_.foreach_refactored(destroy_map_fn);
            if (tmp_ret == OB_SUCCESS) {
              sqc_build_ctx_.slice_mgr_map_.destroy();
            } else {
              ret = tmp_ret;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("[DIRECT_LOAD_FILL_CG] finish fill cg",
          "tablet_id", tablet_id_,
          "row_cnt", row_cnt,
          "slice_cnt", sqc_build_ctx_.sorted_slice_writers_.count(),
          K(thread_cnt), K(thread_id), K(start_idx), K(last_idx),  K(sqc_build_ctx_.slice_mgr_map_.size()));
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::fill_aggregated_column_group(
    const int64_t parallel_idx,
    const int64_t start_idx,
    const int64_t last_idx,
    const ObStorageSchema *storage_schema,
    ObCOSliceWriter *cur_writer,
    int64_t &fill_cg_finish_count,
    int64_t &fill_row_cnt,
    int64_t &last_seq)
{
  int ret = OB_SUCCESS;
  fill_cg_finish_count = -1;
  fill_row_cnt = 0;
  last_seq = -1;
  int64_t fill_aggregated_cg_cnt = 0;
  
  if (OB_ISNULL(cur_writer) || OB_ISNULL(storage_schema) || OB_UNLIKELY(parallel_idx < 0 || start_idx < 0 || last_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cur_writer), KP(storage_schema), K(parallel_idx), K(start_idx), K(last_idx));
  } else {
    const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema->get_column_groups();
    for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_schemas.count(); ++cg_idx) {
      cur_writer->reset();
      if (start_idx == last_idx || start_idx >= sqc_build_ctx_.sorted_slice_writers_.count() || last_idx > sqc_build_ctx_.sorted_slice_writers_.count()) {
        // skip
      } else {
        ObDirectLoadSliceWriter *first_slice_writer = sqc_build_ctx_.sorted_slice_writers_.at(start_idx);
        if (OB_ISNULL(first_slice_writer)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null slice writer", K(ret), KP(first_slice_writer));
        } else if (OB_UNLIKELY(first_slice_writer->get_row_offset() < 0)) {
          ret = OB_ERR_SYS;
          LOG_WARN("invalid row offset", K(ret), K(first_slice_writer->get_row_offset()));
        } else if (OB_FAIL(cur_writer->init(storage_schema, cg_idx, this,first_slice_writer->get_start_seq(),
                first_slice_writer->get_row_offset(), get_start_scn(), need_process_cs_replica_, parallel_idx))) {
          LOG_WARN("init co ddl writer failed", K(ret), KPC(cur_writer), K(cg_idx), KPC(this));
        } else {
          for (int64_t i = start_idx; OB_SUCC(ret) && i < last_idx; ++i) {
            ObDirectLoadSliceWriter *slice_writer = sqc_build_ctx_.sorted_slice_writers_.at(i);
            if (OB_ISNULL(slice_writer) || !slice_writer->need_column_store()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("wrong slice writer",  K(ret), KPC(slice_writer));
            } else if (OB_FAIL(slice_writer->fill_aggregated_column_group(cg_idx, cur_writer))) {
              LOG_WARN("slice writer rescan failed", K(ret), K(cg_idx), KPC(cur_writer));
            } else if (cg_idx == cg_schemas.count() - 1) {
              // after fill last cg, inc finish cnt
              fill_row_cnt += slice_writer->get_row_count();
              ++fill_aggregated_cg_cnt;

            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (cur_writer->is_inited() && OB_FAIL(cur_writer->close())) {
          LOG_WARN("close co ddl writer failed", K(ret));
        }
      }
      // next cg
    }
    if (OB_SUCC(ret)) {
      /*
      To avoid concurrent release of slice_writer (datum_store) with slice_mgr_map_.destroy()
       we must reset datum_store first and then increase fill_column_group_finish_count_.
      */
      fill_cg_finish_count = ATOMIC_AAF(&sqc_build_ctx_.fill_column_group_finish_count_, fill_aggregated_cg_cnt);
    }
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::get_tablet_handle(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService * ls_service = nullptr;
  
  tablet_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
     LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  }
  return ret;
}

int ObTabletFullDirectLoadMgrV2::set_total_slice_cnt(const int64_t slice_cnt)
{
  int ret = OB_SUCCESS;
  int64_t local_slice_cnt = ATOMIC_LOAD(&total_slice_cnt_);
  if (local_slice_cnt > -1) {
    /* skip already set */
  } else  {
    if (slice_cnt < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(slice_cnt));
    } else if (!lob_mgr_handle_.is_valid()) {
    } else if (OB_FAIL(lob_mgr_handle_.get_obj()->set_total_slice_cnt(slice_cnt))) {
      LOG_WARN("failed to set lob mgr handle", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else {
      ATOMIC_SET(&total_slice_cnt_, slice_cnt);
    }
  }
  return ret;
}
