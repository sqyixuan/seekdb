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

#include "close_modules/shared_storage/storage/shared_storage/ob_public_block_gc_service.h"
#include "logservice/ob_log_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "close_modules/shared_storage/meta_store/ob_shared_storage_obj_meta.h"
#include "share/schema/ob_part_mgr_util.h"            // ObPartitionSchemaIter
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_info.h"
#include "close_modules/shared_storage/storage/compaction/ob_tablet_id_obj.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

// The time interval for tablet gc is 5s
const int64_t ObPublicBlockGCTask::GC_CHECK_INTERVAL = 5 * 1000 * 1000L;

// The time interval for tablet gc is 2h
const int64_t ObPublicBlockGCTask::LOOP_CHECK_INTERVAL = 2 * 60 * 60 * 1000 * 1000L;

// The default time interval for tablet meta gc is 5d
const int64_t ObPublicBlockGCTask::DEFALT_PUBLIC_TABLET_META_GC_SAFE_TIME = 5 * 24 * 60 * 60 * 1000 * 1000L;

// The default time interval for tablet gc is 5d
const int64_t ObPublicBlockGCTask::DEFALT_PUBLIC_TABLET_GC_SAFE_TIME = 5 * 24 * 60 * 60 * 1000 * 1000L;

const int64_t ObPublicBlockGCTask::BATCH_DETECT_MACRO_SIZE = 1000;

// sizeof(version) + sizeof(len) + sizeof(delete_ts) < 64;
const int64_t ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE = 64;

int TabletMetaGCOP::operator()(
      const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObPublicBlockGCHandler handler(tablet_id, retain_tablet_meta_ts_, major_scn_);
  if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
    LOG_INFO("public_block_gc_service is stopped", K(ret));
  } else if (OB_FAIL(gc_guard_.add_gc_task(handler))) {
    LOG_WARN("failed to add gc task", K(ret), K(handler));
  }
  return ret;
}

bool TabletGCOP::operator()(
      const ObTabletID &tablet_id, 
      bool &is_mark)
{
  int ret = OB_SUCCESS;
  ObPublicBlockGCHandler handler(tablet_id);
  if (is_stopped()) {
    LOG_INFO("public_block_gc_service is stopped", K(ret));
    return false;
  } else if (is_mark) {
  } else if (OB_FAIL(gc_guard_.add_gc_task(handler))) {
    LOG_WARN("failed to add gc task", K(ret), K(handler));
  }
  return true;
}

int MarkTabletIDOP::operator()(
      const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  bool is_mark = false;
  if (is_stopped()) {
    ret = OB_IN_STOP_STATE;
    LOG_INFO("public_block_gc_service is stopped", K(ret));
  } else if (OB_FAIL(tablet_id_map_.get(tablet_id, is_mark))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get", KR(ret), K(tablet_id));
    }
  } else if (OB_FAIL(tablet_id_map_.insert_or_update(tablet_id, true))) {
    LOG_WARN("failed to insert", KR(ret));
  }
  return ret;
}

int ObPublicBlockGCService::mtl_init(ObPublicBlockGCService* &m)
{
  return m->init();
}

int ObPublicBlockGCService::init()
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode()) {
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
      LOG_WARN("ObPublicBlockGCService init twice.", KR(ret));
    } else if (OB_FAIL(timer_for_public_block_gc_.set_run_wrapper_with_ret(MTL_CTX()))) {
      LOG_ERROR("fail to set timer's run wrapper", KR(ret));
    } else if (OB_FAIL(timer_for_public_block_gc_.init("PubBlkGCTimer", ObMemAttr(MTL_ID(), "PubBlkGCTimer")))) {
      LOG_ERROR("fail to init timer", KR(ret));
    } else if (OB_FAIL(gc_thread_.init())) {
      LOG_ERROR("fail to init gc thread", KR(ret), K_(gc_thread));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPublicBlockGCService::start()
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode()) {
    if (OB_UNLIKELY(!is_inited_)) { 
      ret = OB_NOT_INIT;
      LOG_WARN("ObPublicBlockGCService is not initialized", KR(ret));
    } else if (OB_FAIL(gc_thread_.start())) {
      LOG_ERROR("fail to init gc thread", KR(ret), K_(gc_thread));
    }
  }
  return ret;
}

int ObPublicBlockGCService::inner_switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) { 
    ret = OB_NOT_INIT;
    LOG_WARN("ObPublicBlockGCService is not initialized", KR(ret));
  } else if (FALSE_IT(public_block_gc_task_.clear_is_stopped())) {
  } else if (OB_FAIL(timer_for_public_block_gc_.schedule(public_block_gc_task_, ObPublicBlockGCTask::GC_CHECK_INTERVAL, true))) {
    LOG_WARN("fail to schedule task", KR(ret));
  } else {
    LOG_INFO("ObPublicBlockGCService started");
  }
  return ret;
}

int ObPublicBlockGCService::inner_switch_to_follower()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) { 
    ret = OB_NOT_INIT;
    LOG_WARN("ObPublicBlockGCService is not initialized", KR(ret));
  } else {
    public_block_gc_task_.set_is_stopped();
    timer_for_public_block_gc_.cancel(public_block_gc_task_);
    LOG_INFO("ObPublicBlockGCService stoped");
  }
  return ret;
}

int ObPublicBlockGCService::stop()
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode()) {
    if (OB_UNLIKELY(!is_inited_)) { 
      ret = OB_NOT_INIT;
      LOG_WARN("ObPublicBlockGCService is not initialized", KR(ret));
    } else {
      if (OB_FAIL(gc_thread_.stop())) {
        LOG_WARN("failed to stop gc thread", KR(ret));
      }
      public_block_gc_task_.set_is_stopped();
      timer_for_public_block_gc_.stop();
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("ObPublicBlockGCService stoped");
  }
  return ret;
}

void ObPublicBlockGCService::wait()
{
  if (GCTX.is_shared_storage_mode()) {
    timer_for_public_block_gc_.wait();
    gc_thread_.wait();
  }
}

void ObPublicBlockGCService::destroy()
{
  if (GCTX.is_shared_storage_mode()) {
    is_inited_ = false;
    timer_for_public_block_gc_.destroy();
    gc_thread_.destroy();
  }
}

template <typename OP>
int ObPublicBlockGCTask::loop_tablet_id_(
    OP &op)
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = MTL_ID();
  int64_t schema_version = OB_INVALID_VERSION;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_ids;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(GSCHEMASERVICE.get_schema_version_in_inner_table(
        *GCTX.sql_proxy_, schema_status, schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret));
  } else if (OB_INVALID_VERSION == schema_version) {
    ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schema_version is invalid", KR(ret), K(schema_status));
  } else {
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(MTL_ID(), schema_guard, schema_version))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(schema_version));
    } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(MTL_ID(), table_schemas))) {
      LOG_WARN("fail to get tenant table schemas", KR(ret), K(schema_version));
    } else {
      LOG_INFO("get tablet_ids from table_schemas", K(table_schemas.count()), K(schema_version));
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table_schema is null", KR(ret), K(i), K(table_schemas.count()), K(schema_version));
        } else if (is_sys_view(table_schema->get_table_id())
            || is_virtual_table(table_schema->get_table_id())) {
        } else {
          ObPartitionSchemaIter iter(*table_schema, CHECK_PARTITION_MODE_NORMAL);
          ObPartitionSchemaIter::Info info;
          while (OB_SUCC(ret)) {
            if (OB_FAIL(iter.next_partition_info(info))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("failed to iterate", KR(ret), K(info), K(i), K(table_schemas.count()), K(schema_version));
              }
            } else if (!info.tablet_id_.is_valid()) {
              LOG_INFO("tablet_id is invalid", K(ret), K(info), K(table_schemas.count()), K(schema_version));
            } else if (OB_FAIL(tablet_ids.push_back(info.tablet_id_))) {
              LOG_WARN("fail to push_back", K(ret), K(info), K(table_schemas.count()), K(schema_version));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("start loop tablet id", K(schema_version), K(tablet_ids.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      ObTabletID &tablet_id = tablet_ids.at(i);
      if (OB_TMP_FAIL(op(tablet_id))) {
        if (OB_IN_STOP_STATE == tmp_ret) {
          ret = OB_IN_STOP_STATE;
        }
        LOG_WARN("failed to tablet_id process", KR(tmp_ret), K(tablet_ids.count()), K(i), K(tablet_id), K(schema_version), K(tablet_ids));
      }
      LOG_INFO("loop tablet finish", K(ret), K(tablet_ids.count()), K(i), K(tablet_id), K(schema_version));
    }
  }
  return ret;
}

void ObPublicBlockGCTask::loop_tablet_meta_version_gc_()
{
  int ret = OB_SUCCESS;
  const int64_t curr_ts = ObTimeUtility::fast_current_time();
  const int64_t gc_tablet_meta_safe_time_val = ATOMIC_LOAD(&gc_tablet_meta_safe_time_val_);
  const int64_t retain_tablet_meta_ts = curr_ts - gc_tablet_meta_safe_time_val;
  const int64_t next_gc_tablet_meta_ts = ATOMIC_LOAD(&next_gc_tablet_meta_ts_);
  ObGlobalMergeInfo info;
  SCN major_scn;
  FLOG_INFO("====== tablet meta gc in shared dir ======", K(retain_tablet_meta_ts), K(next_gc_tablet_meta_ts));
  ObPublicBlockGCService *public_block_gc_service = MTL(ObPublicBlockGCService*);
  if (OB_ISNULL(public_block_gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("public_block_gc_service should not be null", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(*GCTX.sql_proxy_,
              MTL_ID(), info))) {
    LOG_WARN("fail to get global merge info", KR(ret));
  } else if (FALSE_IT(major_scn = info.last_merged_scn_.get_scn())) {
  } else if (!major_scn.is_valid()) {
    LOG_WARN("major_scn is invalid", KR(ret));
  } else if (next_gc_tablet_meta_ts < curr_ts) {
    common::ObArenaAllocator allocator("PubBlkGCThrd");
    ObPublicBlockGCThread &gc_thread = public_block_gc_service->get_gc_thread();
    ObPublicBlockGCThreadGuard thread_guard(gc_thread, true /* tablet meta gc */, is_paused_, is_stopped_, allocator);
    TabletMetaGCOP op(thread_guard, is_paused_, is_stopped_, retain_tablet_meta_ts, major_scn);
    if (OB_FAIL(loop_tablet_id_(op))) {
      LOG_WARN("failed to loop_tablet_id", K(ret));
    } 

    thread_guard.wait_gc_task_finished();
    const int64_t gc_tablet_meta_ts = thread_guard.get_next_gc_tablet_meta_ts();
    if (OB_FAIL(ret)) {
    } else if (INT64_MAX == gc_tablet_meta_ts) {
      // no tablet_meta or one tablet_meta, need wait gc_tablet_meta_safe_time_val;
      ATOMIC_CAS(&next_gc_tablet_meta_ts_, next_gc_tablet_meta_ts, curr_ts + gc_tablet_meta_safe_time_val);
    } else {
      ATOMIC_CAS(&next_gc_tablet_meta_ts_, next_gc_tablet_meta_ts, gc_tablet_meta_ts + gc_tablet_meta_safe_time_val);
    }
    LOG_INFO("finish loop_tablet_meta_version_gc_", KR(ret), K(op), K(retain_tablet_meta_ts), KPC(this));
    if (OB_SUCC(ret) && thread_guard.get_gc_macro_block_cnt() > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "shared_dir_tablet_meta_gc", "tenant_id", MTL_ID(), "gc_macro_block_cnt", thread_guard.get_gc_macro_block_cnt());
    }
  }
}

void ObPublicBlockGCTask::loop_tablet_gc_()
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = MTL_ID();
  int64_t schema_version = OB_INVALID_VERSION;
  TabletIDMap tablet_id_map;
  ObArray<int64_t> tablet_ids;
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  ObGlobalMergeInfo info;
  ObPublicBlockGCService *public_block_gc_service = MTL(ObPublicBlockGCService*);
  FLOG_INFO("====== tablet gc in shared dir ======");
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_ISNULL(public_block_gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("public_block_gc_service should not be null", K(ret));
  } else if (OB_FAIL(GSCHEMASERVICE.get_schema_version_in_inner_table(
        *GCTX.sql_proxy_, schema_status, schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret));
  } else if (OB_INVALID_VERSION == schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_version is invalid", KR(ret), K(schema_status));
  } else if (schema_version_ > schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_version is unexpected", K(ret), K_(schema_version), K(schema_version));
  // } else if (schema_version_ == schema_version) {
  } else if (OB_FAIL(tablet_id_map.init(ObModIds::OB_STORAGE_FILE_BLOCK_REF, OB_SERVER_TENANT_ID))) {
    LOG_WARN("failed to init tablet_id_map", K(ret));
  } else if (OB_FAIL(tfm->list_shared_tablet_ids(tablet_ids))) {
    LOG_WARN("failed to list_shared_tablet_ids", K(ret));
  } else if (tablet_ids.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      if (OB_FAIL(tablet_id_map.insert(ObTabletID(tablet_ids.at(i)), false))) {
        LOG_WARN("failed to insert", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      common::ObArenaAllocator allocator("PubBlkGCThrd");
      ObPublicBlockGCThread &gc_thread = public_block_gc_service->get_gc_thread();
      ObPublicBlockGCThreadGuard thread_guard(gc_thread, false /* tablet gc */, is_paused_, is_stopped_, allocator);
      MarkTabletIDOP mark_tablet_id_op(is_paused_, is_stopped_, tablet_id_map);
      TabletGCOP gc_tablet_op(thread_guard, is_paused_, is_stopped_);
      if (OB_FAIL(loop_tablet_id_(mark_tablet_id_op))) {
        LOG_WARN("failed to loop_tablet_id", K(ret), K(schema_version));
      } else if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(*GCTX.sql_proxy_,
                  MTL_ID(), info))) {
        LOG_WARN("fail to get global merge info", KR(ret));
      } else if (0 != info.merge_status_.value_) {
        LOG_INFO("global merge is processing, retry", K(ret), K(mark_tablet_id_op), K(schema_version));
      } else if (OB_FAIL(tablet_id_map.for_each(gc_tablet_op))) {
        LOG_WARN("failed to for_each", K(ret), K(schema_version));
      }
      thread_guard.wait_gc_task_finished();

      LOG_INFO("finish loop_tablet_gc_", K(ret), K(gc_tablet_op), K(mark_tablet_id_op), K(schema_version));
      if (OB_SUCC(ret) && thread_guard.get_gc_macro_block_cnt() > 0) {
        SERVER_EVENT_ADD("ss_macro_block_gc", "shared_dir_tablet_gc", "tenant_id", MTL_ID(), "gc_macro_block_cnt", thread_guard.get_gc_macro_block_cnt());
      }
    }
  }
}

void ObPublicBlockGCTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== public block gc timer task ======", K(GC_CHECK_INTERVAL), KPC(this));

  int64_t curr_ts = ObTimeUtility::fast_current_time();
  const int64_t last_gc_tablet_meta_loop_ts = ATOMIC_LOAD(&last_gc_tablet_meta_loop_ts_);
  const int64_t gc_tablet_meta_safe_time_val = ATOMIC_LOAD(&gc_tablet_meta_safe_time_val_);
  int64_t loop_interval = ObPublicBlockGCTask::LOOP_CHECK_INTERVAL > gc_tablet_meta_safe_time_val ? gc_tablet_meta_safe_time_val : ObPublicBlockGCTask::LOOP_CHECK_INTERVAL;
  if (last_gc_tablet_meta_loop_ts + loop_interval < curr_ts) {
    clear_is_paused();
    loop_tablet_meta_version_gc_();
    ATOMIC_STORE(&last_gc_tablet_meta_loop_ts_, curr_ts);
  }

  curr_ts = ObTimeUtility::fast_current_time();
  const int64_t last_gc_tablet_loop_ts = ATOMIC_LOAD(&last_gc_tablet_loop_ts_);
  const int64_t gc_tablet_safe_time_val = ATOMIC_LOAD(&gc_tablet_safe_time_val_);
  loop_interval = ObPublicBlockGCTask::LOOP_CHECK_INTERVAL > gc_tablet_safe_time_val ? gc_tablet_safe_time_val : ObPublicBlockGCTask::LOOP_CHECK_INTERVAL;
  if (last_gc_tablet_loop_ts + loop_interval < curr_ts) {
    clear_is_paused();
    loop_tablet_gc_();
    ATOMIC_STORE(&last_gc_tablet_loop_ts_, curr_ts);
  }
}

int ObPublicBlockGCTask::update_safe_time_config(
    const int64_t new_gc_tablet_safe_time_val,
    const int64_t new_gc_tablet_meta_safe_time_val)
{
  int64_t ret = OB_SUCCESS;
  bool is_updated = false;
  if (new_gc_tablet_safe_time_val < 0
      || new_gc_tablet_safe_time_val > 365 * 24 * 60 * 60 * 1000000L
      || new_gc_tablet_meta_safe_time_val < 0
      || new_gc_tablet_meta_safe_time_val > 365 * 24 * 60 * 60 * 1000000L) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gc time invalid", KR(ret), K(new_gc_tablet_safe_time_val), K(new_gc_tablet_meta_safe_time_val));
  } else {
    const int64_t old_gc_tablet_safe_time_val = ATOMIC_LOAD(&gc_tablet_safe_time_val_);
    const int64_t old_gc_tablet_meta_safe_time_val = ATOMIC_LOAD(&gc_tablet_meta_safe_time_val_);
    if (new_gc_tablet_safe_time_val != old_gc_tablet_safe_time_val) {
      // ATOMIC_STORE(&last_gc_tablet_loop_ts_, 0);
      ATOMIC_STORE(&gc_tablet_safe_time_val_, new_gc_tablet_safe_time_val);
      is_updated = true;
    }
    if (new_gc_tablet_meta_safe_time_val != old_gc_tablet_meta_safe_time_val) {
      // ATOMIC_STORE(&last_gc_tablet_meta_loop_ts_, 0);
      ATOMIC_STORE(&gc_tablet_meta_safe_time_val_, new_gc_tablet_meta_safe_time_val);
      // update next_gc_tablet_meta_ts;
      ATOMIC_FAA(&next_gc_tablet_meta_ts_, new_gc_tablet_meta_safe_time_val - old_gc_tablet_meta_safe_time_val);
      is_updated = true;
    }
  }
  if (is_updated) {
    set_is_paused();
  }
  LOG_INFO("update safe time config finish", KR(ret), K(new_gc_tablet_safe_time_val), 
      K(new_gc_tablet_meta_safe_time_val), KPC(this));
  return ret;
}

int ObPublicBlockGCHandler::get_seq_ids_(
    ObIArray<blocksstable::MacroBlockId> &seq_ids)
{
  int ret = OB_SUCCESS;
  seq_ids.reset();
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  ObGCTabletMetaInfoList tablet_meta_version_list;
  int64_t last_tablet_meta_version = 0;
  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handler is invalid", K(ret));
  } else if (OB_FAIL(meta_service->get_gc_tablet_scn_arr(tablet_id_, ObStorageObjectType::SHARED_MAJOR_META_LIST, tablet_meta_version_list))) {
    LOG_WARN("failed to get tablet_meta_versions", K(ret), KPC(this));
  } else if (0 == tablet_meta_version_list.tablet_version_arr_.count()) {
  } else {
    last_tablet_meta_version = tablet_meta_version_list.tablet_version_arr_.at(tablet_meta_version_list.tablet_version_arr_.count() - 1).scn_.get_val_for_inner_table_field();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(meta_service->get_next_major_shared_blocks_for_tablet(tablet_id_, last_tablet_meta_version, seq_ids))) {
    LOG_WARN("failed to get_next_major_shared_blocks_for_tablet", K(ret), KPC(this), K(last_tablet_meta_version), K(seq_ids));
  }
  return ret;
}

int ObPublicBlockGCHandler::detect_and_gc_block_(
      const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::MacroBlockId> batch_start_block_ids;
  blocksstable::MacroBlockId start_block_id = block_id;
  bool is_exist = false;

  // batch detect: delete_macro_blocks is not atomic, so the batch which first detect not exist also need is deleted.
  do {
    if (OB_FAIL(is_exist_macro_block_(start_block_id, is_exist))) {
      LOG_WARN("fail to check existence", K(ret), K(start_block_id));
    } else if (OB_FAIL(batch_start_block_ids.push_back(start_block_id))) {
      LOG_WARN("fail to push_back", K(ret), KPC(this), K(start_block_id));
    } else {
      start_block_id.set_third_id(start_block_id.third_id() + ObPublicBlockGCTask::BATCH_DETECT_MACRO_SIZE);
    }
  } while (OB_SUCC(ret) && is_exist);

  // batch gc
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = batch_start_block_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      ObArray<blocksstable::MacroBlockId> batch_block_ids;
      blocksstable::MacroBlockId start_id = batch_start_block_ids.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < ObPublicBlockGCTask::BATCH_DETECT_MACRO_SIZE; j++) {
        if (OB_FAIL(batch_block_ids.push_back(start_id))) {
          LOG_WARN("failed to push", K(ret), K(i), K(j), K(batch_start_block_ids.count()), K(batch_start_block_ids));
        } else {
          start_id.set_third_id(start_id.third_id() + 1);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(delete_macro_blocks(batch_block_ids))) {
        LOG_WARN("failed to delete_macro_blocks", K(ret), K(batch_start_block_ids.count()), K(batch_start_block_ids));
      }
    }
  }
  LOG_INFO("finish detect and gc blocks", K(ret), K(block_id), K(batch_start_block_ids.count()), KPC(this), K(batch_start_block_ids));
  return ret;
}

int ObPublicBlockGCHandler::gc_macro_block_for_detect_()
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::MacroBlockId> start_block_ids;
  if (OB_FAIL(get_seq_ids_(start_block_ids))) {
    LOG_WARN("failed to get_seq_ids", K(ret), KPC(this));
  }
  for (int i = 0; OB_SUCC(ret) && i < start_block_ids.count(); i++) {
    if (!start_block_ids.at(i).is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("start_block_ids is invalid", K(ret), KPC(this), K(i), K(start_block_ids.count()), K(start_block_ids));
    } else if (OB_FAIL(detect_and_gc_block_(start_block_ids.at(i)))) {
      LOG_WARN("failed to get_seq_ids", K(ret), KPC(this), K(i), K(start_block_ids.count()), K(start_block_ids));
    }
  }
  return ret;
}

int ObPublicBlockGCHandler::list_tablet_meta_version(
    ObIArray<int64_t> &tablet_meta_versions,
    int64_t *min_retain_tablet_meta_version)
{
  int ret = OB_SUCCESS;
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  ObGCTabletMetaInfoList tablet_meta_version_list;
  ObGCTabletMetaInfoList tablet_meta_gc_version;
  SCN gc_scn;
  int64_t gc_version = -1;

  // size is equal with tablet_meta_versions
  ObArray<int64_t> tablet_meta_create_ts_array;

  tablet_meta_versions.reset();
  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handler is invalid", K(ret), KPC(this));
  } else if (NULL != min_retain_tablet_meta_version && !major_scn_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("major_scn is invalid for tablet meta version gc", K(ret), KPC(this));
  } else if (OB_FAIL(meta_service->get_gc_tablet_scn_arr(tablet_id_, ObStorageObjectType::SHARED_MAJOR_META_LIST, tablet_meta_version_list))) {
    LOG_WARN("failed to get tablet_meta_versions", K(ret), KPC(this));
  } else if (OB_FAIL(meta_service->get_gc_tablet_scn_arr(tablet_id_, ObStorageObjectType::SHARED_MAJOR_GC_INFO, tablet_meta_gc_version))) {
    LOG_WARN("failed to get gc info", K(ret), KPC(this));
  } else if (tablet_meta_gc_version.tablet_version_arr_.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc info is invalid", K(ret), KPC(this), K(tablet_meta_gc_version.tablet_version_arr_.count()));
  } else if (0 == tablet_meta_gc_version.tablet_version_arr_.count()) {
    gc_scn.set_min();
  } else if (FALSE_IT(gc_scn = tablet_meta_gc_version.tablet_version_arr_.at(0).scn_)) {
  } else if (!gc_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc scn is invalid", K(ret), KPC(this)); 
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObGCTabletMetaInfo> &tablet_versions_in_version_list = tablet_meta_version_list.tablet_version_arr_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_versions_in_version_list.count(); i++) {
      ObGCTabletMetaInfo meta_info = tablet_versions_in_version_list.at(i);
      if (!meta_info.is_valid()
          || INT64_MAX == meta_info.tablet_meta_create_ts_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet meta scn is invalid", K(ret), K(meta_info), KPC(this), K(i), K(tablet_meta_gc_version.tablet_version_arr_.count())); 
      } else if (gc_scn < meta_info.scn_) {
        // tablet meta version gc need skip unfinish major version
        if (NULL != min_retain_tablet_meta_version && meta_info.scn_ > major_scn_) {
          LOG_INFO("there is uncompaction finish tablet_version, skip it", K(i), K(tablet_versions_in_version_list.count()), K(tablet_meta_version_list), KPC(this));
          break;
        }
        if (OB_FAIL(tablet_meta_versions.push_back(meta_info.scn_.get_val_for_inner_table_field()))) {
          LOG_WARN("failed to push back", K(ret), KPC(this), K(i), K(tablet_meta_gc_version.tablet_version_arr_.count())); 
        } 
        // getting min_retain_tablet_meta_version need tablet_meta_create_ts
        else if (NULL != min_retain_tablet_meta_version
            && OB_FAIL(tablet_meta_create_ts_array.push_back(meta_info.tablet_meta_create_ts_))) {
          LOG_WARN("failed to push back", K(ret), KPC(this), K(i), K(tablet_meta_gc_version.tablet_version_arr_.count())); 
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(min_retain_tablet_meta_version)) {
  } else if (tablet_meta_versions.count() == 0) {
    // shared dir maybe has no tablet_meta
  } else if (tablet_meta_versions.count() == 1) {
    // only one version not be gc
  } else if (INT64_MAX == retain_tablet_meta_ts_ || retain_tablet_meta_ts_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("retain_tablet_meta_ts_ is invalid", KR(ret), KPC(this), KP(min_retain_tablet_meta_version));
  } else {
    int64_t i = 0;
    for (; i < tablet_meta_versions.count(); i++) {
      if (tablet_meta_create_ts_array.at(i) > retain_tablet_meta_ts_) {
        break;
      }
    }
    // i = 0, all meta version need be retain, so min_retain_tablet_meta_version set 0.
    const int64_t retain_idx = (0 == i) ? 0 : i - 1;
    *min_retain_tablet_meta_version = tablet_meta_versions.at(retain_idx);
    // i = tablet_meta_versions.count, only last meta version need be retain, so next_gc_tablet_meta_ts_ set INT64_MAX.
    next_gc_tablet_meta_ts_ = tablet_meta_versions.count() == i ? INT64_MAX : tablet_meta_create_ts_array.at(retain_idx + 1);
  }
  LOG_INFO("list tablet meta version", K(ret), K(tablet_meta_versions), K(tablet_meta_create_ts_array), 
      K(gc_scn), K(tablet_meta_version_list), KP(min_retain_tablet_meta_version), KPC(this));
  return ret;
}

int ObPublicBlockGCHandler::delete_tablet_meta_version(
  int64_t tablet_meta_version)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId file_id;
  ObStorageObjectOpt opt;
  opt.set_ss_share_tablet_meta_object_opt(tablet_id_.id(), tablet_meta_version);
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(tfm->delete_file(file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(file_id), KPC(this));
    }
  }
  return ret;
}

int ObPublicBlockGCHandler::delete_shared_tablet_id_()
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId file_id;
  ObStorageObjectOpt opt;
  opt.set_ss_shared_tablet_id_object_opt(tablet_id_.id());
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(tfm->delete_file(file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(file_id), KPC(this));
    }
  }
  return ret;
}

int ObPublicBlockGCHandler::delete_major_prewarm_(const int64_t tablet_meta_version)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId data_index_file_id;
  blocksstable::MacroBlockId data_file_id;
  blocksstable::MacroBlockId meta_index_file_id;
  blocksstable::MacroBlockId meta_file_id;
  ObStorageObjectOpt data_index_opt;
  ObStorageObjectOpt data_opt;
  ObStorageObjectOpt meta_index_opt;
  ObStorageObjectOpt meta_opt;
  data_index_opt.set_ss_major_prewarm_opt(ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX, tablet_id_.id(), tablet_meta_version);
  data_opt.set_ss_major_prewarm_opt(ObStorageObjectType::MAJOR_PREWARM_DATA, tablet_id_.id(), tablet_meta_version);
  meta_index_opt.set_ss_major_prewarm_opt(ObStorageObjectType::MAJOR_PREWARM_META_INDEX, tablet_id_.id(), tablet_meta_version);
  meta_opt.set_ss_major_prewarm_opt(ObStorageObjectType::MAJOR_PREWARM_META, tablet_id_.id(), tablet_meta_version);
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(data_index_opt, data_index_file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(data_opt, data_file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(meta_index_opt, meta_index_file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(meta_opt, meta_file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(tfm->delete_file(data_index_file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(data_index_file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(data_index_file_id), KPC(this));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tfm->delete_file(data_file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(data_file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(data_file_id), KPC(this));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tfm->delete_file(meta_index_file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(meta_index_file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(meta_index_file_id), KPC(this));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tfm->delete_file(meta_file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(meta_file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(meta_file_id), KPC(this));
    }
  }
  return ret;
}

int ObPublicBlockGCHandler::delete_compaction_file_()
{
  int ret = OB_SUCCESS;
  // drop ObTabletIDObj
  compaction::ObTabletIDObj tablet_id_obj(tablet_id_);
  if (OB_FAIL(tablet_id_obj.delete_object())) {
    LOG_WARN("failed to delete ObTabletIDObj", KR(ret), KPC(this));
  }
  return ret;
}

int ObPublicBlockGCHandler::post_gc_tablet_process_(
    const ObIArray<int64_t> &tablet_versions)
{
  int ret = OB_SUCCESS;
  // add delete file when tablet gc
  if (OB_FAIL(post_gc_tablet_meta_process_(tablet_versions, INT64_MAX))) {
    LOG_WARN("failed to post_gc_tablet_meta_process", KR(ret), KPC(this));
  } else if (OB_FAIL(delete_compaction_file_())) {
    LOG_WARN("failed to delete_compaction_file_", KR(ret), KPC(this));
  }
  return ret;
}

int ObPublicBlockGCHandler::post_gc_tablet_meta_process_(
    const ObIArray<int64_t> &tablet_versions,
    const int64_t min_retain_tablet_meta_version)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) &&  i < tablet_versions.count(); i++) {
    if (tablet_versions.at(i) < min_retain_tablet_meta_version) {
      // add delete file when tablet meta gc
      if (OB_FAIL(delete_major_prewarm_(tablet_versions.at(i)))) {
        LOG_WARN("failed to delete_major_prewarm_", KR(ret), KPC(this), K(i), K(tablet_versions));
      }
    }
  }
  LOG_INFO("post_gc_tablet_meta_process finish", KR(ret), KPC(this), K(min_retain_tablet_meta_version), K(tablet_versions));
  return ret;
}

int ObPublicBlockGCHandler::delete_shared_tablet_meta_()
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId meta_list_file_id;
  blocksstable::MacroBlockId gc_info_file_id;
  ObStorageObjectOpt meta_list_opt;
  ObStorageObjectOpt gc_info_opt;
  meta_list_opt.set_ss_meta_list_object_opt(tablet_id_.id());
  gc_info_opt.set_ss_gc_info_object_opt(tablet_id_.id());
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(meta_list_opt, meta_list_file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(gc_info_opt, gc_info_file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(tfm->delete_file(meta_list_file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(meta_list_file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(meta_list_file_id), KPC(this));
    }
  } else if (OB_FAIL(tfm->delete_file(gc_info_file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(gc_info_file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(gc_info_file_id), KPC(this));
    }
  }
  return ret;
}

int ObPublicBlockGCHandler::get_blocks_for_tablet(
  int64_t tablet_meta_version,
  ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  block_ids.reset();
  ObTenantStorageMetaService *tsms = MTL(ObTenantStorageMetaService*);
  if (OB_ISNULL(tsms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(tsms->get_shared_blocks_for_tablet(tablet_id_, tablet_meta_version, block_ids))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("block is deleted", K(ret), KPC(this));
    }
    LOG_WARN("failed to get_shared_blocks_for_tablet", K(ret), KPC(this), K(tablet_meta_version));
  }
  return ret;
}

int ObPublicBlockGCHandler::read_is_deleted_obj_(int64_t &delete_ts)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  MacroBlockId block_id;
  ObIsDeletedObj obj;
  int64_t pos = 0;
  opt.set_ss_is_deleted_object_opt(tablet_id_.id());
  char buf[ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE];

  if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, block_id))) {
    LOG_WARN("failed to generate obj id", KR(ret), K(opt), KPC(this));
  } else {
    ObStorageObjectReadInfo read_info;
    ObStorageObjectHandle handle;
    read_info.offset_ = 0;
    read_info.buf_ = buf;
    read_info.size_ = ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE;
    read_info.io_timeout_ms_ = 10_s;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read_info.macro_block_id_ = block_id;
    read_info.mtl_tenant_id_ = MTL_ID();
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.read_object(read_info, handle))) {
      if (OB_OBJECT_NOT_EXIST != ret) {
        LOG_WARN("failed to read obj", KR(ret), K(read_info), KPC(this));
      }
    } else if (OB_FAIL(obj.deserialize(buf, ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE, pos))) {
      LOG_WARN("failed to deserialize", KR(ret), K(opt), KPC(this));
    } else {
      delete_ts = obj.delete_ts_;
    }
  }
  LOG_INFO("is_deleted read finish", KR(ret), K(obj), KPC(this));
  return ret;
}

int ObPublicBlockGCHandler::write_is_deleted_obj_()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObIsDeletedObj obj;
  obj.delete_ts_ = ObTimeUtility::fast_current_time();
  ObStorageObjectOpt opt;
  opt.set_ss_is_deleted_object_opt(tablet_id_.id());
  ObStorageObjectWriteInfo write_info;
  ObStorageObjectHandle handle;
  char buf[ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE];
  if (OB_FAIL(obj.serialize(buf, ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE, pos))) {
    LOG_WARN("failed to serialize", KR(ret), K(opt), KPC(this));
  } else {
    write_info.buffer_ = buf;
    write_info.size_ = pos;
    write_info.offset_ = 0;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_sealed();
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.mtl_tenant_id_ = MTL_ID();
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.write_object(opt, write_info, handle))) {
      LOG_WARN("failed to write obj", KR(ret), K(opt), K(write_info), KPC(this));
    }
  } 
  LOG_INFO("is_deleted write finish", KR(ret), K(obj), KPC(this));
  return ret;
}

int ObPublicBlockGCHandler::delete_is_deleted_obj_()
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId file_id;
  ObStorageObjectOpt opt;
  opt.set_ss_is_deleted_object_opt(tablet_id_.id());
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(tfm->delete_file(file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(file_id), KPC(this));
    }
  }
  return ret;
}

int ObPublicBlockGCHandler::try_delete_tablet_data_dir()
{
  // todo : zk250686_ delete all files in dir (macro check) before partition split, delete this code after partition split
  int ret = OB_SUCCESS;
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(tfm->delete_shared_tablet_data_dir(tablet_id_.id()))) {
    LOG_WARN("failed to delete_shared_tablet_data_dir", K(ret), KPC(this));
  }
  return ret;
}

int ObPublicBlockGCHandler::gc_tablet_meta_versions()
{
  int ret = OB_SUCCESS;
  int64_t min_retain_tablet_meta_version = OB_INVALID_VERSION;
  ObGCTabletMetaInfoList tablet_meta_gc_version;
  SCN gc_tablet_meta_scn;
  ObTenantStorageMetaService *tsms = MTL(ObTenantStorageMetaService*);
  ObSEArray<int64_t, 4> tablet_versions;
  if (OB_ISNULL(tsms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(list_tablet_meta_version(tablet_versions, &min_retain_tablet_meta_version))) {
    LOG_WARN("failed to list_tablet_meta_version", K(ret), KPC(this));
  } else if (tablet_versions.count() == 0) {
    // shared dir maybe has no tablet_meta
  } else if (tablet_versions.count() == 1) {
    // only one version not be gc
  } else if (min_retain_tablet_meta_version <= 0
      || INT64_MAX == min_retain_tablet_meta_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min_retain_tablet_meta_version is invalid", KR(ret), KPC(this), K(min_retain_tablet_meta_version));
  } else if (min_retain_tablet_meta_version == tablet_versions.at(0)) {
    // last version need retain, so not need gc
  } else if (OB_FAIL(ObBlockGCHandler::gc_tablet_meta_versions(tablet_versions, min_retain_tablet_meta_version))) {
    LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), KPC(this), K(min_retain_tablet_meta_version));
  } else if (OB_FAIL(post_gc_tablet_meta_process_(tablet_versions, min_retain_tablet_meta_version))) {
    LOG_WARN("failed to post_gc_tablet_meta_process", KR(ret), K(min_retain_tablet_meta_version));
  } else if (OB_FAIL(gc_tablet_meta_scn.convert_for_inner_table_field(min_retain_tablet_meta_version - 1))) {
    LOG_WARN("failed to convert_for_inner_table_field", KR(ret), KPC(this), K(min_retain_tablet_meta_version));
  } else if (OB_FAIL(tablet_meta_gc_version.tablet_version_arr_.push_back(ObGCTabletMetaInfo(gc_tablet_meta_scn)))) {

    LOG_WARN("failed to tablet_meta_version_list", K(ret), K(gc_tablet_meta_scn), KPC(this));
  } else if (OB_FAIL(tsms->write_gc_tablet_scn_arr(tablet_id_, ObStorageObjectType::SHARED_MAJOR_GC_INFO, tablet_meta_gc_version))) {
    LOG_WARN("failed to write_gc_tablet_scn_arr", K(ret), K(min_retain_tablet_meta_version), KPC(this));
  }
  LOG_INFO("finish shared tablet meta gc", K(ret), K(gc_tablet_meta_scn), K(tablet_meta_gc_version), K(min_retain_tablet_meta_version), KPC(this));
  return ret;
}

int ObPublicBlockGCHandler::gc_tablet()
{
  int ret = OB_SUCCESS;
  int64_t delete_ts = 0;
  ObPublicBlockGCService *public_block_gc_service = MTL(ObPublicBlockGCService*);
  bool need_gc = false;
  int64_t gc_tablet_safe_time_val = 0;
  ObSEArray<int64_t, 4> tablet_versions;
  if (OB_ISNULL(public_block_gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("public_block_gc_service should not be null", K(ret));
  } else if (FALSE_IT(gc_tablet_safe_time_val = public_block_gc_service->get_gc_tablet_safe_time_val())) {
  } else if (0 == gc_tablet_safe_time_val) {
    // inmidately gc
    need_gc = true;
  } else if (OB_FAIL(read_is_deleted_obj_(delete_ts))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      if (OB_FAIL(write_is_deleted_obj_())) {
        LOG_WARN("failed to write is_deleted_obj", KR(ret), KPC(this));
      }
    } else {
      LOG_WARN("failed to read is_deleted_obj", KR(ret), KPC(this));
    }
  } else {
    const int64_t curr_ts = ObTimeUtility::fast_current_time();
    if (delete_ts + gc_tablet_safe_time_val <= curr_ts) {
      need_gc = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!need_gc) {
  } else if (OB_FAIL(gc_macro_block_for_detect_())) {
    LOG_WARN("failed to gc_macro_block_for_detect", KR(ret), KPC(this));
  } else if (OB_FAIL(list_tablet_meta_version(tablet_versions))) {
    LOG_WARN("failed to list_tablet_meta_version", K(ret), KPC(this));
  } else if (tablet_versions.count() == 0) {
    LOG_INFO("there is not tablet_versions, maybe have been gc", KR(ret), KPC(this));
  } else if (OB_FAIL(ObBlockGCHandler::gc_tablet(tablet_versions))) {
    LOG_WARN("failed to gc_tablet", KR(ret), KPC(this));
  } else if (OB_FAIL(post_gc_tablet_process_(tablet_versions))) {
    LOG_WARN("failed to post_gc_tablet_process_", KR(ret), KPC(this));
  } else if (OB_FAIL(delete_shared_tablet_meta_())) {
    LOG_WARN("failed to delete_shared_tablet_meta", KR(ret), KPC(this));
  } else if (OB_FAIL(delete_shared_tablet_id_())) {
    LOG_WARN("failed to delete_shared_tablet_id", KR(ret), KPC(this));
  } else if (OB_FAIL(delete_is_deleted_obj_())) {
    LOG_WARN("failed to delete_is_deleted_obj", KR(ret), KPC(this));
  }
  
  LOG_INFO("finish shared tablet gc", KR(ret), K(need_gc), K(delete_ts), K(gc_tablet_safe_time_val), KPC(this));
  return ret;
}

void ObPublicBlockGCThread::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObPublicBlockGCHandler *handler = NULL;
  bool is_stopped = get_gc_task_ctx()->is_stopped();
  if (is_stopped) {
    LOG_INFO("public gc service is exiting", KR(ret), KPC(this), KP(task));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is NULL", KR(ret), KPC(this));
  } else if (!is_valid_ctx()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is invalid", KR(ret), KPC(this));
  } else if (FALSE_IT(handler = static_cast<ObPublicBlockGCHandler*>(task))) {
  } else if (get_gc_task_ctx()->is_tablet_meta_gc()) {
    if(OB_FAIL(handler->gc_tablet_meta_versions())) {
      LOG_WARN("failed to gc_tablet", KR(ret), KPC(this), KPC(handler));
    } else {
      get_gc_task_ctx()->update_next_gc_tablet_meta_ts(handler->next_gc_tablet_meta_ts_);
    }
  } else {
    if (OB_FAIL(handler->gc_tablet())) {
      LOG_WARN("failed to gc_tablet", KR(ret), KPC(this), KPC(handler));
    }
  }
  if (OB_FAIL(ret) || is_stopped) {
    if (get_gc_task_ctx()->is_tablet_meta_gc()) {
      // need retry
      get_gc_task_ctx()->update_next_gc_tablet_meta_ts(0);
    }
  } else if (handler->gc_macro_block_cnt_ > 0) {
    get_gc_task_ctx()->update_gc_macro_block_cnt(handler->gc_macro_block_cnt_);
  }
  handle_end(task);
}

void ObPublicBlockGCThread::handle_drop(void *task)
{
  if (get_gc_task_ctx()->is_tablet_meta_gc()) {
    get_gc_task_ctx()->update_next_gc_tablet_meta_ts(0);
  }
  handle_end(task);
  STORAGE_LOG(INFO, "public thread pool is stop", KP(task), KPC(this));
}

} // storage
} // oceanbase
