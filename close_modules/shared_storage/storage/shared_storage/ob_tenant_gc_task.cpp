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

#include "ob_tenant_gc_task.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_file_manager.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_public_block_gc_service.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_file_manager.h"
#include "src/share/ob_common_rpc_proxy.h"

namespace oceanbase
{
namespace storage
{

const int64_t ObTenantGCTask::GC_INTERVAL = 30L * 1000L * 1000L; //30s

ObTenantGCTask::ObTenantGCTask() 
  : is_inited_(false),
    last_gc_tenant_shared_dir_loop_ts_(0)
{}

int ObTenantGCTask::init(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantGCTask has a already been inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, GC_INTERVAL, true))) {
    LOG_WARN("fail to schedule task ObTenantGCTask", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantGCTask has not been inited", K(ret));
  } else {
    loop_check_tenant_private_dir_gc_();

    // tenant shared dir gc
    if (rootserver::ObRootServiceRoleChecker::is_rootserver()) {
      int64_t gc_safe_time_val = 0;
      if (OB_FAIL(get_gc_safe_time_val_(gc_safe_time_val))) {
        LOG_WARN("failed to get_gc_safe_time_val_", K(gc_safe_time_val));
      } else {
        const int64_t curr_ts = ObTimeUtility::fast_current_time();
        const int64_t last_gc_tenant_shared_dir_loop_ts = ATOMIC_LOAD(&last_gc_tenant_shared_dir_loop_ts_);
        const int64_t loop_interval = ObPublicBlockGCTask::LOOP_CHECK_INTERVAL > gc_safe_time_val ? gc_safe_time_val : ObPublicBlockGCTask::LOOP_CHECK_INTERVAL;
        if (last_gc_tenant_shared_dir_loop_ts + loop_interval < curr_ts) {
          loop_check_tenant_shared_dir_gc_();
          ATOMIC_STORE(&last_gc_tenant_shared_dir_loop_ts_, curr_ts);
        }
      }
    }
  }
}

int ObTenantGCTask::get_gc_safe_time_val_(
    int64_t &gc_safe_time_val) const
{
  int ret = OB_SUCCESS;
  gc_safe_time_val = 0;
  ObPublicBlockGCService *public_block_gc_service = NULL;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_FAIL(guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to switch to sys tenant", K(ret));
  } else if (OB_ISNULL(public_block_gc_service = MTL(ObPublicBlockGCService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("public_block_gc_service not init, retry", K(ret));
  } else {
    gc_safe_time_val = public_block_gc_service->get_gc_tablet_safe_time_val();
  }
  return ret;
}

void ObTenantGCTask::loop_check_tenant_shared_dir_gc_() const
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== tenant public dir gc ======", KPC(this));
  ObArray<uint64_t> tenant_ids_in_dir;
  ObArray<uint64_t> tenant_ids_in_schema;
  int64_t schema_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = OB_SYS_TENANT_ID;
  if (OB_FAIL(OB_SERVER_FILE_MGR.list_shared_tenant_ids(OB_SERVER_TENANT_ID, tenant_ids_in_dir))) {
    LOG_WARN("failed to list_shared_tenant_ids", KR(ret));
  } else if (OB_FAIL(GSCHEMASERVICE.get_schema_version_in_inner_table(
        *GCTX.sql_proxy_, schema_status, schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret));
  } else if (OB_INVALID_VERSION == schema_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_version is invalid", KR(ret), K(schema_status));
  } else {
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard, schema_version))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(schema_version));
    } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids_in_schema))) {
      LOG_WARN("fail to get tenant ids", KR(ret), K(schema_version));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; i < tenant_ids_in_dir.count(); i++) {
      bool need_delete = true;
      for (int64_t j = 0; j < tenant_ids_in_schema.count(); j++) {
        if (tenant_ids_in_dir.at(i) == tenant_ids_in_schema.at(j)) {
          need_delete = false;
          break;
        }
      }
      if (need_delete && OB_FAIL(process_delete_tenant_shared_dir_(tenant_ids_in_dir.at(i)))) {
        LOG_WARN("fail to process_delete_tenant_shared_dir_", KR(ret), K(i), K(tenant_ids_in_dir), K(tenant_ids_in_schema));
      }
    }
  }
  LOG_INFO("finish loop_check_tenant_shared_dir_gc_", KR(ret), K(schema_version), K(tenant_ids_in_dir), K(tenant_ids_in_schema));
}

int ObTenantGCTask::process_delete_tenant_shared_dir_(
    const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  int64_t gc_safe_time_val = 0;
  bool need_gc = false;
  int64_t delete_ts = 0;
  if (OB_FAIL(get_gc_safe_time_val_(gc_safe_time_val))) {
    LOG_WARN("failed to get_gc_safe_time_val_", K(gc_safe_time_val));
  } else if (0 == gc_safe_time_val) {
    // inmidately gc
    need_gc = true;
  } else if (OB_FAIL(read_is_shared_tenant_deleted_obj_(tenant_id, delete_ts))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      if (OB_FAIL(write_is_shared_tenant_deleted_obj_(tenant_id))) {
        LOG_WARN("failed to write is_deleted_obj", KR(ret));
      }
    } else {
      LOG_WARN("failed to read is_deleted_obj", KR(ret));
    }
  } else {
    const int64_t curr_ts = ObTimeUtility::fast_current_time();
    if (delete_ts + gc_safe_time_val <= curr_ts) {
      need_gc = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!need_gc) {
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.delete_shared_tenant_dir(tenant_id))) {
    LOG_WARN("failed to delete_shared_tenant_dir", KR(ret), K(tenant_id));
  }
  LOG_INFO("finish tenant shared dir gc", K(ret), K(need_gc), K(tenant_id));
  return ret;
}

int ObTenantGCTask::read_is_shared_tenant_deleted_obj_(
    const uint64_t tenant_id,
    int64_t &delete_ts) const
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectOpt opt;
  blocksstable::MacroBlockId block_id;
  ObIsDeletedObj obj;
  int64_t pos = 0;
  opt.set_ss_is_shared_tenant_deleted_object_opt(tenant_id);
  char buf[ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE];

  if (OB_FAIL(blocksstable::ObObjectManager::ss_get_object_id(opt, block_id))) {
    LOG_WARN("failed to generate obj id", KR(ret), K(opt), KPC(this));
  } else {
    blocksstable::ObStorageObjectReadInfo read_info;
    blocksstable::ObStorageObjectHandle handle;
    read_info.offset_ = 0;
    read_info.buf_ = buf;
    read_info.size_ = ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE;
    read_info.io_timeout_ms_ = 10_s;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read_info.macro_block_id_ = block_id;
    read_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
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

int ObTenantGCTask::write_is_shared_tenant_deleted_obj_(
    const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObIsDeletedObj obj;
  obj.delete_ts_ = ObTimeUtility::fast_current_time();
  blocksstable::ObStorageObjectOpt opt;
  opt.set_ss_is_shared_tenant_deleted_object_opt(tenant_id);
  blocksstable::ObStorageObjectWriteInfo write_info;
  blocksstable::ObStorageObjectHandle handle;
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
    write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.write_object(opt, write_info, handle))) {
      LOG_WARN("failed to write obj", KR(ret), K(opt), K(write_info), KPC(this));
    }
  } 
  LOG_INFO("is_deleted write finish", KR(ret), K(obj), KPC(this));
  return ret;
}

void ObTenantGCTask::loop_check_tenant_private_dir_gc_() const
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== tenant private dir gc ======", KPC(this));
  ObArray<storage::ObTenantItem> deleted_tenant_items;
  ObArray<storage::ObTenantItem> abort_tenant_items;
  if (OB_FAIL(SERVER_STORAGE_META_SERVICE.get_tenant_items_by_status(ObTenantCreateStatus::DELETED, deleted_tenant_items))) {
    LOG_WARN("failed to get_tenant_items_by_status", K(ret), K(deleted_tenant_items.count()));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.get_tenant_items_by_status(ObTenantCreateStatus::CREATE_ABORT, abort_tenant_items))) {
    LOG_WARN("failed to get_tenant_items_by_status", K(ret), K(abort_tenant_items.count()));
  } else {
    delete_tenants_(deleted_tenant_items);
    delete_tenants_(abort_tenant_items);
  }
}

int ObTenantGCTask::delete_tenant_(
    const storage::ObTenantItem &item) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_SERVER_FILE_MGR.delete_local_tenant_dir(item.tenant_id_, item.epoch_))) {
    LOG_WARN("failed to delete local tenant dir files", KR(ret), K(item));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.delete_remote_tenant_dir(item.tenant_id_, item.epoch_))) {
    LOG_WARN("failed to delete remote tenant dir files", KR(ret), K(item));
  }
  LOG_INFO("finish private tenant dir gc", K(ret), K(item));
  return ret;
}

void ObTenantGCTask::delete_tenants_(
  ObIArray<storage::ObTenantItem> &items) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < items.count(); i++) {
    const storage::ObTenantItem &item = items.at(i);
    if (!item.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ObTenantItem is invalid", KR(ret), K(item));
    } else if (OB_FAIL(delete_tenant_(item))) {
      LOG_WARN("failed to delete_tenant", KR(ret), K(item));
    } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.delete_super_block_tenant_item(item.tenant_id_, item.epoch_))) {
      LOG_WARN("failed to delete_super_block_tenant_item", KR(ret), K(item));
    }
  }
}

} /* namespace storage */
} /* namespace oceanbase */
