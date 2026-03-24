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

#include "ob_file_manager.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "deps/oblib/src/lib/file/file_directory_utils.h"
#include "share/ob_ss_file_util.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "share/io/ob_io_manager.h"


namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/**
 * --------------------------------ObSegmentFileInfo------------------------------------
 */
void ObSegmentFileInfo::reset()
{
  file_id_.reset();
  func_type_ = 0;
  is_sealed_ = false;
}

/**
 * --------------------------------ObBaseFileManager------------------------------------
 */
ObBaseFileManager::ObBaseFileManager()
  : tenant_id_(OB_INVALID_TENANT_ID),
    io_callback_allocator_(),
    is_inited_(false),
    is_started_(false),
    start_server_time_s_(0),
    fd_cache_node_pool_()
{
}

ObBaseFileManager::~ObBaseFileManager() {}

int ObBaseFileManager::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t mem_limit = 1 * 1024 * 1024 * 1024LL; // 1GB
  const int64_t FIXED_COUNT = 10000; // 1w
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("share storage disk space manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(io_callback_allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, "SSIOCallback",
                                                 tenant_id, mem_limit))) {
    LOG_WARN("fail to init io callback allocator", KR(ret), K(tenant_id), K(mem_limit));
  } else if (OB_FAIL(fd_cache_node_pool_.init(FIXED_COUNT, "UniqueFdPool", tenant_id))) {
    LOG_WARN("fail to init fd cache node pool", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    start_server_time_s_ = ObTimeUtility::current_time_s(); // second level
    is_inited_ = true;
    LOG_INFO("succ to init file manager", K_(tenant_id));
  }
  return ret;
}

int ObBaseFileManager::start()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObBaseFileManager::destroy()
{
  const int64_t start_us = ObTimeUtility::fast_current_time();
  LOG_INFO("start to destroy base file manager");
  // to destroy fd_cache_node_pool safely, wait all fd_cache_node memory are free
  while (fd_cache_node_pool_.get_alloc_count() != fd_cache_node_pool_.get_free_count()) {
    if (TC_REACH_TIME_INTERVAL(10 * 1000L * 1000L)) { // 10s
      LOG_INFO("wait fd cache node free", "alloc_count", fd_cache_node_pool_.get_alloc_count(),
               "free_count", fd_cache_node_pool_.get_free_count(), K_(tenant_id));
    }
    ob_usleep(500 * 1000L); // 500ms
  }
  fd_cache_node_pool_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  io_callback_allocator_.destroy();
  is_inited_ = false;
  start_server_time_s_ = 0;
  is_started_ = false;
  const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to destroy base file manager", K(cost_us));
}

int ObBaseFileManager::get_storage_dest(ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDeviceConfig, device_config) {
    if (OB_FAIL(ObDeviceConfigMgr::get_instance().get_device_config(
        ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config))) {
      LOG_WARN("fail to get device config", KR(ret));
    } else if (OB_UNLIKELY(!device_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid device config", KR(ret), K(device_config));
    } else if (OB_FAIL(storage_dest.set(device_config.path_, device_config.endpoint_,
        device_config.access_info_, device_config.extension_))) {
      LOG_WARN("fail to set ObBackupDest", KR(ret), K(device_config));
    } else if (OB_UNLIKELY(!storage_dest.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage dest", KR(ret), K(storage_dest));
    }
  }
  return ret;
}

int ObBaseFileManager::append_file(const ObStorageObjectWriteInfo &write_info,
                                   ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_append_file(write_info, object_handle))) {
    LOG_WARN("fail to sync append file", KR(ret), K(write_info), K(object_handle));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait io finish", KR(ret), K(write_info));
  }
  return ret;
}

int ObBaseFileManager::write_file(const ObStorageObjectWriteInfo &write_info,
                                  ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_write_file(write_info, object_handle))) {
    LOG_WARN("fail to sync write file", KR(ret), K(write_info), K(object_handle));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait io finish", KR(ret), K(write_info));
  }
  return ret;
}

int ObBaseFileManager::do_aio_write(const ObStorageObjectWriteInfo &write_info,
                                    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid() || !object_handle.get_macro_id().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(write_info), K(object_handle));
  } else {
    ObStorageObjectType object_type = object_handle.get_macro_id().storage_object_type();
    if (is_read_through_storage_object_type(object_type)) {
      ObSSObjectStorageWriter object_storage_writer;
      if (OB_FAIL(object_storage_writer.aio_write(write_info, object_handle))) {
        LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
      }
    } else if (is_pin_storage_object_type(object_type)) {
      ObSSLocalCacheWriter local_cache_writer;
      if (OB_FAIL(local_cache_writer.aio_write(write_info, object_handle))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
          LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
        }
      }
    } else {
      switch (object_type) {
        case ObStorageObjectType::PRIVATE_DATA_MACRO:
        case ObStorageObjectType::PRIVATE_META_MACRO: {
          ObSSPrivateMacroWriter private_macro_writer;
          if (OB_FAIL(private_macro_writer.aio_write(write_info, object_handle))) {
            if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
              LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
            }
          }
          break;
        }
        case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
        case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
          ObSSShareMacroWriter share_macro_writer;
          if (OB_FAIL(share_macro_writer.aio_write(write_info, object_handle))) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
          break;
        }
        case ObStorageObjectType::TMP_FILE: {
          ObSSTmpFileWriter tmp_file_writer;
          if (OB_FAIL(tmp_file_writer.aio_write(write_info, object_handle))) {
            if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
              LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
            }
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected storage object type", KR(ret), K(object_type), "object_type_str",
                   get_storage_objet_type_str(object_type), K(write_info), K(object_handle));
          break;
        }
      }
    }
  }
  return ret;
}

int ObBaseFileManager::pread_file(const ObStorageObjectReadInfo &read_info,
                                  ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_pread_file(read_info, object_handle))) {
    LOG_WARN("fail to sync read file", KR(ret), K(read_info));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait io finish", KR(ret), K(read_info));
  }
  return ret;
}

int ObBaseFileManager::async_pread_file(const ObStorageObjectReadInfo &read_info,
                                        ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info), K(object_handle));
  } else if (OB_FAIL(do_aio_read(read_info, object_handle))) {
    LOG_WARN("fail to do aio read", KR(ret), K(read_info), K(object_handle));
  }
  return ret;
}

int ObBaseFileManager::do_aio_read(const ObStorageObjectReadInfo &read_info,
                                   ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info), K(object_handle));
  } else {
    // use macro_block_id in read_info, macro_id_ in object_handle has not been set yet
    ObStorageObjectType object_type = read_info.macro_block_id_.storage_object_type();
    if (is_read_through_storage_object_type(object_type)) {
      ObSSObjectStorageReader object_storage_reader;
      if (OB_FAIL(object_storage_reader.aio_read(read_info, object_handle))) {
        LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
      }
    } else if (is_pin_storage_object_type(object_type)) {
      ObSSLocalCacheReader local_cache_reader;
      if (OB_FAIL(local_cache_reader.aio_read(read_info, object_handle))) {
        LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
      }
    } else {
      switch (object_type) {
        case ObStorageObjectType::PRIVATE_DATA_MACRO:
        case ObStorageObjectType::PRIVATE_META_MACRO: {
          ObSSPrivateMacroReader private_macro_reader;
          if (OB_FAIL(private_macro_reader.aio_read(read_info, object_handle))) {
            LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
          }
          break;
        }
        case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
        case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
          ObSSShareMacroReader share_macro_reader;
          if (OB_FAIL(share_macro_reader.aio_read(read_info, object_handle))) {
            LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
          }
          break;
        }
        case ObStorageObjectType::TMP_FILE: {
          ObSSTmpFileReader tmp_file_reader;
          if (OB_FAIL(tmp_file_reader.aio_read(read_info, object_handle))) {
            LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected storage object type", KR(ret), K(object_type), "object_type_str",
                   get_storage_objet_type_str(object_type), K(read_info), K(object_handle));
          break;
        }
      }
    }
  }
  return ret;
}

int ObBaseFileManager::delete_local_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(dir_path))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to delete local dir", KR(ret), K(dir_path));
    }
  } else {
    FLOG_INFO("succ to delete local dir", K(dir_path), "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
  }
  return ret;
}

int ObBaseFileManager::delete_remote_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest storage_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path) || strlen(dir_path) >= common::MAX_PATH_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(get_storage_dest(storage_dest))) {
    LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObBackupIoAdapter adapter;
    if (OB_FAIL(adapter.del_dir(dir_path, storage_dest.get_storage_info(), true/*recursive*/))) {
      LOG_WARN("fail to delete dir", KR(ret), K(dir_path));
    } else {
      FLOG_INFO("succ to delete remote dir", K(dir_path), "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObBaseFileManager::fsync_file(const MacroBlockId &file_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  ObPathContext ctx;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_cache*/))) {
    LOG_WARN("fail to convert file path", KR(ret), K(ctx));
  } else if (OB_FAIL(ObSSFileUtil::fsync_file(ctx.get_path()))) {
    LOG_WARN("fail to fsync file", KR(ret), K(ctx));
  }
  return ret;
}

int ObBaseFileManager::get_file_length(const MacroBlockId &file_id, const int64_t ls_epoch_id, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObStorageObjectType object_type = file_id.storage_object_type();
    // step 1: get file length from local cache
    if (is_object_type_only_store_remote(object_type)) {
      // this object type only store in remote object storage, do not get file length from local cache
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    } else if (OB_FAIL(get_local_file_length(file_id, ls_epoch_id, file_length))) {
      LOG_WARN("fail to get local file length", KR(ret), K(file_id), K(ls_epoch_id), K(file_length));
    }
    // step 2: get file length from remote object storage
    if (is_pin_storage_object_type(object_type)) {
      // do nothing, this object type only store in local cache, do not get file length from remote object storage
    } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(get_remote_file_length(file_id, ls_epoch_id, file_length))) {
        LOG_WARN("fail to get remote file length", KR(ret), K(file_id), K(ls_epoch_id), K(file_length));
      }
    }
  }
  return ret;
}

int ObBaseFileManager::get_local_file_length(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    int64_t &file_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObIODFileStat statbuf;
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_cache*/))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf))) {
      LOG_WARN("fail to stat", KR(ret), K(ctx));
    } else {
      file_length = statbuf.size_;
    }
  }
  return ret;
}

int ObBaseFileManager::get_remote_file_length(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    int64_t &file_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObBackupIoAdapter adapter;
    ObBackupDest storage_dest;
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, false/*is_local_cache*/))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
    } else if (OB_FAIL(adapter.get_file_length(ctx.get_path(), storage_dest.get_storage_info(), file_length))) {
      LOG_WARN("fail to get file length", KR(ret), K(ctx));
    }
  }
  return ret;
}

int ObBaseFileManager::is_exist_file(const MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObStorageObjectType object_type = file_id.storage_object_type();
    // step 1: is exist in local cache
    if (is_object_type_only_store_remote(object_type)) {
      // do nothing, this object type only store in remote object storage, do not do not judge is exist in local cache
    } else if (OB_FAIL(is_exist_local_file(file_id, ls_epoch_id, is_exist))) {
      LOG_WARN("fail to judge local file exist", KR(ret), K(file_id), K(ls_epoch_id));
    }
    // step 2: is exist in remote object storage
    if (OB_FAIL(ret)) {
    } else if (is_pin_storage_object_type(object_type)) {
      // do nothing, this object type only store in local cache, do not judge is exist in remote object storage
    } else if (!is_exist) {
      if (OB_FAIL(is_exist_remote_file(file_id, ls_epoch_id, is_exist))) {
        LOG_WARN("fail to judge remote file exist", KR(ret), K(file_id), K(ls_epoch_id));
      }
    }
  }
  return ret;
}

int ObBaseFileManager::is_exist_local_file(const MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_cache*/))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::exist(ctx.get_path(), is_exist))) {
      LOG_WARN("fail to exist", KR(ret), K(ctx));
    }
  }
  return ret;
}

int ObBaseFileManager::is_exist_remote_file(const MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObBackupIoAdapter adapter;
    ObBackupDest storage_dest;
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, false/*is_local_cache*/))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
    } else if (OB_FAIL(adapter.is_exist(ctx.get_path(), storage_dest.get_storage_info(), is_exist))) {
      LOG_WARN("fail to list files", KR(ret), K(ctx));
    }
  }
  return ret;
}

/**
 * --------------------------------ObTenantFileManager------------------------------------
 */
ObTenantFileManager::ObTenantFileManager()
  : ObBaseFileManager(),
    is_cache_file_exist_(false),
    micro_cache_file_fd_(OB_INVALID_FD),
    flush_queue_(),
    preread_cache_mgr_(),
    segment_file_mgr_(),
    tg_id_(INVALID_TG_ID),
    tmp_file_flush_task_(),
    find_tmp_file_flush_task_(),
    persist_disk_space_task_(),
    calibrate_disk_space_task_(),
    gc_local_major_data_task_(),
    check_expand_disk_task_(),
    fd_cache_mgr_(),
    is_pause_gc_(false),
    is_tmp_file_cache_pause_gc_(false),
    is_stop_(false),
    disk_meta_lock_(ObLatchIds::FILE_MANAGER_LOCK)
{
}

ObTenantFileManager::~ObTenantFileManager()
{
  destroy();
}

int ObTenantFileManager::mtl_init(ObTenantFileManager *&tenant_file_manager)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_file_manager->init(MTL_ID()))) {
    LOG_WARN("fail to init", KR(ret));
  } else {
    LOG_INFO("succ to init tenant file manager", K(MTL_ID()));
  }
  return ret;
}

int ObTenantFileManager::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBaseFileManager::init(tenant_id))) {
    LOG_WARN("fail to init ObBaseFileManager", KR(ret), K(tenant_id));
  } else if (OB_FAIL(preread_cache_mgr_.init(this))) {
    LOG_WARN("fail to init preread cache manager", KR(ret));
  } else if (OB_FAIL(segment_file_mgr_.init(this))) {
    LOG_WARN("fail to init segment file manager", KR(ret));
  } else if (OB_FAIL(fd_cache_mgr_.init())) {
    LOG_WARN("fail to init fd cache mgr", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_file_flush_task_.init(this))) {
    LOG_WARN("fail to init tmp file flush task", KR(ret));
  } else if (OB_FAIL(find_tmp_file_flush_task_.init())) {
    LOG_WARN("fail to init find tmp file flush task", KR(ret));
  } else if (OB_FAIL(persist_disk_space_task_.init(tenant_id))) {
    LOG_WARN("fail to init persist disk space task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(calibrate_disk_space_task_.init(tenant_id))) {
    LOG_WARN("fail to init calibrate disk space task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(gc_local_major_data_task_.init(tenant_id_))) {
    LOG_WARN("fail to init gc local major data task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_expand_disk_task_.init(tenant_id_))) {
    LOG_WARN("fail to init check expand disk task", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantFileManager::start()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int sys_ret = 0;
  char micro_cache_file_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(OB_DIR_MGR.create_tenant_dir(tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to create tenant dir", KR(ret), K_(tenant_id), K(MTL_EPOCH_ID()));
  } else if (OB_FAIL(get_micro_cache_file_path(micro_cache_file_path, sizeof(micro_cache_file_path), MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get micaro cache file path", KR(ret), K(micro_cache_file_path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::exist(micro_cache_file_path, is_cache_file_exist_))) {
    LOG_WARN("fail to check if file exist", KR(ret), K(micro_cache_file_path));
  } else {
    int open_flag = is_cache_file_exist_ ? O_DIRECT | O_RDWR | O_LARGEFILE
                                         : O_CREAT | O_EXCL | O_DIRECT | O_RDWR | O_LARGEFILE;
    if (OB_FAIL(ObSSFileUtil::open(micro_cache_file_path, open_flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH,
                                   micro_cache_file_fd_))) {
      LOG_ERROR("fail to open micro cache file", KR(ret), K(micro_cache_file_path));
    } else {
      // read tenant disk space meta file
      ObTenantDiskSpaceMeta disk_space_meta;
      if (OB_FAIL(read_tenant_disk_space_meta(disk_space_meta))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to read tenant disk space meta", KR(ret), K(disk_space_meta));
        }
      } else if (OB_UNLIKELY(disk_space_meta.body_.tenant_id_ != tenant_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected error", KR(ret), K_(tenant_id), K(disk_space_meta));
      } else if (OB_FAIL(MTL(ObTenantDiskSpaceManager *)->update_cache_disk_ratio(disk_space_meta.body_.disk_cache_ratio_))) {
        LOG_WARN("fail to update cache disk ratio info", KR(ret), K(disk_space_meta));
      } else if (!is_cache_file_exist_) {
        if (OB_FAIL(MTL(ObTenantDiskSpaceManager*)->check_micro_cache_file_size(0))) {
          LOG_WARN("fail to resize micro cache file size", KR(ret));
        }
      } else {
        // check micro_cache_file_size when observer restart
        ObIODFileStat f_stat;
        if (OB_FAIL(ObIODeviceLocalFileOp::stat(micro_cache_file_path, f_stat))) {
          LOG_WARN("fail to stat file", KR(ret), K(micro_cache_file_path));
        } else if (OB_FAIL(MTL(ObTenantDiskSpaceManager*)->check_micro_cache_file_size(f_stat.size_))) {
          LOG_WARN("fail to resize micro cache file size", KR(ret), K(f_stat.size_));
        }
      }
      
      int64_t tmp_file_write_cache_alloc_size = 0;
      int64_t tmp_file_read_cache_alloc_size = 0;
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(tmp_file_write_cache_alloc_size = disk_space_meta.body_.tmp_file_write_cache_alloc_size_ + disk_space_meta.body_.tmp_file_read_cache_alloc_size_)) {
      } else if (FALSE_IT(tmp_file_read_cache_alloc_size = 0)) {
      } else if (OB_FAIL(MTL(ObTenantDiskSpaceManager*)->calibrate_alloc_size(disk_space_meta.body_.private_macro_alloc_size_, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
        LOG_WARN("fail to calibrate alloc private macro disk size", KR(ret), K(disk_space_meta));
      } else if (OB_FAIL(MTL(ObTenantDiskSpaceManager*)->calibrate_alloc_size(disk_space_meta.body_.meta_file_alloc_size_, ObStorageObjectType::PRIVATE_TABLET_META))) {
        LOG_WARN("fail to calibrate alloc meta file disk size", KR(ret), K(disk_space_meta));
      } else if (OB_FAIL(MTL(ObTenantDiskSpaceManager*)->calibrate_alloc_size(tmp_file_write_cache_alloc_size, ObStorageObjectType::TMP_FILE, false/*is_tmp_file_read_cache*/))) {
        LOG_WARN("fail to calibrate tmp file write cache alloc size", KR(ret), K(disk_space_meta));
      } else if (OB_FAIL(MTL(ObTenantDiskSpaceManager*)->calibrate_alloc_size(tmp_file_read_cache_alloc_size, ObStorageObjectType::TMP_FILE, true/*is_tmp_file_read_cache*/))) {
        LOG_WARN("fail to calibrate alloc tmp file read disk size", KR(ret), K(disk_space_meta));
      }
      // delete local major_data during start server,
      // because async gc_major_data_task to delete major_data maybe have opened major_macro, resulting in no unlink files
      if (OB_TMP_FAIL(delete_local_major_data_dir(start_server_time_s_))) {
        LOG_WARN("fail to delete local major data dir", KR(tmp_ret), K_(start_server_time_s));
      }
      // start tenant timer task
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TmpFileFlushTimer, tg_id_))) {
        LOG_WARN("fail to create timer thread", KR(ret));
      } else if (OB_FAIL(TG_START(tg_id_))) {
        LOG_WARN("fail to start timer thread", KR(ret), K_(tg_id));
      } else if (OB_FAIL(start_timer_task())) {
        LOG_WARN("fail to start tenant timer task", KR(ret));
      } else if (OB_FAIL(preread_cache_mgr_.start())) {
        LOG_WARN("fail to start preread cache manager", KR(ret));
      } else if (OB_FAIL(segment_file_mgr_.start())) {
        LOG_WARN("fail to start segment file manager", KR(ret));
      } else if (OB_FAIL(fd_cache_mgr_.start(tg_id_))) {
        LOG_WARN("fail to start fd cache mgr", KR(ret), K_(tg_id));
      }
      // when restart observer, calibrate disk space
      if (OB_TMP_FAIL(calibrate_disk_space())) {
        LOG_WARN("fail to calibrate disk space", KR(tmp_ret));
      }
      if (OB_SUCC(ret)) {
        is_started_ = true;
        LOG_INFO("succeed to start tenant file manager", K_(is_cache_file_exist), K(micro_cache_file_path));
      }
    }
  }
  return ret;
}

int ObTenantFileManager::start_timer_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(tmp_file_flush_task_.start(tg_id_))) {
    LOG_WARN("fail to start tmp file flush task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(find_tmp_file_flush_task_.start(tg_id_))) {
    LOG_WARN("fail to start find unsealed tmp file to flush task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(persist_disk_space_task_.start(tg_id_))) {
    LOG_WARN("fail to start persist disk space task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(calibrate_disk_space_task_.start())) {
    LOG_WARN("fail to start calibrate disk space task", KR(ret));
  } else if (OB_FAIL(gc_local_major_data_task_.start(tg_id_))) {
    LOG_WARN("fail to start gc local major data task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(check_expand_disk_task_.start(tg_id_))) {
    LOG_WARN("fail to start check expand disk task", KR(ret), K_(tg_id));
  }
  return ret;
}

void ObTenantFileManager::destroy()
{
  int ret = OB_SUCCESS;
  is_stop_ = true;
  is_pause_gc_ = false;
  is_tmp_file_cache_pause_gc_ = false;
  if (micro_cache_file_fd_ != OB_INVALID_FD) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObSSFileUtil::close(micro_cache_file_fd_))) {
      LOG_ERROR("fail to close fd", KR(tmp_ret), K_(micro_cache_file_fd));
    }
  }
  micro_cache_file_fd_ = OB_INVALID_FD;
  if (INVALID_TG_ID != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  find_tmp_file_flush_task_.destroy();
  tmp_file_flush_task_.destroy();
  persist_disk_space_task_.destroy();
  calibrate_disk_space_task_.destroy();
  gc_local_major_data_task_.destroy();
  check_expand_disk_task_.destroy();
  fd_cache_mgr_.destroy();
  segment_file_mgr_.destroy();
  preread_cache_mgr_.destroy();
  const int64_t queue_size = flush_queue_.size();
  ObLink *ptr = nullptr;
  ObSegmentFileInfo *seg_file_info = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && (i < queue_size); i++) {
    if (OB_FAIL(flush_queue_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to pop", KR(ret));
      }
    } else {
      seg_file_info = static_cast<ObSegmentFileInfo *>(ptr);
      OB_DELETE(ObSegmentFileInfo, attr, seg_file_info);
    }
  }
  // must wait all io finish, before destroy io_callback_allocator_ in ObBaseFileManager::destroy()
  if (IS_INIT) {
    const int64_t start_us = ObTimeUtility::current_time_us();
    ObTenantIOManager *tenant_io_mgr = nullptr;
    if (OB_ISNULL(tenant_io_mgr = MTL(ObTenantIOManager *))) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tenant io manager should not be null", "tenant_id", MTL_ID());
    } else {
      while (1 != tenant_io_mgr->get_ref_cnt()) {
        if (REACH_TIME_INTERVAL(1000L * 1000L)) { // 1s
          LOG_INFO("wait tenant io manager quit", "tenant_id", MTL_ID(), K(start_us), "ref_cnt",
                   tenant_io_mgr->get_ref_cnt());
        }
        ob_usleep((useconds_t)10L * 1000L); //10ms
      }
    }
  }
  ObBaseFileManager::destroy();
}

void ObTenantFileManager::stop()
{
  is_stop_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  fd_cache_mgr_.stop();
  segment_file_mgr_.stop();
  preread_cache_mgr_.stop();
  calibrate_disk_space_task_.stop();
}

void ObTenantFileManager::wait()
{
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  fd_cache_mgr_.wait();
  segment_file_mgr_.wait();
  preread_cache_mgr_.wait();
  calibrate_disk_space_task_.wait();
}

int ObTenantFileManager::pwrite_cache_block(const int64_t offset,
                                            const int64_t size,
                                            const char *buf,
                                            int64_t &write_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(is_started), K_(tenant_id));
  } else if (OB_UNLIKELY(buf == nullptr || size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(size), K(offset));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::pwrite_impl(micro_cache_file_fd_, buf, size, offset, write_size))) {
    LOG_WARN("failed to pwrite", KR(ret), K_(micro_cache_file_fd), K(size), K(offset));
  }
  return ret;
}

int ObTenantFileManager::pread_cache_block(const int64_t offset,
                                           const int64_t size,
                                           char *buf,
                                           int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(is_started), K_(tenant_id));
  } else if (OB_UNLIKELY(buf == nullptr || size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(size), K(offset));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(micro_cache_file_fd_, buf, size, offset, read_size))) {
    LOG_WARN("failed to pread", KR(ret), K_(micro_cache_file_fd), K(size), K(offset));
  }
  return ret;
}

int ObTenantFileManager::fsync_cache_file()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(is_started), K_(tenant_id));
  } else if (OB_UNLIKELY(micro_cache_file_fd_ < 0)) {
    ret = OB_IO_ERROR;
    LOG_WARN("invalid micro cache file fd", KR(ret), K_(micro_cache_file_fd));
  } else if (OB_UNLIKELY(0 != ::fsync(micro_cache_file_fd_))) {
    ret = ObIODeviceLocalFileOp::convert_sys_errno();
    LOG_WARN("::fsync failed", KR(ret), K_(micro_cache_file_fd), K(errno), KERRMSG);
  } else {
    LOG_INFO("micro cache file fsync cost time us", K_(tenant_id), K_(micro_cache_file_fd),
             "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
  }
  return ret;
}

int ObTenantFileManager::resize_total_disk_space(const int64_t new_disk_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    lib::ObMutexGuard guard(disk_meta_lock_);

    ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
    if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K_(tenant_id));
    } else if (OB_FAIL(tnt_disk_space_mgr->resize_total_disk_size(new_disk_size))) {
      LOG_WARN("fail to resize total disk size", KR(ret), K(new_disk_size));
    }
  }
  return ret;
}

int ObTenantFileManager::adjust_cache_disk_ratio(
    const int64_t new_micro_cache_pct, 
    const int64_t new_macro_cache_pct,
    bool &succ_adjust,
    int64_t &micro_cache_reserved_size)
{
  int ret = OB_SUCCESS;
  succ_adjust = false;
  micro_cache_reserved_size = 0;
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
    ObTenantDiskSpaceMeta disk_space_meta;
    disk_space_meta.body_.tenant_id_ = tenant_id_;

    lib::ObMutexGuard guard(disk_meta_lock_);
    int64_t ori_micro_cache_reserver_size = 0;
    if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K_(tenant_id));
    // 1. Firstly, we need to update cache_percent in memory(all_disk_cache_info), cuz we need to ensure that,
    //    when we write tenant_disk_space_meta later, it is a valid operation(Because alloc_size will be changed
    //    during this period, and it will affect update_cache_disk_ratio).
    } else if (OB_FAIL(tnt_disk_space_mgr->update_cache_disk_ratio(new_micro_cache_pct, new_macro_cache_pct, 
               succ_adjust, ori_micro_cache_reserver_size))) {
      LOG_WARN("fail to update cache disk ratio", KR(ret), K(new_micro_cache_pct), K(new_macro_cache_pct));
    } else if (succ_adjust) {
      // 2. Secondly, we can persist tenant_disk_space_meta after we ensure that we can update cache ratio.
      if (OB_FAIL(do_build_tenant_disk_space_meta(disk_space_meta))) {
        LOG_WARN("fail to do build tenant disk space meta", KR(ret), K(disk_space_meta));
      } else if (OB_FAIL(do_write_tenant_disk_space_meta(disk_space_meta))) {
        LOG_WARN("fail to do write tenant disk space meta", KR(ret), K(disk_space_meta));
      } else {
        micro_cache_reserved_size = tnt_disk_space_mgr->get_micro_cache_reserved_size();
        const int64_t delta_size = micro_cache_reserved_size - ori_micro_cache_reserver_size;
        // 3. Finally, we should fallocate micro_cache_file after persist disk_space_meta
        if (OB_UNLIKELY(delta_size <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("delta size is invalid", KR(ret), K(delta_size), K(new_micro_cache_pct), K(new_macro_cache_pct), 
            K(disk_space_meta), K(micro_cache_reserved_size), K(ori_micro_cache_reserver_size));
        } else if (OB_FAIL(tnt_disk_space_mgr->falloc_micro_cache_file(ori_micro_cache_reserver_size, delta_size))) {
          LOG_WARN("fail to falloc micro_cache file", KR(ret), K(ori_micro_cache_reserver_size), K(delta_size));
        } else {
          FLOG_INFO("succ to adjust cache disk ratio", K(new_micro_cache_pct), K(new_macro_cache_pct));
        }
      }
    }
  }
  return ret;
}

int ObTenantFileManager::read_tenant_disk_space_meta(ObTenantDiskSpaceMeta &disk_space_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    DefaultPageAllocator allocator("DiskSpaceMeta", tenant_id_);
    MacroBlockId file_id;
    file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TENANT_DISK_SPACE_META);
    file_id.set_second_id(tenant_id_);
    file_id.set_third_id(MTL_EPOCH_ID());
    int64_t ls_epoch = 0;
    int64_t file_size = 0;
    char *buf = nullptr;
    lib::ObMutexGuard guard(disk_meta_lock_);

    if (OB_FAIL(get_file_length(file_id, ls_epoch, file_size))) {
      LOG_WARN("fail to get file size", KR(ret), K(file_id), K(ls_epoch), K(file_size));
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(file_size));
    } else {
      ObStorageObjectHandle object_handle;
      ObStorageObjectReadInfo read_info;
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      read_info.buf_ = buf;
      read_info.macro_block_id_ = file_id;
      read_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
      read_info.offset_ = 0;
      read_info.size_ = file_size;
      read_info.ls_epoch_id_ = ls_epoch;
      read_info.mtl_tenant_id_ = tenant_id_;
      int64_t pos = 0;
      if (OB_FAIL(pread_file(read_info, object_handle))) {
        LOG_WARN("fail to pread file", KR(ret), K(read_info));
      } else if (OB_FAIL(disk_space_meta.deserialize(
          object_handle.get_buffer(), object_handle.get_data_size(), pos))) {
        LOG_WARN("fail to deserialize", KR(ret), K(file_id), K(read_info), K(object_handle));
      } else if (OB_UNLIKELY(!disk_space_meta.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid disk space meta", KR(ret), K(disk_space_meta));
      } else {
        LOG_INFO("succeed read tenant disk space meta", KR(ret), K(disk_space_meta), K(pos));
      }
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObTenantFileManager::write_tenant_disk_space_meta()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    lib::ObMutexGuard guard(disk_meta_lock_);
    ObTenantDiskSpaceMeta disk_space_meta;
    disk_space_meta.body_.tenant_id_ = tenant_id_;
    if (OB_FAIL(do_build_tenant_disk_space_meta(disk_space_meta))) {
      LOG_WARN("fail to do build tenant disk space meta", KR(ret), K(disk_space_meta));
    } else if (OB_FAIL(do_write_tenant_disk_space_meta(disk_space_meta))) {
      LOG_WARN("fail to do write tenant disk space meta", KR(ret), K(disk_space_meta));
    }
  }
  return ret;
}

int ObTenantFileManager::do_build_tenant_disk_space_meta(ObTenantDiskSpaceMeta &disk_space_meta)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K_(tenant_id));
  } else {
    const ObTenantAllDiskCacheInfo &cache_disk_info = tnt_disk_space_mgr->get_all_disk_cache_info();
    if (OB_FAIL(disk_space_meta.body_.assign_by_disk_info(cache_disk_info))) {
      LOG_WARN("fail to assign disk space meta body", KR(ret), K(cache_disk_info));
    } else if (OB_FAIL(disk_space_meta.header_.construct_header(disk_space_meta.body_))) {
      LOG_WARN("fail to construct disk space meta header", KR(ret), K(disk_space_meta));
    } else if (OB_UNLIKELY(!disk_space_meta.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("disk space meta is invalid", KR(ret), K(disk_space_meta));
    }
  }
  return ret;
}

int ObTenantFileManager::do_write_tenant_disk_space_meta(const ObTenantDiskSpaceMeta &disk_space_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!disk_space_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(disk_space_meta));
  } else {
    DefaultPageAllocator allocator("DiskSpaceMeta", tenant_id_);
    MacroBlockId file_id;
    file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TENANT_DISK_SPACE_META);
    file_id.set_second_id(tenant_id_);
    file_id.set_third_id(MTL_EPOCH_ID());
    const int64_t serialize_size = disk_space_meta.get_serialize_size();
    const int64_t buf_len = upper_align(serialize_size, DIO_ALIGN_SIZE);
    int64_t pos = 0;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(buf_len));
    } else if (OB_FAIL(disk_space_meta.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize disk space meta", KR(ret), K(disk_space_meta));
    } else if (OB_UNLIKELY(pos != serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pos", KR(ret), K(pos), K(serialize_size));
    } else {
      memset(buf + pos, 0, buf_len - pos);
      int64_t ls_epoch = 0;
      ObStorageObjectWriteInfo write_info;
      ObStorageObjectHandle object_handle;
      write_info.buffer_ = buf;
      write_info.size_ = buf_len;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      write_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
      write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      write_info.ls_epoch_id_ = ls_epoch;
      write_info.mtl_tenant_id_ = tenant_id_;
      if (OB_FAIL(object_handle.set_macro_block_id(file_id))) {
        LOG_WARN("fail to set file id", KR(ret), K(file_id));
      } else if (OB_FAIL(write_file(write_info, object_handle))) {
        LOG_WARN("fail to write tenant disk space meta", KR(ret), K(disk_space_meta));
      }
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObTenantFileManager::calc_disk_space(const char *dir_path, const int64_t start_calc_time_s, int64_t &total_file_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0) || OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s), KP(dir_path));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    total_file_size = 0;
    ObDirCalcSizeOp dir_calc_size_op(is_stop_);
    ObDirCalcSizeOp file_calc_size_op(is_stop_);
    if (OB_FAIL(dir_calc_size_op.set_start_calc_size_time(start_calc_time_s))) {
      LOG_WARN("fail to set start calculate size time", KR(ret), K(start_calc_time_s));
    } else if (OB_FAIL(file_calc_size_op.set_start_calc_size_time(start_calc_time_s))) {
      LOG_WARN("fail to set start calculate size time", KR(ret), K(start_calc_time_s));
    } else if (OB_FAIL(list_local_files_rec(dir_path, file_calc_size_op, dir_calc_size_op))) {
      LOG_WARN("fail to list local files recursion", KR(ret), K(dir_path));
    } else {
      total_file_size += file_calc_size_op.get_total_file_size();
      total_file_size += dir_calc_size_op.get_total_file_size();
      LOG_INFO("succ to calc disk space", K(dir_path), K(total_file_size),
               "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::calc_private_macro_disk_space(const int64_t start_calc_time_s, int64_t &total_file_size)
{
  int ret = OB_SUCCESS;
  char tablet_data_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tablet_data_dir(tablet_data_dir, sizeof(tablet_data_dir), MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get tablet data dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(tablet_data_dir, start_calc_time_s, total_file_size))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(tablet_data_dir));
  } else {
    LOG_INFO("succ to calc private macro disk space", K(total_file_size));
  }
  return ret;
}

int ObTenantFileManager::calc_tmp_data_disk_space(const int64_t start_calc_time_s,
                                                  int64_t &total_read_cache_size,
                                                  int64_t &total_write_cache_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    total_read_cache_size = 0;
    total_write_cache_size = 0;
    ObDirCalcSizeOp dir_calc_size_op(is_stop_);
    ObDirCalcSizeOp file_calc_size_op(is_stop_);
    file_calc_size_op.set_tmp_file_calc();
    char tmp_data_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(file_calc_size_op.set_start_calc_size_time(start_calc_time_s))) {
      LOG_WARN("fail to set start calculate size time", KR(ret), K(start_calc_time_s));
    } else if (OB_FAIL(dir_calc_size_op.set_start_calc_size_time(start_calc_time_s))) {
      LOG_WARN("fail to set start calculate size time", KR(ret), K(start_calc_time_s));
    } else if (OB_FAIL(OB_DIR_MGR.get_local_tmp_data_dir(tmp_data_path, sizeof(tmp_data_path), MTL_ID(), MTL_EPOCH_ID()))) {
      LOG_WARN("fail to get tmp data dir", KR(ret));
    } else if (OB_FAIL(list_local_files_rec(tmp_data_path, file_calc_size_op, dir_calc_size_op))) {
      LOG_WARN("fail to list local files", KR(ret), K(tmp_data_path));
    } else {
      total_read_cache_size += file_calc_size_op.get_total_tmp_file_read_cache_size();
      total_write_cache_size += file_calc_size_op.get_total_file_size();
      total_write_cache_size += dir_calc_size_op.get_total_file_size(); // tmp_file_dir size stat in tmp_file_write_cache
      LOG_INFO("succ to calc tmp data disk space", K(total_write_cache_size), K(total_read_cache_size),
               "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::calc_major_macro_disk_space(const int64_t start_calc_time_s, int64_t &total_file_size)
{
  int ret = OB_SUCCESS;
  char major_data_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_major_data_dir(major_data_path, sizeof(major_data_path),  MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get major data dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(major_data_path, start_calc_time_s, total_file_size))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(major_data_path));
  } else {
    LOG_INFO("succ to calc major data disk space", K(total_file_size));
  }
  return ret;
}

int ObTenantFileManager::calc_meta_file_disk_space(const int64_t start_calc_time_s, int64_t &total_file_size)
{
  int ret = OB_SUCCESS;
  char ls_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s));
  } else if (OB_FAIL(OB_DIR_MGR.get_ls_dir(ls_dir, sizeof(ls_dir),  MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get ls dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(ls_dir, start_calc_time_s, total_file_size))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(ls_dir));
  } else {
    LOG_INFO("succ to calc meta file disk space", K(total_file_size));
  }
  return ret;
}

// this append interface is designed for tmp file
int ObTenantFileManager::async_append_file(const ObStorageObjectWriteInfo &write_info,
                                           ObStorageObjectHandle &object_handle)
{
  return segment_file_mgr_.async_append_file(write_info, object_handle);
}

int ObTenantFileManager::async_write_file(const ObStorageObjectWriteInfo &write_info,
                                          ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle ori_object_handle(object_handle);  // when aio write failed, object_handle can reset
  ObStorageObjectWriteInfo new_write_info(write_info);
  const MacroBlockId &file_id = object_handle.get_macro_id();
  ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((ObStorageObjectType::TMP_FILE == object_type) ||
                         !new_write_info.is_valid() || !file_id.is_valid() ||
                         new_write_info.io_desc_.is_write_through())) {
    // disable user layer to set write_through/back, only io layer can determine write_through/back
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type), K(new_write_info), K(object_handle));
  } else {
    bool need_free_file_size = true;
    if (OB_FAIL(try_fsync_private_macro_parent_dir(object_type, file_id,
                                                   new_write_info.ls_epoch_id_))) {
      LOG_WARN("fail to try fsync private macro dir", KR(ret), "macro_id",
               file_id, "ls_epoch_id", new_write_info.ls_epoch_id_);
    // if current object type only store remote, do not need alloc file size
    } else if (!is_object_type_only_store_remote(object_type) &&
               OB_FAIL(alloc_file_size(object_type, new_write_info.size_))) {
      // for PRIVATE_DATA_MACRO/PRIVATE_META_MACRO, if disk space is not enough, then write through object storage
      if ((OB_SERVER_OUTOF_DISK_SPACE == ret) &&
          (ObStorageObjectType::PRIVATE_DATA_MACRO == object_type ||
          ObStorageObjectType::PRIVATE_META_MACRO == object_type)) {
        ret = OB_SUCCESS; // ignore ret, and then write through
        new_write_info.io_desc_.set_write_through(true);
        need_free_file_size = false; // write through no need to alloc and free file size
      } else {
        LOG_WARN("fail to alloc file size", KR(ret), K(object_type), "object_type_str",
                get_storage_objet_type_str(object_type), "size", new_write_info.size_);
      }
    }
    if (FAILEDx(aio_write_with_create_parent_dir(new_write_info, ori_object_handle,
                need_free_file_size, new_write_info.size_, object_handle))) {
      LOG_WARN("fail to aio write with create parent dir", KR(ret), K(new_write_info), K(object_handle));
    }
  }
  return ret;
}

int ObTenantFileManager::async_pread_file(const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info), K(object_handle));
  } else if (OB_FAIL(do_aio_read(read_info, object_handle))) {
    LOG_WARN("fail to do aio read", KR(ret), K(read_info), K(object_handle));
  } else if (OB_TMP_FAIL(preread_cache_mgr_.refresh_lru_node_if_need(read_info))) { // do not affect async_read
    LOG_WARN("fail to check preread lru if need refresh", KR(tmp_ret), K(read_info));
  }
  return ret;
}

int ObTenantFileManager::push_to_flush_queue(const MacroBlockId &file_id, const int64_t ls_epoch_id, const bool is_sealed)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = file_id.storage_object_type();
  if (OB_UNLIKELY(!file_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (ObStorageObjectType::TMP_FILE == object_type) {
    ObMemAttr attr(MTL_ID(), "FlushFile");
    ObSegmentFileInfo *seg_file_info = nullptr;
   if (OB_ISNULL(seg_file_info = OB_NEW(ObSegmentFileInfo, attr, file_id, GET_FUNC_TYPE(), is_sealed))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(file_id));
    } else if (OB_FAIL(flush_queue_.push(seg_file_info))) {
      LOG_WARN("fail to ", KR(ret), KPC(seg_file_info));
    }
    // free memory on fail
    if (OB_FAIL(ret)) {
      OB_DELETE(ObSegmentFileInfo, attr, seg_file_info);
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("object type not support seal", KR(ret), K(file_id), K(ls_epoch_id));
  }
  return ret;
}

int ObTenantFileManager::delete_file(const MacroBlockId &file_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObStorageObjectType object_type = file_id.storage_object_type();
    // step 1: delete remote file
    if (is_pin_storage_object_type(object_type)) {
      // do nothing, this object type only store in local cache, do not delete remote file
    } else if (OB_FAIL(delete_remote_file(file_id, ls_epoch_id))) {
      LOG_WARN("fail to delete remote file", KR(ret), K(file_id), K(ls_epoch_id));
    }
    // step 2: delete local cache file
    if (OB_FAIL(ret)) {
    } else if (is_object_type_only_store_remote(object_type)) {
      // do nothing, this object type only store in remote object storage, do not delete local cache file
    } else if (OB_FAIL(delete_local_file(file_id, ls_epoch_id))) {
      LOG_WARN("fail to delete local file", KR(ret), K(file_id), K(ls_epoch_id));
    }
  }
  return ret;
}

int ObTenantFileManager::delete_local_file(const MacroBlockId &file_id, const int64_t ls_epoch_id,
                                           const bool is_print_log, const bool is_del_seg_meta,
                                           const bool is_logical_delete)
{
  int ret = OB_SUCCESS;
  const ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if ((ObStorageObjectType::TMP_FILE != object_type) && is_pause_gc()) {
    // if it delete other file when calibrating disk space, pause gc.
    ret = OB_FILE_DELETE_FAILED;
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_WARN("the tenant is calibrating data disk alloc size and pausing gc, so do not allow to delete files", KR(ret), K_(tenant_id));
    }
  } else if ((ObStorageObjectType::TMP_FILE == object_type) && is_tmp_file_cache_pause_gc()) {
    // if it delete tmp file when calibrating disk space, rename the file to .deleted file.
    // After calibrating disk space, list all files and delete the .deleted file.
    if (OB_FAIL(logical_delete_local_file(file_id, ls_epoch_id))) {
      LOG_WARN("fail to logical delete local file", KR(ret), K(file_id), K(ls_epoch_id));
    }
  } else {
    ObIODFileStat statbuf;
    bool is_del_file_succ = true;
    ObPathContext ctx;
    if (is_logical_delete && OB_FAIL(ctx.set_logical_delete_ctx(file_id, ls_epoch_id))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (!is_logical_delete && OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_file*/))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (is_object_type_need_stat(object_type) &&
               OB_FAIL(ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf))) {
      // because OB_NO_SUCH_FILE_OR_DIRECTORY is ignored, should add one bool variable is_del_file_succ.
      // if need_erase_fd_cache == false, then no need to erase_fd_cache_when_del_file.
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
        is_del_file_succ = false;
      } else {
        LOG_WARN("fail to get file length", KR(ret), K(ctx));
      }
    } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(ctx.get_path()))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
        is_del_file_succ = false;
      } else {
        LOG_WARN("fail to del file", KR(ret), K(ctx));
      }
    } else if (FALSE_IT(is_del_file_succ = true)) {
    } else if (OB_FAIL(free_file_size(file_id, statbuf.size_))) {
      LOG_WARN("fail to free file size", KR(ret), K(file_id), K(statbuf.size_));
    } else if (is_print_log) {
      FLOG_INFO("succ to delete local file", K(file_id), K(ls_epoch_id), K_(tenant_id));
    }
    int tmp_ret = OB_SUCCESS;
    if (ObSSIOCommonOp::is_supported_fd_cache_obj_type(object_type) && is_del_file_succ &&
        OB_TMP_FAIL(fd_cache_mgr_.erase_fd_cache_when_del_file(file_id))) {
      LOG_WARN("fail to erase fd cache when del file", KR(tmp_ret), K(file_id));
    }
    // Note: segment_file_mgr_.delete_meta expect will not fail
    // when seg file write to object storage, need delete local old version seg file, cannot delete seg_meta
    if ((ObStorageObjectType::TMP_FILE == object_type) && is_del_file_succ && is_del_seg_meta &&
        OB_FAIL(segment_file_mgr_.delete_meta(TmpFileSegId(file_id.second_id(), file_id.third_id())))) {
      LOG_WARN("fail to delete segment file meta", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObTenantFileManager::delete_remote_file(const MacroBlockId &file_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  const ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObBackupIoAdapter adapter;
    ObBackupDest storage_dest;
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, false/*is_local_cache*/))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
    } else if (OB_FAIL(adapter.del_file(ctx.get_path(), storage_dest.get_storage_info()))) {
      LOG_WARN("fail to del file", KR(ret), K(ctx));
    } else {
      FLOG_INFO("succ to delete remote file", K(file_id), K(ls_epoch_id), K_(tenant_id));
      // Note: segment_file_mgr_.delete_meta expect will not fail
      if ((ObStorageObjectType::TMP_FILE == object_type) &&
          OB_FAIL(segment_file_mgr_.delete_meta(TmpFileSegId(file_id.second_id(), file_id.third_id())))) {
        LOG_WARN("fail to delete segment file meta", KR(ret), K(file_id));
      }
    }
  }
  return ret;
}

int ObTenantFileManager::delete_tmp_file(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = file_id.storage_object_type();
  const int64_t tmp_file_id = file_id.second_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || object_type != ObStorageObjectType::TMP_FILE || tmp_file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type), K(tmp_file_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    char tmp_file_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    // step 1: delete remote tmp_file dir's segment files
    if (OB_FAIL(OB_DIR_MGR.get_remote_tmp_file_dir(tmp_file_path, sizeof(tmp_file_path),
                                                      tenant_id, tenant_epoch_id, tmp_file_id))) {
      LOG_WARN("fail to get remote tmp file dir",KR(ret), K(tmp_file_path));
    } else if (OB_FAIL(delete_remote_dir(tmp_file_path))) {
      LOG_WARN("fail to delete remote dir", KR(ret), K(tmp_file_path));
    }
    // step 2: delete local tmp_file dir's segment files
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(delete_local_tmp_file(file_id))) {
      LOG_WARN("fail to delete local tmp file", KR(ret), K(file_id));
    } else {
      FLOG_INFO("succ to delete tmp file", K(file_id), K(tenant_id), K(tenant_epoch_id),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::is_delete_read_cache_tmp_file(const blocksstable::MacroBlockId &file_id, bool &is_need_delete)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id));
  } else if (OB_FAIL(preread_cache_mgr_.is_exist_in_lru(file_id, is_exist))) {
    LOG_WARN("fail to check file id exist in lru", KR(ret), K(file_id));
  } else if (is_exist) {
    // if exist in lru map, file_id is read cache, need delete
    is_need_delete = true;
  } else {
    // if not exist in lru map, file_id is write cache, cannot need delete
    is_need_delete = false;
  }
  return ret;
}

int ObTenantFileManager::delete_local_tmp_file(const MacroBlockId &file_id, const bool is_only_delete_read_cache)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = file_id.storage_object_type();
  int64_t tmp_file_id = file_id.second_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || object_type != ObStorageObjectType::TMP_FILE || tmp_file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type), K(tmp_file_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    char tmp_file_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    ObSingleNumFileListOp local_cache_file_op;
    if (OB_FAIL(OB_DIR_MGR.get_local_tmp_file_dir(tmp_file_path,
                       sizeof(tmp_file_path), tenant_id, tenant_epoch_id, tmp_file_id))) {
      LOG_WARN("fail to get tmp file dir", KR(ret), K(tmp_file_path));
    } else if (OB_FAIL(list_local_files(tmp_file_path, local_cache_file_op))) {
      LOG_WARN("fail to list local files", KR(ret), K(tmp_file_path));
    } else {
      ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> segment_file_ids;
      segment_file_ids.set_attr(ObMemAttr(tenant_id_, "DelFiles"));
      for (int64_t i = 0; OB_SUCC(ret) && i < local_cache_file_op.file_list_.count(); i++) {
        MacroBlockId segment_file_id;
        segment_file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        segment_file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        segment_file_id.set_second_id(tmp_file_id);
        segment_file_id.set_third_id(local_cache_file_op.file_list_.at(i));
        bool is_need_delete = true;
        // if only delete tmp_file read cache, need check if exist in lru map
        if (is_only_delete_read_cache) {
          if (OB_FAIL(is_delete_read_cache_tmp_file(file_id, is_need_delete))) {
            LOG_WARN("fail to check tmp file read cache need delete", KR(ret), K(file_id));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (is_need_delete && OB_FAIL(segment_file_ids.push_back(segment_file_id))) {
          LOG_WARN("fail to push back", KR(ret), K(segment_file_id));
        }
      }
      // delete local tmp_file dir's segment files
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(delete_local_files(segment_file_ids))) {
        LOG_WARN("fail to delete local files", KR(ret), K(segment_file_ids));
      }
    }
    // delete local tmp_file directory
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_DIR_MGR.delete_tmp_file_dir(tenant_id, tenant_epoch_id, tmp_file_id))) {
      LOG_WARN("fail to delete tmp file dir", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
    } else {
      FLOG_INFO("succ to delete local tmp file", K(file_id), K(tenant_id), K(tenant_epoch_id));
    }
  }
  return ret;
}

int ObTenantFileManager::delete_tmp_file(const MacroBlockId &file_id, const int64_t length)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = file_id.storage_object_type();
  const int64_t tmp_file_id = file_id.second_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || object_type != ObStorageObjectType::TMP_FILE || tmp_file_id < 0 || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type), K(tmp_file_id), K(length));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> segment_file_ids;
    segment_file_ids.set_attr(ObMemAttr(tenant_id_, "DelFiles"));
    // step 1: delete tmp_file dir's segment file
    const int64_t max_segment_id = std::ceil((double)length / OB_DEFAULT_MACRO_BLOCK_SIZE);
    const int64_t last_segment_id = MAX(0, max_segment_id - 1);
    for (int64_t i = 0; OB_SUCC(ret) && i < max_segment_id; i++) {
      MacroBlockId segment_file_id;
      segment_file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      segment_file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
      segment_file_id.set_second_id(tmp_file_id);
      segment_file_id.set_third_id(i);
      if (OB_FAIL(segment_file_ids.push_back(segment_file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(segment_file_id));
      }
    }
    TmpFileSegId last_seg_id(tmp_file_id, last_segment_id);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_delete_last_unsealed_segment(last_seg_id))) {
      LOG_WARN("fail to delete last unsealed seg meta", KR(ret), K(last_seg_id));
    } else if (OB_FAIL(delete_files(segment_file_ids))) {
      LOG_WARN("fail to delete files", KR(ret), K(segment_file_ids));
    }
    // step 2: delete local tmp_file directory
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_DIR_MGR.delete_tmp_file_dir(tenant_id_, MTL_EPOCH_ID(), tmp_file_id))) {
      LOG_WARN("fail to delete tmp file dir", KR(ret), K(tmp_file_id), K_(tenant_id));
    } else {
      FLOG_INFO("succ to delete tmp file", K(file_id), K(length), K_(tenant_id),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::delete_ls_dir(const int64_t ls_id,
                                       const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  char ls_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  int64_t size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (is_pause_gc()) {
    ret = OB_FILE_DELETE_FAILED;
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_WARN("the tenant is calibrating data disk alloc size and pausing gc, so do not allow to delete ls dir", KR(ret), K_(tenant_id));
    }
  } else if (OB_FAIL(OB_DIR_MGR.get_ls_id_dir(ls_dir_path, sizeof(ls_dir_path),
                     tenant_id_, MTL_EPOCH_ID(), ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls id dir", KR(ret), K(ls_id), K(ls_epoch_id), K_(tenant_id));
  } else if (OB_FAIL(ObDirManager::delete_dir_rec(ls_dir_path, size))) {
    LOG_WARN("fail to delete dir file", KR(ret), K(ls_dir_path), K(size));
  } else {
    FLOG_INFO("succ to delete ls dir", K(ls_id), K(ls_epoch_id), K(size), K_(tenant_id),
              "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
  }

  // free file size of deleted dirs and files, no matter OB_SUCC or OB_FAIL
  if (OB_LIKELY(size > 0)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(disk_space_mgr->free_file_size(size, ObStorageObjectType::PRIVATE_TABLET_META,
                                                   false/*is_tmp_file_read_cache*/))) { // expect won't fail
      LOG_WARN("fail to free file size", KR(tmp_ret), K(size));
    }
  }
  return ret;
}

int ObTenantFileManager::delete_files(const ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    if (OB_FAIL(delete_remote_files(block_ids))) {
      LOG_WARN("fail to delete remote files", KR(ret));
    } else if (OB_FAIL(delete_local_files(block_ids))) {
      LOG_WARN("fail to delete local files", KR(ret));
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("succ to delete files", "block_ids_cnt", block_ids.count(),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::delete_local_files(const ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> deleted_block_ids;
    deleted_block_ids.set_attr(ObMemAttr(tenant_id_, "DelFiles"));
    for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); i++) {
      const MacroBlockId file_id = block_ids.at(i);
      ObStorageObjectType object_type = file_id.storage_object_type();
      if (is_object_type_only_store_remote(object_type)) {
        // do nothing, this object type only store in remote object storage, do not delete local cache file
      } else if (OB_FAIL(delete_local_file(file_id, 0/*ls_epoch_id*/, false/*is_print_log*/))) {
        LOG_WARN("fail to delete local file", KR(ret),  K(file_id));
      } else if (OB_FAIL(deleted_block_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
    if (OB_SUCC(ret)) {
      batch_print_deleted_file_ids(deleted_block_ids, true/*is_local*/);
      FLOG_INFO("succ to delete local files", "block_ids_cnt", block_ids.count(), "deleted_ids_cnt", deleted_block_ids.count(),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::delete_remote_files(const ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObMemAttr attr(tenant_id_, "DelFiles");
    ObSEArray<ObString, OB_DEFAULT_ARRAY_CAPACITY> file_path_array;
    ObSEArray<int64_t, OB_DEFAULT_ARRAY_CAPACITY> failed_file_idx;
    ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> deleted_block_ids;
    file_path_array.set_attr(attr);
    failed_file_idx.set_attr(attr);
    deleted_block_ids.set_attr(attr);
    ObBackupIoAdapter adapter;
    ObBackupDest storage_dest;
    ObArenaAllocator allocator(attr);
    if (OB_FAIL(get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); i++) {
      const MacroBlockId file_id = block_ids.at(i);
      ObStorageObjectType object_type = file_id.storage_object_type();
      char *file_path = nullptr;
      ObPathContext ctx;
      if (is_pin_storage_object_type(object_type)) {
        // do nothing, this object type only store in local cache, do not delete remote file
      } else if (OB_ISNULL(file_path = static_cast<char *>(allocator.alloc(OB_MAX_FILE_PATH_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret));
      } else if (OB_FAIL(ctx.set_file_ctx(file_id, 0/*ls_epoch_id*/, false/*is_local_cache*/))) {
        LOG_WARN("fail to convert file path", KR(ret), K(ctx));
      } else if (FALSE_IT(MEMCPY(file_path, ctx.get_path(), OB_MAX_FILE_PATH_LENGTH))) {
        LOG_WARN("fail to convert file path", KR(ret), K(ctx));
      } else if (OB_FAIL(file_path_array.push_back(ObString(file_path)))) {
        LOG_WARN("fail to push back", KR(ret), K(file_path));
      } else if (OB_FAIL(deleted_block_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      } else if ((file_path_array.count() >= BATCH_DELETE_FILE_COUNT) || (i + 1 == block_ids.count())) {
        if (OB_FAIL(adapter.batch_del_files(storage_dest.get_storage_info(), file_path_array, failed_file_idx))) {
          LOG_WARN("fail to batch del files", KR(ret), K(failed_file_idx));
        } else {
          // not all files deleted success return OB_FILE_DELETE_FAILED
          if (!failed_file_idx.empty()) {
            ret = OB_FILE_DELETE_FAILED;
            LOG_WARN("fail to delete remote files", KR(ret), "failed_file_count", failed_file_idx.count());
          }
          file_path_array.reset();
          failed_file_idx.reset();
          allocator.reuse();
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_block_ids.count(); i++) {
      const MacroBlockId &file_id = deleted_block_ids.at(i);
      // Note: segment_file_mgr_.delete_meta expect will not fail
      if ((ObStorageObjectType::TMP_FILE == file_id.storage_object_type()) &&
          OB_FAIL(segment_file_mgr_.delete_meta(TmpFileSegId(file_id.second_id(), file_id.third_id())))) {
        LOG_WARN("fail to delete segment file meta", KR(ret), K(file_id));
      }
    }
    if (OB_SUCC(ret)) {
      batch_print_deleted_file_ids(deleted_block_ids, false/*is_local*/);
      FLOG_INFO("succ to delete remote files", "block_ids_cnt", block_ids.count(), "deleted_ids_cnt", deleted_block_ids.count(),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

void ObTenantFileManager::batch_print_deleted_file_ids(const ObIArray<MacroBlockId> &block_ids,
                                                       const bool is_local)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, BATCH_PRINT_FILE_PATH_COUNT> batch_file_id_array;
  batch_file_id_array.set_max_print_count(BATCH_PRINT_FILE_PATH_COUNT);
  for (int64_t i = 0; i < block_ids.count(); i++) {
    ret = OB_SUCCESS; // ignore error code
    if (OB_FAIL(batch_file_id_array.push_back(block_ids.at(i)))) {
      LOG_WARN("fail to push back", KR(ret), K(block_ids.at(i)));
    } else if ((batch_file_id_array.count() >= BATCH_PRINT_FILE_PATH_COUNT) ||
               (i + 1 == block_ids.count())) {
      FLOG_INFO("succ to delete batch files", K(is_local), "array_cnt", batch_file_id_array.count(), K(batch_file_id_array));
      batch_file_id_array.reuse();
    }
  }
  // because array maybe fail to push back, lead to array's elements to not be printed
  if (batch_file_id_array.count() > 0) {
    FLOG_INFO("succ to delete batch files", K(is_local), "array_cnt", batch_file_id_array.count(), K(batch_file_id_array));
  }
}

int ObTenantFileManager::delete_shared_tablet_data_dir(const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  char tablet_data_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(tablet_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_data_dir(tablet_data_dir_path,
                     sizeof(tablet_data_dir_path), tablet_id))) {
    LOG_WARN("fail to get shared tablet data dir", KR(ret), K(tablet_data_dir_path), K(tablet_id));
  } else if (OB_FAIL(delete_remote_dir(tablet_data_dir_path))) {
    LOG_WARN("fail to delete remote dir", KR(ret), K(tablet_data_dir_path));
  } else {
    FLOG_INFO("succ to delete shared tablet data dir", K(tablet_id));
  }
  return ret;
}

int ObTenantFileManager::delete_local_major_data_dir(const int64_t delete_time_stamp_s)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(delete_time_stamp_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(delete_time_stamp_s));
  } else if (is_pause_gc()) {
    ret = OB_FILE_DELETE_FAILED;
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_WARN("the tenant is calibrating data disk alloc size and pausing gc, so do not allow to delete files", KR(ret), K_(tenant_id));
    }
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    ObMajorDataDeleteOp delete_op;
    char major_data_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_local_major_data_dir(major_data_path, sizeof(major_data_path), tenant_id, tenant_epoch_id))) {
      LOG_WARN("fail to get major data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
    } else if (OB_FAIL(delete_op.set_dir(major_data_path))) {
      LOG_WARN("fail to set dir", KR(ret), K(major_data_path));
    } else if (OB_FAIL(delete_op.set_time_stamp(delete_time_stamp_s))) {
      LOG_WARN("fail to set time stamp", KR(ret), K_(start_server_time_s));
    } else if (OB_FAIL(list_local_files(major_data_path, delete_op))) {
      LOG_WARN("fail to list local files", KR(ret), K(major_data_path));
    } else {
      LOG_INFO("succ to delete major data dir", KR(ret), K(delete_op), K(tenant_id), K(tenant_epoch_id),
               "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::list_tmp_file(ObIArray<MacroBlockId> &tmp_file_ids)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> tmp_file_id_set;
  lib::ObMemAttr attr(tenant_id_, "ListTmpFile");
  char tmp_data_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObSingleNumFileListOp tmp_file_dir_op;
  tmp_file_dir_op.set_dir_flag();
  // step 1: list tmp file in local cache
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(tmp_file_id_set.create(OB_DEFAULT_BUCKET_NUM, attr))){
    LOG_WARN("create block id set failed", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tmp_data_dir(tmp_data_path,
                     sizeof(tmp_data_path), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get tmp data dir", KR(ret), K(tmp_data_path), K_(tenant_id));
  } else if (OB_FAIL(list_local_files(tmp_data_path, tmp_file_dir_op))) {
    LOG_WARN("fail to list local files", KR(ret), K(tmp_data_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_file_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tmp_file_id_set.set_refactored(tmp_file_dir_op.file_list_.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(tmp_file_dir_op.file_list_.at(i)));
      }
    }
  }
  ObSingleNumFileListOp remote_tmp_data_dir_op;
  remote_tmp_data_dir_op.set_dir_flag();
  tmp_data_path[0] = '\0';
  // step 2: list tmp file in remote object storage
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tmp_data_dir(tmp_data_path,
                     sizeof(tmp_data_path), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get shared tablet meta dir", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(list_remote_files(tmp_data_path, remote_tmp_data_dir_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(tmp_data_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_tmp_data_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tmp_file_id_set.set_refactored(remote_tmp_data_dir_op.file_list_.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(remote_tmp_data_dir_op.file_list_.at(i)));
      }
    }
  }
  hash::ObHashSet<int64_t>::const_iterator iter;
  for (iter = tmp_file_id_set.begin(); OB_SUCC(ret) && iter != tmp_file_id_set.end(); ++iter) {
    MacroBlockId file_id;
    file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    file_id.set_second_id(iter->first);
    if (OB_FAIL(tmp_file_ids.push_back(file_id))) {
      LOG_WARN("fail to push back", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObTenantFileManager::list_tablet_data_dir(ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  // TODO @xiaotao.ht list all tablets, it occupies a lot of memory, implement streaming interface later
  hash::ObHashSet<int64_t> tablet_id_set;
  lib::ObMemAttr attr(tenant_id_, "ListTabletDa");
  char tablet_data_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObSingleNumFileListOp tablet_data_dir_op;
  tablet_data_dir_op.set_dir_flag();
  // step 1: list tablet id in local dir
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(tablet_id_set.create(OB_DEFAULT_BUCKET_NUM, attr))){  // When there are millions of tablets, it may take a long time
    LOG_WARN("create block id set failed", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tablet_data_dir(tablet_data_dir_path,
                     sizeof(tablet_data_dir_path), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get tenant dir", KR(ret), K(tablet_data_dir_path), K_(tenant_id));
  } else if (OB_FAIL(list_local_files(tablet_data_dir_path, tablet_data_dir_op))) {
    LOG_WARN("fail to list local files", KR(ret), K(tablet_data_dir_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_data_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tablet_id_set.set_refactored(tablet_data_dir_op.file_list_.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(tablet_data_dir_op.file_list_.at(i)));
      }
    }
  }
  ObSingleNumFileListOp remote_tablet_data_dir_op;
  remote_tablet_data_dir_op.set_dir_flag();
  tablet_data_dir_path[0] = '\0';
  // step 2: list tablet id in remote dir
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tablet_data_dir(tablet_data_dir_path,
                     sizeof(tablet_data_dir_path), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get shared tablet meta dir", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(list_remote_files(tablet_data_dir_path, remote_tablet_data_dir_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(tablet_data_dir_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_tablet_data_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tablet_id_set.set_refactored(remote_tablet_data_dir_op.file_list_.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(remote_tablet_data_dir_op.file_list_.at(i)));
      }
    }
  }
  hash::ObHashSet<int64_t>::const_iterator iter;
  for (iter = tablet_id_set.begin(); OB_SUCC(ret) && iter != tablet_id_set.end(); ++iter) {
    if (OB_FAIL(tablet_ids.push_back(iter->first))) {
      LOG_WARN("fail to push back", KR(ret), K(iter->first));
    }
  }
  return ret;
}

int ObTenantFileManager::list_shared_tablet_ids(ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSingleNumFileListOp shared_tablet_id_op;
  char shared_tablet_ids_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_ids_dir(shared_tablet_ids_path, sizeof(shared_tablet_ids_path)))) {
    LOG_WARN("fail to get shared tablet ids dir", KR(ret));
  } else if (OB_FAIL(list_remote_files(shared_tablet_ids_path, shared_tablet_id_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(shared_tablet_ids_path));
  } else if (OB_FAIL(shared_tablet_id_op.get_file_list(tablet_ids))) {
    LOG_WARN("fail to get file list", KR(ret));
  }
  return ret;
}

int ObTenantFileManager::list_tablet_meta_dir(const int64_t ls_id,
                                              const int64_t ls_epoch_id,
                                              ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  char ls_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObSingleNumFileListOp tablet_meta_dir_op;
  tablet_meta_dir_op.set_dir_flag();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_tablet_meta_dir(ls_dir_path, sizeof(ls_dir_path),
                     tenant_id_, MTL_EPOCH_ID(), ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls dir", KR(ret), K(ls_id), K(ls_epoch_id), K_(tenant_id));
  } else if (OB_FAIL(list_local_files(ls_dir_path, tablet_meta_dir_op))) {
    LOG_WARN("fail to list local files", KR(ret), K(ls_dir_path));
  } else if (OB_FAIL(tablet_meta_dir_op.get_file_list(tablet_ids))) {
    LOG_WARN("fail to get file list", KR(ret));
  }
  return ret;
}

int ObTenantFileManager::list_private_macro_file(const int64_t tablet_id,
                                                 const int64_t transfer_seq,
                                                 ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<MacroBlockId> block_id_set;
  lib::ObMemAttr attr(tenant_id_, "ListPriMacro");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(tablet_id <= 0 || transfer_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(transfer_seq));
  } else if (OB_FAIL(block_id_set.create(OB_DEFAULT_BUCKET_NUM, attr))){
    LOG_WARN("create block id set failed", KR(ret));
  }
  ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> listed_block_ids;
  listed_block_ids.set_attr(ObMemAttr(tenant_id_, "ListPriMacro"));
  // step 1: list private data macro file in local dir
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(list_local_private_macro_file(tablet_id, transfer_seq, ObMacroType::DATA_MACRO, listed_block_ids))) {
    LOG_WARN("fail to list local private data macro dir", KR(ret), K(tablet_id), K(transfer_seq));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < listed_block_ids.count(); i++) {
      if (OB_FAIL(block_id_set.set_refactored(listed_block_ids.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(listed_block_ids.at(i)));
      }
    }
  }
  // step 2: list private meta macro file in local dir
  listed_block_ids.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(list_local_private_macro_file(tablet_id, transfer_seq, ObMacroType::META_MACRO, listed_block_ids))) {
    LOG_WARN("fail to list local private meta macro dir", KR(ret), K(tablet_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < listed_block_ids.count(); i++) {
      if (OB_FAIL(block_id_set.set_refactored(listed_block_ids.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(listed_block_ids.at(i)));
      }
    }
  }
  // step 3: list private data macro file in remote dir
  listed_block_ids.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(list_remote_private_macro_file(tablet_id, transfer_seq, ObMacroType::DATA_MACRO, listed_block_ids))) {
    LOG_WARN("fail to list remote private data macro dir", KR(ret), K(tablet_id), K(transfer_seq));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < listed_block_ids.count(); i++) {
      if (OB_FAIL(block_id_set.set_refactored(listed_block_ids.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(listed_block_ids.at(i)));
      }
    }
  }
  // step 4: list private meta macro file in remote dir
  listed_block_ids.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(list_remote_private_macro_file(tablet_id, transfer_seq, ObMacroType::META_MACRO, listed_block_ids))) {
    LOG_WARN("fail to list remote private meta macro dir", KR(ret), K(tablet_id), K(transfer_seq));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < listed_block_ids.count(); i++) {
      if (OB_FAIL(block_id_set.set_refactored(listed_block_ids.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(listed_block_ids.at(i)));
      }
    }
  }
  hash::ObHashSet<MacroBlockId>::const_iterator iter;
  for (iter = block_id_set.begin(); OB_SUCC(ret) && iter != block_id_set.end(); ++iter) {;
    if (OB_FAIL(block_ids.push_back(iter->first))) {
      LOG_WARN("fail to push back", KR(ret), K(iter->first));
    }
  }
  return ret;
}

int ObTenantFileManager::list_local_private_macro_file(const int64_t tablet_id, const int64_t transfer_seq,
                                                       const ObMacroType macro_type,
                                                       ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  char private_macro_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObDoubleNumFileListOp local_private_macro_op;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(tablet_id <= 0 || transfer_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(transfer_seq));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tablet_id_macro_dir(private_macro_path,
                     sizeof(private_macro_path), tenant_id_, MTL_EPOCH_ID(), tablet_id, transfer_seq, macro_type))) {
    LOG_WARN("fail to get tablet macro dir", KR(ret), K_(tenant_id), K(tablet_id), K(transfer_seq), K(macro_type));
  } else if (OB_FAIL(list_local_files(private_macro_path, local_private_macro_op))) {
    LOG_WARN("fail to list local files", KR(ret), K(private_macro_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < local_private_macro_op.file_list_.count(); i++) {
      MacroBlockId file_id;
      file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      if (ObMacroType::DATA_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
      } else if (ObMacroType::META_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_META_MACRO);
      }
      file_id.set_second_id(tablet_id);
      file_id.set_third_id(local_private_macro_op.file_list_.at(i).first);
      file_id.set_macro_transfer_seq(transfer_seq);
      file_id.set_tenant_seq(local_private_macro_op.file_list_.at(i).second);
      if (OB_FAIL(block_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
  }
  return ret;
}

int ObTenantFileManager::list_remote_private_macro_file(const int64_t tablet_id, const int64_t transfer_seq,
                                                        const ObMacroType macro_type, ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObDoubleNumFileListOp remote_private_macro_op;
  char private_macro_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(tablet_id <= 0 || transfer_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(transfer_seq));
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tablet_id_macro_dir(
             private_macro_path, sizeof(private_macro_path), tenant_id_, MTL_EPOCH_ID(), tablet_id, transfer_seq, macro_type))) {
    LOG_WARN("fail to get remote tablet macro dir", KR(ret), K(tablet_id), K(transfer_seq), K_(tenant_id), K(macro_type));
  } else if (OB_FAIL(list_remote_files(private_macro_path, remote_private_macro_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(private_macro_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_private_macro_op.file_list_.count(); i++) {
      MacroBlockId file_id;
      file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      if (ObMacroType::DATA_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
      } else if (ObMacroType::META_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_META_MACRO);
      }
      file_id.set_second_id(tablet_id);
      file_id.set_third_id(remote_private_macro_op.file_list_.at(i).first);
      file_id.set_macro_transfer_seq(transfer_seq);
      file_id.set_tenant_seq(remote_private_macro_op.file_list_.at(i).second);
      if (OB_FAIL(block_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
  }
  return ret;
}

int ObTenantFileManager::list_local_files(const char *dir_path, ObBaseDirEntryOperator &list_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(dir_path, list_op))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to list dirs", KR(ret), K(dir_path));
    }
  }
  return ret;
}

int ObTenantFileManager::list_local_files_rec(const char *dir_path,
                                              ObScanDirOp &list_file_op,
                                              ObScanDirOp &list_dir_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir_rec(dir_path, list_file_op, list_dir_op))) {
    LOG_WARN("fail to list dirs", KR(ret), K(dir_path));
  }
  return ret;
}

int ObTenantFileManager::list_remote_files(const char *dir_path, ObBaseDirEntryOperator &list_op)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  ObBackupDest storage_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(get_storage_dest(storage_dest))) {
    LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
  } else if (OB_FAIL(adapter.list_files(dir_path, storage_dest.get_storage_info(), list_op))) {
    LOG_WARN("fail to list files", KR(ret), K(dir_path));
  }
  return ret;
}

int ObTenantFileManager::get_micro_cache_file_path(char *path, const int64_t length,
                                                   const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(nullptr == path || length <= 0 || length > OB_MAX_FILE_PATH_LENGTH ||
                         !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
             OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, MICRO_CACHE_FILE_NAME))) {
    LOG_WARN("fail to databuff printf", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObTenantFileManager::alloc_file_size(const ObStorageObjectType object_type, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (!is_object_type_need_stat(object_type)) {
    // do nothing, current object_type do not need statistics file size
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K_(tenant_id));
  } else if (ObStorageObjectType::TMP_FILE == object_type) {
    if (OB_FAIL(try_alloc_tmp_file_write_cache_size(size))) {
      LOG_WARN("fail to alloc tmp file size", KR(ret), K(size));
    }
  } else if (OB_FAIL(tnt_disk_space_mgr->alloc_file_size(size, object_type, false/*is_tmp_file_read_cache*/))) {
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_WARN("fail to alloc file size", KR(ret), K(size), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
    }
  }
  return ret;
}

int ObTenantFileManager::try_alloc_tmp_file_write_cache_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  const bool is_tmp_file_read_cache = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(size));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_disk_space_mgr is null", KR(ret), KP(tnt_disk_space_mgr));
  } else if (OB_FAIL(tnt_disk_space_mgr->alloc_file_size(size, ObStorageObjectType::TMP_FILE, is_tmp_file_read_cache))) {
    // try evict some files to free tmp_file_read_cache space, make tmp_file_write_cache can alloc size
    int64_t retry_cnt = 0;
    while ((OB_SERVER_OUTOF_DISK_SPACE == ret) && (retry_cnt < MAX_RETRY_ALLOC_CACHE_SPACE_CNT)) {
      ret = OB_SUCCESS;
      if (OB_FAIL(preread_cache_mgr_.evict_tail_lru_node())) {
        LOG_WARN("fail to evict tail lru node", KR(ret));
      } else if (OB_FAIL(tnt_disk_space_mgr->alloc_file_size(size, ObStorageObjectType::TMP_FILE, is_tmp_file_read_cache))) {
        if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
          LOG_WARN("fail to alloc tmp file write cache size", KR(ret), K(size));
        }
      }
      ++retry_cnt;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to alloc tmp_file write cache size", KR(ret), K(size), K(retry_cnt));
    }
  }
  return ret;
}

int ObTenantFileManager::try_delete_last_unsealed_segment(const TmpFileSegId &seg_id)
{
  int ret = OB_SUCCESS;
  bool is_meta_exist = false;
  TmpFileMetaHandle meta_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(segment_file_mgr_.try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to get seg meta", KR(ret), K(seg_id));
  } else if (!is_meta_exist) {
    // do nothing
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, meta handle is invalid", KR(ret), K(meta_handle));
  } else if (!meta_handle.is_in_local()) {
    // the last segment file is unsealed and it is in remote, so need delete unsealed segment file
    if (OB_FAIL(segment_file_mgr_.push_seg_file_to_remove_queue(seg_id, meta_handle.get_valid_length()))) {
      LOG_WARN("fail to push seg file to remove queue", KR(ret), K(seg_id), K(meta_handle));
    }
  }
  return ret;
}

int ObTenantFileManager::free_file_size(const MacroBlockId &file_id, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  const ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (!is_object_type_need_stat(object_type)) {
    // do nothing, current object_type do not need statistics file size
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr));
  } else if (ObStorageObjectType::TMP_FILE == object_type) {
    // 1.if file_id exists in lru map, free read cache disk size;
    // 2.if file_id does not exist in lru map, free write cache disk size.
    bool is_exist = false;
    if (OB_FAIL(preread_cache_mgr_.is_exist_in_lru(file_id, is_exist))) {
      LOG_WARN("fail to judge is exist in lru", KR(ret), K(file_id));
    } else if (is_exist) {
      // 1.delete local tmp_file 2.free tmp_file size 3.remove lru node
      if (OB_FAIL(tnt_disk_space_mgr->free_file_size(size, object_type, true/*is_tmp_file_read_cache*/))) {
        LOG_WARN("fail to free tmp file read cache size", KR(ret), K(size), K(object_type), "object_type_str", 
          get_storage_objet_type_str(object_type));
      } else if (OB_FAIL(preread_cache_mgr_.remove_lru_node(file_id))) {
        LOG_WARN("fail to remove lru node", KR(ret), K(file_id));
      }
    } else {
      if (OB_FAIL(tnt_disk_space_mgr->free_file_size(size, object_type, false/*is_tmp_file_read_cache*/))) {
        LOG_WARN("fail to free tmp file write cache size", KR(ret), K(size), K(object_type), "object_type_str",
          get_storage_objet_type_str(object_type));
      }
    }
  } else if (OB_FAIL(tnt_disk_space_mgr->free_file_size(size, object_type, false/*is_tmp_file_read_cache*/))) {
    LOG_WARN("fail to free file size", KR(ret), K(size), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
  } else if (tnt_disk_space_mgr->is_major_macro_objtype(object_type)) {
    // 1.delete local major_macro 2.free major_macro size 3.remove lru node
    if (OB_FAIL(preread_cache_mgr_.remove_lru_node(file_id))) {
      LOG_WARN("fail to remove lru node", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObTenantFileManager::calibrate_disk_space()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(calibrate_disk_space_task_.schedule_calibrate_disk_space())) {
    LOG_WARN("fail to schedule calibrate disk space", KR(ret));
  }
  return ret;
}

int ObTenantFileManager::aio_write_with_create_parent_dir(
    const ObStorageObjectWriteInfo &write_info,
    const ObStorageObjectHandle &ori_object_handle,
    const bool need_free_file_size,
    const int64_t free_size,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  MacroBlockId file_id = object_handle.get_macro_id();
  ObStorageObjectType object_type = file_id.storage_object_type();
  if (OB_UNLIKELY(need_free_file_size && (free_size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(need_free_file_size), K(free_size));
  } else if (OB_FAIL(do_aio_write(write_info, object_handle))) {
    // No such file or directory, need create file parent dir
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      object_handle = ori_object_handle;
      if (OB_FAIL(ObFileHelper::create_file_parent_dir(file_id, write_info.ls_epoch_id_))) {
        LOG_WARN("fail to create file parent dir", KR(ret), "macro_id", file_id,
                "ls_epoch_id", write_info.ls_epoch_id_);
      } else if (OB_FAIL(do_aio_write(write_info, object_handle))) {
        LOG_WARN("fail to do aio write", KR(ret), K(write_info), K(object_handle));
      }
    }
    // fail to open file, need recovery disk size
    if (need_free_file_size && OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      // if current object type only store remote, do not need free file size
      if (!is_object_type_only_store_remote(object_type) &&
          OB_TMP_FAIL(free_file_size(file_id, free_size))) {
        LOG_WARN("fail to free file size", KR(tmp_ret), K(file_id), "object_type_str",
                 get_storage_objet_type_str(object_type), K(free_size));
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to do aio write", KR(ret), K(object_handle), K(write_info));
    }
  }
  return ret;
}

int ObTenantFileManager::try_fsync_private_macro_parent_dir(
    const ObStorageObjectType object_type,
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (ObStorageObjectType::PRIVATE_TABLET_META == object_type) {
    if (OB_FAIL(fsync_private_tablet_dir(ObStorageObjectType::PRIVATE_DATA_MACRO, file_id.third_id()))) {
      LOG_WARN("fail to fsync priate tablet dir", KR(ret), K(file_id));
    } else if (OB_FAIL(fsync_private_tablet_dir(ObStorageObjectType::PRIVATE_META_MACRO, file_id.third_id()))) {
      LOG_WARN("fail to fsync priate tablet dir", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObTenantFileManager::fsync_private_tablet_dir(const ObStorageObjectType object_type, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(((ObStorageObjectType::PRIVATE_DATA_MACRO != object_type) &&
                   (ObStorageObjectType::PRIVATE_META_MACRO != object_type)) || (tablet_id <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(object_type), "object_type_str", get_storage_objet_type_str(object_type), K(tablet_id));
  } else {
    MacroBlockId mock_private_macro_id;
    mock_private_macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    mock_private_macro_id.set_storage_object_type(static_cast<uint64_t>(object_type));
    mock_private_macro_id.set_second_id(tablet_id);
    char private_macro_dir[OB_MAX_FILE_PATH_LENGTH];
    bool is_private_macro_dir_exist = false;
    if (OB_FAIL(ObFileHelper::get_file_parent_dir(private_macro_dir, OB_MAX_FILE_PATH_LENGTH, mock_private_macro_id, 0/*ls_epoch_id*/))) {
      LOG_WARN("fail to get private macro parent dir", KR(ret), K(private_macro_dir));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::exist(private_macro_dir, is_private_macro_dir_exist))) {
      LOG_WARN("fail to exist", KR(ret), K(private_macro_dir));
    } else if (is_private_macro_dir_exist && OB_FAIL(ObDirManager::fsync_dir(private_macro_dir))) {
      LOG_WARN("fail to fsync private macro parent dir", KR(ret), K(private_macro_dir));
    }
  }
  return ret;
}

// logical delete local_file is renamed file to .deleted file
int ObTenantFileManager::logical_delete_local_file(const MacroBlockId &file_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  ObPathContext old_path_ctx;
  ObPathContext new_path_ctx;
  if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_FAIL(old_path_ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_cache*/))) {
    LOG_WARN("fail to convert file path", KR(ret), K(old_path_ctx));
  } else if (OB_FAIL(new_path_ctx.set_logical_delete_ctx(file_id, ls_epoch_id))) {
    LOG_WARN("fail to convert file path", KR(ret), K(new_path_ctx));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rename(old_path_ctx.get_path(), new_path_ctx.get_path()))) {
    // if report OB_NO_SUCH_FILE_OR_DIRECTORY, maybe the file has been renamed
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to rename", KR(ret), K(old_path_ctx), K(new_path_ctx));
    }
  }
  return ret;
}

bool ObTenantFileManager::is_object_type_need_stat(const ObStorageObjectType object_type)
{
  // because preread major_macro, so need stat file size when delete local major_macro
  // because statistics meta_file_size for private_tablet_meta and tablet_current_version, so need stat file_size
  bool is_need_stat = ((ObStorageObjectType::PRIVATE_DATA_MACRO == object_type) ||
                       (ObStorageObjectType::PRIVATE_META_MACRO == object_type) ||
                       (ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == object_type) ||
                       (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type) ||
                       (ObStorageObjectType::TMP_FILE == object_type) ||
                       (ObStorageObjectType::PRIVATE_TABLET_META == object_type) ||
                       (ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION == object_type));
  return is_need_stat;
}

/**
 * --------------------------------ObServerFileManager------------------------------------
 */
ObServerFileManager::ObServerFileManager()
  : ObBaseFileManager(),
    resize_device_size_lock_(ObLatchIds::FILE_MANAGER_LOCK),
    io_calibration_fd_(OB_INVALID_FD)
{
}

ObServerFileManager::~ObServerFileManager()
{
  destroy();
}

ObServerFileManager &ObServerFileManager::get_instance()
{
  static ObServerFileManager instance_;
  return instance_;
}

int ObServerFileManager::start(const int64_t reserved_size)
{
  int ret = OB_SUCCESS;
  int64_t total_disk_size = 0;
  ObIODOpts io_d_opts;
  ObIODOpt opt;
  io_d_opts.opt_cnt_ = 1;
  io_d_opts.opts_ = &(opt);
  opt.set("reserved size", reserved_size);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.start(io_d_opts))) {
    LOG_WARN("start local cache device fail", KR(ret));
  } else if (FALSE_IT(total_disk_size = LOCAL_DEVICE_INSTANCE.get_total_block_size())) {
  } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.init(total_disk_size))) {
    LOG_WARN("fail to init server disk space manager", KR(ret), K(total_disk_size));
  } else if (OB_FAIL(check_data_disk_avail_task_.init(lib::TGDefIDs::ServerGTimer))) {
    LOG_WARN("fail to init check data disk available task", KR(ret));
  }
  if (OB_SUCC(ret)) {
    is_started_ = true;
    LOG_INFO("start server file manager", K_(is_started), K(total_disk_size));
  }
  return ret;
}

void ObServerFileManager::destroy()
{
  int ret = OB_SUCCESS;
  if (io_calibration_fd_ != OB_INVALID_FD) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObSSFileUtil::close(io_calibration_fd_))) {
      LOG_ERROR("fail to close fd", KR(tmp_ret), K_(io_calibration_fd));
    }
    io_calibration_fd_ = OB_INVALID_FD;
  }
  ObBaseFileManager::destroy();
}

int ObServerFileManager::resize_device_size(const int64_t new_device_size,
                                            const int64_t new_device_disk_percentage,
                                            const int64_t reserved_size,
                                            storage::ObServerSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(resize_device_size_lock_);
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(reserved_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(reserved_size));
  } else {
    const int64_t old_data_file_size = LOCAL_DEVICE_INSTANCE.get_total_block_size();
    ObIODOpts io_d_opts;
    ObIODOpt opts[3];
    opts[0].set("datafile_size", new_device_size);
    opts[1].set("datafile_disk_percentage", new_device_disk_percentage);
    opts[2].set("reserved_size", reserved_size);
    io_d_opts.opts_ = opts;
    io_d_opts.opt_cnt_ = 3;
    if (OB_FAIL(LOCAL_DEVICE_INSTANCE.reconfig(io_d_opts))) {
      LOG_WARN("fail to resize file", KR(ret), K(new_device_size));
    } else {
      const int64_t new_actual_file_size = LOCAL_DEVICE_INSTANCE.get_total_block_size();
      const int64_t new_macro_block_cnt = new_actual_file_size / OB_STORAGE_OBJECT_MGR.get_macro_block_size();
      if (old_data_file_size < new_actual_file_size) {
        super_block.body_.total_file_size_ = new_actual_file_size;
        super_block.body_.total_macro_block_count_ = new_macro_block_cnt;
        super_block.body_.modify_timestamp_ = ObTimeUtility::current_time();
        if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.resize(new_actual_file_size))) {
          LOG_WARN("fail to resize total disk size", KR(ret));
        } else {
          FLOG_INFO("succeed to resize data file", K(new_actual_file_size),
                    K(new_device_size), K(new_device_disk_percentage));
        }
      } else if (old_data_file_size == new_actual_file_size) {
        LOG_INFO("the data file size is not changed", K(old_data_file_size), K(new_actual_file_size));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("new data file size is less than old data file size, not supported", KR(ret),
                  K(new_actual_file_size), K(old_data_file_size));
      }

      // super block may have format upgrade. Whatever body changed, reconstruct header for safe
      if (FAILEDx(super_block.construct_header())) {
        LOG_WARN("fail to construct header", K(ret));
      }
    }
  }
  return ret;
}

int ObServerFileManager::async_append_file(const ObStorageObjectWriteInfo &write_info,
                                           ObStorageObjectHandle &object_handle)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("server file manager does not support append file", KR(ret), K(write_info), K(object_handle));
  return ret;
}

int ObServerFileManager::async_write_file(const ObStorageObjectWriteInfo &write_info,
                                          ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle ori_object_handle(object_handle);  // when aio write failed, object_handle can reset
  ObStorageObjectWriteInfo new_write_info(write_info);
  ObStorageObjectType object_type = object_handle.get_macro_id().storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!new_write_info.is_valid() || !object_handle.get_macro_id().is_valid())
                         || new_write_info.io_desc_.is_write_through()) {
    // disable user layer to set write_through/back, only io layer can determine write_through/back
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_write_info), K(object_handle));
  } else {
    // defence for 500 tenant write
    if (((ObStorageObjectType::SERVER_META != object_type)
         && (ObStorageObjectType::TENANT_SUPER_BLOCK != object_type)
         && (ObStorageObjectType::IS_SHARED_TENANT_DELETED != object_type)
         && (ObStorageObjectType::TENANT_UNIT_META != object_type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected server tenant io", KR(ret), K(object_type), "object_type_str",
                get_storage_objet_type_str(object_type), K(write_info), K(object_handle));
    } else if (OB_FAIL(aio_write_with_create_parent_dir(new_write_info, ori_object_handle, object_handle))) {
      LOG_WARN("fail to aio write with create parent dir", KR(ret), K(new_write_info), K(object_handle));
    }
  }
  return ret;
}

int ObServerFileManager::delete_local_tenant_dir(const uint64_t tenant_id,
                                                 const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  char tenant_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(tenant_dir_path, sizeof(tenant_dir_path),
                                                        tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(delete_local_dir(tenant_dir_path))) {
    LOG_WARN("fail to delete dir file", KR(ret), K(tenant_dir_path));
  } else {
    FLOG_INFO("succ to delete local tenant dir", K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

int ObServerFileManager::delete_remote_tenant_dir(const uint64_t tenant_id,
                                                  const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  char tenant_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tenant_dir(tenant_dir_path,
                     sizeof(tenant_dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get remote tenant dir", KR(ret), K(tenant_dir_path),
             K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(delete_remote_dir(tenant_dir_path))) {
    LOG_WARN("fail to delete remote dir", KR(ret), K(tenant_dir_path));
  } else {
    FLOG_INFO("succ to delete remote tenant dir", K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

int ObServerFileManager::delete_shared_tenant_dir(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tenant_dir(tenant_dir_path,
                     sizeof(tenant_dir_path), tenant_id))) {
    LOG_WARN("fail to get shared tenant dir", KR(ret), K(tenant_dir_path), K(tenant_id));
  } else if (OB_FAIL(delete_remote_dir(tenant_dir_path))) {
    LOG_WARN("fail to delete remote dir", KR(ret), K(tenant_dir_path));
  } else {
    FLOG_INFO("succ to delete shared tenant dir", K(tenant_id));
  }
  return ret;
}

int ObServerFileManager::list_shared_tenant_ids(const uint64_t tenant_id, ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObBackupIoAdapter adapter;
    ObBackupDest storage_dest;
    ObSharedTenantDirListOp shared_tenant_id_op(tenant_id);
    char cluster_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_cluster_dir(cluster_path, sizeof(cluster_path)))) {
      LOG_WARN("fail to get shared cluster dir", KR(ret), K(cluster_path));
    } else if (OB_FAIL(get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
    } else if (OB_FAIL(adapter.list_directories(cluster_path, storage_dest.get_storage_info(), shared_tenant_id_op))) {
      LOG_WARN("fail to list files", KR(ret), K(cluster_path));
    } else if (OB_FAIL(shared_tenant_id_op.get_file_list(tenant_ids))) {
      LOG_WARN("fail to get file list", KR(ret));
    }
  }
  return ret;
}

int ObServerFileManager::check_disk_space_available()
{
  int ret = OB_SUCCESS;
  int64_t used_disk_size = 0;
  int64_t total_disk_size = 0;
  int64_t reserved_disk_size = 0;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_FAIL(ObDiskSpaceManager::get_used_disk_size(used_disk_size))) {
    LOG_WARN("fail to get used disk size", KR(ret), K(used_disk_size));
  } else if (FALSE_IT(total_disk_size = OB_SERVER_DISK_SPACE_MGR.get_total_disk_size())) {
  } else if (FALSE_IT(reserved_disk_size = OB_SERVER_DISK_SPACE_MGR.get_reserved_disk_size())) {
  } else if (OB_FAIL(ObIODeviceLocalFileOp::check_disk_space_available(OB_DIR_MGR.get_local_cache_root_dir(),
             total_disk_size, reserved_disk_size, used_disk_size, true/*report user error*/))) {
    LOG_WARN("fail to check disk space available", KR(ret), K(total_disk_size), K(reserved_disk_size), K(used_disk_size));
  }
  return ret;
}

int ObServerFileManager::aio_write_with_create_parent_dir(
    const ObStorageObjectWriteInfo &write_info,
    const ObStorageObjectHandle &ori_object_handle,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = object_handle.get_macro_id().storage_object_type();
  if (OB_FAIL(do_aio_write(write_info, object_handle))) {
    // No such file or directory, need create file parent dir
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      object_handle = ori_object_handle;
      if (OB_FAIL(ObFileHelper::create_file_parent_dir(object_handle.get_macro_id(), write_info.ls_epoch_id_))) {
        LOG_WARN("fail to create file parent dir", KR(ret), "macro_id", object_handle.get_macro_id(),
                "ls_epoch_id", write_info.ls_epoch_id_);
      } else if (OB_FAIL(do_aio_write(write_info, object_handle))) {
        LOG_WARN("fail to do aio write", KR(ret), K(write_info), K(object_handle));
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to do aio write", KR(ret), K(object_handle), K(write_info));
    }
  }
  return ret;
}

int ObServerFileManager::create_io_calibration_file(const int64_t block_count)
{
  int ret = OB_SUCCESS;
  char io_calibration_file_path[OB_MAX_FILE_PATH_LENGTH] = { 0 };
  int open_flag = O_CREAT | O_EXCL | O_DIRECT | O_RDWR | O_LARGEFILE;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(block_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_count));
  } else {
    const int64_t alloc_file_size = block_count * DEFAULT_MACRO_BLOCK_SIZE;
    if (OB_UNLIKELY(io_calibration_fd_ >= 0)) {
      ret = OB_FILE_ALREADY_EXIST;
      LOG_WARN("io calibration file already exist", KR(ret), K_(io_calibration_fd));
    // must delete io calibration file before create io calibration file,
    // because the observer crushes after creating io_calibration_file, and has not deleted io_calibration_file
    } else if (OB_FAIL(delete_io_calibration_file())) {
      LOG_WARN("fail to delete io calibration file", KR(ret));
    } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.alloc(alloc_file_size))) {
      LOG_WARN("fail to alloc io_calibration_file size", KR(ret), K(alloc_file_size));
    } else {
      // The io_calibration_blk_cnt_ is used to represent the size of
      // the cloud disk cache occupied by io_calibration_file.
      // Therefore, space must be preallocated for io_calibration_file before its creation.
      // Once the OB_SERVER_DISK_SPACE_MGR successfully allocates space,
      // it becomes occupied by io_calibration_file,
      // and remains so until io_calibration_file is deleted.
      if (OB_FAIL(databuff_printf(io_calibration_file_path, sizeof(io_calibration_file_path),
                                  "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(), IO_CALIBRATION_FILE))) {
        LOG_WARN("fail to construct io_calibration_file_path", KR(ret));
      } else if (OB_FAIL(ObSSFileUtil::open(io_calibration_file_path, open_flag,
                         S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH, io_calibration_fd_))) {
        LOG_WARN("fail to open io_calibration_file", KR(ret), K(io_calibration_file_path));
      } else if (0 != ::fallocate(io_calibration_fd_, 0/*mode*/, 0/*offset*/, alloc_file_size)) {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        LOG_WARN("fail to fallocate io_calibration_file",
            KR(ret), K(io_calibration_file_path), K(alloc_file_size), KERRMSG);
      }
    }

    if (OB_FAIL(ret) && io_calibration_fd_ >= 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSFileUtil::close(io_calibration_fd_))) {
        LOG_ERROR("fail to close fd", KR(tmp_ret), K_(io_calibration_fd));
      } else {
        io_calibration_fd_ = OB_INVALID_FD;
      }
    }
  }
  return ret;
}

int ObServerFileManager::delete_io_calibration_file()
{
  int ret = OB_SUCCESS;
  char io_calibration_file_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else {
    if (io_calibration_fd_ >= 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSFileUtil::close(io_calibration_fd_))) {
        LOG_ERROR("fail to close fd", KR(tmp_ret), K_(io_calibration_fd));
      } else {
        io_calibration_fd_ = OB_INVALID_FD;
      }
    }
    ObIODFileStat statbuf;
    if (FAILEDx(databuff_printf(io_calibration_file_path, sizeof(io_calibration_file_path),
                                "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(), IO_CALIBRATION_FILE))) {
      LOG_WARN("fail to construct io_calibration_file_path", KR(ret));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(io_calibration_file_path, statbuf))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get file length", KR(ret), K(io_calibration_file_path));
      }
    } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(io_calibration_file_path))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("fail to del io_calibration_file", KR(ret), K(io_calibration_file_path));
      }
    } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.free(statbuf.size_))) {
      LOG_ERROR("fail to free io_calibration_file size", KR(ret), K(statbuf.size_));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
