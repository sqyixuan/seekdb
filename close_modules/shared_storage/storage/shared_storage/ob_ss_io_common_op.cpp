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

#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "share/ob_ss_file_util.h"
#include "share/io/ob_io_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_helper.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObSSIOCommonOp::get_object_device_and_fd(
    const ObStorageAccessType access_type,
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info) const
{
  int ret = OB_SUCCESS;
  ObIOFd fd;
  ObBackupDest storage_dest;
  uint64_t storage_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!is_supported_access_type(access_type) || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(access_type), K(macro_id));
  } else if (OB_FAIL(get_storage_dest_and_id(storage_dest, storage_id))) {
    LOG_WARN("fail to get storage dest", KR(ret));
  } else if (OB_FAIL(open_object_fd(io_info, macro_id, ls_epoch_id, storage_dest,
                                    storage_id, access_type, fd))) {
    LOG_WARN("fail to get and open object device", KR(ret), K(io_info), K(macro_id), K(ls_epoch_id),
              K(storage_dest), K(access_type));
  } else {
    io_info.timeout_us_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(io_info.tenant_id_) * 1000L;
    io_info.fd_ = fd;
    io_info.flag_.set_sync(); // ObObjectDevice do_sync_io in ObSyncIOChannel
    if ((ObStorageAccessType::OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER != access_type)
        && (ObStorageAccessType::OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER != access_type)) {
      io_info.flag_.set_need_close_dev_and_fd(); // close ObObjectDevice and fd
    }
  }
  return ret;
}

int ObSSIOCommonOp::get_storage_dest_and_id(
    ObBackupDest &storage_dest,
    uint64_t &storage_id) const
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
      LOG_WARN("fail to set storage dest", KR(ret), K(device_config));
    } else if (FALSE_IT(storage_id = device_config.storage_id_)) {
    } else if (OB_UNLIKELY(!storage_dest.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage dest", KR(ret), K(storage_dest));
    }
  }
  return ret;
}

int ObSSIOCommonOp::open_object_fd(
    const ObIOInfo &io_info,
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    const ObBackupDest &storage_dest,
    const uint64_t storage_id,
    const ObStorageAccessType access_type,
    ObIOFd &fd) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = io_info.tenant_id_;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || !macro_id.is_valid()
                  || !storage_dest.is_valid()
                  || !is_supported_access_type(access_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(macro_id), K(storage_dest), K(access_type));
  } else {
    ObBackupIoAdapter io_adapter;
    ObIODevice *io_device = nullptr;
    ObStorageIdMod storage_id_mod(storage_id, ObStorageUsedMod::STORAGE_USED_DATA);
    const ObStorageObjectType object_type = macro_id.storage_object_type();
    ObPathContext ctx;
    if ((ObStorageObjectType::TMP_FILE == object_type) && OB_FAIL(get_tmp_file_path(io_info, macro_id, ctx))) {
      LOG_WARN("fail to get tmp file path", KR(ret), K(io_info), K(macro_id));
    } else if ((ObStorageObjectType::TMP_FILE != object_type) && OB_FAIL(ctx.set_file_ctx(macro_id, ls_epoch_id, false/*is_local_cache*/))) {
      LOG_WARN("fail to construct file path", KR(ret), K(ctx));
    } else if (OB_FAIL(io_adapter.open_with_access_type(io_device, fd, storage_dest.get_storage_info(),
                       ctx.get_path(), access_type, storage_id_mod))) {
      LOG_WARN("fail to open with access type", KR(ret), K(storage_dest), K(ctx));
    } else {
      fd.device_handle_ = io_device;
    }
  }
  return ret;
}

int ObSSIOCommonOp::get_local_cache_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || !is_valid_tenant_id(io_info.tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), "tenant_id", io_info.tenant_id_);
  } else {
    ObStorageObjectType object_type = macro_id.storage_object_type();
    bool is_get_from_fd_cache = need_get_from_fd_cache(io_info.tenant_id_, object_type);
    if (is_get_from_fd_cache) {
      if (OB_FAIL(get_from_fd_cache(macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get from fd cache", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    } else { // !is_get_from_fd_cache
      if (OB_FAIL(get_fd_by_open(macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get fd by open", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    }
  }
  return ret;
}

bool ObSSIOCommonOp::is_supported_access_type(const ObStorageAccessType access_type) const
{
  return ((ObStorageAccessType::OB_STORAGE_ACCESS_READER == access_type)
          || (ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER == access_type)
          || (ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER == access_type)
          || (ObStorageAccessType::OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER == access_type)
          || (ObStorageAccessType::OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type));
}

int ObSSIOCommonOp::get_file_manager(
    const uint64_t tenant_id,
    ObBaseFileManager *&file_manager) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_SERVER_TENANT_ID == tenant_id) {
    file_manager = &OB_SERVER_FILE_MGR;
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSIOCommonOp::get_io_callback_allocator(
    const uint64_t tenant_id,
    common::ObIAllocator *&allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    ObBaseFileManager *file_manager = nullptr;
    if (OB_FAIL(get_file_manager(tenant_id, file_manager))) {
      LOG_WARN("fail to get file manager", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(file_manager)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(allocator = &(file_manager->get_io_callback_allocator()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io callback allocator is null", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

bool ObSSIOCommonOp::is_supported_fd_cache_obj_type(const ObStorageObjectType object_type)
{
  return ((ObStorageObjectType::PRIVATE_META_MACRO == object_type) ||
          (ObStorageObjectType::PRIVATE_DATA_MACRO == object_type) ||
          (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type) ||
          (ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == object_type));
}

int ObSSIOCommonOp::get_fd_cache_handle(
    const char *file_path,
    const int open_flag,
    const MacroBlockId &macro_id,
    ObBaseFileManager &file_manager,
    ObIOInfo &io_info) const
{
  int ret = OB_SUCCESS;
  ObSSFdCacheHandle fd_cache_handle;
  ObSSFdCacheNode *fd_cache_node = nullptr;
  ObSmallObjPool<ObSSFdCacheNode> &fd_cache_node_pool = file_manager.get_fd_cache_node_pool();
  ObSSFdCacheKey fd_cache_key(macro_id);
  int fd = OB_INVALID_FD;
  if (OB_ISNULL(file_path) || OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(file_path), K(macro_id));
  } else if (OB_FAIL(ObSSFileUtil::open(file_path, open_flag, SS_FILE_OPEN_MODE, fd))) {
    LOG_WARN("fail to open", KR(ret), K(file_path));
  } else if (OB_FAIL(fd_cache_node_pool.alloc(fd_cache_node))) {
    LOG_WARN("fail to alloc fd cache node", KR(ret));
  } else if (OB_ISNULL(fd_cache_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fd cache node alloc from pool is null", KR(ret));
  } else if (FALSE_IT(fd_cache_node->reset())) {
  } else if (FALSE_IT(fd_cache_node->key_ = fd_cache_key)) {
  } else if (FALSE_IT(fd_cache_node->fd_ = fd)) {
  } else if (FALSE_IT(fd_cache_node->timestamp_us_ = ObTimeUtility::fast_current_time())) {
  } else if (OB_FAIL(fd_cache_handle.set_fd_cache_node(fd_cache_node, &fd_cache_node_pool))) {
    LOG_WARN("fail to set fd cache node", KR(ret), KPC(fd_cache_node), KP(&fd_cache_node_pool));
  } else {
    io_info.fd_.first_id_ = macro_id.first_id(); // first_id is not used in shared storage mode
    io_info.fd_.second_id_ = fd;
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    if (OB_FAIL(io_info.fd_cache_handle_.assign(fd_cache_handle))) {
      LOG_WARN("fail to assign fd cache handle", KR(ret), K(fd_cache_handle));
    }
  }

  // if has not set fd_cache_node into fd_cache_handle, need to close fd and free memory
  if (OB_FAIL(ret) && !fd_cache_handle.is_valid()) {
    if (OB_INVALID_FD != fd) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSFileUtil::close(fd))) {
        LOG_ERROR("fail to close fd", KR(tmp_ret), K(fd));
      }
    }
    if (OB_NOT_NULL(fd_cache_node)) {
      int tmp_ret = OB_SUCCESS;
      fd_cache_node->reset();
      if (OB_TMP_FAIL(fd_cache_node_pool.free(fd_cache_node))) {
        LOG_ERROR("fail to free fd cache node", KR(tmp_ret), KPC(fd_cache_node));
      }
    }
  }
  return ret;
}

bool ObSSIOCommonOp::need_get_from_fd_cache(
    const uint64_t tenant_id,
    const ObStorageObjectType object_type) const
{
  bool is_read_from_fd_cache = false;
  is_read_from_fd_cache = ((OB_SERVER_TENANT_ID != tenant_id) &&
                           is_supported_fd_cache_obj_type(object_type));
  return is_read_from_fd_cache;
}

int ObSSIOCommonOp::get_from_fd_cache(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info) const
{
  int ret = OB_SUCCESS;
  UNUSED(ls_epoch_id);
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else {
    ObSSFdCacheMgr &fd_cache_mgr = file_manager->get_fd_cache_mgr();
    ObSSFdCacheHandle fd_cache_handle;
    if (OB_FAIL(fd_cache_mgr.get_fd(macro_id, fd_cache_handle))) {
      LOG_WARN("fail to get fd from fd cache mgr", KR(ret), K(macro_id));
    } else if (OB_UNLIKELY(!fd_cache_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fd cache handle should not be invalid", KR(ret), K(fd_cache_handle));
    } else if (OB_UNLIKELY(OB_INVALID_FD == fd_cache_handle.get_fd())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fd should not be invalid", KR(ret), K(fd_cache_handle));
    } else {
      io_info.fd_.first_id_ = macro_id.first_id(); // first_id is not used in shared storage mode
      io_info.fd_.second_id_ = fd_cache_handle.get_fd();
      io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
      if (OB_FAIL(io_info.fd_cache_handle_.assign(fd_cache_handle))) {
        LOG_WARN("fail to assign fd cache handle", KR(ret), K(fd_cache_handle));
      }
    }
  }
  return ret;
}

int ObSSIOCommonOp::get_fd_by_open(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info) const
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_manager = nullptr;
  ObPathContext ctx;
  if (OB_FAIL(get_file_manager(io_info.tenant_id_, file_manager))) {
    LOG_WARN("fail to get file manager", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_FAIL(ctx.set_file_ctx(macro_id, ls_epoch_id, true/*is_local_cache*/))) {
    LOG_WARN("fail to construct file path", KR(ret), K(ctx));
  } else {
    ObStorageObjectType object_type = macro_id.storage_object_type();
    const bool is_tmp_file = ((ObStorageObjectType::TMP_FILE == object_type) ? true : false);
    const int open_flag = (is_tmp_file ? SS_TMP_FILE_READ_FLAG : SS_DEFAULT_READ_FLAG);
    if (OB_FAIL(get_fd_cache_handle(ctx.get_path(), open_flag, macro_id, *file_manager, io_info))) {
      LOG_WARN("fail to get fd cache handle", KR(ret), K(ctx), K(open_flag));
    }
  }
  return ret;
}

int ObSSIOCommonOp::get_tmp_file_path(
    const ObIOInfo &io_info,
    const MacroBlockId &macro_id,
    ObPathContext &ctx) const
{
  int ret = OB_SUCCESS;
  const ObStorageObjectType object_type = macro_id.storage_object_type();
  MacroBlockId tmp_file_id(macro_id);
  if (OB_UNLIKELY(ObStorageObjectType::TMP_FILE != object_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected object type", KR(ret), K(macro_id));
  } else if (io_info.flag_.is_sealed()) {
    // do nothing, only read/write unsealed tmp_file need convert TMP_FILE to UNSEALED_REMOTE_SEG_FILE
  } else if (io_info.flag_.is_read()) {
    TmpFileSegId seg_id(tmp_file_id.second_id(), tmp_file_id.third_id());
    TmpFileMetaHandle meta_handle;
    bool is_meta_exist = false;
    if (OB_FAIL(try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
      LOG_WARN("fail to try get segment meta", KR(ret), K(seg_id));
    } else if (is_meta_exist) {
      if (meta_handle.is_valid() && !meta_handle.is_in_local()) {
        // if seg_id is in object storage, tmp_file is unsealed, seg_id's path need valid_length, use UNSEALED_REMOTE_SEG_FILE object_type
        tmp_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE));
        tmp_file_id.set_fourth_id(meta_handle.get_valid_length());
      } else {
        // if read remote unsealed seg_id, seg is must in remote, otherwise unexpected error
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, meta handle is invalid or seg is in local", KR(ret), K(io_info), K(is_meta_exist), K(tmp_file_id), K(meta_handle), K(seg_id));
      }
    } else {
      // When multiple threads read the same segment concurrently, thread 1 reads the second half of segment and finishes reading it,
      // so the PreReadCache will evict this segment and delete it's seg_meta;
      // when thread 2 reads the first half of segment, it will get_seg_meta when get_read_device_and_fd. At this time, seg_meta exists and is_meta_exist is true,
      // and get_tmp_file_path is called. Here, it gets_seg_meta again but it has been deleted by thread 1, so is_meta_exist is false, report error.
      ret = OB_EAGAIN;
      LOG_WARN("unsealed segment on object storage must has seg meta", KR(ret), K(io_info), K(is_meta_exist), K(tmp_file_id), K(meta_handle), K(seg_id));
    }
  } else if (io_info.flag_.is_write()) {
    // if write unsealed tmp_file need valid_length concat path, use UNSEALED_REMOTE_SEG_FILE object_type
    tmp_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE));
    tmp_file_id.set_fourth_id(io_info.tmp_file_valid_length_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected io mode", KR(ret), K(io_info), K(tmp_file_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ctx.set_file_ctx(tmp_file_id, 0/*ls_epoch_id*/, false/*is_local_cache*/))) {
    LOG_WARN("fail to convert file path", KR(ret), K(ctx));
  }
  return ret;
}

int ObSSIOCommonOp::check_if_file_exist(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    bool &is_file_exist) const
{
  int ret = OB_SUCCESS;
  is_file_exist = false;
  bool is_fd_cache_exist = false;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else {
    ObSSFdCacheMgr &fd_cache_mgr = file_manager->get_fd_cache_mgr();
    if (OB_FAIL(fd_cache_mgr.check_fd_cache_exist(macro_id, is_fd_cache_exist))) {
      LOG_WARN("fail to check fd cache exist", KR(ret), K(macro_id));
    } else if (is_fd_cache_exist) {
      is_file_exist = true; // fd cache exist represents file exist
    } else if (OB_FAIL(file_manager->is_exist_local_file(macro_id, ls_epoch_id, is_file_exist))) {
      LOG_WARN("fail to check is exist local file", KR(ret), K(macro_id), K(ls_epoch_id));
    }
  }
  return ret;
}

int ObSSIOCommonOp::try_get_seg_meta(
    const TmpFileSegId &seg_id,
    TmpFileMetaHandle &meta_handle,
    bool &is_meta_exist) const
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else {
    ObSegmentFileManager &segment_file_manager = file_manager->get_segment_file_mgr();
    if (OB_FAIL(segment_file_manager.try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
      LOG_WARN("fail to try get segment meta", KR(ret), K(seg_id));
    }
  }
  return ret;
}


} /* namespace storage */
} /* namespace oceanbase */
