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

#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/ob_segment_file_manager.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

/*************************************ObSSBaseReader***********************************/
int ObSSBaseReader::aio_read(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), K(read_info));
  } else {
    object_handle.reuse();
    ObIOInfo io_info;
    io_info.tenant_id_ = read_info.mtl_tenant_id_;
    io_info.offset_ = read_info.offset_;
    io_info.size_ = static_cast<int32_t>(read_info.size_);
    io_info.flag_ = read_info.io_desc_;
    io_info.callback_ = read_info.io_callback_;
    io_info.flag_.set_sys_module_id(read_info.io_desc_.get_sys_module_id());
    const int64_t real_timeout_ms = min(read_info.io_timeout_ms_,
                                        GCONF._data_storage_io_timeout / 1000L);
    io_info.timeout_us_ = real_timeout_ms * 1000L;
    io_info.user_data_buf_ = read_info.buf_;
    io_info.buf_ = read_info.buf_; // for sync io
    io_info.flag_.set_read();

    const MacroBlockId macro_id = read_info.macro_block_id_;
    const int64_t ls_epoch_id = read_info.ls_epoch_id_;
    bool bypass_micro_cache = read_info.bypass_micro_cache_;
    bool need_read_cache = (!bypass_micro_cache && is_read_micro_cache(io_info.offset_));
    if (need_read_cache) {
      // 1. read from micro cache
      if (OB_FAIL(read_micro_cache(macro_id, read_info.logic_micro_id_, read_info.micro_crc_,
                                   io_info, object_handle))) {
        if (OB_SS_MICRO_CACHE_DISABLED == ret) {
          ret = OB_SUCCESS; // ignore ret, and bypass micro cache
          bypass_micro_cache = true;
          need_read_cache = false;
        } else {
          LOG_WARN("fail to read micro cache", KR(ret), K(macro_id), K(read_info), K(io_info));
        }
      }
    }
    if (OB_SUCC(ret) && !need_read_cache) {
      // 2. do not read from micro cache
      if (OB_FAIL(get_read_device_and_fd(macro_id, ls_epoch_id, io_info, bypass_micro_cache, read_info.is_major_macro_preread_))) {
        LOG_WARN("fail to get read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info),
                 K(bypass_micro_cache), "is_major_macro_preread", read_info.is_major_macro_preread_);
      } else if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, object_handle.get_io_handle()))) {
        LOG_WARN("fail to aio_read", KR(ret), K(io_info));
      } else if (OB_FAIL(object_handle.set_macro_block_id(read_info.macro_block_id_))) {
        LOG_WARN("fail to set macro block id", KR(ret), "macro_block_id", read_info.macro_block_id_);
      }
    }
    // eliminate the impact of FLOG to local cache read io (e.g., private macro read)
    bool is_local_io = true;
    if (OB_NOT_NULL(io_info.fd_.device_handle_)) {
      if (io_info.fd_.device_handle_->is_object_device()) {
        is_local_io = false;
        FLOG_INFO("Async read object", KR(ret), K(io_info), K(macro_id), K(need_read_cache));
      } else {
        LOG_TRACE("Async read object", KR(ret), K(io_info), K(macro_id), K(need_read_cache));
      }
    }

    if (OB_SUCC(ret) && ObTenantDiskSpaceManager::is_private_macro_objtype(macro_id.storage_object_type())) {
      record_local_cache_stat(macro_id.storage_object_type(), 1 /* delta_cnt */, io_info.size_, is_local_io);
    }
  }
  return ret;
}

void ObSSBaseReader::record_local_cache_stat(
    const ObStorageObjectType object_type,
    const int64_t delta_cnt, 
    const int64_t delta_size,
    const bool cache_hit)
{
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_NOT_NULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    if (cache_hit) {
      disk_space_mgr->update_local_cache_hit_stat(object_type, delta_cnt /*hit_cnt*/, delta_size /*hit_bytes*/);
    } else {
      disk_space_mgr->update_local_cache_miss_stat(object_type, delta_cnt /*miss_cnt*/, delta_size /*miss_bytes*/);
    }
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "disk sapce manager is null");
  }
}

int ObSSBaseReader::read_from_local_cache_or_object_storage(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  UNUSED(macro_id);
  UNUSED(ls_epoch_id);
  UNUSED(io_info);
  return OB_NOT_SUPPORTED;
}

int ObSSBaseReader::read_micro_cache(
    const MacroBlockId &macro_id,
    const ObLogicMicroBlockId &logic_micro_id,
    const int64_t micro_crc,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_UNLIKELY(!macro_id.is_valid() || (io_info.offset_ <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id), "offset", io_info.offset_);
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else {
    ObSSMicroBlockCacheKey micro_key;
    if (logic_micro_id.is_valid()) {
      micro_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
      micro_key.logic_micro_id_ = logic_micro_id;
      micro_key.micro_crc_ = micro_crc;
    } else {
      micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
      micro_key.micro_id_.macro_id_ = macro_id;
      micro_key.micro_id_.offset_ = io_info.offset_;
      micro_key.micro_id_.size_ = io_info.size_;
    }
    ObSSMicroBlockId phy_micro_id(macro_id, io_info.offset_, io_info.size_);
    if (OB_FAIL(micro_cache->get_micro_block_cache(micro_key, phy_micro_id,
                MicroCacheGetType::FORCE_GET_DATA, io_info, object_handle,
                ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
      LOG_WARN("fail to get micro block cache", KR(ret), K(macro_id), K(io_info));
    }
  }
  return ret;
}

int ObSSBaseReader::try_adjust_read_size(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else {
    ObIODFileStat statbuf;
    const int64_t ori_size = io_info.size_;
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(macro_id, ls_epoch_id, true/*is_local_cache*/))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf))) {
      LOG_WARN("fail to stat", KR(ret), K(ctx));
    } else if (io_info.offset_ >= statbuf.size_) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("read offset exceeds actual file length", KR(ret), "offset", io_info.offset_,
               "actual_file_len", statbuf.size_);
    } else if ((io_info.offset_ + io_info.size_) > statbuf.size_) {
      io_info.size_ = (statbuf.size_ - io_info.offset_);
      LOG_INFO("succ to adjust read size", "offset", io_info.offset_, "ori_read_size", ori_size,
               "actual_file_len", statbuf.size_, "adjust_read_size", io_info.size_);
    }
  }
  return ret;
}

int ObSSBaseReader::read_file_from_read_cache(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info,
    const bool is_major_macro_preread)
{
  int ret = OB_SUCCESS;
  const ObStorageObjectType object_type = macro_id.storage_object_type();
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY((ObStorageObjectType::SHARED_MAJOR_DATA_MACRO != object_type) &&
                         (ObStorageObjectType::SHARED_MAJOR_META_MACRO != object_type))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the object type is not supported", KR(ret), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
  // step 1: if major_macro preread, read from local cache first, then read from object storage
  } else if (is_major_macro_preread) {
    // Note: check_exist_before_open is to reduce warning log about OB_NO_SUCH_FILE_OR_DIRECTORY
    bool is_file_exist = false;
    if (OB_FAIL(check_if_file_exist(macro_id, ls_epoch_id, is_file_exist))) {
      LOG_WARN("fail to check if file exist", KR(ret), K(macro_id), K(ls_epoch_id));
    } else if (is_file_exist) {
      if (OB_FAIL(try_read_from_read_cache(object_type, macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to try read from read cache", KR(ret), K(object_type), "object_type_str",
                  get_storage_objet_type_str(object_type), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    } else { // !is_file_exist
      if (OB_FAIL(push_lru_and_read_object_storage(object_type, macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to push lru and read object storage", KR(ret), K(object_type), "object_type_str",
                  get_storage_objet_type_str(object_type), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    }
  // step 2: if major_macro not preread, read from object storage
  } else {
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

int ObSSBaseReader::preread_next_segment_file(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id));
  } else {
    // tmp_file preread next segment file in advance
    // tmp_file's last segment file is unsealed, do not need preread, so need check next_file_id if exist in local
    MacroBlockId next_file_id(file_id);
    next_file_id.set_third_id(file_id.third_id() + 1);
    bool is_exist = false;
    ObTenantFileManager *file_manager = nullptr;
    if (OB_ISNULL(file_manager = MTL(ObTenantFileManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret));
    } else if (OB_FAIL(file_manager->is_exist_local_file(next_file_id, 0/*ls_epoch_id*/, is_exist))) {
      LOG_WARN("fail to check local file exist", KR(ret), K(next_file_id));
    } else if (!is_exist && OB_FAIL(file_manager->get_preread_cache_mgr().push_file_id_to_lru(next_file_id))) {
      LOG_WARN("fail to push file id to lru", KR(ret), K(next_file_id));
    }
  }
  return ret;
}

bool ObSSBaseReader::is_need_preread(const ObStorageObjectType object_type,
                                     const int64_t offset,
                                     const int64_t read_size)
{
  // for TMP_FILE, avoid random read, only preread first IO in segment file
  bool is_need_push = ((ObStorageObjectType::TMP_FILE == object_type) && (offset == 0) &&
                       (read_size < ObPrereadCacheManager::NOT_PREREAD_IO_SIZE)) ||
                      (ObStorageObjectType::TMP_FILE != object_type);
  return is_need_push;
}

int ObSSBaseReader::try_read_from_read_cache(
    const ObStorageObjectType object_type,
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk sapce manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_FAIL(get_local_cache_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS; // ignore ret, and read from object storage
      if (OB_FAIL(push_lru_and_read_object_storage(object_type, macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to push lru and read object storage", KR(ret), K(object_type), "object_type_str",
                 get_storage_objet_type_str(object_type), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    } else {
      LOG_WARN("fail to get local cache read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  } else {
    // if get local_cache_fd success, read hit local_cache
    disk_space_mgr->update_local_cache_hit_stat(object_type, 1/*hit_cnt*/, io_info.size_/*hit_bytes*/);
  }
  return ret;
}

int ObSSBaseReader::push_lru_and_read_object_storage(
    const ObStorageObjectType object_type,
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk sapce manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (is_need_preread(object_type, io_info.offset_, io_info.size_)) {
    // Note: push_segment_file_to_lru failure should not affect this read io
    if (OB_TMP_FAIL(file_manager->get_preread_cache_mgr().push_file_id_to_lru(macro_id))) {
      LOG_WARN("fail to put file id to lru", KR(tmp_ret), K(macro_id));
    // when tmp_file read current segment file, preread next segment file in advance
    } else if ((ObStorageObjectType::TMP_FILE == object_type) &&
                OB_TMP_FAIL(preread_next_segment_file(macro_id))) {
      LOG_WARN("fail to preread next segment file", KR(tmp_ret), K(macro_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                              macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(io_info));
  } else {
    // if get object_storage_fd success, read miss local_cache
    if (io_info.size_ < ObPrereadCacheManager::NOT_PREREAD_IO_SIZE) { // if big read IO do not preread, so do not need stat miss count
      disk_space_mgr->update_local_cache_miss_stat(object_type, 1/*miss_cnt*/, io_info.size_/*miss_bytes*/);
    }
  }
  return ret;
}

/*************************************ObSSBaseWriter***********************************/
int ObSSBaseWriter::aio_write(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid() || (OB_NOT_NULL(write_info.io_callback_) &&
                 (ObIOCallbackType::SS_TMP_FILE_CALLBACK != write_info.io_callback_->get_type())))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write info", KR(ret), K(write_info));
  } else {
    ObIOInfo io_info;
    io_info.tenant_id_ = write_info.mtl_tenant_id_;
    io_info.offset_ = write_info.offset_;
    io_info.size_ = write_info.size_;
    io_info.buf_ = write_info.buffer_;
    io_info.flag_ = write_info.io_desc_;
    io_info.callback_ = write_info.io_callback_;
    io_info.tmp_file_valid_length_ = write_info.tmp_file_valid_length_;
    io_info.flag_.set_sys_module_id(write_info.io_desc_.get_sys_module_id());
    const int64_t real_timeout_ms = min(write_info.io_timeout_ms_,
                                        GCONF._data_storage_io_timeout / 1000L);
    io_info.timeout_us_ = real_timeout_ms * 1000L;
    io_info.flag_.set_write();

    const MacroBlockId macro_id = object_handle.get_macro_id();
    const int64_t ls_epoch_id = write_info.ls_epoch_id_;
    if (OB_FAIL(get_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to get write device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
      }
    } else if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, object_handle.get_io_handle()))) {
      LOG_WARN("fail to aio_write", KR(ret), "macro_id", object_handle.get_macro_id(), K(write_info));
      if (OB_NOT_NULL(io_info.callback_)) {
        if (ObIOCallbackType::ATOMIC_WRITE_CALLBACK == io_info.callback_->get_type()) {
          free_io_callback<ObAtomicWriteIOCallback>(io_info.callback_);
        } else {
          int tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid callback type", KR(tmp_ret), "callback_type", io_info.callback_->get_type());
        }
      }
    } else {
      FLOG_INFO("Async write object", "macro_id", object_handle.get_macro_id(), K(io_info),
                "crc", ob_crc64(io_info.buf_, io_info.size_));
    }
    if (OB_FAIL(ret)) {
      object_handle.reset();
    }
  }
  return ret;
}

int ObSSBaseWriter::get_local_cache_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else {
    uint64_t seq = OB_INVALID_ID;
    ObBaseFileManager *file_manager = nullptr;
    ObStorageObjectType object_type = macro_id.storage_object_type();
    const bool is_tmp_file = ((ObStorageObjectType::TMP_FILE == object_type) ? true : false);
    ObPathContext ctx;
    // TMP_FILE does not write filename.tmp.seq, non-TMP_FILE write filename.tmp.seq
    if (need_atomic_write() && OB_FAIL(get_atomic_write_seq(macro_id, seq))) {
      LOG_WARN("fail to get atomic write seq", KR(ret));
    } else if (OB_FAIL(get_file_manager(io_info.tenant_id_, file_manager))) {
      LOG_WARN("fail to get file manager", KR(ret), "tenant_id", io_info.tenant_id_);
    } else if (OB_ISNULL(file_manager)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
    } else if (need_atomic_write() && OB_FAIL(ctx.set_atomic_write_ctx(macro_id, ls_epoch_id, seq))) {
      LOG_WARN("fail to construct file path", KR(ret), K(ctx));
    } else if (!need_atomic_write() && OB_FAIL(ctx.set_file_ctx(macro_id, ls_epoch_id, true/*is_local_cache*/))) {
      LOG_WARN("fail to construct file path", KR(ret), K(ctx));
    } else {
      const int open_flag = (is_tmp_file ? SS_TMP_FILE_WRITE_FLAG : SS_DEFAULT_WRITE_FLAG);
      if (OB_FAIL(get_fd_cache_handle(ctx.get_path(), open_flag, macro_id, *file_manager, io_info))) {
        LOG_WARN("fail to get fd cache handle", KR(ret), K(ctx), K(open_flag));
      } else if (need_atomic_write() && OB_FAIL(set_atomic_write_io_callback(macro_id, ls_epoch_id, seq, io_info))) {
        LOG_WARN("fail to set io callback", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    }
  }
  return ret;
}

int ObSSBaseWriter::set_atomic_write_io_callback(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObAtomicWriteIOCallbackType type = ObAtomicWriteIOCallbackType::MAX_TYPE;
  ObIAllocator *callback_allocator = nullptr;
  int64_t tenant_epoch_id = -1;
  if (OB_UNLIKELY(!macro_id.is_valid() || (io_info.offset_ < 0) || (io_info.size_ <= 0)
      || !is_valid_tenant_id(io_info.tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(ls_epoch_id), K(seq), K(io_info));
  } else if (OB_FAIL(get_atomic_write_io_callback_type(macro_id, type))) {
    LOG_WARN("fail to get atomic write io callback type", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_io_callback_allocator(io_info.tenant_id_, callback_allocator))) {
    LOG_WARN("fail to get io callback allocator", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(callback_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("callback allocator is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_FAIL(alloc_atomic_write_io_callback(type, *callback_allocator, macro_id,
      io_info.tenant_id_, ls_epoch_id, seq, io_info.offset_, io_info.size_, io_info.callback_))) {
    LOG_WARN("fail to alloc atomic write io callback", KR(ret), K(type), K(macro_id),
      K(tenant_epoch_id), K(ls_epoch_id), K(seq), K(io_info));
  }
  return ret;
}

int ObSSBaseWriter::get_atomic_write_io_callback_type(
    const MacroBlockId &macro_id,
    ObAtomicWriteIOCallbackType &type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else {
    ObStorageObjectType object_type = macro_id.storage_object_type();
    switch (object_type) {
      case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:  // only major macro preread task write local need atomic write
      case ObStorageObjectType::SHARED_MAJOR_META_MACRO:  // only major macro preread task write local need atomic write
      case ObStorageObjectType::TMP_FILE:  // only tmp file preread task write local need atomic write
      case ObStorageObjectType::PRIVATE_DATA_MACRO:
      case ObStorageObjectType::PRIVATE_META_MACRO: {
        type = ObAtomicWriteIOCallbackType::RENAME_TYPE;
        break;
      }
      case ObStorageObjectType::LS_META:
      case ObStorageObjectType::PRIVATE_TABLET_META:
      case ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION:
      case ObStorageObjectType::LS_TRANSFER_TABLET_ID_ARRAY:
      case ObStorageObjectType::LS_ACTIVE_TABLET_ARRAY:
      case ObStorageObjectType::LS_PENDING_FREE_TABLET_ARRAY:
      case ObStorageObjectType::LS_DUP_TABLE_META:
      case ObStorageObjectType::SERVER_META:
      case ObStorageObjectType::TENANT_DISK_SPACE_META:
      case ObStorageObjectType::TENANT_SUPER_BLOCK:
      case ObStorageObjectType::TENANT_UNIT_META: {
        type = ObAtomicWriteIOCallbackType::RENAME_FSYNC_TYPE;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected object type", KR(ret), K(object_type), "object_type_str",
                 get_storage_objet_type_str(object_type), K(macro_id));
      }
    }
  }
  return ret;
}

int ObSSBaseWriter::alloc_atomic_write_io_callback(
    const ObAtomicWriteIOCallbackType &type,
    ObIAllocator &allocator,
    const MacroBlockId &macro_id,
    const uint64_t tenant_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    ObIOCallback *&callback)
{
  int ret = OB_SUCCESS;
  if (ObAtomicWriteIOCallbackType::RENAME_TYPE == type) {
    if (OB_FAIL(create_atomic_write_io_callback<ObRenameIOCallback>(allocator,
                macro_id, tenant_id, ls_epoch_id, seq, offset, size, callback))) {
      LOG_WARN("fail to create atomic write io callback", KR(ret), K(macro_id), K(tenant_id),
               K(ls_epoch_id), K(seq), K(offset), K(size));
    }
  } else if (ObAtomicWriteIOCallbackType::RENAME_FSYNC_TYPE == type) {
    if (OB_FAIL(create_atomic_write_io_callback<ObRenameFsyncIOCallback>(allocator,
                macro_id, tenant_id, ls_epoch_id, seq, offset, size, callback))) {
      LOG_WARN("fail to create atomic write io callback", KR(ret), K(macro_id), K(tenant_id),
               K(ls_epoch_id), K(seq), K(offset), K(size));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected atomic write io callback type", KR(ret), K(type), K(macro_id),
             K(tenant_id), K(ls_epoch_id), K(seq), K(offset), K(size));
  }
  return ret;
}


/*********************ObSSObjectStorageReader & ObSSObjectStorageWriter****************/
int ObSSObjectStorageReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info,
    const bool bypass_micro_cache,
    const bool is_major_macro_preread)
{
  int ret = OB_SUCCESS;
  UNUSED(bypass_micro_cache);
  UNUSED(is_major_macro_preread);
  // for object whose offset and size are known, adopt NOHEAD_READER to avoid the overhead of head
  ObStorageObjectType object_type = macro_id.storage_object_type();
  ObStorageAccessType access_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  if ((ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == object_type)
      || (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type)
      || (ObStorageObjectType::MAJOR_PREWARM_DATA == object_type)
      || (ObStorageObjectType::MAJOR_PREWARM_META == object_type)
      || (ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX == object_type)
      || (ObStorageObjectType::MAJOR_PREWARM_META_INDEX == object_type)
      || (ObStorageObjectType::TMP_FILE == object_type)) {
    // tmp_file preread do not need HEAD, becase tmp_file preread offset is 0, when offset is 0, allow read_size larger than file_size
    // tmp_file preread use ObSSObjectStorageReader, because tmp_file preread directly from object storage
    access_type = ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER;
  } else {
    access_type = ObStorageAccessType::OB_STORAGE_ACCESS_READER;
  }
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_object_device_and_fd(access_type, macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), K(access_type), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSObjectStorageReader::is_read_micro_cache(const int64_t offset)
{
  UNUSED(offset);
  return false;
}

int ObSSObjectStorageWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                              macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSObjectStorageWriter::need_atomic_write()
{
  return false;
}

int ObSSObjectStorageWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(macro_id);
  UNUSED(seq);
  LOG_WARN("object storage writer does not support get atomic write seq", KR(ret));
  return ret;
}


/************************ObSSLocalCacheReader & ObSSLocalCacheWriter*******************/
int ObSSLocalCacheReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info,
    const bool bypass_micro_cache,
    const bool is_major_macro_preread)
{
  int ret = OB_SUCCESS;
  UNUSED(bypass_micro_cache);
  UNUSED(is_major_macro_preread);
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_local_cache_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get local cache read device and fd", KR(ret), K(macro_id),
             K(ls_epoch_id), K(io_info));
  } else if (OB_FAIL(try_adjust_read_size(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to try adjust read size", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSLocalCacheReader::is_read_micro_cache(const int64_t offset)
{
  UNUSED(offset);
  return false;
}

int ObSSLocalCacheWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
              K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

bool ObSSLocalCacheWriter::need_atomic_write()
{
  return true;
}

int ObSSLocalCacheWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else {
    ObStorageObjectType object_type = macro_id.storage_object_type();
    switch (object_type) {
      // to avoid circular dependencies, the following object types use timestamp as write seq
      case ObStorageObjectType::SERVER_META:
      case ObStorageObjectType::TENANT_SUPER_BLOCK:
      case ObStorageObjectType::TENANT_UNIT_META: {
        seq = static_cast<uint64_t>(ObTimeUtility::current_time());
        break;
      }
      default: {
        if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(seq))) {
          LOG_WARN("fail to get write seq", KR(ret));
        }
        break;
      }
    }
  }
  return ret;
}


/**********************ObSSPrivateMacroReader & ObSSPrivateMacroWriter*****************/
int ObSSPrivateMacroReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info,
    const bool bypass_micro_cache,
    const bool is_major_macro_preread)
{
  int ret = OB_SUCCESS;
  UNUSED(is_major_macro_preread);
  // offset < 0 is invalid, offset > 0 should read micro cache
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (!bypass_micro_cache && (io_info.offset_ != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "offset", io_info.offset_);
  } else if (OB_FAIL(read_from_local_cache_or_object_storage(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to read from local cache or object storage", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
  }
  return ret;
}

int ObSSPrivateMacroReader::read_from_local_cache_or_object_storage(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_local_cache_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                            macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
      }
    } else {
      LOG_WARN("fail to get local cache read device and fd", KR(ret), K(macro_id),
              K(ls_epoch_id), K(io_info));
    }
  } else if ((0 == io_info.offset_)
             && (OB_STORAGE_OBJECT_MGR.get_macro_block_size() == io_info.size_)
             && OB_FAIL(try_adjust_read_size(macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to try adjust read size", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSPrivateMacroReader::is_read_micro_cache(const int64_t offset)
{
  return (offset > 0); // micro cache only cache micro, whose offset is larger than zero
}

int ObSSPrivateMacroWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (io_info.flag_.is_write_through()) { // disk space is not enough
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
    }
  } else { // user set write_back and disk space is enough
    if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
                K(ls_epoch_id), K(io_info));
      }
    }
  }
  return ret;
}

bool ObSSPrivateMacroWriter::need_atomic_write()
{
  return true;
}

int ObSSPrivateMacroWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_SUCCESS;
  UNUSED(macro_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(seq))) {
    LOG_WARN("fail to get write seq", KR(ret));
  }
  return ret;
}


/************************ObSSShareMacroReader & ObSSShareMacroWriter*******************/
int ObSSShareMacroReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info,
    const bool bypass_micro_cache,
    const bool is_major_macro_preread)
{
  int ret = OB_SUCCESS;
  // offset < 0 is invalid, offset > 0 should read micro cache
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (!bypass_micro_cache && (io_info.offset_ != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "offset", io_info.offset_);
  } else if (OB_FAIL(read_file_from_read_cache(macro_id, ls_epoch_id, io_info, is_major_macro_preread))) {
    LOG_WARN("fail to read file from read cache", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info), K(is_major_macro_preread));
  }
  return ret;
}

int ObSSShareMacroReader::read_from_local_cache_or_object_storage(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                              macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSShareMacroReader::is_read_micro_cache(const int64_t offset)
{
  return (offset > 0); // micro cache only cache micro, whose offset is larger than zero
}

int ObSSShareMacroWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                              macro_id, ls_epoch_id, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
  }
  return ret;
}

bool ObSSShareMacroWriter::need_atomic_write()
{
  return false;
}

int ObSSShareMacroWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(macro_id);
  UNUSED(seq);
  LOG_WARN("share macro writer does not support get atomic write seq", KR(ret));
  return ret;
}


/***************************ObSSTmpFileReader & ObSSTmpFileWriter**********************/
int ObSSTmpFileReader::get_read_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info,
    const bool bypass_micro_cache,
    const bool is_major_macro_preread)
{
  int ret = OB_SUCCESS;
  UNUSED(bypass_micro_cache);
  UNUSED(is_major_macro_preread);
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(inner_get_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    // When multiple threads read the same segment concurrently, thread 1 reads the second half of segment and finishes reading it,
    // so the PreReadCache will evict this segment and delete it's seg_meta;
    // when thread 2 reads the first half of segment, it will get_seg_meta when get_read_device_and_fd. At this time, seg_meta exists and is_meta_exist is true,
    // and get_tmp_file_path is called. Here, it gets_seg_meta again but it has been deleted by thread 1, so is_meta_exist is false, report error.
    // so retry again.
    if (OB_EAGAIN == ret) {
      if (OB_FAIL(inner_get_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    } else {
      LOG_WARN("fail to get read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

int ObSSTmpFileReader::inner_get_read_device_and_fd(
    const blocksstable::MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    common::ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  TmpFileSegId seg_id(macro_id.second_id(), macro_id.third_id());
  TmpFileMetaHandle meta_handle;
  bool is_meta_exist = false;
  if (OB_FAIL(try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to try get seg meta", KR(ret), K(seg_id));
  } else if (is_meta_exist) { // case 1: is_meta_exist = true
    if (OB_FAIL(handle_read_of_meta_exist(meta_handle, macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to handle read of meta exist", KR(ret), K(meta_handle), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  } else { // case 2: is_meta_exist = false
    if (OB_FAIL(handle_read_of_meta_not_exist(macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to handle read of meta not exist", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

bool ObSSTmpFileReader::is_read_micro_cache(const int64_t offset)
{
  UNUSED(offset);
  return false;
}

int ObSSTmpFileReader::handle_read_of_meta_exist(
    const TmpFileMetaHandle &meta_handle,
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk sapce manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmpfile meta handle", KR(ret), K(meta_handle));
  } else {
    // Note: lock to avoid concurrency with 'flush and delte_local_file'
    SpinRLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
    if (meta_handle.is_in_local()) { // case 1: seg is in local
      if (OB_FAIL(get_local_cache_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get local cache read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
      } else {
        // if get local_cache_fd success, read hit local_cache
        disk_space_mgr->update_local_cache_hit_stat(ObStorageObjectType::TMP_FILE, 1/*hit_cnt*/, io_info.size_/*hit_bytes*/);
        // when tmp_file read current segment file, preread next segment file in advance
        if (is_need_preread(ObStorageObjectType::TMP_FILE, io_info.offset_, io_info.size_)) {
          if (OB_TMP_FAIL(preread_next_segment_file(macro_id))) {
            LOG_WARN("fail to preread next segment file", KR(tmp_ret), K(macro_id));
          }
        }
      }
    } else {  // case 2: seg is in remote (which is unsealed)
      io_info.flag_.set_unsealed(); // remote meta exist, must be unsealed
      if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                          macro_id, ls_epoch_id, io_info))) {
        LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(io_info));
      } else {
        // if get object_storage_fd success, read miss local_cache
        if (io_info.size_ < ObPrereadCacheManager::NOT_PREREAD_IO_SIZE) { // if big read IO do not preread, so do not need stat miss count
          disk_space_mgr->update_local_cache_miss_stat(ObStorageObjectType::TMP_FILE, 1/*miss_cnt*/, io_info.size_/*miss_bytes*/);
        }
      }
    }
  }
  return ret;
}

int ObSSTmpFileReader::handle_read_of_meta_not_exist(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  ObTenantFileManager *file_manager = nullptr;
  bool is_exist = false;
  io_info.flag_.set_sealed(); // meta not exist, must be sealed
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk sapce manager is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager->get_preread_cache_mgr().is_exist_in_lru(macro_id, is_exist))) {
    LOG_WARN("fail to check is exist in lru", KR(ret), K(macro_id));
  } else if (is_exist) { // case 1: exist preread cache
    if (OB_FAIL(get_local_cache_read_device_and_fd(macro_id, ls_epoch_id, io_info))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS; // ignore ret, and read from object storage
        if (OB_FAIL(push_lru_and_read_object_storage(ObStorageObjectType::TMP_FILE, macro_id, ls_epoch_id, io_info))) {
          LOG_WARN("fail to push lru and read object storage", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
        }
      } else {
        LOG_WARN("fail to get local cache read device and fd", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
      }
    } else {
      // if get local_cache_fd success, read hit local_cache
      disk_space_mgr->update_local_cache_hit_stat(ObStorageObjectType::TMP_FILE, 1/*hit_cnt*/, io_info.size_/*hit_bytes*/);
      // when tmp_file read current segment file, preread next segment file in advance
      int tmp_ret = OB_SUCCESS;
      if (is_need_preread(ObStorageObjectType::TMP_FILE, io_info.offset_, io_info.size_)) {
        if (OB_TMP_FAIL(preread_next_segment_file(macro_id))) {
          LOG_WARN("fail to preread next segment file", KR(tmp_ret), K(macro_id));
        }
      }
    }
  } else { // case 2: not exist preread cache
    if (OB_FAIL(push_lru_and_read_object_storage(ObStorageObjectType::TMP_FILE, macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to push lru and read object storage", KR(ret), K(macro_id), K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

int ObSSTmpFileWriter::get_write_device_and_fd(
    const MacroBlockId &macro_id,
    const int64_t ls_epoch_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (io_info.flag_.is_write_through()) { // alloc write cache disk space is threshold reached
    if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_OVERWRITER,
                                         macro_id, ls_epoch_id, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id), K(ls_epoch_id));
    }
  } else if (OB_FAIL(get_local_cache_write_device_and_fd(macro_id, ls_epoch_id, io_info))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      LOG_WARN("fail to get local cache write device and fd", KR(ret), K(macro_id),
              K(ls_epoch_id), K(io_info));
    }
  }
  return ret;
}

bool ObSSTmpFileWriter::need_atomic_write()
{
  return false;
}

int ObSSTmpFileWriter::get_atomic_write_seq(
    const MacroBlockId &macro_id,
    uint64_t &seq)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(macro_id);
  UNUSED(seq);
  LOG_WARN("tmp file writer does not support get atomic write seq", KR(ret));
  return ret;
}


} /* namespace storage */
} /* namespace oceanbase */
