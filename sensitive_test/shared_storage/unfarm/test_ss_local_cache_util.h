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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include "lib/ob_errno.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "lib/random/ob_random.h"

namespace oceanbase 
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSLocalCacheUtil
{
public:
  static MacroBlockId gen_macro_block_id(const ObStorageObjectType obj_type = ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
    const int64_t second_id = 1, const int64_t third_id = 1000, const int64_t fourth_id = 1000000);
  static int gen_random_data(char *buf, const int64_t size);
  static int gen_write_info(ObStorageObjectWriteInfo &write_info, const uint64_t tenant_id, char *buf,
    const int64_t offset, const int64_t size);
  static int gen_read_info(ObStorageObjectReadInfo &read_info, const MacroBlockId &macro_id, const uint64_t tenant_id, 
    char *buf, const int64_t offset, const int64_t size);
  static int write_private_tablet_meta(ObTenantFileManager *file_mgr, const uint64_t tenant_id, const MacroBlockId &macro_id,
    const int64_t size, const int64_t ls_epoch_id, char *write_buf = nullptr);
  static int write_tmp_file(const uint64_t tenant_id, const MacroBlockId &macro_id, const int64_t offset, 
    const int64_t size, const bool is_sealed, char *write_buf = nullptr);
  static int read_tmp_file(const uint64_t tenant_id, const MacroBlockId &macro_id, const int64_t offset, 
    const int64_t size, char *read_buf = nullptr);
  // for private_data_macro or shared_major_data_macro
  static int write_data_macro(ObTenantFileManager *file_mgr, const uint64_t tenant_id, const MacroBlockId &macro_id,
    const int64_t size, char *write_buf = nullptr);
  static int read_micro_block(ObTenantFileManager *file_mgr, const uint64_t tenant_id, const MacroBlockId &macro_id,
    const int64_t offset, const int64_t size, char *read_buf = nullptr, const bool check_data = false);
};

MacroBlockId TestSSLocalCacheUtil::gen_macro_block_id(
    const ObStorageObjectType obj_type,
    const int64_t second_id,
    const int64_t third_id,
    const int64_t fourth_id)
{
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)obj_type);
  macro_id.set_second_id(second_id);
  macro_id.set_third_id(third_id);
  macro_id.set_fourth_id(fourth_id);
  return macro_id;
}

int TestSSLocalCacheUtil::gen_random_data(char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObRandom rand;
  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t end = size - 1;
    for (int64_t i = 0; i < end; ++i) {
      buf[i] = static_cast<char>(ObRandom::rand(0, 128));
    }
    buf[end] = '\0';
  }
  return ret;
}

int TestSSLocalCacheUtil::gen_write_info(
    ObStorageObjectWriteInfo &write_info,
    const uint64_t tenant_id,
    char *write_buf,
    const int64_t offset,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(offset < 0 || size <= 0 || !is_valid_tenant_id(tenant_id)) || OB_ISNULL(write_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(offset), K(size), K(tenant_id), KP(write_buf));
  } else {
    write_info.io_desc_.set_wait_event(1);
    write_info.buffer_ = write_buf;
    write_info.offset_ = offset;
    write_info.size_ = size;
    write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info.mtl_tenant_id_ = tenant_id;
  }
  return ret;
}

int TestSSLocalCacheUtil::gen_read_info(
    ObStorageObjectReadInfo &read_info,
    const MacroBlockId &macro_id,
    const uint64_t tenant_id,
    char *read_buf,
    const int64_t offset,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(offset < 0 || size <= 0 || !is_valid_tenant_id(tenant_id)) || OB_ISNULL(read_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(offset), K(size), K(tenant_id), KP(read_buf));
  } else {
    read_buf[0] = '\0';
    read_info.macro_block_id_ = macro_id;
    read_info.io_desc_.set_wait_event(1);
    read_info.buf_ = read_buf;
    read_info.offset_ = offset;
    read_info.size_ = size;
    read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    read_info.mtl_tenant_id_ = tenant_id;
  }
  return ret;
}

int TestSSLocalCacheUtil::write_private_tablet_meta(
    ObTenantFileManager *file_mgr,
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const int64_t size,
    const int64_t ls_epoch_id,
    char *write_buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_mgr) || OB_UNLIKELY(!macro_id.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(size), KP(file_mgr));
  } else {
    ObStorageObjectWriteInfo write_info;
    ObStorageObjectHandle write_object_handle;
    ObArenaAllocator allocator;
    if (nullptr == write_buf) {
      if (nullptr ==(write_buf = static_cast<char*>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(size));
      }
    }

    if (OB_SUCC(ret)) {
      char c = 'a' + (macro_id.hash() % 26);
      MEMSET(write_buf, c, size);
      if (OB_FAIL(gen_write_info(write_info, tenant_id, write_buf, 0, size))) {
        LOG_WARN("fail to gen write info", KR(ret), K(tenant_id), K(size), KP(write_buf));
      } else if (OB_FAIL(write_object_handle.set_macro_block_id(macro_id))) {
        LOG_WARN("fail to set macro_block id", KR(ret), K(macro_id));
      } else if (FALSE_IT(write_info.ls_epoch_id_ = ls_epoch_id)) {
      } else if (OB_FAIL(file_mgr->write_file(write_info, write_object_handle))) {
        LOG_WARN("fail to write file", KR(ret), K(write_info));
      }
    }
  }
  return ret;
}

int TestSSLocalCacheUtil::write_tmp_file(
    const uint64_t tenant_id, 
    const MacroBlockId &macro_id, 
    const int64_t offset,
    const int64_t size,
    const bool is_sealed,
    char *write_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || offset < 0 || size <= 0 || OB_ISNULL(write_buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(offset), K(size), KP(write_buf));
  } else {
    ObStorageObjectWriteInfo write_info;
    ObStorageObjectHandle write_object_handle;

    const char c = 'a' + (macro_id.hash() % 26);
    MEMSET(write_buf, c, size);
    if (OB_FAIL(gen_write_info(write_info, tenant_id, write_buf, offset, size))) {
      LOG_WARN("fail to gen write info", KR(ret), K(tenant_id), K(size), KP(write_buf));
    } else {
      if (is_sealed) {
        write_info.io_desc_.set_sealed();
      } else {
        write_info.io_desc_.set_unsealed();
      }

      write_info.tmp_file_valid_length_ = offset + size;
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(macro_id, write_info, write_object_handle))) {
        LOG_WARN("fail to async write object", KR(ret), K(macro_id), K(write_info));
      } else if (OB_FAIL(write_object_handle.wait())) {
        LOG_WARN("fail to wait io finish", KR(ret), K(macro_id), K(write_info));
      } else if (is_sealed && OB_FAIL(OB_STORAGE_OBJECT_MGR.seal_object(macro_id, 0 /*ls_epoch_id*/))) {
        LOG_WARN("fail to seal object", KR(ret), K(macro_id), K(write_info));
      }
    }
  }
  return ret;
}

int TestSSLocalCacheUtil::read_tmp_file(
    const uint64_t tenant_id, 
    const MacroBlockId &macro_id, 
    const int64_t offset, 
    const int64_t size, 
    char *read_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || size <= 0 || OB_ISNULL(read_buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(size), KP(read_buf));
  } else {
    ObStorageObjectReadInfo read_info;
    ObStorageObjectHandle read_object_handle;
    char c = 'a' + (macro_id.hash() % 26);

    if (OB_FAIL(gen_read_info(read_info, macro_id, tenant_id, read_buf, offset, size))) {
      LOG_WARN("fail to gen read info", KR(ret), K(macro_id), K(tenant_id), K(size), K(offset), KP(read_buf));
    } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_read_object(read_info, read_object_handle))) {
      LOG_WARN("fail to async read tmpfile", KR(ret), K(macro_id), K(read_info));
    } else if (OB_FAIL(read_object_handle.wait())) {
      LOG_WARN("fail to wait io finish", KR(ret), K(macro_id), K(read_info));
    } else if (OB_ISNULL(read_object_handle.get_buffer())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read data is null", KR(ret), K(macro_id), K(size), K(offset));
    } else if (OB_UNLIKELY(read_object_handle.get_data_size() != size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read size is wrong", KR(ret), K(macro_id), K(offset), K(size), K(read_object_handle.get_data_size()));
    } else {
      const char *data_buf = read_object_handle.get_buffer();
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        if (data_buf[i] != c) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data is wrong", KR(ret), K(macro_id), K(offset), K(size), K(i), K(data_buf[i]), K(c));
        }
      }
    }
  }
  return ret;
}

int TestSSLocalCacheUtil::write_data_macro(
    ObTenantFileManager *file_mgr, 
    const uint64_t tenant_id, 
    const MacroBlockId &macro_id,
    const int64_t size, 
    char *write_buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_mgr) || OB_UNLIKELY(!macro_id.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(size), KP(file_mgr));
  } else {
    ObStorageObjectWriteInfo write_info;
    ObStorageObjectHandle write_object_handle;
    ObArenaAllocator allocator;
    if (nullptr == write_buf) {
      if (nullptr ==(write_buf = static_cast<char*>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(size));
      }
    }

    if (OB_SUCC(ret)) {
      char c = 'a' + (macro_id.hash() % 26);
      MEMSET(write_buf, c, size);
      if (OB_FAIL(gen_write_info(write_info, tenant_id, write_buf, 0, size))) {
        LOG_WARN("fail to gen write info", KR(ret), K(tenant_id), K(size), KP(write_buf));
      } else if (OB_FAIL(write_object_handle.set_macro_block_id(macro_id))) {
        LOG_WARN("fail to set macro_block id", KR(ret), K(macro_id));
      } else if (OB_FAIL(file_mgr->write_file(write_info, write_object_handle))) {
        LOG_WARN("fail to write file", KR(ret), K(write_info));
      }
    }
  }
  return ret;
}

int TestSSLocalCacheUtil::read_micro_block(
    ObTenantFileManager *file_mgr, 
    const uint64_t tenant_id, 
    const MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size, 
    char *read_buf,
    const bool check_data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_mgr) || OB_UNLIKELY(!macro_id.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(size), KP(file_mgr));
  } else {
    ObStorageObjectReadInfo read_info;
    ObStorageObjectHandle read_object_handle;
    ObArenaAllocator allocator;
    if (nullptr == read_buf) {
      if (nullptr ==(read_buf = static_cast<char*>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(size));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(gen_read_info(read_info, macro_id, tenant_id, read_buf, offset, size))) {
        LOG_WARN("fail to gen read info", KR(ret), K(macro_id), K(tenant_id), K(size), K(offset), KP(read_buf));
      } else if (OB_FAIL(read_object_handle.set_macro_block_id(macro_id))) {
        LOG_WARN("fail to set macro_block id", KR(ret), K(macro_id));
      } else if (OB_FAIL(file_mgr->pread_file(read_info, read_object_handle))) {
        LOG_WARN("fail to pread file", KR(ret), K(read_info));
      } else if (OB_ISNULL(read_object_handle.get_buffer())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read data is null", KR(ret), K(macro_id), K(size), K(offset));
      } else if (check_data) {
        char c = 'a' + (macro_id.hash() % 26);
        const char *read_data = read_object_handle.get_buffer();
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          if (read_data[i] != c) {
            ret = OB_IO_ERROR;
            LOG_WARN("read data mismatch", KR(ret), K(i), K(c), K(read_data));
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
