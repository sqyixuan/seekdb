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
#include "share/compaction/ob_compaction_obj_interface.h"
#include "storage/shared_storage/ob_file_manager.h"
namespace oceanbase
{
using namespace blocksstable;
namespace compaction
{
/**
 * -------------------------------------------------------------------ObCompactionObjInterface-------------------------------------------------------------------
 */
// will update last_refresh_ts_, func can not be const
int ObCompactionObjInterface::write_object(ObCompactionObjBuffer &buf)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  int64_t pos = 0;
  (void) set_obj_opt(opt);

  if (OB_UNLIKELY(!buf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf", KR(ret), K(buf));
  } else {
    const int64_t obj_size = get_serialize_size();
    if (obj_size > buf.get_buf_len() && OB_FAIL(buf.ensure_space(obj_size))) {
      LOG_WARN("failed to ensure space for buf", K(ret), K(obj_size), "buf_len", buf.get_buf_len());
    }
  }

  if (FAILEDx(serialize(buf.get_buf(), buf.get_buf_len(), pos))) {
    LOG_WARN("failed to serialize", KR(ret), K(opt));
  } else {
    ObStorageObjectWriteInfo write_info;
    ObStorageObjectHandle handle;
    write_info.buffer_ = buf.get_buf();
    write_info.size_ = pos;
    write_info.offset_ = 0;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_sealed();
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.mtl_tenant_id_ = MTL_ID();
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.write_object(opt, write_info, handle))) {
      LOG_WARN("failed to write obj", KR(ret), K(opt), K(write_info));
    } else {
      last_refresh_ts_ = ObTimeUtility::fast_current_time();
    }
  }
  return ret;
}

int ObCompactionObjInterface::read_object(ObCompactionObjBuffer &buf)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  MacroBlockId block_id;
  (void) set_obj_opt(opt);
  int64_t pos = 0;

  if (OB_UNLIKELY(!buf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf", KR(ret), K(buf));
  } else if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, block_id))) {
    LOG_WARN("failed to generate obj id", KR(ret), K(opt));
  } else if (OB_FAIL(read_object_in_buffer(block_id, buf))) {
    if (OB_OBJECT_NOT_EXIST != ret) {
      LOG_WARN("failed to read obj", KR(ret), K(block_id));
    }
  } else if (OB_FAIL(deserialize(buf.get_buf(), buf.get_buf_len(), pos))) {
    LOG_WARN("failed to deserialize", KR(ret), K(opt));
  } else {
    last_refresh_ts_ = ObTimeUtility::fast_current_time();
  }
  return ret;
}

int ObCompactionObjInterface::read_object_in_buffer(
  const MacroBlockId &block_id,
  ObCompactionObjBuffer &buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!buf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf", KR(ret), K(buf));
  } else {
    ObStorageObjectReadInfo read_info;
    ObStorageObjectHandle handle;
    read_info.offset_ = 0;
    read_info.buf_ = buf.get_buf();
    read_info.size_ = buf.get_buf_len();
    read_info.io_timeout_ms_ = 10_s;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    read_info.macro_block_id_ = block_id;
    read_info.mtl_tenant_id_ = MTL_ID();
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.read_object(read_info, handle))) {
      if (OB_OBJECT_NOT_EXIST != ret) {
        LOG_WARN("failed to read obj", KR(ret), K(read_info));
      }
    }
  }
  return ret;
}

int ObCompactionObjInterface::check_exist(bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  MacroBlockId block_id;
  (void) set_obj_opt(opt);
  if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, block_id))) {
    LOG_WARN("failed to generate obj id", KR(ret), K(opt));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id, 0/*ls_epoch*/, is_exist))) {
    LOG_WARN("failed to check exist", KR(ret), K(block_id), K(opt));
  }
  return ret;
}

int ObCompactionObjInterface::try_reload_obj(const bool alloc_big_buf)
{
  int ret = OB_SUCCESS;
  ObCompactionObjBuffer obj_buf;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("compaction obj has been inited, cannot reload", K(ret));
  } else if (OB_FAIL(obj_buf.init(alloc_big_buf))) {
    LOG_WARN("failed to init obj buf", K(ret));
  } else if (OB_FAIL(read_object(obj_buf))) {
    if (OB_OBJECT_NOT_EXIST != ret) {
      LOG_WARN("failed to read object", K(ret));
    } else {
      ret = OB_SUCCESS;
      is_reloaded_ = false;
    }
  } else {
    is_reloaded_ = true;
  }
  return ret;
}

int ObCompactionObjInterface::delete_object()
{
  int ret = OB_SUCCESS;
  MacroBlockId block_id;
  ObStorageObjectOpt opt;
  (void) set_obj_opt(opt);
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, block_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(MTL(storage::ObTenantFileManager*)->delete_file(block_id, 0/*ls_epoch*/))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(block_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(block_id), KPC(this));
    }
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObCompactionObjBuffer-------------------------------------------------------------------
 */
const int64_t ObCompactionObjBuffer::INITIAL_BUF_SIZE;
const int64_t ObCompactionObjBuffer::ALIGNMENT;
ObCompactionObjBuffer::ObCompactionObjBuffer()
  : buf_(NULL),
    buf_len_(0),
    allocator_("CompObjBuf", MTL_ID())
{}

void ObCompactionObjBuffer::destroy()
{
  free();
}

void ObCompactionObjBuffer::free()
{
  if (OB_NOT_NULL(buf_)) {
    allocator_.free(buf_);
    buf_ = NULL;
    buf_len_ = 0;
  }
}

int64_t ObCompactionObjBuffer::get_aligned_size(const int64_t size) const
{
  int64_t aligned_size = (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
  return aligned_size;
}

int ObCompactionObjBuffer::init(const bool alloc_big_buf)
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = alloc_big_buf
                         ? ObCompactionObjBuffer::BIG_BUF_SIZE
                         : ObCompactionObjBuffer::INITIAL_BUF_SIZE;
  const int64_t aligned_size = get_aligned_size(buf_size);

  if (OB_NOT_NULL(buf_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP_(buf));
  } else if (OB_UNLIKELY(aligned_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(buf_size), K(aligned_size));
  } else if (OB_ISNULL(buf_ = (char *)allocator_.alloc(aligned_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", KR(ret), K(INITIAL_BUF_SIZE));
  } else {
    buf_len_ = aligned_size;
  }
  return ret;
}

int ObCompactionObjBuffer::ensure_space(const int64_t size)
{
  int ret = OB_SUCCESS;
  // new size should be aligned to 8KB
  const int64_t aligned_size = get_aligned_size(size);
  char *new_data = nullptr;

  if (OB_UNLIKELY(aligned_size <= buf_len_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("obj buffer doesn't support shrinking", K(ret), K(size), K(aligned_size), K(buf_len_));
  } else if (OB_ISNULL(new_data = (char *)allocator_.alloc(aligned_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", KR(ret), K(aligned_size));
  } else {
    MEMCPY(new_data, buf_, buf_len_);
    free();
    buf_len_ = aligned_size;
    buf_ = new_data;
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
