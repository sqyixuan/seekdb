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

#include "storage/shared_storage/ob_segment_file_manager.h"
#include "share/ob_io_device_helper.h"
#include "share/io/ob_io_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_tmp_file_io_callback.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

/**
 * --------------------------------TmpFileMeta------------------------------------
 */
TmpFileMeta::TmpFileMeta()
  : lock_(), is_in_local_(true), valid_length_(0), target_length_(0),
    ref_cnt_(0), append_timestamp_us_(OB_INVALID_TIMESTAMP)
{
}

TmpFileMeta::TmpFileMeta(const bool is_in_local, const int32_t valid_length, const int32_t target_length)
    : lock_(), is_in_local_(is_in_local), valid_length_(valid_length), target_length_(target_length),
      ref_cnt_(0), append_timestamp_us_(ObTimeUtility::fast_current_time())
{
}

void TmpFileMeta::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void TmpFileMeta::dec_ref_count()
{
  int ret = OB_SUCCESS;
  const int64_t ref_cnt = ATOMIC_SAF(&ref_cnt_, 1);
  if (0 == ref_cnt) {
    reset();
    TmpFileMeta *meta_ptr = this;
    OB_DELETE(TmpFileMeta, attr, meta_ptr);
  } else if (ref_cnt < 0) {
    LOG_ERROR("tmp file meta's ref cnt cannot be less than 0", KPC(this));
  }
}

int64_t TmpFileMeta::get_ref() const
{
  return ATOMIC_LOAD(&ref_cnt_);
}

void TmpFileMeta::set_target_length(const int32_t target_length)
{
  target_length_ = target_length;
}

bool TmpFileMeta::is_tmp_file_appending(const int64_t file_len)
{
  // unsealed seg file's target_length is MAX_UNREACHABLE_LENTH or file_length is not equal target_length, means seg file has been appended or seg file is appending
  return ((ObSegmentFileManager::MAX_UNREACHABLE_LENTH == target_length_) || (file_len != target_length_));
}

void TmpFileMeta::reset()
{
  int ret = OB_SUCCESS;
  if (ref_cnt_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ref count > 0 when reset", KR(ret), KPC(this));
  }
  is_in_local_ = true;
  valid_length_ = 0;
  target_length_ = 0;
  ref_cnt_ = 0;
  append_timestamp_us_ = OB_INVALID_TIMESTAMP;
}

bool TmpFileMeta::is_valid() const
{
  return ((valid_length_ > 0) && (OB_INVALID_TIMESTAMP != append_timestamp_us_));
}

/**
 * --------------------------------TmpFileMetaHandle------------------------------------
 */
TmpFileMetaHandle::TmpFileMetaHandle() : ObPtrHandle<TmpFileMeta>()
{
}

TmpFileMetaHandle::~TmpFileMetaHandle()
{
  reset();
}

void TmpFileMetaHandle::reset()
{
  ObPtrHandle<TmpFileMeta>::reset();
}

int TmpFileMetaHandle::set_tmpfile_meta(const bool is_in_local, const int32_t valid_length, const int32_t target_length)
{
  int ret = OB_SUCCESS;
  reset();
  ObMemAttr attr(MTL_ID(), "TmpFileMeta");
  // because deleting tmp file is async, when the sql is completed, tmp file has not been deleted, and the farm will report memory leak
  SET_IGNORE_MEM_VERSION(attr);
  TmpFileMeta *meta_ptr = nullptr;
  if (OB_UNLIKELY((valid_length < 0) || (target_length < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(valid_length), K(target_length));
  } else if (OB_ISNULL(meta_ptr = OB_NEW(TmpFileMeta, attr, is_in_local, valid_length, target_length))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    set_ptr(meta_ptr);
  }
  return ret;
}

int TmpFileMetaHandle::assign(const TmpFileMetaHandle &other)
{
  return ObPtrHandle<TmpFileMeta>::assign(other);
}

// Note: already wlock by caller, cannot wlock again here
void TmpFileMetaHandle::update_tmpfile_meta(const bool is_in_local, const int64_t valid_length)
{
  if (OB_NOT_NULL(ptr_)) {
    ptr_->is_in_local_ = is_in_local;
    ptr_->valid_length_ = valid_length;
    ptr_->append_timestamp_us_ = ObTimeUtility::fast_current_time();
  }
}

void TmpFileMetaHandle::set_target_length(const int32_t target_length)
{
  if (OB_NOT_NULL(ptr_)) {
    ptr_->set_target_length(target_length);
  }
}

ObSegmentFileManager::GetWildTmpFileSegIdFunc::GetWildTmpFileSegIdFunc(const int64_t tmp_file_id)
  : tmp_file_id_(tmp_file_id), wild_seg_ids_()
{
  wild_seg_ids_.set_attr(ObMemAttr(MTL_ID(), "GetWildTmpF"));
}

int ObSegmentFileManager::GetWildTmpFileSegIdFunc::operator()(
    const HashMapPair<TmpFileSegId, TmpFileMetaHandle> &entry)
{
  int ret = OB_SUCCESS;
  const TmpFileSegId &cur_seg_id = entry.first;
  if (cur_seg_id.tmp_file_id_ == tmp_file_id_) {
    if (OB_FAIL(wild_seg_ids_.push_back(cur_seg_id))) {
      LOG_WARN("fail to push back", KR(ret), K(cur_seg_id));
    }
  }
  return OB_SUCCESS;
}

/**
 * --------------------------------ObSSTmpFileAppendParam------------------------------------
 */
ObSSTmpFileAppendParam::ObSSTmpFileAppendParam()
  : arena_allocator_(), need_write_io_(true), need_free_file_size_(true), free_file_size_(0)
{
  arena_allocator_.set_attr(ObMemAttr(MTL_ID(), "TmpFileWrite"));
}

ObSSTmpFileAppendParam::ObSSTmpFileAppendParam(
    const bool need_write_io,
    const bool need_free_file_size,
    const int64_t free_file_size)
  : arena_allocator_(), need_write_io_(need_write_io), need_free_file_size_(need_free_file_size),
    free_file_size_(free_file_size)
{
  arena_allocator_.set_attr(ObMemAttr(MTL_ID(), "TmpFileWrite"));
}


/**
 * --------------------------------ObSegmentFileManager------------------------------------
 */
ObSegmentFileManager::ObSegmentFileManager()
  : is_inited_(false), is_stop_(false), file_manager_(nullptr), allocator_(),
    seg_meta_map_(), gc_segment_file_task_(), unsealed_seg_file_remove_queue_()
{
}

ObSegmentFileManager::~ObSegmentFileManager() { destroy(); }

int ObSegmentFileManager::init(ObTenantFileManager *file_manager)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), "SegMetaMap");
  const int64_t MEM_LIMIT = 1 * 1024 * 1024 * 1024LL; // 1GB
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("segment file manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_manager));
  } else if (OB_FAIL(seg_meta_map_.create(ObBaseFileManager::OB_DEFAULT_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create seg meta map", KR(ret));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, "SegFileMGR", MTL_ID(), MEM_LIMIT))) {
    LOG_WARN("fail to init allocator", KR(ret), K(MEM_LIMIT));
  } else if (OB_FAIL(gc_segment_file_task_.init(this))) {
    LOG_WARN("fail to init gc unsealed tmpfile task", KR(ret));
  } else {
    file_manager_ = file_manager;
    is_inited_ = true;
    LOG_INFO("succ to init segment file manager");
  }
  return ret;
}

int ObSegmentFileManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(gc_segment_file_task_.start())) {
    LOG_WARN("fail to start gc unsealed tmpfile task", KR(ret));
  } else {
    LOG_INFO("succ to start segment file manager");
  }
  return ret;
}

void ObSegmentFileManager::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  file_manager_ = nullptr;
  gc_segment_file_task_.destroy();
  const int64_t queue_size = unsealed_seg_file_remove_queue_.size();
  for (int64_t i = 0; OB_SUCC(ret) && (i < queue_size); ++i) {
    ObLink *ptr = nullptr;
    ObUnsealedSegFile *seg_file = nullptr;
    if (OB_FAIL(unsealed_seg_file_remove_queue_.pop(ptr))) {
      LOG_WARN("fail to pop remove task", KR(ret), K(i), K(queue_size));
    } else {
      seg_file = static_cast<ObUnsealedSegFile *>(ptr);
      OB_DELETEx(ObUnsealedSegFile, &allocator_, seg_file);
    }
  }
  allocator_.destroy();
  seg_meta_map_.destroy();
}

void ObSegmentFileManager::stop()
{
  is_stop_ = true;
  gc_segment_file_task_.stop();
}

void ObSegmentFileManager::wait()
{
  gc_segment_file_task_.wait();
}

int ObSegmentFileManager::async_append_file(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle ori_object_handle(object_handle);  // when aio write failed, object_handle will be reset
  ObStorageObjectWriteInfo new_write_info(write_info);
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager is not inited", KR(ret));
  } else if (OB_UNLIKELY((ObStorageObjectType::TMP_FILE != object_type) ||
                         !new_write_info.is_valid() || !file_id.is_valid() ||
                         new_write_info.io_desc_.is_write_through() ||
                         ((new_write_info.size_ % tmp_file::ObTmpFileGlobal::ALLOC_PAGE_SIZE) != 0) || // tmp_file is 8KB aligned
                         (new_write_info.size_ > OB_DEFAULT_MACRO_BLOCK_SIZE))) { // tmp_file segment <= 2MB
    // disable user layer to set write_through/back, only io layer can determine write_through/back
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type), K(new_write_info), K(object_handle));
  } else {
    ObSSTmpFileAppendParam append_param(true/*need_write_io*/, true/*need_free_file_size*/, new_write_info.size_/*free_file_size*/);
    if (OB_FAIL(set_tmp_file_write_through_if_need(file_id, new_write_info))) {
      LOG_WARN("fail to set tmp file write through", KR(ret),  K(new_write_info),
               K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
    } else if (new_write_info.io_desc_.is_write_through()) { // case 1: is_write_through = true
      append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
      if (append_param.need_write_io_ &&
          OB_FAIL(file_manager_->aio_write_with_create_parent_dir(new_write_info, ori_object_handle,
                  append_param.need_free_file_size_, append_param.free_file_size_, object_handle))) {
        LOG_WARN("fail to aio write with create parent dir", KR(ret), K(new_write_info), K(object_handle), K(append_param));
      }
    } else { // case 2: is_write_through = false
      bool is_meta_exist = false;
      const TmpFileSegId seg_id(file_id.second_id(), file_id.third_id());
      TmpFileMetaHandle meta_handle;
      if (OB_FAIL(try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
        LOG_WARN("fail to try get seg meta", KR(ret), K(seg_id));
      } else if (is_meta_exist) { // case 2.1: is_meta_exist = true
        if (OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("meta handle is invalid", KR(ret), K(seg_id), K(meta_handle));
        } else {
          SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
          if (OB_FAIL(handle_write_on_meta_exist(meta_handle, new_write_info, object_handle, append_param))) {
            LOG_WARN("fail to handle write on meta exist", KR(ret), K(meta_handle), K(new_write_info), K(object_handle));
          } else if (append_param.need_write_io_ &&
                     OB_FAIL(file_manager_->aio_write_with_create_parent_dir(new_write_info, ori_object_handle,
                             append_param.need_free_file_size_, append_param.free_file_size_, object_handle))) {
            LOG_WARN("fail to aio write with create parent dir", KR(ret), K(new_write_info), K(object_handle), K(append_param));
          }
        }
      } else { // case 2.2: is_meta_exist = false
        if (OB_FAIL(handle_write_on_meta_not_exist(new_write_info, object_handle, append_param))) {
          LOG_WARN("fail to handle write on meta not exist", KR(ret), K(new_write_info), K(object_handle));
        } else if (append_param.need_write_io_ &&
                   OB_FAIL(file_manager_->aio_write_with_create_parent_dir(new_write_info, ori_object_handle,
                           append_param.need_free_file_size_, append_param.free_file_size_, object_handle))) {
          LOG_WARN("fail to aio write with create parent dir", KR(ret), K(new_write_info), K(object_handle), K(append_param));
        }
      }
    }
    // IOManger::aio_write failed or failed before IOManger::aio_write, free memory of ObSSTmpFileIOCallback
    if (OB_FAIL(ret) && OB_NOT_NULL(new_write_info.io_callback_) &&
        (ObIOCallbackType::SS_TMP_FILE_CALLBACK == new_write_info.io_callback_->get_type())) {
      free_io_callback<ObSSTmpFileIOCallback>(new_write_info.io_callback_);
    }
  }
  return ret;
}

int ObSegmentFileManager::async_pread_file(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObSegmentFileManager::insert_meta(const TmpFileSegId &seg_id, const TmpFileMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle));
  } else if (OB_FAIL(seg_meta_map_.set_refactored(seg_id, meta_handle, false/*overwrite*/))) {
    LOG_WARN("fail to set refactored seg meta", KR(ret), K(seg_id), K(meta_handle));
  }
  return ret;
}

int ObSegmentFileManager::delete_meta(const TmpFileSegId &seg_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(seg_meta_map_.erase_refactored(seg_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret, maybe meta is not exist or meta has already been deleted
    } else {
      LOG_WARN("fail to erase refactored seg meta", KR(ret), K(seg_id));
    }
  }
  return ret;
}

// This function is specially designed for ObTmpFileAsyncRemoveTask::exec_remove() Scan GC.
// It deletes metas of the tmp file that has already been GC.
int ObSegmentFileManager::delete_wild_meta(const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  GetWildTmpFileSegIdFunc func(tmp_file_id);
  if (OB_FAIL(seg_meta_map_.foreach_refactored(func))) {
    LOG_WARN("fail to foreach refactored", KR(ret), K(func));
  } else {
    const int64_t wild_seg_id_cnt = func.wild_seg_ids_.count();
    for (int64_t i = 0; (i < wild_seg_id_cnt) && OB_SUCC(ret); ++i) {
      const TmpFileSegId &cur_seg_id = func.wild_seg_ids_.at(i);
      if (OB_FAIL(seg_meta_map_.erase_refactored(cur_seg_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // ignore ret
        } else {
          LOG_WARN("fail to erase refactored seg meta", KR(ret), K(cur_seg_id));
        }
      }
    }
    if (wild_seg_id_cnt > 0) {
      LOG_INFO("finish to delete wild seg metas", KR(ret), K(wild_seg_id_cnt), "wild_seg_ids",
               func.wild_seg_ids_);
    }
  }
  return ret;
}

int ObSegmentFileManager::update_meta(const TmpFileSegId &seg_id, const TmpFileMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle));
  } else {
    ObSegMetaUpdateOp update_callback(meta_handle);
    if (OB_FAIL(seg_meta_map_.atomic_refactored(seg_id, update_callback))) {
      LOG_WARN("fail to update seg meta map", KR(ret));
    }
  }
  return ret;
}

int ObSegmentFileManager::try_get_seg_meta(
    const TmpFileSegId &seg_id,
    TmpFileMetaHandle &meta_handle,
    bool &is_meta_exist)
{
  int ret = OB_SUCCESS;
  is_meta_exist = false;
  meta_handle.reset();
  if (OB_FAIL(seg_meta_map_.get_refactored(seg_id, meta_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get refactored", KR(ret), K(seg_id));
    }
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta handle is invalid", KR(ret), K(meta_handle));
  } else {
    is_meta_exist = true;
  }
  return ret;
}

int ObSegmentFileManager::push_seg_file_to_remove_queue(const TmpFileSegId &seg_id, const int64_t valid_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager has not been inited", KR(ret), K_(is_inited));
  } else {
    ObUnsealedSegFile *seg_file = nullptr;
    if (OB_ISNULL(seg_file = OB_NEWx(ObUnsealedSegFile, &allocator_, seg_id, valid_length))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate ObUnsealedSegFile", KR(ret));
    } else if (OB_FAIL(unsealed_seg_file_remove_queue_.push(seg_file))) {
      LOG_WARN("fail to push remove task", KR(ret), K(seg_id), K(valid_length));
    }
    // if push seg file to remove queue failed, the seg_file will be deleted by SSTmpFileARemove thread finally
    if (OB_FAIL(ret)) {
      OB_DELETEx(ObUnsealedSegFile, &allocator_, seg_file);
    }
  }
  return ret;
}

int ObSegmentFileManager::exec_remove_task_once()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager has not been inited", KR(ret), K_(is_inited));
  }
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  ObSEArray<MacroBlockId, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> file_ids;
  const int64_t queue_size = unsealed_seg_file_remove_queue_.size();
  for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && (i < queue_size); ++i) {
    ObUnsealedSegFile *seg_file = nullptr;
    ObLink *ptr = nullptr;
    if (OB_FAIL(unsealed_seg_file_remove_queue_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;  // ignore ret
        break;
      } else {
        LOG_WARN("fail to pop seg file remove task", KR(ret));
      }
    } else if (OB_ISNULL(ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr is null", KR(ret), KP(ptr));
    } else {
      seg_file = static_cast<ObUnsealedSegFile *>(ptr);
      MacroBlockId file_id;
      file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      file_id.set_storage_object_type((uint64_t)ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE);
      file_id.set_second_id(seg_file->seg_id_.tmp_file_id_);
      file_id.set_third_id(seg_file->seg_id_.segment_id_);
      file_id.set_fourth_id(seg_file->valid_length_);
      if (OB_FAIL(file_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
    // if failed, push seg_file to remove_queue again
    if (OB_FAIL(ret) && OB_NOT_NULL(seg_file)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(push_seg_file_to_remove_queue(seg_file->seg_id_, seg_file->valid_length_))) {
        LOG_WARN("fail to push seg file to remove queue", KR(tmp_ret), KPC(seg_file));
      }
    }
    // free memory
    OB_DELETEx(ObUnsealedSegFile, &allocator_, seg_file);
  }
  if (OB_FAIL(ret)) {
  } else if (file_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(file_manager_->delete_remote_files(file_ids))) {
    LOG_WARN("fail to delete remote files", KR(ret));
  } else {
    LOG_INFO("succ to execute remove task once", K(file_ids), "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
  }
  return ret;
}

int ObSegmentFileManager::find_unsealed_tmp_file_to_flush(ObIArray<TmpFileSegId> &seg_files)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("seg file mgr is not inited", KR(ret));
  } else {
    HeapCompare heap_cmp;
    ObBinaryHeap<HeapItem, HeapCompare> seg_file_heap(heap_cmp);
    int64_t total_flush_size = 0;
    SegMetaMap::bucket_iterator bucket_iter = seg_meta_map_.bucket_begin();
    while ((bucket_iter != seg_meta_map_.bucket_end()) && OB_SUCC(ret)) {
      // bucket read lock
      SegMetaMap::hashtable::bucket_lock_cond blk(*bucket_iter);
      SegMetaMap::hashtable::readlocker locker(blk.lock());
      SegMetaMap::hashtable::hashbucket::iterator node_iter = bucket_iter->node_begin();
      while (OB_SUCC(ret) && (node_iter != bucket_iter->node_end())) {
        TmpFileMetaHandle &meta_handle = node_iter->second;
        if (OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("meta handle should not be invalid", KR(ret), K(meta_handle));
        } else if (OB_UNLIKELY(!meta_handle.get_tmpfile_meta()->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tmp file meta should not be invalid", KR(ret));
        // when unsealed seg file is in local and append_time_stamp exceeds 60s, need flush to object storage
        } else if ((true == meta_handle.is_in_local()) &&
                   (meta_handle.get_target_length() < OB_DEFAULT_MACRO_BLOCK_SIZE) &&
                   ((ObTimeUtility::fast_current_time() - meta_handle.get_append_timestamp_us()) > UNSEALED_TMP_FILE_FLUSH_THRESHOLD)) {
          HeapItem heap_item((node_iter->first).tmp_file_id_, (node_iter->first).segment_id_, meta_handle.get_append_timestamp_us(), meta_handle.get_target_length());
          // use max_heap(largest append timestamp is latest segment file) to evict the oldest unsealed segment file, keep unsealed tmp file flush size reach the low water mark tmp_file write cache size(4%)
          if (OB_FAIL(push_item_to_heap(heap_item, seg_file_heap, total_flush_size))) {
            LOG_WARN("fail to push item to heap", KR(ret), K(heap_item));
          }
        }
        node_iter++;
      }
      ++bucket_iter;
    }
    for (int64_t i = 0; i < seg_file_heap.count() && OB_SUCC(ret); ++i) {
      TmpFileSegId seg_id(seg_file_heap.at(i).tmp_file_id_, seg_file_heap.at(i).segment_id_);
      if (OB_FAIL(seg_files.push_back(seg_id))) {
        LOG_WARN("fail to push back", KR(ret), K(seg_id));
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::set_tmp_file_write_through_if_need(
    const MacroBlockId &file_id,
    ObStorageObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  const ObStorageObjectType object_type = file_id.storage_object_type();
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(write_info));
  } else if ((ObStorageObjectType::TMP_FILE == object_type) &&
             (OB_DEFAULT_MACRO_BLOCK_SIZE == write_info.size_) &&
             (write_info.io_desc_.is_sealed())) {
    write_info.io_desc_.set_write_through(true);
    // because tmp file may be overwritten, local file need to be deleted before write through
    bool is_exist = false;
    if (OB_ISNULL(file_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret));
    } else if (OB_FAIL(file_manager_->is_exist_local_file(file_id, 0/*ls_epoch_id*/, is_exist))) {
      LOG_WARN("fail to judge local file exist", KR(ret), K(file_id));
    } else if (is_exist) {
      if (OB_FAIL(file_manager_->delete_local_file(file_id))) {
        LOG_WARN("fail to delete file", KR(ret), K(file_id));
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::handle_write_on_meta_exist(
    TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle));
  } else if (write_info.tmp_file_valid_length_ <= meta_handle.get_valid_length()) { // case 1: no need write io
    append_param.need_write_io_ = false;
    if (OB_FAIL(simulate_io_result(write_info, object_handle))) {
      LOG_WARN("fail to simulate io result", KR(ret), K(write_info), K(object_handle));
    } else {
      LOG_INFO("current io's valid length is smaller than or equal to seg meta's valid length, "
               "no need write io", K(write_info), K(meta_handle));
    }
  } else { // case 2: write_info.tmp_file_valid_length_ > seg_meta.valid_length_, need write io
    if (meta_handle.is_in_local()) { // case 2.1: current segment is in local
      if (OB_FAIL(handle_write_with_local_seg(meta_handle, write_info, object_handle, append_param))) {
        LOG_WARN("fail to handle write with local seg", KR(ret), K(meta_handle), K(write_info), K(object_handle));
      }
    } else { // case 2.2: current segment is in remote
      if (OB_FAIL(handle_write_with_remote_seg(meta_handle, write_info, object_handle, append_param))) {
        LOG_WARN("fail to handle write with remote seg", KR(ret), K(meta_handle), K(write_info), K(object_handle));
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::handle_write_on_meta_not_exist(
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const ObStorageObjectType object_type = file_id.storage_object_type();
  const TmpFileSegId seg_id(file_id.second_id(), file_id.third_id());
  const int64_t need_alloc_size = write_info.offset_ + write_info.size_;
  const int32_t target_length = write_info.offset_ + write_info.size_;
  append_param.need_free_file_size_ = true;
  append_param.free_file_size_ = need_alloc_size;
  if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager_->alloc_file_size(object_type, need_alloc_size))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) { // case 1: local disk space is not enough, write through
      ret = OB_SUCCESS; // ignore ret
      write_info.io_desc_.set_write_through(true);
      append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
      if (!write_info.io_desc_.is_sealed()) {
        TmpFileMetaHandle meta_handle;
        if (OB_FAIL(meta_handle.set_tmpfile_meta(false/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
          LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
        } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, meta_handle, ObSSTmpFileSegMetaOpType::INSERT,
                    ObSSTmpFileSegDeleteType::NONE, write_info.io_callback_))) {
          LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
        }
      }
    } else {
      LOG_WARN("fail to alloc file size", KR(ret), K(object_type), "object_type_str",
               get_storage_objet_type_str(object_type), K(need_alloc_size));
    }
  } else { // case 2: local disk space is enough, write local
    // insert meta for sealed && unsealed
    TmpFileMetaHandle meta_handle;
    if (OB_FAIL(meta_handle.set_tmpfile_meta(true/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
      LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
    } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, meta_handle, ObSSTmpFileSegMetaOpType::INSERT,
               ObSSTmpFileSegDeleteType::NONE, write_info.io_callback_))) {
      LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
    }
    if (OB_FAIL(ret)) { // free file size on fail
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(file_manager_->free_file_size(file_id, need_alloc_size))) {
        LOG_WARN("fail to free file size", KR(tmp_ret), K(file_id), K(need_alloc_size));
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::handle_write_with_local_seg(
    TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const ObStorageObjectType object_type = file_id.storage_object_type();
  const TmpFileSegId seg_id(file_id.second_id(), file_id.third_id());
  // tmp_file is 8KB aligned
  const int64_t current_seg_file_size = common::upper_align(meta_handle.get_valid_length(), tmp_file::ObTmpFileGlobal::ALLOC_PAGE_SIZE);
  // because tmp_file maybe overwrite, it need calculate need_alloc_size according to current_seg_file_size and offset
  const int64_t need_alloc_size = MAX(0, write_info.offset_ + write_info.size_ - current_seg_file_size);
  // if segement file overwrite, set target_length as MAX_UNREACHABLE_LENTH, this situation cannot flush segment file
  const int32_t target_length = ((need_alloc_size == 0) ? MAX_UNREACHABLE_LENTH : (write_info.offset_ + write_info.size_));
  meta_handle.set_target_length(target_length); // when segment file start to append, need set target_length
  append_param.free_file_size_ = need_alloc_size; // if async_write_file failed, need free_file_size is equal need_alloc_size
  append_param.need_free_file_size_ = (0 == need_alloc_size) ? false : true; // if need_alloc_size = 0, do not need free_file_size when async_write_file failed
  if ((need_alloc_size > 0) && OB_FAIL(file_manager_->alloc_file_size(object_type, need_alloc_size))) { // if need_alloc_size = 0, do not need alloc_file_size
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) { // case 1: local disk space is not enough, write through
      ret = OB_SUCCESS; // ignore ret
      if (OB_FAIL(read_existing_data_and_append_buf(file_id, meta_handle, write_info, append_param))) {
        LOG_WARN("fail to read existing data and append buf", KR(ret), K(file_id), K(meta_handle), K(write_info));
      } else {
        write_info.io_desc_.set_write_through(true);
        append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
        if (write_info.io_desc_.is_sealed()) { // case 1.1: sealed
          TmpFileMetaHandle fake_meta_handle;
          if (OB_FAIL(fake_meta_handle.set_tmpfile_meta(false/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
            LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
          } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, fake_meta_handle, ObSSTmpFileSegMetaOpType::DELETE,
                      ObSSTmpFileSegDeleteType::LOCAL, write_info.io_callback_))) {
            LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
          }
        } else { // case 1.2: unsealed
          TmpFileMetaHandle cur_meta_handle;
          if (OB_FAIL(cur_meta_handle.set_tmpfile_meta(false/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
            LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
          } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, cur_meta_handle, ObSSTmpFileSegMetaOpType::UPDATE,
                      ObSSTmpFileSegDeleteType::LOCAL, write_info.io_callback_))) {
            LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(cur_meta_handle));
          }
        }
      }
    } else {
      LOG_WARN("fail to alloc file size", KR(ret), K(object_type), K(write_info), "object_type_str",
               get_storage_objet_type_str(object_type), "size", need_alloc_size);
    }
  } else { // case 2: local disk space is enough, write local
    // update meta for sealed && unsealed
    TmpFileMetaHandle cur_meta_handle;
    if (OB_FAIL(cur_meta_handle.set_tmpfile_meta(true/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
      LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
    } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, cur_meta_handle, ObSSTmpFileSegMetaOpType::UPDATE,
                ObSSTmpFileSegDeleteType::NONE, write_info.io_callback_))) {
      LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(cur_meta_handle));
    }
    if (OB_FAIL(ret)) { // free file size on fail
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(file_manager_->free_file_size(file_id, need_alloc_size))) {
        LOG_WARN("fail to free file size", KR(tmp_ret), K(file_id), K(need_alloc_size));
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::handle_write_with_remote_seg(
    TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const ObStorageObjectType object_type = file_id.storage_object_type();
  const TmpFileSegId seg_id(file_id.second_id(), file_id.third_id());
  const int64_t need_alloc_size = write_info.offset_ + write_info.size_;
  const int32_t target_length = write_info.offset_ + write_info.size_;
  meta_handle.set_target_length(target_length); // when segment file start to append, need set target_length
  append_param.free_file_size_ = need_alloc_size;
  if (OB_FAIL(read_existing_data_and_append_buf(file_id, meta_handle, write_info, append_param))) {
    LOG_WARN("fail to read remote data and append buf", KR(ret), K(file_id), K(meta_handle), K(write_info));
  } else if (OB_FAIL(file_manager_->alloc_file_size(object_type, need_alloc_size))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) { // case 1: local disk space is not enough, write through
      ret = OB_SUCCESS; // ignore ret
      write_info.io_desc_.set_write_through(true);
      append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
      if (write_info.io_desc_.is_sealed()) { // case 1.1: sealed
        TmpFileMetaHandle fake_meta_handle;
        if (OB_FAIL(fake_meta_handle.set_tmpfile_meta(false/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
          LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
        } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, fake_meta_handle, ObSSTmpFileSegMetaOpType::DELETE,
                    ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE, write_info.io_callback_, meta_handle.get_valid_length()))) {
          LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
        }
      } else { // case 1.2: unsealed
        TmpFileMetaHandle cur_meta_handle;
        if (OB_FAIL(cur_meta_handle.set_tmpfile_meta(false/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
          LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
        } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, cur_meta_handle, ObSSTmpFileSegMetaOpType::UPDATE,
                    ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE, write_info.io_callback_, meta_handle.get_valid_length()))) {
          LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(cur_meta_handle));
        }
      }
    } else {
      LOG_WARN("fail to alloc file size", KR(ret), K(object_type), "object_type_str",
               get_storage_objet_type_str(object_type), K(need_alloc_size));
    }
  } else { // case 2: local disk space is enough, write local for unsealed and write remote for sealed
    if (write_info.io_desc_.is_sealed()) { // case 2.1: sealed
      // free file size alloc above, and write through
      if (OB_FAIL(file_manager_->free_file_size(file_id, need_alloc_size))) {
        LOG_WARN("fail to free file size", KR(ret), K(file_id), K(write_info));
      } else {
        write_info.io_desc_.set_write_through(true);
        append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
        TmpFileMetaHandle fake_meta_handle;
        if (OB_FAIL(fake_meta_handle.set_tmpfile_meta(false/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
          LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
        } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, fake_meta_handle, ObSSTmpFileSegMetaOpType::DELETE,
                    ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE, write_info.io_callback_, meta_handle.get_valid_length()))) {
          LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
        }
      }
    } else { // case 2.2: unsealed
      TmpFileMetaHandle cur_meta_handle;
      if (OB_FAIL(cur_meta_handle.set_tmpfile_meta(true/*is_in_local*/, write_info.tmp_file_valid_length_, target_length))) {
        LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
      } else if (OB_FAIL(construct_tmp_file_io_callback(seg_id, cur_meta_handle, ObSSTmpFileSegMetaOpType::UPDATE,
                  ObSSTmpFileSegDeleteType::REMOTE, write_info.io_callback_, meta_handle.get_valid_length()))) {
        LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(cur_meta_handle));
      }
      if (OB_FAIL(ret)) { // free file size on fail
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(file_manager_->free_file_size(file_id, need_alloc_size))) {
          LOG_WARN("fail to free file size", KR(tmp_ret), K(file_id), K(need_alloc_size));
        }
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::read_existing_data_and_append_buf(
    const MacroBlockId &file_id,
    const TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::fast_current_time();
  ObMemAttr attr(write_info.mtl_tenant_id_, "TmpFileWrite");
  ObArenaAllocator allocator(attr);
  char *write_buf = nullptr;
  int64_t write_size = 0;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = file_id;
  read_info.offset_ = 0;
  read_info.size_ = meta_handle.get_valid_length();
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.mtl_tenant_id_ = write_info.mtl_tenant_id_;
  read_info.io_desc_.set_unsealed(); // existing data must be unsealed
  ObStorageObjectHandle read_object_handle;
  ObSSLocalCacheReader local_cache_reader; // for seg_meta.is_in_local_ == true
  ObSSObjectStorageReader object_storage_reader; // for seg_meta.is_in_local_ == false
  if (OB_ISNULL(read_info.buf_ = static_cast<char *>(allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for read buf", KR(ret), "size", read_info.size_);
  } else if (meta_handle.is_in_local() && OB_FAIL(local_cache_reader.aio_read(read_info, read_object_handle))) {
    LOG_WARN("fail to aio read", KR(ret), K(read_info), K(read_object_handle));
  } else if (!meta_handle.is_in_local() && OB_FAIL(object_storage_reader.aio_read(read_info, read_object_handle))) {
    LOG_WARN("fail to aio read", KR(ret), K(read_info), K(read_object_handle));
  } else if (OB_FAIL(read_object_handle.wait())) {
    LOG_WARN("fail to wait", KR(ret), K(read_info), K(read_object_handle));
  } else if (FALSE_IT(write_size = (write_info.offset_ + write_info.size_))) {
  // Note: this write_buf will be free after IOManager::aio_write, which copies write_buf into IOManager
  } else if (OB_ISNULL(write_buf = static_cast<char *>(append_param.arena_allocator_.alloc(write_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for write buf", KR(ret), K(write_size));
  } else if (OB_ISNULL(write_info.buffer_) || OB_UNLIKELY(write_size < read_info.size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null or write buffer size is too small", KR(ret), K(write_info), K(read_info));
  } else {
    MEMCPY(write_buf, read_info.buf_, read_info.size_);
    MEMCPY(write_buf + write_info.offset_, write_info.buffer_, write_info.size_);
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = write_size;
  }
  const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
  if (OB_UNLIKELY(cost_us > 200 * 1000L)) { // 200ms
    LOG_WARN("append tmp file cost too much time", KR(ret), K(cost_us), K(file_id), K(write_info));
  }
  return ret;
}

int ObSegmentFileManager::simulate_io_result(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle) const
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  io_info.tenant_id_ = write_info.mtl_tenant_id_;
  io_info.offset_ = write_info.offset_;
  io_info.size_ = write_info.size_;
  io_info.buf_ = write_info.buffer_;
  io_info.flag_ = write_info.io_desc_;
  io_info.callback_ = write_info.io_callback_;
  io_info.tmp_file_valid_length_ = write_info.tmp_file_valid_length_;
  io_info.flag_.set_sys_module_id(write_info.io_desc_.get_sys_module_id());
  io_info.flag_.set_write();
  io_info.timeout_us_ = write_info.io_timeout_ms_;
  io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;

  ObIOResult *io_result = nullptr;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(io_info.tenant_id_, tenant_holder))) {
    LOG_WARN("fail to get tenant io manager", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(tenant_holder.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant holder ptr is null", KR(ret));
  } else if (OB_FAIL(tenant_holder.get_ptr()->alloc_and_init_result(io_info, io_result))) {
    LOG_WARN("fail to alloc and init result", KR(ret), K(io_info));
  } else if (OB_ISNULL(io_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io result is null", KR(ret));
  } else if (OB_FAIL(object_handle.get_io_handle().set_result(*io_result))) {
    LOG_WARN("fail to set result", KR(ret));
  } else {
    io_result->finish_without_accumulate(ret);
  }
  return ret;
}

int ObSegmentFileManager::construct_tmp_file_io_callback(
    const TmpFileSegId &seg_id,
    const TmpFileMetaHandle &meta_handle,
    const ObSSTmpFileSegMetaOpType seg_meta_op_type,
    const ObSSTmpFileSegDeleteType seg_del_type,
    ObIOCallback *&io_callback,
    const int64_t del_seg_valid_len)
{
  int ret = OB_SUCCESS;
  ObSSTmpFileIOCallback *tmp_file_io_callback = nullptr;
  ObIAllocator &io_callback_allocator = file_manager_->get_io_callback_allocator();
  if (OB_UNLIKELY(nullptr != io_callback || !meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write io callback is not null", KR(ret), KP(io_callback), K(seg_id), K(meta_handle));
  } else if (OB_ISNULL(tmp_file_io_callback = static_cast<ObSSTmpFileIOCallback *>(
                       io_callback_allocator.alloc(sizeof(ObSSTmpFileIOCallback))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ss tmp file io callback memory", KR(ret));
  } else if (FALSE_IT(tmp_file_io_callback = new (tmp_file_io_callback) ObSSTmpFileIOCallback())) {
  } else if (OB_FAIL(tmp_file_io_callback->set_ss_tmpfile_io_callback(seg_id, meta_handle,
                                                      seg_meta_op_type, seg_del_type,
                                                      del_seg_valid_len, &io_callback_allocator))) {
    LOG_WARN("fail to set io callback", KR(ret), K(seg_id), K(meta_handle));
  } else {
    io_callback = tmp_file_io_callback;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_file_io_callback)) {
    io_callback_allocator.free(tmp_file_io_callback);
  }
  return ret;
}

int ObSegmentFileManager::push_item_to_heap(const HeapItem &heap_item, ObBinaryHeap<HeapItem, HeapCompare> &seg_file_heap, int64_t &total_flush_size)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager or disk space manager is null", KR(ret), KP(disk_space_mgr));
  } else if (OB_FAIL(seg_file_heap.push(heap_item))) {
    LOG_WARN("fail to push heap", KR(ret), K(heap_item));
  } else {
    total_flush_size += heap_item.file_length_;
    // when unsealed tmp file flush size reach the low water mark(4%), seg_file_heap need pop the latest segment file(append timsatmp is largest), only evict the oldest segment file
    while (OB_SUCC(ret) && !seg_file_heap.empty() && disk_space_mgr->is_unsealed_tmp_file_flush_reach_low_water_mark(total_flush_size)) {
      const int32_t heap_top_length = seg_file_heap.top().file_length_;
      if (OB_FAIL(seg_file_heap.pop())) {
        LOG_WARN("fail to pop heap", KR(ret));
      } else {
        total_flush_size -= heap_top_length;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
