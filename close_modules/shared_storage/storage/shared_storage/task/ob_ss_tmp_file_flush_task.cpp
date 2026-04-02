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

#include "ob_ss_tmp_file_flush_task.h"
#include "share/io/ob_io_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

/*-----------------------------------------ObFlushTmpFileMeta-----------------------------------------*/
void ObFlushTmpFileMeta::reset()
{
  file_id_.reset();
  file_len_ = 0;
  func_type_ = 0;
  is_sealed_ = true;
  valid_length_ = 0;
}

ObFlushTmpFileMeta &ObFlushTmpFileMeta::operator=(const ObFlushTmpFileMeta &other)
{
  if (this != &other) {
    file_id_ = other.file_id_;
    file_len_ = other.file_len_;
    func_type_ = other.func_type_;
    is_sealed_ = other.is_sealed_;
    valid_length_ = other.valid_length_;
  }
  return *this;
}

/*-----------------------------------------ObSSTmpFileFlushEntry-----------------------------------------*/
int ObSSTmpFileFlushEntry::init(const ObFlushTmpFileMeta &file_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr != buf_) || read_handle_.is_valid() || write_handle_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(*this));
  } else if (OB_UNLIKELY(!file_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_meta));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(file_meta.file_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K_(file_meta.file_len));
  } else {
    read_handle_.reset();
    write_handle_.reset();
    file_meta_ = file_meta;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObSSTmpFileFlushEntry::reset()
{
  file_meta_.reset();
  read_handle_.reset();
  write_handle_.reset();
  if (nullptr != buf_) {
    // Need to pay attention!!!
    // The allocator is used to allocate io data buffer, and its memory life cycle needs to be longer than the object handle.
    allocator_.free(buf_);
    buf_ = nullptr;
  }
}

/*-----------------------------------------ObSSTmpFileFlushTask-----------------------------------------*/
ObSSTmpFileFlushTask::ObSSTmpFileFlushTask()
    : is_inited_(false), tg_id_(-1), flush_parallel_cnt_(0), interval_us_(-1), file_manager_(nullptr),
      flush_entry_arr_(nullptr), free_list_(), async_read_list_(), async_write_list_(), seg_files_(),
      flushed_files_(), allocator_()
{}

int ObSSTmpFileFlushTask::init(ObTenantFileManager *file_manager)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "TMP_FILE_FLUSH");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss_tmp_file flush task init twice", KR(ret));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(file_manager));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE, attr))) {
    LOG_WARN("fail to init allocatro", KR(ret));
  } else if (OB_FAIL(free_list_.init(MAX_FLUSH_PARALLELISM))) {
    LOG_WARN("fail to init free_list", KR(ret));
  } else if (OB_FAIL(async_write_list_.init(MAX_FLUSH_PARALLELISM))) {
    LOG_WARN("fail to init async_write_list", KR(ret));
  } else if (OB_FAIL(async_read_list_.init(MAX_FLUSH_PARALLELISM))) {
    LOG_WARN("fail to init async_read_list", KR(ret));
  } else if (OB_FAIL(pre_alloc_flush_entry())) {
    LOG_WARN("fail to pre alloc flush_entry", KR(ret));
  } else {
    file_manager_ = file_manager;
    interval_us_ = SLOW_SCHEDULE_INTERVAL_US;
    is_inited_ = true;
    set_flush_parallel_cnt();
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  LOG_INFO("finish to init ss_tmp_file flush task", KR(ret), K_(flush_parallel_cnt), K(MTL_MEM_SIZE()));
  return ret;
}

int ObSSTmpFileFlushTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file flush task is not init", KR(ret));
  } else if (OB_UNLIKELY(-1 == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(schedule_tmp_file_flush_task())) {
    LOG_WARN("fail to schedule tmp file flush task", KR(ret));
  } else {
    LOG_INFO("succ to start tmp file flush task", K(tg_id));
  }
  return ret;
}

void ObSSTmpFileFlushTask::destroy()
{
  destroy_flush_entry_arr();
  tg_id_ = -1;
  flush_parallel_cnt_ = 0;
  interval_us_ = -1;
  file_manager_ = nullptr;
  free_list_.destroy();
  async_read_list_.destroy();
  async_write_list_.destroy();
  seg_files_.destroy();
  flushed_files_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

int ObSSTmpFileFlushTask::schedule_tmp_file_flush_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, interval_us_, false/*repeat*/))) {
    LOG_WARN("fail to schedule ss tmp file flush task", KR(ret), K_(tg_id), K_(interval_us));
  } else {
    LOG_TRACE("succ to schedule ss tmp file flush task", K_(tg_id), K_(interval_us));
  }
  return ret;
}

void ObSSTmpFileFlushTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tmp file flush task is not inited", KR(ret));
  } else if (OB_FAIL(flush_tmp_files())) {
    LOG_WARN("fail to flush tmp_files", KR(ret));
  }

  // adjust flush_task_parallel_cnt if memory size changes.
  if (TC_REACH_TIME_INTERVAL(UPDATE_PARALLEL_INTERVAL_US)) {
    set_flush_parallel_cnt();
  }

  adjust_flush_task_interval();
  if (OB_TMP_FAIL(schedule_tmp_file_flush_task())) {
    if (OB_CANCELED == tmp_ret) {
      LOG_INFO("tmp file flush task has stopped");
    } else {
      LOG_ERROR("fail to reschedule tmp file flush task", KR(ret), K(tmp_ret), K_(interval_us));
    }
  }
}

void ObSSTmpFileFlushTask::destroy_flush_entry_arr()
{
  if (nullptr != flush_entry_arr_) {
    for (int64_t i = 0; i < MAX_FLUSH_PARALLELISM; ++i) {
      flush_entry_arr_[i].~ObSSTmpFileFlushEntry();
    }
    allocator_.free(flush_entry_arr_);
    flush_entry_arr_ = nullptr;
  }
}

int ObSSTmpFileFlushTask::pre_alloc_flush_entry()
{
  int ret = OB_SUCCESS;
  const int64_t mem_size = sizeof(ObSSTmpFileFlushEntry) * MAX_FLUSH_PARALLELISM;
  if (OB_ISNULL(flush_entry_arr_ = static_cast<ObSSTmpFileFlushEntry *>(allocator_.alloc(mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(mem_size));
  } else {
    for (int64_t i = 0; i < MAX_FLUSH_PARALLELISM; ++i) {
      new (flush_entry_arr_ + i) ObSSTmpFileFlushEntry(allocator_);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_FLUSH_PARALLELISM; ++i) {
      ObSSTmpFileFlushEntry *flush_entry = flush_entry_arr_ + i;
      if (OB_FAIL(free_list_.push(flush_entry))) {
        LOG_WARN("fail to push flush_entry into free_list", K(ret), K(i), K(free_list_.get_curr_total()));
      }
    }
  }

  if (OB_FAIL(ret)) {
    destroy_flush_entry_arr();
  }
  return ret;
}

int ObSSTmpFileFlushTask::flush_tmp_files()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_disk_space_manager should not be null", KR(ret), KP(tnt_disk_space_mgr));
  } else {
    clear_for_next_round();
    const int64_t start_us = ObTimeUtility::current_time();
    if (OB_FAIL(batch_get_seg_files())) {
      LOG_WARN("fail to batch get seg files", KR(ret));
    } else if (OB_FAIL(async_read_seg_files())) {
      LOG_WARN("fail to async read seg files", KR(ret));
    } else if (OB_FAIL(async_write_seg_files())) {
      LOG_WARN("fail to async write seg files", KR(ret));
    } else if (OB_FAIL(complete_flush_seg_files())) {
      LOG_WARN("fail to complete flush seg files", KR(ret));
    }
    if (flushed_files_.count() > 0) {
      const int64_t cost_us = ObTimeUtility::current_time() - start_us;
      LOG_INFO("finish tmp file flush task", KR(ret), K(cost_us), K(flushed_files_.count()), K_(flushed_files), 
        K(async_read_list_.get_curr_total()), K(async_write_list_.get_curr_total()), "all_disk_cache_info",
        (tnt_disk_space_mgr->get_all_disk_cache_info()));
    }
  }
  return ret;
}

int ObSSTmpFileFlushTask::batch_get_seg_files()
{
  int ret = OB_SUCCESS;
  const int64_t total_entry_cnt =
      free_list_.get_curr_total() + async_read_list_.get_curr_total() + async_write_list_.get_curr_total();
  if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_manager is null", KR(ret), KP_(file_manager));
  } else if (OB_UNLIKELY(total_entry_cnt != MAX_FLUSH_PARALLELISM)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("flush entry leak!!!", KR(ret), K(total_entry_cnt));
  }
  const int64_t max_cnt = MIN(flush_parallel_cnt_, free_list_.get_curr_total());
  for (int64_t i = 0; (i < max_cnt) && OB_SUCC(ret); ++i) {
    ObSegmentFileInfo *seg_file_info = nullptr;
    int64_t file_len = 0;
    ObLink *ptr = nullptr;
    if (OB_FAIL(file_manager_->flush_queue_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;  // ignore ret
        break;
      } else {
        LOG_WARN("fail to pop seg file id", KR(ret));
      }
    } else if (OB_ISNULL(ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("seg file id is null", KR(ret));
    } else {
      seg_file_info = static_cast<ObSegmentFileInfo *>(ptr);
      TmpFileSegId seg_id(seg_file_info->file_id_.second_id(), seg_file_info->file_id_.third_id());
      bool is_meta_exist = false;
      TmpFileMetaHandle meta_handle;
      ObFlushTmpFileMeta file_meta;
      if (OB_FAIL(file_manager_->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
        LOG_WARN("fail to get seg meta", KR(ret), K(seg_id), K(meta_handle));
      } else if (!is_meta_exist) {
        // do nothing, seg_meta is not existed, maybe segment file has been gc,cannot flush to object storage
        LOG_INFO("the segment file meta does not exist", K(file_meta));
      } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, meta handle is invalid", KR(ret), K(meta_handle));
      } else if (OB_FAIL(file_manager_->get_local_file_length(seg_file_info->file_id_, 0 /*ls_epoch_id*/, file_len))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          // has already been gc, ignore ret
          ret = OB_SUCCESS;
          LOG_INFO("seg file does not exist, may be already gc", "file_id", seg_file_info->file_id_);
        } else {
          LOG_WARN("fail to get file length", KR(ret), "file_id", seg_file_info->file_id_);
        }
      } else if (FALSE_IT(file_meta = ObFlushTmpFileMeta(seg_file_info->file_id_, file_len, seg_file_info->func_type_, seg_file_info->is_sealed_, meta_handle.get_valid_length()))) {
      } else if (!seg_file_info->is_sealed_) {
        // Note: this lock does not ensure no appending during flush. need check appending again after flush
        SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
        if (meta_handle.is_tmp_file_appending(file_len)) {
          // do nothing, seg file is appending, cannot flush to object storage
        } else if (OB_FAIL(seg_files_.push_back(file_meta))) {
          LOG_WARN("fail to push back", KR(ret), K(file_meta));
        }
      } else if (OB_FAIL(seg_files_.push_back(file_meta))) {
        LOG_WARN("fail to push back", KR(ret), K(file_meta));
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(seg_file_info)) {
      int tmp_ret = OB_SUCCESS;
      ObFlushTmpFileMeta file_meta(seg_file_info->file_id_, file_len, seg_file_info->func_type_, seg_file_info->is_sealed_, 0/*valid_length*/);
      if (OB_TMP_FAIL(push_again_on_fail(file_meta))) {
        LOG_WARN("fail to push again on fail", KR(tmp_ret), K(file_meta));
      }
    }

    // free memory
    OB_DELETE(ObSegmentFileInfo, attr, seg_file_info);
  }

  return ret;
}

int ObSSTmpFileFlushTask::do_async_read_seg_file(ObSSTmpFileFlushEntry &flush_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!flush_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("flush_entry should be valid", KR(ret), K(flush_entry));
  } else if (OB_UNLIKELY(flush_entry.is_read_handle_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_handle should not be valid", KR(ret), K_(flush_entry.read_handle));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(flush_entry.get_function_type());

    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = flush_entry.file_meta_.file_id_;
    read_info.offset_ = 0;
    read_info.size_ = flush_entry.file_meta_.file_len_;
    read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.buf_ = flush_entry.buf_;
    read_info.mtl_tenant_id_ = MTL_ID();
    ObSSLocalCacheReader local_cache_reader;
    if (OB_FAIL(local_cache_reader.aio_read(read_info, flush_entry.read_handle_))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        // has already been gc
        LOG_INFO("seg file does not exist, may be already gc", K_(flush_entry.file_meta));
      } else {
        LOG_WARN("fail to aio read", KR(ret), K(read_info), K(flush_entry));
      }
    }
  }
  return ret;
}

int ObSSTmpFileFlushTask::async_read_seg_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t seg_file_cnt = seg_files_.count();
  for (int64_t i = 0; (i < seg_file_cnt) && OB_SUCC(ret); ++i) {
    ObSSTmpFileFlushEntry *flush_entry = nullptr;
    const ObFlushTmpFileMeta &seg_file = seg_files_.at(i);
    bool need_free = true;
    if (OB_FAIL(free_list_.pop(flush_entry))) {
      LOG_WARN("fail to pop flush_entry from free_list", KR(ret), K(free_list_.get_curr_total()));
    } else if (OB_ISNULL(flush_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry should not be null", KR(ret), KP(flush_entry));
    } else if (OB_FAIL(flush_entry->init(seg_file))) {
      LOG_WARN("fail to init flush_entry", KR(ret), K(seg_file), KPC(flush_entry));
    } else if (OB_FAIL(do_async_read_seg_file(*flush_entry))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) { // file has been gced, ignore ret.
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to do async read seg_file", KR(ret), KPC(flush_entry));
      }
    } else if (OB_FAIL(async_read_list_.push(flush_entry))) {
      LOG_ERROR("fail to push flush_entry into read_list", KR(ret), K(async_read_list_.get_curr_total()));
    } else {
      need_free = false;
    }

    if (OB_FAIL(ret)) {
      if (OB_TMP_FAIL(push_again_on_fail(seg_file))) {
        LOG_WARN("fail to push again on fail", KR(tmp_ret), K(seg_file));
      }
    }
    if (need_free && (nullptr != flush_entry)) {
      if (OB_TMP_FAIL(recycle_flush_entry(*flush_entry))) {
        LOG_WARN("fail to recycle flush_entry", KR(tmp_ret), KPC(flush_entry), K(free_list_.get_curr_total()));
      }
    }

    ret = OB_SUCCESS; // ignore ret, process next seg_file.
  }
  return ret;
}

int ObSSTmpFileFlushTask::do_async_write_seg_file(ObSSTmpFileFlushEntry &flush_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!flush_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("flush_entry should be valid", KR(ret), K(flush_entry));
  } else if (OB_UNLIKELY(!flush_entry.is_read_handle_valid() || flush_entry.is_write_handle_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_handle should be valid and write_handle should be invalid", KR(ret), K(flush_entry));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(flush_entry.get_function_type());

    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = flush_entry.read_handle_.get_buffer();
    write_info.offset_ = 0;
    write_info.size_ = flush_entry.read_handle_.get_data_size();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(write_info.mtl_tenant_id_);
    if (!flush_entry.file_meta_.is_sealed_) {
      write_info.io_desc_.set_unsealed();
      write_info.tmp_file_valid_length_ = flush_entry.file_meta_.valid_length_; // unsealed tmp file write must set tmp_file_valid_length used for concat file path
    }
    ObSSObjectStorageWriter object_storage_writer;
    if (OB_FAIL(flush_entry.write_handle_.set_macro_block_id(flush_entry.file_meta_.file_id_))) {
      LOG_WARN("fail to set macro block id", KR(ret), K_(flush_entry.file_meta));
    } else if (OB_FAIL(object_storage_writer.aio_write(write_info, flush_entry.write_handle_))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K_(flush_entry.write_handle));
    }
  }
  return ret;
}

int ObSSTmpFileFlushTask::async_write_seg_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t io_cnt = async_read_list_.get_curr_total();
  for (int64_t i = 0; (i < io_cnt) && OB_SUCC(ret); ++i) {
    ObSSTmpFileFlushEntry *flush_entry = nullptr;
    bool need_free = true;
    bool is_finished = false;
    if (OB_FAIL(async_read_list_.pop(flush_entry))) {
      LOG_WARN("fail to pop flush_entry from async_read_list", KR(ret), K(i), K(io_cnt));
    } else if (OB_ISNULL(flush_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry should not be null", KR(ret), KP(flush_entry));
    } else if (OB_UNLIKELY(!flush_entry->is_valid() || !flush_entry->is_read_handle_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry and read_handle should be valid", KR(ret), KPC(flush_entry));
    } else if (OB_FAIL(flush_entry->read_handle_.check_is_finished(is_finished))) {
      LOG_WARN("fail to check read_handle is_finished", KR(ret), K_(flush_entry->read_handle));
    } else {
      if (is_finished) {
        if (OB_FAIL(flush_entry->read_handle_.wait())) {
          LOG_WARN("fail to wait read_handle", KR(ret), K_(flush_entry->read_handle));
        } else if (OB_FAIL(do_async_write_seg_file(*flush_entry))) {
          LOG_WARN("fail do async write seg_file", KR(ret), KPC(flush_entry));
        } else if (OB_FAIL(async_write_list_.push(flush_entry))) {
          LOG_ERROR("fail to push flush_entry into write_list", KR(ret), K(async_write_list_.get_curr_total()));
        } else {
          need_free = false;
        }
      } else {
        if (OB_FAIL(async_read_list_.push(flush_entry))) {
          LOG_ERROR("fail to push flush_entry into async_read_list", KR(ret), K(async_read_list_.get_curr_total()));
        } else {
          need_free = false;
        }
      }
    }

    if (OB_FAIL(ret) && (nullptr != flush_entry) && flush_entry->is_valid()) {
      const ObFlushTmpFileMeta &file_meta = flush_entry->file_meta_;
      if (OB_TMP_FAIL(push_again_on_fail(file_meta))) {
        LOG_WARN("fail to push again on fail", KR(tmp_ret), K(file_meta));
      }
    }
    if (need_free && (nullptr != flush_entry)) {
      if (OB_TMP_FAIL(recycle_flush_entry(*flush_entry))) {
        LOG_WARN("fail to recycle flush_entry", KR(tmp_ret), KPC(flush_entry), K(free_list_.get_curr_total()));
      }
    }

    ret = OB_SUCCESS; // ignore ret, process next flush_entry
  }
  return ret;
}

int ObSSTmpFileFlushTask::prepare_check_before_move(const ObFlushTmpFileMeta &file_meta)
{
  int ret = OB_SUCCESS;
  bool is_exist_lru = false;
  bool is_file_exist = true;
  if (OB_UNLIKELY(!file_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_meta));
  } else if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_mgr is null", KR(ret), KP_(file_manager));
  } else if (OB_FAIL(file_manager_->get_preread_cache_mgr().is_exist_in_lru(file_meta.file_id_, is_exist_lru))) {
    LOG_WARN("fail to check file id exist in lru", KR(ret), K(file_meta));
  } else if (is_exist_lru) {
    // before push file_id to lru need check file id if exist in lru, if exist, report unexpected error, maybe file id has been sealed twice, lead to tmp_file read cache is inaccurate
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, file id should not exit in lru, maybe file id has been sealed twice", KR(ret), K(file_meta), K(is_exist_lru));
  } else if (OB_FAIL(file_manager_->is_exist_local_file(file_meta.file_id_, 0/*ls_epoch_id*/, is_file_exist))) {
    LOG_WARN("fail to check file exist in local", KR(ret), K(file_meta));
  } else if (!is_file_exist) {
    // because tmp_file has been gc, do not need move from write cache to read cache, otherwise local cache size leak
    // there are concurrency issues between GC and Flush, which lead to inaccurate space. This can reduce the probability and be optimized later.
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    LOG_WARN("file is not exit in local cache", KR(ret), K(file_meta), K(is_file_exist));
  }
  return ret;
}

int ObSSTmpFileFlushTask::move_from_write_cache_to_read_cache(const ObFlushTmpFileMeta &file_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  const ObStorageObjectType object_type = ObStorageObjectType::TMP_FILE;
  TmpFileSegId seg_id(file_meta.file_id_.second_id(), file_meta.file_id_.third_id());
  if (OB_UNLIKELY(!file_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_meta));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *)) || OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk_space_mgr or file_mgr is null", KR(ret), KP(disk_space_mgr), KP_(file_manager));
  } else if (OB_FAIL(disk_space_mgr->alloc_file_size(file_meta.file_len_, object_type, true/*is_tmp_file_read_cache*/))) {
    if (TC_REACH_TIME_INTERVAL(ObBaseFileManager::PRINT_LOG_INTERVAL)) {
      LOG_WARN("fail to alloc preread cache size", KR(ret), K_(file_meta.file_len), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
    }
  } else if (OB_FAIL(file_manager_->get_preread_cache_mgr().push_file_id_to_lru(
             file_meta.file_id_, true/*already_exist_in_cache*/, file_meta.file_len_))) {
    LOG_WARN("fail to push file id to lru", KR(ret), K(file_meta));
    // free the read cache size alloc above
    if (OB_TMP_FAIL(disk_space_mgr->free_file_size(file_meta.file_len_, object_type, true/*is_tmp_file_read_cache*/))) {
      LOG_WARN("fail to free preread cache size", KR(tmp_ret), K_(file_meta.file_len), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
    }
  } // Note: do not delete tmp file meta here, delete tmp file meta when evict by preread_cache_mgr

  if (OB_SUCC(ret)) {  // 1. succ to move from write cache space to read cache space, free write cache size
    if (OB_TMP_FAIL(disk_space_mgr->free_file_size(file_meta.file_len_, object_type, false/*is_tmp_file_read_cache*/))) {
      LOG_WARN("fail to free tmp file write cache size", KR(tmp_ret), K_(file_meta.file_len), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
    }
    // used to count which tmp_files have completed flushing in a round.
    if (OB_TMP_FAIL(flushed_files_.push_back(file_meta))) {
      LOG_WARN("fail to push file_meta into flushed_file_arr", KR(tmp_ret), K(file_meta));
    }
  } else {  // 2. fail to move from write cache space to read cache space, delete local file
    bool is_meta_exist = false;
    TmpFileMetaHandle meta_handle;
    if (OB_TMP_FAIL(file_manager_->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
      LOG_WARN("fail to try get seg meta", KR(ret), K(seg_id));
    } else if (!is_meta_exist) {
      // do nothing. may be gc concurrently
    } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle should not be invalid", KR(ret), K(seg_id), K(meta_handle));
    } else {
      SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
      // Note: must set is_in_local_ = false, otherwise concurrent read may fail to read local file.
      // e.g., concurrent read get meta with is_in_local = true, then here delete local file,
      // finally concurrent read fail to read local file.
      meta_handle.get_tmpfile_meta()->is_in_local_ = false;
      // Note: must delete meta before delete file
      if (OB_TMP_FAIL(file_manager_->get_segment_file_mgr().delete_meta(seg_id))) { // expect will not fail
        LOG_WARN("fail to delete meta", KR(ret), K(seg_id));
      } else if (OB_TMP_FAIL(file_manager_->delete_local_file(file_meta.file_id_))) {
        LOG_WARN("fail to delete local tmp file", KR(tmp_ret), K_(file_meta.file_id));
      }
    }
  }
  return ret;
}

int ObSSTmpFileFlushTask::handle_unsealed_file_meta(const ObFlushTmpFileMeta &file_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  TmpFileSegId seg_id(file_meta.file_id_.second_id(), file_meta.file_id_.third_id());
  bool is_meta_exist = false;
  TmpFileMetaHandle meta_handle;
  if (OB_FAIL(file_manager_->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to get seg meta", KR(ret), K(seg_id));
  } else if (!is_meta_exist) {
    // do nothing, seg_meta is not existed, maybe segment file has been gc,cannot flush to object storage
    LOG_INFO("the segment file meta does not exist", K(file_meta));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, meta handle is invalid", KR(ret), K(meta_handle));
  } else {
    SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
    // check whether seg has been appended or is appending in flush process
    if (meta_handle.is_tmp_file_appending(file_meta.file_len_)) {
      // target_length is is not equal file_len or target_length is MAX_UNREACHABLE_LENTH, means seg file has been appended in flush process
      // cannot flush to object storage, do not need update seg meta and delete local file
    } else {
      meta_handle.get_tmpfile_meta()->is_in_local_ = false; // update seg meta
      if (OB_FAIL(file_manager_->delete_local_file(file_meta.file_id_, 0/*ls_epoch_id*/, true/*is_print_log*/, false/*is_del_seg_meta*/))) { // 5.delete local seg,free size
        LOG_WARN("fail to delete local tmp file", KR(ret), K_(file_meta.file_id));
      } else if (OB_TMP_FAIL(flushed_files_.push_back(file_meta))) { // used to count which tmp_files have completed flushing in a round
        LOG_WARN("fail to push back", KR(tmp_ret), K(file_meta));
      }
    }
  }
  return ret;
}

int ObSSTmpFileFlushTask::complete_flush_seg_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t io_cnt = async_write_list_.get_curr_total();
  ObTenantDiskSpaceManager *disk_space_manager = nullptr;
  if (OB_ISNULL(disk_space_manager = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk_space_manager should not be null", KR(ret), KP(disk_space_manager));
  }
  for (int64_t i = 0; (i < io_cnt) && OB_SUCC(ret); ++i) {
    ObSSTmpFileFlushEntry *flush_entry = nullptr;
    bool need_free = true;
    bool need_push_again = false;
    bool is_finished = false;
    if (OB_FAIL(async_write_list_.pop(flush_entry))) {
      LOG_WARN("fail to pop flush_entry from async_write_list", KR(ret), K(i), K(io_cnt));
    } else if (OB_ISNULL(flush_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry should not be null", KR(ret), KP(flush_entry));
    } else if (OB_UNLIKELY(!flush_entry->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry should be valid", KR(ret), KPC(flush_entry));
    } else if (OB_UNLIKELY(!flush_entry->is_read_handle_valid() || !flush_entry->is_write_handle_valid())) {
      need_push_again = true;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read_handle and write_handle should be valid", KR(ret), KPC(flush_entry));
    } else if (OB_FAIL(flush_entry->write_handle_.check_is_finished(is_finished))) {
      need_push_again = true;
      LOG_WARN("fail to check io_handle is_finished", KR(ret), K_(flush_entry->write_handle));
    } else {
      if (is_finished) {
        if (OB_FAIL(flush_entry->write_handle_.wait())) {
          need_push_again = true;
          LOG_WARN("fail to wait write_handle", KR(ret), K(flush_entry->write_handle_));
        } else if (flush_entry->file_meta_.is_sealed_) { // sealed file need move to read cache
          if (OB_FAIL(prepare_check_before_move(flush_entry->file_meta_))) {
            LOG_WARN("fail to prepare check", KR(ret), K_(flush_entry->file_meta));
          } else if (OB_FAIL(move_from_write_cache_to_read_cache(flush_entry->file_meta_))) {
            // if fail to alloc read cache space, delete local tmp_file directly, so no need to seal again.
            LOG_WARN("fail to alloc tmp file read cache size", KR(ret), K_(flush_entry->file_meta));
          }
        } else { // unsealed file cannot move to read cache
          if (OB_FAIL(handle_unsealed_file_meta(flush_entry->file_meta_))) {
            LOG_WARN("fail to handle unsealed file meta", KR(ret), K_(flush_entry->file_meta));
          }
        }
      } else {
        if (OB_FAIL(async_write_list_.push(flush_entry))) {
          need_push_again = true;
          LOG_ERROR("fail to push flush_entry into async_write_list", KR(ret), K(async_write_list_.get_curr_total()));
        } else {
          need_free = false;
        }
      }
    }

    if (OB_FAIL(ret) && need_push_again) {
      const ObFlushTmpFileMeta &file_meta = flush_entry->file_meta_;
      if (OB_TMP_FAIL(push_again_on_fail(file_meta))) {
        LOG_WARN("fail to push again on fail", KR(tmp_ret), K(file_meta));
      }
    }
    if (need_free && (nullptr != flush_entry)) {
      if (OB_TMP_FAIL(recycle_flush_entry(*flush_entry))) {
        LOG_WARN("fail to recycle flush_entry", KR(tmp_ret), KPC(flush_entry), K(free_list_.get_curr_total()));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next flush entry
  }
  return ret;
}

int ObSSTmpFileFlushTask::push_again_on_fail(const ObFlushTmpFileMeta &file_meta)
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(file_meta.func_type_);

  if (OB_ISNULL(file_manager_) || !file_meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_manager is null or file_meta is invalid", KR(ret), K(file_meta));
  } else if (OB_FAIL(file_manager_->push_to_flush_queue(file_meta.file_id_, 0 /*ls_epoch_id*/, file_meta.is_sealed_))) {
    LOG_WARN("fail to push to flush_queue", KR(ret), K(file_meta));
  }
  return ret;
}

int ObSSTmpFileFlushTask::recycle_flush_entry(ObSSTmpFileFlushEntry &flush_entry)
{
  int ret = OB_SUCCESS;
  flush_entry.reset();
  if (OB_FAIL(free_list_.push(&flush_entry))) {
    LOG_ERROR("fail to push flush_entry into free_list", KR(ret), K(flush_entry), K(free_list_.get_curr_total()));
  }
  return ret;
}

void ObSSTmpFileFlushTask::clear_for_next_round()
{
  seg_files_.reuse();
  flushed_files_.reuse();
}

void ObSSTmpFileFlushTask::set_flush_parallel_cnt()
{
  // default parallel is 5, max parallel is 30, each 1GB of tenant memory increase one parallel
  flush_parallel_cnt_ = MIN(DEFAULT_FLUSH_PARALLELISM + MTL_MEM_SIZE() / GB, MAX_FLUSH_PARALLELISM);
}

void ObSSTmpFileFlushTask::adjust_flush_task_interval()
{
  // exist unprocessed seg_file or unfinished async read io on loacl disk.
  if ((OB_NOT_NULL(file_manager_) && (file_manager_->flush_queue_.size() > 0) && (free_list_.get_curr_total() > 0)) ||
      (async_read_list_.get_curr_total() > 0)) {
    interval_us_ = FAST_SCHEDULE_INTERVAL_US;
  } else if (async_write_list_.get_curr_total() > 0) {  // exist unfinished async write io on object storage.
    interval_us_ = MODERATE_SCHEDULE_INTERVAL_US;
  } else {
    interval_us_ = SLOW_SCHEDULE_INTERVAL_US;
  }
}
} // namespace storage
} // namespace oceanbase
