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

#include "ob_ss_preread_task.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "share/io/ob_io_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObPreReadFileMeta-----------------------------------------*/
void ObPreReadFileMeta::reset()
{
  file_id_.reset();
  func_type_ = 0;
}

ObPreReadFileMeta &ObPreReadFileMeta::operator=(const ObPreReadFileMeta &other)
{
  if (this != &other) {
    file_id_ = other.file_id_;
    func_type_ = other.func_type_;
  }
  return *this;
}

/*-----------------------------------------ObSSPreReadEntry-----------------------------------------*/
int ObSSPreReadEntry::init(const ObPreReadFileMeta &file_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr != buf_) || read_handle_.is_valid() || write_handle_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(*this));
  } else if (OB_UNLIKELY(!file_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_meta));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(OB_DEFAULT_MACRO_BLOCK_SIZE));
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

void ObSSPreReadEntry::reset()
{
  if (nullptr != buf_) {
    allocator_.free(buf_);
    buf_ = nullptr;
  }
  file_meta_.reset();
  read_handle_.reset();
  write_handle_.reset();
}

/*-----------------------------------------ObSSPreReadTask-----------------------------------------*/
ObSSPreReadTask::ObSSPreReadTask()
  : is_inited_(false), tg_id_(-1), tenant_id_(OB_INVALID_TENANT_ID), preread_parallel_cnt_(0), interval_us_(-1),
    preread_cache_mgr_(nullptr), preread_entry_arr_(nullptr), free_list_(), async_read_list_(),
    async_write_list_(), segment_files_(), preread_files_(), allocator_()
{}

int ObSSPreReadTask::init(ObPrereadCacheManager *preread_cache_mgr)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), "SSPreRead");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSPreReadTask init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(preread_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(preread_cache_mgr));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE, attr))) {
    LOG_WARN("fail to init allocatro", KR(ret));
  } else if (OB_FAIL(free_list_.init(MAX_PRE_READ_PARALLELISM))) {
    LOG_WARN("fail to init free_list", KR(ret));
  } else if (OB_FAIL(async_write_list_.init(MAX_PRE_READ_PARALLELISM))) {
    LOG_WARN("fail to init async_write_list", KR(ret));
  } else if (OB_FAIL(async_read_list_.init(MAX_PRE_READ_PARALLELISM))) {
    LOG_WARN("fail to init async_read_list", KR(ret));
  } else if (OB_FAIL(pre_alloc_preread_entry())) {
    LOG_WARN("fail to pre alloc preread_entry", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    preread_cache_mgr_ = preread_cache_mgr;
    interval_us_ = SLOW_SCHEDULE_INTERVAL_US;
    is_inited_ = true;
    set_read_cache_parallel_cnt();
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  LOG_INFO("finish to init preread task", KR(ret), K_(preread_parallel_cnt), K(MTL_MEM_SIZE()));
  return ret;
}

int ObSSPreReadTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread is not init", KR(ret));
  } else if (OB_UNLIKELY(-1 == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(schedule_preread_task())) {
    LOG_WARN("fail to schedule preread task", KR(ret));
  } else {
    LOG_INFO("succ to start preread task", K(tg_id));
  }
  return ret;
}

void ObSSPreReadTask::destroy()
{
  destroy_preread_entry_arr();
  tg_id_ = -1;
  tenant_id_ = OB_INVALID_TENANT_ID;
  preread_parallel_cnt_ = 0;
  interval_us_ = -1;
  preread_cache_mgr_ = nullptr;
  free_list_.destroy();
  async_read_list_.destroy();
  async_write_list_.destroy();
  segment_files_.destroy();
  preread_files_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

void ObSSPreReadTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(preread_files())) {
    LOG_WARN("fail to preread file to local cache", KR(ret), K_(tenant_id));
  }

  // adjust preread_parallel_cnt if memory size changes.
  if (TC_REACH_TIME_INTERVAL(UPDATE_PARALLEL_INTERVAL_US)) {
    set_read_cache_parallel_cnt();
  }

  adjust_pre_read_task_interval();
  if (OB_TMP_FAIL(schedule_preread_task())) {
    if (OB_CANCELED == tmp_ret) {
      LOG_INFO("preread task has stopped");
    } else {
      LOG_ERROR("fail to reschedule preread task", KR(ret), K(tmp_ret), K_(interval_us));
    }
  }
}

int ObSSPreReadTask::schedule_preread_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, interval_us_, false/*repeat*/))) {
    LOG_WARN("fail to schedule ss preread task", KR(ret), K_(tg_id), K_(interval_us));
  } else {
    LOG_TRACE("succ to schedule ss preread task", K_(tg_id), K_(interval_us));
  }
  return ret;
}

void ObSSPreReadTask::destroy_preread_entry_arr()
{
  if (nullptr != preread_entry_arr_) {
    for (int64_t i = 0; i < MAX_PRE_READ_PARALLELISM; ++i) {
      preread_entry_arr_[i].~ObSSPreReadEntry();
    }
    allocator_.free(preread_entry_arr_);
    preread_entry_arr_ = nullptr;
  }
}

int ObSSPreReadTask::pre_alloc_preread_entry()
{
  int ret = OB_SUCCESS;
  const int64_t mem_size = sizeof(ObSSPreReadEntry) * MAX_PRE_READ_PARALLELISM;
  if (OB_ISNULL(preread_entry_arr_ = static_cast<ObSSPreReadEntry *>(allocator_.alloc(mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(mem_size));
  } else {
    for (int64_t i = 0; i < MAX_PRE_READ_PARALLELISM; ++i) {
      new (preread_entry_arr_ + i) ObSSPreReadEntry(allocator_);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_PRE_READ_PARALLELISM; ++i) {
      ObSSPreReadEntry *preread_entry = preread_entry_arr_ + i;
      if (OB_FAIL(free_list_.push(preread_entry))) {
        LOG_WARN("fail to push preread_entry into free_list", K(ret), K(i), K(free_list_.get_curr_total()));
      }
    }
  }

  if (OB_FAIL(ret)) {
    destroy_preread_entry_arr();
  }
  return ret;
}

int ObSSPreReadTask::preread_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*)) || OB_ISNULL(preread_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_disk_space_mgr or read_cache_mgr is null", KR(ret), KP(tnt_disk_space_mgr), KP_(preread_cache_mgr));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    clear_for_next_round();
    if (OB_FAIL(batch_get_segment_file_ids())) {
      LOG_WARN("fail to get segment file ids", KR(ret), K_(segment_files));
    } else if (OB_FAIL(async_read_segment_files())) {
      LOG_WARN("fail to async read segment_files", KR(ret));
    } else if (OB_FAIL(async_write_segment_files())) {
      LOG_WARN("fail to async write segment_files", KR(ret));
    } else if (OB_FAIL(complete_pre_read_segment_files())) {
      LOG_WARN("fail to complete preread segment_files", KR(ret));
    }
    if (preread_files_.count() > 0) {
      LOG_INFO("finish to read file to local cache", KR(ret), K(preread_files_.count()), K_(preread_files), K(async_read_list_.get_curr_total()), 
        K(async_write_list_.get_curr_total()), "all_disk_cache_info", (tnt_disk_space_mgr->get_all_disk_cache_info()),
        "tmp_file_cache_stat", (tnt_disk_space_mgr->get_tmp_file_cache_stat()), "major_macro_cache_stat", (tnt_disk_space_mgr->get_major_macro_cache_stat()),
        "segment_queue_size", (preread_cache_mgr_->preread_queue_.size()), "segment_file_map_size", (preread_cache_mgr_->get_segment_file_map_size()),
        "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObSSPreReadTask::batch_get_segment_file_ids()
{
  int ret = OB_SUCCESS;
  const int64_t total_entry_cnt =
      free_list_.get_curr_total() + async_read_list_.get_curr_total() + async_write_list_.get_curr_total();
  const int64_t remaining_parallel_cnt =
      preread_parallel_cnt_ - async_read_list_.get_curr_total() - async_write_list_.get_curr_total();
  if (OB_ISNULL(preread_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("preread_cache_mr is null", KR(ret), KP_(preread_cache_mgr));
  } else if (OB_UNLIKELY(total_entry_cnt != MAX_PRE_READ_PARALLELISM)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("preread entry leak!!!", KR(ret), K(total_entry_cnt));
  } else if (OB_UNLIKELY(remaining_parallel_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("parallel_cnt is wrong", KR(ret), K(remaining_parallel_cnt), K_(preread_parallel_cnt),
        K(async_read_list_.get_curr_total()), K(async_write_list_.get_curr_total()));
  }

  const int64_t max_cnt = MIN(remaining_parallel_cnt, free_list_.get_curr_total());
  for (int64_t i = 0; OB_SUCC(ret) && (i < max_cnt); ++i) {
    ObLink *ptr = nullptr;
    ObSegmentFileInfo *segment_file_info = nullptr;
    if (OB_FAIL(preread_cache_mgr_->preread_queue_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to pop segment_file_info", KR(ret));
      }
    } else if (OB_ISNULL(ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment_file_info is null", KR(ret));
    } else {
      segment_file_info = static_cast<ObSegmentFileInfo *>(ptr);
      ObPreReadFileMeta file_meta(segment_file_info->file_id_, segment_file_info->func_type_);
      if (OB_FAIL(segment_files_.push_back(file_meta))) {
        LOG_WARN("fail to push back file_meta", KR(ret), K(file_meta));
      }
    }

    // free memory
    OB_DELETE(ObSegmentFileInfo, attr, segment_file_info);
  }
  return ret;
}

int ObSSPreReadTask::do_async_read_segment_file(ObSSPreReadEntry &preread_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!preread_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("preread_entry should be valid", KR(ret), K(preread_entry));
  } else if (OB_UNLIKELY(preread_entry.is_read_handle_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_handle should not be valid", KR(ret), K(preread_entry.read_handle_));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(preread_entry.get_function_type());

    const MacroBlockId &file_id = preread_entry.file_meta_.file_id_;
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = file_id;
    read_info.offset_ = 0;
    read_info.size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.io_desc_.set_sealed(); // preread tmp_file must be sealed
    read_info.buf_ = preread_entry.buf_;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(read_info.mtl_tenant_id_);
    ObSSObjectStorageReader object_storage_reader;
    if (OB_FAIL(object_storage_reader.aio_read(read_info, preread_entry.read_handle_))) {
      if (OB_OBJECT_NOT_EXIST == ret) {
        // because segment file has been gc, fail to read from remote object storage
        LOG_INFO("segment file does not exist, may be already gc", K(file_id));
      } else {
        LOG_WARN("fail to aio read", KR(ret), K(read_info), K(preread_entry));
      }
    }
  }
  return ret;
}

int ObSSPreReadTask::async_read_segment_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t segment_file_cnt = segment_files_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < segment_file_cnt; ++i) {
    const ObPreReadFileMeta &file_meta = segment_files_.at(i);
    ObSSPreReadEntry *preread_entry = nullptr;
    bool need_free = true;
    bool is_need_preread = true;
    if (OB_FAIL(free_list_.pop(preread_entry))) {
      LOG_WARN("fail to pop preread_entry from free_list", KR(ret), K(free_list_.get_curr_total()));
    } else if (OB_ISNULL(preread_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread_entry should not be null", KR(ret), KP(preread_entry));
    } else if (OB_FAIL(preread_entry->init(file_meta))) {
      LOG_WARN("fail to init preread_entry", KR(ret), K(file_meta), KPC(preread_entry));
    } else if (OB_FAIL(preread_cache_mgr_->is_file_id_need_preread(file_meta.file_id_, is_need_preread))) {
      LOG_WARN("fail to check need preread", KR(ret), K_(file_meta.file_id));
    } else if (!is_need_preread) {
      // do nothing, 1.NORMAL node do not need preread, 2.FAKE node has been read whole do not need preread, 3.lru node has been remove(gc) do not need preread
    } else if (OB_FAIL(do_async_read_segment_file(*preread_entry))) {
      if (OB_OBJECT_NOT_EXIST != ret) {
        LOG_WARN("fail to do async read segment_file", KR(ret), KPC(preread_entry));
      }
    } else if (OB_FAIL(async_read_list_.push(preread_entry))) {
      LOG_ERROR("fail to push preread_entry into read_list", KR(ret), K(async_read_list_.get_curr_total()));
    } else {
      need_free = false;
    }

    if (OB_FAIL(ret) && OB_TMP_FAIL(preread_cache_mgr_->remove_lru_node(file_meta.file_id_))) {
      LOG_WARN("fail to remove lru node", KR(tmp_ret), K_(file_meta.file_id));
    }

    if (need_free && (nullptr != preread_entry)) {
      if (OB_TMP_FAIL(recycle_preread_entry(*preread_entry))) {
        LOG_WARN("fail to recycle preread_entry", KR(tmp_ret), KPC(preread_entry), K(free_list_.get_curr_total()));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next segment_file.
  }
  return ret;
}

int ObSSPreReadTask::try_alloc_preread_cache_size(
    const int64_t file_size,
    ObStorageObjectType object_type)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_UNLIKELY(file_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_size), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *)) || OB_ISNULL(preread_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk_space_mgr or read_cache_mgr is null", KR(ret), KP(disk_space_mgr), K_(preread_cache_mgr));
  } else if (OB_FAIL(disk_space_mgr->alloc_file_size(file_size, object_type, true/*is_tmp_file_read_cache*/))) {
    // try evict some files to free up space, evict file is a sync op.
    int64_t retry_cnt = 0;
    while ((OB_SERVER_OUTOF_DISK_SPACE == ret) && (retry_cnt < ObBaseFileManager::MAX_RETRY_ALLOC_CACHE_SPACE_CNT)) {
      ret = OB_SUCCESS;
      if (OB_FAIL(preread_cache_mgr_->evict_tail_lru_node())) {
        LOG_WARN("fail to evict tail lru node", KR(ret));
      } else if (OB_FAIL(disk_space_mgr->alloc_file_size(file_size, object_type, true/*is_tmp_file_read_cache*/))) {
        if (TC_REACH_TIME_INTERVAL(ObBaseFileManager::PRINT_LOG_INTERVAL)) {
          LOG_WARN("fail to alloc preread cache size", KR(ret), K(file_size),
                   K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
        }
      }
      ++retry_cnt;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to alloc preread cache size", KR(ret), K(file_size), K(object_type), "object_type_str", get_storage_objet_type_str(object_type), K(retry_cnt));
    }
  }
  return ret;
}

int ObSSPreReadTask::do_async_write_segment_file(ObSSPreReadEntry &preread_entry)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!preread_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("preread_entry should be valid", KR(ret), K(preread_entry));
  } else if (OB_UNLIKELY(!preread_entry.is_read_handle_valid() || preread_entry.is_write_handle_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_handle should be valid and write_handle should be invalid", KR(ret), K(preread_entry));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(preread_entry.get_function_type());

    const MacroBlockId &file_id = preread_entry.file_meta_.file_id_;
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = preread_entry.read_handle_.get_buffer();
    write_info.offset_ = 0;
    write_info.size_ = preread_entry.read_handle_.get_data_size();
    write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    // Warning: must set unsealed, otherwise tmpfile_write_cache thread will evict segment file to object storage
    write_info.io_desc_.set_unsealed();
    ObSSLocalCacheWriter local_cache_writer;
    const ObStorageObjectType object_type = file_id.storage_object_type();
    if (OB_FAIL(preread_entry.write_handle_.set_macro_block_id(file_id))) {
      LOG_WARN("fail to set macro block id", KR(ret), K(file_id));
    } else if (OB_FAIL(try_alloc_preread_cache_size(write_info.size_, object_type))) {
      LOG_WARN("fail to try alloc preread cache size", KR(ret), K_(write_info.size), K(object_type), K(file_id));
    } else if (OB_FAIL(local_cache_writer.aio_write(write_info, preread_entry.write_handle_))) {
      // if tmp_file aio_write report OB_NO_SUCH_FILE_OR_DIRECTORY need create tmp_file_dir
      if ((OB_NO_SUCH_FILE_OR_DIRECTORY == ret) && (ObStorageObjectType::TMP_FILE == object_type)) {
        ret = OB_SUCCESS;
        if (OB_FAIL(OB_DIR_MGR.create_tmp_file_dir(tenant_id_, MTL_EPOCH_ID(), file_id.second_id()))) {
          LOG_WARN("fail to create tmp file dir", KR(ret), K(file_id));
        } else if (OB_FAIL(preread_entry.write_handle_.set_macro_block_id(file_id))) {
          LOG_WARN("fail to set macro block id", KR(ret), K(file_id));
        } else if (OB_FAIL(local_cache_writer.aio_write(write_info, preread_entry.write_handle_))) {
          LOG_WARN("fail to aio write", KR(ret), K(write_info), K_(preread_entry.write_handle), K(file_id));
        }
      } else {
        LOG_WARN("fail to aio write", KR(ret), K(write_info), K_(preread_entry.write_handle), K(file_id));
      }
      // if create dir failed or aio write failed, need free file size
      if (OB_FAIL(ret)) {
        ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
        if (OB_NOT_NULL(disk_space_mgr) &&
            OB_TMP_FAIL(disk_space_mgr->free_file_size(write_info.size_, object_type, true/*is_tmp_file_read_cache*/))) {
          LOG_WARN("fail to free preread cache size", KR(tmp_ret), K_(write_info.size),
                   K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
        }
      }
    }
  }
  return ret;
}

int ObSSPreReadTask::async_write_segment_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(disk_space_mgr), K_(tenant_id));
  }
  const int64_t io_cnt = async_read_list_.get_curr_total();
  for (int64_t i = 0; OB_SUCC(ret) && (i < io_cnt); ++i) {
    ObSSPreReadEntry *preread_entry = nullptr;
    bool need_free = true;
    bool is_finished = false;
    if (OB_FAIL(async_read_list_.pop(preread_entry))) {
      LOG_WARN("fail to pop preread_entry from async_read_list", KR(ret), K(i), K(io_cnt));
    } else if (OB_ISNULL(preread_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread_entry should not be null", KR(ret), KP(preread_entry));
    } else if (OB_UNLIKELY(!preread_entry->is_valid() || !preread_entry->is_read_handle_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread_entry and read_handle should be valid", KR(ret), KPC(preread_entry));
    } else if (OB_FAIL(preread_entry->read_handle_.check_is_finished(is_finished))) {
      LOG_WARN("fail to check read_handle is_finished", KR(ret), K_(preread_entry->read_handle));
    } else {
      const MacroBlockId &file_id = preread_entry->file_meta_.file_id_;
      if (is_finished) {
        bool is_need_preread = true;
        if (OB_FAIL(preread_entry->read_handle_.wait())) {
          LOG_WARN("fail to wait read_handle", KR(ret), K_(preread_entry->read_handle));
        } else if (OB_FAIL(preread_cache_mgr_->is_file_id_need_preread(file_id, is_need_preread))) {
          LOG_WARN("fail to check need preread", KR(ret), K(file_id));
        } else if (!is_need_preread) {
          // do nothing, 1.NORMAL node do not need preread, 2.FAKE node has been read whole do not need preread, 3.lru node has been remove(gc) do not need preread
        } else if (OB_FAIL(do_async_write_segment_file(*preread_entry))) {
          LOG_WARN("fail to do async write segment_file", KR(ret), KPC(preread_entry));
        } else if (OB_FAIL(async_write_list_.push(preread_entry))) {
          LOG_ERROR("fail to push preread_entry into write_list", KR(ret), K(async_write_list_.get_curr_total()));
        } else {
          need_free = false;
        }
      } else {
        if (OB_FAIL(async_read_list_.push(preread_entry))) {
          LOG_ERROR("fail to push preread_entry into async_read_list", KR(ret), K(async_read_list_.get_curr_total()));
        } else {
          need_free = false;
        }
      }
    }

    if (OB_FAIL(ret) && (nullptr != preread_entry) && preread_entry->is_valid()) {
      const MacroBlockId &file_id = preread_entry->file_meta_.file_id_;
      if (OB_TMP_FAIL(preread_cache_mgr_->remove_lru_node(file_id))) {
        LOG_WARN("fail to remove lru node", KR(tmp_ret), K(file_id));
      }
    }
    if (need_free && (nullptr != preread_entry)) {
      if (OB_TMP_FAIL(recycle_preread_entry(*preread_entry))) {
        LOG_WARN("fail to recycle preread_entry", KR(tmp_ret), KPC(preread_entry), K(free_list_.get_curr_total()));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next preread_entry
  }
  return ret;
}

int ObSSPreReadTask::complete_pre_read_segment_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(preread_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("preread_cache_mr is null", KR(ret), KP_(preread_cache_mgr));
  }
  const int64_t io_cnt = async_write_list_.get_curr_total();
  for (int64_t i = 0; OB_SUCC(ret) && (i < io_cnt); ++i) {
    bool is_finished = false;
    bool need_free = true;
    ObSSPreReadEntry *preread_entry = nullptr;
    if (OB_FAIL(async_write_list_.pop(preread_entry))) {
      LOG_WARN("fail to pop preread_entry from async_write_list", KR(ret), K(i), K(io_cnt));
    } else if (OB_ISNULL(preread_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread_entry should not be null", KR(ret), KP(preread_entry));
    } else if (OB_UNLIKELY(!preread_entry->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread_entry should be valid", KR(ret), KPC(preread_entry));
    } else if (OB_UNLIKELY(!preread_entry->is_read_handle_valid() || !preread_entry->is_write_handle_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read_handle and write_handle should be valid", KR(ret), KPC(preread_entry));
    } else if (OB_FAIL(preread_entry->write_handle_.check_is_finished(is_finished))) {
      LOG_WARN("fail to check io_handle is_finished", KR(ret), K_(preread_entry->write_handle));
    } else {
      const MacroBlockId &file_id = preread_entry->file_meta_.file_id_;
      if (is_finished) {
        if (OB_FAIL(preread_entry->write_handle_.wait())) {
          LOG_WARN("fail to wait write_handle", KR(ret), K_(preread_entry->write_handle));
        } else {
          // update lru_node state to NORMAL
          if (OB_FAIL(preread_cache_mgr_->update_to_normal_status(file_id, preread_entry->write_handle_.get_data_size()))) {
            LOG_WARN("fail to update lru node status to normal", KR(ret), K(file_id), K_(preread_entry->write_handle));
          } else if (OB_FAIL(preread_files_.push_back(preread_entry->file_meta_))){
            LOG_WARN("fail to push file_id into preread_files", KR(ret), K(file_id));
          }
        }
      } else {
        if (OB_FAIL(async_write_list_.push(preread_entry))) {
          LOG_ERROR("fail to push preread_entry into async_write_list", KR(ret), K(async_write_list_.get_curr_total()));
        } else {
          need_free = false;
        }
      }
    }

    // if fail to write, remove lru node.
    if (OB_FAIL(ret) && (nullptr != preread_entry) && preread_entry->is_valid()) {
      const MacroBlockId &file_id = preread_entry->file_meta_.file_id_;
      if (OB_TMP_FAIL(preread_cache_mgr_->remove_lru_node(file_id))) {
        LOG_WARN("fail to remove lru node", KR(tmp_ret), K(file_id));
      }
    }
    if (need_free && (nullptr != preread_entry)) {
      if (OB_TMP_FAIL(recycle_preread_entry(*preread_entry))) {
        LOG_WARN("fail to recycle preread_entry", KR(tmp_ret), KPC(preread_entry), K(free_list_.get_curr_total()));
      }
    }
    ret = OB_SUCCESS; // ignore error code, process next preread_entry
  }
  return ret;
}

int ObSSPreReadTask::recycle_preread_entry(ObSSPreReadEntry &preread_entry)
{
  int ret = OB_SUCCESS;
  preread_entry.reset();
  if (OB_FAIL(free_list_.push(&preread_entry))) {
    LOG_ERROR("fail to push preread_entry into free_list", KR(ret), K(preread_entry), K(free_list_.get_curr_total()));
  }
  return ret;
}

void ObSSPreReadTask::clear_for_next_round()
{
  segment_files_.reuse();
  preread_files_.reuse();
}

void ObSSPreReadTask::set_read_cache_parallel_cnt()
{
  // default parallel is 5, max parallel is 30, each 1GB of tenant memory increase one parallel
  preread_parallel_cnt_ = MIN(DEFAULT_PRE_READ_PARALLELISM + MTL_MEM_SIZE() / GB, MAX_PRE_READ_PARALLELISM);
}

void ObSSPreReadTask::adjust_pre_read_task_interval()
{
  // exist unprocessed segment_file or unfinished async write io on loacl disk.
  if ((OB_NOT_NULL(preread_cache_mgr_) && (preread_cache_mgr_->preread_queue_.size() > 0) &&
          (free_list_.get_curr_total() > 0)) ||
      (async_write_list_.get_curr_total() > 0)) {
    interval_us_ = FAST_SCHEDULE_INTERVAL_US;
  } else if (async_read_list_.get_curr_total() > 0) {  // exist unfinished async read io on object storage.
    interval_us_ = MODERATE_SCHEDULE_INTERVAL_US;
  } else {
    interval_us_ = SLOW_SCHEDULE_INTERVAL_US;
  }
}
} // storage
} // oceanbase
