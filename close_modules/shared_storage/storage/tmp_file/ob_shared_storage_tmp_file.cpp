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

#include "observer/omt/ob_th_worker.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/tmp_file/ob_shared_storage_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{
void ObSSTmpFileInfo::reset()
{
  aggregate_read_io_cnt_ = 0;
  ObTmpFileInfo::reset();
}

/* -------------------------- ObSSTmpFileFlushContext --------------------------- */

ObSSTmpFileFlushContext::ObSSTmpFileFlushContext(const blocksstable::MacroBlockId &id,
                                                 const common::ObIOFlag &io_desc,
                                                 const int64_t io_timeout_ms,
                                                 const int64_t max_flush_page_count)
    : is_finished_(false),
      id_(id),
      io_desc_(io_desc),
      io_timeout_ms_(io_timeout_ms),
      object_handle_(nullptr),
      flush_buff_(nullptr),
      cur_flush_page_count_(0),
      max_flush_page_count_(max_flush_page_count),
      write_info_offset_(-1),
      this_batch_flush_data_size_(0),
      cur_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      cur_virtual_page_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
      can_seal_(false)
{
}

void ObSSTmpFileFlushContext::init(
    const int64_t write_info_offset,
    const uint32_t cur_page_id,
    const int64_t cur_virtual_page_id)
{
  cur_flush_page_count_ = 0;
  write_info_offset_ = write_info_offset;
  cur_page_id_ = cur_page_id;
  cur_virtual_page_id_ = cur_virtual_page_id;
}

void ObSSTmpFileFlushContext::append_batch(
    blocksstable::ObStorageObjectHandle *object_handle,
    char *flush_buff)
{
  // Set `object_handle_` and `flush_buff_` for this batch.
  object_handle_ = object_handle;
  flush_buff_ = flush_buff;
  // Reset previous status for this batch.
  this_batch_flush_data_size_ = 0;
  can_seal_ = false;
}

int ObSSTmpFileFlushContext::flush_io()
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectWriteInfo info;
  blocksstable::MacroBlockId id = id_;
  int64_t fd = id_.second_id();
  const int64_t offset_in_segment = write_info_offset_ % ObTmpFileGlobal::SS_BLOCK_SIZE;
  if (OB_UNLIKELY(offset_in_segment + this_batch_flush_data_size_ > ObTmpFileGlobal::SS_BLOCK_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush size is invalid", KR(ret), KPC(this));
  } else {
    // Fill in write info and async write to disk.
    info.io_desc_ = io_desc_;
    info.io_timeout_ms_ = io_timeout_ms_;
    info.mtl_tenant_id_ = MTL_ID();
    info.buffer_ = flush_buff_;
    info.offset_ = offset_in_segment;
    // TODO: wanyue.wy
    // pass this_batch_flush_data_size_ to info.val_len after the logic of file manager has optimized
    info.size_ = common::upper_align(
        this_batch_flush_data_size_,
        ObTmpFileGlobal::ALLOC_PAGE_SIZE); // IO align to wbp page size (8KB)
    info.tmp_file_valid_length_ = offset_in_segment + this_batch_flush_data_size_;
    if (can_seal_) {
      info.io_desc_.set_sealed();
    } else {
      info.io_desc_.set_unsealed();
    }
    id.set_third_id(write_info_offset_ / ObTmpFileGlobal::SS_BLOCK_SIZE);

    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(id, info, *object_handle_))) {
      LOG_WARN("fail to async write", KR(ret), K(fd), K(info));
    }
  }

  LOG_DEBUG("async flush io", KR(ret), K(fd), K(id), K(info));
  return ret;
}

void ObSSTmpFileFlushContext::append_page(const int64_t this_page_cpy_len,
                                          const uint32_t next_page_id,
                                          const int64_t next_virtual_page_id)
{
  // Accumulate `this_page_cpy_len` to `this_batch_flush_data_size` and continue to next page.
  this_batch_flush_data_size_ += this_page_cpy_len;
  cur_flush_page_count_++;
  cur_page_id_ = next_page_id;
  cur_virtual_page_id_ = next_virtual_page_id;
  can_seal_ = this_batch_flush_data_size_ == get_batch_expected_flush_size();
}

void ObSSTmpFileFlushContext::finish_batch()
{
  // Update `write_info_offset_` for next batch.
  write_info_offset_ += this_batch_flush_data_size_;
}

/* -------------------------- ObSharedStorageTmpFile --------------------------- */

ObSharedStorageTmpFile::ObSharedStorageTmpFile()
    : id_(), aggregate_read_io_cnt_(0)
{
  mode_ = ObTmpFileMode::SHARED_STORAGE;
}

ObSharedStorageTmpFile::~ObSharedStorageTmpFile()
{
  reset();
}

int ObSharedStorageTmpFile::init(const uint64_t tenant_id,
                                 const blocksstable::MacroBlockId id,
                                 ObTmpWriteBufferPool *wbp,
                                 ObSSTmpFileFlushManager *flush_mgr,
                                 ObSSTmpFileRemoveManager *remove_mgr,
                                 ObIAllocator *callback_allocator,
                                 ObIAllocator *wbp_index_cache_allocator,
                                 ObIAllocator *wbp_index_cache_bkt_allocator,
                                 const char* label)
{
  int ret = OB_SUCCESS;
  const int64_t fd = id.second_id();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_ISNULL(wbp) ||
             OB_ISNULL(flush_mgr) || OB_ISNULL(remove_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(tenant_id), KP(wbp), KP(flush_mgr), KP(remove_mgr));
  } else if (OB_FAIL(ObITmpFile::init(tenant_id, ObTmpFileGlobal::SHARE_STORAGE_DIR_ID,
                                      fd, wbp, &flush_mgr->get_flush_prio_mgr(),
                                      callback_allocator,
                                      wbp_index_cache_allocator,
                                      wbp_index_cache_bkt_allocator,
                                      label))) {
    LOG_WARN("init ObITmpFile failed", KR(ret), K(fd), K(tenant_id), KP(label));
  } else {
    id_ = id;
    flush_mgr_ = flush_mgr;
    remove_mgr_ = remove_mgr;
  }

  return ret;
}

void ObSharedStorageTmpFile::reset()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    id_.reset();
    /******for virtual table begin******/
    aggregate_read_io_cnt_ = 0;
    /******for virtual table end  ******/
    ObITmpFile::reset();
  }
}

int ObSharedStorageTmpFile::release_resource()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    LOG_INFO("tmp file release_resource start", KR(ret), K(fd_), KPC(this));
    const bool all_data_in_mem = cal_wbp_begin_offset_() == 0;
    while (OB_SUCC(ret) && cached_page_nums_ > 0) {
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      if (OB_FAIL(wbp_->free_page(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
        LOG_ERROR("fail to free page", KR(ret), K(fd_), K(cached_page_nums_), K(begin_page_id_));
      } else {
        // clean page cache, ignore unfilled page
        ObTmpPageCacheKey key(fd_, 0, begin_page_virtual_id_, tenant_id_);
        ObTmpPageCache::get_instance().erase(key);

        begin_page_id_ = next_page_id;
        if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_) {
          end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        } else {
          begin_page_virtual_id_ += 1;
        }
        cached_page_nums_--;
      }
    }

    if (OB_SUCC(ret)) {
      if (cached_page_nums_ != 0 ||
          begin_page_id_ != ObTmpFileGlobal::INVALID_PAGE_ID ||
          end_page_id_ != ObTmpFileGlobal::INVALID_PAGE_ID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("clear shared storage temporary file succeed but meta data is incorrect",
                  KR(ret), K(fd_), K(cached_page_nums_), K(begin_page_id_), K(end_page_id_), K(file_size_));
      }
    } else {
      LOG_WARN("clear shared storage temporary file failed", KR(ret), K(fd_),
              K(cached_page_nums_), K(begin_page_id_), K(end_page_id_));
    }

    // Push async remove task for removing persistent data
    if (!all_data_in_mem) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(remove_mgr_->remove_task_enqueue(id_, file_size_))) {
        LOG_ERROR("fail to push remove task", K(tmp_ret), K(fd_), K(file_size_));
      }
    }
  }
  return ret;
}

int ObSharedStorageTmpFile::inner_delete_file_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_prio_mgr_->remove_file(*this))) {
    LOG_WARN("fail to remove file from flush priority manager", KR(ret), KPC(this));
  }
  return ret;
}

int ObSharedStorageTmpFile::inner_seal_()
{
  int ret = OB_SUCCESS;
  common::TCRWLock::WLockGuard guard(meta_lock_);
  if (cached_page_nums_ == 0 && file_size_ % ObTmpFileGlobal::SS_BLOCK_SIZE != 0) {
    blocksstable::MacroBlockId id = id_;
    id.set_third_id(get_block_virtual_id_(file_size_, true));
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.seal_object(id, 0/*ls_epoch_id*/))) {
      LOG_WARN("fail to seal file", KR(ret), K(fd_), K(id));
    } else {
      is_sealed_ = true;
    }
    LOG_INFO("seal uncompleted block", KR(ret), K(fd_), K(id_), K(is_sealed_));
  } else {
    is_sealed_ = true;
  }
  LOG_INFO("inner seal over", KR(ret), K(fd_), K(is_sealed_));
  return ret;
}

int ObSharedStorageTmpFile::inner_read_from_disk_(const int64_t expected_read_disk_size,
                                                  ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t start_read_offset = io_ctx.get_read_offset_in_file();
  const int64_t begin_block_id = get_block_virtual_id_(start_read_offset, false);
  const int64_t end_block_id = get_block_virtual_id_(start_read_offset + expected_read_disk_size, true);
  int64_t total_kv_cache_page_read_cnt = 0;
  int64_t total_uncached_page_read_cnt = 0;
  int64_t kv_cache_page_read_hits = 0;
  int64_t uncached_page_read_hits = 0;
  int64_t aggregate_read_io_cnt = 0;

  // Iterate to read each block.
  int64_t remain_read_size = expected_read_disk_size;
  for (int64_t block_index = begin_block_id; OB_SUCC(ret) && block_index <= end_block_id && 0 < remain_read_size; block_index++) {
    const int64_t begin_read_offset_in_block = block_index == begin_block_id ?
                                               get_offset_in_block_(start_read_offset) : 0;
    const int64_t end_read_offset_in_block =  block_index == end_block_id ?
                                              begin_read_offset_in_block + remain_read_size :
                                              ObTmpFileGlobal::SS_BLOCK_SIZE;
    const int64_t read_size = end_read_offset_in_block - begin_read_offset_in_block;
    if (OB_UNLIKELY(end_read_offset_in_block - begin_read_offset_in_block <= 0 ||
                    begin_read_offset_in_block < 0 ||
                    end_read_offset_in_block > ObTmpFileGlobal::SS_BLOCK_SIZE)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected read offset", KR(ret), K(fd_), K(block_index), K(expected_read_disk_size),
               K(start_read_offset), K(begin_block_id), K(end_block_id),
               K(begin_read_offset_in_block), K(end_read_offset_in_block),
               K(remain_read_size), K(io_ctx), KPC(this));
    } else if (io_ctx.is_disable_page_cache()) {
      if (OB_FAIL(inner_direct_read_from_block_(block_index,
                                                begin_read_offset_in_block,
                                                end_read_offset_in_block,
                                                io_ctx))) {
        LOG_WARN("fail to direct read from block",
            KR(ret), K(fd_), K(block_index), K(begin_block_id), K(end_block_id),
            K(begin_read_offset_in_block), K(end_read_offset_in_block),
            K(remain_read_size), K(expected_read_disk_size),
            K(io_ctx), KPC(this));
      } else {
        uncached_page_read_hits++;
        total_uncached_page_read_cnt += (get_page_end_offset_(io_ctx.get_read_offset_in_file()) -
                                         get_page_begin_offset_(io_ctx.get_read_offset_in_file() - read_size)) /
                                        ObTmpFileGlobal::ALLOC_PAGE_SIZE;
      }
    } else {
      if (OB_FAIL(inner_aggregate_read_from_block_(block_index,
                                                   begin_read_offset_in_block,
                                                   end_read_offset_in_block,
                                                   io_ctx,
                                                   total_kv_cache_page_read_cnt,
                                                   total_uncached_page_read_cnt,
                                                   kv_cache_page_read_hits,
                                                   uncached_page_read_hits,
                                                   aggregate_read_io_cnt))) {
        LOG_WARN("fail to aggregate read from block",
            KR(ret), K(fd_), K(block_index), K(begin_block_id), K(end_block_id),
            K(begin_read_offset_in_block), K(end_read_offset_in_block),
            K(remain_read_size),K(expected_read_disk_size),
            K(io_ctx), KPC(this));
      }
    }
    // Update read offset and read size.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(read_size));
    } else {
      remain_read_size -= read_size;
    }
    LOG_DEBUG("read from block over",
              KR(ret), K(fd_), K(block_index), K(begin_block_id), K(end_block_id),
              K(begin_read_offset_in_block), K(end_read_offset_in_block),
              K(remain_read_size), K(expected_read_disk_size),
              K(io_ctx), KPC(this));
  } // end for

  if (OB_SUCC(ret)) {
    io_ctx.update_read_kv_cache_page_stat(total_kv_cache_page_read_cnt, kv_cache_page_read_hits);
    io_ctx.update_ss_read_uncached_page_stat(total_uncached_page_read_cnt, uncached_page_read_hits,
                                             aggregate_read_io_cnt);
  }
  LOG_DEBUG("read from disk over", K(ret), K(expected_read_disk_size), K(io_ctx), KPC(this));
  return ret;
}

int ObSharedStorageTmpFile::inner_direct_read_from_block_(
    const int64_t block_index,
    const int64_t begin_read_offset_in_block,
    const int64_t end_read_offset_in_block,
    ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t expected_read_size = end_read_offset_in_block - begin_read_offset_in_block;

  if (OB_UNLIKELY(block_index < 0 || expected_read_size <= 0 ||
                  expected_read_size > ObTmpFileGlobal::SS_BLOCK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(block_index),
                                 K(begin_read_offset_in_block),
                                 K(end_read_offset_in_block));
  } else {
    blocksstable::MacroBlockId subfile_id = id_;
    subfile_id.set_third_id(block_index);
    char *read_buf = io_ctx.get_todo_buffer();
    ObTmpFileIOCtx::ObIOReadHandle io_read_handle(read_buf,
                                                  0 /*offset_in_src_data_buf_*/,
                                                  expected_read_size);
    ObTmpPageCacheReadInfo read_info;
    if (OB_FAIL(io_ctx.get_io_handles().push_back(io_read_handle))) {
      LOG_WARN("Fail to push back into io_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(read_info.init_read(subfile_id,
                                           expected_read_size, begin_read_offset_in_block,
                                           io_ctx.get_io_flag(), io_ctx.get_io_timeout_ms(),
                                           &io_ctx.get_io_handles().at(io_ctx.get_io_handles().count()-1).handle_))) {
      LOG_WARN("fail to init sn read info", KR(ret), K(fd_), K(expected_read_size),
                                            K(begin_read_offset_in_block), K(io_ctx));
    } else if (OB_FAIL(ObTmpPageCache::get_instance().direct_read(read_info, *callback_allocator_))) {
      LOG_WARN("fail to direct_read", KR(ret), K(fd_), K(read_info), K(io_ctx), KP(callback_allocator_));
    }
  }

  LOG_DEBUG("inner_direct_read_from_block over", KR(ret), K(fd_), K(block_index),
            K(begin_read_offset_in_block), K(end_read_offset_in_block),
            K(expected_read_size), K(io_ctx), KPC(this));
  return ret;
}

int ObSharedStorageTmpFile::inner_aggregate_read_from_block_(
    const int64_t block_index,
    const int64_t begin_read_offset_in_block,
    const int64_t end_read_offset_in_block,
    ObTmpFileIOCtx &io_ctx,
    int64_t &total_kv_cache_page_read_cnt,
    int64_t &total_uncached_page_read_cnt,
    int64_t &kv_cache_page_read_hits,
    int64_t &uncached_page_read_hits,
    int64_t &aggregate_read_io_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t begin_read_page_virtual_id_in_block = get_page_virtual_id_(begin_read_offset_in_block, false);
  const int64_t end_read_page_virtual_id_in_block = get_page_virtual_id_(end_read_offset_in_block, true);
  const int64_t begin_read_page_virtual_id = block_index * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS +
                                             begin_read_page_virtual_id_in_block;
  const int64_t end_read_page_virtual_id = block_index * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS +
                                           end_read_page_virtual_id_in_block;
  const int64_t expected_read_page_num = end_read_page_virtual_id - begin_read_page_virtual_id + 1;
  const bool need_aggregate_read = 2 * expected_read_page_num >= ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS || io_ctx.is_prefetch();
  int64_t last_flushed_page_virtual_id = get_page_virtual_id_(cal_wbp_begin_offset_(), true);
  const bool is_last_block = block_index == get_last_flushed_block_id_(last_flushed_page_virtual_id);
  int64_t flushed_page_num_in_last_block = last_flushed_page_virtual_id - begin_read_page_virtual_id + 1;
  int64_t aggregate_expected_read_page_num = is_last_block ?
                                        flushed_page_num_in_last_block :
                                        ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS - begin_read_page_virtual_id_in_block;
  int64_t total_read_page_num = -1;
  common::ObArray<ObTmpFileIOCtx::ObPageCacheHandle> page_cache_handles;
  common::ObArray<std::pair<ObTmpPageCacheKey, int64_t>> page_infos;

  if (OB_UNLIKELY(expected_read_page_num <= 0 || expected_read_page_num > ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected page num", KR(ret), K(fd_), K(block_index),
             K(begin_read_offset_in_block), K(end_read_offset_in_block));
  } else if (OB_FAIL(collect_page_io_infos_(block_index, begin_read_offset_in_block, end_read_offset_in_block,
                                            need_aggregate_read, io_ctx, page_cache_handles, page_infos))) {
    LOG_WARN("fail to collect page io infos", KR(ret), K(fd_), K(block_index),
             K(begin_read_offset_in_block), K(end_read_offset_in_block), K(need_aggregate_read), K(io_ctx));
  } else if (FALSE_IT(total_read_page_num = page_cache_handles.count() + page_infos.count())) {
  } else if (OB_UNLIKELY(total_read_page_num != expected_read_page_num &&
                         total_read_page_num != aggregate_expected_read_page_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected page num", KR(ret), K(fd_), K(block_index), K(need_aggregate_read),
             K(begin_read_offset_in_block), K(end_read_offset_in_block),
             K(page_cache_handles.count()), K(page_infos.count()),
             K(aggregate_expected_read_page_num), K(flushed_page_num_in_last_block));
  } else if (page_infos.empty()) {
    // all page cache hit
    for (int64_t i = 0; OB_SUCC(ret) && i < page_cache_handles.count(); ++i) {
      if (OB_FAIL(io_ctx.get_page_cache_handles().push_back(page_cache_handles.at(i)))) {
        LOG_WARN("Fail to push back into page_handles", KR(ret), K(fd_), K(page_cache_handles.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      total_kv_cache_page_read_cnt += expected_read_page_num;
      kv_cache_page_read_hits++;
    }
  } else {
    blocksstable::MacroBlockId subfile_id = id_;
    subfile_id.set_third_id(block_index);
    ObTmpPageCacheReadInfo read_info;
    char *read_buf = io_ctx.get_todo_buffer();
    const int64_t expected_read_size = end_read_offset_in_block - begin_read_offset_in_block;
    const int64_t align_read_begin_offset_in_block = begin_read_page_virtual_id_in_block *
                                                     ObTmpFileGlobal::ALLOC_PAGE_SIZE;
    // ObIOReadHandle will copy `io_buffer + io_read_handle.offset_` to
    // `user_buffer` with `io_read_handle.size_`. `io_buffer` is the data
    // buffer in object handle. This copy is happen in ObTmpFileIOCtx::wait().
    ObTmpFileIOCtx::ObIOReadHandle io_read_handle(
        read_buf /* user_buffer */,
        begin_read_offset_in_block - align_read_begin_offset_in_block/* offset_ */,
        expected_read_size /* size_ */);

    int64_t io_read_size = 0;
    if (need_aggregate_read) {
      io_read_size = min(aggregate_expected_read_page_num * ObTmpFileGlobal::ALLOC_PAGE_SIZE,
                         ObTmpFileGlobal::SS_BLOCK_SIZE - align_read_begin_offset_in_block);
      LOG_DEBUG("calculate io_read_size cmp", K(fd_), K(io_read_size), K(align_read_begin_offset_in_block),
          K(flushed_page_num_in_last_block), K(aggregate_expected_read_page_num));
    } else {
      io_read_size = ObTmpFileGlobal::ALLOC_PAGE_SIZE * (end_read_page_virtual_id_in_block -
                                                   begin_read_page_virtual_id_in_block + 1);
    }
    if (OB_UNLIKELY(io_read_size < expected_read_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected io read size", KR(ret), K(fd_), K(io_read_size), K(expected_read_size),
               K(block_index), K(begin_read_offset_in_block), K(end_read_offset_in_block),
               K(need_aggregate_read), KPC(this));
    } else if (io_read_size > (get_page_end_offset_(end_read_offset_in_block) -
                               get_page_begin_offset_(begin_read_offset_in_block))
               && !need_aggregate_read) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected io read size", KR(ret), K(fd_), K(io_read_size), K(expected_read_size),
                K(block_index), K(begin_read_offset_in_block), K(end_read_page_virtual_id),
                K(need_aggregate_read), KPC(this));
    } else if (OB_FAIL(io_ctx.get_io_handles().push_back(io_read_handle))) {
      LOG_WARN("fail to push ObIOReadHandle", KR(ret), K(fd_), K(io_read_handle));
    } else if (OB_FAIL(read_info.init_read(subfile_id, io_read_size, align_read_begin_offset_in_block,
                                           io_ctx.get_io_flag(), io_ctx.get_io_timeout_ms(),
                                           &io_ctx.get_io_handles().at(io_ctx.get_io_handles().count()-1).handle_))) {
      LOG_WARN("fail to init read info", KR(ret), K(fd_), K(io_read_size),
                K(align_read_begin_offset_in_block), K(io_ctx));
    } else if (OB_FAIL(ObTmpPageCache::get_instance().aggregate_read(page_infos, read_info, *callback_allocator_))) {
      LOG_WARN("shared storage temporary file fail to prefetch", KR(ret), K(fd_), K(read_info));
    } else {
      total_uncached_page_read_cnt += expected_read_page_num;
      uncached_page_read_hits++;
      if (need_aggregate_read) {
        aggregate_read_io_cnt++;
      }
    }
    LOG_DEBUG("aggregate read over", KR(ret), K(fd_), K(io_read_size), K(need_aggregate_read),
              K(align_read_begin_offset_in_block), K(io_read_handle), K(io_ctx));
  }

  LOG_DEBUG("inner_aggregate_read_from_block over", KR(ret), K(fd_), K(block_index),
            K(need_aggregate_read), K(flushed_page_num_in_last_block),
            K(begin_read_offset_in_block), K(end_read_offset_in_block), K(expected_read_page_num),
            K(begin_read_page_virtual_id_in_block), K(end_read_page_virtual_id_in_block),
            K(page_cache_handles.count()), K(page_infos.count()),
            K(io_ctx), KPC(this));
  return ret;
}

int ObSharedStorageTmpFile::collect_page_io_infos_(const int64_t block_index,
                                                   const int64_t begin_read_offset_in_block,
                                                   const int64_t end_read_offset_in_block,
                                                   const bool need_aggregate_read,
                                                   ObTmpFileIOCtx &io_ctx,
                                                   common::ObIArray<ObTmpFileIOCtx::ObPageCacheHandle> &page_cache_handles,
                                                   common::ObIArray<std::pair<ObTmpPageCacheKey, int64_t>> &page_infos)
{
  int ret = OB_SUCCESS;
  const int64_t begin_read_page_virtual_id = block_index * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS +
                                             get_page_virtual_id_(begin_read_offset_in_block, false);
  const int64_t end_read_page_virtual_id = block_index * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS +
                                           get_page_virtual_id_(end_read_offset_in_block, true);
  int64_t total_read_size = 0;
  for (int64_t vir_page_id = begin_read_page_virtual_id;
       OB_SUCC(ret) && vir_page_id <= end_read_page_virtual_id;
       vir_page_id++) {
    const int64_t cur_begin_offset_in_block = vir_page_id == begin_read_page_virtual_id ?
                                              begin_read_offset_in_block :
                                              get_offset_in_block_(get_page_begin_offset_by_virtual_id_(vir_page_id));
    const int64_t cur_end_offset_in_block = vir_page_id == end_read_page_virtual_id ?
                                            end_read_offset_in_block :
                                            get_offset_in_block_(get_page_end_offset_by_virtual_id_(vir_page_id));
    const int64_t read_size = cur_end_offset_in_block - cur_begin_offset_in_block;
    const bool is_end_flushed_page = (vir_page_id == get_page_virtual_id_(file_size_, true /*is_open_interval*/));
    const uint64_t unfilled_page_length = is_end_flushed_page ?
                                          get_offset_in_page_(file_size_) : 0;
    ObTmpPageCacheKey key(fd_, unfilled_page_length, vir_page_id, tenant_id_);
    ObTmpPageValueHandle p_handle;
    if (OB_SUCC(ObTmpPageCache::get_instance().get_page(key, p_handle))) {
      // Pin this page in the kvcache through the handle.
      ObTmpFileIOCtx::ObPageCacheHandle page_handle(
          io_ctx.get_todo_buffer() + total_read_size, get_offset_in_page_(cur_begin_offset_in_block), read_size);
      page_handle.page_handle_.move_from(p_handle);
      if (OB_UNLIKELY(!page_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected page handle", KR(ret), K(fd_), K(key), K(page_handle), K(block_index),
                 K(begin_read_offset_in_block), K(end_read_offset_in_block),
                 K(cur_begin_offset_in_block), K(cur_end_offset_in_block));
      } else if (OB_FAIL(page_cache_handles.push_back(page_handle))) {
        LOG_WARN("Fail to push back into page_handles", KR(ret), K(fd_), K(key));
      }
    } else if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("fail to get page", KR(ret), K(fd_), K(key));
    } else {
      ret = OB_SUCCESS;
      const int64_t page_offset_in_data_buf = get_page_begin_offset_(cur_begin_offset_in_block) -
                                              get_page_begin_offset_(begin_read_offset_in_block);
      // Cache miss, push page_io_info to read from io.
      if (OB_FAIL(page_infos.push_back(std::make_pair(key, page_offset_in_data_buf)))) {
        LOG_WARN("fail to push back key", KR(ret), K(key), K(page_offset_in_data_buf));
      }
    }
    total_read_size += read_size;
  }
  LOG_DEBUG("collect expected range of page infos over", KR(ret), K(fd_), K(block_index),
            K(begin_read_offset_in_block), K(end_read_offset_in_block), K(need_aggregate_read),
            K(begin_read_page_virtual_id), K(end_read_page_virtual_id), K(total_read_size),
            K(page_infos.count()), K(page_cache_handles.count()), KPC(this), K(io_ctx));

  if (OB_SUCC(ret) && !page_infos.empty() && need_aggregate_read) {
    // some pages are not in kv cache and need to read a big io,
    // for the last block on disk, read the range [begin_aggregate_read_virtual_id, last_flushed_page_virtual_id]
    const int64_t begin_aggregate_read_virtual_id = end_read_page_virtual_id + 1;
    const int64_t last_flushed_page_virtual_id = get_page_virtual_id_(cal_wbp_begin_offset_(), true);
    const int64_t end_aggregate_read_virtual_id = MIN((block_index + 1) * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS - 1,
                                                      last_flushed_page_virtual_id);
    for (int64_t vir_page_id = begin_aggregate_read_virtual_id;
         OB_SUCC(ret) && vir_page_id <= end_aggregate_read_virtual_id;
         vir_page_id++) {
      const int64_t cur_begin_offset_in_block = get_offset_in_block_(get_page_begin_offset_by_virtual_id_(vir_page_id));
      const int64_t read_size = ObTmpFileGlobal::SS_BLOCK_SIZE;
      const bool is_end_flushed_page = (vir_page_id == get_page_virtual_id_(file_size_, true /*is_open_interval*/));
      const uint64_t unfilled_page_length = is_end_flushed_page ?
                                            get_offset_in_page_(file_size_) : 0;
      ObTmpPageCacheKey key(fd_, unfilled_page_length, vir_page_id, tenant_id_);
      const int64_t page_offset_in_data_buf = get_page_begin_offset_(cur_begin_offset_in_block) -
                                              get_page_begin_offset_(begin_read_offset_in_block);
      if (OB_FAIL(page_infos.push_back(std::make_pair(key, page_offset_in_data_buf)))) {
        LOG_WARN("fail to push back key", KR(ret), K(key), K(page_offset_in_data_buf));
      }
    }
    LOG_DEBUG("collect aggregate read range of page infos over", KR(ret), K(fd_), K(block_index),
              K(begin_read_offset_in_block), K(end_read_offset_in_block), K(need_aggregate_read),
              K(begin_aggregate_read_virtual_id), K(end_aggregate_read_virtual_id), K(total_read_size),
              K(page_infos.count()), K(page_cache_handles.count()), KPC(this), K(io_ctx));
  }
  return ret;
}

int ObSharedStorageTmpFile::swap_page_to_disk_(const ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t wash_size = 0;
  const int64_t expected_wash_size =
        MAX(MIN(io_ctx.get_todo_size(), ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_PAGE_NUM * ObTmpFileGlobal::ALLOC_PAGE_SIZE),
            ObTmpFileGlobal::SS_TMP_FILE_FLUSH_PROP * wbp_->get_max_page_num() * ObTmpFileGlobal::ALLOC_PAGE_SIZE);
  ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
  const int64_t begin_ts = common::ObTimeUtility::current_time_ms();
  const int64_t timeout_ms = io_ctx.get_io_timeout_ms();
  bool need_retry = false;
  do {
    ret = OB_SUCCESS;
    need_retry = false;
    if (OB_UNLIKELY(is_deleting())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("attempt to swap a deleting file", KR(ret), K(fd_));
    } else if (OB_UNLIKELY(wbp_->get_free_data_page_num() > 0)) {
      // do nothing
    } else if (OB_FAIL(flush_mgr_->wash(expected_wash_size, io_ctx.get_io_flag(),
                                        wait_task_handle, wash_size))) {
      LOG_WARN("fail to wash shared storage temporary file", KR(ret), KPC(this), K(wash_size), K(io_ctx));
    } else if (OB_UNLIKELY(0 == wash_size)) {
      usleep(10 * 1000); // 10ms
      if (REACH_COUNT_INTERVAL(1000)) {
        LOG_INFO("all files is in washing, wait too long", KPC(this));
      }
      int64_t cur_ts = common::ObTimeUtility::current_time_ms();
      if (cur_ts - begin_ts > timeout_ms) {
        ret = OB_TIMEOUT;
        need_retry = false;
        LOG_WARN("swap_page_to_disk_ timeout", KR(ret), K(fd_), K(io_ctx));
      } else {
        need_retry = true;
      }
    } else if (OB_ISNULL(wait_task_handle.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get async flush wait handle", KR(ret), K(id_));
    } else if (OB_FAIL(wait_task_handle.wait(timeout_ms))) {
      LOG_WARN("fail to async flush wait cond", KR(ret), K(fd_));
    } else if (OB_FAIL(wait_task_handle.get()->get_ret_code())) {
      int64_t cur_ts = common::ObTimeUtility::current_time_ms();
      LOG_WARN("fail to async flush failed", KR(ret), K(fd_), K(cur_ts), K(begin_ts),
               K(wait_task_handle), KPC(this));
      if (OB_TIMEOUT == ret && cur_ts - begin_ts < timeout_ms) {
        need_retry = true;
      }
    }
  } while (need_retry);

  return ret;
}

int ObSharedStorageTmpFile::load_disk_tail_page_and_rewrite_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  // `file_size_` is already under multi-write lock's protection, no need to fetch meta lock.
  const int64_t has_written_size = get_offset_in_page_(file_size_);
  const int64_t tail_page_virtual_id = get_page_virtual_id_(file_size_, true /*is_open_interval*/);
  const int64_t write_size = MIN(ObTmpFileGlobal::ALLOC_PAGE_SIZE - has_written_size, io_ctx.get_todo_size());
  char *write_buff = io_ctx.get_todo_buffer();
  uint32_t new_page_id = UINT32_MAX;
  bool has_update_file_meta = false;
  LOG_DEBUG("load_disk_tail_page_and_rewrite_ start", K(fd_), K(io_ctx));

  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == tail_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("begin page virtual id is invalid", KR(ret), K(fd_), K(tail_page_virtual_id), K(file_size_));
  } else if (OB_UNLIKELY(has_written_size + write_size > ObTmpFileGlobal::ALLOC_PAGE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need write size is invalid", KR(ret), K(fd_), K(has_written_size), K(write_size));
  } else if (OB_ISNULL(write_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", KR(ret), K(fd_), K(write_buff));
  }

  // No page in memory, this partially written page is flushed, allocate page and load it up.
  if (OB_SUCC(ret)) {
    char * new_page_buff = nullptr;
    if (OB_FAIL(wbp_->alloc_page(fd_, ObTmpFilePageUniqKey(tail_page_virtual_id), new_page_id, new_page_buff))) {
      LOG_WARN("fail to alloc page when load tail page", KR(ret), K(fd_));
    } else {
      // Fill in read info.
      blocksstable::MacroBlockId subfile_id = id_;
      subfile_id.set_third_id(file_size_ / ObTmpFileGlobal::SS_BLOCK_SIZE);
      blocksstable::ObStorageObjectReadInfo info;
      info.mtl_tenant_id_ = tenant_id_;
      info.io_desc_ = io_ctx.get_io_flag();
      info.macro_block_id_ = subfile_id;
      info.size_ = has_written_size;
      info.offset_ = get_page_begin_offset_(file_size_ % ObTmpFileGlobal::SS_BLOCK_SIZE);
      info.buf_ = new_page_buff;
      info.io_callback_ = nullptr;
      info.io_timeout_ms_ = io_ctx.get_io_timeout_ms();

      // Construct callback and object handle, async read.
      blocksstable::ObStorageObjectHandle object_handle;
      if (OB_FAIL(object_handle.async_read(info))) {
        LOG_WARN("fail to async read", KR(ret), K(fd_), K(info));
      } else if (OB_FAIL(object_handle.wait())) {
        LOG_WARN("fail to wait in object handle", KR(ret), K(fd_));
      } else if (object_handle.get_data_size() < has_written_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to read expected size", KR(ret), K(fd_), K(info), K(has_written_size));
      } else if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(write_buff, write_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid buf range", KR(ret), K(fd_), K(write_buff), K(write_size), K(io_ctx));
      } else {
        MEMCPY(new_page_buff + has_written_size, write_buff, write_size);
      }
      if (FAILEDx(wbp_->notify_dirty(fd_, new_page_id, ObTmpFilePageUniqKey(tail_page_virtual_id)))) {
        LOG_WARN("fail to notify dirty", KR(ret), K(fd_), K(new_page_id));
      }

    }
  }

  // update meta data
  if (OB_SUCC(ret)) {
    LOG_DEBUG("load_disk_tail_page_and_rewrite_ modify meta start", K(fd_), K(io_ctx));
    common::TCRWLock::WLockGuard guard(meta_lock_);
    if (OB_UNLIKELY(is_deleting_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file is deleting", KR(ret), K(fd_));
    } else if (OB_FAIL(io_ctx.update_data_size(write_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(write_size));
    } else if (OB_FAIL(page_idx_cache_.push(new_page_id))) {
      LOG_WARN("fail to push back page idx array", KR(ret), K(fd_), K(new_page_id));
    } else {
      cached_page_nums_ = 1;
      begin_page_id_ = new_page_id;
      begin_page_virtual_id_ = tail_page_virtual_id;
      end_page_id_ = new_page_id;
      file_size_ += write_size;
      has_update_file_meta = true;
    }

    if (FAILEDx(insert_or_update_data_flush_node_())) {
      LOG_WARN("fail to insert or update flush data list", KR(ret), K(fd_), KPC(this));
    }

    if (OB_FAIL(ret) && has_update_file_meta) {
      cached_page_nums_ = 0;
      file_size_ -= write_size;
      begin_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      page_idx_cache_.reset();
    }
    LOG_DEBUG("load_disk_tail_page_and_rewrite_ modify meta over", KR(ret), K(fd_), KPC(this), K(io_ctx));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    if (new_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      if (OB_TMP_FAIL(wbp_->free_page(fd_, new_page_id, ObTmpFilePageUniqKey(tail_page_virtual_id), unused_page_id))) {
        LOG_WARN("fail to free page", KR(tmp_ret), K(fd_), K(new_page_id));
      }
    }
  }
  LOG_DEBUG("load_disk_tail_page_and_rewrite_ over", KR(ret), K(fd_), K(io_ctx));

  return ret;
}

int ObSharedStorageTmpFile::append_write_memory_tail_page_(ObTmpFileIOCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  char *page_buff = nullptr;
  const int64_t has_written_size = get_offset_in_page_(file_size_);
  const int64_t need_write_size = MIN(ObTmpFileGlobal::ALLOC_PAGE_SIZE - has_written_size,
                                      io_ctx.get_todo_size());
  const int64_t end_page_virtual_id = get_end_page_virtual_id_();
  char *write_buff = io_ctx.get_todo_buffer();
  uint32_t unused_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  LOG_DEBUG("append_write_memory_tail_page_ start", K(fd_), K(io_ctx));

  if (OB_UNLIKELY(has_written_size + need_write_size > ObTmpFileGlobal::ALLOC_PAGE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need write size is invalid", KR(ret), K(fd_), K(has_written_size), K(need_write_size));
  } else if (OB_ISNULL(write_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", KR(ret), K(fd_), K(write_buff));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == end_page_virtual_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("end page virtual id is invalid", KR(ret), K(fd_), K(end_page_virtual_id), K(file_size_));
  } else if (OB_FAIL(wbp_->read_page(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id),
                                     page_buff, unused_page_id))) {
    LOG_WARN("fail to fetch page", KR(ret), K(fd_), K(end_page_id_), K(end_page_virtual_id));
  } else if (OB_ISNULL(page_buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page buff is null", KR(ret), K(fd_), K(end_page_id_));
  } else if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(write_buff, need_write_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid buf range", KR(ret), K(fd_), K(write_buff), K(need_write_size), K(io_ctx));
  } else {
    MEMCPY(page_buff + has_written_size, write_buff, need_write_size);
  }

  // update meta data
  if (OB_SUCC(ret)) {
    LOG_DEBUG("append_write_memory_tail_page_ modify meta start", K(fd_), K(io_ctx));
    common::TCRWLock::WLockGuard guard(meta_lock_);
    const bool is_write_back = wbp_->is_write_back(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id));

    if (OB_UNLIKELY(is_deleting_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file is deleting", KR(ret), K(fd_));
    } else if (is_write_back) {
      if (OB_FAIL(wbp_->notify_dirty(fd_, end_page_id_, ObTmpFilePageUniqKey(end_page_virtual_id)))) {
        LOG_WARN("fail to notify dirty", KR(ret), K(fd_), K(end_page_id_), K(end_page_virtual_id));
      } else {
        write_back_data_page_num_--;
      }

      if (FAILEDx(insert_or_update_data_flush_node_())) {
        LOG_WARN("fail to insert or update flush data list", KR(ret), K(fd_), KPC(this));
      }
    }

    if (FAILEDx(io_ctx.update_data_size(need_write_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(need_write_size));
    } else {
      file_size_ += need_write_size;
    }
    LOG_DEBUG("append_write_memory_tail_page_ modify meta over", KR(ret), K(fd_), KPC(this), K(io_ctx));
  }

  LOG_DEBUG("append_write_memory_tail_page_ over", KR(ret), K(fd_), K(io_ctx));
  return ret;
}

// TODO: wanyue.wy
int ObSharedStorageTmpFile::truncate_persistent_pages_(const int64_t truncate_offset)
{
  int ret = OB_SUCCESS;

  return ret;
}

int ObSharedStorageTmpFile::truncate_the_first_wbp_page_()
{
  int ret = OB_SUCCESS;
  bool is_write_back_page = false;
  uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_ ||
      ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("begin_page_id_ is already INVALID", KR(ret), K(fd_), K(begin_page_id_),
             K(begin_page_virtual_id_));
  } else if (wbp_->is_write_back(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_))) {
    is_write_back_page = true;
  }

  if (FAILEDx(wbp_->free_page(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
    LOG_WARN("fail to free page", KR(ret), K(fd_), K(begin_page_id_), K(begin_page_virtual_id_));
  } else {
    ObTmpPageCacheKey key(fd_, 0, begin_page_virtual_id_, tenant_id_);
    ObTmpPageCache::get_instance().erase(key);
    if (is_write_back_page) {
      write_back_data_page_num_--;
    }
    cached_page_nums_--;
    begin_page_id_ = next_page_id;
    if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_) {
      end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    } else {
      begin_page_virtual_id_ += 1;
    }
  }
  return ret;
}

int ObSharedStorageTmpFile::update_meta_after_flush(const ObSSTmpFileAsyncFlushWaitTask &wait_task)
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("update_meta_after_flush start", K(fd_), K(wait_task));
  ObSpinLockGuard last_page_lock_guard(last_page_lock_);
  common::TCRWLock::WLockGuard guard(meta_lock_);
  LOG_DEBUG("update_meta_after_flush update meta start", K(fd_), KPC(this));

  // Validate arguments.
  const int64_t page_nums_before_update = cached_page_nums_;
  const int64_t flushed_offset = wait_task.flushed_offset_;
  const int64_t file_size_when_flush = wait_task.current_length_;
  uint32_t wait_task_begin_page_id = wait_task.current_begin_page_id_;
  int64_t wait_task_begin_page_virtual_id = wait_task.current_begin_page_virtual_id_;
  int64_t expected_flushed_page_num = wait_task.expected_flushed_page_num_;
  int64_t flushed_page_num = wait_task.succeed_wait_page_nums_;
  int64_t free_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(flushed_offset < 0 || file_size_when_flush < 0 ||
                         wait_task_begin_page_id == ObTmpFileGlobal::INVALID_PAGE_ID ||
                         wait_task_begin_page_virtual_id == ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID ||
                         expected_flushed_page_num <= 0 ||
                         flushed_page_num < 0) ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this), K(flushed_offset), K(file_size_when_flush),
             K(wait_task_begin_page_id), K(wait_task_begin_page_virtual_id),
             K(expected_flushed_page_num), K(flushed_page_num), K(wait_task));
  } else if (wait_task.io_tasks_.count() > 0) {
    if (OB_ISNULL(wait_task.io_tasks_.at(wait_task.io_tasks_.count() - 1).first)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("io task is null", KR(ret), KPC(this), K(wait_task));
    } else {
      try_to_seal_in_flush_(flushed_offset, expected_flushed_page_num,
                            *wait_task.io_tasks_.at(wait_task.io_tasks_.count() - 1).first,
                            flushed_page_num);
    }
  }

  if (FAILEDx(adjust_flush_wait_task_info_(flushed_offset, file_size_when_flush,
                                           wait_task_begin_page_id,
                                           wait_task_begin_page_virtual_id,
                                           expected_flushed_page_num,
                                           flushed_page_num))) {
    LOG_WARN("fail to adjust flush wait task info", KR(ret), KPC(this), K(flushed_offset),
              K(file_size_when_flush), K(wait_task_begin_page_id),
              K(wait_task_begin_page_virtual_id),
              K(expected_flushed_page_num), K(flushed_page_num), K(wait_task));
  } else if (OB_UNLIKELY(flushed_page_num < 0 || flushed_page_num > cached_page_nums_ ||
                         flushed_page_num > expected_flushed_page_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page nums to free is invalid", KR(ret), K(fd_), K(flushed_page_num),
              K(expected_flushed_page_num), KPC(this), K(wait_task));
  } else if (0 == flushed_page_num) {
    // do nothing
  } else if (OB_UNLIKELY(expected_flushed_page_num > cached_page_nums_ ||
                         wait_task_begin_page_id != begin_page_id_ ||
                         wait_task_begin_page_virtual_id != begin_page_virtual_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(expected_flushed_page_num),
              K(cached_page_nums_), K(wait_task_begin_page_id), K(wait_task_begin_page_virtual_id),
              K(begin_page_id_), K(begin_page_virtual_id_), KPC(this), K(wait_task));
  } else {
    const int64_t evict_end_virtual_id = begin_page_virtual_id_ + flushed_page_num;
    if (FAILEDx(page_idx_cache_.truncate(evict_end_virtual_id))) {
      LOG_WARN("fail to truncate page idx cache", KR(ret), K(fd_), K(evict_end_virtual_id), KPC(this));
    }

    // Free pages and decrease cached_page_nums_.
    for (; OB_SUCC(ret) && free_count < flushed_page_num; ++free_count) {
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      if (OB_FAIL(wbp_->free_page(fd_, begin_page_id_, ObTmpFilePageUniqKey(begin_page_virtual_id_), next_page_id))) {
        LOG_WARN("fail to free page", KR(ret), K(fd_), K(free_count),
                 K(cached_page_nums_), K(begin_page_id_), K(flushed_page_num));
      } else {
        write_back_data_page_num_--;
        begin_page_id_ = next_page_id;
        if (ObTmpFileGlobal::INVALID_PAGE_ID == begin_page_id_) {
          end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
          begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        } else {
          begin_page_virtual_id_ += 1;
        }
        cached_page_nums_--;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(write_back_data_page_num_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("write_back_data_page_num_ is unexpected", KR(ret), KPC(this));
  } else if (write_back_data_page_num_ > 0) {
    if (OB_UNLIKELY(expected_flushed_page_num == flushed_page_num)) {
      // all wait tasks of flushing pages are successful.
      // we expect that all of them are freed and write_back_data_page_num_ is 0
      const int64_t abort_page_num_in_flushing = wait_task.succeed_wait_page_nums_ - flushed_page_num;
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("all wait tasks are success, but write_back_data_page_num_ is positive",
                KR(ret), K(abort_page_num_in_flushing), K(wait_task), KPC(this));
    } else if (OB_UNLIKELY(expected_flushed_page_num < flushed_page_num)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("flushed page num is larger than expected", KR(ret), K(expected_flushed_page_num),
                K(flushed_page_num), KPC(this));
    } else if (OB_UNLIKELY(expected_flushed_page_num - flushed_page_num != write_back_data_page_num_)) {
      // some wait tasks of flushing pages are failed. but the num is not equal the remain write_back_data_page_num_
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("remain write_back_data_page_num_ is invalid", KR(ret),
                K(expected_flushed_page_num), K(flushed_page_num), K(write_back_data_page_num_), KPC(this));
    } else {
      // in this case, the wait io tasks of following flushing data pages are failed.
      // we just cancel the flushing state of them. they will be flushed in the next flush.
      LOG_DEBUG("remain failed write back page num", K(fd_), K(write_back_data_page_num_));
      uint32_t failed_flushing_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      int64_t failed_flushing_page_virtual_id = wait_task_begin_page_virtual_id + flushed_page_num;
      if (OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, failed_flushing_page_virtual_id,
                                                  begin_page_id_, failed_flushing_page_id))) {
        LOG_WARN("fail to get page id by virtual id", KR(ret), K(wait_task_begin_page_virtual_id),
                 K(flushed_page_num), KPC(this));
      } else if (OB_FAIL(rollback_failed_flush_pages_(write_back_data_page_num_, failed_flushing_page_id,
                                                      failed_flushing_page_virtual_id))) {
        LOG_WARN("fail to rollback failed flush pages", KR(ret), K(fd_), K(failed_flushing_page_id),
                 K(failed_flushing_page_virtual_id));
      }
    }
  }

  if (FAILEDx(reinsert_data_flush_node_())) {
    LOG_WARN("fail to reinsert data flush node", KR(ret), KPC(this));
  }

  LOG_DEBUG("update meta data over", KR(ret), K(flushed_page_num), K(page_nums_before_update),
            K(free_count), K(wait_task), KPC(this));

  return ret;
}

int ObSharedStorageTmpFile::adjust_flush_wait_task_info_(const int64_t flushed_offset,
                                                         const int64_t file_size_when_flush,
                                                         uint32_t &wait_task_begin_page_id,
                                                         int64_t &wait_task_begin_page_virtual_id,
                                                         int64_t &expected_flushed_page_num,
                                                         int64_t &flushed_page_num)
{
  int ret = OB_SUCCESS;
  // flushing pages have been truncated before this callback,
  // we will treat these pages as if they have never been flushed
  if (wait_task_begin_page_virtual_id != begin_page_virtual_id_) {
    if (OB_UNLIKELY(wait_task_begin_page_virtual_id > begin_page_virtual_id_ &&
                    ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID != begin_page_virtual_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("wait task begin page virtual id is invalid", KR(ret), K(wait_task_begin_page_virtual_id),
                K(begin_page_virtual_id_), KPC(this));
    } else if (ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == begin_page_virtual_id_ ||
               wait_task_begin_page_virtual_id + expected_flushed_page_num <= begin_page_virtual_id_) {
      // all flushing pages have been truncated
      if (OB_UNLIKELY(flushed_offset > truncated_offset_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("flushed offset is larger than truncated offset", KR(ret), K(flushed_offset),
                  K(truncated_offset_), KPC(this));
      } else {
        LOG_INFO("all flushing pages have been truncated", K(flushed_offset), K(file_size_when_flush),
                 K(wait_task_begin_page_id), K(wait_task_begin_page_virtual_id),
                 K(expected_flushed_page_num), K(flushed_page_num), KPC(this));
        wait_task_begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        wait_task_begin_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
        expected_flushed_page_num = 0;
        flushed_page_num = 0;
      }
    } else {
      // wait_task_begin_page_virtual_id < begin_page_virtual_id_ < wait_task_begin_page_virtual_id + expected_flushed_page_num
      // part of flushing pages have been truncated
      const int64_t truncated_page_num = begin_page_virtual_id_ - wait_task_begin_page_virtual_id;
      wait_task_begin_page_id = begin_page_id_;
      wait_task_begin_page_virtual_id = begin_page_virtual_id_;
      expected_flushed_page_num -= truncated_page_num;
      flushed_page_num = MAX(0, flushed_page_num - truncated_page_num);
      LOG_INFO("flushing pages have been truncated", K(fd_), K(truncated_page_num),
               K(flushed_offset), K(file_size_when_flush),
               K(wait_task_begin_page_id), K(wait_task_begin_page_virtual_id),
               K(expected_flushed_page_num), K(flushed_page_num), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    // flushing tail page has been re-written, we will treat this page as if it has never been flushed
    const bool has_flushed_unfinished_tail_page = flushed_offset == file_size_when_flush &&
                                                  file_size_when_flush % ObTmpFileGlobal::ALLOC_PAGE_SIZE != 0;
    if (has_flushed_unfinished_tail_page && file_size_ != file_size_when_flush) {
      expected_flushed_page_num = MAX(0, expected_flushed_page_num - 1);
      flushed_page_num = MAX(0, flushed_page_num - 1);
      LOG_INFO("tail page append written, do not free last page", K(fd_),
                K(flushed_offset), K(file_size_when_flush),
                K(wait_task_begin_page_id), K(wait_task_begin_page_virtual_id),
                K(expected_flushed_page_num), K(flushed_page_num), KPC(this));
    }
  }

  return ret;
}

int ObSharedStorageTmpFile::rollback_failed_flush_pages_(const int64_t failed_page_num,
                                                         const uint32_t start_page_id,
                                                         const int64_t start_virtual_page_id)
{
  int ret = OB_SUCCESS;
  const int64_t truncated_virtual_page_id = get_page_virtual_id_(get_page_begin_offset_(truncated_offset_), false);
  int64_t actual_failed_page_num = 0;
  uint32_t failed_flushing_page_id = ObTmpFileGlobal::INVALID_PAGE_ID ;
  int64_t failed_flushing_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  if (OB_UNLIKELY(failed_page_num <= 0 || start_page_id == ObTmpFileGlobal::INVALID_PAGE_ID ||
                  start_virtual_page_id == ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(failed_page_num), K(start_page_id), K(start_virtual_page_id));
  } else if (truncated_virtual_page_id <= start_virtual_page_id) {
    // all flushing page has not been truncated
    actual_failed_page_num = failed_page_num;
    failed_flushing_page_virtual_id = start_virtual_page_id;
    failed_flushing_page_id = start_page_id;
  } else if (truncated_virtual_page_id > start_virtual_page_id + failed_page_num - 1) {
    // all flushing pages have been truncated, we do not need to notify flush failed
    actual_failed_page_num = 0;
  } else {
    // some flushing pages have been truncated, we need to notify flush failed from new start page
    actual_failed_page_num = failed_page_num - (truncated_virtual_page_id - start_virtual_page_id);
    failed_flushing_page_virtual_id = truncated_virtual_page_id;
    if (OB_FAIL(wbp_->get_page_id_by_virtual_id(fd_, failed_flushing_page_virtual_id,
                                                begin_page_id_, failed_flushing_page_id))) {
      LOG_WARN("fail to get page id by virtual id", KR(ret), K(failed_flushing_page_virtual_id),
                K(actual_failed_page_num), K(failed_page_num), K(start_virtual_page_id),
                K(truncated_virtual_page_id), KPC(this));
    }
  }
  LOG_DEBUG("cal actual flush failed page num over", KR(ret), K(actual_failed_page_num),
            K(failed_flushing_page_id), K(failed_flushing_page_virtual_id),
            K(truncated_offset_), K(truncated_virtual_page_id),
            K(failed_page_num), K(start_page_id), K(start_virtual_page_id), KPC(this));
  if (OB_SUCC(ret) && actual_failed_page_num > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < actual_failed_page_num; ++i) {
      if (OB_FAIL(wbp_->notify_write_back_fail(fd_, failed_flushing_page_id,
                                      ObTmpFilePageUniqKey(failed_flushing_page_virtual_id)))) {
        LOG_WARN("fail to notify write back fail",
            KR(ret), K(fd_), K(failed_flushing_page_id), K(failed_flushing_page_virtual_id));
      } else if (OB_FAIL(wbp_->get_next_page_id(fd_, failed_flushing_page_id,
                                                ObTmpFilePageUniqKey(failed_flushing_page_virtual_id),
                                                failed_flushing_page_id))) {
        LOG_WARN("fail to get next page id", KR(ret), K(fd_), K(failed_flushing_page_id), K(failed_flushing_page_virtual_id));
      } else {
        failed_flushing_page_virtual_id += 1;
        write_back_data_page_num_--;
      }
    }
  }
  LOG_DEBUG("notify flush failed over", KR(ret), K(actual_failed_page_num),
            K(failed_flushing_page_id), K(failed_flushing_page_virtual_id),
            K(truncated_offset_), K(truncated_virtual_page_id),
            K(failed_page_num), K(start_page_id), K(start_virtual_page_id), KPC(this));
  return ret;
}

int ObSharedStorageTmpFile::flush(const bool flush_all_pages,
                                  const common::ObIOFlag io_flag,
                                  const int64_t io_timeout_ms,
                                  int64_t &flush_size,
                                  ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("flush start", K(fd_));
  ObSpinLockGuard last_page_lock_guard(last_page_lock_);
  common::TCRWLock::RLockGuard guard(meta_lock_);
  flush_size = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_flushing_())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the shared storage temporary file is not flushing state", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to flush a deleting file", KR(ret), K(fd_));
  } else if (OB_FAIL(inner_async_flush_(flush_all_pages, io_flag, io_timeout_ms, flush_size, wait_task_handle))) {
    LOG_WARN("fail to flush data", KR(ret), K(fd_), K(flush_all_pages), K(flush_size), KPC(this),
              K(io_timeout_ms), K(io_flag), K(wait_task_handle));
  }

  LOG_DEBUG("flush over", KR(ret), K(flush_size), KPC(this));
  return ret;
}

int ObSharedStorageTmpFile::inner_async_flush_(const bool flush_all_pages,
                                               const common::ObIOFlag io_flag,
                                               const int64_t io_timeout_ms,
                                               int64_t &flush_size,
                                               ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle)
{
  int ret = OB_SUCCESS;
  flush_size = 0;
  ObIAllocator &allocator = flush_mgr_->get_wait_task_allocator();
  char *buff = nullptr;
  ObSSTmpFileAsyncFlushWaitTask *task = nullptr;
  const int64_t max_flush_page_nums = cal_max_flush_page_nums_(flush_all_pages);
  const int64_t write_info_offset = cal_wbp_begin_offset_(); // align offset
  const int64_t better_flush_page_nums = flush_all_pages ?
                                         max_flush_page_nums :
                                         try_align_flush_page_nums_to_block_(
                                            max_flush_page_nums, write_info_offset);
  ObSSTmpFileFlushContext flush_context(id_, io_flag, io_timeout_ms, better_flush_page_nums);
  const ObMemAttr attr(tenant_id_, "SSTmpFileAWait");
  FlushCtxState cur_flush_state = FlushCtxState::FSM_FINISHED;

  if (OB_ISNULL(buff = static_cast<char *>(
                    allocator.alloc(sizeof(ObSSTmpFileAsyncFlushWaitTask))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate tmp file async wait task memory", KR(ret), K(fd_));
  } else if (FALSE_IT(task = new (buff) ObSSTmpFileAsyncFlushWaitTask)) {
  } else if (OB_FAIL(ObTmpFileGlobal::switch_data_list_level_to_flush_state(
                  static_cast<ObTmpFileGlobal::FileList>(data_page_flush_level_), cur_flush_state))) {
    LOG_WARN("fail to switch data list level to flush state", KR(ret), K(fd_), K(data_page_flush_level_));
  } else if (OB_FAIL(task->init(fd_, file_size_, begin_page_id_, begin_page_virtual_id_,
                                better_flush_page_nums, attr, cur_flush_state, flush_mgr_))) {
    LOG_WARN("fail to init async wait task", KR(ret), K(fd_), K(better_flush_page_nums),
             K(attr), K(cur_flush_state), KPC(task), KPC(this));
  } else {
    flush_context.init(write_info_offset, begin_page_id_, begin_page_virtual_id_);
  }

  int64_t actual_flush_size = 0;
  // Continuously flush pages until unable to continue.
  while (OB_SUCC(ret) && flush_context.should_continue()) {
    // Allocate object handle and buffer, flush data to block.
    char *buff = nullptr;
    blocksstable::ObStorageObjectHandle * object_handle = nullptr;
    char * flush_buff = nullptr;
    int64_t batch_flush_size = 0;
    const int batch_expected_flush_size = flush_context.get_batch_expected_flush_size();
    int flushed_page_num = 0;
    if (OB_UNLIKELY(batch_expected_flush_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch expected flush size is invalid", KR(ret), K(batch_expected_flush_size));
    } else if (OB_ISNULL(buff =
          static_cast<char *>(allocator.alloc(sizeof(blocksstable::ObStorageObjectHandle))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate blocksstable::ObStorageObjectHandle", KR(ret), K(fd_));
    } else if (FALSE_IT(object_handle = new (buff) blocksstable::ObStorageObjectHandle)) {
    } else if (OB_ISNULL(flush_buff =
          static_cast<char *>(allocator.alloc(batch_expected_flush_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate flush buffer", KR(ret), K(fd_));
    } else if (FALSE_IT(flush_context.append_batch(object_handle, flush_buff))) {  // Init context for this batch.
    } else if (OB_FAIL(inner_async_flush_block_(flush_context, batch_flush_size))) { // Memcpy data and flush io.
      LOG_WARN("fail to inner_async_flush_block", KR(ret), K(fd_), K(flush_context));
    } else if (FALSE_IT(flushed_page_num = upper_align(batch_flush_size, ObTmpFileGlobal::ALLOC_PAGE_SIZE) / ObTmpFileGlobal::ALLOC_PAGE_SIZE)) {
    } else if (OB_FAIL(task->push_back_io_task(flush_context, flushed_page_num))) {
      LOG_ERROR("fail to push back flush data size", KR(ret), K(fd_), K(flush_context), K(flushed_page_num));
    }
    if (OB_FAIL(ret)) {
      // Free object_handle and flush_buff if an error occurs.
      if (OB_NOT_NULL(object_handle)) {
        object_handle->~ObStorageObjectHandle();
        allocator.free(object_handle);
      }
      if (OB_NOT_NULL(flush_buff)) {
        allocator.free(flush_buff);
      }
      if (OB_LIKELY(task->have_io_task())) {
        // some blocks have been flushed, just ignore the error of current block
        // and only flush the former blocks
        ret = OB_SUCCESS;
        break;
      }
    } else {
      actual_flush_size += batch_flush_size;
    }
  } // end of while.

  if (OB_SUCC(ret) && !flush_context.should_continue()) {
    flush_context.is_finished_ = true;
  }

  // Set async wait task into io_handle and tmp file manager's wait queue.
  if (OB_SUCC(ret) && task->io_tasks_.count() > 0) {
    if (!flush_context.is_finished_) {
      LOG_INFO("some flush io tasks are failed", K(fd_), KPC(this), KPC(task));
    }
    wait_task_handle.set_wait_task(task);
    if (OB_FAIL(flush_mgr_->wait_task_enqueue(task))) {
      LOG_WARN("fail to enqueue wait task", KR(ret), K(fd_), KPC(task));
    } else {
      flush_size = actual_flush_size;
    }
  } else if (OB_NOT_NULL(task)) {
    // No page to flush or some error happen, free async wait task.
    task->~ObSSTmpFileAsyncFlushWaitTask();
    allocator.free(task);
    task = nullptr;
  }

  LOG_DEBUG("shared storage temporary file finish async flush", KR(ret),
            K(fd_), KP(task), KPC(this), K(flush_all_pages),
            K(write_info_offset), K(max_flush_page_nums),
            K(better_flush_page_nums), K(flush_context), K(flush_size));

  return ret;
}

int ObSharedStorageTmpFile::inner_async_flush_block_(ObSSTmpFileFlushContext &ctx, int64_t &batch_flush_size)
{
  int ret = OB_SUCCESS;
  batch_flush_size = 0;

  // Fetch page from wbp and memcpy to flush buffer.
  const int64_t begin_flush_page_id = ctx.cur_page_id_;
  const int64_t begin_flush_virtual_page_id = ctx.cur_virtual_page_id_;
  int64_t write_back_page_num = 0;
  for (int64_t i = 0;
       OB_SUCC(ret) && i < ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS && ctx.should_this_batch_continue();
       ++i) {
    char *page_buff = nullptr;
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    if (OB_FAIL(wbp_->read_page(fd_, ctx.cur_page_id_, ObTmpFilePageUniqKey(ctx.cur_virtual_page_id_), page_buff, next_page_id))) {
      LOG_WARN("fail to fetch page", KR(ret), K(fd_), K(ctx));
    } else {
      int64_t cpy_len = 0;
      if (ctx.cur_page_id_ == end_page_id_) {
        const int64_t tail_page_length = file_size_ % ObTmpFileGlobal::ALLOC_PAGE_SIZE;
        cpy_len = tail_page_length == 0 ? ObTmpFileGlobal::ALLOC_PAGE_SIZE : tail_page_length;
      } else {
        cpy_len = ObTmpFileGlobal::ALLOC_PAGE_SIZE;
      }
      MEMCPY(ctx.flush_buff_ + i * ObTmpFileGlobal::ALLOC_PAGE_SIZE, page_buff, cpy_len);
      write_back_page_num++;
      ctx.append_page(cpy_len, next_page_id, ctx.cur_virtual_page_id_ + 1);
    }
  }
  if (OB_SUCC(ret)) {
    if (is_sealed_ && write_back_data_page_num_ + write_back_page_num == cached_page_nums_) {
      ctx.set_can_seal();
      LOG_INFO("seal uncompleted block", K(fd_), K(ctx), KPC(this));
    }
  }
  // Fill in write info and async write to disk.
  if (FAILEDx(ctx.flush_io())) {
    LOG_WARN("fail to flush_io", KR(ret), K(fd_), KPC(this), K(ctx));
  } else {
    int64_t cur_flush_page_id = begin_flush_page_id;
    int64_t cur_flush_virtual_page_id = begin_flush_virtual_page_id;
    for (int64_t i = 0; i < write_back_page_num && OB_SUCC(ret); ++i) {
      char *page_buff = nullptr;
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      if (OB_FAIL(wbp_->read_page(fd_, cur_flush_page_id, ObTmpFilePageUniqKey(cur_flush_virtual_page_id), page_buff, next_page_id))) {
        LOG_WARN("fail to fetch page", KR(ret), K(fd_), K(ctx));
      } else if (OB_FAIL(wbp_->notify_write_back(fd_, cur_flush_page_id, ObTmpFilePageUniqKey(cur_flush_virtual_page_id)))) {
        LOG_WARN("fail to notify write back", KR(ret), K(fd_), K(ctx));
      } else {
        const int64_t unfilled_page_length = cur_flush_page_id == end_page_id_ ?
                                             file_size_ % ObTmpFileGlobal::ALLOC_PAGE_SIZE : 0;
        ObTmpPageCacheKey cache_key(fd_, unfilled_page_length, cur_flush_virtual_page_id, tenant_id_);
        ObTmpPageCacheValue cache_value(page_buff);
        ObTmpPageCache::get_instance().try_put_page_to_cache(cache_key, cache_value);

        cur_flush_page_id = next_page_id;
        cur_flush_virtual_page_id += 1;
      }
    }
    if (OB_SUCC(ret)) {
      write_back_data_page_num_ += write_back_page_num;
      batch_flush_size = write_back_page_num * ObTmpFileGlobal::ALLOC_PAGE_SIZE;
    }

    ctx.finish_batch();  // Update context for next batch.
  }

  return ret;
}

int64_t ObSharedStorageTmpFile::cal_max_flush_page_nums_(const bool flush_all_pages)
{
  // Control the minimum and maximum flush page nums to prevent IO from bursting.
  int64_t max_flush_page_nums = 0;
  if (flush_all_pages) {
    // Flush all pages directly.
    max_flush_page_nums = get_data_page_nums_(false);
    LOG_DEBUG("going to flush all pages", K(fd_), K(max_flush_page_nums));
  } else {
    // Flush pages depend on watermark status.
    int64_t wbp_watermark = ObTmpFileGlobal::SS_TMP_FILE_SAFE_WBP_PROP * wbp_->get_max_page_num();  // current watermark
    int64_t wbp_currmark = wbp_->get_used_page_num();   // current usage

    const int64_t full_page_nums = get_data_page_nums_(true);
    // If flush the following numbers of pages, then the watermark will be at low status.
    const int64_t flush_to_low_watermark_page_nums = MAX(wbp_currmark - wbp_watermark, 0);
    // The following numbers of pages is going to free in async wait thread.
    const int64_t pending_free_page_nums =
        flush_mgr_->get_flushing_data_size() / ObTmpFileGlobal::ALLOC_PAGE_SIZE;

    if (pending_free_page_nums >= flush_to_low_watermark_page_nums) {
      // The watermark is safe, flushing one block (256 pages) at least.
      max_flush_page_nums = MIN(full_page_nums, ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS);
    } else {
      // The watermark is dangerous, flushing pages to keep low watermark.
      max_flush_page_nums =
          MIN3(full_page_nums, flush_to_low_watermark_page_nums - pending_free_page_nums,
               ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS * ObTmpFileGlobal::MAX_FLUSHING_BLOCK_NUM);
    }
    LOG_DEBUG("going to flush full pages", K(fd_), K(max_flush_page_nums),
              K(wbp_watermark), K(wbp_currmark), K(full_page_nums),
              K(flush_to_low_watermark_page_nums), K(pending_free_page_nums));
  }
  return max_flush_page_nums;
}

int64_t ObSharedStorageTmpFile::try_align_flush_page_nums_to_block_(
    const int64_t max_flush_page_nums, const int64_t write_info_offset) const
{
  int64_t result_page_nums = 0;
  const int64_t fill_prev_block_page_nums =
      (common::upper_align(write_info_offset, ObTmpFileGlobal::SS_BLOCK_SIZE) -
       write_info_offset) /
      ObTmpFileGlobal::ALLOC_PAGE_SIZE;
  if (max_flush_page_nums <= fill_prev_block_page_nums ||
      max_flush_page_nums < ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS) {
    result_page_nums = max_flush_page_nums;
  } else {
    result_page_nums =
        common::lower_align(max_flush_page_nums - fill_prev_block_page_nums,
                            ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS) +
        fill_prev_block_page_nums;
  }
  return result_page_nums;
}

void ObSharedStorageTmpFile::try_to_seal_in_flush_(const int64_t flushed_offset,
                                                   const int64_t expected_flushed_page_num,
                                                   blocksstable::ObStorageObjectHandle &obj_handle,
                                                   int64_t &flushed_page_num)
{
  int ret = OB_SUCCESS;
  const bool has_flushed_unsealed_tail_block = flushed_offset == file_size_ &&
                                               file_size_ % ObTmpFileGlobal::SS_BLOCK_SIZE != 0;
  if (is_sealed_ && has_flushed_unsealed_tail_block &&
      expected_flushed_page_num == flushed_page_num) {
    // attention:
    // it is possible that the flushed pages have been truncated.
    // however, it will not take effect to read file correctly.
    // thus, we don't handle with this case.
    // just seal the file and upload it to oss.
    LOG_INFO("seal uncompleted block", K(fd_), KPC(this), K(obj_handle));
    blocksstable::MacroBlockId id = id_;
    id.set_third_id(get_block_virtual_id_(flushed_offset, true));
    ObIOFlag flag;
    if (OB_FAIL(obj_handle.get_io_handle().get_io_flag(flag))) {
      LOG_WARN("fail to get io flag", KR(ret), K(fd_), K(obj_handle));
    } else if (!flag.is_sync() && !flag.is_sealed() &&
               OB_FAIL(OB_STORAGE_OBJECT_MGR.seal_object(id, 0/*ls_epoch_id*/))) {
      LOG_WARN("fail to seal file", KR(ret), K(flushed_offset), K(expected_flushed_page_num),
               K(flushed_page_num), KPC(this), K(obj_handle));
      flushed_page_num--; // if seal the last block failed, we will treat it as failed IO task.
    }
  }
}

bool ObSharedStorageTmpFile::is_flushing_()
{
  return data_page_flush_level_ >= 0 && OB_ISNULL(data_flush_node_.get_next());
}

int64_t ObSharedStorageTmpFile::get_dirty_data_page_size_() const
{
  int ret = OB_SUCCESS;

  int64_t dirty_size = 0;
  // cached_page_nums == flushed_data_page_num + dirty_data_page_num
  if (0 == cached_page_nums_ || write_back_data_page_num_ == cached_page_nums_) {
    dirty_size = 0;
  } else if (OB_UNLIKELY(cached_page_nums_ < write_back_data_page_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("page num is invalid", KR(ret), K(cached_page_nums_), K(write_back_data_page_num_), KPC(this));
    dirty_size = 0;
  } else if (0 == file_size_ % ObTmpFileGlobal::ALLOC_PAGE_SIZE) {
    dirty_size =
      (cached_page_nums_ - write_back_data_page_num_) * ObTmpFileGlobal::ALLOC_PAGE_SIZE;
  } else {
    dirty_size =
      (cached_page_nums_ - write_back_data_page_num_ - 1) * ObTmpFileGlobal::ALLOC_PAGE_SIZE
      + file_size_ % ObTmpFileGlobal::ALLOC_PAGE_SIZE;
  }

  if (dirty_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dirty_size is unexpected", KR(ret), K(dirty_size), KPC(this));
    dirty_size = 0;
  }
  return dirty_size;
}

int64_t ObSharedStorageTmpFile::get_data_page_nums(bool only_full_page)
{
  common::TCRWLock::RLockGuard guard(meta_lock_);

  return get_data_page_nums_(only_full_page);
}

int64_t ObSharedStorageTmpFile::get_data_page_nums_(bool only_full_page)
{
  int64_t full_page_nums = cached_page_nums_;
  if (full_page_nums > 0 && file_size_ % ObTmpFileGlobal::ALLOC_PAGE_SIZE != 0) {
    full_page_nums--;
  }
  return only_full_page ? full_page_nums : cached_page_nums_;
}

void ObSharedStorageTmpFile::inner_set_read_stats_vars_(const ObTmpFileIOCtx &ctx, const int64_t read_size)
{
  aggregate_read_io_cnt_ += ctx.get_aggregate_read_io_cnt();
  ObITmpFile::inner_set_read_stats_vars_(ctx, read_size);
}

int ObSharedStorageTmpFile::copy_info_for_virtual_table(ObTmpFileInfo &tmp_file_info)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + 100 * 1000L;
  common::TCRWLock::RLockGuardWithTimeout lock_guard(meta_lock_, abs_timeout_us, ret);
  ObSSTmpFileInfo &ss_tmp_file_info = static_cast<ObSSTmpFileInfo&>(tmp_file_info);

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to get lock for reading in virtual table", KR(ret), KPC(this));
  } else {
    ObSpinLockGuard guard(stat_lock_);
    if (OB_FAIL(ss_tmp_file_info.init(trace_id_, tenant_id_, dir_id_, fd_, file_size_,
                                      truncated_offset_, is_deleting_,
                                      cached_page_nums_, write_back_data_page_num_,
                                      0, ref_cnt_, birth_ts_, this, label_.ptr(),
                                      write_req_cnt_, unaligned_write_req_cnt_,
                                      write_persisted_tail_page_cnt_, lack_page_cnt_, last_modify_ts_,
                                      read_req_cnt_, unaligned_read_req_cnt_,
                                      total_truncated_page_read_cnt_, total_kv_cache_page_read_cnt_,
                                      total_uncached_page_read_cnt_, total_wbp_page_read_cnt_,
                                      truncated_page_read_hits_, kv_cache_page_read_hits_,
                                      uncached_page_read_hits_, wbp_page_read_hits_,
                                      total_read_size_, last_access_ts_))) {
      LOG_WARN("fail to init tmp_file_info", KR(ret), KPC(this));
    } else {
      ss_tmp_file_info.aggregate_read_io_cnt_ = aggregate_read_io_cnt_;
    }
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
