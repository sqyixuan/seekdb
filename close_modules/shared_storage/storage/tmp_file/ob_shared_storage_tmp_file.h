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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_STORAGE_TMP_FILE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_STORAGE_TMP_FILE_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_ss_tmp_file_flush_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_remove_manager.h"
#include "storage/tmp_file/ob_i_tmp_file.h"

namespace oceanbase
{
namespace tmp_file
{
//for virtual table show
class ObSSTmpFileInfo final : public ObTmpFileInfo
{
public:
  ObSSTmpFileInfo() :
    ObTmpFileInfo(),
    aggregate_read_io_cnt_(0) {}
  virtual ~ObSSTmpFileInfo() { reset(); }
  virtual void reset() override;
public:
  int64_t aggregate_read_io_cnt_;
  INHERIT_TO_STRING_KV("ObTmpFileInfo", ObTmpFileInfo,
                       K(aggregate_read_io_cnt_));
};

struct ObSSTmpFileFlushContext
{
public:
  explicit ObSSTmpFileFlushContext(const blocksstable::MacroBlockId &id,
                                   const common::ObIOFlag &io_desc,
                                   const int64_t io_timeout_ms,
                                   const int64_t max_flush_page_count);
  void init(const int64_t write_info_offset, const uint32_t cur_page_id, const int64_t cur_virtual_page_id);
  void append_batch(blocksstable::ObStorageObjectHandle *object_handle, char *flush_buff);
  void append_page(const int64_t this_page_cpy_len, const uint32_t next_page_id, const int64_t next_virtual_page_id);
  int flush_io();
  void finish_batch();
  OB_INLINE bool should_continue() const
  {
    return cur_flush_page_count_ < max_flush_page_count_;
  }
  OB_INLINE bool should_this_batch_continue() const
  {
    return cur_flush_page_count_ < max_flush_page_count_ && !can_seal_;
  }
  OB_INLINE int32_t get_batch_expected_flush_size() const
  {
    return ObTmpFileGlobal::SS_BLOCK_SIZE - write_info_offset_ % ObTmpFileGlobal::SS_BLOCK_SIZE;
  }
  OB_INLINE void set_can_seal()
  {
    can_seal_ = true;
  }
  TO_STRING_KV(K(is_finished_), K(id_.second_id()), K(io_desc_), K(io_timeout_ms_),
               KP(object_handle_), KP(flush_buff_),
               K(cur_flush_page_count_), K(max_flush_page_count_),
               K(write_info_offset_), K(this_batch_flush_data_size_),
               K(cur_page_id_), K(cur_virtual_page_id_), K(can_seal_));

public:
  bool is_finished_;
  const blocksstable::MacroBlockId id_;                  // temporary file id
  const common::ObIOFlag io_desc_;                       // io flage from user
  int64_t io_timeout_ms_;
  blocksstable::ObStorageObjectHandle * object_handle_;  // write handle for once batch
  char * flush_buff_;                      // write buffer for once batch
  int64_t cur_flush_page_count_;           // total flushed page nums for once flush
  const int64_t max_flush_page_count_;     // max flushed page nums for once flush
  int64_t write_info_offset_;              // write_info_offset_ % SS_BLOCK_SIZE = write offset in macro block
                                           // this var records the offset has been flushed
  int64_t this_batch_flush_data_size_;     // flushed data size for once batch
  uint32_t cur_page_id_;                   // current page id in wbp for each `append_page`
  int64_t cur_virtual_page_id_;            // current virtual page id in wbp for each `append_page`
  bool can_seal_;                          // can this io batch set `sealed_`
};

class ObSharedStorageTmpFile final : public ObITmpFile
{
public:
  ObSharedStorageTmpFile();
  ~ObSharedStorageTmpFile();
  int init(const uint64_t tenant_id, const blocksstable::MacroBlockId id, ObTmpWriteBufferPool *wbp,
           ObSSTmpFileFlushManager *flush_mgr,
           ObSSTmpFileRemoveManager *remove_mgr,
           ObIAllocator *callback_allocator,
           ObIAllocator *wbp_index_cache_allocator,
           ObIAllocator *wbp_index_cache_bkt_allocator,
           const char* label);
  virtual void reset() override;
  virtual int release_resource() override;

  // for flush
  int update_meta_after_flush(const ObSSTmpFileAsyncFlushWaitTask &wait_task);
  int flush(const bool flush_as_block,
            const common::ObIOFlag io_flag,
            const int64_t io_timeout_ms,
            int64_t &flush_size,
            ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle);
  // for virtual table
  virtual int copy_info_for_virtual_table(ObTmpFileInfo &tmp_file_info) override;

  int64_t get_data_page_nums(const bool only_full_page = false);
  INHERIT_TO_STRING_KV("ObITmpFile", ObITmpFile,
                       K(id_), KP(flush_mgr_), KP(remove_mgr_), K(aggregate_read_io_cnt_));

private:
  virtual int inner_read_from_disk_(const int64_t expected_read_disk_size, ObTmpFileIOCtx &io_ctx) override;
  int inner_direct_read_from_block_(const int64_t block_index,
                                    const int64_t begin_read_offset_in_block,
                                    const int64_t end_read_offset_in_block,
                                    ObTmpFileIOCtx &io_ctx);
  int inner_aggregate_read_from_block_(const int64_t block_index,
                                       const int64_t begin_read_offset_in_block,
                                       const int64_t end_read_offset_in_block,
                                       ObTmpFileIOCtx &io_ctx,
                                       int64_t &total_kv_cache_page_read_cnt,
                                       int64_t &total_uncached_page_read_cnt,
                                       int64_t &kv_cache_page_read_hits,
                                       int64_t &uncached_page_read_hits,
                                       int64_t &aggregate_read_io_cnt);
  int collect_page_io_infos_(const int64_t block_index,
                             const int64_t begin_read_offset_in_block,
                             const int64_t end_read_offset_in_block,
                             const bool need_aggregate_read,
                             ObTmpFileIOCtx &io_ctx,
                             common::ObIArray<ObTmpFileIOCtx::ObPageCacheHandle> &page_cache_handles,
                             common::ObIArray<std::pair<ObTmpPageCacheKey, int64_t>> &page_infos);
  virtual int swap_page_to_disk_(const ObTmpFileIOCtx &io_ctx) override;
  virtual int load_disk_tail_page_and_rewrite_(ObTmpFileIOCtx &io_ctx) override;
  virtual int append_write_memory_tail_page_(ObTmpFileIOCtx &io_ctx) override;

  int adjust_flush_wait_task_info_(const int64_t flushed_offset, const int64_t file_size_when_flush,
                                   uint32_t &wait_task_begin_page_id,
                                   int64_t &wait_task_begin_page_virtual_id,
                                   int64_t &expected_flushed_page_num,
                                   int64_t &flushed_page_num);
  void try_to_seal_in_flush_(const int64_t flushed_offset, const int64_t expected_flushed_page_num,
                             blocksstable::ObStorageObjectHandle &obj_handle,
                             int64_t &flushed_page_num);
  int rollback_failed_flush_pages_(const int64_t failed_page_num, const uint32_t start_page_id,
                                   const int64_t start_virtual_page_id);
  int inner_async_flush_(const bool flush_all_pages,
                         const common::ObIOFlag io_flag,
                         const int64_t io_timeout_ms,
                         int64_t &flush_size,
                         ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle);
  int inner_async_flush_block_(ObSSTmpFileFlushContext &flush_context, int64_t &batch_flush_size);

  virtual int truncate_persistent_pages_(const int64_t truncate_offset) override;
  virtual int truncate_the_first_wbp_page_() override;

  virtual int inner_seal_() override;
  virtual int inner_delete_file_() override;

  // for virtual table
  virtual void inner_set_read_stats_vars_(const ObTmpFileIOCtx &ctx, const int64_t read_size) override;
private:
  // `cal_max_flush_page_nums_()` should be under the protection of meta lock.
  int64_t cal_max_flush_page_nums_(const bool flush_all_pages);
  int64_t get_data_page_nums_(const bool only_full_page = false);
  int64_t try_align_flush_page_nums_to_block_(const int64_t max_flush_page_nums,
                                              const int64_t write_info_offset) const;
  virtual bool is_flushing_() override;
  virtual int64_t get_dirty_data_page_size_() const override;
  OB_INLINE int64_t get_last_flushed_block_id_(const int64_t last_flushed_page_virtual_id)
  {
    return last_flushed_page_virtual_id / ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS;
  }
  OB_INLINE int64_t get_block_virtual_id_(const int64_t offset_in_file, const bool is_open_interval) const
  {
    return is_open_interval ?
           common::upper_align(offset_in_file, ObTmpFileGlobal::SS_BLOCK_SIZE) / ObTmpFileGlobal::SS_BLOCK_SIZE - 1 :
           offset_in_file / ObTmpFileGlobal::SS_BLOCK_SIZE;
  }
private:
  blocksstable::MacroBlockId id_;
  ObSSTmpFileFlushManager *flush_mgr_;
  ObSSTmpFileRemoveManager *remove_mgr_;

  /********for virtual table begin********/
  int64_t aggregate_read_io_cnt_;
  /********for virtual table end  ********/
};

class ObSSTmpFileHandle final : public ObITmpFileHandle
{
public:
  ObSSTmpFileHandle() : ObITmpFileHandle() {}
  ObSSTmpFileHandle(ObSharedStorageTmpFile *ptr) : ObITmpFileHandle(ptr) {}
  ObSSTmpFileHandle(const ObSSTmpFileHandle &handle) : ObITmpFileHandle(handle) {}
  ObSSTmpFileHandle & operator=(const ObSSTmpFileHandle &other)
  {
    ObITmpFileHandle::operator=(other);
    return *this;
  }
  OB_INLINE ObSharedStorageTmpFile * get() const { return static_cast<ObSharedStorageTmpFile*>(ptr_); }
};
}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_STORAGE_TMP_FILE_H_
