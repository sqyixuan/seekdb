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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_TMP_FILE_FLUSH_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_TMP_FILE_FLUSH_TASK_H_

#include "deps/oblib/src/lib/task/ob_timer.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace storage
{

class ObTenantFileManager;
class ObTenantDiskSpaceManager;

class ObFlushTmpFileMeta final
{
public:
  ObFlushTmpFileMeta() : file_id_(), file_len_(0), func_type_(0), is_sealed_(true), valid_length_(0) {}
  ObFlushTmpFileMeta(
      const blocksstable::MacroBlockId &file_id, const int64_t file_len, const uint8_t func_type, const bool is_sealed, const int64_t valid_length)
      : file_id_(file_id), file_len_(file_len), func_type_(func_type), is_sealed_(is_sealed), valid_length_(valid_length) {}
  ObFlushTmpFileMeta &operator=(const ObFlushTmpFileMeta &other);
  ~ObFlushTmpFileMeta() { reset(); }
  void reset();
  bool is_valid() const { return file_id_.is_valid() && file_len_ > 0; }
  TO_STRING_KV(K_(file_id), K_(file_len), K_(func_type), K_(is_sealed), K_(valid_length));

public:
  blocksstable::MacroBlockId file_id_;
  int64_t file_len_;
  uint8_t func_type_;
  bool is_sealed_; // mark whether seg file is sealed when flush
  int64_t valid_length_; // when unsealed seg file flush, used for concat file path
};

class ObSSTmpFileFlushEntry final
{
public:
  ObSSTmpFileFlushEntry(common::ObIAllocator &allocator)
      : buf_(nullptr), file_meta_(), read_handle_(), write_handle_(), allocator_(allocator) {}
  ~ObSSTmpFileFlushEntry() { reset(); }
  int init(const ObFlushTmpFileMeta &file_meta);
  bool is_valid() const { return buf_ != nullptr && file_meta_.is_valid(); }
  bool is_read_handle_valid() const { return read_handle_.is_valid(); }
  bool is_write_handle_valid() const { return write_handle_.is_valid(); }
  void reset();
  share::ObFunctionType get_function_type() const
  {
    return static_cast<share::ObFunctionType>(file_meta_.func_type_);
  }
  TO_STRING_KV(K_(buf), K_(file_meta), K_(read_handle), K_(write_handle));

public:
  char *buf_;
  ObFlushTmpFileMeta file_meta_;
  blocksstable::ObStorageObjectHandle read_handle_;
  blocksstable::ObStorageObjectHandle write_handle_;
  common::ObIAllocator &allocator_;
};

class ObSSTmpFileFlushTask : public common::ObTimerTask
{
public:
  ObSSTmpFileFlushTask();
  virtual ~ObSSTmpFileFlushTask() { destroy(); }
  int init(ObTenantFileManager *file_manager);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;
  int schedule_tmp_file_flush_task();

private:
  void destroy_flush_entry_arr();
  int pre_alloc_flush_entry();
  int flush_tmp_files();
  int batch_get_seg_files();
  int async_write_seg_files();
  int async_read_seg_files();
  int do_async_read_seg_file(ObSSTmpFileFlushEntry &flush_entry);
  int do_async_write_seg_file(ObSSTmpFileFlushEntry &flush_entry);
  int prepare_check_before_move(const ObFlushTmpFileMeta &file_meta);
  int move_from_write_cache_to_read_cache(const ObFlushTmpFileMeta &file_meta);
  int handle_unsealed_file_meta(const ObFlushTmpFileMeta &file_meta);
  int complete_flush_seg_files();
  int push_again_on_fail(const ObFlushTmpFileMeta &file_meta);
  int recycle_flush_entry(ObSSTmpFileFlushEntry &flush_entry);
  void clear_for_next_round();
  void set_flush_parallel_cnt();
  void adjust_flush_task_interval();

public:
  static const int64_t SLOW_SCHEDULE_INTERVAL_US = 100 * 1000L; // 100ms
  static const int64_t MODERATE_SCHEDULE_INTERVAL_US = 10 * 1000L; // 10ms
  static const int64_t FAST_SCHEDULE_INTERVAL_US = 100L; // 100us
  static const int64_t UPDATE_PARALLEL_INTERVAL_US = 60 * 1000 * 1000L; // 1min
  static const int64_t MAX_FLUSH_PARALLELISM = 30;
  static const int64_t DEFAULT_FLUSH_PARALLELISM = 5;
  static const int64_t GB = 1024L * 1024L * 1024L;

private:
  bool is_inited_;
  int tg_id_;
  int64_t flush_parallel_cnt_;
  int64_t interval_us_;
  ObTenantFileManager *file_manager_;
  ObSSTmpFileFlushEntry *flush_entry_arr_;
  common::ObFixedQueue<ObSSTmpFileFlushEntry> free_list_;
  common::ObFixedQueue<ObSSTmpFileFlushEntry> async_read_list_;
  common::ObFixedQueue<ObSSTmpFileFlushEntry> async_write_list_;
  common::ObArray<ObFlushTmpFileMeta> seg_files_;
  common::ObArray<ObFlushTmpFileMeta> flushed_files_; // tmp_files succeed to flush.
  common::ObFIFOAllocator allocator_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_TMP_FILE_FLUSH_TASK_H_ */
