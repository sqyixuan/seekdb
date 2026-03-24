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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_TASK_H_

#include "lib/task/ob_timer.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace storage
{

class ObPrereadCacheManager;

class ObPreReadFileMeta final
{
public:
  ObPreReadFileMeta() : file_id_(), func_type_(0) {}
  ObPreReadFileMeta(const blocksstable::MacroBlockId &file_id, const uint8_t func_type)
      : file_id_(file_id), func_type_(func_type) {}
  ObPreReadFileMeta &operator=(const ObPreReadFileMeta &other);
  ~ObPreReadFileMeta() { reset(); }
  void reset();
  bool is_valid() const { return file_id_.is_valid(); }
  TO_STRING_KV(K_(file_id), K_(func_type));

public:
  blocksstable::MacroBlockId file_id_;
  uint8_t func_type_;
};

class ObSSPreReadEntry final
{
public:
  ObSSPreReadEntry(common::ObIAllocator &allocator)
      : buf_(nullptr), file_meta_(), read_handle_(), write_handle_(), allocator_(allocator) {}
  ~ObSSPreReadEntry() { reset(); }
  int init(const ObPreReadFileMeta &file_meta);
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
  ObPreReadFileMeta file_meta_;
  blocksstable::ObStorageObjectHandle read_handle_;
  blocksstable::ObStorageObjectHandle write_handle_;
  common::ObIAllocator &allocator_;
};

class ObSSPreReadTask : public common::ObTimerTask
{
public:
  ObSSPreReadTask();
  virtual ~ObSSPreReadTask() { destroy(); }
  int init(ObPrereadCacheManager *preread_cache_mgr);
  int start(const int tg_id);
  void destroy();

  virtual void runTimerTask() override;
  int schedule_preread_task();

private:
  void destroy_preread_entry_arr();
  int pre_alloc_preread_entry();
  int preread_files();
  int batch_get_segment_file_ids();
  int try_alloc_preread_cache_size(const int64_t file_size, blocksstable::ObStorageObjectType object_type);
  int do_async_read_segment_file(ObSSPreReadEntry &preread_entry);
  int do_async_write_segment_file(ObSSPreReadEntry &preread_entry);
  int async_read_segment_files();
  int async_write_segment_files();
  int complete_pre_read_segment_files();
  int recycle_preread_entry(ObSSPreReadEntry &preread_entry);
  void clear_for_next_round();
  void set_read_cache_parallel_cnt();
  void adjust_pre_read_task_interval();

private:
  static const int64_t SLOW_SCHEDULE_INTERVAL_US = 10 * 1000L; // 10ms
  static const int64_t MODERATE_SCHEDULE_INTERVAL_US = 100L; // 100us, wait for in flight object storage io
  static const int64_t FAST_SCHEDULE_INTERVAL_US = 10L; // 10us, wait for in flight local disk io
  static const int64_t UPDATE_PARALLEL_INTERVAL_US = 60 * 1000 * 1000L; // 1min
  static const int64_t MAX_PRE_READ_PARALLELISM = 30;
  static const int64_t DEFAULT_PRE_READ_PARALLELISM = 5;
  static const int64_t GB = 1024L * 1024L * 1024L;

private:
  bool is_inited_;
  int tg_id_;
  uint64_t tenant_id_;
  int64_t preread_parallel_cnt_;
  int64_t interval_us_;
  ObPrereadCacheManager *preread_cache_mgr_;
  ObSSPreReadEntry *preread_entry_arr_;
  common::ObFixedQueue<ObSSPreReadEntry> free_list_;
  common::ObFixedQueue<ObSSPreReadEntry> async_read_list_;
  common::ObFixedQueue<ObSSPreReadEntry> async_write_list_;
  common::ObArray<ObPreReadFileMeta> segment_files_;
  common::ObArray<ObPreReadFileMeta> preread_files_;
  common::ObFIFOAllocator allocator_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_TASK_H_ */
