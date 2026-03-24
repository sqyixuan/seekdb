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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_MANAGER_H_

#include "lib/list/ob_dlist.h"
#include "lib/task/ob_timer.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/tmp_file/ob_tmp_file_flush_priority_manager.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace tmp_file
{
class ObSSTmpFileAsyncFlushWaitTaskHandle;
class ObSSTmpFileFlushContext;
class ObSSTmpFileFlushManager;
class ObSSTenantTmpFileManager;
class ObSharedStorageTmpFile;

struct ObSSTmpFileAsyncFlushWaitTask
{
public:
  friend class ObSSTmpFileAsyncFlushWaitTaskHandle;

public:
  ObSSTmpFileAsyncFlushWaitTask();
  ~ObSSTmpFileAsyncFlushWaitTask();
  int init(const int64_t fd, const int64_t length, const uint32_t begin_page_id,
           const int64_t current_begin_page_virtual_id,
           const int64_t expected_flushed_page_num,
           const ObMemAttr attr,
           const ObTmpFileGlobal::FlushCtxState current_flush_state,
           ObSSTmpFileFlushManager *flush_mgr);
  int push_back_io_task(const ObSSTmpFileFlushContext &ctx, const int64_t flushed_page_num);
  int exec_wait();
  int cond_broadcast(int32_t ret_code);
  int cond_wait(const int64_t timeout_ms);
  OB_INLINE bool have_io_task() const { return !io_tasks_.empty(); }
  OB_INLINE int32_t get_ret_code() const { return ret_code_; }

  TO_STRING_KV(K(fd_), K(current_length_), K(current_begin_page_id_),
               K(current_begin_page_virtual_id_),
               K(flushed_offset_), K(expected_flushed_page_num_),
               K(succeed_wait_page_nums_), K(current_flush_state_),
               K(ref_cnt_), K(wait_has_finished_),
               K(is_inited_), K(ret_code_),
               K(io_page_num_.count()),
               K(io_tasks_.count()));

private:
  int release_io_tasks_();
  OB_INLINE void inc_ref_cnt_()
  {
    ATOMIC_INC(&ref_cnt_);
  }
  OB_INLINE void dec_ref_cnt_(int32_t *new_ref_cnt = nullptr)
  {
    int32_t ref_cnt = -1;
    ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
    if (OB_NOT_NULL(new_ref_cnt)) {
      *new_ref_cnt = ref_cnt;
    }
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTmpFileAsyncFlushWaitTask);

public:
  int64_t fd_;
  int64_t current_length_;                  // when generate a flush task, record current file length
  uint32_t current_begin_page_id_;          // when generate a flush task, record current page id
  int64_t current_begin_page_virtual_id_;   // when generate a flush task, record current virtual page id
  int64_t flushed_offset_;                  // when flush task are generated over, record current flushed offset
                                            // (it is target flushing offset of this task, which is aligned with page size [8KB])
  ObArray<std::pair<blocksstable::ObStorageObjectHandle *, char *>> io_tasks_;
  ObArray<int64_t> io_page_num_;
  int64_t expected_flushed_page_num_;      // flushing page nums in async thread
  int64_t succeed_wait_page_nums_;
  ObTmpFileGlobal::FlushCtxState current_flush_state_;

private:
  ObSSTmpFileFlushManager *flush_mgr_;
  ObThreadCond cond_;
  int32_t ref_cnt_;
  bool wait_has_finished_;
  bool is_inited_;
  int32_t ret_code_;
};

struct ObSSTmpFileAsyncFlushWaitTaskHandle : public common::ObDLink
{
public:
  ObSSTmpFileAsyncFlushWaitTaskHandle();
  ObSSTmpFileAsyncFlushWaitTaskHandle(ObSSTmpFileAsyncFlushWaitTask *wait_task);
  ~ObSSTmpFileAsyncFlushWaitTaskHandle();
  void set_wait_task(ObSSTmpFileAsyncFlushWaitTask *wait_task);
  void reset();
  OB_INLINE ObSSTmpFileAsyncFlushWaitTask * get() { return wait_task_; }
  int wait(const int64_t timeout_ms);
  TO_STRING_KV(KPC(wait_task_));

public:
  ObSSTmpFileAsyncFlushWaitTask *wait_task_;
};

class ObTmpFileAsyncWaitTaskQueue
{
public:
  ObTmpFileAsyncWaitTaskQueue();
  ~ObTmpFileAsyncWaitTaskQueue();
  int push(ObSSTmpFileAsyncFlushWaitTaskHandle *task_handle);
  int pop(ObSSTmpFileAsyncFlushWaitTaskHandle *&task_handle);
  OB_INLINE bool is_empty() const { return queue_.is_empty(); }
  OB_INLINE int64_t get_queue_length() const { return ATOMIC_LOAD(&queue_length_); }

  TO_STRING_KV(K(queue_length_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileAsyncWaitTaskQueue);

private:
  common::ObSpLinkQueue queue_;
  int64_t queue_length_;
};

class ObSSTmpFileWaitTimerTask : public common::ObTimerTask
{
public:
  ObSSTmpFileWaitTimerTask() : is_inited_(false), flush_mgr_(nullptr) {}
  virtual ~ObSSTmpFileWaitTimerTask() {}
  int init(ObSSTmpFileFlushManager *flush_mgr);
  virtual void runTimerTask() override;
  TO_STRING_KV(K(is_inited_), KP(flush_mgr_));
private:

private:
  bool is_inited_;
  ObSSTmpFileFlushManager *flush_mgr_;
};

class ObSSTmpFileFlushManager
{
public:
  ObSSTmpFileFlushManager() : is_inited_(false), tg_id_(OB_INVALID_INDEX),
                              flush_prio_mgr_(),
                              wait_task_allocator_(), wait_task_queue_(),
                              wait_timer_task_(),
                              file_mgr_(nullptr),
                              wbp_(nullptr),
                              stat_lock_(),
                              total_flushing_page_num_(0),
                              f1_cnt_(0),
                              f2_cnt_(0),
                              f3_cnt_(0)
                              {}
  virtual ~ObSSTmpFileFlushManager() {}
  int init(const uint64_t tenant_id, const ObSSTenantTmpFileManager *file_mgr,
           ObTmpWriteBufferPool *wbp);
  int start();
  void stop();
  int wait();
  void destroy();
  int exec_wait_task_once();
  void print_stat_info();

public:
  typedef ObTmpFileGlobal::FlushCtxState FlushCtxState;
  int wash(const int64_t expect_wash_size, common::ObIOFlag io_flag,
           ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle, int64_t &actual_wash_size);
  int update_meta_after_flush(const ObSSTmpFileAsyncFlushWaitTask &wait_task) const;
  int notify_flush_breakdown(const int64_t fd) const;
  int wait_task_enqueue(ObSSTmpFileAsyncFlushWaitTask *task);

  ObTmpFileFlushPriorityManager &get_flush_prio_mgr() { return flush_prio_mgr_; }
  common::ObFIFOAllocator& get_wait_task_allocator() { return wait_task_allocator_; }
  ObTmpFileAsyncWaitTaskQueue& get_wait_task_queue() { return wait_task_queue_; }
  OB_INLINE int64_t get_task_num() const { return wait_task_queue_.get_queue_length(); }
  OB_INLINE bool have_task() const { return wait_task_queue_.get_queue_length() > 0; }
  OB_INLINE int64_t get_flushing_data_size() const { return total_flushing_page_num_; }

  TO_STRING_KV(K(is_inited_), K(tg_id_), KP(&flush_prio_mgr_), KP(&wait_task_allocator_), K(wait_task_queue_),
               K(total_flushing_page_num_), K(f1_cnt_), K(f2_cnt_), K(f3_cnt_));
private:
  int record_stat_info_(const FlushCtxState &stage, const int64_t flushing_page_num);
  int modify_stat_info_(const FlushCtxState &stage, const int64_t flushed_page_num);
private:
  bool is_inited_;
  int tg_id_;
  ObTmpFileFlushPriorityManager flush_prio_mgr_;
  common::ObFIFOAllocator wait_task_allocator_;
  ObTmpFileAsyncWaitTaskQueue wait_task_queue_;
  ObSSTmpFileWaitTimerTask wait_timer_task_;
  const ObSSTenantTmpFileManager *file_mgr_;
  ObTmpWriteBufferPool *wbp_;

  // statistics
  ObSpinLock stat_lock_;
  int64_t total_flushing_page_num_;
  int64_t f1_cnt_;
  int64_t f2_cnt_;
  int64_t f3_cnt_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_MANAGER_H_
