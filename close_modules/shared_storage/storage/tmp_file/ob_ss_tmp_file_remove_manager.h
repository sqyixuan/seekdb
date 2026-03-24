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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_REMOVE_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_REMOVE_MANAGER_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/task/ob_timer.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_i_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{
class ObSSTmpFileRemoveManager;

class ObTmpFileAsyncRemoveTask : public common::ObDLink
{
public:
  explicit ObTmpFileAsyncRemoveTask(const blocksstable::MacroBlockId &tmp_file_id, const int64_t length);
  ~ObTmpFileAsyncRemoveTask() = default;
  int exec_remove() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  const blocksstable::MacroBlockId tmp_file_id_;
  const int64_t length_;
};

class ObTmpFileAsyncRemoveTaskQueue
{
public:
  ObTmpFileAsyncRemoveTaskQueue();
  ~ObTmpFileAsyncRemoveTaskQueue();
  int push(ObTmpFileAsyncRemoveTask * remove_task);
  int pop(ObTmpFileAsyncRemoveTask *& remove_task);
  int top(ObTmpFileAsyncRemoveTask *& remove_task);
  OB_INLINE bool is_empty() const { return queue_.is_empty(); }
  OB_INLINE int64_t get_queue_length() const { return ATOMIC_LOAD(&queue_length_); }
  TO_STRING_KV(K(queue_length_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileAsyncRemoveTaskQueue);

private:
  common::ObSpLinkQueue queue_;
  int64_t queue_length_;
};

class ObSSTmpFileRemoveTimerTask : public common::ObTimerTask
{
public:
  ObSSTmpFileRemoveTimerTask() : is_inited_(false),
                          scan_invalid_files_ts_(-1),
                          remove_mgr_(nullptr) {}
  virtual ~ObSSTmpFileRemoveTimerTask() { destroy(); }
  int init(ObSSTmpFileRemoveManager *remove_mgr);
  void destroy();
  virtual void runTimerTask() override;
  TO_STRING_KV(K(is_inited_), K(scan_invalid_files_ts_), KP(remove_mgr_));

  static const int64_t SCAN_INVALID_FILES_INTERVAL = 12 * 60L * 60 * 1000 * 1000 /* 12 hours */;
private:
  bool is_inited_;
  int64_t scan_invalid_files_ts_;
  ObSSTmpFileRemoveManager *remove_mgr_;
};

class ObSSTmpFileRemoveManager
{
private:
  typedef ObITenantTmpFileManager::TmpFileMap TmpFileMap;
public:
  ObSSTmpFileRemoveManager() : is_inited_(false),
                               tg_id_(OB_INVALID_INDEX),
                               remove_task_allocator_(),
                               remove_task_queue_(),
                               first_tmp_file_id_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
                               remove_timer_task_() {}
  virtual ~ObSSTmpFileRemoveManager() {}
  int init(const uint64_t tenant_id, const TmpFileMap *files);
  int start();
  void stop();
  int wait();
  int destroy();
  int scan_invalid_files();
  int exec_remove_task_once();
  int remove_task_enqueue(const blocksstable::MacroBlockId &tmp_file_id, const int64_t length);
  OB_INLINE int64_t get_task_num() const { return remove_task_queue_.get_queue_length(); }
  OB_INLINE bool have_task() const { return remove_task_queue_.get_queue_length() > 0; }
  TO_STRING_KV(K(is_inited_), KP(files_), KP(&remove_task_allocator_), K(remove_task_queue_),
               K(first_tmp_file_id_), K(remove_timer_task_));
private:
  int exec_reboot_gc_once_();
  int get_first_tmp_file_id_();

private:
  bool is_inited_;
  int tg_id_;
  const TmpFileMap *files_;
  common::ObFIFOAllocator remove_task_allocator_;
  ObTmpFileAsyncRemoveTaskQueue remove_task_queue_;
  int64_t first_tmp_file_id_;
  ObSSTmpFileRemoveTimerTask remove_timer_task_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_REMOVE_MANAGER_H_
