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

#ifndef OCEANBASE_STORAGE_CACHE_TABLET_SCHEDULER_H_
#define OCEANBASE_STORAGE_CACHE_TABLET_SCHEDULER_H_
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_ptr_handle.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
namespace oceanbase
{
namespace storage
{
class ObStorageCacheTabletTask;
typedef ObPtrHandle<ObStorageCacheTabletTask> ObStorageCacheTabletTaskHandle;
typedef hash::ObHashMap<int64_t, ObStorageCacheTabletTaskHandle> SCPTabletTaskMap;
class ObStorageCacheTabletScheduler : public lib::TGTaskHandler
{
public:
  ObStorageCacheTabletScheduler();
  virtual ~ObStorageCacheTabletScheduler() { destroy(); };
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait(); 
  void destroy();
  int push_task(const uint64_t tenant_id, const int64_t ls_id, const int64_t tablet_id, const PolicyStatus &policy_status);
  int cancel_task(ObStorageCacheTabletTaskHandle &task_handle);
  virtual void handle(void *task_handle) override;
  virtual void handle_drop(void *task_handle) override;

  TO_STRING_KV(K_(tenant_id), K_(is_inited), K_(tg_id));
public:
  static const int64_t MAX_TASK_NUM = 10000; // Maximum number of tasks in a queue thread
  static const int64_t DEFAULT_TABLET_TASK_MAP_SIZE = 10000; // The maximum number of tasks in the task map
public:
  SCPTabletTaskMap tablet_task_map_;

private:
  bool is_inited_;
  int tg_id_;
  uint64_t tenant_id_;
};

struct ObStorageCacheTaskStatus
{
  enum TaskStatus : uint8_t
  {
    OB_STORAGE_CACHE_TASK_INIT = 0,
    OB_STORAGE_CACHE_TASK_DOING = 1,
    OB_STORAGE_CACHE_TASK_FINISHED = 2,
    OB_STORAGE_CACHE_TASK_FAILED = 3,
    OB_STORAGE_CACHE_TASK_SUSPENDED = 4,
    OB_STORAGE_CACHE_TASK_CANCELED = 5,
    OB_STORAGE_CACHE_TASK_STATUS_MAX
  };
  static const char *get_str(const TaskStatus &type);
  static TaskStatus get_type(const char *type_str);
  static OB_INLINE bool is_valid(const TaskStatus &type)
  {
    return type >= 0 && type < TaskStatus::OB_STORAGE_CACHE_TASK_STATUS_MAX;
  }
  static bool is_completed_status(const TaskStatus &type)
  {
    return type == TaskStatus::OB_STORAGE_CACHE_TASK_FINISHED
        || type == TaskStatus::OB_STORAGE_CACHE_TASK_FAILED
        || type == TaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED
        || type == TaskStatus::OB_STORAGE_CACHE_TASK_CANCELED;
  }
};
typedef ObStorageCacheTaskStatus::TaskStatus ObStorageCacheTaskStatusType;
class ObStorageCacheTabletTask
{
public:
  ObStorageCacheTabletTask();
  ~ObStorageCacheTabletTask() {
    destroy();
  };
  int init(const uint64_t tenant_id, const int64_t ls_id, const int64_t tablet_id, const PolicyStatus &policy_status);
  void destroy();
  void inc_ref_count();
  void dec_ref_count();
  int process();
  ObStorageCacheTaskStatusType get_status() const;
  PolicyStatus get_policy_status() const;
  bool is_canceled() const;
  bool is_completed() const;
  int64_t get_tablet_id() const;
  int64_t get_ls_id() const;
  uint64_t get_tenant_id() const;
  int64_t get_ref_count() const;
  TabletMajorPrewarmStat get_prewarm_stat() const;
  double get_speed() const;
  int64_t get_start_time() const;
  int64_t get_end_time() const;
  int get_result() const;
  const ObString &get_comment() const;

  int set_canceled();
  /**
   * @brief Attempts to transition the task to the target status with concurrency control
   * 
   * @param target_status Target state to transition to
   * @param origin_status [out] Returns the original state before transition attempt
   * @param succ          [out] Indicates whether the state transition succeeded
   * 
   * @return int - Operation status codes:
   *   OB_FAIL: Indicates invalid task state or illegal transition path
   *   OB_SUCCESS with succ=false: Valid state but transition failed due to concurrent modifications
   *   OB_SUCCESS with succ=true: Successful state transition to target_status
   */
  int set_status(
      const ObStorageCacheTaskStatusType &target_status,
      ObStorageCacheTaskStatusType &origin_status,
      bool &succ);
  // == set_status(target_status, origin_status, succ) without [out] param
  int set_status(const ObStorageCacheTaskStatusType &target_status);
  void set_tablet_id(const int64_t tablet_id);
  void set_start_time(const int64_t start_time);
  // This function is only used for test!!!!
  void set_end_time(const int64_t end_time);
  void set_ls_id(const int64_t ls_id);
  void set_tenant_id(const uint64_t tenant_id);
  void set_prewarm_stat(const TabletMajorPrewarmStat &stat);

  int do_prewarm();

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(ref_count),
      K_(is_inited), K_(is_stop), K_(is_canceled), K(prewarm_stat_),
      K(start_time_), K(end_time_), K(comment_), K(result_),
      K_(status), K(ObStorageCacheTaskStatus::get_str(status_)), K(policy_status_));

private:
  bool is_inited_;
  bool is_stop_;
  volatile bool is_canceled_;
  uint64_t tenant_id_;
  int64_t ls_id_;
  int64_t tablet_id_;
  int64_t ref_count_;
  common::SpinRWLock task_lock_;
  ObStorageCacheTaskStatusType status_;
  PolicyStatus policy_status_;
  int64_t start_time_;
  int64_t end_time_;
  ObString comment_;
  int result_;
  TabletMajorPrewarmStat prewarm_stat_;
  ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageCacheTabletTask);
};
}
}
#endif
