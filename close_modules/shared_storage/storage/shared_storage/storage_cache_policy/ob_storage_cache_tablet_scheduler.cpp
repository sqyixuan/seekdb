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
#include "share/ob_define.h"
#include "share/ob_thread_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
/******************ObStorageCacheTabletScheduler*******************/
ObStorageCacheTabletScheduler::ObStorageCacheTabletScheduler()
  : tablet_task_map_(),
    is_inited_(false),   
    tg_id_(OB_INVALID_TG_ID),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

int ObStorageCacheTabletScheduler::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(tablet_task_map_.create(DEFAULT_TABLET_TASK_MAP_SIZE, ObMemAttr(MTL_ID(),"TabletTaskMap")))) {
    LOG_WARN("fail to create tablet task map", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StorageCacheTabletScheduler, tg_id_))) {
    LOG_WARN("fail to create tenant", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObStorageCacheTabletScheduler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("fail to start tenant", KR(ret));
  }   
  return ret;
}

void ObStorageCacheTabletScheduler::stop()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
  }
}

void ObStorageCacheTabletScheduler::wait()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
}

int ObStorageCacheTabletScheduler::push_task(
                                   const uint64_t tenant_id, 
                                   const int64_t ls_id, 
                                   const int64_t tablet_id, 
                                   const PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCacheTabletTask *task = nullptr;
  ObStorageCacheTabletTaskHandle ori_task_handle;
  ObStorageCacheTabletTaskHandle task_handle;
  bool should_skip = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant tablet storage cache tablet scheduler not init", KR(ret));
  } else if (OB_UNLIKELY(PolicyStatus::MAX_STATUS == policy_status || PolicyStatus::NONE == policy_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid policy status", KR(ret), K(tenant_id), K(ls_id), K(tablet_id), K(policy_status));
  } else if (FALSE_IT(should_skip = (PolicyStatus::AUTO == policy_status))) {
    // Currently, auto type tasks do not perform any operations. We will skip this task.
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_LOG_ID == ls_id || common::ObTabletID::INVALID_TABLET_ID == tablet_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(task = OB_NEW(ObStorageCacheTabletTask, ObMemAttr(tenant_id_, "SCPTask")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for tablet task", KR(ret));
  } else if (FALSE_IT(task_handle.set_ptr(task))) {
  } else if (OB_FAIL(task->init(tenant_id, ls_id, tablet_id, policy_status))) {
    LOG_WARN("fail to init task", KR(ret), K(task));
  } else if (OB_FAIL(tablet_task_map_.get_refactored(tablet_id, ori_task_handle))) {
    // If the task is not in the map, just insert it into the map
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get task from map", KR(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(ori_task_handle())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ori task handle is null", KR(ret), K(ori_task_handle));
  } else if (OB_FAIL(ori_task_handle()->set_status(
      ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED))) {
    LOG_WARN("fail to cancel origin task", KR(ret), KPC(ori_task_handle.get_ptr()));
  }

  // When task type is auto, we will skip the task.
  // TODO(baonian.wcx): After 4.4, we will support the auto type task
  if (OB_FAIL(ret)) {
  } else if (!should_skip) {
    // Do not wait for the queue thread to remove the task, 
    // overwrite the task here directly to insert the new task
    if (OB_FAIL(tablet_task_map_.set_refactored(tablet_id, task_handle, true/*overwrite*/))) {
      LOG_WARN("fail to set task", KR(ret), K(task_handle));
    } else {
      ObStorageCacheTabletTaskHandle *queue_task_handle = nullptr;
      if (OB_ISNULL(queue_task_handle = OB_NEW(ObStorageCacheTabletTaskHandle, ObMemAttr(tenant_id_, "SCPTaskHandle")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for queue task handle", KR(ret));
      } else if (FALSE_IT(queue_task_handle->set_ptr(task))) {
      } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, queue_task_handle))) {
        LOG_WARN("fail to push task", KR(ret), KPC(task), K(queue_task_handle), K(task->get_ref_count()), K(tg_id_));
      }
       // If push task fails, free memory and remove task from tablet_task_map
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        int64_t current_task_num = 0;
        if (OB_NOT_NULL(queue_task_handle)) {
          OB_DELETE(ObStorageCacheTabletTaskHandle, ObMemAttr(tenant_id_, "SCPTaskHandle"), queue_task_handle);
        } else if (OB_FAIL(tablet_task_map_.erase_refactored(tablet_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to erase task after pushing task failed", KR(ret), K(tablet_id), K(tenant_id), K(ls_id), K(policy_status));
          }
        }
      }
    }
  }
  LOG_TRACE("[SCP]storage cache policy push task into map and queue", KR(ret), K(should_skip), K(tenant_id), K(ls_id), K(tablet_id), K(policy_status), KPC(task));
  return ret;
}

void ObStorageCacheTabletScheduler::handle(void *task_handle)
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard scp_trace_id_guard;
  ObStorageCacheTabletTaskHandle *tablet_task_handle = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else if (OB_ISNULL(task_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task_handle));
  } else {
    tablet_task_handle = static_cast<ObStorageCacheTabletTaskHandle *>(task_handle);
    if (OB_ISNULL(tablet_task_handle) || OB_ISNULL((*tablet_task_handle)())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet task handle is null", KR(ret), K(tablet_task_handle), K((*tablet_task_handle)()));
    } else if (OB_FAIL((*tablet_task_handle)()->process())) {
      LOG_WARN("fail to process task", KR(ret), K(tablet_task_handle));
    } else if (!((*tablet_task_handle)()->is_completed())) {
      LOG_WARN("invalid task status", KR(ret), K(tablet_task_handle), KPC((*tablet_task_handle)()));
    }
    LOG_TRACE("[SCP]storage cache policy handle task process", KR(ret), KPC(tablet_task_handle->get_ptr()), K(tablet_task_map_.size()));
    // After the task is completed, the task record is not deleted actively, waiting for the scheduled task to be cleaned up.
    // Todo(baonian.wcx): Users can filter the virtual table to see the task records in the completed state
  }
  // free memory
  if (OB_NOT_NULL(tablet_task_handle)) {
    OB_DELETE(ObStorageCacheTabletTaskHandle, "SCPTaskHandle", tablet_task_handle);
  }
  LOG_TRACE("storage cache policy handle task end", KR(ret), K(tablet_task_map_.size()));
}

void ObStorageCacheTabletScheduler::handle_drop(void *task_handle)
{
  int ret = OB_SUCCESS;
  ObStorageCacheTabletTaskHandle *tablet_task_handle = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else if (OB_ISNULL(task_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task_handle));
  } else {
    // if the thread has set stop, just try to free memory
    tablet_task_handle = static_cast<ObStorageCacheTabletTaskHandle *>(task_handle);
    if (OB_ISNULL(tablet_task_handle) || OB_ISNULL((*tablet_task_handle)())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tablet task handle is null", KR(ret), K(tablet_task_handle), K((*tablet_task_handle)()));
    } else if (OB_FAIL(tablet_task_map_.erase_refactored((*tablet_task_handle)()->get_tablet_id()))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to erase task", KR(ret), K((*tablet_task_handle)()->get_tablet_id()));
      }
    } 

    if (OB_NOT_NULL(tablet_task_handle)) {
      OB_DELETE(ObStorageCacheTabletTaskHandle, "SCPTaskHandle", tablet_task_handle);
    }
  }
}

void ObStorageCacheTabletScheduler::destroy()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = OB_INVALID_TG_ID;
  tablet_task_map_.destroy();
  is_inited_ = false;
}

/******************ObStorageCacheTabletTaskStatus*******************/
static const char *task_status_strs[] = {"WAITING", "DOING", "FINISHED", "FAILED", "SUSPENDED", "CANCELED"};
const char *ObStorageCacheTaskStatus::get_str(const TaskStatus &type)
{
  const char *str = nullptr;
  if (type < 0 || type >= TaskStatus::OB_STORAGE_CACHE_TASK_STATUS_MAX) {
    str = "UNKNOWN";
  } else {
    str = task_status_strs[type];
  }
  return str;
}



/******************ObStorageCacheTabletTask*******************/
ObStorageCacheTabletTask::ObStorageCacheTabletTask()
  : is_inited_(false),
    is_stop_(false),
    is_canceled_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(OB_INVALID_LOG_ID),
    tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
    ref_count_(0),
    task_lock_(common::ObLatchIds::STORAGE_CACHE_POLICY_TASK_LOCK),
    status_(ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX),
    policy_status_(PolicyStatus::MAX_STATUS),
    start_time_(0),
    end_time_(0),
    comment_(),
    result_(OB_SUCCESS),
    prewarm_stat_(),
    allocator_("SCPTabletTask")
{
}

int ObStorageCacheTabletTask::init(
                              const uint64_t tenant_id, 
                              const int64_t ls_id, 
                              const int64_t tablet_id, 
                              const PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_LOG_ID == ls_id
      || common::ObTabletID::INVALID_TABLET_ID == tablet_id
      || !ObStorageCachePolicyStatus::is_valid(policy_status)
      || PolicyStatus::NONE == policy_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(ls_id), K(tablet_id), K(policy_status));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT;
    policy_status_ = policy_status;
    is_inited_ = true;
    allocator_.set_tenant_id(tenant_id);
  }
  return ret;
}

void ObStorageCacheTabletTask::destroy()
{
  is_inited_ = false;
  is_stop_ = false;
  is_canceled_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_ = OB_INVALID_LOG_ID;
  tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  ref_count_ = 0;
  status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_STATUS_MAX;
  policy_status_ = PolicyStatus::MAX_STATUS;
  start_time_ = 0;
  end_time_ = 0;
  comment_.reset();
  result_ = OB_SUCCESS;
  prewarm_stat_.reset();
  allocator_.reset();
  task_lock_.destroy();
}

int ObStorageCacheTabletTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not init", KR(ret));
  } else {
    bool succ = false;
    ObStorageCacheTaskStatusType origin_status =
        ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX;
    set_start_time(ObTimeUtility::fast_current_time());

    switch (get_status()) {
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED: 
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED: {
        LOG_WARN("task is canceled or suspended", KR(ret), KPC(this));
        break;
      }
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT: {
        if (OB_FAIL(set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING,
            origin_status, succ))) {
          LOG_WARN("fail to update task to doing status", KR(ret), KPC(this));
        } else if (!succ) {
          // do nothing
        } else if (OB_FAIL(do_prewarm())) {
          // if ret is OB_CANCELED, means task is canceled by new task or suspended
          if (OB_CANCELED == ret) {
            const ObStorageCacheTaskStatusType cur_task_status = get_status();
            if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED != cur_task_status
                && ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED != cur_task_status) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to do prewarm, task is in unexpected status", KR(ret), KPC(this));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            LOG_WARN("fail to do prewarm", KR(ret), KPC(this));
          }
        } else if (OB_FAIL(set_status(
            ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED))) {
          LOG_WARN("fail to update task status", KR(ret), KPC(this));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid task status", KR(ret), KPC(this));
        break;
      }
    }

    if (OB_FAIL(ret) && OB_TMP_FAIL(set_status(
        ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED))) {
      LOG_WARN("fail to update task status", KR(ret), KR(tmp_ret), KPC(this));
    }

    set_end_time(ObTimeUtility::fast_current_time());
  }
  return ret;
}

int ObStorageCacheTabletTask::do_prewarm()
{
  int ret = OB_SUCCESS;
  const int64_t tenant_mem = MTL_MEM_SIZE();
  // < 8GB: 8
  // < 16GB: 16
  // < 24GB: 24
  // ...
  // max: 64
  const int64_t parallelism = MIN(64, (tenant_mem / 8 / 1024 / 1024 / 1024) * 8 + 8);
  ObStorageCachePolicyPrewarmer prewarmer(parallelism);
  
  const ObLSID ls_id(ls_id_);
  const ObTabletID tablet_id(tablet_id_);
  if (OB_FAIL(prewarmer.prewarm_hot_tablet(ls_id, tablet_id, this))) {
    LOG_WARN("fail to prewarm hot tablet", KR(ret), KPC(this));
  }

  // update result and comment
  SpinWLockGuard guard(task_lock_);
  result_ = ret;

  int tmp_ret = OB_SUCCESS;
  comment_.reset();
  char comment_buf[1024] = {0};
  int64_t pos = 0;
  const bool micro_cache_not_enough = 
      (prewarm_stat_.get_micro_block_bytes() >= prewarm_stat_.get_micro_cache_max_available_size());
  if (OB_TMP_FAIL(databuff_printf(
      comment_buf, sizeof(comment_buf), pos, "size: %ld.", prewarm_stat_.get_macro_block_bytes()))) {
    LOG_WARN("fail to construct comment", KR(ret), KPC(this));
  }
  if (micro_cache_not_enough) {
    if (OB_TMP_FAIL(databuff_printf(
        comment_buf, sizeof(comment_buf), pos, " current micro cache available size(%ld) is not enough.",
        prewarm_stat_.get_micro_cache_max_available_size()))) {
      LOG_WARN("fail to construct comment", KR(ret), KPC(this));
    } else {
      LOG_INFO("current task status", KR(ret), KR(result_), KPC(this));
    }
  }
  if (OB_SUCC(result_) || OB_CANCELED == result_) { // omit comment for successful or canceled task
  } else if (OB_TMP_FAIL(databuff_printf(
      comment_buf, sizeof(comment_buf), "%s, %s",
      common::ob_error_name(ret), common::ObCurTraceId::get_trace_id_str()))) {
    LOG_WARN("fail to construct comment", KR(ret), KR(tmp_ret), KPC(this));
  }
  return ret;
}

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
int ObStorageCacheTabletTask::set_status(
    const ObStorageCacheTaskStatusType &target_status,
    ObStorageCacheTaskStatusType &origin_status,
    bool &succ)
{
  int ret = OB_SUCCESS;
  origin_status = ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX;
  succ = false;
  SpinWLockGuard guard(task_lock_);
  origin_status = status_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageCacheTabletTask not init", KR(ret));
  } else if (OB_UNLIKELY(!ObStorageCacheTaskStatus::is_valid(target_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", KR(ret), K(target_status), KPC(this));
  } else if (target_status == origin_status) {
    succ = true;
  } else {
    // There are two concurrent scenarios:
    // 1. Canceled by new tasks (concurrent cancellation) or 
    // 2. Suspended by background threads (concurrent suspension).
    switch (status_) {
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT: {
        if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED == target_status) {
          succ = false;
          ret = OB_INVALID_ARGUMENT;
        } else {
          succ = true;
        }
        break;
      }
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING: {
        if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT == target_status) {
          succ = false;
          ret = OB_INVALID_ARGUMENT;
        } else {
          succ = true;
        }
        break;
      }
      // resume a suspended task will create an identical duplicate task
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED: {
        if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT == target_status
            || ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING == target_status) {
          ret = OB_INVALID_ARGUMENT;
        }
        succ = false;
        break;
      }
      default: {
        succ = false;
        ret = OB_INVALID_ARGUMENT;
        break;
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("invalid status", KR(ret), K(target_status), KPC(this));
    } else if (succ) {
      status_ = target_status;
    }
  }
  return ret;
}

int ObStorageCacheTabletTask::set_status(const ObStorageCacheTaskStatusType &target_status)
{
  bool succ = false;
  ObStorageCacheTaskStatusType origin_status =
      ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX;
  return set_status(target_status, origin_status, succ);
}

ObStorageCacheTaskStatusType ObStorageCacheTabletTask::get_status() const
{
  SpinRLockGuard guard(task_lock_);
  return status_;
}

PolicyStatus ObStorageCacheTabletTask::get_policy_status() const
{
  SpinRLockGuard guard(task_lock_);
  return policy_status_;
}

int ObStorageCacheTabletTask::set_canceled()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else {
    SpinWLockGuard guard(task_lock_);
    switch (status_) {
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING: {
        status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED;
        break;
      }
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED: {
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid status", KR(ret), K_(status));
        break;
      }
    }
  }
  return ret;
}

bool ObStorageCacheTabletTask::is_canceled() const
{
  SpinRLockGuard guard(task_lock_);
  return is_canceled_;
}

bool ObStorageCacheTabletTask::is_completed() const
{
  SpinRLockGuard guard(task_lock_);
  return ObStorageCacheTaskStatus::is_completed_status(status_);
}
void ObStorageCacheTabletTask::set_tablet_id(const int64_t tablet_id)
{
  SpinWLockGuard guard(task_lock_);  
  tablet_id_ = tablet_id;
}

int64_t ObStorageCacheTabletTask::get_tablet_id() const
{
  SpinRLockGuard guard(task_lock_);
  return tablet_id_;
}

int64_t ObStorageCacheTabletTask::get_ls_id() const
{
  SpinRLockGuard guard(task_lock_);
  return ls_id_;
}

uint64_t ObStorageCacheTabletTask::get_tenant_id() const
{
  SpinRLockGuard guard(task_lock_);
  return tenant_id_;
}

int64_t ObStorageCacheTabletTask::get_ref_count() const
{
  SpinRLockGuard guard(task_lock_);
  return ref_count_;
}

void ObStorageCacheTabletTask::inc_ref_count()
{
  SpinWLockGuard guard(task_lock_);
  ref_count_++;
}

void ObStorageCacheTabletTask::dec_ref_count()
{
  int ref_count = 0;
  int ret = OB_SUCCESS;
  {
    SpinWLockGuard guard(task_lock_);
    ref_count_--;
    ref_count = ref_count_;
  }
  if (ref_count == 0) {
    ObStorageCacheTabletTask *tmp_task = this;
    OB_DELETE(ObStorageCacheTabletTask, "SCPTask", tmp_task);
  } else if (OB_UNLIKELY(ref_count < 0)) {
    LOG_ERROR("invalid ref count", KR(ret), K(ref_count));
  }
}

TabletMajorPrewarmStat ObStorageCacheTabletTask::get_prewarm_stat() const
{
  SpinRLockGuard guard(task_lock_);
  return prewarm_stat_;
}

void ObStorageCacheTabletTask::set_prewarm_stat(const TabletMajorPrewarmStat &stat)
{
  SpinWLockGuard guard(task_lock_);
  prewarm_stat_ = stat;
}

double ObStorageCacheTabletTask::get_speed() const
{
  double speed = -1.0;  // B/s
  SpinRLockGuard guard(task_lock_);
  if (IS_INIT) {
    if (start_time_ > 0) {  // <= 0 means it hasn't started yet
      const int64_t used_time_us = 
          (end_time_ > 0 ? end_time_ : ObTimeUtility::fast_current_time()) - start_time_;
      if (used_time_us > 0) {
        speed = static_cast<double>(prewarm_stat_.get_macro_block_bytes())
              / (static_cast<double>(used_time_us) / 1000.0 / 1000.0);
      } else {
        LOG_WARN_RET(OB_SUCCESS, "task stat invalid", KPC(this), K(used_time_us));
      }
    } else {
      speed = 0.0;
    }
  }
  return speed;
}

int64_t ObStorageCacheTabletTask::get_start_time() const
{
  SpinRLockGuard guard(task_lock_);
  return start_time_;
}

int64_t ObStorageCacheTabletTask::get_end_time() const
{
  SpinRLockGuard guard(task_lock_);
  return end_time_;
}

int ObStorageCacheTabletTask::get_result() const
{
  SpinRLockGuard guard(task_lock_);
  return result_;
}

const ObString &ObStorageCacheTabletTask::get_comment() const
{
  SpinRLockGuard guard(task_lock_);
  return comment_;
}

void ObStorageCacheTabletTask::set_start_time(const int64_t start_time)
{
  SpinWLockGuard guard(task_lock_);
  start_time_ = start_time;
}

void ObStorageCacheTabletTask::set_end_time(const int64_t end_time)
{
  SpinWLockGuard guard(task_lock_);
  end_time_ = end_time;
}

}
}
