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

#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/ob_malloc.h"
#include "share/change_stream/ob_change_stream_worker.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{

// ---------------------------------------------------------------------------
// ObCSExecutor
// ---------------------------------------------------------------------------

ObCSExecutor::ObCSExecutor()
  : common::ObLinkQueueThreadPool(),
    is_inited_(false),
    executor_id_(-1)
{
}

ObCSExecutor::~ObCSExecutor()
{
  destroy();
}

int ObCSExecutor::init(int64_t executor_id, int64_t thread_num, int64_t task_queue_limit,
                       const char *name, uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("ObCSExecutor already inited", K(ret), K(executor_id));
  } else if (executor_id < 0 || thread_num <= 0 || task_queue_limit <= 0 || OB_ISNULL(name)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("ObCSExecutor invalid argument", K(ret), K(executor_id), K(thread_num), K(task_queue_limit), KP(name));
  } else if (OB_FAIL(ObLinkQueueThreadPool::init(thread_num, task_queue_limit, name, tenant_id))) {
    LOG_WARN("ObCSExecutor base init failed", K(ret), K(executor_id));
  } else if (OB_FAIL(set_adaptive_thread(0, thread_num))) {
    LOG_WARN("ObCSExecutor set_adaptive_thread failed", K(ret), K(executor_id));
  } else {
    executor_id_ = executor_id;
    is_inited_ = true;
    LOG_INFO("ObCSExecutor inited (LinkQueue, adaptive 1~N)", K(executor_id), K(thread_num), K(task_queue_limit));
  }
  if (OB_FAIL(ret) && is_inited_) {
    ObCSExecutor::destroy();
  }
  return ret;
}

int ObCSExecutor::start()
{
  // Thread pool is already started in base init() (ObLinkQueueThreadPool::init calls ThreadPool::start).
  // No-op here for API compatibility with mgr init/start sequence.
  return common::OB_SUCCESS;
}

void ObCSExecutor::stop()
{
  if (is_inited_) {
    ObLinkQueueThreadPool::stop();
  }
}

void ObCSExecutor::wait()
{
  if (is_inited_) {
    ObLinkQueueThreadPool::wait();
  }
}

void ObCSExecutor::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    ObLinkQueueThreadPool::destroy();
    is_inited_ = false;
    executor_id_ = -1;
  }
}

int ObCSExecutor::push_subtask(ObCSExecSubTask *sub_task)
{
  int ret = common::OB_SUCCESS;
  if (!is_inited_ || OB_ISNULL(sub_task)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("ObCSExecutor push_subtask invalid", K(ret), K(executor_id_));
  } else {
    ObCSSubTaskLinkNode *node = nullptr;
    if (OB_ISNULL(node = OB_NEW(ObCSSubTaskLinkNode, common::ObMemAttr(MTL_ID(), "CSSubTaskLink")))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ObCSExecutor alloc link node failed", K(ret), K(executor_id_));
      return ret;
    }
    node->sub_task_ = sub_task;
    if (OB_FAIL(push(node))) {
      OB_DELETE(ObCSSubTaskLinkNode, "CSSubTaskLink", node);
      LOG_WARN("ObCSExecutor push failed", K(ret), K(executor_id_));
    }
  }
  return ret;
}

void ObCSExecutor::handle(common::LinkTask *task)
{
  if (OB_ISNULL(task)) {
    return;
  }
  ObCSSubTaskLinkNode *node = static_cast<ObCSSubTaskLinkNode *>(task);
  ObCSExecSubTask *sub_task = node->sub_task_;
  if (OB_NOT_NULL(sub_task)) {
    (void)process_sub_task(sub_task);
    OB_DELETE(ObCSExecSubTask, "CSExecSubTask", sub_task);
  }
  OB_DELETE(ObCSSubTaskLinkNode, "CSSubTaskLink", node);
}

void ObCSExecutor::handle_drop(common::LinkTask *task)
{
  if (OB_ISNULL(task)) {
    return;
  }
  ObCSSubTaskLinkNode *node = static_cast<ObCSSubTaskLinkNode *>(task);
  ObCSExecSubTask *sub_task = node->sub_task_;
  if (OB_NOT_NULL(sub_task)) {
    OB_DELETE(ObCSExecSubTask, "CSExecSubTask", sub_task);
  }
  OB_DELETE(ObCSSubTaskLinkNode, "CSSubTaskLink", node);
}

int ObCSExecutor::process_sub_task(ObCSExecSubTask *sub_task)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(sub_task)) {
    return common::OB_INVALID_ARGUMENT;
  }
  (void)sub_task->get_slice_id();
  (void)sub_task->get_exec_ctx();
  (void)sub_task->get_rows();
  // TODO: plugin->map() per row; when batch done plugin->reduce(), advance refresh scn
  return ret;
}

// ---------------------------------------------------------------------------
// ObCSWorker
// ---------------------------------------------------------------------------

ObCSWorker::ObCSWorker()
  : is_inited_(false),
    executors_(nullptr),
    executor_count_(0)
{
}

ObCSWorker::~ObCSWorker()
{
  destroy();
}

int ObCSWorker::init()
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
    LOG_WARN("ObCSWorker already inited", K(ret));
  } else {
    executor_count_ = 4; // TODO from config
    const uint64_t tenant_id = MTL_ID();
    const common::ObMemAttr attr(tenant_id, "CSExecutors");
    void *buf = ob_malloc(executor_count_ * sizeof(ObCSExecutor *), attr);
    if (OB_ISNULL(buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ObCSWorker alloc executors array failed", K(ret), K(executor_count_));
    } else {
      executors_ = static_cast<ObCSExecutor **>(buf);
      for (int64_t i = 0; i < executor_count_; ++i) {
        executors_[i] = nullptr;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < executor_count_; ++i) {
        if (OB_ISNULL(executors_[i] = OB_NEW(ObCSExecutor, attr))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("ObCSWorker alloc executor failed", K(ret), K(i));
          break;
        }
        char name[64];
        (void)snprintf(name, sizeof(name), "CSWorker%ld", i);
        if (OB_FAIL(executors_[i]->init(i, 1, CS_EXECUTOR_QUEUE_LIMIT, name, tenant_id))) {
          LOG_WARN("ObCSWorker: executor init failed", K(ret), K(i));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
        LOG_INFO("ObCSWorker init success", K(executor_count_));
      } else {
        for (int64_t j = 0; j < executor_count_; ++j) {
          if (OB_NOT_NULL(executors_[j])) {
            executors_[j]->destroy();
            OB_DELETE(ObCSExecutor, "CSExecutor", executors_[j]);
            executors_[j] = nullptr;
          }
        }
        ob_free(executors_);
        executors_ = nullptr;
        executor_count_ = 0;
      }
    }
  }
  return ret;
}

int ObCSWorker::start()
{
  int ret = common::OB_SUCCESS;
  if (!is_inited_) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObCSWorker not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < executor_count_; ++i) {
      if (OB_NOT_NULL(executors_[i]) && OB_FAIL(executors_[i]->start())) {
        LOG_WARN("ObCSWorker: executor start failed", K(ret), K(i));
        break;
      }
    }
  }
  return ret;
}

void ObCSWorker::stop()
{
  if (is_inited_ && OB_NOT_NULL(executors_)) {
    for (int64_t i = 0; i < executor_count_; ++i) {
      if (OB_NOT_NULL(executors_[i])) {
        executors_[i]->stop();
      }
    }
  }
}

void ObCSWorker::wait()
{
  if (is_inited_ && OB_NOT_NULL(executors_)) {
    for (int64_t i = 0; i < executor_count_; ++i) {
      if (OB_NOT_NULL(executors_[i])) {
        executors_[i]->wait();
      }
    }
  }
}

void ObCSWorker::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    if (OB_NOT_NULL(executors_)) {
      for (int64_t i = 0; i < executor_count_; ++i) {
        if (OB_NOT_NULL(executors_[i])) {
          executors_[i]->destroy();
          OB_DELETE(ObCSExecutor, "CSExecutor", executors_[i]);
          executors_[i] = nullptr;
        }
      }
      ob_free(executors_);
      executors_ = nullptr;
    }
    executor_count_ = 0;
    is_inited_ = false;
  }
}

int ObCSWorker::push_subtask(int64_t slice_id, ObCSExecSubTask *sub_task)
{
  int ret = common::OB_SUCCESS;
  if (!is_inited_ || executor_count_ <= 0 || OB_ISNULL(executors_) || OB_ISNULL(sub_task)
      || slice_id < 0 || slice_id >= executor_count_) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("ObCSWorker push_subtask invalid", K(ret), K(executor_count_), K(slice_id));
    return ret;
  }
  if (OB_ISNULL(executors_[slice_id])) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("ObCSWorker executor is null", K(ret), K(slice_id));
  } else {
    ret = executors_[slice_id]->push_subtask(sub_task);
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
