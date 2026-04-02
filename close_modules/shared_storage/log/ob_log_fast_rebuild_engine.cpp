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

#include "ob_log_fast_rebuild_engine.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
using namespace common;
namespace logservice
{
ObFastRebuildLogTask::ObFastRebuildLogTask(
    const int64_t palf_id,
    const int64_t palf_epoch) : palf::LogSharedTask(palf_id, palf_epoch)
{
  destroy();
}

ObFastRebuildLogTask::~ObFastRebuildLogTask()
{
  destroy();
}

int ObFastRebuildLogTask::init(const common::ObAddr &server,
                               const palf::PalfBaseInfo &palf_base_info,
                               const palf::LSN &curr_max_lsn,
                               const palf::LSN &curr_end_lsn,
                               ObLogFastRebuildEngine *fast_rebuild_engine)
{
  int ret = OB_SUCCESS;
  if (false == server.is_valid() ||
      false == palf_base_info.is_valid() ||
      false == curr_max_lsn.is_valid() ||
      false == curr_end_lsn.is_valid() ||
      OB_ISNULL(fast_rebuild_engine)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server), K(palf_base_info),
        K(curr_max_lsn), K(curr_end_lsn), KP(fast_rebuild_engine));
  } else if (INVALID_PALF_ID == palf_id_ || -1 == palf_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid variables", K_(palf_id), K_(palf_epoch));
  } else {
    server_ = server;
    palf_base_info_ = palf_base_info;
    curr_max_lsn_ = curr_max_lsn;
    curr_end_lsn_ = curr_end_lsn;
    fast_rebuild_engine_ = fast_rebuild_engine;
    is_inited_ = true;
  }
  return ret;
}

void ObFastRebuildLogTask::destroy()
{
  is_inited_ = false;
  server_.reset();
  palf_base_info_.reset();
  curr_max_lsn_.reset();
  curr_end_lsn_.reset();
  fast_rebuild_engine_ = NULL;
}

bool ObFastRebuildLogTask::is_valid() const
{
  return is_inited_;
}

ObFastRebuildLogTask& ObFastRebuildLogTask::operator=(const ObFastRebuildLogTask &task)
{
  if (&task != this) {
    is_inited_ = task.is_inited_;
    palf_id_ = task.palf_id_;
    palf_epoch_ = task.palf_epoch_;
    server_ = task.server_;
    palf_base_info_ = task.palf_base_info_;
    curr_max_lsn_ = task.curr_max_lsn_;
    curr_end_lsn_ = task.curr_end_lsn_;
  }
  return *this;
}

int ObFastRebuildLogTask::do_task(palf::IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  palf::IPalfHandleImplGuard guard;
  palf::LSN max_lsn, end_lsn;
  bool is_sync_enabled = false, is_disable_sync_succ = false;
  int64_t curr_palf_epoch = -1;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "not inited", K(ret), KPC(this));
  } else if (OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(palf_env_impl));
  } else if (OB_FAIL(palf_env_impl->get_palf_handle_impl(palf_id_, guard))) {
    CLOG_LOG(WARN, "get_palf_handle_impl failed", K(ret), K_(palf_id));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->get_palf_epoch(curr_palf_epoch))) {
    CLOG_LOG(WARN, "palf_epoch has been changed", K(ret), K(this), K(curr_palf_epoch));
  } else if (curr_palf_epoch != palf_epoch_) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "palf_epoch has been changed", K(ret), K(this), K(curr_palf_epoch));
  } else if (FALSE_IT(is_sync_enabled = guard.get_palf_handle_impl()->is_sync_enabled())) {
  } else if (is_sync_enabled && OB_FAIL(guard.get_palf_handle_impl()->disable_sync())) {
    CLOG_LOG(WARN, "disable_sync failed", K(ret), K_(palf_id));
  } else if (FALSE_IT(is_disable_sync_succ = true)) {
  } else if (FALSE_IT(max_lsn = guard.get_palf_handle_impl()->get_max_lsn())) {
  } else if (FALSE_IT(end_lsn = guard.get_palf_handle_impl()->get_end_lsn())) {
  } else if (max_lsn != curr_max_lsn_ || end_lsn != curr_end_lsn_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "LSN don't match", K(ret), KPC(this), K(max_lsn), K(end_lsn));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->advance_base_info(palf_base_info_, true /*is_rebuild*/))) {
    CLOG_LOG(WARN, "advance_base_info failed", K(ret), KPC(this));
  } else {
    CLOG_LOG(INFO, "advance_base_info success", K(ret), KPC(this));
  }
  if (is_sync_enabled && is_disable_sync_succ) {
    (void) guard.get_palf_handle_impl()->enable_sync();
  }
  if (OB_NOT_NULL(fast_rebuild_engine_)) {
    (void) fast_rebuild_engine_->after_do_task(palf_id_);
  }
  return ret;
}

void ObFastRebuildLogTask::free_this(palf::IPalfEnvImpl *palf_env_impl)
{
  palf_env_impl->get_log_allocator()->free_palf_fast_rebuild_log_task(this);
}


ObLogFastRebuildEngine::ObLogFastRebuildEngine()
  : is_inited_(false),
    allocator_(NULL),
    thread_queue_(NULL),
    task_map_()
{}

int ObLogFastRebuildEngine::init(common::ObILogAllocator *alloc_mgr,
                                 palf::LogSharedQueueTh *thread_queue)
{
  int ret = OB_SUCCESS;
  const ObMemAttr bucket_attr(MTL_ID(), "FastRebuildTask");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "ObLogFastRebuildEngine init twice", K(ret));
  } else if (OB_ISNULL(alloc_mgr) || OB_ISNULL(thread_queue)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(alloc_mgr), KP(thread_queue));
  } else if (OB_FAIL(task_map_.create(MAX_TASK_COUNT, bucket_attr))) {
    CLOG_LOG(WARN, "task_map_ create failed", K(ret));
  } else {
    allocator_ = alloc_mgr;
    thread_queue_ = thread_queue;
    is_inited_ = true;
  }
  return ret;
}

void ObLogFastRebuildEngine::destroy()
{
  is_inited_ = false;
  allocator_ = NULL;
  thread_queue_ = NULL;
  task_map_.destroy();
  CLOG_LOG(INFO, "destroy ObLogFastRebuildEngine success");
}

bool ObLogFastRebuildEngine::is_fast_rebuilding(const int64_t palf_id) const
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  ObFastRebuildLogTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    bool_ret = false;
  } else if (OB_HASH_NOT_EXIST == (ret = task_map_.get_refactored(share::ObLSID(palf_id), task))) {
    bool_ret = false;
  }
  return bool_ret;
}

int ObLogFastRebuildEngine::on_fast_rebuild_log(const int64_t palf_id,
                                                const int64_t palf_epoch,
                                                const common::ObAddr &server,
                                                const palf::PalfBaseInfo &palf_base_info,
                                                const palf::LSN &curr_max_lsn,
                                                const palf::LSN &curr_end_lsn)
{
  int ret = OB_SUCCESS;
  ObFastRebuildLogTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogFastRebuildEngine not init", K(ret));
  } else if (OB_HASH_NOT_EXIST != (ret = task_map_.get_refactored(share::ObLSID(palf_id), task))) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "ObFastRebuildLogTask exists", K(ret), KP(task), K(palf_id),
        K(server), K(palf_base_info), K(curr_max_lsn), K(curr_end_lsn));
  } else if (OB_ISNULL(task = alloc_fast_rebuild_log_task_(palf_id, palf_epoch))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc_fast_rebuild_log_task_ failed", K(ret), KP(task));
  } else if (OB_FAIL(task->init(server, palf_base_info, curr_max_lsn, curr_end_lsn, this))) {
    CLOG_LOG(WARN, "ObFastRebuildLogTask init failed", K(ret), KP(task), K(palf_id),
        K(server), K(palf_base_info), K(curr_max_lsn), K(curr_end_lsn));
    free_fast_rebuild_log_task_(task);
  } else if (OB_FAIL(task_map_.set_refactored(share::ObLSID(palf_id), task))) {
    CLOG_LOG(WARN, "insert_into_map_ failed", K(ret), KP(task));
    free_fast_rebuild_log_task_(task);
  } else if (OB_FAIL(thread_queue_->push_task(task))) {
    CLOG_LOG(WARN, "push_task failed", K(ret), KP(task));
    task_map_.erase_refactored(share::ObLSID(palf_id));
    free_fast_rebuild_log_task_(task);
  } else {
    CLOG_LOG(INFO, "on_fast_rebuild_log success", K(ret), KPC(task));
  }
  return ret;
}

void ObLogFastRebuildEngine::after_do_task(const int64_t palf_id)
{
  if (palf::is_valid_palf_id(palf_id)) {
    task_map_.erase_refactored(share::ObLSID(palf_id));
  }
}

ObFastRebuildLogTask *ObLogFastRebuildEngine::alloc_fast_rebuild_log_task_(
    const int64_t palf_id, const int64_t palf_epoch)
{
  return allocator_->alloc_palf_fast_rebuild_log_task(palf_id, palf_epoch);
}

void ObLogFastRebuildEngine::free_fast_rebuild_log_task_(ObFastRebuildLogTask *task)
{
  allocator_->free_palf_fast_rebuild_log_task(task);
}

} // namespace palf
} // namespace oceanbase
