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
#include "storage/tmp_file/ob_ss_tmp_file_flush_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{

/* -------------------------- ObSSTmpFileAsyncFlushWaitTask --------------------------- */

ObSSTmpFileAsyncFlushWaitTask::ObSSTmpFileAsyncFlushWaitTask()
    : fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
      current_length_(0),
      current_begin_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      current_begin_page_virtual_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
      flushed_offset_(0),
      io_tasks_(),
      expected_flushed_page_num_(0),
      succeed_wait_page_nums_(0),
      cond_(),
      ref_cnt_(0),
      wait_has_finished_(false),
      is_inited_(false),
      ret_code_(OB_SUCCESS)
{
}

ObSSTmpFileAsyncFlushWaitTask::~ObSSTmpFileAsyncFlushWaitTask()
{
  release_io_tasks_();
}

int ObSSTmpFileAsyncFlushWaitTask::init(const int64_t fd, const int64_t length, const uint32_t begin_page_id,
                                        const int64_t current_begin_page_virtual_id,
                                        const int64_t expected_flushed_page_num,
                                        const ObMemAttr attr,
                                        const ObTmpFileGlobal::FlushCtxState current_flush_state,
                                        ObSSTmpFileFlushManager *flush_mgr)
{
  int ret = OB_SUCCESS;
  const int64_t vacant_page_num_in_last_flushed_block =
          common::upper_align(current_begin_page_virtual_id, ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS) -
          current_begin_page_virtual_id;
  const int64_t expected_flush_block_num = (expected_flushed_page_num - vacant_page_num_in_last_flushed_block) /
                                            ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS +
                                            vacant_page_num_in_last_flushed_block > 0 ? 1 : 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init tmp file async wait task, init twice", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(fd == ObTmpFileGlobal::INVALID_TMP_FILE_FD || length < 0 ||
                         begin_page_id == ObTmpFileGlobal::INVALID_PAGE_ID ||
                         current_begin_page_virtual_id == ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID ||
                         expected_flushed_page_num < 0 ||
                         current_begin_page_virtual_id + expected_flushed_page_num >
                              (common::upper_align(length, ObTmpFileGlobal::ALLOC_PAGE_SIZE) / ObTmpFileGlobal::ALLOC_PAGE_SIZE) ||
                         OB_ISNULL(flush_mgr) || current_flush_state == ObTmpFileGlobal::FlushCtxState::FSM_FINISHED)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(length), K(begin_page_id),
              K(current_begin_page_virtual_id), K(expected_flushed_page_num),
              K(current_flush_state), KP(flush_mgr));
  } else if (FALSE_IT(io_tasks_.set_attr(attr))) {
  } else if (OB_FAIL(io_tasks_.prepare_allocate_and_keep_count(expected_flush_block_num))) {
    LOG_WARN("fail to prepare allocate io tasks", KR(ret), K(expected_flush_block_num));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::TMP_FILE_FLUSH_COND_WAIT))) {
    LOG_WARN("fail to init conditional variable in tmp file async wait task", KR(ret));
  } else {
    fd_ = fd;
    current_length_ = length;
    current_begin_page_id_ = begin_page_id;
    current_begin_page_virtual_id_ = current_begin_page_virtual_id;
    current_flush_state_ = current_flush_state;
    flush_mgr_ = flush_mgr;
    is_inited_ = true;
  }

  return ret;
}

int ObSSTmpFileAsyncFlushWaitTask::push_back_io_task(const ObSSTmpFileFlushContext &ctx,
                                                     const int64_t flushed_page_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(io_tasks_.count() != io_page_num_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io tasks and io page num count not equal", K(ret), K(io_tasks_.count()), K(io_page_num_.count()));
  } else if (OB_UNLIKELY(flushed_page_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flushed page num is invalid", K(ret), K(flushed_page_num));
  } else if (OB_FAIL(io_tasks_.push_back(std::make_pair(ctx.object_handle_, ctx.flush_buff_)))) {
    LOG_WARN("fail to push back flush handle and flush buffer", K(ret), K(fd_), K(ctx));
  } else if (OB_FAIL(io_page_num_.push_back(flushed_page_num))) {
    LOG_WARN("fail to push back flushing page num", K(ret), K(fd_), K(ctx), K(flushed_page_num));
  } else {
    expected_flushed_page_num_ = ctx.cur_flush_page_count_;
    flushed_offset_ = ctx.write_info_offset_;
  }
  return ret;
}

int ObSSTmpFileAsyncFlushWaitTask::release_io_tasks_()
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = flush_mgr_->get_wait_task_allocator();
  for (int i = 0; OB_SUCC(ret) && i < io_tasks_.count(); ++i) {
    std::pair<blocksstable::ObStorageObjectHandle *, char *> &io_pair = io_tasks_.at(i);
    io_pair.first->~ObStorageObjectHandle();
    allocator.free(io_pair.first);
    allocator.free(io_pair.second);
  }
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  current_length_ = 0;
  current_begin_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
  current_begin_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  io_tasks_.reset();
  expected_flushed_page_num_ = 0;
  succeed_wait_page_nums_ = 0;
  current_flush_state_ = ObTmpFileGlobal::FlushCtxState::FSM_FINISHED;
  ref_cnt_ = 0;
  wait_has_finished_ = false;
  is_inited_ = false;
  ret_code_ = OB_SUCCESS;
  return ret;
}

int ObSSTmpFileAsyncFlushWaitTask::exec_wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to exec wait, tmp file async wait task not inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(io_tasks_.count() != io_page_num_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("io tasks and io page num count is not equal", KR(ret), K(*this));
  } else {
    // Execute `wait` for each async flush handle.
    for (int64_t i = 0; OB_SUCC(ret) && i < io_tasks_.count(); ++i) {
      std::pair<blocksstable::ObStorageObjectHandle *, char *> &p = io_tasks_.at(i);
      blocksstable::ObStorageObjectHandle *obj_handle = p.first;
      ObIOFlag flag;
      if (OB_ISNULL(obj_handle)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, ObStorageObjectHandle is null", KR(ret),
                 K(i), KP(&io_tasks_), KPC(this));
      } else if (OB_FAIL(obj_handle->wait())) {
        LOG_WARN("fail to wait ObStorageObjectHandle", KR(ret), K(i),
                 K(io_tasks_.count()), KP(&io_tasks_), KPC(obj_handle), KPC(this));
      } else if (OB_FAIL(obj_handle->get_io_handle().get_io_flag(flag))) {
        LOG_WARN("fail to get io flag", KR(ret));
      } else if (OB_UNLIKELY(!flag.is_write())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, io flag is not write", KR(ret), K(fd_), KPC(obj_handle), K(flag));
      } else if (!flag.is_sync() && flag.is_sealed() &&
                 OB_FAIL(OB_STORAGE_OBJECT_MGR.seal_object(obj_handle->get_macro_id(), 0/*ls_epoch_id*/))) {
        // the io that flush sealed tmp file from local cache to object storage is sync io,
        // no need to seal in this case
        LOG_WARN("fail to seal object", KR(ret), K(fd_), KPC(obj_handle));
      } else if (OB_UNLIKELY(io_page_num_.at(i) <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected flushed page num", K(ret), K(i), K(*this));
      } else {
        succeed_wait_page_nums_ += io_page_num_.at(i);
      }
    }

    bool flush_breakdown = false;
    const bool has_flushed_unfinished_tail_page = flushed_offset_ == current_length_ &&
                                                  current_length_ % ObTmpFileGlobal::ALLOC_PAGE_SIZE != 0;
    if (has_flushed_unfinished_tail_page) {
      int tmp_ret = OB_SUCCESS;
      blocksstable::ObStorageObjectHandle *obj_handle = io_tasks_.at(io_tasks_.count() - 1).first;
      if (OB_ISNULL(obj_handle)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, ObStorageObjectHandle is null", KR(ret),
                 K(io_tasks_.count()), KP(&io_tasks_), KPC(this));
      } else if (OB_TMP_FAIL(obj_handle->wait())) {
        if (OB_TIMEOUT == tmp_ret) {
          flush_breakdown = true;
          // if the flushing of tail page is failed by timeout, we could not to know when
          // the io request will be completed. thus, there is a risk that the old data
          // cover the new data. so we have to discard the file.
          LOG_WARN("the flush is breakdown, discard current file", KR(tmp_ret), KPC(this));
          if (OB_TMP_FAIL(flush_mgr_->notify_flush_breakdown(fd_))) {
            LOG_ERROR("fail to delete file", KR(tmp_ret), KPC(this));
          }
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
        }
      }
    }

    if (!flush_breakdown) {
      if (succeed_wait_page_nums_ > 0) {
        // we think flush task is successful even thought only one wait task is successful
        ret = OB_SUCCESS;
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(flush_mgr_->update_meta_after_flush(*this))) {
        LOG_ERROR("fail to update meta data", KR(tmp_ret), KPC(this));
      }
      ret = OB_FAIL(ret) ? ret : tmp_ret;
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(cond_broadcast(ret))) {
      LOG_ERROR("fail to cond broadcast", KR(tmp_ret), KR(ret), KPC(this));
    }
    ret = OB_FAIL(ret) ? ret : tmp_ret;
  }
  LOG_DEBUG("async flush wait over", KR(ret), K(io_tasks_.count()), KPC(this));
  return ret;
}

int ObSSTmpFileAsyncFlushWaitTask::cond_wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to cond wait, tmp file async wait task not inited",
             KR(ret), KP(this), K(fd_), K(is_inited_));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_ERROR("fail to cond wait, lock failed", KR(ret), KP(this), K(fd_));
    }
    while (OB_SUCC(ret) && !ATOMIC_LOAD(&wait_has_finished_)) {
      if (OB_FAIL(cond_.wait(timeout_ms))) {
        LOG_WARN("fail to cond wait", KR(ret), KP(this), K(fd_));
      }
    }

    LOG_DEBUG("shared storage temporary file finish cond wait", KR(ret), KR(ret_code_), K(fd_),
              K(ATOMIC_LOAD(&wait_has_finished_)), KPC(this));
  }
  return ret;
}

int ObSSTmpFileAsyncFlushWaitTask::cond_broadcast(int32_t ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to cond broadcast, tmp file async wait task not inited", KR(ret), K(is_inited_));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_ERROR("fail to cond broadcast, lock failed", KR(ret), KP(this));
    } else if (FALSE_IT(ret_code_ = ret_code)) {
    } else if (ATOMIC_SET(&wait_has_finished_, true)) {
    } else if (OB_FAIL(cond_.broadcast())) {
      LOG_WARN("fail to cond broadcast", KR(ret), KP(this));
    } else {
      LOG_DEBUG("cond broadcast succeed", KP(this), K(fd_));
    }
  }
  return ret;
}

/* -------------------------- ObSSTmpFileAsyncFlushWaitTaskHandle --------------------------- */
// TODO: wanyue.wy
// Remove ObSSTmpFileAsyncFlushWaitTaskHandle in future.
// We need to block fg thread which is waiting for flushing over with a condition var
// like sn mode.
// Then, the destroying of wait task only exists in iterating ObTmpFileAsyncWaitTaskQueue over.
// Thus, we need to remove ObSSTmpFileAsyncFlushWaitTaskHandle.
ObSSTmpFileAsyncFlushWaitTaskHandle::ObSSTmpFileAsyncFlushWaitTaskHandle()
    : wait_task_(nullptr)
{
}

ObSSTmpFileAsyncFlushWaitTaskHandle::ObSSTmpFileAsyncFlushWaitTaskHandle(
    ObSSTmpFileAsyncFlushWaitTask *wait_task)
    : wait_task_(nullptr)
{
  set_wait_task(wait_task);
}

ObSSTmpFileAsyncFlushWaitTaskHandle::~ObSSTmpFileAsyncFlushWaitTaskHandle()
{
  reset();
}

void ObSSTmpFileAsyncFlushWaitTaskHandle::set_wait_task(ObSSTmpFileAsyncFlushWaitTask *wait_task)
{
  if (OB_NOT_NULL(wait_task_)) {
    reset();
  }
  if (OB_NOT_NULL(wait_task)) {
    wait_task->inc_ref_cnt_();
  }
  wait_task_ = wait_task;
}

void ObSSTmpFileAsyncFlushWaitTaskHandle::reset()
{
  if (OB_NOT_NULL(wait_task_)) {
    int32_t new_ref_cnt = -1;
    wait_task_->dec_ref_cnt_(&new_ref_cnt);
    if (new_ref_cnt == 0) {
      wait_task_->~ObSSTmpFileAsyncFlushWaitTask();
      MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_wait_task_allocator()->free(wait_task_);
    }
    wait_task_ = nullptr;
  }
}

int ObSSTmpFileAsyncFlushWaitTaskHandle::wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wait_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null wait task", KR(ret), KP(this));
  } else if (OB_FAIL(wait_task_->cond_wait(timeout_ms))) {
    LOG_WARN("fail to cond wait", KR(ret), K(*wait_task_));
  }
  return ret;
}

/* -------------------------- ObTmpFileAsyncWaitTaskQueue --------------------------- */

ObTmpFileAsyncWaitTaskQueue::ObTmpFileAsyncWaitTaskQueue()
    : queue_(), queue_length_(0)
{
}

ObTmpFileAsyncWaitTaskQueue::~ObTmpFileAsyncWaitTaskQueue()
{
}

int ObTmpFileAsyncWaitTaskQueue::push(
    ObSSTmpFileAsyncFlushWaitTaskHandle *task_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_.push(task_handle))) {
    LOG_WARN("fail to push async wait task", KR(ret), K(task_handle));
  } else {
    const int64_t queue_length = ATOMIC_AAF(&queue_length_, 1);
    const int64_t this_task_flush_page_nums = task_handle->wait_task_->expected_flushed_page_num_;
    LOG_DEBUG("wait task enqueue", KPC(task_handle), K(queue_length), K(this_task_flush_page_nums));
  }
  return ret;
}

int ObTmpFileAsyncWaitTaskQueue::pop(
    ObSSTmpFileAsyncFlushWaitTaskHandle *&task_handle)
{
  int ret = OB_SUCCESS;
  ObSpLinkQueue::Link *node = nullptr;
  if (OB_FAIL(queue_.pop(node))) {
    LOG_WARN("fail to pop async wait task", KR(ret),
             K(ATOMIC_LOAD(&queue_length_)));
  } else if (FALSE_IT(task_handle = static_cast<ObSSTmpFileAsyncFlushWaitTaskHandle *>(node))) {
  } else {
    const int64_t queue_length = ATOMIC_SAF(&queue_length_, 1);
    const int64_t this_task_flush_page_nums = task_handle->wait_task_->expected_flushed_page_num_;
    LOG_DEBUG("wait task dequeue", KPC(task_handle), K(queue_length), K(this_task_flush_page_nums));
  }
  return ret;
}

/* -------------------------- ObSSTmpFileWaitTG --------------------------- */
int ObSSTmpFileWaitTimerTask::init(ObSSTmpFileFlushManager *flush_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init, already inited", KR(ret), KPC(this));
  } else if (OB_ISNULL(flush_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(flush_mgr));
  } else {
    flush_mgr_ = flush_mgr;
    is_inited_ = true;
  }
  return ret;
}

void ObSSTmpFileWaitTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to run timer task, not inited", KR(ret), KPC(this));
  } else if (OB_FAIL(flush_mgr_->exec_wait_task_once())) {
    LOG_WARN("fail to exec wait task once", KR(ret));
  }

  if (TC_REACH_TIME_INTERVAL(ObTmpFileGlobal::TMP_FILE_STAT_FREQUENCY)) {
    if (OB_NOT_NULL(flush_mgr_)) {
      flush_mgr_->print_stat_info();
    }
  }
}

/* -------------------------- ObSSTmpFileFlushManager --------------------------- */

int ObSSTmpFileFlushManager::init(const uint64_t tenant_id, const ObSSTenantTmpFileManager *file_mgr,
                                  ObTmpWriteBufferPool *wbp)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_ISNULL(file_mgr) || OB_ISNULL(wbp)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(file_mgr), KP(wbp));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTmpFileAFlush, tg_id_))) {
    LOG_WARN("fail to create async wait thread", KR(ret));
  } else if (OB_FAIL(wait_task_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                               OB_MALLOC_NORMAL_BLOCK_SIZE,
                                               ObMemAttr(tenant_id, "TmpFileMgrWTA", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init wait task allocator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(wait_timer_task_.init(this))) {
    LOG_WARN("fail to init wait timer task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(flush_prio_mgr_.init())) {
    LOG_WARN("fail to init ObTmpFileFlushPriorityManager", KR(ret));
  } else {
    file_mgr_ = file_mgr;
    wbp_ = wbp;
    is_inited_ = true;
  }

  return ret;
}

int ObSSTmpFileFlushManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start async wait thread", KR(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, wait_timer_task_, 10 * 1000 /* 10 ms */, true /* repeat */))) {
    LOG_WARN("fail to schedule async wait thread", KR(ret), K(tg_id_));
  }

  return ret;
}

void ObSSTmpFileFlushManager::stop()
{
  if (OB_INVALID_INDEX != tg_id_) {
    TG_STOP(tg_id_);
  }
}

int ObSSTmpFileFlushManager::wait()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX != tg_id_) {
    TG_WAIT(tg_id_);
  }
  // Make sure all wait task finish.
  while (OB_SUCC(ret) && have_task()) {
    if (OB_FAIL(exec_wait_task_once())) {
      LOG_WARN("fail to exec wait task once", KR(ret));
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_UNLIKELY(have_task())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected task queue length during tmp file manager mtl wait",
              KR(ret), KPC(this));
  }
  return ret;
}

void ObSSTmpFileFlushManager::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    // Make sure all remove task finish.
    if (OB_UNLIKELY(have_task())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected remove task queue length during tmp file manager destroy",
                KR(ret), KPC(this));
    }
    if (OB_INVALID_INDEX != tg_id_) {
      TG_DESTROY(tg_id_);
      tg_id_ = OB_INVALID_INDEX;
    }

    wait_task_allocator_.reset();
    flush_prio_mgr_.destroy();
    total_flushing_page_num_ = 0;
    f1_cnt_ = 0;
    f2_cnt_ = 0;
    f3_cnt_ = 0;
    is_inited_ = false;
  }
}

int ObSSTmpFileFlushManager::exec_wait_task_once()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const int64_t queue_size = wait_task_queue_.get_queue_length();
    ObSSTmpFileAsyncFlushWaitTaskHandle * task_handle = nullptr;
    // Perform the wait task once for all tasks in the queue.
    for (int64_t i = 0; OB_SUCC(ret) && i < queue_size && !wait_task_queue_.is_empty(); ++i) {
      task_handle = nullptr;
      if (OB_FAIL(wait_task_queue_.pop(task_handle))) {
        LOG_WARN("fail to pop wait task queue", KR(ret), K(i), K(queue_size));
      } else if (OB_ISNULL(task_handle->get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null wait task handle", KR(ret), KPC(task_handle));
      } else if (OB_FAIL(task_handle->get()->exec_wait())) {
        LOG_WARN("fail to wait task finish", KR(ret), KPC(task_handle->get()));
        // Swallow error code to continue other wait tasks.
        ret = OB_SUCCESS;
      }
      if (OB_NOT_NULL(task_handle)) {
        if (OB_NOT_NULL(task_handle->get())) {
          const FlushCtxState origin_stage = task_handle->get()->current_flush_state_;
          const int64_t expected_flushed_page_num = task_handle->get()->expected_flushed_page_num_;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(modify_stat_info_(origin_stage, expected_flushed_page_num))) {
            LOG_WARN("fail to modify stat info", KR(tmp_ret), K(origin_stage), K(expected_flushed_page_num), KPC(task_handle->get()));
          }
        }
        // Regardless of whether the IO wait is successful, resources are
        // released. For failed IO, it is treated as if nothing happened.
        LOG_DEBUG("async wait task exec_wait over", KR(ret), KPC(task_handle), K(queue_size), KPC(task_handle->get()));
        task_handle->~ObSSTmpFileAsyncFlushWaitTaskHandle();
        wait_task_allocator_.free(task_handle);
      }
    }
  }

  return ret;
}

int ObSSTmpFileFlushManager::wash(const int64_t expect_wash_size, common::ObIOFlag io_flag,
                                  ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle, int64_t &actual_wash_size)
{
  int ret = OB_SUCCESS;
  static const int64_t io_timeout_ms = ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS;
  actual_wash_size = 0;
  ObTmpFileFlushListIterator iter;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(iter.init(&flush_prio_mgr_))) {
    LOG_WARN("fail to init flush list iterator", KR(ret));
  } else {
    FlushCtxState cur_stage = FlushCtxState::FSM_F1;
    const FlushCtxState end_stage = FlushCtxState::FSM_F3;
    while(OB_SUCC(ret) && cur_stage <= end_stage && actual_wash_size < expect_wash_size) {
      ObSSTmpFileHandle file_handle;
      if (OB_FAIL(iter.next(cur_stage, file_handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next file in flush list", KR(ret), K(cur_stage));
        } else if (OB_FAIL(ObTmpFileGlobal::advance_flush_ctx_state(cur_stage, cur_stage))) {
          LOG_WARN("fail to advance flush ctx state", KR(ret), K(cur_stage));
        }
      } else if (OB_ISNULL(file_handle.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get temporary file", KR(ret), KP(file_handle.get()));
      } else {
        const bool flush_all_pages = FlushCtxState::FSM_F1 != cur_stage;
        ObSharedStorageTmpFile &ss_tmp_file = *file_handle.get();
        int64_t file_flush_size = 0;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ss_tmp_file.flush(flush_all_pages, io_flag, io_timeout_ms, file_flush_size, wait_task_handle))) {
          LOG_WARN("fail to flush temporary file", KR(tmp_ret), K(ss_tmp_file), K(flush_all_pages), K(io_flag), K(io_timeout_ms));
        }
        if (OB_LIKELY(OB_SUCCESS == tmp_ret && file_flush_size > 0)) {
          actual_wash_size += file_flush_size;
        } else if (OB_TMP_FAIL(tmp_ret) || file_flush_size == 0)  {
          if (!ss_tmp_file.is_deleting() && OB_TMP_FAIL(ss_tmp_file.reinsert_data_flush_node())) {
            LOG_ERROR("fail to reinsert data flush node", KR(tmp_ret), K(ss_tmp_file));
          }
        } else{
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected free size", KR(tmp_ret), K(cur_stage), K(file_flush_size), K(ss_tmp_file));
        }
        LOG_DEBUG("wash shared storage temporary file", KR(ret), KR(tmp_ret),
                  K(ss_tmp_file), K(flush_all_pages), K(file_flush_size));
      }
    } // end while
  }
  LOG_DEBUG("ObSSTenantTmpFileManager wash, shared storage mode", KR(ret), K(expect_wash_size), K(actual_wash_size));

  return ret;
}

int ObSSTmpFileFlushManager::update_meta_after_flush(const ObSSTmpFileAsyncFlushWaitTask &wait_task) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t fd = wait_task.fd_;
  ObSSTmpFileHandle tmp_file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(file_mgr_->get_tmp_file(fd, tmp_file_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tmp file has been removed", KR(ret), K(fd));
    } else {
      LOG_WARN("fail to get tmp file handle", KR(ret), K(fd));
    }
  } else if (OB_FAIL(tmp_file_handle.get()->update_meta_after_flush(wait_task))) {
    LOG_ERROR("fail to update meta data", KR(ret), K(fd), KP(&wait_task));
  }

  return ret;
}

int ObSSTmpFileFlushManager::notify_flush_breakdown(const int64_t fd) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSTmpFileHandle tmp_file_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(file_mgr_->get_tmp_file(fd, tmp_file_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tmp file has been removed", KR(ret), K(fd));
    } else {
      LOG_WARN("fail to get tmp file handle", KR(ret), K(fd));
    }
  } else if (OB_FAIL(tmp_file_handle.get()->delete_file())) {
    LOG_WARN("fail to notify flush failed", KR(ret), K(fd), KPC(tmp_file_handle.get()));
  }

  return ret;
}

int ObSSTmpFileFlushManager::wait_task_enqueue(ObSSTmpFileAsyncFlushWaitTask *task)
{
  int ret = OB_SUCCESS;
  char * buff = nullptr;
  ObSSTmpFileAsyncFlushWaitTaskHandle * task_handle = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(task));
  } else if (OB_ISNULL(buff = static_cast<char *>(wait_task_allocator_.alloc(
                              sizeof(ObSSTmpFileAsyncFlushWaitTaskHandle))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc task handle", KR(ret), KPC(task));
  } else if (FALSE_IT(task_handle = new (buff) ObSSTmpFileAsyncFlushWaitTaskHandle(task))) {
  } else if (OB_FAIL(wait_task_queue_.push(task_handle))) {
    LOG_WARN("fail to push wait task", KR(ret), KPC(task));
  } else if (OB_FAIL(record_stat_info_(task->current_flush_state_,
                                       task->expected_flushed_page_num_))) {
    LOG_WARN("fail to record stat info", KR(ret), KPC(task));
  }

  if (OB_FAIL(ret) && task_handle != nullptr) {
    wait_task_allocator_.free(task_handle);
  }

  return ret;
}

int ObSSTmpFileFlushManager::record_stat_info_(const FlushCtxState &stage, const int64_t flushing_page_num)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(stat_lock_);
  if (OB_UNLIKELY(flushing_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid flush size", KR(ret), K(flushing_page_num));
  } else {
    total_flushing_page_num_ += flushing_page_num;
  }

  if (OB_FAIL(ret)) {
  } else if (stage == FlushCtxState::FSM_F1) {
    f1_cnt_ += 1;
  } else if (stage == FlushCtxState::FSM_F2) {
    f2_cnt_ += 1;
  } else if (FlushCtxState::FSM_F3) {
    f3_cnt_ += 1;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected flush state", KR(ret), K(stage));
  }
  return ret;
}

int ObSSTmpFileFlushManager::modify_stat_info_(const FlushCtxState &stage, const int64_t flushed_page_num)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(stat_lock_);
  if (OB_UNLIKELY(flushed_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid flush size", KR(ret), K(flushed_page_num));
  } else {
    total_flushing_page_num_ -= flushed_page_num;
  }

  if (OB_FAIL(ret)) {
  } else if (stage == FlushCtxState::FSM_F1) {
    f1_cnt_ -= 1;
  } else if (stage == FlushCtxState::FSM_F2) {
    f2_cnt_ -= 1;
  } else if (FlushCtxState::FSM_F3) {
    f3_cnt_ -= 1;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected flush state", KR(ret), K(stage));
  }
  return ret;
}

void ObSSTmpFileFlushManager::print_stat_info()
{
  int ret = OB_SUCCESS;
  ObSSTenantTmpFileManager *mutator_file_mgr = const_cast<ObSSTenantTmpFileManager *>(file_mgr_);
  int64_t disk_data_size = 0;
  int64_t occupied_disk_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(mutator_file_mgr->get_tmp_file_disk_usage(disk_data_size, occupied_disk_size))) {
    LOG_WARN("fail to get tmp file disk usage", KR(ret));
  } else {
    ObSpinLockGuard guard(stat_lock_);
    int64_t flush_task_cnt = f1_cnt_ + f2_cnt_ + f3_cnt_;
    int64_t avg_flush_page_num = total_flushing_page_num_ / max(1, flush_task_cnt);
    wbp_->print_statistics();
    STORAGE_LOG(INFO, "tmp file flush statistics information", K(flush_task_cnt), K(total_flushing_page_num_),
                K(avg_flush_page_num), K(f1_cnt_), K(f2_cnt_), K(f3_cnt_), K(wait_task_queue_));
    STORAGE_LOG(INFO, "tmp file disk statistics information", K(disk_data_size), K(occupied_disk_size));
  }
}

} // end namespace tmp_file
} // end namespace oceanbase
