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
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_remove_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{

/* -------------------------- ObTmpFileAsyncRemoveTask --------------------------- */

ObTmpFileAsyncRemoveTask::ObTmpFileAsyncRemoveTask(
    const blocksstable::MacroBlockId &tmp_file_id, const int64_t length)
    : tmp_file_id_(tmp_file_id), length_(length)
{
}

int ObTmpFileAsyncRemoveTask::exec_remove() const
{
  int ret = OB_SUCCESS;
  const int64_t fd = tmp_file_id_.second_id();
  LOG_INFO("start to remove tmp file in bg thread", K(fd), K(tmp_file_id_), K(length_));
  ObTimeGuard time_guard("ss_tmp_file_remove", 4 * 1000 * 1000 /* 4s */);

  if (length_ == INT64_MAX) {
    // Reboot GC or Scan GC, we don't know temporary file length.
    if (OB_FAIL(MTL(ObTenantFileManager *)->delete_tmp_file(tmp_file_id_))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to delete previous temporary file before this reboot", KR(ret), K(fd), K(tmp_file_id_));
      }
    } else if (OB_FAIL(MTL(ObTenantFileManager *)->get_segment_file_mgr().delete_wild_meta(fd))) {
      LOG_WARN("fail to delete wild tmp file meta", KR(ret), K_(tmp_file_id));
    }
  } else {
    // Normal delete, we know temporary file length.
    if (OB_FAIL(MTL(ObTenantFileManager *)->delete_tmp_file(tmp_file_id_, length_))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to delete temporary file", KR(ret), K(fd), K(tmp_file_id_), K(length_));
      }
    }
  }

  time_guard.click("remove_ss_tmp_file");
  LOG_INFO("remove tmp file in bg thread over", KR(ret), K(fd), K(tmp_file_id_), K(length_));

  return ret;
}

int64_t ObTmpFileAsyncRemoveTask::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t fd = tmp_file_id_.second_id();
  J_OBJ_START();
  J_KV(K(fd), K(length_), K(tmp_file_id_));
  J_OBJ_END();
  return pos;
}

/* -------------------------- ObTmpFileAsyncRemoveTaskQueue --------------------------- */

ObTmpFileAsyncRemoveTaskQueue::
    ObTmpFileAsyncRemoveTaskQueue()
    : queue_(), queue_length_(0)
{
}

ObTmpFileAsyncRemoveTaskQueue::~ObTmpFileAsyncRemoveTaskQueue()
{
}

int ObTmpFileAsyncRemoveTaskQueue::push(ObTmpFileAsyncRemoveTask *remove_task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(remove_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(remove_task));
  } else if (OB_FAIL(queue_.push(remove_task))) {
    LOG_WARN("fail to push remove task", KR(ret), KPC(remove_task));
  } else {
    const int64_t queue_length = ATOMIC_AAF(&queue_length_, 1);
    LOG_INFO("remove task enqueue", KPC(remove_task), K(queue_length));
  }

  return ret;
}

int ObTmpFileAsyncRemoveTaskQueue::pop(ObTmpFileAsyncRemoveTask *& remove_task)
{
  int ret = OB_SUCCESS;

  ObSpLinkQueue::Link *node = nullptr;
  remove_task = nullptr;
  if (OB_FAIL(queue_.pop(node))) {
    LOG_WARN("fail to pop remove task", KR(ret));
  } else if (FALSE_IT(remove_task = static_cast<ObTmpFileAsyncRemoveTask *>(node))) {
  } else {
    const int64_t queue_length = ATOMIC_SAF(&queue_length_, 1);
    LOG_INFO("remove task dequeue", KPC(remove_task), K(queue_length));
  }

  return ret;
}

int ObTmpFileAsyncRemoveTaskQueue::top(ObTmpFileAsyncRemoveTask *& remove_task)
{
  int ret = OB_SUCCESS;

  ObSpLinkQueue::Link *node = nullptr;
  remove_task = nullptr;
  if (OB_FAIL(queue_.top(node))) {
    LOG_WARN("fail to get top remove task", KR(ret));
  } else if (FALSE_IT(remove_task = static_cast<ObTmpFileAsyncRemoveTask *>(node))) {
  }

  return ret;
}

/* -------------------------- ObSSTmpFileRemoveTimerTask --------------------------- */
int ObSSTmpFileRemoveTimerTask::init(ObSSTmpFileRemoveManager *remove_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(remove_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(remove_mgr));
  } else {
    remove_mgr_ = remove_mgr;
    scan_invalid_files_ts_ = ObTimeUtility::current_time();
    is_inited_ = true;
  }
  return ret;
}

void ObSSTmpFileRemoveTimerTask::destroy()
{
  remove_mgr_ = nullptr;
  scan_invalid_files_ts_ = -1;
  is_inited_ = false;
}

void ObSSTmpFileRemoveTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (now - scan_invalid_files_ts_ > SCAN_INVALID_FILES_INTERVAL) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(remove_mgr_->scan_invalid_files())) {
        LOG_ERROR("fail to scan invalid files", KR(tmp_ret), K(scan_invalid_files_ts_), K(now));
      }
      scan_invalid_files_ts_ = now;
    }
    if (OB_FAIL(remove_mgr_->exec_remove_task_once())) {
      LOG_WARN("fail to exec remove task once", KR(ret));
    }
  }

  if (TC_REACH_TIME_INTERVAL(ObTmpFileGlobal::TMP_FILE_STAT_FREQUENCY)) {
    STORAGE_LOG(INFO, "tmp file remove statistics information", KPC(remove_mgr_));
  }
}

/* -------------------------- ObSSTmpFileRemoveManager --------------------------- */
int ObSSTmpFileRemoveManager::init(const uint64_t tenant_id, const TmpFileMap *files)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_ISNULL(files))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(files));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTmpFileARemove, tg_id_))) {
    LOG_WARN("fail to create async remove thread", KR(ret));
  } else if (OB_FAIL(remove_task_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                 OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                 ObMemAttr(tenant_id, "TmpFileMgrRTA", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init remove task allocator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(remove_timer_task_.init(this))) {
    LOG_WARN("fail to init remove timer task", KR(ret));
  } else {
    files_ = files;
    is_inited_ = true;
  }

  return ret;
}

int ObSSTmpFileRemoveManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start async remove thread", KR(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, remove_timer_task_, 5 * 1000 * 1000 /* 5 s */, true /* repeat */))) {
    LOG_WARN("fail to schedule async remove thread", KR(ret), K(tg_id_));
  } else if (OB_FAIL(get_first_tmp_file_id_())) {
    LOG_WARN("fail to get first tmp file id", KR(ret), KPC(this));
  }
  return ret;
}

void ObSSTmpFileRemoveManager::stop()
{
  if (OB_INVALID_INDEX != tg_id_) {
    TG_STOP(tg_id_);
  }
}

int ObSSTmpFileRemoveManager::wait()
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_INDEX != tg_id_) {
    TG_WAIT(tg_id_);
  }
  // Make sure all remove task finish.
  while (OB_SUCC(ret) && have_task()) {
    if (OB_FAIL(exec_remove_task_once())) {
      LOG_WARN("fail to exec remove task once", KR(ret));
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

int ObSSTmpFileRemoveManager::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    // Make sure all remove task finish.
    // if (OB_UNLIKELY(have_task())) {
    //   ret = OB_ERR_UNEXPECTED;
    //   LOG_ERROR("unexpected remove task queue length during tmp file manager destroy",
    //             KR(ret), KPC(this));
    // }
    while (OB_SUCC(ret) && have_task()) {
      // some modules remove tmp files when they are destroying.
      // thus, it is possible that some removing tasks exist in queue when destroying.
      if (OB_FAIL(exec_remove_task_once())) {
        LOG_WARN("fail to exec remove task once", KR(ret));
        if (OB_TIMEOUT == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_INVALID_INDEX != tg_id_) {
      TG_DESTROY(tg_id_);
      tg_id_ = OB_INVALID_INDEX;
    }
    remove_task_allocator_.reset();
    first_tmp_file_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    is_inited_ = false;
  }
  return ret;
}

int ObSSTmpFileRemoveManager::remove_task_enqueue(const blocksstable::MacroBlockId &tmp_file_id, const int64_t length)
{
  int ret = OB_SUCCESS;

  char * buf = nullptr;
  ObTmpFileAsyncRemoveTask * remove_task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tmp_file_id.is_valid() || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tmp_file_id), K(length));
  } else {
    do {
      ret = OB_SUCCESS;
      if (OB_ISNULL(buf = static_cast<char *>(remove_task_allocator_.alloc(sizeof(ObTmpFileAsyncRemoveTask))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate ObTmpFileAsyncRemoveTask", KR(ret), K(tmp_file_id));
      } else {
        remove_task = new (buf) ObTmpFileAsyncRemoveTask(tmp_file_id, length);
      }
    } while (ret == OB_ALLOCATE_MEMORY_FAILED);
  }

  if (FAILEDx(remove_task_queue_.push(remove_task))) {
    LOG_ERROR("fail to push async remove task", KR(ret), K(tmp_file_id), K(length));
  }

  if (OB_FAIL(ret) && remove_task != nullptr) {
    remove_task_allocator_.free(remove_task);
  }

  return ret;
}

int ObSSTmpFileRemoveManager::exec_remove_task_once()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    // Execute reboot gc.
    if (ObTmpFileGlobal::INVALID_TMP_FILE_FD != first_tmp_file_id_) {
      if (OB_FAIL(exec_reboot_gc_once_())) {
        LOG_WARN("fail to exec reboot gc once", KR(ret), K(first_tmp_file_id_));
      } else {
        LOG_INFO("succeed to exec reboot gc", K(first_tmp_file_id_));
        first_tmp_file_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
      }
    }

    // Continue async remove.
    ret = OB_SUCCESS;
    const int64_t queue_size = remove_task_queue_.get_queue_length();
    ObTmpFileAsyncRemoveTask * remove_task = nullptr;
    // Perform the remove task once for all tasks in the queue.
    for (int64_t i = 0; OB_SUCC(ret) && i < queue_size && !remove_task_queue_.is_empty(); ++i) {
      remove_task = nullptr;
      if (OB_FAIL(remove_task_queue_.pop(remove_task))) {
        LOG_WARN("fail to top remove task queue", KR(ret), K(i), K(queue_size));
      } else if (OB_FAIL(remove_task->exec_remove())) {
        LOG_WARN("fail to exec remove task", KR(ret), KPC(remove_task));
        // Swallow error code to continue other remove tasks.
        ret = OB_SUCCESS;
        // Push back failed remove task to queue again, expect next batch succeed.
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(remove_task_queue_.push(remove_task))) {
          LOG_ERROR("fail to push failed remove task, remove task lease", K(tmp_ret), KPC(remove_task));
        }
      } else {
        LOG_INFO("async remove task succeed exec_remove", K(i), K(queue_size), KPC(remove_task), K(first_tmp_file_id_));
        remove_task->~ObTmpFileAsyncRemoveTask();
        remove_task_allocator_.free(remove_task);
      }
    }
  }

  return ret;
}

int ObSSTmpFileRemoveManager::exec_reboot_gc_once_()
{
  int ret = OB_SUCCESS;

  ObArray<blocksstable::MacroBlockId> list_tmp_file_res;
  if (OB_FAIL(MTL(ObTenantFileManager *)->list_tmp_file(list_tmp_file_res))) {
    LOG_WARN("fail to list tmp file", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < list_tmp_file_res.count(); ++i) {
      const blocksstable::MacroBlockId &curr_tmp_file_id = list_tmp_file_res.at(i);
      if (curr_tmp_file_id.second_id() < first_tmp_file_id_) {
        if (OB_FAIL(remove_task_enqueue(curr_tmp_file_id, INT64_MAX))) {
          LOG_ERROR("fail to remove task enqueue in reboot gc",
                    KR(ret), K(first_tmp_file_id_), K(curr_tmp_file_id));
          ret = OB_SUCCESS;
        } else {
          LOG_INFO("previous temporary file enqueue", K(curr_tmp_file_id));
        }
      } else if (curr_tmp_file_id.second_id() == first_tmp_file_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected tmp file id", KR(ret), K(first_tmp_file_id_), K(curr_tmp_file_id));
      } else {
        // new temporary file after this reboot, do nothing.
      }
    }
  }
  FLOG_INFO("finish reboot gc", KR(ret), K(first_tmp_file_id_));

  return ret;
}

int ObSSTmpFileRemoveManager::get_first_tmp_file_id_()
{
  int ret = OB_SUCCESS;

  blocksstable::MacroBlockId max_tmp_file_id;
  max_tmp_file_id.set_id_mode(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
  max_tmp_file_id.set_storage_object_type(static_cast<uint64_t>(blocksstable::ObStorageObjectType::TMP_FILE));
  blocksstable::ObStorageObjectOpt opt;
  opt.set_ss_tmp_file_object_opt();
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, max_tmp_file_id))) {
    LOG_WARN("fail to get current max tmp file id", KR(ret), K(opt));
  } else {
    first_tmp_file_id_ = max_tmp_file_id.second_id();
  }

  return ret;
}

int ObSSTmpFileRemoveManager::scan_invalid_files()
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::MacroBlockId> list_tmp_file_res;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(MTL(ObTenantFileManager *)->list_tmp_file(list_tmp_file_res))) {
    LOG_WARN("fail to list tmp file", KR(ret));
  } else {
    for (int64_t i = 0; i < list_tmp_file_res.count() && OB_SUCC(ret); ++i) {
      const blocksstable::MacroBlockId &file_id = list_tmp_file_res.at(i);
      ObSSTmpFileHandle tmp_file_handle;
      if (OB_FAIL(files_->get(ObTmpFileKey(file_id.second_id()), tmp_file_handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get tmp file handle", KR(ret), K(file_id));
        } else if (OB_FAIL(remove_task_enqueue(file_id, INT64_MAX))) { // find a invalid remain file
          LOG_ERROR("fail to add invalid file into remove task queue", KR(ret), K(file_id));
        } else {
          LOG_INFO("invalid temporary file enqueue", K(file_id));
        }
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
