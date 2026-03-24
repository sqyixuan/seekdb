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
#include "storage/tmp_file/ob_ss_tmp_file_shrink_manager.h"
#include "share/ob_thread_define.h"
#include "lib/thread/thread_mgr.h"

namespace oceanbase
{
namespace tmp_file
{

int ObTmpFileShrinkWBPTask::init(ObTmpWriteBufferPool *wbp, ObSSTmpFileFlushManager *flush_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileShrinkWBPTask init twice", KR(ret));
  } else if (OB_ISNULL(wbp) || OB_ISNULL(flush_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wbp or flush mgr is null", KR(ret), KP(wbp), KP(flush_mgr));
  } else {
    wbp_ = wbp;
    flush_mgr_ = flush_mgr;
    is_inited_ = true;
  }

  return ret;
}

void ObTmpFileShrinkWBPTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool is_auto = false;
  ObTimeGuard time_guard("wbp_shrink", 1 * 1000 * 1000);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(wbp_) || OB_ISNULL(flush_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), KP(wbp_), KP(flush_mgr_));
  } else if (wbp_->need_to_shrink(is_auto)) {
    int64_t wash_size = 0;
    int64_t wbp_max_size = wbp_->get_memory_limit();
    ObSSTmpFileAsyncFlushWaitTaskHandle task_handle;
    common::ObIOFlag io_desc;
    io_desc.set_wait_event(ObWaitEventIds::TMP_FILE_WRITE);
    LOG_DEBUG("current wbp shrinking state", K(wbp_->get_shrink_ctx()));
    switch (wbp_->get_wbp_state()) {
      case WBPShrinkContext::INVALID:
        if (OB_FAIL(wbp_->begin_shrinking(is_auto))) {
          LOG_WARN("fail to init shrink context", KR(ret));
        } else {
          wbp_->advance_shrink_state();
        }
        break;
      case WBPShrinkContext::SHRINKING_SWAP:
        if (!wbp_->get_shrink_ctx().is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("shrink context is invalid", KR(ret));
        } else if (OB_FAIL(flush_mgr_->wash(wbp_max_size, io_desc,
                                            task_handle, wash_size))) {
          LOG_WARN("fail to wash all pages", KR(ret), K(wbp_->get_shrink_ctx()));
        } else {
          wbp_->advance_shrink_state();
        }
        break;
      case WBPShrinkContext::SHRINKING_RELEASE_BLOCKS:
        if (OB_FAIL(wbp_->release_blocks_in_shrink_range())) {
          LOG_WARN("fail to shrink wbp", KR(ret));
        } else {
          wbp_->advance_shrink_state();
        }
        break;
      case WBPShrinkContext::SHRINKING_FINISH:
        if (OB_FAIL(wbp_->finish_shrinking())) {
          LOG_ERROR("fail to finish shrinking", KR(ret));
        }
        break;
      default:
        break;
    }
  }
  if (wbp_->get_shrink_ctx().is_valid() && !wbp_->need_to_shrink(is_auto)) {
    wbp_->finish_shrinking();
  }
  time_guard.click("wbp_shrink finish one step");
}

int ObSSTmpFileShrinkManager::init(ObTmpWriteBufferPool *wbp, ObSSTmpFileFlushManager *flush_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(wbp) || OB_ISNULL(flush_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wbp or flush mgr is null", KR(ret), KP(wbp), KP(flush_mgr));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTmpFileShrinkWBP, tg_id_))) {
    LOG_WARN("fail to create shrinking thread", KR(ret));
  } else if (OB_FAIL(shrink_wbp_task_.init(wbp, flush_mgr))) {
    LOG_WARN("fail to init shrink wbp task", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObSSTmpFileShrinkManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTmpFileShrinkManager not init", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start shrink thread", KR(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, shrink_wbp_task_, 10 * 1000 /* 10ms */, true /* repeat */))) {
    LOG_WARN("fail to schedule async remove thread", KR(ret), K(tg_id_));
  }

  return ret;
}

void ObSSTmpFileShrinkManager::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTmpFileShrinkManager not init", KR(ret));
  } else if (OB_INVALID_INDEX != tg_id_) {
    TG_STOP(tg_id_);
  }
}

int ObSSTmpFileShrinkManager::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTmpFileShrinkManager not init", KR(ret));
  } else if (OB_INVALID_INDEX != tg_id_) {
    TG_WAIT(tg_id_);
  }
  return ret;
}

void ObSSTmpFileShrinkManager::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTmpFileShrinkManager not init", KR(ret));
  } else {
    if (OB_INVALID_INDEX != tg_id_) {
      TG_DESTROY(tg_id_);
      tg_id_ = OB_INVALID_INDEX;
    }
    is_inited_ = false;
  }
}

}  // end namespace tmp_file
}  // end namespace oceanbase
