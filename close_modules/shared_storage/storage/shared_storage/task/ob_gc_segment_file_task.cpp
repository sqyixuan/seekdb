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

#include "storage/shared_storage/task/ob_gc_segment_file_task.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObGCSegmentFileTask::ObGCSegmentFileTask()
  : is_inited_(false), segment_file_mgr_(nullptr)
{
}

int ObGCSegmentFileTask::init(ObSegmentFileManager *segment_file_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObGCSegmentFileTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(segment_file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSegmentFileManager is NULL", KR(ret), KP(segment_file_mgr));
  } else {
    segment_file_mgr_ = segment_file_mgr;
    is_inited_ = true;
    LOG_INFO("succ to init gc segment file task");
  }
  return ret;
}

int ObGCSegmentFileTask::start()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  const int64_t INTERVAL_US = 10L * 1000L * 1000L; // 10s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gc segment file task is not init", KR(ret));
  } else if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, INTERVAL_US, true/*schedule repeatly*/))) {
    LOG_WARN("fail to schedule ObGCSegmentFileTask", KR(ret));
  } else {
    LOG_INFO("succ to start gc segment file task");
  }
  return ret;
}

void ObGCSegmentFileTask::stop()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else {
    TG_CANCEL_TASK(timer->get_tg_id(), *this);
    LOG_INFO("ObGCSegmentFileTask stop finished");
  }
}

void ObGCSegmentFileTask::wait()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else {
    TG_WAIT_TASK(timer->get_tg_id(), *this);
    LOG_INFO("ObGCSegmentFileTask wait finished");
  }
}

void ObGCSegmentFileTask::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  segment_file_mgr_ = nullptr;
}

void ObGCSegmentFileTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(segment_file_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("segment file manager is null", KR(ret), KP(segment_file_mgr_));
  } else if (OB_FAIL(segment_file_mgr_->exec_remove_task_once())) {
    LOG_WARN("fail to exec remove task once", KR(ret));
  }
}

} // storage
} // oceanbase
