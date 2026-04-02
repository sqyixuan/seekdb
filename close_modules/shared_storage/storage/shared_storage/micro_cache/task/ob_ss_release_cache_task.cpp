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

#include "ob_ss_release_cache_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSReleaseCacheTask------------------------------------------*/
ObSSReleaseCacheTask::ObSSReleaseCacheTask(ObSSMicroCacheTaskRunner &runner)
  : ObSSMicroCacheTaskBase(), runner_(runner), update_arc_ctx_(), evict_op_(), reorganize_op_()
{}

void ObSSReleaseCacheTask::destroy()
{
  update_arc_ctx_.reset();
  evict_op_.destroy();
  reorganize_op_.destroy();
  ObSSMicroCacheTaskBase::destroy();
}

int ObSSReleaseCacheTask::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheTaskBase::init(tenant_id, interval_us, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id), K(interval_us));
  } else if (OB_FAIL(evict_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init evict_op", KR(ret), K(tenant_id));
  } else if (OB_FAIL(reorganize_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init reorganize_op", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSReleaseCacheTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(evict_op_.execute_eviction())) {
      LOG_WARN("fail to execute eviction", KR(ret));
    } else if (OB_FAIL(reorganize_op_.execute_reorganization())) {
      LOG_WARN("fail to execute reorganization", KR(ret));
    }
    update_arc_limit_for_prewarm();

    clear_for_next_round();
  }

  adjust_arc_task_interval();
  if (OB_TMP_FAIL(runner_.schedule_arc_cache_task(adjusted_interval_us_))) {
    LOG_WARN("fail to reschedule arc cache task", KR(ret), K(tmp_ret), K_(adjusted_interval_us));
  }
}

void ObSSReleaseCacheTask::clear_for_next_round()
{
  evict_op_.clear_for_next_round();
  reorganize_op_.clear_for_next_round();
}

void ObSSReleaseCacheTask::adjust_arc_task_interval()
{
  adjusted_interval_us_ = interval_us_;
  if (IS_INIT) {
    const bool need_speed_up = task_ctx_->micro_meta_mgr_->get_arc_info().trigger_eviction();
    if (need_speed_up) {
      adjusted_interval_us_ = FAST_SCHEDULE_ARC_INTERVAL_US;
    }
  }
}

void ObSSReleaseCacheTask::update_arc_limit_for_prewarm(const bool need_trigger)
{
  int ret = OB_SUCCESS;
  if (update_arc_ctx_.try_to_execute()) {
    // Default start to execute this operation at 01:00am, and finish this operation at 01:45~1:59am
    const int start_hour = 1;
    const int start_minute = 0;
    const int64_t finish_hour = 1;
    const int64_t finish_minute = 45;

    time_t cur_time = -1;
    time(&cur_time);
    struct tm human_time;
    struct tm *human_time_ptr = nullptr;
    if (nullptr == (human_time_ptr = (localtime_r(&cur_time, &human_time)))) {
      ret = OB_ERR_SYS;
      LOG_WARN("fail to get localtime", KR(ret), K(errno));
    } else {
      bool need_start = (((human_time_ptr->tm_hour == start_hour) && (human_time_ptr->tm_min == start_minute)) ||
                         need_trigger);
      if (need_start && (!update_arc_ctx_.is_started())) {
        if (OB_FAIL(task_ctx_->micro_meta_mgr_->update_arc_work_limit_for_prewarm(true/*start_update*/))) {
          LOG_WARN("fail to update arc_work_limit for prewarm", KR(ret));
        } else {
          update_arc_ctx_.set_started();
        }
      }

      bool need_finish = ((human_time_ptr->tm_hour == finish_hour) && (human_time_ptr->tm_min >= finish_minute));
      if (need_finish && update_arc_ctx_.is_started()) {
        if (OB_FAIL(task_ctx_->micro_meta_mgr_->update_arc_work_limit_for_prewarm(false/*start_update*/))) {
          LOG_WARN("fail to update arc_work_limit for prewarm", KR(ret));
        } else {
          update_arc_ctx_.set_finished();
        }
      }
    }
    update_arc_ctx_.reset_round();
  }
}

} // storage
} // oceanbase
