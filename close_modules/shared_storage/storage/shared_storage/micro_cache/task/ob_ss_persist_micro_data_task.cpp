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

#include "ob_ss_persist_micro_data_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_data_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSPersistMicroDataTask-----------------------------------------*/
ObSSPersistMicroDataTask::ObSSPersistMicroDataTask(ObSSMicroCacheTaskRunner &runner)
    : ObSSMicroCacheTaskBase(),
      runner_(runner),
      persist_op_()
{}

void ObSSPersistMicroDataTask::destroy()
{
  persist_op_.destroy();
  ObSSMicroCacheTaskBase::destroy();
}

int ObSSPersistMicroDataTask::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheTaskBase::init(tenant_id, interval_us, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id), K(interval_us));
  } else if (OB_FAIL(persist_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init persist_op", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObSSPersistMicroDataTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(persist_op_.execute_persist_micro())) {
      LOG_WARN("fail to execute persist_micro", KR(ret));
    }
    const int64_t alloc_blk_fail_cnt = persist_op_.get_alloc_blk_fail_cnt();
    adjust_persist_task_interval(alloc_blk_fail_cnt);
  }
  
  if (OB_TMP_FAIL(runner_.schedule_persist_data_task(adjusted_interval_us_))) {
    LOG_WARN("fail to reschedule persist micro task", KR(ret), K(tmp_ret), K_(adjusted_interval_us));
  }
}

void ObSSPersistMicroDataTask::adjust_persist_task_interval(const int64_t alloc_phy_blk_fail_cnt)
{
  adjusted_interval_us_ = interval_us_;
  if (nullptr != task_ctx_) {
    if ((alloc_phy_blk_fail_cnt < 1) && (task_ctx_->mem_data_mgr_->get_sealed_mem_block_cnt() > 0)) {
      adjusted_interval_us_ = MIN(FAST_SCHEDULE_PERSIST_INTERVAL_US, adjusted_interval_us_);
    }
  }
}

} // storage
} // oceanbase
