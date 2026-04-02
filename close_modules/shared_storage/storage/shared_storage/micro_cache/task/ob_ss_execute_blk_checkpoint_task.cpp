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

#include "storage/shared_storage/micro_cache/task/ob_ss_execute_blk_checkpoint_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSExecuteBlkCheckpointTask------------------------------------------*/
ObSSExecuteBlkCheckpointTask::ObSSExecuteBlkCheckpointTask(ObSSMicroCacheTaskRunner &runner)
  : ObSSMicroCacheTaskBase(), runner_(runner), ckpt_op_()
{}

void ObSSExecuteBlkCheckpointTask::destroy()
{
  ckpt_op_.destroy();
  ObSSMicroCacheTaskBase::destroy();
}

int ObSSExecuteBlkCheckpointTask::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheTaskBase::init(tenant_id, interval_us, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id), K(interval_us));
  } else if (OB_FAIL(ckpt_op_.init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init ckpt_op", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSExecuteBlkCheckpointTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(ckpt_op_.execute_checkpoint())) {
      LOG_WARN("fail to execute checkpoint", KR(ret));
    }
  }

  if (OB_TMP_FAIL(runner_.schedule_do_blk_checkpoint_task(interval_us_))) {
    LOG_WARN("fail to reschedule do_blk_checkpoint task", KR(ret), K(tmp_ret), K_(interval_us));
  }
}

} // storage
} // oceanbase
