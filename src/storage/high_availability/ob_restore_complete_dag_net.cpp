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

#include "ob_restore_complete_dag_net.h"

namespace oceanbase
{
namespace restore
{

// ObCompleteRestoreCtx
ObCompleteRestoreCtx::ObCompleteRestoreCtx()
  : tenant_id_(0),
    task_(),
    start_ts_(0),
    finish_ts_(0)
{
}

ObCompleteRestoreCtx::~ObCompleteRestoreCtx()
{
}

void ObCompleteRestoreCtx::reset()
{
  // do nothing
}

void ObCompleteRestoreCtx::reuse()
{
  // do nothing
}

int ObCompleteRestoreCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

ObIHADagNetCtx::DagNetCtxType ObCompleteRestoreCtx::get_dag_net_ctx_type()
{
  return ObIHADagNetCtx::DagNetCtxType::MAX;
}

bool ObCompleteRestoreCtx::is_valid() const
{
  return false;
}

// ObCompleteRestoreParam
ObCompleteRestoreParam::ObCompleteRestoreParam()
  : task_(),
    result_(0)
{
}

bool ObCompleteRestoreParam::is_valid() const
{
  return false;
}

void ObCompleteRestoreParam::reset()
{
  // do nothing
}

// ObCompleteRestoreDagNet
ObCompleteRestoreDagNet::ObCompleteRestoreDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_RESTORE)
{
}

ObCompleteRestoreDagNet::~ObCompleteRestoreDagNet()
{
}

int ObCompleteRestoreDagNet::init_by_param(const share::ObIDagInitParam *param)
{
  return OB_NOT_SUPPORTED;
}

bool ObCompleteRestoreDagNet::is_valid() const
{
  return false;
}

int ObCompleteRestoreDagNet::start_running()
{
  return OB_NOT_SUPPORTED;
}

bool ObCompleteRestoreDagNet::operator==(const share::ObIDagNet &other) const
{
  return false;
}

uint64_t ObCompleteRestoreDagNet::hash() const
{
  return 0;
}

int ObCompleteRestoreDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObCompleteRestoreDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObCompleteRestoreDagNet::clear_dag_net_ctx()
{
  return OB_NOT_SUPPORTED;
}

int ObCompleteRestoreDagNet::deal_with_cancel()
{
  return OB_NOT_SUPPORTED;
}

// ObCompleteRestoreDag
ObCompleteRestoreDag::ObCompleteRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObRestoreDag(dag_type)
{
}

ObCompleteRestoreDag::~ObCompleteRestoreDag()
{
}

bool ObCompleteRestoreDag::operator==(const share::ObIDag &other) const
{
  return false;
}

uint64_t ObCompleteRestoreDag::hash() const
{
  return 0;
}

int ObCompleteRestoreDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  return OB_NOT_SUPPORTED;
}

int ObCompleteRestoreDag::prepare_ctx(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObInitialCompleteRestoreDag
ObInitialCompleteRestoreDag::ObInitialCompleteRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObCompleteRestoreDag(dag_type)
{
}

ObInitialCompleteRestoreDag::~ObInitialCompleteRestoreDag()
{
}

int ObInitialCompleteRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObInitialCompleteRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObInitialCompleteRestoreDag::init(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObInitialCompleteRestoreTask
ObInitialCompleteRestoreTask::ObInitialCompleteRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_RESTORE_TAILORED_PREPARE)
{
}

ObInitialCompleteRestoreTask::~ObInitialCompleteRestoreTask()
{
}

int ObInitialCompleteRestoreTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObInitialCompleteRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObWaitDataReadyRestoreDag
ObWaitDataReadyRestoreDag::ObWaitDataReadyRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObCompleteRestoreDag(dag_type)
{
}

ObWaitDataReadyRestoreDag::~ObWaitDataReadyRestoreDag()
{
}

int ObWaitDataReadyRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObWaitDataReadyRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObWaitDataReadyRestoreDag::init(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObWaitDataReadyRestoreTask
ObWaitDataReadyRestoreTask::ObWaitDataReadyRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_RESTORE_TAILORED_PROCESS)
{
}

ObWaitDataReadyRestoreTask::~ObWaitDataReadyRestoreTask()
{
}

int ObWaitDataReadyRestoreTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObWaitDataReadyRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObFinishCompleteRestoreDag
ObFinishCompleteRestoreDag::ObFinishCompleteRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObCompleteRestoreDag(dag_type)
{
}

ObFinishCompleteRestoreDag::~ObFinishCompleteRestoreDag()
{
}

int ObFinishCompleteRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObFinishCompleteRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObFinishCompleteRestoreDag::init(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObFinishCompleteRestoreTask
ObFinishCompleteRestoreTask::ObFinishCompleteRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_RESTORE_TAILORED_FINISH)
{
}

ObFinishCompleteRestoreTask::~ObFinishCompleteRestoreTask()
{
}

int ObFinishCompleteRestoreTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObFinishCompleteRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

} // namespace restore
} // namespace oceanbase
