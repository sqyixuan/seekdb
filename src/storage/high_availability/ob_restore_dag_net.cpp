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

#include "ob_restore_dag_net.h"

namespace oceanbase
{
namespace restore
{

// ObRestoreDagNetCtx
ObRestoreDagNetCtx::ObRestoreDagNetCtx()
  : tenant_id_(0),
    local_clog_checkpoint_scn_(),
    task_(),
    allocator_("RestoreDagCtx"),
    ha_table_info_mgr_(),
    tablet_group_mgr_(),
    src_ls_meta_package_(),
    sys_tablet_id_array_(),
    data_tablet_id_array_(),
    start_ts_(0),
    finish_ts_(0),
    check_tablet_info_cost_time_(0),
    tablet_simple_info_map_()
{
}

ObRestoreDagNetCtx::~ObRestoreDagNetCtx()
{
}

int ObRestoreDagNetCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

bool ObRestoreDagNetCtx::is_valid() const
{
  return false;
}

void ObRestoreDagNetCtx::reset()
{
  // do nothing
}

void ObRestoreDagNetCtx::reuse()
{
  // do nothing
}

// ObCopyTabletCtx
ObCopyTabletCtx::ObCopyTabletCtx()
  : tablet_id_(),
    tablet_handle_(),
    extra_info_(),
    lock_(),
    status_(ObCopyTabletStatus::STATUS::TABLET_EXIST)
{
}

ObCopyTabletCtx::~ObCopyTabletCtx()
{
}

bool ObCopyTabletCtx::is_valid() const
{
  return false;
}

void ObCopyTabletCtx::reset()
{
  // do nothing
}

int ObCopyTabletCtx::set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status)
{
  return OB_NOT_SUPPORTED;
}

int ObCopyTabletCtx::get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const
{
  return OB_NOT_SUPPORTED;
}

int ObCopyTabletCtx::get_copy_tablet_record_extra_info(ObCopyTabletRecordExtraInfo *&extra_info)
{
  return OB_NOT_SUPPORTED;
}

// ObRestoreDagNetInitParam
ObRestoreDagNetInitParam::ObRestoreDagNetInitParam()
  : task_(),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    sql_proxy_(nullptr)
{
}

bool ObRestoreDagNetInitParam::is_valid() const
{
  return false;
}

// ObRestoreDagNet
ObRestoreDagNet::ObRestoreDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_RESTORE)
{
}

ObRestoreDagNet::~ObRestoreDagNet()
{
}

int ObRestoreDagNet::init_by_param(const share::ObIDagInitParam *param)
{
  return OB_NOT_SUPPORTED;
}

bool ObRestoreDagNet::is_valid() const
{
  return false;
}

int ObRestoreDagNet::start_running()
{
  return OB_NOT_SUPPORTED;
}

bool ObRestoreDagNet::operator==(const share::ObIDagNet &other) const
{
  return false;
}

uint64_t ObRestoreDagNet::hash() const
{
  return 0;
}

int ObRestoreDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObRestoreDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObRestoreDagNet::clear_dag_net_ctx()
{
  return OB_NOT_SUPPORTED;
}

int ObRestoreDagNet::deal_with_cancel()
{
  return OB_NOT_SUPPORTED;
}

// ObRestoreDag
ObRestoreDag::ObRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObRestoreDag::~ObRestoreDag()
{
}

int ObRestoreDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  return OB_NOT_SUPPORTED;
}

int ObRestoreDag::prepare_ctx(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObInitialRestoreDag
ObInitialRestoreDag::ObInitialRestoreDag()
  : ObRestoreDag(share::ObDagType::DAG_TYPE_INITIAL_LS_RESTORE)
{
}

ObInitialRestoreDag::~ObInitialRestoreDag()
{
}

bool ObInitialRestoreDag::operator==(const share::ObIDag &other) const
{
  return false;
}

int ObInitialRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObInitialRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObInitialRestoreDag::init(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObInitialRestoreTask
ObInitialRestoreTask::ObInitialRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_RESTORE_TAILORED_PREPARE)
{
}

ObInitialRestoreTask::~ObInitialRestoreTask()
{
}

int ObInitialRestoreTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObInitialRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObStartRestoreDag
ObStartRestoreDag::ObStartRestoreDag()
  : ObRestoreDag(share::ObDagType::DAG_TYPE_START_LS_RESTORE)
{
}

ObStartRestoreDag::~ObStartRestoreDag()
{
}

bool ObStartRestoreDag::operator==(const share::ObIDag &other) const
{
  return false;
}

int ObStartRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObStartRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObStartRestoreDag::init(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObStartRestoreTask
ObStartRestoreTask::ObStartRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_RESTORE_TAILORED_PROCESS)
{
}

ObStartRestoreTask::~ObStartRestoreTask()
{
}

int ObStartRestoreTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObStartRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObSysTabletsRestoreDag
ObSysTabletsRestoreDag::ObSysTabletsRestoreDag()
  : ObRestoreDag(share::ObDagType::DAG_TYPE_SYS_TABLETS_RESTORE)
{
}

ObSysTabletsRestoreDag::~ObSysTabletsRestoreDag()
{
}

bool ObSysTabletsRestoreDag::operator==(const share::ObIDag &other) const
{
  return false;
}

int ObSysTabletsRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObSysTabletsRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObSysTabletsRestoreDag::init(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObSysTabletsRestoreTask
ObSysTabletsRestoreTask::ObSysTabletsRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_MIGRATE_PREPARE)
{
}

ObSysTabletsRestoreTask::~ObSysTabletsRestoreTask()
{
}

int ObSysTabletsRestoreTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObSysTabletsRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObTabletRestoreDag
ObTabletRestoreDag::ObTabletRestoreDag()
  : ObRestoreDag(share::ObDagType::DAG_TYPE_TABLET_GROUP_META_RESTORE)
{
}

ObTabletRestoreDag::~ObTabletRestoreDag()
{
}

bool ObTabletRestoreDag::operator==(const share::ObIDag &other) const
{
  return false;
}

uint64_t ObTabletRestoreDag::hash() const
{
  return 0;
}

int ObTabletRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreDag::inner_reset_status_for_retry()
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreDag::generate_next_dag(share::ObIDag *&dag)
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreDag::init(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    share::ObIDagNet *dag_net,
    ObHATabletGroupCtx *tablet_group_ctx,
    ObTabletType tablet_type)
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreDag::get_tablet_group_ctx(ObHATabletGroupCtx *&tablet_group_ctx)
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreDag::check_is_migrate_data_tablet(bool &is_migrate_data_tablet)
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreDag::get_ls(ObLS *&ls)
{
  return OB_NOT_SUPPORTED;
}

// ObTabletRestoreTask
ObTabletRestoreTask::ObTabletRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_MIGRATE_COPY_LOGIC)
{
}

ObTabletRestoreTask::~ObTabletRestoreTask()
{
}

int ObTabletRestoreTask::init(ObCopyTabletCtx &ctx)
{
  return OB_NOT_SUPPORTED;
}

int ObTabletRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObTabletFinishRestoreTask
ObTabletFinishRestoreTask::ObTabletFinishRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_MIGRATE_FINISH_PHYSICAL)
{
}

ObTabletFinishRestoreTask::~ObTabletFinishRestoreTask()
{
}

int ObTabletFinishRestoreTask::init(const int64_t task_gen_time, const int64_t copy_table_count,
    ObCopyTabletCtx &ctx, ObLS &ls)
{
  return OB_NOT_SUPPORTED;
}

int ObTabletFinishRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObDataTabletsRestoreDag
ObDataTabletsRestoreDag::ObDataTabletsRestoreDag()
  : ObRestoreDag(share::ObDagType::DAG_TYPE_DATA_TABLETS_META_RESTORE)
{
}

ObDataTabletsRestoreDag::~ObDataTabletsRestoreDag()
{
}

bool ObDataTabletsRestoreDag::operator==(const share::ObIDag &other) const
{
  return false;
}

int ObDataTabletsRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObDataTabletsRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObDataTabletsRestoreDag::init(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObDataTabletsRestoreTask
ObDataTabletsRestoreTask::ObDataTabletsRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_MIGRATE_FINISH)
{
}

ObDataTabletsRestoreTask::~ObDataTabletsRestoreTask()
{
}

int ObDataTabletsRestoreTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObDataTabletsRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObTabletGroupRestoreDag
ObTabletGroupRestoreDag::ObTabletGroupRestoreDag()
  : ObRestoreDag(share::ObDagType::DAG_TYPE_TABLET_GROUP_META_RESTORE)
{
}

ObTabletGroupRestoreDag::~ObTabletGroupRestoreDag()
{
}

bool ObTabletGroupRestoreDag::operator==(const share::ObIDag &other) const
{
  return false;
}

uint64_t ObTabletGroupRestoreDag::hash() const
{
  return 0;
}

int ObTabletGroupRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

int ObTabletGroupRestoreDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObTabletGroupRestoreDag::generate_next_dag(share::ObIDag *&dag)
{
  return OB_NOT_SUPPORTED;
}

int ObTabletGroupRestoreDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  return OB_NOT_SUPPORTED;
}

int ObTabletGroupRestoreDag::init(
    const common::ObIArray<ObLogicTabletID> &tablet_id_array,
    share::ObIDagNet *dag_net,
    share::ObIDag *finish_dag,
    ObHATabletGroupCtx *tablet_group_ctx)
{
  return OB_NOT_SUPPORTED;
}

// ObTabletGroupRestoreTask
ObTabletGroupRestoreTask::ObTabletGroupRestoreTask()
  : ObITask(ObITaskType::TASK_TYPE_GROUP_MIGRATE)
{
}

ObTabletGroupRestoreTask::~ObTabletGroupRestoreTask()
{
}

int ObTabletGroupRestoreTask::init(
    const common::ObIArray<ObLogicTabletID> &tablet_id_array,
    share::ObIDag *finish_dag,
    ObHATabletGroupCtx *tablet_group_ctx)
{
  return OB_NOT_SUPPORTED;
}

int ObTabletGroupRestoreTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObRestoreFinishDag
ObRestoreFinishDag::ObRestoreFinishDag()
  : ObRestoreDag(share::ObDagType::DAG_TYPE_FINISH_LS_RESTORE)
{
}

ObRestoreFinishDag::~ObRestoreFinishDag()
{
}

int ObRestoreFinishDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  return OB_NOT_SUPPORTED;
}

bool ObRestoreFinishDag::operator==(const share::ObIDag &other) const
{
  return false;
}

int ObRestoreFinishDag::create_first_task()
{
  return OB_NOT_SUPPORTED;
}

int ObRestoreFinishDag::init(share::ObIDagNet *dag_net)
{
  return OB_NOT_SUPPORTED;
}

// ObRestoreFinishTask
ObRestoreFinishTask::ObRestoreFinishTask()
  : ObITask(ObITaskType::TASK_TYPE_MIGRATE_FINISH)
{
}

ObRestoreFinishTask::~ObRestoreFinishTask()
{
}

int ObRestoreFinishTask::init()
{
  return OB_NOT_SUPPORTED;
}

int ObRestoreFinishTask::process()
{
  return OB_NOT_SUPPORTED;
}

// ObLSRestoreUtils
int ObLSRestoreUtils::init_ha_tablets_builder(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    const ObStorageHASrcInfo src_info,
    const int64_t local_rebuild_seq,
    const ObRestoreType &type,
    ObLS *ls,
    ObStorageHATableInfoMgr *ha_table_info_mgr,
    ObStorageHATabletsBuilder &ha_tablets_builder)
{
  return OB_NOT_SUPPORTED;
}

} // namespace restore
} // namespace oceanbase