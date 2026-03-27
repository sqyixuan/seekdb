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

#include "ob_restore_helper.h"

namespace oceanbase
{
namespace restore
{

const char *ObRestoreTaskType::get_str(const TYPE &type)
{
  return "UNKNOW";
}

ObRestoreTask::ObRestoreTask()
  : task_id_(),
    type_(ObRestoreTaskType::STANDBY_RESTORE_TASK),
    src_info_()
{
}

ObRestoreTask::~ObRestoreTask()
{
}

void ObRestoreTask::reset()
{
  // do nothing
}

bool ObRestoreTask::is_valid() const
{
  return false;
}

ObStandbyRestoreHelper::ObStandbyRestoreHelper()
  : src_(),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr)
{
}

ObStandbyRestoreHelper::~ObStandbyRestoreHelper()
{
}

bool ObStandbyRestoreHelper::is_valid() const
{
  return false;
}

void ObStandbyRestoreHelper::reset()
{
  // do nothing
}

int ObStandbyRestoreHelper::init(
    const ObStorageHASrcInfo &fixed_src,
    obrpc::ObStorageRpcProxy *svr_rpc_proxy,
    storage::ObStorageRpc *storage_rpc,
    common::ObMySQLProxy *sql_proxy,
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
{
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_ls_meta(ObLSMetaPackage &ls_meta)
{
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_tablet_meta(const common::ObTabletID &tablet_id, ObMigrationTabletParam &tablet_meta)
{
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_sstable_meta(
    const common::ObTabletID &tablet_id,
    blocksstable::ObMigrationSSTableParam &sstable_meta)
{
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_macro_block(
    const ObRestoreMacroBlockId &macro_id,
    blocksstable::ObBufferReader &buffer)
{
  return OB_NOT_SUPPORTED;
}

} // namespace restore
} // namespace oceanbase
