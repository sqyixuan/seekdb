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

DEF_TO_STRING(ObIRestoreHelper)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("type", "ObIRestoreHelper");
  J_OBJ_END();
  return pos;
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

void ObStandbyRestoreHelper::destroy()
{
}

int ObStandbyRestoreHelper::init(
    const common::ObAddr &src,
    obrpc::ObStorageRpcProxy *svr_rpc_proxy,
    storage::ObStorageRpc *storage_rpc,
    common::ObMySQLProxy *sql_proxy,
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
{
  UNUSED(src);
  UNUSED(svr_rpc_proxy);
  UNUSED(storage_rpc);
  UNUSED(sql_proxy);
  UNUSED(bandwidth_throttle);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_ls_meta(ObLSMetaPackage &ls_meta)
{
  UNUSED(ls_meta);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::copy_for_task(common::ObIAllocator &allocator, ObIRestoreHelper *&helper) const
{
  UNUSED(allocator);
  UNUSED(helper);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::check_disk_space()
{
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_next_tablet_info(obrpc::ObCopyTabletInfo &tablet_info)
{
  UNUSED(tablet_info);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_tablet_meta(const common::ObTabletID &tablet_id, ObCopyTabletInfo &tablet_info)
{
  UNUSED(tablet_id);
  UNUSED(tablet_info);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::init_for_build_tablets_sstable_info(const common::ObIArray<ObTabletID> &tablet_id_array)
{
  UNUSED(tablet_id_array);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_next_tablet_sstable_header(obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  UNUSED(copy_header);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_next_sstable_meta(obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  UNUSED(sstable_info);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::init_for_sstable_macro_range(const common::ObIArray<storage::ObITable::TableKey> &copy_table_key_array)
{
  UNUSED(copy_table_key_array);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_next_sstable_macro_range_info(storage::ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  UNUSED(sstable_macro_range_info);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::init_for_macro_block_copy(
    const storage::ObITable::TableKey &copy_table_key,
    const storage::ObCopyMacroRangeInfo &macro_range_info,
    const share::SCN &backfill_tx_scn,
    const int64_t data_version,
    storage::ObMacroBlockReuseMgr *macro_block_reuse_mgr)
{
  UNUSED(copy_table_key);
  UNUSED(macro_range_info);
  UNUSED(backfill_tx_scn);
  UNUSED(data_version);
  UNUSED(macro_block_reuse_mgr);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_next_macro_block(storage::ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data)
{
  UNUSED(read_data);
  return OB_NOT_SUPPORTED;
}

int ObStandbyRestoreHelper::fetch_macro_block(
    const ObRestoreMacroBlockId &macro_id,
    blocksstable::ObBufferReader &buffer)
{
  UNUSED(macro_id);
  UNUSED(buffer);
  return OB_NOT_SUPPORTED;
}

} // namespace restore
} // namespace oceanbase
