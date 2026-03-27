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

#ifndef OCEANBASE_STORAGE_LS_RESTORE_HELPER_
#define OCEANBASE_STORAGE_LS_RESTORE_HELPER_

#include "share/ob_task_define.h"
#include "ob_storage_restore_struct.h"
#include "storage/ob_storage_rpc.h"
#include "share/ob_common_rpc_proxy.h"
#include "ob_storage_ha_struct.h"
#include "lib/allocator/page_arena.h"
#include "storage/ls/ob_ls_meta_package.h"
#include "storage/tablet/ob_tablet_meta.h"            // ObMigrationTabletParam
#include "storage/tablet/ob_tablet_create_sstable_param.h" // blocksstable::ObMigrationSSTableParam
#include "ob_storage_ha_tablet_builder.h"
#include "ob_storage_ha_dag.h"
#include "ob_storage_ha_reader.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace blocksstable
{
class ObBufferReader;
}

namespace oceanbase
{
namespace restore
{
struct ObRestoreTaskType
{
  enum TYPE
  {
    STANDBY_RESTORE_TASK = 0,
    MAX_RESTORE_TASK_TYPE,
  };
  static const char *get_str(const TYPE &type);
  static OB_INLINE bool is_valid(const TYPE &type) { return type >= 0 && type < MAX_RESTORE_TASK_TYPE; }
};

struct ObRestoreTask final
{
public:
  ObRestoreTask();
  ~ObRestoreTask();
  void reset();
  bool is_valid() const;
  bool is_standby_restore() const { return ObRestoreTaskType::STANDBY_RESTORE_TASK == type_; }
  TO_STRING_KV(K_(task_id), K_(type), K_(src_info));
public:
  share::ObTaskId task_id_;
  ObRestoreTaskType::TYPE type_;
  common::ObAddr src_info_;
};

class ObIRestoreHelper
{
public:
  ObIRestoreHelper() {}
  virtual ~ObIRestoreHelper() {}
  virtual int init() = 0;
  virtual bool is_valid() const { return false; }
  virtual void destroy() = 0;
public:
  virtual int copy_for_task(common::ObIAllocator &allocator, ObIRestoreHelper *&helper) const = 0;
  virtual bool is_standby_restore_helper() const { return false; }
  virtual bool is_leader_restore() const = 0;
  virtual int check_disk_space() = 0;
  virtual int fetch_next_tablet_info(obrpc::ObCopyTabletInfo &tablet_info) = 0;
  virtual int fetch_ls_meta(ObLSMetaPackage &ls_meta) = 0;
  virtual int fetch_tablet_meta(const common::ObTabletID &tablet_id, ObCopyTabletInfo &tablet_info) = 0;
  virtual int init_for_build_tablets_sstable_info(const common::ObIArray<ObTabletID> &tablet_id_array) = 0;
  virtual int fetch_next_tablet_sstable_header(obrpc::ObCopyTabletSSTableHeader &copy_header) = 0;
  virtual int fetch_next_sstable_meta(obrpc::ObCopyTabletSSTableInfo &sstable_info) = 0;
  // Build sstable macro range info for copy chain. The helper is responsible for iterating
  // and returning range info for each sstable key in the input list.
  virtual int init_for_sstable_macro_range(const common::ObIArray<storage::ObITable::TableKey> &copy_table_key_array) = 0;
  virtual int fetch_next_sstable_macro_range_info(storage::ObCopySSTableMacroRangeInfo &sstable_macro_range_info) = 0;
  // Macro block copy iteration for a single sstable range.
  virtual int init_for_macro_block_copy(
      const storage::ObITable::TableKey &copy_table_key,
      const storage::ObCopyMacroRangeInfo &macro_range_info,
      const share::SCN &backfill_tx_scn,
      const int64_t data_version,
      storage::ObMacroBlockReuseMgr *macro_block_reuse_mgr) = 0;
  virtual int fetch_next_macro_block(storage::ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data) = 0;
  virtual int fetch_macro_block(const ObRestoreMacroBlockId &macro_id, blocksstable::ObBufferReader &buffer) = 0;
  DECLARE_VIRTUAL_TO_STRING;
  DISALLOW_COPY_AND_ASSIGN(ObIRestoreHelper);
};

class ObStandbyRestoreHelper : public ObIRestoreHelper
{
public:
  ObStandbyRestoreHelper();
  virtual ~ObStandbyRestoreHelper();
  virtual int init() override { return OB_NOT_SUPPORTED; }  // Pure virtual from base class
  virtual bool is_valid() const override;
  virtual void destroy() override;
  virtual int copy_for_task(common::ObIAllocator &allocator, ObIRestoreHelper *&helper) const override;
  int init(
      const common::ObAddr &src,
      obrpc::ObStorageRpcProxy *svr_rpc_proxy,
      storage::ObStorageRpc *storage_rpc,
      common::ObMySQLProxy *sql_proxy,
      common::ObInOutBandwidthThrottle *bandwidth_throttle);
  bool is_standby_restore_helper() const override { return true; }
  bool is_leader_restore() const override { return false; }
  virtual int check_disk_space() override;
  virtual int fetch_next_tablet_info(obrpc::ObCopyTabletInfo &tablet_info) override;
  virtual int fetch_ls_meta(ObLSMetaPackage &ls_meta) override;
  virtual int fetch_tablet_meta(const common::ObTabletID &tablet_id, ObCopyTabletInfo &tablet_info) override;
  virtual int init_for_build_tablets_sstable_info(const common::ObIArray<ObTabletID> &tablet_id_array) override;
  virtual int fetch_next_tablet_sstable_header(obrpc::ObCopyTabletSSTableHeader &copy_header) override;
  virtual int fetch_next_sstable_meta(obrpc::ObCopyTabletSSTableInfo &sstable_info) override;
  // Build sstable macro range info for copy chain. The helper is responsible for iterating
  // and returning range info for each sstable key in the input list.
  virtual int init_for_sstable_macro_range(const common::ObIArray<storage::ObITable::TableKey> &copy_table_key_array) override;
  virtual int fetch_next_sstable_macro_range_info(storage::ObCopySSTableMacroRangeInfo &sstable_macro_range_info) override;
  // Macro block copy iteration for a single sstable range.
  virtual int init_for_macro_block_copy(
      const storage::ObITable::TableKey &copy_table_key,
      const storage::ObCopyMacroRangeInfo &macro_range_info,
      const share::SCN &backfill_tx_scn,
      const int64_t data_version,
      storage::ObMacroBlockReuseMgr *macro_block_reuse_mgr) override;
  virtual int fetch_next_macro_block(storage::ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data) override;
  virtual int fetch_macro_block(const ObRestoreMacroBlockId &macro_id, blocksstable::ObBufferReader &buffer) override;
private:
  bool is_inited_;
  share::ObTaskId task_id_;
  common::ObAddr src_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObStandbyRestoreHelper);
};

} // namespace restore
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LS_RESTORE_HELPER_
