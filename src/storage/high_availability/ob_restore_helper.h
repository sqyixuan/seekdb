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
#include "storage/high_availability/ob_restore_helper_ctx.h"

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
  static bool is_valid(const TYPE &type) { return type >= 0 && type < MAX_RESTORE_TASK_TYPE; }
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
  ObIRestoreHelper() = default;
  virtual ~ObIRestoreHelper() = default;
  virtual bool is_valid() const { return false; }
  virtual void destroy() = 0;
public:
  virtual int copy_for_task(common::ObIAllocator &allocator, ObIRestoreHelper *&helper) const = 0;
  virtual bool is_standby_restore_helper() const { return false; }
  virtual int check_restore_precondition() = 0;
  // Init LS view streaming context before fetching ls meta/tablet infos.
  virtual int init_for_ls_view() = 0;
  virtual int fectch_next_tablet_info(obrpc::ObCopyTabletInfo &tablet_info) = 0;
  virtual int fetch_ls_meta(ObLSMetaPackage &ls_meta) = 0;
  virtual int fetch_tablet_meta(const common::ObTabletID &tablet_id, ObCopyTabletInfo &tablet_info) = 0;
  virtual int init_for_build_tablets_sstable_info(
      ObLS *ls,
      const common::ObIArray<ObTabletHandle> &tablet_handle_array) = 0;
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
};

class ObStandbyRestoreHelper : public ObIRestoreHelper
{
public:
  ObStandbyRestoreHelper();
  virtual ~ObStandbyRestoreHelper();
  virtual bool is_valid() const override;
  virtual void destroy() override;
  virtual int copy_for_task(common::ObIAllocator &allocator, ObIRestoreHelper *&helper) const override;
  int init(
      const common::ObAddr &src,
      const share::ObTaskId &task_id,
      common::ObMySQLProxy *sql_proxy,
      common::ObInOutBandwidthThrottle *bandwidth_throttle);
  bool is_standby_restore_helper() const override { return true; }
  virtual int check_restore_precondition() override;
  virtual int init_for_ls_view() override;
  virtual int fectch_next_tablet_info(obrpc::ObCopyTabletInfo &tablet_info) override;
  virtual int fetch_ls_meta(ObLSMetaPackage &ls_meta) override;
  // TODO(xingzhi): change to use fetch_tablet_info to get tablet meta.
  virtual int fetch_tablet_meta(const common::ObTabletID &tablet_id, ObCopyTabletInfo &tablet_info) override;
  virtual int init_for_build_tablets_sstable_info(
      ObLS *ls,
      const common::ObIArray<ObTabletHandle> &tablet_handle_array) override;
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
  int create_ctx_(const ObRestoreHelperCtxType ctx_type);
  int build_copy_tablet_sstable_info_arg_for_restore_(
      const ObTabletHandle &tablet_handle,
      obrpc::ObCopyTabletSSTableInfoArg &arg);
  int get_major_sstable_max_snapshot_for_restore_(
      const ObSSTableArray &major_sstable_array,
      int64_t &max_snapshot_version);
  int get_need_copy_ddl_sstable_range_for_restore_(
      const ObTablet *tablet,
      const ObSSTableArray &ddl_sstable_array,
      share::ObScnRange &need_copy_scn_range);
  int fetch_macro_block_header_(
      ObRestoreHelperMacroBlockCtx *macro_block_ctx,
      obrpc::ObCopyMacroBlockHeader &header);
  int fetch_macro_block_data_(
      ObRestoreHelperMacroBlockCtx *macro_block_ctx,
      const obrpc::ObCopyMacroBlockHeader &header,
      blocksstable::ObBufferReader &data_reader);
private:
  bool is_inited_;
  share::ObTaskId task_id_;
  common::ObAddr src_;
  ObMySQLProxy *sql_proxy_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  ObIRestoreHelperCtx *ctx_;
  common::ObArenaAllocator ctx_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObStandbyRestoreHelper);
};

} // namespace restore
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LS_RESTORE_HELPER_
