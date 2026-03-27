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
  VIRTUAL_TO_STRING_KV(K_(task_id), K_(type), K_(src_info));
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
  virtual int init() = 0;
  virtual bool is_valid() const { return is_inited_; }
  virtual void reset() { is_inited_ = false; }
public:
  virtual bool is_standby_restore_helper() const { return false; }
  virtual int fetch_ls_meta(ObLSMetaPackage &ls_meta) = 0;
  virtual int fetch_tablet_meta(const common::ObTabletID &tablet_id, ObMigrationTabletParam &tablet_meta) = 0;
  virtual int fetch_sstable_meta(
      const common::ObTabletID &tablet_id, 
      blocksstable::ObMigrationSSTableParam &sstable_meta) = 0;
  virtual int fetch_macro_block(const ObRestoreMacroBlockId &macro_id, blocksstable::ObBufferReader &buffer) = 0;
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(task_id));
private: 
  bool is_inited_;
  share::ObTaskId task_id_;
};

class ObStandbyRestoreHelper : public ObIRestoreHelper
{
public:
  ObStandbyRestoreHelper();
  virtual ~ObStandbyRestoreHelper();
  virtual bool is_valid() const override;
  virtual void reset() override;
  int init(
      const ObStorageHASrcInfo &fixed_src,
      obrpc::ObStorageRpcProxy *svr_rpc_proxy,
      storage::ObStorageRpc *storage_rpc,
      common::ObMySQLProxy *sql_proxy,
      common::ObInOutBandwidthThrottle *bandwidth_throttle);
  bool is_standby_restore_helper() const override { return true; }
  virtual int fetch_ls_meta(ObLSMetaPackage &ls_meta) override;
  virtual int fetch_tablet_meta(const common::ObTabletID &tablet_id, ObMigrationTabletParam &tablet_meta) override;
  virtual int fetch_sstable_meta(
      const common::ObTabletID &tablet_id, 
      blocksstable::ObMigrationSSTableParam &sstable_meta) override;
  virtual int fetch_macro_block(
      const ObRestoreMacroBlockId &macro_id,
      blocksstable::ObBufferReader &buffer) override;
private:
  ObStorageHASrcInfo src_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObStandbyRestoreHelper);
};

} // namespace restore
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LS_RESTORE_HELPER_
