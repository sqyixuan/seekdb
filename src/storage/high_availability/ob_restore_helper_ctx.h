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

#ifndef OCEANBASE_STORAGE_RESTORE_HELPER_CTX_
#define OCEANBASE_STORAGE_RESTORE_HELPER_CTX_

#include "lib/container/ob_array.h"
#include "storage/ob_storage_rpc.h"
#include "grpc/storageservice.grpc.pb.h"
#include "storage/tablet/ob_tablet_common.h"
#include "ob_storage_ha_struct.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace storage
{
class ObStorageGrpcClient;
}

namespace storage
{
class ObStorageGrpcClient;
}

namespace restore
{

enum class ObRestoreHelperCtxType
{
  RESTORE_HELPER_CTX_NONE = 0,
  RESTORE_HELPER_CTX_LS_VIEW = 1,
  RESTORE_HELPER_CTX_TABLET_INFO = 2,
  RESTORE_HELPER_CTX_SSTABLE_INFO = 3,
  RESTORE_HELPER_CTX_SSTABLE_MACRO_RANGE =4,
  RESTORE_HELPER_CTX_MACRO_BLOCK =5,
  RESTORE_HELPER_CTX_MAX
};

struct ObIRestoreHelperCtx
{
public:
  ObIRestoreHelperCtx() = default;
  virtual ~ObIRestoreHelperCtx() = default;
  virtual ObRestoreHelperCtxType get_type() const = 0;
  virtual bool is_valid() const = 0;
  virtual void reset() = 0;
  virtual void destroy() = 0;
};

struct ObRestoreHelperLSViewCtx final : public ObIRestoreHelperCtx
{
public:
  ObRestoreHelperLSViewCtx();
  virtual ~ObRestoreHelperLSViewCtx() = default;
  virtual ObRestoreHelperCtxType get_type() const override;
  virtual bool is_valid() const override;
  virtual void reset() override;
  virtual void destroy() override;
  bool ls_meta_fetched_;
  storage::ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *tablet_info_context_;
  grpc::ClientReader<storageservice::FetchTabletInfoRes> *tablet_info_reader_;
  common::ObSArray<common::ObTabletID> tablet_id_list_;
};

struct ObRestoreHelperTabletInfoCtx final : public ObIRestoreHelperCtx
{
public:
  ObRestoreHelperTabletInfoCtx();
  virtual ~ObRestoreHelperTabletInfoCtx() = default;
  virtual ObRestoreHelperCtxType get_type() const override;
  virtual bool is_valid() const override;
  virtual void reset() override;
  virtual void destroy() override;
  storage::ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *tablet_info_context_;
  grpc::ClientReader<storageservice::FetchTabletInfoRes> *tablet_info_reader_;
};

struct ObRestoreHelperSSTableInfoCtx final : public ObIRestoreHelperCtx
{
public:
  ObRestoreHelperSSTableInfoCtx();
  virtual ~ObRestoreHelperSSTableInfoCtx() = default;
  virtual ObRestoreHelperCtxType get_type() const override;
  virtual bool is_valid() const override;
  virtual void reset() override;
  virtual void destroy() override;

  // Stream reader related members
  storage::ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *sstable_info_context_;
  grpc::ClientReader<storageservice::FetchTabletSSTableInfoRes> *sstable_info_reader_;
  int64_t pending_sstable_count_;
  common::ObTabletID cur_tablet_id_;
};

struct ObRestoreHelperSSTableMacroRangeCtx final : public ObIRestoreHelperCtx
{
public:
  ObRestoreHelperSSTableMacroRangeCtx();
  virtual ~ObRestoreHelperSSTableMacroRangeCtx() = default;
  virtual ObRestoreHelperCtxType get_type() const override;
  virtual bool is_valid() const override;
  virtual void reset() override;
  virtual void destroy() override;

  storage::ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *macro_info_context_;
  grpc::ClientReader<storageservice::FetchSSTableMacroInfoRes> *macro_info_reader_;
};

struct ObRestoreHelperMacroBlockCtx final : public ObIRestoreHelperCtx
{
public:
  ObRestoreHelperMacroBlockCtx();
  virtual ~ObRestoreHelperMacroBlockCtx() = default;
  virtual ObRestoreHelperCtxType get_type() const override;
  virtual bool is_valid() const override;
  virtual void reset() override;
  virtual void destroy() override;

  storage::ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *macro_block_context_;
  grpc::ClientReader<storageservice::FetchMacroBlockRes> *macro_block_reader_;
  storage::ObMacroBlockReuseMgr *macro_block_reuse_mgr_;
  int64_t data_version_;
  storage::ObITable::TableKey copy_table_key_;
};

class ObRestoreHelperCtxUtil
{
public:
  static int create_ctx(
      const ObRestoreHelperCtxType ctx_type,
      common::ObIAllocator &allocator,
      ObIRestoreHelperCtx *&ctx);
};

} // namespace restore
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_RESTORE_HELPER_CTX_
