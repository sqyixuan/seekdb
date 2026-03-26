/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this software except in compliance with the License.
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

#ifndef OCEABASE_STORAGE_GRPC
#define OCEABASE_STORAGE_GRPC

#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "grpc/storageservice.grpc.pb.h"
#include "grpc/ob_grpc_context.h"
#include "storage/ob_storage_rpc.h"
#include "storage/ls/ob_ls_meta_package.h"
#include "lib/oblog/ob_log_module.h"
#include <string>

namespace oceanbase
{
namespace restore
{
  struct ObRestoreHelperLSViewCtx;
  struct ObRestoreHelperSSTableInfoCtx;
  struct ObRestoreHelperSSTableMacroRangeCtx;
  struct ObRestoreHelperMacroBlockCtx;
  struct ObRestoreHelperTabletInfoCtx;
}

namespace storage
{

class ObStorageGrpcClient;

struct ObLSViewStreamInitResult
{
public:
  ObLSViewStreamInitResult();
  bool is_valid() const;

  ObLSMetaPackage ls_meta_;
  common::ObSArray<common::ObTabletID> tablet_id_list_;
  ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *tablet_info_context_;
  grpc::ClientReader<storageservice::FetchTabletInfoRes> *tablet_info_reader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSViewStreamInitResult);
};

struct ObSSTableInfoStreamInitResult
{
public:
  ObSSTableInfoStreamInitResult();
  bool is_valid() const;

  ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *sstable_info_context_;
  grpc::ClientReader<storageservice::FetchTabletSSTableInfoRes> *sstable_info_reader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableInfoStreamInitResult);
};

struct ObSSTableMacroInfoStreamInitResult
{
public:
  ObSSTableMacroInfoStreamInitResult();
  bool is_valid() const;

  ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *macro_info_context_;
  grpc::ClientReader<storageservice::FetchSSTableMacroInfoRes> *macro_info_reader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMacroInfoStreamInitResult);
};

struct ObMacroBlockStreamInitResult
{
public:
  ObMacroBlockStreamInitResult();
  bool is_valid() const;

  ObStorageGrpcClient *grpc_client_;
  grpc::ClientContext *macro_block_context_;
  grpc::ClientReader<storageservice::FetchMacroBlockRes> *macro_block_reader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockStreamInitResult);
};

class ObStorageGrpcServiceImpl final : public storageservice::StorageService::Service
{
public:
  ObStorageGrpcServiceImpl() {}
  virtual ~ObStorageGrpcServiceImpl() {}

  grpc::Status copy_ls_info(grpc::ServerContext* context,
                            const storageservice::CopyLSInfoReq* request,
                            storageservice::CopyLSInfoRes* response) override;

  grpc::Status fetch_tablet_info(grpc::ServerContext* context,
                                    const storageservice::FetchTabletInfoReq* request,
                                    grpc::ServerWriter<storageservice::FetchTabletInfoRes>* writer) override;

  grpc::Status fetch_tablet_sstable_info(grpc::ServerContext* context,
                                            const storageservice::FetchTabletSSTableInfoReq* request,
                                            grpc::ServerWriter<storageservice::FetchTabletSSTableInfoRes>* writer) override;

  grpc::Status fetch_sstable_macro_info(grpc::ServerContext* context,
                                            const storageservice::FetchSSTableMacroInfoReq* request,
                                            grpc::ServerWriter<storageservice::FetchSSTableMacroInfoRes>* writer) override;

  grpc::Status fetch_macro_block(grpc::ServerContext* context,
                                    const storageservice::FetchMacroBlockReq* request,
                                    grpc::ServerWriter<storageservice::FetchMacroBlockRes>* writer) override;

  grpc::Status check_restore_precondition(grpc::ServerContext* context,
                                            const storageservice::CheckRestorePreconditionReq* request,
                                            storageservice::CheckRestorePreconditionRes* response) override;

private:
  int build_tablet_sstable_info_(
      grpc::ServerContext* context,
      const obrpc::ObCopyTabletSSTableInfoArg &tablet_arg,
      ObLS *ls,
      grpc::ServerWriter<storageservice::FetchTabletSSTableInfoRes>* writer);
  int build_sstable_macro_info_(
      grpc::ServerContext* context,
      const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
      const obrpc::ObCopySSTableMacroRangeInfoArg &arg,
      grpc::ServerWriter<storageservice::FetchSSTableMacroInfoRes>* writer);
};

class ObStorageGrpcClient
{
public:
  ObStorageGrpcClient();
  ~ObStorageGrpcClient();

  int init(const common::ObAddr& addr, int64_t timeout);
  int copy_ls_info(obrpc::ObCopyLSInfo& result); // Single-replica: no args needed
  int check_restore_precondition(obrpc::ObCheckRestorePreconditionResult& result); // Single-replica: no args needed
  int fetch_tablet_info(const obrpc::ObCopyTabletInfoArg& arg,
                        std::function<int(const obrpc::ObCopyTabletInfo&)> callback);
  int create_tablet_info_stream(
      const obrpc::ObCopyTabletInfoArg &arg,
      grpc::ClientContext &context,
      grpc::ClientReader<storageservice::FetchTabletInfoRes> *&reader);
  int create_tablet_sstable_info_stream(
      const obrpc::ObCopyTabletsSSTableInfoArg &arg,
      grpc::ClientContext &context,
      grpc::ClientReader<storageservice::FetchTabletSSTableInfoRes> *&reader);
  int create_sstable_macro_info_stream(
      const obrpc::ObCopySSTableMacroRangeInfoArg &arg,
      grpc::ClientContext &context,
      grpc::ClientReader<storageservice::FetchSSTableMacroInfoRes> *&reader);
  int create_macro_block_stream(
      const obrpc::ObCopyMacroBlockRangeArg &arg,
      grpc::ClientContext &context,
      grpc::ClientReader<storageservice::FetchMacroBlockRes> *&reader);
  int translate_error(const grpc::Status &status);
  static int init_ls_view_stream(
      const common::ObAddr &src_addr,
      int64_t timeout,
      common::ObIAllocator &allocator,
      ObLSViewStreamInitResult &result); // Single-replica: no ls_info_arg needed
  static int init_tablet_sstable_info_stream(
      const common::ObAddr &src_addr,
      int64_t timeout,
      const obrpc::ObCopyTabletsSSTableInfoArg &arg,
      common::ObIAllocator &allocator,
      ObSSTableInfoStreamInitResult &result);
  static int init_sstable_macro_info_stream(
      const common::ObAddr &src_addr,
      int64_t timeout,
      const obrpc::ObCopySSTableMacroRangeInfoArg &arg,
      common::ObIAllocator &allocator,
      ObSSTableMacroInfoStreamInitResult &result);
  static int init_macro_block_stream(
      const common::ObAddr &src_addr,
      int64_t timeout,
      const obrpc::ObCopyMacroBlockRangeArg &arg,
      common::ObIAllocator &allocator,
      ObMacroBlockStreamInitResult &result);
  static int init_tablet_info_stream(
      const common::ObAddr &src_addr,
      int64_t timeout,
      const obrpc::ObCopyTabletInfoArg &arg,
      common::ObIAllocator &allocator,
      restore::ObRestoreHelperTabletInfoCtx &tablet_info_ctx);
private:
  bool is_inited_;
  obgrpc::ObGrpcClient<storageservice::StorageService> grpc_client_;
};

} // storage
} // oceanbase

#endif
