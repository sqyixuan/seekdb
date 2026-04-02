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
#include "lib/oblog/ob_log_module.h"
#include <string>

namespace oceanbase
{
namespace storage
{

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
};

class ObStorageGrpcClient
{
public:
  ObStorageGrpcClient();
  ~ObStorageGrpcClient();

  int init(const common::ObAddr& addr, int64_t timeout);
  int copy_ls_info(const obrpc::ObCopyLSInfoArg& arg, obrpc::ObCopyLSInfo& result);
  int fetch_tablet_info(const obrpc::ObCopyTabletInfoArg& arg,
                        std::function<int(const obrpc::ObCopyTabletInfo&)> callback);
private:
  bool is_inited_;
  obgrpc::ObGrpcClient<storageservice::StorageService> grpc_client_;
};

} // storage
} // oceanbase

#endif
