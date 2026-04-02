/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_GRPC_SERVER_H_
#define OCEANBASE_GRPC_SERVER_H_

#include <thread>
#include <grpcpp/grpcpp.h>
using grpc::Server;
using grpc::ServerBuilder;
using grpc::Service;

namespace oceanbase
{
namespace obgrpc
{

class ObGrpcServer
{
public:
  ObGrpcServer();
  void register_service(grpc::Service*);
  int start(int port);
  void stop();
  void wait();
private:
  std::thread thread_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<grpc::ServerBuilder> builder_;
};


} // end of namespace obgrpc
} // end of namespace oceanbase

#endif /* OCEANBASE_GRPC_SERVER_H_ */
