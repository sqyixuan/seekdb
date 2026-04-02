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
#include <iostream>
#include <memory>
#include <thread>
#include <string>
#include <grpcpp/grpcpp.h>
#include "grpc/ob_grpc_server.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::Status;

namespace oceanbase {
namespace obgrpc {

ObGrpcServer::ObGrpcServer()
  : builder_(std::make_unique<ServerBuilder>()) {}

void ObGrpcServer::register_service(grpc::Service *service )
{
  builder_->RegisterService(service);
}

void ObGrpcServer::start(int port)
{
  thread_ = std::thread([port, this] {
    std::string addr = "0.0.0.0:" + std::to_string(port);
    builder_->AddListeningPort(addr, grpc::InsecureServerCredentials());
    server_ = builder_->BuildAndStart();
    if (!server_) {
      LOG_DBA_ERROR(OB_ERROR, "msg", "failed to build and start server");
    } else {
      server_->Wait();
    }
  });
}

void ObGrpcServer::stop()
{
  if (server_) {
    server_->Shutdown();
  }
  if (thread_.joinable()) {
    thread_.join();
  }
}

} // end of namespace obgrpc
} // end of namespace oceanbase
