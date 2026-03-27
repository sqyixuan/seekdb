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

#define USING_LOG_PREFIX LIB

#include <climits>
#include <iostream>
#include <memory>
#include <thread>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <grpcpp/grpcpp.h>
#include "grpc/ob_grpc_server.h"
#include "grpc/ob_grpc_context.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::Status;

namespace oceanbase {
namespace obgrpc {

ObGrpcServer::ObGrpcServer()
  : server_(nullptr) {}

void ObGrpcServer::register_service(grpc::Service *service)
{
  services_.push_back(service);
}

int ObGrpcServer::start(int port)
{
  int ret = OB_SUCCESS;
  if (services_.empty()) {
    ret = OB_NOT_INIT;
    LOG_WARN("no gRPC services registered, cannot start server", K(port));
    return ret;
  }
  ServerBuilder builder;
  for (grpc::Service *svc : services_) {
    builder.RegisterService(svc);
  }
  std::string addr = "0.0.0.0:" + std::to_string(port);
  builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
  const int MAX_MESSAGE_SIZE = 512 * 1024 * 1024;
  builder.SetMaxReceiveMessageSize(MAX_MESSAGE_SIZE);
  builder.SetMaxSendMessageSize(MAX_MESSAGE_SIZE);
  std::shared_ptr<grpc::ServerCredentials> creds;
  if (ob_grpc_is_rpc_tls_enabled()) {
    creds = create_server_credentials(cert_provider_);
    if (!creds) {
      ret = OB_INIT_FAIL;
      LOG_DBA_ERROR(OB_INIT_FAIL, "msg", "failed to create mTLS server credentials, "
                    "check that wallet/ca.pem, wallet/cert.pem and wallet/key.pem exist", K(port));
    }
  } else {
    creds = grpc::InsecureServerCredentials();
  }
  if (OB_SUCC(ret)) {
    builder.AddListeningPort(addr, creds);
    server_ = builder.BuildAndStart();
    if (!server_) {
      ret = OB_ERROR;
      LOG_DBA_ERROR(OB_ERROR, "msg", "failed to build and start server", K(port));
    } else {
      thread_ = std::thread([this] {
        server_->Wait();
      });
      LOG_INFO("gRPC server started successfully", K(port));
    }
  }
  return ret;
}

void ObGrpcServer::stop()
{
  if (server_) {
    server_->Shutdown();
    server_.reset();
    LOG_INFO("gRPC server stopped");
  }
}

void ObGrpcServer::wait()
{
  if (thread_.joinable()) {
    thread_.join();
  }
}

} // end of namespace obgrpc
} // end of namespace oceanbase
