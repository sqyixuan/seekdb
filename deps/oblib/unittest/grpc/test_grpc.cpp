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

#include "lib/oblog/ob_log_module.h"
#include "grpc/example.grpc.pb.h"
#include "lib/utility/ob_test_util.h"
#include "grpc/ob_grpc_context.h"
#include <gtest/gtest.h>
#include "rpc/frame/ob_req_transport.h"
#include "lib/coro/testing.h"
#include <linux/futex.h>
#include "grpc/ob_grpc_server.h"

namespace oceanbase
{
using namespace common;
using namespace example;

namespace unittest
{

class TestGrpc : public ::testing::Test
{
public:
  TestGrpc() {}
  virtual ~TestGrpc() {}
public:
  virtual void SetUp();
  virtual void TearDown();
};

const int64_t RPC_TIMEOUT = 2000 * 1000 * 1000L;

void TestGrpc::SetUp()
{

}

void TestGrpc::TearDown()
{
}

using example::HelloRequest;
using example::HelloReply;

class ExampleServiceImpl final : public example::ExampleService::Service {
public:
    Status SayHello(grpc::ServerContext* context, const HelloRequest* request,
                    HelloReply* reply) override {
      std::string prefix("Hello ");
      reply->set_message(prefix + request->name());
      std::cout << "Received SayHello request from " << request->name() << std::endl;
      return Status::OK;
    }

};


TEST_F(TestGrpc, BASE)
{
  oceanbase::obgrpc::ObGrpcClient<example::ExampleService> grpc_client_;

  oceanbase::common::ObAddr addr;
  addr.set_ip_addr("127.0.0.1", 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, grpc_client_.init(addr, RPC_TIMEOUT, 1, 500));

  addr.set_ip_addr("127.0.0.1", 8001);
  int ret = grpc_client_.init(addr, RPC_TIMEOUT, 1, 500);
  ASSERT_EQ(OB_SUCCESS, ret);
  example::HelloRequest req;
  req.set_name("Grpc");
  example::HelloReply rep;
  ASSERT_EQ(OB_SUCCESS, GRPC_CALL(grpc_client_, SayHello, req, &rep));
  ASSERT_EQ(rep.message(), std::string("Hello Grpc"));
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  oceanbase::obgrpc::ObGrpcServer grpc_server;
  oceanbase::unittest::ExampleServiceImpl service;
  grpc_server.register_service(&service);
  grpc_server.start(8001);
  sleep(3);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  int rc = RUN_ALL_TESTS();
  grpc_server.stop();
  return rc;
}
