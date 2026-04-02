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

#ifndef _OCEANBASE_OBSERVER_OB_ARB_SRV_DELIVER_H_
#define _OCEANBASE_OBSERVER_OB_ARB_SRV_DELIVER_H_

#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr_interface.h"
#include "rpc/frame/ob_req_deliver.h"
#include "share/ob_thread_pool.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace arbserver
{

using rpc::ObRequest;
using rpc::frame::ObReqQueue;
using rpc::frame::ObiReqQHandler;

class RpcQueueThread
{
public:
  RpcQueueThread(const char *thread_name=nullptr)
    : thread_(queue_, thread_name)
  {}

  int init() { return queue_.init(); }
public:
  class Thread : public lib::TGRunnable {
  public:
    Thread(ObReqQueue &queue, const char *thread_name)
      : queue_(queue), thread_name_(thread_name)
    {}
    void run1()
    {
      if (thread_name_ != nullptr) {
        lib::set_thread_name(thread_name_, get_thread_idx());
      }
      queue_.loop();
    }
  private:
    ObReqQueue &queue_;
    const char *thread_name_;
  } thread_;
  ObReqQueue queue_;
};

class ObArbSrvDeliver
  : public rpc::frame::ObReqQDeliver
{
public:
  ObArbSrvDeliver(ObiReqQHandler &qhandler);
  int init() {return OB_SUCCESS;};
  int init(const common::ObAddr&host);
  void destroy();
  int start(ObiReqQHandler &normal_rpc_qhandler,
            ObiReqQHandler &server_rpc_qhandler);
  void stop() override final;
  int deliver(rpc::ObRequest &req) override final;

public:
  static const int64_t MIN_NORMAL_RPC_QUEUE_THREAD_CNT;
  static const int64_t MIN_SERVER_RPC_QUEUE_THREAD_CNT;
  static const int64_t MINI_MODE_RPC_QUEUE_CNT = 1;
  static const int64_t MAX_RPC_QUEUE_LEN = 1024;
public:
  static int64_t get_normal_rpc_thread_num()
  {
    return std::max(MIN_NORMAL_RPC_QUEUE_THREAD_CNT, common::get_cpu_count() / 2);
  }
  static int64_t get_server_rpc_thread_num()
  {
    return std::max(MIN_SERVER_RPC_QUEUE_THREAD_CNT, common::get_cpu_count() / 2);
  }

private:
  int choose_queue_thread_(const rpc::ObRequest &req,
                           ObReqQueue *&queue);
  int create_queue_thread_(const int tg_id,
                           const char *thread_name,
                           RpcQueueThread *&qthread,
                           ObiReqQHandler *qhandler);
  void destroy_queue_thread_(RpcQueueThread *&qthread);;
private:
  bool is_inited_;
  bool stop_;
  common::ObAddr host_;
  RpcQueueThread *normal_rpc_queue_thread_;
  RpcQueueThread *server_rpc_queue_thread_;
};

} // end namespace arbserver
} // end namespace oceanbase

#endif // _OCEANBASE_OBSERVER_OB_ARB_SRV_DELIVER_H_
