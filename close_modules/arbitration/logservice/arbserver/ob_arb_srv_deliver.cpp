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

#define USING_LOG_PREFIX SERVER

#include "ob_arb_srv_deliver.h"
#include "ob_arb_srv_xlator.h"
#include "rpc/ob_request.h"

namespace oceanbase
{
namespace arbserver
{

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::observer;
using namespace oceanbase::share;

const int64_t ObArbSrvDeliver::MIN_NORMAL_RPC_QUEUE_THREAD_CNT = 1L;
const int64_t ObArbSrvDeliver::MIN_SERVER_RPC_QUEUE_THREAD_CNT = 1L;

ObArbSrvDeliver::ObArbSrvDeliver(ObiReqQHandler &qhandler)
    : ObReqQDeliver(qhandler),
      is_inited_(false),
      stop_(true),
      host_(),
      normal_rpc_queue_thread_(NULL),
      server_rpc_queue_thread_(NULL)
{}

int ObArbSrvDeliver::init(const common::ObAddr &host)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObArbSrvDeliver init twice", K(ret));
  } else if (false == host.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(host));
  } else {
    stop_ = true;
    is_inited_ = true;
    LOG_INFO("ObArbSrvDeliver init successfully");
  }
  return ret;
}

void ObArbSrvDeliver::destroy()
{
  LOG_WARN_RET(OB_SUCCESS, "ObArbSrvDeliver destroy failed");
  stop();
  is_inited_ = false;
  host_.reset();
  destroy_queue_thread_(normal_rpc_queue_thread_);
  destroy_queue_thread_(server_rpc_queue_thread_);
}

int ObArbSrvDeliver::start(ObiReqQHandler &normal_rpc_qhandler,
                           ObiReqQHandler &server_rpc_qhandler)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArbSrvDeliver start success", K(ret));
  } else if (OB_FAIL(create_queue_thread_(lib::TGDefIDs::ArbNormalRpcQueueTh,
                                          "ArbNorRpcTh",
                                          normal_rpc_queue_thread_,
                                          &normal_rpc_qhandler))) {
    LOG_WARN("create_queue_thread_ failed", K(ret));
  } else if (OB_FAIL(create_queue_thread_(lib::TGDefIDs::ArbServerRpcQueueTh,
                                          "ArbSrvRpcTh",
                                          server_rpc_queue_thread_,
                                          &server_rpc_qhandler))) {
    LOG_WARN("create_queue_thread_ failed", K(ret));
  } else {
    stop_ = false;
    LOG_INFO("ObArbSrvDeliver start success");
  }
  return ret;
}

void ObArbSrvDeliver::stop()
{
  if (NULL != normal_rpc_queue_thread_) {
    TG_STOP(lib::TGDefIDs::ArbNormalRpcQueueTh);
    stop_ = true;
    TG_WAIT(lib::TGDefIDs::ArbNormalRpcQueueTh);
  }
  if (NULL != server_rpc_queue_thread_) {
    TG_STOP(lib::TGDefIDs::ArbServerRpcQueueTh);
    stop_ = true;
    TG_WAIT(lib::TGDefIDs::ArbServerRpcQueueTh);
  }
}

int ObArbSrvDeliver::deliver(ObRequest &req)
{
  int ret = OB_SUCCESS;
  const obrpc::ObRpcPacket &pkt
      = reinterpret_cast<const obrpc::ObRpcPacket &>(req.get_packet());
  req.set_group_id(pkt.get_group_id());
  ObReqQueue *queue = NULL;

  req.set_trace_point(ObRequest::OB_EASY_REQUEST_RPC_DELIVER);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArbSrvDeliver not init", K(ret), K(req));
  } else if (stop_
      || SS_STOPPING == GCTX.status_
      || SS_STOPPED == GCTX.status_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("receive request when server is stopping",
             K(req),
             K(ret));
  } else if (OB_FAIL(choose_queue_thread_(req, queue))) {
    LOG_WARN("choose_queue_thread_ failed", K(ret), KP(queue));
  } else if (NULL != queue) {
    LOG_DEBUG("deliver packet", K(queue));
    if (!queue->push(&req, MAX_RPC_QUEUE_LEN)) {
      ret = OB_QUEUE_OVERFLOW;
    }
  }


  if (OB_FAIL(ret)) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      SERVER_LOG(WARN, "can't deliver request", K(req), K(ret));
    }
    on_translate_fail(&req, ret);
  }
  SERVER_LOG(TRACE, "ObArbSrvDeliver deliver", K(ret));

  return ret;
}

int ObArbSrvDeliver::create_queue_thread_(const int tg_id,
                                          const char *thread_name,
                                          RpcQueueThread *&qthread,
                                          ObiReqQHandler *qhandler)
{
  int ret = OB_SUCCESS;
  qthread = OB_NEW(RpcQueueThread, ObModIds::OB_RPC, thread_name);
  if (OB_ISNULL(qthread)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), KP(qthread));
  } else if (OB_FAIL(qthread->init())) {
    LOG_WARN("init qthread failed", K(ret));
  } else {
    qthread->queue_.set_qhandler(qhandler);
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(qthread)) {
    ret = TG_SET_RUNNABLE_AND_START(tg_id, qthread->thread_);
  }
  return ret;
}

void ObArbSrvDeliver::destroy_queue_thread_(RpcQueueThread *&qthread)
{
  if (OB_NOT_NULL(qthread)) {
    OB_DELETE(RpcQueueThread, ObModIds::OB_RPC, qthread);
    qthread = NULL;
  }
}

int ObArbSrvDeliver::choose_queue_thread_(const ObRequest &request,
                                          ObReqQueue *&queue)
{
  int ret = OB_SUCCESS;
  const ObRpcPacket *packet
    = &reinterpret_cast<const ObRpcPacket&>(request.get_packet());
  const ObRpcPacketCode pcode = packet->get_pcode();
  if (is_server_pcode(pcode)) {
    queue = &server_rpc_queue_thread_->queue_;
  } else {
    queue = &normal_rpc_queue_thread_->queue_;
  }
  return ret;
}
} // end namespace arbserver
} // end namespace oceanbase
