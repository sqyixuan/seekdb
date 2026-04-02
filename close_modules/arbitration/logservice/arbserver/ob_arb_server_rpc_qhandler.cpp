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

#define USING_LOG_PREFIX RPC_FRAME

#include "ob_arb_server_rpc_qhandler.h"
#include "ob_arb_srv_xlator.h"
#include "logservice/arbserver/palf_env_lite_mgr.h"

using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::common;

namespace oceanbase
{
namespace arbserver
{
ObArbServerRpcQHandler::ObArbServerRpcQHandler()
    : translator_(NULL), palf_env_mgr_(NULL), is_inited_(false)
{
  // empty
}

ObArbServerRpcQHandler::~ObArbServerRpcQHandler()
{
  destroy();
}

int ObArbServerRpcQHandler::init(ObArbSrvRpcXlator *translator,
                                 palflite::PalfEnvLiteMgr *palf_env_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObArbServerRpcQHandler init failed", K(ret));
  } else if (OB_ISNULL(translator) || OB_ISNULL(palf_env_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(translator), KP(palf_env_mgr));
  } else {
    translator_ = translator;
    palf_env_mgr_ = palf_env_mgr;
    is_inited_ = true;
    LOG_INFO("ObArbServerRpcQHandler init success");
  }
  return ret;
}

void ObArbServerRpcQHandler::destroy()
{
  LOG_WARN_RET(OB_SUCCESS, "ObArbServerRpcQHandler destroy");
  is_inited_ = false;
  translator_ = NULL;
  palf_env_mgr_ = NULL;
}

int ObArbServerRpcQHandler::onThreadCreated(obsys::CThread *th)
{
  int ret = OB_SUCCESS;
  UNUSED(th);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArbServerRpcQHandler not init", K(ret));
  } else {
    LOG_INFO("new task thread create", KP(translator_));
    ret = translator_->th_init();
  }
  return ret;
}

int ObArbServerRpcQHandler::onThreadDestroy(obsys::CThread *th)
{
  int ret = OB_SUCCESS;
  UNUSED(th);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArbServerRpcQHandler not init", K(ret));
  } else {
    LOG_INFO("task thread destroy", KP(translator_));
    ret = translator_->th_destroy();
  }
  return ret;
}

bool ObArbServerRpcQHandler::handlePacketQueue(ObRequest *req, void */* arg */)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
      ret = OB_NOT_INIT; 
      LOG_WARN("ObArbServerRpcQHandler not init", K(ret));
  } else if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(req), K(ret));
  }

  ObReqProcessor *processor = NULL;
  const obrpc::ObRpcPacket *rpc_pkt = &reinterpret_cast<const obrpc::ObRpcPacket&>(req->get_packet());
  const int64_t cluster_id = rpc_pkt->get_src_cluster_id();
  const int64_t tenant_id = rpc_pkt->get_tenant_id();
  GET_CLUSTER_ID() = cluster_id;
  GET_ARB_TENANT_ID() = tenant_id;
  if (OB_SUCC(ret)) {
    ret = handlePacket_(processor, req);
  }

  // We just test processor is created correctly, but ignore the
  // returning code before.
  if (NULL == processor) {
    if (NULL != req) {
      on_translate_fail(req, ret);
    }
  } else {
    req->set_trace_point(ObRequest::OB_EASY_REQUEST_QHANDLER_PROCESSOR_RUN);
    if (OB_FAIL(processor->run())) {
      LOG_WARN("process rpc fail", K(ret));
    }
    translator_->release(processor);
  }

  
  LOG_TRACE("ObArbServerRpcQHandler handlePacketQueue success", K(ret), KP(rpc_pkt));
  return OB_SUCC(ret);
}

int ObArbServerRpcQHandler::handlePacket_(rpc::frame::ObReqProcessor *&processor,
                                          rpc::ObRequest *req)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(translator_->translate(*req, processor, palf_env_mgr_, NULL))) {
    LOG_WARN("translate reqeust fail", K(*req), K(ret));
    processor = NULL;
  }
  return ret;
}
}
} // end of namespace oceanbase
