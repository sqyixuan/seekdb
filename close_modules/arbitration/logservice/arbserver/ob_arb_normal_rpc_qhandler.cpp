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

#include "ob_arb_normal_rpc_qhandler.h"
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
ObArbNormalRpcQHandler::ObArbNormalRpcQHandler()
    : translator_(NULL), palf_env_mgr_(NULL), filter_(NULL), is_inited_(false)
{
  // empty
}

ObArbNormalRpcQHandler::~ObArbNormalRpcQHandler()
{
  destroy();
}

int ObArbNormalRpcQHandler::init(ObArbSrvRpcXlator *translator,
                                 palflite::PalfEnvLiteMgr *palf_env_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObArbNormalRpcQHandler init failed", K(ret));
  } else if (OB_ISNULL(translator) || OB_ISNULL(palf_env_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(translator), KP(palf_env_mgr));
  } else {
    translator_ = translator;
    palf_env_mgr_ = palf_env_mgr;
    is_inited_ = true;
    LOG_INFO("ObArbNormalRpcQHandler init success");
  }
  return ret;
}

void ObArbNormalRpcQHandler::destroy()
{
  LOG_WARN_RET(OB_SUCCESS, "ObArbNormalRpcQHandler destroy");
  is_inited_ = false;
  translator_ = NULL;
  palf_env_mgr_ = NULL;
  filter_ = NULL;
}

int ObArbNormalRpcQHandler::onThreadCreated(obsys::CThread *th)
{
  int ret = OB_SUCCESS;
  UNUSED(th);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArbNormalRpcQHandler not init", K(ret));
  } else {
    LOG_INFO("new task thread create", KP(translator_));
    ret = translator_->th_init();
  }
  return ret;
}

int ObArbNormalRpcQHandler::onThreadDestroy(obsys::CThread *th)
{
  int ret = OB_SUCCESS;
  UNUSED(th);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArbNormalRpcQHandler not init", K(ret));
  } else {
    LOG_INFO("task thread destroy", KP(translator_));
    ret = translator_->th_destroy();
  }
  return ret;
}

bool ObArbNormalRpcQHandler::handlePacketQueue(ObRequest *req, void */* arg */)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
      ret = OB_NOT_INIT; 
      LOG_WARN("ObArbNormalRpcQHandler not init", K(ret));
  } else if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(req), K(ret));
  }
  palflite::PalfEnvLite *palf_env_lite = NULL;
  ObReqProcessor *processor = NULL;
  const obrpc::ObRpcPacket *rpc_pkt = &reinterpret_cast<const obrpc::ObRpcPacket&>(req->get_packet());
  if (OB_SUCC(ret)) {
    ret = handlePacket_(processor, req, palf_env_lite);
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

  if (NULL != palf_env_lite) {
    palf_env_mgr_->revert_palf_env_lite(palf_env_lite);
    palf_env_lite = NULL;
  }

  LOG_TRACE("ObArbNormalRpcQHandler handlePacketQueue success", K(ret), KP(rpc_pkt));
  return OB_SUCC(ret);
}

int ObArbNormalRpcQHandler::handlePacket_(rpc::frame::ObReqProcessor *&processor,
                                          rpc::ObRequest *req,
                                          palflite::PalfEnvLite *&palf_env_lite)
{
  int ret = OB_SUCCESS;
  palf_env_lite = NULL;
  palf::IPalfEnvImpl *palf_env_impl = NULL;
  const obrpc::ObRpcPacket *rpc_pkt = &reinterpret_cast<const obrpc::ObRpcPacket&>(req->get_packet());
  const int64_t cluster_id = rpc_pkt->get_src_cluster_id();
  const int64_t tenant_id = rpc_pkt->get_tenant_id();
  GET_CLUSTER_ID() = cluster_id;
  GET_ARB_TENANT_ID() = tenant_id;
  palflite::PalfEnvKey key(cluster_id, tenant_id);
  if (OB_FAIL(palf_env_mgr_->get_palf_env_lite(key, palf_env_lite))) {
    ret = OB_TENANT_NOT_IN_SERVER;
    LOG_WARN("get_palf_env_lite failed", KR(ret), K(key), KP(palf_env_impl), KPC(rpc_pkt));
  } else {
    palf_env_impl = palf_env_lite;
    if (OB_FAIL(translator_->translate(*req, processor, palf_env_impl, filter_))) {
      LOG_WARN("translate reqeust fail", K(*req), K(ret));
      processor = NULL;
    }
  } 
  return ret;
}
}
} // end of namespace oceanbase
