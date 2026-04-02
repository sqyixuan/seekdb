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

#include "ob_arb_srv_xlator.h"
#include "logservice/palf/log_rpc_processor.h"
#include "logservice/arbserver/ob_arb_rpc_processor.h"

using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::palflite;

namespace oceanbase
{
namespace arbserver
{

void init_arb_srv_xlator_for_palfenv(ObArbSrvRpcXlator *xlator)
{
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogPushReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogPushRespP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogFetchReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogBatchFetchRespP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogPrepareReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogPrepareRespP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogChangeConfigMetaReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogChangeConfigMetaRespP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogChangeModeMetaReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogChangeModeMetaRespP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogNotifyRebuildReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogNotifyFetchLogReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::CommittedInfoP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogLearnerReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogRegisterParentReqP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogRegisterParentRespP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::ElectionPrepareRequestMsgP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::ElectionPrepareResponseMsgP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::ElectionAcceptRequestMsgP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::ElectionAcceptResponseMsgP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::ElectionChangeLeaderMsgP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::LogGetMCStP);
  //ARB_NORMAL_RPC_PROCESSOR(palf::ObRpcGetArbMemberInfoP);
}

void init_arb_srv_xlator_for_server(ObArbSrvRpcXlator *xlator)
{
  //ARB_MODIFY_META_RPC_PROCESSOR(arbserver::ObRpcCreateArbP);
  //ARB_MODIFY_META_RPC_PROCESSOR(arbserver::ObRpcDeleteArbP);
  //ARB_MODIFY_META_RPC_PROCESSOR(arbserver::ObRpcSetMemberListP);
  //ARB_MODIFY_META_RPC_PROCESSOR(arbserver::ObRpcGCNotifyP);
  //ARB_MODIFY_META_RPC_PROCESSOR(arbserver::ObRpcForceCleanArbClusterInfoP);
  //ARB_MODIFY_META_RPC_PROCESSOR(arbserver::ObRpcArbSetConfigP);
  //ARB_MODIFY_META_RPC_PROCESSOR(arbserver::ObRpcArbClusterOpP);
}

bool is_server_pcode(const ObRpcPacketCode pcode)
{
  return OB_CREATE_ARB == pcode || OB_SET_MEMBER_LIST == pcode || OB_DELETE_ARB == pcode
    || OB_ARB_GC_NOTIFY == pcode || OB_LOG_FORCE_CLEAR_ARB_CLUSTER_INFO == pcode
    || OB_SET_CONFIG == pcode || OB_ARB_CLUSTER_OP == pcode;
}

int ObArbSrvRpcXlator::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObArbSrvRpcXlator has inited twice", K(ret));
  } else {
    init_arb_srv_xlator_for_palfenv(this);
    init_arb_srv_xlator_for_server(this);
    is_inited_ = true;
    LOG_INFO("ObArbSrvRpcXlator success", K(ret));
  }
  return ret;
}

void ObArbSrvRpcXlator::destroy()
{
  is_inited_ = false;
  if (0 != ATOMIC_LOAD(&flying_cnt_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "ObArbSrvRpcXlator may be leaked", K(flying_cnt_));
  }
}

void ObArbSrvRpcXlator::register_rpc_process_function(int pcode, RPCProcessFunc func) {
  if(pcode >= MAX_PCODE || pcode < 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "(SHOULD NEVER HAPPEN) input pcode is out of range in server rpc xlator", K(pcode));
  } else if (funcs_[pcode] != nullptr) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "(SHOULD NEVER HAPPEN) duplicate pcode in server rpc xlator", K(pcode));
  } else {
    funcs_[pcode] = func;
    LOG_TRACE("arb server register_rpc_process_function", K(pcode));
  }
}

int ObArbSrvRpcXlator::translate(rpc::ObRequest &req,
                                 ObReqProcessor *&processor,
                                 void *args1,
                                 void *args2)
{
  int ret = OB_SUCCESS;
  processor = NULL;
  const ObRpcPacket &pkt = reinterpret_cast<const ObRpcPacket&>(req.get_packet());
  int pcode = pkt.get_pcode();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArbSrvRpcXlator not inited", K(ret), K(pcode), K(req));
  } else if (OB_UNLIKELY(pcode < 0 || pcode >= MAX_PCODE || funcs_[pcode] == nullptr)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support packet", K(pkt), K(ret), K(MAX_PCODE));
  } else if (OB_FAIL(funcs_[pcode](processor, session_handler_, args1, args2))) {
    LOG_WARN("acquire processor failed", K(ret), K(pcode));
  } else {
    ATOMIC_INC(&flying_cnt_);
    processor->set_ob_request(req);
    LOG_TRACE("ObArbSrvRpcXlator acquire processor success", K(flying_cnt_));
  }

  if (OB_SUCC(ret) && NULL == processor) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get processor failed", K(pcode), K(req));
  }

  if (!OB_SUCC(ret) && NULL != processor) {
    ob_delete(processor);
    processor = NULL;
  }
  return ret;
}

int ObArbSrvRpcXlator::release(ObReqProcessor *processor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArbSrvRpcXlator not inited", K(ret));
  } else if (NULL == processor) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(processor), K(ret));
  } else {
    processor->destroy();

    // task request is allocated when new task composed, then delete
    // here.
    ObRequest *req = const_cast<ObRequest*>(processor->get_ob_request());
    ObRequest::Type req_type = (ObRequest::Type)processor->get_req_type();
    ObRequest::TransportProto nio_protocol = (ObRequest::TransportProto)processor->get_nio_protocol();
    bool need_retry = processor->get_need_retry();
    bool async_resp_used = processor->get_async_resp_used();
    if (ObRequest::OB_RPC == req_type) {
      ob_delete(processor);
      processor = NULL;
      ATOMIC_DEC(&flying_cnt_);
    } else {
    }
    LOG_TRACE("ObArbSrvRpcXlator release processor success", K(flying_cnt_), K(req_type));
  }
  return ret;
}

} // end of namespace arbserver
} // end of namespace oceanbase
