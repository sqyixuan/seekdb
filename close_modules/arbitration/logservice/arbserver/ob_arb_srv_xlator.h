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

#ifndef _OCEABASE_OBSERVER_OB_ARB_SRV_XLATOR_H_
#define _OCEABASE_OBSERVER_OB_ARB_SRV_XLATOR_H_

#include "lib/utility/ob_macro_utils.h"
#include "rpc/frame/ob_req_translator.h"
#include "rpc/obrpc/ob_rpc_translator.h"
#include "rpc/obrpc/ob_rpc_packet.h"
namespace oceanbase
{

using rpc::frame::ObReqProcessor;
using obrpc::ObRpcTranslator;
using obrpc::ObRpcSessionHandler;

namespace palf
{
class IPalfEnvImpl;
}
namespace palflite
{
class PalfEnvLiteMgr;
}

namespace arbserver
{
#define ARB_MODIFY_META_RPC_PROCESSOR(ObRpcP, ...)                             \
xlator->register_rpc_process_function(ObRpcP::PCODE, []                        \
    (ObReqProcessor *&processor,                                               \
     ObRpcSessionHandler &handler_,                                            \
     void *args1,                                                              \
     void *args2)                                                              \
{                                                                              \
  int ret = OB_SUCCESS;                                                        \
  ObRpcP *p = OB_NEW(ObRpcP, "ArbRPC", __VA_ARGS__);                           \
  palflite::PalfEnvLiteMgr *palf_env_mgr                                       \
    = reinterpret_cast<palflite::PalfEnvLiteMgr*>(args1);                      \
  if (NULL == p) {                                                             \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                           \
  } else if (OB_FAIL(p->init())) {                                             \
    SERVER_LOG(ERROR, "Init " #ObRpcP "fail", K(ret));                         \
    worker_allocator_delete(p);                                                \
    p = NULL;                                                                  \
  } else {                                                                     \
    p->set_session_handler(handler_);                                          \
    p->set_palf_env_mgr(palf_env_mgr);                                         \
    UNUSED(args2);                                                             \
    processor = p;                                                             \
    SERVER_LOG(TRACE, "arbserver processor set success", KP(p));               \
  }                                                                            \
  return ret;                                                                  \
});

#define ARB_NORMAL_RPC_PROCESSOR(ObRpcP, ...)                                  \
xlator->register_rpc_process_function(ObRpcP::PCODE, []                        \
    (ObReqProcessor *&processor,                                               \
     ObRpcSessionHandler &handler_,                                            \
     void *args1,                                                              \
     void *args2)                                                              \
{                                                                              \
  int ret = OB_SUCCESS;                                                        \
  palf::IPalfEnvImpl *palf_env_impl =                                          \
    reinterpret_cast<palf::IPalfEnvImpl*>(args1);                              \
  ObRpcP *p = OB_NEW(ObRpcP, "ArbRPC", __VA_ARGS__);                           \
  if (NULL == p) {                                                             \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                           \
  } else if (OB_FAIL(p->init())) {                                             \
    SERVER_LOG(ERROR, "Init " #ObRpcP "fail", K(ret));                         \
    worker_allocator_delete(p);                                                \
    p = NULL;                                                                  \
  } else {                                                                     \
    p->set_session_handler(handler_);                                          \
    p->set_palf_env_impl(palf_env_impl);                                       \
    p->set_filter(args2);                                                      \
    processor = p;                                                             \
    SERVER_LOG(TRACE, "arbserver processor set success", KP(p));               \
  }                                                                            \
  return ret;                                                                  \
});

template <typename T>
void worker_allocator_delete(T *&ptr)
{
  if (NULL != ptr) {
    OB_DELETE(T, "ArbRPC", ptr);
    ptr = NULL;
  }
}

using RPCProcessFunc = int(*)(ObReqProcessor *&processor,
                              ObRpcSessionHandler &handler_,
                              void *args1,
                              void *args2);

class ObArbSrvRpcXlator;

void init_arb_srv_xlator_for_palfenv(ObArbSrvRpcXlator *xlator);
void init_arb_srv_xlator_for_server(ObArbSrvRpcXlator *xlator);
bool is_server_pcode(const obrpc::ObRpcPacketCode pcode);

class ObArbSrvRpcXlator
    : public ObRpcTranslator
{
public:
  ObArbSrvRpcXlator()  : flying_cnt_(0), is_inited_(false)
  {
    memset(funcs_, 0, sizeof(funcs_));
  }

  int init();

  void destroy();

  void register_rpc_process_function(int pcode, RPCProcessFunc func);

  int translate(rpc::ObRequest &req,
                ObReqProcessor *&processor,
                void *args1,
                void *args2);
  int release(ObReqProcessor *processor) override final;

protected:
  ObReqProcessor *get_processor(rpc::ObRequest &) { return NULL; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObArbSrvRpcXlator);
  RPCProcessFunc funcs_[0];
  int64_t flying_cnt_;
  bool is_inited_;
}; // end of class ObArbSrvRpcXlator

} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_SRV_RPC_XLATOR_H_ */
