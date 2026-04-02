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
#ifndef OCEANBASE_ARB_SRV_RPC_PROCESSOR_H
#define OCEANBASE_ARB_SRV_RPC_PROCESSOR_H
#include "share/ob_srv_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace palflite
{
class PalfEnvLiteMgr;
}
namespace arbserver
{
#define OB_ARB_DEFINE_PROCESSOR(cls, pcode, pname)                          \
  class pname : public obrpc::ObRpcProcessor<                           \
    obrpc::Ob ## cls ## RpcProxy::ObRpc<pcode> >

#define OB_ARB_DEFINE_PROCESSOR_S(cls, pcode, pname)                        \
  OB_ARB_DEFINE_PROCESSOR(cls, obrpc::pcode, pname)                         \
  {                                                                     \
  public:                                                               \
    pname() : palf_env_mgr_(NULL)                                       \
    {}                                                                  \
    void set_palf_env_mgr(palflite::PalfEnvLiteMgr *palf_env_mgr)       \
    { palf_env_mgr_ = palf_env_mgr; }                                   \
  protected: int process();                                             \
  private:                                                              \
    palflite::PalfEnvLiteMgr *palf_env_mgr_;                            \
  }

OB_ARB_DEFINE_PROCESSOR_S(Srv, OB_CREATE_ARB, ObRpcCreateArbP);
OB_ARB_DEFINE_PROCESSOR_S(Srv, OB_DELETE_ARB, ObRpcDeleteArbP);
OB_ARB_DEFINE_PROCESSOR_S(Srv, OB_SET_MEMBER_LIST, ObRpcSetMemberListP);
OB_ARB_DEFINE_PROCESSOR_S(Srv, OB_ARB_GC_NOTIFY, ObRpcGCNotifyP);
OB_ARB_DEFINE_PROCESSOR_S(Srv, OB_LOG_FORCE_CLEAR_ARB_CLUSTER_INFO, ObRpcForceCleanArbClusterInfoP);
OB_ARB_DEFINE_PROCESSOR_S(Srv, OB_SET_CONFIG, ObRpcArbSetConfigP);
OB_ARB_DEFINE_PROCESSOR_S(Srv, OB_ARB_CLUSTER_OP, ObRpcArbClusterOpP);
} // end namespace arbserver
} // end namespace oceanbase
#endif
