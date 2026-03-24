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
 
#ifndef _OCEABASE_OBSERVER_OB_ARB_SRV_NETWORK_FRAME_H_
#define _OCEABASE_OBSERVER_OB_ARB_SRV_NETWORK_FRAME_H_

#include "rpc/frame/ob_net_easy.h"
#include "rpc/frame/ob_req_handler.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "ob_arb_srv_xlator.h"
#include "ob_arb_srv_deliver.h"
#include "ob_arb_normal_rpc_qhandler.h"
#include "ob_arb_server_rpc_qhandler.h"

namespace oceanbase
{
namespace palflite
{
class PalfEnvLiteMgr;
}
namespace arbserver
{

struct ArbNetOptions
{
  ArbNetOptions() : opts_(), rpc_port_(0), negotiation_enable_(0), mittest_(false) {}
  rpc::frame::ObNetOptions opts_;
  uint32_t rpc_port_;
  uint8_t negotiation_enable_;
  bool mittest_;
  common::ObAddr self_;
  TO_STRING_KV(K_(opts), K_(rpc_port), K_(negotiation_enable), K_(mittest), K_(self));
};

class ObArbSrvNetworkFrame
{
public:
  ObArbSrvNetworkFrame();

  virtual ~ObArbSrvNetworkFrame();
  
  static ObArbSrvNetworkFrame& get_instance();
public:
  int init(const ArbNetOptions &opts,
           palflite::PalfEnvLiteMgr *palf_env_mgr);
  void destroy();
  int start();
  void wait();
  int stop();
  int get_proxy(obrpc::ObRpcProxy &proxy);
  rpc::frame::ObReqTransport *get_req_transport();
private:
  // Message to processor conversion
  arbserver::ObArbSrvRpcXlator rpc_xlator_;
  // Background thread processing of req
  arbserver::ObArbNormalRpcQHandler normal_rpc_qhandler_;
  arbserver::ObArbServerRpcQHandler server_rpc_qhandler_;
  // easy thread and ob interaction
  arbserver::ObArbSrvDeliver deliver_;
  // easy message to ob format conversion

  rpc::frame::ObReqTransport *rpc_transport_;
  bool is_mittest_mode_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObArbSrvNetworkFrame);
}; // end of class ObSrvNetworkFrame

} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_ARB_SRV_NETWORK_FRAME_H_ */
