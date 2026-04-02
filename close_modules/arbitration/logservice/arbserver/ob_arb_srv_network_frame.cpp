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
#include "ob_arb_srv_network_frame.h"
#include "share/ob_rpc_share.h"
#include "observer/ob_srv_network_frame.h"

namespace oceanbase
{
namespace arbserver 
{

using namespace oceanbase::rpc::frame;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share;

ObArbSrvNetworkFrame::ObArbSrvNetworkFrame()
    : rpc_xlator_(),
      normal_rpc_qhandler_(),
      server_rpc_qhandler_(),
      deliver_(normal_rpc_qhandler_),
      rpc_transport_(NULL),
      is_mittest_mode_(false),
      is_inited_(false)
{
  // empty;
}

ObArbSrvNetworkFrame::~ObArbSrvNetworkFrame()
{
  // empty
}

ObArbSrvNetworkFrame& ObArbSrvNetworkFrame::get_instance()
{
  static ObArbSrvNetworkFrame net_work_frame;
  return net_work_frame;
}

int ObArbSrvNetworkFrame::init(const ArbNetOptions &opts,
                               palflite::PalfEnvLiteMgr *palf_env_mgr)
{
  int ret = OB_SUCCESS;
  const uint32_t rpc_port = opts.rpc_port_;
  uint8_t negotiation_enable = opts.negotiation_enable_;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObArbSrvNetworkFrame init twice", K(ret));
  } else if (OB_FAIL(rpc_xlator_.init())) {
    LOG_ERROR("ObArbSrvRpcXlator init failed", K(ret));
  } else if (OB_FAIL(normal_rpc_qhandler_.init(&rpc_xlator_, palf_env_mgr))) {
    LOG_ERROR("ObArbNormalRpcQHandler init failed", K(ret));
  } else if (OB_FAIL(server_rpc_qhandler_.init(&rpc_xlator_, palf_env_mgr))) {
    LOG_ERROR("ObArbServerRpcQHandler init failed", K(ret));
  } else if (OB_FAIL(deliver_.init(opts.self_))) {
    LOG_ERROR("ObArbSrvDeliver init failed", K(ret), K(opts));
  } else if (OB_FAIL(ObSrvNetworkFrame::reload_rpc_auth_method())) {
    LOG_ERROR("reload_rpc_auth_method fail", K(ret));
  } else {
    share::set_obrpc_transport(rpc_transport_);
    LOG_INFO("init rpc network frame successfully", K(opts), KP(rpc_transport_));
    is_mittest_mode_ = opts.mittest_;
    is_inited_ = true;
  }
  return ret;
}

void ObArbSrvNetworkFrame::destroy()
{
  is_inited_ = false;
  stop();
  wait();
  deliver_.destroy();
  normal_rpc_qhandler_.destroy();
  server_rpc_qhandler_.destroy();
  rpc_xlator_.destroy();
  is_mittest_mode_ = false;
  LOG_WARN_RET(OB_SUCCESS, "ObArbSrvNetworkFrame destroy");
}

int ObArbSrvNetworkFrame::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deliver_.start(normal_rpc_qhandler_, server_rpc_qhandler_))) {
    LOG_WARN("ObArbSrvDeliver start failed", K(ret));
  } else {
    LOG_INFO("ObArbSrvNetworkFrame start success");
  }
  return ret;
}

void ObArbSrvNetworkFrame::wait()
{
}

int ObArbSrvNetworkFrame::stop()
{
  int ret = OB_SUCCESS;
  deliver_.stop();
  return ret;
}

int ObArbSrvNetworkFrame::get_proxy(obrpc::ObRpcProxy &proxy)
{
  return proxy.init(rpc_transport_);
}

ObReqTransport *ObArbSrvNetworkFrame::get_req_transport()
{
  return rpc_transport_;
}

}
}
