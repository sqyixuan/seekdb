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

#ifndef _OCEABASE_ARBSERVER_SERVER_RPC_QHANDLER_H_
#define _OCEABASE_ARBSERVER_SERVER_RPC_QHANDLER_H_

#include "rpc/frame/obi_req_qhandler.h"

namespace oceanbase
{
namespace obrpc
{
class ObRpcPacket;
}
namespace palflite
{
class PalfEnvLiteMgr;
}
namespace palf
{
class IPalfEnvImpl;
}
namespace rpc
{
class ObRequest;
namespace frame
{
class ObReqTranslator;
class ObReqProcessor;
} // end of namespace rpc
} // end of namespace fram
namespace arbserver
{
class ObArbSrvRpcXlator;
class ObArbServerRpcQHandler
    : public rpc::frame::ObiReqQHandler
{
public:
  ObArbServerRpcQHandler();
  virtual ~ObArbServerRpcQHandler();
  int init(ObArbSrvRpcXlator *translator,
           palflite::PalfEnvLiteMgr *palf_env_mgr);
  void destroy();
  // invoke when queue thread created.
  int onThreadCreated(obsys::CThread *) override final;
  int onThreadDestroy(obsys::CThread *) override final;

  bool handlePacketQueue(rpc::ObRequest *req, void *args) override final;

private:
  int handlePacket_(rpc::frame::ObReqProcessor *&processor,
                    rpc::ObRequest *req);
private:
  ObArbSrvRpcXlator *translator_;
  palflite::PalfEnvLiteMgr *palf_env_mgr_;
  bool is_inited_;
}; // end of class ObArbServerRpcQHandler

} // end of namespace arbserver
} // end of namespace oceanbase

#endif /* _OCEABASE_ARBSERVER_SERVER_RPC_QHANDLER_H_ */
