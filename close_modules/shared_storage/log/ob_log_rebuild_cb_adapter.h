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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_REBUILD_CB_ADAPTER_
#define OCEANBASE_LOGSERVICE_OB_LOG_REBUILD_CB_ADAPTER_
#include "lib/lock/ob_spin_lock.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_base_info.h"
#include "ob_log_fast_rebuild_engine.h"
#include "share/ob_ls_id.h"
#include "logservice/logrpc/ob_log_rpc_req.h"

namespace oceanbase
{
namespace obrpc
{
class ObLogServiceRpcProxy;
}
// struct LogAcquireRebuildInfoMsg;
namespace logservice
{

class ObLogRebuildCbAdapter : public palf::PalfRebuildCb
{
public:
  ObLogRebuildCbAdapter();
  ~ObLogRebuildCbAdapter() { destroy(); }
  void destroy();
public:
  int init(const common::ObAddr &self,
           const int64_t palf_id,
           ObLogFastRebuildEngine *fast_rebuild_engine,
           obrpc::ObLogServiceRpcProxy *rpc_proxy,
           palf::PalfLocationCacheCb *lc_cb);
  int on_rebuild(const int64_t id,
                 const palf::LSN &lsn) override final;
  bool is_rebuilding(const int64_t id) const override final;
  int register_rebuild_cb(palf::PalfRebuildCb *rebuild_cb);
  int unregister_rebuild_cb();
  int handle_acquire_log_rebuild_info_resp(const LogAcquireRebuildInfoMsg &resp);
  TO_STRING_KV(K_(self), K_(palf_id));
private:
  bool is_inited_;
  mutable common::ObSpinLock lock_;
  common::ObAddr self_;
  int64_t palf_id_;
  palf::PalfRebuildCb *storage_rebuild_cb_;
  logservice::ObLogFastRebuildEngine *fast_rebuild_engine_;
  obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  palf::PalfLocationCacheCb *lc_cb_;
};
} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_REBUILD_CB_ADAPTER_
