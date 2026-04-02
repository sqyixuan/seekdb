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

#include "ob_log_rebuild_cb_adapter.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace logservice
{

ObLogRebuildCbAdapter::ObLogRebuildCbAdapter()
    : is_inited_(false),
      lock_(),
      self_(),
      palf_id_(palf::INVALID_PALF_ID),
      storage_rebuild_cb_(NULL),
      fast_rebuild_engine_(NULL),
      rpc_proxy_(NULL),
      lc_cb_(NULL) { }

int ObLogRebuildCbAdapter::init(const common::ObAddr &self,
                                const int64_t palf_id,
                                ObLogFastRebuildEngine *fast_rebuild_engine,
                                obrpc::ObLogServiceRpcProxy *rpc_proxy,
                                palf::PalfLocationCacheCb *lc_cb)
{
  int ret = OB_SUCCESS;
  if (false == self.is_valid() ||
      false == palf::is_valid_palf_id(palf_id) ||
      OB_ISNULL(fast_rebuild_engine) ||
      OB_ISNULL(rpc_proxy) ||
      OB_ISNULL(lc_cb)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(self), K(palf_id), KP(fast_rebuild_engine),
        KP(rpc_proxy), KP(lc_cb));
  } else {
    self_ = self;
    palf_id_ = palf_id;
    fast_rebuild_engine_ = fast_rebuild_engine;
    rpc_proxy_ = rpc_proxy;
    lc_cb_ = lc_cb;
    is_inited_ = true;
  }
  return ret;
}

void ObLogRebuildCbAdapter::destroy()
{
  common::ObSpinLockGuard guard(lock_);
  is_inited_ = false;
  self_.reset();
  palf_id_ = palf::INVALID_PALF_ID;
  storage_rebuild_cb_ = NULL;
  fast_rebuild_engine_ = NULL;
  rpc_proxy_ = NULL;
  lc_cb_ = NULL;
}

int ObLogRebuildCbAdapter::register_rebuild_cb(palf::PalfRebuildCb *rebuild_cb)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(rebuild_cb)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(rebuild_cb));
  } else if (OB_NOT_NULL(storage_rebuild_cb_)) {
    ret = OB_OP_NOT_ALLOW;
    CLOG_LOG(WARN, "can not register rebuild_cb repeatedly", KP(rebuild_cb));
  } else {
    storage_rebuild_cb_ = rebuild_cb;
  }
  return ret;
}

int ObLogRebuildCbAdapter::unregister_rebuild_cb()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(storage_rebuild_cb_)) {
  } else {
    storage_rebuild_cb_ = NULL;
  }
  return ret;
}

int ObLogRebuildCbAdapter::on_rebuild(const int64_t id,
                                      const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  const bool enable_shared_storage = GCTX.is_shared_storage_mode();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRebuildCbAdapter not init");
  } else if (false == enable_shared_storage) {
    if (OB_FAIL(storage_rebuild_cb_->on_rebuild(palf_id_, lsn))) {
      CLOG_LOG(WARN, "on_rebuild failed", K_(palf_id), K_(self), K(lsn));
    } else { }
  } else {
    palf::LSN end_lsn;
    palf::PalfHandleGuard guard;
    common::ObAddr leader;
    ObLogService *log_srv = MTL(ObLogService*);
    if (OB_ISNULL(log_srv)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "ObLogService is NULL", K(ret), K_(palf_id));
    } else if (OB_FAIL(log_srv->open_palf(share::ObLSID(palf_id_), guard))) {
      CLOG_LOG(WARN, "open_palf failed", K(ret), K_(palf_id));
    } else if (OB_FAIL(guard.get_election_leader(leader)) &&
        OB_FAIL(lc_cb_->nonblock_get_leader(palf_id_, leader))) {
      CLOG_LOG(WARN, "get_leader failed", K(ret), K_(palf_id));
      // get election leader for rebuilding replica when there is not leader in the PALF layer
      if (REACH_TIME_INTERVAL(500 * 1000)) {
        lc_cb_->nonblock_renew_leader(palf_id_);
      }
    } else if (OB_FAIL(guard.get_end_lsn(end_lsn))) {
      CLOG_LOG(WARN, "get_end_lsn failed", K(ret), K_(palf_id));
    } else {
      // send a async RPC to get PalfBaseInfo from the leader
      const int64_t CONN_TIMEOUT_US = GCONF.rpc_timeout;
      LogAcquireRebuildInfoMsg req(self_, palf_id_, end_lsn);
      if (OB_FAIL(rpc_proxy_->to(leader).timeout(CONN_TIMEOUT_US).trace_time(true).
          max_process_handler_time(CONN_TIMEOUT_US).by(MTL_ID()).acquire_log_rebuild_info(req, NULL))) {
        CLOG_LOG(WARN, "acquire_log_rebuild_info failed", K(ret), K_(palf_id), K(leader), K(req));
      } else {
        CLOG_LOG(INFO, "acquire_log_rebuild_info success", K(ret), K(req));
      }
    }
  }
  return ret;
}

bool ObLogRebuildCbAdapter::is_rebuilding(const int64_t id) const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    const bool is_fast_rebuilding = fast_rebuild_engine_->is_fast_rebuilding(palf_id_);
    const bool is_storage_rebuilding = OB_NOT_NULL(storage_rebuild_cb_) && \
        storage_rebuild_cb_->is_rebuilding(palf_id_);
    bool_ret = is_fast_rebuilding || is_storage_rebuilding;
  }
  return bool_ret;
}

int ObLogRebuildCbAdapter::handle_acquire_log_rebuild_info_resp(const LogAcquireRebuildInfoMsg &resp)
{
  int ret = OB_SUCCESS;
  palf::LSN max_lsn, end_lsn;
  palf::PalfHandleGuard palf_guard;
  ObLogService *log_srv = MTL(ObLogService*);
  const common::ObAddr &server = resp.src_;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT || OB_ISNULL(storage_rebuild_cb_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRebuildCbAdapter not init", K_(is_inited), KP_(storage_rebuild_cb));
  } else if (false == resp.is_valid() || true == resp.is_req()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(resp));
  } else if (OB_ISNULL(log_srv)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ObLogService is NULL", K(ret), K_(palf_id));
  } else if (OB_FAIL(log_srv->open_palf(share::ObLSID(palf_id_), palf_guard))) {
    CLOG_LOG(WARN, "open_palf failed", K(ret), K_(palf_id));
  } else if (OB_FAIL(palf_guard.get_max_lsn(max_lsn))) {
    CLOG_LOG(WARN, "get_max_lsn failed", K(ret), K_(palf_id));
  } else if (OB_FAIL(palf_guard.get_end_lsn(end_lsn))) {
    CLOG_LOG(WARN, "get_end_lsn failed", K(ret), K_(palf_id));
  } else if (end_lsn != resp.rebuild_replica_end_lsn_) {
    CLOG_LOG(INFO, "ignore stale msg", K(ret), K_(palf_id), K(resp), K(end_lsn));
  } else if (FULL_REBUILD == resp.type_) {
    if (true == fast_rebuild_engine_->is_fast_rebuilding(palf_id_)) {
      CLOG_LOG(WARN, "rebuilding, do not rebuild logs", K_(palf_id), K_(self), K(resp));
    } else if (OB_FAIL(storage_rebuild_cb_->on_rebuild(palf_id_, resp.base_info_.curr_lsn_))) {
      CLOG_LOG(WARN, "on_rebuild failed", K_(palf_id), K_(self), K(resp));
    } else {
      CLOG_LOG(INFO, "on_rebuild success", K_(palf_id), K_(self), K(resp));
    }
  } else if (FAST_REBUILD == resp.type_) {
    int64_t palf_epoch = -1;
    if (storage_rebuild_cb_->is_rebuilding(palf_id_) &&
        true == palf_guard.is_vote_enabled()) {
      CLOG_LOG(INFO, "rebuilding, do not fast_rebuild logs", K(ret), K(server), K(resp));
    } else if (OB_FAIL(palf_guard.get_palf_epoch(palf_epoch))) {
      CLOG_LOG(WARN, "get_palf_epoch failed", K(ret), K(palf_epoch));
    } else if (OB_FAIL(fast_rebuild_engine_->on_fast_rebuild_log(palf_id_,
        palf_epoch, server, resp.base_info_, max_lsn, end_lsn))) {
      CLOG_LOG(WARN, "on_fast_rebuild_log failed", K(ret), K_(self), K_(palf_id), K(resp));
    } else {
      CLOG_LOG(INFO, "on_fast_rebuild_log success", K(ret), K_(self), K_(palf_id), K(resp),
          K(max_lsn), K(end_lsn));
    }
  }
  return ret;
}
} // namespace palf
} // namespace oceanbase
