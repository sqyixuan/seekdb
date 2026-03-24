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

#define USING_LOG_PREFIX CLOG
#include "ob_arbitration_service.h"
#include "logservice/palf/palf_env.h"
#include "logservice/logrpc/ob_log_rpc_proxy.h"                // RpcProxy
#include "logservice/ob_net_keepalive_adapter.h"    // IObNetKeepAliveAdapter
#include "logservice/leader_coordinator/ob_failure_detector.h" // ObFailureDetector
#include "logservice/ob_log_monitor.h"              // record_degrade_event

namespace oceanbase
{
using namespace common;
using namespace palf;
namespace logservice
{

// Note: get servers whose explicit status are ALIVE
bool is_all_server_alive_(ServerProbeService *probe_srv,
                          const LogMemberStatusList &servers,
                          const int64_t palf_id)
{
  bool bool_ret = true;
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < servers.count(); i++) {
    const common::ObAddr &server = servers.at(i).member_.get_server();
    if (false == probe_srv->is_server_normal(server, palf_id)) {
      bool_ret = false;
      break;
    }
  }
  return bool_ret;
}

// Note: get servers whose explicit status are DEAD, do not include UNKNOWN
int get_dead_servers_(
    ServerProbeService *probe_srv,
    const LogMemberStatusList &servers,
    const int64_t palf_id,
    LogMemberStatusList &dead_servers)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < servers.count(); i++) {
    const LogMemberStatus &server = servers.at(i);
    const common::ObAddr &server_addr = servers.at(i).member_.get_server();
    LogReplicaStatus abnormal_reason;
    if (false == probe_srv->is_server_normal(server_addr, palf_id, abnormal_reason) &&
        abnormal_reason != LogReplicaStatus::UNKNOWN) {
      dead_servers.push_back(LogMemberStatus(server, abnormal_reason));
    }
  }
  return ret;
}

ObArbitrationService::ObArbitrationService()
    : self_(),
      probe_srv_(),
      may_be_degraded_palfs_(),
      may_be_upgraded_palfs_(),
      arb_timeout_us_(0),
      follower_last_probe_time_us_(OB_INVALID_TIMESTAMP),
      palf_env_(NULL),
      rpc_proxy_(NULL),
      monitor_(NULL),
      location_adapter_(NULL),
      is_inited_(false)
  {}

ObArbitrationService::~ObArbitrationService()
{
  destroy();
}

void ObArbitrationService::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    stop();
    wait();
    probe_srv_.destroy();
    may_be_degraded_palfs_.destroy();
    may_be_upgraded_palfs_.destroy();
    palf_env_ = NULL;
    rpc_proxy_ = NULL;
    monitor_ = NULL;
    location_adapter_ = NULL;
  }
}

int ObArbitrationService::init(const common::ObAddr &self,
                               palf::PalfEnv *palf_env,
                               obrpc::ObLogServiceRpcProxy *rpc_proxy,
                               IObNetKeepAliveAdapter *net_keepalive,
                               IObArbitrationMonitor *monitor,
                               ObLocationAdapter *location_adapter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObArbitrationService has been inited", K(ret));
  } else if (false == self.is_valid() || OB_ISNULL(palf_env) || OB_ISNULL(rpc_proxy) ||
             OB_ISNULL(net_keepalive) || OB_ISNULL(monitor) || OB_ISNULL(location_adapter)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(self), KP(palf_env),
        KP(rpc_proxy), KP(net_keepalive), KP(monitor), KP(location_adapter));
  } else if (OB_FAIL(probe_srv_.init(self, rpc_proxy, net_keepalive, palf_env))) {
    CLOG_LOG(WARN, "probe_srv_ init failed", K(ret));
  } else if (OB_FAIL(may_be_degraded_palfs_.init("LogArbSrv", MTL_ID()))) {
    CLOG_LOG(WARN, "may_be_degraded_palfs_ init failed", K(ret));
  } else if (OB_FAIL(may_be_upgraded_palfs_.init("LogArbSrv", MTL_ID()))) {
    CLOG_LOG(WARN, "may_be_upgraded_palfs_ init failed", K(ret));
  } else {
    self_ = self;
    palf_env_ = palf_env;
    rpc_proxy_ = rpc_proxy;
    monitor_ = monitor;
    location_adapter_ = location_adapter;
    share::ObThreadPool::set_run_wrapper(MTL_CTX());
    is_inited_ = true;
  }

  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }
  CLOG_LOG(INFO, "ObArbitrationService init finished", K(ret), K(self));
  return ret;
}

int ObArbitrationService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    CLOG_LOG(ERROR, "ObArbitrationService failed to start");
  } else {
    CLOG_LOG(INFO, "ObArbitrationService start success", K(ret));
  }
  return ret;
}

void ObArbitrationService::stop()
{
  share::ObThreadPool::stop();
  share::ObThreadPool::wait();
  probe_srv_.stop();
}

void ObArbitrationService::wait()
{
  probe_srv_.wait();
}

void ObArbitrationService::run1()
{
  ObDIActionGuard("LogService", "LogArbitrationService", "run_loop");
  lib::set_thread_name("LogArb");
  run_loop_();
}

void ObArbitrationService::start_probe_server_(const common::ObAddr &server, const int64_t palf_id)
{
  (void) probe_srv_.start_probe_server(server, palf_id);
}

int ObArbitrationService::start_probe_servers_(const LogMemberStatusList &servers, const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < servers.count(); i++) {
    const common::ObMember &member = servers.at(i).member_;
    start_probe_server_(member.get_server(), palf_id);
  }
  return ret;
}

int ObArbitrationService::start_probe_servers_(const common::ObMemberList &servers, const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < servers.get_member_number(); i++) {
    common::ObAddr server;
    if (OB_FAIL(servers.get_server_by_index(i, server))) {
      CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K(server));
    } else {
      start_probe_server_(server, palf_id);
    }
  }
  return ret;
}

int ObArbitrationService::follower_probe_others_(const int64_t palf_id, const common::ObMemberList &paxos_member_list)
{
  int ret = OB_SUCCESS;
  const int64_t follower_probe_interval_us = arb_timeout_us_ / 2;
  const int64_t curr_time_us = ObTimeUtility::current_time();
  if (!paxos_member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!paxos_member_list.contains(self_)) {
    // self is not in paxos_member_list, skip
  } else if (follower_probe_interval_us <= 0) {
    // arb_timeout_us_ has not been updated, skip
  } else if (curr_time_us - follower_last_probe_time_us_ <= follower_probe_interval_us) {
    // not reach probe interval, skip
  } else {
    for (int i = 0; OB_SUCC(ret) && i < paxos_member_list.get_member_number(); i++) {
      common::ObAddr server;
      if (OB_FAIL(paxos_member_list.get_server_by_index(i, server))) {
        CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K(server));
      } else {
        (void) start_probe_server_(server, palf_id);
      }
    }
    follower_last_probe_time_us_ = curr_time_us;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      CLOG_LOG(INFO, "follower_probe_others_ finished", K(ret), K(palf_id), K(paxos_member_list));
    }
  }
  return ret;
}

int ObArbitrationService::get_server_sync_info_()
{
  int ret = OB_SUCCESS;
  common::ObFunction<int(const PalfHandle&)> get_palf_sync_info =
  [&](const PalfHandle &palf_handle)
  {
    int ret = OB_SUCCESS;
    LogMemberAckInfoList ack_info_array;
    common::ObMemberList paxos_member_list;
    common::GlobalLearnerList degraded_list;
    int64_t palf_id, replica_num;
    if (OB_FAIL(palf_handle.get_palf_id(palf_id))) {
      CLOG_LOG(WARN, "get_palf_id failed", K(ret), K(palf_handle));
    } else if (OB_FAIL(palf_handle.get_paxos_member_list(paxos_member_list, replica_num))) {
      CLOG_LOG(WARN, "get_palf_id failed", K(ret), K(palf_id));
    } else if (OB_FAIL(palf_handle.get_ack_info_array(ack_info_array, degraded_list))) {
      if (OB_NOT_MASTER == ret) {
        ret = OB_SUCCESS;
        // If self is not leader, try probe other members to speed up later possible
        // degrade procedure when it takeovers as leader.
        (void) follower_probe_others_(palf_id, paxos_member_list);
      } else {
        CLOG_LOG(WARN, "get_ack_info_array failed", K(ret), K(palf_id), K(palf_handle));
      }
    } else {
      CLOG_LOG(TRACE, "get_ack_info_array", K(ret), K(palf_id), K_(self), K(ack_info_array), K(degraded_list));
      // 1. record may_be_degraded_servers and may_be_upgraded_servers according to ack_info
      LogMemberStatusList may_be_degraded_servers, may_be_upgraded_servers, unused_list;
      int64_t curr_time_us = ObTimeUtility::current_time();
      for (int i = 0; i < ack_info_array.count(); i++) {
        int tmp_ret = OB_SUCCESS;
        LogReplicaStatus status = LogReplicaStatus::LOG_NORMAL;
        const common::ObMember &member = ack_info_array.at(i).member_;
        const int64_t last_ack_time_us = ack_info_array.at(i).last_ack_time_us_;
        const LSN last_flushed_end_lsn = ack_info_array.at(i).last_flushed_end_lsn_;
        const bool is_degraded_member = degraded_list.contains(member.get_server());
        int64_t last_probe_normal_time_us = OB_INVALID_TIMESTAMP;
        (void) probe_srv_.is_server_normal(member.get_server(), palf_id, status, last_probe_normal_time_us);
        int64_t last_keepalive_resp_time_us = OB_INVALID_TIMESTAMP;
        (void) probe_srv_.get_last_resp_ts(member.get_server(), last_keepalive_resp_time_us);

        const bool is_invalid_log_ack = (OB_INVALID_TIMESTAMP == last_ack_time_us);
        const bool is_log_ack_timeout = (curr_time_us - last_ack_time_us >= arb_timeout_us_);
        const bool is_keepalive_resp_timeout = (last_keepalive_resp_time_us != OB_INVALID_TIMESTAMP) && \
            (curr_time_us - last_keepalive_resp_time_us >= arb_timeout_us_);
        // do not degrade in UNKNOWN status (e.g., RPC queue overflow)
        const bool is_probe_resp_timeout = (last_probe_normal_time_us != OB_INVALID_TIMESTAMP) && \
            (curr_time_us - last_probe_normal_time_us >= arb_timeout_us_) && status != LogReplicaStatus::UNKNOWN;
        if (self_ == member.get_server()) {
          // pass
        } else if (false == is_degraded_member &&
            (probe_srv_.is_server_stopped(member.get_server()) ||
            (arb_timeout_us_ > 0 && (is_invalid_log_ack ||
             is_log_ack_timeout || is_keepalive_resp_timeout || is_probe_resp_timeout)))) {
          // - need degrade when server is stopped
          // - if arb_timeout_us_ == 0, do not degrade
          // - degrade if log acks reach timeout or invalid
          // - degrade if last_resp_ts of netkeepalive reaches timeout (for kill -9, server crash, etc.)
          // - degrade if last_probe_normal_ts of ProbeService reaches timeout (for disable_sync, log disk full, etc.)
          if (OB_SUCCESS != (tmp_ret = may_be_degraded_servers.push_back(LogMemberStatus(ack_info_array.at(i), status)))) {
            CLOG_LOG(WARN, "insert to may_be_degraded_servers failed", K(tmp_ret), K(member));
          }
        } else if (true == is_degraded_member) {
        }
        CLOG_LOG(TRACE, "member ack info", K(member), K(status), K(curr_time_us),
            K(last_ack_time_us), K(last_keepalive_resp_time_us), K(last_probe_normal_time_us),
            K(arb_timeout_us_), K(is_degraded_member), K(is_invalid_log_ack),
            K(is_log_ack_timeout), K(is_keepalive_resp_timeout), K(is_probe_resp_timeout),
            K(may_be_degraded_servers), K(may_be_upgraded_servers));
      }
      // degraded learners may not return ack to the leader after we pre-push logs to degraded learners.
      // therefore, we probe the status of degraded learners and decide if they need to be upgraded.
      for (int i = 0; i < degraded_list.get_member_number(); i++) {
        int tmp_ret = OB_SUCCESS;
        const common::ObAddr &server = degraded_list.get_learner(i).get_server();
        LogReplicaStatus replica_status = LogReplicaStatus::LOG_NORMAL;
        if (probe_srv_.is_server_normal(server, palf_id, replica_status)) {
          LogMemberStatus status;
          status.member_ = degraded_list.get_learner(i);
          status.last_ack_time_us_ = 0;
          status.last_flushed_end_lsn_ = LSN(PALF_INITIAL_LSN_VAL);
          status.status_ = replica_status;
          if (OB_SUCCESS != (tmp_ret = may_be_upgraded_servers.push_back(status))) {
            CLOG_LOG(WARN, "insert to may_be_upgraded_servers failed", K(tmp_ret), K(server));
          }
        }
      }

      // 2. check if need upgrade and degrade
      const bool need_upgrade = may_be_upgraded_servers.count() > 0;
      const bool need_degrade = may_be_degraded_servers.count() > 0;

      const bool is_upgrading = (OB_ENTRY_NOT_EXIST != may_be_upgraded_palfs_.get(LSKey(palf_id), unused_list));
      const bool is_degrading = (OB_ENTRY_NOT_EXIST != may_be_degraded_palfs_.get(LSKey(palf_id), unused_list));

      // 3. do upgrade/degrade
      // Note: In the 4F1A scenario, if two F have been degraded and there are no appending logs,
      // need_upgrade and need_degrade may be true at the same time, we upgrade the cluster preferentially.
      if (need_upgrade) {
        // probe server status before upgrading
        // do upgrade only when may_be_upgraded_servers are all alive
        bool is_updated = false;
        if (OB_FAIL(start_probe_servers_(may_be_upgraded_servers, palf_id))) {
          CLOG_LOG(WARN, "start_probe_servers failed", K(ret), K(may_be_upgraded_servers));
        } else if (OB_FAIL(update_server_map_(may_be_upgraded_palfs_, LSKey(palf_id), may_be_upgraded_servers, is_updated))) {
          // insert may_be_upgraded_servers to palf level hashmap
          CLOG_LOG(WARN, "update may_be_upgraded_palfs_ failed", K(ret), K(palf_id), K(may_be_upgraded_servers));
        } else if (is_updated) {
          FLOG_INFO("member may be upgraded, ARB_REASON: receive its log_ack", K_(self),
              K(palf_id), K(may_be_upgraded_servers), K(curr_time_us), K_(arb_timeout_us));
        }
      } else if (need_degrade) {
        // probe server status before degrading
        // do degrade only when may_be_degraded_servers are all dead and other servers are all alive
        bool is_updated = false;
        if (OB_FAIL(start_probe_servers_(paxos_member_list, palf_id))) {
          CLOG_LOG(WARN, "start_probe_servers failed", K(ret), K(paxos_member_list));
        } else if (OB_FAIL(update_server_map_(may_be_degraded_palfs_, LSKey(palf_id), may_be_degraded_servers, is_updated))) {
          // insert may_be_degraded_servers to palf level hashmap
          CLOG_LOG(WARN, "update may_be_degraded_palfs_ failed", K(palf_id), K(may_be_degraded_servers));
        } else if (is_updated) {
          FLOG_INFO("server may be degraded, ARB_REASON: don't receive log_ack in arbitration_timeout",
              K(palf_id), K_(self), K(may_be_degraded_servers), K_(arb_timeout_us));
        }
      }
    }
    // always return OB_SUCCESS
    return OB_SUCCESS;
  };
  if (OB_ISNULL(palf_env_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObArbitrationService not inited", K(ret), K_(self), KP_(palf_env));
  } else if (false == get_palf_sync_info.is_valid()) {
    // ObFunction invalid, need retry
    CLOG_LOG(WARN, "ObFunction get_palf_sync_info invalid", K(ret), K_(self));
  } else if (OB_FAIL(palf_env_->for_each(get_palf_sync_info))) {
    CLOG_LOG(WARN, "get_server_sync_info_ failed", K(ret), K_(self));
  } else {
  }
  return ret;
}

void ObArbitrationService::update_arb_timeout_()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    arb_timeout_us_ = (tenant_config->arbitration_timeout);
  } else {
    arb_timeout_us_ = 0;
  }
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    CLOG_LOG(TRACE, "update_arb_timeout_", K_(self), K_(arb_timeout_us));
  }
}

void ObArbitrationService::run_loop_()
{
  int ret = OB_SUCCESS;
  const int64_t min_loop_interval = 100 * 1000; // 100ms

  while(!has_set_stop()) {
    const int64_t start_time_us = ObTimeUtility::current_time();
    update_arb_timeout_();
    ProbeRSFunctor probe_rs_func(this);
    if (OB_FAIL(get_server_sync_info_())) {
      CLOG_LOG(WARN, "get_server_sync_info_ failed", K(ret));
    } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_4_0 && OB_FAIL(palf_env_->for_each(probe_rs_func))) {
      CLOG_LOG(WARN, "probe_rs_func failed", K(ret));
    } else {
      CLOG_LOG(TRACE, "runloop_", K(self_));
      DoDegradeFunctor do_degrade_func(self_, palf_env_, &probe_srv_, this);
      DoUpgradeFunctor do_upgrade_func(palf_env_, &probe_srv_, this);

      // do degrade
      if (OB_FAIL(may_be_degraded_palfs_.for_each(do_degrade_func))) {
        CLOG_LOG(WARN, "do_degrade failed", K(ret));
      } else {
        for (int i = 0; i < do_degrade_func.need_removed_palfs_.count(); i++) {
          const int64_t palf_id = do_degrade_func.need_removed_palfs_.at(i);
          (void) may_be_degraded_palfs_.erase(LSKey(palf_id));
        }
      }
      // do upgrade
      if (OB_FAIL(may_be_upgraded_palfs_.for_each(do_upgrade_func))) {
        // overwrite ret
        CLOG_LOG(WARN, "do_upgrade failed", K(ret));
      } else {
        for (int i = 0; i < do_upgrade_func.need_removed_palfs_.count(); i++) {
          const int64_t palf_id = do_upgrade_func.need_removed_palfs_.at(i);
          (void) may_be_upgraded_palfs_.erase(LSKey(palf_id));
        }
      }
    }
    const int64_t curr_time_us = ObTimeUtility::current_time();
    if (curr_time_us - start_time_us < MIN_LOOP_INTERVAL_US) {
      ob_usleep(MIN_LOOP_INTERVAL_US + start_time_us - curr_time_us, true /*is_idle_sleep*/);
    }
  }
}


int ObArbitrationService::handle_server_probe_msg(
    const common::ObAddr &sender,
    const LogServerProbeMsg &req)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(probe_srv_.handle_server_probe_msg(sender, req))) {
    CLOG_LOG(WARN, "handle_server_probe_msg failed", K(ret), K(sender), K(req));
  }
  return ret;
}

int ObArbitrationService::diagnose(const share::ObLSID &ls_id,
                                   LogArbSrvDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  PalfHandle palf_handle;
  common::ObMemberList member_list;
  int64_t replica_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(palf_env_->open(ls_id.id(), palf_handle))) {
    CLOG_LOG(WARN, "palf_env open failed", KR(ret), K(ls_id), KP(palf_env_));
  } else if (OB_FAIL(palf_handle.get_paxos_member_list(member_list, replica_num))) {
    CLOG_LOG(WARN, "palf_env open failed", KR(ret), K(ls_id), KP(palf_env_));
  } else {
    bool all_normal = true;
    int tmp_ret = OB_SUCCESS;
    for (int i = 0; i < member_list.get_member_number(); i++) {
      common::ObAddr server;
      LogReplicaStatus status;
      char ADDR_STR_BUF[OB_MAX_SERVER_ADDR_SIZE] = {'\0'};
      bool is_normal = false;
      if (OB_TMP_FAIL(member_list.get_server_by_index(i, server))) {
        CLOG_LOG_RET(WARN, tmp_ret, "get_server_by_index failed", K(member_list), K(i));
      } else if (OB_TMP_FAIL(server.ip_port_to_string(ADDR_STR_BUF, OB_MAX_SERVER_ADDR_SIZE))) {
        CLOG_LOG_RET(ERROR, tmp_ret, "ip_port_to_string failed", K(server));
      } else if (FALSE_IT(is_normal = probe_srv_.is_server_normal(server, ls_id.id(), status))) {
      } else if (OB_TMP_FAIL(diagnose_info.diagnose_str_.append_fmt("SRV: %s, STATUS: %s;",
          ADDR_STR_BUF, replica_status_to_table_str(status)))) {
        CLOG_LOG_RET(WARN, tmp_ret, "append_fmt failed", K(ADDR_STR_BUF), K(status));
      }
      all_normal = all_normal && is_normal;
    }
    if (false == all_normal &&
        OB_TMP_FAIL(diagnose_info.diagnose_str_.append("REASON: abnormal replicas exist"))) {
        CLOG_LOG_RET(WARN, tmp_ret, "get_server_by_index failed", K(member_list));
    }
  }
  if (OB_NOT_NULL(palf_env_) && palf_handle.is_valid()) {
    palf_env_->close(palf_handle);
  }
  return ret;
}

bool ObArbitrationService::DoDegradeFunctor::operator()(
      const LSKey &ls_key,
      const LogMemberStatusList &degrade_servers)
{
  int ret = OB_SUCCESS;
  PalfHandle handle;
  int64_t replica_num;
  common::ObMemberList expected_member_list;
  LogMemberAckInfoList unused_array;
  LogMemberStatusList dead_servers;
  common::GlobalLearnerList degraded_list;
  LogMemberAckInfoList degrade_servers_for_palf;
  const int64_t palf_id = ls_key.id_;
  common::ObMember arb_member;
  if (is_valid() == false) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "DoDegradeFunctor not inited", K(ret), K(palf_id), KP(palf_env_), KP(probe_srv_));
  } else if (degrade_servers.count() == 0) {
    CLOG_LOG(WARN, "degrade_servers is empty", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_env_->open(palf_id, handle))) {
    CLOG_LOG(WARN, "palf_env open failed", K(ret), K(palf_id));
  } else if (OB_FAIL(handle.get_paxos_member_list(expected_member_list, replica_num))) {
    CLOG_LOG(WARN, "get_paxos_member_list failed", K(ret), K(palf_id));
  } else if (OB_FAIL(handle.get_ack_info_array(unused_array, degraded_list))) {
    CLOG_LOG(WARN, "get_ack_info_array failed", K(ret), K(palf_id));
    (void) need_removed_palfs_.push_back(palf_id);
  } else if (OB_FAIL(handle.get_arbitration_member(arb_member)) || !arb_member.is_valid()) {
    CLOG_LOG(WARN, "get_arbitration_member failed", K(ret), K(palf_id), K(arb_member));
    (void) need_removed_palfs_.push_back(palf_id);
  } else if (true == probe_srv_->is_server_in_black(arb_member.get_server())) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(INFO, "do not degrade, ARB_REASON: arb_member is not alive", K(ret), K(palf_id),
          K(degrade_servers), K(arb_member));
      (void) need_removed_palfs_.push_back(palf_id);
    }
  } else if (true == is_all_server_alive_(probe_srv_, degrade_servers, palf_id)) {
    // clean need_removed_palfs_ after 1s
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(INFO, "do not degrade, ARB_REASON: degraded_servers are all normal", K(ret), K(palf_id), K(degrade_servers));
      (void) need_removed_palfs_.push_back(palf_id);
    }
  } else if (OB_FAIL(get_dead_servers_(probe_srv_, degrade_servers, palf_id, dead_servers))) {
    CLOG_LOG(WARN, "get_dead_servers_ failed", K(ret), K(palf_id), K(degrade_servers));
  } else if (dead_servers.count() == 0) {
    // Note: skip servers whose status are UNKNOWN
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(INFO, "do not degrade, ARB_REASON: status of degraded_servers are UNKNOWN",
          K(ret), K(palf_id), K(degrade_servers));
      (void) need_removed_palfs_.push_back(palf_id);
    }
  } else if (false == is_allow_degrade_(expected_member_list, replica_num, degraded_list, dead_servers, palf_id)) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(INFO, "do not degrade, ARB_REASON: dead_servers are not allowed to be degraded",
          K(palf_id), K(replica_num), K(degraded_list), K(dead_servers));
    }
    (void) need_removed_palfs_.push_back(palf_id);
  } else if (false == is_all_other_servers_alive_(expected_member_list, degraded_list, dead_servers, palf_id)) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(INFO, "do not degrade, ARB_REASON: is_all_other_servers_alive_ returns false",
          K(ret), K(palf_id), K(expected_member_list), K(degraded_list), K(dead_servers));
    }
  } else if (OB_FAIL(convert_list_for_palf_(dead_servers, degrade_servers_for_palf))) {
    CLOG_LOG(ERROR, "convert_list_for_palf_ failed", K(ret), K(dead_servers), K(degrade_servers_for_palf));
  } else if (OB_FAIL(handle.degrade_acceptor_to_learner(degrade_servers_for_palf, DEGRADE_ACTION_TIMEOUT_US))) {
    if (OB_TIMEOUT == ret || OB_EAGAIN == ret) {
    } else if (OB_NOT_MASTER == ret || OB_OP_NOT_ALLOW == ret) {
      // do not match double check or not master
      (void) need_removed_palfs_.push_back(palf_id);
    } else {
      CLOG_LOG(WARN, "degrade_acceptor_to_learner failed", K(ret), K(palf_id), K(dead_servers));
    }
  } else {
    FLOG_INFO("[ARB_ACTION] degrade_acceptor_to_learner success, ARB_REASON: all degrade_servers are abnormal",
        K(ret), K(palf_id), K(dead_servers));
    if (!is_all_server_status_equal_(dead_servers, LogReplicaStatus::SERVER_STOPPED_OR_ZONE_STOPPED)) {
      LOG_DBA_ERROR(OB_ARB_DEGRADE, "tenant_id", MTL_ID(), "ls_id", palf_id, "degraded_servers", dead_servers);
    }
    const bool is_degrade = true;
    arb_srv_->report_arb_action_(palf_id, dead_servers, is_degrade);
    (void) need_removed_palfs_.push_back(palf_id);
  }
  if (OB_NOT_NULL(palf_env_) && handle.is_valid()) {
    palf_env_->close(handle);
  }
  return true;
}

void ObArbitrationService::report_arb_action_(const int64_t palf_id, const LogMemberStatusList &members, const bool is_degrade)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(members_to_table_str_(members, members_buf_))) {
    CLOG_LOG(WARN, "members_to_table_str_ failed", K(ret), K(palf_id), K(members), K(is_degrade));
  } else if (OB_FAIL(reasons_to_table_str_(members, reasons_buf_, MAX_MEMBER_LIST_LENGTH))) {
    CLOG_LOG(WARN, "reasons_to_table_str_ failed", K(ret), K(palf_id), K(members), K(is_degrade));
  } else {
    if (is_degrade) {
      monitor_->record_degrade_event(palf_id, members_buf_.ptr(), reasons_buf_);
    } else {
      monitor_->record_upgrade_event(palf_id, members_buf_.ptr(), reasons_buf_);
    }
  }
}

int ObArbitrationService::members_to_table_str_(const LogMemberStatusList &memberlist, ObSqlString &sql_str)
{
  int ret = OB_SUCCESS;
  share::ObLSReplica::MemberList tmp_member_list;
  for (int i = 0; i < memberlist.count() && OB_SUCC(ret); i++) {
    const LogMemberStatus &member = memberlist.at(i);
    if (OB_FAIL(tmp_member_list.push_back(share::SimpleMember(member.member_.get_server(),
        member.member_.get_timestamp())))) {
      CLOG_LOG(WARN, "push_back failed", K(ret), K(memberlist));
    }
  }
  if (FAILEDx(share::ObLSReplica::member_list2text(tmp_member_list, sql_str))) {
    CLOG_LOG(WARN, "member_list2text failed", K(ret), K(tmp_member_list));
  }
  return ret;
}

int ObArbitrationService::reasons_to_table_str_(const LogMemberStatusList &memberlist, char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  memset(buf, 0, len);
  int64_t pos = 0;
  for (int i = 0; i < memberlist.count() && OB_SUCC(ret); i++) {
    if (0 != pos) {
      int n = snprintf(buf + pos, len - pos, "%s", ",");
      if (n < 0 || n >= MAX_LEARNER_LIST_LENGTH - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        CLOG_LOG(WARN, "snprintf error or buf not enough", KR(ret), K(n), K(pos));
      } else {
        pos += n;
      }
    }
    const LogMemberStatus &member = memberlist.at(i);
    if (OB_FAIL(ret)) {
    } else {
      int n = snprintf(buf + pos, len - pos, "%s", replica_status_to_table_str(member.status_));
      if (n < 0 || n >= MAX_LEARNER_LIST_LENGTH - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        CLOG_LOG(WARN, "snprintf error or buf not enough", KR(ret), K(n), K(pos));
      } else {
        pos += n;
      }
    }
  }
  return ret;
}

int ObArbitrationService::try_probe_rs_(bool &connected)
{
  int ret = OB_SUCCESS;
  connected = false;
  common::ObAddr rs_leader;
  const int64_t timeout_us = 1 * 1000 * 1000L;   // 1s
  const int64_t retry_interval_us = 10 * 1000L;  // 10ms
  TimeoutChecker not_timeout(timeout_us);
  bool force_renew = false;
  while (OB_SUCC(ret) && OB_SUCC(not_timeout())) {
    if (OB_FAIL(location_adapter_->get_leader(OB_SYS_TENANT_ID, ObLSID::SYS_LS_ID, force_renew, rs_leader))) {
      if (true == location_adapter_->is_location_service_renew_error(ret)) {
        // faile to refresh location or no leader temporarily, retry until timeout
        ret = OB_SUCCESS;
      } else {
        CLOG_LOG(WARN, "location_adapter get_leader for sys ls failed", K(ret));
      }
    } else if (OB_SUCC(probe_srv_.sync_probe_rs(rs_leader))) {
      connected = true;
      CLOG_LOG(INFO, "sync_probe_rs succeed, self is connected with rs", K_(self), K(rs_leader), K(connected));
      break;
    } else if (OB_NOT_MASTER == ret) {
      // rs_leader from get_leader() may be expired, retry until timeout
      ret = OB_SUCCESS;
    } else if (is_process_timeout_error(ret) || is_server_down_error(ret)) {
      // retry until timeout
      CLOG_LOG(WARN, "rpc timeout or unable to connect rs", K(ret), K(rs_leader), K(connected));
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(WARN, "sync_probe_rs failed", K(ret), K(rs_leader), K(connected));
    }

    if (OB_SUCC(ret)) {
      force_renew = true;
      ob_usleep(retry_interval_us);
    }
  }

  if (OB_TIMEOUT == ret) {
    connected = false;
    ret = OB_SUCCESS;
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      CLOG_LOG(INFO, "self is disconnected with rs", K(self_), K(rs_leader), K(connected));
    }
  }
  return ret;
}

bool ObArbitrationService::is_in_cluster_policy_() const
{
  bool is_in_cluster_policy = (0 == GCONF.arbitration_degradation_policy.case_compare("CLUSTER_POLICY"));
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    CLOG_LOG(INFO, "is_in_cluster_policy_", K_(self), K(is_in_cluster_policy));
  }
  return is_in_cluster_policy;
}

int ObArbitrationService::check_connected_with_rs_(const int64_t palf_id, bool &connected)
{
  int ret = OB_SUCCESS;
  connected = false;
  bool new_election_silent_flag = false;
  if (OB_FAIL(try_probe_rs_(connected))) {
    CLOG_LOG(WARN, "connect with rs failed", K(ret), K(connected), K(palf_id));
  } else if (FALSE_IT(new_election_silent_flag = connected ? false : true)) {
  } else if (OB_FAIL(set_election_silent_flag_(palf_id, new_election_silent_flag))) {
    CLOG_LOG(WARN, "failed to set election silent flag", K(ret), K(new_election_silent_flag), K(palf_id));
  }

  return ret;
}

int ObArbitrationService::set_election_silent_flag_(const int64_t palf_id, const bool &new_election_silent_flag)
{
  int ret = OB_SUCCESS;
  PalfHandle handle;
  if (OB_FAIL(palf_env_->open(palf_id, handle))) {
    CLOG_LOG(WARN, "get_palf_id failed", K(ret), K(palf_id));
  } else {
    bool old_election_silent_flag = handle.is_election_silent();
    if (old_election_silent_flag == new_election_silent_flag) {
    } else if (OB_FAIL(handle.set_election_silent_flag(new_election_silent_flag))) {
      CLOG_LOG(WARN, "set_election_silent_flag failed", K(ret), K(palf_id), K(new_election_silent_flag));
    } else {
      monitor_->record_election_silent_event(new_election_silent_flag, palf_id);
      CLOG_LOG(INFO, "set_election_silent_flag succeed", K(palf_id), K(new_election_silent_flag));
    }
  }
  if (OB_NOT_NULL(palf_env_) && handle.is_valid()) {
    palf_env_->close(handle);
  }
  return ret;
}

bool ObArbitrationService::DoDegradeFunctor::is_all_other_servers_alive_(
      const common::ObMemberList &all_servers,
      const common::GlobalLearnerList &degraded_list,
      const LogMemberStatusList &excepted_servers,
      const int64_t palf_id)
{
  // besides degraded servers and expected server, whether all servers are alive
  bool bool_ret = true;
  int ret = OB_SUCCESS;
  common::ObMemberList member_list = all_servers;
  for (int i = 0; i < excepted_servers.count(); i++) {
    const common::ObAddr &server = excepted_servers.at(i).member_.get_server();
    if (OB_FAIL(member_list.remove_server(server))) {
      CLOG_LOG(WARN, "remove_server failed", K(ret), K(server), K(member_list));
    }
  }
  for (int i = 0; i < degraded_list.get_member_number(); i++) {
    common::ObAddr server;
    if (OB_FAIL(degraded_list.get_server_by_index(i, server))) {
      // overwrite ret
      CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K(i), K(server), K(degraded_list));
    } else if (OB_FAIL(member_list.remove_server(server))) {
      CLOG_LOG(WARN, "remove_server failed", K(ret), K(server), K(member_list));
    }
  }
  for (int i = 0; i < member_list.get_member_number(); i++) {
    common::ObAddr server;
    LogReplicaStatus status;
    if (OB_FAIL(member_list.get_server_by_index(i, server))) {
      // overwrite ret
    // Note: do not degrade if other servers' status are UNKNOWN
    } else if (false == probe_srv_->is_server_normal(server, palf_id, status)) {
      bool_ret = false;
      CLOG_LOG(INFO, "server is not alive", K(ret), K(server), K(palf_id), K(all_servers),
          K(degraded_list), K(excepted_servers), K(status));
      break;
    }
  }
  return bool_ret;
}

bool ObArbitrationService::DoDegradeFunctor::is_allow_degrade_(
    const common::ObMemberList &expected_member_list,
    const int64_t replica_num,
    const common::GlobalLearnerList &degraded_list,
    const LogMemberStatusList &may_be_degraded_servers,
    const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  bool need_degrade = true;
  common::ObRegion leader_region;
  const int64_t may_degrade_server_num = may_be_degraded_servers.count();
  const int64_t degraded_server_num = degraded_list.get_member_number();
  const int64_t curr_member_num = expected_member_list.get_member_number();
  for (int i = 0; i < may_be_degraded_servers.count(); i++) {
    const common::ObAddr &server = may_be_degraded_servers.at(i).member_.get_server();
    if (degraded_list.contains(server)) {
      CLOG_LOG(WARN, "the server has been degraded", K(may_be_degraded_servers), K(degraded_list));
      need_degrade = false;
      break;
    }
  }
  if (false == need_degrade) {
  } else if (may_degrade_server_num == 0 ||
      (replica_num & 1) == 1 ||
      (curr_member_num - may_degrade_server_num - degraded_server_num != replica_num / 2)) {
    // 1. do not allow empty degrade list
    // 2. do not allow an odd number of deployment
    // 3. the number of alive servers should be equal to replica_num / 2
    need_degrade = false;
  } else {
    need_degrade = true;
  }
  
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_4_0) {
    // 4. not allowed to degrade when leader disconnect with RS in cluster policy
    if (true == is_sys_tenant(MTL_ID())) {
      // only degrade in LS_POLICY for log stream of sys tenant
    } else if (true == arb_srv_->is_in_cluster_policy_() && true == need_degrade) {
      bool connected_with_rs = false;
      if (OB_FAIL(arb_srv_->check_connected_with_rs_(palf_id, connected_with_rs))) {
        CLOG_LOG(WARN, "check_connected_with_rs_ failed", K(ret), K(palf_id), K(connected_with_rs));
      } else {
        need_degrade = connected_with_rs;
      }

      if (OB_FAIL(ret)) {
        need_degrade = false;
        CLOG_LOG(ERROR, "failed to check connected with rs before degradation", K(ret), K(MTL_ID()), K(palf_id), K(connected_with_rs));
      } else if (false == need_degrade) {
        CLOG_LOG(ERROR, "not allowed to degrade because of disconnected with rs!", K(MTL_ID()), K(palf_id), K(connected_with_rs));
      }
    }
  }

  CLOG_LOG(TRACE, "is_allow_degrade_ trace", K(need_degrade));
  return need_degrade;
}

bool ObArbitrationService::DoDegradeFunctor::is_all_server_status_equal_(
    const LogMemberStatusList &servers,
    const LogReplicaStatus &status) const
{
  bool bool_ret = (0 != servers.count());
  for (int i = 0; i < servers.count() && true == bool_ret; i++) {
    if (status != servers.at(i).status_) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool ObArbitrationService::DoUpgradeFunctor::operator()(
      const LSKey &ls_key,
      const LogMemberStatusList &upgrade_servers)
{
  int ret = OB_SUCCESS;
  PalfHandle handle;
  const int64_t palf_id = ls_key.id_;
  LogMemberAckInfoList upgrade_servers_for_palf;
  if (OB_ISNULL(palf_env_) || OB_ISNULL(probe_srv_) || OB_ISNULL(arb_srv_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "DoUpgradeFunctor not inited", K(ret), K(palf_id));
  } else if (upgrade_servers.count() == 0) {
    CLOG_LOG(WARN, "upgrade_servers is empty", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_env_->open(palf_id, handle))) {
    CLOG_LOG(WARN, "palf_env open failed", K(ret), K(palf_id));
  // NB: why don't remove this palf from map when all upgrade_servers are DEAD like DoDegradeFunctor?
  // Server status in ServerProbeService is just a cache, if ServerProbeService told you a server is DEAD, that
  // just means this server did not reply serverprobe message in DEAD_SERVER_TIMEOUT(2s). E.g. ServerProbeService sent
  // probe msg in 10:00:00 to A, and don't receive response within DEAD_SERVER_TIMEOUT, so ServerProbeService sets status
  // of A to DEAD and send another probe msg in 10:00:02. Then A recovers in 10:00:02:xxx(don't receive probe msg) and send
  // log_ack to leader. So ArbitrationSerivce in leader will try to upgrade A, but server status of A in ServerProbeService
  // is still DEAD. If we remove this palf from map when "A is DEAD", this palf may be add to map repeatedly.
  // so we keep this upgrade palf in map is safe, because this palf needs to be upgraded after all.
  // But for degrading, wrong degrading desicion must be removed from map, otherwise ArbitrationService
  // will try to degrade repeatedly.
  } else if (false == is_all_server_alive_(probe_srv_, upgrade_servers, palf_id)) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(INFO, "do not upgrade, ARB_REASON: some upgrade_servers are abnormal", K(ret), K(palf_id), K(upgrade_servers));
      (void) need_removed_palfs_.push_back(palf_id);
    }
  } else if (OB_FAIL(convert_list_for_palf_(upgrade_servers, upgrade_servers_for_palf))) {
    CLOG_LOG(ERROR, "convert_list_for_palf_ failed", K(ret), K(upgrade_servers), K(upgrade_servers_for_palf));
  } else if (OB_FAIL(handle.upgrade_learner_to_acceptor(upgrade_servers_for_palf, UPGRADE_ACTION_TIMEOUT_US))) {
    if (OB_TIMEOUT == ret || OB_EAGAIN == ret) {
    } else if (OB_NOT_MASTER == ret || OB_OP_NOT_ALLOW == ret) {
      // do not match double check or not master
      (void) need_removed_palfs_.push_back(palf_id);
    } else {
      CLOG_LOG(WARN, "upgrade_learner_to_acceptor failed", K(ret), K(palf_id), K(upgrade_servers));
    }
  } else {
    FLOG_INFO("[ARB_ACTION] upgrade_learner_to_acceptor success, ARB_REASON: all upgrade_servers are normal",
        K(ret), K(palf_id), K(upgrade_servers));
    const bool is_degrade = false;
    arb_srv_->report_arb_action_(palf_id, upgrade_servers, is_degrade);
    (void) need_removed_palfs_.push_back(palf_id);
  }
  if (OB_NOT_NULL(palf_env_) && handle.is_valid()) {
    palf_env_->close(handle);
  }
  return true;
}

int ServerProbeService::init(const common::ObAddr &self,
                             obrpc::ObLogServiceRpcProxy *rpc_proxy,
                             IObNetKeepAliveAdapter *net_keepalive,
                             palf::PalfEnv *palf_env)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ServerProbeService has been inited", K(ret));
  } else if (false == self.is_valid() || OB_ISNULL(rpc_proxy) || OB_ISNULL(palf_env)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(self), KP(rpc_proxy), KP(palf_env));
  } else if (OB_FAIL(probing_servers_.init("LogProbeArv", MTL_ID()))) {
    CLOG_LOG(WARN, "probing_servers_ init failed", K(ret));
  } else {
    self_ = self;
    rpc_proxy_ = rpc_proxy;
    net_keepalive_ = net_keepalive;
    palf_env_ = palf_env;
    is_inited_ = true;
  }
  return ret;
}

void ServerProbeService::stop()
{
  timer_.stop();
  is_running_ = false;
}

void ServerProbeService::wait()
{
  timer_.wait();
}

void ServerProbeService::destroy()
{
  if (IS_NOT_INIT) {
    CLOG_LOG_RET(WARN, OB_NOT_INIT, "ServerProbeService not inited");
  } else {
    WLockGuard guard(lock_);
    is_inited_ = false;
    is_running_ = false;
    probing_servers_.destroy();
    timer_.destroy();
    rpc_proxy_ = NULL;
    net_keepalive_ = NULL;
    palf_env_ = NULL;
  }
}

int ServerProbeService::start_probe_server(const common::ObAddr &server, const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ServerProbeService not inited", K(ret));
  } else {
    WLockGuard guard(lock_);
    if (server == self_) {
    } else if (OB_FAIL(probing_servers_.insert(ServerLSKey(server, palf_id), LSProbeCtx()))
               && OB_ENTRY_EXIST != ret) {
      CLOG_LOG(WARN, "probing_servers_ insert failed", K(server));
    } else if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(INFO, "start_probe_server", K_(self), K(server));
      run_probe_once_();
    }
    if (false == is_running_) {
      ret = OB_SUCCESS;
      common::ObFunction<bool()> server_probe = [this]()
      {
        this->run_probe_once();
        this->try_clear_server_list();
        this->print_all_server_status();
        return false;
      };
      if (OB_FAIL(timer_.init_and_start(1, 10_ms, "ServerProbeSrv"))) {
        CLOG_LOG(ERROR, "ObOccamTimer init failed", K(ret), K_(self));
      } else if (OB_FAIL(timer_.schedule_task_ignore_handle_repeat_and_immediately(PROBE_INTERVAL, server_probe))) {
        CLOG_LOG(ERROR, "schedule_task failed", K(ret), K_(self));
      } else {
        is_running_ = true;
      }
    }
  }
  return ret;
}

void ServerProbeService::try_clear_server_list()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ServerProbeService not inited", K(ret));
  } else {
    WLockGuard guard(lock_);
    auto clear_dead_server = [&](const ServerLSKey &server_ls_key, LSProbeCtx &probe_ctx) {
      bool bool_ret = false;
      if (probe_ctx.is_staled()) {
        bool_ret = true;
        CLOG_LOG(INFO, "clear_server success", K_(self), K(server_ls_key), K(probe_ctx));
      }
      return bool_ret;
    };
    if (OB_FAIL(probing_servers_.remove_if(clear_dead_server))) {
      CLOG_LOG(WARN, "probing_servers_ erase failed", K(ret));
    }
  }
}


bool ServerProbeService::is_server_normal(const common::ObAddr &server,
                                          const int64_t palf_id)
{
  LogReplicaStatus unused_status;
  int64_t unused_time_us = OB_INVALID_TIMESTAMP;
  return is_server_normal(server, palf_id, unused_status, unused_time_us);
}

bool ServerProbeService::is_server_normal(const common::ObAddr &server,
                                          const int64_t palf_id,
                                          LogReplicaStatus &status)
{
  int64_t unused_time_us = OB_INVALID_TIMESTAMP;
  return is_server_normal(server, palf_id, status, unused_time_us);
}

bool ServerProbeService::is_server_normal(const common::ObAddr &server,
                                          const int64_t palf_id,
                                          LogReplicaStatus &status,
                                          int64_t &last_normal_resp_time_us)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  status = LogReplicaStatus::UNKNOWN;
  last_normal_resp_time_us = OB_INVALID_TIMESTAMP;
  LSProbeCtx probe_ctx;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ServerProbeService not inited", K(ret));
  } else if (server == self_) {
    status = LogReplicaStatus::LOG_NORMAL;
    last_normal_resp_time_us = common::ObTimeUtility::current_time();
    bool_ret = true;
  } else {
    RLockGuard guard(lock_);
    if (OB_FAIL(probing_servers_.get(ServerLSKey(server, palf_id), probe_ctx))) {
      CLOG_LOG(WARN, "probing_servers_ get failed", K(ret), K(server), K(palf_id));
    } else {
      // Note: if is_black_or_stopped == false, but ServerStatus == UNKNOWN, the server is abnormal
      status = convert_replica_status_(server, probe_ctx);
      last_normal_resp_time_us = probe_ctx.get_last_normal_resp_time_us();
      bool_ret = (LogReplicaStatus::LOG_NORMAL == status);
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    if (OB_FAIL(start_probe_server(server, palf_id))) {
      CLOG_LOG(WARN, "start_probe_server failed", K_(self), K(server), K(palf_id));
    }
  }
  CLOG_LOG(TRACE, "is_server_normal", K_(self), K(server), K(palf_id), K(bool_ret), K(probe_ctx));
  return bool_ret;
}

LogReplicaStatus ServerProbeService::convert_replica_status_(const common::ObAddr &server,
                                                             const LSProbeCtx &probe_ctx) const
{
  if (net_keepalive_->is_server_stopped(server)) {
    return LogReplicaStatus::SERVER_STOPPED_OR_ZONE_STOPPED;
  } else if (net_keepalive_->in_black_or_stopped(server)) {
    return LogReplicaStatus::CRASHED_OR_BROKEN_NETWORK;
  } else {
    return probe_ctx.get_status();
  }
}

bool ServerProbeService::is_server_stopped(const common::ObAddr &server) const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ServerProbeService not inited", K(ret));
  } else {
    bool_ret = net_keepalive_->is_server_stopped(server);
  }
  return bool_ret;
}

bool ServerProbeService::is_server_in_black(const common::ObAddr &server) const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ServerProbeService not inited", K(ret));
  } else {
    bool_ret = net_keepalive_->in_black(server);
  }
  return bool_ret;
}

int ServerProbeService::get_last_resp_ts(const common::ObAddr &server,
                                         int64_t &last_resp_ts) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ServerProbeService not inited", K(ret));
  } else {
    ret = net_keepalive_->get_last_resp_ts(server, last_resp_ts);
  }
  return ret;
}

void ServerProbeService::run_probe_once()
{
  if (IS_NOT_INIT) {
    CLOG_LOG_RET(WARN, OB_NOT_INIT, "ServerProbeService not inited");
  } else {
    WLockGuard guard(lock_);
    run_probe_once_();
  }
}

void ServerProbeService::print_all_server_status()
{
  if (palf::palf_reach_time_interval(20 * 1000 * 1000, last_print_status_time_us_)) {
    RLockGuard guard(lock_);
    auto print_info = [&](const ServerLSKey &server_ls_key, LSProbeCtx &probe_ctx){
      const common::ObAddr &server = server_ls_key.server_;
      const int64_t palf_id = server_ls_key.palf_id_;
      const LogReplicaStatus &status = convert_replica_status_(server, probe_ctx);
      CLOG_LOG_RET(INFO, OB_SUCCESS, "ARB_DUMP", K_(self), K(palf_id), K(server),
          "status", replica_status_to_string(status), K(probe_ctx));
      return true;
    };
    (void) probing_servers_.for_each(print_info);
  }
}

void ServerProbeService::run_probe_once_()
{
  int ret = OB_SUCCESS;
  auto update_status = [&](const ServerLSKey &server_ls_key, LSProbeCtx &probe_ctx){
    const common::ObAddr &server = server_ls_key.server_;
    const int64_t palf_id = server_ls_key.palf_id_;
    int64_t req_id = 0;
    if (server == self_ || OB_SUCCESS != probe_ctx.run_probe_once(req_id)) {
      // skip self or frequent probes
      return true;
    }
    LogServerProbeMsg req(self_, palf_id, req_id, LogServerProbeType::PROBE_REQ, LogReplicaStatus::LOG_NORMAL);
    if (OB_FAIL(rpc_proxy_->to(server).by(MTL_ID()) \
        .group_id(share::OBCG_CLOG).send_server_probe_req(req, NULL))) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        CLOG_LOG(TRACE, "send_server_probe_req failed", KR(ret), K(req));
      }
    } else {
      CLOG_LOG(TRACE, "send_server_probe_req success", KR(ret), K(server), K(req));
    }
    return true;
  };
  if (OB_FAIL(probing_servers_.for_each(update_status))) {
    CLOG_LOG(WARN, "probing_servers_ for_each failed", K(ret));
  }
}

int ServerProbeService::handle_server_probe_msg(
    const common::ObAddr &sender,
    const LogServerProbeMsg &req)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ServerProbeService not inited", K(ret));
  } else if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(sender), K(req));
  } else if (req.msg_type_ == LogServerProbeType::PROBE_RESP) {
    WLockGuard guard(lock_);
    auto update_resp_ts = [&](const ServerLSKey &server_ls_key, LSProbeCtx &probe_ctx) {
      (void) probe_ctx.receive_probe_resp(req.req_id_, (LogReplicaStatus)req.server_status_);
      return true;
    };
    if (OB_FAIL(probing_servers_.operate(ServerLSKey(sender, req.palf_id_), update_resp_ts))) {
      CLOG_LOG(WARN, "probing_servers_ get failed", K(ret), K(sender));
    } else {
      CLOG_LOG(TRACE, "receive_server_probe_req success", KR(ret), K(sender), K(req));
    }
  } else if (req.msg_type_ == LogServerProbeType::PROBE_REQ) {
    LogReplicaStatus server_status = LogReplicaStatus::LOG_NORMAL;
    logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
    bool log_disk_error = false;
    if (NULL != detector) {
      bool slog_hang = false;
      log_disk_error = detector->is_clog_disk_has_fatal_error();
    }
    PalfHandle palf_handle;
    if (OB_FAIL(palf_env_->open(req.palf_id_, palf_handle))) {
      CLOG_LOG(WARN, "palf_env open failed", KR(ret), K(req.palf_id_), KP(palf_env_));
    } else {
      server_status = (palf_handle.is_sync_enabled())? server_status: LogReplicaStatus::LOG_SYNC_DISABLED;
      server_status = (palf_handle.is_vote_enabled())? server_status: LogReplicaStatus::LOG_VOTE_DISABLED;
      server_status = (log_disk_error)? LogReplicaStatus::LOG_DISK_HANG_OR_FULL: server_status;
      palf_env_->close(palf_handle);
    }
    LogServerProbeMsg resp(self_, req.palf_id_, req.req_id_, LogServerProbeType::PROBE_RESP, server_status);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rpc_proxy_->to(sender).by(MTL_ID()) \
        .group_id(share::OBCG_CLOG).send_server_probe_req(resp, NULL))) {
      CLOG_LOG(WARN, "send_server_probe_resp failed", KR(ret), K(resp));
    } else {
      CLOG_LOG(TRACE, "send_server_probe_resp success", KR(ret), K(sender), K(resp));
    }
  }
  return ret;
}

int ServerProbeService::sync_probe_rs(const common::ObAddr &rs_leader)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = 1 * 1000 * 1000;     // 1s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ServerProbeService not inited", K(ret));
  } else if (!rs_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(rs_leader));
  } else {
    // send rpc to rs
    LogProbeRsReq req(self_);
    LogProbeRsResp resp;
    if (OB_FAIL(rpc_proxy_->to(rs_leader).timeout(timeout_us).trace_time(true).
                            max_process_handler_time(timeout_us).by(OB_SYS_TENANT_ID).log_probe_rs(req, resp))) {
      CLOG_LOG(WARN, "send rpc to rs failed", K(ret), K(self_), K(rs_leader), K(resp));
    } else {
      ret = resp.ret_;
      CLOG_LOG(INFO, "sync_probe_rs succeed", K(self_), K(rs_leader), K(resp));
    }
  }

  return ret;
}

int ObArbitrationService::ProbeRSFunctor::operator()(const palf::PalfHandle &palf_handle)
{
  int ret = OB_SUCCESS;
  int64_t palf_id = INVALID_PALF_ID;
  ObRole role = INVALID_ROLE;
  if (OB_FAIL(palf_handle.get_palf_id(palf_id))) {
    CLOG_LOG(WARN, "get_palf_id failed", K(ret), K(palf_handle));
  } else if (false == palf_handle.is_election_silent()) {
    // do nothing
  } else if (false == arb_srv_->is_in_cluster_policy_()) {
    // for emergency situations, make replica not silent by set arbitration_degradation_policy
    const bool election_silent_flag = false;
    if (OB_FAIL(arb_srv_->set_election_silent_flag_(palf_id, election_silent_flag))) {
      CLOG_LOG(WARN, "fail to set election_silent_flag", K(palf_id), K(election_silent_flag));
    };
  } else {
    // In cluster_policy and election silent, when it find itself connected with rs again, make election not silent
    bool connected = false;
    (void)arb_srv_->check_connected_with_rs_(palf_id, connected);
    if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
      CLOG_LOG(INFO, "slient replica probes rs", K(palf_id), K(connected));
    }
  }
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase
