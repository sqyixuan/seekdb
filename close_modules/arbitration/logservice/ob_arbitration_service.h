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

#ifndef OCEANBASE_LOGSERVICE_OB_ARBITRATION_SERVICE_H_
#define OCEANBASE_LOGSERVICE_OB_ARBITRATION_SERVICE_H_

#include "share/ob_thread_pool.h"
#include "share/ob_occam_timer.h"
#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/lock/ob_tc_rwlock.h"              // RWLock
#include "common/ob_member_list.h"              // common::ObMemberList
#include "logservice/logrpc/ob_log_rpc_req.h"   // ProbeMsg
#include "logservice/ob_location_adapter.h"     // ObLocationAdapter
#include "logservice/palf/palf_handle.h"        // PalfHandle

namespace oceanbase
{
namespace palf
{
class PalfEnv;
class LogMemberAckInfo;
}

namespace obrpc
{
class ObLogServiceRpcProxy;
}

namespace logservice
{
class IObNetKeepAliveAdapter;

class IObArbitrationMonitor
{
public:
  virtual int record_degrade_event(const int64_t palf_id, const char *degraded_list, const char *reasons) = 0;
  virtual int record_upgrade_event(const int64_t palf_id, const char *upgraded_list, const char *reasons) = 0;
  virtual int record_election_silent_event(const bool is_silent, const int64_t palf_id) = 0;
};

struct LogArbSrvDiagnoseInfo
{
  LogArbSrvDiagnoseInfo() { reset(); }
  ~LogArbSrvDiagnoseInfo() { reset(); }
  ObSqlString diagnose_str_;
  TO_STRING_KV(K_(diagnose_str));
  void reset() {
    diagnose_str_.reset();
  }
};

// Attention: Do not change the order or values of these enum vars.
enum LogReplicaStatus
{
  UNKNOWN = 0,
  CRASHED_OR_BROKEN_NETWORK,
  LOG_NORMAL,
  LOG_DISK_HANG_OR_FULL,
  LOG_SYNC_DISABLED,
  LOG_VOTE_DISABLED,
  SERVER_STOPPED_OR_ZONE_STOPPED,
};

enum ArbDegradationPolicy
{
  LS_POLICY = 0,
  CLUSTER_POLICY,
};

static inline const char *replica_status_to_string(LogReplicaStatus status)
{
  #define SERVER_STATUS_TO_STR(x) case(LogReplicaStatus::x): return #x
  switch(status)
  {
    SERVER_STATUS_TO_STR(CRASHED_OR_BROKEN_NETWORK);
    SERVER_STATUS_TO_STR(SERVER_STOPPED_OR_ZONE_STOPPED);
    case(LogReplicaStatus::LOG_NORMAL): return "NORMAL";
    SERVER_STATUS_TO_STR(LOG_DISK_HANG_OR_FULL);
    SERVER_STATUS_TO_STR(LOG_SYNC_DISABLED);
    SERVER_STATUS_TO_STR(LOG_VOTE_DISABLED);
    default:
      return "UNKNOWN";
  }
  #undef SERVER_STATUS_TO_STR
}

static inline const char*replica_status_to_table_str(LogReplicaStatus status)
{
  return (status == LogReplicaStatus::LOG_SYNC_DISABLED ||
          status == LogReplicaStatus::LOG_VOTE_DISABLED)?
          "REBUILDING":
          replica_status_to_string(status);
}

class LogMemberStatus : public palf::LogMemberAckInfo
{
public:
  LogMemberStatus ()
      : palf::LogMemberAckInfo(),
        status_(LogReplicaStatus::LOG_NORMAL)
  { }
  LogMemberStatus (const palf::LogMemberAckInfo &ack_info)
      : palf::LogMemberAckInfo(ack_info.member_,
        ack_info.last_ack_time_us_,
        ack_info.last_flushed_end_lsn_),
        status_(LogReplicaStatus::LOG_NORMAL)
  { }
  LogMemberStatus (const palf::LogMemberAckInfo &ack_info,
                   const LogReplicaStatus &status)
      : palf::LogMemberAckInfo(ack_info.member_,
        ack_info.last_ack_time_us_,
        ack_info.last_flushed_end_lsn_),
        status_(status)
  { }
  LogReplicaStatus status_;

  void operator=(const LogMemberStatus &ack_info)
  {
    member_ = ack_info.member_;
    last_ack_time_us_ = ack_info.last_ack_time_us_;
    last_flushed_end_lsn_ = ack_info.last_flushed_end_lsn_;
    status_ = ack_info.status_;
  }

  TO_STRING_KV(K_(member), K_(last_ack_time_us), K_(last_flushed_end_lsn),
      "status", replica_status_to_string(status_));
};

typedef common::ObSEArray<LogMemberStatus, common::OB_MAX_MEMBER_NUMBER> LogMemberStatusList;

class ServerProbeService
{
public:
  ServerProbeService()
      : self_(),
        probing_servers_(),
        rpc_proxy_(NULL),
        timer_(),
        net_keepalive_(NULL),
        palf_env_(NULL),
        is_running_(false),
        last_print_status_time_us_(OB_INVALID_TIMESTAMP),
        is_inited_(false)
  { }
  ~ServerProbeService() { destroy(); }
  int init(const common::ObAddr &self,
           obrpc::ObLogServiceRpcProxy *rpc_proxy,
           IObNetKeepAliveAdapter *net_keepalive,
           palf::PalfEnv *palf_env);
  void stop();
  void wait();
  void destroy();
  int start_probe_server(const common::ObAddr &server, const int64_t palf_id);
  bool is_server_normal(const common::ObAddr &server, const int64_t palf_id);
  bool is_server_normal(const common::ObAddr &server, const int64_t palf_id, LogReplicaStatus &status);
  bool is_server_normal(const common::ObAddr &server,
                        const int64_t palf_id,
                        LogReplicaStatus &status,
                        int64_t &last_normal_resp_time_us);
  bool is_server_stopped(const common::ObAddr &server) const;
  bool is_server_in_black(const common::ObAddr &server) const;
  int get_last_resp_ts(const common::ObAddr &server, int64_t &last_resp_ts) const;
  void run_probe_once();
  void try_clear_server_list();
  int handle_server_probe_msg(const common::ObAddr &sender, const LogServerProbeMsg &req);
  void print_all_server_status();
  int sync_probe_rs(const common::ObAddr &rs_leader);
private:
  void run_probe_once_();
private:
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  static constexpr int64_t MAX_CREDIBLE_WINDOW = 5;
  static constexpr int64_t PROBE_INTERVAL = 1 * 1000 * 1000;       // 1s
  static constexpr int64_t CLEAR_SERVER_THRESHOLD = 10 * 60 * 1000 * 1000L; // 10 mins
private:
  struct LSProbeCtx
  {
    LSProbeCtx()
        : last_req_time_us_(0),
          last_resp_time_us_(ObTimeUtil::current_time()),
          last_normal_resp_time_us_(ObTimeUtil::current_time()),
          last_req_id_(MAX_CREDIBLE_WINDOW - 1),
          current_status_(LogReplicaStatus::LOG_NORMAL)
    {
      for (int i = 0; i < MAX_CREDIBLE_WINDOW; i++) {
        probe_list_[i] = LogReplicaStatus::LOG_NORMAL;
      }
    }

    int run_probe_once(int64_t &current_req_id)
    {
      int ret = OB_SUCCESS;
      const int64_t curr_time_us = common::ObTimeUtility::current_time();
      // Note:: do not control probe frequency for now, because this function is invoked
      // by a timer thread. If the function is invoked by loop thread, we need control frequency.
      // if (curr_time_us - last_req_time_us_ >= PROBE_INTERVAL) {
      last_req_id_++;
      probe_list_[last_req_id_ % MAX_CREDIBLE_WINDOW] = LogReplicaStatus::UNKNOWN;
      current_req_id = last_req_id_;
      update_current_status_();
      last_req_time_us_ = curr_time_us;
      return ret;
    }

    int receive_probe_resp(const int64_t req_id, const LogReplicaStatus &status)
    {
      int ret = OB_SUCCESS;
      if (req_id > last_req_id_ || req_id <= (last_req_id_ - MAX_CREDIBLE_WINDOW)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        last_resp_time_us_ = common::ObTimeUtility::current_time();
        last_normal_resp_time_us_ = (LogReplicaStatus::LOG_NORMAL == status)? \
            last_resp_time_us_: last_normal_resp_time_us_;
        probe_list_[req_id % MAX_CREDIBLE_WINDOW] = status;
        update_current_status_();
      }
      return ret;
    }

    LogReplicaStatus get_status() const
    {
      return current_status_;
    }

    bool is_staled() const
    {
      const int64_t curr_time_us = common::ObTimeUtility::current_time();
      return last_resp_time_us_ != OB_INVALID_TIMESTAMP &&
          curr_time_us - last_resp_time_us_ > CLEAR_SERVER_THRESHOLD;
    }

    int64_t get_last_normal_resp_time_us() const { return last_normal_resp_time_us_; }
    
    TO_STRING_KV(K_(last_req_time_us), K_(last_resp_time_us), K_(last_req_id),
        "replica_status", replica_status_to_string(current_status_));
  private:
    void update_current_status_()
    {
      LogReplicaStatus current_status = LogReplicaStatus::UNKNOWN;
      for (int i = 0; i < MAX_CREDIBLE_WINDOW; i++) {
        const int64_t index = (last_req_id_ - i) % MAX_CREDIBLE_WINDOW;
        if (probe_list_[index] != LogReplicaStatus::UNKNOWN) {
          current_status = probe_list_[index];
          break;
        }
      }
      current_status_ = current_status;
    }

    int64_t last_req_time_us_;
    int64_t last_resp_time_us_;
    int64_t last_normal_resp_time_us_;
    int64_t last_req_id_;
    LogReplicaStatus current_status_;
    LogReplicaStatus probe_list_[MAX_CREDIBLE_WINDOW];
  };

  struct ServerLSKey {
    ServerLSKey() : server_(), palf_id_(-1) {}
    explicit ServerLSKey(const common::ObAddr &server, const int64_t palf_id)
      : server_(server), palf_id_(palf_id) {}
    ~ServerLSKey() {reset();}
    bool operator==(const ServerLSKey &server_ls_key) const
    {
      return (server_ls_key.server_ == server_) && (server_ls_key.palf_id_ == palf_id_);
    }
    uint64_t hash() const
    {
      uint64_t hash_val = 0;
      hash_val = common::murmurhash(&server_, sizeof(server_), hash_val);
      hash_val = common::murmurhash(&palf_id_, sizeof(palf_id_), hash_val);
      return hash_val;
    }
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    void reset() {server_.reset(); palf_id_ = -1;}
    common::ObAddr server_;
    int64_t palf_id_;
    TO_STRING_KV(K_(server), K_(palf_id));
  };
private:
  LogReplicaStatus convert_replica_status_(const common::ObAddr &server,
                                           const LSProbeCtx &probe_ctx) const;
private:
  typedef common::ObLinearHashMap<ServerLSKey, LSProbeCtx> ServerProbeMap;
private:
  mutable RWLock lock_;
  common::ObAddr self_;
  ServerProbeMap probing_servers_;
  obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  ObOccamTimer timer_;
  IObNetKeepAliveAdapter *net_keepalive_;
  palf::PalfEnv *palf_env_;
  bool is_running_;
  int64_t last_print_status_time_us_;
  bool is_inited_;
};

class ObArbitrationService : public share::ObThreadPool
{
public:
  ObArbitrationService();
  virtual ~ObArbitrationService();
  int init(const common::ObAddr &self,
           palf::PalfEnv *palf_env,
           obrpc::ObLogServiceRpcProxy *rpc_proxy,
           IObNetKeepAliveAdapter *net_keepalive,
           IObArbitrationMonitor *monitor,
           ObLocationAdapter *location_adapter);
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();
  int handle_server_probe_msg(const common::ObAddr &sender, const LogServerProbeMsg &req);
  int diagnose(const share::ObLSID &id, LogArbSrvDiagnoseInfo &diagnose_info);
private:
  static constexpr int64_t MIN_LOOP_INTERVAL_US = 10 * 1000;                        // 10ms
  static constexpr int64_t DEGRADE_ACTION_TIMEOUT_US = 10 * 1000 * 1000L;           // 10s
  static constexpr int64_t UPGRADE_ACTION_TIMEOUT_US = 10 * 1000 * 1000L;           // 10s
  static constexpr int64_t MAX_PALF_COUNT = 200;
  typedef common::ObLinearHashMap<palf::LSKey, LogMemberStatusList> PalfAckInfoMap;
private:
  class DoDegradeFunctor
  {
  public:
    explicit DoDegradeFunctor(const common::ObAddr &addr,
                              palf::PalfEnv *palf_env,
                              ServerProbeService *probe_srv,
                              ObArbitrationService *arb_srv)
        : self_(addr),
          palf_env_(palf_env),
          probe_srv_(probe_srv),
          arb_srv_(arb_srv)
    { }
    bool operator()(const palf::LSKey &ls_key, const LogMemberStatusList &degrade_servers);

  public:
    common::ObSEArray<int64_t, MAX_PALF_COUNT> need_removed_palfs_;
    bool is_valid()
    {
      return self_.is_valid() && OB_NOT_NULL(palf_env_) &&
          OB_NOT_NULL(probe_srv_) && OB_NOT_NULL(arb_srv_);
    }
  private:
    bool is_all_other_servers_alive_(
        const common::ObMemberList &all_servers,
        const common::GlobalLearnerList &degraded_list,
        const LogMemberStatusList &excepted_servers,
        const int64_t palf_id);
    bool is_allow_degrade_(
        const common::ObMemberList &expected_member_list,
        const int64_t replica_num,
        const common::GlobalLearnerList &degraded_list,
        const LogMemberStatusList &may_be_degraded_servers,
        const int64_t palf_id);
    bool is_all_server_status_equal_(const LogMemberStatusList &servers,
                                     const LogReplicaStatus &status) const;
    common::ObAddr self_;
    palf::PalfEnv *palf_env_;
    ServerProbeService *probe_srv_;
    ObArbitrationService *arb_srv_;
  };

  class DoUpgradeFunctor
  {
  public:
    explicit DoUpgradeFunctor(palf::PalfEnv *palf_env, ServerProbeService *probe_srv, ObArbitrationService *arb_srv)
        : palf_env_(palf_env),
          probe_srv_(probe_srv),
          arb_srv_(arb_srv)
    { }
    bool operator()(const palf::LSKey &ls_key, const LogMemberStatusList &upgrade_servers);

  public:
    common::ObSEArray<int64_t, MAX_PALF_COUNT> need_removed_palfs_;
  private:
    palf::PalfEnv *palf_env_;
    ServerProbeService *probe_srv_;
    ObArbitrationService *arb_srv_;
  };

  class ProbeRSFunctor
  {
  public:
    explicit ProbeRSFunctor(ObArbitrationService *arb_srv) : arb_srv_(arb_srv) {}
    // probe rs when in election_silent
    // only used by replicas which become followers from leader because of disconnted with rs.
    int operator()(const palf::PalfHandle &palf_handle);
  private:
    ObArbitrationService *arb_srv_;
  };

private:
  int start_probe_servers_(const LogMemberStatusList &servers, const int64_t palf_id);
  int start_probe_servers_(const common::ObMemberList &servers, const int64_t palf_id);
  void start_probe_server_(const common::ObAddr &server, const int64_t palf_id);
  int follower_probe_others_(const int64_t palf_id, const common::ObMemberList &paxos_member_list);
  int get_server_sync_info_();
  void run_loop_();
  void update_arb_timeout_();
  int update_server_map_(PalfAckInfoMap &palf_map,
                         const palf::LSKey &key,
                         const LogMemberStatusList &val,
                         bool &is_updated)
  {
    int ret = OB_SUCCESS;
    LogMemberStatusList existed_val;
    is_updated = false;
    if (OB_FAIL(palf_map.get(key, existed_val))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(palf_map.insert(key, val))) {
          CLOG_LOG(WARN, "palf_map insert failed", K(key), K(val));
        } else {
          is_updated = true;
        }
      }
    } else if (palf::ack_info_list_addr_equal(existed_val, val)) {
      // pass, do not insert
    } else if (OB_FAIL(palf_map.insert_or_update(key, val))) {
      CLOG_LOG(WARN, "palf_map insert_or_update failed", K(key), K(val));
    } else {
      is_updated = true;
      CLOG_LOG(TRACE, "update_server_map_ success", KR(ret), K(key), K(val), K(is_updated));
    }
    CLOG_LOG(TRACE, "update_server_map_ finish", KR(ret), K(key), K(val), K(is_updated));
    return ret;
  }
  static int convert_list_for_palf_(const LogMemberStatusList &status_list, palf::LogMemberAckInfoList &ack_list)
  {
    int ret = OB_SUCCESS;
    ack_list.reset();
    for (int i = 0; i < status_list.count() && OB_SUCC(ret); i++) {
      const LogMemberStatus &status = status_list.at(i);
      if (OB_FAIL(ack_list.push_back(palf::LogMemberAckInfo(status.member_,
          status.last_ack_time_us_, status.last_flushed_end_lsn_)))) {
        CLOG_LOG(WARN, "LogMemberAckInfoList push_back failed", K(status));
      }
    }
    return ret;
  }
  static int members_to_table_str_(const LogMemberStatusList &memberlist, ObSqlString &sql_str);
  static int reasons_to_table_str_(const LogMemberStatusList &memberlist, char *buf, const int64_t len);
  void report_arb_action_(const int64_t palf_id, const LogMemberStatusList &members, const bool is_degrade);
  int try_probe_rs_(bool &connected);
  bool is_in_cluster_policy_() const;
  int check_connected_with_rs_(const int64_t palf_id, bool &connected);
  int set_election_silent_flag_(const int64_t palf_id, const bool &new_election_silent_flag);
private:
  common::ObAddr self_;
  ServerProbeService probe_srv_;
  PalfAckInfoMap may_be_degraded_palfs_;
  PalfAckInfoMap may_be_upgraded_palfs_;
  int64_t arb_timeout_us_;
  int64_t follower_last_probe_time_us_;
  palf::PalfEnv *palf_env_;
  obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  IObArbitrationMonitor *monitor_;
  ObSqlString members_buf_;
  char reasons_buf_[MAX_MEMBER_LIST_LENGTH] = {'\0'};
  ObLocationAdapter *location_adapter_;
  bool is_inited_;
};

} // logservice
} // oceanbase

#endif
