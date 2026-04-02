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

#include "ob_arb_srv_garbage_collect_service.h"
#include "share/arbitration_service/ob_arbitration_service_info.h"                            // ObArbitrationServiceInfo
#include "logservice/ob_log_service.h"                                                        // PalfHandleGuard
#include "logservice/common_util/ob_log_table_operator.h"                                     // ObLogTableOperator

namespace oceanbase 
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace palf;
namespace arbserver
{

ObArbGarbageCollectService::ObArbGarbageCollectService() : proposal_id_(palf::INVALID_PALF_ID),
                                                           seq_(-1),
                                                           is_master_(false),
                                                           self_addr_(),
                                                           rpc_proxy_(NULL),
                                                           sql_proxy_(NULL),
                                                           tg_id_(-1),
                                                           is_inited_(false)
{
}

ObArbGarbageCollectService::~ObArbGarbageCollectService()
{
  destroy();
}

int ObArbGarbageCollectService::init(const common::ObAddr &self_addr,
                                     const int tg_id,
                                     obrpc::ObSrvRpcProxy *rpc_proxy,
                                     common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObArbGarbageCollectService has been inited", KR(ret));
  } else if (!self_addr.is_valid() || OB_ISNULL(rpc_proxy) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(self_addr), KP(rpc_proxy), KP(sql_proxy));
  } else {
    self_addr_ = self_addr;
    rpc_proxy_ = rpc_proxy;
    sql_proxy_ = sql_proxy;
    tg_id_ = tg_id;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObArbGarbageCollectService init success", KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int ObArbGarbageCollectService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TG_START(tg_id_))) {
    CLOG_LOG(WARN, "ObArbGarbageCollectService TG_START failed", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, SCAN_TIMER_INTERVAL, true))) {
    CLOG_LOG(WARN, "ObArbGarbageCollectService TG_SCHEDULE failed", KR(ret));
  } else {
    CLOG_LOG(INFO, "ObArbGarbageCollectService start success", KPC(this), K(tg_id_));
  }
  return ret;
}

int ObArbGarbageCollectService::stop()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
    CLOG_LOG(INFO, "ObArbGarbageCollectService stop finished", KPC(this), K(tg_id_));
  }
  return OB_SUCCESS;
}

void ObArbGarbageCollectService::wait()
{
  if (IS_INIT) {
    TG_WAIT(tg_id_);
    CLOG_LOG(INFO, "ObArbGarbageCollectService wait finished", KPC(this), K(tg_id_));
  }
}

void ObArbGarbageCollectService::destroy()
{
  CLOG_LOG_RET(WARN, OB_SUCCESS, "ObArbGarbageCollectService destroy");
  stop();
  wait();
  TG_DESTROY(tg_id_);
  is_inited_ = false;
  tg_id_ = -1;
  sql_proxy_ = NULL;
  rpc_proxy_ = NULL;
  self_addr_.reset();
}

void ObArbGarbageCollectService::runTimerTask()
{
  const int64_t start_ts = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  GCMsgEpoch epoch;
  common::ObSEArray<ObAddr, 1> arb_addrs;
  ObSpinLockGuard guard(lock_);
  // we get role of replica from PALF round-robin for now,
  // a more graceful way is to receive role change notifications from role change service
  if (OB_FAIL(try_update_role_())) {
    CLOG_LOG(WARN, "try_update_role_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(construct_rpc_epoch_(epoch)) && OB_NOT_MASTER != ret) {
    CLOG_LOG(WARN, "construct_rpc_epoch_ failed", K(ret), KPC(this));
  } else if (OB_NOT_MASTER == ret) {
    CLOG_LOG(TRACE, "i am not master, no need do anything", KPC(this));
  } else if (OB_FAIL(get_arb_addr_from_inner_table_(arb_addrs))) {
  } else if (arb_addrs.empty()) {
    CLOG_LOG(INFO, "there is no arb server, no need send gc message", KPC(this), K(arb_addrs));
  } else if (1 != arb_addrs.count()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "there is more than one arb server, unexpected error", K(ret), KPC(this), K(arb_addrs));
  } else {
    int ret = OB_SUCCESS;
    TenantLSIDSArray ls_ids;
    if (OB_FAIL(construct_ls_id_array_(ls_ids))) {
      CLOG_LOG(WARN, "construct_ls_id_array_ failed", KR(ret), KPC(this));
    } else if (OB_FAIL(send_rpc_to_arb_server_(arb_addrs, epoch, ls_ids))) {
      CLOG_LOG(WARN, "sned_rpc_to_arb_server_ failed", KR(ret), K(arb_addrs), K(epoch), K(ls_ids));
    } else {
    }
    const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    if (cost_ts >= 1 * 1000 * 1000) {
      CLOG_LOG(WARN, "ObArbGarbageCollectService cost too much time", K(cost_ts), KPC(this));
    }
  }
}

int ObArbGarbageCollectService::add_arb_service(const common::ObAddr &arb_server,
                                                const int64_t cluster_id,
                                                const common::ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  GCMsgEpoch epoch;
  obrpc::ObArbClusterOpArg arg;
  obrpc::ObArbClusterOpResult result;
  const bool is_add_arb = true;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(try_update_role_())) {
    CLOG_LOG(WARN, "try_update_role_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(construct_rpc_epoch_(epoch)) && OB_NOT_MASTER != ret) {
    CLOG_LOG(WARN, "construct_rpc_epoch_ failed", K(ret), KPC(this));
  } else if (OB_NOT_MASTER == ret) {
    CLOG_LOG(INFO, "not master, can not add arb service", KPC(this), K(arb_server),
        K(cluster_id), K(cluster_name));
  } else if (OB_FAIL(arg.init(cluster_id, cluster_name, epoch, is_add_arb))) {
    CLOG_LOG(WARN, "ObArbGCfNotifyArg init failed", KR(ret), KPC(this));
  } else if (OB_FAIL(rpc_proxy_->to(arb_server).arb_cluster_op(arg, result))) {
    CLOG_LOG(WARN, "add_arb_service failed", KR(ret), KPC(this), K(cluster_id),
        K(cluster_name), K(arb_server), K(epoch));
  } else {
    CLOG_LOG(INFO, "add_arb_service success", KR(ret), KPC(this), K(cluster_id),
        K(cluster_name), K(arb_server), K(epoch));
  }
  return ret;
}

int ObArbGarbageCollectService::remove_arb_service(const common::ObAddr &arb_server,
                                                  const int64_t cluster_id,
                                                  const common::ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  GCMsgEpoch epoch;
  obrpc::ObArbClusterOpArg arg;
  obrpc::ObArbClusterOpResult result;
  const bool is_add_arb = false;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(try_update_role_())) {
    CLOG_LOG(WARN, "try_update_role_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(construct_rpc_epoch_(epoch)) && OB_NOT_MASTER != ret) {
    CLOG_LOG(WARN, "construct_rpc_epoch_ failed", K(ret), KPC(this));
  } else if (OB_NOT_MASTER == ret) {
    CLOG_LOG(INFO, "not master, can not remove arb service", KPC(this), K(arb_server),
        K(cluster_id), K(cluster_name));
  } else if (OB_FAIL(arg.init(cluster_id, cluster_name, epoch, is_add_arb))) {
    CLOG_LOG(WARN, "ObArbGCfNotifyArg init failed", KR(ret), KPC(this));
  } else if (OB_FAIL(rpc_proxy_->to(arb_server).arb_cluster_op(arg, result))) {
    CLOG_LOG(WARN, "remove_arb_service failed", KR(ret), KPC(this), K(cluster_id),
        K(cluster_name), K(arb_server), K(epoch));
  } else {
    CLOG_LOG(INFO, "remove_arb_service success", KR(ret), KPC(this), K(cluster_id),
        K(cluster_name), K(arb_server), K(epoch));
  }
  return ret;
}

int ObArbGarbageCollectService::construct_rpc_epoch_(GCMsgEpoch &epoch)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(role_change_lock_);
  if (!is_master_) {
    ret = OB_NOT_MASTER;
  } else {
    seq_++;
    epoch.proposal_id_ = proposal_id_;
    epoch.seq_ = seq_;
  }
  return ret;
}

// ===================================== Access InnerTable ===========================================
int ObArbGarbageCollectService::construct_ls_id_array_(TenantLSIDSArray &tenant_ls_ids_array)
{
  int ret = OB_SUCCESS;
  uint64_t max_tenant_id;
  TenantIDS tenant_ids;
  if (OB_FAIL(get_max_tenant_id_(max_tenant_id))) {
    CLOG_LOG(WARN, "get_max_tenant_id failed", KR(ret), KPC(this));
  } else if (OB_FAIL(get_tenant_ids_(tenant_ids))) {
    CLOG_LOG(WARN, "get_tenant_ids_ failed", KR(ret), KPC(this));
  } else if (OB_FAIL(construct_tenant_ls_ids_for_each_tenant_(tenant_ids, tenant_ls_ids_array))) {
    CLOG_LOG(WARN, "construct_tenant_ls_ids_for_each_tenant_ failed", KR(ret), K(tenant_ids), KPC(this));
  } else {
    tenant_ls_ids_array.set_max_tenant_id(max_tenant_id);
    CLOG_LOG(INFO, "construct_ls_id_array_ success", KR(ret), K(tenant_ls_ids_array), K(tenant_ids));
  }
  return ret;
}

int ObArbGarbageCollectService::get_max_tenant_id_(uint64_t &max_tenant_id)
{
  int ret = OB_SUCCESS;
  auto table_operator = [&max_tenant_id](common::sqlclient::ObMySQLResult *result) -> int
  {
    int ret = OB_SUCCESS;
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", max_tenant_id, uint64_t);
    return ret;
  };
  // query from __all_tenant_history
  //"select max(tenant_id) as tenant_id from __all_tenant_history";
  logservice::ObLogTableOperator table_op;
  common::ObSqlString sql_string;
  if (OB_FAIL(sql_string.assign_fmt("select max(tenant_id) as tenant_id from %s", 
          OB_ALL_TENANT_HISTORY_TNAME))) {
    CLOG_LOG(WARN, "construct sql failed", KR(ret), KPC(this));
  } else if (OB_FAIL(table_op.exec_read(OB_SYS_TENANT_ID, sql_string, *sql_proxy_, table_operator))) {
    CLOG_LOG(WARN, "exec_read failed", KR(ret));
  } else {
    CLOG_LOG(INFO, "get_max_tenant_id_ success", KR(ret), KPC(this), K(max_tenant_id));
  }
  return ret;
}

int ObArbGarbageCollectService::get_tenant_ids_(TenantIDS &tenant_ids)
{
  int ret = OB_SUCCESS;
  auto table_operator = [&tenant_ids](common::sqlclient::ObMySQLResult *result) -> int
  {
    int ret = OB_SUCCESS;
    uint64_t tenant_id;
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "EXTRACT_INT_FIELD_MYSQL failed", KR(ret));
    } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      CLOG_LOG(WARN, "push_back failed", KR(ret), K(tenant_id));
    } else {
      CLOG_LOG(INFO, "push_back tenant_id success", KR(ret), K(tenant_id));
    }
    return ret;
  };
  // query from __all_tenant
  // select distinct(tenant_id) from __all_tenant;
  // order by is import, we need keep the order
  logservice::ObLogTableOperator table_op;
  common::ObSqlString sql_string;
  if (OB_FAIL(sql_string.assign_fmt("select distinct(tenant_id) as tenant_id from %s order by tenant_id", 
          OB_ALL_TENANT_TNAME))) {
    CLOG_LOG(WARN, "construct sql failed", KR(ret), KPC(this));
  } else if (OB_FAIL(table_op.exec_read(OB_SYS_TENANT_ID, sql_string, *sql_proxy_, table_operator))) {
    CLOG_LOG(WARN, "exec_read failed", KR(ret));
  } else {
    CLOG_LOG(INFO, "get_tenant_ids_ success", KR(ret), KPC(this), K(tenant_ids));
  }
  return ret;
}

int ObArbGarbageCollectService::construct_tenant_ls_ids_for_each_tenant_(
    const TenantIDS &tenant_ids,
    TenantLSIDSArray &tenant_ls_ids_array)
{
  int ret = OB_SUCCESS;
  tenant_ls_ids_array.reset();
  const int64_t count = tenant_ids.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    uint64_t tenant_id = tenant_ids[i];
    TenantLSIDS tenant_ls_ids;
    if (OB_FAIL(get_tenant_max_ls_id_(tenant_id, tenant_ls_ids))) {
      CLOG_LOG(WARN, "get_tenant_max_ls_id_ failed", KR(ret), KPC(this), K(tenant_id));
    } else if (OB_FAIL(get_tenant_ls_ids_(tenant_id, tenant_ls_ids))) {
      CLOG_LOG(WARN, "get_tenant_ls_ids_ failed", K(tenant_id));
    } else if (OB_FAIL(tenant_ls_ids_array.push_back(tenant_ls_ids))) {
      CLOG_LOG(WARN, "push_back failed", KR(ret), KPC(this), K(tenant_id), K(tenant_ls_ids));
    } else {
      CLOG_LOG(INFO, "construct one tenant success", K(tenant_id), K(tenant_ls_ids));
    }
  }
  return ret;
}

int ObArbGarbageCollectService::get_tenant_max_ls_id_(const uint64_t tenant_id,
                                                      TenantLSIDS &tenant_ls_ids)
{
  int ret = OB_SUCCESS;

  ObAllTenantInfo tenant_info;
  int64_t ora_rowscn = 0;
  if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id,
             sql_proxy_, false /* for_update */, ora_rowscn, tenant_info))) {
    CLOG_LOG(WARN, "failed to load tenant info", KR(ret), K(tenant_id));
  } else {
    uint64_t tmp_tenant_id = tenant_info.get_tenant_id();
    ObLSID max_ls_id = tenant_info.get_max_ls_id();
    if (tmp_tenant_id != tenant_id || !max_ls_id.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "tenant_id is not same as , unexpected error!!!",
               K(tmp_tenant_id), K(tenant_id), K(max_ls_id));
    } else {
      TenantLSID tenant_ls_id(tenant_id, max_ls_id);
      tenant_ls_ids.set_max_ls_id(tenant_ls_id);
      CLOG_LOG(INFO, "get_tenant_max_ls_id_ success", KR(ret), KPC(this), K(tenant_id), K(max_ls_id));
    }
  }
  return ret;
}

int ObArbGarbageCollectService::get_tenant_ls_ids_(
    const uint64_t tenant_id,
    TenantLSIDS &tenant_ls_ids)
{
  int ret = OB_SUCCESS;
  auto table_operator = [&tenant_ls_ids](common::sqlclient::ObMySQLResult *result) -> int
  {
    int ret = OB_SUCCESS;
    int64_t ls_id;
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "EXTRACT_INT_FIELD_MYSQL failed", KR(ret));
    } else if (OB_FAIL(tenant_ls_ids.push_back(ObLSID(ls_id)))) {
      CLOG_LOG(WARN, "push_back failed", KR(ret), K(ls_id));
    } else {
      CLOG_LOG(INFO, "push_back ls_id success", KR(ret), K(ls_id));
    }
    return ret;
  };

  // quere from __all_ls_status
  // select distinct(ls_is) from __all_ls_status where tenant_id = xxx;
  common::ObSqlString sql_string;
  // NB: 
  // 1. if we want to get ls which belong to user tenant, we need execute sql on meta tenant.
  // 2. if we want to get ls which belong to meta tenant, we need execute sql on sys tenant.
  // 3. need keep the order by ls_id
  logservice::ObLogTableOperator table_op;
  const uint64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(sql_string.assign_fmt("\
          select distinct(ls_id) as ls_id from %s where tenant_id=%lu order by tenant_id, ls_id", 
          OB_ALL_LS_STATUS_TNAME, tenant_id))) {
    CLOG_LOG(WARN, "construct sql failed", KR(ret), KPC(this));
  } else if (OB_FAIL(table_op.exec_read(exec_tenant_id, sql_string, *sql_proxy_, table_operator))) {
    CLOG_LOG(WARN, "exec_read failed", KR(ret));
  } else {
    CLOG_LOG(INFO, "get_tenant_ids_ success", KR(ret), KPC(this), K(tenant_ls_ids));
  }
  return ret;
}

int ObArbGarbageCollectService::get_arb_addr_from_inner_table_(AddrS &addrs)
{
  int ret = OB_SUCCESS;
  auto table_operator = [&addrs](common::sqlclient::ObMySQLResult *result) -> int 
  {
    int ret = OB_SUCCESS;
    common::ObString arbitration_service_key;
    common::ObString arbitration_service;
    common::ObString previous_arbitration_service;
    common::ObString type;
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "arbitration_service_key", arbitration_service_key);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "arbitration_service", arbitration_service);
    GET_COL_IGNORE_NULL(result->get_varchar, "previous_arbitration_service", previous_arbitration_service);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "type", type);
    ObArbitrationServiceInfo info;
    ObArbitrationServiceType arb_type;
    common::ObAddr addr;
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "EXTRACT_VARCHAR_FIELD_MYSQL failed", KR(ret), K(arbitration_service_key),
          K(arbitration_service), K(previous_arbitration_service), K(type));
    } else if (OB_FAIL(arb_type.parse_from_string(type))) {
      CLOG_LOG(WARN, "parse_from_string failed", KR(ret), K(arbitration_service_key),
          K(arbitration_service), K(previous_arbitration_service), K(type));
    } else if (OB_FAIL(info.init(arbitration_service_key,
                                 arbitration_service,
                                 previous_arbitration_service,
                                 arb_type))) {
      CLOG_LOG(WARN, "ObArbitrationServiceInfo init failed", KR(ret), K(arbitration_service_key),
          K(arbitration_service), K(previous_arbitration_service), K(type));
    } else if (OB_FAIL(info.get_arbitration_service_addr(addr))) {
      CLOG_LOG(WARN, "get_arbitration_service_addr failed", KR(ret), K(arbitration_service_key),
          K(arbitration_service), K(previous_arbitration_service), K(type));
    } else if (OB_FAIL(addrs.push_back(addr))) {
      CLOG_LOG(WARN, "addrs push_back failed", KR(ret), K(arbitration_service_key),
          K(arbitration_service), K(previous_arbitration_service), K(type), K(addr));
    } else {
      CLOG_LOG(INFO, "addrs push_back success", KR(ret), K(arbitration_service_key),
          K(arbitration_service), K(previous_arbitration_service), K(type), K(addr));
    }
    return ret;
  };

  // query from __all_arbitration_service
  // select * from __all_arbitration_service where arbitration_service_key in (select arbitration_service_key from __all_arbitration_service group by arbitration_service);
  logservice::ObLogTableOperator table_op;
  common::ObSqlString sql_string;
  if (OB_FAIL(sql_string.assign_fmt("\
          select * from %s where arbitration_service_key in (select arbitration_service_key from %s\
          group by arbitration_service)",
          OB_ALL_ARBITRATION_SERVICE_TNAME, OB_ALL_ARBITRATION_SERVICE_TNAME))) {
    CLOG_LOG(WARN, "construct sql failed", KR(ret), KPC(this));
  } else if (OB_FAIL(table_op.exec_read(OB_SYS_TENANT_ID, sql_string, *sql_proxy_, table_operator))) {
    CLOG_LOG(WARN, "exec_read failed", KR(ret));
  } else {
    CLOG_LOG(INFO, "get_arb_addr_from_inner_table_ success", KR(ret), KPC(this), K(addrs));
  }
  return ret;
}
//======================================= InnerTable ==============================================

int ObArbGarbageCollectService::send_rpc_to_arb_server_(const AddrS &arb_addrs,
                                                        const GCMsgEpoch &epoch,
                                                        const TenantLSIDSArray &ls_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < arb_addrs.count(); i++) {
    auto &arb_addr = arb_addrs[i];
    obrpc::ObArbGCNotifyArg arg;
    obrpc::ObArbGCNotifyResult result;
    if (OB_FAIL(arg.init(epoch, ls_ids))) {
      CLOG_LOG(WARN, "ObArbGCfNotifyArg init failed", KR(ret), KPC(this));
    } else if (OB_FAIL(rpc_proxy_->to(arb_addr).arb_gc_notify(arg, result))) {
      CLOG_LOG(WARN, "send rpc to arb server", KR(ret), KPC(this), K(arb_addr), K(epoch), K(ls_ids));
    } else {
      CLOG_LOG(INFO, "send rpc to arb server success", KR(ret), KPC(this), K(arb_addr), K(epoch), K(ls_ids));
    }
  }
  return ret;
}

int ObArbGarbageCollectService::try_update_role_()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_SUCC(guard.switch_to(OB_SYS_TENANT_ID))) {
    logservice::ObLogService *log_srv = MTL(logservice::ObLogService*);
    ObLSID ls_id(ObLSID::SYS_LS_ID);
    palf::PalfHandleGuard palf_handle;
    common::ObRole role = INVALID_ROLE;
    bool is_pending_state = false;
    if (OB_ISNULL(log_srv)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "log_srv is nullptr, unexpected error", KR(ret), KPC(this));
    } else if (OB_FAIL(log_srv->open_palf(ls_id, palf_handle))) {
      ObSpinLockGuard guard(role_change_lock_);
      proposal_id_ = palf::INVALID_PROPOSAL_ID;
      is_master_ = false;
      CLOG_LOG(TRACE, "open_palf failed", KR(ret), KPC(this), K(ls_id));
    } else if (OB_FAIL(palf_handle.get_role(role, proposal_id, is_pending_state))) {
      CLOG_LOG(WARN, "get_role failed", KR(ret), KPC(this), K(ls_id), K(role), K(proposal_id), K(is_pending_state));
    } else {
      is_leader = (common::LEADER == role && false == is_pending_state);
    }
  }
  // do not set seq_ in here
  if (is_leader) {
    ObSpinLockGuard guard(role_change_lock_);
    proposal_id_ = proposal_id;
    is_master_ = true;
  } else {
    ObSpinLockGuard guard(role_change_lock_);
    proposal_id_ = palf::INVALID_PROPOSAL_ID;
    is_master_ = false;
  }
  return ret;
}
} // end namespace arbserver
} // end namesapce oceanbase
