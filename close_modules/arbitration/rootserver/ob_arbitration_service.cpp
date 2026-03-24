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

#define USING_LOG_PREFIX RS
#include "ob_arbitration_service.h"
#include "share/ob_service_epoch_proxy.h"     // for ObServiceEpochProxy
#include "logservice/ob_log_service.h"        // for ObLogService
#include "share/arbitration_service/ob_arbitration_service_utils.h"          // for Utils

namespace oceanbase
{
using namespace share;
namespace rootserver
{

int ObArbitrationService::init()
{
  int ret = OB_SUCCESS;
  if (is_user_tenant(MTL_ID())) {
    // do nothing
  } else if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(ObTenantThreadHelper::create(
                         "ArbSer",
                         lib::TGDefIDs::SimpleLSService,
                         *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    service_epoch_ = 0;
    sql_proxy_ = GCTX.sql_proxy_;
    inited_ = true;
    LOG_INFO("[ARB_SERVICE] ObArbitrationService init", K_(tenant_id), KP(this));
  }
  return ret;
}

int ObArbitrationService::start()
{
  int ret = OB_SUCCESS;
  if (is_user_tenant(MTL_ID())) {
    // do nothing
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("arb service is not init", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(rootserver::ObTenantThreadHelper::start())) {
    LOG_WARN("ObArbitrationService start thread failed", KR(ret), KP(this));
  } else {
    LOG_INFO("[ARB_SERVICE] ObArbitrationService start", K_(tenant_id), KP(this));
  }
  return ret;
}

void ObArbitrationService::destroy()
{
  if (is_user_tenant(MTL_ID())) {
    // no nothing
  } else {
    tenant_id_ = OB_INVALID_TENANT_ID;
    service_epoch_ = 0;
    sql_proxy_ = nullptr;
    ObTenantThreadHelper::destroy();
    task_infos_.reset();
    inited_ = false;
    LOG_INFO("[ARB_SERVICE] ObArbitrationService destroy", K_(tenant_id), KP(this));
  }
}

int ObArbitrationService::wait_data_version_update_to_newest_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_UNLIKELY(is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arbitration service must run on sys or meta tenant", KR(ret), K_(tenant_id));
  } else {
    int64_t idle_time_us = IDLE_TIME_US;
    bool data_version_is_newest = false;
    while (!has_set_stop() && !data_version_is_newest) {
      ObCurTraceId::init(GCONF.self_addr_);
      uint64_t data_version = 0;
      // 1. check sys tenant data version
      if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
        LOG_WARN("fail to get sys tenant data version", KR(ret), K(data_version));
      } else if (DATA_VERSION_4_1_0_0 > data_version) {
        // do nothing
        LOG_TRACE("arb service can not run with sys tenant data version lower than 4_1_0_0",
                  K(data_version));
      // 2. check meta tenant data version
      } else if (is_meta_tenant(tenant_id_)
                 && OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("fail to get meta tenant data version", KR(ret), K_(tenant_id), K(data_version));
      } else if (is_meta_tenant(tenant_id_)
                 && DATA_VERSION_4_1_0_0 > data_version) {
        // do nothing
        LOG_TRACE("arb service can not run with meta tenant data version lower than 4_1_0_0",
                  K_(tenant_id), K(data_version));
      // 3. check user tenant data version
      } else if (is_meta_tenant(tenant_id_)
                 && OB_FAIL(GET_MIN_DATA_VERSION(gen_user_tenant_id(tenant_id_), data_version))) {
        LOG_WARN("fail to get meta tenant data version", KR(ret),
                 "tenant_id", gen_user_tenant_id(tenant_id_), K(data_version));
      } else if (is_meta_tenant(tenant_id_)
                 && DATA_VERSION_4_1_0_0 > data_version) {
        // do nothing
        LOG_TRACE("arb service can not run with user tenant data version lower than 4_1_0_0",
                  "tenant_id", gen_user_tenant_id(tenant_id_), K(data_version));
      } else {
        data_version_is_newest = true;
      }
      idle(idle_time_us);
    }
    if (!data_version_is_newest) {
      ret = OB_IN_STOP_STATE;
      LOG_WARN("data version is lower than 4.1 and thread stopped", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

void ObArbitrationService::do_work()
{
  int ret = OB_SUCCESS;
  bool tenant_schema_is_ready = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_UNLIKELY(is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arbitration service must run on sys or meta tenant", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(wait_data_version_update_to_newest_())) {
    LOG_WARN("fail to wait data version updated over 4.1", KR(ret), K_(tenant_id));
  } else {
    int64_t idle_time_us = IDLE_TIME_US;
    while (!has_set_stop()) {
      idle_time_us = IDLE_TIME_US;
      ObCurTraceId::init(GCONF.self_addr_);
      bool can_do_service = false;
      if (OB_FAIL(check_tenant_schema_is_ready_(tenant_schema_is_ready))) {
        LOG_WARN("check tenant schema failed", KR(ret), K_(tenant_id), K(tenant_schema_is_ready));
      } else if (!tenant_schema_is_ready) {
        ret = OB_NEED_WAIT;
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) { // 10s
          LOG_WARN("tenant schema is not ready, need wait", KR(ret), K_(tenant_id));
        }
      } else if (OB_FAIL(load_and_update_service_epoch_(can_do_service))) {
        LOG_WARN("fail to update service epoch", KR(ret), K(can_do_service));
      } else if (!can_do_service) {
        // do nothing
        idle_time_us = IDLE_TIME_US;
      } else {
        if (OB_FAIL(deal_with_task_in_table_(tenant_id_))) {
          LOG_WARN("fail to deal with tasks in table", KR(ret), K_(tenant_id));
        } else if (is_meta_tenant(tenant_id_)
                   && OB_FAIL(deal_with_task_in_table_(gen_user_tenant_id(tenant_id_)))) {
          LOG_WARN("fail to deal with tasks in table", KR(ret),
                   "tenant_id", gen_user_tenant_id(tenant_id_));
        } else if (OB_FAIL(do_tenant_arbitration_service_task_(tenant_id_))) {
          LOG_WARN("fail to do tenant arbitration service tasks", KR(ret), K_(tenant_id));
        } else if (is_meta_tenant(tenant_id_)
                   && OB_FAIL(do_tenant_arbitration_service_task_(gen_user_tenant_id(tenant_id_)))) {
          LOG_WARN("fail to do tenant arbitration service tasks", KR(ret),
                   "tenant_id", gen_user_tenant_id(tenant_id_));
        } else if (OB_FAIL(promote_new_tenant_arbitration_service_status_())) {
          LOG_WARN("fail to promote tenant arbitration service status", KR(ret), K_(tenant_id));
        }
      }

      if (OB_FAIL(ret)) {
        idle_time_us = BUSY_IDLE_TIME_US;
      }
      idle(idle_time_us);
      FLOG_INFO("[ARB_SERVICE] arbitration service finish one round", KR(ret),
                K(idle_time_us), K_(tenant_id));
    }// end while
  }
}

int ObArbitrationService::check_tenant_schema_is_ready_(bool &is_ready)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  const ObSimpleTenantSchema *tenant_schema = NULL;
  is_ready = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    is_ready = false;
  } else if (tenant_schema->is_normal()) {
    is_ready = true;
  }
  return ret;
}

int ObArbitrationService::load_and_update_service_epoch_(bool &can_do_service)
{
  int ret = OB_SUCCESS;
  can_do_service = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_));
  } else {
    MTL_SWITCH(tenant_id_) {
      ObLSService *ls_svr = nullptr;
      ObLSHandle ls_handle;
      palf::PalfHandleGuard palf_handle_guard;
      logservice::ObLogService *log_service = nullptr;
      common::ObRole role = FOLLOWER;
      int64_t proposal_id = 0;
      int64_t service_epoch_in_table = 0;
      int64_t affected_rows = 0;
      ObMySQLTransaction trans;
      if (OB_UNLIKELY(is_user_tenant(tenant_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tenant id", KR(ret), K_(tenant_id));
      } else if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLSService is null", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(ls_svr->get_ls(SYS_LS, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls handle failed", KR(ret), K_(tenant_id));
      } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(log_service->open_palf(SYS_LS, palf_handle_guard))) {
        LOG_WARN("open palf failed", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
        LOG_WARN("get role failed", KR(ret), K(tenant_id_));
      } else if (common::ObRole::LEADER != role) {
        can_do_service = false;
        LOG_INFO("[ARB_SERVICE] can not do service, because not leader", K(tenant_id_), K(role));
      } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
        LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(ObServiceEpochProxy::select_service_epoch_for_update(
                             trans,
                             tenant_id_,
                             ObServiceEpochProxy::ARBITRATION_SERVICE_EPOCH,
                             service_epoch_in_table))) {
        LOG_WARN("fail to get service epoch from inner table", KR(ret), K_(tenant_id));
      } else {
        if (proposal_id < service_epoch_in_table) {
          can_do_service = false;
          LOG_INFO("[ARB_SERVICE] can not do service, because leader epoch is not newest",
                   K(tenant_id_), K(role), K(proposal_id), K(service_epoch_in_table));
        } else if (proposal_id == service_epoch_in_table) {
          can_do_service = true;
        } else if (OB_FAIL(ObServiceEpochProxy::update_service_epoch(
                               trans,
                               tenant_id_,
                               ObServiceEpochProxy::ARBITRATION_SERVICE_EPOCH,
                               proposal_id,
                               affected_rows))) {
          // if proposal_id > service_epoch then update service epoch to newest one
          LOG_WARN("fail to update service epoch to newest", KR(ret), K(tenant_id_),
                   K(proposal_id), K(affected_rows));
        } else if (affected_rows != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect update one row", KR(ret), K(tenant_id_), K(proposal_id),
                   K(service_epoch_in_table), K(affected_rows));
        } else {
          can_do_service = true;
        }
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        service_epoch_ = proposal_id;
      }
    }
  }
  return ret;
}

int ObArbitrationService::deal_with_task_in_table_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool lock_line = false;
  task_infos_.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(task_table_operator_.get_all_tasks(*sql_proxy_, tenant_id, lock_line, task_infos_))) {
    LOG_WARN("fail to get tasks in table", KR(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(task, task_infos_, OB_SUCC(ret)) {
      ObArbitrationServiceStatus current_arb_service_status;
      bool need_execute = false;
      int ret_code = OB_SUCCESS;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(task)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("task is null", KR(ret), KP(task));
      } else if (OB_FAIL(ObArbitrationServiceUtils::get_tenant_arbitration_service_status(
                             task->get_tenant_id(),
                             current_arb_service_status))) {
        LOG_WARN("fail to get tenant arbitration service status", KR(ret), K(tenant_id), K(current_arb_service_status));
      } else if (OB_FAIL(check_task_still_need_execute_(*task, current_arb_service_status, need_execute))) {
        LOG_WARN("fail to check task still need execute", KR(ret), KPC(task), K(current_arb_service_status));
      } else if (!need_execute) {
        // remove it from table in trans and lock service_epoch, in case leader switched
        ret_code = OB_CANCELED;
        if (OB_FAIL(finish_task_(*sql_proxy_, *task, ret_code))) {
          LOG_WARN("fail to finish task", KR(ret), KPC(task), K(ret_code));
        }
        LOG_INFO("[ARB_TASK] no need to execute this task", KR(ret), KPC(task), K(current_arb_service_status));
      } else if (OB_FAIL(execute_task_(*task, current_arb_service_status))) {
        LOG_WARN("[ARB_TASK] fail to execute task", KR(ret), KPC(task), K(current_arb_service_status));
      }
    }
  }
  return ret;
}

int ObArbitrationService::finish_task_(
    common::ObISQLClient &sql_proxy,
    const ObArbitrationServiceReplicaTaskInfo &task_info,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  int64_t service_epoch_in_table = 0;
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!task_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_info), K(ret_code));
  } else if (OB_FAIL(trans.start(&sql_proxy, tenant_id_))) {
    LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_service_epoch_(trans))) {
    LOG_WARN("fail to check service epoch", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(task_table_operator_.remove(trans, task_info.get_tenant_id(), task_info.get_ls_id()))) {
    LOG_WARN("fail to remove task from table", KR(ret), K(task_info));
  } else if (OB_FAIL(task_table_operator_.insert_history(trans, task_info, ret_code))) {
    LOG_WARN("fail to insert into history table", KR(ret), K(task_info), K(ret_code));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObArbitrationService::check_service_epoch_(
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  int64_t service_epoch_in_table = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_FAIL(ObServiceEpochProxy::select_service_epoch_for_update(
                         sql_proxy,
                         tenant_id_,
                         ObServiceEpochProxy::ARBITRATION_SERVICE_EPOCH,
                         service_epoch_in_table))) {
    LOG_WARN("fail to load service epoch in table", KR(ret), K_(tenant_id));
  } else if (service_epoch_in_table != service_epoch_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not current leader, can not provide service", KR(ret), K_(tenant_id),
             K(service_epoch_in_table), K_(service_epoch));
  }
  return ret;
}

int ObArbitrationService::check_task_still_need_execute_(
    const ObArbitrationServiceReplicaTaskInfo &task_info,
    const ObArbitrationServiceStatus &current_arb_service_status,
    bool &need_execute)
{
  int ret = OB_SUCCESS;
  need_execute = false;
  // try to check whether task can execute under these rules:
  // 1. if current_arb_service_status is disable like
  //    1.1 for remove replica task
  //        can execute
  //    1.2 for add replica task
  //        can not execute
  // 2. if current_arb_service_status is enable like
  //    2.1 for remove replica task
  //        can execute only when arb_service in task_info is not the same with setted arb_service
  //    2.2 for add replica task
  //        can execute only when arb_service in task_info is the same with setted arb_service
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!task_info.is_valid() || !current_arb_service_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_info), K(current_arb_service_status));
  } else {
    const ObString arbitration_service_key("default");
    const bool lock_line = false;
    ObArbitrationServiceInfo arb_service_info;
    // current_arb_service_status is disabling or disabled
    if (current_arb_service_status.is_disable_like()) {
      if (task_info.get_task_type().is_remove_task()) {
        // case 1.1 remove task is allowed when status is disable like
        need_execute = true;
      } else {
        // case 1.2 add task is not allowed when status is disable like
        need_execute = false;
      }
    // current_arb_service_status is enabling or enabled
    } else if (OB_FAIL(arbitration_service_table_operator_.get(
                           *sql_proxy_,
                           arbitration_service_key,
                           lock_line,
                           arb_service_info))) {
      LOG_WARN("fail to get arbitration service info", KR(ret), K(arbitration_service_key),
               K(lock_line), K(arb_service_info));
    } else if (task_info.get_task_type().is_remove_task()
               && 0 != arb_service_info.get_arbitration_service_string().case_compare(
                  task_info.get_arbitration_service_string())) {
      // case 2.1 remove task is allowed when status is enable like and current A-replica is not the one wanted
      need_execute = true;
    } else if (task_info.get_task_type().is_add_task()
               && 0 == arb_service_info.get_arbitration_service_string().case_compare(
                  task_info.get_arbitration_service_string())) {
      // case 2.2 add task is allowed when status is enable like and the added A-replica is the one wanted
      need_execute = true;
    }
  }
  return ret;
}

int ObArbitrationService::execute_task_(
    const ObArbitrationServiceReplicaTaskInfo &task_info,
    const ObArbitrationServiceStatus &arbitration_service_status)
{
  int ret = OB_SUCCESS;
  ObAddr leader_addr;
  DEBUG_SYNC(BEFORE_EXECUTE_ARB_REPLICA_TASK);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!task_info.is_valid()
                         || !arbitration_service_status.is_valid()
                         || (task_info.get_tenant_id() != tenant_id_
                             && task_info.get_tenant_id() != gen_user_tenant_id(tenant_id_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_info), K(arbitration_service_status));
  } else if (OB_FAIL(ObArbitrationServiceUtils::get_leader_from_log_stat_table(
                         task_info.get_tenant_id(),
                         task_info.get_ls_id(),
                         leader_addr))) {
    LOG_WARN("fail to get leader from log stat table", KR(ret), K(task_info));
  } else {
    if (task_info.get_task_type().is_add_task()) {
      // is a add replica task
      if (OB_FAIL(do_execute_add_replica_task_(leader_addr, task_info, arbitration_service_status))) {
        LOG_WARN("fail to execute a add replica task", KR(ret), K(task_info), K(leader_addr), K(arbitration_service_status));
      } else {
        LOG_INFO("[ARB_TASK] success to execute a add replica task", K(task_info), K(leader_addr), K(arbitration_service_status));
      }
    } else {
      // is a remove replica task
      if (OB_FAIL(do_execute_remove_replica_task_(leader_addr, task_info, arbitration_service_status))) {
        LOG_WARN("fail to execute a remove replica task", KR(ret), K(task_info), K(leader_addr), K(arbitration_service_status));
      } else {
        LOG_INFO("[ARB_TASK] success to execute a remove replica task", K(task_info), K(leader_addr), K(arbitration_service_status));
      }
    }
  }
  return ret;
}

int ObArbitrationService::do_execute_add_replica_task_(
    const ObAddr &leader_addr,
    const ObArbitrationServiceReplicaTaskInfo &task_info,
    const ObArbitrationServiceStatus &arbitration_service_status)
{
  int ret = OB_SUCCESS;
  bool service_epoch_is_newest = false;
  ObAddr arb_service_to_add;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)
             || OB_ISNULL(sql_proxy_)
             || OB_UNLIKELY(!task_info.is_valid()
             || (task_info.get_tenant_id() != tenant_id_
                 && task_info.get_tenant_id() != gen_user_tenant_id(tenant_id_))
             || !task_info.get_task_type().is_add_task()
             || !leader_addr.is_valid()
             || !arbitration_service_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_info), K(leader_addr),
             KP(GCTX.srv_rpc_proxy_), KP(sql_proxy_), K(arbitration_service_status));
  } else if (OB_FAIL(task_info.get_arbitration_service_addr(arb_service_to_add))) {
    LOG_WARN("fail to get arb service addr", KR(ret), K(task_info));
  } else {
    ObAddArbArg add_arg;
    ObAddArbResult add_result;
    int64_t timestamp = 1;
    ObMember arb_member_to_add = ObMember(arb_service_to_add, timestamp);
    int64_t timeout = get_sync_rpc_timeout_();
    int ret_code = OB_SUCCESS;
    ObMySQLTransaction trans;
    int64_t service_epoch_in_table = 0;
    ObArbitrationServiceStatus current_arb_service_status;
    if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(check_service_epoch_(trans))) {
      LOG_WARN("fail to check service epoch", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObArbitrationServiceUtils::get_tenant_arbitration_service_status(
                           task_info.get_tenant_id(),
                           current_arb_service_status))) {
      LOG_WARN("fail to get arbitration service status", KR(ret), K(task_info));
    } else if (current_arb_service_status != arbitration_service_status) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("add task should not execute with arbitration service is disable like",
                KR(ret), K(current_arb_service_status), K(arbitration_service_status), K(task_info));
    } else if (OB_FAIL(add_arg.init(
                           task_info.get_tenant_id(),
                           task_info.get_ls_id(),
                           arb_member_to_add,
                           timeout))) {
      LOG_WARN("fail to init add arbitration service replica arg", KR(ret),
               K(task_info), K(arb_member_to_add), K(timeout));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    if (OB_FAIL(ret)){
    } else if (OB_SUCCESS != (ret_code = GCTX.srv_rpc_proxy_->to(leader_addr)
                                                              .by(task_info.get_tenant_id())
                                                              .timeout(timeout)
                                                              .add_arb(add_arg, add_result))) {
      LOG_WARN("fail to add arbitration service replica", KR(ret_code), K(leader_addr), K(task_info), K(add_arg));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(finish_task_(*sql_proxy_, task_info, ret_code))) {
      LOG_WARN("fail to finish task", KR(ret), K(task_info), K(ret_code));
    }
  }
  return ret;
}

int ObArbitrationService::do_execute_remove_replica_task_(
    const ObAddr &leader_addr,
    const ObArbitrationServiceReplicaTaskInfo &task_info,
    const ObArbitrationServiceStatus &arbitration_service_status)
{
  int ret = OB_SUCCESS;
  bool service_epoch_is_newest = false;
  ObAddr arb_service_to_remove;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)
             || OB_UNLIKELY(!task_info.is_valid()
             || !task_info.get_task_type().is_remove_task()
             || !leader_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_info), K(leader_addr), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(task_info.get_arbitration_service_addr(arb_service_to_remove))) {
    LOG_WARN("fail to get arb service addr", KR(ret), K(task_info));
  } else {
    ObRemoveArbArg remove_arg;
    ObRemoveArbResult remove_result;
    int64_t timestamp = 1;
    ObMember arb_member_to_remove = ObMember(arb_service_to_remove, timestamp);
    int64_t timeout = get_sync_rpc_timeout_();
    int ret_code = OB_SUCCESS;
    ObMySQLTransaction trans;
    int64_t service_epoch_in_table = 0;
    ObArbitrationServiceStatus current_arb_service_status;
    if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(check_service_epoch_(trans))) {
      LOG_WARN("fail to check service epoch", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObArbitrationServiceUtils::get_tenant_arbitration_service_status(
                           task_info.get_tenant_id(),
                           current_arb_service_status))) {
      LOG_WARN("fail to get arbitration service status", KR(ret), K(task_info));
    } else if (current_arb_service_status != arbitration_service_status) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("add task should not execute with arbitration service is disable like",
                KR(ret), K(current_arb_service_status), K(arbitration_service_status), K(task_info));
    } else if (OB_FAIL(remove_arg.init(
                           task_info.get_tenant_id(),
                           task_info.get_ls_id(),
                           arb_member_to_remove,
                           timeout))) {
      LOG_WARN("fail to init remove arbitration service replica arg", KR(ret),
               K(task_info), K(arb_member_to_remove), K(timeout));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }

    if (OB_FAIL(ret)){
    } else if (OB_SUCCESS != (ret_code = GCTX.srv_rpc_proxy_->to(leader_addr)
                                                              .by(task_info.get_tenant_id())
                                                              .timeout(timeout)
                                                              .remove_arb(remove_arg, remove_result))) {
      LOG_WARN("fail to remove arbitration service replica", KR(ret_code), K(leader_addr), K(task_info), K(remove_arg));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(finish_task_(*sql_proxy_, task_info, ret_code))) {
      LOG_WARN("fail to finish task", KR(ret), K(task_info), K(ret_code));
    }
  }
  return ret;
}

int ObArbitrationService::do_tenant_arbitration_service_task_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::ObLSStatusOperator ls_status_operator;
  common::ObArray<share::ObLSStatusInfo> ls_status_info_array;
  ObArbitrationServiceStatus current_arb_service_status;
  FLOG_INFO("[ARB_TENANT] start to do tenant arbitration replica task", K(tenant_id));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order(tenant_id, ls_status_info_array, *sql_proxy_))) {
    LOG_WARN("fail to get all ls status", KR(ret), K(tenant_id));
  } else if (ls_status_info_array.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_status_info_array has no member", KR(ret), K(tenant_id), K(ls_status_info_array));
  } else if (OB_FAIL(ObArbitrationServiceUtils::get_tenant_arbitration_service_status(
                         tenant_id,
                         current_arb_service_status))) {
    LOG_WARN("fail to get tenant arbitration service status", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_info_array.count(); ++i) {
      share::ObLSStatusInfo ls_status_info = ls_status_info_array.at(i);
      if (OB_FAIL(do_ls_arbitration_service_task_(
                      tenant_id,
                      ls_status_info,
                      current_arb_service_status))) {
        LOG_WARN("fail to do ls arbitration service task", KR(ret), K(tenant_id), K(i), K(ls_status_info), K(current_arb_service_status));
      }
    }
  }
  return ret;
}

int ObArbitrationService::do_ls_arbitration_service_task_(
    const uint64_t tenant_id,
    const ObLSStatusInfo &ls_status_info,
    const ObArbitrationServiceStatus &current_arb_service_status)
{
  int ret = OB_SUCCESS;
  ObString arbitration_service_key("default");
  ObArbitrationServiceInfo arb_info;
  bool need_execute = false;
  ObSqlString current_arb_member;
  FLOG_INFO("[ARB_TENANT] start to do ls arbitration replica task",
             K(tenant_id), K(ls_status_info), K(current_arb_service_status));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)
             || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || !ls_status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_status_info), KP(sql_proxy_));
  } else if (ls_status_info.ls_is_creating()) {
    FLOG_INFO("this log stream is creating, do nothing", KR(ret), K(tenant_id), K(ls_status_info));
  } else if (OB_FAIL(arbitration_service_table_operator_.get(
                         *sql_proxy_,
                         arbitration_service_key,
                         false/*lock_line*/,
                         arb_info))) {
    if (OB_ARBITRATION_SERVICE_NOT_EXIST == ret) {
      // if arb service not exist, make sure there are no remained replicas
      LOG_TRACE("arbitration service info not exists", KR(ret), K(arbitration_service_key));
      ret = OB_SUCCESS;
      if (OB_FAIL(clean_remained_replicas_without_arb_service_(
                      tenant_id,
                      ls_status_info.ls_id_,
                      current_arb_service_status))) {
        LOG_WARN("fail to clean remained A-replicas", KR(ret), K(tenant_id),
                 "ls_id", ls_status_info.ls_id_.id(), K(current_arb_service_status));
      }
    } else {
      LOG_WARN("fail to get arbitration service info", KR(ret), K(arbitration_service_key));
    }
  } else if (OB_FAIL(ObArbitrationServiceUtils::get_arb_member(
                         tenant_id,
                         ls_status_info.ls_id_,
                         current_arb_member))) {
    LOG_WARN("fail to get arb member from log stat table", KR(ret), K(tenant_id), "ls_id",
             ls_status_info.ls_id_);
  } else {
    FLOG_INFO("[ARB_TASK] check whether need to generate task", K(tenant_id),
              K(current_arb_member), K(current_arb_service_status),
              K(arb_info));
    // we need to remove arb service replica when all conditions below satisfied
    // (1) current arb service replica is valid
    // (2) arb service status is disable like
    //     or setted arb service is not the same with current one
    if (!current_arb_member.empty()
       && (current_arb_service_status.is_disable_like()
           || !arb_info.arbitration_service_is_equal_to_string(current_arb_member.string()))) {
      FLOG_INFO("[ARB_TASK] find a remove replica task to build", K(tenant_id),
                K(ls_status_info), K(current_arb_service_status), K(current_arb_member),
                K(arb_info));
      if (OB_FAIL(construct_and_execute_remove_replica_task_(
                      tenant_id,
                      ls_status_info.ls_id_,
                      current_arb_member.string(),
                      current_arb_service_status))) {
        LOG_WARN("[ARB_TASK] fail to construct and execute remove replica task",
                 KR(ret), K(tenant_id), K(ls_status_info), K(current_arb_member), K(current_arb_service_status));
      }
    }

    // we need to add arb service replica when all conditions below satisfied
    // (1) arb service status is enable like
    // (2) current arb service is null
    //     or setted arb service is not the same with current one
    if (OB_FAIL(ret)) {
    } else if (current_arb_service_status.is_enable_like()
               && (current_arb_member.empty()
                   || !arb_info.arbitration_service_is_equal_to_string(current_arb_member.string()))) {
      FLOG_INFO("[ARB_TASK] find a add replica task to build", K(tenant_id),
                K(ls_status_info), K(current_arb_service_status), K(current_arb_member),
                K(arb_info));
      if (OB_FAIL(construct_and_execute_add_replica_task_(
                      tenant_id,
                      ls_status_info.ls_id_,
                      arb_info.get_arbitration_service_string(),
                      current_arb_service_status))) {
        LOG_WARN("[ARB_TASK] fail to construct and execute add replica task", KR(ret),
                 K(tenant_id), K(ls_status_info), K(arb_info), K(current_arb_service_status));
      }
    }
  }
  return ret;
}

int ObArbitrationService::clean_remained_replicas_without_arb_service_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObArbitrationServiceStatus &current_arb_service_status)
{
  int ret = OB_SUCCESS;
  ObSqlString current_arb_member;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)
             || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || !ls_id.is_valid_with_tenant(tenant_id)
             || !current_arb_service_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_), K(tenant_id), K(ls_id), K(current_arb_service_status));
  } else if (OB_FAIL(ObArbitrationServiceUtils::get_arb_member(
                         tenant_id,
                         ls_id,
                         current_arb_member))) {
    LOG_WARN("fail to get arb member from log stat table", KR(ret), K(tenant_id), K(ls_id));
  } else if (current_arb_member.empty()) {
    // ignore ret and do nothing
    LOG_TRACE("log stream has no valid arb member, no need to remove",
              K(tenant_id), K(ls_id), K(current_arb_member));
  } else if (OB_FAIL(construct_and_execute_remove_replica_task_(
                         tenant_id,
                         ls_id,
                         current_arb_member.string(),
                         current_arb_service_status))) {
    LOG_WARN("[ARB_TASK] fail to construct and execute remove replica task", KR(ret), K(tenant_id),
             K(ls_id), K(current_arb_member), K(current_arb_service_status));
  }
  return ret;
}

int ObArbitrationService::construct_and_execute_remove_replica_task_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObString &arb_service,
    const ObArbitrationServiceStatus &arb_service_status)
{
  int ret = OB_SUCCESS;
  ObArbitrationServiceReplicaTaskInfo remove_task;
  const ObArbitrationServiceReplicaTaskType task_type(ObArbitrationServiceReplicaTaskType::REMOVE_REPLICA);
  const ObArbitrationServiceType arb_service_type(ObArbitrationServiceType::ADDR);
  ObCurTraceId::TraceId trace_id = *common::ObCurTraceId::get_trace_id();
  int64_t task_id = ObTimeUtility::current_time();
  ObMySQLTransaction trans;
  int64_t service_epoch_in_table = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)
             || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || !ls_id.is_valid_with_tenant(tenant_id)
             || arb_service.empty()
             || !arb_service_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_), K(tenant_id),
             K(ls_id), K(arb_service_status), K(arb_service));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_service_epoch_(trans))) {
    LOG_WARN("fail to check service epoch", KR(ret), K_(tenant_id));
  //TODO: after transfer branch merged into master, use this instead of gen_taks_id_
  //} else if (OB_FAIL(MTL(transaction::ObUniqueIDService*)->gen_unique_id(task_id))) {
  //  LOG_WARN("gen_unique_id failed", KR(ret), K(task_id), K_(tenant_id));
  } else if (OB_FAIL(remove_task.build(
                      ObTimeUtility::current_time(), /*create_time*/
                      tenant_id,
                      ls_id,
                      task_id,
                      task_type,
                      trace_id,
                      arb_service,
                      arb_service_type,
                      "remove arbitration replica service replica"))) {
    LOG_WARN("fail to build a remove arb replica task", KR(ret), K(tenant_id), K(ls_id),
             K(task_id), K(task_type), K(trace_id), K(arb_service), K(arb_service_type));
  // no need to check arb service status, baceuse remove task can execute under all status
  // for disbaling/disabled status, it is surely permiited to execute a remove task
  // for enabling/enabled status, a arb service addr could be changed, thus triggering replacement
  // if current arb member is different from the new setted one, a remove task can be executed
  } else if (OB_FAIL(task_table_operator_.insert(trans, remove_task))) {
    LOG_WARN("fail to persist task info into table", KR(ret), K(remove_task));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(execute_task_(remove_task, arb_service_status))) {
    LOG_WARN("fail to execute task", KR(ret), K(tenant_id), K(ls_id),
             K(remove_task), K(arb_service_status));
  }
  return ret;
}

int ObArbitrationService::construct_and_execute_add_replica_task_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObString &arb_service,
    const ObArbitrationServiceStatus &arb_service_status)
{
  int ret = OB_SUCCESS;
  bool service_epoch_is_newest  = false;
  ObArbitrationServiceReplicaTaskInfo add_task;
  const ObArbitrationServiceReplicaTaskType task_type(ObArbitrationServiceReplicaTaskType::ADD_REPLICA);
  const ObArbitrationServiceType arb_service_type(ObArbitrationServiceType::ADDR);
  ObCurTraceId::TraceId trace_id = *common::ObCurTraceId::get_trace_id();
  int64_t task_id = ObTimeUtility::current_time();
  int64_t service_epoch_in_table = 0;
  ObArbitrationServiceStatus current_arb_service_status;
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)
             || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || !ls_id.is_valid_with_tenant(tenant_id)
             || arb_service.empty()
             || !arb_service_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(sql_proxy_), K(arb_service), K(tenant_id), K(ls_id), K(arb_service_status));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_service_epoch_(trans))) {
    LOG_WARN("fail to check service epoch", KR(ret), K_(tenant_id));
  //TODO: after transfer branch merged into master, use this instead of gen_taks_id_
  //} else if (OB_FAIL(MTL(transaction::ObUniqueIDService*)->gen_unique_id(task_id))) {
  //  LOG_WARN("gen_unique_id failed", KR(ret), K(task_id), K_(tenant_id));
  } else if (OB_FAIL(add_task.build(
                      ObTimeUtility::current_time(), /*create_time*/
                      tenant_id,
                      ls_id,
                      task_id,
                      task_type,
                      trace_id,
                      arb_service,
                      arb_service_type,
                      "add arbitration replica service replica"))) {
    LOG_WARN("fail to build a add arb replica task", KR(ret), K(tenant_id), K(ls_id), K(task_id),
             K(task_type), K(trace_id), K(arb_service), K(arb_service_type));
  } else if (OB_FAIL(ObArbitrationServiceUtils::get_tenant_arbitration_service_status(
                         tenant_id, current_arb_service_status))) {
    LOG_WARN("fail to get tenant current arb service status", KR(ret), K(tenant_id));
  // need to ensure current arb service status is not disable like
  // because in a disbale like status, a add replica is not allowed to execute
  } else if (current_arb_service_status != arb_service_status
             || current_arb_service_status.is_disable_like()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("arb service status not expected", KR(ret), K(current_arb_service_status), K(arb_service_status));
  } else if (OB_FAIL(task_table_operator_.insert(trans, add_task))) {
    LOG_WARN("fail to persist task info into table", KR(ret), K(add_task));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(execute_task_(add_task, arb_service_status))) {
    LOG_WARN("fail to execute task", KR(ret), K(tenant_id), K(ls_id),
             K(add_task), K(arb_service_status));
  }
  return ret;
}

int ObArbitrationService::promote_new_tenant_arbitration_service_status_()
{
  // STEP 1. lock service epoch in __all_service_epoch
  // STEP 2. get current arb service status for this tenant
  // STEP 3. get remained tasks in table
  // STEP 4. check whether need to promote arb service status
  int ret = OB_SUCCESS;
  ObArbitrationServiceStatus current_status;
  ObArbitrationServiceStatus user_status;
  bool can_do_promote = false;
  ObMySQLTransaction trans;
  task_infos_.reset();
  LOG_INFO("[ARB_TENANT] try to check whether to promote tenant arbitration service status", K_(tenant_id));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.rs_rpc_proxy_));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
    // STEP 1. lock service epoch in __all_service_epoch
  } else if (OB_FAIL(check_service_epoch_(trans))) {
    LOG_WARN("fail to check service epoch", KR(ret), K_(tenant_id));
    // STEP 2. get current arb service status for this tenant
  } else if (OB_FAIL(ObArbitrationServiceUtils::get_tenant_arbitration_service_status(
                           tenant_id_,
                           current_status))) {
    LOG_WARN("fail to get tenant arbitration service current status", KR(ret), K_(tenant_id), K(current_status));
  } else if (is_meta_tenant(tenant_id_)
             && OB_FAIL(ObArbitrationServiceUtils::get_tenant_arbitration_service_status(
                            gen_user_tenant_id(tenant_id_),
                            user_status))) {
    LOG_WARN("fail to get tenant arbitration service current status", KR(ret), K_(tenant_id), K(user_status));
  } else if (is_meta_tenant(tenant_id_) && user_status != current_status) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user tenant arb service status is different from meta tenant", KR(ret),
             K_(tenant_id), K(user_status), K(current_status));
    // STEP 3. get remained tasks in table
  } else if (OB_FAIL(task_table_operator_.get_all_tasks(
                         trans,
                         tenant_id_,
                         false/*lock_line*/,
                         task_infos_))) {
    LOG_WARN("fail to get task info in table", KR(ret), K_(tenant_id));
    // STEP 4. check whether need to promote arb service status
  } else if (current_status.is_disabled() || current_status.is_enabled()) {
    // do nothing
  } else if (OB_FAIL(previous_check_for_promote_operation_(current_status, task_infos_.count(), can_do_promote))) {
    LOG_WARN("fail to check can do promote", KR(ret), K_(tenant_id), K(current_status),
             K(can_do_promote), "task_count", task_infos_.count());
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  // promote status
  if (OB_FAIL(ret)) {
  } else if (can_do_promote && OB_FAIL(construct_arg_and_promote_(
                                           current_status.is_enabling()
                                           ? ObArbitrationServiceStatus(ObArbitrationServiceStatus::ENABLED)
                                           : ObArbitrationServiceStatus(ObArbitrationServiceStatus::DISABLED)))) {
    LOG_WARN("fail to construct and promote status", KR(ret), K_(tenant_id), K(current_status));
  }
  return ret;
}

int ObArbitrationService::construct_arg_and_promote_(
    const ObArbitrationServiceStatus &new_status)
{
  int ret = OB_SUCCESS;
  ObModifyTenantArg arg;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *orig_tenant_schema = NULL;

  if (OB_ISNULL(GCTX.schema_service_) || OB_UNLIKELY(!new_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_status), KP(GCTX.schema_service_));
  } else if (OB_FAIL(arg.alter_option_bitset_.add_member(obrpc::ObModifyTenantArg::ENABLE_ARBITRATION_SERVICE))) {
    LOG_WARN("failed to add member to bitset!", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                         OB_SYS_TENANT_ID,
                         schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(gen_user_tenant_id(tenant_id_), orig_tenant_schema))) {
    LOG_WARN("fali to get tenant info", KR(ret), "tenant_id", gen_user_tenant_id(tenant_id_));
  } else if (OB_ISNULL(orig_tenant_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(orig_tenant_schema));
  } else if (OB_FAIL(arg.tenant_schema_.assign(*orig_tenant_schema))) {
    LOG_WARN("fail to assign tenant schema", KR(ret), KPC(orig_tenant_schema));
  } else if (FALSE_IT(arg.tenant_schema_.set_arbitration_service_status(new_status))) {
    // shall never be here
  } else if (FALSE_IT(arg.exec_tenant_id_ = OB_SYS_TENANT_ID)) {
    // shall never be here
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->modify_tenant(arg))) {
    LOG_WARN("[ARB_TENANT] fail to promote tenant arbitration service status", KR(ret), K(arg));
  } else {
    LOG_INFO("[ARB_TENANT] success to promote tenant arbitration service status", K_(tenant_id), K(arg));
  }
  return ret;
}

int ObArbitrationService::previous_check_for_promote_operation_(
    const ObArbitrationServiceStatus &current_status,
    const int64_t task_count,
    bool &can_do_promote)
{
  int ret = OB_SUCCESS;
  can_do_promote = false;
  ObArbitrationServiceInfo arbitration_service_info;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited), K_(tenant_id));
  } else if (0 != task_count) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[ARB_TENANT] no need to promote status, because some tasks not finished",
             KR(ret), K_(tenant_id), K(task_count), K(current_status));
    // construct a sql by left join __all_virtual_ls_status with __all_virtual_log_stat
    // to check whether has
    // (1) log stream with not normal status
    // (2) log stream with no leader
    // (3) log stream's arbitrtion member is not the one wanted
  } else if (OB_FAIL(ObArbitrationServiceUtils::check_can_promote_arbitration_service_status(
                         *sql_proxy_,
                         gen_user_tenant_id(tenant_id_),
                         current_status,
                         current_status.is_disabling()
                         ? ObArbitrationServiceStatus(ObArbitrationServiceStatus::DISABLED)
                         : ObArbitrationServiceStatus(ObArbitrationServiceStatus::ENABLED),
                         can_do_promote))) {
    LOG_WARN("fail to check can promote arbitration service status", KR(ret), K_(tenant_id),
             K(current_status), K(can_do_promote));
  }
  return ret;
}

int64_t ObArbitrationService::get_sync_rpc_timeout_() const
{
  // When communicate with leader to add/remove A-replica, a sync rpc may be sent.
  // We use at least 9s to avoid adding or remving replica timeout.
  return GCONF.rpc_timeout > obrpc::LogRpcProxyV2::MAX_RPC_TIMEOUT
         ? GCONF.rpc_timeout                    /*default:2s*/
         : obrpc::LogRpcProxyV2::MAX_RPC_TIMEOUT/*default:9s*/;
}

}//namespace rootserver end
}//namespace oceanbase end
