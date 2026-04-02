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
#include "share/arbitration_service/ob_arbitration_service_utils.h"
#include "share/arbitration_service/ob_arbitration_service_table_operator.h"
#include "observer/omt/ob_multi_tenant.h" // ObMultiTenant
#include "rootserver/ob_root_utils.h" // for ObTenantUtils
#include "rpc/obrpc/ob_net_keepalive.h" // ObNetKeepAlive
#include "share/location_cache/ob_location_service.h" // for ObLocationService
#include "share/ob_srv_rpc_proxy.h" // for ObSrvRpcProxy
#include "logservice/arbserver/ob_arb_srv_garbage_collect_service.h" // for ObArbGarbageCollectService
#include "logservice/ob_log_service.h" // for ObLogService
#include "logservice/palf_handle_guard.h" // PalfHandleGuard

namespace oceanbase
{
using namespace rootserver;
using namespace common;
using namespace sql;
namespace share
{

int ObArbitrationServiceUtils::add_arbitration_service(
    ObISQLClient &sql_proxy,
    const obrpc::ObAdminAddArbitrationServiceArg &arg)
{
  int ret = OB_SUCCESS;
  // TODO: arbitration_service_key should dynamicaly generated
  ObString arbitration_service_key("default");
  ObArbitrationServiceType type(ObArbitrationServiceType::INVALID_ARBITRATION_SERVICE_TYPE);
  ObString previous_arbitration_service("");
  ObArbitrationServiceInfo arbitration_service_info;
  ObArbitrationServiceTableOperator arbitration_service_table_operator;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("[ARB_SERVICE] execute add arbitration service request", K(arg));
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(ObArbitrationServiceInfo::parse_arbitration_service_with_type(arg.get_arbitration_service(), type))) {
    LOG_WARN("invalid arbitration service", KR(ret), K(arg));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "arbitration service");
  } else if (OB_FAIL(arbitration_service_info.init(
                         arbitration_service_key,
                         arg.get_arbitration_service(),
                         previous_arbitration_service,
                         type))) {
    LOG_WARN("fail to init a arbitration service info", KR(ret), K(arbitration_service_key), K(arg),
             K(previous_arbitration_service), K(type));
  } else if (OB_FAIL(add_cluster_info_to_arb_server(arg.get_arbitration_service()))) {
    LOG_WARN("fail to add cluster info to arb server", KR(ret), K(arg));
  } else if (OB_FAIL(arbitration_service_table_operator.insert(
                         sql_proxy,
                         arbitration_service_info))) {
    if (OB_ARBITRATION_SERVICE_ALREADY_EXIST == ret) {
      LOG_WARN("an arbitration service already exist, can not add again", KR(ret), K(arg));
      LOG_USER_ERROR(OB_ARBITRATION_SERVICE_ALREADY_EXIST, "can not add again");
    } else {
      LOG_WARN("fail to insert arbitration service into table", KR(ret), K(arg));
      if (OB_TMP_FAIL(remove_cluster_info_from_arb_server(arg.get_arbitration_service()))) {
        LOG_WARN("fail to remove cluster info from arb server", KR(tmp_ret), K(arg));
        LOG_USER_WARN(OB_CLUSTER_INFO_MAYBE_REMAINED, arg.get_arbitration_service().length(), arg.get_arbitration_service().ptr());
      }
    }
  }
  FLOG_INFO("[ARB_SERVICE] finish add arbitration service", KR(ret), K(arg));
  return ret;
}

int ObArbitrationServiceUtils::remove_arbitration_service(
    ObISQLClient &sql_proxy,
    const obrpc::ObAdminRemoveArbitrationServiceArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[ARB_SERVICE] execute remove arbitration service request", K(arg));
  ObArbitrationServiceTableOperator arbitration_service_table_operator;
  ObArbitrationServiceType type(ObArbitrationServiceType::INVALID_ARBITRATION_SERVICE_TYPE);
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(ObArbitrationServiceInfo::parse_arbitration_service_with_type(arg.get_arbitration_service(), type))) {
    LOG_WARN("invalid arbitration service", KR(ret), K(arg));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "arbitration service");
  } else if (OB_UNLIKELY(!type.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arbitration service type", KR(ret), K(arg), K(type));
  } else {
    common::ObMySQLTransaction trans;
    const uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
    ObArbitrationServiceInfo arbitration_service_info;
    ObArbitrationServiceInfo arbitration_service_info_to_update;
    // TODO: arbitration_service_key should dynamicaly generated
    ObString arbitration_service_key("default");
    bool lock_line = true;
    if (OB_FAIL(trans.start(&sql_proxy, sql_tenant_id))) {
      LOG_WARN("start transaction failed", KR(ret), K(sql_tenant_id));
    } else if (OB_FAIL(arbitration_service_table_operator.get(
                           trans,
                           arbitration_service_key,
                           lock_line,
                           arbitration_service_info))) {
      LOG_WARN("fail to get arbitration service info", KR(ret), K(arg), K(arbitration_service_key),
               K(lock_line));
    } else if (!arbitration_service_info.arbitration_service_is_equal_to_string(arg.get_arbitration_service())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("arbitration service mismatch, remove arbitration service not allowed", KR(ret), K(arg), K(arbitration_service_info));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "arbitration service mismatch, remove arbitration service");
    } else if (OB_FAIL(check_can_remove_arbitration_service_(trans))) {
      LOG_WARN("fail to check can remove arbitration service", KR(ret));
    } else if (OB_FAIL(arbitration_service_table_operator.remove(trans, arbitration_service_key))) {
      LOG_WARN("fail to update arbitration service info", KR(ret), K(arbitration_service_key));
    }
    if (trans.is_started()) {
      int trans_ret = trans.end(OB_SUCCESS == ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("end transaction failed", KR(trans_ret));
        ret = OB_SUCCESS == ret ? trans_ret : ret;
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (OB_TMP_FAIL(remove_cluster_info_from_arb_server(arg.get_arbitration_service()))) {
    LOG_WARN("fail to remove cluster info from arb server", KR(tmp_ret), K(arg));
    LOG_USER_WARN(OB_CLUSTER_INFO_MAYBE_REMAINED, arg.get_arbitration_service().length(), arg.get_arbitration_service().ptr());
  }
  FLOG_INFO("[ARB_SERVICE] finish remove arbitration service", KR(ret), K(arg));
  return ret;
}

int ObArbitrationServiceUtils::check_can_remove_arbitration_service_(
    ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  bool has_tenant_with_not_disabled_status = true;
  ObSqlString sql;
  ObSqlString tenant_list;
  ObSqlString message_to_user;
  const char* table_name = OB_ALL_TENANT_TNAME;
  if (OB_FAIL(sql.assign_fmt("SELECT tenant_id "
                             "FROM %s "
                             "WHERE arbitration_service_status != 'DISABLED'", table_name))) {
    LOG_WARN("fail to construct sql", KR(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy.read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get sql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_tenant_list_from_sql_result_(
                             *result.get_result(),
                             tenant_list,
                             has_tenant_with_not_disabled_status))) {
        LOG_WARN("fail to construct tenant list from result", KR(ret));
      } else if (has_tenant_with_not_disabled_status) {
        if (OB_FAIL(message_to_user.assign_fmt("arbitration service not disabled for tenant (%.*s), "
                                               "remove arbitration service",
                                               static_cast<int>(tenant_list.length()), tenant_list.ptr()))) {
          LOG_WARN("fail to construct message to user", KR(ret), K(tenant_list));
        } else {
          _LOG_INFO("[ARB_SERVICE] arbitration service not disabled for tenant (%.*s), "
                    "remove arbitration service not allowed, and the check sql is %.*s",
                    static_cast<int>(message_to_user.length()), message_to_user.ptr(),
                    static_cast<int>(sql.length()), sql.ptr());
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, message_to_user.ptr());
        }
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::construct_tenant_list_from_sql_result_(
    common::sqlclient::ObMySQLResult &res,
    ObSqlString &tenant_list,
    bool &has_tenant)
{
  int ret = OB_SUCCESS;
  has_tenant = true;
  tenant_list.reset();
  int64_t tenant_id = 0;
  int64_t not_satisfied_tenant_cnt = 0;
  bool append_comma = false;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
      break;
    } else if (OB_FAIL(res.get_int("tenant_id", tenant_id))) {
      LOG_WARN("fail to get tenant id from result", KR(ret));
    } else if (append_comma && OB_FAIL(tenant_list.append(", "))) {
      LOG_WARN("fail to append comma to tenant list", KR(ret));
    } else if (OB_FAIL(tenant_list.append_fmt("%ld", tenant_id))) {
      LOG_WARN("fail to append tenant list", KR(ret), K(tenant_id));
    } else {
      not_satisfied_tenant_cnt++;
      append_comma = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == not_satisfied_tenant_cnt){
    has_tenant = false;
  }
  return ret;
}

int ObArbitrationServiceUtils::try_add_cluster_info_if_needed_(
    ObISQLClient &sql_proxy,
    const ObString &arbitration_service_key,
    const obrpc::ObAdminReplaceArbitrationServiceArg &arg,
    bool &success_add_cluster_info)
{
  int ret = OB_SUCCESS;
  ObArbitrationServiceTableOperator arbitration_service_table_operator;
  ObArbitrationServiceInfo old_arbitration_service_info;
  ObArbitrationServiceInfo new_arbitration_service_info;
  ObArbitrationServiceType type(ObArbitrationServiceType::INVALID_ARBITRATION_SERVICE_TYPE);
  bool can_replace = false;
  bool need_replace = false;
  bool need_add_cluster_info  = false;
  success_add_cluster_info = false;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (OB_FAIL(ObArbitrationServiceInfo::parse_arbitration_service_with_type(arg.get_arbitration_service(), type))) {
    LOG_WARN("fail to parse arbitration service from string", KR(ret), K(arg));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "new arbitration service");
  } else if (OB_FAIL(ObArbitrationServiceInfo::parse_arbitration_service_with_type(arg.get_previous_arbitration_service(), type))) {
    LOG_WARN("fail to parse previous_arbitration service from string", KR(ret), K(arg));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "current arbitration service");
  } else if (OB_FAIL(arbitration_service_table_operator.get(
                         sql_proxy,
                         arbitration_service_key,
                         false/*lock_line*/,
                         old_arbitration_service_info))) {
    LOG_WARN("fail to get current arbitration service info", KR(ret), K(arbitration_service_key));
  } else if (OB_FAIL(new_arbitration_service_info.init(
                         arbitration_service_key,
                         arg.get_arbitration_service(),
                         arg.get_previous_arbitration_service(),
                         type))) {
    LOG_WARN("fail to build new arbitration service info", KR(ret), K(arbitration_service_key),
             K(arg), K(type));
  // precheck for add_cluster_info action, if it is an invalid replacement, do not continue
  } else if (OB_FAIL(check_can_replace_arbitration_service_(
                         old_arbitration_service_info,
                         new_arbitration_service_info,
                         can_replace,
                         need_replace,
                         need_add_cluster_info))) {
    LOG_WARN("fail to check can replace arbitration service", KR(ret), K(old_arbitration_service_info),
             K(new_arbitration_service_info), K(arg));
  } else if (!can_replace) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not replace arbitration service", KR(ret), K(arg),
             K(old_arbitration_service_info), K(new_arbitration_service_info));
  } else if (need_add_cluster_info && OB_FAIL(add_cluster_info_to_arb_server(arg.get_arbitration_service()))) {
    LOG_WARN("fail to add cluster info to arbitration server", KR(ret), K(arg));
  } else {
    success_add_cluster_info = true;
  }
  return ret;
}

int ObArbitrationServiceUtils::replace_arbitration_service(
    ObISQLClient &sql_proxy,
    const obrpc::ObAdminReplaceArbitrationServiceArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[ARB_SERVICE] execute replace arbitration service request", K(arg));
  ObArbitrationServiceTableOperator arbitration_service_table_operator;
  common::ObMySQLTransaction trans;
  const uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
  ObArbitrationServiceInfo old_arbitration_service_info;
  ObArbitrationServiceInfo new_arbitration_service_info;
  // TODO: arbitration_service_key should dynamicaly generated
  ObString arbitration_service_key("default");
  bool lock_line = true;
  bool can_replace = false;
  bool need_replace = false;
  bool success_add_cluster_info = false;
  bool need_add_cluster_info = false;
  ObArbitrationServiceType type(ObArbitrationServiceType::INVALID_ARBITRATION_SERVICE_TYPE);
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  // TODO: do not suppose arbitration service is addr
  } else if (OB_FAIL(ObArbitrationServiceInfo::parse_arbitration_service_with_type(arg.get_arbitration_service(), type))) {
    LOG_WARN("fail to parse arbitration service from string", KR(ret), K(arg));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "new arbitration service");
  } else if (OB_FAIL(ObArbitrationServiceInfo::parse_arbitration_service_with_type(arg.get_previous_arbitration_service(), type))) {
    LOG_WARN("fail to parse previous_arbitration service from string", KR(ret), K(arg));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "current arbitration service");
  } else if (OB_FAIL(try_add_cluster_info_if_needed_(
                         sql_proxy,
                         arbitration_service_key,
                         arg,
                         success_add_cluster_info))) {
    LOG_WARN("fail to try add cluster info", KR(ret), K(arbitration_service_key), K(arg));
  } else if (OB_FAIL(trans.start(&sql_proxy, sql_tenant_id))) {
    LOG_WARN("start transaction failed", KR(ret), K(sql_tenant_id));
  } else if (OB_FAIL(arbitration_service_table_operator.get(
                         trans,
                         arbitration_service_key,
                         lock_line,
                         old_arbitration_service_info))) {
    LOG_WARN("fail to get arbitration service info", KR(ret), K(arbitration_service_key), K(lock_line));
  } else if (OB_FAIL(new_arbitration_service_info.init(
                         arbitration_service_key,
                         arg.get_arbitration_service(),
                         arg.get_previous_arbitration_service(),
                         type))) {
    LOG_WARN("fail to build new arbitration service info", KR(ret), K(arbitration_service_key),
             K(arg), K(type));
  } else if (OB_FAIL(check_can_replace_arbitration_service_(
                         old_arbitration_service_info,
                         new_arbitration_service_info,
                         can_replace,
                         need_replace,
                         need_add_cluster_info/*not used*/))) {
    LOG_WARN("fail to check can replace arbitration service", KR(ret), K(old_arbitration_service_info),
             K(new_arbitration_service_info), K(arg));
  } else if (!can_replace) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not replace arbitration service", KR(ret), K(arg),
             K(old_arbitration_service_info), K(new_arbitration_service_info));
  } else if (need_replace && OB_FAIL(arbitration_service_table_operator.update(trans, new_arbitration_service_info))) {
    LOG_WARN("fail to update arbitration service info", KR(ret), K(arg), K(new_arbitration_service_info));
  }
  if (trans.is_started()) {
    int trans_ret = trans.end(OB_SUCCESS == ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret));
      ret = OB_SUCCESS == ret ? trans_ret : ret;
    }
  }
  if (OB_FAIL(ret) && success_add_cluster_info) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(remove_cluster_info_from_arb_server(arg.get_arbitration_service()))) {
      LOG_WARN("fail to remove cluster info from arb server", KR(tmp_ret), K(arg));
      LOG_USER_WARN(OB_CLUSTER_INFO_MAYBE_REMAINED, arg.get_arbitration_service().length(), arg.get_arbitration_service().ptr());
    }
  }
  // try wakeup arbitration service
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (OB_TMP_FAIL(wakeup_all_tenant_arbitration_service_())) {
    LOG_WARN("fail to wakeup all tenant arbitration service", KR(tmp_ret));
  }
  FLOG_INFO("[ARB_SERVICE] finish replace arbitration service", KR(ret), K(arg));
  return ret;
}

int ObArbitrationServiceUtils::check_can_replace_arbitration_service_(
    const ObArbitrationServiceInfo &old_arbitration_service_info,
    const ObArbitrationServiceInfo &new_arbitration_service_info,
    bool &can_replace,
    bool &need_replace,
    bool &need_add_cluster_info)
{
  // TODO: need to consider how to do replacement between a ObAddr and URL type
  // only under these 3 cases can_replace = true
  // case 1. a rollback operation
  //         replace A with B when it is replacing B with A now
  // case 2. a same operation as previous one
  //         replace A with B when it is replacing A with B now,
  //         this case do not need to do the real replacement again, so set need_replace to false
  // case 3. a valid replace operation
  //         replace A with B when it is not in replacing now
  int ret = OB_SUCCESS;
  can_replace = false;
  need_replace = false;
  need_add_cluster_info = false;
  ObSqlString message_to_user;
  if (OB_FAIL(check_degrading_log_stream_for_replace_arb_service_())) {
    // LOG_USER_ERROR is raised in function
    LOG_WARN("fail to check whether has degrading log stream", KR(ret));
  } else if (new_arbitration_service_info.arbitration_service_is_equal_to_string(
          new_arbitration_service_info.get_previous_arbitration_service_string())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("replace to self not allowed", KR(ret),
              K(old_arbitration_service_info), K(new_arbitration_service_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "replace arbitration service to self");
  } else if (!old_arbitration_service_info.get_previous_arbitration_service_string().empty()
             && old_arbitration_service_info.arbitration_service_is_equal_to_string(
                  new_arbitration_service_info.get_previous_arbitration_service_string())
             && old_arbitration_service_info.previous_arbitration_service_is_equal_to_string(
                  new_arbitration_service_info.get_arbitration_service_string())) {
    // case 1. a rollback operation
    can_replace = true;
    need_replace = true;
    need_add_cluster_info = false;
  } else if (!old_arbitration_service_info.get_previous_arbitration_service_string().empty()) {
    if (old_arbitration_service_info.is_equal(new_arbitration_service_info)) {
      // case 2. a same replacement as previous one
      can_replace = true;
      need_replace = false;
      need_add_cluster_info = false;
    } else {
      // already in replacing and not the same as last time
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("arbitration service is already in replacing", KR(ret),
                K(old_arbitration_service_info), K(new_arbitration_service_info));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "replace arbitration service while already in replacing");
    }
  } else if (!old_arbitration_service_info.arbitration_service_is_equal_to_string(
                new_arbitration_service_info.get_previous_arbitration_service_string())) {
    // the arbitration service want to be replaced not exist
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("arbitration service mismatch, replace arbitration service not allowed", KR(ret),
             K(old_arbitration_service_info), K(new_arbitration_service_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "arbitration service mismatch, replace arbitration service");
  } else {
    // case 3. a new replace operation
    can_replace = true;
    need_replace = true;
    need_add_cluster_info = true;
  }
  return ret;
}

// check_can_promote_arbitration_service_status
// ========================================================================================
// |                  |   to DISABLED  |  to DISABLING  |   to ENABLED   |  to ENABLING   |
// ========================================================================================
// | change DISABLED  |   do nothing   |  do nothing    |   do nothing   | user-triggered |
// | change DISABLING |  svr-triggered |  do nothing    |   do nothing   | user_triggered |
// | change ENABLED   |   do nothing   | user_triggered |   do nothing   | do nothing     |
// | change ENABLING  |   do nothing   | user_triggered |  svr_triggered | do nothing     |
// ========================================================================================
int ObArbitrationServiceUtils::check_can_promote_arbitration_service_status(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObArbitrationServiceStatus &old_status,
    const ObArbitrationServiceStatus &new_status,
    bool &can_promote)
{
  int ret = OB_SUCCESS;
  can_promote = false;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || is_meta_tenant(tenant_id)
      || !old_status.is_valid()
      || !new_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(old_status), K(new_status));
    // can promote only under these cases, so deal with different cases below
    // case 1. enabling -> enabled
    // case 2. disabling -> disabled
    // case 3. disabled  -> enabling
    // case 4. disabling -> enabling
    // case 5. enabling -> disabling
    // case 6. enabled  -> disabling
  } else if ((old_status.is_enabling() && new_status.is_enabled())
              || (old_status.is_disabling() && new_status.is_disabled())) {
    // case 1-2. triggered by arb service only, so do not LOG_USER_ERROR
    if (OB_FAIL(check_can_promote_status_to_stable_one_(
                    sql_proxy,
                    tenant_id,
                    old_status,
                    new_status,
                    can_promote))) {
      LOG_WARN("fail to check can promote status to stable one", KR(ret), K(tenant_id),
               K(old_status), K(new_status));
    }
  } else if (old_status.is_disable_like() && new_status.is_enabling()) {
    // case 3-4. triggered by user, should raise LOG_USER_ERROR
    // enable arb service
    if (OB_FAIL(check_can_enable_arb_service_(
                    sql_proxy,
                    tenant_id,
                    old_status,
                    new_status,
                    can_promote))) {
      LOG_WARN("fail to check can enable arb service", KR(ret), K(tenant_id),
               K(old_status), K(new_status));
    }
  } else if (old_status.is_enable_like() && new_status.is_disabling()) {
    // case 5-6. triggered by user, should raise LOG_USER_ERROR
    // disable arb service
    if (OB_FAIL(check_can_disable_arb_service_(
                    sql_proxy,
                    tenant_id,
                    old_status,
                    new_status,
                    can_promote))) {
      LOG_WARN("fail to check can disable arb service", KR(ret), K(tenant_id),
               K(old_status), K(new_status));
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::check_can_enable_arb_service_(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObArbitrationServiceStatus &old_status,
    const ObArbitrationServiceStatus &new_status,
    bool &can_promote)
{
  int ret = OB_SUCCESS;
  ObArbitrationServiceInfo arb_info;
  ObString arb_service_key("default");
  ObArbitrationServiceTableOperator arbitration_service_table_operator;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  int64_t schema_paxos_replica_cnt = 0;
  bool is_two_paxos_replica = false;
  bool is_four_paxos_replica = false;
  can_promote = false;
  if (OB_ISNULL(GCTX.schema_service_)
      || OB_UNLIKELY(!old_status.is_valid()
                     || !new_status.is_valid()
                     || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_), K(tenant_id), K(old_status), K(new_status));
  } else if (OB_FAIL(arbitration_service_table_operator.get(
                         sql_proxy,
                         arb_service_key,
                         true/*lock_line*/,
                         arb_info))) {
    LOG_WARN("fail to get arbitration service info", KR(ret), K(arb_service_key), K(arb_info));
    if (OB_ARBITRATION_SERVICE_NOT_EXIST == ret) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "arbitration service not exist, enable tenant arbitration service");
    }
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                         OB_SYS_TENANT_ID,
                         schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
  } else if (!tenant_schema->get_previous_locality_str().empty()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not enable arb service while tenant locality in changing", KR(ret), KP(tenant_schema));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant locality is changing, enable tenant arbitration service");
  } else if (OB_FAIL(check_tenant_paxos_replica_number_equals_to_given(
                         schema_guard,
                         tenant_id,
                         2/*paxos_replica_number_to_compare*/,
                         is_two_paxos_replica))) {
    LOG_WARN("fail to check tenant paxos replica number", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_paxos_replica_number_equals_to_given(
                         schema_guard,
                         tenant_id,
                         4/*paxos_replica_number_to_compare*/,
                         is_four_paxos_replica))) {
    LOG_WARN("fail to check tenant paxos replica number", KR(ret), K(tenant_id));
  } else if (!is_two_paxos_replica && !is_four_paxos_replica) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not enable arb service with paxos replica is neither 2F nor 4F",
             K(tenant_id), K(is_two_paxos_replica), K(is_four_paxos_replica));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The number of paxos replicas in locality is neither 2 nor 4, enable tenant arbitration service");
  } else {
    can_promote = true;
  }
  return ret;
}

int ObArbitrationServiceUtils::check_can_disable_arb_service_(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObArbitrationServiceStatus &old_status,
    const ObArbitrationServiceStatus &new_status,
    bool &can_promote)
{
  int ret = OB_SUCCESS;
  ObArbitrationServiceInfo arb_info;
  ObString arb_service_key("default");
  can_promote = false;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObArbitrationServiceTableOperator arbitration_service_table_operator;
  if (OB_ISNULL(GCTX.schema_service_)
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                     || !old_status.is_valid()
                     || !new_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_), K(tenant_id), K(old_status), K(new_status));
  } else if (OB_FAIL(arbitration_service_table_operator.get(
                         sql_proxy,
                         arb_service_key,
                         true/*lock_line*/,
                         arb_info))) {
    LOG_WARN("fail to get arbitration service info", KR(ret), K(arb_service_key), K(arb_info));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                         OB_SYS_TENANT_ID,
                         schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
  } else if (!tenant_schema->get_previous_locality_str().empty()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not disable arb service while tenant locality in changing", KR(ret), KP(tenant_schema));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant locality is changing, disable tenant arbitration service");
  } else if (OB_FAIL(check_degrading_log_stream_for_disable_tenant_arb_service_(tenant_id, sql_proxy))) {
    LOG_WARN("fail to check has degrading log stream while disable arb service", KR(ret), K(tenant_id));
  } else {
    can_promote = true;
  }
  return ret;
}

int ObArbitrationServiceUtils::check_degrading_log_stream_for_replace_arb_service_()
{
  int ret = OB_SUCCESS;
  bool only_consider_2f_tenant = false;
  ObSqlString tenant_sub_sql;
  ObSqlString degrading_ls_list;
  int64_t degrading_ls_cnt = 0;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(construct_tenants_with_arbitration_service_(
                  only_consider_2f_tenant,
                  tenant_sub_sql))) {
    LOG_WARN("fail to construct tenants with arbitration service", KR(ret), K(only_consider_2f_tenant));
  } else if (!tenant_sub_sql.empty()) {
    if (OB_FAIL(check_has_degrading_log_stream_(
                    tenant_sub_sql,
                    *GCTX.sql_proxy_,
                    degrading_ls_list,
                    degrading_ls_cnt))) {
      LOG_WARN("fail to check has degrading log stream", KR(ret), K(tenant_sub_sql));
    } else if (0 != degrading_ls_cnt) {
      ObSqlString message_to_user;
      if (OB_FAIL(message_to_user.assign_fmt("There are LS that may be in degraded state or have no leader, please check: "
                                             "%.*s. Replace arbitration service",
                                             static_cast<int>(degrading_ls_list.length()), degrading_ls_list.ptr()))) {
        LOG_WARN("fail to construct message to user", KR(ret), K(degrading_ls_list));
      } else {
        ret = OB_OP_NOT_ALLOW;
        _LOG_INFO("[ARB_TENANT] %.*s not allowed", static_cast<int>(message_to_user.length()), message_to_user.ptr());
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, message_to_user.ptr());
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::check_degrading_log_stream_for_disable_tenant_arb_service_(
    const uint64_t tenant_id,
    ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  uint64_t pair_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString tenant_sub_sql;
  ObSqlString degrading_ls_list;
  int64_t degrading_ls_cnt = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (FALSE_IT(pair_tenant_id = is_user_tenant(tenant_id)
                                     ? gen_meta_tenant_id(tenant_id)
                                     : tenant_id)) {
    // shall never be here
  } else if (OB_FAIL(tenant_sub_sql.append_fmt("%lu", tenant_id))) {
    LOG_WARN("fail to pusk tenant id into tenant_sub_sql", KR(ret), K(tenant_id));
  } else if (pair_tenant_id != tenant_id
             && OB_FAIL(tenant_sub_sql.append_fmt(", %lu", pair_tenant_id))) {
    LOG_WARN("fail to pusk pair tenant id into tenant_sub_sql", KR(ret), K(pair_tenant_id));
  } else if (OB_FAIL(check_has_degrading_log_stream_(
                         tenant_sub_sql,
                         sql_proxy,
                         degrading_ls_list,
                         degrading_ls_cnt))) {
    LOG_WARN("fail to check has degrading log stream", KR(ret), K(tenant_sub_sql));
  } else if (0 != degrading_ls_cnt) {
    ObSqlString message_to_user;
    if (OB_FAIL(message_to_user.assign_fmt("There are LS that may be in degraded state or have no leader, please check: "
                                           "%.*s. Disable tenant arbitration service",
                                           static_cast<int>(degrading_ls_list.length()), degrading_ls_list.ptr()))) {
      LOG_WARN("fail to construct message to user", KR(ret), K(degrading_ls_list));
    } else {
      ret = OB_OP_NOT_ALLOW;
      _LOG_INFO("[ARB_TENANT] %.*s not allowed", static_cast<int>(message_to_user.length()), message_to_user.ptr());
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, message_to_user.ptr());
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::check_has_degrading_log_stream_(
    const ObSqlString &tenant_sub_sql,
    ObISQLClient &sql_proxy,
    ObSqlString &degrading_ls_list,
    int64_t &degrading_ls_cnt)
{
  int ret = OB_SUCCESS;
  degrading_ls_list.reset();
  degrading_ls_cnt = 1;
  ObSqlString sql;
  const char* left_table_name = OB_ALL_VIRTUAL_LS_STATUS_TNAME;
  const char* right_table_name = OB_ALL_VIRTUAL_LOG_STAT_TNAME;
  int64_t retry_timeout = GCONF.rpc_timeout; // default 2s
  int64_t start_time = ObTimeUtility::current_time();
  const int64_t retry_interval_us = 500l * 1000l; // 500ms
  FLOG_INFO("[ARB_TENANT] start check has degrading log stream", K(tenant_sub_sql),
            K(start_time), K(retry_timeout), K(retry_interval_us));
  if (OB_UNLIKELY(tenant_sub_sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_sub_sql));
  } else if (OB_FAIL(sql.assign_fmt("SELECT a.tenant_id, a.ls_id "
                                    "FROM %s AS a LEFT JOIN %s AS b "
                                    "ON a.tenant_id = b.tenant_id AND a.ls_id = b.ls_id AND b.role = 'LEADER' "
                                    "WHERE (a.tenant_id in (%.*s)) "
                                    "AND ((b.role = 'LEADER' AND b.degraded_list != '') " /*those in degrading*/
                                    "OR b.role IS NULL)",                                 /*those without leader*/
                                    left_table_name, right_table_name,
                                    tenant_sub_sql.string().length(),
                                    tenant_sub_sql.string().ptr()))) {
    LOG_WARN("fail to construct sql", KR(ret), K(tenant_sub_sql));
  } else {
    while (OB_SUCC(ret)
           && 0 != degrading_ls_cnt
           && ObTimeUtility::current_time() - start_time < retry_timeout) {
      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (OB_FAIL(sql_proxy.read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(sql));
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get sql result failed", KR(ret), K(sql));
        } else if (OB_FAIL(construct_log_message_from_sql_result_(
                               *result.get_result(),
                               degrading_ls_list,
                               degrading_ls_cnt))) {
          LOG_WARN("fail to construct degrading log stream", KR(ret));
        } else if (0 != degrading_ls_cnt) {
          _LOG_INFO("[ARB_TENANT] some log stream has no leader or has degraded member, "
                    "and the check sql is [%.*s]",
                    static_cast<int>(sql.length()), sql.ptr());
          ob_usleep(retry_interval_us);
        }
      }
    }// end while
  }
  FLOG_INFO("[ARB_TENANT] finish check has degrading log stream", K(tenant_sub_sql),
            K(start_time), K(retry_timeout), K(retry_interval_us), K(degrading_ls_list),
            K(degrading_ls_cnt));
  return ret;
}

int ObArbitrationServiceUtils::check_can_promote_status_to_stable_one_(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObArbitrationServiceStatus &old_status,
    const ObArbitrationServiceStatus &new_status,
    bool &can_promote)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  uint64_t pair_tenant_id = OB_INVALID_TENANT_ID;
  const char* left_table_name = OB_ALL_VIRTUAL_LS_STATUS_TNAME;
  const char* right_table_name = OB_ALL_VIRTUAL_LOG_STAT_TNAME;
  can_promote = false;
  ObSqlString expected_arb_member;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || is_meta_tenant(tenant_id)
                  || !old_status.is_valid()
                  || !new_status.is_valid()
                  || (!(old_status.is_disabling() && new_status.is_disabled())
                      && !(old_status.is_enabling() && new_status.is_enabled())))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(old_status), K(new_status));
  } else if (FALSE_IT(pair_tenant_id = is_sys_tenant(tenant_id)
                                     ? tenant_id
                                     : gen_meta_tenant_id(tenant_id))) { // shall never be here
  } else if (old_status.is_enabling()) {
    ObString arbitration_service_key("default");
    ObArbitrationServiceInfo arbitration_service_info;
    ObArbitrationServiceTableOperator arbitration_service_table_operator;
    if (OB_FAIL(arbitration_service_table_operator.get(
                    sql_proxy,
                    arbitration_service_key,
                    false/*lock_line*/,
                    arbitration_service_info))) {
      LOG_WARN("fail to get arbitration service info", KR(ret), K(arbitration_service_key), K(arbitration_service_info));
    } else if (OB_FAIL(expected_arb_member.assign(arbitration_service_info.get_arbitration_service_string()))) {
      LOG_WARN("fail to assign arb service", KR(ret), K(tenant_id), K(arbitration_service_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.assign_fmt(
                         "SELECT a.tenant_id, a.ls_id "
                         "FROM %s AS a LEFT JOIN %s AS b "
                         "ON a.tenant_id = b.tenant_id AND a.ls_id = b.ls_id AND b.role = 'LEADER' "
                         "WHERE (a.tenant_id = %lu OR a.tenant_id = %lu) "
                         "AND ((b.role = 'LEADER' AND b.arbitration_member != '%.*s') " /*those with unexpected arb_member*/
                         "OR b.role IS NULL)",                                          /*those without leader*/
                         left_table_name, right_table_name, tenant_id, pair_tenant_id,
                         static_cast<int>(expected_arb_member.length()), expected_arb_member.ptr()))) {
    LOG_WARN("fail to construct sql", KR(ret), K(left_table_name), K(right_table_name),
             K(tenant_id), K(pair_tenant_id), K(expected_arb_member));
  } else {
    int64_t is_satisfied = 0;
    ObSqlString log_message;
    int64_t not_satisfied_ls_cnt = 0;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql_proxy.read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get sql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_log_message_from_sql_result_(
                             *result.get_result(),
                             log_message,
                             not_satisfied_ls_cnt))) {
        LOG_WARN("fail to deal with check promotion sql result", KR(ret), K(tenant_id), K(sql));
      } else if (0 != not_satisfied_ls_cnt) {
        _LOG_INFO("[ARB_TENANT] can not promote arbitration service status, "
                  "because these %ld log streams have unexpected arb_member:%.*s "
                  "and the check sql is %.*s",
                  not_satisfied_ls_cnt, static_cast<int>(log_message.length()), log_message.ptr(),
                  static_cast<int>(sql.length()), sql.ptr());
      } else {
        can_promote = true;
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::construct_log_message_from_sql_result_(
    common::sqlclient::ObMySQLResult &res,
    ObSqlString &log_message,
    int64_t &not_satisfied_ls_cnt)
{
  int ret = OB_SUCCESS;
  log_message.reset();
  not_satisfied_ls_cnt = 0;
  int64_t tenant_id = 0;
  int64_t ls_id = ObLSID::INVALID_LS_ID;
  bool append_comma = false;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
      break;
    } else if (OB_FAIL(res.get_int("tenant_id", tenant_id))) {
      LOG_WARN("fail to get tenant id from result", KR(ret));
    } else if (OB_FAIL(res.get_int("ls_id", ls_id))) {
      LOG_WARN("fail to get ls id from result", KR(ret));
    } else if (append_comma && OB_FAIL(log_message.append(", "))) {
      LOG_WARN("fail to append comma", KR(ret));
    } else if (OB_FAIL(log_message.append_fmt("Tenant(%ld) LS(%ld)", tenant_id, ls_id))) {
      LOG_WARN("fail to append log message", KR(ret), K(tenant_id), K(ls_id));
    } else {
      append_comma = true;
      not_satisfied_ls_cnt++;
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::get_tenant_arbitration_service_status(
    const uint64_t tenant_id,
    ObArbitrationServiceStatus &arb_service_status)
{
  int ret = OB_SUCCESS;
  arb_service_status.reset();
  bool is_compatible = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(
                         tenant_id, is_compatible))) {
    LOG_WARN("fail to check compat version for arbitration service", KR(ret), K(tenant_id));
  } else if (!is_compatible) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data version is below 4.1", KR(ret), K(tenant_id));
  } else {
    const char* table_name = OB_ALL_TENANT_TNAME;
    ObSqlString sql;
    const uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
    ObString arbitration_service_status;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
                  "SELECT arbitration_service_status FROM %s WHERE tenant_id = %lu",
                   table_name, tenant_id))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql_tenant_id), K(tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get sql result failed", KR(ret), K(tenant_id), "sql", sql.ptr());
      } else if (OB_FAIL(result.get_result()->next())) {
        LOG_WARN("get next result failed", KR(ret), K(tenant_id));
        if (OB_ITER_END == ret) {
          ret = OB_TENANT_NOT_EXIST;
        }
      } else if (OB_FAIL(result.get_result()->get_varchar("arbitration_service_status", arbitration_service_status))) {
        LOG_WARN("fail to get arbitration service status from res", KR(ret), K(tenant_id), K(arbitration_service_status));
      } else if (OB_FAIL(arb_service_status.parse_from_string(arbitration_service_status))) {
        LOG_WARN("fail to parse arbitration service status from string", KR(ret), K(arbitration_service_status));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::construct_tenants_with_arbitration_service_(
    const bool only_consider_2f_tenant,
    ObSqlString &tenant_list)
{
  int ret = OB_SUCCESS;
  tenant_list.reset();
  share::schema::ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTenantSchema *> tenant_schemas;
  bool is_two_paxos_replica_tenant = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys tenant schema guard", KR(ret));
  //STEP 1: get all tenant schemas
  } else if (OB_FAIL(schema_guard.get_simple_tenant_schemas(tenant_schemas))) {
    LOG_WARN("fail to get all tenant schemas", KR(ret));
  } else {
    //STEP 2: construct tenant to check: those tenant with arbitration service
    bool need_append_comma = false;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_schemas.count(); ++i) {
      if (OB_ISNULL(tenant_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema is null", KR(ret), "schema", tenant_schemas.at(i));
      } else if (!tenant_schemas.at(i)->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant schema", KR(ret), "schema", tenant_schemas.at(i));
      } else if (FALSE_IT(tenant_id = tenant_schemas.at(i)->get_tenant_id())) {
        // shall never be here
      } else if (is_meta_tenant(tenant_id)) {
        // do nothing, only to deal with user tenant and sys tenant
      } else if (!tenant_schemas.at(i)->get_arbitration_service_status().is_enable_like()) {
        // by pass
      } else if (only_consider_2f_tenant
                 && OB_FAIL(check_tenant_paxos_replica_number_equals_to_given(
                             schema_guard,
                             tenant_id,
                             2,/*paxos_replica_number_to_compare*/
                             is_two_paxos_replica_tenant))) {
        LOG_WARN("fail to check tenant paxos replica number", KR(ret), K(tenant_id));
      } else if (only_consider_2f_tenant && !is_two_paxos_replica_tenant) {
        // no nothing
      } else if (need_append_comma && OB_FAIL(tenant_list.append(", "))) {
        LOG_WARN("fali to append comma", KR(ret));
      } else if (OB_FAIL(tenant_list.append_fmt("%lu", tenant_id))) {
        LOG_WARN("fail to append tenant id to sql", KR(ret), K(tenant_id));
      } else if (is_user_tenant(tenant_id)
                 && OB_FAIL(tenant_list.append_fmt(", %lu", gen_meta_tenant_id(tenant_id)))) {
        LOG_WARN("fail to append meta tenant id to sql", KR(ret), "tenant_id", gen_meta_tenant_id(tenant_id));
      } else {
        need_append_comma = true;
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::construct_sql_for_replacement_waiting_(
    const ObString &arb_member,
    ObSqlString &sql,
    bool &has_tenant_need_to_wait)
{
  int ret = OB_SUCCESS;
  sql.reset();
  has_tenant_need_to_wait = true;
  ObSqlString tenant_sub_sql;
  bool only_consider_2f_tenant = false;
  if (OB_UNLIKELY(arb_member.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arb_member));
  } else if (OB_FAIL(construct_tenants_with_arbitration_service_(
                         only_consider_2f_tenant,
                         tenant_sub_sql))) {
    LOG_WARN("fail to construct tenant list with arbitration service", KR(ret), K(only_consider_2f_tenant));
  } else if (tenant_sub_sql.empty()) {
    // do nothing
    has_tenant_need_to_wait = false;
    LOG_TRACE("has no tenant with arbitration service");
  } else if (OB_FAIL(sql.assign_fmt(
                       "SELECT a.tenant_id, a.ls_id "
                       "FROM %s AS a LEFT JOIN %s AS b "
                       "ON a.tenant_id = b.tenant_id AND a.ls_id = b.ls_id "
                       "WHERE (a.tenant_id in (%.*s)) "       // those tenant with arbitration service
                       "AND ((b.role IS NULL) "             // those log stream without leader
                             "OR (b.role = 'LEADER' AND (arbitration_member != '%.*s')))",  // those log stream with not expected arb_member
                       share::OB_ALL_VIRTUAL_LS_STATUS_TNAME,
                       share::OB_ALL_VIRTUAL_LOG_STAT_TNAME,
                       tenant_sub_sql.string().length(),
                       tenant_sub_sql.string().ptr(),
                       arb_member.length(),
                       arb_member.ptr()))) {
    LOG_WARN("assign_fmt failed", KR(ret), K(tenant_sub_sql), K(arb_member));
  }
  return ret;
}

int ObArbitrationServiceUtils::wait_all_tenant_with_arb_has_arb_member(
    ObISQLClient &sql_proxy,
    const ObString &arb_member,
    const ObString &previous_arb_member)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval_us = 1000l * 1000l; // 1s
  ObSqlString sql;
  ObTimeoutCtx ctx;
  bool has_tenant_need_to_wait = false;
  LOG_INFO("[ARB_SERVICE] begin to wait all tenant with expected arb service", K(arb_member));
  if (OB_UNLIKELY(arb_member.empty() || previous_arb_member.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arb_member), K(previous_arb_member));
  } else if (OB_FAIL(construct_sql_for_replacement_waiting_(
                         arb_member,
                         sql,
                         has_tenant_need_to_wait))) {
    LOG_WARN("fail to construct sql", KR(ret), K(arb_member));
  } else if (!has_tenant_need_to_wait) {
    // do nothing
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    // wait until certain log stream has expected arb_member
    bool stop = false;
    while (OB_SUCC(ret) && !stop) {
      ObSqlString log_message;
      int64_t not_satisfied_ls_cnt = 0;
      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (0 > ctx.get_timeout()) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait arbitration service replacement finished timeout", KR(ret));
        } else if (OB_FAIL(sql_proxy.read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(sql));
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get sql result failed", KR(ret), K(sql));
        } else if (OB_FAIL(construct_log_message_from_sql_result_(
                               *result.get_result(),
                               log_message,
                               not_satisfied_ls_cnt))) {
          LOG_WARN("fail to deal with check arb_member sql result", KR(ret), K(sql));
        } else if (0 != not_satisfied_ls_cnt) {
          _LOG_INFO("[ARB_SERVICE] can not finish arb member's replacement, "
                    "because these %ld log streams have unexpected arb_member:%.*s "
                    "and the check sql is %.*s",
                    not_satisfied_ls_cnt, static_cast<int>(log_message.length()), log_message.ptr(),
                    static_cast<int>(sql.length()), sql.ptr());
          ob_usleep(retry_interval_us);
        } else {
          stop = true;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSqlString update_sql;
    int64_t affected_rows = 0;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(update_sql.assign_fmt("update %s "
                                      "set previous_arbitration_service = '' "
                                      "where arbitration_service_key = 'default'", OB_ALL_ARBITRATION_SERVICE_TNAME))) {
      LOG_WARN("fail to construct update sql", KR(ret));
    } else if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, update_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute write sql", KR(ret), K(update_sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected update single row", KR(ret), K(affected_rows), K(update_sql));
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::get_leader_from_log_stat_table(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  leader_addr.reset();
  if (OB_ISNULL(GCTX.sql_proxy_)
          || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
          || !ls_id.is_valid()
          || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), KP(GCTX.sql_proxy_));
  } else {
    ObSqlString sql;
    const char* table_name = OB_ALL_VIRTUAL_LOG_STAT_TNAME;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT svr_ip, svr_port FROM %s "
          "WHERE tenant_id = %lu AND ls_id = %lu AND role = 'LEADER'",
          table_name, tenant_id, ls_id.id()))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(ls_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get sql result failed", KR(ret), K(tenant_id), K(ls_id), "sql", sql.ptr());
      } else if (OB_FAIL(construct_leader_from_res_(
                             *result.get_result(),
                             leader_addr))) {
        LOG_WARN("construct leader addr from result failed", KR(ret), K(tenant_id), K(ls_id), K(leader_addr));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::construct_leader_from_res_(
    common::sqlclient::ObMySQLResult &res,
    ObAddr &leader_addr)
{
  int ret = OB_SUCCESS;
  ObString ip;
  int64_t port = 0;
  leader_addr.reset();
  if (OB_FAIL(res.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("leader not exist", KR(ret));
    } else {
      LOG_WARN("get next result failed", KR(ret));
    }
  } else if (OB_FAIL(res.get_varchar("svr_ip", ip))) {
    LOG_WARN("fail to get svr_ip from res", KR(ret), K(ip));
  } else if (OB_FAIL(res.get_int("svr_port", port))) {
    LOG_WARN("fail to get svr_port from res", KR(ret), K(port));
  } else if (false == leader_addr.set_ip_addr(ip, static_cast<uint32_t>(port))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid server address", KR(ret), K(ip), K(port));
  } else if (!leader_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("leader addr is invalid", KR(ret), K(leader_addr));
  }
  return ret;
}

int ObArbitrationServiceUtils::get_arb_member(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObSqlString &arb_member_str)
{
  int ret = OB_SUCCESS;
  arb_member_str.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (MOCK_CLUSTER_VERSION_4_2_5_4 > GET_MIN_CLUSTER_VERSION()
             || (CLUSTER_VERSION_4_3_0_0 <= GET_MIN_CLUSTER_VERSION()
                 && CLUSTER_VERSION_4_3_5_2 >= GET_MIN_CLUSTER_VERSION())) {
    // 4.2.5.4 > cluster_version
    // or 4.3.5.2 >= cluster_version >= 4.3.0.0
    // use old logic to get arb member, execute inner sql
    // TODO@jingyu_cr: need to adjust version check when cherry-pick on 4.4
    if (OB_FAIL(get_arb_member_from_log_stat_table_(tenant_id, ls_id, arb_member_str))) {
      LOG_WARN("fail to get arb member from log stat table", KR(ret), K(tenant_id), K(ls_id));
    }
  } else {
    if (OB_FAIL(get_arb_member_through_rpc_(tenant_id, ls_id, arb_member_str))) {
      LOG_WARN("fail to get arb member ");
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::get_arb_member_through_rpc_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObSqlString &arb_member_str)
{
  int ret = OB_SUCCESS;
  arb_member_str.reset();
  ObAddr leader;
  ObMember arb_member;
  obrpc::ObFetchArbMemberArg arg;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(GCTX.location_service_)
             || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.location_service_), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id,
                         tenant_id, ls_id, false/*force_renew*/, leader))) {
    LOG_WARN("failed to get ls leader", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(arg.init(tenant_id, ls_id))) {
    LOG_WARN("fail to init ObFetchArbMemberArg", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader).by(tenant_id)
                        .timeout(GCONF.rpc_timeout).fetch_arb_member(arg, arb_member))) {
    LOG_WARN("fail to send rpc", KR(ret), K(arg), K(leader), K(tenant_id), K(ls_id));
  } else if (!arb_member.is_valid()) {
    // arb_member is invalid means this log stream do not have arb replica
    // just return empty ObSqlString
    arb_member_str.reset();
  } else {
    const ObAddr arb_server = arb_member.get_server();
    char arb_member_buf[MAX_SINGLE_MEMBER_LENGTH] = {'\0'};
    if (OB_FAIL(arb_server.ip_port_to_string(arb_member_buf, MAX_SINGLE_MEMBER_LENGTH))) {
      LOG_WARN("ip_port_to_string failed", KR(ret), K(arb_member), K(arb_server));
    } else if (OB_FAIL(arb_member_str.assign(arb_member_buf))) {
      LOG_WARN("fail to assign arb member to ObSqlString", KR(ret), K(arb_server), K(arb_member_buf));
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::get_arb_member_from_log_stat_table_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObSqlString &arb_member)
{
  int ret = OB_SUCCESS;
  arb_member.reset();
  bool is_compatible = false;
  if (OB_ISNULL(GCTX.sql_proxy_)
          || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
          || !ls_id.is_valid()
          || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(
                         tenant_id, is_compatible))) {
    LOG_WARN("fail to check compat version for arbitration service", KR(ret), K(tenant_id));
  } else if (!is_compatible) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("data version is below 4.1", KR(ret), K(tenant_id), K(ls_id));
  } else {
    ObSqlString sql;
    ObString arbitration_service;
    const char* table_name = OB_ALL_VIRTUAL_LOG_STAT_TNAME;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObASHSetInnerSqlWaitGuard ash_inner_sql_guard(ObInnerSqlWaitTypeId::RS_GET_ARBITRATION_MEMBER);
      if (OB_FAIL(sql.assign_fmt(
          "SELECT arbitration_member FROM %s "
          "WHERE tenant_id = %lu AND ls_id = %lu AND role = 'LEADER'",
          table_name, tenant_id, ls_id.id()))) {
        LOG_WARN("assign sql string failed", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(ls_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get sql result failed", KR(ret), K(tenant_id), K(ls_id), "sql", sql.ptr());
      } else if (OB_FAIL(construct_arb_member_from_res_(
                             *result.get_result(),
                             arb_member))) {
        LOG_WARN("construct arbitration service addr from result failed",
                 KR(ret), K(tenant_id), K(ls_id), K(arb_member));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::construct_arb_member_from_res_(
    common::sqlclient::ObMySQLResult &res,
    ObSqlString &arb_member)
{
  int ret = OB_SUCCESS;
  arb_member.reset();
  ObString arb_member_str;
  if (OB_FAIL(res.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("leader may not exist", KR(ret));
    } else {
      LOG_WARN("get next result failed", KR(ret));
    }
  } else if (OB_FAIL(res.get_varchar("arbitration_member", arb_member_str))) {
    LOG_WARN("fail to get arbitration member from res", KR(ret));
  } else if (OB_FAIL(arb_member.assign(arb_member_str))) {
    LOG_WARN("fail to assign arb member", KR(ret), K(arb_member_str));
  }
  return ret;
}

int ObArbitrationServiceUtils::generate_arb_replica_num(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    int64_t &arb_replica_num)
{
  int ret = OB_SUCCESS;
  ObSqlString arb_member;
  arb_replica_num = 0;
  ObArbitrationServiceStatus arb_service_status;
  bool is_compatible = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(arb_service_status));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_arbitration_service(
                         tenant_id, is_compatible))) {
    LOG_WARN("fail to check compat version for arbitration service", KR(ret), K(tenant_id));
  } else if (!is_compatible) {
    arb_replica_num = 0;
  } else if (OB_FAIL(get_tenant_arbitration_service_status(
                        tenant_id,
                        arb_service_status))) {
    LOG_WARN("fail to get tenant arbitration service status", KR(ret), K(tenant_id));
  } else if (arb_service_status.is_enable_like()) {
    if (OB_FAIL(get_arb_member(
                    tenant_id,
                    ls_id,
                    arb_member))) {
      LOG_WARN("fail to get arb member from log stat table", KR(ret), K(tenant_id), K(ls_id));
    } else if (!arb_member.empty()) {
      // In 4.1, observer keep detecting the status of arb_server since first communication established.
      // Send rpc to detect the status of arb server may cause incompactible problem between 4.1 bp-versions.
      // So we use KEEPALIVE to get the status of arb_server. Under the most worst cases, in_balck() will
      // dely 3 seconds to return the correct status.
      // Above 4.1, we can use rpc to detect the status of arb_server.
      bool is_in_black_list = true;
      ObAddr arb_service_addr;
      obrpc::ObNetKeepAliveData alive_data;
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(arb_service_addr.parse_from_string(arb_member.string()))) {
        LOG_WARN("fail to construct arb service addr", KR(ret), K(arb_member));
      } else if (OB_TMP_FAIL(obrpc::ObNetKeepAlive::get_instance().in_black(
                                 arb_service_addr, is_in_black_list, &alive_data))) {
        LOG_WARN("fail to check arb server whether alive", KR(tmp_ret), K(arb_service_addr));
      } else if (!is_in_black_list) {
        arb_replica_num = 1;
        FLOG_INFO("found a not in black_list A-replica", K(tenant_id), K(ls_id), K(arb_member));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::check_tenant_paxos_replica_number_equals_to_given(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const int64_t paxos_replica_number_to_compare,
    bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  const ObTenantSchema *tenant_schema = NULL;
  int64_t schema_paxos_replica_cnt = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || 0 >= paxos_replica_number_to_compare)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(paxos_replica_number_to_compare));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_schema->get_paxos_replica_num(
                         schema_guard,
                         schema_paxos_replica_cnt))) {
    LOG_WARN("fail to get paxos replica cnt recorded in tenant schema", KR(ret), KPC(tenant_schema));
  } else if (paxos_replica_number_to_compare == schema_paxos_replica_cnt) {
    is_equal = true;
  }
  return ret;
}

int ObArbitrationServiceUtils::wait_all_2f_tenants_arb_service_degration(
    ObISQLClient &sql_proxy,
    const obrpc::ObServerList &svr_list)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval_us = 1000l * 1000l; // 1s
  ObSqlString sql;
  ObTimeoutCtx ctx;
  bool has_tenant_need_to_wait = false;
  if (OB_UNLIKELY(0 >= svr_list.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(svr_list));
  } else if (OB_FAIL(construct_sql_for_degration_waiting_(svr_list, has_tenant_need_to_wait, sql))) {
    LOG_WARN("fail to construct sql for degration waiting", KR(ret), K(svr_list));
  } else if (!has_tenant_need_to_wait) {
    // do nothing
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    const int64_t idx = 0;
    bool stop = false;
    while (OB_SUCC(ret) && !stop) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res_for_arb) {
        sqlclient::ObMySQLResult *result = NULL;
        int64_t not_satisfied_ls_cnt = 0;
        if (0 > ctx.get_timeout()) {
          ret = OB_WAIT_DEGRATION_TIMEOUT;
          LOG_WARN("wait degration finished timeout", KR(ret));
        } else if (OB_FAIL(THIS_WORKER.check_status())) {
          LOG_WARN("check status failed", KR(ret));
        } else if (OB_FAIL(sql_proxy.read(res_for_arb, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(sql));
        } else if (OB_ISNULL(result = res_for_arb.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get result failed", KR(ret), K(sql));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is empty", KR(ret));
          } else {
            LOG_WARN("get next result failed", KR(ret));
          }
        } else if (OB_FAIL(result->get_int(idx, not_satisfied_ls_cnt))) {
          LOG_WARN("get int failed", KR(ret));
        } else if (0 == not_satisfied_ls_cnt) {
          stop = true;
        } else {
          LOG_INFO("waiting degrading servers", KR(ret), K(sql), "left count", not_satisfied_ls_cnt);
          ob_usleep(retry_interval_us);
        }
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::construct_sql_for_degration_waiting_(
    const obrpc::ObServerList &svr_list,
    bool &has_tenant_need_to_wait,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObSqlString tenant_sub_sql;
  ObSqlString server_sub_sql;
  sql.reset();
  has_tenant_need_to_wait = true;
  bool only_consider_2f_tenant = true;
  if (OB_UNLIKELY(0 >= svr_list.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(svr_list));
  //STEP 1: construct tenant to check: those tenant with arbitration service and 2F
  } else if (OB_FAIL(construct_tenants_with_arbitration_service_(
                         only_consider_2f_tenant,
                         tenant_sub_sql))) {
    LOG_WARN("fail to construct tenant list with arbitration service", KR(ret), K(only_consider_2f_tenant));
  } else if (tenant_sub_sql.empty()) {
    // do nothing
    has_tenant_need_to_wait = false;
    LOG_TRACE("has no tenant need to wait");
  } else {
    // STEP 2: construct servers to check: those servers to stop
    const int64_t size = svr_list.size();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < size; ++idx) {
      const ObAddr &server = svr_list[idx];
      char svr_ip[MAX_IP_ADDR_LENGTH] = "\0";
      if (!server.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("addr is not vaild", KR(ret), K(server));
      } else if (!server.ip_to_string(svr_ip, sizeof(svr_ip))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("format ip str failed", KR(ret), K(server));
      } else if (OB_FAIL(server_sub_sql.append_fmt(
                         "OR (b.paxos_member_list LIKE '%%%s:%d%%' AND b.degraded_list NOT LIKE '%%%s:%d%%') ",
                         svr_ip, server.get_port(), svr_ip, server.get_port()))) {
        LOG_WARN("append_fmt failed", KR(ret), K(server));
      }
    }

    //STEP 3: construct the whole sql
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt(
                           "SELECT CAST(COUNT(*) AS SIGNED) AS not_satisfied_ls_cnt "
                           "FROM %s AS a LEFT JOIN %s AS b "
                           "ON a.tenant_id = b.tenant_id AND a.ls_id = b.ls_id AND b.role = 'LEADER' "
                           "WHERE (a.tenant_id in (%.*s)) "                // certain tenant: "where tenant_id in (xxx, xxx)"
                           "AND ((b.role IS NULL) "             // those log stream without leader
                                 "OR (b.role = 'LEADER' AND (FALSE %.*s)))",  // those log stream with not degraded member
                           share::OB_ALL_VIRTUAL_LS_STATUS_TNAME,
                           share::OB_ALL_VIRTUAL_LOG_STAT_TNAME,
                           tenant_sub_sql.string().length(),
                           tenant_sub_sql.string().ptr(),
                           server_sub_sql.string().length(),
                           server_sub_sql.string().ptr()))) {
      LOG_WARN("assign_fmt failed", KR(ret), K(tenant_sub_sql), K(server_sub_sql));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_ADD_CLUSTER_INFO_ERROR);
int ObArbitrationServiceUtils::add_cluster_info_to_arb_server(
    const ObString &arb_member)
{
  int ret = OB_SUCCESS;
  ObAddr arb_server;
  if (OB_UNLIKELY(ERRSIM_ADD_CLUSTER_INFO_ERROR)) {
    ret = ERRSIM_ADD_CLUSTER_INFO_ERROR;
  } else if (OB_UNLIKELY(arb_member.empty()) || OB_ISNULL(GCTX.arb_gcs_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arb_member));
  } else if (OB_FAIL(arb_server.parse_from_string(arb_member))) {
    LOG_WARN("fail to parse ObAddr from ObString", KR(ret), K(arb_member));
  } else if (OB_FAIL(GCTX.arb_gcs_->add_arb_service(arb_server, GCONF.cluster_id, GCONF.cluster.str()))) {
    // add_arb_service is reentrant
    LOG_WARN("failed to add cluster info to arb server", KR(ret), K(arb_server));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_REMOVE_CLUSTER_INFO_ERROR);
int ObArbitrationServiceUtils::remove_cluster_info_from_arb_server(
    const ObString &arb_member)
{
  int ret = OB_SUCCESS;
  ObAddr arb_server;
  if (OB_UNLIKELY(ERRSIM_REMOVE_CLUSTER_INFO_ERROR)) {
    ret = ERRSIM_REMOVE_CLUSTER_INFO_ERROR;
  } else if (OB_UNLIKELY(arb_member.empty()) || OB_ISNULL(GCTX.arb_gcs_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arb_member));
  } else if (OB_FAIL(arb_server.parse_from_string(arb_member))) {
    LOG_WARN("fail to parse ObAddr from ObString", KR(ret), K(arb_member));
  } else if (OB_FAIL(GCTX.arb_gcs_->remove_arb_service(arb_server, GCONF.cluster_id, GCONF.cluster.str()))) {
    LOG_WARN("failed to remove cluster info to arb server", KR(ret), K(arb_server));
  }
  return ret;
}

int ObArbitrationServiceUtils::wakeup_all_tenant_arbitration_service_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_id_array;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(GCTX.schema_service_, tenant_id_array))) {
    LOG_WARN("fail to construct tenant_ids", KR(ret));
  } else {
    FLOG_INFO("start to wakeup all tenant arbitration service", K(tenant_id_array));
    for (int64_t index = 0; index < tenant_id_array.count(); ++index) {
      const uint64_t tenant_id_to_wakeup = tenant_id_array.at(index);
      if (is_user_tenant(tenant_id_to_wakeup)) {
        // skip user tenant, because user tenant has no arbitration service
        LOG_TRACE("skip wakeup user tenant", K(tenant_id_to_wakeup));
      } else if (OB_TMP_FAIL(wakeup_single_tenant_arbitration_service(tenant_id_to_wakeup))) {
        LOG_WARN("fail to wakeup tenant arbitration service", KR(tmp_ret), K(tenant_id_to_wakeup));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::wakeup_single_tenant_arbitration_service(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObNotifyTenantThreadArg arg;
  int tmp_ret = OB_SUCCESS;
  ObAddr leader;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arg.init(tenant_id, obrpc::ObNotifyTenantThreadArg::ARBITRATION_SERVICE))) {
    LOG_WARN("fail to init notify tenant thread arg", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ is NULL", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id,
                     tenant_id, SYS_LS, false/*force_renew*/, leader))) {
    LOG_WARN("failed to get ls leader", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader).by(tenant_id)
                        .timeout(GCONF.rpc_timeout).notify_tenant_thread(arg))) {
    LOG_WARN("fail to send rpc", KR(ret), K(arg));
  } else {
    LOG_INFO("success to wakeup tenant arbitration service", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObArbitrationServiceUtils::get_arb_member_from_leader(
    const obrpc::ObFetchArbMemberArg &arg,
    ObMember &arb_member)
{
  int ret = OB_SUCCESS;
  arb_member.reset();
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.omt_));
  } else if (OB_UNLIKELY(!GCTX.omt_->has_tenant(arg.get_tenant_id()))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("local server does not have tenant resource", KR(ret), K(arg));
  } else {
    MTL_SWITCH(arg.get_tenant_id()) {
      logservice::ObLogService *log_service = nullptr;
      if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLogService is null", KR(ret), K(arg));
      } else if (OB_FAIL(inner_check_leader_and_get_arb_member_(arg, *log_service, arb_member))){
        LOG_WARN("fail to inner check leader and get arbitration member", KR(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceUtils::inner_check_leader_and_get_arb_member_(
    const obrpc::ObFetchArbMemberArg &arg,
    logservice::ObLogService &log_service,
    ObMember &arb_member)
{
  int ret = OB_SUCCESS;
  arb_member.reset();
  common::ObRole role = FOLLOWER;
  int64_t proposal_id = 0;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(log_service.open_palf(arg.get_ls_id(), palf_handle_guard))) {
    LOG_WARN("open palf failed", KR(ret), K(arg));
  } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
    LOG_WARN("get role failed", KR(ret), K(arg));
  } else if (OB_UNLIKELY(!is_strong_leader(role))) {
    ret = OB_LS_NOT_LEADER;
    LOG_WARN("local replica is not log stream leader", KR(ret), K(arg), K(role), K(proposal_id));
  } else if (OB_FAIL(palf_handle_guard.get_arbitration_member(arb_member))) {
    LOG_WARN("fail to get arbitration member", KR(ret), K(arg));
  }
  return ret;
}
} // end share
} // end oceanbase
