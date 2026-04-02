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

#ifndef _OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_UTILS_H
#define _OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_UTILS_H

#include "share/arbitration_service/ob_arbitration_service_info.h" // ObArbitrationServiceInfo
#include "share/ob_rpc_struct.h"                                   // ObAdminAddArbitrationServiceArg
#include "observer/ob_server_struct.h"                             // GCTX
#include "lib/mysqlclient/ob_mysql_transaction.h"                  // ObMySQLTransaction

namespace oceanbase
{
namespace share
{
class ObArbitrationServiceUtils
{
public:
  static int add_arbitration_service(
      ObISQLClient &sql_proxy,
      const obrpc::ObAdminAddArbitrationServiceArg &arg);

  static int remove_arbitration_service(
      ObISQLClient &sql_proxy,
      const obrpc::ObAdminRemoveArbitrationServiceArg &arg);

  static int replace_arbitration_service(
      ObISQLClient &sql_proxy,
      const obrpc::ObAdminReplaceArbitrationServiceArg &arg);

  // check whether can promote arb service status
  // @params[in]  sql_proxy, sql proxy to use
  // @params[in]  tenant_id, which tenant to promote
  // @params[in]  old_status, old tenant's arb service status
  // @params[in]  new_status, new tenant's arb service status
  // @params[out] can_promote, the result
  static int check_can_promote_arbitration_service_status(
      ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObArbitrationServiceStatus &old_status,
      const ObArbitrationServiceStatus &new_status,
      bool &can_promote);

  // get tenant's arbitration service status
  // @params[in]  tenant_id, which tenant status to get
  // @params[out] arb_service_status, the result
  static int get_tenant_arbitration_service_status(
      const uint64_t tenant_id,
      ObArbitrationServiceStatus &arb_service_status);

  // get leader from __all_virtual_log_stat
  // @params[in]  tenant_id, which tenant info to get
  // @params[in]  ls_id, which log stream info to get
  // @params[out] leader_addr, the leader of log stream
  static int get_leader_from_log_stat_table(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObAddr &leader_addr);

  // generate the count of arb replica of a log stream
  // @params[in]  tenant_id, which tenant to check
  // @params[in]  ls_id, which log stream to check
  // @params[out] arb_replica_num, the result
  static int generate_arb_replica_num(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      int64_t &arb_replica_num);

  // check tenant's paxos replica number equals to given one
  // @params[in]  schema_guard, schema guard to use
  // @params[in]  tenant_id, which tenant to check
  // @params[in]  paxos_replica_number_to_compare, the given paxos replica number
  // @params[out] is_equal, the result
  static int check_tenant_paxos_replica_number_equals_to_given(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const int64_t paxos_replica_number_to_compare,
      bool &is_equal);

  // wait tenant with 2F and arb service finish degration
  // @params[in]  sql_proxy, the proxy to use
  // @params[in]  svr_list, which servers to degrade
  static int wait_all_2f_tenants_arb_service_degration(
      ObISQLClient &sql_proxy,
      const obrpc::ObServerList &svr_list);

  // wait all tenant with arb service has expected arb member
  // @params[in]  sql_proxy, the proxy to use
  // @params[in]  arb_member, expected arb_mmeber
  // @params[in]  previous_arb_member, the old arb member
  static int wait_all_tenant_with_arb_has_arb_member(
      ObISQLClient &sql_proxy,
      const ObString &arb_member,
      const ObString &previous_arb_member);

  static int add_cluster_info_to_arb_server(
      const ObString &arb_member);

  static int remove_cluster_info_from_arb_server(
      const ObString &arb_member);

  static int wakeup_single_tenant_arbitration_service(const uint64_t meta_tenant_id);

  // get arbitration member of log stream
  // @params[in]  arg, tenant_id and ls_id
  // @params[in]  arb_member, arb_member of this log stream
  static int get_arb_member_from_leader(
      const obrpc::ObFetchArbMemberArg &arg,
      ObMember &arb_member);

  // get arb member from table or rpc
  // @params[in]  tenant_id, which tenant
  // @params[in]  ls_id, which ls_id
  // @params[in]  arb_member_str, arb_member of this log stream
  static int get_arb_member(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObSqlString &arb_member_str);

  // disallow construct
  ObArbitrationServiceUtils() {}
  ~ObArbitrationServiceUtils() {}

private:
  // add cluster info to arb server if needed
  // @params[in]  sql_proxy, the proxy to use
  // @params[in]  arbitration_service_key, the key to compare
  // @params[in]  arg, replacement arg
  // @params[out] success_add_cluster_info, the result
  static int try_add_cluster_info_if_needed_(
      ObISQLClient &sql_proxy,
      const ObString &arbitration_service_key,
      const obrpc::ObAdminReplaceArbitrationServiceArg &arg,
      bool &success_add_cluster_info);

  // make sure all tenant has disabled arb service
  // @params[in]  sql_proxy, the proxy to use
  static int check_can_remove_arbitration_service_(
      ObISQLClient &sql_proxy);

  // construct tenant list to display from sql result
  // @params[in]  res, sql result
  // @params[out] tenant_list, comma connected tenant list in form "1001, 1002, ...."
  // @params[out] has_tenant, whether tenant_list has tenant
  static int construct_tenant_list_from_sql_result_(
      common::sqlclient::ObMySQLResult &res,
      ObSqlString &tenant_list,
      bool &has_tenant);

  // check whether can replace arbitration service
  // @params[in]  old_arbitration_service_info, info before replacement
  // @params[in]  new_arbitration_service_info, info after replacement
  // @params[out] can_replace, whether can do replace
  // @params[out] need_replace, whether need to de replace,
  // @params[out] need_add_cluster_info, whether add cluster info to new arb server
  // comment: can_replace && !need_replace only when replace A with B again when it is replacing A with B
  //          need_add_cluster_info only when it is a valid replacement except these actions(rollback, do current replacement again)
  static int check_can_replace_arbitration_service_(
      const ObArbitrationServiceInfo &old_arbitration_service_info,
      const ObArbitrationServiceInfo &new_arbitration_service_info,
      bool &can_replace,
      bool &need_replace,
      bool &need_add_cluster_info);

  // construct log message from sql result
  // @params[in]  res, the check sql result
  // @params[out] log_message, the log message to build
  // @params[out] not_satisfied_ls_cnt, the ls cnt of not satisfied ones
  // @params[out] can_promote, the result
  static int construct_log_message_from_sql_result_(
      common::sqlclient::ObMySQLResult &res,
      ObSqlString &log_message,
      int64_t &not_satisfied_ls_cnt);

  // check can promote status from disabling/enabling to stable status
  // @params[in]  sql_proxy, sql proxy to use
  // @params[in]  tenant_id, which tenant to promote
  // @params[in]  old_status, old tenant status
  // @params[in]  new_status, new status
  // @params[out] can_promote, the result
  static int check_can_promote_status_to_stable_one_(
      ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObArbitrationServiceStatus &old_status,
      const ObArbitrationServiceStatus &new_status,
      bool &can_promote);

  // check can enable tenant arb service
  // @params[in]  sql_proxy, sql proxy to use
  // @params[in]  tenant_id, which tenant to promote
  // @params[in]  old_status, old tenant status
  // @params[in]  new_status, new status
  // @params[out] can_promote, the result
  static int check_can_enable_arb_service_(
      ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObArbitrationServiceStatus &old_status,
      const ObArbitrationServiceStatus &new_status,
      bool &can_promote);

  // check can disable tenant arb service
  // @params[in]  sql_proxy, sql proxy to use
  // @params[in]  tenant_id, which tenant to promote
  // @params[in]  old_status, old tenant status
  // @params[in]  new_status, new status
  // @params[out] can_promote, the result
  static int check_can_disable_arb_service_(
      ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObArbitrationServiceStatus &old_status,
      const ObArbitrationServiceStatus &new_status,
      bool &can_promote);

  // before replace arb service, check whether has log stream in degrading status
  static int check_degrading_log_stream_for_replace_arb_service_();

  // before disable tenant arb service, check whether has log stream in degrading status
  // @params[in]  tenant_id, which tenant to disable arb service
  // @params[in]  sql_proxy, the proxy to use
  static int check_degrading_log_stream_for_disable_tenant_arb_service_(
      const uint64_t tenant_id,
      ObISQLClient &sql_proxy);

  // check has log stream in degrading
  // @params[in]  tenant_sub_sql, which tenant to check
  // @params[in]  sql_proxy, sql proxy to use
  // @params[out] degrading_ls_list, the list of log stream in degrading
  // @params[out] degrading_ls_cnt, the number of log stream in degrading
  static int check_has_degrading_log_stream_(
      const ObSqlString &tenant_sub_sql,
      ObISQLClient &sql_proxy,
      ObSqlString &degrading_ls_list,
      int64_t &degrading_ls_cnt);

  // construct arb member from sql result
  // @params[in]  res, sql result
  // @params[out] arb_member, leader addr of log stream
  static int construct_leader_from_res_(
      common::sqlclient::ObMySQLResult &res,
      ObAddr &leader_addr);

  // construct arb member from sql result
  // @params[in]  res, sql result
  // @params[out] arb_member, arb_member of log stream
  static int construct_arb_member_from_res_(
      common::sqlclient::ObMySQLResult &res,
      ObSqlString &arb_member);

  // construct sql to check whether degration finished
  // @params[in]  svr_list, which servers to degrade
  // @params[out] has_tenant_need_to_wait, whether has tenant to wait
  // @params[out] sql, the sql builded
  static int construct_sql_for_degration_waiting_(
      const obrpc::ObServerList &svr_list,
      bool &has_tenant_need_to_wait,
      ObSqlString &sql);

  // construct sql to wait replacement finish
  // @params[in]  arb_member, expected arb member
  // @params[out] sql, the check sql builded
  // @params[out] has_tenant_need_to_wait, whether need to wait
  static int construct_sql_for_replacement_waiting_(
      const ObString &arb_member,
      ObSqlString &sql,
      bool &has_tenant_need_to_wait);

  // contruct tenant list with arbitration service
  // @params[in]  only_consider_2f_tenant, only consider 2F tenant
  // @params[out] tenant_list, the list of tenants with arbitration service
  static int construct_tenants_with_arbitration_service_(
      const bool only_consider_2f_tenant,
      ObSqlString &tenant_list);

  // check local is log stream leader and get arb member
  // @params[in]  arg, which tenant and which log stream
  // @params[in]  log_service, pald to use
  // @params[out] arb_member, arbitration member of this log stream
  static int inner_check_leader_and_get_arb_member_(
      const obrpc::ObFetchArbMemberArg &arg,
      logservice::ObLogService &log_service,
      ObMember &arb_member);

  static int wakeup_all_tenant_arbitration_service_();

  // get arb_member from __all_virtual_log_stat
  // @params[in]  tenant_id, which tenant info to get
  // @params[in]  ls_id, which log stream info to get
  // @params[out] arb_member, arb_member of log stream
  static int get_arb_member_from_log_stat_table_(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObSqlString &arb_member);

  static int get_arb_member_through_rpc_(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObSqlString &arb_member_str);
};
} // end share
} // end oceanbase

#endif // _OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_UTILS_H
