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

#ifndef OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_REPLICA_TASK_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_REPLICA_TASK_TABLE_OPERATOR_H_

#include "lib/mysqlclient/ob_mysql_result.h" // for ObMySQLResult
#include "share/ob_ls_id.h"                  // for ObLSID
#include "share/ob_define.h"                 //  for ObTaskID
#include "share/arbitration_service/ob_arbitration_service_replica_task_info.h" // for ObArbitrationServiceReplicaTaskInfo

namespace oceanbase
{
namespace share
{
class ObArbitrationServiceReplicaTaskInfo;
// [class_full_name] ObArbitrationServiceReplicaTaskTableOperator
// [class_functions] Use this class to build info in __all_arbitration_service_replica_task
// [class_attention] None
class ObArbitrationServiceReplicaTaskTableOperator
{
  OB_UNIS_VERSION(1);
public:
  ObArbitrationServiceReplicaTaskTableOperator() {}
  virtual ~ObArbitrationServiceReplicaTaskTableOperator() {}

  static const int64_t DEFAULT_ARBITRATION_SERVICE_REPLICA_TASK_COUNT = 100;
  typedef common::ObSEArray<ObArbitrationServiceReplicaTaskInfo,
                  DEFAULT_ARBITRATION_SERVICE_REPLICA_TASK_COUNT,
                  ObNullAllocator> ObArbitrationServiceReplicaTaskInfoList;

  // insert a new info of arbitration service replica task
  // @params[in]  sql_proxy, sql_proxy to get infos
  // @params[in]  arb_replica_task_info, the info to insert
  // @return OB_ENTRY_EXIST if this task already exist
  static int insert(
      common::ObISQLClient &sql_proxy,
      const ObArbitrationServiceReplicaTaskInfo &arb_replica_task_info);

  // remove arbitration service replica task info by key
  // @params[in]  sql_proxy, sql_proxy to update infos
  // @oarams[in]  tenant_id, which tenant's task to remove
  // @params[in]  ls_id, which log stream's task to remove
  // @return OB_ENTRY_NOT_EXIST if this task not exist
  static int remove(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id);

  // get all arbitration service replica task info belongs to this tenant
  // @params[in]  sql_proxy, sql_proxy to update infos
  // @params[in]  tenant_id, which tenant's task to get
  // @params[in]  lock_line, whether to lock lines
  // @params[out] task_infos, task infos to get
  static int get_all_tasks(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const bool lock_line,
    ObArbitrationServiceReplicaTaskInfoList &task_infos);

  // insert the result of execution of this task into table
  // @params[in]  sql_proxy, sql_proxy to update infos
  // @params[in]  arb_replica_task_info, task info
  // @params[in]  task_ret, the result of task
  static int insert_history(
    common::ObISQLClient &sql_proxy,
    const ObArbitrationServiceReplicaTaskInfo &arb_replica_task_info,
    const int ret_code);

private:
  // construct arbitration service replica task info from sql res line
  // @params[in]  res, result read from table
  // @params[out] arb_replica_task_info, single info to build
  static int construct_arb_replica_task_info_(
      common::sqlclient::ObMySQLResult &res,
      ObArbitrationServiceReplicaTaskInfo &arb_replica_task_info);

  // construct arbitration service replica task infos from table
  // @params[in]  res, result read from table
  // @params[out] task_infos, the infos to build
  static int construct_arb_replica_task_infos_(
      common::sqlclient::ObMySQLResult &res,
      ObArbitrationServiceReplicaTaskInfoList &task_infos);
};
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_REPLICA_TASK_TABLE_OPERATOR_H_
