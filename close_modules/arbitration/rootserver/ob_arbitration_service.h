#ifdef OB_BUILD_ARBITRATION
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

#ifndef OCEANBASE_ROOTSERVER_OB_ARBITRATION_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_ARBITRATION_SERVICE_H

#include "rootserver/ob_tenant_thread_helper.h" //ObTenantThreadHelper
#include "share/arbitration_service/ob_arbitration_service_table_operator.h"    // for OBArbitrationServiceTableOperator
#include "share/arbitration_service/ob_arbitration_service_replica_task_info.h" // for ObArbitrationServiceReplicaInfo
#include "share/arbitration_service/ob_arbitration_service_replica_task_table_operator.h" // for ObArbitrationServiceReplicaTaskTableOperator
#include "share/scn.h"//SCN

namespace oceanbase
{
using namespace share;
namespace share
{
struct ObLSStatusInfo;
class SCN;
}
namespace common
{
class ObMySQLProxy;
}
namespace rootserver
{
class ObArbitrationService : public rootserver::ObTenantThreadHelper,
                             public logservice::ObICheckpointSubHandler,
                             public logservice::ObIReplaySubHandler
{
public:
  ObArbitrationService()
    : inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      service_epoch_(0),
      sql_proxy_(nullptr),
      task_table_operator_(),
      arbitration_service_table_operator_(),
      task_infos_() {}
  virtual ~ObArbitrationService() {}
  int init();
  int start();
  void destroy();
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObArbitrationService)
public:
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(SCN &rec_scn) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }

private:
  // only data version is up to 4.1, arb service can run
  int wait_data_version_update_to_newest_();

  int check_tenant_schema_is_ready_(
      bool &is_ready);

  // load service epoch from table and update if needed
  // @params[out]  can_do_service, is current leader and has biggest epoch
  int load_and_update_service_epoch_(
      bool &can_do_service);

  // remove task from table and insert history in trans
  // @params[in]  sql_proxy, proxy to use
  // @params[in]  task_info, which task to do finish
  // @params[in]  ret_code, the execution result of this task
  int finish_task_(
      common::ObISQLClient &sql_proxy,
      const ObArbitrationServiceReplicaTaskInfo &task_info,
      const int ret_code);

  // check whether it is current leader
  // @params[in]  sql_proxy, proxy to use
  // @return OB_STATE_NOT_MATCH if service epoch in memory is different from the one in table
  int check_service_epoch_(
      common::ObISQLClient &sql_proxy);

  // deal with remained tasks by previous leader
  // @params[in]  tenant_id, which tenant tasks to handle
  int deal_with_task_in_table_(
      const uint64_t tenant_id);

  // check a remained task still need to process
  // @params[in]  task_info, one task in table
  // @params[in]  arbitration_service_status
  // @params[out] need_execute, whether need to execute
  int check_task_still_need_execute_(
      const ObArbitrationServiceReplicaTaskInfo &task_info,
      const ObArbitrationServiceStatus &arbitration_service_status,
      bool &need_execute);

  // execute a task
  // @params[in]  task_info, the task to execute
  // @params[in]  current_arb_service_status, tenant's arb service status
  int execute_task_(
      const ObArbitrationServiceReplicaTaskInfo &task_info,
      const ObArbitrationServiceStatus &arbitration_service_status);

  // execute a remove replica task
  // @params[in]  leader_addr, which server to execute this task
  // @params[in]  task_info, the task to execute
  // @params[in]  current_arb_service_status, tenant's arb service status
  int do_execute_remove_replica_task_(
      const ObAddr &leader_addr,
      const ObArbitrationServiceReplicaTaskInfo &task_info,
      const ObArbitrationServiceStatus &arbitration_service_status);

  // execute a add replica task
  // @params[in]  leader_addr, which server to execute this task
  // @params[in]  task_info, the task to execute
  // @params[in]  current_arb_service_status, tenant's arb service status
  int do_execute_add_replica_task_(
      const ObAddr &leader_addr,
      const ObArbitrationServiceReplicaTaskInfo &task_info,
      const ObArbitrationServiceStatus &arbitration_service_status);

  // do one round tenant arbitration service tasks
  // @params[in]  tenant_id, which tenant to handle
  int do_tenant_arbitration_service_task_(
      const uint64_t tenant_id);

  // do one round ls arbitration service tasks
  // @params[in]  tenant_id, which tenant to handle
  // @params[in]  ls_status_info, infos about the log stream to handle
  // @params[in]  arbitration_service_status, current arb service status
  int do_ls_arbitration_service_task_(
      const uint64_t tenant_id,
      const share::ObLSStatusInfo &ls_status_info,
      const ObArbitrationServiceStatus &arbitration_service_status);

  // clean remained replica which may created by slow rpc
  // @params[in]  tenant_id, which tenant info to hanndle
  // @params[in]  ls_id, which log stream info to hanndle
  // @params[in]  current_arb_service_status, tenant's arb service status
  int clean_remained_replicas_without_arb_service_(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObArbitrationServiceStatus &current_arb_service_status);

  // construct and execute remove replica task
  // @params[in]  tenant_id, which tenant info to hanndle
  // @params[in]  ls_id, which log stream info to hanndle
  // @params[in]  arb_service, arbitration service addr
  // @params[in]  arb_service_status, tenant's arb service status
  int construct_and_execute_remove_replica_task_(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObString &arb_service,
      const ObArbitrationServiceStatus &arb_service_status);

  // construct and execute add replica task
  // @params[in]  tenant_id, which tenant info to hanndle
  // @params[in]  ls_id, which log stream info to hanndle
  // @params[in]  arb_service, arbitration service addr
  // @params[in]  arb_service_status, tenant's arb service status
  int construct_and_execute_add_replica_task_(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObString &arb_service,
      const ObArbitrationServiceStatus &arb_service_status);

  // try to promote tenant arbitration service status
  int promote_new_tenant_arbitration_service_status_();

  // construct ddl arg and do promotion
  // @params[in]  new_status, new arb service status
  int construct_arg_and_promote_(
      const ObArbitrationServiceStatus &new_status);

  // check whether can do promote
  // @params[in]  old_status, old arb service status
  // @params[in]  task_count, task count in table
  // @params[out] can_do_promote, the result
  int previous_check_for_promote_operation_(
      const ObArbitrationServiceStatus &old_status,
      const int64_t task_count,
      bool &can_do_promote);

  // get sync_rpc timeout between observers to add/remove arb replica
  // At least 9s
  int64_t get_sync_rpc_timeout_() const;

private:
  static const int64_t IDLE_TIME_US = 10 * 1000 * 1000L;  // 10s
  static const int64_t BUSY_IDLE_TIME_US = 100 * 1000L;  // 100ms

  bool inited_;
  uint64_t tenant_id_;
  int64_t service_epoch_;
  common::ObMySQLProxy *sql_proxy_;
  ObArbitrationServiceReplicaTaskTableOperator task_table_operator_;
  ObArbitrationServiceTableOperator arbitration_service_table_operator_;
  ObArbitrationServiceReplicaTaskTableOperator::ObArbitrationServiceReplicaTaskInfoList task_infos_;
};
}//namespace rootserver end
}//namespce oceanbase end

#endif /* !OCEANBASE_ROOTSERVER_OB_ARBITRATION_SERVICE_H */
#endif
