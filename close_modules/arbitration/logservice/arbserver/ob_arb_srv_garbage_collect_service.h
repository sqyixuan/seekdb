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

#ifndef _OCEANBASE_OBSERVER_OB_ARB_SRV_GARBAGE_COLLECT_SERVICE_H_
#define _OCEANBASE_OBSERVER_OB_ARB_SRV_GARBAGE_COLLECT_SERVICE_H_

#include "lib/utility/ob_macro_utils.h"                 // DISALLOW_COPY_AND_ASSIGN
#include "lib/utility/ob_print_utils.h"                 // TO_STRING_KV
#include "lib/lock/ob_spin_lock.h"                      // ObSpinLock
#include "lib/container/ob_se_array.h"                  // ObSEArray
#include "lib/task/ob_timer.h"                          // ObTimerTask
#include "logservice/ob_log_base_type.h"                // ObIReplaySubHandler...
#include "share/ob_rpc_struct.h"                        // LSIDArray

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObAddr;
}
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace share
{
class ObLSID;
class SCN;
}
namespace arbserver
{

class ObArbGarbageCollectService : public common::ObTimerTask
{
public:
  ObArbGarbageCollectService();
  ~ObArbGarbageCollectService();

public:

  int init(const common::ObAddr &self_addr,
           const int tg_id,
           obrpc::ObSrvRpcProxy *rpc_proxy,
           common::ObMySQLProxy *sql_proxy);
  // NB: start when detect there is arb server in this cluster.
  int start();
  // NB: stop when detect there is no arb server in this cluster.
  int stop();
  void wait();
  void destroy();
  virtual void runTimerTask();
  // desc: add a arb service to the cluster, errors returned by this function
  //       can not be ignored.
  // @param[in]
  // const common::ObAddr &arb_server: the arb server wanted to add
  // const int64_t cluster_id: the cluster_id of this cluster
  // const common::ObString &cluster_name: the cluster_name of this cluster
  // @param[out]
  // OB_SUCCESS: the arb service has been added successfully
  // OB_INVALID_ARGUMENT: invalid argument
  // OB_NOT_MASTER: self is not the leader
  // OB_ARBITRATION_SERVICE_ALREADY_EXIST: a cluster has already been
  //   added to the arb service, its cluster_id is same to the cluster_id
  //   arg, but its cluster_name id different from the cluster_name arg
  // OTHER: errors
  int add_arb_service(const common::ObAddr &arb_server,
                      const int64_t cluster_id,
                      const common::ObString &cluster_name);
  // desc: remove the arb service of the cluster, errors returned by
  //        this function can be ignored (besides OB_NOT_MASTER).
  // @param[in]
  // const common::ObAddr &arb_server: the arb server wanted to remove
  // const int64_t cluster_id: the cluster_id of this cluster
  // const common::ObString &cluster_name: the cluster_name of this cluster
  // @param[out]
  // OB_SUCCESS: the arb service has been removed successfully
  // OB_INVALID_ARGUMENT: invalid argument 
  // OB_NOT_MASTER: self is not the leader
  // OTHER: errors
  int remove_arb_service(const common::ObAddr &arb_server,
                         const int64_t cluster_id,
                         const common::ObString &cluster_name);
  TO_STRING_KV(K_(self_addr), KP(rpc_proxy_), KP(sql_proxy_), 
      K_(proposal_id), K_(seq), K_(is_master));
private:
  int try_update_role_();
  int64_t proposal_id_;
  int64_t seq_;
  bool is_master_;
  common::ObSpinLock role_change_lock_;
private:
  static constexpr int64_t DEFAULT_TENANT_NUM = 32;
  static constexpr int64_t DEFAULT_ADDR_NUM = 1;
  static constexpr int64_t SCAN_TIMER_INTERVAL = 5 * 1000 * 1000;
  typedef common::ObSEArray<uint64_t, DEFAULT_TENANT_NUM> TenantIDS;
  typedef common::ObSEArray<common::ObAddr, DEFAULT_ADDR_NUM> AddrS;

  int construct_rpc_epoch_(GCMsgEpoch &epoch);

  //============================== InnerTable ===================================
  int construct_ls_id_array_(TenantLSIDSArray &tenant_ls_ids_array);
  // @brief get max tenant id from __all_tenant_history
  int get_max_tenant_id_(uint64_t &max_tenant_id);
  // @brief get all tenant ids from __all_tenant
  int get_tenant_ids_(TenantIDS &tenant_ids);

  int construct_tenant_ls_ids_for_each_tenant_(const TenantIDS &tenant_ids,
                                               TenantLSIDSArray &tenant_ls_ids_array);

  // @brief get max ls id for specified 'tenant_id' and set it into 'tenant_ls_ids'
  int get_tenant_max_ls_id_(const uint64_t tenant_id, TenantLSIDS &tenant_ls_ids);
  // @brief get all ls ids for specified 'tenant_id' and set it into 'tenant_ls_ids'
  int get_tenant_ls_ids_(const uint64_t tenant_id, TenantLSIDS &tenant_ls_ids);
  int get_arb_addr_from_inner_table_(AddrS &addrs);
  //============================== InnerTable ===================================

  int send_rpc_to_arb_server_(const AddrS &addrs,
                              const GCMsgEpoch &epoch,
                              const TenantLSIDSArray &ls_ids);

private:
  DISALLOW_COPY_AND_ASSIGN(ObArbGarbageCollectService);

private:
  common::ObAddr self_addr_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  common::ObMySQLProxy *sql_proxy_;
  int tg_id_;
  bool is_inited_;
  common::ObSpinLock lock_;
};
} // end namespace arbserver
} // end namespace oceanbase

#endif // _OCEANBASE_OBSERVER_OB_ARB_SRV_GARBAGE_COLLOECT_SERVICE_H_
