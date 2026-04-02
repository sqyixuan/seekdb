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

#ifndef OCEANBASE_ROOTSERVER_OB_SERVER_ZONE_OP_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_SERVER_ZONE_OP_SERVICE_H

#include "share/ob_server_table_operator.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
struct ObRsListArg;
// struct ObAdminServerArg;
}
namespace share
{
class ObLSTableOperator;
class ObAllServerTracer;
}
namespace rootserver
{
class ObIServerChangeCallback;
class ObUnitManager;
class ObServerZoneOpService
{
public:
  ObServerZoneOpService();
  virtual ~ObServerZoneOpService();
  int init(
      ObIServerChangeCallback &server_change_callback,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      share::ObLSTableOperator &lst_operator,
      ObUnitManager &unit_manager,
      ObMySQLProxy &sql_proxy);
  // Add new servers to a specified（optional) zone in the cluster.
  // The servers should be empty and the zone should be active.
  // This operation is successful
  // if the servers' info are inserted into __all_server table successfully.
  //
  // @param[in]  servers	the servers which we want to add
  // @param[in]  zone     the zone in which the servers will be located. If it's empty,
  //                      the zone specified in the servers' local config will be picked
  //
  // @ret OB_SUCCESS 		           add successfully
  // @ret OB_ZONE_NOT_ACTIVE       the specified zone is not active
  // @ret OB_SERVER_ZONE_NOT_MATCH the zone specified in the server's local config is not the same
  //                               as the zone specified in the system command ADD SERVER
  //                               or both are empty
  // @ret OB_ENTRY_EXIST           there exists servers which are already added
  //
  // @ret other error code		     failure
  int add_servers(const ObIArray<ObAddr> &servers,
      const ObZone &zone,
      const bool is_bootstrap = false);
private:
  int check_startup_mode_match_(const share::ObServerMode startup_mode);
  int zone_checking_for_adding_server_(
      const common::ObZone &rpc_zone,
      ObZone &picked_zone);
  int add_server_(
      const common::ObAddr &server,
      const uint64_t server_id,
      const common::ObZone &zone,
      const int64_t sql_port,
      const share::ObServerInfoInTable::ObBuildVersion &build_version,
      const ObIArray<share::ObZoneStorageTableInfo> &storage_infos);
  int check_and_update_service_epoch_(common::ObMySQLTransaction &trans);
  int fetch_new_server_id_(uint64_t &server_id);
  bool check_server_index_(
      const uint64_t candidate_server_id,
      const common::ObIArray<uint64_t> &server_id_in_cluster) const;
  void end_trans_and_on_server_change_(
      int &ret,
      common::ObMySQLTransaction &trans,
      const char *op_print_str,
      const common::ObAddr &server,
      const common::ObZone &zone,
      const int64_t start_time);
  int precheck_server_empty_and_get_zone_(const ObAddr &server,
      const ObTimeoutCtx &timeout,
      const bool is_bootstrap,
      ObZone &zone);
  int prepare_server_for_adding_server_(const ObAddr &server,
      const ObTimeoutCtx &timeout,
      const bool &is_bootstrap,
      ObZone &picked_zone,
      obrpc::ObPrepareServerForAddingServerArg &rpc_arg,
      obrpc::ObPrepareServerForAddingServerResult &rpc_result);
  bool is_inited_;
  ObIServerChangeCallback *server_change_callback_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObMySQLProxy *sql_proxy_;
  share::ObLSTableOperator *lst_operator_;
  share::ObServerTableOperator st_operator_;
  ObUnitManager *unit_manager_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerZoneOpService);
};
} // rootserver
} // oceanbase

#endif
