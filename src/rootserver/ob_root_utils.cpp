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

#include "ob_root_utils.h"
#include "logservice/ob_log_service.h"
#include "share/ob_primary_zone_util.h"           // ObPrimaryZoneUtil
#include "share/ob_zone_table_operation.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;

int ObTenantUtils::get_tenant_ids(
    ObMultiVersionSchemaService *schema_service,
    ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service not init", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  }
  return ret;
}




bool ObRootServiceRoleChecker::is_rootserver()
{
  bool bret = false;
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  MTL_SWITCH(tenant_id) {
    int64_t proposal_id = -1;
    ObRole role = FOLLOWER;
    palf::PalfHandleGuard palf_handle_guard;
    logservice::ObLogService *log_service = nullptr;

    if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(log_service->open_palf(SYS_LS, palf_handle_guard))) {
      LOG_WARN("open palf failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
      LOG_WARN("get role failed", KR(ret), K(tenant_id));
    } else {
      bret = (is_strong_leader(role));
      LOG_DEBUG("get __all_core_table role", K(role), K(bret));
    }
  } else {
    if (OB_TENANT_NOT_IN_SERVER == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant storage", KR(ret), "tenant_id", OB_SYS_TENANT_ID);
    }
  }
  return bret;
}

int ObRootUtils::get_rs_default_timeout_ctx(ObTimeoutCtx &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = GCONF.rpc_timeout; // default is 2s
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT_US))) {
    LOG_WARN("fail to set default_timeout_ctx", KR(ret));
  }
  return ret;
}

int ObRootUtils::get_server_resource_info(
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &server_resources_info,
    const ObAddr &server,
    share::ObServerResourceInfo &resource_info)
{
  int ret = OB_SUCCESS;
  bool server_exists = false;
  resource_info.reset();
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !server_exists && i < server_resources_info.count(); i++) {
      const obrpc::ObGetServerResourceInfoResult &server_resource_info_i = server_resources_info.at(i);
      if (OB_UNLIKELY(!server_resource_info_i.is_valid())){
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("server_resource_info_i is not valid", KR(ret), K(server_resource_info_i));
      } else if (server == server_resource_info_i.get_server()) {
        server_exists = true;
        resource_info = server_resource_info_i.get_resource_info();
      }
    }
  }
  if (OB_SUCC(ret) && !server_exists) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("server not exists", KR(ret), K(server));
  }
  return ret;
}

int ObRootUtils::try_notify_switch_ls_leader(
      obrpc::ObSrvRpcProxy *rpc_proxy,
      const share::ObLSInfo &ls_info,
      const obrpc::ObNotifySwitchLeaderArg::SwitchLeaderComment &comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_info.is_valid()) || OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_info), K(rpc_proxy));
  } else {
    ObArray<ObAddr> server_list;
    obrpc::ObNotifySwitchLeaderArg arg;
    const uint64_t tenant_id = ls_info.get_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_info.get_replicas_cnt(); ++i) {
      if (OB_FAIL(server_list.push_back(ls_info.get_replicas().at(i).get_server()))) {
        LOG_WARN("failed to push back server", KR(ret), K(i), K(ls_info));
      }
    }
    if (FAILEDx(arg.init(tenant_id, ls_info.get_ls_id(), ObAddr(), comment))) {
      LOG_WARN("failed to init switch leader arg", KR(ret), K(tenant_id), K(ls_info), K(comment));
    } else if (OB_FAIL(notify_switch_leader(rpc_proxy, tenant_id, arg, server_list))) {
      LOG_WARN("failed to notify switch leader", KR(ret), K(arg), K(tenant_id), K(server_list));
    }
  }
  return ret;

}

int ObRootUtils::notify_switch_leader(
      obrpc::ObSrvRpcProxy *rpc_proxy,
      const uint64_t tenant_id,
      const obrpc::ObNotifySwitchLeaderArg &arg,
      const ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
        || !arg.is_valid() || 0 == addr_list.count()) || OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(arg), K(addr_list), KP(rpc_proxy));
  } else {
    ObTimeoutCtx ctx;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else {
      ObNotifySwitchLeaderProxy proxy(*rpc_proxy, &obrpc::ObSrvRpcProxy::notify_switch_leader);
      for (int64_t i = 0; i < addr_list.count(); ++i) {
        const int64_t timeout =  ctx.get_timeout();
        if (OB_TMP_FAIL(proxy.call(addr_list.at(i), timeout, GCONF.cluster_id, tenant_id, arg))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          LOG_WARN("failed to send rpc", KR(ret), K(i), K(tenant_id), K(arg), K(addr_list));
        }
      }//end for
      if (OB_TMP_FAIL(proxy.wait())) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to wait all result", KR(ret), KR(tmp_ret));
      } else if (OB_SUCC(ret)) {
        // arg/dest/result can be used here.
      }
    }
  }
  return ret;
}

///////////////////////////////

ObClusterRole ObClusterInfoGetter::get_cluster_role_v2()
{
  ObClusterRole cluster_role = PRIMARY_CLUSTER;
  return cluster_role;
}

ObClusterRole ObClusterInfoGetter::get_cluster_role()
{
  ObClusterRole cluster_role = PRIMARY_CLUSTER;
  
  return cluster_role;
}

const char *oceanbase::rootserver::resource_type_to_str(const ObResourceType &t)
{
  const char* str = "UNKNOWN";
  if (RES_CPU == t) { str = "CPU"; }
  else if (RES_MEM == t) { str = "MEMORY"; }
  else if (RES_LOG_DISK == t) { str = "LOG_DISK"; }
  else if (RES_DATA_DISK == t) { str = "DATA_DISK"; }
  else { str = "NONE"; }
  return str;
}
