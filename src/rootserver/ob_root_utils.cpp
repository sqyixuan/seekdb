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

int ObTenantGroupParser::get_next_tenant_group(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObIArray<TenantNameGroup> &tenant_groups)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    // reach to the end
  } else if ('(' != ttg_str[pos]) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else {
    // begin with a left brace
    ++pos;
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('"' == ttg_str[pos]) {
      ret = parse_vector_tenant_group(pos, end, ttg_str, tenant_groups);
    } else if ('(' == ttg_str[pos]) {
      ret = parse_matrix_tenant_group(pos, end, ttg_str, tenant_groups);
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    }
    // end with a right brace
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else if (')' != ttg_str[pos]) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else {
        ++pos;
      }
    }
  }
  return ret;
}

int ObTenantGroupParser::get_next_tenant_name(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else if ('"' != ttg_str[pos]) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else {
    ++pos; // jump over the " symbol
    jump_over_space(pos, end, ttg_str);
    int64_t start = pos;
    while (pos < end && !isspace(ttg_str[pos]) && '"' != ttg_str[pos]) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else {
      tenant_name.assign_ptr(ttg_str.ptr() + start, static_cast<int32_t>(pos - start));
    }
    // end with a " symbol
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else if ('"' != ttg_str[pos]) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
      } else {
        ++pos;
      }
    }
  }
  return ret;
}

int ObTenantGroupParser::jump_to_next_tenant_name(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str)
{
  int ret = OB_SUCCESS;
  jump_over_space(pos, end, ttg_str);
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
  } else if (',' == ttg_str[pos]) {
    ++pos;
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    } else if ('"' != ttg_str[pos]) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
    } else {} // good, come across a " symbol
  } else if (')' == ttg_str[pos]) {
    // good, reach the end of this vector
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(pos));
  }
  return ret;
}

int ObTenantGroupParser::parse_tenant_vector(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObIArray<common::ObString> &tenant_names)
{
  int ret = OB_SUCCESS;
  tenant_names.reset();
  while (OB_SUCC(ret) && pos < end && ')' != ttg_str[pos]) {
    ObString tenant_name;
    if (OB_FAIL(get_next_tenant_name(pos, end, ttg_str, tenant_name))) {
      LOG_WARN("fail to get next tenant name", K(ret));
    } else if (has_exist_in_array(all_tenant_names_, tenant_name)) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if (OB_FAIL(tenant_names.push_back(tenant_name))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(all_tenant_names_.push_back(tenant_name))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(jump_to_next_tenant_name(pos, end, ttg_str))) {
      LOG_WARN("fail to jump to next tenant name", K(ret));
    } else {} // next loop round
  }
  return ret;
}

int ObTenantGroupParser::parse_vector_tenant_group(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObIArray<TenantNameGroup> &tenant_groups)
{
  int ret = OB_SUCCESS;
  TenantNameGroup tenant_group;
  common::ObArray<common::ObString> tenant_names;
  if (OB_FAIL(parse_tenant_vector(pos, end, ttg_str, tenant_names))) {
    LOG_WARN("fail to parse tenant vector", K(ret));
  } else if (OB_FAIL(append(tenant_group.tenants_, tenant_names))) {
    LOG_WARN("fail to append", K(ret));
  } else {
    tenant_group.row_ = 1;
    tenant_group.column_ = tenant_group.tenants_.count();
    if (OB_FAIL(tenant_groups.push_back(tenant_group))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObTenantGroupParser::parse_matrix_tenant_group(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str,
    common::ObIArray<TenantNameGroup> &tenant_groups)
{
  int ret = OB_SUCCESS;
  TenantNameGroup tenant_group;
  common::ObArray<common::ObString> tenant_names;
  bool first = true;
  // parse the second layer of two layer bracket structure
  while (OB_SUCC(ret) && pos < end && ')' != ttg_str[pos]) {
    tenant_names.reset();
    jump_over_space(pos, end, ttg_str);
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' != ttg_str[pos]) { // start with left brace
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else {
      ++pos; // jump over left brace
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(parse_tenant_vector(pos, end, ttg_str, tenant_names))) {
        LOG_WARN("fail to parse tenant vector", K(ret));
      } else if (first) {
        tenant_group.column_ = tenant_names.count();
        tenant_group.row_ = 0;
        first = false;
      } else if (tenant_group.column_ != tenant_names.count()) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append(tenant_group.tenants_, tenant_names))) {
        LOG_WARN("fail to append", K(ret));
      } else {
        ++tenant_group.row_;
      }
    }
    if (OB_SUCC(ret)) {
      jump_over_space(pos, end, ttg_str);
      if (pos >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      } else if (')' != ttg_str[pos]) { // end up with right brace for a row tenant
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
      } else {
        ++pos;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(jump_to_next_tenant_vector(pos, end, ttg_str))) {
        LOG_WARN("fail to jump to next tenant vector", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (tenant_group.row_ <= 0 || tenant_group.column_ <= 0) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if (OB_FAIL(tenant_groups.push_back(tenant_group))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObTenantGroupParser::jump_to_next_tenant_vector(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str)
{
  int ret = OB_SUCCESS;
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  if (pos >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  } else if (')' == ttg_str[pos]) {
    // reach to the end of this tenant group
  } else if (',' == ttg_str[pos]) {
    ++pos; // good, and jump over the ',' symbol
    while (pos < end && isspace(ttg_str[pos])) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' == ttg_str[pos]) {
      // the next tenant vector left brace
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    }
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  }
  return ret;
}

void ObTenantGroupParser::jump_over_space(
     int64_t &pos,
     const int64_t end,
     const common::ObString &ttg_str)
{
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  return;
}

int ObTenantGroupParser::jump_to_next_ttg(
    int64_t &pos,
    const int64_t end,
    const common::ObString &ttg_str)
{
  int ret = OB_SUCCESS;
  while (pos < end && isspace(ttg_str[pos])) {
    ++pos;
  }
  if (pos >= end) {
    // good, reach end
  } else if (',' == ttg_str[pos]) {
    ++pos; // good, and jump over the ',' symbol
    while (pos < end && isspace(ttg_str[pos])) {
      ++pos;
    }
    if (pos >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    } else if ('(' == ttg_str[pos]) {
      // good, the next ttg left brace symbol
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
    }
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid tenantgroup string syntax", K(ret), K(ttg_str), K(pos));
  }
  return ret;
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

//get all observer that is stopeed, start_service_time<=0 and lease expire
int ObRootUtils::get_invalid_server_list(
    const ObIArray<ObServerInfoInTable> &servers_info,
    ObIArray<ObAddr> &invalid_server_list)
{
  int ret = OB_SUCCESS;
  invalid_server_list.reset();
  ObArray<ObAddr> stopped_server_list;
  ObArray<ObZone> stopped_zone_list;
  ObZone empty_zone;
  if (OB_FAIL(get_stopped_zone_list(stopped_zone_list, stopped_server_list))) {
    LOG_WARN("fail to get stopped zone list", KR(ret));
  } else if (OB_FAIL(invalid_server_list.assign(stopped_server_list))) {
    LOG_WARN("fail to assign array", KR(ret), K(stopped_zone_list));
  } else {
    for (int64_t i = 0; i < servers_info.count() && OB_SUCC(ret); i++) {
      const ObServerInfoInTable &server_info = servers_info.at(i);
      if ((!server_info.is_alive() || !server_info.in_service())
          && !has_exist_in_array(invalid_server_list, server_info.get_server())) {
        if (OB_FAIL(invalid_server_list.push_back(server_info.get_server()))) {
          LOG_WARN("fail to push back", KR(ret), K(server_info));
        }
      }
    }
  }
  return ret;
}

int ObRootUtils::find_server_info(
  const ObIArray<share::ObServerInfoInTable> &servers_info,
  const common::ObAddr &server,
  share::ObServerInfoInTable &server_info)
{
  int ret = OB_SUCCESS;
  bool server_exists = false;
  server_info.reset();
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !server_exists && i < servers_info.count(); i++) {
      const ObServerInfoInTable & server_info_i = servers_info.at(i);
      if (OB_UNLIKELY(!server_info_i.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("server_info_i is not valid", KR(ret), K(server_info_i));
      } else if (server == server_info_i.get_server()) {
        server_exists = true;
        if (OB_FAIL(server_info.assign(server_info_i))) {
          LOG_WARN("fail to assign server_info", KR(ret), K(server_info_i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !server_exists) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("server not exists", KR(ret), K(server));
  }
  return ret;
}

int ObRootUtils::get_servers_of_zone(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const common::ObZone &zone,
    ObIArray<common::ObAddr> &servers,
    bool only_active_servers)
{
  int ret = OB_SUCCESS;
  servers.reset();
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid zone", KR(ret), K(zone));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
      const ObServerInfoInTable &server_info = servers_info.at(i);
      if (OB_UNLIKELY(!server_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid server_info", KR(ret), K(server_info));
      } else if (zone != server_info.get_zone() || (only_active_servers && !server_info.is_active())) {
        // do nothing
      } else if (OB_FAIL(servers.push_back(server_info.get_server()))) {
        LOG_WARN("fail to push an element into servers", KR(ret), K(server_info));
      }
    }
  }
  return ret;
}
int ObRootUtils::get_server_count(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObZone &zone,
    int64_t &alive_count,
    int64_t &not_alive_count)
{
  int ret = OB_SUCCESS;
  alive_count = 0;
  not_alive_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); ++i) {
    const ObServerInfoInTable &server_info = servers_info.at(i);
    if (server_info.get_zone() == zone || zone.is_empty()) {
      if (server_info.is_alive()) {
        ++alive_count;
      } else {
        ++not_alive_count;
      }
    }
  }
  return ret;
}
int ObRootUtils::check_server_alive(
      const ObIArray<ObServerInfoInTable> &servers_info,
      const ObAddr &server,
      bool &is_alive)
{
  int ret = OB_SUCCESS;
  is_alive = false;
  ObServerInfoInTable server_info;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else if (OB_FAIL(find_server_info(servers_info, server, server_info))) {
    LOG_WARN("fail to find server_info", KR(ret), K(servers_info), K(server));
  } else {
    is_alive = server_info.is_alive();
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

int ObRootUtils::get_stopped_zone_list(
    ObIArray<ObZone> &stopped_zone_list,
    ObIArray<ObAddr> &stopped_server_list)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  stopped_zone_list.reset();
  stopped_server_list.reset();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("SELECT s.svr_ip, s.svr_port, s.zone "
      "FROM %s AS s JOIN (SELECT zone, info FROM %s WHERE name = 'status') AS z "
      "ON s.zone = z.zone WHERE s.stop_time > 0 OR z.info = 'INACTIVE'",
      OB_ALL_SERVER_TNAME, OB_ALL_ZONE_TNAME))) {
    LOG_WARN("fail to append sql", KR(ret));
  } else if (OB_FAIL(ObZoneTableOperation::get_inactive_zone_list(*GCTX.sql_proxy_, stopped_zone_list))) {
    LOG_WARN("fail to get inactive zone_list", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      int tmp_ret = OB_SUCCESS;
      ObMySQLResult *result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql));
      } else {
        ObZone zone;
        ObAddr server;
        ObString tmp_zone;
        ObString svr_ip;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("result next failed", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            int64_t svr_port = 0;
            server.reset();
            zone.reset();
            svr_ip.reset();
            tmp_zone.reset();
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip);
            EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "zone", tmp_zone);
            if (OB_UNLIKELY(!server.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port)))) {
              ret = OB_INVALID_DATA;
              LOG_WARN("fail to set ip addr", KR(ret), K(svr_ip), K(svr_port));
            } else if (OB_FAIL(zone.assign(tmp_zone))) {
              LOG_WARN("fail to assign zone", KR(ret), K(tmp_zone));
            } else if (OB_FAIL(stopped_server_list.push_back(server))) {
              LOG_WARN("fail to push an element into stopped_server_list", KR(ret), K(server));
            } else if (has_exist_in_array(stopped_zone_list, zone)) {
              // do nothing
            } else if (OB_FAIL(stopped_zone_list.push_back(zone))) {
              LOG_WARN("fail to push an element into stopped_zone_list", KR(ret), K(zone));
            }
          }
        }
      }
    }
  }
  LOG_INFO("get stopped zone list", KR(ret), K(stopped_server_list), K(stopped_zone_list));
  return ret;
}
bool ObRootUtils::have_other_stop_task(const ObZone &zone)
{
  int ret = OB_SUCCESS;
  bool bret = true;
  int64_t cnt = 0;
  ObSqlString sql;
  ObTimeoutCtx ctx;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt("SELECT COUNT(*) AS cnt FROM "
      "(SELECT zone FROM %s WHERE stop_time > 0 AND zone != '%s' UNION "
      "SELECT zone FROM %s WHERE name = 'status' AND info = 'INACTIVE' AND zone != '%s')",
      OB_ALL_SERVER_TNAME, zone.ptr(), OB_ALL_ZONE_TNAME, zone.ptr()))) {
    LOG_WARN("fail to append sql", KR(ret), K(zone));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      int tmp_ret = OB_SUCCESS;
      ObMySQLResult *result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret), K(sql));;
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
      }
      if (OB_SUCC(ret) && (OB_ITER_END != (tmp_ret = result->next()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
      }
    }
  }
  if (OB_SUCC(ret) && 0 == cnt) {
    bret = false;
  }
  LOG_INFO("have other stop task", KR(ret), K(bret), K(zone), K(cnt));
  return bret;
}

int ObRootUtils::get_proposal_id_from_sys_ls(int64_t &proposal_id, ObRole &role)
{
  int ret = OB_SUCCESS;
  storage::ObLSHandle ls_handle;
  logservice::ObLogHandler *handler = nullptr;
  MTL_SWITCH(OB_SYS_TENANT_ID) {
    if (OB_FAIL(MTL(ObLSService*)->get_ls(SYS_LS, ls_handle, ObLSGetMod::RS_MOD))) {
      LOG_WARN("fail to get ls", KR(ret));
    } else if (OB_UNLIKELY(!ls_handle.is_valid())
        || OB_ISNULL(ls_handle.get_ls())
        || OB_ISNULL(handler = ls_handle.get_ls()->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), KP(ls_handle.get_ls()),
          KP(ls_handle.get_ls()->get_log_handler()));
    } else if (OB_FAIL(handler->get_role(role, proposal_id))) {
      LOG_WARN("fail to get role", KR(ret));
          }
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

ERRSIM_POINT_DEF(ERRSIM_USER_LS_SYNC_SCN);
int ObRootUtils::wait_user_ls_sync_scn_locally(
    const share::SCN &sys_ls_target_scn,
    logservice::ObLogService *log_ls_svr,
    storage::ObLS &ls)
{
  int ret = OB_SUCCESS;
  logservice::ObLogHandler *log_handler = ls.get_log_handler();
  transaction::ObKeepAliveLSHandler *keep_alive_handler = ls.get_keep_alive_ls_handler();
  ObLSID ls_id = ls.get_ls_id();
  uint64_t tenant_id = ls.get_tenant_id();
  ObTimeoutCtx ctx;
  if (OB_ISNULL(keep_alive_handler) || OB_ISNULL(log_handler ) || OB_ISNULL(log_ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("keep_alive_ls_handler, log_handler or ls_svr is null", KR(ret), K(ls_id),
        KP(keep_alive_handler), KP(log_handler), KP(log_ls_svr));
  } else if (OB_UNLIKELY(!sys_ls_target_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sys_ls_target_scn", KR(ret), K(sys_ls_target_scn));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout", KR(ret));
  } else {
    bool need_retry = true;
    share::SCN curr_end_scn;
    curr_end_scn.set_min();
    common::ObRole role;
    int64_t leader_epoch = 0;
    (void) keep_alive_handler->set_sys_ls_end_scn(sys_ls_target_scn);
    do {
      if (OB_UNLIKELY(ctx.is_timeouted())) {
        ret = OB_TIMEOUT;
        need_retry = false;
        LOG_WARN("ctx timeout", KR(ret), K(ctx));
      } else if (OB_FAIL(log_ls_svr->get_palf_role(ls_id, role, leader_epoch))) {
        LOG_WARN("fail to get palf role", KR(ret), K(ls_id));
      } else if (OB_UNLIKELY(!is_strong_leader(role))) {
        ret = OB_NOT_MASTER;
        LOG_WARN("ls on this server is not master", KR(ret), K(ls_id), K(role));
      } else {
        if (OB_FAIL(log_handler->get_end_scn(curr_end_scn))) {
          LOG_WARN("fail to get ls end scn", KR(ret), K(ls_id));
        } else {
          curr_end_scn = ERRSIM_USER_LS_SYNC_SCN ? SCN::scn_dec(sys_ls_target_scn) : curr_end_scn;
          LOG_TRACE("wait curr_end_scn >= sys_ls_target_scn", K(curr_end_scn), K(sys_ls_target_scn),
              "is_errsim_opened", ERRSIM_USER_LS_SYNC_SCN ? true : false);
        }
        if (OB_SUCC(ret) && curr_end_scn >= sys_ls_target_scn) {
          LOG_INFO("current user ls end scn >= sys ls target scn now", K(curr_end_scn),
              K(sys_ls_target_scn), "is_errsim_opened", ERRSIM_USER_LS_SYNC_SCN ? true : false,
              K(tenant_id), K(ls_id));
          need_retry = false;
        }
      }
      if (need_retry && OB_SUCC(ret)) {
        ob_usleep(50 * 1000); // wait 50ms
      }
    } while (need_retry && OB_SUCC(ret));
    if (OB_UNLIKELY(need_retry && OB_SUCC(ret))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the wait loop should not be terminated", KR(ret), K(curr_end_scn), K(sys_ls_target_scn));
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
