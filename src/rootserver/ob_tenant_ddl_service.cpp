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
#include "rootserver/ob_tenant_ddl_service.h"

#include "rootserver/restore/ob_restore_util.h"
#include "share/ob_primary_zone_util.h"
#include "share/ls/ob_ls_creator.h"
#include "rootserver/ob_tenant_thread_helper.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_root_service.h"
#include "share/ls/ob_ls_life_manager.h"
#include "share/location_cache/ob_location_service.h"
#include "rootserver/ob_table_creator.h"
#include "share/ob_global_stat_proxy.h"
#include "rootserver/standby/ob_standby_service.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/backup/ob_backup_config.h"
#include "share/ob_schema_status_proxy.h"
#include "share/backup/ob_log_restore_config.h"//ObLogRestoreSourceServiceConfigParser
#include "storage/tx/ob_ts_mgr.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "rootserver/ob_tenant_parallel_create_executor.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/ob_zone_merge_info.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_table_operator.h"
#include "share/ob_license_utils.h"
#include "rootserver/ob_load_inner_table_schema_executor.h"

// The input of value must be a string
#define SET_TENANT_VARIABLE(sysvar_id, value) \
        if (OB_SUCC(ret)) {\
          int64_t store_idx = OB_INVALID_INDEX; \
          if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(sysvar_id, store_idx))) { \
            LOG_WARN("failed to calc sys var store idx", KR(ret), K(sysvar_id)); \
          } else if (OB_UNLIKELY(store_idx < 0 \
                     || store_idx >= ObSysVarFactory::ALL_SYS_VARS_COUNT)) { \
            ret = OB_ERR_UNEXPECTED; \
            LOG_WARN("got store_idx is invalid", K(ret), K(store_idx)); \
          } else if (OB_FAIL(sys_params[store_idx].init( \
                     sys_variable_schema.get_tenant_id(),\
                     ObSysVariables::get_name(store_idx),\
                     ObSysVariables::get_type(store_idx),\
                     value,\
                     ObSysVariables::get_min(store_idx),\
                     ObSysVariables::get_max(store_idx),\
                     ObSysVariables::get_info(store_idx),\
                     ObSysVariables::get_flags(store_idx)))) {\
            LOG_WARN("failed to set tenant variable", \
                     KR(ret), K(value), K(sysvar_id), K(store_idx));\
          }\
        }
// Convert macro integer to string for setting into system variable
#define VAR_INT_TO_STRING(buf, value) \
        if (OB_SUCC(ret)) {\
          if (OB_FAIL(databuff_printf(buf, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%d", static_cast<int>(value)))) {\
            LOG_WARN("failed to print value in buf", K(value), K(ret));\
          }\
        }
#define VAR_UINT_TO_STRING(buf, value) \
        if (OB_SUCC(ret)) {\
          if (OB_FAIL(databuff_printf(buf, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%lu", static_cast<uint64_t>(value)))) {\
            LOG_WARN("failed to print value in buf", K(value), K(ret));\
          }\
        }

namespace oceanbase
{
using namespace obrpc;
using namespace share;
namespace rootserver
{

int ObTenantDDLService::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantDDLService is not inited", KR(ret), K(inited_));
  } else if (OB_ISNULL(ddl_service_) || OB_ISNULL(rpc_proxy_) || OB_ISNULL(common_rpc_)
      || OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_) || OB_ISNULL(lst_operator_)
      || OB_ISNULL(ddl_trans_controller_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), KP(ddl_service_), KP(rpc_proxy_), KP(sql_proxy_),
        KP(schema_service_), KP(lst_operator_), KP(ddl_trans_controller_));
  }
  return ret;
}

#define USE_DDL_FUNCTION(function_name, ...) \
  int ret = OB_SUCCESS; \
  if (OB_ISNULL(ddl_service_)) { \
    ret = OB_NOT_INIT; \
    LOG_WARN("ddl_service_ is null", KR(ret), KP(ddl_service_)); \
  } else if (OB_FAIL(ddl_service_->function_name(__VA_ARGS__))) { \
    LOG_WARN("failed to call " #function_name , KR(ret)); \
  } \
  return ret;

int ObTenantDDLService::get_tenant_schema_guard_with_version_in_inner_table(
    const uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  USE_DDL_FUNCTION(get_tenant_schema_guard_with_version_in_inner_table, tenant_id, schema_guard);
}

int ObTenantDDLService::publish_schema(const uint64_t tenant_id)
{
  USE_DDL_FUNCTION(publish_schema, tenant_id);
}

int ObTenantDDLService::publish_schema(const uint64_t tenant_id, const common::ObAddrIArray &addrs)
{
  USE_DDL_FUNCTION(publish_schema, tenant_id, addrs);
}

#undef USE_DDL_FUNCTION

int ObTenantDDLService::schedule_create_tenant(const obrpc::ObCreateTenantArg &arg, obrpc::UInt64 &tenant_id)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObParallelCreateTenantExecutor, executor) {
    if (OB_FAIL(executor.init(arg, GCTX.srv_rpc_proxy_, GCTX.rs_rpc_proxy_, GCTX.sql_proxy_,
            GCTX.schema_service_, GCTX.lst_operator_, GCTX.location_service_))) {
      LOG_WARN("failed to init executor", KR(ret), K(arg));
    } else if (OB_FAIL(executor.execute(tenant_id))) {
      LOG_WARN("failed to execute", KR(ret), K(executor));
    }
  }
  return ret;
}

int ObTenantDDLService::init_tenant_configs_(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t tenant_idx = !is_user_tenant(tenant_id) ? 0 : 1;
  if (OB_UNLIKELY(
      init_configs.count() < tenant_idx + 1
      || tenant_id != init_configs.at(tenant_idx).get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid init_configs", KR(ret), K(tenant_idx), K(tenant_id), K(init_configs));
  } else if (OB_FAIL(init_tenant_config_(tenant_id, init_configs.at(tenant_idx), trans))) {
    LOG_WARN("fail to init tenant config", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_tenant_config_from_seed_(tenant_id, trans))) {
    LOG_WARN("fail to init tenant config from seed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantDDLService::init_tenant_config_(
    const uint64_t tenant_id,
    const common::ObConfigPairs &tenant_config,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard hard_code_config(TENANT_CONF(OB_SYS_TENANT_ID));
  int64_t config_cnt = tenant_config.get_configs().count();
  if (OB_UNLIKELY(tenant_id != tenant_config.get_tenant_id() || config_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant config", KR(ret), K(tenant_id), K(tenant_config));
  } else if (!hard_code_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get hard code config", KR(ret), K(tenant_id));
  } else {
    ObDMLSqlSplicer dml;
    ObConfigItem *item = NULL;
    char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "ANY";
    int64_t svr_port = 0;
    int64_t config_version = ObServerConfig::INITIAL_TENANT_CONF_VERSION + 1;
    FOREACH_X(config, tenant_config.get_configs(), OB_SUCC(ret)) {
      const ObConfigStringKey key(config->key_.ptr());
      if (OB_ISNULL(hard_code_config->get_container().get(key))
          || OB_ISNULL(item = *(hard_code_config->get_container().get(key)))) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("config not exist", KR(ret), KPC(config));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
                 || OB_FAIL(dml.add_pk_column("zone", ""))
                 || OB_FAIL(dml.add_pk_column("svr_type", print_server_role(OB_SERVER)))
                 || OB_FAIL(dml.add_pk_column(K(svr_ip)))
                 || OB_FAIL(dml.add_pk_column(K(svr_port)))
                 || OB_FAIL(dml.add_pk_column("name", config->key_.ptr()))
                 || OB_FAIL(dml.add_column("data_type", item->data_type()))
                 || OB_FAIL(dml.add_column("value", config->value_.ptr()))
                 || OB_FAIL(dml.add_column("info", ""))
                 || OB_FAIL(dml.add_column("config_version", config_version))
                 || OB_FAIL(dml.add_column("section", item->section()))
                 || OB_FAIL(dml.add_column("scope", item->scope()))
                 || OB_FAIL(dml.add_column("source", item->source()))
                 || OB_FAIL(dml.add_column("edit_level", item->edit_level()))) {
        LOG_WARN("fail to add column", KR(ret), K(tenant_id), KPC(config));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(tenant_id), KPC(config));
      }
    } // end foreach
    ObSqlString sql;
    int64_t affected_rows = 0;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    if (FAILEDx(dml.splice_batch_insert_sql(OB_TENANT_PARAMETER_TNAME, sql))) {
      LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
    } else if (config_cnt != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows not match", KR(ret), K(tenant_id), K(config_cnt), K(affected_rows));
    }
  }
  return ret;
}

int ObTenantDDLService::init_tenant_config_from_seed_(
    const uint64_t tenant_id,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObSqlString sql;
  const static char *from_seed = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
                "data_type, value, info, section, scope, source, edit_level "
                "from __all_seed_parameter";
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    int64_t expected_rows = 0;
    int64_t config_version = ObServerConfig::INITIAL_TENANT_CONF_VERSION + 1;
    bool is_first = true;
    if (OB_FAIL(sql_client_retry_weak.read(result, OB_SYS_TENANT_ID, from_seed))) {
      LOG_WARN("read config from __all_seed_parameter failed", K(from_seed), K(ret));
    } else {
      sql.reset();
      if (OB_FAIL(sql.assign_fmt("INSERT IGNORE INTO %s "
          "(TENANT_ID, ZONE, SVR_TYPE, SVR_IP, SVR_PORT, NAME, DATA_TYPE, VALUE, INFO, "
          "SECTION, SCOPE, SOURCE, EDIT_LEVEL, CONFIG_VERSION) VALUES",
          OB_TENANT_PARAMETER_TNAME))) {
        LOG_WARN("sql assign failed", K(ret));
      }

      while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
        common::sqlclient::ObMySQLResult *rs = result.get_result();
        if (OB_ISNULL(rs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("system config result is null", K(ret));
        } else {
          ObString var_zone, var_svr_type, var_svr_ip, var_name, var_data_type;
          ObString var_value, var_info, var_section, var_scope, var_source, var_edit_level;
          int64_t var_svr_port = 0;
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "zone", var_zone);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "svr_type", var_svr_type);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "svr_ip", var_svr_ip);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "name", var_name);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "data_type", var_data_type);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "value", var_value);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "info", var_info);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "section", var_section);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "scope", var_scope);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "source", var_source);
          EXTRACT_VARCHAR_FIELD_MYSQL(*rs, "edit_level", var_edit_level);
          EXTRACT_INT_FIELD_MYSQL(*rs, "svr_port", var_svr_port, int64_t);
          if (FAILEDx(sql.append_fmt("%s('%lu', '%.*s', '%.*s', '%.*s', %ld, '%.*s', '%.*s', '%.*s',"
              "'%.*s', '%.*s', '%.*s', '%.*s', '%.*s', %ld)",
              is_first ? " " : ", ",
              tenant_id,
              var_zone.length(), var_zone.ptr(),
              var_svr_type.length(), var_svr_type.ptr(),
              var_svr_ip.length(), var_svr_ip.ptr(), var_svr_port,
              var_name.length(), var_name.ptr(),
              var_data_type.length(), var_data_type.ptr(),
              var_value.length(), var_value.ptr(),
              var_info.length(), var_info.ptr(),
              var_section.length(), var_section.ptr(),
              var_scope.length(), var_scope.ptr(),
              var_source.length(), var_source.ptr(),
              var_edit_level.length(), var_edit_level.ptr(), config_version))) {
            LOG_WARN("sql append failed", K(ret));
          }
        }
        expected_rows++;
        is_first = false;
      } // while

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
        if (expected_rows > 0) {
          int64_t affected_rows = 0;
          if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
          } else if (OB_UNLIKELY(affected_rows < 0
                     || expected_rows < affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected affected_rows", KR(ret),  K(expected_rows), K(affected_rows));
          }
        }
      } else {
        LOG_WARN("failed to get result from result set", K(ret));
      }
    } // else
    LOG_INFO("init tenant config", K(ret), K(tenant_id),
               "cost", ObTimeUtility::current_time() - start);
  }
  return ret;

}

int ObTenantDDLService::set_sys_ls_(const uint64_t tenant_id, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
  } else {
    share::ObLSAttr new_ls;
    share::ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
    uint64_t ls_group_id = 0;
    SCN create_scn = SCN::base_scn();
    share::ObLSAttrOperator ls_operator(tenant_id, sql_proxy_);
    if (OB_FAIL(new_ls.init(SYS_LS, ls_group_id, flag,
            share::OB_LS_NORMAL, share::OB_LS_OP_CREATE_END, create_scn))) {
      LOG_WARN("failed to init new operation", KR(ret), K(flag), K(create_scn));
    } else if (OB_FAIL(ls_operator.insert_ls(new_ls, share::NORMAL_SWITCHOVER_STATUS, &trans))) {
      LOG_WARN("failed to insert new ls", KR(ret), K(new_ls), K(ls_group_id));
    }
  }
  return ret;
}

int ObTenantDDLService::init_user_tenant_env_(const uint64_t tenant_id, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(set_sys_ls_(tenant_id, trans))) {
    LOG_WARN("failed to set sys ls", KR(ret), K(tenant_id));
  }
  return ret;
}

// 1. run ls_life_agent
//   a. __all_ls_status
//   b. __all_ls_election_reference_info
//   c. __all_ls_recovery_stat
int ObTenantDDLService::fill_user_sys_ls_info_(
    const ObTenantSchema &meta_tenant_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = meta_tenant_schema.get_tenant_id();
  const uint64_t user_tenant_id = gen_user_tenant_id(meta_tenant_id);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (!is_meta_tenant(meta_tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id is not meta", KR(ret), K(meta_tenant_schema));
  } else {
    share::ObLSLifeAgentManager ls_life_agent(*sql_proxy_);
    share::ObLSStatusOperator ls_operator;
    const SCN create_scn = SCN::base_scn();
    share::ObLSStatusInfo status_info;
    ObLSFlag flag(ObLSFlag::NORMAL_FLAG); // TODO: sys ls should be duplicate
    ObZone primary_zone;
    ObSqlString zone_priority;
    if (OB_FAIL(get_tenant_zone_priority(meta_tenant_schema, primary_zone,
            zone_priority))) {
      LOG_WARN("failed to get tenant zone priority", KR(ret), K(primary_zone), K(zone_priority));
    } else if (OB_FAIL(status_info.init(user_tenant_id, SYS_LS, 0/*ls_group_id*/, share::OB_LS_CREATING,
            0/*unit_group_id*/, primary_zone, flag))) {
      LOG_WARN("failed to init ls info", KR(ret), K(primary_zone),
          K(user_tenant_id), K(flag));
    } else if (OB_FAIL(ls_life_agent.create_new_ls_in_trans(status_info, create_scn, zone_priority.string(),
            share::NORMAL_SWITCHOVER_STATUS, trans))) {
      LOG_WARN("failed to create new ls", KR(ret), K(status_info), K(create_scn), K(zone_priority));
    } else if (OB_FAIL(ls_operator.update_ls_status_in_trans(
                  user_tenant_id, SYS_LS, share::OB_LS_CREATING, share::OB_LS_NORMAL,
                  share::NORMAL_SWITCHOVER_STATUS, trans))) {
      LOG_WARN("failed to update ls status", KR(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_zone_priority(const ObTenantSchema &tenant_schema,
    ObZone &primary_zone,
    ObSqlString &zone_priority)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> primary_zone_list;
  if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(tenant_schema, primary_zone_list))) {
    LOG_WARN("failed to get tenant primary zone array", KR(ret));
  } else if (OB_UNLIKELY(0 == primary_zone_list.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary zone is empty", KR(ret), K(tenant_schema));
  } else if (FALSE_IT(primary_zone = primary_zone_list.at(0))) {
  } else if (OB_FAIL(ObTenantThreadHelper::get_zone_priority(
          primary_zone, tenant_schema, zone_priority))) {
    LOG_WARN("failed to get zone priority", KR(ret), K(primary_zone_list), K(tenant_schema));
  }
  return ret;
}

int ObTenantDDLService::init_tenant_sys_stats_(const uint64_t tenant_id,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();
  ObSysStat sys_stat;
  if (OB_FAIL(sys_stat.set_initial_values(tenant_id))) {
    LOG_WARN("set initial values failed", K(ret));
  } else if (sys_stat.item_list_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not system stat item", KR(ret), K(tenant_id));
  } else if (OB_FAIL(replace_sys_stat(tenant_id, sys_stat, trans))) {
    LOG_WARN("replace system stat failed", K(ret));
  }
  LOG_INFO("init sys stat", K(ret), K(tenant_id),
           "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObTenantDDLService::replace_sys_stat(const uint64_t tenant_id,
                                    ObSysStat &sys_stat,
                                    ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (sys_stat.item_list_.is_empty()) {
    // skip
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
      "(TENANT_ID, ZONE, NAME, DATA_TYPE, VALUE, INFO, gmt_modified) VALUES ",
      OB_ALL_SYS_STAT_TNAME))) {
    LOG_WARN("sql append failed", K(ret));
  } else {
    DLIST_FOREACH_X(it, sys_stat.item_list_, OB_SUCC(ret)) {
      if (OB_ISNULL(it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("it is null", K(ret));
      } else {
        char buf[2L<<10] = "";
        int64_t pos = 0;
        if (OB_FAIL(it->value_.print_sql_literal(
                      buf, sizeof(buf), pos))) {
          LOG_WARN("print obj failed", K(ret), "obj", it->value_);
        } else {
          ObString value(pos, buf);
          uint64_t schema_id = OB_INVALID_ID;
          if (OB_FAIL(ObMaxIdFetcher::str_to_uint(value, schema_id))) {
            LOG_WARN("fail to convert str to uint", K(ret), K(value));
          } else if (FALSE_IT(schema_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, schema_id))) {
          } else if (OB_FAIL(sql.append_fmt("%s(%lu, '', '%s', %d, '%ld', '%s', now())",
              (it == sys_stat.item_list_.get_first()) ? "" : ", ",
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              it->name_, it->value_.get_type(),
              static_cast<int64_t>(schema_id),
              it->info_))) {
            LOG_WARN("sql append failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("create system stat sql", K(sql));
      int64_t affected_rows = 0;
      if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (sys_stat.item_list_.get_size() != affected_rows
          && sys_stat.item_list_.get_size() != affected_rows / 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(affected_rows),
            "expected", sys_stat.item_list_.get_size());
      }
    }
  }
  return ret;
}

int ObTenantDDLService::init_meta_tenant_env_(
    const ObTenantSchema &tenant_schema,
    const obrpc::ObCreateTenantArg &create_tenant_arg,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const ObTenantRole &tenant_role = create_tenant_arg.get_tenant_role();
  const SCN recovery_until_scn = create_tenant_arg.is_restore_tenant()
    ? create_tenant_arg.recovery_until_scn_ : SCN::max_scn();
  if (!is_meta_tenant(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not user tenant", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!recovery_until_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid recovery_until_scn", KR(ret), K(recovery_until_scn));
  } else {
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
    ObAllTenantInfo tenant_info;
    if (OB_FAIL(tenant_info.init(user_tenant_id, tenant_role, NORMAL_SWITCHOVER_STATUS, 0,
            SCN::base_scn(), SCN::base_scn(), SCN::base_scn(), recovery_until_scn))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id), K(tenant_role));
    } else if (OB_FAIL(ObAllTenantInfoProxy::init_tenant_info(tenant_info, &trans))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_info));
    } else if (OB_FAIL(fill_user_sys_ls_info_(tenant_schema, trans))) {
      LOG_WARN("failed to fill user sys ls info", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(init_tenant_configs_(tenant_id, init_configs, trans))) {
      LOG_WARN("failed to init tenant config", KR(ret), K(tenant_id), K(init_configs));
    } else if (OB_FAIL(init_tenant_configs_(user_tenant_id, init_configs, trans))) {
      LOG_WARN("failed to init tenant config for user tenant", KR(ret), K(user_tenant_id), K(init_configs));
    }
  }
  return ret;
}

int ObTenantDDLService::create_sys_tenant(
    const obrpc::ObCreateTenantArg &arg,
    share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  ObDDLSQLTransaction trans(schema_service_, true, false, false, false);
  ObSchemaService *schema_service = NULL;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    schema_service = schema_service_->get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("schema_service must not null", K(ret));
    } else {
      ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
      ObRefreshSchemaStatus tenant_status(OB_SYS_TENANT_ID,
          OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
      ObSysVariableSchema sys_variable;
      tenant_schema.set_tenant_id(OB_SYS_TENANT_ID);
      const ObSchemaOperationType operation_type = OB_DDL_MAX_OP;
      // When the system tenant is created, the log_operation of the system variable is not recorded separately
      // The update of __all_core_table must be a single-partition transaction.
      // Failure to create a tenant will result in garbage data, but it will not affect
      int64_t refreshed_schema_version = 0; // won't lock
      common::ObConfigPairs config;
      common::ObSEArray<common::ObConfigPairs, 1> init_configs;
      if (OB_ISNULL(schema_status_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_status_proxy is null", K(ret));
      } else if (OB_FAIL(generate_tenant_init_configs(arg, OB_SYS_TENANT_ID, init_configs))) {
        LOG_WARN("failed to gen tenant init config", KR(ret));
      } else if (OB_FAIL(schema_status_proxy->set_tenant_schema_status(tenant_status))) {
        LOG_WARN("init tenant create partition status failed", K(ret), K(tenant_status));
      } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret));
      } else if (OB_FAIL(set_tenant_compatibility_(arg, tenant_schema))) {
        LOG_WARN("failed to set tenant compatibility", KR(ret), K(arg));
      } else if (OB_FAIL(ddl_operator.create_tenant(tenant_schema, OB_DDL_ADD_TENANT, trans))) {
        LOG_WARN("create tenant failed", K(tenant_schema), K(ret));
      } else if (OB_FAIL(init_system_variables(arg, tenant_schema, sys_variable))) {
        LOG_WARN("fail to init tenant sys params", K(ret), K(tenant_schema));
      } else if (OB_FAIL(ddl_operator.replace_sys_variable(
              sys_variable, tenant_schema.get_schema_version(), trans, operation_type))) {
        LOG_WARN("fail to replace sys variable", K(ret), K(sys_variable));
      } else if (OB_FAIL(ddl_operator.init_tenant_schemas(tenant_schema, sys_variable, trans))) {
        LOG_WARN("init tenant env failed", K(tenant_schema), K(ret));
      } else if (OB_FAIL(init_tenant_sys_stats_(OB_SYS_TENANT_ID, trans))) {
        LOG_WARN("insert default sys stats failed", K(OB_SYS_TENANT_ID), K(ret));
      } else if (OB_FAIL(init_tenant_configs_(OB_SYS_TENANT_ID, init_configs, trans))) {
        LOG_WARN("failed to init tenant config", KR(ret), K(init_configs));
      } else if (OB_FAIL(insert_tenant_merge_info_(OB_DDL_ADD_TENANT, tenant_schema, trans))) {
        LOG_WARN("fail to insert tenant merge info", KR(ret));
      } else if (OB_FAIL(ObServiceEpochProxy::init_service_epoch(
          trans,
          OB_SYS_TENANT_ID,
          0, /*freeze_service_epoch*/
          0, /*arbitration_service_epoch*/
          0, /*server_zone_op_service_epoch*/
          0, /*heartbeat_service_epoch*/
          0 /* service_name_epoch */))) {
        LOG_WARN("fail to init service epoch", KR(ret));
      }
      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        LOG_INFO("end create tenant", "is_commit", OB_SUCCESS == ret, K(ret));
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        }
      }
    }
  }
  return ret;
}

int ObTenantDDLService::set_tenant_compatibility_(
    const obrpc::ObCreateTenantArg &arg,
    ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  const int64_t set_sys_var_count = arg.sys_var_list_.count();
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  // the default compatibility_mode is MYSQL
  tenant_schema.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(arg));
  } else if (!is_user_tenant(tenant_id)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < set_sys_var_count; ++i) {
      ObSysVarIdValue sys_var;
      if (OB_FAIL(arg.sys_var_list_.at(i, sys_var))) {
        LOG_WARN("failed to get sys var", K(i), K(ret));
      } else {
        if (SYS_VAR_OB_COMPATIBILITY_MODE == sys_var.sys_id_) {
          if (0 == sys_var.value_.compare("1")) {
            tenant_schema.set_compatibility_mode(ObCompatibilityMode::ORACLE_MODE);
          } else {
            tenant_schema.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantDDLService::generate_tenant_schema(
    const ObCreateTenantArg &arg,
    const share::ObTenantRole &tenant_role,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObTenantSchema &user_tenant_schema,
    ObTenantSchema &meta_tenant_schema,
    common::ObIArray<common::ObConfigPairs> &init_configs)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = arg.tenant_schema_.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_)
      || OB_ISNULL(schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(schema_service));
  } else if (OB_FAIL(user_tenant_schema.assign(arg.tenant_schema_))) {
    LOG_WARN("fail to assign user tenant schema", KR(ret), K(arg));
  } else if (OB_FAIL(meta_tenant_schema.assign(user_tenant_schema))) {
    LOG_WARN("fail to assign meta tenant schema", KR(ret), K(arg));
  } else if (OB_FAIL(schema_service_->get_schema_service()->fetch_new_tenant_id(user_tenant_id))) {
    LOG_WARN("fetch_new_tenant_id failed", KR(ret));
  } else if (OB_INVALID_ID == user_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id is invalid", KR(ret), K(user_tenant_id));
  } else {
    // user tenant
    if (OB_SUCC(ret)) {
      user_tenant_schema.set_tenant_id(user_tenant_id);
      if (!tenant_role.is_primary()) {
        //standby cluster and restore tenant no need init user tenant system variables
        if (tenant_role.is_restore()) {
          user_tenant_schema.set_status(TENANT_STATUS_RESTORE);
        } else if (arg.is_creating_standby_) {
          user_tenant_schema.set_status(TENANT_STATUS_CREATING_STANDBY);
        }
      } else if (OB_FAIL(set_tenant_compatibility_(arg, user_tenant_schema))) {
        LOG_WARN("fail to set tenant compatibility", KR(ret), K(user_tenant_schema), K(arg));
      }
    }
    // meta tenant
    if (OB_SUCC(ret)) {
      const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
      ObSqlString table_name;
      if (OB_FAIL(table_name.assign_fmt("META$%ld", user_tenant_id))) {
        LOG_WARN("fail to assign tenant name",KR(ret), K(user_tenant_id));
      } else {
        meta_tenant_schema.set_tenant_id(meta_tenant_id);
        meta_tenant_schema.set_tenant_name(table_name.string());
        meta_tenant_schema.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
      }
    }
    // init tenant configs
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_tenant_init_configs(arg, user_tenant_id, init_configs))) {
        LOG_WARN("failed to generate tenant init configs", KR(ret), K(arg), K(user_tenant_id));
      }
    }
  }
  return ret;
}

int ObTenantDDLService::generate_tenant_init_configs(const obrpc::ObCreateTenantArg &arg,
      const uint64_t user_tenant_id,
      common::ObIArray<common::ObConfigPairs> &init_configs)
{
  int ret = OB_SUCCESS;
  init_configs.reset();
  common::ObConfigPairs config;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  if (OB_FAIL(gen_tenant_init_config(meta_tenant_id, DATA_CURRENT_VERSION, config))) {
    LOG_WARN("fail to gen tenant init config", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(init_configs.push_back(config))) {
    LOG_WARN("fail to push back config", KR(ret), K(meta_tenant_id), K(config));
    // } else if (!is_sys_tenant(user_tenant_id) && !arg.is_creating_standby_) {
    //
    // FIXME(msy164651) : Data Version scheme is not suitable for Create
    // Standby Tenant. The DDL will fail because GET_MIN_DATA_VERSION will
    // return OB_ENTRY_NOT_EXIST;
    //
    // msy164651 wil fix it.
  } else if (!is_sys_tenant(user_tenant_id)) {
    /**
     * When the primary tenant has done upgrade and create a standby tenant for it,
     * the standby tenant must also perform the upgrade process. Don't set compatible_version for
     * standby tenant so that it can be upgraded from 0 to ensure that the compatible_version matches
     * the internal table. and it also prevent loss of the upgrade action.
     */
    uint64_t compatible_version = arg.is_restore_tenant()
      ? arg.compatible_version_
      : DATA_CURRENT_VERSION;
    if (OB_FAIL(gen_tenant_init_config(user_tenant_id, compatible_version, config))) {
      LOG_WARN("fail to gen tenant init config", KR(ret), K(user_tenant_id), K(compatible_version));
    } else if (OB_FAIL(init_configs.push_back(config))) {
      LOG_WARN("fail to push back config", KR(ret), K(user_tenant_id), K(config));
    }
  }

return ret;
}

int ObTenantDDLService::gen_tenant_init_config(
    const uint64_t tenant_id,
    const uint64_t compatible_version,
    common::ObConfigPairs &tenant_config)
{
  int ret = OB_SUCCESS;
  ObString config_name("compatible");
  ObString config_value;
  char version[common::OB_CLUSTER_VERSION_LENGTH] = {'\0'};
  int64_t len = ObClusterVersion::print_version_str(
                version, common::OB_CLUSTER_VERSION_LENGTH, compatible_version);
  tenant_config.reset();
  (void) tenant_config.init(tenant_id);
  if (len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", KR(ret), K(tenant_id), K(compatible_version));
  } else if (FALSE_IT(config_value.assign_ptr(version, len))) {
  } else if (OB_FAIL(tenant_config.add_config(config_name, config_value))) {
    LOG_WARN("fail to add config", KR(ret), K(config_name), K(config_value));
  }
  return ret;
}

int ObTenantDDLService::init_schema_status(
    const uint64_t tenant_id,
    const share::ObTenantRole &tenant_role)
{
  int ret = OB_SUCCESS;
  ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (is_meta_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_status_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init schema status", KR(ret), K(tenant_id));
  } else {
    // user tenant
    if (OB_SUCC(ret) && is_user_tenant(tenant_id)) {
    ObRefreshSchemaStatus partition_status(tenant_id,
        OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
      if (!tenant_role.is_primary()) {
        // reset tenant's schema status in standby cluster or in physical restore
        partition_status.snapshot_timestamp_ = 0;
        partition_status.readable_schema_version_ = 0;
      }
      if (FAILEDx(schema_status_proxy->set_tenant_schema_status(partition_status))) {
        LOG_WARN("fail to set refreshed schema status", KR(ret), K(partition_status));
      }
    }
    // sys or meta tenant
    if (OB_SUCC(ret)) {
      // sys tenant's meta tenant is itself
      const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
      ObRefreshSchemaStatus meta_partition_status(meta_tenant_id, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
      if (OB_FAIL(schema_status_proxy->set_tenant_schema_status(meta_partition_status))) {
        LOG_WARN("fail to set refreshed schema status", KR(ret), K(meta_partition_status));
      }
    }
  }
  return ret;
}

int ObTenantDDLService::create_tenant(const ObCreateTenantArg &arg,
    ObCreateTenantSchemaResult &result)
{
  LOG_INFO("begin to create tenant schema", K(arg));
  int ret = OB_SUCCESS;
  bool need_create = false;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (OB_FAIL(create_tenant_check_(arg, need_create, schema_guard))) {
    LOG_WARN("failed to check create tenant", KR(ret), K(arg));
  } else if (!need_create) {
    LOG_INFO("no need to create tenant", KR(ret), K(need_create), K(arg));
    if (OB_FAIL(result.init_with_tenant_exist())) {
      LOG_WARN("failed to init result when tenant exist", KR(ret));
    }
  } else {
    HEAP_VARS_2((ObTenantSchema, user_tenant_schema),
                (ObTenantSchema, meta_tenant_schema)) {
      uint64_t user_tenant_id = OB_INVALID_TENANT_ID;
      int64_t paxos_replica_number = 0;
      ObSEArray<ObConfigPairs, 2> init_configs;
      ObTenantRole tenant_role = arg.get_tenant_role();
      if (OB_FAIL(generate_tenant_schema(arg, tenant_role, schema_guard,
              user_tenant_schema, meta_tenant_schema, init_configs))) {
        LOG_WARN("fail to generate tenant schema", KR(ret), K(arg), K(tenant_role));
      } else if (FALSE_IT(user_tenant_id = user_tenant_schema.get_tenant_id())) {
      } else if (OB_FAIL(init_schema_status(
              user_tenant_schema.get_tenant_id(), tenant_role))) {
        LOG_WARN("fail to init schema status", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(create_tenant_schema(
                 arg, schema_guard, user_tenant_schema,
                 meta_tenant_schema, init_configs))) {
        LOG_WARN("fail to create tenant schema", KR(ret), K(arg));
      } else if (OB_FAIL(result.init(user_tenant_id))) {
        LOG_WARN("failed to init result", KR(ret), K(user_tenant_id));
      }
      DEBUG_SYNC(AFTER_CREATE_TENANT_SCHEMA);
    } // end HEAP_VARS_4
  }
  return ret;
}

int ObTenantDDLService::generate_drop_tenant_arg(
    const uint64_t tenant_id,
    const ObString &tenant_name,
    ObSqlString &ddl_stmt,
    ObDropTenantArg &arg)
{
  int ret = OB_SUCCESS;
  arg.tenant_name_ = tenant_name;
  arg.tenant_id_ = tenant_id;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.if_exist_ = true;
  arg.delay_to_drop_ = false;
  if (OB_FAIL(ddl_stmt.append_fmt("DROP TENANT IF EXISTS %s FORCE", tenant_name.ptr()))) {
    LOG_WARN("fail to generate sql", KR(ret), K(tenant_id));
  }
  arg.ddl_stmt_str_ = ddl_stmt.string();
  return ret;
}

int ObTenantDDLService::try_force_drop_tenant(const ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    obrpc::ObDropTenantArg arg;
    const uint64_t tenant_id = tenant_schema.get_tenant_id();
    const ObString tenant_name = tenant_schema.get_tenant_name();
    ObSqlString ddl_stmt;
    if (OB_FAIL(generate_drop_tenant_arg(tenant_id, tenant_name, ddl_stmt, arg))) {
      LOG_WARN("failed to generate drop tenant arg", KR(ret), K(tenant_id), K(tenant_name));
    } else if (OB_FAIL(drop_tenant(arg))) {
      LOG_WARN("fail to drop tenant", KR(ret), K(arg));
    }
  }
  return ret;
}


// 1. create new tenant schema
// 2. grant resource pool to new tenant
int ObTenantDDLService::create_tenant_schema(
    const ObCreateTenantArg &arg,
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObTenantSchema &user_tenant_schema,
    ObTenantSchema &meta_tenant_schema,
    const common::ObIArray<common::ObConfigPairs> &init_configs)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 1. start create tenant schema", K(arg));
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = user_tenant_schema.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(schema_service_)
             || OB_ISNULL(sql_proxy_)
             || OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(schema_service), KP_(sql_proxy),
             KP_(unit_mgr));
  } else {
    ObDDLSQLTransaction trans(schema_service_);
    int64_t refreshed_schema_version = OB_INVALID_VERSION;
    ObArray<ObResourcePoolName> pools;
    common::ObArray<uint64_t> new_ug_id_array;
    if (OB_FAIL(get_pools(arg.pool_list_, pools))) {
      LOG_WARN("get_pools failed", KR(ret), K(arg));
    } else if (OB_FAIL(schema_guard.get_schema_version(
               OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("fail to get schema version", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("start transaction failed, ", KR(ret), K(refreshed_schema_version));
    }
    // 1. create tenant schema
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.1. start create tenant schema", K(arg));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
      int64_t user_tenant_count = 0;
      if (OB_FAIL(schema_guard.get_user_tenant_count(user_tenant_count))) {
        LOG_WARN("fail to get tenant ids", KR(ret));
      } else if (OB_FAIL(ObLicenseUtils::check_for_create_tenant(user_tenant_count, arg.is_standby_tenant()))) {
        LOG_WARN("create more tenant is not allowd", KR(ret), K(user_tenant_count));
      } else if (OB_FAIL(ddl_operator.create_tenant(meta_tenant_schema, OB_DDL_ADD_TENANT_START, trans))) {
        LOG_WARN("create tenant failed", KR(ret), K(meta_tenant_schema));
      } else if (OB_FAIL(ddl_operator.create_tenant(
                     user_tenant_schema, OB_DDL_ADD_TENANT_START, trans, &arg.ddl_stmt_str_))) {
        LOG_WARN("create tenant failed", KR(ret), K(user_tenant_schema));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.1. finish create tenant schema", KR(ret), K(arg),
               "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }

    // 2. grant pool
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.2. start grant pools", K(user_tenant_id));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      lib::Worker::CompatMode compat_mode = get_worker_compat_mode(
                                         user_tenant_schema.get_compatibility_mode());
      if (OB_FAIL(unit_mgr_->grant_pools(
                         trans, new_ug_id_array,
                         compat_mode,
                         pools, user_tenant_id,
                         false/*is_bootstrap*/,
                         false/*check_data_version*/))) {
        LOG_WARN("grant_pools_to_tenant failed", KR(ret), K(arg), K(pools), K(user_tenant_id));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.2. finish grant pools", KR(ret), K(user_tenant_id),
               "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }

    // 3. persist initial tenant config
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.3. start persist tenant config", K(user_tenant_id));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      ObArray<ObAddr> addrs;
      if (OB_FAIL(unit_mgr_->get_servers_by_pools(pools, addrs))) {
        LOG_WARN("fail to get tenant's servers", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(notify_init_tenant_config(*rpc_proxy_, init_configs, addrs))) {
        LOG_WARN("fail to notify broadcast tenant config", KR(ret), K(addrs), K(init_configs));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.3. finish persist tenant config",
               KR(ret), K(user_tenant_id), "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }

    // If the transaction returns successfully, modify the unit_mgr memory data structure
    // If the transaction fails, the transaction may be submitted successfully. At this time,
    // the transaction is considered to have failed, and the unit_mgr memory state is not modified at this time,
    // and the transaction 1 is subsequently rolled back through drop tenant.
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] STEP 1.4. start reload unit_manager", K(user_tenant_id));
      const int64_t tmp_start_time = ObTimeUtility::fast_current_time();
      if (OB_FAIL(unit_mgr_->load())) {
        LOG_WARN("unit_manager reload failed", K(ret));
      }
      FLOG_INFO("[CREATE_TENANT] STEP 1.4. finish reload unit_manager", KR(ret), K(user_tenant_id),
               "cost", ObTimeUtility::fast_current_time() - tmp_start_time);
    }

    if (OB_SUCC(ret)) {
      ObArray<ObAddr> addrs;
      ObZone zone; // empty means get all zone's servers
      if (OB_FAIL(unit_mgr_->get_tenant_unit_servers(user_tenant_id, zone, addrs))) {
        LOG_WARN("fail to get tenant's servers", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID, addrs))) {
        LOG_WARN("publish schema failed", KR(ret), K(addrs));
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 1. finish create tenant schema", KR(ret), K(user_tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::notify_init_tenant_config(
    obrpc::ObSrvRpcProxy &rpc_proxy,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    const common::ObIArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L; // 10s
  if (OB_UNLIKELY(
      init_configs.count() <= 0
      || addrs.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init configs count is invalid", KR(ret), K(init_configs), K(addrs));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT))) {
    LOG_WARN("fail to set default timeout", KR(ret));
  } else {
    ObArenaAllocator allocator("InitTenantConf");
    // 1. construct arg
    obrpc::ObInitTenantConfigArg arg;
    for (int64_t i = 0; OB_SUCC(ret) && i < init_configs.count(); i++) {
     const common::ObConfigPairs &pairs = init_configs.at(i);
     obrpc::ObTenantConfigArg config;
     char *buf = NULL;
     int64_t length = pairs.get_config_str_length();
     if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_WARN("alloc memory failed", KR(ret), K(length));
     } else {
       MEMSET(buf, '\0', length);
       if (OB_FAIL(pairs.get_config_str(buf, length))) {
         LOG_WARN("fail to get config str", KR(ret), K(length), K(pairs));
       } else {
         config.tenant_id_ = pairs.get_tenant_id();
         config.config_str_.assign_ptr(buf, strlen(buf));
         if (OB_FAIL(arg.add_tenant_config(config))) {
           LOG_WARN("fail to add config", KR(ret), K(config));
         }
       }
     }
    } // end for
    // 2. send rpc
    rootserver::ObInitTenantConfigProxy proxy(
        rpc_proxy, &obrpc::ObSrvRpcProxy::init_tenant_config);
    bool call_rs = false;
    ObAddr rs_addr = GCONF.self_addr_;
    int64_t timeout = ctx.get_timeout();
    for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); i++) {
      const ObAddr &addr = addrs.at(i);
      if (OB_FAIL(proxy.call(addr, timeout, arg))) {
        LOG_WARN("send rpc failed", KR(ret), K(addr), K(timeout), K(arg));
      } else if (rs_addr == addr) {
        call_rs = true;
      }
    } // end for
    if (OB_FAIL(ret) || call_rs) {
    } else if (OB_FAIL(proxy.call(rs_addr, timeout, arg))) {
      LOG_WARN("fail to call rs", KR(ret), K(rs_addr), K(timeout), K(arg));
    }
    // 3. check result
    ObArray<int> return_ret_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_ret_array))) { // ignore ret
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_ret_array.count()))) {
      LOG_WARN("return cnt not match", KR(ret), "return_cnt", return_ret_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_ret_array.count(); i++) {
        int return_ret = return_ret_array.at(i);
        const ObAddr &addr = proxy.get_dests().at(i);
        const ObInitTenantConfigRes *result = proxy.get_results().at(i);
        if (OB_SUCCESS != return_ret) {
          ret = return_ret;
          LOG_WARN("rpc return error", KR(ret), K(addr), K(timeout));
        } else if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get empty result", KR(ret), K(addr), K(timeout));
        } else if (OB_SUCCESS != result->get_ret()) {
          ret = result->get_ret();
          LOG_WARN("persist tenant config failed", KR(ret), K(addr), K(timeout));
        }
      } // end for
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_schema_(
    const obrpc::ObParallelCreateNormalTenantArg &arg,
    ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tmp_tenant_schema = nullptr;
  const uint64_t tenant_id = arg.tenant_id_;
  if (!arg.is_valid() || !is_valid_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(arg));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tmp_tenant_schema))) {
    LOG_WARN("failed to get tenant info", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tmp_tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is NULL", KR(ret), KP(tmp_tenant_schema));
  } else if (OB_FAIL(tenant_schema.assign(*tmp_tenant_schema))) {
    LOG_WARN("failed to assign tenant_schema", KR(ret), KP(tmp_tenant_schema));
  } else if (!tenant_schema.is_creating()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant status is not creating", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(set_tenant_schema_charset_and_collation(tenant_schema, arg.create_tenant_arg_))) {
    // charset and collation are not written in __all_tenant
    // create database will use the charset
    // so if user set charset in create_tenant sql, we should assign it to tenant_schema
    LOG_WARN("failed to set tenant charset and collation", KR(ret));
  }
  return ret;
}

int ObTenantDDLService::insert_tenant_merge_info_(
    const ObSchemaOperationType op,
    const ObTenantSchema &tenant_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    // add zone merge info
    if ((OB_DDL_ADD_TENANT_START == op) || (OB_DDL_ADD_TENANT == op)) {
      HEAP_VARS_4((ObGlobalMergeInfo, global_info),
                  (ObZoneMergeInfoArray, merge_info_array),
                  (ObZoneArray, zone_list),
                  (ObZoneMergeInfo, tmp_merge_info)) {

        global_info.tenant_id_ = tenant_id;
        tmp_merge_info.tenant_id_ = tenant_id;
        if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
          LOG_WARN("fail to get zone list", KR(ret));
        }

        for (int64_t i = 0; (i < zone_list.count()) && OB_SUCC(ret); ++i) {
          tmp_merge_info.zone_ = zone_list.at(i);
          if (OB_FAIL(merge_info_array.push_back(tmp_merge_info))) {
            LOG_WARN("fail to push_back", KR(ret));
          }
        }
        // add zone merge info of current tenant(sys tenant or meta tenant)
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObGlobalMergeTableOperator::insert_global_merge_info(trans,
              tenant_id, global_info))) {
            LOG_WARN("fail to insert global merge info of current tenant", KR(ret), K(global_info));
          } else if (OB_FAIL(ObZoneMergeTableOperator::insert_zone_merge_infos(
                     trans, tenant_id, merge_info_array))) {
            LOG_WARN("fail to insert zone merge infos of current tenant", KR(ret), K(tenant_id),
              K(merge_info_array));
          }
        }
        // add zone merge info of relative user tenant if current tenant is meta tenant
        if (OB_SUCC(ret) && is_meta_tenant(tenant_id)) {
          const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
          global_info.tenant_id_ = user_tenant_id;
          for (int64_t i = 0; i < merge_info_array.count(); ++i) {
            merge_info_array.at(i).tenant_id_ = user_tenant_id;
          }
          if (OB_FAIL(ObGlobalMergeTableOperator::insert_global_merge_info(trans,
              user_tenant_id, global_info))) {
            LOG_WARN("fail to insert global merge info of user tenant", KR(ret), K(global_info));
          } else if (OB_FAIL(ObZoneMergeTableOperator::insert_zone_merge_infos(
                    trans, user_tenant_id, merge_info_array))) {
            LOG_WARN("fail to insert zone merge infos of user tenant", KR(ret), K(user_tenant_id),
              K(merge_info_array));
          }
        }
      }
    }
  }

  return ret;
}
int ObTenantDDLService::init_tenant_env_after_schema_(
    const ObTenantSchema &tenant_schema,
    const obrpc::ObParallelCreateNormalTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.6. start init_tenant_env_after_schema_", K(tenant_id));
  const ObCreateTenantArg &create_tenant_arg = arg.create_tenant_arg_;
  const ObTenantRole &tenant_role = create_tenant_arg.get_tenant_role();
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (!tenant_role.is_primary() && OB_FAIL(insert_restore_tenant_job_(
          user_tenant_id, create_tenant_arg.tenant_schema_.get_tenant_name(), tenant_role, trans))) {
    LOG_WARN("failed to insert restore or clone tenant job", KR(ret), K(create_tenant_arg));
  } else if (create_tenant_arg.is_creating_standby_ &&
      OB_FAIL(set_log_restore_source_(user_tenant_id, create_tenant_arg.log_restore_source_, trans))) {
    LOG_WARN("failed to set_log_restore_source", KR(ret), K(user_tenant_id), K(create_tenant_arg));
  } else if (is_meta_tenant(tenant_id)) {
    if (OB_FAIL(insert_tenant_merge_info_(OB_DDL_ADD_TENANT_START, tenant_schema, trans))) {
      LOG_WARN("fail to insert tenant merge info", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(ObServiceEpochProxy::init_service_epoch(
            trans,
            tenant_id,
            0, /*freeze_service_epoch*/
            0, /*arbitration_service_epoch*/
            0, /*server_zone_op_service_epoch*/
            0, /*heartbeat_service_epoch*/
            0 /* service_name_epoch */))) {
      LOG_WARN("fail to init service epoch", KR(ret));
    }
  }
  if (trans.is_started()) {
    bool commit = OB_SUCC(ret);
    if (OB_TMP_FAIL(trans.end(commit))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      LOG_WARN("trans end failed", K(commit), K(tmp_ret));
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.6. finish init_tenant_env_after_schema_", K(tenant_id),
      "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::init_tenant_env_before_schema_(
    const ObTenantSchema &tenant_schema,
    const obrpc::ObParallelCreateNormalTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.4. start init_tenant_env_before_schema_", K(tenant_id));
  const ObCreateTenantArg &create_tenant_arg = arg.create_tenant_arg_;
  common::ObArray<common::ObConfigPairs> init_configs;
  if (OB_FAIL(generate_tenant_init_configs(create_tenant_arg, user_tenant_id, init_configs))) {
    LOG_WARN("failed to generate tenant init configs", KR(ret), K(arg), K(tenant_id));
  } else if (OB_FAIL(add_extra_tenant_init_config_(user_tenant_id, init_configs))) {
    LOG_WARN("fail to add_extra_tenant_init_config", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_tenant_global_stat_(tenant_id, init_configs, trans))) {
    LOG_WARN("failed to init tenant global stat", KR(ret), K(tenant_id), K(init_configs));
  } else if (OB_FAIL(init_tenant_sys_stats_(tenant_id, trans))) {
    LOG_WARN("insert default sys stats failed", K(tenant_id), K(ret));
  } else if (is_meta_tenant(tenant_id) && OB_FAIL(init_meta_tenant_env_(tenant_schema,
          create_tenant_arg, init_configs, trans))) {
    LOG_WARN("failed to init meta tenant env", KR(ret), K(tenant_schema), K(init_configs));
  } else if (is_user_tenant(tenant_id) && OB_FAIL(init_user_tenant_env_(tenant_id, trans))) {
    LOG_WARN("failed to init user tenant env", KR(ret), K(tenant_id));
  }
  if (trans.is_started()) {
    bool commit = OB_SUCC(ret);
    if (OB_TMP_FAIL(trans.end(commit))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      LOG_WARN("trans end failed", K(commit), K(tmp_ret));
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.4. finish init_tenant_env_before_schema_", K(tenant_id),
      "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CREATE_META_NORMAL_TENANT_ERROR);
ERRSIM_POINT_DEF(ERRSIM_CREATE_USER_NORMAL_TENANT_ERROR);
// this function is running on parallel ddl thread while not hold any ddl lock
// it does the following things:
// 1. broadcast schema
// 2. create tablet
// 3. create other schema
int ObTenantDDLService::create_normal_tenant(obrpc::ObParallelCreateNormalTenantArg &arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const ObCreateTenantArg &create_tenant_arg = arg.create_tenant_arg_;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObCurTraceId::TraceId old_trace_id = OB_ISNULL(ObCurTraceId::get_trace_id()) ?
    ObCurTraceId::TraceId() : *ObCurTraceId::get_trace_id();
  ObCurTraceId::TraceId new_trace_id;
  new_trace_id.init(GCONF.self_addr_);
  FLOG_INFO("[CREATE_TENANT] change trace_id for create tenant", K(old_trace_id), K(new_trace_id),
      K(tenant_id));
  common::ObTraceIdGuard trace_id_guard(new_trace_id);
  TIMEGUARD_INIT(create_normal_tenant, 10_s);
  FLOG_INFO("[CREATE_TENANT] STEP 2. start create normal tenant", K(tenant_id));
  ObArenaAllocator arena_allocator("InnerTableSchem", OB_MALLOC_MIDDLE_BLOCK_SIZE);
  ObSArray<ObTableSchema> tables;
  ObTenantSchema tenant_schema;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(get_tenant_schema_(arg, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), "tenant_id", arg.tenant_id_);
  }
  if (is_user_tenant(tenant_id)) {
    DEBUG_SYNC(BEFORE_CREATE_USER_TENANT);
  } else if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(BEFORE_CREATE_META_TENANT);
  }
  // user tenant sys ls is created before meta tenant table writable
  // which may cause user tenant trying to write meta tenant's table when meta tenant's tablets are creating
  // in this case, SQL.ENG will return -4138 which will retry immediately.
  // the retries will explod user's work queue .
  // so we create tablet before braodcast schema, make them retrun OB_TABLE_NOT_EXIST which will not be retried
  if (FAILEDx(ObSchemaUtils::construct_inner_table_schemas(tenant_id, tables, arena_allocator))) {
    LOG_WARN("fail to get inner table schemas in tenant space", KR(ret), K(tenant_id));
  } else if (CLICK_FAIL(create_tenant_sys_tablets(tenant_id, tables))) {
    LOG_WARN("fail to create tenant partitions", KR(ret), K(tenant_id));
  } else if (CLICK_FAIL(broadcast_sys_table_schemas(tenant_id))) {
    LOG_WARN("fail to broadcast sys table schemas", KR(ret), K(tenant_id));
  }
  if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_META_TENANT_TABLETS);
  } else if (is_user_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_USER_TENANT_TABLETS);
  }
  if (OB_FAIL(ret)) {
    // for code we need to execute before creating tenant user ls, we should put it in `before`
    // otherwise, they should be put in `after`
  } else if (CLICK_FAIL(init_tenant_env_before_schema_(tenant_schema, arg))) {
    LOG_WARN("failed to init tenant env before schema", KR(ret), K(tenant_schema), K(arg));
  } else if (CLICK_FAIL(init_tenant_schema(arg.create_tenant_arg_, tenant_schema, tables))) {
    LOG_WARN("fail to init tenant schema", KR(ret), K(tenant_id));
  } else if (CLICK_FAIL(init_tenant_env_after_schema_(tenant_schema, arg))) {
    LOG_WARN("failed to init tenant env after schema", KR(ret), K(tenant_schema), K(arg));
  }
  CLICK();
  if (is_user_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_USER_NORMAL_TENANT);
  } else if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_META_NORMAL_TENANT);
  }
  if (OB_FAIL(ret)) {
  } else if (is_user_tenant(tenant_id) && OB_FAIL(ERRSIM_CREATE_USER_NORMAL_TENANT_ERROR)) {
    LOG_WARN("ERRSIM_CREATE_USER_NORMAL_TENANT_ERROR hit", KR(ret));
  } else if (is_meta_tenant(tenant_id) && OB_FAIL(ERRSIM_CREATE_META_NORMAL_TENANT_ERROR)) {
    LOG_WARN("ERRSIM_CREATE_META_NORMAL_TENANT_ERROR hit", KR(ret));
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2. finish create normal tenant", KR(ret), K(new_trace_id),
      K(old_trace_id), K(tenant_id), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObTenantDDLService::broadcast_sys_table_schemas(const uint64_t tenant_id)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.3. start broadcast sys table schemas", K(tenant_id));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (is_sys_tenant(tenant_id) || is_virtual_tenant_id(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    // Ensure observer which contains rs or tenant's sys ls leader has avaliable schemas.
    const uint64_t user_tenant_id = is_user_tenant(tenant_id) ? tenant_id : gen_user_tenant_id(tenant_id);
    ObArray<ObAddr> addrs;
    ObArray<ObUnit> units;
    ObAddr leader;
    ObUnitTableOperator unit_operator;
    if (OB_FAIL(get_ls_member_list_for_creating_tenant_(tenant_id, ObLSID::SYS_LS_ID, leader, addrs))) {
      // we just need leader, so addrs is not used, it will be reset later
      LOG_WARN("failed to get ls member list for creating tenant", KR(ret), K(tenant_id));
    } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
      LOG_WARN("failed to init unit operator", KR(ret));
    } else if (OB_FAIL(unit_operator.get_units_by_tenant(user_tenant_id, units))) {
      LOG_WARN("failed to get units by tenant", KR(ret), K(user_tenant_id));
    } else {
      ObTimeoutCtx ctx;
      ObBatchBroadcastSchemaProxy proxy(*rpc_proxy_,
                                        &ObSrvRpcProxy::batch_broadcast_schema);
      obrpc::ObBatchBroadcastSchemaArg arg;
      int64_t sys_schema_version = OB_INVALID_VERSION;
      ObArray<ObTableSchema> tables;
      addrs.reset();
      FOREACH_X(unit, units, OB_SUCC(ret)) {
        if (!unit->is_valid() || !unit->is_active_status()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid unit", KR(ret), K(*unit));
        } else if (OB_FAIL(addrs.push_back(unit->server_))) {
          LOG_WARN("failed to push_back addr", KR(ret), K(*unit));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!is_contain(addrs, GCONF.self_addr_) && OB_FAIL(addrs.push_back(GCONF.self_addr_))) {
          LOG_WARN("fail to push back rs addr", KR(ret));
      } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
        LOG_WARN("fail to set timeout ctx", KR(ret));
      } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(
                 OB_SYS_TENANT_ID, sys_schema_version))) {
      } else if (OB_FAIL(arg.init(tenant_id, sys_schema_version, tables, true/*generate_schema*/))) {
        LOG_WARN("fail to init arg", KR(ret), K(tenant_id), K(sys_schema_version));
      }
      const int64_t timeout_ts = ctx.get_timeout(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); i++) {
        const ObAddr &addr = addrs.at(i);
        if (OB_FAIL(proxy.call(addr, timeout_ts, arg))) {
          LOG_WARN("fail to send rpc", KR(ret), K(tenant_id),
                   K(sys_schema_version), K(addr), K(timeout_ts));
        }
      } // end for

      ObArray<int> return_code_array;
      int tmp_ret = OB_SUCCESS; // always wait all
      if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
        LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_FAIL(ret)) {
      } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
        LOG_WARN("return cnt not match", KR(ret), "return_cnt", return_code_array.count());
      } else {
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
          const ObAddr &addr = proxy.get_dests().at(i);
          if (OB_TMP_FAIL(return_code_array.at(i))) {
            LOG_WARN("broadcast schema failed", KR(tmp_ret), K(addr), K(tenant_id));
            if (addr == leader || addr == GCONF.self_addr_) {
              ret = tmp_ret;
              LOG_WARN("rs or tenant sys ls leader braodcast schema failed", KR(ret), K(addr), K(leader));
            }
          }
        } // end for
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.3. finish broadcast sys table schemas", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CREATE_SYS_TABLETS_ERROR);
int ObTenantDDLService::create_tenant_sys_tablets(
    const uint64_t tenant_id,
    common::ObIArray<ObTableSchema> &tables)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.2. start create sys table tablets", K(tenant_id));
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_ISNULL(rpc_proxy_)
             || OB_ISNULL(lst_operator_)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(rpc_proxy), KP_(lst_operator));
  } else {
    // FIXME: (yanmu.ztl) use actual trans later
    ObMySQLTransaction trans;
    share::schema::ObSchemaGetterGuard dummy_guard;
    SCN frozen_scn = SCN::base_scn();
    ObTableCreator table_creator(tenant_id,
                                 frozen_scn,
                                 trans);
    common::ObArray<share::ObLSID> ls_id_array;
    const ObTablegroupSchema *dummy_tablegroup_schema = NULL;
    ObArray<const share::schema::ObTableSchema*> table_schemas;
    ObArray<uint64_t> index_tids;
    ObArray<bool> need_create_empty_majors;
    if (OB_FAIL(trans.start(sql_proxy_, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_creator.init(false/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init tablet creator", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ls_id_array.push_back(ObLSID(SYS_LS)))) {
      LOG_WARN("fail to push back sys ls", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObTableSchema &data_table = tables.at(i);
      const uint64_t data_table_id = data_table.get_table_id();
      if (data_table.has_partition()) {
        table_schemas.reset();
        need_create_empty_majors.reset();
        if (OB_FAIL(table_schemas.push_back(&data_table)) || OB_FAIL(need_create_empty_majors.push_back(true))) {
          LOG_WARN("fail to push back data table ptr", KR(ret), K(data_table_id));
        } else if (ObSysTableChecker::is_sys_table_has_index(data_table_id)) {
          if (OB_FAIL(ObSysTableChecker::get_sys_table_index_tids(data_table_id, index_tids))) {
            LOG_WARN("fail to get sys index tids", KR(ret), K(data_table_id));
          } else if (i + index_tids.count()  >= tables.count()
                     || index_tids.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sys table's index should be next to its data table",
                     KR(ret), K(i), "index_cnt",  index_tids.count());
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < index_tids.count(); j++) {
              const ObTableSchema &index_schema = tables.at(i + j + 1);
              const int64_t index_id = index_schema.get_table_id();
              if (index_id != index_tids.at(j)
                  || data_table_id != index_schema.get_data_table_id()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("sys index schema order is not match", KR(ret), K(data_table_id), K(j), K(index_schema));
              } else if (OB_FAIL(table_schemas.push_back(&index_schema))) {
                LOG_WARN("fail to push back index schema", KR(ret), K(index_id), K(data_table_id));
              } else if (OB_FAIL(need_create_empty_majors.push_back(true))) {
                LOG_WARN("fail to push back need create empty major", KR(ret), K(index_id), K(data_table_id));
              }
            } // end for
          }
        }

        if (OB_SUCC(ret) && is_system_table(data_table_id)) {
          uint64_t lob_meta_table_id = OB_INVALID_ID;
          uint64_t lob_piece_table_id = OB_INVALID_ID;
          if (OB_ALL_CORE_TABLE_TID == data_table_id) {
            // do nothing
          } else if (!get_sys_table_lob_aux_table_id(data_table_id, lob_meta_table_id, lob_piece_table_id)) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("fail to get_sys_table_lob_aux_table_id", KR(ret), K(data_table_id));
          } else {
            int64_t meta_idx = -1;
            int64_t piece_idx = -1;
            for (int64_t k = i + 1; OB_SUCC(ret) && k < tables.count(); k++) {
              if (tables.at(k).get_table_id() == lob_meta_table_id) {
                meta_idx = k;
              }
              if (tables.at(k).get_table_id() == lob_piece_table_id) {
                piece_idx = k;
              }
              if (meta_idx != -1 && piece_idx != -1) {
                break;
              }
            }
            if (meta_idx == -1 || piece_idx == -1) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("sys table's lob table not matched", KR(ret), K(meta_idx), K(piece_idx),
                       K(lob_piece_table_id), K(lob_meta_table_id), K(data_table_id));
            } else {
              if (OB_FAIL(table_schemas.push_back(&tables.at(meta_idx))) || OB_FAIL(need_create_empty_majors.push_back(true))) {
                LOG_WARN("fail to push back lob meta aux table ptr", KR(ret), K(meta_idx), K(data_table_id));
              } else if (OB_FAIL(table_schemas.push_back(&tables.at(piece_idx))) || OB_FAIL(need_create_empty_majors.push_back(true))) {
                LOG_WARN("fail to push back lob piece aux table ptr", KR(ret), K(piece_idx), K(data_table_id));
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
          // failed, bypass
        } else if (OB_FAIL(table_creator.add_create_tablets_of_tables_arg(
                table_schemas,
                ls_id_array,
                DATA_CURRENT_VERSION,
                need_create_empty_majors/*need_create_empty_major_sstable*/))) {
          LOG_WARN("fail to add create tablets of table", KR(ret), K(data_table), K(table_schemas), K(need_create_empty_majors));
        }
      }
    } // end for
    if (FAILEDx(table_creator.execute())) {
      LOG_WARN("fail to execute creator", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ERRSIM_CREATE_SYS_TABLETS_ERROR)) {
      LOG_WARN("ERRSIM_CREATE_SYS_TABLETS_ERROR", KR(ret));
    } else {
      ALLOW_NEXT_LOG();
      LOG_INFO("create tenant sys tables tablet", KR(ret), K(tenant_id));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.2. finish create sys table tablets", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::insert_restore_tenant_job_(
    const uint64_t tenant_id,
    const ObString &tenant_name,
    const share::ObTenantRole &tenant_role,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id));
  } else if (is_meta_tenant(tenant_id)) {
    // no need to insert job for meta tenant
  } else if (!tenant_role.is_restore()) {
    // no need to insert restore job
  } else if (OB_FAIL(ObRestoreUtil::insert_user_tenant_restore_job(*sql_proxy_, tenant_name, tenant_id, trans))) {
    LOG_WARN("failed to insert user tenant restore job", KR(ret), K(tenant_id), K(tenant_name));
  }
  return ret;
}

int ObTenantDDLService::set_log_restore_source_(
    const uint64_t tenant_id,
    const common::ObString &log_restore_source,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  share::ObBackupConfigParserMgr config_parser_mgr;
  common::ObSqlString name;
  common::ObSqlString value;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id) || log_restore_source.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(log_restore_source));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (OB_FAIL(name.assign("log_restore_source"))) {
    LOG_WARN("assign sql failed", KR(ret));
  } else if (OB_FAIL(value.assign(log_restore_source))) {
    LOG_WARN("fail to assign value", KR(ret), K(log_restore_source));
  } else if (OB_FAIL(config_parser_mgr.init(name, value, gen_user_tenant_id(tenant_id)))) {
    LOG_WARN("fail to init backup config parser mgr", KR(ret), K(name), K(value), K(tenant_id));
    // TODO use the interface without rpc_proxy_
  } else if (OB_FAIL(config_parser_mgr.update_inner_config_table(*rpc_proxy_, trans))) {
    LOG_WARN("fail to update inner config table", KR(ret), K(name), K(value));
  }
  return ret;
}

int ObTenantDDLService::init_tenant_schema(
    const obrpc::ObCreateTenantArg &create_tenant_arg,
    const ObTenantSchema &tenant_schema,
    common::ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  TIMEGUARD_INIT(init_tenant_schema, 8_s);
  const uint64_t SCHEMA_VERSION_MAX_COUNT = 100000; // 10w
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.5. start init tenant schemas", K(tenant_id));
  DEBUG_SYNC(BEFORE_CREATE_TENANT_TABLE_SCHEMA);
  ObDDLSQLTransaction trans(schema_service_, true, true, false, false);
  const int64_t init_schema_version = tenant_schema.get_schema_version();
  const int64_t refreshed_schema_version = 0;
  int64_t new_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_FAIL(ddl_trans_controller_->reserve_schema_version(tenant_id, SCHEMA_VERSION_MAX_COUNT))) {
    LOG_WARN("failed to reserve schema version", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObSysVariableSchema, sys_variable) {
      ObSchemaService *schema_service_impl = schema_service_->get_schema_service();
      ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
      if (OB_ISNULL(schema_service_impl)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pointer is null", KR(ret), KP(schema_service_impl));
      } else if (CLICK_FAIL(load_sys_table_schemas(tenant_schema, tables))) {
        LOG_WARN("fail to load sys tables", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_service_impl->gen_new_schema_version(
              tenant_id, init_schema_version, new_schema_version))) {
        LOG_WARN("failed to gen_new_schema_version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(init_system_variables(create_tenant_arg, tenant_schema, sys_variable))) {
        LOG_WARN("failed to generate tenant sys variable", KR(ret), K(create_tenant_arg), K(tenant_schema));
      } else if (CLICK_FAIL(ddl_operator.replace_sys_variable(
              sys_variable, new_schema_version, trans, OB_DDL_ALTER_SYS_VAR))) {
        LOG_WARN("fail to replace sys variable", KR(ret), K(sys_variable));
      } else if (CLICK_FAIL(ddl_operator.init_tenant_schemas(tenant_schema, sys_variable, trans))) {
        LOG_WARN("init tenant env failed", KR(ret), K(tenant_schema), K(sys_variable));
      } else if (CLICK_FAIL(ObLoadInnerTableSchemaExecutor::load_core_schema_version(tenant_id, trans,
          ObSchemaUtils::get_inner_table_core_schema_version(tables)))) {
        LOG_WARN("failed to load core schema version", KR(ret), K(tenant_id));
      }
    }
  }
  if (trans.is_started()) {
    bool commit = OB_SUCC(ret);
    if (OB_TMP_FAIL(trans.end(commit))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      LOG_WARN("trans end failed", K(commit), K(tmp_ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (CLICK_FAIL(refresh_creating_tenant_schema_(tenant_schema))) {
    LOG_WARN("failed to refresh creating tenant schema", KR(ret), K(tenant_schema));
  }

  FLOG_INFO("[CREATE_TENANT] STEP 2.5. finish init tenant schemas", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::load_sys_table_schemas(
    const ObTenantSchema &tenant_schema,
    common::ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  FLOG_INFO("[CREATE_TENANT] start load all schemas", "table count", tables.count());
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  ObLoadInnerTableSchemaExecutor executor;
  ObUnitTableOperator table;
  common::ObArray<ObUnitConfig> configs;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (OB_FAIL(table.init(*sql_proxy_))) {
    LOG_WARN("failed to init unit table operator", KR(ret));
  } else if (OB_FAIL(table.get_unit_configs_by_tenant(gen_user_tenant_id(tenant_id), configs))) {
    LOG_WARN("failed to get unit configs by tenant", KR(ret), K(tenant_id));
  } else if (configs.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config is empty", KR(ret), K(configs));
  } else if (OB_FAIL(executor.init(tables, tenant_schema.get_tenant_id(), configs.at(0).max_cpu(),
          rpc_proxy_))) {
    LOG_WARN("failed to init executor", KR(ret));
  } else if (OB_FAIL(executor.execute())) {
    LOG_WARN("failed to execute load all schema", KR(ret));
  }
  FLOG_INFO("[CREATE_TENANT] finish load all schemas", KR(ret), K(configs),
      "cost", ObTimeUtility::current_time() - begin_time);
  return ret;
}

int ObTenantDDLService::init(
    ObUnitManager &unit_mgr,
    ObDDLService &ddl_service,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    obrpc::ObCommonRpcProxy &common_rpc,
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    share::ObLSTableOperator &lst_operator,
    ObZoneManager &zone_mgr)
{
  int ret = OB_SUCCESS;
  unit_mgr_ = &unit_mgr;
  ddl_service_ = &ddl_service;
  rpc_proxy_ = &rpc_proxy;
  common_rpc_ = &common_rpc;
  sql_proxy_ = &sql_proxy;
  schema_service_ = &schema_service;
  lst_operator_ = &lst_operator;
  ddl_trans_controller_ = &schema_service.get_ddl_trans_controller();
  zone_mgr_ = &zone_mgr;
  inited_ = true;
  stopped_ = false;
  return ret;
}

int ObTenantDDLService::add_extra_tenant_init_config_(
                  const uint64_t tenant_id,
                  common::ObIArray<common::ObConfigPairs> &init_configs)
{
  // For clusters upgraded from a lower version, modifying config default values may affect existing tenants.
  // Therefore, you can add config default values here that should apply to newly created tenants.
  int ret = OB_SUCCESS;
  bool find = false;
  ObString config_name("_parallel_ddl_control");
  ObSqlString config_value;
  ObString config_name_mysql_compatible_dates("_enable_mysql_compatible_dates");
  ObString config_value_mysql_compatible_dates("true");
  ObString config_name_immediate_check_unique("_ob_immediate_row_conflict_check");
  ObString config_value_immediate_check("False");
  // TODO(fanfangzhou.ffz): temporarily disable config adjustment for ddl thread isolation
  ObString config_name_ddl_thread_isolution("_enable_ddl_worker_isolation");
  ObString config_value_ddl_thread_isolution("false");

  if (OB_FAIL(ObParallelDDLControlMode::generate_parallel_ddl_control_config_for_create_tenant(config_value))) {
    LOG_WARN("fail to generate parallel ddl control config value", KR(ret));
  }
  for (int index = 0 ; !find && OB_SUCC(ret) && index < init_configs.count(); ++index) {
    if (tenant_id == init_configs.at(index).get_tenant_id()) {
      find = true;
      common::ObConfigPairs &tenant_init_config = init_configs.at(index);
      if (OB_FAIL(tenant_init_config.add_config(config_name, config_value.string()))) {
        LOG_WARN("fail to add config", KR(ret), K(config_name), K(config_value));
      } else if (OB_FAIL(tenant_init_config.add_config(config_name_mysql_compatible_dates, config_value_mysql_compatible_dates))) {
        LOG_WARN("fail to add config", KR(ret), K(config_name_mysql_compatible_dates), K(config_value_mysql_compatible_dates));
      } else if (OB_FAIL(tenant_init_config.add_config(config_name_immediate_check_unique, config_value_immediate_check))) {
        LOG_WARN("fail to add config", KR(ret), K(config_name_immediate_check_unique), K(config_value_immediate_check));
      } else if (OB_FAIL(tenant_init_config.add_config(config_name_ddl_thread_isolution, config_value_ddl_thread_isolution))) {
        LOG_WARN("fail to add config", KR(ret), K(config_name_ddl_thread_isolution), K(config_value_ddl_thread_isolution));
      }
    }
  }
  // ---- Add new tenant init config above this line -----
  // At the same time, to verify modification, you need modify test case tenant_init_config(_oracle).test
  if (OB_SUCC(ret) && !find) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no matched tenant config", KR(ret), K(tenant_id), K(init_configs));
  }
  return ret;

}

/*
 * After the schema is split, there are two scenarios for cross-tenant transactions involved in modify_tenant:
 *
 * Scenario 1: Modify tenant option and system variable at the same time through alter_tenant.
 *  For this scenario, the following restrictions are introduced:
 *  1. It is not allowed to modify tenant option and system variable at the same time.
 *  2. For redundant system variables in the tenant schema and system variable schema,
 *    the synchronization of the two will no longer be guaranteed in the future
 * - read only: For the read only attribute, in order to avoid the failure of inner sql to write user tenant system tables,
 *   inner sql skips the read only check. For external SQL, the read only attribute is subject to the system variable;
 * - name_case_mode: This value is specified when the tenant is created. It is a read only system variable
 *   (lower_case_table_names), and subsequent modifications are not allowed;
 * - ob_compatibility_mode: This value needs to be specified when the tenant is created.
 *   It is a read only system variable and cannot be modified later.

 * Scenario 2:
 * When the tenant locality is modified, the primary_zone is set in database/tablegroup/table
 * and the locality of the tablegroup/table adopts inherited semantics, there will be a scenario
 * where the primary_zone does not match the locality. In this case, it need to modify the primary_zone
 * of each database object under the tenant through DDL.
 *
 * After the schema is split, in order to avoid cross-tenant transactions, the process is split into two transactions.
 * The first transaction modifies the primary_zone of the database object under the tenant, and the second transaction
 * modifies the tenant schema. DDL failed, manual intervention to modify the schema.
 */
int ObTenantDDLService::modify_tenant(const ObModifyTenantArg &arg)
{
  LOG_INFO("receive modify tenant request", K(arg));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *orig_tenant_schema = NULL;
  const ObString &tenant_name = arg.tenant_schema_.get_tenant_name();
  bool is_restore = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (0 != arg.sys_var_list_.count() &&
             !arg.alter_option_bitset_.is_empty()) {
    // After the schema is split, because __all_sys_variable is split under the tenant, in order to avoid
    // cross-tenant transactions, it is forbidden to modify the tenant option and the system variable at the same time.
    // For this reason, the read only column of the tenant option is no longer maintained,
    // and it is subject to system variables.
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("modify tenant option and system variable at the same time", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "modify tenant option and system variable at the same time");
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, orig_tenant_schema))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
    LOG_WARN("tenant not exists", K(arg), K(ret));
  } else if (OB_UNLIKELY(NULL == orig_tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
    LOG_WARN("tenant not exists", K(arg), K(ret));
  } else if (FALSE_IT(is_restore = orig_tenant_schema->is_restore())) {
  } else if (!is_restore) {
    // The physical recovery may be in the system table recovery stage, and it is necessary to avoid
    // the situation where SQL cannot be executed and hang
    if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(
                orig_tenant_schema->get_tenant_id(), schema_guard))) {
      LOG_WARN("fail to get schema guard with version in inner table",
               K(ret), "tenant_id",  orig_tenant_schema->get_tenant_id());
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, orig_tenant_schema))) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
      LOG_WARN("tenant not exists", K(arg), K(ret));
    } else if (OB_UNLIKELY(NULL == orig_tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_USER_ERROR(OB_TENANT_NOT_EXIST, tenant_name.length(), tenant_name.ptr());
      LOG_WARN("tenant not exists", K(arg), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(modify_tenant_inner_phase(arg, orig_tenant_schema, schema_guard, is_restore))) {
    LOG_WARN("modify_tenant_inner_phase fail", K(ret));
  }
  return ret;
}

/*
 * The locality of tenant is changed in the following function. At present,
 * the locality settings of tenant and table have the following forms:
 * # describe
 *   1. The locality of the tenant must not be empty. The tenant locality upgraded from the version before 1.3 is empty
 *    in the internal table, but when the schema is refreshed, the locality of the tenant will be filled in
 *    as a full-featured replication of each zone.
 *   2. The locality of the table can be empty, which means that the locality of the tenant is inherited.
 *    When the locality of the table is not empty, it means that it does not completely match the locality of the tenant;
 * # locality change semantics
 *   1. When the locality of a tenant changes, the distribution of replications of all tables whose locality is empty
 *    under that tenant will change accordingly. When the locality of the tenant is changed for a table
 *    whose locality is not empty, the distribution of the corresponding replication will not change.
 *   2. Alter table can change the distribution of replications of a table whose locality is not empty.
 * # Mutual restriction of tenant and table locality changes
 *   1. When the old round of tenant locality has not been changed,
 *    the new round of tenant locality changes are not allowed to be executed.
 *   2. When the change of the table whose locality is not empty under tenant is not completed,
 *    the change of tenant locality is not allowed to be executed.
 *   3. When the locality change of tenant is not initiated, the locality change of the table
 *    whose locality is not empty is not allowed to be executed.
 * # Change rules
 *   1. One locality change is only allowed to do one of the operations of adding paxos,
 *    subtracting paxos and paxos type conversion (paxos->paxos), paxos->non_paxos is regarded as subtracting paxos,
 *    non_paxos->paxos is regarded as adding paxos;
 *   2. In a locality change:
 *    2.1. For adding paxos operation, orig_locality's paxos num >= majority(new_locality's paxos num);
 *    2.2. For subtracting paxos operation, new_locality's paxos num >= majority(orig_locality's paxos num);
 *    2.3. For converting paxos type operation, only one paxos type conversion is allowed for one locality change;
 *   3. For replication type conversion, the following constraints need to be met:
 *    3.1. For L-type replications, the replications other than F are not allowed to be converted to L,
 *      and L is not allowed to be converted to other replication types;
 *    3.2. There will be no restrictions for the rest of the situation
 *   4. In particular, in a scenario where only one replication of paxos is added,
 *    paxos num is allowed to go from 1 -> 2, but paxos num is not allowed to go from 2-> 1;
 *   5. Non_paxos replications can occur together with the above changes, and there is no limit to the number.
 * # after 1.4.7.1, the locality form of @region is no longer supported
 */
int ObTenantDDLService::set_new_tenant_options(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const ObModifyTenantArg &arg,
    share::schema::ObTenantSchema &new_tenant_schema,
    const share::schema::ObTenantSchema &orig_tenant_schema,
    AlterLocalityOp &alter_locality_op)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zones_in_pool;
  alter_locality_op = ALTER_LOCALITY_OP_INVALID;
  if (OB_FAIL(set_raw_tenant_options(arg, new_tenant_schema))) {
    LOG_WARN("fail to set raw tenant options", K(ret));
  } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::LOCALITY)) {
    common::ObArray<share::schema::ObZoneRegion> zone_region_list;
    AlterLocalityType alter_locality_type = ALTER_LOCALITY_INVALID;
    bool tenant_pools_in_shrinking = false;
    common::ObArray<share::ObResourcePoolName> resource_pool_names;
    if (new_tenant_schema.get_locality_str().empty()) {
      // It is not allowed to change the locality as an inherited attribute
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter locality to empty");
      LOG_WARN("alter locality to empty is not allowed", K(ret));
    } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
    } else if (OB_FAIL(unit_mgr_->check_tenant_pools_in_shrinking(
            orig_tenant_schema.get_tenant_id(), tenant_pools_in_shrinking))) {
      LOG_WARN("fail to check tenant pools in shrinking", K(ret));
    } else if (tenant_pools_in_shrinking) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter tenant locality when tenant pool is shrinking");
      LOG_WARN("alter tenant locality not allowed", K(ret), K(orig_tenant_schema));
    } else if (OB_FAIL(get_new_tenant_pool_zone_list(
            arg, new_tenant_schema, resource_pool_names, zones_in_pool, zone_region_list))) {
      LOG_WARN("fail to get new tenant pool zone list", K(ret));
    } else if (OB_FAIL(new_tenant_schema.set_locality(arg.tenant_schema_.get_locality_str()))) {
      LOG_WARN("fail to set locality", K(ret));
    } else if (OB_FAIL(parse_and_set_create_tenant_new_locality_options(
            schema_guard, new_tenant_schema, resource_pool_names, zones_in_pool, zone_region_list))) {
      LOG_WARN("fail to parse and set new locality option", K(ret));
    } else if (ALTER_LOCALITY_INVALID == alter_locality_type) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter tenant locality when previous operation is in progress");
      LOG_WARN("alter tenant locality not allowed", K(ret), K(orig_tenant_schema));
    } else if (ROLLBACK_LOCALITY == alter_locality_type) {
    } else if (TO_NEW_LOCALITY == alter_locality_type) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected alter locality type", K(ret), K(alter_locality_type));
    }
    if (OB_SUCC(ret)) {
      common::ObArray<share::ObResourcePoolName> pool_names;
      if (OB_UNLIKELY(NULL == unit_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
      } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST)) {
        ret = get_pools(arg.pool_list_, pool_names);
      } else {
        ret = unit_mgr_->get_pool_names_of_tenant(new_tenant_schema.get_tenant_id(), pool_names);
      }
    }
  } else {} // locality do not changed, do nothing
  LOG_DEBUG("set new tenant options", K(arg), K(new_tenant_schema), K(orig_tenant_schema));
  return ret;
}

int ObTenantDDLService::set_raw_tenant_options(
    const ObModifyTenantArg &arg,
    ObTenantSchema &new_tenant_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema &alter_tenant_schema = arg.tenant_schema_;
  //replace alter options
  for (int32_t i = ObModifyTenantArg::REPLICA_NUM;
       ret == OB_SUCCESS && i < ObModifyTenantArg::MAX_OPTION; ++i) {
    if (arg.alter_option_bitset_.has_member(i)) {
      switch (i) {
        case ObModifyTenantArg::REPLICA_NUM: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify replica num is not supported!", K(i), K(ret));
          break;
        }
        case ObModifyTenantArg::CHARSET_TYPE: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify replica num is not supported!", K(i), K(ret));
          break;
        }
        case ObModifyTenantArg::COLLATION_TYPE: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify replica num is not supported!", K(i), K(ret));
          break;
        }
        case ObModifyTenantArg::PRIMARY_ZONE: {
          new_tenant_schema.set_primary_zone(alter_tenant_schema.get_primary_zone());
          break;
        }
        case ObModifyTenantArg::ZONE_LIST: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify zone list is not supported!", K(i), K(ret));
          break;
        }
        case ObModifyTenantArg::RESOURCE_POOL_LIST: {
          break;
        }
        case ObModifyTenantArg::READ_ONLY: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("modify tenant readonly option not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify tenant readonly option");
          break;
        }
        case ObModifyTenantArg::COMMENT: {
          new_tenant_schema.set_comment(alter_tenant_schema.get_comment());
          break;
        }
        case ObModifyTenantArg::LOCALITY: {
          // locality change is processed in try_modify_tenant_locality, skip
          break;
        }
        case ObModifyTenantArg::DEFAULT_TABLEGROUP: {
          if (OB_FAIL(new_tenant_schema.set_default_tablegroup_name(
              alter_tenant_schema.get_default_tablegroup_name()))) {
            LOG_WARN("failed to set default tablegroup name", K(ret));
          } else if (OB_FAIL(ddl_service_->set_default_tablegroup_id(new_tenant_schema))) {
            LOG_WARN("failed to set default tablegroup id", K(ret));
          }
          break;
        }
        case ObModifyTenantArg::FORCE_LOCALITY: {
          // do nothing
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unknown option!", K(i));
        }
      }
    }
  }
  return ret;
}

// What we need to retrieve is the zone of all resource_pools under the tenant's name,
// not just the zone_list of the tenant itself
int ObTenantDDLService::get_new_tenant_pool_zone_list(
    const ObModifyTenantArg &arg,
    const share::schema::ObTenantSchema &tenant_schema,
    common::ObIArray<share::ObResourcePoolName> &resource_pool_names,
    common::ObIArray<common::ObZone> &zones_in_pool,
    common::ObIArray<share::schema::ObZoneRegion> &zone_region_list)
{
  int ret = OB_SUCCESS;
  zones_in_pool.reset();
  zone_region_list.reset();
  if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST)) {
    if (OB_FAIL(get_pools(arg.pool_list_, resource_pool_names))) {
      LOG_WARN("fail to get pools", K(ret), "pool_list", arg.pool_list_);
    } else {} // got pool names, ok
  } else {
    uint64_t tenant_id = tenant_schema.get_tenant_id();
    if (OB_UNLIKELY(NULL == unit_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
    } else if (OB_FAIL(unit_mgr_->get_pool_names_of_tenant(tenant_id, resource_pool_names))) {
      LOG_WARN("fail to get pools of tenant", K(ret));
    } else {} // got pool names, ok
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_zones_of_pools(resource_pool_names, zones_in_pool))) {
    LOG_WARN("fail to get zones of pools", K(ret));
  } else if (OB_FAIL(construct_zone_region_list(zone_region_list, zones_in_pool))) {
    LOG_WARN("fail to construct zone region list", K(ret));
  } else {} // no more to do
  return ret;
}

int ObTenantDDLService::get_zones_of_pools(
    const common::ObIArray<share::ObResourcePoolName> &resource_pool_names,
    common::ObIArray<common::ObZone> &zones_in_pool)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> temp_zones;
  zones_in_pool.reset();
  if (OB_UNLIKELY(resource_pool_names.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "resource pool count", resource_pool_names.count());
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_zones_of_pools(resource_pool_names, temp_zones))) {
    LOG_WARN("get zones of pools failed", K(ret), K(resource_pool_names));
  } else if (temp_zones.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty zone array", K(ret));
  } else {
    lib::ob_sort(temp_zones.begin(), temp_zones.end());
    FOREACH_X(zone, temp_zones, OB_SUCC(ret)) {
      if (OB_ISNULL(zone)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone is null", K(ret));
      } else if (0 == zones_in_pool.count()
          || zones_in_pool.at(zones_in_pool.count() - 1) != *zone) {
        if (OB_FAIL(zones_in_pool.push_back(*zone))) {
          LOG_WARN("fail to push back", K(ret));
        } else {}
      } else {} // duplicated zone, no need to push into
    }
  }
  return ret;
}


// not used
// When alter tenant, tenant option and sys variable are both set to readonly,
// the current implementation is based on sys variable
int ObTenantDDLService::update_sys_variables(const common::ObIArray<obrpc::ObSysVarIdValue> &sys_var_list,
    const share::schema::ObSysVariableSchema &orig_sys_variable,
    share::schema::ObSysVariableSchema &new_sys_variable,
    bool &value_changed)
{
  int ret = OB_SUCCESS;
  bool found = false;

  value_changed = false;
  if (!sys_var_list.empty()) {
    const int64_t set_sys_var_count = sys_var_list.count();
    const ObSysVarSchema *sysvar = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < set_sys_var_count; ++i) {
      const obrpc::ObSysVarIdValue &sysvar_value = sys_var_list.at(i);
      /* look ahead to find same variable, if found, jump this action.
         After doing so, only the rightmost set action can be accepted. */
      found = false;
      for (int64_t j = i + 1; OB_SUCC(ret) && j < set_sys_var_count && (!found); ++j) {
        const obrpc::ObSysVarIdValue &tmp_var = sys_var_list.at(j);
        if (sysvar_value.sys_id_ == tmp_var.sys_id_) {
          found = true;
        }
      }
      if (OB_SUCC(ret) && !found) {
        if (OB_FAIL(orig_sys_variable.get_sysvar_schema(sysvar_value.sys_id_, sysvar))) {
          LOG_WARN("failed to get sysvar schema", K(sysvar_value), K(ret));
        } else if (OB_ISNULL(sysvar)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sysvar is null", K(sysvar_value), K(ret));
        } else {
          ObSysVarSchema new_sysvar;
          if (OB_FAIL(new_sysvar.assign(*sysvar))) {
            LOG_WARN("fail to assign sys var schema", KR(ret), KPC(sysvar));
          } else if (SYS_VAR_OB_COMPATIBILITY_MODE
              == ObSysVarFactory::find_sys_var_id_by_name(new_sysvar.get_name())) {
            ret = OB_OP_NOT_ALLOW;
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "change tenant compatibility mode");
          } else if (new_sysvar.is_read_only()) {
            ret = OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR;
            LOG_USER_ERROR(OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR,
                new_sysvar.get_name().length(),
                new_sysvar.get_name().ptr(),
                (int)strlen("read only"),
                "read only");
          } else if (new_sysvar.get_value() != sysvar_value.value_) {
            value_changed = true;
            if (OB_FAIL(new_sysvar.set_value(sysvar_value.value_))) {
              LOG_WARN("failed to set_value", K(ret));
            } else if (OB_FAIL(new_sys_variable.add_sysvar_schema(new_sysvar))) {
              LOG_WARN("failed to add sysvar schema", K(ret));
            } else {
              LOG_DEBUG("succ to update sys value", K(sysvar_value));
              sysvar = NULL;
            }
          }
        }
      }
    }
  }
  return ret;
}

/* long_pool_name_list and short_pool_name_list has sorted
 * in parameter condition:
 *   The length of long_pool_name_list is 1 larger than the length of short_pool_name_list
 * This function has two functions:
 *   1 check whether long_pool_name_list is only one more resource_pool_name than short_pool_name_list
 *   2 Put this extra resource_pool_name into the diff_pools array.
 */
int ObTenantDDLService::cal_resource_pool_list_diff(
    const common::ObIArray<ObResourcePoolName> &long_pool_name_list,
    const common::ObIArray<ObResourcePoolName> &short_pool_name_list,
    common::ObIArray<ObResourcePoolName> &diff_pools)
{
  int ret = OB_SUCCESS;
  if (long_pool_name_list.count() != short_pool_name_list.count() + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(long_pool_name_list), K(short_pool_name_list));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
  } else {
    diff_pools.reset();
    int64_t index = 0;
    for (; OB_SUCC(ret) && index < short_pool_name_list.count(); ++index) {
      if (short_pool_name_list.at(index) != long_pool_name_list.at(index)) {
        if (OB_FAIL(diff_pools.push_back(long_pool_name_list.at(index)))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          break; // got it, exit loop
        }
      } else {} // still the same, go on next
    }
    if (OB_FAIL(ret)) {
    } else if (index >= short_pool_name_list.count()) {
      // The pool of diff is the last element of long_pool_name_list
      if (index >= long_pool_name_list.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid resource pool list", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
      } else if (OB_FAIL(diff_pools.push_back(long_pool_name_list.at(index)))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    } else {
      // The pool of diff is not the last element of long_pool_name_list. The diff has been found in the previous for loop.
      // It is necessary to further check whether short_pool_name_list and long_pool_name_list are consistent after index.
      for (; OB_SUCC(ret) && index < short_pool_name_list.count(); ++index) {
        if (index + 1 >= long_pool_name_list.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid resource pool list", K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
        } else if (short_pool_name_list.at(index) != long_pool_name_list.at(index + 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid resource pool list", K(ret), K(short_pool_name_list), K(long_pool_name_list));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
        } else {} // go on next
      }
    }
  }
  return ret;
}

int ObTenantDDLService::check_normal_tenant_revoke_pools_permitted(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
    const share::schema::ObTenantSchema &tenant_schema,
    bool &is_permitted)
{
  int ret = OB_SUCCESS;
  is_permitted = true;
  common::ObArray<common::ObZone> zone_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
    LOG_WARN("fail to get zones of pools", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count() && is_permitted; ++i) {
      const common::ObZone &zone = zone_list.at(i);
      int64_t total_unit_num = 0;
      int64_t full_unit_num = 0;
      int64_t logonly_unit_num = 0;
      bool enough = false;
      if (OB_FAIL(unit_mgr_->get_zone_pools_unit_num(
              zone, new_pool_name_list, total_unit_num, full_unit_num, logonly_unit_num))) {
        LOG_WARN("fail to get pools unit num", K(ret));
      } else if (total_unit_num != full_unit_num + logonly_unit_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit num value not match", K(ret),
                 K(total_unit_num), K(full_unit_num), K(logonly_unit_num));
      } else if (!tenant_schema.get_previous_locality_str().empty()) {
        is_permitted = false;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                       "revoking resource pools when tenant in locality modification");
      } else if (OB_FAIL(unit_mgr_->check_schema_zone_unit_enough(
              zone, total_unit_num, full_unit_num, logonly_unit_num,
              tenant_schema, schema_guard, enough))) {
        LOG_WARN("fail to check schema zone unit enough", K(ret));
      } else if (!enough) {
        is_permitted = false;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                       "revoking resource pools with tenant locality on");
      } else { /* good */ }
    }
  }
  return ret;
}

int ObTenantDDLService::check_revoke_pools_permitted(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
    const share::schema::ObTenantSchema &tenant_schema,
    bool &is_permitted)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", K(ret));
  } else {
    if (OB_FAIL(check_normal_tenant_revoke_pools_permitted(
            schema_guard, new_pool_name_list, tenant_schema, is_permitted))) {
      LOG_WARN("fail to check normal tenant revoke pools permitted", K(ret));
    }
  }
  return ret;
}


/* Modify the internal table related to the resource pool, and calculate the transformation of
 * the resource pool list of the alter tenant at the same time. Currently, only one is allowed to be added,
 * one resource pool is reduced or the resource pool remains unchanged.
 * input:
 *   tenant_id:       tenant_id corresponding to alter tenant
 *   new_pool_list:   The new resource pool list passed in by alter tenant
 * output:
 *   grant:           subtract resource pool: false; add resource pool: true
 *   diff_pools:      the diff from newresource pool list and old resource pool list.
 */
int ObTenantDDLService::modify_and_cal_resource_pool_diff(
    common::ObMySQLTransaction &trans,
    common::ObIArray<uint64_t> &new_ug_id_array,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTenantSchema &new_tenant_schema,
    const common::ObIArray<common::ObString> &new_pool_list,
    bool &grant,
    common::ObIArray<ObResourcePoolName> &diff_pools)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  common::ObArray<ObResourcePoolName> new_pool_name_list;
  common::ObArray<ObResourcePoolName> old_pool_name_list;
  const uint64_t tenant_id = new_tenant_schema.get_tenant_id();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)
      || OB_UNLIKELY(new_pool_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(new_pool_list));
  } else if (OB_FAIL(unit_mgr_->get_pool_names_of_tenant(tenant_id, old_pool_name_list))) {
    LOG_WARN("fail to get pool names of tenant", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get compat mode", K(ret));
  } else if (OB_UNLIKELY(old_pool_name_list.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old pool name list null", K(ret), K(old_pool_name_list));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_pool_list.count(); ++i) {
      if (OB_FAIL(new_pool_name_list.push_back(new_pool_list.at(i).ptr()))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(new_pool_name_list.begin(), new_pool_name_list.end());
      lib::ob_sort(old_pool_name_list.begin(), old_pool_name_list.end());
      bool is_permitted = false;
      if (new_pool_name_list.count() == old_pool_name_list.count() + 1) {
        grant = true;
        if (OB_FAIL(cal_resource_pool_list_diff(
                new_pool_name_list, old_pool_name_list, diff_pools))) {
          LOG_WARN("fail to cal resource pool list diff", K(ret));
        } else if (OB_FAIL(unit_mgr_->grant_pools(
                trans, new_ug_id_array, compat_mode, diff_pools, tenant_id,
                false/*is_bootstrap*/, true/*check_data_version*/))) {
          LOG_WARN("fail to grant pools", K(ret));
        }
      } else if (new_pool_name_list.count() + 1 == old_pool_name_list.count()) {
        grant = false;
        if (OB_FAIL(cal_resource_pool_list_diff(
                old_pool_name_list, new_pool_name_list, diff_pools))) {
          LOG_WARN("fail to cal resource pool list diff", K(ret));
        } else if (OB_FAIL(check_revoke_pools_permitted(
                schema_guard, new_pool_name_list, new_tenant_schema, is_permitted))) {
          LOG_WARN("fail to check revoke pools permitted", K(ret));
        } else if (!is_permitted) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("revoking resource pools is not allowed", K(ret), K(diff_pools));
        } else if (OB_FAIL(unit_mgr_->revoke_pools(
                trans, new_ug_id_array, diff_pools, tenant_id))) {
          LOG_WARN("fail to revoke pools", K(ret));
        } else {} // no more to do
      } else if (new_pool_name_list.count() == old_pool_name_list.count()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < new_pool_name_list.count(); ++i) {
          if (new_pool_name_list.at(i) != old_pool_name_list.at(i)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret), K(new_pool_name_list), K(old_pool_name_list));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(new_pool_name_list), K(old_pool_name_list));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool list");
      }
    }
    LOG_INFO("cal resource pool list result",
             K(new_pool_name_list),
             K(old_pool_name_list),
             K(diff_pools),
             K(grant));
  }
  return ret;
}

int ObTenantDDLService::modify_tenant_inner_phase(const ObModifyTenantArg &arg, const ObTenantSchema *orig_tenant_schema, ObSchemaGetterGuard &schema_guard, bool is_restore)
{
  int ret = OB_SUCCESS;
  if (0 != arg.sys_var_list_.count()) {
    // modify system variable
    const ObSysVariableSchema *orig_sys_variable = NULL;
    const uint64_t tenant_id = orig_tenant_schema->get_tenant_id();
    int64_t schema_version = OB_INVALID_VERSION;
    ObDDLSQLTransaction trans(schema_service_);
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    bool value_changed = false;
    if (is_restore && is_user_tenant(tenant_id)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ddl operation is not allowed in standby cluster", K(ret));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "ddl operation in standby cluster");
    } else if (OB_FAIL(schema_guard.get_sys_variable_schema(
                       orig_tenant_schema->get_tenant_id(), orig_sys_variable))) {
      LOG_WARN("get sys variable schema failed", K(ret));
    } else if (OB_ISNULL(orig_sys_variable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable schema is null", K(ret));
    } else {
      ObSysVariableSchema new_sys_variable;
      if (OB_FAIL(new_sys_variable.assign(*orig_sys_variable))) {
        LOG_WARN("fail to assign sys variable schema", KR(ret), KPC(orig_sys_variable));
      } else if (FALSE_IT(new_sys_variable.reset_sysvars())) {
      } else if (OB_FAIL(update_sys_variables(arg.sys_var_list_, *orig_sys_variable, new_sys_variable, value_changed))) {
        LOG_WARN("failed to update_sys_variables", K(ret));
      } else if (value_changed == true) {
        int64_t refreshed_schema_version = 0;
        if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
          LOG_WARN("failed to get tenant schema version", KR(ret), K(tenant_id));
        } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
          LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
        } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id, schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
        } else {
          const ObSchemaOperationType operation_type = OB_DDL_ALTER_SYS_VAR;
          if (OB_FAIL(ddl_operator.replace_sys_variable(new_sys_variable, schema_version, trans, operation_type, &arg.ddl_stmt_str_))) {
            LOG_WARN("failed to replace sys variable", K(ret), K(new_sys_variable));
          }
        }
        if (trans.is_started()) {
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
            LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
            ret = (OB_SUCC(ret)) ? temp_ret : ret;
          }
        }
        // publish schema
        if (OB_SUCC(ret) && OB_FAIL(publish_schema(tenant_id))) {
          LOG_WARN("publish schema failed, ", K(ret));
        }
      }
    }
  } else if (!arg.alter_option_bitset_.is_empty()) {
    // modify tenant option
    const uint64_t tenant_id = orig_tenant_schema->get_tenant_id();
    bool grant = true;
    ObArray<ObResourcePoolName> diff_pools;
    AlterLocalityOp alter_locality_op = ALTER_LOCALITY_OP_INVALID;
    ObTenantSchema new_tenant_schema;
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);

    if (OB_FAIL(new_tenant_schema.assign(*orig_tenant_schema))) {
      LOG_WARN("fail to assign tenant schema", KR(ret));
    } else if (is_meta_tenant(tenant_id)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not allowed to modify meta tenant's options manually", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_new_tenant_options(schema_guard, arg, new_tenant_schema,
        *orig_tenant_schema, alter_locality_op))) {
      LOG_WARN("failed to set new tenant options", K(ret));
    }
    // modify tenant option
    if (OB_SUCC(ret)) {
      ObDDLSQLTransaction trans(schema_service_);
      int64_t refreshed_schema_version = 0;
      common::ObArray<uint64_t> new_ug_id_array;
      if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("failed to get tenant schema version", KR(ret));
      } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
      } else if (arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST)
            && OB_FAIL(modify_and_cal_resource_pool_diff(
                trans, new_ug_id_array, schema_guard,
                new_tenant_schema, arg.pool_list_, grant, diff_pools))) {
        LOG_WARN("fail to grant_pools", K(ret));
      }

      if (FAILEDx(ddl_operator.alter_tenant(new_tenant_schema, trans, &arg.ddl_stmt_str_))) {
        LOG_WARN("failed to alter tenant", K(ret));
      }

      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
      }
      // publish schema
      if (OB_SUCC(ret) && OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
        LOG_WARN("publish schema failed, ", K(ret));
      }

      // When the new and old resource pool lists are consistent, no diff is generated, diff_pools is empty,
      // and there is no need to call the following function
      if (OB_SUCC(ret)
          && arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::RESOURCE_POOL_LIST)
          && diff_pools.count() > 0) {
        if (OB_FAIL(unit_mgr_->load())) {
          LOG_WARN("unit_manager reload failed", K(ret));
        }
      }
    }
  } else if (!arg.new_tenant_name_.empty()) {
    // rename tenant
    const uint64_t tenant_id = orig_tenant_schema->get_tenant_id();
    const ObString new_tenant_name = arg.new_tenant_name_;
    ObTenantSchema new_tenant_schema;
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObDDLSQLTransaction trans(schema_service_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(new_tenant_schema.assign(*orig_tenant_schema))) {
      LOG_WARN("fail to assign tenant schema", KR(ret));
    } else if (is_meta_tenant(tenant_id)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not allowed to modify meta tenant's options manually", KR(ret), K(tenant_id));
    } else if (orig_tenant_schema->is_restore()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("rename tenant while tenant is in physical restore status is not allowed",
               KR(ret), KPC(orig_tenant_schema));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "rename tenant while tenant is in physical restore status is");
    } else if (orig_tenant_schema->get_tenant_id() <= OB_MAX_RESERVED_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rename special tenant not supported",
               K(ret), K(orig_tenant_schema->get_tenant_id()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "rename special tenant");
    } else if (NULL != schema_guard.get_tenant_info(new_tenant_name)) {
      ret = OB_TENANT_EXIST;
      LOG_USER_ERROR(OB_TENANT_EXIST, to_cstring(new_tenant_name));
      LOG_WARN("tenant already exists", K(ret), K(new_tenant_name));
    } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret));
    } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(new_tenant_schema.set_tenant_name(new_tenant_name))) {
      LOG_WARN("failed to rename tenant", K(ret),
                                          K(new_tenant_name),
                                          K(new_tenant_schema));
    } else if (OB_FAIL(ddl_operator.rename_tenant(new_tenant_schema,
                                                  trans,
                                                  &arg.ddl_stmt_str_))) {
      LOG_WARN("failed to rename tenant", K(ret), K(new_tenant_schema));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    // publish schema
    if (OB_SUCC(ret) && OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("publish schema failed, ", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable or tenant option should changed", K(ret), K(arg));
  }
  return ret;
}

int ObTenantDDLService::init_system_variables(
    const ObCreateTenantArg &arg,
    const ObTenantSchema &tenant_schema,
    ObSysVariableSchema &sys_variable_schema)
{
  int ret = OB_SUCCESS;
  const int64_t params_capacity = ObSysVarFactory::ALL_SYS_VARS_COUNT;
  int64_t var_amount = ObSysVariables::get_amount();
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
  ObPtrGuard<ObSysParam, ObSysVarFactory::ALL_SYS_VARS_COUNT> sys_params_guard(alloc);
  sys_variable_schema.reset();
  sys_variable_schema.set_tenant_id(tenant_id);
  ObSysParam *sys_params = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)
             || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(schema_service), KP_(sql_proxy));
  } else if (OB_FAIL(sys_params_guard.init())) {
    LOG_WARN("alloc sys parameters failed", KR(ret));
  } else if (FALSE_IT(sys_params = sys_params_guard.ptr())) {
  } else if (OB_ISNULL(sys_params) || OB_UNLIKELY(var_amount > params_capacity)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(sys_params), K(params_capacity), K(var_amount));
  } else {
    HEAP_VARS_2((char[OB_MAX_SYS_PARAM_VALUE_LENGTH], val_buf),
                (char[OB_MAX_SYS_PARAM_VALUE_LENGTH], version_buf)) {
      // name_case_mode
      if (OB_NAME_CASE_INVALID == arg.name_case_mode_) {
        sys_variable_schema.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
      } else {
        sys_variable_schema.set_name_case_mode(arg.name_case_mode_);
      }

      // init default values
      for (int64_t i = 0; OB_SUCC(ret) && i < var_amount; ++i) {
        if (OB_FAIL(sys_params[i].init(tenant_id,
                                       ObSysVariables::get_name(i),
                                       ObSysVariables::get_type(i),
                                       ObSysVariables::get_value(i),
                                       ObSysVariables::get_min(i),
                                       ObSysVariables::get_max(i),
                                       ObSysVariables::get_info(i),
                                       ObSysVariables::get_flags(i)))) {
          LOG_WARN("fail to init param", KR(ret), K(tenant_id), K(i));
        }
      }

      int64_t set_sys_var_count = arg.sys_var_list_.count();
      bool use_default_parallel_servers_target = true;
      bool explicit_set_compatibility_version = false;
      bool explicit_set_security_version = false;
      bool read_only = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < set_sys_var_count; ++j) {
        ObSysVarIdValue sys_var;
        if (OB_FAIL(arg.sys_var_list_.at(j, sys_var))) {
          LOG_WARN("failed to get sys var", K(j), K(ret));
        } else {
          const ObString &new_value = sys_var.value_;
          SET_TENANT_VARIABLE(sys_var.sys_id_, new_value);
          // sync tenant schema
          if (SYS_VAR_READ_ONLY == sys_var.sys_id_) {
            if (is_user_tenant(tenant_id)) {
              read_only = (0 == sys_var.value_.compare("1"));
            }
          } else if (SYS_VAR_PARALLEL_SERVERS_TARGET == sys_var.sys_id_) {
            use_default_parallel_servers_target = false;
          } else if (SYS_VAR_OB_COMPATIBILITY_VERSION == sys_var.sys_id_) {
            explicit_set_compatibility_version = true;
          } else if (SYS_VAR_OB_SECURITY_VERSION == sys_var.sys_id_) {
            explicit_set_security_version = true;
          }
        }
      } // end for

      // For read_only, its priority: sys variable > tenant option.
      if (OB_SUCC(ret)) {
        ObString read_only_value = read_only ? "1" : "0";
        SET_TENANT_VARIABLE(SYS_VAR_READ_ONLY, read_only_value);
      }

      // For compatibility_mode, its priority: sys variable > tenant option.
      if (OB_SUCC(ret)) {
        ObString compat_mode_value = tenant_schema.is_oracle_tenant() ? "1" : "0";
        SET_TENANT_VARIABLE(SYS_VAR_OB_COMPATIBILITY_MODE, compat_mode_value);
      }

      if (OB_SUCC(ret)) {
        char version[common::OB_CLUSTER_VERSION_LENGTH] = {0};
        int64_t len = ObClusterVersion::print_version_str(
                  version, common::OB_CLUSTER_VERSION_LENGTH, DATA_CURRENT_VERSION);
        SET_TENANT_VARIABLE(SYS_VAR_PRIVILEGE_FEATURES_ENABLE, ObString(len, version));
      }

      if (OB_SUCC(ret)) {
        ObString enable = "1";
        SET_TENANT_VARIABLE(SYS_VAR__ENABLE_MYSQL_PL_PRIV_CHECK, enable);
      }

      // If the user does not specify parallel_servers_target when creating tenant,
      // then calculate a default value based on cpu_count.
      // Considering that a tenant may have multiple resource pools, it is currently rudely considered
      // that the units in the pool are of the same structure, and directly take the unit config of the first resource pool
      // WARNING: If the unit is not structured, the number of threads allocated by default may be too large/too small
      int64_t default_px_thread_count = 0;
      if (OB_SUCC(ret) && (use_default_parallel_servers_target)) {
        HEAP_VAR(ObUnitConfig, unit_config) {
          if (OB_SYS_TENANT_ID == sys_variable_schema.get_tenant_id()) {
            // When creating a system tenant, the default value of px_thread_count is related to
            // default sys tenant min cpu
            const int64_t sys_default_min_cpu =
                static_cast<int64_t>(GCONF.get_sys_tenant_default_min_cpu());
            default_px_thread_count = ObTenantCpuShare::calc_px_pool_share(
                sys_variable_schema.get_tenant_id(), sys_default_min_cpu);
          } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
          } else if (arg.pool_list_.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant should have at least one pool", K(ret));
          } else if (OB_FAIL(unit_mgr_->get_unit_config_by_pool_name(
                      arg.pool_list_.at(0), unit_config))) {
            LOG_WARN("fail to get unit config", K(ret));
          } else {
            int64_t cpu_count = static_cast<int64_t>(unit_config.unit_resource().min_cpu());
            default_px_thread_count = ObTenantCpuShare::calc_px_pool_share(
                sys_variable_schema.get_tenant_id(), cpu_count);
          }
        }
      }

      if (OB_SUCC(ret) && use_default_parallel_servers_target && default_px_thread_count > 0) {
        // target cannot be less than 3, otherwise any px query will not come in
        int64_t default_px_servers_target = std::max(3L, static_cast<int64_t>(default_px_thread_count));
        VAR_INT_TO_STRING(val_buf, default_px_servers_target);
        SET_TENANT_VARIABLE(SYS_VAR_PARALLEL_SERVERS_TARGET, val_buf);
      }

      VAR_UINT_TO_STRING(version_buf, CLUSTER_CURRENT_VERSION);
      if (OB_SUCC(ret) && !(is_user_tenant(tenant_id) && explicit_set_compatibility_version)) {
        SET_TENANT_VARIABLE(SYS_VAR_OB_COMPATIBILITY_VERSION, version_buf);
      }

      if (OB_SUCC(ret) && !(is_user_tenant(tenant_id) && explicit_set_security_version)) {
        SET_TENANT_VARIABLE(SYS_VAR_OB_SECURITY_VERSION, version_buf);
      }

      if (FAILEDx(update_mysql_tenant_sys_var(
          tenant_schema, sys_variable_schema, sys_params, params_capacity))) {
        LOG_WARN("failed to update_mysql_tenant_sys_var",
                 KR(ret), K(tenant_schema), K(sys_variable_schema));
      } else if (OB_FAIL(update_oracle_tenant_sys_var(
          tenant_schema, sys_variable_schema, sys_params, params_capacity))) {
        LOG_WARN("failed to update_oracle_tenant_sys_var",
                 KR(ret), K(tenant_schema), K(sys_variable_schema));
      } else if (OB_FAIL(update_special_tenant_sys_var(
                 sys_variable_schema, sys_params, params_capacity))) {
        LOG_WARN("failed to update_special_tenant_sys_var", K(ret), K(sys_variable_schema));
      }

      // set sys_variable
      if (OB_SUCC(ret)) {
        ObSysVarSchema sysvar_schema;
        for (int64_t i = 0; OB_SUCC(ret) && i < var_amount; i++) {
          sysvar_schema.reset();
          if (OB_FAIL(ObSchemaUtils::convert_sys_param_to_sysvar_schema(sys_params[i], sysvar_schema))) {
            LOG_WARN("convert to sysvar schema failed", K(ret));
          } else if (OB_FAIL(sys_variable_schema.add_sysvar_schema(sysvar_schema))) {
            LOG_WARN("add system variable failed", K(ret));
          }
        } //end for
      }
    } // end HEAP_VAR
  }
  return ret;
}

int ObTenantDDLService::update_mysql_tenant_sys_var(
    const ObTenantSchema &tenant_schema,
    const ObSysVariableSchema &sys_variable_schema,
    ObSysParam *sys_params,
    int64_t params_capacity)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sys_variable_schema.get_tenant_id();
  if (OB_ISNULL(sys_params) || OB_UNLIKELY(params_capacity < ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(sys_params), K(params_capacity));
  } else if (tenant_schema.is_mysql_tenant()) {
    HEAP_VAR(char[OB_MAX_SYS_PARAM_VALUE_LENGTH], val_buf) {
      // If it is a tenant in mysql mode, you need to consider setting the charset and collation
      // corresponding to the tenant to sys var
      VAR_INT_TO_STRING(val_buf, tenant_schema.get_collation_type());
      // set collation and char set
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_DATABASE, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_SERVER, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_DATABASE, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_SERVER, val_buf);
    } // end HEAP_VAR
  }
  return ret;
}

int ObTenantDDLService::update_oracle_tenant_sys_var(
    const ObTenantSchema &tenant_schema,
    const ObSysVariableSchema &sys_variable_schema,
    ObSysParam *sys_params,
    int64_t params_capacity)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sys_variable_schema.get_tenant_id();
  if (OB_ISNULL(sys_params) || OB_UNLIKELY(params_capacity < ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(sys_params), K(params_capacity));
  } else if (tenant_schema.is_oracle_tenant()) {
    HEAP_VAR(char[OB_MAX_SYS_PARAM_VALUE_LENGTH], val_buf) {
      // For oracle tenants, the collation of sys variable and tenant_option is set to binary by default.
      // set group_concat_max_len = 4000
      // set autocommit = off
      // When setting oracle variables, try to keep the format consistent
      VAR_INT_TO_STRING(val_buf, OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE);
      SET_TENANT_VARIABLE(SYS_VAR_GROUP_CONCAT_MAX_LEN, val_buf);

      SET_TENANT_VARIABLE(SYS_VAR_AUTOCOMMIT, "0");

      VAR_INT_TO_STRING(val_buf, tenant_schema.get_collation_type());
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_DATABASE, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_SERVER, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_DATABASE, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_SERVER, val_buf);

      // Here is the collation of the connection, OB currently only supports the client as utf8mb4
      VAR_INT_TO_STRING(val_buf, CS_TYPE_UTF8MB4_BIN);
      SET_TENANT_VARIABLE(SYS_VAR_COLLATION_CONNECTION, val_buf);
      SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_CONNECTION, val_buf);

      /*
       * In Oracle mode, we are only compatible with binary mode, so collate can only end with _bin
       */
      if (ObCharset::is_bin_sort(tenant_schema.get_collation_type())) {
        VAR_INT_TO_STRING(val_buf, tenant_schema.get_collation_type());
        SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_SERVER, val_buf);
        SET_TENANT_VARIABLE(SYS_VAR_CHARACTER_SET_DATABASE, val_buf);
        ObCharsetType charset_type = ObCharset::charset_type_by_coll(tenant_schema.get_collation_type());
        OZ(databuff_printf(val_buf, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%s",
                           ObCharset::get_oracle_charset_name_by_charset_type(charset_type)));
        SET_TENANT_VARIABLE(SYS_VAR_NLS_CHARACTERSET, val_buf);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant collation set error", K(ret), K(tenant_schema.get_collation_type()));
      }

      // update oracle tenant schema
      if (OB_SUCC(ret)) {
        if (OB_FAIL(databuff_printf(sys_params[SYS_VAR_SQL_MODE].value_,
            sizeof(sys_params[SYS_VAR_SQL_MODE].value_), "%llu", DEFAULT_ORACLE_MODE))) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("set oracle tenant default sql mode failed",  K(ret));
        }
      }
    } // end HEAP_VAR
  }
  return ret;
}

// The value of certain system variables of the system/meta tenant
int ObTenantDDLService::update_special_tenant_sys_var(
    const ObSysVariableSchema &sys_variable_schema,
    ObSysParam *sys_params,
    int64_t params_capacity)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = sys_variable_schema.get_tenant_id();
  if (OB_ISNULL(sys_params) || OB_UNLIKELY(params_capacity < ObSysVarFactory::ALL_SYS_VARS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(sys_params), K(params_capacity));
  } else {
    HEAP_VAR(char[OB_MAX_SYS_PARAM_VALUE_LENGTH], val_buf) {
      if (is_sys_tenant(tenant_id)) {
        VAR_INT_TO_STRING(val_buf, sys_variable_schema.get_name_case_mode());
        SET_TENANT_VARIABLE(SYS_VAR_LOWER_CASE_TABLE_NAMES, val_buf);

        OZ(databuff_printf(val_buf, OB_MAX_SYS_PARAM_VALUE_LENGTH, "%s", OB_SYS_HOST_NAME));
        SET_TENANT_VARIABLE(SYS_VAR_OB_TCP_INVITED_NODES, val_buf);
      } else if (is_meta_tenant(tenant_id)) {
        ObString compatibility_mode("0");
        SET_TENANT_VARIABLE(SYS_VAR_OB_COMPATIBILITY_MODE, compatibility_mode);
      }
    } // end HEAP_VAR
  }
  return ret;
}


int ObTenantDDLService::get_pools(const ObIArray<ObString> &pool_strs,
                            ObIArray<ObResourcePoolName> &pools)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pool_strs.count(); ++i) {
    ObResourcePoolName pool;
    if (OB_FAIL(pool.assign(pool_strs.at(i)))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(pools.push_back(pool))) {
      LOG_WARN("push_back failed", K(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::parse_and_set_create_tenant_new_locality_options(
    share::schema::ObSchemaGetterGuard &schema_guard,
    ObTenantSchema &schema,
    const common::ObIArray<share::ObResourcePoolName> &pools,
    const common::ObIArray<common::ObZone> &zone_list,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list)
{
  int ret = OB_SUCCESS;
  char locality_str[MAX_LOCALITY_LENGTH + 1];
  int64_t pos = 0;
  ObLocalityDistribution locality_dist;
  ObArray<ObUnitInfo> unit_infos;
  if (OB_FAIL(locality_dist.init())) {
    LOG_WARN("fail to init locality dist", K(ret));
  } else if (OB_FAIL(locality_dist.parse_locality(
              schema.get_locality_str(), zone_list, &zone_region_list))) {
    LOG_WARN("fail to parse locality", K(ret));
  } else if (OB_FAIL(locality_dist.output_normalized_locality(
              locality_str, MAX_LOCALITY_LENGTH, pos))) {
    LOG_WARN("fail to normalized locality", K(ret));
  } else if (OB_FAIL(schema.set_locality(locality_str))) {
    LOG_WARN("fail to set normalized locality back to schema", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_unit_infos(pools, unit_infos))) {
    LOG_WARN("fail to get unit infos", K(ret));
  } else if (OB_FAIL(set_schema_replica_num_options(schema, locality_dist, unit_infos))) {
    LOG_WARN("fail to set schema replica num options", K(ret));
  } else if (OB_FAIL(set_schema_zone_list(
              schema_guard, schema, zone_region_list))) {
    LOG_WARN("fail to set table schema zone list", K(ret));
  } else {} // no more to do
  LOG_DEBUG("parse and set new locality", K(ret), K(locality_str), K(schema),
            K(pools), K(zone_list), K(zone_region_list), K(unit_infos));
  return ret;
}

template<typename T>
int ObTenantDDLService::set_schema_zone_list(
    share::schema::ObSchemaGetterGuard &schema_guard,
    T &schema,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
    LOG_WARN("fail to get zone replica num arrary", K(ret));
  } else if (OB_FAIL(generate_zone_list_by_locality(
          zone_locality, zone_region_list, zone_list))) {
    LOG_WARN("fail to generate zone list by locality",
             K(ret), K(zone_locality), K(zone_region_list));
  } else if (OB_FAIL(schema.set_zone_list(zone_list))) {
    LOG_WARN("fail to set zone list", K(ret), K(zone_list));
  } else {} // no more to do
  return ret;
}

int ObTenantDDLService::generate_zone_list_by_locality(
    const ZoneLocalityIArray &zone_locality,
    const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
    common::ObArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  UNUSED(zone_region_list);
  common::ObArray<common::ObZone> tmp_zone_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
    const ObZoneReplicaAttrSet &zone_num_set = zone_locality.at(i);
    const ObIArray<common::ObZone> &zone_set = zone_num_set.zone_set_;
    if (OB_FAIL(append(tmp_zone_list, zone_set))) {
      LOG_WARN("fail to append zone set", K(ret));
    } else {} // ok, go on next
  }

  if (OB_SUCC(ret)) {
    lib::ob_sort(tmp_zone_list.begin(), tmp_zone_list.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_zone_list.count(); ++i) {
      common::ObZone &this_zone = tmp_zone_list.at(i);
      if (0 == zone_list.count() || zone_list.at(zone_list.count() - 1) != this_zone) {
        if (OB_FAIL(zone_list.push_back(this_zone))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      } else {} // duplicated zone, no need to push into.
    }
  }
  return ret;
}

template<typename SCHEMA>
int ObTenantDDLService::set_schema_replica_num_options(
    SCHEMA &schema,
    ObLocalityDistribution &locality_dist,
    ObIArray<ObUnitInfo> &unit_infos)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_replica_attr_array;
  if (OB_FAIL(locality_dist.get_zone_replica_attr_array(
          zone_replica_attr_array))) {
    LOG_WARN("fail to get zone region replica num array", K(ret));
  } else if (OB_FAIL(schema.set_zone_replica_attr_array(zone_replica_attr_array))) {
    LOG_WARN("fail to set zone replica num set", K(ret));
  } else {
    int64_t full_replica_num = 0;
    for (int64_t i = 0; i < zone_replica_attr_array.count(); ++i) {
      ObZoneReplicaNumSet &zone_replica_num_set = zone_replica_attr_array.at(i);
      full_replica_num += zone_replica_num_set.get_full_replica_num();
    }
    if (full_replica_num <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, should have at least one paxos replica");
      LOG_WARN("full replica num is zero", K(ret), K(full_replica_num), K(schema));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < zone_replica_attr_array.count() && OB_SUCC(ret); ++i) {
      ObZoneReplicaAttrSet &zone_replica_set = zone_replica_attr_array.at(i);
      if (zone_replica_set.zone_set_.count() > 1) {
        if (zone_replica_set.zone_set_.count() != zone_replica_set.get_paxos_replica_num()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, too many paxos replicas in multiple zones");
          LOG_WARN("too many paxos replicas in multi zone", K(ret));
        }
      } else if (zone_replica_set.get_full_replica_num() > 1
                 || zone_replica_set.get_logonly_replica_num() > 1
                 || zone_replica_set.get_encryption_logonly_replica_num() > 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality");
        LOG_WARN("one zone should only have one paxos replica", K(ret), K(zone_replica_set));
      } else if (zone_replica_set.get_full_replica_num() == 1
                 && (zone_replica_set.get_logonly_replica_num() == 1
                     || zone_replica_set.get_encryption_logonly_replica_num() == 1)) {
        bool find = false;
        for (int64_t j = 0; j < unit_infos.count() && OB_SUCC(ret); j++) {
          if (unit_infos.at(j).unit_.zone_ == zone_replica_set.zone_
              && REPLICA_TYPE_LOGONLY == unit_infos.at(j).unit_.replica_type_) {
            find = true;
            break;
          }
        } //end for unit_infos
        if (!find) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality");
          LOG_WARN("no logonly unit exist", K(ret), K(zone_replica_set));
        }
      }
    }
  }
  return ret;
}

int ObTenantDDLService::construct_zone_region_list(
    common::ObIArray<ObZoneRegion> &zone_region_list,
    const common::ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_region_list.reset();
  if (OB_UNLIKELY(NULL == zone_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("zone mgr is null", K(ret));
  } else {
    common::ObArray<share::ObZoneInfo> zone_infos;
    if (OB_FAIL(zone_mgr_->get_zone(zone_infos))) {
      LOG_WARN("fail to get zone", K(ret));
    } else {
      ObZoneRegion zone_region;
      for (int64_t i = 0; i < zone_infos.count() && OB_SUCC(ret); ++i) {
        zone_region.reset();
        share::ObZoneInfo &zone_info = zone_infos.at(i);
        if (OB_FAIL(zone_region.zone_.assign(zone_info.zone_.ptr()))) {
          LOG_WARN("fail to assign zone", K(ret));
        } else if (OB_FAIL(zone_region.region_.assign(zone_info.region_.info_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        } else if (OB_FAIL(zone_region.set_check_zone_type(zone_info.zone_type_.value_))) {
          LOG_WARN("fail to set check zone type", KR(ret));
        } else if (!has_exist_in_array(zone_list, zone_region.zone_)) {
          // this zone do not exist in my zone list, ignore it
        } else if (OB_FAIL(zone_region_list.push_back(zone_region))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
  }
  return ret;
}

int ObTenantDDLService::check_schema_zone_list(
    common::ObArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  lib::ob_sort(zone_list.begin(), zone_list.end());
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count() - 1; ++i) {
    if (zone_list.at(i) == zone_list.at(i+1)) {
      ret = OB_ZONE_DUPLICATED;
      LOG_USER_ERROR(OB_ZONE_DUPLICATED, to_cstring(zone_list.at(i)), to_cstring(zone_list));
      LOG_WARN("duplicate zone in zone list", K(zone_list), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      bool zone_exist = false;
      if (OB_FAIL(zone_mgr_->check_zone_exist(zone_list.at(i), zone_exist))) {
        LOG_WARN("check_zone_exist failed", "zone", zone_list.at(i), K(ret));
      } else if (!zone_exist) {
        ret = OB_ZONE_INFO_NOT_EXIST;
        LOG_USER_ERROR(OB_ZONE_INFO_NOT_EXIST, to_cstring(zone_list.at(i)));
        LOG_WARN("zone not exist", "zone", zone_list.at(i), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObTenantDDLService::try_drop_sys_ls_(const uint64_t meta_tenant_id,
                       common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (OB_UNLIKELY(!is_meta_tenant(meta_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not meta tenant", KR(ret), K(meta_tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    //check ls exist in status
    ObLSLifeAgentManager life_agent(*sql_proxy_);
    ObLSStatusOperator ls_status;
    ObLSStatusInfo sys_ls_info;
    if (OB_FAIL(ls_status.get_ls_status_info(meta_tenant_id, SYS_LS, sys_ls_info, trans))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("sys ls not exist, no need to drop", KR(ret), K(meta_tenant_id));
      } else {
        LOG_WARN("failed to get ls status info", KR(ret), K(meta_tenant_id));
      }
    } else if (OB_FAIL(life_agent.drop_ls_in_trans(meta_tenant_id, SYS_LS, share::NORMAL_SWITCHOVER_STATUS, trans))) {
      LOG_WARN("failed to drop ls in trans", KR(ret), K(meta_tenant_id));
    }
  }
  return ret;
}


int ObTenantDDLService::drop_resource_pool_pre(const uint64_t tenant_id,
                                         common::ObIArray<uint64_t> &drop_ug_id_array,
                                         ObIArray<ObResourcePoolName> &pool_names,
                                         ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (OB_FAIL(unit_mgr_->get_pool_names_of_tenant(tenant_id, pool_names))) {
    LOG_WARN("get_pool_names_of_tenant failed", K(tenant_id), KR(ret));
  } else if (OB_FAIL(unit_mgr_->revoke_pools(trans, drop_ug_id_array, pool_names, tenant_id))) {
    LOG_WARN("revoke_pools failed", K(pool_names), K(tenant_id), KR(ret));
  }
  return ret;
}

int ObTenantDDLService::drop_resource_pool_final(const uint64_t tenant_id,
                                           common::ObIArray<uint64_t> &drop_ug_id_array,
                                           ObIArray<ObResourcePoolName> &pool_names)
{
  int ret = OB_SUCCESS;
  const bool grant = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (OB_FAIL(unit_mgr_->load())) {
    LOG_WARN("unit_manager reload failed", K(ret));
  }

  // delete from __all_schema_status
  // The update of __all_core_table must be guaranteed to be a single partition transaction,
  // so a separate transaction is required here.
  // The failure of the transaction will not affect it, but there is garbage data in __all_core_table.
  if (OB_SUCC(ret)) {
    int temp_ret = OB_SUCCESS;
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      temp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", K(temp_ret));
    } else if (OB_SUCCESS !=
              (temp_ret = schema_status_proxy->del_tenant_schema_status(tenant_id))) {
      LOG_ERROR("del tenant schema status failed", KR(temp_ret), "tenant_id", tenant_id);
    }
  }
  return ret;
}

/*
 * This interface includes 4 situations of primary and standalone cluster in total
 * primary tenant
 * drop tenant force: The tenant is forced to be deleted, with the highest priority. Variable identification: drop_force
 * drop tenant and recyclebin is enable: put tenant into recyclebin. Variable identification: to_recyclebin
 * drop tenant and recyclebin is disable or drop tenant purge: Both cases take the path of delayed deletion
 * Variable identification: delay_to_drop
 * The priority of the 3 variables is reduced, and there can only be one state at the same time
 *
 * standby tenant
 * drop tenant force: The tenant is forced to be deleted, with the highest priority. Variable identification: drop_force
 * drop tenant and recyclebin is enable: put tenant into recyclebin. Variable identification: to_recyclebin
 * drop tenant and recyclebin is disable: equal to drop tneant force;
 *
 * meta tenant can only be force dropped with its user tenant.
 */
int ObTenantDDLService::drop_tenant(const ObDropTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObDDLSQLTransaction trans(schema_service_);
  const bool if_exist = arg.if_exist_;
  bool drop_force = !arg.delay_to_drop_;
  const bool open_recyclebin = arg.open_recyclebin_;
  const ObTenantSchema *tenant_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  ObArray<ObResourcePoolName> pool_names;
  ObArray<share::ObResourcePool*> pools;
  ret = OB_E(EventTable::EN_DROP_TENANT_FAILED) OB_SUCCESS;
  bool is_standby = false;
  uint64_t user_tenant_id = common::OB_INVALID_ID;
  int64_t refreshed_schema_version = 0;
  common::ObArray<uint64_t> drop_ug_id_array;
  bool specify_tenant_id = OB_INVALID_TENANT_ID != arg.tenant_id_;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (specify_tenant_id && OB_FAIL(schema_guard.get_tenant_info(arg.tenant_id_, tenant_schema))) {
    LOG_WARN("fail to gt tenant info", KR(ret), K(arg));
  } else if (!specify_tenant_id && OB_FAIL(schema_guard.get_tenant_info(arg.tenant_name_, tenant_schema))) {
    LOG_WARN("fail to gt tenant info", KR(ret), K(arg));
  } else if (OB_ISNULL(tenant_schema)) {
    if (if_exist) {
      LOG_USER_NOTE(OB_TENANT_NOT_EXIST, arg.tenant_name_.length(), arg.tenant_name_.ptr());
      LOG_INFO("tenant not exist, no need to delete it", K(arg));
    } else {
      ret = OB_TENANT_NOT_EXIST;
      LOG_USER_ERROR(OB_TENANT_NOT_EXIST, arg.tenant_name_.length(), arg.tenant_name_.ptr());
      LOG_WARN("tenant not exist, can't delete it", K(arg), KR(ret));
    }
  } else if (FALSE_IT(user_tenant_id = tenant_schema->get_tenant_id())) {
  } else if (!is_user_tenant(user_tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't drop sys or meta tenant", KR(ret), K(user_tenant_id));
  } else if (drop_force) {
    //is drop force, no need to check
    //pay attention
  } else if (tenant_schema->is_in_recyclebin()) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant in recyclebin, can't delete it", K(arg), KR(ret));
    LOG_USER_ERROR(OB_TENANT_NOT_EXIST, arg.tenant_name_.length(), arg.tenant_name_.ptr());
  } else if (tenant_schema->is_restore() ||
          tenant_schema->is_creating() || tenant_schema->is_dropping()) {
    // Due to the particularity of restore tenants, in order to avoid abnormal behavior of the cluster,
    // restore tenants cannot be placed in the recycle bin.
    // The creating state is the intermediate state of tenant creation, and it will become the normal state
    // if it is successfully created
    // The dropping state is the previous delayed deletion state. The two states are managed by the gc thread,
    // responsible for deletion and cannot be placed in the recycle bin.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drop tenant to recyclebin is not supported", KR(ret), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "should drop tenant force, delay drop tenant");
  } else {
    ObAllTenantInfo tenant_info;
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
          user_tenant_id, sql_proxy_, false, tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(arg), K(user_tenant_id));
    } else if (!tenant_info.is_primary() && !tenant_info.is_standby()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("drop tenant not in primary or standby role is not supported",
               KR(ret), K(arg), K(tenant_info));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "should drop tenant force, drop tenant");
    } else if (tenant_info.is_standby() && !open_recyclebin) {
      //if standby tenant and no recyclebin, need drop force
      drop_force = true;
      FLOG_INFO("is standby tenant, need drop force", K(tenant_info));
    }
  }
  if (OB_FAIL(ret)) {
    // ignore
  } else if (OB_ISNULL(tenant_schema)) {
    // We need to ignore the drop tenant if exists statement in the case that the tenant has already been deleted
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("start transaction failed", KR(ret), K(user_tenant_id), K(refreshed_schema_version));
  } else {
    /*
    * drop tenant force: delay_to_drop_ is false
    * delay_to_drop_ is true in rest the situation
    * drop tenant && recyclebin enable: in recyclebin
    * (drop tenant && recyclebin disable) || drop tenant purge: delay delete
    */
    const bool to_recyclebin = (arg.delay_to_drop_ && open_recyclebin);
    const bool delay_to_drop = (arg.delay_to_drop_ && !open_recyclebin);
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    //1.drop tenant force
    if (drop_force) {
      const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
      if (arg.drop_only_in_restore_) {
        // if drop_restore_tenant is true, it demands that the tenant must be in restore status after drop tenant trans start.
        if (OB_ISNULL(tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_USER_ERROR(OB_TENANT_NOT_EXIST, arg.tenant_name_.length(), arg.tenant_name_.ptr());
          LOG_WARN("tenant not exist, can't delete it", K(arg), KR(ret));
        } else if (!tenant_schema->is_restore()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Cancel tenant not in restore is");
          LOG_WARN("Cancel tenant not in restore is not allowed", K(ret), K(user_tenant_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(drop_resource_pool_pre(
              user_tenant_id, drop_ug_id_array, pool_names, trans))) {
        LOG_WARN("fail to drop resource pool pre", KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_tenant(user_tenant_id, trans, &arg.ddl_stmt_str_))) {
        LOG_WARN("ddl_operator drop_tenant failed", K(user_tenant_id), KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_tenant(meta_tenant_id, trans))) {
        LOG_WARN("ddl_operator drop_tenant failed", K(meta_tenant_id), KR(ret));
      } else if (OB_FAIL(try_drop_sys_ls_(meta_tenant_id, trans))) {
        LOG_WARN("failed to drop sys ls", KR(ret), K(meta_tenant_id));
      } else if (tenant_schema->is_in_recyclebin()) {
        // try recycle record from __all_recyclebin
        ObArray<ObRecycleObject> recycle_objs;
        ObSchemaService *schema_service_impl = NULL;
        if (OB_ISNULL(schema_service_)
            || OB_ISNULL(schema_service_->get_schema_service())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema service is null", KR(ret), KP_(schema_service));
        } else if (FALSE_IT(schema_service_impl = schema_service_->get_schema_service())) {
        } else if (OB_FAIL(schema_service_impl->fetch_recycle_object(
                           OB_SYS_TENANT_ID,
                           tenant_schema->get_tenant_name_str(),
                           ObRecycleObject::TENANT,
                           trans,
                           recycle_objs))) {
            LOG_WARN("get_recycle_object failed", KR(ret), KPC(tenant_schema));
        } else if (0 == recycle_objs.size()) {
          // skip
        } else if (1 < recycle_objs.size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("records should not be more than 1",
                   KR(ret), KPC(tenant_schema), K(recycle_objs));
        } else if (OB_FAIL(schema_service_impl->delete_recycle_object(
                           OB_SYS_TENANT_ID,
                           recycle_objs.at(0),
                           trans))) {
          LOG_WARN("delete_recycle_object failed", KR(ret), KPC(tenant_schema));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_->reset_parallel_cache(meta_tenant_id))) {
          LOG_WARN("fail to reset parallel cache", KR(ret), K(meta_tenant_id));
        } else if (OB_FAIL(ddl_service_->reset_parallel_cache(user_tenant_id))) {
          LOG_WARN("fail to reset parallel cache", KR(ret), K(user_tenant_id));
        }
      }
    } else {// put tenant into recyclebin
      ObTenantSchema new_tenant_schema;
      ObSqlString new_tenant_name;
      if (OB_FAIL(new_tenant_schema.assign(*tenant_schema))) {
        LOG_WARN("failed to assign tenant schema", KR(ret), KPC(tenant_schema));
      } else if (OB_FAIL(ddl_operator.construct_new_name_for_recyclebin(
              new_tenant_schema, new_tenant_name))) {
        LOG_WARN("fail to construct new name", K(ret));
      } else if (to_recyclebin) {
        //2. tenant in recyclebin
        if (new_tenant_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant name is null", K(ret));
        } else if (OB_FAIL(ddl_operator.drop_tenant_to_recyclebin(
                new_tenant_name,
                new_tenant_schema,
                trans, &arg.ddl_stmt_str_))) {
          LOG_WARN("fail to drop tenant in recyclebin", KR(ret), K(user_tenant_id));
        }
      } else if (delay_to_drop) {
        //3. tenant delay delete
        if (OB_FAIL(ddl_operator.delay_to_drop_tenant(new_tenant_schema, trans,
                &arg.ddl_stmt_str_))) {
          LOG_WARN("fail to delay_to drop tenant", K(ret));
        } else {
          // ObLSManager will process force_drop_tenant() logic each 100ms.
        }
      }
    }
  }

  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
    }
  }

  if (drop_force) {
    if (OB_SUCC(ret) && OB_NOT_NULL(tenant_schema)) {
      if (OB_FAIL(drop_resource_pool_final(
              tenant_schema->get_tenant_id(), drop_ug_id_array,
              pool_names))) {
        LOG_WARN("fail to drop resource pool finsl", KR(ret), KPC(tenant_schema));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("publish schema failed", KR(ret));
    }
  }

  LOG_INFO("drop tenant", K(arg), KR(ret));
  return ret;
}

int ObTenantDDLService::flashback_tenant(const obrpc::ObFlashBackTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObArenaAllocator allocator(ObModIds::OB_TENANT_INFO);
  ObString final_tenant_name;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (arg.tenant_id_ != OB_SYS_TENANT_ID) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "falshback tenant must in sys tenant");
    LOG_WARN("falshback tenant must in sys tenant", K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(
                     OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(arg.origin_tenant_name_, tenant_schema))) {
    LOG_WARN("failt to get tenant info", K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    const bool is_flashback = true;
    ObString new_tenant_name;
    if (OB_FAIL(get_tenant_object_name_with_origin_name_in_recyclebin(arg.origin_tenant_name_,
                                                                      new_tenant_name, &allocator,
                                                                      is_flashback))) {
      LOG_WARN("fail to get tenant obfect name", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(new_tenant_name, tenant_schema))) {
      LOG_WARN("fail to get tenant info", K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant name is not exist", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!tenant_schema->is_in_recyclebin()) {
      ret = OB_ERR_OBJECT_NOT_IN_RECYCLEBIN;
      LOG_WARN("tenant schema is not in recyclebin", K(ret), K(arg), K(*tenant_schema));
    } else if (!arg.new_tenant_name_.empty()) {
      final_tenant_name = arg.new_tenant_name_;
    } else {}//nothing todo
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(flashback_tenant_in_trans(*tenant_schema,
                                          final_tenant_name,
                                          schema_guard,
                                          arg.ddl_stmt_str_))) {
      LOG_WARN("flashback tenant in trans failed", K(ret));
    } else if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("publish_schema failed", K(ret));
    }
  }
  LOG_INFO("finish flashback tenant", K(arg), K(ret));
  return ret;
}

int ObTenantDDLService::flashback_tenant_in_trans(const share::schema::ObTenantSchema &tenant_schema,
                                            const ObString &new_tenant_name,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObDDLSQLTransaction trans(schema_service_);
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret));
    } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(ddl_operator.flashback_tenant_from_recyclebin(tenant_schema,
                                                                     trans,
                                                                     new_tenant_name,
                                                                     schema_guard,
                                                                     ddl_stmt_str))) {
      LOG_WARN("flashback tenant from recyclebin failed", K(ret));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_object_name_with_origin_name_in_recyclebin(
    const ObString &origin_tenant_name,
    ObString &object_name,
    common::ObIAllocator *allocator,
    const bool is_flashback)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    const char *desc_or_asc = (is_flashback ? "desc" : "asc");
    if (OB_FAIL(sql.append_fmt(
        "select object_name from oceanbase.__all_recyclebin where "
        "original_name = '%.*s' and TYPE = 7 order by gmt_create %s limit 1",
        origin_tenant_name.length(),
        origin_tenant_name.ptr(),
        desc_or_asc))) {
      LOG_WARN("failed to append sql",
             K(ret), K(origin_tenant_name), K(*desc_or_asc));
    } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
      LOG_WARN("failed to execute sql", K(sql), K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_OBJECT_NOT_IN_RECYCLEBIN;
        LOG_WARN("origin tenant_name not exist in recyclebin", K(ret), K(sql));
      } else {
        LOG_WARN("iterate next result fail", K(ret), K(sql));
      }
    } else {
      ObString tmp_object_name;
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "object_name", tmp_object_name);
      if (OB_FAIL(deep_copy_ob_string(*allocator, tmp_object_name, object_name))) {
        LOG_WARN("failed to deep copy member list", K(ret), K(object_name));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ITER_END != result->next()) {
      // The result will not exceed one line
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result failed", K(ret), K(sql));
    }
  }
  return ret;
}

int ObTenantDDLService::purge_tenant(
    const obrpc::ObPurgeTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObArenaAllocator allocator(ObModIds::OB_TENANT_INFO);
  ObArray<ObResourcePoolName> pool_names;
  ObAllTenantInfo tenant_info;
  bool is_standby_tenant = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (arg.tenant_id_ != OB_SYS_TENANT_ID) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "purge tenant must in sys tenant");
    LOG_WARN("purge tenant must in sys tenant", K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(
                     OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(arg.tenant_name_, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    const bool is_flashback = false;
    ObString new_tenant_name;
    if (OB_FAIL(get_tenant_object_name_with_origin_name_in_recyclebin(arg.tenant_name_,
                                                                      new_tenant_name, &allocator,
                                                                      is_flashback))) {
      LOG_WARN("fail to get tenant obfect name", K(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(new_tenant_name, tenant_schema))) {
      LOG_WARN("fail to get tenant info", K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant name is not exist", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!tenant_schema->is_in_recyclebin()) {
      ret = OB_ERR_OBJECT_NOT_IN_RECYCLEBIN;
      LOG_WARN("tenant not in recyclebin, can not be purge", K(arg), K(*tenant_schema), K(ret));
    }
  }
  if (FAILEDx(ObAllTenantInfoProxy::load_tenant_info(
          tenant_schema->get_tenant_id(), sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(arg), KPC(tenant_schema));
  } else if (FALSE_IT(is_standby_tenant = tenant_info.is_standby())) {
    //can not be there
  }

  if (OB_FAIL(ret)) {
  } else if (is_standby_tenant) {
    //drop tenant force
    if (OB_FAIL(try_force_drop_tenant(*tenant_schema))) {
      LOG_WARN("failed to try drop tenant force", KR(ret), KPC(tenant_schema));
    }
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    ObDDLSQLTransaction trans(schema_service_);
    const uint64_t tenant_id = tenant_schema->get_tenant_id();
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("failed to get tenant schema version", KR(ret));
    } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
    } else if (OB_FAIL(ddl_operator.purge_tenant_in_recyclebin(
                       *tenant_schema,
                       trans,
                       &arg.ddl_stmt_str_))) {
      LOG_WARN("purge tenant failed", K(ret));
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
        LOG_WARN("publish_schema failed", K(ret));
      }
    }
  }
  LOG_INFO("finish purge tenant", K(arg), K(ret));
  return ret;
}

int ObTenantDDLService::lock_tenant(const ObString &tenant_name, const bool is_lock)
{
  int ret = OB_SUCCESS;
  ObDDLSQLTransaction trans(schema_service_);
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  int64_t refreshed_schema_version = 0;
  ObTenantSchema new_tenant_schema;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init");
  } else if (tenant_name.length() <= 0) {
    ret = OB_INVALID_TENANT_NAME;
    LOG_WARN("invalid tenant name", K(tenant_name), K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, tenant_schema)) ||
      NULL == tenant_schema) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist, can't lock it", K(tenant_name), K(ret));
  } else if (tenant_schema->get_locked() == is_lock) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("failed to get tenant schema version", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("start transaction failed", KR(ret), K(refreshed_schema_version));
  } else if (OB_FAIL(new_tenant_schema.assign(*tenant_schema))) {
    LOG_WARN("fail to assign tenant schema", KR(ret));
  } else {
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    new_tenant_schema.set_locked(is_lock);
    if (OB_FAIL(ddl_operator.alter_tenant(new_tenant_schema, trans))) {
      LOG_WARN("ddl_operator alter tenant failed", K(new_tenant_schema), K(ret));
    }
  }

  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(publish_schema(OB_SYS_TENANT_ID))) {
      LOG_WARN("publish schema failed", K(ret));
    }
  }
  return ret;
}

int ObTenantDDLService::get_tenant_external_consistent_ts(const int64_t tenant_id, SCN &scn)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = THIS_WORKER.is_timeout_ts_valid() ?
      THIS_WORKER.get_timeout_remain() : GCONF.rpc_timeout;
  bool is_external_consistent = false;
  if (OB_FAIL(transaction::ObTsMgr::get_instance().get_ts_sync(tenant_id, timeout_us, scn,
                                                               is_external_consistent))) {
    LOG_WARN("fail to get_ts_sync", K(ret), K(tenant_id));
  } else if (!is_external_consistent) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("got ts of tenant is not external consistent", K(ret), K(tenant_id), K(scn),
             K(is_external_consistent));
  } else {
    LOG_INFO("success to get_tenant_external_consistent_ts", K(tenant_id), K(scn),
             K(is_external_consistent));
  }
  return ret;
}

int ObTenantDDLService::create_tenant_end(const uint64_t tenant_id)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 3. start create tenant end", K(tenant_id));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObAllTenantInfo tenant_info;
  int64_t sys_schema_version = OB_INVALID_VERSION;
  ObDDLSQLTransaction trans(schema_service_, true, false, false, false);
  DEBUG_SYNC(BEFORE_CREATE_TENANT_END);
  ObTenantSchema new_tenant_schema;
  ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
  ObRefreshSchemaStatus schema_status;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (OB_ISNULL(schema_status_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema status proxy", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
          tenant_id, sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret), KP_(schema_service));
  /*
    After the inner-table is synchronized by the network standby tenant, the schema refresh switch
    is turned on, but standby tenant may not be in the same observer with RS, causing RS to use the
    old cache when creating tenant end, which may cause create tenant end to fail.
    So here, force trigger schema refresh refresh cache
  */
  } else if (OB_FAIL(schema_status_proxy->load_refresh_schema_status(tenant_id, schema_status))) {
    LOG_WARN("fail to load refresh schema status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, sys_schema_version))) {
    LOG_WARN("fail to get tenant schema version", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID, sys_schema_version))) {
    LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(sys_schema_version));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", K(ret), K(tenant_id));
  } else if (tenant_schema->is_normal()) {
    // skip, Guaranteed reentrant
  } else if (!tenant_schema->is_creating()
             && !tenant_schema->is_restore()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("state not match", K(ret), K(tenant_id));
  } else if (OB_FAIL(new_tenant_schema.assign(*tenant_schema))) {
    LOG_WARN("fail to assign tenant schema", KR(ret));
  } else {
    ObDDLSQLTransaction tenant_trans(schema_service_);
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    int64_t refreshed_schema_version = OB_INVALID_VERSION;
    if (!tenant_info.is_standby()) {
      // Push the system tenant schema_version, and the standalone cluster may fail due to unsynchronized heartbeat.
      // The standalone cluster uses the synchronized schema_version,
      // and there is no need to increase the system tenant schema_version.
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObSchemaService *schema_service_impl = schema_service_->get_schema_service();
      // Ensure that the schema_version monotonically increases among tenants' cross-tenant transactions
      //
      if (OB_ISNULL(schema_service_impl)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_service_impl is null", K(ret));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("fail to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
        LOG_WARN("start transaction failed", KR(ret), K(tenant_id), K(refreshed_schema_version));
      } else {
        refreshed_schema_version = sys_schema_version > refreshed_schema_version ? sys_schema_version : refreshed_schema_version;
        if (OB_FAIL(schema_service_impl->gen_new_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version, new_schema_version))) {
          LOG_WARN("fail to gen new schema_version", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const ObString *ddl_stmt_str_ptr = NULL;
      const int64_t DDL_STR_BUF_SIZE = 128;
      char ddl_str_buf[DDL_STR_BUF_SIZE];
      MEMSET(ddl_str_buf, 0, DDL_STR_BUF_SIZE);
      ObString ddl_stmt_str;
      if (tenant_schema->is_restore()) {
        SCN gts;
        int64_t pos = 0;
        if (OB_FAIL(get_tenant_external_consistent_ts(tenant_id, gts))) {
          SERVER_LOG(WARN, "failed to get_tenant_gts", KR(ret), K(tenant_id));
        } else if (OB_FAIL(databuff_printf(ddl_str_buf, DDL_STR_BUF_SIZE, pos,
                                           "schema_version=%ld; tenant_gts=%lu",
                                           refreshed_schema_version, gts.get_val_for_inner_table_field()))) {
          SERVER_LOG(WARN, "failed to construct ddl_stmt_str", KR(ret), K(tenant_id), K(refreshed_schema_version), K(gts));
        } else {
          ddl_stmt_str.assign_ptr(ddl_str_buf, pos);
          ddl_stmt_str_ptr = &ddl_stmt_str;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ddl_operator.create_tenant(new_tenant_schema, OB_DDL_ADD_TENANT_END, trans, ddl_stmt_str_ptr))) {
        LOG_WARN("create tenant failed", K(new_tenant_schema), K(ret));
      } else {/*do nothing*/}
    }

    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_CREATE_TENANT_TRANS_THREE_FAILED) OB_SUCCESS;
    }
    int temp_ret = OB_SUCCESS;
    if (trans.is_started()) {
      LOG_INFO("end create tenant", "is_commit", OB_SUCCESS == ret, K(ret));
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      }
    }
    if (tenant_trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      const bool is_commit = false;//no need commit, only for check and lock
      if (OB_SUCCESS != (temp_ret = tenant_trans.end(is_commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed",  KR(ret), KR(temp_ret), K(is_commit));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_SUCCESS != (temp_ret = publish_schema(OB_SYS_TENANT_ID))) {
        LOG_WARN("publish schema failed", K(temp_ret));
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 3. finish create tenant end", KR(ret), K(tenant_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObTenantDDLService::refresh_creating_tenant_schema_(const ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  int tenant_id = tenant_schema.get_tenant_id();
  ObAddrArray addrs;
  ObAddr leader;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id;
  int64_t baseline_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (!tenant_schema.is_creating()) {
    // only primary creating tenant need to refresh schema
  } else if (OB_FAIL(get_ls_member_list_for_creating_tenant_(tenant_id, ObLSID::SYS_LS_ID,
          leader, addrs))) {
    LOG_WARN("failed to get sys ls member list", KR(ret));
  } else if (OB_FAIL(publish_schema(tenant_id, addrs))) {
    LOG_WARN("fail to publish schema", KR(ret), K(tenant_id), K(addrs));
    // set baseline schema version
  } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(
          *sql_proxy_, schema_status, baseline_schema_version))) {
    LOG_WARN("fail to gen new schema version", KR(ret), K(schema_status));
  } else {
    ObGlobalStatProxy global_stat_proxy(*sql_proxy_, tenant_id);
    if (OB_FAIL(global_stat_proxy.set_baseline_schema_version(baseline_schema_version))) {
      LOG_WARN("fail to set baseline schema version",
          KR(ret), K(tenant_id), K(baseline_schema_version));
    }
  }
  return ret;
}

int ObTenantDDLService::init_tenant_global_stat_(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObConfigPairs> &init_configs,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[CREATE_TENANT] STEP 2.4.1 start init global stat", K(tenant_id));
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else {
    const int64_t core_schema_version = OB_CORE_SCHEMA_VERSION + 1;
    const int64_t baseline_schema_version = OB_INVALID_VERSION;
    const int64_t ddl_epoch = 0;
    const SCN snapshot_gc_scn  = SCN::min_scn();
    // find compatible version
    uint64_t data_version = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < init_configs.count(); i++) {
      const ObConfigPairs &config = init_configs.at(i);
      if (tenant_id == config.get_tenant_id()) {
        for (int64_t j = 0; data_version == 0 && OB_SUCC(ret) && j < config.get_configs().count(); j++) {
          const ObConfigPairs::ObConfigPair &pair = config.get_configs().at(j);
          if (0 != pair.key_.case_compare("compatible")) {
          } else if (OB_FAIL(ObClusterVersion::get_version(pair.value_.ptr(), data_version))) {
            LOG_WARN("fail to get compatible version", KR(ret), K(tenant_id), K(pair));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      ObGlobalStatProxy global_stat_proxy(trans, tenant_id);
      if (OB_FAIL(ret)) {
      } else if (0 == data_version) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("compatible version not defined", KR(ret), K(tenant_id), K(init_configs));
      } else if (OB_FAIL(global_stat_proxy.set_tenant_init_global_stat(
              core_schema_version, baseline_schema_version,
              snapshot_gc_scn, ddl_epoch, data_version, data_version, data_version))) {
        LOG_WARN("fail to set tenant init global stat", KR(ret), K(tenant_id),
            K(core_schema_version), K(baseline_schema_version),
            K(snapshot_gc_scn), K(ddl_epoch), K(data_version));
      } else if (is_user_tenant(tenant_id) && OB_FAIL(OB_STANDBY_SERVICE.write_upgrade_barrier_log(
              trans, tenant_id, data_version))) {
        LOG_WARN("fail to write_upgrade_barrier_log", KR(ret), K(tenant_id), K(data_version));
      } else if (is_user_tenant(tenant_id) &&
          OB_FAIL(OB_STANDBY_SERVICE.write_upgrade_data_version_barrier_log(
              trans, tenant_id, data_version))) {
        LOG_WARN("fail to write_upgrade_data_version_barrier_log", KR(ret),
            K(tenant_id), K(data_version));
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.4.1 finish init global stat", KR(ret), K(tenant_id));
  return ret;
}

int ObTenantDDLService::get_ls_member_list_for_creating_tenant_(
    const uint64_t tenant_id,
    const int64_t ls_id,
    ObAddr &leader,
    ObIArray<ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  ObLSLocation location;
  ObLSID ls(ls_id);
  leader.reset();
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get(GCONF.cluster_id, tenant_id, ls,
            0/*expire_renew_time*/, is_cache_hit, location))) {
    LOG_WARN("fail to get sys ls info", KR(ret), K(tenant_id));
  } else {
    const ObIArray<ObLSReplicaLocation> &replica_locations = location.get_replica_locations();
    for (int64_t i = 0; i < replica_locations.count() && OB_SUCC(ret); i++) {
      const ObLSReplicaLocation &replica = replica_locations.at(i);
      const ObAddr &addr = replica.get_server();
      if (!addr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("addr is invalid", KR(ret), K(addr));
      } else if (OB_FAIL(addrs.push_back(addr))) {
        LOG_WARN("failed to push_back replica addr", KR(ret), K(replica));
      } else if (replica.is_strong_leader()) {
        leader = addr;
      }
    }
  }
  return ret;
}

int ObTenantDDLService::set_tenant_schema_charset_and_collation(
    ObTenantSchema &tenant_schema,
    const ObCreateTenantArg &create_tenant_arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  const ObTenantSchema &src_tenant_schema = create_tenant_arg.tenant_schema_;
  if (!tenant_schema.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_schema is invalid", KR(ret));
  } else if (FALSE_IT(tenant_id = tenant_schema.get_tenant_id())) {
  } else if (is_user_tenant(tenant_id)) {
    tenant_schema.set_charset_type(src_tenant_schema.get_charset_type());
    tenant_schema.set_collation_type(src_tenant_schema.get_collation_type());
  } else if (is_meta_tenant(tenant_id)) {
    tenant_schema.set_charset_type(ObCharset::get_default_charset());
    tenant_schema.set_collation_type(ObCharset::get_default_collation(
          ObCharset::get_default_charset()));
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CREATE_TENANT_CHECK_FAIL);
int ObTenantDDLService::create_tenant_check_(const obrpc::ObCreateTenantArg &arg,
    bool &need_create,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = arg.tenant_schema_.get_tenant_name_str();
  need_create = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_MISS_ARGUMENT;
    if (tenant_name.empty()) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "tenant name");
    } else if (arg.pool_list_.count() <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource_pool_list");
    }
    LOG_WARN("missing arg to create tenant", KR(ret), K(arg));
  } else if (tenant_name.case_compare(OB_DIAG_TENANT_NAME) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_name \'diag\' is reserved for diagnose tenant", KR(ret), K(arg));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant_name (\'diag\' is reserved for diagnose tenant)");
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("create tenant when cluster is upgrading not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "create tenant when cluster is upgrading");
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP_(sql_proxy));
  } else if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(ERRSIM_CREATE_TENANT_CHECK_FAIL)) {
    LOG_WARN("ERRSIM_CREATE_TENANT_CHECK_FAIL", KR(ret));
  } else {
    // check tenant exist
    bool tenant_exist = false;
    if (OB_NOT_NULL(schema_guard.get_tenant_info(tenant_name))) {
      tenant_exist = true;
    } else {
      if (!arg.is_restore_tenant()) {
        if (OB_FAIL(ObRestoreUtil::check_has_physical_restore_job(*sql_proxy_, tenant_name, tenant_exist))) {
          LOG_WARN("failed to check has physical restore job", KR(ret), K(tenant_name));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (tenant_exist) {
      if (arg.if_not_exist_) {
        ret = OB_SUCCESS;
        LOG_USER_NOTE(OB_TENANT_EXIST, to_cstring(tenant_name));
        LOG_INFO("tenant already exists, not need to create", KR(ret), K(tenant_name));
      } else {
        ret = OB_TENANT_EXIST;
        LOG_USER_ERROR(OB_TENANT_EXIST, to_cstring(tenant_name));
        LOG_WARN("tenant already exists", KR(ret), K(tenant_name));
      }
    } else {
      need_create = true;
    }
  }
  return ret;
}
}
}
