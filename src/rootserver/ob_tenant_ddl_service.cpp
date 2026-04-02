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

#include "rootserver/ob_tenant_thread_helper.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_root_service.h"
#include "share/location_cache/ob_location_service.h"
#include "rootserver/ob_table_creator.h"
#include "share/ob_global_stat_proxy.h"
#include "share/backup/ob_backup_config.h"
#include "share/ob_schema_status_proxy.h"
#include "share/backup/ob_log_restore_config.h"//ObLogRestoreSourceServiceConfigParser
#include "storage/tx/ob_ts_mgr.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/ob_zone_merge_info.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_table_operator.h"
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
      || OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)
      || OB_ISNULL(ddl_trans_controller_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), KP(ddl_service_), KP(rpc_proxy_), KP(sql_proxy_),
        KP(schema_service_), KP(ddl_trans_controller_));
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

int ObTenantDDLService::init(
    ObUnitManager &unit_mgr,
    ObDDLService &ddl_service,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    obrpc::ObCommonRpcProxy &common_rpc,
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObZoneManager &zone_mgr)
{
  int ret = OB_SUCCESS;
  unit_mgr_ = &unit_mgr;
  ddl_service_ = &ddl_service;
  rpc_proxy_ = &rpc_proxy;
  common_rpc_ = &common_rpc;
  sql_proxy_ = &sql_proxy;
  schema_service_ = &schema_service;
  ddl_trans_controller_ = &schema_service.get_ddl_trans_controller();
  zone_mgr_ = &zone_mgr;
  inited_ = true;
  stopped_ = false;
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

}
}
