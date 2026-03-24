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

#include "ob_arbitration_service_table_operator.h"
#include "ob_arbitration_service_info.h"                       // for ObArbitrationServiceInfo
#include "observer/omt/ob_tenant_timezone_mgr.h"               // for OTTZ_MGR
#include "share/inner_table/ob_inner_table_schema_constants.h" // for xxx_TNAME

namespace oceanbase
{
namespace share
{
int ObArbitrationServiceTableOperator::construct_arbitration_service_info_(
    const common::sqlclient::ObMySQLResult &res,
    ObArbitrationServiceInfo &arbitration_service_info)
{
  int ret = OB_SUCCESS;
  common::ObString arbitration_service_key;
  common::ObString arbitration_service;
  common::ObString previous_arbitration_service;
  common::ObString type;
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "arbitration_service_key", arbitration_service_key);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "arbitration_service", arbitration_service);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "previous_arbitration_service", previous_arbitration_service);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "type", type);

  ObArbitrationServiceType type_to_set;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(type_to_set.parse_from_string(type))) {
    LOG_WARN("fail to parse from string", KR(ret), K(type));
  } else if (OB_FAIL(arbitration_service_info.init(
                         arbitration_service_key,
                         arbitration_service,
                         previous_arbitration_service,
                         type_to_set))) {
    LOG_WARN("fail to init a arbitration_service_info", KR(ret), K(arbitration_service_key),
             K(arbitration_service), K(previous_arbitration_service), K(type_to_set));
  }
  return ret;
}

int ObArbitrationServiceTableOperator::get(
    common::ObISQLClient &sql_proxy,
    const ObString &arbitration_service_key,
    bool lock_line,
    ObArbitrationServiceInfo &arbitration_service_info)
{
  int ret = OB_SUCCESS;
  if (arbitration_service_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service_key));
  } else {
    arbitration_service_info.reset();
    const char *table_name = OB_ALL_ARBITRATION_SERVICE_TNAME;
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT * FROM %s WHERE arbitration_service_key = '%.*s'%s",
          table_name, arbitration_service_key.length(), arbitration_service_key.ptr(),
          lock_line ? " FOR UPDATE" : ""))) {
        LOG_WARN("assign sql string failed", KR(ret), K(arbitration_service_key), K(lock_line));
      } else if (OB_FAIL(sql_proxy.read(result, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), "tenant_id", OB_SYS_TENANT_ID, "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed");
      } else if (OB_FAIL(result.get_result()->next())) {
        if (ret == OB_ITER_END) {
          LOG_TRACE("arbitration service may not exists", KR(ret), K(arbitration_service_key));
          ret = OB_ARBITRATION_SERVICE_NOT_EXIST;
        } else {
          LOG_WARN("fail to get result, result maybe null", KR(ret), K(arbitration_service_key), K(lock_line));
        }
      } else if (OB_FAIL(construct_arbitration_service_info_(
          *result.get_result(),
          arbitration_service_info))) {
        LOG_WARN("construct arbitration service info failed", KR(ret), K(arbitration_service_key), K(lock_line));
      } else {
        LOG_INFO("[ARB_SERVICE] success to build a arbitration service info from table", K(arbitration_service_info));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceTableOperator::insert(
    common::ObISQLClient &sql_proxy,
    const ObArbitrationServiceInfo &arbitration_service_info)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
  const char *table_name = OB_ALL_ARBITRATION_SERVICE_TNAME;
  ObDMLExecHelper exec(sql_proxy, sql_tenant_id);

  if (!arbitration_service_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service_info));
  } else if (OB_FAIL(arbitration_service_info.fill_dml_splicer(dml))) {
    LOG_WARN("fill dml splicer failed", KR(ret), K(arbitration_service_info));
  } else if (OB_FAIL(dml.splice_insert_sql(table_name, sql))) {
    LOG_WARN("fail to splice insert sql", KR(ret), K(sql), K(arbitration_service_info));
  } else if (OB_FAIL(sql_proxy.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_ARBITRATION_SERVICE_ALREADY_EXIST;
    }
    LOG_WARN("fail to execute insert sql", KR(ret), K(sql_tenant_id), K(sql),
             K(affected_rows), K(arbitration_service_info));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect delete single row", KR(ret), K(arbitration_service_info), K(affected_rows));
  } else {
    FLOG_INFO("[ARB_SERVICE] insert arbitration service into inner table succeed", K(arbitration_service_info));
  }
  return ret;
}

int ObArbitrationServiceTableOperator::update(
    common::ObISQLClient &sql_proxy,
    const ObArbitrationServiceInfo &arbitration_service_info)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
  const char *table_name = OB_ALL_ARBITRATION_SERVICE_TNAME;
  ObDMLExecHelper exec(sql_proxy, sql_tenant_id);

  if (!arbitration_service_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service_info));
  } else if (OB_FAIL(arbitration_service_info.fill_dml_splicer(dml))) {
    LOG_WARN("fill dml splicer failed", KR(ret), K(arbitration_service_info));
  } else if (OB_FAIL(exec.exec_update(table_name, dml, affected_rows))) {
    LOG_WARN("fail to exec insert update sql", KR(ret), K(arbitration_service_info)); 
  } else if (affected_rows == 0) {
    ret = OB_ARBITRATION_SERVICE_NOT_EXIST;
    LOG_WARN("expected update one row", KR(ret), K(arbitration_service_info), K(affected_rows));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect update single row", KR(ret), K(arbitration_service_info), K(affected_rows));
  } else {
    FLOG_INFO("[ARB_SERVICE] update arbitration service into inner table succeed", K(arbitration_service_info));
  }
  return ret;
}

int ObArbitrationServiceTableOperator::remove(
    common::ObISQLClient &sql_proxy,
    const ObString &arbitration_service_key)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
  int64_t affected_rows = 0;
  if (arbitration_service_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service_key));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE arbitration_service_key = '%.*s'",
            OB_ALL_ARBITRATION_SERVICE_TNAME, arbitration_service_key.length(), arbitration_service_key.ptr()))) {
    LOG_WARN("assign sql string failed", KR(ret), K(arbitration_service_key));
  } else if (OB_FAIL(sql_proxy.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr(), K(sql_tenant_id));
  } else if (affected_rows == 0) {
    ret = OB_ARBITRATION_SERVICE_NOT_EXIST;
    LOG_WARN("expect delete one row", KR(ret), K(arbitration_service_key), K(affected_rows), "sql", sql.ptr());
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect delete single row", KR(ret), K(arbitration_service_key), K(affected_rows), "sql", sql.ptr());
  } else {
    FLOG_INFO("[ARB_SERVICE]delete row from inner table", K(arbitration_service_key), K(affected_rows), K(sql));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
