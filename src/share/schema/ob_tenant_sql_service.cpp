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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_tenant_sql_service.h"
#include "sql/ob_sql_utils.h"
#include "rootserver/ob_rs_job_table_operator.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObTenantSqlService::insert_tenant(
    const ObTenantSchema &tenant_schema,
    const ObSchemaOperationType op,
    ObISQLClient &sql_client,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (!tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant schema", K(tenant_schema), K(ret));
  } else if (OB_FAIL(replace_tenant(tenant_schema, op, sql_client, ddl_stmt_str))) {
    LOG_WARN("replace_tenant failed", K(tenant_schema), K(op), K(ret));
  }
  return ret;
}

int ObTenantSqlService::replace_tenant(
    const ObTenantSchema &tenant_schema,
    const ObSchemaOperationType op,
    common::ObISQLClient &sql_client,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;
  char *zone_list_buf = NULL;
  if (!tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ObCStringHelper helper;
    LOG_WARN("tenant_schema is invalid", "tenant_schema",
        helper.convert(tenant_schema), K(ret));
  } else if (OB_DDL_ADD_TENANT != op
             && OB_DDL_ADD_TENANT_START != op
             && OB_DDL_ADD_TENANT_END != op
             && OB_DDL_ALTER_TENANT != op
             && OB_DDL_DEL_TENANT_START != op
             && OB_DDL_DROP_TENANT_TO_RECYCLEBIN != op
             && OB_DDL_RENAME_TENANT != op
             && OB_DDL_FLASHBACK_TENANT != op) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replace tenant op", K(op), K(ret));
  } else if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_UNLIKELY(NULL == (zone_list_buf = static_cast<char *>(ob_malloc(
              MAX_ZONE_LIST_LENGTH, ObModIds::OB_SCHEMA))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(tenant_schema.zone_array2str(zone_list, zone_list_buf, MAX_ZONE_LIST_LENGTH))) {
    LOG_WARN("fial to convert to str", K(ret), K(zone_list));
  } else {
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    const char *locality = "";
    const char *previous_locality = "";
    const char *primary_zone = OB_RANDOM_PRIMARY_ZONE;
    if (OB_SUCC(ret)) {
      const int64_t INVALID_REPLICA_NUM = -1;
      if (OB_SUCC(ret) && (OB_FAIL(dml.add_pk_column(OBJ_GET_K(tenant_schema, tenant_id)))
          || OB_FAIL(dml.add_column("tenant_name", ObHexEscapeSqlStr(tenant_schema.get_tenant_name_str())))
          || OB_FAIL(dml.add_column(OBJ_GET_K(tenant_schema, locked)))
          || OB_FAIL(dml.add_column("zone_list", zone_list_buf))
          || OB_FAIL(dml.add_column("primary_zone", ObHexEscapeSqlStr(primary_zone)))
          || OB_FAIL(dml.add_column("info", tenant_schema.get_comment()))
          || OB_FAIL(dml.add_column("collation_type", CS_TYPE_INVALID))
          || OB_FAIL(dml.add_column("locality", ObHexEscapeSqlStr(locality)))
          || OB_FAIL(dml.add_column("previous_locality", ObHexEscapeSqlStr(previous_locality)))
          || OB_FAIL(dml.add_column("default_tablegroup_id", tenant_schema.get_default_tablegroup_id()))
          || OB_FAIL(dml.add_column("compatibility_mode", tenant_schema.get_compatibility_mode()))
          || OB_FAIL(dml.add_column("drop_tenant_time", OB_INVALID_TIMESTAMP))
          || OB_FAIL(dml.add_column("status", ob_tenant_status_str(tenant_schema.get_status())))
          || OB_FAIL(dml.add_column("in_recyclebin", tenant_schema.is_in_recyclebin())))) {
        LOG_WARN("add column failed", K(ret));
      }
    }
    // TODO@jingyu.cr: remove insert __all_tenant and __all_tenant_history after OBD ready
    // insert into __all_tenant
    if (OB_SUCC(ret)) {
      ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);
      if (OB_FAIL(exec.exec_insert_update(OB_ALL_TENANT_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert update failed", KR(ret));
      } else if (0 != affected_rows && 1 != affected_rows && 2 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
      } else {
        OB_LOG(INFO, "replace tenant success", K(affected_rows), K(tenant_schema));
      }
    }

    // insert into __all_tenant_history
    if (OB_SUCC(ret)) {
      ObDMLExecHelper exec(sql_client, OB_SYS_TENANT_ID);
      const int64_t is_deleted = 0;
      if (OB_FAIL(dml.add_pk_column("schema_version", tenant_schema.get_schema_version()))
          || OB_FAIL(dml.add_column("is_deleted", is_deleted))) {
        LOG_WARN("add column failed", K(ret));
      } else if (OB_FAIL(exec.exec_replace(OB_ALL_TENANT_HISTORY_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", K(ret));
      } else if (0 != affected_rows && 1 != affected_rows && 2 != affected_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected", K(affected_rows), K(ret));
      }
    }

    // log operation
    if (OB_SUCC(ret)) {
      ObSchemaOperation tenant_op;
      tenant_op.tenant_id_ = tenant_schema.get_tenant_id();
      tenant_op.op_type_ = op;
      tenant_op.schema_version_ = tenant_schema.get_schema_version();
      tenant_op.ddl_stmt_str_ = ddl_stmt_str ? *ddl_stmt_str : ObString();
      int64_t sql_tenant_id = OB_SYS_TENANT_ID;
      if (OB_FAIL(log_operation(tenant_op, sql_client, sql_tenant_id))) {
        LOG_WARN("log add tenant ddl operation failed", K(ret));
      }
    }
    if (NULL != zone_list_buf) {
      ob_free(zone_list_buf);
      zone_list_buf = NULL;
    }
  }
  return ret;
}

} //end of schema
} //end of share
} //end of oceanbase
