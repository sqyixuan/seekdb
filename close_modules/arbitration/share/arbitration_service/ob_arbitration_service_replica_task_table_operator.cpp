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

#include "ob_arbitration_service_replica_task_table_operator.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"               // for OTTZ_MGR
#include "share/inner_table/ob_inner_table_schema_constants.h" // for xxx_TNAME


namespace oceanbase
{
namespace share
{

int ObArbitrationServiceReplicaTaskTableOperator::construct_arb_replica_task_infos_(
    common::sqlclient::ObMySQLResult &res,
    ObArbitrationServiceReplicaTaskInfoList &task_infos)
{
  int ret = OB_SUCCESS;
  ObArbitrationServiceReplicaTaskInfo task_info;
  task_infos.reset();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", KR(ret));
      }
      break;
    } else {
      task_info.reset();
      if (OB_FAIL(construct_arb_replica_task_info_(res, task_info))) {
        LOG_WARN("fail to construct single task info", KR(ret), K(task_info));
      } else if (OB_UNLIKELY(!task_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("construct invalid task info", KR(ret), K(task_info));
      } else if (OB_FAIL(task_infos.push_back(task_info))) {
        LOG_WARN("fail to push back into task infos", KR(ret), K(task_info));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceReplicaTaskTableOperator::construct_arb_replica_task_info_(
    common::sqlclient::ObMySQLResult &res,
    ObArbitrationServiceReplicaTaskInfo &arb_replica_task_info)
{
  int ret = OB_SUCCESS;
  // declare initial member read from res
  int64_t create_time_us = 0;
  int64_t tenant_id = 0;
  int64_t ls_id = ObLSID::INVALID_LS_ID;
  int64_t task_id = 0;
  common::ObString task_type;
  common::ObString trace_id;
  common::ObString arbitration_service;
  common::ObString arbitration_service_type;
  common::ObString comment;
  // read from res
  EXTRACT_INT_FIELD_MYSQL(res, "tenant_id", tenant_id, uint64_t);

  (void)GET_COL_IGNORE_NULL(res.get_int, "create_time", create_time_us);
  (void)GET_COL_IGNORE_NULL(res.get_int, "tenant_id", tenant_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "ls_id", ls_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "task_id", task_id);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "trace_id", trace_id);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "task_type", task_type);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "arbitration_service", arbitration_service);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "arbitration_service_type", arbitration_service_type);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "comment", comment);
  // transform members to certain type
  ObArbitrationServiceReplicaTaskType task_type_to_set;
  share::ObTaskId trace_id_to_set;
  ObArbitrationServiceType arbitration_service_type_to_set;
  // build info with transformed type
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(task_type_to_set.parse_from_string(task_type))) {
    LOG_WARN("fail to parse task type from string", KR(ret), K(task_type));
  } else if (OB_FAIL(trace_id_to_set.set(trace_id.ptr()))) {
    LOG_WARN("fail to set task id", KR(ret), K(trace_id));
  } else if (OB_FAIL(arbitration_service_type_to_set.parse_from_string(arbitration_service_type))) {
    LOG_WARN("fail to parse arbitration service type", KR(ret), K(arbitration_service_type));
  } else if (OB_FAIL(arb_replica_task_info.build(
                         create_time_us,
                         tenant_id,
                         ObLSID(ls_id),
                         task_id,
                         task_type_to_set,
                         trace_id_to_set,
                         arbitration_service,
                         arbitration_service_type_to_set,
                         comment))) {
    LOG_WARN("fail to build a arb_replica_task_info", KR(ret),
             K(create_time_us), K(tenant_id), K(ls_id), K(task_id),
             K(task_type_to_set), K(trace_id_to_set), K(arbitration_service),
             K(arbitration_service_type_to_set), K(comment));
  }
  return ret;
}

int ObArbitrationServiceReplicaTaskTableOperator::insert(
    common::ObISQLClient &sql_proxy,
    const ObArbitrationServiceReplicaTaskInfo &arb_replica_task_info)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(arb_replica_task_info.get_tenant_id());
  const char *table_name = OB_ALL_LS_ARB_REPLICA_TASK_TNAME;
  ObDMLExecHelper exec(sql_proxy, sql_tenant_id);

  if (!arb_replica_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arb_replica_task_info));
  } else if (OB_FAIL(arb_replica_task_info.fill_dml_splicer(dml))) {
    LOG_WARN("fill dml splicer failed", KR(ret), K(arb_replica_task_info));
  } else if (OB_FAIL(dml.splice_insert_sql(table_name, sql))) {
    LOG_WARN("fail to splice insert sql", KR(ret), K(sql), K(arb_replica_task_info));
  } else if (OB_FAIL(sql_proxy.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_ENTRY_EXIST;
    }
    LOG_WARN("fail to execute insert sql", KR(ret), K(sql_tenant_id), K(sql),
             K(affected_rows), K(arb_replica_task_info));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect insert single row", KR(ret), K(arb_replica_task_info), K(affected_rows));
  } else {
    FLOG_INFO("[ARB_SERVICE] insert arbitration service replica task succeed",
              K(arb_replica_task_info));
  }
  return ret;
}

int ObArbitrationServiceReplicaTaskTableOperator::remove(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
                  || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu "
                                    "AND ls_id = %lu",
                                    OB_ALL_LS_ARB_REPLICA_TASK_TNAME,
                                    tenant_id, ls_id.id()))) {
    LOG_WARN("assign sql string failed", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(sql_proxy.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr(), K(sql_tenant_id));
  } else if (affected_rows == 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("expect delete one row", KR(ret), K(tenant_id), K(ls_id),
             K(affected_rows), "sql", sql.ptr());
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect delete single row", KR(ret), K(tenant_id), K(ls_id),
             K(affected_rows), "sql", sql.ptr());
  } else {
    FLOG_INFO("[ARB_SERVICE]delete task from inner table succeed",
              K(tenant_id), K(ls_id), K(affected_rows), K(sql));
  }
  return ret; 
}

int ObArbitrationServiceReplicaTaskTableOperator::get_all_tasks(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const bool lock_line,
    ObArbitrationServiceReplicaTaskInfoList &task_infos)
{
  int ret = OB_SUCCESS;
  task_infos.reset();
  ObSqlString sql;
  const char *table_name = OB_ALL_LS_ARB_REPLICA_TASK_TNAME;
  uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
  ObArbitrationServiceReplicaTaskInfo task_info;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
             "SELECT time_to_usec(gmt_create) AS create_time, "
             "* FROM %s WHERE tenant_id = %lu ORDER BY tenant_id, ls_id ASC%s",
             table_name, tenant_id, lock_line ? " FOR UPDATE" : ""))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy.read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql_tenant_id), "sql", sql.ptr());
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed");
      } else if (OB_FAIL(construct_arb_replica_task_infos_(
                         *result.get_result(), task_infos))) {
        LOG_WARN("fail to build task infos from table", KR(ret), K(tenant_id),
                 K(lock_line), K(task_infos));
      } else {
        LOG_INFO("[ARB_SERVICE] success to read task infos from table", K(tenant_id),
                 K(lock_line), K(task_infos));
      }
    }
  }
  return ret;
}

int ObArbitrationServiceReplicaTaskTableOperator::insert_history(
    common::ObISQLClient &sql_proxy,
    const ObArbitrationServiceReplicaTaskInfo &arb_replica_task_info,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  int64_t create_time_us = arb_replica_task_info.get_create_time_us();
  int64_t finish_time_us = ObTimeUtility::current_time();
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObSqlString execute_result;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(arb_replica_task_info.get_tenant_id());
  const char *table_name = OB_ALL_LS_ARB_REPLICA_TASK_HISTORY_TNAME;
  ObDMLExecHelper exec(sql_proxy, sql_tenant_id);

  if (!arb_replica_task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arb_replica_task_info));
  } else if (OB_FAIL(execute_result.assign_fmt("[ret:%d; elapsed:%ld;]", ret_code, finish_time_us - create_time_us))) {
    LOG_WARN("fail to build task result", KR(ret), K(ret_code), K(create_time_us), K(finish_time_us));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", arb_replica_task_info.get_tenant_id()))
      || OB_FAIL(dml.add_pk_column("ls_id", arb_replica_task_info.get_ls_id().id()))
      || OB_FAIL(dml.add_pk_column("task_id", arb_replica_task_info.get_task_id()))
      || OB_FAIL(dml.add_column("execute_result", execute_result))
      || OB_FAIL(dml.add_time_column("create_time", create_time_us))
      || OB_FAIL(dml.add_time_column("finish_time", finish_time_us))
      || OB_FAIL(dml.add_column("trace_id", arb_replica_task_info.get_trace_id()))
      || OB_FAIL(dml.add_column("task_type", arb_replica_task_info.get_task_type_str()))
      || OB_FAIL(dml.add_column("arbitration_service", arb_replica_task_info.get_arbitration_service_string()))
      || OB_FAIL(dml.add_column("arbitration_service_type", arb_replica_task_info.get_arbitration_service_type_str()))
      || OB_FAIL(dml.add_column("comment", arb_replica_task_info.get_comment()))) {
    LOG_WARN("add column failed", KR(ret), K(arb_replica_task_info), K(execute_result),
             K(create_time_us), K(finish_time_us));
  } else if (OB_FAIL(dml.splice_insert_sql(table_name, sql))) {
    LOG_WARN("fail to splice insert sql", KR(ret), K(sql), K(arb_replica_task_info), K(execute_result));
  } else if (OB_FAIL(sql_proxy.write(sql_tenant_id, sql.ptr(), affected_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_ENTRY_EXIST;
    }
    LOG_WARN("fail to execute insert sql", KR(ret), K(sql_tenant_id), K(sql),
             K(create_time_us), K(finish_time_us),
             K(affected_rows), K(arb_replica_task_info));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect insert single row", KR(ret), K(arb_replica_task_info), K(affected_rows));
  } else {
    FLOG_INFO("[ARB_SERVICE] insert arb service replica task into history table succeed",
              K(arb_replica_task_info), K(ret_code));
  }
  return ret;
}
} // end namespace share
} // end namespace oceanbase
