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
 
#define USING_LOG_PREFIX SHARE
#include "observer/sys_table/ob_all_tablet_checksum_error_info_operator.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "share/object_storage/ob_device_config_mgr.h"
namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace compaction;
using namespace share;
namespace share
{

int ObTabletCkmErrorInfoOperator::write_tablet_ckm_error_info(const ObTabletCkmErrorInfo &error_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml_splicer;
  char storage_path[OB_STORAGE_PATH_STR_LENGTH];
  MEMSET(storage_path, '\0', sizeof(char) * OB_STORAGE_PATH_STR_LENGTH);
  if (OB_UNLIKELY(!error_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status info", KR(ret), K(error_info));
  } else if (OB_FAIL(basic_check())) {
    LOG_WARN("failed to check", KR(ret));
  } else if (OB_FAIL(ObDeviceConfigMgr::get_instance().get_device_config_str(ObStorageUsedType::USED_TYPE_DATA, storage_path, OB_STORAGE_PATH_STR_LENGTH))) {
    LOG_WARN("failed to get device config str", KR(ret));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(error_info.tenant_id_);
    int64_t affected_rows = 0;
    if (OB_FAIL(dml_splicer.add_uint64_pk_column("tenant_id", error_info.tenant_id_))
        || OB_FAIL(dml_splicer.add_uint64_pk_column("ls_id", error_info.ls_id_))
        || OB_FAIL(dml_splicer.add_pk_column("shared_storage_path", storage_path))
        || OB_FAIL(dml_splicer.add_uint64_column("tablet_id", error_info.tablet_id_))
        || OB_FAIL(dml_splicer.add_uint64_column("compaction_scn", error_info.compaction_scn_))
        || OB_FAIL(dml_splicer.add_column("check_error_info", error_info.check_error_info_))) {
      LOG_WARN("failed to add column into sml splicer", KR(ret), K(error_info));
    } else if (OB_FAIL(dml_splicer.splice_insert_update_sql(OB_ALL_TABLET_CHECKSUM_ERROR_INFO_TNAME, sql))) {
      LOG_WARN("failed to splice sql string", K(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(error_info), K(meta_tenant_id), K(sql));
    } else if (affected_rows > 0) {
      LOG_INFO("[SharedStorage] success to write tablet error info", K(ret), K(sql), K(error_info), K(affected_rows));
    }
  }
  return ret;
}

int ObTabletCkmErrorInfoOperator::check_exist_ckm_error_tablet(const uint64_t tenant_id, const int64_t compaction_scn, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObSqlString sql;
  ObDMLSqlSplicer dml_splicer;
  if (OB_UNLIKELY(0 == tenant_id || compaction_scn <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(compaction_scn));
  } else if (OB_FAIL(basic_check())) {
    LOG_WARN("failed to check", KR(ret));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    int64_t exist_cnt = 0;
    ObMySQLResult *result = NULL;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql.append_fmt(
          "SELECT count(*) > 0 as c FROM %s WHERE ", OB_ALL_TABLET_CHECKSUM_ERROR_INFO_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(compaction_scn));
      } else if (OB_FAIL(dml_splicer.add_uint64_pk_column("tenant_id", tenant_id))
          || OB_FAIL(dml_splicer.add_uint64_column("compaction_scn", compaction_scn))) {
        LOG_WARN("failed to add column into sml splicer", KR(ret), K(tenant_id), K(compaction_scn));
      } else if (OB_FAIL(dml_splicer.splice_predicates(sql))) {
        LOG_WARN("failed to splice predicates", K(ret), K(sql));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(meta_tenant_id), K(sql));
      } else if (OB_FAIL(result->get_int("c", exist_cnt))) {
        LOG_WARN("failed to get int", KR(ret), K(compaction_scn));
      } else if (exist_cnt > 0) {
        LOG_INFO("exist ckm error info", KR(ret), K(exist_cnt));
        exist = true;
      }
    }
  }
  return ret;
}

int ObTabletCkmErrorInfoOperator::get_ckm_error_tablets(
    const uint64_t tenant_id, const int64_t compaction_scn,
    ObIArray<ObTabletLSPair> &error_tablet)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(0 == tenant_id || compaction_scn <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(compaction_scn));
  } else if (OB_FAIL(basic_check())) {
    LOG_WARN("failed to check", KR(ret));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    ObMySQLResult *result = NULL;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql.append_fmt(
          "SELECT ls_id, tablet_id FROM %s WHERE tenant_id=%lu AND compaction_scn=%ld ORDER BY ls_id",
          OB_ALL_TABLET_CHECKSUM_ERROR_INFO_TNAME, tenant_id, compaction_scn))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(compaction_scn));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(meta_tenant_id), K(sql));
      } else if (OB_FAIL(construct_ls_tablet_pair_array(*result, error_tablet))) {
        LOG_WARN("fail to construct tablet id array", KR(ret), K(meta_tenant_id), K(sql));
      }
    }
  }
  return ret;
}

int ObTabletCkmErrorInfoOperator::construct_ls_tablet_pair_array(
    sqlclient::ObMySQLResult &result,
    ObIArray<ObTabletLSPair> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  int64_t ls_id = 0;
  int64_t tablet_id = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next result", KR(ret));
      }
      break;
    } else if (OB_FAIL(result.get_int("ls_id", ls_id))) {
      LOG_WARN("fail to get uint", KR(ret));
    } else if (OB_FAIL(result.get_int("tablet_id", tablet_id))) {
      LOG_WARN("fail to get uint", KR(ret));
    } else if (OB_FAIL(tablet_id_array.push_back(ObTabletLSPair(tablet_id, ls_id)))) {
      LOG_WARN("failed to push back ls tablet pair", K(ret), K(ls_id), K(tablet_id));
    }
  } // while
  return ret;
}

int ObTabletCkmErrorInfoOperator::delete_ckm_error_info(const uint64_t tenant_id, const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_sql;
    if (OB_UNLIKELY(0 == tenant_id || compaction_scn <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(compaction_scn));
  } else if (OB_FAIL(basic_check())) {
    LOG_WARN("failed to check", KR(ret));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(delete_sql.assign_fmt("DELETE FROM %s WHERE tenant_id = '%lu' AND compaction_scn <= %lu",
             OB_ALL_TABLET_CHECKSUM_ERROR_INFO_TNAME, tenant_id, compaction_scn))) {
      LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(compaction_scn));
    } else if (OB_FAIL(GCTX.sql_proxy_->write(meta_tenant_id, delete_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(compaction_scn), K(meta_tenant_id), K(delete_sql));
    } else if (affected_rows > 0) {
      LOG_INFO("[SharedStorage] success to delete tablet error info", K(ret), K(delete_sql), K(compaction_scn), K(affected_rows));
    }
  }
  return ret;
}

int ObTabletCkmErrorInfoOperator::basic_check()
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(MERGE_SCHEDULER_PTR->get_min_data_version(compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("in compat, can't read/write inner table", K(ret), K(compat_version));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
