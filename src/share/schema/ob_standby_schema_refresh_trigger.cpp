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
#include "ob_standby_schema_refresh_trigger.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"  // OB_ALL_DDL_OPERATION_TNAME
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/mysqlclient/ob_mysql_result.h"  // ObMySQLResult
#include "share/schema/ob_schema_getter_guard.h"  // ObSchemaGetterGuard

namespace oceanbase
{
namespace share
{
namespace schema
{

void ObStandbySchemaRefreshTrigger::run1()
{
  LOG_INFO("ObStandbySchemaRefreshTrigger thread start");
  lib::set_thread_name("StandbySchemaRefresh");

  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    int64_t schema_version = OB_INVALID_VERSION;
    ObRefreshSchemaStatus schema_status;
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(OB_SYS_TENANT_ID, schema_status))) {
      LOG_WARN("failed to get tenant refresh schema status", KR(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_schema_version_in_inner_table(*GCTX.sql_proxy_,
          schema_status, schema_version))) {
      LOG_WARN("fail to get latest schema version in inner table", K(ret));
    } else if (OB_FAIL(GCTX.ob_service_->submit_async_refresh_schema_task(
                         OB_SYS_TENANT_ID, schema_version))) {
      LOG_WARN("fail to submit async refresh schema task",
               KR(ret), K(schema_version));
    }

    if (!has_set_stop()) {
      ob_usleep(static_cast<uint32_t>(REFRESH_INTERVAL_US));
    }
  }

  LOG_INFO("ObStandbySchemaRefreshTrigger thread end");
}

} // namespace schema
} // namespace share
} // namespace oceanbase
