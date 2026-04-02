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
#include "observer/virtual_table/ob_virtual_proxy_server_stat.h"
#include "observer/ob_sql_client_decorator.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{
ObVirtualProxyServerStat::ObVirtualProxyServerStat()
  : is_inited_(false),
    server_idx_(-1),
    table_schema_(NULL)
{
}

ObVirtualProxyServerStat::~ObVirtualProxyServerStat()
{
}

int ObVirtualProxyServerStat::init(ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", KR(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schema(OB_SYS_TENANT_ID,
    OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TID, table_schema_))) {
    SERVER_LOG(WARN, "failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table_schema_ is NULL", KP_(table_schema), K(ret));
  } else {
    server_idx_ = -1;
    is_inited_ = true;
  }
  return ret;
}

int ObVirtualProxyServerStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is null" , K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited" , K(ret));
  } else if (!start_to_read_) {
    start_to_read_ = true;
  }

  if (OB_SUCC(ret)) {
    if (server_idx_ == 0) {
      ret = OB_ITER_END;
      SERVER_LOG(WARN, "fail to get next server info", KR(ret));
    } else {
      ObArray<Column> columns;
      if (OB_FAIL(get_full_row(table_schema_, columns))) {
        SERVER_LOG(WARN, "failed to get full row", "table_schema", *table_schema_, K(ret));
      } else if (OB_FAIL(project_row(columns, cur_row_))) {
        SERVER_LOG(WARN, "failed to project row", K(ret));
      } else {
        row = &cur_row_;
        server_idx_ = 0;
      }
    }
  }
  return ret;
}

int ObVirtualProxyServerStat::get_full_row(const share::schema::ObTableSchema *table,
                                           ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  char *ip = NULL;
  char *zone = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited" , K(ret));
  } else if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "table is null", K(ret));
  } else if (OB_ISNULL(GCTX.config_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", KR(ret), KP(GCTX.config_));
  } else if (NULL == (ip = static_cast<char *>(allocator_->alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "alloc ip buf failed", "size", OB_MAX_SERVER_ADDR_SIZE, KR(ret));
  } else if (NULL == (zone = static_cast<char *>(allocator_->alloc(MAX_ZONE_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "alloc zone buf failed", "size", MAX_ZONE_LENGTH, KR(ret));
  } else if (false == GCTX.self_addr().ip_to_string(ip, OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "convert server ip to string failed", KR(ret), "server", GCTX.self_addr());
  } else if (OB_FAIL(databuff_printf(zone, MAX_ZONE_LENGTH, "%s", GCTX.config_->zone.str()))) {
    ret = OB_BUF_NOT_ENOUGH;
    SERVER_LOG(WARN, "snprintf failed", "buf_len", MAX_ZONE_LENGTH, "src_len", strlen(GCTX.config_->zone.str()), KR(ret));
  } else {
    ADD_COLUMN(set_varchar, table, "svr_ip", ip, columns);
    ADD_COLUMN(set_int, table, "svr_port", GCTX.self_addr().get_port(), columns);
    ADD_COLUMN(set_varchar, table, "zone", zone, columns);
    ADD_COLUMN(set_varchar, table, "status", "ACTIVE", columns);
    ADD_COLUMN(set_int, table, "start_service_time", GCTX.start_service_time_, columns);
    ADD_COLUMN(set_int, table, "stop_time", 0, columns);
  }
  if (OB_FAIL(ret)) {
    if (NULL != ip) {
      allocator_->free(ip);
      ip = NULL;
    }
    if (NULL != zone) {
      allocator_->free(zone);
      zone = NULL;
    }
  }
  return ret;
}

}//end namespace observer
}//end namespace oceanbase
