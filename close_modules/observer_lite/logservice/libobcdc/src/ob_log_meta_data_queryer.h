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

#ifndef OCEANBASE_OB_LOG_META_SQL_QUERYER_H_
#define OCEANBASE_OB_LOG_META_SQL_QUERYER_H_

#include "ob_cdc_tenant_query.h"
#include "close_modules/observer_lite/logservice/logfetcher/ob_log_data_dictionary_in_log_table.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
namespace sqlclient
{
class ObMySQLResult;
}
} // end namespace common

namespace libobcdc
{

// query OB_SYS_TENANT_ID in tenant_sync_mode and query specific tenant_id in cluster_sync_mode
class ObLogMetaDataSQLQueryer : public ObCDCTenantQuery<logfetcher::DataDictionaryInLogInfo>
{
public:
  ObLogMetaDataSQLQueryer(const int64_t start_timstamp_ns, common::ObMySQLProxy &sql_proxy)
    : ObCDCTenantQuery(sql_proxy), start_timstamp_ns_(start_timstamp_ns) {}
  ~ObLogMetaDataSQLQueryer() { start_timstamp_ns_ = OB_INVALID_TIMESTAMP; }
public:
  int get_data_dict_in_log_info(
      const uint64_t tenant_id,
      const int64_t start_timestamp_ns,
      logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info);
private:
  int build_sql_statement_(const uint64_t tenant_id, ObSqlString &sql) override;
  int parse_row_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<logfetcher::DataDictionaryInLogInfo> &result) override;
private:
  static const char* QUERY_SQL_FORMAT;
private:
  int64_t start_timstamp_ns_;
  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataSQLQueryer);
};

} // namespace libobcdc
} // namespace oceanbase

#endif

