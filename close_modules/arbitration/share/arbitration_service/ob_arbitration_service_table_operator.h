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

#ifndef OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_TABLE_OPERATOR_H_

#include "lib/mysqlclient/ob_mysql_result.h" // for ObMySQLResult

namespace oceanbase
{
namespace share
{
class ObArbitrationServiceInfo;
// [class_full_name] ObArbitrationServiceInfo
// [class_functions] Use this class to build info in __all_arbitration_service
// [class_attention] None
class ObArbitrationServiceTableOperator
{
  OB_UNIS_VERSION(1);
public:
  ObArbitrationServiceTableOperator() {}
  virtual ~ObArbitrationServiceTableOperator() {}
  // get infos of arbitration service by key
  // @params[in]  sql_proxy, sql_proxy to get infos
  // @params[in]  arbitration_service_key, key to identify a certain service
  // @params[in]  lock_line, whether to use select for update
  // @params[out] arbitration_service_info, the info to get
  // @return OB_ARBITRATION_SERVICE_NOT_EXIST if this service not exist
  static int get(
      common::ObISQLClient &sql_proxy,
      const ObString &arbitration_service_key,
      bool lock_line,
      ObArbitrationServiceInfo &arbitration_service_info);
  // insert a new info of arbitration service
  // @params[in]  sql_proxy, sql_proxy to get infos
  // @params[in]  arbitration_service_info, the info to update
  // @return OB_ARBITRATION_SERVICE_ALREADY_EXIST if this service already exist
  static int insert(
      common::ObISQLClient &sql_proxy,
      const ObArbitrationServiceInfo &arbitration_service_info);
  // update arbitration service info
  // @params[in]  sql_proxy, sql_proxy to update infos
  // @params[in]  arbitration_service_info, the info to update
  // @return OB_ARBITRATION_SERVICE_NOT_EXIST if this service not exist
  static int update(
      common::ObISQLClient &sql_proxy,
      const ObArbitrationServiceInfo &arbitration_service_info);
  // remove arbitration service info by key
  // @params[in]  sql_proxy, sql_proxy to update infos
  // @params[in]  arbitration_service_key, the key of the info
  // @return OB_ARBITRATION_SERVICE_NOT_EXIST if this service not exist
  static int remove(
    common::ObISQLClient &sql_proxy,
    const ObString &arbitration_service_key);
private:
  // construct arbitration service info from sql res
  // @params[in]  res, result read from table
  // @params[out] arbitration_service_info, the info to build
  static int construct_arbitration_service_info_(
      const common::sqlclient::ObMySQLResult &res,
      ObArbitrationServiceInfo &arbitration_service_info);
};
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_TABLE_OPERATOR_H_
