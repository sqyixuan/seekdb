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

#ifndef OCEANBASE_LOG_META_DATA_BASELINE_LOADER_H_
#define OCEANBASE_LOG_META_DATA_BASELINE_LOADER_H_

#include "close_modules/observer_lite/logservice/data_dictionary/ob_data_dict_iterator.h"  // ObDataDictIterator
#include "ob_log_config.h"
#include "close_modules/observer_lite/logservice/logfetcher/ob_log_data_dictionary_in_log_table.h"
#include "ob_log_meta_data_struct.h"

namespace oceanbase
{
namespace libobcdc
{
class IObLogErrHandler;

class ObLogMetaDataBaselineLoader
{
public:
  ObLogMetaDataBaselineLoader();
  ~ObLogMetaDataBaselineLoader();

  int init(
      const ObLogConfig &cfg);
  void destroy();

public:
  // Create the tenant structure based on the Tenant ID
  //
  // @param [in]   tenant_id     Tenant ID
  //
  // @retval OB_SUCCESS          Add tenant success
  // @retval other_err_code      unexpected error
  int add_tenant(
      const uint64_t tenant_id);

  // Revert tenant
  //
  // @param [in]   tenant        Data Dictionary Tenant structure
  //
  // @retval OB_SUCCESS          Revert success
  // @retval other_err_code      unexpected error
  int revert_tenant(ObDictTenantInfo *tenant);

  // Get ObDictTenantInfoGuard based on the Tenant ID
  //
  // @param [in]   tenant_id     Tenant ID
  // @param [out]  guard         Return ObDictTenantInfoGuard
  //
  // @retval OB_SUCCESS          Add tenant success
  // @retval other_err_code      unexpected error
  int get_tenant_info_guard(
      const uint64_t tenant_id,
      ObDictTenantInfoGuard &guard);

  // Read log entry to get Data Dictionary
  //
  // @param [in]   buf                   buf contains data dictionary log
  // @param [in]   buf_len               length of buf
  // @param [in]   pos_after_log_header  pos in buf that after ObLogBaseHeader
  // @param [in]   submit_ts             submit_ts of log_entry(group_entry_log)
  //
  // @retval OB_SUCCESS          handle all data dictionary log in log_entry success
  // @retval OB_IN_STOP_STATE    EXIT
  // @retval other_err_code      unexpected error
  int read(
      const uint64_t tenant_id,
      datadict::ObDataDictIterator &data_dict_iterator,
      const char *buf,
      const int64_t buf_len,
      const int64_t pos_after_log_header,
      const palf::LSN &lsn,
      const int64_t submit_ts);

private:
  int get_tenant_(
      const uint64_t tenant_id,
      ObDictTenantInfo *&tenant_info);
  int revert_tenant_(ObDictTenantInfo *tenant);

private:
  bool is_inited_;
  DataDictTenantMap data_dict_tenant_map_;

  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataBaselineLoader);
};
} // namespace libobcdc
} // namespace oceanbase

#endif

