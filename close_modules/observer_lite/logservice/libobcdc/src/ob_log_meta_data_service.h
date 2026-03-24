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

#ifndef OCEANBASE_LOG_META_DATA_SERVICE_H_
#define OCEANBASE_LOG_META_DATA_SERVICE_H_

#include "common/ob_region.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"  // ObConcurrentFIFOAllocator
#include "share/backup/ob_backup_struct.h"  // ObBackupPathString
#include "close_modules/observer_lite/logservice/data_dictionary/ob_data_dict_meta_info.h" // ObDataDictMetaInfo
#include "ob_log_fetching_mode.h"           // ClientFetchingMode
#include "ob_log_config.h"
#include "ob_log_task_pool.h"
#include "ob_log_entry_task_pool.h"
#include "close_modules/observer_lite/logservice/logfetcher/ob_log_data_dictionary_in_log_table.h"
#include "ob_log_meta_data_baseline_loader.h"
#include "ob_log_meta_data_replayer.h" // ObLogMetaDataReplayer
#include "ob_log_meta_data_fetcher.h"  // ObLogMetaDataFetcher
#include "ob_log_meta_data_fetcher_dispatcher.h"  // ObLogMetaDataFetcherDispatcher

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace libobcdc
{
class IObLogSysLsTaskHandler;
class ObLogSysTableHelper;
class IObLogErrHandler;
class IObLogPartTransParser;

class ObLogMetaDataService
{
  // 15 day
  static const int64_t DATADICT_META_RECYCLE_INTERVAL_NS = 15L * 24 * 60 * 60 * 1000 * 1000 * 1000;
public:
  ObLogMetaDataService();
  ~ObLogMetaDataService();

  int init(
      const int64_t start_tstamp_ns,
      const ClientFetchingMode fetching_mode,
      const share::ObBackupPathString &archive_dest,
      IObLogSysLsTaskHandler *sys_ls_handler,
      common::ObMySQLProxy *proxy,
      IObLogErrHandler *err_handler,
      IObLogPartTransParser &part_trans_parser,
      const int64_t cluster_id,
      const ObLogConfig &cfg,
      const int64_t start_seq,
      const bool enable_direct_load_inc);
  void destroy();

  static ObLogMetaDataService &get_instance();
  ObLogMetaDataBaselineLoader &get_baseline_loader() { return baseline_loader_; }

public:
  // Refresh baseline meta data based on Tenant ID
  //
  // @param [in]    tenant_id          Tenant ID
  // @param [in]    start_timestamp_ns start timestamp(ns)
  // @param [in]    timeout            Timeout
  //
  // @retval OB_SUCCESS        Success
  // @retval other error code  Fail
  int refresh_baseline_meta_data(
      const uint64_t tenant_id,
      const int64_t start_timestamp_ns,
      const int64_t timeout);

  // Call the function when all tenants are referenced at statup time
  //
  // @retval OB_SUCCESS        Success
  // @retval other error code  Fail
  int finish_when_all_tennats_are_refreshed();

  int read(
      const uint64_t tenant_id,
      datadict::ObDataDictIterator &data_dict_iterator,
      const char *buf,
      const int64_t buf_len,
      const int64_t pos_after_log_header,
      const palf::LSN &lsn,
      const int64_t submit_ts);

  int get_tenant_info_guard(
      const uint64_t tenant_id,
      ObDictTenantInfoGuard &guard);

  int get_tenant_id_in_archive(
      const int64_t start_timestamp_ns,
      uint64_t &tenant_id);

private:
  int get_data_dict_in_log_info_(
      const uint64_t tenant_id,
      const int64_t start_timestamp_ns,
      logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info);

  int get_data_dict_in_log_info_in_archive_(
      const int64_t start_timestamp_ns,
      logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info);

  int read_meta_info_in_archive_log_(
      const int64_t start_timestamp_ns,
      datadict::ObDataDictMetaInfo &data_dict_meta_info);

private:
  bool is_inited_;
  ObLogMetaDataFetcher fetcher_;
  ObLogMetaDataBaselineLoader baseline_loader_;
  ObLogMetaDataReplayer incremental_replayer_;
  ObLogMetaDataFetcherDispatcher fetcher_dispatcher_;
  IObLogPartTransParser *part_trans_parser_;

  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataService);
};

#define GLOGMETADATASERVICE (::oceanbase::libobcdc::ObLogMetaDataService::get_instance())

} // namespace libobcdc
} // namespace oceanbase

#endif
