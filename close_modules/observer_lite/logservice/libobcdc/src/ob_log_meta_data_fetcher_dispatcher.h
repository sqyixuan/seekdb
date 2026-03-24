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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_META_DATA_FETCHER_DISPATCHER
#define OCEANBASE_LIBOBCDC_OB_LOG_META_DATA_FETCHER_DISPATCHER

#include "lib/utility/ob_macro_utils.h"         // DISALLOW_COPY_AND_ASSIGN, CACHE_ALIGNED
#include "ob_log_fetcher_dispatcher_interface.h"  // IObLogFetcherDispatcher
#include "ob_log_utils.h"
#include "ob_log_meta_data_replayer.h"

namespace oceanbase
{
namespace libobcdc
{
class ObLogMetaDataFetcherDispatcher : public IObLogFetcherDispatcher
{
  static const int64_t DATA_OP_TIMEOUT = 10 * _SEC_;

public:
  ObLogMetaDataFetcherDispatcher();
  virtual ~ObLogMetaDataFetcherDispatcher();

  virtual int dispatch(PartTransTask &task, volatile bool &stop_flag);

public:
  int init(
      IObLogMetaDataReplayer *log_meta_data_replayer,
      const int64_t start_seq);
  void destroy();

private:
  int dispatch_to_log_meta_data_replayer_(PartTransTask &task, volatile bool &stop_flag);

private:
  bool is_inited_;
  IObLogMetaDataReplayer *log_meta_data_replayer_;

  // DML and Global HeartBeat checkpoint seq
  // DDL global checkpoint seq:
  // 1. DDL trans
  // 2. DDL HeartBeat
  // 3. DDL Offline Task
  int64_t           checkpoint_seq_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataFetcherDispatcher);
};

} // namespace libobcdc
} // namespace oceanbase

#endif

