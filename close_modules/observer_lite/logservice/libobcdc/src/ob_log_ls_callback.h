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

#ifndef OCEANBASE_LOG_LS_CALLBACK_H_
#define OCEANBASE_LOG_LS_CALLBACK_H_

#include "lib/container/ob_se_array.h"        // ObSEArray
#include "logservice/palf/lsn.h"              // LSN
#include "logservice/common_util/ob_log_ls_define.h"                 // logservice::TenantLSID
#include "close_modules/observer_lite/logservice/logfetcher/ob_log_fetcher_start_parameters.h"  // logfetcher::ObLogFetcherStartParameters

namespace oceanbase
{
namespace libobcdc
{
struct LSAddCallback
{
public:
  virtual ~LSAddCallback() {}

public:
  // Add LS
  virtual int add_ls(const logservice::TenantLSID &tls_id,
      const logfetcher::ObLogFetcherStartParameters &start_parameters) = 0;
};

struct LSRecycleCallback
{
public:
  virtual ~LSRecycleCallback() {}

public:
  // Recycling LS
  virtual int recycle_ls(const logservice::TenantLSID &tls_id) = 0;
};

typedef common::ObSEArray<int64_t, 4> LSCBArray;

} // namespace libobcdc
} // namespace oceanbase

#endif
