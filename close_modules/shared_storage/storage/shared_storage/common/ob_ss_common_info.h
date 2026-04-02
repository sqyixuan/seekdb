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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_COMMON_INFO_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_COMMON_INFO_H_

#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

const bool OB_SS_ENABLE_LOCAL_CACHE_SYSSTAT = true;

static constexpr char OB_SS_MICRO_CACHE_NAME[] = "ss_micro_cache";
static constexpr char OB_SS_LOCAL_CACHE_TMPFILE_NAME[] = "ss_local_cache_tmpfile";
static constexpr char OB_SS_LOCAL_CACHE_MAJOR_MACRO_NAME[] = "ss_local_cache_major_macro";
static constexpr char OB_SS_LOCAL_CACHE_PRIVATE_MACRO_NAME[] = "ss_local_cache_private_macro";
static constexpr int OB_SS_MICRO_CACHE_PRIORITY = 0;
static constexpr int OB_LOCAL_CACHE_TMPFILE_PRIORITY = 0;
static constexpr int OB_LOCAL_CACHE_MAJOR_MACRO_PRIORITY = 0;
static constexpr int OB_LOCAL_CACHE_PRIVATE_MACRO_PRIORITY = 0;

int set_ss_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info);

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_COMMON_INFO_H_ */
