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

#include "ob_arb_monitor.h"

namespace oceanbase
{
namespace arbserver
{
#define LOG_MONITOR_EVENT_FMT_PREFIX "LOG", type_to_string_(event), "TENANT_ID", mtl_id, "LS_ID", palf_id

// =========== PALF Event Reporting ===========
int ObArbMonitor::record_create_or_delete_event(const int64_t cluster_id,
                                                const uint64_t tenant_id,
                                                const int64_t ls_id,
                                                const bool is_create,
                                                const char *extra_info)
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}
// =========== PALF Event Reporting ===========

#undef LOG_MONITOR_EVENT_FMT_PREFIX
} // end namespace logservice
} // end namespace oceanbase
