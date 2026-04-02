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

#ifndef OCEANBASE_LOGSERVICE_OB_ARB_MONITOR_H_
#define OCEANBASE_LOGSERVICE_OB_ARB_MONITOR_H_

#include "logservice/palf/palf_callback.h"

namespace oceanbase
{
namespace arbserver
{

class ObArbMonitor : public palf::PalfLiteMonitorCb
{
public:
  ObArbMonitor() { }
  virtual ~ObArbMonitor() { destroy(); }
  int init() { return OB_SUCCESS; }
  void destroy() { }
  // PALF event reporting
public:
  // =========== PALF Event Reporting ===========
  int record_create_or_delete_event(const int64_t cluster_id,
                                    const uint64_t tenant_id,
                                    const int64_t ls_id,
                                    const bool is_create,
                                    const char *extra_info = NULL) override final;
  // =========== PALF Event Reporting ===========
private:
  enum EventType
  {
    UNKNOWN = 0,
    ADD_CLUSTER,
    REMOVE_CLUSTER,
    ADD_TENANT,
    REMOVE_TENANT,
    ADD_LS,
    REMOVE_LS,
  };

  const char *type_to_string_(const EventType &event) const
  {
    switch (event)
    {
      case (EventType::ADD_CLUSTER):
        return "ADD CLUSTER";
      case (EventType::REMOVE_CLUSTER):
        return "REMOVE CLUSTER";
      case (EventType::ADD_TENANT):
        return "ADD TENANT";
      case (EventType::REMOVE_TENANT):
        return "REMOVE TENANT";
      case (EventType::ADD_LS):
        return "ADD LS";
      case (EventType::REMOVE_LS):
        return "REMOVE LS";
      default:
        return "UNKNOWN";
    }
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObArbMonitor);
};

} // arbserver
} // oceanbase

#endif
