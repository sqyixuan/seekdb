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

#ifndef OCEANBASE_SHARE_OB_LICENSE_TIMESTAMP_SERVICE_H
#define OCEANBASE_SHARE_OB_LICENSE_TIMESTAMP_SERVICE_H

#include "lib/ob_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
class ObLicenseTimestampService
{
private:
  static int64_t UPDATE_TIME_DURATION;
public:
  ObLicenseTimestampService()
      : is_start_(0), current_time_(0), last_tsc_(0), cpu_mhz_(0), lock_(), is_unittest_(0), modified_sys_time_(0)
  {
  }
  int start_from_inner_table();
  int start_with_time(int64_t time);
  int update_time();
  int get_time(int64_t &time);
  TO_STRING_KV(K_(is_start),
               KTIME_(current_time),
               K_(last_tsc));
private:
  int store_to_inner_table(int64_t time);
  int load_from_inner_table(int64_t &time);
  int64_t get_current_sys_time();
  int is_master_node(bool &is_master);
  bool is_start_;
  int64_t current_time_;
  int64_t last_tsc_;
  int64_t cpu_mhz_;
  common::ObSpinLock lock_;
  bool is_unittest_; // only for unit test
  int64_t modified_sys_time_; // only for unit test
};
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_OB_LICENSE_TIMESTAMP_SERVICE_H
