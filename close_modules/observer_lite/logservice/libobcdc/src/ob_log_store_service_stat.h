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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_STORE_SERVICE_STAT_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_STORE_SERVICE_STAT_H_

#include "lib/utility/ob_print_utils.h"         // TO_STRING_KV

namespace oceanbase
{
namespace libobcdc
{
struct StoreServiceStatInfo
{
  int64_t total_data_size_ CACHE_ALIGNED;
  int64_t last_total_data_size_ CACHE_ALIGNED;

  StoreServiceStatInfo() { reset(); }
  ~StoreServiceStatInfo() { reset(); }

  void reset();

  void do_data_stat(int64_t record_len);

  double calc_rate(const int64_t delta_time);

  double get_total_data_size() const;

  TO_STRING_KV(K_(total_data_size),
               K_(last_total_data_size));
};

}
}

#endif
