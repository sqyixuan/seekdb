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

#ifndef OCEANBASE_SHARE_OB_LICENSE_H
#define OCEANBASE_SHARE_OB_LICENSE_H

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace share
{
struct ObLicense
{
  ObLicense()
      : issuance_date_(0), expiration_time_(0), activation_time_(0), core_num_(0), node_num_(0),
       license_trail_(0), allow_stand_by_(0), allow_multi_tenant_(0), allow_olap_(0), ref_cnt_(0)
  {
  }
  using ObString = common::ObString;

  TO_STRING_KV(K_(license_type),
               K_(product_type),
               K_(end_user),
               K_(license_id),
               K_(license_code),
               KTIME_(issuance_date),
               KTIME_(expiration_time),
               KTIME_(activation_time),
               K_(options),
               K_(core_num),
               K_(node_num),
               K_(license_trail),
               K_(allow_stand_by),
               K_(allow_multi_tenant),
               K_(allow_olap),
               K_(ref_cnt));
  int64_t inc_ref()
  {
    int64_t ref_cnt = ATOMIC_AAF(&ref_cnt_, 1);
    return ref_cnt;
  }
  void dec_ref()
  {
    if (0 == ATOMIC_SAF(&ref_cnt_, 1)) {
      ob_free(this);
    }
  }
  ObString license_type_;
  ObString product_type_;
  ObString end_user_;
  ObString license_id_;
  ObString license_code_;
  uint64_t issuance_date_;   /* timestamp(6) microseconds */
  uint64_t expiration_time_; /* timestamp(6) microseconds */
  uint64_t activation_time_; /* timestamp(6) microseconds */
  ObString options_;
  int64_t core_num_;
  int64_t node_num_;
  bool license_trail_;
  bool allow_stand_by_;
  bool allow_multi_tenant_;
  bool allow_olap_;
  int64_t ref_cnt_;
  ObArenaAllocator allocator_;
};
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_LICENSE_H
