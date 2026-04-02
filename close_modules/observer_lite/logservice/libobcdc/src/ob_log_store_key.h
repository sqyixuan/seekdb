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

#ifndef OCEANBASE_LIBOBCDC_STORE_KEY_H_
#define OCEANBASE_LIBOBCDC_STORE_KEY_H_

#include <stdint.h>
#include <string>
#include "lib/utility/ob_macro_utils.h"       // DISALLOW_COPY_AND_ASSIGN
#include "ob_log_utils.h"                     // logservice::TenantLSID

namespace oceanbase
{
namespace libobcdc
{
class ObLogStoreKey
{
public:
  ObLogStoreKey();
  ~ObLogStoreKey();
  void reset();
  int init(const logservice::TenantLSID &tenant_ls_id, const palf::LSN &log_lsn);
  bool is_valid() const;
  uint64_t get_tenant_id() const { return tenant_ls_id_.get_tenant_id(); }

public:
  int get_key(std::string &key);
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  logservice::TenantLSID  tenant_ls_id_;
  // StorageKey: tenant_ls_id_+log_lsn_
  // Log LSN, for redo data
  // 1. non-LOB record corresponding to LogEntry log_lsn
  // 2. First LogEntry log_lsn for LOB records
  palf::LSN     log_lsn_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStoreKey);
};

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
