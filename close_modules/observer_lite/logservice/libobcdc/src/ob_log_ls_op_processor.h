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

#ifndef OCEANBASE_LIBOBCDC_LOG_OP_PROCESSOR_H_
#define OCEANBASE_LIBOBCDC_LOG_OP_PROCESSOR_H_

#include "share/ls/ob_ls_operator.h"          // ObLSAttr
#include "logservice/palf/lsn.h"              // LSN
#include "ob_log_tenant.h"                    // ObLogTenant

namespace oceanbase
{
namespace libobcdc
{
class ObLogLSOpProcessor
{
public:
  ObLogLSOpProcessor() : inited_(false) {}
  virtual ~ObLogLSOpProcessor() { inited_ = false; }
  int init();
  void destroy();

  static int process_ls_op(
      const uint64_t tenant_id,
      const palf::LSN &lsn,
      const int64_t start_tstamp_ns,
      const share::ObLSAttr &ls_attr);

private:
  static int create_new_ls_(
      ObLogTenant *tenant,
      const int64_t start_tstamp_ns,
      const share::ObLSAttr &ls_attr);

private:
  bool inited_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
