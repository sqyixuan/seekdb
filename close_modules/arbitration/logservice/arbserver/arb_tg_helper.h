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
#ifndef OCEANBASE_LOGSERVICE_ARB_TG_HELPER_
#define OCEANBASE_LOGSERVICE_ARB_TG_HELPER_

#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace arbserver
{
class ArbTGHelper : public share::ObTenantBase
{
public:
  ArbTGHelper(const int64_t cluster_id,
              const int64_t mtl_id,
              const uint64_t tenant_id_used_to_print_log);
  ~ArbTGHelper();
  virtual int pre_run() override;
  virtual int end_run() override;

  virtual void tg_create_cb(int tg_id) override;
  virtual void tg_destroy_cb(int tg_id) override;
private:
  int64_t cluster_id_;
  int64_t tenant_id_used_to_print_log_;
};

} // end namespace arbserver
} // end namespace oceanbase
#endif
