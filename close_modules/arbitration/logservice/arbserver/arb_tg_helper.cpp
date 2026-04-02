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
#include "arb_tg_helper.h"

namespace oceanbase
{
namespace arbserver
{
ArbTGHelper::ArbTGHelper(const int64_t cluster_id,
                         const int64_t mtl_id,
                         const uint64_t tenant_id_used_to_print_log)
                         : ObTenantBase(mtl_id), cluster_id_(cluster_id), tenant_id_used_to_print_log_(tenant_id_used_to_print_log)
{
}

ArbTGHelper::~ArbTGHelper()
{
  cluster_id_ = -1; 
  tenant_id_used_to_print_log_ = -1;
}

int ArbTGHelper::pre_run()
{
  ob_get_tenant_id() = id_;
  ob_get_arb_tenant_id() = tenant_id_used_to_print_log_;
  ob_get_cluster_id() = cluster_id_; 
  CLOG_LOG(INFO, "ArbTGHelper pre_run"); 
  return OB_SUCCESS;
}

int ArbTGHelper::end_run()
{
  CLOG_LOG(INFO, "ArbTGHelper end_run"); 
  ob_get_tenant_id() = 0;
  ob_get_arb_tenant_id() = 0;
  ob_get_cluster_id() = 0; 
  return OB_SUCCESS;
}

void ArbTGHelper::tg_create_cb(int tg_id)
{
}

void ArbTGHelper::tg_destroy_cb(int tg_id)
{
}

} // end namespace oceanbase
} // end namespace oceanbase
