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

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_global_info.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace libobcdc
{
ObCDCGlobalInfo::ObCDCGlobalInfo() :
  lob_aux_table_schema_info_(),
  min_cluster_version_(0)
{
}

int ObCDCGlobalInfo::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lob_aux_table_schema_info_.init())) {
    LOG_ERROR("table_schema_ init failed", KR(ret));
  } else {}

  return ret;
}

void ObCDCGlobalInfo::reset()
{
  lob_aux_table_schema_info_.reset();
  min_cluster_version_ = 0;
}

} // namespace libobcdc
} // namespace oceanbase
