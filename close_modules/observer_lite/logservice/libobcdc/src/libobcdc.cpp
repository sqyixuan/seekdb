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


#include "libobcdc.h"
#include <locale.h>

#include "ob_log_instance.h"        // ObLogInstance

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

ObCDCFactory::ObCDCFactory()
{
  // set max memory limit
  lib::set_memory_limit(get_phy_mem_size() * MAX_MEMORY_USAGE_PERCENT / 100);

  CURLcode curl_code = curl_global_init(CURL_GLOBAL_ALL);

  if (OB_UNLIKELY(CURLE_OK != curl_code)) {
    OBLOG_LOG_RET(ERROR, OB_ERR_SYS, "curl_global_init fail", K(curl_code));
  }

  setlocale(LC_ALL, "");
  setlocale(LC_TIME, "en_US.UTF-8");
}

ObCDCFactory::~ObCDCFactory()
{
  curl_global_cleanup();
}

IObCDCInstance *ObCDCFactory::construct_obcdc()
{
  return ObLogInstance::get_instance();
}

void ObCDCFactory::deconstruct(IObCDCInstance *log)
{
  UNUSED(log);

  ObLogInstance::destroy_instance();
}

} // namespace libobcdc
} // namespace oceanbase
