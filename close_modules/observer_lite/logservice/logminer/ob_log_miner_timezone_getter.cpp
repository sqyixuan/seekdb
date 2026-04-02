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

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_timezone_getter.h"
#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace oblogminer
{

ObLogMinerTimeZoneGetter &ObLogMinerTimeZoneGetter::get_instance()
{
  static ObLogMinerTimeZoneGetter tz_getter_instance;
  return tz_getter_instance;
}

ObLogMinerTimeZoneGetter::ObLogMinerTimeZoneGetter():
    tz_info_() { }

int ObLogMinerTimeZoneGetter::set_timezone(const char *timezone)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_.set_timezone(ObString(timezone)))) {
    LOG_ERROR("parse timezone failed", K(ret), KCSTRING(timezone));
  }
  return ret;
}

}
}
