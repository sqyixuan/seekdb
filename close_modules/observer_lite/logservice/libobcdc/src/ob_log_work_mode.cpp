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

#include "ob_log_work_mode.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

const char *print_working_mode(const WorkingMode mode)
{
  const char *mode_str = "INVALID";

  switch (mode) {
    case MEMORY_MODE: {
      mode_str = "Memory Working Mode";
      break;
    }
    case STORAGER_MODE: {
      mode_str = "Storager Working Mode";
      break;
    }
    case AUTO_MODE: {
      mode_str = "Auto Working Mode";
      break;
    }
    default: {
      mode_str = "INVALID";
      break;
    }
  }

  return mode_str;
}

// TODO support auto mode
WorkingMode get_working_mode(const char *working_mode_str)
{
  WorkingMode ret_mode = UNKNOWN_MODE;

  if (OB_ISNULL(working_mode_str)) {
  } else {
    if (0 == strcmp("memory", working_mode_str)) {
      ret_mode = MEMORY_MODE;
    } else if (0 == strcmp("storage", working_mode_str)) {
      ret_mode = STORAGER_MODE;
    } else {
    }
  }

  return ret_mode;
}

bool is_working_mode_valid(WorkingMode mode)
{
  bool bool_ret = false;

  bool_ret = (mode > WorkingMode::UNKNOWN_MODE)
    && (mode < WorkingMode::MAX_MODE);

  return bool_ret;
}

bool is_memory_working_mode(const WorkingMode mode)
{
  return WorkingMode::MEMORY_MODE == mode;
}

bool is_storage_working_mode(const WorkingMode mode)
{
  return WorkingMode::STORAGER_MODE == mode;
}

bool is_auto_working_mode(const WorkingMode mode)
{
  return WorkingMode::AUTO_MODE == mode;
}

}
}
