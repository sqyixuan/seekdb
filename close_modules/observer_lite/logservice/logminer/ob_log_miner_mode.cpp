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

#include "ob_log_miner_mode.h"

namespace oceanbase
{
namespace oblogminer
{

const char *logminer_mode_to_str(LogMinerMode mode)
{
  const char *mode_str = nullptr;
  switch(mode) {
    case LogMinerMode::UNKNOWN: {
      mode_str = "UNKNOWN";
      break;
    }

    case LogMinerMode::ANALYSIS: {
      mode_str = "ANALYSIS";
      break;
    }

    case LogMinerMode::FLASHBACK: {
      mode_str = "FLASHBACK";
      break;
    }

    case LogMinerMode::MAX_MODE: {
      mode_str = "MAX_MODE";
      break;
    }

    default: {
      mode_str = "INVALID";
      break;
    }
  }

  return mode_str;
}

LogMinerMode get_logminer_mode(const common::ObString &mode_str)
{
  LogMinerMode mode = LogMinerMode::UNKNOWN;
  if (0 == mode_str.case_compare("analysis")) {
    mode = LogMinerMode::ANALYSIS;
  } else if (0 == mode_str.case_compare("flashback")) {
    mode = LogMinerMode::FLASHBACK;
  } else {
    // return UNKNOWN mode
  }
  return mode;
}

LogMinerMode get_logminer_mode(const char *mode_str)
{
  LogMinerMode mode = LogMinerMode::UNKNOWN;

  if (0 == strcasecmp(mode_str, "analysis")) {
    mode = LogMinerMode::ANALYSIS;
  } else if (0 == strcasecmp(mode_str, "flashback")) {
    mode = LogMinerMode::FLASHBACK;
  } else {
    // return UNKNOWN mode
  }

  return mode;
}

bool is_logminer_mode_valid(const LogMinerMode mode)
{
  return mode > LogMinerMode::UNKNOWN && mode < LogMinerMode::MAX_MODE;
}

bool is_analysis_mode(const LogMinerMode mode)
{
  return LogMinerMode::ANALYSIS == mode;
}

bool is_flashback_mode(const LogMinerMode mode)
{
  return LogMinerMode::FLASHBACK == mode;
}

}
}
