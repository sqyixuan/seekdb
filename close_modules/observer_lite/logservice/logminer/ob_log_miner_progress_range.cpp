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

#include "ob_log_miner_progress_range.h"
#include "ob_log_miner_utils.h"

namespace oceanbase
{
namespace oblogminer
{

////////////////////////////// ObLogMinerProgressRange //////////////////////////////

const char *ObLogMinerProgressRange::min_commit_ts_key = "MIN_COMMIT_TS";
const char *ObLogMinerProgressRange::max_commit_ts_key = "MAX_COMMIT_TS";

int ObLogMinerProgressRange::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%ld\n%s=%ld\n",
      min_commit_ts_key, min_commit_ts_, max_commit_ts_key, max_commit_ts_))) {
    LOG_ERROR("failed to serialize progress range into buffer", K(buf_len), K(pos), KPC(this));
  }
  return ret;
}

int ObLogMinerProgressRange::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_line(min_commit_ts_key, buf, data_len, pos, min_commit_ts_))) {
    LOG_ERROR("parse line for min_commit_ts failed", K(min_commit_ts_key), K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(max_commit_ts_key, buf, data_len, pos, max_commit_ts_))) {
    LOG_ERROR("parse line for max_commit_ts failed", K(max_commit_ts_key), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObLogMinerProgressRange::get_serialize_size() const
{
  int64_t size = 0;
  const int64_t max_digit_size = 30;
  char digit_str[max_digit_size];
  size += snprintf(digit_str, max_digit_size, "%ld", min_commit_ts_);
  size += snprintf(digit_str, max_digit_size, "%ld", max_commit_ts_);
  // min_commit_ts_key + max_commit_ts_key + '=' + '\n' + '=' + '\n'
  size += strlen(min_commit_ts_key) + strlen(max_commit_ts_key) + 4;
  return size;
}

}
}
