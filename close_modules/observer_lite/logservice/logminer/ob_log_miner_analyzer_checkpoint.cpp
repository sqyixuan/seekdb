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

#include "ob_log_miner_analyzer_checkpoint.h"
#include "ob_log_miner_utils.h"

namespace oceanbase
{
namespace oblogminer
{

////////////////////////////// ObLogMinerCheckpoint //////////////////////////////
const char * ObLogMinerCheckpoint::progress_key_str = "PROGRESS";
const char * ObLogMinerCheckpoint::cur_file_id_key_str = "CUR_FILE_ID";
const char * ObLogMinerCheckpoint::max_file_id_key_str = "MAX_FILE_ID";

int ObLogMinerCheckpoint::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%ld\n%s=%ld\n%s=%ld\n",
      progress_key_str, progress_, cur_file_id_key_str, cur_file_id_, max_file_id_key_str, max_file_id_))) {
    LOG_ERROR("failed to fill formatted checkpoint into buffer", K(buf_len), K(pos), KPC(this));
  }
  return ret;
}

int ObLogMinerCheckpoint::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(parse_line(progress_key_str, buf, data_len, pos, progress_))) {
    LOG_ERROR("parse progress failed", K(progress_key_str), K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(cur_file_id_key_str, buf, data_len, pos, cur_file_id_))) {
    LOG_ERROR("parse cur_file_id failed", K(cur_file_id_key_str), K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(max_file_id_key_str, buf, data_len, pos, max_file_id_))) {
    LOG_ERROR("parse max_file_id failed", K(max_file_id_key_str), K(data_len), K(pos));
  }

  return ret;
}

int64_t ObLogMinerCheckpoint::get_serialize_size() const
{
  int64_t len = 0;
  const int64_t digit_max_len = 30;
  char digit[digit_max_len] = {0};
  len += snprintf(digit, digit_max_len, "%ld", progress_);
  len += snprintf(digit, digit_max_len, "%ld", cur_file_id_);
  len += snprintf(digit, digit_max_len, "%ld", max_file_id_);
  len += strlen(progress_key_str) + 2 + strlen(cur_file_id_key_str) + 2 + strlen(max_file_id_key_str) + 2;
  return len;
}



}
}
