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

#ifndef OCEANBASE_LOG_MINER_FILE_META_H_
#define OCEANBASE_LOG_MINER_FILE_META_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_log_miner_progress_range.h"

namespace oceanbase
{
namespace oblogminer
{

struct ObLogMinerFileMeta
{
public:

  static const char *data_len_key;

  ObLogMinerFileMeta() {reset();}
  ~ObLogMinerFileMeta() {reset();}

  void reset() {
    range_.reset();
    data_length_ = 0;
  }

  bool operator==(const ObLogMinerFileMeta &that) const
  {
    return range_ == that.range_ && data_length_ == that.data_length_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    K(range_),
    K(data_length_)
  )

public:
  ObLogMinerProgressRange range_;
  int64_t data_length_;
};

}
}

#endif
