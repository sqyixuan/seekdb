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

#ifndef OCEANBASE_LOG_MINER_PROGRESS_RANGE_H_
#define OCEANBASE_LOG_MINER_PROGRESS_RANGE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace oblogminer
{

struct ObLogMinerProgressRange
{
  static const char *min_commit_ts_key;
  static const char *max_commit_ts_key;

  ObLogMinerProgressRange()
  { reset(); }

  void reset()
  {
    min_commit_ts_ = OB_INVALID_TIMESTAMP;
    max_commit_ts_ = OB_INVALID_TIMESTAMP;
  }

  bool is_valid() const
  {
    return min_commit_ts_ != OB_INVALID_TIMESTAMP && max_commit_ts_ != OB_INVALID_TIMESTAMP;
  }

  ObLogMinerProgressRange &operator=(const ObLogMinerProgressRange &that)
  {
    min_commit_ts_ = that.min_commit_ts_;
    max_commit_ts_ = that.max_commit_ts_;
    return *this;
  }

  bool operator==(const ObLogMinerProgressRange &that) const
  {
    return min_commit_ts_ == that.min_commit_ts_ && max_commit_ts_ == that.max_commit_ts_;
  }

  TO_STRING_KV(
    K(min_commit_ts_),
    K(max_commit_ts_)
  );

  NEED_SERIALIZE_AND_DESERIALIZE;

  int64_t min_commit_ts_;
  int64_t max_commit_ts_;
};

}
}
#endif
