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

#ifndef OCEANBASE_LOG_MINER_ANALYZER_CHECKPOINT_H_
#define OCEANBASE_LOG_MINER_ANALYZER_CHECKPOINT_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace oblogminer
{

struct ObLogMinerCheckpoint
{
public:
  ObLogMinerCheckpoint() { reset(); }
  ~ObLogMinerCheckpoint() { reset(); }

  void reset() {
    progress_ = OB_INVALID_TIMESTAMP;
    max_file_id_ = -1;
    cur_file_id_ = -1;
  }

  ObLogMinerCheckpoint &operator=(const ObLogMinerCheckpoint &that)
  {
    progress_ = that.progress_;
    max_file_id_ = that.max_file_id_;
    cur_file_id_ = that.cur_file_id_;
    return *this;
  }

  bool operator==(const ObLogMinerCheckpoint &that) const
  {
    return progress_ == that.progress_ && cur_file_id_ == that.cur_file_id_ && max_file_id_ == that.max_file_id_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    K(progress_),
    K(max_file_id_),
    K(cur_file_id_)
  );
private:
  static const char *progress_key_str;
  static const char *cur_file_id_key_str;
  static const char *max_file_id_key_str;

public:
  int64_t progress_;
  int64_t max_file_id_;
  int64_t cur_file_id_;
};

}
}

#endif
