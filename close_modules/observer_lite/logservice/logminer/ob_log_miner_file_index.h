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

#ifndef OCEANBASE_LOG_MINER_FILE_INDEX_H_
#define OCEANBASE_LOG_MINER_FILE_INDEX_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_log_miner_progress_range.h"

namespace oceanbase
{
namespace oblogminer
{

class FileIndexItem
{
public:
  FileIndexItem() { reset(); }
  FileIndexItem(const int64_t file_id, const int64_t min_commit_ts, const int64_t max_commit_ts)
  { reset(file_id, min_commit_ts, max_commit_ts); }
  ~FileIndexItem() { reset(); }

  void reset() {
    file_id_ = -1;
    range_.reset();
  }

  void reset(const int64_t file_id, const int64_t min_commit_ts, const int64_t max_commit_ts)
  {
    file_id_ = file_id;
    range_.min_commit_ts_ = min_commit_ts;
    range_.max_commit_ts_ = max_commit_ts;
  }

  bool operator==(const FileIndexItem &that) const
  {
    return file_id_ == that.file_id_ && range_ == that.range_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    K(file_id_),
    K(range_)
  )

  int64_t file_id_;
  ObLogMinerProgressRange range_;
};

class FileIndex
{
public:
  FileIndex():
      alloc_(),
      index_file_len_(0),
      index_array_() {}
  ~FileIndex() { reset(); }

  void reset() {
    index_file_len_ = 0;
    index_array_.reset();
    alloc_.reset();
  }

  int insert_index_item(const int64_t file_id, const int64_t min_commit_ts, const int64_t max_commit_ts);

  int insert_index_item(const FileIndexItem &item);

  int get_index_item(const int64_t file_id, FileIndexItem *&item) const;

  int64_t get_index_file_len() const;

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    "file_num", index_array_.count()
  )
private:
  int alloc_index_item_(FileIndexItem *&item);

private:
  ObArenaAllocator alloc_;
  int64_t index_file_len_;
  ObSEArray<FileIndexItem* , 16> index_array_;
};

}
}

#endif
