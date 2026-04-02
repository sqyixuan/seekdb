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

#ifndef OCEANBASE_SHARED_STORAGE_SHARE_OB_FILE_UTIL_H_
#define OCEANBASE_SHARED_STORAGE_SHARE_OB_FILE_UTIL_H_

#include <sys/stat.h>

namespace oceanbase
{
namespace share
{

// Retry on EINTR for functions like open() that return -1 on error.
#define RETRY_ON_EINTR(err, expr) do { \
  static_assert(std::is_signed<decltype(err)>::value, \
                #err " must be a signed integer"); \
  (err) = (expr); \
  if (((err) == -1) && (errno == EINTR)) { \
    OB_LOG(WARN, "retry on EINTR"); \
  } \
} while (((err) == -1) && (errno == EINTR))

class ObSSFileUtil
{
public:
  static int open(const char *file_path, int open_flag, mode_t open_mode, int &fd);
  static int close(const int fd);
  static int fsync_file(const char *file_path);
};

} /* namespace share */
} /* namespace oceanbase */

#endif /* OCEANBASE_SHARED_STORAGE_SHARE_OB_FILE_UTIL_H_ */
