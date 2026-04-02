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

#ifndef OCEANBASE_LIBOBCDC_BUF_H_
#define OCEANBASE_LIBOBCDC_BUF_H_

#include "common/data_buffer.h"

namespace oceanbase
{
namespace libobcdc
{
template <int64_t BUFSIZE>
class FixedBuf
{
public:
  FixedBuf() { reset(); }
  ~FixedBuf() { reset(); }
  void reset()
  {
    buf_[0] = '\0';
    pos_ = 0;
  }
  void destroy() { reset(); }

  char *get_buf() { return buf_; }
  int64_t get_size() const { return BUFSIZE; }
  int64_t get_len() const { return pos_; }
  int fill(const char *data, const int64_t data_len)
  {
    int ret = common::OB_SUCCESS;

    if (OB_ISNULL(data) || OB_UNLIKELY(data_len <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (OB_UNLIKELY(pos_ + data_len >= BUFSIZE)) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf_ + pos_, data, data_len);
      pos_ += data_len;
      buf_[pos_] = '\0';
    }

    return ret;
  }

public:
  static const int64_t RP_MAX_FREE_LIST_NUM = 1024;

private:
  char buf_[BUFSIZE];
  int64_t pos_;
};

struct SimpleBuf
{
  SimpleBuf();
  ~SimpleBuf();

  int init(const int64_t size);
  void destroy();

  int alloc(const int64_t sz, void *&ptr);
  void free();

  int64_t get_buf_len() const { return buf_len_; }
  const char *get_buf() const;

  common::ObDataBuffer data_buf_;
  bool use_data_buf_;
  char *big_buf_;
  int64_t buf_len_;
};


} // namespace libobcdc
} // namespace oceanbase
#endif
