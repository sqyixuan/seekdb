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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_buf.h"
#include "ob_log_utils.h"       // ob_cdc_malloc, ob_cdc_free

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
SimpleBuf::SimpleBuf() :
    data_buf_(),
    use_data_buf_(true),
    big_buf_(NULL),
    buf_len_(0)
{
}

SimpleBuf::~SimpleBuf()
{
  destroy();
}

int SimpleBuf::init(const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(size <= 0)) {
    LOG_ERROR("invalid argument", K(size));
    ret = OB_INVALID_ARGUMENT;
  } else {
    void *ptr = ob_cdc_malloc(size, "CDCSimpleBuf");

    if (OB_ISNULL(ptr)) {
      LOG_ERROR("ptr is NULL");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      data_buf_.set_data(static_cast<char *>(ptr), size);
      use_data_buf_ = true;
      buf_len_ = 0;
    }
  }

  return ret;
}

void SimpleBuf::destroy()
{
  char *data = data_buf_.get_data();
  if (NULL != data) {
    ob_cdc_free(data);
    data_buf_.reset();
  }

  if (NULL != big_buf_) {
    ob_cdc_free(big_buf_);
    big_buf_ = NULL;
  }

  use_data_buf_ = true;
  buf_len_ = 0;
}

int SimpleBuf::alloc(const int64_t sz, void *&ptr)
{
  int ret = OB_SUCCESS;
  ptr = data_buf_.alloc(sz);
  buf_len_ = sz;

  if (NULL != ptr) {
    use_data_buf_ = true;
  } else {
    if (OB_ISNULL(big_buf_ = static_cast<char *>(ob_cdc_malloc(sz, "CDCSimpleBuf")))) {
      LOG_ERROR("alloc big_buf_ fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      use_data_buf_ = false;
      ptr = big_buf_;
    }
  }

  return ret;
}

void SimpleBuf::free()
{
  if (use_data_buf_) {
    data_buf_.free();
  } else {
    if (NULL != big_buf_) {
      ob_cdc_free(big_buf_);
      big_buf_ = NULL;
    }
  }
}

const char *SimpleBuf::get_buf() const
{
  const char *ret_buf = NULL;

  if (use_data_buf_) {
    ret_buf = data_buf_.get_data();
  } else {
    ret_buf = big_buf_;
  }

  return ret_buf;
}

} // namespace libobcdc
} // namespace oceanbase
