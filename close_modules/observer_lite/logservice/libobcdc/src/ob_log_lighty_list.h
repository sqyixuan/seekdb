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

#ifndef OCEANBASE_LIBOBCDC_LIGHTY_LIST_H__
#define OCEANBASE_LIBOBCDC_LIGHTY_LIST_H__

#include "share/ob_define.h"

namespace oceanbase
{
namespace libobcdc
{

template <class Type>
struct LightyList
{
  Type *head_;
  Type *tail_;
  int64_t num_;

  LightyList() : head_(NULL), tail_(NULL), num_(0)
  {}

  ~LightyList() { reset(); }

  void reset()
  {
    head_ = NULL;
    tail_ = NULL;
    num_ = 0;
  }

  int add(Type *node)
  {
    int ret = common::OB_SUCCESS;

    if (NULL == node) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (NULL == head_) {
      head_ = node;
      tail_ = node;
      node->set_next(NULL);
      num_ = 1;
    } else {
      tail_->set_next(node);
      tail_ = node;
      node->set_next(NULL);
      num_++;
    }
    return ret;
  }

  TO_STRING_KV(KP_(head), KP_(tail), K_(num));
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_LIGHTY_LIST_H__ */
