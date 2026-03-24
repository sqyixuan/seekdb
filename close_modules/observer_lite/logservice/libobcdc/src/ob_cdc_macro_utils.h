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

#ifndef _OB_CDC_MACRO_UTILS_H_
#define _OB_CDC_MACRO_UTILS_H_

#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace libobcdc
{

// define private field and corresponding getter and setter method
#define DEFINE_PRIVATE_FIELD(TYPE, FIELD_NAME) \
  private: \
    TYPE    FIELD_NAME##_;

#define DEFINE_PUBLIC_GETTER(TYPE, FIELD_NAME) \
  public: \
    TYPE get_##FIELD_NAME() const { return ATOMIC_LOAD(&FIELD_NAME##_); }

#define DEFINE_PUBLIC_SETTER(TYPE, FIELD_NAME) \
  public: \
    void set_##FIELD_NAME(const TYPE FIELD_NAME) { ATOMIC_SET(&FIELD_NAME##_, FIELD_NAME); }


#define DEFINE_FIELD_WITH_GETTER(TYPE, FIELD_NAME) \
  DEFINE_PRIVATE_FIELD(TYPE, FIELD_NAME); \
  DEFINE_PUBLIC_GETTER(TYPE, FIELD_NAME); \
  DEFINE_PUBLIC_SETTER(TYPE, FIELD_NAME);

// other macros

} // namespace libobcdc
} // namespace oceanbase

#endif
