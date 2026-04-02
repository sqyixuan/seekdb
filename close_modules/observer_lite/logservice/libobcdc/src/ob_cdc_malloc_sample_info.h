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

#ifndef OCEANBASE_LIBOBCDC_MALLOC_SAMPLE_INFO_H__
#define OCEANBASE_LIBOBCDC_MALLOC_SAMPLE_INFO_H__

#include "lib/alloc/ob_malloc_sample_struct.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
using namespace lib;
namespace libobcdc
{

struct ObCDCMallocSamplePair
{
  ObCDCMallocSamplePair() : key_(), value_() {}
  ObCDCMallocSamplePair(const ObMallocSampleKey &key, const ObMallocSampleValue &value): key_(), value_() { reset(key, value); }

  ObCDCMallocSamplePair operator=(const ObCDCMallocSamplePair &other)
  {
    if (this != &other) {
      reset(other.key_, other.value_);
    }
    return *this;
  }
  void reset(const ObMallocSampleKey &key, const ObMallocSampleValue &value)
  {
    key_.tenant_id_ = key.tenant_id_;
    key_.ctx_id_ = key.ctx_id_;
    STRNCPY(key_.label_, key.label_, sizeof(key.label_));
    key_.label_[sizeof(key.label_) - 1] = '\0';
    MEMCPY((char*)key_.bt_, key.bt_, AOBJECT_BACKTRACE_SIZE);
    value_.alloc_bytes_ = value.alloc_bytes_;
    value_.alloc_count_ = value.alloc_count_;
  }
  ObMallocSampleKey     key_;
  ObMallocSampleValue   value_;
  TO_STRING_KV("tenant_id", key_.tenant_id_,
      "label", key_.label_,
      "ctx_id", key_.ctx_id_,
      "alloc_bytes", value_.alloc_bytes_,
      "alloc_cnt", value_.alloc_count_);
};

struct ObCDCMallocSamplePairCompartor
{
  bool operator()(const ObCDCMallocSamplePair &lhs, const ObCDCMallocSamplePair &rhs) const
  { return lhs.value_.alloc_bytes_ > rhs.value_.alloc_bytes_; }
};

typedef common::ObSEArray<ObCDCMallocSamplePair, 32> MallocSampleArray;

class ObCDCMallocSampleInfo
{
public:
  ObCDCMallocSampleInfo() : samples_() {}
  ~ObCDCMallocSampleInfo() { reset(); }
  void reset()
  {
    samples_.reset();
  }
public:
  int init(const ObMallocSampleMap &sample_map);
  void print_topk(int64_t k);
  void print_with_filter(const char *label_str, const int64_t alloc_size);
private:
  MallocSampleArray samples_;
};

} // end of namespace libobcdc
} // end of namespace oceanbase

#endif
