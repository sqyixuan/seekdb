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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include "lib/ob_errno.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "lib/random/ob_random.h"

namespace oceanbase 
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

struct TestSSLocalCacheSpaceCtx
{
public:
  int64_t total_size_; // total disk size of local_cache in the tenant
  int64_t total_meta_size_;
  int64_t total_tmp_rcache_size_;
  int64_t total_tmp_wcache_size_;
  int64_t total_private_macro_size_;
  int64_t total_micro_cache_size_;
  int64_t arc_limit_;

  TestSSLocalCacheSpaceCtx() { reset(); }
  void reset()
  {
    total_size_ = 0;
    total_meta_size_ = 0;
    total_tmp_rcache_size_ = 0;
    total_tmp_wcache_size_ = 0;
    total_private_macro_size_ = 0;
    total_micro_cache_size_ = 0;
    arc_limit_ = 0;
  }
  bool is_valid() const { return (total_size_ > 0) && (total_meta_size_ > 0) && (total_tmp_rcache_size_) > 0 &&
                          (total_tmp_wcache_size_ > 0) && (total_private_macro_size_ > 0) && (total_micro_cache_size_ > 0) &&
                          (arc_limit_ > 0); }

  TO_STRING_KV(K_(total_size), K_(total_meta_size), K_(total_tmp_rcache_size), K_(total_tmp_wcache_size),
    K_(total_private_macro_size), K_(total_micro_cache_size), K_(arc_limit));
};

struct TestSSLocalCacheModuleCtx
{
public:
  ObTenantDiskSpaceManager *disk_space_mgr_;
  ObTenantFileManager *file_mgr_;
  ObSSMicroCache *micro_cache_;

  bool is_valid() const { return (nullptr != disk_space_mgr_) && (nullptr != file_mgr_) && (nullptr != micro_cache_); }

  TO_STRING_KV(KP_(disk_space_mgr), KP_(file_mgr), KP_(micro_cache));
};

enum class TestSSLocalCacheMode : uint8
{
  FAST_CHECK = 0,
  SMALL_DATA = 1,
  NORMAL_DATA = 2,
  HUGE_DATA = 3,
  MAX_TYPE,
};

struct TestSSLocalCacheEnvCtx
{
public:
  const double FAST_CHECK_MULTIPLE = 0.05;
  const double SMALL_DATA_MULTIPLE = 0.5;
  const double NORMAL_DATA_MULTIPLE = 1.0;
  const double HUGE_DATA_MULTIPLE = 1.4;
public:
  int64_t thread_num_;
  TestSSLocalCacheMode test_mode_;
  double data_multiple_;

  TestSSLocalCacheEnvCtx() : thread_num_(0), test_mode_(TestSSLocalCacheMode::MAX_TYPE), data_multiple_(0)
  {}

  bool is_valid() const { return thread_num_ > 0 && test_mode_ != TestSSLocalCacheMode::MAX_TYPE && data_multiple_ > 0; }

  bool is_fast_check_mode() { return test_mode_ == TestSSLocalCacheMode::FAST_CHECK; }
  bool is_small_data_mode() { return test_mode_ == TestSSLocalCacheMode::SMALL_DATA; }
  bool is_normal_data_mode() { return test_mode_ == TestSSLocalCacheMode::NORMAL_DATA; }
  bool is_huge_data_mode() { return test_mode_ == TestSSLocalCacheMode::HUGE_DATA; }
  void set_test_mode(const TestSSLocalCacheMode mode)
  {
    test_mode_ = mode;
    if (is_fast_check_mode()) {
      data_multiple_ = FAST_CHECK_MULTIPLE;
    } else if (is_small_data_mode()) {
      data_multiple_ = SMALL_DATA_MULTIPLE;
    } else if (is_normal_data_mode()) {
      data_multiple_ = NORMAL_DATA_MULTIPLE;
    } else if (is_huge_data_mode()) {
      data_multiple_ = HUGE_DATA_MULTIPLE;
    } else {
      data_multiple_ = 0;
    }
  }

  TO_STRING_KV(K_(thread_num), K_(test_mode), K_(data_multiple));
};

}  // namespace storage
}  // namespace oceanbase
