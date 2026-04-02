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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_CTX_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_CTX_H_

#include <stdint.h>
#include "lib/ob_define.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace storage
{
class ObSSMemDataManager;
class ObSSMicroMetaManager;
class ObSSPhysicalBlockManager;
class ObTenantFileManager;
class ObSSLAMicroKeyManager;

struct ObSSMicroCacheTaskCtx
{
public:
  int32_t block_size_;
  common::ObIAllocator *allocator_;
  ObSSMemDataManager *mem_data_mgr_;
  ObSSMicroMetaManager *micro_meta_mgr_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  ObTenantFileManager *tnt_file_mgr_;
  ObSSMicroCacheStat *cache_stat_;
  ObSSLAMicroKeyManager *la_micro_mgr_;

  ObSSMicroCacheTaskCtx()
    : block_size_(0), allocator_(nullptr), mem_data_mgr_(nullptr), micro_meta_mgr_(nullptr), 
      phy_blk_mgr_(nullptr), tnt_file_mgr_(nullptr), cache_stat_(nullptr), la_micro_mgr_(nullptr)
  {}
  ~ObSSMicroCacheTaskCtx() { reset(); }

  int init(common::ObIAllocator &allocator, ObSSMemDataManager &mem_data_mgr,
           ObSSMicroMetaManager &micro_meta_mgr, ObSSPhysicalBlockManager &phy_blk_mgr,
           ObTenantFileManager &tnt_file_mgr, ObSSMicroCacheStat &cache_stat,
           ObSSLAMicroKeyManager &la_micro_mgr);
  bool is_valid() const { return (allocator_ != nullptr) && (mem_data_mgr_ != nullptr) && 
                                 (micro_meta_mgr_ != nullptr) && (phy_blk_mgr_ != nullptr) &&
                                 (tnt_file_mgr_ != nullptr) && (cache_stat_ != nullptr) &&
                                 (la_micro_mgr_ != nullptr) && (block_size_ > 0); }
  void reset();
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_CTX_H_ */
