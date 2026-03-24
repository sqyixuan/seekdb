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
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_ctx.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_data_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_la_micro_key_manager.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

int ObSSMicroCacheTaskCtx::init(
    ObIAllocator &allocator, 
    ObSSMemDataManager &mem_data_mgr,
    ObSSMicroMetaManager &micro_meta_mgr, 
    ObSSPhysicalBlockManager &phy_blk_mgr,
    ObTenantFileManager &tnt_file_mgr,
    ObSSMicroCacheStat &cache_stat,
    ObSSLAMicroKeyManager &la_micro_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_data_mgr.is_inited() || !micro_meta_mgr.is_inited() ||
      !phy_blk_mgr.is_inited() || !tnt_file_mgr.is_inited() || !la_micro_mgr.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    allocator_ = &allocator;
    mem_data_mgr_ = &mem_data_mgr;
    micro_meta_mgr_ = &micro_meta_mgr;
    phy_blk_mgr_ = &phy_blk_mgr;
    tnt_file_mgr_ = &tnt_file_mgr;
    cache_stat_ = &cache_stat;
    la_micro_mgr_ = &la_micro_mgr;
    block_size_ = phy_blk_mgr.get_block_size();
  }
  return ret;
}

void ObSSMicroCacheTaskCtx::reset()
{
  allocator_ = nullptr;
  mem_data_mgr_ = nullptr;
  micro_meta_mgr_ = nullptr;
  phy_blk_mgr_ = nullptr;
  tnt_file_mgr_ = nullptr;
  cache_stat_ = nullptr;
  la_micro_mgr_ = nullptr;
  block_size_ = 0;
}

} // storage
} // oceanbase
