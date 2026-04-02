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
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"

namespace oceanbase
{
namespace storage
{
OB_SERIALIZE_MEMBER(ObSSMicroCacheHitStat, // FARM COMPAT WHITELIST
    tenant_id_, cache_hit_cnt_, cache_hit_bytes_, cache_miss_cnt_, cache_miss_bytes_,
    fail_get_cnt_, fail_get_bytes_, fail_add_cnt_, fail_add_bytes_, add_cnt_, add_bytes_,
    major_compaction_prewarm_cnt_, major_compaction_prewarm_bytes_, migration_prewarm_cnt_, 
    migration_prewarm_bytes_, replica_prewarm_cnt_, replica_prewarm_bytes_, ddl_prewarm_cnt_, 
    ddl_prewarm_bytes_, storage_cache_policy_prewarm_cnt_, storage_cache_policy_prewarm_bytes_,
    prewarm_new_add_cnt_, hot_micro_lack_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCachePhyBlkStat,  // FARM COMPAT WHITELIST
    tenant_id_, block_size_, cache_file_size_, total_blk_cnt_, normal_blk_cnt_, reorgan_blk_cnt_, reusable_blk_cnt_,
    normal_blk_free_cnt_, reorgan_blk_free_cnt_, phy_ckpt_blk_cnt_, phy_ckpt_blk_used_cnt_, meta_blk_cnt_,
    meta_blk_used_cnt_, data_blk_cnt_, data_blk_used_cnt_, shared_blk_cnt_, shared_blk_used_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheMemBlkStat, tenant_id_, mem_blk_def_cnt_, mem_blk_fg_max_cnt_, mem_blk_fg_used_cnt_,
    mem_blk_bg_max_cnt_, mem_blk_bg_used_cnt_, total_mem_blk_size_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheMicroStat,  // FARM COMPAT WHITELIST
    tenant_id_, total_micro_cnt_, total_micro_size_, valid_micro_cnt_, valid_micro_size_, micro_pool_fixed_cnt_, 
    micro_pool_mem_size_, micro_map_mem_size_, micro_total_mem_size_, micro_cnt_limit_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheTaskStat, // FARM COMPAT WHITELIST
    tenant_id_, reorgan_cnt_, reorgan_free_blk_cnt_, t1_evict_cnt_, t2_evict_cnt_, b1_delete_cnt_, b2_delete_cnt_, 
    phy_ckpt_cnt_, cur_phy_ckpt_item_cnt_, micro_ckpt_cnt_, cur_micro_ckpt_item_cnt_, phy_blk_min_len_,
    expired_cnt_, expired_ghost_cnt_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheIOStat::ObSSMicroCacheIOStatParam, add_cnt_, add_bytes_, get_cnt_, get_bytes_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheIOStat, common_io_param_, prewarm_io_param_);

OB_SERIALIZE_MEMBER(ObSSMicroCacheStat, // FARM COMPAT WHITELIST
    tenant_id_, hit_stat_, phy_blk_stat_, mem_blk_stat_, micro_stat_, task_stat_, io_stat_);
} /* namespace storage */
} /* namespace oceanbase */
