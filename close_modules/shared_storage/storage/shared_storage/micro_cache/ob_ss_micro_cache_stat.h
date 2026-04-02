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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_STAT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_STAT_H_

#include <stdint.h>
#include "share/ob_define.h"
#include "storage/shared_storage/common/ob_ss_common_info.h"

namespace oceanbase
{
namespace storage
{

// sizeof(ObS2MicroBlockMeta)=88, but actual size is 120 when allocated by ObSliceAlloc
// Due to one 8K mem_blk can only store 62 item, thus we treat its size as 132.
constexpr int64_t SS_MICRO_META_POOL_ITEM_SIZE = 132;
// Each map item needs 32 bytes
constexpr int64_t SS_MICRO_META_MAP_ITEM_SIZE = 32;

enum class ObSSMicroCacheAccessType : uint8_t
{
  COMMON_IO_TYPE = 0,
  MAJOR_COMPACTION_PREWARM_TYPE = 1,
  MIGRATION_PREWARM_TYPE = 2,
  REPLICA_PREWARM_TYPE = 3,
  DDL_PREWARM_TYPE = 4,
  STORAGE_CACHE_POLICY_PREWARM_TYPE = 5,
  MAX_TYPE
};

/*
 * All request of prewarm won't be counted into this stat
 */
struct ObSSMicroCacheHitStat
{
public:
  uint64_t tenant_id_;
  int64_t cache_hit_cnt_;
  int64_t cache_hit_bytes_;
  int64_t cache_miss_cnt_;
  int64_t cache_miss_bytes_;
  int64_t fail_get_cnt_;
  int64_t fail_get_bytes_;
  int64_t fail_add_cnt_;
  int64_t fail_add_bytes_;
  int64_t add_cnt_;
  int64_t add_bytes_;
  int64_t new_add_cnt_;
  int64_t major_compaction_prewarm_cnt_;
  int64_t major_compaction_prewarm_bytes_;
  int64_t migration_prewarm_cnt_;
  int64_t migration_prewarm_bytes_;
  int64_t replica_prewarm_cnt_;
  int64_t replica_prewarm_bytes_;
  int64_t ddl_prewarm_cnt_;
  int64_t ddl_prewarm_bytes_;
  int64_t storage_cache_policy_prewarm_cnt_;
  int64_t storage_cache_policy_prewarm_bytes_;
  int64_t prewarm_new_add_cnt_;
  int64_t hot_micro_lack_cnt_;
  int64_t temp_hot_micro_lack_cnt_;

  ObSSMicroCacheHitStat() { reset(); }
  void reset() 
  { 
    tenant_id_ = OB_INVALID_TENANT_ID;
    reset_dynamic_info();
  }

  void reset_dynamic_info()
  {
    cache_hit_cnt_ = 0; 
    cache_hit_bytes_ = 0; 
    cache_miss_cnt_ = 0; 
    cache_miss_bytes_ = 0; 
    fail_get_cnt_ = 0;
    fail_get_bytes_ = 0;
    fail_add_cnt_ = 0;
    fail_add_bytes_ = 0;
    add_cnt_ = 0;
    add_bytes_ = 0;
    new_add_cnt_ = 0;
    major_compaction_prewarm_cnt_ = 0; 
    major_compaction_prewarm_bytes_ = 0; 
    migration_prewarm_cnt_ = 0;
    migration_prewarm_bytes_ = 0; 
    replica_prewarm_cnt_ = 0; 
    replica_prewarm_bytes_ = 0;
    ddl_prewarm_cnt_ = 0; 
    ddl_prewarm_bytes_ = 0;
    storage_cache_policy_prewarm_cnt_ = 0;
    storage_cache_policy_prewarm_bytes_ = 0;
    prewarm_new_add_cnt_ = 0;
    hot_micro_lack_cnt_ = 0;
    temp_hot_micro_lack_cnt_ = 0;
  }

  void update_cache_hit(const int64_t delta_cnt, const int64_t delta_size) 
  { 
    ATOMIC_AAF(&cache_hit_cnt_, delta_cnt);
    ATOMIC_AAF(&cache_hit_bytes_, delta_size);
  }
  void update_cache_miss(const int64_t delta_cnt, const int64_t delta_size) 
  { 
    ATOMIC_AAF(&cache_miss_cnt_, delta_cnt);
    ATOMIC_AAF(&cache_miss_bytes_, delta_size);
  }
  void update_fail_get_cnt(const int64_t delta_cnt, const int64_t delta_bytes) 
  { 
    ATOMIC_AAF(&fail_get_cnt_, delta_cnt);
    ATOMIC_AAF(&fail_get_bytes_, delta_bytes);
  }
  void update_fail_add_cnt(const int64_t delta_cnt, const int64_t delta_bytes) 
  { 
    ATOMIC_AAF(&fail_add_cnt_, delta_cnt);
    ATOMIC_AAF(&fail_add_bytes_, delta_bytes);
  }
  int64_t get_add_cnt() const { return ATOMIC_LOAD(&add_cnt_); }
  int64_t get_add_bytes() const { return ATOMIC_LOAD(&add_bytes_); }
  int64_t get_fail_add_cnt() const { return ATOMIC_LOAD(&fail_add_cnt_); }
  int64_t get_common_new_add_cnt() const { return ATOMIC_LOAD(&new_add_cnt_) - ATOMIC_LOAD(&prewarm_new_add_cnt_); }
  void update_add_info(const bool is_first_add, const bool is_prewarm_io, const int64_t delta_cnt, const int64_t delta_bytes)
  {
    if (is_first_add) {
      ATOMIC_AAF(&new_add_cnt_, delta_cnt);
      if (is_prewarm_io) {
        ATOMIC_AAF(&prewarm_new_add_cnt_, delta_cnt);
      }
    }
    ATOMIC_AAF(&add_cnt_, delta_cnt);
    ATOMIC_AAF(&add_bytes_, delta_bytes);
  }
  void update_prewarm_info(const ObSSMicroCacheAccessType access_type, const int64_t delta_cnt, const int64_t delta_size)
  {
    switch (access_type) {
      case ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE: {
        ATOMIC_AAF(&major_compaction_prewarm_cnt_, delta_cnt);
        ATOMIC_AAF(&major_compaction_prewarm_bytes_, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::MIGRATION_PREWARM_TYPE: {
        ATOMIC_AAF(&migration_prewarm_cnt_, delta_cnt);
        ATOMIC_AAF(&migration_prewarm_bytes_, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE: {
        ATOMIC_AAF(&replica_prewarm_cnt_, delta_cnt);
        ATOMIC_AAF(&replica_prewarm_bytes_, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::DDL_PREWARM_TYPE: {
        ATOMIC_AAF(&ddl_prewarm_cnt_, delta_cnt);
        ATOMIC_AAF(&ddl_prewarm_bytes_, delta_size);
        break;
      }
      case ObSSMicroCacheAccessType::STORAGE_CACHE_POLICY_PREWARM_TYPE: {
        ATOMIC_AAF(&storage_cache_policy_prewarm_cnt_, delta_cnt);
        ATOMIC_AAF(&storage_cache_policy_prewarm_bytes_, delta_size);
        break;
      }
      default: {
        break;
      }
    }
  }

  void update_hot_micro_lack_cnt_info(const bool update_formal_val, const int64_t lack_cnt_delta)
  {
    if (lack_cnt_delta >= 0) {
      const int64_t temp_val = ATOMIC_AAF(&temp_hot_micro_lack_cnt_, lack_cnt_delta);
      if (update_formal_val) {
        ATOMIC_STORE(&hot_micro_lack_cnt_, temp_val);
        ATOMIC_STORE(&temp_hot_micro_lack_cnt_, 0);
      }
    }
  }

  TO_STRING_KV(K_(cache_hit_cnt), K_(cache_hit_bytes), K_(cache_miss_cnt), K_(cache_miss_bytes), K_(fail_get_cnt),
    K_(fail_get_bytes), K_(fail_add_cnt), K_(fail_add_bytes), K_(add_cnt), K_(add_bytes), K_(new_add_cnt),
    K_(major_compaction_prewarm_cnt), K_(major_compaction_prewarm_bytes), K_(migration_prewarm_cnt),
    K_(migration_prewarm_bytes), K_(replica_prewarm_cnt), K_(replica_prewarm_bytes), K_(ddl_prewarm_cnt), 
    K_(ddl_prewarm_bytes), K_(prewarm_new_add_cnt), K_(hot_micro_lack_cnt), K_(temp_hot_micro_lack_cnt));

public:
  static const int64_t OB_SS_MICRO_CACHE_HIT_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_HIT_STAT_VERSION);
};

struct ObSSMicroCachePhyBlkStat
{
public:
  uint64_t tenant_id_;
  int32_t block_size_;
  int64_t cache_file_size_;
  int64_t total_blk_cnt_;
  int64_t super_blk_cnt_;
  int64_t normal_blk_cnt_;          // [Deprecated]
  int64_t reorgan_blk_cnt_;         // [Deprecated]
  int64_t reusable_blk_cnt_;
  int64_t normal_blk_free_cnt_;     // [Deprecated]
  int64_t reorgan_blk_free_cnt_;    // [Deprecated]
  int64_t phy_ckpt_blk_cnt_;
  int64_t phy_ckpt_blk_used_cnt_;

  int64_t meta_blk_cnt_;
  int64_t meta_blk_used_cnt_;
  int64_t data_blk_cnt_;
  int64_t data_blk_used_cnt_;
  int64_t shared_blk_cnt_;
  int64_t shared_blk_used_cnt_;

  int64_t sparse_blk_cnt_;

  ObSSMicroCachePhyBlkStat() { reset(); }
  void reset() 
  { 
    tenant_id_ = OB_INVALID_TENANT_ID;
    block_size_ = 0;
    cache_file_size_ = 0; 
    total_blk_cnt_ = 0;
    super_blk_cnt_ = 0;
    shared_blk_cnt_ = 0;
    reset_dynamic_info();
  }
  void reset_dynamic_info()
  {
    shared_blk_used_cnt_ = 0;
    phy_ckpt_blk_cnt_ = 0;
    phy_ckpt_blk_used_cnt_ = 0;
    meta_blk_cnt_ = 0;
    meta_blk_used_cnt_ = 0;
    data_blk_cnt_ = 0;
    data_blk_used_cnt_ = 0;
    reusable_blk_cnt_ = 0;
    sparse_blk_cnt_ = 0;
  }
  void set_cache_file_size(const int64_t size)
  {
    ATOMIC_STORE(&cache_file_size_, size);
  }
  void set_block_size(const int64_t block_size)
  {
    ATOMIC_STORE(&block_size_, block_size);
  }
  void set_super_block_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&super_blk_cnt_, cnt);
  }
  void set_total_block_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&total_blk_cnt_, cnt);
  }
  void set_reusable_block_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&reusable_blk_cnt_, cnt);
  }
  void set_sparse_block_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&sparse_blk_cnt_, cnt);
  }
  void set_shared_block_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&shared_blk_cnt_, cnt);
  }
  void set_shared_block_used_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&shared_blk_used_cnt_, cnt);
  }
  void update_shared_block_used_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&shared_blk_used_cnt_, delta);
  }
  void set_phy_ckpt_block_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&phy_ckpt_blk_cnt_, cnt);
  }
  void update_phy_ckpt_block_used_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&phy_ckpt_blk_used_cnt_, delta);
  }
  void set_data_block_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&data_blk_cnt_, cnt);
  }
  void update_data_block_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&data_blk_cnt_, delta);
  }
  void update_data_block_used_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&data_blk_used_cnt_, delta);
  }
  void set_meta_block_cnt(const int64_t cnt)
  {
    ATOMIC_STORE(&meta_blk_cnt_, cnt);
  }
  void update_meta_block_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&meta_blk_cnt_, delta);
  }
  void update_meta_block_used_cnt(const int64_t delta)
  {
    ATOMIC_AAF(&meta_blk_used_cnt_, delta);
  }

  int64_t get_used_disk_size() const
  {
    const int64_t cur_used_blk_cnt = ATOMIC_LOAD(&super_blk_cnt_) + ATOMIC_LOAD(&phy_ckpt_blk_used_cnt_) +
                                     ATOMIC_LOAD(&meta_blk_used_cnt_) + ATOMIC_LOAD(&data_blk_used_cnt_);
    const int64_t used_disk_size = cur_used_blk_cnt * block_size_;
    return used_disk_size;
  }

  TO_STRING_KV(K_(cache_file_size), K_(block_size), K_(total_blk_cnt), K_(super_blk_cnt), K_(shared_blk_cnt),
      K_(shared_blk_used_cnt), K_(phy_ckpt_blk_cnt), K_(phy_ckpt_blk_used_cnt), K_(meta_blk_cnt), K_(meta_blk_used_cnt),
      K_(data_blk_cnt), K_(data_blk_used_cnt), K_(reusable_blk_cnt), K_(sparse_blk_cnt));

public:
  static const int64_t OB_SS_MICRO_CACHE_PHY_BLK_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_PHY_BLK_STAT_VERSION);
};

struct ObSSMicroCacheMemBlkStat
{
public:
  uint64_t tenant_id_;
  int64_t mem_blk_def_cnt_;
  int64_t mem_blk_fg_max_cnt_;
  int64_t mem_blk_fg_used_cnt_;
  int64_t mem_blk_bg_max_cnt_;
  int64_t mem_blk_bg_used_cnt_;
  int64_t total_mem_blk_size_;
  
  ObSSMicroCacheMemBlkStat() { reset(); }
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    mem_blk_def_cnt_ = 0;
    mem_blk_fg_max_cnt_ = 0;
    mem_blk_bg_max_cnt_ = 0;
    reset_dynamic_info();
  }
  void reset_dynamic_info()
  {
    mem_blk_fg_used_cnt_ = 0;
    mem_blk_bg_used_cnt_ = 0;
    total_mem_blk_size_ = 0;
  }
  OB_INLINE void set_mem_blk_def_cnt(const int64_t cnt) { ATOMIC_STORE(&mem_blk_def_cnt_, cnt); }
  void set_mem_blk_fg_max_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&mem_blk_fg_max_cnt_, cnt);
  }
  void set_mem_blk_bg_max_cnt(const int64_t cnt) 
  { 
    ATOMIC_STORE(&mem_blk_bg_max_cnt_, cnt);
  }
  void update_mem_blk_fg_used_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&mem_blk_fg_used_cnt_, delta);
  }
  void update_mem_blk_bg_used_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&mem_blk_bg_used_cnt_, delta);
  }
  void update_total_mem_blk_size(const int64_t delta) 
  { 
    ATOMIC_AAF(&total_mem_blk_size_, delta);
  }
  int64_t get_total_mem_blk_used_cnt() 
  {
    return ATOMIC_LOAD(&mem_blk_fg_used_cnt_) + ATOMIC_LOAD(&mem_blk_bg_used_cnt_);
  }
  TO_STRING_KV(K_(mem_blk_def_cnt), K_(mem_blk_fg_max_cnt), K_(mem_blk_fg_used_cnt), K_(mem_blk_bg_max_cnt), 
    K_(mem_blk_bg_used_cnt), K_(total_mem_blk_size));

public:
  static const int64_t OB_SS_MICRO_CACHE_MEM_BLK_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_MEM_BLK_STAT_VERSION);
};

struct ObSSMicroCacheMicroStat
{
public:
  uint64_t tenant_id_;
  int64_t total_micro_cnt_; // total count of micro in micro_map
  int64_t total_micro_size_;// total size of micro in micro_map
  int64_t valid_micro_cnt_; // total count of micro in micro_map which belongs to T1/T2
  int64_t valid_micro_size_;// total size of micro in micro_map which belongs to T1/T2
  int64_t micro_pool_fixed_cnt_; // actual allocated micro_meta count
  int64_t micro_pool_mem_size_;
  int64_t micro_map_mem_size_;
  int64_t micro_total_mem_size_;
  int64_t micro_cnt_limit_;

  ObSSMicroCacheMicroStat() { reset(); }
  void reset() 
  { 
    tenant_id_ = OB_INVALID_TENANT_ID;
    micro_cnt_limit_ = 0;
    reset_dynamic_info();
  }
  void reset_dynamic_info()
  {
    total_micro_cnt_ = 0; 
    total_micro_size_ = 0;
    valid_micro_cnt_ = 0;
    valid_micro_size_ = 0;
    micro_pool_fixed_cnt_ = 0;
    micro_pool_mem_size_ = 0;
    micro_map_mem_size_ = 0;
    micro_total_mem_size_ = 0;
  }
  int64_t get_avg_micro_size() const
  {
    const int64_t valid_cnt = ATOMIC_LOAD(&valid_micro_cnt_);
    const int64_t valid_size = ATOMIC_LOAD(&valid_micro_size_);
    const int64_t avg_size = (valid_cnt > 0 ? (valid_size / valid_cnt) : 0);
    return avg_size;
  }
  void update_valid_micro_info(const int64_t valid_micro_cnt, const int64_t valid_micro_size)
  {
    ATOMIC_STORE(&valid_micro_cnt_, valid_micro_cnt);
    ATOMIC_STORE(&valid_micro_size_, valid_micro_size);
  }
  void update_micro_map_info(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&total_micro_size_, delta_size);
    const int64_t cur_micro_cnt = ATOMIC_AAF(&total_micro_cnt_, delta_cnt);
    const int64_t micro_map_mem_size = cur_micro_cnt * SS_MICRO_META_MAP_ITEM_SIZE;
    ATOMIC_STORE(&micro_map_mem_size_, micro_map_mem_size);
    const int64_t micro_total_mem_size = micro_map_mem_size + ATOMIC_LOAD(&micro_pool_mem_size_);
    ATOMIC_STORE(&micro_total_mem_size_, micro_total_mem_size);
  }
  void update_micro_pool_info()
  {
    const int64_t micro_pool_mem_size = get_micro_pool_alloc_cnt() * SS_MICRO_META_POOL_ITEM_SIZE;
    ATOMIC_STORE(&micro_pool_mem_size_, micro_pool_mem_size);
    const int64_t micro_total_mem_size = micro_pool_mem_size + ATOMIC_LOAD(&micro_map_mem_size_);
    ATOMIC_STORE(&micro_total_mem_size_, micro_total_mem_size);
  }

  void update_micro_pool_alloc_cnt(const int64_t delta) { ATOMIC_AAF(&micro_pool_fixed_cnt_, delta); }
  int64_t get_micro_pool_alloc_cnt() const { return ATOMIC_LOAD(&micro_pool_fixed_cnt_); }
  void set_micro_cnt_limit(const int64_t micro_cnt_limit) { ATOMIC_STORE(&micro_cnt_limit_, micro_cnt_limit); }
  int64_t get_micro_total_mem_size() const { return ATOMIC_LOAD(&micro_total_mem_size_); }
  int64_t get_valid_micro_size() const { return ATOMIC_LOAD(&valid_micro_size_); }
  int64_t get_total_micro_cnt() const { return ATOMIC_LOAD(&total_micro_cnt_); }
  TO_STRING_KV(K_(total_micro_cnt), K_(total_micro_size), K_(valid_micro_cnt), K_(valid_micro_size),
      K_(micro_pool_fixed_cnt), K_(micro_pool_mem_size), K_(micro_map_mem_size), K_(micro_total_mem_size), K_(micro_cnt_limit));

public:
  static const int64_t OB_SS_MICRO_CACHE_MICRO_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_MICRO_STAT_VERSION);
};

struct ObSSMicroCacheTaskStat
{
public:
  uint64_t tenant_id_;
  int64_t reorgan_cnt_;
  int64_t reorgan_free_blk_cnt_;
  int64_t t1_evict_cnt_;
  int64_t t2_evict_cnt_;
  int64_t b1_delete_cnt_;
  int64_t b2_delete_cnt_;
  int64_t phy_ckpt_cnt_;
  int64_t cur_phy_ckpt_item_cnt_;
  int64_t micro_ckpt_cnt_;
  int64_t cur_micro_ckpt_item_cnt_;
  int32_t phy_blk_min_len_;
  int64_t expired_cnt_; // total expired micro count of T1/T2
  int64_t expired_ghost_cnt_; // total expired micro count of B1/B2

  ObSSMicroCacheTaskStat() { reset(); }
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    reset_dynamic_info();
  }
  void reset_dynamic_info()
  {
    reorgan_cnt_ = 0; 
    reorgan_free_blk_cnt_ = 0; 
    t1_evict_cnt_ = 0;
    t2_evict_cnt_ = 0;
    b1_delete_cnt_ = 0;
    b2_delete_cnt_ = 0;
    phy_ckpt_cnt_ = 0; 
    cur_phy_ckpt_item_cnt_ = 0;
    micro_ckpt_cnt_ = 0; 
    cur_micro_ckpt_item_cnt_ = 0;
    phy_blk_min_len_ = 0;
    expired_cnt_ = 0;
    expired_ghost_cnt_ = 0;
  }
  void update_reorgan_cnt(const int64_t delta) 
  { 
    ATOMIC_AAF(&reorgan_cnt_, delta);
  }
  OB_INLINE void update_reorgan_free_blk_cnt(const int64_t delta) { ATOMIC_AAF(&reorgan_free_blk_cnt_, delta); }
  void update_phy_ckpt_cnt(const int64_t delta, const int64_t item_cnt)
  { 
    ATOMIC_AAF(&phy_ckpt_cnt_, delta);
    ATOMIC_STORE(&cur_phy_ckpt_item_cnt_, item_cnt);
  }
  void update_micro_ckpt_cnt(const int64_t delta, const int64_t item_cnt) 
  { 
    ATOMIC_AAF(&micro_ckpt_cnt_, delta);
    ATOMIC_STORE(&cur_micro_ckpt_item_cnt_, item_cnt);
  }
  void update_arc_evict_cnt(const bool is_in_l1, const bool is_in_ghost, const int64_t delta) 
  {
    if (is_in_l1 && !is_in_ghost) { // T1
      ATOMIC_AAF(&t1_evict_cnt_, delta); 
    } else if (!is_in_l1 && !is_in_ghost) { // T2
      ATOMIC_AAF(&t2_evict_cnt_, delta); 
    }
  }
  void update_expired_cnt(const bool is_in_ghost, const int64_t delta)
  {
    if (is_in_ghost) {
      ATOMIC_AAF(&expired_ghost_cnt_, delta); 
    } else {
      ATOMIC_AAF(&expired_cnt_, delta);
    }
  }
  bool exist_arc_evict() const
  {
    return ((t1_evict_cnt_ > 0) || (t2_evict_cnt_ > 0));
  }
  void update_arc_delete_cnt(const bool is_in_l1, const bool is_in_ghost, const int64_t delta) 
  { 
    if (is_in_l1 && is_in_ghost) { // B1
      ATOMIC_AAF(&b1_delete_cnt_, delta); 
    } else if (!is_in_l1 && is_in_ghost) { // B2
      ATOMIC_AAF(&b2_delete_cnt_, delta); 
    }
  }
  bool exist_arc_delete() const
  {
    return ((b1_delete_cnt_ > 0) || (b2_delete_cnt_ > 0));
  }
  bool exist_arc_evict_or_delete() const
  {
    return exist_arc_evict() || exist_arc_delete();
  }
  void set_phy_blk_min_len(const int32_t min_len) 
  {
    ATOMIC_STORE(&phy_blk_min_len_, min_len);
  }
  int64_t get_total_evict_cnt() const
  {
    return (ATOMIC_LOAD(&t1_evict_cnt_) + ATOMIC_LOAD(&t2_evict_cnt_));
  }
  int64_t get_total_delete_cnt() const
  {
    return (ATOMIC_LOAD(&b1_delete_cnt_) + ATOMIC_LOAD(&b2_delete_cnt_));
  }

  TO_STRING_KV(K_(reorgan_cnt), K_(reorgan_free_blk_cnt), K_(phy_ckpt_cnt), K_(cur_phy_ckpt_item_cnt), 
    K_(micro_ckpt_cnt), K_(cur_micro_ckpt_item_cnt), K_(phy_blk_min_len), K_(t1_evict_cnt), K_(t2_evict_cnt), 
    K_(b1_delete_cnt), K_(b2_delete_cnt), K_(expired_cnt), K_(expired_ghost_cnt));

public:
  static const int64_t OB_SS_MICRO_CACHE_TASK_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_TASK_STAT_VERSION);
};

// To shows the upper layer's IO request pressure
struct ObSSMicroCacheIOStat
{
public:
  struct ObSSMicroCacheIOStatParam
  {
  public:
    int64_t add_cnt_;
    int64_t add_bytes_;
    int64_t get_cnt_;
    int64_t get_bytes_;

    ObSSMicroCacheIOStatParam() { reset(); }
    void reset()
    {
      add_cnt_ = 0;
      add_bytes_ = 0;
      get_cnt_ = 0;
      get_bytes_ = 0;
    }

    void update_io_stat_param(const bool is_read, const int64_t delta_cnt, const int64_t delta_bytes)
    {
      if (is_read) {
        ATOMIC_AAF(&get_cnt_, delta_cnt);
        ATOMIC_AAF(&get_bytes_, delta_bytes);
      } else {
        ATOMIC_AAF(&add_cnt_, delta_cnt);
        ATOMIC_AAF(&add_bytes_, delta_bytes);
      }
    }

    int64_t add_cnt() const { return ATOMIC_LOAD(&add_cnt_); }
    int64_t add_bytes() const { return ATOMIC_LOAD(&add_bytes_); }
    int64_t get_cnt() const { return ATOMIC_LOAD(&get_cnt_); }
    int64_t get_bytes() const { return ATOMIC_LOAD(&get_bytes_); }

    TO_STRING_KV(K_(add_cnt), K_(add_bytes), K_(get_cnt), K_(get_bytes));
  public:
    static const int64_t OB_SS_MICRO_CACHE_IO_STAT_PARAM_VERSION = 1;
    OB_UNIS_VERSION(OB_SS_MICRO_CACHE_IO_STAT_PARAM_VERSION);
  };
  
  ObSSMicroCacheIOStatParam common_io_param_;
  ObSSMicroCacheIOStatParam prewarm_io_param_;
  ObSSMicroCacheIOStat() { reset(); }
  void reset()
  {
    common_io_param_.reset();
    prewarm_io_param_.reset();
  }

  void update_io_stat_info(const bool is_prewarm, const bool is_read, const int64_t delta_cnt, const int64_t delta_bytes) 
  { 
    if (is_prewarm) {
      prewarm_io_param_.update_io_stat_param(is_read, delta_cnt, delta_bytes);
    } else {
      common_io_param_.update_io_stat_param(is_read, delta_cnt, delta_bytes);
    }
  }

  int64_t get_add_cnt() const
  {
    return common_io_param_.add_cnt_ + prewarm_io_param_.add_cnt_;
  }

  TO_STRING_KV(K_(common_io_param), K_(prewarm_io_param));
public:
  static const int64_t OB_SS_MICRO_CACHE_IO_STAT_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_IO_STAT_VERSION);
};

struct ObSSMicroCacheHealthStat
{
public:
  static const int64_t MIN_INCREATMENT_COUNT = 1;
public:
  int64_t pre_add_cnt_;
  int64_t cur_add_cnt_;
  int64_t pre_fail_add_cnt_;
  int64_t cur_fail_add_cnt_;

  ObSSMicroCacheHealthStat() { reset(); }

  void reset()
  {
    pre_add_cnt_ = 0;
    cur_add_cnt_ = 0;
    pre_fail_add_cnt_ = 0;
    cur_fail_add_cnt_ = 0;
  }

  void update_newest_add_info(const int64_t cur_add_cnt, const int64_t cur_fail_add_cnt)
  {
    pre_add_cnt_ = cur_add_cnt_;
    pre_fail_add_cnt_ = cur_fail_add_cnt_;
    cur_add_cnt_ = cur_add_cnt;
    cur_fail_add_cnt_ = cur_fail_add_cnt;
  }

  bool exist_increament_io(const int64_t min_increament_cnt = MIN_INCREATMENT_COUNT) const
  {
    bool is_exist = false;
    if ((pre_add_cnt_ > 0) && (cur_add_cnt_ > 0) && (cur_add_cnt_ - pre_add_cnt_ >= min_increament_cnt)) {
      is_exist = true;
    }
    return is_exist;
  }

  bool is_all_increament_io_failed() const
  {
    return ((cur_add_cnt_ - pre_add_cnt_) == (cur_fail_add_cnt_ - pre_fail_add_cnt_));
  }
};

struct ObSSMicroCacheLoad
{
public:
  static const int64_t COMMON_IO_HIGH_THROUGHPUT = 6291456; // 6MB/s
public:
  struct ObSSMicroCacheLoadParam
  {
  public:
    int64_t interval_s_;
    int64_t pre_cnt_;
    int64_t pre_bytes_;
    int64_t cur_cnt_;
    int64_t cur_bytes_;

    ObSSMicroCacheLoadParam() { reset(); }
    void reset()
    {
      interval_s_ = 0;
      pre_cnt_ = 0;
      pre_bytes_ = 0;
      cur_cnt_ = 0;
      cur_bytes_ = 0;
    }

    void update_load_param(const int64_t interval_s, const int64_t cur_cnt, const int64_t cur_bytes)
    {
      interval_s_ = interval_s;
      pre_cnt_ = cur_cnt_;
      pre_bytes_ = cur_bytes_;
      cur_cnt_ = cur_cnt;
      cur_bytes_ = cur_bytes;
    }

    int64_t get_iops() const
    {
      int64_t iops_result = 0;
      if (interval_s_ > 0) {
        iops_result = (cur_cnt_ - pre_cnt_) / interval_s_;
      }
      return iops_result;
    }

    int64_t get_throughput() const
    {
      int64_t throughput_result = 0;
      if (interval_s_ > 0) {
        throughput_result = (cur_bytes_ - pre_bytes_) / interval_s_;
      }
      return throughput_result;
    }

    TO_STRING_KV("iops", get_iops(), "throughput", get_throughput());
  };

  struct ObSSMicroCacheLoadInfo
  {
  public:
    ObSSMicroCacheLoadParam read_load_;
    ObSSMicroCacheLoadParam write_load_;

    ObSSMicroCacheLoadInfo() { reset(); }
    void reset()
    {
      read_load_.reset();
      write_load_.reset();
    }

    void update_read_load_param(const int64_t interval_s, const int64_t cur_cnt, const int64_t cur_bytes)
    {
      read_load_.update_load_param(interval_s, cur_cnt, cur_bytes);
    }

    void update_write_load_param(const int64_t interval_s, const int64_t cur_cnt, const int64_t cur_bytes)
    {
      write_load_.update_load_param(interval_s, cur_cnt, cur_bytes);
    }

    TO_STRING_KV(K_(read_load), K_(write_load));
  };
  
  ObSSMicroCacheLoadInfo common_io_load_;
  ObSSMicroCacheLoadInfo prewarm_io_load_;

  ObSSMicroCacheLoad() { reset(); }
  void reset()
  {
    common_io_load_.reset();
    prewarm_io_load_.reset();
  }

  void update_common_io_load_info(const ObSSMicroCacheIOStat::ObSSMicroCacheIOStatParam &io_stat, const int64_t interval_s)
  {
    common_io_load_.update_write_load_param(interval_s, io_stat.add_cnt_, io_stat.add_bytes_);
    common_io_load_.update_read_load_param(interval_s, io_stat.get_cnt_, io_stat.get_bytes_);
  }

  void update_prewarm_io_load_info(const ObSSMicroCacheIOStat::ObSSMicroCacheIOStatParam &io_stat, const int64_t interval_s)
  {
    prewarm_io_load_.update_write_load_param(interval_s, io_stat.add_cnt_, io_stat.add_bytes_);
    prewarm_io_load_.update_read_load_param(interval_s, io_stat.get_cnt_, io_stat.get_bytes_);
  }

  bool is_common_io_high_throughput() const
  {
    return common_io_load_.write_load_.get_throughput() >= COMMON_IO_HIGH_THROUGHPUT;
  }

  TO_STRING_KV(K_(common_io_load), K_(prewarm_io_load));
};

struct ObSSMicroCacheStat
{
public:
  uint64_t tenant_id_;
  ObSSMicroCacheHitStat hit_stat_;
  ObSSMicroCachePhyBlkStat phy_blk_stat_;
  ObSSMicroCacheMemBlkStat mem_blk_stat_;
  ObSSMicroCacheMicroStat micro_stat_;
  ObSSMicroCacheTaskStat task_stat_;
  ObSSMicroCacheIOStat io_stat_;
  ObSSMicroCacheLoad cache_load_;

  ObSSMicroCacheStat() { reset(); }
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
    hit_stat_.tenant_id_ = tenant_id;
    phy_blk_stat_.tenant_id_ = tenant_id;
    mem_blk_stat_.tenant_id_ = tenant_id;
    micro_stat_.tenant_id_ = tenant_id;
    task_stat_.tenant_id_ = tenant_id;
  }
  void reset() 
  { 
    tenant_id_ = OB_INVALID_TENANT_ID;
    hit_stat_.reset(); 
    phy_blk_stat_.reset(); 
    mem_blk_stat_.reset(); 
    micro_stat_.reset(); 
    task_stat_.reset();
    io_stat_.reset();
    cache_load_.reset();
  }

  // only used when clear micro cache
  void clear_cache_stat_dynamic_info()
  {
    hit_stat_.reset_dynamic_info();
    task_stat_.reset_dynamic_info();
    micro_stat_.reset_dynamic_info();
    io_stat_.reset();
    cache_load_.reset();
  }
  ObSSMicroCacheHitStat &hit_stat() { return hit_stat_; }
  ObSSMicroCachePhyBlkStat &phy_blk_stat() { return phy_blk_stat_; }
  ObSSMicroCacheMemBlkStat &mem_blk_stat() { return mem_blk_stat_; }
  ObSSMicroCacheMicroStat &micro_stat() { return micro_stat_; }
  ObSSMicroCacheTaskStat &task_stat() { return task_stat_; }
  ObSSMicroCacheIOStat &io_stat() { return io_stat_; }
  ObSSMicroCacheLoad &cache_load() { return cache_load_; }

  TO_STRING_KV(K_(tenant_id), K_(hit_stat), K_(phy_blk_stat), K_(mem_blk_stat), K_(micro_stat), K_(task_stat),
    K_(io_stat), K_(cache_load));
  
public:
  static const int64_t OB_SS_MICRO_CACHE_STAT = 1;
  OB_UNIS_VERSION(OB_SS_MICRO_CACHE_STAT);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_STAT_H_ */
