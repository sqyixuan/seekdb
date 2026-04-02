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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_MANAGER_H_

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_reorganize_micro_op.h"
#include "lib/container/ob_bitmap.h"
#include "lib/container/ob_2d_array.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace common
{
class ObConcurrentFIFOAllocator;
}
namespace storage
{
class ObSSLinkedPhyBlockItemReader;
struct ObSSMicroCacheStat;

struct SSPhyBlockCntInfo
{
public:
  struct DynamicPhyBlkStat
  {
  public:
    int64_t hold_cnt_; // range: [min_cnt_, max_cnt_]
    int64_t used_cnt_;
    int64_t min_cnt_;
    int64_t max_cnt_;

    void reset();
    void reuse();
    OB_INLINE bool reach_limit() const { return hold_cnt_ >= max_cnt_; }
    OB_INLINE int64_t free_blk_cnt() const { return hold_cnt_ - used_cnt_; }
    OB_INLINE bool is_valid() const
    {
      return hold_cnt_ >= min_cnt_ && hold_cnt_ <= max_cnt_ && min_cnt_ > 0 && used_cnt_ >= 0 && used_cnt_ <= hold_cnt_;
    }
    TO_STRING_KV(K_(hold_cnt), K_(used_cnt), K_(min_cnt), K_(max_cnt));
  };
public:
  int64_t total_blk_cnt_;
  int64_t super_blk_cnt_;
  int64_t phy_ckpt_blk_cnt_;
  int64_t phy_ckpt_blk_used_cnt_;
  int64_t shared_blk_cnt_;
  int64_t shared_blk_used_cnt_;
  DynamicPhyBlkStat data_blk_;    // record count info of block which used to store micro block data
  DynamicPhyBlkStat meta_blk_;    // record count info of block which used to store micro block meta
  // indicate how many blocks will be reserved for reorganize_task, these blocks will be allocated from data_blk.
  int64_t reorgan_blk_cnt_;
  ObSSMicroCacheStat &cache_stat_;

public:
  SSPhyBlockCntInfo(ObSSMicroCacheStat &cache_stat);
  ~SSPhyBlockCntInfo();
  void reset();
  void reuse();
  bool is_valid() const;
  void init_block_cnt_info(const int64_t total_blk_cnt, const int64_t block_size);
  int64_t calc_phy_ckpt_blk_cnt(const int32_t block_size) const;
  int64_t cache_limit_blk_cnt() const;
  int64_t shared_blk_free_cnt() const { return shared_blk_cnt_ - shared_blk_used_cnt_; }
  void try_reserve_dynamic_blk(const ObSSPhyBlockType type, const int64_t expect_cnt, int64_t &reserve_cnt);

  int64_t cache_data_blk_max_cnt() const;
  bool has_free_blk(const ObSSPhyBlockType type) const;
  int increase_blk_used_cnt(const ObSSPhyBlockType block_type);
  int decrease_blk_used_cnt(const ObSSPhyBlockType block_type);
  static bool is_dynamic_blk_type(const ObSSPhyBlockType block_type)
  {
    return ObSSPhyBlockType::SS_CACHE_DATA_BLK == block_type || ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK == block_type;
  }
  TO_STRING_KV(K_(total_blk_cnt), K_(super_blk_cnt), K_(phy_ckpt_blk_cnt), K_(phy_ckpt_blk_used_cnt),
      K_(shared_blk_cnt), K_(shared_blk_used_cnt), K_(data_blk), K_(meta_blk), K_(reorgan_blk_cnt));

private:
  void update_blk_used_cnt(const ObSSPhyBlockType type, const int64_t delta_cnt, const bool update_shared_blk);
};

/* Manage all blocks of micro_cache_file, to alloc and free block, to manage block info.
 *
 * 1. The first/second block(phy_blk_idx = 0/1) are used to store micro_cache super_block interchangeably.
 * 2. We won't use all blocks to store micro_block_data as cache. We will reserve some blocks.
 */
class ObSSPhysicalBlockManager
{
public:
  ObSSPhysicalBlockManager(ObSSMicroCacheStat &cache_stat, common::ObConcurrentFIFOAllocator &allocator);
  virtual ~ObSSPhysicalBlockManager() {}

  bool is_inited() const { return is_inited_; }
  int init(const uint64_t tenant_id, const int64_t total_file_size, const int32_t block_size);
  void destroy();
  void clear_phy_block_manager();
  int resize_file_size(const int64_t new_file_size, const int32_t block_size);
  int alloc_block(int64_t &phy_blk_idx, ObSSPhysicalBlockHandle &phy_blk_handle, const ObSSPhyBlockType block_type);
  // blocks that need to be free must be added into reusable_set first and then freed by reuse_task.
  // free() will only be called in the reuse task.
  // If this block is internal, we also need to set its valid length as 0
  int free_block(const int64_t phy_blk_idx, bool &succ_free);
  int try_free_batch_blocks(const common::ObIArray<int64_t> &block_idx_arr, int64_t &free_cnt);
  // divide cache_data block of @ls_id into @split_count ranges, which are left-open and
  // right-closed intervals: (start_blk_idx_, end_blk_idx_]
  // Note: @block_ranges.count() may be less than @split_count, since physical blocks are
  // dynamically and in real-time changing during divide block range
  int divide_cache_data_block_range(const share::ObLSID &ls_id,
                                     const int64_t split_count,
                                     ObIArray<ObSSPhyBlockIdxRange> &block_ranges);
  int update_block_valid_length(const uint64_t data_dest, const uint64_t reuse_version, const int32_t delta_len,
                                int64_t &phy_blk_idx);
  int get_block_handle(const uint64_t data_dest, const uint64_t exp_reuse_version, ObSSPhysicalBlockHandle &phy_blk_handle,
                       uint64_t &real_reuse_version);
  int get_block_handle(const uint64_t data_dest, const uint64_t exp_reuse_version, ObSSPhysicalBlockHandle &phy_blk_handle);
  int get_block_handle(const int64_t phy_blk_idx, ObSSPhysicalBlockHandle &phy_blk_handle);
  int get_block_reuse_version(common::ObIArray<uint64_t> &reuse_version_arr);
  int update_block_gc_reuse_version(const common::ObIArray<uint64_t> &version_arr);
  int update_block_state();

  // For ss_super_block and relative checkpoint
  int format_ss_super_block();
  int update_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk);
  int update_ss_super_block(const int64_t exp_modify_time, ObSSMicroCacheSuperBlock &ss_super_blk);
  int update_ss_super_block_for_ckpt(const bool is_micro_ckpt,
                                     ObSSMicroCacheSuperBlock &old_super_blk,
                                     ObSSMicroCacheSuperBlock &new_super_blk);
  int get_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk) const;
  int read_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk);
  int read_phy_block_checkpoint(ObSSLinkedPhyBlockItemReader &item_reader);

  int64_t get_reusable_set_count() const { return reusable_set_.size(); }
  int add_reusable_block(const int64_t phy_blk_idx);
  int add_batch_reusable_blocks(const common::ObIArray<int64_t> &block_idx_arr);
  int delete_reusable_block(const int64_t phy_blk_idx);
  int get_reusable_blocks(common::ObIArray<int64_t> &block_idx_arr, const int64_t max_cnt);
  int scan_blocks_to_reuse();
  int scan_blocks_to_ckpt(common::ObIArray<ObSSPhyBlockReuseInfo> &reuse_info_arr);
  void get_cache_data_block_info(int64_t &cache_limit_blk_cnt, int64_t &cache_data_blk_usage_pct) const;
  int64_t get_data_block_used_cnt() const;
  int64_t get_cache_limit_size() const;
  int32_t get_block_size() const;
  bool exist_free_block(const ObSSPhyBlockType blk_type) const;
  const SSPhyBlockCntInfo &get_blk_cnt_info() const { return blk_cnt_info_; }

  int64_t get_sparse_block_cnt() const { return sparse_blk_map_.count(); }
  void reserve_blk_for_reorganize(int64_t &available_cnt);
  void reserve_blk_for_micro_ckpt(int64_t &available_cnt);
  int add_sparse_block(const ObSSPhyBlockIdx &phy_blk_idx);
  int delete_sparse_block(const ObSSPhyBlockIdx &phy_blk_idx);
  int get_batch_sparse_blocks(common::ObArray<ObSSPhyBlockEntry> &candidate_phy_blks, const int64_t max_reorgan_cnt);
  int scan_sparse_blocks();

  // For obAdmin
  int get_phy_block_info(const int64_t phy_blk_idx, ObSSPhysicalBlock& block_info);
private:
  int init_phy_block_array(const uint64_t tenant_id, const int64_t count);
  int reinit_phy_block_array(const int64_t new_count);
  int init_free_bitmap(const uint64_t tenant_id, const int64_t count);
  int reinit_free_bitmap(const int64_t new_count);
  int init_block_count(const int64_t total_file_size, const int32_t block_size);
  int reinit_block_count(const int64_t new_file_size, const int32_t block_size);

  int do_resize_file_size(const int64_t new_file_size, const int32_t block_size);
  int do_write_ss_super_block(const ObSSMicroCacheSuperBlock &ss_super_blk, const bool is_format,
                              const bool allow_failed = false);
  int do_read_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk);
  int do_update_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk);
  int do_check_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk, char *read_buf, const int64_t read_size);
  // used for getting relative item of phy_block_arr, will do some operation, thus not const!!!
  ObSSPhysicalBlock *get_phy_block_by_idx_nolock(const int64_t phy_blk_idx);
  void set_phy_blk_stat(const int64_t cache_file_size);
  OB_INLINE bool is_valid_block_idx(const int64_t phy_blk_idx) const
  {
    return (phy_blk_idx >= SS_CACHE_SUPER_BLOCK_CNT) && (phy_blk_idx < blk_cnt_info_.total_blk_cnt_);
  }

private:
  struct SparsePhyBlockInfo
  {
  public:
    int64_t phy_blk_idx_;
    int32_t valid_len_;

    SparsePhyBlockInfo() : phy_blk_idx_(-1), valid_len_(0) {}
    SparsePhyBlockInfo(const int64_t phy_blk_idx, const int32_t valid_len)
        : phy_blk_idx_(phy_blk_idx), valid_len_(valid_len) {}
    TO_STRING_KV(K_(phy_blk_idx), K_(valid_len));
  };

  struct SparsePhyBlockInfoCmp
  {
  public:
    bool operator()(const SparsePhyBlockInfo &l, const SparsePhyBlockInfo &r)
    {
      return l.valid_len_ < r.valid_len_;
    }
  };

private:
  static constexpr int32_t PHY_BLOCK_SUB_ARR_SIZE = 4096; // sub_arr block count = 4096/sizeof(ObSSPhysicalBlock)

private:
  typedef common::Ob2DArray<ObSSPhysicalBlock, PHY_BLOCK_SUB_ARR_SIZE> PhyBlockArr;
  typedef common::ObLinearHashMap<ObSSPhyBlockIdx, bool> SparsePhyBlockMap;

  bool is_inited_;
  int32_t block_size_;
  int32_t sub_arr_blk_cnt_;
  uint64_t tenant_id_;
  int64_t total_file_size_;
  SSPhyBlockCntInfo blk_cnt_info_;
  common::SpinRWLock resize_lock_; // solve read<->update conflict about free_bitmap/phy_block_arr's pointer_address
  common::SpinRWLock info_lock_;   // solve read<->update conflict about free_bitmap/phy_block_arr's element, super_blk and blk_cnt_info.
  ObSSMicroCacheSuperBlock super_block_;
  ObBitmap *free_bitmap_; // 1: free, 0: used
  common::hash::ObHashSet<int64_t> reusable_set_; // ready to reuse these phy_block
  SparsePhyBlockMap sparse_blk_map_; // store sparse phy_blocks which can be reorganized
  PhyBlockArr phy_block_arr_; // use 2D array to manage, we need to promise that the existed phy_block's ptr won't be changed.
  common::ObConcurrentFIFOAllocator &allocator_;
  ObSSMicroCacheStat &cache_stat_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PHYSICAL_BLOCK_MANAGER_H_ */
