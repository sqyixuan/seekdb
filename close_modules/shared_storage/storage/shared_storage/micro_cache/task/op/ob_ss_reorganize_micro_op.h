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
#ifndef OCEANBASE_STORAGE_SHARE_STORAGE_OB_REORGANIZE_MICRO_OP_H_
#define OCEANBASE_STORAGE_SHARE_STORAGE_OB_REORGANIZE_MICRO_OP_H_

#include "share/io/ob_io_define.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_op_base.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_basic_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_arc_info.h"

namespace oceanbase
{
namespace storage
{
struct ObSSPhyBlockEntry
{
public:
  bool reorganized_; // if any of its micro_block is reorganized, set as true
  int64_t phy_blk_idx_;
  ObSSPhysicalBlockHandle phy_blk_handle_;
  common::ObIOHandle io_handle_;

  ObSSPhyBlockEntry()
    : reorganized_(false), phy_blk_idx_(-1), phy_blk_handle_(), io_handle_() {}
  ObSSPhyBlockEntry(const int64_t phy_blk_idx, ObSSPhysicalBlock *phy_blk);
  ~ObSSPhyBlockEntry() { reset(); }
  void reset();
  int assign(const ObSSPhyBlockEntry &other);
  bool is_valid() const { return phy_blk_idx_ >= 0 && phy_blk_handle_.is_valid(); }

  TO_STRING_KV(K_(reorganized), K_(phy_blk_idx), K_(phy_blk_handle), K_(io_handle));
};

struct SSReorganizeBlkCtx
{
public:
  bool has_scanned_;
  int32_t block_size_;
  int64_t last_scan_time_;
  int64_t last_reorgan_time_;
  int64_t candidate_blk_cnt_;
  int64_t choosen_micro_cnt_;
  int64_t reorgan_micro_cnt_;
  int64_t reorgan_total_size_;
  int64_t revert_micro_cnt_;
  int64_t reorgan_used_blk_cnt_;
  int64_t reorgan_free_blk_cnt_;
  common::hash::ObHashMap<ObSSMicroBlockCacheKey, int64_t> marked_map_;

  SSReorganizeBlkCtx() {}
  ~SSReorganizeBlkCtx() { reset(); }
  int init(const uint64_t tenant_id, const int32_t block_size);
  void reset();
  void reuse();

  int add_reorgan_flag(const ObSSMicroBlockCacheKey &micro_key, const int32_t micro_size);
  int remove_reorgan_flag(const ObSSMicroBlockCacheKey &micro_key);
  bool need_revert_reorgan_flag() const;
  OB_INLINE void update_reorgan_total_size(const int64_t delta) { reorgan_total_size_ += delta; }
  OB_INLINE void update_revert_micro_cnt(const int64_t delta) { revert_micro_cnt_ += delta; }
  OB_INLINE void update_reorgan_used_blk_cnt(const int64_t delta) { reorgan_used_blk_cnt_ += delta; }
  OB_INLINE void update_reorgan_free_blk_cnt(const int64_t delta) { reorgan_free_blk_cnt_ += delta; }
  OB_INLINE int64_t new_free_blk_cnt() const { return reorgan_free_blk_cnt_ - reorgan_used_blk_cnt_; }

  TO_STRING_KV(K_(has_scanned), K_(last_scan_time), K_(last_reorgan_time), K_(candidate_blk_cnt), K_(choosen_micro_cnt), 
    K_(reorgan_micro_cnt), K_(reorgan_total_size), K_(revert_micro_cnt), K_(reorgan_used_blk_cnt), K_(reorgan_free_blk_cnt), 
    "free_blk_cnt", new_free_blk_cnt());
};

struct SSMicroBlockMetaIdx
{
public:
  int32_t size_;
  int32_t choosen_idx_;

  SSMicroBlockMetaIdx() : size_(0), choosen_idx_(-1) {}
  SSMicroBlockMetaIdx(const int32_t size, const int32_t choosen_idx)
    : size_(size), choosen_idx_(choosen_idx)
  {}

  TO_STRING_KV(K_(size), K_(choosen_idx));
};

class ObSSReorganizeMicroOp : public ObSSMicroCacheOpBase
{
public:
  ObSSReorganizeMicroOp();
  virtual ~ObSSReorganizeMicroOp() { destroy(); }
  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();

  int execute_reorganization();
  void clear_for_next_round();

private:
  bool can_trigger_reorganize();
  bool reach_scan_interval() const;
  int reorganize_phy_block();
  int read_sparse_phy_blocks();
  int handle_sparse_phy_blocks();
  int select_valid_micro_blocks(const int64_t idx, ObSSPhyBlockEntry &phy_blk_entry, char *data_buf);
  int reaggregate_micro_blocks();
  int write_micro_blocks(const common::ObIArray<SSMicroBlockMetaIdx> &micro_idx_arr);
  int write_micro_block(const int64_t micro_idx);
  int revert_micro_block_reorganizing_flag();
  int alloc_all_read_buf();
  void free_all_read_buf();

private:
  struct SSReorganMicroEntry
  {
  public:
    int64_t idx_; // the candidate_arr index of phy_blk which contains this micro_block
    ObSSMicroBlockMetaHandle micro_meta_handle_;
    char *data_ptr_;

    SSReorganMicroEntry() : idx_(-1), micro_meta_handle_(), data_ptr_(nullptr) {}
    ~SSReorganMicroEntry() { reset(); }

    void reset() { idx_ = -1; micro_meta_handle_.reset(); data_ptr_ = nullptr;}
    bool is_valid() const { return (idx_ >= 0) && (micro_meta_handle_.is_valid()) && (nullptr != data_ptr_) && 
                                   (micro_meta_handle_()->is_valid_field()); }
    // when use below func, please ensure this entry is valid
    bool is_in_l1() const { return micro_meta_handle_()->is_in_l1(); }
    bool is_in_ghost() const { return micro_meta_handle_()->is_in_ghost(); }
    int32_t length() const { return micro_meta_handle_()->length(); }
    uint64_t data_dest() const { return micro_meta_handle_()->data_dest(); }
    uint64_t reuse_version() const { return micro_meta_handle_()->reuse_version(); }
    uint32_t crc() const { return micro_meta_handle_()->crc(); }
    const ObSSMicroBlockCacheKey &micro_key() const { return micro_meta_handle_()->micro_key(); }
    int assign(const SSReorganMicroEntry &other);

    TO_STRING_KV(K_(idx), K_(micro_meta_handle), KP_(data_ptr));
  };

  struct SSMicroBlockMetaIdxCmp
  {
  public:
    bool operator() (const SSMicroBlockMetaIdx &le, const SSMicroBlockMetaIdx &re)
    {
      return le.size_ > re.size_;
    }
  };

private:
  static constexpr int64_t FAST_SCHEDULE_ARC_INTERVAL_US = 1000; // 1ms
  static constexpr int64_t SCAN_INTERVAL_US = 20L * 60 * 1000 * 1000; // 20min

private:
  SSReorganizeBlkCtx reorgan_ctx_;
  common::ObArray<void *> read_buf_arr_;
  common::ObArray<ObSSPhyBlockEntry> candidate_phy_blks_;
  common::ObArray<SSReorganMicroEntry> choosen_micro_blocks_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARE_STORAGE_OB_REORGANIZE_MICRO_OP_H_ */
