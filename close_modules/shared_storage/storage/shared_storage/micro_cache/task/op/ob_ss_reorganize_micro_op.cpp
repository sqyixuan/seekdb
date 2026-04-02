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

#include "storage/shared_storage/micro_cache/task/op/ob_ss_reorganize_micro_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_data_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSPhyBlockEntry-----------------------------------------*/
int ObSSPhyBlockEntry::assign(const ObSSPhyBlockEntry &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(phy_blk_handle_.assign(other.phy_blk_handle_))) {
      LOG_WARN("fail to assign phy_block handle", KR(ret));
    } else {
      reorganized_ = other.reorganized_;
      phy_blk_idx_ = other.phy_blk_idx_;
      io_handle_.reset();
    }
  }
  return ret;
}

ObSSPhyBlockEntry::ObSSPhyBlockEntry(const int64_t phy_blk_idx, ObSSPhysicalBlock *phy_blk)
    : reorganized_(false), phy_blk_idx_(phy_blk_idx), phy_blk_handle_(), io_handle_()
{
  phy_blk_handle_.set_ptr(phy_blk);
  io_handle_.reset();
}

void ObSSPhyBlockEntry::reset()
{
  reorganized_ = false;
  phy_blk_idx_ = -1;
  phy_blk_handle_.reset();
  io_handle_.reset();
}

/*-----------------------------------------SSReorganizeBlkCtx------------------------------------------*/
void SSReorganizeBlkCtx::reset()
{
  reuse();
  has_scanned_ = false;
  block_size_ = 0;
  last_scan_time_ = 0;
  last_reorgan_time_ = 0;
  marked_map_.destroy();
}

void SSReorganizeBlkCtx::reuse()
{
  last_reorgan_time_ = ObTimeUtility::current_time();
  candidate_blk_cnt_ = 0;
  choosen_micro_cnt_ = 0;
  reorgan_micro_cnt_ = 0;
  reorgan_total_size_ = 0;
  revert_micro_cnt_ = 0;
  reorgan_used_blk_cnt_ = 0;
  reorgan_free_blk_cnt_ = 0;
  marked_map_.clear();
}

int SSReorganizeBlkCtx::init(const uint64_t tenant_id, const int32_t block_size)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_BUCKET_NUM = 128;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(block_size));
  } else if (OB_FAIL(marked_map_.create(DEFAULT_BUCKET_NUM, ObMemAttr(tenant_id, "SSReorgMap")))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(tenant_id));
  } else {
    reuse();
    has_scanned_ = false;
    last_scan_time_ = 0;
    block_size_ = block_size;
  }
  return ret;
}

int SSReorganizeBlkCtx::add_reorgan_flag(
    const ObSSMicroBlockCacheKey &micro_key,
    const int32_t micro_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid() || micro_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_size));
  } else {
    const int64_t delta = micro_size + micro_key.get_serialize_size();
    if (OB_FAIL(marked_map_.set_refactored(micro_key, delta, 1/*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(micro_key), K(delta));
    } else {
      ATOMIC_INC(&choosen_micro_cnt_);
    }
  }
  return ret;
}

int SSReorganizeBlkCtx::remove_reorgan_flag(const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else if (OB_FAIL(marked_map_.set_refactored(micro_key, 0, 1/*overwrite*/))) {
    LOG_WARN("fail to set refactored", KR(ret), K(micro_key));
  } else {
    ATOMIC_INC(&reorgan_micro_cnt_);
  }
  return ret;
}

bool SSReorganizeBlkCtx::need_revert_reorgan_flag() const
{
  return ATOMIC_LOAD(&reorgan_micro_cnt_) < ATOMIC_LOAD(&choosen_micro_cnt_);
}

int ObSSReorganizeMicroOp::SSReorganMicroEntry::assign(const ObSSReorganizeMicroOp::SSReorganMicroEntry &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(micro_meta_handle_.assign(other.micro_meta_handle_))) {
      LOG_WARN("fail to assign micro_meta_handle", KR(ret), K(other));
    } else {
      idx_ = other.idx_;
      data_ptr_ = other.data_ptr_;
    }
  }
  return ret;
}

/*-----------------------------------------ObSSReorganizeMicroOp------------------------------------------*/
ObSSReorganizeMicroOp::ObSSReorganizeMicroOp()
  : ObSSMicroCacheOpBase(), reorgan_ctx_(), read_buf_arr_(), candidate_phy_blks_(), choosen_micro_blocks_()
{}

void ObSSReorganizeMicroOp::destroy()
{
  reorgan_ctx_.reset();
  candidate_phy_blks_.reset();
  choosen_micro_blocks_.reset();
  free_all_read_buf();
  ObSSMicroCacheOpBase::destroy();
}

int ObSSReorganizeMicroOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheOpBase::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(reorgan_ctx_.init(tenant_id, task_ctx.phy_blk_mgr_->get_block_size()))) {
    LOG_WARN("fail to init reorgan_ctx", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSReorganizeMicroOp::execute_reorganization()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (FALSE_IT(is_closed_ = (!is_op_enabled()))) { // reset 'is_closed' flag
  } else if (is_op_enabled()) {
    reorganize_phy_block();
  }
  return ret;
}

void ObSSReorganizeMicroOp::clear_for_next_round()
{
  candidate_phy_blks_.reset(); // not use 'ObArray.reuse()', cuz phy_block_handle needs to dec ref_cnt
  choosen_micro_blocks_.reset();
  free_all_read_buf();
  reorgan_ctx_.reuse();
}

OB_INLINE bool ObSSReorganizeMicroOp::reach_scan_interval() const
{
  return (ObTimeUtility::current_time() - reorgan_ctx_.last_scan_time_ > SCAN_INTERVAL_US);
}

OB_INLINE bool ObSSReorganizeMicroOp::can_trigger_reorganize()
{
  // To aggregate hot micro data, a single reorgan_task must be able to reorganize 
  // at least MIN_REORGAN_BLK_CNT sparse blocks.
  return phy_blk_mgr()->get_sparse_block_cnt() >= MIN_REORGAN_BLK_CNT;
}

int ObSSReorganizeMicroOp::reorganize_phy_block()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    // There are two situations that trigger a global scan:
    // 1. Collect all sparse phy_blocks after observer restart.
    // 2. No phy_block in sparse_blk_map and reach scan interval.
    if ((phy_blk_mgr()->get_sparse_block_cnt() == 0 && reach_scan_interval()) || !reorgan_ctx_.has_scanned_) {
      if (OB_FAIL(phy_blk_mgr()->scan_sparse_blocks())) {
        LOG_WARN("fail to scan sparse_block to reorganize", KR(ret));
      } else {
        reorgan_ctx_.last_scan_time_ = ObTimeUtility::current_time();
        reorgan_ctx_.has_scanned_ = true;
      }
    }

    if (OB_SUCC(ret) && can_trigger_reorganize()) {
      int64_t reserved_blk_cnt = 0;
      phy_blk_mgr()->reserve_blk_for_reorganize(reserved_blk_cnt);
      if (OB_UNLIKELY(0 == reserved_blk_cnt)) {
        reserved_blk_cnt = mem_data_mgr()->get_free_mem_blk_cnt(false);
        LOG_INFO("all data phy_blk were used up, will write into mem_blk", K(reserved_blk_cnt));
      }
      
      if (OB_FAIL(phy_blk_mgr()->get_batch_sparse_blocks(candidate_phy_blks_, reserved_blk_cnt))) {
        LOG_WARN("fail to get batch sparse blocks", KR(ret), K(reserved_blk_cnt));
      } else if (candidate_phy_blks_.count() >= MIN_REORGAN_BLK_CNT) {
        if (OB_FAIL(read_sparse_phy_blocks())) {
          LOG_WARN("fail to read sparse phy_blocks", KR(ret));
        } else if (OB_FAIL(handle_sparse_phy_blocks())) {
          LOG_WARN("fail to handle sparse phy_blocks", KR(ret));
        } else if (OB_FAIL(reaggregate_micro_blocks())) {
          LOG_WARN("fail to reaggregate micro_blocks", KR(ret));
        }

        if (OB_TMP_FAIL(revert_micro_block_reorganizing_flag())) {
          LOG_WARN("fail to revert micro_block reorganizing flag", KR(tmp_ret));
        }

        if (reorgan_ctx_.new_free_blk_cnt() < 0) {
          LOG_WARN("ss_micro_cache reorgan task not free phy_block", K_(reorgan_ctx));
        }
        const int64_t cost_us = get_cost_us();
        LOG_TRACE("ss_cache: finish reorgan task", KR(ret), K(cost_us), K_(reorgan_ctx));
      }
    }
  }
  
  return ret;
}

int ObSSReorganizeMicroOp::read_sparse_phy_blocks()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_FAIL(alloc_all_read_buf())) {
    LOG_WARN("fail to alloc read_buf", KR(ret), "phy_blk_cnt", candidate_phy_blks_.count());
  } else {
    reorgan_ctx_.candidate_blk_cnt_ = candidate_phy_blks_.count();
    LOG_TRACE("ss_cache: start reorgan task", "candidate_phy_blk_cnt", candidate_phy_blks_.count());

    for (int64_t i = 0; OB_SUCC(ret) && (i < reorgan_ctx_.candidate_blk_cnt_); ++i) {
      ObSSPhyBlockEntry &phy_blk_entry = candidate_phy_blks_[i];
      if (OB_UNLIKELY(!phy_blk_entry.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block entry should be valid", KR(ret), K(i), K(phy_blk_entry));
      } else {
        const int32_t block_size = task_ctx_->block_size_;
        const int64_t phy_blk_offset = phy_blk_entry.phy_blk_idx_ * block_size;
        ObSSPhysicalBlockHandle &phy_blk_handle = phy_blk_entry.phy_blk_handle_;
        ObIOHandle &io_handle = phy_blk_entry.io_handle_;
        char *buf = static_cast<char *>(read_buf_arr_.at(i));
        if (OB_ISNULL(buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("read buf should not be null", KR(ret), K(i), K_(reorgan_ctx), K(read_buf_arr_.count()));
        } else if (OB_FAIL(ObSSMicroCacheIOHelper::async_read_block(
                       phy_blk_offset, block_size, buf, phy_blk_handle, io_handle))) {
          LOG_WARN("fail to read micro_cache block", KR(ret), K(i), K(phy_blk_offset), KP(buf), K_(reorgan_ctx));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && (i < reorgan_ctx_.candidate_blk_cnt_); ++i) {
      ObSSPhyBlockEntry &phy_blk_entry = candidate_phy_blks_[i];
      if (OB_FAIL(phy_blk_entry.io_handle_.wait())) {
        LOG_WARN("fail to wait read phy_block", KR(ret), K(i), K(phy_blk_entry));
      }
    }
  }

  return ret;
}

int ObSSReorganizeMicroOp::handle_sparse_phy_blocks()
{
  int ret = OB_SUCCESS;
  const int64_t phy_blk_cnt = candidate_phy_blks_.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < phy_blk_cnt); ++i) {
    ObSSPhyBlockEntry &phy_blk_entry = candidate_phy_blks_[i];
    char *blk_data_buf = static_cast<char *>(read_buf_arr_.at(i));
    if (OB_ISNULL(blk_data_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block data buf should not be null", KR(ret), K(i), K(read_buf_arr_.count()));
    } else if (OB_FAIL(select_valid_micro_blocks(i, phy_blk_entry, blk_data_buf))) {
      LOG_WARN("fail to select valid micro_block of one phy_block", KR(ret), K(i), K(phy_blk_entry));
    }
    ret = OB_SUCCESS; // ignore ret, continue to handle next phy_block
  }
  return ret;
}

int ObSSReorganizeMicroOp::select_valid_micro_blocks(
    const int64_t candidate_idx,
    ObSSPhyBlockEntry &phy_blk_entry,
    char *data_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_ISNULL(data_buf) || OB_UNLIKELY(!phy_blk_entry.is_valid() || candidate_idx < 0 ||
             candidate_idx >= candidate_phy_blks_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data_buf), K(candidate_idx), K(phy_blk_entry), "candidate_cnt",
      candidate_phy_blks_.count());
  } else {
    const int32_t block_size = task_ctx_->block_size_;
    const int64_t phy_blk_idx = phy_blk_entry.phy_blk_idx_;
    ObSSPhyBlockCommonHeader common_header;
    ObSSNormalPhyBlockHeader blk_header;
    int64_t pos = 0;
    if (OB_FAIL(common_header.deserialize(data_buf, block_size, pos))) {
      LOG_WARN("fail to deserialize phy_blk common header", KR(ret), KP(data_buf), K(block_size));
    } else if (OB_UNLIKELY(!common_header.is_valid() || !common_header.is_cache_data_blk() || 
               pos != common_header.header_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common header is invalid or wrong type or deserialize error", KR(ret), K(common_header), K(pos));
    } else if (OB_FAIL(common_header.check_payload_checksum(data_buf + pos, common_header.payload_size_))) {
      LOG_WARN("fail to check payload checksum", KR(ret), K(common_header), K(block_size), K(pos), KP(data_buf));
    } else if (OB_FAIL(blk_header.deserialize(data_buf, block_size, pos))) {
      LOG_WARN("fail to deserialize phy_blk header", KR(ret), KP(data_buf), K(block_size));
    } else if (OB_UNLIKELY(!blk_header.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block_header is invalid", KR(ret), K(blk_header), K(candidate_idx), K(phy_blk_entry));
    } else if (OB_LIKELY(blk_header.micro_count_ > 0)) {
      ObSSMicroBlockIndex micro_index;
      const int32_t payload_offset = blk_header.payload_offset_;
      int32_t micro_data_offset = payload_offset;
      const int32_t micro_index_offset = blk_header.micro_index_offset_;
      int64_t index_pos = micro_index_offset;
      for (int64_t i = 0; OB_SUCC(ret) && (i < blk_header.micro_count_); ++i) {
        micro_index.reset();
        SSReorganMicroEntry reorgan_entry;
        bool is_exist = false;
        if (OB_FAIL(micro_index.deserialize(data_buf, block_size, index_pos))) {
          LOG_WARN("fail to deserialize phy_block micro index", KR(ret), K(i), K(block_size));
        } else if (OB_UNLIKELY(!micro_index.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block micro index should be valid", KR(ret), K(micro_index), K(i));
        } else {
          const ObSSMicroBlockCacheKey &micro_key = micro_index.get_micro_key();
          // 'is_reorganizing': false-> true;
          if (OB_FAIL(micro_meta_mgr()->try_mark_reorganizing_if_exist(micro_key, phy_blk_idx, 
              micro_data_offset, reorgan_entry.micro_meta_handle_))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS; // skip it, try to reorganize next micro_block
            } else {
              LOG_WARN("fail to try mark reorganizing", KR(ret), K(i), K(micro_key), K(phy_blk_idx));
            }
          } else if (OB_UNLIKELY(!reorgan_entry.micro_meta_handle_.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("micro_meta_handle in reorgan_entry should be valid", KR(ret), K(micro_key), K(reorgan_entry));
          } else if (OB_FAIL(reorgan_ctx_.add_reorgan_flag(micro_key, micro_index.get_size()))) {
            LOG_WARN("fail to add reorgan flag for micro_block", KR(ret), K(micro_key), K(micro_index));
          } else {
            phy_blk_entry.reorganized_ = true;
            reorgan_entry.data_ptr_ = data_buf + (reorgan_entry.data_dest() - phy_blk_idx * block_size);
            reorgan_entry.idx_ = candidate_idx;
            if (OB_FAIL(choosen_micro_blocks_.push_back(reorgan_entry))) {
              LOG_WARN("fail to push_back", KR(ret), K(i), K(micro_key), K(reorgan_entry));
            }
          }
          micro_data_offset += micro_index.get_size();
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY((index_pos - micro_index_offset) != blk_header.micro_index_size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("micro_block index size mismatch", KR(ret), K(blk_header), K(index_pos));
        }
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::reaggregate_micro_blocks()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (!choosen_micro_blocks_.empty()) {
    ObArray<SSMicroBlockMetaIdx> t1_micro_idx_arr;
    ObArray<SSMicroBlockMetaIdx> t2_micro_idx_arr;
    const int64_t choosen_micro_cnt = choosen_micro_blocks_.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < choosen_micro_cnt); ++i) {
      const SSReorganMicroEntry &reorgan_entry = choosen_micro_blocks_.at(i);
      const bool is_in_l1 = reorgan_entry.is_in_l1();
      const int32_t length = reorgan_entry.length();
      SSMicroBlockMetaIdx micro_idx(length, i);
      if (is_in_l1 && OB_FAIL(t1_micro_idx_arr.push_back(micro_idx))) {
        LOG_WARN("fail to push back", KR(ret), K(i), K(choosen_micro_cnt), K(micro_idx));
      } else if (!is_in_l1 && OB_FAIL(t2_micro_idx_arr.push_back(micro_idx))) {
        LOG_WARN("fail to push back", KR(ret), K(i), K(choosen_micro_cnt), K(micro_idx));
      } 
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(choosen_micro_cnt != (t1_micro_idx_arr.count() + t2_micro_idx_arr.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_idx_arr are mismatched", KR(ret), K(choosen_micro_cnt), K(t1_micro_idx_arr), K(t2_micro_idx_arr));
    } else if (choosen_micro_cnt > 0) {
      if (t1_micro_idx_arr.count() > 0) {
        std::sort(&t1_micro_idx_arr.at(0), &t1_micro_idx_arr.at(0) + t1_micro_idx_arr.count(), 
                  SSMicroBlockMetaIdxCmp());
      }
      if (t2_micro_idx_arr.count() > 0) {
        std::sort(&t2_micro_idx_arr.at(0), &t2_micro_idx_arr.at(0) + t2_micro_idx_arr.count(), 
                  SSMicroBlockMetaIdxCmp());
      }
      
      // WRITE STRATEGY:
      // 1. All micro_blocks will be splited into two arr, T1_ARRAY and T2_ARRAY. Each array is sorted by micro
      //    size with descending order
      // 2. Write large_size micro_block firstly! Write T2 micro_block firstly!
      // 3. If we have already handled all T2 micro_blocks, then we can handle T1 micro_blocks
      // 4. If current mem_block can't hold T2 large_size micro_block, we try to add T2 small_size micro_block.
      //    If it can't hold T2 small_size micro_block, create new bg_mem_block. The same with T1 micro_blocks.
      // 5. If we handled all T2 micro_blocks, and current bg_mem_block has some space, we can handle T1 micro_blocks.
      if (OB_FAIL(write_micro_blocks(t2_micro_idx_arr))) {
        LOG_WARN("fail to write micro_blocks", KR(ret), K(t2_micro_idx_arr.count()));
      } else if (OB_FAIL(write_micro_blocks(t1_micro_idx_arr))) {
        LOG_WARN("fail to write micro_blocks", KR(ret), K(t1_micro_idx_arr.count()));
      } else {
        cache_stat()->task_stat().update_reorgan_cnt(1);
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::write_micro_blocks(const ObIArray<SSMicroBlockMetaIdx> &micro_idx_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    const int64_t MAX_FAIL_ALLOC_CNT = 1;
    int64_t fail_alloc_cnt = 0; // if fail to alloc bg_mem_blk, add this value.
    int64_t low_idx = 0;
    int64_t high_idx = micro_idx_arr.count() - 1;
    bool handle_large_size = true;
    int64_t cur_idx = -1;
    while (OB_SUCC(ret) && (low_idx <= high_idx) && (fail_alloc_cnt < MAX_FAIL_ALLOC_CNT)) {
      cur_idx = handle_large_size ? low_idx : high_idx;
      const int64_t choosen_idx = micro_idx_arr.at(cur_idx).choosen_idx_;
      if (OB_FAIL(write_micro_block(choosen_idx))) {
        if (OB_SIZE_OVERFLOW == ret) { // 1. If cur bg_mem_blk can't hold this micro_block
          ret = OB_SUCCESS;
          if (handle_large_size) {     // 1.1 If can't hold large_size, try to hold small_size
            handle_large_size = false; 
          } else {                     // 1.2 If can't hold small_size, seal old and create new bg_mem_blk
            if (OB_FAIL(mem_data_mgr()->seal_and_alloc_new_bg_mem_blk())) {
              if (OB_ENTRY_NOT_EXIST == ret) {
                ++fail_alloc_cnt;
              }
              LOG_WARN("fail to seal and alloc new bg_mem_blk", KR(ret), K(cur_idx), K(low_idx), K(high_idx), K(choosen_idx),
                K(fail_alloc_cnt));
            } else {
              handle_large_size = true;
              reorgan_ctx_.update_reorgan_used_blk_cnt(1);
            }
          }
        } else {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ++fail_alloc_cnt;
          }
          LOG_WARN("fail to write micro_block in reorganize_task", KR(ret), K(cur_idx), K(low_idx), K(high_idx), K(choosen_idx),
            K(fail_alloc_cnt));
          ret = OB_SUCCESS; // ignore ret, continue handle next micro_block
          if (handle_large_size) {
            ++low_idx;
          } else {
            --high_idx;
          }
        }
      } else {                         // 2. If cur bg_mem_blk can hold this micro_block
        if (handle_large_size) {
          ++low_idx;                   // 2.1 If added a large_size, continue to add a large_size
        } else {
          --high_idx;                  // 2.2 If added a small_size, continue to add a small_size
        }
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::write_micro_block(const int64_t choosen_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_UNLIKELY(choosen_idx < 0 || choosen_idx >= choosen_micro_blocks_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(choosen_idx), "total_choosen_cnt", choosen_micro_blocks_.count());
  } else if (OB_UNLIKELY(!choosen_micro_blocks_.at(choosen_idx).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reorgan_entry should be valid", KR(ret), K(choosen_idx), "reorgan_entry",
      choosen_micro_blocks_.at(choosen_idx));
  } else {
    const SSReorganMicroEntry &reorgan_entry = choosen_micro_blocks_.at(choosen_idx);
    const ObSSMicroBlockCacheKey &micro_key = reorgan_entry.micro_key();
    ObSSMemBlockHandle mem_blk_handle;
    char *content = reorgan_entry.data_ptr_;
    const int32_t micro_size = reorgan_entry.length();
    uint32_t crc = 0;
    if (OB_FAIL(mem_data_mgr()->add_bg_micro_block_data(micro_key, content, micro_size, mem_blk_handle, crc))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("fail to add micro_block data", KR(ret), K(reorgan_entry));
      }
    } else {
      if (OB_UNLIKELY(crc != reorgan_entry.crc())) {
        ret = OB_CHECKSUM_ERROR;
        LOG_WARN("micro_block meta crc should not change", KR(ret), K(crc), K(reorgan_entry));
      // 'is_reorganizing': true-> false; 'is_persisted': true -> false; 'data_dest': disk -> memory
      } else if (OB_FAIL(micro_meta_mgr()->try_unmark_reorganizing_if_exist(micro_key, mem_blk_handle))) {
        LOG_WARN("fail to try unmark micro_block reorganizing", KR(ret), K(choosen_idx), K(micro_key), K(mem_blk_handle));
      } else if (OB_FAIL(reorgan_ctx_.remove_reorgan_flag(micro_key))) {
        LOG_WARN("fail to remove reorgan flag for micro_block", KR(ret), K(micro_key));
      } else {
        reorgan_ctx_.update_reorgan_total_size(micro_size);
        const int64_t idx = reorgan_entry.idx_;
        ObSSPhyBlockEntry &phy_blk_entry = candidate_phy_blks_.at(idx);
        int64_t phy_blk_idx = -1;
        if (OB_UNLIKELY(idx >= candidate_phy_blks_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block index is unexpected", KR(ret), K(reorgan_entry), "candidate_cnt",
            candidate_phy_blks_.count());
        } else if (OB_UNLIKELY(!phy_blk_entry.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("candidate phy_block is not valid", KR(ret), K(idx), K(phy_blk_entry));
        } else {
          phy_blk_idx = phy_blk_entry.phy_blk_idx_;
          ObSSPhysicalBlockHandle &phy_blk_handle = phy_blk_entry.phy_blk_handle_;
          const int64_t delta_micro_len = micro_size * -1;
          if (OB_FAIL(phy_blk_handle()->dec_valid_len(phy_blk_idx, delta_micro_len))) {
            LOG_WARN("fail to dec valid_len", KR(ret), K(phy_blk_idx), K(delta_micro_len), K(micro_key));
          } else if (phy_blk_handle()->is_empty()) {
            candidate_phy_blks_.at(idx).reorganized_ = false;
            phy_blk_handle.reset(); // dec phy_blk ref_cnt in advance
            cache_stat()->task_stat().update_reorgan_free_blk_cnt(1);
            reorgan_ctx_.update_reorgan_free_blk_cnt(1);
          }
        }
        LOG_TRACE("ss_cache: reorganize micro", KR(ret), K(micro_key), K(micro_size), K(phy_blk_idx), K(mem_blk_handle));
      }

      if (OB_LIKELY(mem_blk_handle()->is_valid())) {
        mem_blk_handle()->inc_handled_count();
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::revert_micro_block_reorganizing_flag()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (reorgan_ctx_.need_revert_reorgan_flag()) {
    hash::ObHashMap<ObSSMicroBlockCacheKey, int64_t>::const_iterator iter = reorgan_ctx_.marked_map_.begin();
    for (; OB_SUCC(ret) && (iter != reorgan_ctx_.marked_map_.end()); ++iter) {
      const ObSSMicroBlockCacheKey &micro_key = iter->first;
      if (iter->second > 0) {
        if (OB_FAIL(micro_meta_mgr()->force_unmark_reorganizing(micro_key))) {
          LOG_WARN("fail to force unmark reorganizing flag", KR(ret), K(micro_key));
        } else {
          reorgan_ctx_.update_revert_micro_cnt(1);
        }
      }
    }
  }
  return ret;
}

int ObSSReorganizeMicroOp::alloc_all_read_buf()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (read_buf_arr_.empty()) {
    ObMemAttr attr(tenant_id_, "SSReorgBuf");
    const int64_t buf_cnt = candidate_phy_blks_.count();
    const int32_t block_size = task_ctx_->block_size_;
    for (int64_t i = 0; OB_SUCC(ret) && (i < buf_cnt); ++i) {
      void *read_buf = nullptr;
      if (OB_ISNULL(read_buf = ob_malloc_align(SS_MEM_BUF_ALIGNMENT, block_size, attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(block_size), K(attr));
      } else if (OB_FAIL(read_buf_arr_.push_back(read_buf))) {
        LOG_WARN("fail to push back", KR(ret), K(i), K(buf_cnt), KP(read_buf));
      }
    }
  }
  return ret;
}

void ObSSReorganizeMicroOp::free_all_read_buf()
{
  const int64_t buf_cnt = read_buf_arr_.count();
  for (int64_t i = 0; i < buf_cnt; ++i) {
    if (nullptr != read_buf_arr_.at(i)) {
      ob_free_align(read_buf_arr_.at(i));
      read_buf_arr_.at(i) = nullptr;
    }
  }
  read_buf_arr_.reuse();
}

} // storage
} // oceanbase
