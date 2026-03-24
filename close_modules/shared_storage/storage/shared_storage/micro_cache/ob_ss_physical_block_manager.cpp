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

#include "ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_reader.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace hash;

/*-----------------------------------------SSPhyBlockCntInfo::DynamicPhyBlkStat------------------------------------------*/
void SSPhyBlockCntInfo::DynamicPhyBlkStat::reset()
{
  hold_cnt_ = 0;
  used_cnt_ = 0;
  min_cnt_ = 0;
  max_cnt_ = 0;
}

void SSPhyBlockCntInfo::DynamicPhyBlkStat::reuse()
{
  used_cnt_ = 0;
}

/*-----------------------------------------SSPhyBlockCntInfo------------------------------------------*/
SSPhyBlockCntInfo::SSPhyBlockCntInfo(ObSSMicroCacheStat &cache_stat) : cache_stat_(cache_stat)
{
  reset();
}

SSPhyBlockCntInfo::~SSPhyBlockCntInfo()
{
  reset();
}

void SSPhyBlockCntInfo::reset()
{
  total_blk_cnt_ = 0;
  super_blk_cnt_ = 0;
  shared_blk_cnt_ = 0;
  shared_blk_used_cnt_ = 0;
  phy_ckpt_blk_cnt_ = 0;
  phy_ckpt_blk_used_cnt_ = 0;
  reorgan_blk_cnt_ = 0;
  meta_blk_.reset();
  data_blk_.reset();
}

void SSPhyBlockCntInfo::reuse()
{
  phy_ckpt_blk_used_cnt_ = 0;
  data_blk_.reuse();
  meta_blk_.reuse();
}

bool SSPhyBlockCntInfo::is_valid() const
{
  return (total_blk_cnt_ > 0) && (super_blk_cnt_ > 0) && (shared_blk_cnt_ >= shared_blk_used_cnt_) &&
         (shared_blk_used_cnt_ == meta_blk_.hold_cnt_ + data_blk_.hold_cnt_) && (phy_ckpt_blk_cnt_ > 0) &&
         (phy_ckpt_blk_used_cnt_ >= 0) && meta_blk_.is_valid() && data_blk_.is_valid();
}

int64_t SSPhyBlockCntInfo::calc_phy_ckpt_blk_cnt(const int32_t block_size) const
{
  int64_t blk_cnt = 0; 
  const int32_t phy_ckpt_item_size = ObSSLinkedPhyBlockItemHeader::get_max_serialize_size() + 
                                     ObSSPhyBlockPersistInfo::get_max_serialize_size();
  if (phy_ckpt_item_size > 0) {
    ObSSLinkedPhyBlockHeader ckpt_header;
    const int32_t ckpt_header_size = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                     ckpt_header.get_fixed_serialize_size();
    const int32_t each_blk_item_cnt = (block_size - ckpt_header_size) / phy_ckpt_item_size;
    if (each_blk_item_cnt > 0) {
      const int64_t phy_ckpt_blk_cnt = ((total_blk_cnt_ + each_blk_item_cnt - 1) / each_blk_item_cnt);
      blk_cnt = MAX(phy_ckpt_blk_cnt * 2, 2);
    }
  }
  return blk_cnt;
}

void SSPhyBlockCntInfo::init_block_cnt_info(const int64_t total_blk_cnt, const int64_t block_size)
{
  total_blk_cnt_ = total_blk_cnt;
  super_blk_cnt_ = SS_CACHE_SUPER_BLOCK_CNT;
  phy_ckpt_blk_cnt_ = calc_phy_ckpt_blk_cnt(block_size);
  shared_blk_cnt_ = total_blk_cnt_ - super_blk_cnt_ - phy_ckpt_blk_cnt_;

  reorgan_blk_cnt_ = MTL_IS_MINI_MODE() ? MIN_REORGAN_BLK_CNT : MAX_REORGAN_BLK_CNT;
  data_blk_.min_cnt_ = MAX(reorgan_blk_cnt_, shared_blk_cnt_ * MIN_CACHE_DATA_BLOCK_CNT_PCT / 100);
  meta_blk_.min_cnt_ = MAX(2, shared_blk_cnt_ * MIN_CACHE_META_BLOCK_CNT_PCT / 100);
  
  data_blk_.max_cnt_ = shared_blk_cnt_ - meta_blk_.min_cnt_;
  meta_blk_.max_cnt_ = shared_blk_cnt_ - data_blk_.min_cnt_;

  // if resize cache_file_size, must ensure hold_cnt_ >= min_cnt_
  meta_blk_.hold_cnt_ = MAX(meta_blk_.hold_cnt_, meta_blk_.min_cnt_);
  data_blk_.hold_cnt_ = MAX(data_blk_.hold_cnt_, data_blk_.min_cnt_);
  shared_blk_used_cnt_ = meta_blk_.hold_cnt_ + data_blk_.hold_cnt_;
}

int64_t SSPhyBlockCntInfo::cache_limit_blk_cnt() const
{
  return MIN(data_blk_.hold_cnt_ + shared_blk_free_cnt(), data_blk_.max_cnt_) - reorgan_blk_cnt_;
}

void SSPhyBlockCntInfo::try_reserve_dynamic_blk(
    const ObSSPhyBlockType type, 
    const int64_t expect_cnt, 
    int64_t &available_cnt)
{
  available_cnt = 0;
  if (is_dynamic_blk_type(type)) {
    DynamicPhyBlkStat &dynamic_blk = (type == ObSSPhyBlockType::SS_CACHE_DATA_BLK ? data_blk_ : meta_blk_);
    if (expect_cnt <= dynamic_blk.free_blk_cnt()) {
      // don't need to reserve extra block from shared_block
      available_cnt = expect_cnt;
    } else {
      const int64_t delta_cnt = expect_cnt - dynamic_blk.free_blk_cnt();
      const int64_t limit_cnt = dynamic_blk.max_cnt_ - dynamic_blk.hold_cnt_;
      const int64_t shared_free_cnt = shared_blk_free_cnt();
      const int64_t extra_reserve_cnt = MIN(delta_cnt, MIN(limit_cnt, shared_free_cnt));

      shared_blk_used_cnt_ += extra_reserve_cnt;
      dynamic_blk.hold_cnt_ += extra_reserve_cnt;

      cache_stat_.phy_blk_stat().update_shared_block_used_cnt(extra_reserve_cnt);
      if (type == ObSSPhyBlockType::SS_CACHE_DATA_BLK) {
        cache_stat_.phy_blk_stat().update_data_block_cnt(extra_reserve_cnt);
      } else if (type == ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK) {
        cache_stat_.phy_blk_stat().update_meta_block_cnt(extra_reserve_cnt);
      }
      available_cnt = dynamic_blk.free_blk_cnt();
    }
  }
}

int64_t SSPhyBlockCntInfo::cache_data_blk_max_cnt() const
{
  return MIN(data_blk_.hold_cnt_ + shared_blk_free_cnt(), data_blk_.max_cnt_) - reorgan_blk_cnt_;
}

bool SSPhyBlockCntInfo::has_free_blk(const ObSSPhyBlockType type) const
{
  bool b_ret = false;
  if (ObSSPhyBlockType::SS_REORGAN_BLK == type) {
    b_ret = (data_blk_.free_blk_cnt() > 0) || (!data_blk_.reach_limit() && shared_blk_free_cnt() > 0);
  } else if (ObSSPhyBlockType::SS_CACHE_DATA_BLK == type) {
    b_ret = (data_blk_.free_blk_cnt() > reorgan_blk_cnt_) || (!data_blk_.reach_limit() && shared_blk_free_cnt() > 0);
  } else if (ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK == type) {
    b_ret = (meta_blk_.free_blk_cnt() > 0) || (!meta_blk_.reach_limit() && shared_blk_free_cnt() > 0);
  } else if (ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK == type) {
    b_ret = (phy_ckpt_blk_used_cnt_ < phy_ckpt_blk_cnt_);
  }
  return b_ret;
}

void SSPhyBlockCntInfo::update_blk_used_cnt(
    const ObSSPhyBlockType type, 
    const int64_t delta_cnt, 
    const bool update_shared_blk)
{
  if (ObSSPhyBlockType::SS_CACHE_DATA_BLK == type) {
    if (update_shared_blk) {
      shared_blk_used_cnt_ += delta_cnt;
      data_blk_.hold_cnt_ += delta_cnt;
      cache_stat_.phy_blk_stat().update_shared_block_used_cnt(delta_cnt);
      cache_stat_.phy_blk_stat().update_data_block_cnt(delta_cnt);
    }
    data_blk_.used_cnt_ += delta_cnt;
    cache_stat_.phy_blk_stat().update_data_block_used_cnt(delta_cnt);
  } else if (ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK == type) {
    if (update_shared_blk) {
      shared_blk_used_cnt_ += delta_cnt;
      meta_blk_.hold_cnt_ += delta_cnt;
      cache_stat_.phy_blk_stat().update_shared_block_used_cnt(delta_cnt);
      cache_stat_.phy_blk_stat().update_meta_block_cnt(delta_cnt);
    }
    meta_blk_.used_cnt_ += delta_cnt;
    cache_stat_.phy_blk_stat().update_meta_block_used_cnt(delta_cnt);
  } else if (ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK == type) {
    phy_ckpt_blk_used_cnt_ += delta_cnt;
    cache_stat_.phy_blk_stat().update_phy_ckpt_block_used_cnt(delta_cnt);
  }
}

int SSPhyBlockCntInfo::increase_blk_used_cnt(const ObSSPhyBlockType block_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSPhyBlockType::SS_INVALID_BLK_TYPE == block_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(block_type));
  } else if (ObSSPhyBlockType::SS_CACHE_DATA_BLK == block_type || ObSSPhyBlockType::SS_REORGAN_BLK == block_type) {
    if ((ObSSPhyBlockType::SS_CACHE_DATA_BLK == block_type && data_blk_.free_blk_cnt() > reorgan_blk_cnt_) ||
        (ObSSPhyBlockType::SS_REORGAN_BLK == block_type && data_blk_.free_blk_cnt() > 0)) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_CACHE_DATA_BLK, 1, /* update_shared_blk */false);
    } else if (!data_blk_.reach_limit() && shared_blk_free_cnt() > 0) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_CACHE_DATA_BLK, 1, /* update_shared_blk */true);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to increase blk_used_cnt", KR(ret), K(block_type), KPC(this));
    }
  } else if (ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK == block_type) {
    if (meta_blk_.free_blk_cnt() > 0) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK, 1, /* update_shared_blk */false);
    } else if (!meta_blk_.reach_limit() && shared_blk_free_cnt() > 0) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK, 1, /* update_shared_blk */true);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to increase blk_used_cnt", KR(ret), K(block_type), KPC(this));
    }
  } else if (ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK == block_type) {
    if (phy_ckpt_blk_used_cnt_ < phy_ckpt_blk_cnt_) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK, 1, /* update_shared_blk */false);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to increase blk_used_cnt", KR(ret), K(block_type), KPC(this));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("block_type is wrong", KR(ret), K(block_type), KPC(this));
  }

  if (FAILEDx(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("blk_cnt_info is invalid", KR(ret), K(block_type), KPC(this));
  }
  return ret;
}

int SSPhyBlockCntInfo::decrease_blk_used_cnt(const ObSSPhyBlockType block_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSPhyBlockType::SS_INVALID_BLK_TYPE == block_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(block_type));
  } else if (ObSSPhyBlockType::SS_CACHE_DATA_BLK == block_type) {
    if ((data_blk_.free_blk_cnt() < reorgan_blk_cnt_) || (data_blk_.hold_cnt_ == data_blk_.min_cnt_)) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_CACHE_DATA_BLK, -1, /* update_shared_blk */false);
    } else {
      update_blk_used_cnt(ObSSPhyBlockType::SS_CACHE_DATA_BLK, -1, /* update_shared_blk */true);
    }
  } else if (ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK == block_type) {
    if (meta_blk_.hold_cnt_ == meta_blk_.min_cnt_) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK, -1, /* update_shared_blk */false);
    } else {
      update_blk_used_cnt(ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK, -1, /* update_shared_blk */true);
    }
  } else if (ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK == block_type) {
      update_blk_used_cnt(ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK, -1, /* update_shared_blk */false);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("block_type is wrong", KR(ret), K(block_type), KPC(this));
  }

  if (FAILEDx(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("blk_cnt_info is invalid", KR(ret), K(block_type), KPC(this));
  }
  return ret;
}

/*-----------------------------------------ObSSPhysicalBlockManager------------------------------------------*/
ObSSPhysicalBlockManager::ObSSPhysicalBlockManager(ObSSMicroCacheStat &cache_stat, ObConcurrentFIFOAllocator &allocator)
    : is_inited_(false),
      block_size_(0),
      sub_arr_blk_cnt_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      total_file_size_(0),
      blk_cnt_info_(cache_stat),
      resize_lock_(),
      info_lock_(),
      super_block_(),
      free_bitmap_(nullptr),
      reusable_set_(),
      sparse_blk_map_(),
      phy_block_arr_(),
      allocator_(allocator),
      cache_stat_(cache_stat)
{}

void ObSSPhysicalBlockManager::destroy()
{
  SpinWLockGuard resize_guard(resize_lock_);
  SpinWLockGuard info_guard(info_lock_);
  if (nullptr != free_bitmap_) {
    free_bitmap_->~ObBitmap();
    allocator_.free(free_bitmap_);
    free_bitmap_ = nullptr;
  }
  phy_block_arr_.destroy();
  reusable_set_.destroy();
  sparse_blk_map_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  total_file_size_ = 0;
  blk_cnt_info_.reset();
  sub_arr_blk_cnt_ = 0;
  block_size_ = 0;
  super_block_.reset();
  is_inited_ = false;
}

void ObSSPhysicalBlockManager::clear_phy_block_manager()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t SLEEP_INTERVAL_US = 1000;
  const int64_t PRINT_LOG_INTERVAL_US = 10 * 1000 * 1000;
  {
    SpinWLockGuard resize_guard(resize_lock_);
    SpinWLockGuard info_guard(info_lock_);
    reusable_set_.reuse();
    for (int64_t i = SS_CACHE_SUPER_BLOCK_CNT; i < blk_cnt_info_.total_blk_cnt_; ++i) {
      bool succ_free = false;
      ObSSPhysicalBlock *phy_block = get_phy_block_by_idx_nolock(i);
      if (OB_NOT_NULL(phy_block) && !phy_block->is_free()) {
        const ObSSPhyBlockType block_type = phy_block->get_block_type();
        phy_block->set_valid_len(0); // all its data was already been cleared
        phy_block->try_free(succ_free); // ignore ret
        if (succ_free) {
          free_bitmap_->set(i, true);
          if (OB_TMP_FAIL(blk_cnt_info_.decrease_blk_used_cnt(block_type))) {
            LOG_ERROR("fail to decrease blk_used_cnt", KR(tmp_ret), K(i), K(block_type), KPC(phy_block));
          }
        } else if (OB_TMP_FAIL(add_reusable_block(i))) {
          LOG_ERROR("fail to add into reusable_set, phy_blk leak!!!", KR(tmp_ret), K(i), KPC(phy_block));
        }
      }
    }
    cache_stat_.phy_blk_stat().set_reusable_block_cnt(reusable_set_.size());
    
    ObSSMicroCacheSuperBlock new_super_block(tenant_id_, total_file_size_);
    if (OB_FAIL(do_update_ss_super_block(new_super_block))) {
      LOG_ERROR("fail to clear ss_super_block", KR(ret), K(new_super_block));
    }
  }
}

int ObSSPhysicalBlockManager::init(
    const uint64_t tenant_id,
    const int64_t total_file_size, 
    const int32_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || total_file_size <= 0 || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(total_file_size), K(block_size));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited), K(tenant_id));
  } else {
    const int64_t tmp_blk_cnt = total_file_size / block_size;
    const int64_t tmp_file_size = tmp_blk_cnt * block_size;
    sub_arr_blk_cnt_ = PhyBlockArr::BLOCK_CAPACITY;
    block_size_ = block_size;
    const int64_t DEFAULT_BUCKET_NUM = 128;
    if (OB_FAIL(reusable_set_.create(DEFAULT_BUCKET_NUM, ObMemAttr(tenant_id, "SSReuseSet")))) {
      LOG_WARN("fail to create reusable_set", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sparse_blk_map_.init(ObMemAttr(tenant_id, "SparseBlkMap")))) {
      LOG_WARN("fail to init sparse_blk_map", KR(ret), K(tenant_id));
    } else if (OB_FAIL(init_phy_block_array(tenant_id, tmp_blk_cnt))) {
      LOG_WARN("fail to init phy_block_array", KR(ret), K(tenant_id), K(tmp_blk_cnt), K(block_size));
    } else if (OB_FAIL(init_free_bitmap(tenant_id, tmp_blk_cnt))) {
      LOG_WARN("fail to init free_bitmap", KR(ret), K(tenant_id), K(tmp_blk_cnt));
    } else if (OB_FAIL(init_block_count(tmp_file_size, block_size))) {
      LOG_WARN("fail to init phy_block count", KR(ret), K(tmp_file_size), K(block_size));
    } else {
      tenant_id_ = tenant_id;
      total_file_size_ = tmp_file_size;
      is_inited_ = true;
      set_phy_blk_stat(total_file_size);
      LOG_INFO("succ to init ss_phy_blk_mgr", K_(total_file_size), K_(block_size), K_(blk_cnt_info));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::resize_file_size(
    const int64_t new_file_size, 
    const int32_t block_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(do_resize_file_size(new_file_size, block_size))) {
    LOG_WARN("fail to do resize file size", KR(ret), K_(tenant_id), K_(total_file_size), 
      K(new_file_size), K(block_size));
  }
  return ret;
}

int ObSSPhysicalBlockManager::do_resize_file_size(
    const int64_t new_file_size, 
    const int32_t block_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_total_block_cnt = 0;
  int64_t tmp_total_file_size = 0;
  ObSSMicroCacheSuperBlock new_super_block;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    SpinWLockGuard resize_guard(resize_lock_);
    SpinWLockGuard info_guard(info_lock_);
    const int64_t ori_file_size = total_file_size_;
    // Now only support 'increase file size'
    if (OB_UNLIKELY(new_file_size < ori_file_size || block_size <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(new_file_size), K(ori_file_size), K(block_size));
    } else if (OB_UNLIKELY(!super_block_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_super_block should be valid", KR(ret), K_(super_block));
    } else if (OB_FAIL(new_super_block.assign(super_block_))) {
      LOG_WARN("fail to assign ss_super_block", KR(ret), K_(super_block));
    } else if (OB_FAIL(new_super_block.update_cache_file_size(new_file_size))) {
      LOG_WARN("fail to update cache_file_size in super_block", KR(ret), K(new_file_size), K(new_super_block));
    } else if (FALSE_IT(tmp_total_block_cnt = new_file_size / block_size)) {
    } else if (FALSE_IT(tmp_total_file_size = tmp_total_block_cnt * block_size)) {
    } else if (tmp_total_file_size == total_file_size_) { // skip it
    } else if (OB_FAIL(reinit_phy_block_array(tmp_total_block_cnt))) {
      LOG_WARN("fail to reinit phy_block_array", KR(ret), K(tmp_total_block_cnt), K_(blk_cnt_info));
    } else if (OB_FAIL(reinit_free_bitmap(tmp_total_block_cnt))) {
      LOG_WARN("fail to reinit free_bitmap", KR(ret), K(tmp_total_block_cnt), K_(blk_cnt_info));
    } else if (OB_FAIL(reinit_block_count(tmp_total_file_size, block_size))) {
      LOG_WARN("fail to reinit block count", KR(ret), K(tmp_total_file_size), K(block_size));
    } else {
      total_file_size_ = tmp_total_file_size;
      set_phy_blk_stat(new_file_size);
    }

    if (FAILEDx(do_write_ss_super_block(new_super_block, false/*is_format*/, true/*allow_failed*/))) {
      LOG_WARN("fail to do write ss_super_block", KR(ret), K(new_super_block));
    } else if (OB_FAIL(super_block_.assign(new_super_block))) {
      LOG_WARN("fail to assign ss_super_block", KR(ret), K(new_super_block));
    }
    FLOG_INFO("finish to resize ss_micro_cache file size", KR(ret), K(ori_file_size), K(new_file_size), K_(blk_cnt_info));
  }
  return ret;
}

int ObSSPhysicalBlockManager::alloc_block(
    int64_t &phy_blk_idx, 
    ObSSPhysicalBlockHandle &phy_blk_handle,
    const ObSSPhyBlockType block_type)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard resize_guard(resize_lock_);
  SpinWLockGuard info_guard(info_lock_);
  phy_blk_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(ObSSPhyBlockType::SS_INVALID_BLK_TYPE == block_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_type));
  } else if (OB_UNLIKELY(!blk_cnt_info_.has_free_blk(block_type))) {
    ret = OB_EAGAIN;
    LOG_WARN("fail to alloc block cuz no free to alloc", KR(ret), K(block_type), K_(blk_cnt_info));
  } else if (OB_FAIL(free_bitmap_->next_valid_idx(SS_CACHE_SUPER_BLOCK_CNT,
             blk_cnt_info_.total_blk_cnt_ - SS_CACHE_SUPER_BLOCK_CNT, false, phy_blk_idx))) {
    LOG_ERROR("fail to get next valid idx", KR(ret), K(block_type), K_(blk_cnt_info));
  } else if (OB_UNLIKELY(!is_valid_block_idx(phy_blk_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid phy_block index", KR(ret), K(phy_blk_idx), K(block_type), K_(blk_cnt_info));
  } else if (FALSE_IT(free_bitmap_->set(phy_blk_idx, false))) {
    // No matter the meta of this phy_block is normal or not, we mark it as used in free_bitmap.
    // Otherwise, it will still alloc this phy_block next time.
  } else if (OB_FAIL(blk_cnt_info_.increase_blk_used_cnt(block_type))) {
    LOG_WARN("fail to increase blk_used_cnt", KR(ret), K(block_type), K_(blk_cnt_info));
  } else {
    ObSSPhysicalBlock *phy_block = get_phy_block_by_idx_nolock(phy_blk_idx);
    if (OB_ISNULL(phy_block)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(phy_blk_idx), K_(blk_cnt_info));
    } else if (OB_FAIL(phy_block->set_first_used(block_type))) {
      LOG_ERROR("fail to first set phy_block used", KR(ret), K(phy_blk_idx), K(block_type), KPC(phy_block));
      // revise this phy_blk's state, wait for scaning to reuse, avoid this phy_blk 'leak'
      phy_block->set_reusable(block_type);
    } else {
      phy_blk_handle.set_ptr(phy_block);
      LOG_TRACE("ss_cache: succ to alloc phy_blk", K(phy_blk_idx), K(block_type), K_(blk_cnt_info), KPC(phy_block));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::free_block(const int64_t phy_blk_idx, bool &succ_free)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard resize_guard(resize_lock_);
  SpinWLockGuard info_guard(info_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_block_idx(phy_blk_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid phy_block index", KR(ret), K(phy_blk_idx), K_(blk_cnt_info));
  } else {
    ObSSPhysicalBlock *phy_block = get_phy_block_by_idx_nolock(phy_blk_idx);
    if (OB_ISNULL(phy_block)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(phy_blk_idx), K_(blk_cnt_info));
    } else if (OB_UNLIKELY(phy_block->is_free())){
      // If phy_block is free, do nothing.
      // Skip the following free process to prevent 'double free'.
      LOG_INFO("phy_block has been freed", K(phy_blk_idx), KPC(phy_block));
    } else {
      const bool is_sealed = phy_block->is_sealed();
      const ObSSPhyBlockType block_type = phy_block->get_block_type();
      if (is_ckpt_block_type(block_type)) {
        // cuz ckpt phy_blk won't update its length to 0, so here we set its valid_len as 0.
        phy_block->set_valid_len(0);
      }
      succ_free = false;
      if (OB_FAIL(phy_block->try_free(succ_free))) {
        LOG_WARN("fail to try free phy_blk", KR(ret), KPC(phy_block));
      } else if (succ_free) {
        free_bitmap_->set(phy_blk_idx, true);
        if (OB_FAIL(blk_cnt_info_.decrease_blk_used_cnt(block_type))) {
          LOG_WARN("fail to decrease blk_used_cnt", KR(ret), K(phy_blk_idx), K(block_type), KPC(phy_block));
        } else if (is_sealed) {
          if (OB_FAIL(delete_sparse_block(ObSSPhyBlockIdx(phy_blk_idx)))) {
            LOG_WARN("fail to delete sparse block", KR(ret), K(phy_blk_idx), KPC(phy_block));
          }
          LOG_TRACE("ss_cache: succ to free phy_blk", K(phy_blk_idx), K(block_type), K_(blk_cnt_info), KPC(phy_block));
        } else {
          LOG_INFO("ss_cache: succ to free unsealed phy_blk", K(phy_blk_idx), K(block_type), K_(blk_cnt_info),
            KPC(phy_block));
        }
      }

      if (OB_SUCC(ret) && succ_free) {
        if (OB_FAIL(delete_reusable_block(phy_blk_idx))) {
          LOG_WARN("fail to delete block from reusable set", KR(ret), K(phy_blk_idx));
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::try_free_batch_blocks(const ObIArray<int64_t> &block_idx_arr, int64_t &free_cnt)
{
  int ret = OB_SUCCESS;
  free_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_LIKELY(!block_idx_arr.empty())) {
    ObSEArray<int64_t, 64> unfree_blk_arr;
    const int64_t total_cnt = block_idx_arr.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < total_cnt); ++i) {
      bool succ_free = false;
      const int64_t blk_idx = block_idx_arr.at(i);
      if (OB_FAIL(free_block(blk_idx, succ_free))) {
        LOG_WARN("fail to free block", KR(ret), K(blk_idx));
      } else if (succ_free) {
        ++free_cnt;
      } else if (OB_FAIL(unfree_blk_arr.push_back(blk_idx))) {
        LOG_WARN("fail to push back", KR(ret), K(i), K(blk_idx));
      }
    }

    if (OB_SUCC(ret) && (unfree_blk_arr.count() > 0)) {
      if (OB_FAIL(add_batch_reusable_blocks(unfree_blk_arr))) {
        LOG_ERROR("fail to add reusable phy_blocks", KR(ret), K(unfree_blk_arr), "arr_cnt", unfree_blk_arr.count());
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::add_reusable_block(const int64_t phy_blk_idx)
{
  int ret = OB_SUCCESS;  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(phy_blk_idx < SS_CACHE_SUPER_BLOCK_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_idx));
  } else if (OB_FAIL(reusable_set_.set_refactored(phy_blk_idx, 1/*overwrite*/))) {
    LOG_WARN("fail to add block into reusable set", KR(ret), K(phy_blk_idx));
  } else {
    LOG_TRACE("ss_cache: add reusable phy_blk", K(phy_blk_idx));
  }
  return ret;
}

int ObSSPhysicalBlockManager::add_batch_reusable_blocks(const ObIArray<int64_t> &block_idx_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < block_idx_arr.count(); i++) {
      const int64_t phy_blk_idx = block_idx_arr.at(i);
      if (OB_FAIL(add_reusable_block(phy_blk_idx))) {
        LOG_WARN("fail to do add block reusable block", KR(ret), K(i), K(phy_blk_idx));
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::delete_reusable_block(const int64_t phy_blk_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(phy_blk_idx < SS_CACHE_SUPER_BLOCK_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_idx));
  } else if (OB_FAIL(reusable_set_.erase_refactored(phy_blk_idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase from reusable_set", KR(ret), K(phy_blk_idx));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_reusable_blocks(
    ObIArray<int64_t> &block_idx_arr, 
    const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(max_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_idx_arr.count()), K(max_cnt));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);

    int64_t phy_blk_idx = -1;
    const int64_t count = MIN(max_cnt, reusable_set_.size());
    ObHashSet<int64_t>::iterator iter = reusable_set_.begin();
    for (int64_t i = 0; OB_SUCC(ret) && i < count && iter != reusable_set_.end(); i++, iter++) {
      phy_blk_idx = iter->first;
      if (OB_UNLIKELY(!is_valid_block_idx(phy_blk_idx))) {
        LOG_WARN("invalid block_idx", KR(ret), K(phy_blk_idx), K_(blk_cnt_info));
        if (OB_TMP_FAIL(delete_reusable_block(phy_blk_idx))) {
          LOG_WARN("fail to delete block from resuable_set", KR(tmp_ret), K(phy_blk_idx));
        }
      } else if (OB_FAIL(block_idx_arr.push_back(phy_blk_idx))) {
        LOG_WARN("fail to push block idx", KR(ret), K(phy_blk_idx));
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::scan_blocks_to_reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);

    ObSSPhysicalBlock *phy_blk = nullptr;
    for (int64_t i = SS_CACHE_SUPER_BLOCK_CNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); i++) {
      if (OB_ISNULL(phy_blk = get_phy_block_by_idx_nolock(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else if (phy_blk->can_reuse() && OB_FAIL(add_reusable_block(i))) {
        LOG_WARN("fail to add block into reusable_set", KR(ret), K(i), K_(blk_cnt_info), KPC(phy_blk));
      }
    }
  }
  return ret;
}

void ObSSPhysicalBlockManager::reserve_blk_for_reorganize(int64_t &available_cnt)
{
  if (IS_INIT) {
    // estimate the count of blocks needed for reorganize_task based on current avg_micro_size
    int64_t estimate_cnt = 0;
    const int64_t avg_micro_size = cache_stat_.micro_stat().get_avg_micro_size();
    if (avg_micro_size <= REORGAN_MIN_MICRO_SIZE) {
      estimate_cnt = MIN_REORGAN_BLK_CNT;
    } else {
      const int64_t scale_cnt = (avg_micro_size - REORGAN_MIN_MICRO_SIZE) / REORGAN_BLK_SCALING_FACTOR;
      estimate_cnt = scale_cnt + MIN_REORGAN_BLK_CNT;
    }

    SpinRLockGuard resize_guard(resize_lock_);
    SpinWLockGuard info_guard(info_lock_);

    estimate_cnt = MIN(estimate_cnt, blk_cnt_info_.reorgan_blk_cnt_);
    blk_cnt_info_.try_reserve_dynamic_blk(ObSSPhyBlockType::SS_CACHE_DATA_BLK, estimate_cnt, available_cnt);
  }
}

void ObSSPhysicalBlockManager::reserve_blk_for_micro_ckpt(int64_t &available_cnt)
{
  if (IS_INIT) {
    const int64_t total_micro_cnt = cache_stat_.micro_stat().get_total_micro_cnt();
    const int64_t estimate_cnt = total_micro_cnt * AVG_MICRO_META_PERSIST_COST / block_size_ + 1;

    SpinRLockGuard resize_guard(resize_lock_);
    SpinWLockGuard info_guard(info_lock_);

    blk_cnt_info_.try_reserve_dynamic_blk(ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK, estimate_cnt, available_cnt);
  }
}

int ObSSPhysicalBlockManager::add_sparse_block(const ObSSPhyBlockIdx &phy_blk_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(phy_blk_idx.block_idx_ < SS_CACHE_SUPER_BLOCK_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_idx));
  } else if (OB_FAIL(sparse_blk_map_.insert(phy_blk_idx, true))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to add sparse_block", KR(ret), K(phy_blk_idx));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::delete_sparse_block(const ObSSPhyBlockIdx &phy_blk_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(phy_blk_idx.block_idx_ < SS_CACHE_SUPER_BLOCK_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_idx));
  } else if (OB_FAIL(sparse_blk_map_.erase(phy_blk_idx))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase from sparse_blk_map", KR(ret), K(phy_blk_idx));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_batch_sparse_blocks(
    ObArray<ObSSPhyBlockEntry> &candidate_phy_blks, 
    const int64_t max_reorgan_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!candidate_phy_blks.empty() || max_reorgan_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(candidate_phy_blks), K(max_reorgan_cnt));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);

    const int64_t max_scan_cnt = MIN(MAX_REORGAN_TASK_SCAN_CNT, sparse_blk_map_.count() + 1);
    ObArray<SparsePhyBlockInfo> scan_blk_arr;
    if (OB_FAIL(scan_blk_arr.reserve(max_scan_cnt))) {
      LOG_WARN("fail to reserve arr", KR(ret), K(max_scan_cnt));
    }

    // 1. Scan some phy_blocks randomly.
    ObSSPhysicalBlock *phy_block = nullptr;
    SparsePhyBlockMap::BlurredIterator iter(sparse_blk_map_);
    iter.rewind_randomly();
    int64_t scan_cnt = 0;
    while (OB_SUCC(ret) && (++scan_cnt <= max_scan_cnt)) {
      ObSSPhyBlockIdx phy_blk_idx;
      bool is_exist = false;
      bool invalid_block = false;
      if (OB_FAIL(iter.next(phy_blk_idx, is_exist))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          iter.rewind();
        } else {
          LOG_WARN("fail to next phy_block", KR(ret), K(scan_cnt), K(max_scan_cnt));
        }
      } else if (OB_UNLIKELY(!is_valid_block_idx(phy_blk_idx.block_idx_))) {
        LOG_WARN("block_idx is invalid", KR(ret), K(phy_blk_idx), K_(blk_cnt_info));
        invalid_block = true;
      } else if (OB_ISNULL(phy_block = get_phy_block_by_idx_nolock(phy_blk_idx.block_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(phy_blk_idx), K_(blk_cnt_info));
      } else if (OB_UNLIKELY(phy_block->is_empty())) {
        invalid_block = true;
      } else if (OB_UNLIKELY(!phy_block->can_reorganize(block_size_))) {
        LOG_ERROR("phy_block should be reorganizable", KR(ret), K(phy_blk_idx), K_(block_size), KPC(phy_block));
        invalid_block = true;
      } else {
        SparsePhyBlockInfo sparse_blk_info(phy_blk_idx.block_idx_, phy_block->get_valid_len());
        if (OB_FAIL(scan_blk_arr.push_back(sparse_blk_info))) {
          LOG_WARN("fail to push sparse_blk_info", KR(ret), K(sparse_blk_info));
        }
      }

      if (OB_SUCC(ret) && invalid_block) {
        if (OB_TMP_FAIL(delete_sparse_block(phy_blk_idx))) {
          LOG_WARN("fail to delete sparse_block", KR(tmp_ret), K(phy_blk_idx));
        }
      }
    } 

    // 2. Sort and select some phy_blocks with the smallest valid_len to reorganize.
    if (OB_SUCC(ret) && scan_blk_arr.count() > 0) {
      std::sort(scan_blk_arr.begin(), scan_blk_arr.end(), SparsePhyBlockInfoCmp());
      
      for (int64_t i = 0; OB_SUCC(ret) && candidate_phy_blks.count() < max_reorgan_cnt && i < scan_blk_arr.count(); ++i) {
        const int64_t cur_blk_idx = scan_blk_arr[i].phy_blk_idx_;
        // avoid pushing duplicate phy_block_idx
        bool repeated = false;
        for (int64_t j = 0; j < candidate_phy_blks.count(); ++j) {
          if (candidate_phy_blks[j].phy_blk_idx_ == cur_blk_idx) {
            repeated = true;
            break;
          }
        }

        if (!repeated) {
          phy_block = get_phy_block_by_idx_nolock(cur_blk_idx);
          if (OB_ISNULL(phy_block)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("phy_block should not be null", KR(ret), K(cur_blk_idx), K_(blk_cnt_info));
          } else {
            ObSSPhyBlockEntry phy_blk_entry(cur_blk_idx, phy_block);  // will gen phy_blk_handle, inc ref_cnt
            if (OB_FAIL(candidate_phy_blks.push_back(phy_blk_entry))) {
              LOG_WARN("fail to push back", KR(ret), K(phy_blk_entry));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        const uint64_t phy_blk_min_len = scan_blk_arr[0].valid_len_;
        cache_stat_.task_stat().set_phy_blk_min_len(phy_blk_min_len);
      }
    }
  }

  return ret;
}

int ObSSPhysicalBlockManager::scan_sparse_blocks()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    SpinRLockGuard guard_for_resize(resize_lock_);

    ObSSPhysicalBlock *phy_block = nullptr;
    for (int64_t i = SS_CACHE_SUPER_BLOCK_CNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); i++) {
      phy_block = get_phy_block_by_idx_nolock(i);

      if (OB_ISNULL(phy_block)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else {
        const int32_t blk_valid_len = phy_block->get_valid_len();
        if (OB_UNLIKELY(blk_valid_len > block_size_ || block_size_ <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("abnormal phy_block valid_len or block_size", KR(ret), KPC(phy_block), K_(block_size));
        } else if (phy_block->can_reorganize(block_size_) && OB_FAIL(add_sparse_block(ObSSPhyBlockIdx(i)))) {
          LOG_WARN("fail to add sparse_block", KR(ret), K(i), K_(block_size), KPC(phy_block));
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::format_ss_super_block()
{
  int ret = OB_SUCCESS;
  // invoked when start, thus no need to add resize_lock
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(super_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_super_block is already valid", KR(ret), K_(super_block));
  } else {
    ObSSMicroCacheSuperBlock tmp_super_blk(tenant_id_, total_file_size_);
    if (OB_FAIL(do_write_ss_super_block(tmp_super_blk, true/*is_format*/))) {
      LOG_WARN("fail to do write ss_super_block", KR(ret), K(tmp_super_blk));
    } else if (OB_FAIL(super_block_.assign(tmp_super_blk))) {
      LOG_WARN("fail to assign ss_super_block", KR(ret), K(tmp_super_blk));
    } else {
      FLOG_INFO("succ to format ss_super_block", K_(super_block));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_ss_super_block(ObSSMicroCacheSuperBlock &new_super_blk)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard resize_guard(resize_lock_);
  if (OB_UNLIKELY(!new_super_blk.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_super_blk));
  } else if (OB_FAIL(do_update_ss_super_block(new_super_blk))) {
    LOG_WARN("fail to do update ss_super_block", KR(ret), K(new_super_blk));
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_ss_super_block(
    const int64_t exp_modify_time, 
    ObSSMicroCacheSuperBlock &new_super_blk)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard resize_guard(resize_lock_);
  if (OB_UNLIKELY(!new_super_blk.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_super_blk));
  } else if (OB_UNLIKELY(exp_modify_time != super_block_.modify_time_us_)) {
    ret = OB_EAGAIN;
    LOG_WARN("modify_time mismatch", KR(ret), K(exp_modify_time), K_(super_block));
  } else if (OB_FAIL(do_update_ss_super_block(new_super_blk))) {
    LOG_WARN("fail to do update ss_super_block", KR(ret), K(new_super_blk));
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_ss_super_block_for_ckpt(
    const bool is_micro_ckpt,
    ObSSMicroCacheSuperBlock &old_super_blk,
    ObSSMicroCacheSuperBlock &new_super_blk)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard resize_guard(resize_lock_);
  if (OB_FAIL(old_super_blk.assign(super_block_))) {
    LOG_WARN("fail to assign ss_super_block", KR(ret), K_(super_block));
  } else if (OB_FAIL(new_super_blk.assign_by_ckpt(is_micro_ckpt, old_super_blk))) {
    LOG_WARN("fail to assign super_blk by ckpt", KR(ret), K(is_micro_ckpt), K(old_super_blk), K(new_super_blk));
  } else if (OB_FAIL(do_write_ss_super_block(new_super_blk, false/*is_format*/))) {
    LOG_WARN("fail to do write ss_super_block", KR(ret), K(new_super_blk));
  } else if (OB_FAIL(super_block_.assign(new_super_blk))) {
    LOG_WARN("fail to assign ss_super_block", KR(ret), K(new_super_blk), K_(super_block));
  } else {
    FLOG_INFO("ss_micro_cache: succ to update ss_super_block for ckpt", K_(super_block));
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    SpinRLockGuard resize_guard(resize_lock_);
    if (OB_FAIL(ss_super_blk.assign(super_block_))) {
      LOG_WARN("fail to assign ss_super_block", KR(ret), K_(super_block));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::read_ss_super_block(
    ObSSMicroCacheSuperBlock &ss_super_blk)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    SpinRLockGuard resize_guard(resize_lock_);
    if (OB_FAIL(do_read_ss_super_block(ss_super_blk))) {
      LOG_WARN("fail to read ss_super_block", KR(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(!ss_super_blk.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_super_block is invalid", KR(ret), K(ss_super_blk));
    } else if (OB_FAIL(super_block_.assign(ss_super_blk))) {
      LOG_WARN("fail to assign ss_super_block", KR(ret), K(ss_super_blk));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::do_write_ss_super_block(
    const ObSSMicroCacheSuperBlock &ss_super_blk,
    const bool is_format,
    const bool allow_failed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char *buf = nullptr;
  ObMemAttr attr(tenant_id_, "SSSuperBlk");
  const int64_t alignment = SS_MEM_BUF_ALIGNMENT;
  const int64_t super_blk_size = ss_super_blk.get_serialize_size();
  ObSSPhyBlockCommonHeader common_header;
  if (OB_UNLIKELY(!ss_super_blk.is_valid() || block_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ss_super_blk), K_(block_size));
  } else if (OB_UNLIKELY(super_blk_size + common_header.header_size_ > block_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf not enough", KR(ret), K_(block_size), K(super_blk_size), K(common_header));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc_align(alignment, block_size_, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(ss_super_blk), K(alignment), K(attr), K_(block_size));
  } else {
    int64_t pos = common_header.header_size_;
    if (OB_FAIL(ss_super_blk.serialize(buf, block_size_, pos))) {
      LOG_WARN("fail to serialize ss_super_block", KR(ret), K(ss_super_blk), K_(block_size), K(pos), KP(buf));
    } else {
      common_header.set_payload_size(pos - common_header.header_size_);
      common_header.set_block_type(ObSSPhyBlockType::SS_SUPER_BLK);
      common_header.calc_payload_checksum(buf + common_header.header_size_, common_header.payload_size_);
      pos = 0;
      if (OB_FAIL(common_header.serialize(buf, block_size_, pos))) {
        LOG_WARN("fail to serialize common header", KR(ret), K_(block_size), KP(buf));
      } else if (OB_UNLIKELY(pos != common_header.header_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pos is unexpected", KR(ret), K(pos), K(common_header));
      } else {
        const int64_t total_size = pos + common_header.payload_size_;
        ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
        const int64_t align_size = lib::align_up(total_size, alignment);
        int64_t write_size = 0;
        bool succ_write = false;
        if (OB_ISNULL(tnt_file_mgr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant file manager should not be null", KR(ret));
        } else {
          const int64_t MAX_RETRY_TIME = 100;
          for (int64_t i = 0; !succ_write && (i < MAX_RETRY_TIME); ++i) {
            // we allow one failure to write these two super_blocks.
            tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(tnt_file_mgr->pwrite_cache_block(0, align_size, buf, write_size))) {
              LOG_WARN("fail to persist 1st ss_super_block", KR(tmp_ret), K(align_size), K(write_size));
              tmp_ret = OB_SUCCESS;
            } else {
              succ_write = true;
            }

            if (OB_TMP_FAIL(tnt_file_mgr->pwrite_cache_block(block_size_, align_size, buf, write_size))) {
              LOG_WARN("fail to persist 2nd ss_super_block", KR(tmp_ret), K(pos), K(align_size), K(write_size));
            } else {
              succ_write = true;
            }

            if (OB_UNLIKELY(!succ_write)) {
              ret = tmp_ret;
              if (i >= 3) {
                LOG_ERROR("fail to write ss_super_blk multi times", KR(ret), K(i), K(ss_super_blk), K(is_format));
              }
              ob_usleep(100 * 1000);
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(tnt_file_mgr->fsync_cache_file())) {
            LOG_WARN("fail to fsync micro_cache data file", KR(ret));
          } else {
            LOG_INFO("succ to persist super block", K_(super_block), "new_super_block", ss_super_blk, K(pos), 
              K(align_size), K(is_format));
          }
        }
      }
    }
  }

  // if we fail to persist when we want to update super_block's cache_file_size, we still return OB_SUCCESS.
  if (OB_FAIL(ret) && allow_failed) {
    ret = OB_SUCCESS;
    FLOG_INFO("although fail to write ss_super_blk, we will ignore ret", K(ss_super_blk), K(is_format));
  }

  if (nullptr != buf) {
    ob_free_align(buf);
    buf = nullptr;
  }
  return ret;
}

int ObSSPhysicalBlockManager::do_read_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, "SSSuperBlk");
  const int64_t align_size = lib::align_up(ss_super_blk.get_serialize_size(), SS_MEM_BUF_ALIGNMENT);
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, align_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(align_size), K(attr));
  } else {
    ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
    int64_t read_size = 0;
    if (OB_ISNULL(tnt_file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant file manager should not be null", KR(ret));
    } else if (OB_FAIL(tnt_file_mgr->pread_cache_block(0, align_size, buf, read_size))) {
      LOG_WARN("fail to read ss_super_block", KR(ret), K(align_size));
    } else if (OB_FAIL(do_check_ss_super_block(ss_super_blk, buf, read_size))) {
      LOG_WARN("fail to try check ss_super_block", KR(ret), K(read_size));
    } else if (OB_FAIL(tnt_file_mgr->pread_cache_block(block_size_, align_size, buf, read_size))) {
      LOG_WARN("fail to read backup ss_super_block", KR(ret), K(align_size), K_(block_size));
    } else if (OB_FAIL(do_check_ss_super_block(ss_super_blk, buf, read_size))) {
      LOG_WARN("fail to try check ss_super_block", KR(ret), K(read_size), K_(block_size));
    } else if (OB_UNLIKELY(!ss_super_blk.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to get valid ss_super_block", KR(ret), K_(super_block));
    }
  }
  
  if (nullptr != buf) {
    ob_free_align(buf);
    buf = nullptr;
  }
  return ret;
}

int ObSSPhysicalBlockManager::do_update_ss_super_block(ObSSMicroCacheSuperBlock &ss_super_blk)
{
  int ret = OB_SUCCESS;
  // in case of resize cache_file_size.
  const int64_t new_cache_file_size = ss_super_blk.cache_file_size_;
  if (new_cache_file_size < super_block_.cache_file_size_) {
    ss_super_blk.cache_file_size_ = super_block_.cache_file_size_;
  }

  if (OB_FAIL(do_write_ss_super_block(ss_super_blk, false/*is_format*/))) {
    LOG_WARN("fail to do write ss_super_block", KR(ret), K(ss_super_blk));
  } else if (OB_FAIL(super_block_.assign(ss_super_blk))) {
    LOG_WARN("fail to assign ss_super_block", KR(ret), K(ss_super_blk));
  } else {
    FLOG_INFO("succ to update ss_super_block", K_(super_block), K(new_cache_file_size));
  }
  return ret;
}

int ObSSPhysicalBlockManager::do_check_ss_super_block(
    ObSSMicroCacheSuperBlock &ss_super_blk,
    char *read_buf, 
    const int64_t read_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroCacheSuperBlock tmp_super_blk;
  if (OB_FAIL(common_header.deserialize(read_buf, read_size, pos))) {
    LOG_WARN("fail to deserialize common header", KR(ret), KP(read_buf), K(read_size), K(pos));
  } else if (OB_UNLIKELY(common_header.header_size_ != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized pos is unexpected", KR(ret), K(common_header), K(pos));
  } else if (OB_UNLIKELY(!common_header.is_valid() || (!common_header.is_super_blk()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized common header is invalid or wrong type", KR(ret), K(common_header));
  } else if (OB_FAIL(common_header.check_payload_checksum(read_buf + pos, common_header.payload_size_))) {
    LOG_WARN("fail to check common header payload checksum", KR(ret), K(common_header), KP(read_buf), K(pos));
  } else if (OB_FAIL(tmp_super_blk.deserialize(read_buf, read_size, pos))) {
    LOG_WARN("fail to deserialize super_block", KR(ret), KP(read_buf), K(read_size), K(pos));
  } else if (FALSE_IT(tmp_super_blk.tenant_id_ = tenant_id_)) {
  } else if (OB_UNLIKELY(!tmp_super_blk.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized super_block is invalid", KR(ret), K(tmp_super_blk), K(pos));
  }

  if (OB_FAIL(ret)) {
    // ignore ret, cuz if main super_block read failed, we can also read backup super_block
    ret = OB_SUCCESS;
  } else {
    if (!ss_super_blk.is_valid()) {
      if (OB_FAIL(ss_super_blk.assign(tmp_super_blk))) {
        LOG_WARN("fail to assign ss_super_block", KR(ret), K(tmp_super_blk));
      }
    } else if (ss_super_blk.modify_time_us_ < tmp_super_blk.modify_time_us_) {
      if (OB_FAIL(ss_super_blk.assign(tmp_super_blk))) {
        LOG_WARN("fail to assign ss_super_block", KR(ret), K(tmp_super_blk));
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::init_phy_block_array(const uint64_t tenant_id, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(count), K_(blk_cnt_info));
  } else {
    phy_block_arr_.set_tenant_id(tenant_id);
    phy_block_arr_.set_label("SSPhyBlk");

    const int64_t actual_cnt = MAX(count, sub_arr_blk_cnt_);
    if (OB_FAIL(phy_block_arr_.prepare_allocate(actual_cnt))) {
      LOG_WARN("fail to prepare allocate", KR(ret), K(actual_cnt), K(count));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::reinit_phy_block_array(const int64_t new_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_count <= blk_cnt_info_.total_blk_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("currently only support increasing count", KR(ret), K_(tenant_id), K(new_count), 
      K_(blk_cnt_info));
  } else {
    const int64_t actual_cnt = MAX(new_count, sub_arr_blk_cnt_);
    if (OB_FAIL(phy_block_arr_.prepare_allocate(actual_cnt))) {
      LOG_WARN("fail to prepare allocate", KR(ret), K(actual_cnt), K(new_count));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::init_free_bitmap(const uint64_t tenant_id, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(count));
  } else if (nullptr != free_bitmap_ && count <= blk_cnt_info_.total_blk_cnt_) {
    // not support decreasing capacity now!
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("currently only support increase count", KR(ret), K(count), K_(blk_cnt_info));
  } else {
    ObMemAttr attr(tenant_id, "SSBlkBitMap");
    ObBitmap *tmp_free_bitmap = nullptr;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBitmap), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), "size", sizeof(ObBitmap));
    } else if (FALSE_IT(tmp_free_bitmap = new (buf) ObBitmap(allocator_))) {
    } else if (OB_ISNULL(tmp_free_bitmap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("created free_bitmap should not be null", KR(ret));
    } else if (OB_FAIL(tmp_free_bitmap->init(count, true/*is_all_true*/))) {
      LOG_WARN("fail to init free_bitmap", KR(ret), K(count));
    }

    if (OB_SUCC(ret)) {
      free_bitmap_ = tmp_free_bitmap;
    } else if (nullptr != buf) {
      allocator_.free(buf);
      buf = nullptr;
    }
    tmp_free_bitmap = nullptr;
  }
  return ret;
}

int ObSSPhysicalBlockManager::reinit_free_bitmap(const int64_t new_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_count <= blk_cnt_info_.total_blk_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(new_count), K_(blk_cnt_info));
  } else {
    ObMemAttr attr(tenant_id_, "SSBlkBitMap");
    ObBitmap *tmp_free_bitmap = nullptr;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObBitmap), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), "size", sizeof(ObBitmap));
    } else if (FALSE_IT(tmp_free_bitmap = new (buf) ObBitmap(allocator_))) {
    } else if (OB_ISNULL(tmp_free_bitmap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("created free_bitmap should not be null", KR(ret));
    } else if (OB_FAIL(tmp_free_bitmap->init(new_count, true/*is_all_true*/))) {
      LOG_WARN("fail to init free_bitmap", KR(ret), K(new_count));
    } else if (nullptr != free_bitmap_) {
      if (OB_FAIL(tmp_free_bitmap->copy_from(*free_bitmap_, 0, blk_cnt_info_.total_blk_cnt_))) {
        LOG_WARN("fail to copy_from another bitmap", KR(ret), K(new_count), K_(blk_cnt_info));
      }
    }

    if (OB_SUCC(ret)) {
      ObBitmap *ori_bitmap = free_bitmap_;
      free_bitmap_ = tmp_free_bitmap;
      if (nullptr != ori_bitmap) {
        ori_bitmap->~ObBitmap();
        allocator_.free(ori_bitmap);
      }
    } else if (nullptr != buf) {
      allocator_.free(buf);
      buf = nullptr;
    }
    tmp_free_bitmap = nullptr;
  }
  return ret;
}

int ObSSPhysicalBlockManager::init_block_count(const int64_t total_file_size, const int32_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(total_file_size <= 0 || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(total_file_size), K(block_size));
  } else {
    const int64_t total_blk_cnt = total_file_size / block_size;
    blk_cnt_info_.init_block_cnt_info(total_blk_cnt, block_size);

    if (OB_UNLIKELY(!blk_cnt_info_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("blk_cnt_info is invalid", KR(ret), K_(blk_cnt_info));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::reinit_block_count(const int64_t new_file_size, const int32_t block_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_file_size <= total_file_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("currently only support increasing file_size", KR(ret), K_(tenant_id), K(new_file_size), 
      K(block_size), K_(total_file_size));
  } else if (OB_FAIL(init_block_count(new_file_size, block_size))) {
    LOG_WARN("fail to init block count", KR(ret), K(new_file_size), K(block_size));
  }
  return ret;
}

ObSSPhysicalBlock* ObSSPhysicalBlockManager::get_phy_block_by_idx_nolock(
    const int64_t phy_blk_idx)
{
  ObSSPhysicalBlock *phy_block = nullptr;
  if (IS_INIT && OB_LIKELY(is_valid_block_idx(phy_blk_idx))) {
    phy_block = &(phy_block_arr_.at(phy_blk_idx));
  }
  return phy_block;
}

int ObSSPhysicalBlockManager::divide_cache_data_block_range(
    const ObLSID &ls_id,
    const int64_t split_count,
    ObIArray<ObSSPhyBlockIdxRange> &block_ranges)
{
  // physical block is not organized by ls_id now, hence cannot filter by ls_id here now
  UNUSED(ls_id);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(split_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(split_count));
  } else {
    const int64_t data_blk_used_cnt = get_data_block_used_cnt();
    const int64_t block_cnt_per_range = (data_blk_used_cnt + split_count - 1) / split_count;
    int64_t cur_split_count = 0;
    int64_t block_cnt = 0;
    int64_t start_blk_idx = -1;
    SpinRLockGuard guard_for_resize(resize_lock_);
    for (int64_t i = SS_CACHE_SUPER_BLOCK_CNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_)
        && (cur_split_count < split_count); ++i) {
      ObSSPhysicalBlock *phy_block = get_phy_block_by_idx_nolock(i);
      if (OB_ISNULL(phy_block)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else if (phy_block->is_cache_data_block()) {
        ++block_cnt;
        if (-1 == start_blk_idx) {
          // left-open and right-closed intervals, thus i plus 1 here
          start_blk_idx = i - 1;
        }
      }

      // if reach block_cnt_per_range or iterate end, then push back block range
      if (OB_FAIL(ret)) {
      } else if (((block_cnt == block_cnt_per_range) || (i == (blk_cnt_info_.total_blk_cnt_ - 1))) &&
                (-1 != start_blk_idx)) {
        ObSSPhyBlockIdxRange block_range(start_blk_idx, i);
        if (OB_FAIL(block_ranges.push_back(block_range))) {
          LOG_WARN("fail to push back", KR(ret), K(block_range));
        } else {
          block_cnt = 0;
          start_blk_idx = -1;
          ++cur_split_count;
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_block_valid_length(
    const uint64_t data_dest, 
    const uint64_t reuse_version,
    const int32_t delta_len,
    int64_t &phy_blk_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(data_dest < 1 || delta_len == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_dest), K(delta_len), K(reuse_version));
  } else {
    ObSSPhysicalBlock *phy_block = nullptr;
    phy_blk_idx = data_dest / block_size_;
    SpinRLockGuard resize_guard(resize_lock_);
    if (OB_UNLIKELY(!is_valid_block_idx(phy_blk_idx))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("invalid phy_block index", KR(ret), K(phy_blk_idx), K_(blk_cnt_info));
    } else if (FALSE_IT(phy_block = get_phy_block_by_idx_nolock(phy_blk_idx))) {
    } else if (OB_NOT_NULL(phy_block)) {
      const uint64_t cur_reuse_version = phy_block->get_reuse_version();
      if (OB_UNLIKELY(reuse_version != cur_reuse_version)) {
        ret = OB_S2_REUSE_VERSION_MISMATCH;
        LOG_TRACE("phy_block maybe reused", KR(ret), K(reuse_version), K(cur_reuse_version), 
          K(phy_blk_idx), KPC(phy_block));
      } else if (delta_len < 0 && OB_FAIL(phy_block->dec_valid_len(phy_blk_idx, delta_len))) {
        LOG_WARN("fail to dec valid_len", KR(ret), K(phy_blk_idx), K(delta_len), KPC(phy_block));
      } else if (delta_len > 0 && OB_FAIL(phy_block->inc_valid_len(phy_blk_idx, delta_len))) {
        LOG_WARN("fail to inc valid_len", KR(ret), K(phy_blk_idx), K(delta_len), KPC(phy_block));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(phy_blk_idx));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_block_handle(
    const uint64_t data_dest, 
    const uint64_t reuse_version, 
    ObSSPhysicalBlockHandle &phy_blk_handle)
{
  uint64_t phy_blk_reuse_version = 0;
  return get_block_handle(data_dest, reuse_version, phy_blk_handle, phy_blk_reuse_version);
}

int ObSSPhysicalBlockManager::get_block_handle(
    const uint64_t data_dest, 
    const uint64_t exp_reuse_version, 
    ObSSPhysicalBlockHandle &phy_blk_handle,
    uint64_t &real_reuse_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(data_dest < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_dest), K(exp_reuse_version));
  } else {
    ObSSPhysicalBlock *phy_block = nullptr;
    const int64_t phy_blk_idx = data_dest / block_size_;
    SpinRLockGuard resize_guard(resize_lock_);
    if (OB_UNLIKELY(!is_valid_block_idx(phy_blk_idx))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("invalid phy_block index", KR(ret), K(phy_blk_idx), K(data_dest), K_(block_size), K_(blk_cnt_info));
    } else if (FALSE_IT(phy_block = get_phy_block_by_idx_nolock(phy_blk_idx))) {
    } else if (OB_NOT_NULL(phy_block)) {
      real_reuse_version = phy_block->get_reuse_version();
      if (OB_UNLIKELY(exp_reuse_version != real_reuse_version)) {
        ret = OB_S2_REUSE_VERSION_MISMATCH;
        LOG_TRACE("phy_block reuse_version mismatch", KR(ret), K(data_dest), K(exp_reuse_version), K(phy_blk_idx),
          K(real_reuse_version), KPC(phy_block));
      } else {
        phy_blk_handle.set_ptr(phy_block);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(phy_blk_idx), K(data_dest), K_(block_size));
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_block_handle(
    const int64_t phy_blk_idx, 
    ObSSPhysicalBlockHandle &phy_blk_handle)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard resize_guard(resize_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_block_idx(phy_blk_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_idx));
  } else {
    ObSSPhysicalBlock *phy_block = get_phy_block_by_idx_nolock(phy_blk_idx);
    if (OB_ISNULL(phy_block)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(phy_blk_idx));
    } else {
      phy_blk_handle.set_ptr(phy_block);
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::scan_blocks_to_ckpt(
    ObIArray<ObSSPhyBlockReuseInfo> &reuse_info_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    ObSSPhysicalBlock *phy_block = nullptr;
    SpinRLockGuard guard_for_resize(resize_lock_);
    for (int64_t i = SS_CACHE_SUPER_BLOCK_CNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); ++i) {
      phy_block = get_phy_block_by_idx_nolock(i);
      if (OB_ISNULL(phy_block)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else {
        ObSSPhyBlockReuseInfo reuse_info;
        phy_block->get_reuse_info(reuse_info);
        reuse_info.blk_idx_ = i;
        if (OB_UNLIKELY(!reuse_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_blk's reuse_info is invalid", KR(ret), K(i), K(reuse_info), KPC(phy_block));
        } else if (OB_FAIL(reuse_info_arr.push_back(reuse_info))) {
          LOG_WARN("fail to push back reuse_info", KR(ret), K(i), K(reuse_info), KPC(phy_block));
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::get_block_reuse_version(ObIArray<uint64_t> &reuse_version_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    ObSSPhysicalBlock *phy_block = nullptr;
    SpinRLockGuard guard_for_resize(resize_lock_);
    for (int64_t i = SS_CACHE_SUPER_BLOCK_CNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); ++i) {
      phy_block = get_phy_block_by_idx_nolock(i);
      if (OB_ISNULL(phy_block)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else if (OB_FAIL(reuse_version_arr.push_back(phy_block->get_reuse_version()))) {
        LOG_WARN("fail to push back", KR(ret), K(i), KPC(phy_block));
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::update_block_gc_reuse_version(const ObIArray<uint64_t> &version_arr)
{
  int ret = OB_SUCCESS;
  const int64_t version_cnt = version_arr.count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_LIKELY(version_cnt > 0)) {
    ObSSPhysicalBlock *phy_block = nullptr;
    SpinRLockGuard guard_for_resize(resize_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && (i < version_cnt); ++i) {
      const int64_t blk_idx = SS_CACHE_SUPER_BLOCK_CNT + i;
      phy_block = get_phy_block_by_idx_nolock(blk_idx);
      if (OB_NOT_NULL(phy_block)) {
        phy_block->update_gc_reuse_version(version_arr.at(i));
      }
    }
  }
  return ret;
}

// Only used for reading phy_block's checkpoint
int ObSSPhysicalBlockManager::update_block_state()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    // NOTICE: one thread, no need to add lock
    ObSSPhysicalBlock *phy_block = nullptr;
    
    for (int64_t i = SS_CACHE_SUPER_BLOCK_CNT; OB_SUCC(ret) && (i < blk_cnt_info_.total_blk_cnt_); ++i) {
      phy_block = get_phy_block_by_idx_nolock(i);
      if (OB_ISNULL(phy_block)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_block should not be null", KR(ret), K(i), K_(blk_cnt_info));
      } else {
        ObSSPhyBlockType block_type = ObSSPhyBlockType::SS_INVALID_BLK_TYPE;
        if (phy_block->get_valid_len() > 0) {
          block_type = ObSSPhyBlockType::SS_REORGAN_BLK;
        } else if (super_block_.is_existed_in_micro_ckpt_entry_list(i)) {
          block_type = ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK;
        } else if (super_block_.is_existed_in_blk_ckpt_entry_list(i)) {
          block_type = ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK;
        }

        if (ObSSPhyBlockType::SS_INVALID_BLK_TYPE != block_type) {
          // Cuz we didn't record ckpt_blk's valid_len, so make it greated than 0 to prevent it from being reused.
          if (is_ckpt_block_type(block_type)) {
            phy_block->set_valid_len(1);
          }
          if (OB_FAIL(blk_cnt_info_.increase_blk_used_cnt(block_type))) {
            LOG_WARN("fail to increase blk_used_cnt", KR(ret), K(i), K(block_type));
          } else {
            free_bitmap_->set(i, false);
            phy_block->set_used_and_sealed(block_type);
          }
        }
      }
    }
  }
  return ret;
}

int ObSSPhysicalBlockManager::read_phy_block_checkpoint(ObSSLinkedPhyBlockItemReader &item_reader)
{
  int ret = OB_SUCCESS;
  int64_t replay_blk_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!item_reader.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(item_reader.get_next_item(item_buf, item_buf_len))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next phy_block persist_info item", KR(ret), K_(tenant_id));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ObSSPhysicalBlock *phy_blk = nullptr;
        ObSSPhyBlockPersistInfo persist_info;
        int64_t pos = 0;
        if (OB_FAIL(persist_info.deserialize(item_buf, item_buf_len, pos))) {
          LOG_WARN("fail to deserialize", KR(ret), K(item_buf_len), KP(item_buf), K(replay_blk_cnt));
        } else if (OB_UNLIKELY(!persist_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block persist info should be valid", KR(ret), K(persist_info), K(replay_blk_cnt));
        } else if (FALSE_IT(phy_blk = get_phy_block_by_idx_nolock(persist_info.blk_idx_))) {
        } else if (OB_ISNULL(phy_blk)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block should not be null", KR(ret), K(persist_info), K_(blk_cnt_info), K(replay_blk_cnt));
        } else {
          // When restart, gc_reuse_version is equal to reuse_version
          phy_blk->set_reuse_info(persist_info.reuse_version_, persist_info.reuse_version_);
          ++replay_blk_cnt;
        }
      }
    }
  }
  FLOG_INFO("finish read phy_blk ckpt", KR(ret), K(replay_blk_cnt), K_(blk_cnt_info));
  return ret;
}

void ObSSPhysicalBlockManager::set_phy_blk_stat(const int64_t cache_file_size)
{
  cache_stat_.phy_blk_stat().set_cache_file_size(cache_file_size);
  cache_stat_.phy_blk_stat().set_block_size(block_size_);
  cache_stat_.phy_blk_stat().set_super_block_cnt(blk_cnt_info_.super_blk_cnt_);
  cache_stat_.phy_blk_stat().set_total_block_cnt(blk_cnt_info_.total_blk_cnt_);
  cache_stat_.phy_blk_stat().set_shared_block_cnt(blk_cnt_info_.shared_blk_cnt_);
  cache_stat_.phy_blk_stat().set_phy_ckpt_block_cnt(blk_cnt_info_.phy_ckpt_blk_cnt_);

  cache_stat_.phy_blk_stat().set_data_block_cnt(blk_cnt_info_.data_blk_.hold_cnt_);
  cache_stat_.phy_blk_stat().set_meta_block_cnt(blk_cnt_info_.meta_blk_.hold_cnt_);
  cache_stat_.phy_blk_stat().set_shared_block_used_cnt(blk_cnt_info_.shared_blk_used_cnt_);
}

int64_t ObSSPhysicalBlockManager::get_cache_limit_size() const
{
  // resize_lock is to protect 'shared_blk_cnt_'
  // info_lock is to protect 'data_blk_.hold_cnt_' and 'shared_blk_used_cnt_'
  SpinRLockGuard resize_guard(resize_lock_);
  SpinRLockGuard info_guard(info_lock_);
  const int64_t cache_limit_blk_cnt = blk_cnt_info_.cache_limit_blk_cnt();
  return cache_limit_blk_cnt * block_size_;
}

void ObSSPhysicalBlockManager::get_cache_data_block_info(int64_t &cache_limit_blk_cnt, int64_t &cache_data_blk_usage_pct) const
{
  SpinRLockGuard resize_guard(resize_lock_);
  SpinRLockGuard info_guard(info_lock_);
  cache_limit_blk_cnt = blk_cnt_info_.cache_limit_blk_cnt();
  cache_data_blk_usage_pct = blk_cnt_info_.data_blk_.used_cnt_ * 100 / cache_limit_blk_cnt;
}

int64_t ObSSPhysicalBlockManager::get_data_block_used_cnt() const
{
  // info_lock is to protect 'data_blk_.used_cnt_'
  SpinRLockGuard info_guard(info_lock_);
  return blk_cnt_info_.data_blk_.used_cnt_;
}

int32_t ObSSPhysicalBlockManager::get_block_size() const
{
  return block_size_; // Not support update it, thus not need lock
}

bool ObSSPhysicalBlockManager::exist_free_block(const ObSSPhyBlockType blk_type) const
{
  SpinRLockGuard resize_guard(resize_lock_);
  SpinRLockGuard info_guard(info_lock_);
  return blk_cnt_info_.has_free_blk(blk_type);
}

int ObSSPhysicalBlockManager::get_phy_block_info(
    const int64_t phy_blk_idx, 
    ObSSPhysicalBlock& block_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    ObSSPhysicalBlockHandle phy_blk_handle;
    if (OB_FAIL(get_block_handle(phy_blk_idx, phy_blk_handle))) {
      LOG_WARN("fail to get block_handle", KR(ret), K(phy_blk_idx));
    } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block handle should be valid", KR(ret), K(phy_blk_idx), K(phy_blk_handle));
    } else {
      block_info = *(phy_blk_handle.get_ptr());
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
