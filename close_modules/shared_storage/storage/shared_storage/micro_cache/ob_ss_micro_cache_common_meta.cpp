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

#include "ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_ss_micro_cache.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

bool is_ckpt_block_type(const ObSSPhyBlockType type)
{
  return (ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK == type) || (ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK == type);
}

ObSSPhyBlockType get_mapping_block_type(const ObSSPhyBlockType type)
{
  return ((ObSSPhyBlockType::SS_REORGAN_BLK == type) ? ObSSPhyBlockType::SS_CACHE_DATA_BLK : type);
}

/*-----------------------------------------ObSSTabletCacheInfo-----------------------------------------*/
void ObSSTabletCacheInfo::add_micro_size(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const int64_t delta_size)
{
  if (is_in_l1 && !is_in_ghost) {
    t1_size_ += delta_size;
  } else if (!is_in_l1 && !is_in_ghost) {
    t2_size_ += delta_size;
  }
}

ObSSTabletCacheInfo& ObSSTabletCacheInfo::operator=(const ObSSTabletCacheInfo &other)
{
  if (this != &other) {
    tablet_id_ = other.tablet_id_;
    t1_size_ = other.t1_size_;
    t2_size_ = other.t2_size_;
  }
  return *this;
}

/*-----------------------------------------ObSSLSCacheInfo-----------------------------------------*/
ObSSLSCacheInfo& ObSSLSCacheInfo::operator=(const ObSSLSCacheInfo &other)
{
  if (this != &other) {
    ls_id_ = other.ls_id_;
    tablet_cnt_ = other.tablet_cnt_;
    t1_size_ = other.t1_size_;
    t2_size_ = other.t2_size_;
  }
  return *this;
}
OB_SERIALIZE_MEMBER(ObSSLSCacheInfo, ls_id_, tablet_cnt_, t1_size_, t2_size_);

/*-----------------------------------------ObSSMicroCacheSuperBlock-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSMicroCacheSuperBlock, magic_, cache_file_size_, modify_time_us_, micro_ckpt_time_us_,
                    micro_ckpt_entry_list_, blk_ckpt_entry_list_);

ObSSMicroCacheSuperBlock::ObSSMicroCacheSuperBlock(const uint64_t tenant_id, const int64_t cache_file_size)
  : magic_(SS_SUPER_BLK_MAGIC), tenant_id_(tenant_id), cache_file_size_(cache_file_size), 
    modify_time_us_(ObTimeUtility::current_time_us()), micro_ckpt_time_us_(0), micro_ckpt_entry_list_(), 
    blk_ckpt_entry_list_(), ls_info_list_(), tablet_info_list_()
{
  set_list_attr(tenant_id_);
}

void ObSSMicroCacheSuperBlock::reset()
{
  magic_ = SS_SUPER_BLK_MAGIC;
  tenant_id_ = OB_INVALID_TENANT_ID;
  cache_file_size_ = 0;
  modify_time_us_ = 0;
  micro_ckpt_time_us_ = 0;
  micro_ckpt_entry_list_.reset();
  blk_ckpt_entry_list_.reset();
  ls_info_list_.reset();
  tablet_info_list_.reset();
}

bool ObSSMicroCacheSuperBlock::is_valid() const
{
  return (is_valid_tenant_id(tenant_id_) && (cache_file_size_ > 0) && 
         (modify_time_us_ > 0) && (SS_SUPER_BLK_MAGIC == magic_));
}

int ObSSMicroCacheSuperBlock::assign(const ObSSMicroCacheSuperBlock &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    set_list_attr(other.tenant_id_);
    if (OB_FAIL(micro_ckpt_entry_list_.assign(other.micro_ckpt_entry_list_))) {
      LOG_WARN("fail to assign", KR(ret), K(*this), K(other));
    } else if (OB_FAIL(blk_ckpt_entry_list_.assign(other.blk_ckpt_entry_list_))) {
      LOG_WARN("fail to assign", KR(ret), K(*this), K(other));
    } else if (OB_FAIL(ls_info_list_.assign(other.ls_info_list_))) {
      LOG_WARN("fail to assign", KR(ret), K(*this), K(other));
    } else if (OB_FAIL(tablet_info_list_.assign(other.tablet_info_list_))) {
      LOG_WARN("fail to assign", KR(ret), K(*this), K(other));
    } else {
      magic_ = other.magic_;
      tenant_id_ = other.tenant_id_;
      cache_file_size_ = other.cache_file_size_;
      modify_time_us_ = other.modify_time_us_;
      micro_ckpt_time_us_ = other.micro_ckpt_time_us_;
    }
  }
  return ret;
}

// When execute micro_ckpt, we should remain micro_ckpt's relative info;
// Otherwise, we should keep blk_ckpt's retive info
int ObSSMicroCacheSuperBlock::assign_by_ckpt(
    const bool is_micro_ckpt,
    const ObSSMicroCacheSuperBlock &other)
{
  int ret = OB_SUCCESS;
  set_list_attr(other.tenant_id_);
  if (is_micro_ckpt) {
    if (OB_FAIL(blk_ckpt_entry_list_.assign(other.blk_ckpt_entry_list_))) {
      LOG_WARN("fail to assign", KR(ret), KPC(this), K(other));
    }
  } else {
    if (OB_FAIL(micro_ckpt_entry_list_.assign(other.micro_ckpt_entry_list_))) {
      LOG_WARN("fail to assign", KR(ret), KPC(this), K(other));
    } else if (OB_FAIL(ls_info_list_.assign(other.ls_info_list_))) {
      LOG_WARN("fail to assign", KR(ret), KPC(this), K(other));
    } else if (OB_FAIL(tablet_info_list_.assign(other.tablet_info_list_))) {
      LOG_WARN("fail to assign", KR(ret), KPC(this), K(other));
    } else {
      micro_ckpt_time_us_ = other.micro_ckpt_time_us_;
    }
  }

  if (OB_SUCC(ret)) {
    tenant_id_ = other.tenant_id_;
    cache_file_size_ = other.cache_file_size_;
    update_modify_time();
  }
  return ret;
}

void ObSSMicroCacheSuperBlock::set_list_attr(const uint64_t tenant_id)
{
  if (is_valid_tenant_id(tenant_id)) {
    micro_ckpt_entry_list_.set_attr(ObMemAttr(tenant_id, "SSMicroCkptList"));
    blk_ckpt_entry_list_.set_attr(ObMemAttr(tenant_id, "SSBlkCkptList"));
    ls_info_list_.set_attr(ObMemAttr(tenant_id, "SSLsInfoList"));
    tablet_info_list_.set_attr(ObMemAttr(tenant_id, "SSTabInfoList"));
  }
}

int ObSSMicroCacheSuperBlock::update_cache_file_size(const int64_t new_cache_file_size)
{
  int ret = OB_SUCCESS;
  // Currently, only support increasing cache file size
  if (OB_UNLIKELY(new_cache_file_size <= cache_file_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(cache_file_size), K(new_cache_file_size));
  } else {
    cache_file_size_ = new_cache_file_size;
    modify_time_us_ = ObTimeUtility::current_time();
  }
  return ret;
}

bool ObSSMicroCacheSuperBlock::exist_checkpoint() const 
{ 
  return (micro_ckpt_entry_list_.count() > 0) || (blk_ckpt_entry_list_.count() > 0);
}

bool ObSSMicroCacheSuperBlock::is_valid_checkpoint() const 
{ 
  return (micro_ckpt_entry_list_.count() > 0) && (blk_ckpt_entry_list_.count() > 0);
}

bool ObSSMicroCacheSuperBlock::is_existed_in_blk_ckpt_entry_list(const int64_t blk_idx) const
{
  bool is_exist = false;
  if (is_valid_checkpoint() && blk_idx >= 0) {
    const int64_t entry_cnt = blk_ckpt_entry_list_.count();
    for (int64_t i = 0; (!is_exist) && (i < entry_cnt); ++i) {
      if (blk_idx == blk_ckpt_entry_list_.at(i)) {
        is_exist = true;
      }
    }
  }
  return is_exist;
}

bool ObSSMicroCacheSuperBlock::is_existed_in_micro_ckpt_entry_list(const int64_t blk_idx) const
{
  bool is_exist = false;
  if (is_valid_checkpoint() && blk_idx >= 0) {
    const int64_t entry_cnt = micro_ckpt_entry_list_.count();
    for (int64_t i = 0; (!is_exist) && (i < entry_cnt); ++i) {
      if (blk_idx == micro_ckpt_entry_list_.at(i)) {
        is_exist = true;
      }
    }
  }
  return is_exist;
}

void ObSSMicroCacheSuperBlock::clear_ckpt_entry_list()
{
  micro_ckpt_entry_list_.reuse();
  blk_ckpt_entry_list_.reuse();
  modify_time_us_ = ObTimeUtility::current_time_us();
}

void ObSSMicroCacheSuperBlock::update_modify_time()
{
  modify_time_us_ = ObTimeUtility::current_time_us();
}

void ObSSMicroCacheSuperBlock::update_micro_ckpt_time()
{
  micro_ckpt_time_us_ = ObTimeUtility::current_time_us();
}

int ObSSMicroCacheSuperBlock::add_ls_cache_info(const ObSSLSCacheInfo &ls_cache_info)
{
  int ret = OB_SUCCESS;
  int64_t ls_idx = -1;
  if (OB_UNLIKELY(!ls_cache_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_cache_info));
  } else if (OB_FAIL(exist_ls_cache_info(ls_cache_info.ls_id_, ls_idx))) {
    LOG_WARN("fail to check ls_info exist", KR(ret), K(ls_cache_info));
  } else if (OB_UNLIKELY(ls_idx != -1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already exist the same ls_id", KR(ret), K(ls_cache_info), K(ls_idx), K_(ls_info_list));
  } else if (OB_FAIL(ls_info_list_.push_back(ls_cache_info))) {
    LOG_WARN("fail to push back", KR(ret), K(ls_cache_info), K_(ls_info_list));
  }
  return ret;
}

int ObSSMicroCacheSuperBlock::get_ls_cache_info(
    const ObLSID &ls_id, 
    ObSSLSCacheInfo &ls_cache_info)
{
  int ret = OB_SUCCESS;
  int64_t ls_idx = -1;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(exist_ls_cache_info(ls_id, ls_idx))) {
    LOG_WARN("fail to check ls_info exist", KR(ret), K(ls_id));
  } else if (-1 == ls_idx) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("not exist this ls_id", KR(ret), K(ls_id));
  } else {
    ls_cache_info = ls_info_list_.at(ls_idx);
  }
  return ret;
}

int ObSSMicroCacheSuperBlock::exist_ls_cache_info(const ObLSID &ls_id, int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    idx = -1;
    const int64_t ls_info_cnt = ls_info_list_.count();
    for (int64_t i = 0; (-1 == idx) && (i < ls_info_cnt); ++i) {
      if (ls_id == ls_info_list_.at(i).ls_id_) {
        idx = i;
      }
    }
  }
  return ret;
}

int ObSSMicroCacheSuperBlock::get_tablet_cache_info(
    const ObTabletID &tablet_id, 
    ObSSTabletCacheInfo &tablet_cache_info)
{
  int ret = OB_SUCCESS;
  int64_t tablet_idx = -1;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_FAIL(exist_tablet_cache_info(tablet_id, tablet_idx))) {
    LOG_WARN("fail to check tablet_info exist", KR(ret), K(tablet_id));
  } else if (-1 == tablet_idx) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("not exist this tablet_id", KR(ret), K(tablet_id));
  } else {
    tablet_cache_info = tablet_info_list_.at(tablet_idx);
  }
  return ret;
}

void ObSSMicroCacheSuperBlock::get_ckpt_entry_list(
    const bool is_micro_ckpt,
    const ObSEArray<int64_t, DEFAULT_ITEM_CNT> *&entry_list) const
{
  if (is_micro_ckpt) {
    entry_list = &micro_ckpt_entry_list_;
  } else {
    entry_list = &blk_ckpt_entry_list_;
  }
}

int ObSSMicroCacheSuperBlock::exist_tablet_cache_info(const ObTabletID &tablet_id, int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    idx = -1;
    const int64_t tablet_info_cnt = tablet_info_list_.count();
    for (int64_t i = 0; (-1 == idx) && (i < tablet_info_cnt); ++i) {
      if (tablet_id == tablet_info_list_.at(i).tablet_id_) {
        idx = i;
      }
    }
  }
  return ret;
}

/*-----------------------------------------ObSSPhysicalBlock-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSPhysicalBlock, ref_cnt_, block_state_, alloc_time_us_);

ObSSPhysicalBlock::ObSSPhysicalBlock() 
  : lock_(), ref_cnt_(1), reuse_version_(1), valid_len_(0), is_free_(1), is_sealed_(0), gc_reuse_version_(1), 
    block_type_(static_cast<uint64_t>(ObSSPhyBlockType::SS_INVALID_BLK_TYPE)), reserved_(0), alloc_time_us_(0)
{}

void ObSSPhysicalBlock::reset()
{
  SpinWLockGuard guard(lock_);
  try_delete_without_lock();
}

void ObSSPhysicalBlock::reuse()
{
  SpinWLockGuard guard(lock_);
  inner_reuse();
}

void ObSSPhysicalBlock::inner_reuse()
{
  ref_cnt_ = 1;
  reuse_version_ = inner_get_next_reuse_version();
  valid_len_ = 0;
  is_free_ = 1;
  is_sealed_ = 0;
  block_type_ = static_cast<uint64_t>(ObSSPhyBlockType::SS_INVALID_BLK_TYPE);
  // not change gc_reuse_version_ here
  reserved_ = 0;
  alloc_time_us_ = 0;
}

uint32_t ObSSPhysicalBlock::get_ref_count() const
{
  SpinRLockGuard guard(lock_);
  return ref_cnt_;
}

bool ObSSPhysicalBlock::has_no_ref() const
{
  SpinRLockGuard guard(lock_);
  return 1 == ref_cnt_;
}

void ObSSPhysicalBlock::update_gc_reuse_version(const uint64_t gc_reuse_version)
{
  SpinWLockGuard guard(lock_);
  gc_reuse_version_ = gc_reuse_version;
}

void ObSSPhysicalBlock::get_reuse_info(ObSSPhyBlockReuseInfo &reuse_info) const
{
  SpinRLockGuard guard(lock_);
  reuse_info.reuse_version_ = reuse_version_;
  reuse_info.next_reuse_version_ = inner_get_next_reuse_version();
  reuse_info.gc_reuse_version_ = gc_reuse_version_;
}

void ObSSPhysicalBlock::set_reuse_info(const uint64_t reuse_version, const uint64_t gc_reuse_version)
{
  SpinWLockGuard guard(lock_);
  reuse_version_ = reuse_version;
  gc_reuse_version_ = gc_reuse_version;
}

uint64_t ObSSPhysicalBlock::get_next_reuse_version() const
{
  SpinRLockGuard guard(lock_);
  return inner_get_next_reuse_version();
}

uint64_t ObSSPhysicalBlock::inner_get_next_reuse_version() const
{
  uint64_t next_version = reuse_version_;
  if (SS_MAX_REUSE_VERSION == next_version) {
    next_version = 1;
  } else {
    ++next_version;
  }
  return next_version;
}

void ObSSPhysicalBlock::inc_ref_count()
{
  SpinWLockGuard guard(lock_);
  ++ref_cnt_;
}

void ObSSPhysicalBlock::dec_ref_count()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(ref_cnt_ <= 0)) {
    LOG_ERROR("phy_block's ref_cnt is invalid", K(*this));
  } else {
    try_delete_without_lock();
  }
}

bool ObSSPhysicalBlock::is_empty() const
{
  SpinRLockGuard guard(lock_);
  return (valid_len_ == 0);
}

bool ObSSPhysicalBlock::can_reuse() const
{
  SpinRLockGuard guard(lock_);
  return (!is_free_) && (valid_len_ == 0) && (is_sealed_ || 
         (ObTimeUtility::current_time() - alloc_time_us_ >= PHY_BLK_MAX_REUSE_TIME)); 
}

bool ObSSPhysicalBlock::can_reorganize(const int64_t block_size) const
{
  bool b_ret = false;
  SpinRLockGuard guard(lock_);
  if (block_size > 0) {
    const double block_usage = static_cast<double>(valid_len_) / block_size;
    b_ret = !is_free_ && (valid_len_ > 0) && is_sealed_ && (block_usage < SS_REORGAN_BLK_USAGE_RATIO) &&
            (ObSSPhyBlockType::SS_CACHE_DATA_BLK == static_cast<ObSSPhyBlockType>(block_type_));
  }
  return b_ret;
}

bool ObSSPhysicalBlock::is_sealed() const
{
  SpinRLockGuard guard(lock_);
  return is_sealed_;
}

bool ObSSPhysicalBlock::is_free() const
{
  SpinRLockGuard guard(lock_);
  return is_free_;
}

int ObSSPhysicalBlock::try_free(bool &succ_free)
{
  int ret = OB_SUCCESS;
  succ_free = false;
  SpinWLockGuard guard(lock_);
  // Notice: phy_block's ref_cnt is inited as 1
  if (OB_LIKELY((0 == valid_len_) && 1 == ref_cnt_)) {
    inner_reuse();
    succ_free = true;
  } else if (OB_UNLIKELY(ref_cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("phy_blk's ref_cnt cannot be less than 0", KR(ret), KPC(this));
  }
  return ret;
}

void ObSSPhysicalBlock::try_delete_without_lock()
{
  --ref_cnt_;
  if (0 == ref_cnt_) {
    block_state_ = 0;
  }
}

int ObSSPhysicalBlock::set_first_used(const ObSSPhyBlockType block_type)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(
          !is_free_ || (valid_len_ > 0) || is_sealed_ || (ObSSPhyBlockType::SS_INVALID_BLK_TYPE == block_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(*this), K(block_type));
  } else {
    is_free_ = 0;
    block_type_ = static_cast<uint64_t>(get_mapping_block_type(block_type));
    alloc_time_us_ = ObTimeUtility::current_time();
  }
  return ret;
}

void ObSSPhysicalBlock::set_used_and_sealed(const ObSSPhyBlockType block_type)
{
  SpinWLockGuard guard(lock_);
  is_free_ = 0;
  is_sealed_ = 1;
  block_type_ = static_cast<uint64_t>(get_mapping_block_type(block_type));
}

void ObSSPhysicalBlock::set_sealed(const uint64_t valid_len)
{
  SpinWLockGuard guard(lock_);
  is_sealed_ = 1;
  valid_len_ = valid_len;
}

void ObSSPhysicalBlock::set_reusable(const ObSSPhyBlockType block_type)
{
  SpinWLockGuard guard(lock_);
  is_free_ = 0;
  valid_len_ = 0;
  is_sealed_ = 1;
  block_type_ = static_cast<uint64_t>(get_mapping_block_type(block_type));
}

bool ObSSPhysicalBlock::is_cache_data_block() const
{
  SpinRLockGuard guard(lock_);
  return (valid_len_ > 0) && !is_free_ &&
         (ObSSPhyBlockType::SS_CACHE_DATA_BLK == static_cast<ObSSPhyBlockType>(block_type_));
}

uint64_t ObSSPhysicalBlock::get_valid_len() const
{
  SpinRLockGuard guard(lock_);
  return valid_len_;
}

uint64_t ObSSPhysicalBlock::get_reuse_version() const
{
  SpinRLockGuard guard(lock_);
  return reuse_version_;
}

uint64_t ObSSPhysicalBlock::get_gc_reuse_version() const
{
  SpinRLockGuard guard(lock_);
  return gc_reuse_version_;
}

void ObSSPhysicalBlock::set_valid_len(const uint64_t valid_len)
{
  SpinWLockGuard guard(lock_);
  valid_len_ = valid_len;
}

int ObSSPhysicalBlock::inc_valid_len(const int64_t phy_blk_idx, const int64_t delta_len)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const int32_t block_size = SSPhyBlockMgr.get_block_size();
  if (OB_UNLIKELY((delta_len <= 0) || (block_size <= 0) || (valid_len_ + delta_len > block_size))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(block_size), K(phy_blk_idx), K(delta_len), KPC(this));
  } else {
    valid_len_ += delta_len;
  }
  return ret;
}

int ObSSPhysicalBlock::dec_valid_len(const int64_t phy_blk_idx, const int64_t delta_len)
{
  int ret = OB_SUCCESS;
  bool is_empty = false;
  bool is_sparse = false;
  {
    SpinWLockGuard guard(lock_);
    const int32_t block_size = SSPhyBlockMgr.get_block_size();
    if (OB_UNLIKELY((delta_len >= 0) || (block_size <= 0) || (valid_len_ + delta_len < 0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid argument", KR(ret), K(block_size), K(phy_blk_idx), K(delta_len), KPC(this));
    } else {
      const double prev_usage = static_cast<double>(valid_len_) / block_size;
      const double cur_usage = static_cast<double>(valid_len_ + delta_len) / block_size;
      valid_len_ += delta_len;
      if (0 == valid_len_) {
        is_sealed_ = true;
        is_empty = true;
      } else if ((prev_usage >= SS_REORGAN_BLK_USAGE_RATIO && cur_usage < SS_REORGAN_BLK_USAGE_RATIO) && is_sealed_ &&
                 (ObSSPhyBlockType::SS_CACHE_DATA_BLK == static_cast<ObSSPhyBlockType>(block_type_))) {
        is_sparse = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_empty && OB_FAIL(SSPhyBlockMgr.add_reusable_block(phy_blk_idx))) {
      LOG_WARN("fail to add phy_block into reusable_list", KR(ret), K(phy_blk_idx));
    } else if (is_sparse && OB_FAIL(SSPhyBlockMgr.add_sparse_block(ObSSPhyBlockIdx(phy_blk_idx)))) {
      LOG_WARN("fail to add sparse_block", KR(ret), K(phy_blk_idx));
    }
  }
  return ret;
}

ObSSPhyBlockType ObSSPhysicalBlock::get_block_type() const
{
  SpinRLockGuard guard(lock_);
  return static_cast<ObSSPhyBlockType>(block_type_);
}

ObSSPhysicalBlock &ObSSPhysicalBlock::operator=(const ObSSPhysicalBlock &other)
{
  SpinRLockGuard r_guard(other.lock_);
  SpinWLockGuard w_guard(lock_);
  ref_cnt_ = other.ref_cnt_;
  reuse_version_ = other.reuse_version_;
  valid_len_ = other.valid_len_;
  is_free_ = other.is_free_;
  is_sealed_ = other.is_sealed_;
  gc_reuse_version_ = other.gc_reuse_version_;
  block_type_ = other.block_type_;
  alloc_time_us_ = other.alloc_time_us_;
  return *this;
}

/*-----------------------------------------ObSSPhyBlockCommonHeader-----------------------------------------*/
ObSSPhyBlockCommonHeader::ObSSPhyBlockCommonHeader()
{
  reset();
}

bool ObSSPhyBlockCommonHeader::is_valid() const
{
  return (header_size_ > 0 && version_ == SS_PHY_BLK_COMMON_HEADER_VERSION && 
          magic_ == SS_PHY_BLK_COMMON_HEADER_MAGIC && 
          blk_type_ != static_cast<int32_t>(ObSSPhyBlockType::SS_INVALID_BLK_TYPE));
}

void ObSSPhyBlockCommonHeader::reset()
{
  header_size_ = (int32_t)(get_serialize_size());
  version_ = SS_PHY_BLK_COMMON_HEADER_VERSION;
  magic_ = SS_PHY_BLK_COMMON_HEADER_MAGIC;
  payload_size_ = 0;
  payload_checksum_ = 0;
  blk_type_ = static_cast<int32_t>(ObSSPhyBlockType::SS_INVALID_BLK_TYPE);
  reserved_ = 0;
}

void ObSSPhyBlockCommonHeader::calc_payload_checksum(const char *buf, const int32_t buf_size)
{
  if (OB_NOT_NULL(buf) && buf_size > 0) {
    payload_checksum_ = static_cast<int32_t>(ob_crc64(buf, buf_size));
  }
}

int ObSSPhyBlockCommonHeader::check_payload_checksum(const char *buf, const int32_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(buf_size), KP(buf));
  } else {
    const int32_t cur_checksum = static_cast<int32_t>(ob_crc64(buf, buf_size));
    if (cur_checksum != payload_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("phy_block header checksum error!", KR(ret), K(cur_checksum), K(buf_size), KP(buf),
        K(*this));
    }
  }
  return ret;
}

int ObSSPhyBlockCommonHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("data buffer is not enough", KR(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("common header is invalid", KR(ret), K(*this));
  } else {
    ObSSPhyBlockCommonHeader *common_header = reinterpret_cast<ObSSPhyBlockCommonHeader*>(buf + pos);
    common_header->header_size_ = header_size_;
    common_header->version_ = version_;
    common_header->magic_ = magic_;
    common_header->payload_size_ = payload_size_;
    common_header->payload_checksum_ = payload_checksum_;
    common_header->attr_ = attr_;
    pos += common_header->get_serialize_size();
  }
  return ret;
}

int ObSSPhyBlockCommonHeader::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObSSPhyBlockCommonHeader *header_ptr = reinterpret_cast<const ObSSPhyBlockCommonHeader *>(buf + pos);
    header_size_ = header_ptr->header_size_;
    version_ = header_ptr->version_;
    magic_ = header_ptr->magic_;
    payload_size_ = header_ptr->payload_size_;
    payload_checksum_ = header_ptr->payload_checksum_;
    attr_ = header_ptr->attr_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("deserialized ss_phy_blk common header is invalid", KR(ret), K(*this));
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

/*-----------------------------------------ObSSNormalPhyBlockHeader-----------------------------------------*/
ObSSNormalPhyBlockHeader::ObSSNormalPhyBlockHeader()
  : magic_(SS_NORMAL_PHY_BLK_HEADER_MAGIC), payload_checksum_(0), payload_offset_(-1),
    payload_size_(0), micro_count_(0), micro_index_offset_(-1), micro_index_size_(0)
{}

void ObSSNormalPhyBlockHeader::reset()
{
  magic_ = SS_NORMAL_PHY_BLK_HEADER_MAGIC;
  payload_checksum_ = 0;
  payload_offset_ = -1;
  payload_size_ = 0;
  micro_count_ = 0;
  micro_index_offset_ = -1;
  micro_index_size_ = 0;
}

bool ObSSNormalPhyBlockHeader::is_valid() const
{
  return (payload_size_ > 0) && (magic_ == SS_NORMAL_PHY_BLK_HEADER_MAGIC);
}

int64_t ObSSNormalPhyBlockHeader::get_fixed_serialize_size()
{
  // If we want to add fields in the future, we should consider 'compat' here.
  // Also we need to notice that, this size must not be less than the variable-encoded
  // size(a int32_t val may occupy 5 bytes)
  return sizeof(ObSSNormalPhyBlockHeader) + SS_SERIALIZE_EXTRA_BUF_LEN;
}

OB_SERIALIZE_MEMBER(ObSSNormalPhyBlockHeader, magic_, payload_checksum_, payload_offset_, payload_size_,
  micro_count_, micro_index_offset_, micro_index_size_);

/*-----------------------------------------ObSSPhyBlockPersistInfo-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSPhyBlockPersistInfo, blk_idx_, reuse_version_);

/*-----------------------------------------ObSSMicroBlockId-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSMicroBlockId, macro_id_, offset_, size_);

/*-----------------------------------------ObSSMicroBlockCacheKey-----------------------------------------*/
ObSSMicroBlockCacheKey::ObSSMicroBlockCacheKey()
  : mode_(ObSSMicroBlockCacheKeyMode::MAX_MODE), micro_id_(), micro_crc_(0)
{}

ObSSMicroBlockCacheKey::ObSSMicroBlockCacheKey(
  const ObSSMicroBlockId &micro_block_id)
  : mode_(ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE), micro_id_(micro_block_id), micro_crc_(0)
{}

ObSSMicroBlockCacheKey::ObSSMicroBlockCacheKey(
  const blocksstable::ObLogicMicroBlockId &logic_micro_id,
  const int64_t micro_crc)
  : mode_(ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE), logic_micro_id_(logic_micro_id), micro_crc_(micro_crc)
{}

ObSSMicroBlockCacheKey::ObSSMicroBlockCacheKey(const ObSSMicroBlockCacheKey &other)
  : mode_(other.mode_), micro_id_(other.micro_id_), micro_crc_(other.micro_crc_)
{}

void ObSSMicroBlockCacheKey::reset()
{
  if (is_logic_key()) {
    logic_micro_id_.reset();
  } else {
    micro_id_.reset();
  }
  micro_crc_ = 0;
  mode_ = ObSSMicroBlockCacheKeyMode::MAX_MODE;
}

bool ObSSMicroBlockCacheKey::is_valid() const
{
  bool b_ret = (mode_ != ObSSMicroBlockCacheKeyMode::MAX_MODE);
  if (b_ret) {
    if (is_logic_key()) {
      b_ret = logic_micro_id_.is_valid();
    } else {
      b_ret = micro_id_.is_valid();
    }
  }
  return b_ret;
}

uint64_t ObSSMicroBlockCacheKey::hash() const
{
  uint64_t hash_val = static_cast<uint64_t>(mode_);
  if (is_logic_key()) {
    hash_val = murmurhash(&logic_micro_id_, sizeof(logic_micro_id_), hash_val);
    hash_val = murmurhash(&micro_crc_, sizeof(micro_crc_), hash_val);
  } else {
    hash_val = murmurhash(&micro_id_, sizeof(micro_id_), hash_val);
  }
  return hash_val;
}

ObSSMicroBlockCacheKey& ObSSMicroBlockCacheKey::operator=(const ObSSMicroBlockCacheKey &other)
{
  if (this != &other) {
    mode_ = other.mode_;
    if (is_logic_key()) {
      logic_micro_id_ = other.logic_micro_id_;
      micro_crc_ = other.micro_crc_;
    } else {
      micro_id_ = other.micro_id_;
    }
  }
  return *this;
}

bool ObSSMicroBlockCacheKey::operator==(const ObSSMicroBlockCacheKey &other) const
{
  bool b_ret = (mode_ == other.mode_);
  if (is_logic_key()) {
    b_ret = ((logic_micro_id_ == other.logic_micro_id_) && (micro_crc_ == other.micro_crc_));
  } else {
    b_ret = (micro_id_ == other.micro_id_);
  }
  return b_ret;
}

int ObSSMicroBlockCacheKey::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize_", KR(ret));
      }
    }
    int64_t serial_size = pos - pos_bak;
    int64_t tmp_pos = 0;
    if (OB_SUCC(ret)) {
      CHECK_SERIALIZE_SIZE(ObSSMicroBlockCacheKey, serial_size);
      ret = serialization::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes, size_nbytes, tmp_pos, serial_size);
    }
  }
  return ret;
}

int ObSSMicroBlockCacheKey::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode(buf, buf_len, pos, mode_))) {
    LOG_WARN("fail to encode", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (is_logic_key() && OB_FAIL(logic_micro_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), KP(buf), K(buf_len), K(pos), K_(logic_micro_id));
  } else if (!is_logic_key() && OB_FAIL(micro_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize", KR(ret), KP(buf), K(buf_len), K(pos), K_(micro_id));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, micro_crc_))) {
    LOG_WARN("fail to encode", KR(ret), KP(buf), K(buf_len), K(pos), K_(micro_crc));
  }
  return ret;
}

int ObSSMicroBlockCacheKey::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    CHECK_VERSION_LENGTH(ObSSMicroBlockCacheKey, version, len);
  }
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos))) {
      LOG_WARN("fail to deserialize_", KR(ret), K(len), K(pos));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObSSMicroBlockCacheKey::deserialize_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, mode_))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (is_logic_key() && OB_FAIL(logic_micro_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (!is_logic_key() && OB_FAIL(micro_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize", KR(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, micro_crc_))) {
    LOG_WARN("fail to decode", KR(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObSSMicroBlockCacheKey::get_serialize_size(void) const
{
  int64_t len = get_serialize_size_();
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  len += NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
  return len;
}

int64_t ObSSMicroBlockCacheKey::get_serialize_size_(void) const
{
  int64_t len = serialization::encoded_length(mode_);
  if (is_logic_key()) {
    len += logic_micro_id_.get_serialize_size();
  } else {
    len += micro_id_.get_serialize_size();
  }
  len += serialization::encoded_length(micro_crc_);
  return len;
}

bool ObSSMicroBlockCacheKey::is_major_macro_key() const
{
  bool b_ret = false;
  // judge according to logical and physical mode
  // 1. logical mode represents SHARED_MAJOR_DATA_MACRO
  // 2. physical mode and SHARED_MAJOR_META_MACRO object_type represents SHARED_MAJOR_META_MACRO
  if (ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == mode_) {
    b_ret = true;
  } else if (ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE == mode_) {
    ObStorageObjectType object_type = micro_id_.macro_id_.storage_object_type();
    if ((ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == object_type)
        || (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type)) {
      b_ret = true;
    }
  }
  return b_ret;
}

ObTabletID ObSSMicroBlockCacheKey::get_major_macro_tablet_id() const
{
  ObTabletID tablet_id;
  // get tablet_id according to logical and physical mode
  // 1. logical mode: ObLogicalMicroBlockId.logic_macro_id_.tablet_id_
  // 2. physical mode: ObMicroBlockId.macro_id_.second_id()
  if (ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == mode_) {
    tablet_id = logic_micro_id_.logic_macro_id_.tablet_id_;
  } else if (ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE == mode_) {
    tablet_id = micro_id_.macro_id_.second_id();
  }
  return tablet_id;
}

int64_t ObSSMicroBlockCacheKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "{");
  ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(mode));
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, ", ");
  if (ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == mode_) {
    ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(logic_micro_id));
  } else {
    ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(micro_id));
  }
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, ", ");
  ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(micro_crc));
  ::oceanbase::common::databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

/**
 * --------------------------------ObSSMicroBlockCacheKeyMeta------------------------------------
 */
OB_SERIALIZE_MEMBER(ObSSMicroBlockCacheKeyMeta, micro_key_, data_crc_, data_size_, is_in_l1_);

ObSSMicroBlockCacheKeyMeta::ObSSMicroBlockCacheKeyMeta()
  : micro_key_(), data_crc_(0), data_size_(0), is_in_l1_(true)
{}

ObSSMicroBlockCacheKeyMeta::ObSSMicroBlockCacheKeyMeta(
  const ObSSMicroBlockCacheKey &micro_key,
  const uint32_t data_crc,
  const int32_t data_size,
  const bool is_in_l1)
  : micro_key_(micro_key), data_crc_(data_crc), data_size_(data_size), is_in_l1_(is_in_l1)
{}

ObSSMicroBlockCacheKeyMeta::ObSSMicroBlockCacheKeyMeta(
  const ObSSMicroBlockCacheKeyMeta &other)
  : micro_key_(other.micro_key_), data_crc_(other.data_crc_),
    data_size_(other.data_size_), is_in_l1_(other.is_in_l1_)
{}

int ObSSMicroBlockCacheKeyMeta::assign(const ObSSMicroBlockCacheKeyMeta &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    micro_key_ = other.micro_key_;
    data_crc_ = other.data_crc_;
    data_size_ = other.data_size_;
    is_in_l1_ = other.is_in_l1_;
  }
  return ret;
}

bool ObSSMicroBlockCacheKeyMeta::operator==(const ObSSMicroBlockCacheKeyMeta &other) const
{
  return (micro_key_ == other.micro_key_) && (data_crc_ == other.data_crc_) && (data_size_ == other.data_size_);
}

uint64_t ObSSMicroBlockCacheKeyMeta::hash() const
{
  // no need to calculate hash value for hit_type
  // because hit_type of the same micro_key may be different
  uint64_t hash_val = micro_key_.hash();
  hash_val = murmurhash(&data_crc_, sizeof(data_crc_), hash_val);
  hash_val = murmurhash(&data_size_, sizeof(data_size_), hash_val);
  return hash_val;
}

bool ObSSMicroBlockCacheKeyMeta::is_valid() const
{
  return micro_key_.is_valid() && (data_size_ > 0);
}

/*-----------------------------------------ObSSMicroBlockIndex-----------------------------------------*/
ObSSMicroBlockIndex& ObSSMicroBlockIndex::operator=(const ObSSMicroBlockIndex &other)
{
  if (this != &other) {
    micro_key_ = other.micro_key_;
    size_ = other.size_;
  }
  return *this;
}

bool ObSSMicroBlockIndex::operator==(const ObSSMicroBlockIndex &other) const
{
  return (micro_key_ == other.micro_key_) && (size_ == other.size_);
}

uint64_t ObSSMicroBlockIndex::hash() const
{
  uint64_t hash_val = micro_key_.hash();
  hash_val = common::murmurhash(&size_, sizeof(size_), hash_val);
  return hash_val;
}

OB_SERIALIZE_MEMBER(ObSSMicroBlockIndex, micro_key_, size_);


/*-----------------------------------------ObSSPhyBlockIdxRange------------------------------------------*/
ObSSPhyBlockIdxRange::ObSSPhyBlockIdxRange() : start_blk_idx_(-1), end_blk_idx_(-1)
{
}

ObSSPhyBlockIdxRange::ObSSPhyBlockIdxRange(const int64_t start_blk_idx,
                                               const int64_t end_blk_idx)
  : start_blk_idx_(start_blk_idx), end_blk_idx_(end_blk_idx)
{
}

bool ObSSPhyBlockIdxRange::is_valid() const
{
  // left-open and right-closed interval, e.g., (1, 100] represents 2~100 blocks.
  // 0 and 1 blocks are super blocks, which should not be accessed by other upper-level modules
  return (start_blk_idx_ < end_blk_idx_) && (start_blk_idx_ > 0);
}

void ObSSPhyBlockIdxRange::reset()
{
  start_blk_idx_ = -1;
  end_blk_idx_ = -1;
}

OB_SERIALIZE_MEMBER(ObSSPhyBlockIdxRange, start_blk_idx_, end_blk_idx_);


/*-----------------------------------------ObSSMemBlock-----------------------------------------*/
ObSSMemBlock::ObSSMemBlock(ObSSMemBlockPool &pool)
  : is_fg_(true), ref_cnt_(1), micro_count_(0), handled_count_(0), data_size_(0), index_size_(0), data_buf_size_(0), 
    index_buf_size_(0), reserved_size_(0), reuse_version_(0), valid_val_(0), valid_count_(0), data_buf_(nullptr), 
    index_buf_(nullptr), micro_offset_map_(), mem_blk_pool_(pool)
{}

void ObSSMemBlock::destroy()
{
  dec_ref_count();
}

void ObSSMemBlock::reuse()
{
  is_fg_ = true;
  ref_cnt_ = 1;
  micro_count_ = 0;
  handled_count_ = 0;
  data_size_ = 0;
  index_size_ = 0;
  valid_val_ = 0;
  valid_count_ = 0;
  reuse_version_ = inner_get_next_reuse_version();
  micro_offset_map_.reuse();
}

void ObSSMemBlock::reset()
{
  is_fg_ = true;
  ref_cnt_ = 0;
  micro_count_ = 0;
  handled_count_ = 0;
  data_size_ = 0;
  index_size_ = 0;
  data_buf_size_ = 0;
  index_buf_size_ = 0;
  reserved_size_ = 0;
  reuse_version_ = 0;
  valid_val_ = 0;
  valid_count_ = 0;
  micro_offset_map_.destroy();
  // data_buf/index_buf need to be released, not set nullptr here
}

int ObSSMemBlock::init(
    const uint64_t tenant_id,
    char *data_buf, 
    const uint32_t data_buf_size,
    char *index_buf,
    const uint32_t index_buf_size)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_BUCKET_NUM = 128;
  if (OB_ISNULL(data_buf) || OB_ISNULL(index_buf) || OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) ||
      OB_UNLIKELY(data_buf_size <= 0 || index_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(data_buf), K(data_buf_size), KP(index_buf), K(index_buf_size));
  } else if (OB_FAIL(micro_offset_map_.create(DEFAULT_BUCKET_NUM, ObMemAttr(tenant_id, "SSMemBlkMap")))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(tenant_id));
  } else {
    reuse();
    data_buf_ = data_buf;
    data_buf_size_ = data_buf_size;
    index_buf_ = index_buf;
    index_buf_size_ = index_buf_size;
    calc_reserved_size();
  }
  return ret;
}

bool ObSSMemBlock::exist(const ObSSMicroBlockCacheKey &micro_key) const
{
  int ret = OB_SUCCESS;
  ObSSMemMicroInfo mem_info;
  bool is_exist = false;
  if (OB_FAIL(micro_offset_map_.get_refactored(micro_key, mem_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get refactored", KR(ret), K(micro_key));
    } else {
      LOG_TRACE("not exist in mem_block", K(micro_key));
    }
  } else {
    is_exist = true;
  }
  return is_exist;
}

bool ObSSMemBlock::is_valid() const
{
  return (reserved_size_ > 0) &&
         (nullptr != data_buf_) && (data_buf_size_ > 0) && 
         (nullptr != index_buf_) && (index_buf_size_ > 0);
}

void ObSSMemBlock::inc_micro_count()
{
  ATOMIC_INC(&micro_count_);
}

void ObSSMemBlock::inc_handled_count()
{
  ATOMIC_INC(&handled_count_);
}

bool ObSSMemBlock::is_completed() const
{
  const uint32_t micro_count = ATOMIC_LOAD(&micro_count_);
  const uint32_t handled_count = ATOMIC_LOAD(&handled_count_);
  return (micro_count > 0) && (micro_count == handled_count);
}

bool ObSSMemBlock::has_no_ref() const
{
  // ref_cnt is inited as 1
  return (1 == ATOMIC_LOAD(&ref_cnt_));
}

void ObSSMemBlock::add_valid_micro_block(const uint32_t delta_size)
{
  ATOMIC_AAF(&valid_val_, delta_size);
  ATOMIC_INC(&valid_count_);
}

bool ObSSMemBlock::is_fg_mem_blk() const
{
  return ATOMIC_LOAD(&is_fg_);
}

void ObSSMemBlock::set_is_fg(const bool is_fg)
{
  ATOMIC_STORE(&is_fg_, is_fg);
}

uint32_t ObSSMemBlock::inner_get_next_reuse_version() const
{
  uint32_t next_version = ATOMIC_LOAD(&reuse_version_);
  if (SS_MAX_REUSE_VERSION == next_version) {
    next_version = 1;
  } else {
    ++next_version;
  }
  return next_version;
}

void ObSSMemBlock::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObSSMemBlock::dec_ref_count()
{
  bool succ_free = false;
  try_free(succ_free);
}

int ObSSMemBlock::try_free()
{
  bool succ_free = false;
  return try_free(succ_free);
}

int ObSSMemBlock::try_free(bool &succ_free)
{
  int ret = OB_SUCCESS;
  succ_free = false;
  const int64_t ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  if (ref_cnt == 0) {
    if (OB_FAIL(mem_blk_pool_.free(this))) {
      LOG_WARN("fail to free mem_block", KR(ret), KP(this));
    } else {
      succ_free = true;
    }
  } else if (ref_cnt < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mem_blk's ref_cnt cannot be less than 0", KR(ret), KPC(this));
  }
  return ret;
}

bool ObSSMemBlock::is_reuse_version_match(const uint32_t reuse_version) const
{
  return (reuse_version == ATOMIC_LOAD(&reuse_version_));
}

int ObSSMemBlock::get_all_micro_keys(ObIArray<ObSSMicroBlockCacheKey> &micro_keys) const
{
  int ret = OB_SUCCESS;
  MicroPhyInfoMap::const_iterator iter = micro_offset_map_.begin();
  for (; OB_SUCC(ret) && (iter != micro_offset_map_.end()); ++iter) {
    if (OB_FAIL(micro_keys.push_back(iter->first))) {
      LOG_WARN("fail to push back", KR(ret), "micro_key", iter->first);
    }
  }
  return ret;
}

uint32_t ObSSMemBlock::get_payload_size() const
{
  return data_size_ + index_size_;
}

uint32_t ObSSMemBlock::get_micro_index_offset() const
{
  return reserved_size_ + data_size_;
}

int ObSSMemBlock::calc_write_location(
    const ObSSMicroBlockCacheKey &micro_key, 
    const int32_t size, 
    int32_t &data_offset, 
    int32_t &idx_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size));
  } else {
    ObSSMicroBlockIndex micro_index(micro_key, size);
    const int32_t idx_size = micro_index.get_serialize_size();
    const int32_t delta_size = size + idx_size;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current mem_block is not valid", KR(ret), K(micro_key), K(size), K(*this));
    } else if (OB_UNLIKELY(is_large_micro(delta_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("this micro_block is too large", KR(ret), K(delta_size), K(micro_key), K(size), K(*this));
    } else if (has_enough_space(delta_size, idx_size)) {
      data_offset = reserved_size_ + data_size_;
      data_size_ += size;
      idx_offset = index_size_;
      index_size_ += idx_size;
      inc_micro_count();
    } else {
      ret = OB_SIZE_OVERFLOW;
      LOG_TRACE("current mem_block has not enough space", KR(ret), K(micro_key), K(size), K(*this));
    }
  }
  return ret;
}

int ObSSMemBlock::write_micro_data(
    const ObSSMicroBlockCacheKey &micro_key, 
    const char *micro_data, 
    const int32_t size,
    const int32_t data_offset, 
    const int32_t idx_offset,
    uint32_t &crc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_data) || OB_UNLIKELY(size <= 0 || data_offset < 0 || idx_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(micro_data), K(data_offset), K(idx_offset), K(size), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current mem_block is not valid", KR(ret), K(*this));
  } else {
    crc = static_cast<uint32_t>(ob_crc64(micro_data, size));
    MEMCPY(data_buf_ + data_offset, micro_data, size);

    int64_t index_pos = idx_offset;
    ObSSMicroBlockIndex micro_index(micro_key, size);
    ObSSMemMicroInfo mem_micro_info(data_offset, size);
    if (OB_FAIL(micro_offset_map_.set_refactored(micro_key, mem_micro_info, 1/*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(micro_key), K(mem_micro_info));
    } else if (OB_FAIL(micro_index.serialize(index_buf_, index_buf_size_, index_pos))) {
      LOG_WARN("fail to serialize micro_index", KR(ret), K(micro_index), K(*this));
    }
  }
  return ret;
}

int ObSSMemBlock::get_micro_data(
    const ObSSMicroBlockCacheKey &micro_key,
    const int32_t size,
    const uint32_t crc,
    char *&buf) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current mem_block is not valid", KR(ret), K(*this));
  } else {
    ObSSMemMicroInfo mem_micro_info;
    if (OB_FAIL(micro_offset_map_.get_refactored(micro_key, mem_micro_info))) {
      LOG_WARN("fail to get refactored", KR(ret), K(micro_key));
    } else if (OB_UNLIKELY(!mem_micro_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem_micro_info is invalid", KR(ret), K(micro_key), K(mem_micro_info));
    } else if (OB_UNLIKELY(size != mem_micro_info.size_)) {
      // There only exists reading a whole macro_block, or just read a micro_block, won't read a
      // batch of micro_blocks. 
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("request size must be equal to micro_cache size", KR(ret), K(size), K(mem_micro_info));
    } else if (OB_FAIL(check_micro_data_crc(mem_micro_info.offset_, size, crc))) {
      LOG_WARN("fail to check micro data crc", KR(ret), K(mem_micro_info), K(size), K(crc));
    } else {
      buf = data_buf_ + mem_micro_info.offset_;
    }
  }
  return ret;
}

// For test
int ObSSMemBlock::get_micro_data(
    const ObSSMicroBlockCacheKey &micro_key,
    char *buf, 
    const int32_t size,
    const uint32_t crc) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), KP(buf), K(size));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current mem_block is not valid", KR(ret), K(*this));
  } else {
    ObSSMemMicroInfo mem_micro_info;
    if (OB_FAIL(micro_offset_map_.get_refactored(micro_key, mem_micro_info))) {
      LOG_WARN("fail to get refactored", KR(ret), K(micro_key));
    } else if (OB_UNLIKELY(!mem_micro_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem_micro_info is invalid", KR(ret), K(micro_key), K(mem_micro_info));
    } else if (OB_UNLIKELY(size != mem_micro_info.size_)) {
      // There only exists reading a whole macro_block, or just read a micro_block, won't read a
      // batch of micro_blocks. 
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("request size must be equal to micro_cache size", KR(ret), K(size), K(mem_micro_info));
    } else if (OB_FAIL(check_micro_data_crc(mem_micro_info.offset_, size, crc))) {
      LOG_WARN("fail to check micro data crc", KR(ret), K(mem_micro_info), K(size), K(crc));
    } else {
      MEMCPY(buf, data_buf_ + mem_micro_info.offset_, size);
    }
  }
  return ret;
}

bool ObSSMemBlock::check_size_valid() const
{
  return (reserved_size_ > 0) && (data_buf_size_ > 0) && (reserved_size_ + data_size_ + index_size_ <= data_buf_size_);
}

int ObSSMemBlock::handle_when_sealed()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_buf_) || OB_ISNULL(index_buf_) || OB_UNLIKELY(!check_size_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cur mem_block has abnormal state", KR(ret), K(*this));
  } else {
    MEMCPY(data_buf_ + reserved_size_ + data_size_, index_buf_, index_size_);
  }
  return ret;
}

int ObSSMemBlock::check_micro_data_crc(
    const int32_t data_pos,
    const int32_t size,
    const uint32_t crc) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_buf_) || OB_UNLIKELY(data_pos < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(data_pos), K(size), KP_(data_buf));
  } else {
    const int32_t real_crc = static_cast<uint32_t>(ob_crc64(data_buf_ + data_pos, size));
    if (OB_UNLIKELY(real_crc != crc)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("micro_block data checksum error", KR(ret), K(real_crc), K(crc), K(data_pos), K(size));
    }
  }
  return ret;
}

void ObSSMemBlock::calc_reserved_size()
{
  reserved_size_ = ObSSPhyBlockCommonHeader::get_serialize_size() +
                   ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
}

bool ObSSMemBlock::is_large_micro(const int32_t delta_size) const
{
  return reserved_size_ + delta_size > data_buf_size_;
}

bool ObSSMemBlock::has_enough_space(const int32_t delta_size, const int32_t idx_size) const
{
  return (reserved_size_ + data_size_ + index_size_ + delta_size <= data_buf_size_) &&
         (index_size_ + idx_size <= index_buf_size_);
}

/*-----------------------------------------ObSSMemBlockPool------------------------------------------*/
ObSSMemBlockPool::ObSSMemBlockPool(ObSSMicroCacheStat &cache_stat)
  : is_inited_(false), block_size_(0), tenant_id_(OB_INVALID_TENANT_ID), def_count_(0), max_extra_count_(0),
    used_extra_count_(0), used_fg_count_(0), max_bg_count_(0), used_bg_count_(0), cond_(),
    total_data_buf_(nullptr), free_list_(), mem_blk_data_buf_size_(0), mem_blk_index_buf_size_(0), 
    cache_stat_(cache_stat)
{}

void ObSSMemBlockPool::destroy()
{
  cond_.destroy();
  destroy_free_list();
  if (nullptr != total_data_buf_) {
    ob_free_align(total_data_buf_);
    total_data_buf_ = nullptr;
  }
  
  block_size_ = 0;
  def_count_ = 0;
  max_extra_count_ = 0;
  used_extra_count_ = 0;
  used_fg_count_ = 0;
  max_bg_count_ = 0;
  used_bg_count_ = 0;
  mem_blk_data_buf_size_ = 0;
  mem_blk_index_buf_size_ = 0;
  is_inited_ = false;
}

int ObSSMemBlockPool::destroy_free_list()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ObSSMemBlock *tmp_mem_blk = nullptr;
    bool has_more = true;
    const int64_t interval_us = 10 * 1000 * 1000;
    while (OB_SUCC(ret) && has_more) {
      if (OB_FAIL(free_list_.pop(tmp_mem_blk))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          has_more = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to pop from free_list", KR(ret));
        }
      } else if (OB_NOT_NULL(tmp_mem_blk)) {
        while (!tmp_mem_blk->has_no_ref()) {
          if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_ERROR("mem_block can't be free, may leak", KR(ret), KP(tmp_mem_blk), KPC(tmp_mem_blk));
          }
          ob_usleep(100);
        }

        if (0 == ATOMIC_AAF(&tmp_mem_blk->ref_cnt_, -1)) {
          inner_destroy(tmp_mem_blk);
          tmp_mem_blk = nullptr;
        } else {
          LOG_ERROR("mem_block can't be free, will leak", KR(ret), KP(tmp_mem_blk), KPC(tmp_mem_blk));
        }
      }
    }
    free_list_.destroy();
  }
  return ret;
}

int ObSSMemBlockPool::init(
    const uint64_t tenant_id,
    const uint32_t block_size,
    const int64_t def_count, 
    const int64_t max_count,
    const int64_t max_bg_mem_blk_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(def_count < 0 || block_size <= 0 || max_count < def_count) ||
             OB_UNLIKELY(max_count <= max_bg_mem_blk_cnt || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(block_size), K(def_count), K(max_count), 
      K(max_bg_mem_blk_cnt));
  // TODO @donglou.zl revise this wait_event_id
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::TIERED_BLOCK_WRITE_REMOTE))) {
    LOG_WARN("fail to init thread cond", K(ret));
  } else if (OB_FAIL(free_list_.init(max_count))) {
    LOG_WARN("fail to init free list", KR(ret), K(max_count));
  } else {
    tenant_id_ = tenant_id;
    def_count_ = def_count;
    max_extra_count_ = max_count - def_count;
    used_extra_count_ = 0;
    used_fg_count_ = 0;
    max_bg_count_ = max_bg_mem_blk_cnt;
    used_bg_count_ = 0;
    block_size_ = block_size;
    mem_blk_data_buf_size_ = block_size;
    mem_blk_index_buf_size_ = block_size / 2;
    if (OB_FAIL(pre_create_mem_blocks())) {
      LOG_WARN("fail to pre create mem_blocks", KR(ret));
    } else {
      cache_stat_.mem_blk_stat().set_mem_blk_def_cnt(def_count);
      cache_stat_.mem_blk_stat().set_mem_blk_fg_max_cnt(max_count - max_bg_count_);
      cache_stat_.mem_blk_stat().set_mem_blk_bg_max_cnt(max_bg_count_);
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMemBlockPool::pre_create_mem_blocks()
{
  int ret = OB_SUCCESS;
  if (def_count_ > 0) {
    const int64_t mem_blk_buf_size = mem_blk_data_buf_size_ + mem_blk_index_buf_size_;
    const int64_t total_buf_size = def_count_ * mem_blk_buf_size;
    ObMemAttr attr(tenant_id_, "SSMemBlk");
    if (OB_ISNULL(total_data_buf_ = static_cast<char*>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, total_buf_size, attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(total_buf_size), K(attr));
    } else {
      cache_stat_.mem_blk_stat().update_total_mem_blk_size(total_buf_size);
    }
    
    for (int64_t i = 0; OB_SUCC(ret) && (i < def_count_); ++i) {
      char *data_buf = total_data_buf_ + i * mem_blk_buf_size;
      char *index_buf = data_buf + mem_blk_data_buf_size_;
      ObSSMemBlock *mem_block = nullptr;
      if (OB_FAIL(inner_alloc(mem_block, data_buf, index_buf))) {
        LOG_WARN("fail to inner_alloc pre_create mem_blk", KR(ret), K(i), K_(block_size), K(mem_blk_buf_size));
      } else if (OB_FAIL(free_list_.push(mem_block))) {
        LOG_WARN("fail to push into free list", KR(ret), K(mem_block));
      } else {
        LOG_INFO("ss_cache: succ to pre_create mem_blk", K_(tenant_id), K(i), KP(mem_block));
      }
    }

    if (OB_FAIL(ret) && (nullptr != total_data_buf_)) {
      ob_free_align(total_data_buf_);
    }
  }
  return ret;
}

void ObSSMemBlockPool::create_mem_block_on_fail(bool is_fg)
{
  ATOMIC_AAF(&used_extra_count_, -1);
  if (is_fg) {
    ATOMIC_AAF(&used_fg_count_, -1);
  } else {
    ObThreadCondGuard guard(cond_);
    --used_bg_count_;
  }
}

int ObSSMemBlockPool::create_dynamic_mem_block(ObSSMemBlock *&mem_block)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, "SSDyMemBlkBuf");
  char *mem_blk_buf = nullptr;
  const int64_t mem_blk_buf_size = mem_blk_data_buf_size_ + mem_blk_index_buf_size_;
  if (OB_ISNULL(mem_blk_buf = static_cast<char*>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, mem_blk_buf_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(mem_blk_buf_size), K(attr));
  } else {
    cache_stat_.mem_blk_stat().update_total_mem_blk_size(mem_blk_buf_size);
    char *data_buf = mem_blk_buf;
    char *index_buf = data_buf + mem_blk_data_buf_size_;
    if (OB_FAIL(inner_alloc(mem_block, data_buf, index_buf))) {
      LOG_WARN("fail to inner_alloc dynamic mem_blk", KR(ret), K(mem_blk_buf_size), KP(data_buf), KP(index_buf));
    } else {
      LOG_INFO("ss_cache: succ to create dynamic mem_blk", KP(mem_block));
    }
  }

  if (OB_FAIL(ret) && (nullptr != mem_blk_buf)) {
    ob_free_align(mem_blk_buf);
    cache_stat_.mem_blk_stat().update_total_mem_blk_size(mem_blk_buf_size * -1);
  }
  return ret;
}

int ObSSMemBlockPool::alloc(ObSSMemBlock *&mem_block, const bool is_fg)
{
  int ret = OB_SUCCESS;
  bool is_temp = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_free_mem_blk_enough(is_fg))) {
    LOG_WARN("free mem_blk count is not enough", KR(ret), K(is_fg));
  } else if (OB_FAIL(free_list_.pop(mem_block))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // allow to create a dynamic mem_block
      if (ATOMIC_AAF(&used_extra_count_, 1) <= max_extra_count_) {
        if (OB_FAIL(create_dynamic_mem_block(mem_block))) {
          LOG_WARN("fail to create dynamic mem_block", KR(ret));
        } else {
          is_temp = true;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("count of dynamic mem_block is abnormal", KR(ret), K(is_fg), K_(def_count), K_(max_extra_count),
          K_(used_extra_count), K_(max_bg_count), K_(used_bg_count), K_(used_fg_count));
      }

      if (OB_FAIL(ret)) {
        create_mem_block_on_fail(is_fg);
      }
    } else {
      LOG_WARN("fail to pop free mem_block", KR(ret), K(is_fg), K_(def_count), K_(max_extra_count));
    }
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(mem_block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("alloc mem_block should not be null", KR(ret), K(is_fg));
  } else {
    mem_block->set_is_fg(is_fg);
    LOG_TRACE("ss_cache: succ to alloc mem_blk", K(is_temp), K(is_fg), K_(used_fg_count), K_(used_bg_count), 
      KP(mem_block), KPC(mem_block));
  }
  return ret;
}

int ObSSMemBlockPool::free(ObSSMemBlock *mem_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_ISNULL(mem_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_block));
  } else if (OB_UNLIKELY(!mem_block->is_valid() || mem_block->ref_cnt_ > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(mem_block));
  } else {
    const bool is_fg = mem_block->is_fg_mem_blk();
    if (is_pre_created_mem_blk(mem_block)) {
      mem_block->reuse();
      if (OB_FAIL(free_list_.push(mem_block))) {
        LOG_WARN("fail to push into free_list", KR(ret), KPC(mem_block));
      } else {
        LOG_TRACE("ss_cache: succ to free mem_blk", K(is_fg), K_(used_fg_count), K_(used_bg_count), KP(mem_block), 
          KPC(mem_block));
      }
    } else {
      LOG_TRACE("ss_cache: succ to erase tmp mem_blk", K(is_fg), K_(used_fg_count), K_(used_bg_count), KP(mem_block), 
        KPC(mem_block));
      inner_destroy(mem_block);
      mem_block = nullptr;
      ATOMIC_AAF(&used_extra_count_, -1);
    }

    if (OB_SUCC(ret)) {
      if (is_fg) {
        const int64_t cur_used_fg_cnt = ATOMIC_AAF(&used_fg_count_, -1);
        cache_stat_.mem_blk_stat().update_mem_blk_fg_used_cnt(-1);
        if (cur_used_fg_cnt < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fg_mem_blk used count is abnormal", KR(ret), K(cur_used_fg_cnt));
        }
      } else {
        ObThreadCondGuard guard(cond_);
        --used_bg_count_;
        cache_stat_.mem_blk_stat().update_mem_blk_bg_used_cnt(-1);
        cond_.signal();
        if (used_bg_count_ < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bg_mem_blk used count is abnormal", KR(ret), K_(used_bg_count));
        }
      }
    }
  }
  return ret;
}

int ObSSMemBlockPool::inner_alloc(
    ObSSMemBlock *&mem_block, 
    char *data_buf, 
    char *index_buf)
{
  int ret = OB_SUCCESS;
  ObSSMemBlock *mem_blk = nullptr;
  void *blk_buf = nullptr;
  ObMemAttr attr(tenant_id_, "ObSSMemBlk");
  const int64_t mem_blk_size = sizeof(ObSSMemBlock);
  if (OB_ISNULL(data_buf) || OB_ISNULL(index_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data_buf), KP(index_buf));
  } else if (OB_ISNULL(blk_buf = ob_malloc(mem_blk_size, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(mem_blk_size), K(attr));
  } else if (FALSE_IT(cache_stat_.mem_blk_stat().update_total_mem_blk_size(mem_blk_size))) {
  } else if (FALSE_IT(mem_blk = new (blk_buf) ObSSMemBlock(*this))) {
  } else if (OB_ISNULL(mem_blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_block should not be nullptr", KR(ret), KP(blk_buf));
  } else if (OB_FAIL(mem_blk->init(tenant_id_, data_buf, mem_blk_data_buf_size_, index_buf, mem_blk_index_buf_size_))) {
    LOG_WARN("fail to init mem_block", KR(ret), K_(tenant_id), KP(data_buf), K_(mem_blk_data_buf_size),
      KP(index_buf), K_(mem_blk_index_buf_size));
  } else if (OB_UNLIKELY(!mem_blk->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_block is invalid", KR(ret), KPC(mem_blk));
  } else {
    mem_block = mem_blk;
  }

  if (OB_FAIL(ret)) {
    if (nullptr != blk_buf) {
      ob_free(blk_buf);
      blk_buf = nullptr;
      cache_stat_.mem_blk_stat().update_total_mem_blk_size(mem_blk_size * -1);
    }
  }
  return ret;
}

void ObSSMemBlockPool::inner_destroy(ObSSMemBlock *mem_block)
{
  if (nullptr != mem_block) {
    if (is_alloc_dynamiclly(mem_block)) {
      ob_free_align(mem_block->data_buf_);
      const int64_t mem_blk_buf_size = mem_blk_data_buf_size_ + mem_blk_index_buf_size_;
      cache_stat_.mem_blk_stat().update_total_mem_blk_size(mem_blk_buf_size * -1);
    }
    mem_block->reset();
    ob_free(mem_block);
    cache_stat_.mem_blk_stat().update_total_mem_blk_size(sizeof(ObSSMemBlock) * -1);
  }
}

bool ObSSMemBlockPool::is_alloc_dynamiclly(ObSSMemBlock *mem_block)
{
  return !is_pre_created_mem_blk(mem_block);
}

int64_t ObSSMemBlockPool::get_free_mem_blk_cnt(const bool is_fg) const
{
  int64_t free_cnt = 0;
  if (is_fg) {
    free_cnt = MAX(0, get_fg_max_count() - ATOMIC_LOAD(&used_fg_count_));
  } else {
    free_cnt = MAX(0, get_bg_max_count() - ATOMIC_LOAD(&used_bg_count_));
  }
  return free_cnt;
}

bool ObSSMemBlockPool::is_pre_created_mem_blk(ObSSMemBlock *mem_block) const
{
  bool is_pre_created = false;
  if (OB_NOT_NULL(mem_block) && def_count_ > 0) {
    char *start_buf_pos = total_data_buf_;
    char *end_buf_pos = total_data_buf_ + def_count_ * (mem_blk_data_buf_size_ + mem_blk_index_buf_size_);
    if ((mem_block->data_buf_ >= start_buf_pos) && (mem_block->data_buf_ <= end_buf_pos) &&
        (mem_block->index_buf_ == mem_block->data_buf_ + mem_blk_data_buf_size_)) {
      is_pre_created = true;
    }
  }
  return is_pre_created;
}

int ObSSMemBlockPool::check_free_mem_blk_enough(const bool is_fg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (is_fg) {
    const int64_t fg_max_cnt = get_fg_max_count();
    if (ATOMIC_AAF(&used_fg_count_, 1) > fg_max_cnt) {
      ATOMIC_AAF(&used_fg_count_, -1);
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("free fg_mem_blk is not enough", KR(ret), K(fg_max_cnt), K_(used_fg_count));
    } else {
      cache_stat_.mem_blk_stat().update_mem_blk_fg_used_cnt(1);
    }
  } else {
    ObThreadCondGuard guard(cond_);
    if (used_bg_count_ >= max_bg_count_) {
      if (OB_SUCCESS != (tmp_ret = cond_.wait(DEF_WAIT_TIMEOUT_MS))) {
        if (OB_TIMEOUT == tmp_ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("free bg_mem_blk is not enough", KR(ret), KR(tmp_ret), K(DEF_WAIT_TIMEOUT_MS), K_(used_bg_count), 
            K_(max_bg_count));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wait thread condition failed", KR(ret), KR(tmp_ret), K(DEF_WAIT_TIMEOUT_MS), K_(used_bg_count), 
            K_(max_bg_count));
        }
      } else {
        ++used_bg_count_;
        cache_stat_.mem_blk_stat().update_mem_blk_bg_used_cnt(1);
      }
    } else {
      ++used_bg_count_;
      cache_stat_.mem_blk_stat().update_mem_blk_bg_used_cnt(1);
    }
  }
  return ret;
}

/*-----------------------------------------ObSSMicroBlockMeta-----------------------------------------*/
ObSSMicroBlockMeta::ObSSMicroBlockMeta()
  : first_val_(0), access_time_(0), length_(0), is_in_l1_(1),
    is_in_ghost_(0), is_persisted_(0), is_reorganizing_(0), reserved_(0),
    ref_cnt_(0), crc_(0), micro_key_()
{}

void ObSSMicroBlockMeta::destroy()
{
  reset();
}

void ObSSMicroBlockMeta::reset()
{
  first_val_ = 0;
  access_time_ = 0;
  length_ = 0;
  is_in_l1_ = 1;
  is_in_ghost_ = 0;
  is_persisted_ = 0;
  is_reorganizing_ = 0;
  reserved_ = 0;
  ref_cnt_ = 0;
  crc_ = 0;
  micro_key_.reset();
}

bool ObSSMicroBlockMeta::is_valid_field() const 
{ 
  return (0 != first_val_) && (length_ > 0) && (access_time_ > 0) && (micro_key_.is_valid());
}

bool ObSSMicroBlockMeta::is_valid() const
{
  bool b_ret = is_valid_field();
  if (b_ret) {
    if (is_persisted_) {
      b_ret = is_phy_block_match();
    } else {
      b_ret = is_mem_block_match();
    }
  }
  return b_ret;
}

bool ObSSMicroBlockMeta::is_valid(const ObSSMemBlockHandle &mem_blk_handle) const
{
  return is_valid_field() && is_mem_block_match(mem_blk_handle);
}

bool ObSSMicroBlockMeta::is_persisted_valid() const
{
  return is_valid_field() && is_phy_block_match();
}

bool ObSSMicroBlockMeta::is_valid_for_ckpt(const bool print_log) const
{
  const bool valid_field = is_valid_field();
  uint64_t phy_blk_reuse_version = 0;
  bool b_ret = (is_persisted_ && (is_in_ghost_ || (valid_field && is_phy_block_match(phy_blk_reuse_version))));
  if (!b_ret && print_log) {
    int ret = OB_SUCCESS;
    LOG_WARN("invalid for micro_cache ckpt", K(valid_field), KPC(this), K(phy_blk_reuse_version));
  }
  return b_ret;
}

bool ObSSMicroBlockMeta::is_expired(const int64_t expiration_time) const
{
  return (inner_get_cur_time_s() >= (expiration_time + access_time_));
}

bool ObSSMicroBlockMeta::is_mem_block_match() const
{
  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(get_mem_block());
  return is_mem_block_match(mem_blk_handle);
}

// micro_meta's is_persisted_ must be false, @mem_blk should not be null
// micro_meta's mem_blk_ must be equal to @mem_blk, micro_meta's reuse_version should be
// equal to mem_blk.reuse_version and this micro_block should be in mem_blk's index map, 
// we think this micro_meta's mem_blk is matched.
bool ObSSMicroBlockMeta::is_mem_block_match(const ObSSMemBlockHandle &mem_blk_handle) const
{
  bool b_ret = (!is_persisted_) && (mem_blk_handle.is_valid()) && (get_mem_block() == mem_blk_handle.get_ptr());
  if (b_ret) {
    b_ret = ((reuse_version_ == mem_blk_handle()->reuse_version_) && (mem_blk_handle()->exist(micro_key_)));
  }
  return b_ret;
}

bool ObSSMicroBlockMeta::is_phy_block_match() const
{
  uint64_t phy_blk_reuse_version = 0;
  return is_phy_block_match(phy_blk_reuse_version);
}

bool ObSSMicroBlockMeta::is_phy_block_match(uint64_t &phy_blk_reuse_version) const
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  if (is_persisted_) {
    ObSSPhysicalBlockHandle phy_blk_handle;
    phy_blk_reuse_version = 0;
    if (OB_FAIL(SSPhyBlockMgr.get_block_handle(data_dest_, reuse_version_, phy_blk_handle, phy_blk_reuse_version))) {
      LOG_WARN("fail to get phy_block handle", KR(ret), KPC(this));
    } else if (OB_LIKELY(phy_blk_handle.is_valid())) {
      b_ret = true;
    }
  }
  return b_ret;
}

void ObSSMicroBlockMeta::mark_invalid()
{
  first_val_ = 0;
}

int ObSSMicroBlockMeta::init(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMemBlockHandle &mem_blk_handle,
    const uint32_t length,
    const uint32_t crc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid()) || OB_UNLIKELY(!micro_key.is_valid() || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(length), K(mem_blk_handle));
  } else {
    inner_validate_micro_meta(micro_key, mem_blk_handle, length, crc);
  } 
  return ret;
}

int ObSSMicroBlockMeta::validate_micro_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMemBlockHandle &mem_blk_handle,
    const uint32_t length,
    const uint32_t crc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid() || length <= 0 || length != length_ || 
      !micro_key.is_valid() || !(micro_key == micro_key_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(length), K(mem_blk_handle), K(*this));
  } else if (OB_UNLIKELY(crc != crc_)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("ss_micro_cache checksum error", KR(ret), K(crc), K(*this));
  } else {
    inner_validate_micro_meta(micro_key, mem_blk_handle, length, crc);
  }
  return ret;
}

void ObSSMicroBlockMeta::inner_validate_micro_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMemBlockHandle &mem_blk_handle,
    const uint32_t length,
    const uint32_t crc)
{
  // NOTICE: we think that mem_blk pointer will use no more than 48 bits. reuse_version will 
  //         use 16 (high)bits. So, here we set mem_blk_ and reuse_version_ together.
  inner_set_mem_block(mem_blk_handle);
  reuse_version_ = mem_blk_handle()->reuse_version_;
  length_ = length;
  is_persisted_ = 0;
  is_reorganizing_ = 0;
  update_access_time();
  crc_ = crc;
  micro_key_ = micro_key;
}

int ObSSMicroBlockMeta::update_reorganizing_state(const ObSSMemBlockHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handle));
  } else if (OB_UNLIKELY(!this->is_valid_reorganizing_state())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this micro_block is not in reorganizing state", KR(ret), KPC(this));
  } else {
    inner_set_mem_block(mem_blk_handle);
    reuse_version_ = mem_blk_handle()->reuse_version_;
    is_persisted_ = 0;
    is_reorganizing_ = 0;
  }
  return ret;
}

ObSSMemBlock* ObSSMicroBlockMeta::get_mem_block() const
{
  ObSSMemBlock* mem_blk = nullptr;
  if (!is_persisted_) {
    mem_blk = reinterpret_cast<ObSSMemBlock *>(data_dest_);
  }
  return mem_blk;
}

void ObSSMicroBlockMeta::inner_set_mem_block(const ObSSMemBlockHandle &mem_blk_handle)
{
  if (mem_blk_handle.is_valid()) {
    data_dest_ = reinterpret_cast<uint64_t>(mem_blk_handle.get_ptr());
  }
}

bool ObSSMicroBlockMeta::can_reorganize() const
{
  return is_valid_field() && is_phy_block_match() && (!is_reorganizing_) && (!is_in_ghost_);
}

bool ObSSMicroBlockMeta::is_valid_reorganizing_state() const
{
  return is_valid_field() && (is_persisted_) && (is_reorganizing_) && (!is_in_ghost_);
}

bool ObSSMicroBlockMeta::can_evict() const 
{ 
  return is_valid_field() && is_persisted_ && !is_reorganizing_; 
}

bool ObSSMicroBlockMeta::can_delete() const 
{ 
  return is_in_ghost_ && is_persisted_ && !is_reorganizing_; 
}

void ObSSMicroBlockMeta::get_micro_base_info(ObSSMicroBaseInfo &micro_info) const
{
  micro_info.is_in_l1_ = is_in_l1_;
  micro_info.is_in_ghost_ = is_in_ghost_;
  micro_info.is_persisted_ = is_persisted_;
  micro_info.size_ = length_;
  micro_info.crc_ = crc_;
  micro_info.reuse_version_ = reuse_version_;
  if (is_persisted_) {
    micro_info.data_dest_ = data_dest_;
  }
}

bool ObSSMicroBlockMeta::has_no_ref() const
{
  return (0 == ATOMIC_LOAD(&ref_cnt_));
}

void ObSSMicroBlockMeta::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObSSMicroBlockMeta::dec_ref_count()
{
  try_free();
}

void ObSSMicroBlockMeta::try_free()
{
  int ret = OB_SUCCESS;
  const int64_t ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
  if (ref_cnt == 0) {
    free();
  } else if (ref_cnt < 0) {
    LOG_ERROR("micro_meta's ref_cnt cannot be less than 0", KPC(this));
  }
}

void ObSSMicroBlockMeta::free()
{
  int ret = OB_SUCCESS;
  ObSSMicroBlockCacheKey micro_key = micro_key_;
  uint32_t cur_ref_cnt = 0;
  if (OB_UNLIKELY(0 != (cur_ref_cnt = ATOMIC_LOAD(&ref_cnt_)))) {
    LOG_ERROR("micro_meta ref_cnt must be 0", K(cur_ref_cnt), KPC(this));
  }

  reset(); // same as 'destruction'
  SSMicroMetaAlloc.free(this);
  SSMicroCacheStat.micro_stat().update_micro_pool_alloc_cnt(-1);
  SSMicroCacheStat.micro_stat().update_micro_pool_info();
  LOG_TRACE("succ free micro_meta", K(micro_key));
}

uint64_t ObSSMicroBlockMeta::get_heat_val() const
{
  return access_time_;
}

bool ObSSMicroBlockMeta::check_reorganizing_state() const
{
  return is_persisted_ && !is_in_ghost_ && is_reorganizing_;
}

void ObSSMicroBlockMeta::transfer_arc_seg(const ObSSARCOpType &op_type)
{
  int ret = OB_SUCCESS;
  if (ObSSARCOpType::SS_ARC_NEW_ADD == op_type) { // Default in T1
    is_in_l1_ = true;
    is_in_ghost_ = false;
  } else if (ObSSARCOpType::SS_ARC_HIT_GHOST == op_type) { // If cache_hit ghost, B1/B2 -> T2
    is_in_l1_ = false;
    is_in_ghost_ = false;
  } else if (ObSSARCOpType::SS_ARC_TASK_EVICT_OP == op_type) { // If evict, T1 -> B1 or T2 -> B2
    is_in_ghost_ = true;
  } else if (ObSSARCOpType::SS_ARC_HIT_T1 == op_type) { // If hit T1 again, T1 -> T2
    is_in_l1_ = false;
  } else if (ObSSARCOpType::SS_ARC_HIT_T2 == op_type) { // If hit T2
    // do nothing
  } else {
    LOG_WARN("invalid op_type", K(op_type));
  }
  update_access_time();
}

void ObSSMicroBlockMeta::update_access_time()
{
  const uint32_t cur_time_s = inner_get_cur_time_s();
  if (access_time_ < cur_time_s) {
    access_time_ = cur_time_s;
  }
}

void ObSSMicroBlockMeta::update_access_time(const int64_t delta_time_s)
{
  access_time_ += delta_time_s;
}

uint32_t ObSSMicroBlockMeta::inner_get_cur_time_s() const
{
  return (uint32_t)(ObTimeUtility::current_time_s());
}

ObSSMicroBlockMeta& ObSSMicroBlockMeta::operator=(const ObSSMicroBlockMeta &other)
{
  if (this != &other) {
    first_val_ = other.first_val_;
    second_val_ = other.second_val_;
    crc_ = other.crc_;
    micro_key_ = other.micro_key_;
  }
  return *this;
}

bool ObSSMicroBlockMeta::operator==(const ObSSMicroBlockMeta &other) const
{
  return (first_val_ == other.first_val_) && (second_val_ == other.second_val_) && 
         (crc_ == other.crc_) && (micro_key_ == other.micro_key_);
}

OB_SERIALIZE_MEMBER(ObSSMicroBlockMeta, first_val_, second_val_, crc_, micro_key_);

/*-----------------------------------------ObSSMicroBaseInfo------------------------------------------*/
void ObSSMicroBaseInfo::reset()
{
  is_in_l1_ = true; 
  is_in_ghost_ = false; 
  is_persisted_ = false; 
  size_ = 0; 
  crc_ = 0; 
  data_dest_ = 0;
  reuse_version_ = 0;
}

ObSSMicroBaseInfo& ObSSMicroBaseInfo::operator=(const ObSSMicroBaseInfo &other)
{
  if (this != &other) {
    is_in_l1_ = other.is_in_l1_;
    is_in_ghost_ = other.is_in_ghost_;
    is_persisted_ = other.is_persisted_;
    size_ = other.size_;
    crc_ = other.crc_;
    data_dest_ = other.data_dest_;
    reuse_version_ = other.reuse_version_;
  }
  return *this;
}

/*-----------------------------------------ObSSMicroMetaSnapshot------------------------------------------*/
ObSSMicroMetaSnapshot& ObSSMicroMetaSnapshot::operator=(const ObSSMicroMetaSnapshot &other)
{
  if (this != &other) {
    // Note: micro_meta's internal ref_cnt_ is not copied in this assignment operation
    micro_meta_ = other.micro_meta_;
  }
  return *this;
}

int ObSSMicroMetaSnapshot::init(const ObSSMicroBlockMeta& micro_meta)
{
  int ret = OB_SUCCESS;
  micro_meta_ = micro_meta;
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
