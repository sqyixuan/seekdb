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

#include "ob_ss_arc_info.h"
#include "share/ob_force_print_log.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_ss_micro_cache.h"

namespace oceanbase 
{
namespace storage 
{
bool is_valid_arc_seg_idx(const int64_t seg_idx)
{
  return (seg_idx >= 0 && seg_idx < SS_ARC_SEG_COUNT);
}

int64_t get_arc_seg_idx(const bool is_in_l1, const bool is_in_ghost)
{
  int ret = OB_SUCCESS;
  int64_t arc_seg_idx = ARC_T1;
  if (is_in_l1 && is_in_ghost) {
    arc_seg_idx = ARC_B1;
  } else if (!is_in_l1 && !is_in_ghost) {
    arc_seg_idx = ARC_T2;
  } else if (!is_in_l1 && is_in_ghost) {
    arc_seg_idx = ARC_B2;
  }
  if (OB_UNLIKELY(!is_valid_arc_seg_idx(arc_seg_idx))) {
    LOG_ERROR("invalid arc seg_idx", K(arc_seg_idx), LITERAL_K(SS_ARC_SEG_COUNT));
  }
  return arc_seg_idx;
}

/*-----------------------------------------ObSSARCSegOpInfo------------------------------------------*/
void ObSSARCSegOpInfo::update_op_info(const int64_t actual_op_cnt, const int64_t exp_iter_cnt)
{
  op_cnt_ = actual_op_cnt;
  exp_iter_cnt_ = exp_iter_cnt;
}

ObSSARCSegOpInfo& ObSSARCSegOpInfo::operator=(const ObSSARCSegOpInfo &other) 
{
  if (this != &other) {
    to_delete_ = other.to_delete_;
    op_cnt_ = other.op_cnt_;
    exp_iter_cnt_ = other.exp_iter_cnt_;
    obtained_cnt_ = other.obtained_cnt_;
  }
  return *this;
}

/*-----------------------------------------ObSSARCIterInfo::ObSSARCIterSegInfo------------------------------------------*/
ObSSARCIterInfo::ObSSARCIterSegInfo& ObSSARCIterInfo::ObSSARCIterSegInfo::operator=(
    const ObSSARCIterInfo::ObSSARCIterSegInfo &other)
{
  if (this != &other) {
    seg_cnt_ = other.seg_cnt_;
    op_info_ = other.op_info_;
  }
  return *this;
}

void ObSSARCIterInfo::ObSSARCIterSegInfo::init_iter_seg_info(
    const int64_t seg_cnt, 
    const bool to_delete, 
    const int64_t op_cnt)
{
  seg_cnt_ = seg_cnt;
  op_info_.set_op_info(to_delete, op_cnt);
}

void ObSSARCIterInfo::ObSSARCIterSegInfo::update_iter_seg_info(const int64_t actual_op_cnt, const int64_t exp_iter_cnt) 
{ 
  op_info_.update_op_info(actual_op_cnt, exp_iter_cnt); 
}

/*-----------------------------------------ObSSARCIterInfo------------------------------------------*/
ObSSARCIterInfo::ObSSARCIterInfo()
  : is_inited_(false), cmp_op_(), cold_micro_map_(), t1_micro_heap_(cmp_op_), b1_micro_heap_(cmp_op_), 
    t2_micro_heap_(cmp_op_), b2_micro_heap_(cmp_op_), micro_heap_arr_(), iter_seg_arr_(), allocator_()
{}

void ObSSARCIterInfo::destroy()
{
  reuse();
  cold_micro_map_.destroy();
  iter_seg_arr_.destroy();
  allocator_.clear();
  is_inited_ = false;
}

void ObSSARCIterInfo::reuse()
{
  for (int64_t seg_idx = 0; seg_idx < SS_ARC_SEG_COUNT; ++seg_idx) {
    reuse(seg_idx);
  }
}

void ObSSARCIterInfo::reuse(const int64_t seg_idx)
{
  if (!iter_seg_arr_.empty()) {
    iter_seg_arr_.at(seg_idx).reset();
  }
  switch (seg_idx) {
    case ARC_T1:
      t1_micro_heap_.reset();
      break;
    case ARC_B1:
      b1_micro_heap_.reset();
      break;
    case ARC_T2:
      t2_micro_heap_.reset();
      break;
    case ARC_B2:
      b2_micro_heap_.reset();
      break;
    default:
      break;
  }
  cold_micro_map_.clear();
  allocator_.clear();
}

int ObSSARCIterInfo::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(cold_micro_map_.create(SS_MAX_ARC_FETCH_CNT, ObMemAttr(tenant_id, "SSColdMap")))) {
    LOG_WARN("fail to create cold micro_block map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_iter_seg_info_arr())) {
    LOG_WARN("fail to init iter_seg_info_arr", KR(ret));
  } else {
    reuse();
    micro_heap_arr_[ARC_T1] = &t1_micro_heap_;
    micro_heap_arr_[ARC_B1] = &b1_micro_heap_;
    micro_heap_arr_[ARC_T2] = &t2_micro_heap_;
    micro_heap_arr_[ARC_B2] = &b2_micro_heap_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSARCIterInfo::init_iter_seg_info_arr()
{
  int ret = OB_SUCCESS;
  iter_seg_arr_.reset();
  ObSSARCIterSegInfo iter_seg_info;
  for (int64_t i = 0; OB_SUCC(ret) && (i < SS_ARC_SEG_COUNT); ++i) {
    if (OB_FAIL(iter_seg_arr_.push_back(iter_seg_info))) {
      LOG_WARN("fail to push back", KR(ret), K(i));
    }
  }
  return ret;
}

bool ObSSARCIterInfo::is_delete_op_type(const int64_t seg_idx) const
{
  bool b_ret = false;
  if (is_valid_arc_seg_idx(seg_idx)) {
    b_ret = iter_seg_arr_.at(seg_idx).is_delete_op();
  }
  return b_ret;
}

bool ObSSARCIterInfo::need_handle_arc_seg(const int64_t seg_idx) const
{
  bool b_ret = false;
  if (is_valid_arc_seg_idx(seg_idx)) {
    b_ret = (iter_seg_arr_.at(seg_idx).get_op_cnt() > 0);
  }
  return b_ret;
}

int64_t ObSSARCIterInfo::get_op_micro_cnt(const int64_t seg_idx) const
{
  int64_t exp_iter_cnt = 0;
  if (is_valid_arc_seg_idx(seg_idx)) {
    exp_iter_cnt = iter_seg_arr_.at(seg_idx).get_op_cnt();
  }
  return exp_iter_cnt;
}

int64_t ObSSARCIterInfo::get_expected_iter_micro_cnt(const int64_t seg_idx) const
{
  int64_t exp_iter_cnt = 0;
  if (is_valid_arc_seg_idx(seg_idx)) {
    exp_iter_cnt = iter_seg_arr_.at(seg_idx).get_exp_iter_cnt();
  }
  return exp_iter_cnt;
}

int64_t ObSSARCIterInfo::get_obtained_micro_cnt(const int64_t seg_idx) const
{
  int64_t obtained_cnt = 0;
  if (is_valid_arc_seg_idx(seg_idx)) {
    obtained_cnt = iter_seg_arr_.at(seg_idx).get_obtained_cnt();
  }
  return obtained_cnt;
}

void ObSSARCIterInfo::adjust_arc_iter_seg_info(const int64_t seg_idx)
{
  if (is_valid_arc_seg_idx(seg_idx)) {
    const int64_t cur_op_cnt = iter_seg_arr_.at(seg_idx).get_op_cnt();
    const int64_t real_op_cnt = MIN(cur_op_cnt, SS_MAX_ARC_HANDLE_OP_CNT);
    const int64_t need_iter_cnt = real_op_cnt * SS_MAX_ARC_FETCH_MULTIPLE;
    const int64_t exp_iter_cnt = MIN(iter_seg_arr_.at(seg_idx).get_seg_cnt(), need_iter_cnt);
    iter_seg_arr_.at(seg_idx).update_iter_seg_info(real_op_cnt, exp_iter_cnt);
  }
}

bool ObSSARCIterInfo::need_iterate_cold_micro(const int64_t seg_idx) const
{
  bool b_ret = false;
  if (is_valid_arc_seg_idx(seg_idx)) {
    b_ret = iter_seg_arr_.at(seg_idx).need_obtain_more_cnt();
  }
  return b_ret;
}

bool ObSSARCIterInfo::need_handle_cold_micro(const int64_t seg_idx) const
{
  bool b_ret = false;
  if (is_valid_arc_seg_idx(seg_idx)) {
    b_ret = ((!micro_heap_arr_[seg_idx]->empty()) && iter_seg_arr_.at(seg_idx).exist_seg_op());
  }
  return b_ret;
}

int ObSSARCIterInfo::get_cold_micro(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroMetaSnapshot &cold_micro) const
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaSnapshot* tmp_micro_ptr = nullptr;
  if (OB_FAIL(inner_get_cold_micro(micro_key, tmp_micro_ptr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_TRACE("not exist this micro in cold_micro_map", KR(ret), K(micro_key));
    } else {
      LOG_WARN("fail to get this cold_micro", KR(ret), K(micro_key));
    }
  } else {
    cold_micro = *tmp_micro_ptr;
  }
  return ret;
}

int ObSSARCIterInfo::exist_cold_micro(const ObSSMicroBlockCacheKey &micro_key, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaSnapshot* tmp_micro_ptr = nullptr;
  is_exist = false;
  if (OB_FAIL(inner_get_cold_micro(micro_key, tmp_micro_ptr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get this cold_micro", KR(ret), K(micro_key));
    }
  } else {
    is_exist = true;
  }
  return ret;
}

int ObSSARCIterInfo::inner_get_cold_micro(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroMetaSnapshot *&cold_micro) const
{
  int ret = OB_SUCCESS;
  cold_micro = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else if (OB_FAIL(cold_micro_map_.get_refactored(micro_key, cold_micro))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get refactored", KR(ret), K(micro_key));
    }
  } else if (OB_ISNULL(cold_micro)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("element in cold_micro_map is nullptr", KR(ret), K(cold_micro));
  }
  return ret;
}

int ObSSARCIterInfo::push_cold_micro(
    const ObSSMicroBlockCacheKey &micro_key, 
    const ObSSMicroMetaSnapshot &cold_micro,
    const int64_t seg_idx)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaSnapshot *cold_micro_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || !is_valid_arc_seg_idx(seg_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(cold_micro), K(seg_idx));
  } else if (OB_ISNULL(cold_micro_ptr = static_cast<ObSSMicroMetaSnapshot*>(allocator_.alloc(sizeof(ObSSMicroMetaSnapshot))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cold_micro", KR(ret), K(sizeof(ObSSMicroMetaSnapshot)));
  } else if (OB_FAIL(cold_micro_ptr->init(cold_micro.micro_meta_))) {
    LOG_WARN("fail to init cold_micro", KR(ret), K(cold_micro));
  }else if (OB_FAIL(cold_micro_map_.set_refactored(micro_key, cold_micro_ptr))) {
    LOG_WARN("fail to set refactored", KR(ret), K(micro_key), KPC(cold_micro_ptr));
  } else if (OB_FAIL(micro_heap_arr_[seg_idx]->push(cold_micro_ptr))) {
    LOG_WARN("fail to push into heap", KR(ret), K(micro_key), K(seg_idx), KPC(cold_micro_ptr));
  } else {
    iter_seg_arr_.at(seg_idx).inc_obtained_cnt();
  }
  return ret;
}

int ObSSARCIterInfo::pop_cold_micro(const int64_t seg_idx, ObSSMicroMetaSnapshot &cold_micro)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_arc_seg_idx(seg_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(seg_idx));
  } else if (need_handle_cold_micro(seg_idx)) {
    ObSSMicroMetaSnapshot *tmp_micro_ptr = micro_heap_arr_[seg_idx]->top();
    ObSSMicroBlockCacheKey tmp_micro_key;
    if (OB_ISNULL(tmp_micro_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cold_micro should not be null", KR(ret), K(seg_idx), K(tmp_micro_ptr), K(*this));
    } else if (FALSE_IT(tmp_micro_key = tmp_micro_ptr->micro_key())) {
    } else if (FALSE_IT(cold_micro = *tmp_micro_ptr)) {
    } else if (OB_FAIL(micro_heap_arr_[seg_idx]->pop())) {
      LOG_WARN("fail to pop from heap", KR(ret), K(seg_idx), K(*this));
    } else if (OB_FAIL(cold_micro_map_.erase_refactored(tmp_micro_key))) {
      LOG_WARN("fail to erase refactored", KR(ret), K(tmp_micro_key));
    } else {
      iter_seg_arr_.at(seg_idx).dec_obtained_cnt();
    }
  }
  return ret;
}

int ObSSARCIterInfo::finish_handle_cold_micro(const int64_t seg_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_arc_seg_idx(seg_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(seg_idx));
  } else if (OB_UNLIKELY(!iter_seg_arr_.at(seg_idx).is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid arc iter info", KR(ret), K(seg_idx), K(*this));
  } else {
    iter_seg_arr_.at(seg_idx).dec_op_cnt();
  }
  return ret;
}

/*-----------------------------------------ObSSARCInfo::ObSSARCSegInfo------------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSARCInfo::ObSSARCSegInfo, cnt_, size_);

ObSSARCInfo::ObSSARCSegInfo &ObSSARCInfo::ObSSARCSegInfo::operator=(const ObSSARCInfo::ObSSARCSegInfo &other)
{
  if (this != &other) {
    SpinRLockGuard r_guard(other.lock_);
    SpinWLockGuard w_guard(lock_);
    cnt_ = other.cnt_;
    size_ = other.size_;
  }
  return *this;
}

void ObSSARCInfo::ObSSARCSegInfo::reset()
{
  SpinWLockGuard guard(lock_);
  cnt_ = 0;
  size_ = 0;
}

int64_t ObSSARCInfo::ObSSARCSegInfo::avg_micro_size() const
{
  SpinRLockGuard guard(lock_);
  int64_t avg_size = 0;
  if (size_ > 0 && cnt_ > 0) {
    avg_size = size_ / cnt_;
  }
  return avg_size;
}

int64_t ObSSARCInfo::ObSSARCSegInfo::avg_micro_cnt(const int64_t total_size) const
{
  SpinRLockGuard guard(lock_);
  int64_t avg_cnt = 0;
  if (size_ > 0 && cnt_ > 0 && size_ > cnt_) {
    avg_cnt = total_size * cnt_ / size_;
  }
  return avg_cnt;
}

int64_t ObSSARCInfo::ObSSARCSegInfo::size() const
{
  SpinRLockGuard guard(lock_);
  return size_;
}

int64_t ObSSARCInfo::ObSSARCSegInfo::count() const
{
  SpinRLockGuard guard(lock_);
  return cnt_;
}

bool ObSSARCInfo::ObSSARCSegInfo::is_empty() const
{
  SpinRLockGuard guard(lock_);
  return (cnt_ <= 0) || (size_ <= 0) || (size_ < cnt_);
}

void ObSSARCInfo::ObSSARCSegInfo::get_seg_info(int64_t &size, int64_t &cnt) const
{
  SpinRLockGuard guard(lock_);
  size = size_;
  cnt = cnt_;
}

void ObSSARCInfo::ObSSARCSegInfo::update_seg_info(
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  SpinWLockGuard guard(lock_);
  size_ += delta_size;
  cnt_ += delta_cnt;
}

/*-----------------------------------------ObSSARCInfo------------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSARCInfo, limit_, work_limit_, max_p_, min_p_, p_, seg_info_arr_[ARC_T1], 
  seg_info_arr_[ARC_B1], seg_info_arr_[ARC_T2], seg_info_arr_[ARC_B2]);

ObSSARCInfo::ObSSARCInfo()
  : limit_(0), work_limit_(0), max_p_(0), min_p_(0), p_(0), seg_info_arr_(), micro_cnt_limit_(0), limit_lock_()
{}

ObSSARCInfo &ObSSARCInfo::operator=(const ObSSARCInfo &other)
{
  if (this != &other) {
    SpinRLockGuard r_guard(other.limit_lock_);
    SpinWLockGuard w_guard(limit_lock_);
    limit_ = other.limit_;
    work_limit_ = other.work_limit_;
    max_p_ = other.max_p_;
    min_p_ = other.min_p_;
    p_ = other.p_;
    micro_cnt_limit_ = other.micro_cnt_limit_;
    for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
      seg_info_arr_[i] = other.seg_info_arr_[i];
    }
  }
  return *this;
}

void ObSSARCInfo::reset()
{
  SpinWLockGuard guard(limit_lock_);
  limit_ = 0;
  work_limit_ = 0;
  max_p_ = 0;
  min_p_ = 0;
  p_ = 0;
  micro_cnt_limit_ = 0;
  reset_seg_info();
}

void ObSSARCInfo::reset_seg_info()
{
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    seg_info_arr_[i].reset();
  }
}

void ObSSARCInfo::reuse()
{
  SpinWLockGuard guard(limit_lock_);
  reset_seg_info();
  p_ = static_cast<int64_t>((static_cast<double>(limit_ * DEF_ARC_P_OF_LIMIT_PCT) / 100.0));
  work_limit_ = limit_;
}

bool ObSSARCInfo::is_valid() const
{
  SpinRLockGuard guard(limit_lock_);
  return (limit_ > 0 && micro_cnt_limit_ > 0);
}

void ObSSARCInfo::get_seg_info(
    const int64_t seg_idx,
    int64_t &size,
    int64_t &cnt) const
{
  if (is_valid_arc_seg_idx(seg_idx)) {
    seg_info_arr_[seg_idx].get_seg_info(size, cnt);
  }
}

int64_t ObSSARCInfo::get_l1_size() const
{ 
  return seg_info_arr_[ARC_T1].size() + seg_info_arr_[ARC_B1].size();
}

int64_t ObSSARCInfo::get_l2_size() const
{ 
  return seg_info_arr_[ARC_T2].size() + seg_info_arr_[ARC_B2].size(); 
}

int64_t ObSSARCInfo::get_valid_size() const
{
  return seg_info_arr_[ARC_T1].size() + seg_info_arr_[ARC_T2].size();
}

int64_t ObSSARCInfo::get_valid_count() const
{
  return seg_info_arr_[ARC_T1].count() + seg_info_arr_[ARC_T2].count();
}

int64_t ObSSARCInfo::get_ghost_count() const
{
  return seg_info_arr_[ARC_B1].count() + seg_info_arr_[ARC_B2].count();
}
  
int ObSSARCInfo::adjust_seg_info(
    const bool is_in_l1,
    const bool is_in_ghost,
    const ObSSARCOpType &op_type,
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(delta_size <= 0 || delta_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(delta_size), K(delta_cnt), K(is_in_l1), K(is_in_ghost));
  } else {
    switch (op_type) {
      case ObSSARCOpType::SS_ARC_NEW_ADD:
        if (OB_FAIL(adjust_seg_for_new_add(is_in_l1, is_in_ghost, delta_size, delta_cnt))) {
          LOG_WARN("fail to adjust seg info for new_add", KR(ret), K(is_in_l1), K(is_in_ghost), K(delta_size),
            K(delta_cnt));
        }
        break;
      case ObSSARCOpType::SS_ARC_HIT_T1:
        if (OB_FAIL(adjust_seg_for_hit_t1(is_in_l1, is_in_ghost, delta_size, delta_cnt))) {
          LOG_WARN("fail to adjust seg info for hit_t1", KR(ret), K(is_in_l1), K(is_in_ghost), K(delta_size),
            K(delta_cnt));
        }
        break;
      case ObSSARCOpType::SS_ARC_HIT_T2:
        // do nothing
        break;
      case ObSSARCOpType::SS_ARC_HIT_GHOST:
        if (OB_FAIL(adjust_seg_for_hit_ghost(is_in_l1, is_in_ghost, delta_size, delta_cnt))) {
          LOG_WARN("fail to adjust seg info for hit_ghost", KR(ret), K(is_in_l1), K(is_in_ghost), K(delta_size),
            K(delta_cnt));
        }
        break;
      case ObSSARCOpType::SS_ARC_TASK_EVICT_OP:
        if (OB_FAIL(adjust_seg_for_evict(is_in_l1, is_in_ghost, delta_size, delta_cnt))) {
          LOG_WARN("fail to adjust seg info for evict_op", KR(ret), K(is_in_l1), K(is_in_ghost), K(delta_size),
            K(delta_cnt));
        }
        break;
      case ObSSARCOpType::SS_ARC_TASK_DELETE_OP:
        if (OB_FAIL(adjust_seg_for_delete(is_in_l1, is_in_ghost, delta_size, delta_cnt))) {
          LOG_WARN("fail to adjust seg info for delete_op", KR(ret), K(is_in_l1), K(is_in_ghost), K(delta_size),
            K(delta_cnt));
        }
        break;
      case ObSSARCOpType::SS_ARC_ABNORMAL_DELETE_OP:
        if (OB_FAIL(adjust_seg_for_abnormal_delete(is_in_l1, is_in_ghost, delta_size, delta_cnt))) {
          LOG_WARN("fail to adjust seg info for abnormal delete_op", KR(ret), K(is_in_l1), K(is_in_ghost), K(delta_size),
            K(delta_cnt));
        }
        break;
      case ObSSARCOpType::SS_ARC_EXPIRED_DELETE_OP:
        if (OB_FAIL(adjust_seg_for_expired_delete(is_in_l1, is_in_ghost, delta_size, delta_cnt))) {
          LOG_WARN("fail to adjust seg info for expired delete_op", KR(ret), K(is_in_l1), K(is_in_ghost), K(delta_size),
            K(delta_cnt));
        }
        break;
      case ObSSARCOpType::SS_ARC_INVALIDATE_OP:
        if (OB_FAIL(adjust_seg_for_invalidate(is_in_l1, is_in_ghost, delta_size, delta_cnt))) {
          LOG_WARN("fail to adjust seg info for invalidate_op", KR(ret), K(is_in_l1), K(is_in_ghost), K(delta_size),
            K(delta_cnt));
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(op_type));
    }
  }
  return ret;
}

void ObSSARCInfo::update_seg_info(
    const bool is_in_l1, 
    const bool is_in_ghost,
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  const int64_t seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
  seg_info_arr_[seg_idx].update_seg_info(delta_size, delta_cnt);
}

void ObSSARCInfo::set_micro_cnt_limit(const int64_t micro_cnt_limit)
{
  SpinWLockGuard guard(limit_lock_);
  micro_cnt_limit_ = micro_cnt_limit;
}

int64_t ObSSARCInfo::get_arc_work_limit() const
{
  SpinRLockGuard guard(limit_lock_);
  return work_limit_;
}

void ObSSARCInfo::update_arc_limit(const int64_t new_limit)
{
  SpinWLockGuard guard(limit_lock_);
  if (new_limit != limit_) {
    do_update_arc_limit(new_limit, /* need_update_limit */ true);
  }
}

void ObSSARCInfo::do_update_arc_limit(const int64_t new_limit_val, const bool need_update_limit)
{
  if (new_limit_val >= 0) {
    const int64_t ori_limit = limit_;
    const int64_t ori_work_limit = work_limit_;
    const int64_t ori_p = p_;
    if (need_update_limit) { // update limit && work_limit
      if (0 == limit_ || 0 == work_limit_) {
        limit_ = new_limit_val;
        work_limit_ = new_limit_val;
        p_ = work_limit_ * DEF_ARC_P_OF_LIMIT_PCT / 100;
      } else if (limit_ == work_limit_) {
        const double ori_pct = static_cast<double>(p_ * 100) / work_limit_;
        limit_ = new_limit_val;
        work_limit_ = new_limit_val;
        p_ = static_cast<int64_t>((static_cast<double>(work_limit_ * ori_pct) / 100.0));
      } else {  // limit_ != work_limit_
        const double ori_pct = static_cast<double>(p_ * 100) / work_limit_;
        const double ratio = static_cast<double>(work_limit_) / limit_;
        limit_ = new_limit_val;
        work_limit_ = static_cast<int64_t>(ratio * new_limit_val);
        p_ = static_cast<int64_t>((static_cast<double>(work_limit_ * ori_pct) / 100.0));
      }
    } else { // only update work_limit
      if (0 == work_limit_) {
        work_limit_ = new_limit_val;
        p_ = static_cast<int64_t>((static_cast<double>(work_limit_ * DEF_ARC_P_OF_LIMIT_PCT) / 100.0));
      } else {
        const double ori_pct = static_cast<double>(p_ * 100) / work_limit_;
        work_limit_ = new_limit_val;
        p_ = static_cast<int64_t>((static_cast<double>(work_limit_ * ori_pct) / 100.0));
      }
    }
    max_p_ = work_limit_ * MAX_ARC_P_OF_LIMIT_PCT / 100;
    min_p_ = work_limit_ * MIN_ARC_P_OF_LIMIT_PCT / 100;

    FLOG_INFO("finish to update arc_limit", K(new_limit_val), K(need_update_limit), 
        K(ori_limit), K(ori_work_limit), K(ori_p), K(*this));
  }
}

void ObSSARCInfo::dec_arc_work_limit_for_prewarm()
{
  SpinWLockGuard guard(limit_lock_);
  const int64_t new_work_limit = static_cast<int64_t>((static_cast<double>(limit_ * SS_ARC_LIMIT_SHRINK_PCT) / 100.0));
  do_update_arc_limit(new_work_limit, /* need_update_limit */ false);
}

void ObSSARCInfo::inc_arc_work_limit_for_prewarm()
{
  SpinWLockGuard guard(limit_lock_);
  const int64_t new_work_limit = limit_;
  do_update_arc_limit(new_work_limit, /* need_update_limit */ false);
}

int ObSSARCInfo::adjust_seg_for_new_add(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  // must in T1, cuz new added micro_block is in T1
  if (OB_UNLIKELY(!(is_in_l1 && !is_in_ghost))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_in_l1), K(is_in_ghost));
  } else {
    seg_info_arr_[ARC_T1].update_seg_info(delta_size, delta_cnt);
  }
  return ret;
}

int ObSSARCInfo::adjust_seg_for_hit_t1(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  // must in T1, thus can transfer T1 -> T2
  if (OB_UNLIKELY(!(is_in_l1 && !is_in_ghost))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_in_l1), K(is_in_ghost));
  } else {
    seg_info_arr_[ARC_T1].update_seg_info(delta_size * -1, delta_cnt * -1);
    seg_info_arr_[ARC_T2].update_seg_info(delta_size, delta_cnt);
  }
  return ret;
}

int ObSSARCInfo::adjust_seg_for_hit_ghost(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  // must in B1/B2, thus can thansfer B1/B2 -> T2
  if (OB_UNLIKELY(!is_in_ghost)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_in_l1), K(is_in_ghost));
  } else {
    const int64_t seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
    const int64_t delta_p = (ARC_B1 == seg_idx) ? delta_size : (-1 * delta_size);
    try_update_p(delta_p);
    seg_info_arr_[seg_idx].update_seg_info(delta_size * -1, delta_cnt * -1);
    seg_info_arr_[ARC_T2].update_seg_info(delta_size, delta_cnt);
  }
  return ret;
}

void ObSSARCInfo::try_update_p(const int64_t delta_p)
{
  if (delta_p > 0) {
    if (ATOMIC_AAF(&p_, delta_p) > ATOMIC_LOAD(&max_p_)) {
      ATOMIC_AAF(&p_, delta_p * -1);
    }
  } else {
    if (ATOMIC_AAF(&p_, delta_p) < ATOMIC_LOAD(&min_p_)) {
      ATOMIC_AAF(&p_, delta_p * -1);
    }
  }
}

int ObSSARCInfo::adjust_seg_for_evict(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  // must in T1/T2, thus can transfer T1/T2 -> B1/B2
  if (OB_UNLIKELY(is_in_ghost)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_in_l1), K(is_in_ghost));
  } else {
    const int64_t seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
    seg_info_arr_[seg_idx].update_seg_info(delta_size * -1, delta_cnt * -1);
    seg_info_arr_[seg_idx + 1].update_seg_info(delta_size, delta_cnt);
  }
  return ret;
}

int ObSSARCInfo::adjust_seg_for_delete(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  // must in T1/B1/B2
  if (OB_UNLIKELY((!is_in_l1 && !is_in_ghost))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_in_l1), K(is_in_ghost));
  } else {
    const int64_t seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
    seg_info_arr_[seg_idx].update_seg_info(delta_size * -1, delta_cnt * -1);
  }
  return ret;
}

int ObSSARCInfo::adjust_seg_for_abnormal_delete(
    const bool is_in_l1,
    const bool is_in_ghost,
    const int64_t delta_size,
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
  seg_info_arr_[seg_idx].update_seg_info(delta_size * -1, delta_cnt * -1);
  return ret;
}

int ObSSARCInfo::adjust_seg_for_expired_delete(
    const bool is_in_l1,
    const bool is_in_ghost,
    const int64_t delta_size,
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
  seg_info_arr_[seg_idx].update_seg_info(delta_size * -1, delta_cnt * -1);
  return ret;
}

// Differ from evict, it can invalidate micro_meta which is in mem_blk
int ObSSARCInfo::adjust_seg_for_invalidate(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const int64_t delta_size, 
    const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_in_ghost)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_in_l1), K(is_in_ghost));
  } else {
    const int64_t seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
    seg_info_arr_[seg_idx].update_seg_info(delta_size * -1, delta_cnt * -1);
    seg_info_arr_[seg_idx + 1].update_seg_info(delta_size, delta_cnt);
  }
  return ret;
}

bool ObSSARCInfo::trigger_eviction() const
{
  bool b_ret = false;
  int64_t work_limit = 0;
  int64_t micro_cnt_limit = 0;
  {
    SpinRLockGuard guard(limit_lock_);
    work_limit = work_limit_;
    micro_cnt_limit = micro_cnt_limit_;
  }

  const int64_t valid_size = get_valid_size(); // T1 + t2
  b_ret = (valid_size - work_limit > 0);
  if (!b_ret) { // T1 + B1
    b_ret = (get_l1_size() - work_limit > 0);
  }
  if (!b_ret) { // T2 + B2
    b_ret = (get_l2_size() - work_limit > 0);
  }
  if (!b_ret) { // check memory
    b_ret = (calc_exceed_micro_cnt_by_mem(micro_cnt_limit) > 0);
  }
  return b_ret;
}

// Check whether the eviction is almost to be triggered due to the data size
bool ObSSARCInfo::close_to_evict() const
{
  bool b_ret = false;
  int64_t check_limit = 0;
  {
    SpinRLockGuard guard(limit_lock_);
    check_limit = MAX(0, work_limit_ - SS_CLOSE_EVICTION_DIFF_SIZE);
  }
  if (check_limit > 0) {
    b_ret = (get_valid_size() >= check_limit);
  }
  return b_ret;
}

void ObSSARCInfo::calc_arc_iter_info(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx) const
{
  if (OB_LIKELY(is_valid_arc_seg_idx(seg_idx))) {
    bool reach_limit = false;
    calc_arc_iter_info_by_disk(arc_iter_info, seg_idx, reach_limit);
    if (!reach_limit) {
      calc_arc_iter_info_by_mem(arc_iter_info, seg_idx, reach_limit);
    }
  }
}

void ObSSARCInfo::calc_arc_iter_info_by_disk(
    ObSSARCIterInfo &arc_iter_info, 
    const int64_t seg_idx,
    bool &reach_limit) const
{
  reach_limit = false;
  int64_t cur_work_limit = 0;
  {
    SpinRLockGuard guard(limit_lock_);
    cur_work_limit = work_limit_;
  }
  if (ARC_T1 == seg_idx || ARC_T2 == seg_idx) { // calc T1/T2 evict size
    const int64_t t1_size = seg_info_arr_[ARC_T1].size();
    const int64_t t2_size = seg_info_arr_[ARC_T2].size();
    const int64_t exceed_valid_size = (t1_size + t2_size) - cur_work_limit;
    if (exceed_valid_size > 0) {
      if (0 == cur_work_limit) {
        const int64_t total_cnt = seg_info_arr_[seg_idx].count();
        arc_iter_info.iter_seg_arr_[seg_idx].init_iter_seg_info(total_cnt, false, total_cnt);
      } else if (0 < cur_work_limit) {
        int64_t t1_delta_size = 0;
        int64_t t2_delta_size = 0;
        calc_arc_seg_delta_size(t1_size, t2_size, cur_work_limit, exceed_valid_size,
          t1_delta_size, t2_delta_size);
        if (ARC_T1 == seg_idx) {
          calc_arc_seg_iter_info(arc_iter_info, ARC_T1, t1_delta_size, false/*to_delete*/);
        } else {
          calc_arc_seg_iter_info(arc_iter_info, ARC_T2, t2_delta_size, false/*to_delete*/);
        }
      }
      reach_limit = true;
    }
  } else if (ARC_B1 == seg_idx) { // calc B1 evict size
    const int64_t exceed_l1_size = get_l1_size() - cur_work_limit;
    if (exceed_l1_size > 0 && !seg_info_arr_[ARC_B1].is_empty()) {
      calc_arc_seg_iter_info(arc_iter_info, ARC_B1, exceed_l1_size, true/*to_delete*/);
      reach_limit = true;
    }
  } else if (ARC_B2 == seg_idx) { // calc B2 evict size
    const int64_t exceed_l2_size = get_l2_size() - cur_work_limit;
    if (exceed_l2_size > 0 && !seg_info_arr_[ARC_B2].is_empty()) {
      calc_arc_seg_iter_info(arc_iter_info, ARC_B2, exceed_l2_size, true/*to_delete*/);
      reach_limit = true;
    }
  }
}

void ObSSARCInfo::calc_arc_iter_info_by_mem(
    ObSSARCIterInfo &arc_iter_info, 
    const int64_t seg_idx,
    bool &reach_limit) const
{
  int64_t cur_micro_cnt_limit = 0;
  {
    SpinRLockGuard guard(limit_lock_);
    cur_micro_cnt_limit = micro_cnt_limit_;
  }
  const int64_t exceed_cnt = calc_exceed_micro_cnt_by_mem(cur_micro_cnt_limit);
  if (exceed_cnt > 0) {
    const int64_t b1_cnt = seg_info_arr_[ARC_B1].count();
    const int64_t b2_cnt = seg_info_arr_[ARC_B2].count();
    if (ARC_T1 == seg_idx || ARC_T2 == seg_idx) {
      if (b1_cnt + b2_cnt >= exceed_cnt) {
        // skip, just need to delete b1/b2
      } else {
        const int64_t t1_cnt = seg_info_arr_[ARC_T1].count();
        const int64_t t2_cnt = seg_info_arr_[ARC_T2].count();
        const int64_t t1_delta_cnt = MIN(exceed_cnt - b1_cnt - b2_cnt, t1_cnt);
        const int64_t t2_delta_cnt = MIN(exceed_cnt - b1_cnt - b2_cnt - t1_delta_cnt, t2_cnt);
        if (ARC_T1 == seg_idx) {
          if (t1_delta_cnt > 0) {
            arc_iter_info.iter_seg_arr_[seg_idx].init_iter_seg_info(t1_cnt, false, t1_delta_cnt);
          }
        } else {
          if (t2_delta_cnt > 0) {
            arc_iter_info.iter_seg_arr_[seg_idx].init_iter_seg_info(t2_cnt, false, t2_delta_cnt);
          }
        }
      }
    } else if (ARC_B1 == seg_idx) {
      arc_iter_info.iter_seg_arr_[seg_idx].init_iter_seg_info(b1_cnt, true, MIN(b1_cnt, exceed_cnt));
    } else if (ARC_B2 == seg_idx) {
      arc_iter_info.iter_seg_arr_[seg_idx].init_iter_seg_info(b2_cnt, true, MIN(b2_cnt, exceed_cnt));
    }
    reach_limit = true;
  }
}

void ObSSARCInfo::calc_arc_seg_delta_size(
    const int64_t t1_size,
    const int64_t t2_size,
    const int64_t work_limit,
    const int64_t total_delta_size,
    int64_t &t1_delta_size, 
    int64_t &t2_delta_size) const
{
  if (work_limit > 0) {
    // TODO @donglou.zl later we will determine which way is better
    const bool USE_ORI_WAY = true;
    if (USE_ORI_WAY) {
      // 1. Evict T1/T2 based on propotion
      const int64_t cur_t1_pct = ATOMIC_LOAD(&p_) * 100 / work_limit;
      const int64_t cur_t2_pct = 100 - cur_t1_pct;
      t1_delta_size = (t1_size * cur_t2_pct + (total_delta_size - t2_size) * cur_t1_pct) / 100;
      if (t1_delta_size < 0) {
        t1_delta_size = 0;
      } else if (t1_delta_size > total_delta_size) {
        t1_delta_size = total_delta_size;
      }
      t2_delta_size = total_delta_size - t1_delta_size;
    } else {
      // 2. Evict T1/T2 based on comparison result with p_
      const int64_t cur_p = ATOMIC_LOAD(&p_);
      t1_delta_size = MAX(t1_size - p_, 0);
      t2_delta_size = MAX(total_delta_size - t1_delta_size, 0);
    }
  }
}

void ObSSARCInfo::calc_arc_seg_iter_info(
    ObSSARCIterInfo &arc_iter_info, 
    const int64_t seg_idx, 
    const int64_t exceed_size,
    const bool to_delete) const
{
  if (OB_LIKELY(is_valid_arc_seg_idx(seg_idx))) {
    const int64_t total_cnt = seg_info_arr_[seg_idx].count();
    const int64_t op_cnt = MIN(total_cnt, seg_info_arr_[seg_idx].avg_micro_cnt(exceed_size));
    arc_iter_info.iter_seg_arr_[seg_idx].init_iter_seg_info(total_cnt, to_delete, op_cnt);
  }
}

int64_t ObSSARCInfo::calc_exceed_micro_cnt_by_mem(const int64_t micro_cnt_limit) const
{
  int64_t exceed_cnt = 0;
  if (micro_cnt_limit > 0) {
    const int64_t cur_micro_cnt = get_valid_count() + get_ghost_count();
    exceed_cnt = cur_micro_cnt - (micro_cnt_limit * MAX_MEM_LIMIT_USAGE_PCT / 100);
  }
  return exceed_cnt;
}
} /* namespace storage */
} /* namespace oceanbase */
