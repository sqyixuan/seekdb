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

#include "ob_ss_micro_cache_basic_op.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"

namespace oceanbase 
{
namespace storage 
{
/*-----------------------------------------SSMicroMapGetMicroHandleFunc-----------------------------------------*/
SSMicroMapGetMicroHandleFunc::SSMicroMapGetMicroHandleFunc(
    ObSSMicroBlockMetaHandle &micro_meta_handle,
    ObSSMemBlockHandle &mem_blk_handle,
    ObSSPhysicalBlockHandle &phy_blk_handle,
    const bool update_arc)
  : ret_(OB_SUCCESS), update_arc_(update_arc), arc_op_type_(ObSSARCOpType::SS_ARC_INVALID_OP), 
    micro_info_(), micro_meta_handle_(micro_meta_handle), mem_blk_handle_(mem_blk_handle), 
    phy_blk_handle_(phy_blk_handle)
{}

bool SSMicroMapGetMicroHandleFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    micro_meta_handle_.set_ptr(micro_meta_handle.get_ptr());
    micro_meta_handle()->get_micro_base_info(micro_info_);

    if (!micro_meta_handle()->is_valid_field()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (micro_meta_handle()->is_persisted()) {
      const uint64_t data_dest = micro_meta_handle()->data_dest();
      const uint64_t reuse_version = micro_meta_handle()->reuse_version();
      if (OB_FAIL(micro_meta_handle()->is_in_ghost())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta is in unexpected state", KR(ret), KPC(micro_meta_handle.get_ptr()));
      } else if (OB_FAIL(SSPhyBlockMgr.get_block_handle(data_dest, reuse_version, phy_blk_handle_))) {
        if (OB_S2_REUSE_VERSION_MISMATCH == ret) { // reuse_version mismatch, treat it as NOT_EXIST
          micro_meta_handle()->mark_invalid();
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("fail to get phy_block handle", KR(ret), KPC(micro_key), KPC(micro_meta_handle.get_ptr()));
        }
      }
    } else {
      ObSSMemBlock *mem_blk = micro_meta_handle()->get_mem_block();
      if (OB_ISNULL(mem_blk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mem_block should not be null", KR(ret), KPC(micro_meta_handle.get_ptr()));
      } else {
        mem_blk_handle_.set_ptr(mem_blk);
        if (!mem_blk_handle_()->is_reuse_version_match(micro_meta_handle()->reuse_version_)) {
          micro_meta_handle()->mark_invalid();
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
    }
    
    if (OB_SUCC(ret)) {
      if (update_arc_) {
        if (micro_meta_handle()->is_in_l1()) { // Hit T1
          arc_op_type_ = ObSSARCOpType::SS_ARC_HIT_T1;
          micro_meta_handle()->transfer_arc_seg(arc_op_type_);
        } else {                     // Hit T2 (only update access_time)
          arc_op_type_ = ObSSARCOpType::SS_ARC_HIT_T2;
          micro_meta_handle()->transfer_arc_seg(arc_op_type_);
        }
      }
    } else {
      micro_meta_handle_.reset();
      mem_blk_handle_.reset();
      phy_blk_handle_.reset();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapUpdateMicroFunc-----------------------------------------*/
SSMicroMapUpdateMicroFunc::SSMicroMapUpdateMicroFunc(
    const int32_t size, const ObSSMemBlockHandle &mem_blk_handle, const uint32_t crc)
    : ret_(OB_SUCCESS), is_updated_(false), validate_ghost_micro_(false), is_in_l1_(true), is_in_ghost_(false),
      size_(size), crc_(crc), mem_blk_handle_(mem_blk_handle), arc_op_type_(ObSSARCOpType::SS_ARC_INVALID_OP)
{}

bool SSMicroMapUpdateMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (OB_UNLIKELY(!mem_blk_handle_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KPC(micro_key), K_(mem_blk_handle));
    } else {
      is_in_l1_ = micro_meta_handle()->is_in_l1();    
      is_in_ghost_ = micro_meta_handle()->is_in_ghost();

      // If micro_meta exists, we treat it as 'hit-again'
      if (micro_meta_handle()->is_in_ghost()) {
        arc_op_type_ = ObSSARCOpType::SS_ARC_HIT_GHOST;
      } else if (micro_meta_handle()->is_in_l1()) {
        arc_op_type_ = ObSSARCOpType::SS_ARC_HIT_T1;
      } else {
        arc_op_type_ = ObSSARCOpType::SS_ARC_HIT_T2;
      }

      // check current state of micro_meta, if it is invalid, validate it.
      if (!micro_meta_handle()->is_valid()) {
        if (OB_UNLIKELY(!(mem_blk_handle_()->exist(*micro_key)))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid mem_blk cuz not contains this micro", KR(ret), KPC(micro_key), K_(mem_blk_handle));
        } else if (OB_FAIL(micro_meta_handle()->validate_micro_meta(*micro_key, mem_blk_handle_, size_, crc_))) {
          LOG_WARN("fail to validate micro_meta", KR(ret), KPC(micro_key), K_(size), K_(crc), K_(mem_blk_handle));
        } else {
          validate_ghost_micro_ = true;
          micro_meta_handle()->transfer_arc_seg(arc_op_type_);
        }
      } else {
        micro_meta_handle()->transfer_arc_seg(arc_op_type_);
      }

      if (OB_SUCC(ret)) {
        is_updated_ = true;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapUpdateMicroHeatFunc-----------------------------------------*/
SSMicroMapUpdateMicroHeatFunc::SSMicroMapUpdateMicroHeatFunc(
  const bool transfer_seg, const bool update_access_time, const int64_t time_delta_s)
  : ret_(OB_SUCCESS), transfer_seg_(transfer_seg), update_access_time_(update_access_time), time_delta_s_(time_delta_s),
    size_(0), arc_op_type_(ObSSARCOpType::SS_ARC_INVALID_OP)
{}

bool SSMicroMapUpdateMicroHeatFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    arc_op_type_ = ObSSARCOpType::SS_ARC_INVALID_OP;
    if (transfer_seg_ && micro_meta_handle()->is_in_l1() && !micro_meta_handle()->is_in_ghost()) {
      if (micro_meta_handle()->is_valid()) {
        micro_meta_handle()->is_in_l1_ = false;
        size_ = micro_meta_handle()->length();
        arc_op_type_ = ObSSARCOpType::SS_ARC_HIT_T1;
      }
    }
    if (update_access_time_) {
      micro_meta_handle()->update_access_time();
    }
    micro_meta_handle()->update_access_time(time_delta_s_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key or micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapUpdateMicroDestFunc-----------------------------------------*/
SSMicroMapUpdateMicroDestFunc::SSMicroMapUpdateMicroDestFunc(
    const int64_t data_dest, 
    const uint32_t reuse_version,
    ObSSMemBlockHandle &mem_blk_handle)
  : ret_(OB_SUCCESS), data_dest_(data_dest), reuse_version_(reuse_version), mem_blk_handle_(mem_blk_handle),
    succ_update_(false)
{}

bool SSMicroMapUpdateMicroDestFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (OB_UNLIKELY(!mem_blk_handle_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K_(mem_blk_handle));
    } else if (OB_UNLIKELY(!micro_meta_handle()->is_valid_field())) {
      // If call clear_tablet_micro_meta(), micro_meta will be marked invalid, skip it.
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_LIKELY(micro_meta_handle()->is_valid(mem_blk_handle_))) {
      micro_meta_handle()->data_dest_ = data_dest_;
      micro_meta_handle()->reuse_version_ = reuse_version_;
      micro_meta_handle()->is_persisted_ = true;
      succ_update_ = true;
    } else {
      if (OB_LIKELY(micro_meta_handle()->is_persisted())) {
        LOG_INFO("duplicate micro in this mem_blk, skip it", KPC(micro_meta_handle.get_ptr()), K_(mem_blk_handle));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta should be valid", KR(ret), KPC(micro_meta_handle.get_ptr()), KPC(mem_blk_handle_.get_ptr()));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapEvictMicroFunc-----------------------------------------*/
SSMicroMapEvictMicroFunc::SSMicroMapEvictMicroFunc(const ObSSMicroMetaSnapshot &cold_micro)
  : ret_(OB_SUCCESS), cold_micro_(cold_micro)
{}

bool SSMicroMapEvictMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cold_micro_.can_evict())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(cold_micro));
  } else if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (OB_UNLIKELY(!((*(micro_meta_handle.get_ptr())) == cold_micro_.micro_meta_))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("can't evict micro_meta cuz meta changed", KPC(micro_meta_handle.get_ptr()), K_(cold_micro));
    } else {
      micro_meta_handle()->transfer_arc_seg(ObSSARCOpType::SS_ARC_TASK_EVICT_OP);
      micro_meta_handle()->mark_invalid();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapDeleteMicroFunc-----------------------------------------*/
SSMicroMapDeleteMicroFunc::SSMicroMapDeleteMicroFunc(const ObSSMicroMetaSnapshot &cold_micro)
 : cold_micro_(cold_micro)
{}

bool SSMicroMapDeleteMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool can_delete = false;
  int ret = OB_SUCCESS;
  
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
      // need to update phy_blk len when delete from T1
      if (OB_LIKELY(cold_micro_.can_delete())) {
        can_delete = (*(micro_meta_handle.get_ptr()) == cold_micro_.micro_meta_);
      }
  } else {
    can_delete = true;
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }
  return can_delete;
}

/*-----------------------------------------SSMicroMapInvalidateMicroFunc-----------------------------------------*/
SSMicroMapInvalidateMicroFunc::SSMicroMapInvalidateMicroFunc(ObSSMicroBaseInfo &micro_info)
  : ret_(OB_SUCCESS), succ_invalidate_(false), micro_info_(micro_info)
{}

bool SSMicroMapInvalidateMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    micro_info_.is_in_l1_ = micro_meta_handle()->is_in_l1();
    micro_info_.is_in_ghost_ = micro_meta_handle()->is_in_ghost();
    micro_info_.size_ = micro_meta_handle()->length();
    micro_info_.reuse_version_ = micro_meta_handle()->reuse_version();
    micro_info_.is_persisted_ = micro_meta_handle()->is_persisted();
    if (micro_info_.is_persisted_) {
      micro_info_.data_dest_ = micro_meta_handle()->data_dest();
    }

    if (micro_meta_handle()->is_in_ghost() || !micro_meta_handle()->is_valid_field()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (micro_meta_handle()->is_reorganizing()) {
      // skip
    } else if (OB_LIKELY(micro_meta_handle()->is_valid())) {
      micro_meta_handle()->is_in_ghost_ = true;
      micro_meta_handle()->mark_invalid();
      succ_invalidate_ = true;
    } else {
      LOG_ERROR("exist T1/T2 invalid micro_meta", KPC(micro_meta_handle.get_ptr()));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapDeleteUnpersistedMicroFunc-----------------------------------------*/
SSMicroMapDeleteUnpersistedMicroFunc::SSMicroMapDeleteUnpersistedMicroFunc(
  ObSSMemBlockHandle &mem_blk_handle)
 : is_in_l1_(true), is_in_ghost_(false), size_(0), mem_blk_handle_(mem_blk_handle)
{}

bool SSMicroMapDeleteUnpersistedMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key,
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool can_delete = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (!micro_meta_handle()->is_persisted() && (micro_meta_handle()->get_mem_block() == mem_blk_handle_.get_ptr())) {
      is_in_l1_ = micro_meta_handle()->is_in_l1();
      is_in_ghost_ = micro_meta_handle()->is_in_ghost();
      size_ = micro_meta_handle()->length();
      can_delete = true;
    }
  } else {
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }
  return can_delete;
}

/*-----------------------------------------SSMicroMapDeleteInvalidMicroFunc-----------------------------------------*/
SSMicroMapDeleteInvalidMicroFunc::SSMicroMapDeleteInvalidMicroFunc()
  : is_in_l1_(true), is_in_ghost_(false), size_(0)
{}

bool SSMicroMapDeleteInvalidMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool can_delete = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (OB_LIKELY(!micro_meta_handle()->is_in_ghost() && !micro_meta_handle()->is_valid())) {
      is_in_l1_ = micro_meta_handle()->is_in_l1();
      is_in_ghost_ = micro_meta_handle()->is_in_ghost();
      size_ = micro_meta_handle()->length();
      can_delete = true;
    }
  } else {
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }
  return can_delete;
}

/*-----------------------------------------SSMicroMapDeleteExpiredMicroFunc-----------------------------------------*/
SSMicroMapDeleteExpiredMicroFunc::SSMicroMapDeleteExpiredMicroFunc(
  const int64_t expiration_time, ObSSMicroBaseInfo &expired_micro_info)
  : expiration_time_(expiration_time), expired_micro_info_(expired_micro_info)
{}

bool SSMicroMapDeleteExpiredMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool can_delete = false;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (OB_LIKELY((micro_meta_handle()->is_in_ghost() || micro_meta_handle()->is_valid()) && 
        micro_meta_handle()->is_expired(expiration_time_))) {
      expired_micro_info_.is_in_l1_ = micro_meta_handle()->is_in_l1();
      expired_micro_info_.is_in_ghost_ = micro_meta_handle()->is_in_ghost();
      expired_micro_info_.size_ = micro_meta_handle()->length();
      expired_micro_info_.is_persisted_ = micro_meta_handle()->is_persisted();
      expired_micro_info_.reuse_version_ = micro_meta_handle()->reuse_version();
      if (expired_micro_info_.is_persisted_) {
        expired_micro_info_.data_dest_ = micro_meta_handle()->data_dest();
      }
      can_delete = true;
    }
  } else {
    LOG_WARN("micro_key or micro_meta should not be null", K(micro_key), K(micro_meta_handle));
  }
  return can_delete;
}

/*-----------------------------------------SSMicroMapTryReorganizeMicroFunc-----------------------------------------*/
SSMicroMapTryReorganizeMicroFunc::SSMicroMapTryReorganizeMicroFunc(
  const int64_t data_dest, ObSSMicroBlockMetaHandle &micro_meta_handle)
  : ret_(OB_SUCCESS), data_dest_(data_dest), micro_meta_handle_(micro_meta_handle)
{}

bool SSMicroMapTryReorganizeMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (OB_UNLIKELY(micro_meta_handle_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K_(micro_meta_handle));
    } else if (!(micro_meta_handle()->can_reorganize()) || (data_dest_ != (micro_meta_handle()->data_dest()))) {
      // if this micro_meta can't be reorganized or data_dest is not consistent, also treat it as not exist.
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("can't set is_reorganizing=true", KR(ret), K_(data_dest), KPC(micro_meta_handle.get_ptr()));
    } else {
      micro_meta_handle_.set_ptr(micro_meta_handle.get_ptr());
      micro_meta_handle()->is_reorganizing_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapCompleteReorganizingMicroFunc-----------------------------------------*/
SSMicroMapCompleteReorganizingMicroFunc::SSMicroMapCompleteReorganizingMicroFunc(ObSSMemBlockHandle &mem_blk_handle)
  : ret_(OB_SUCCESS), mem_blk_handle_(mem_blk_handle)
{}

bool SSMicroMapCompleteReorganizingMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    if (OB_UNLIKELY(!mem_blk_handle_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K_(mem_blk_handle));
    } else if (OB_UNLIKELY(!(micro_meta_handle()->check_reorganizing_state()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_meta should be in reorganizing state", KR(ret), KPC(micro_meta_handle.get_ptr()));
    } else if (OB_FAIL(micro_meta_handle()->update_reorganizing_state(mem_blk_handle_))) {
      LOG_WARN("fail to complete reorganizing micro_block's state", KR(ret), KPC(micro_meta_handle.get_ptr()), K_(mem_blk_handle));
    } else {
      mem_blk_handle_()->add_valid_micro_block(micro_meta_handle()->length());
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

/*-----------------------------------------SSMicroMapUnmarkReorganizingMicroFunc-----------------------------------------*/
SSMicroMapUnmarkReorganizingMicroFunc::SSMicroMapUnmarkReorganizingMicroFunc()
  : ret_(OB_SUCCESS)
{}

bool SSMicroMapUnmarkReorganizingMicroFunc::operator()(
    const ObSSMicroBlockCacheKey *micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  bool b_ret = true;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(micro_key) && micro_meta_handle.is_valid()) {
    micro_meta_handle()->is_reorganizing_ = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_key and micro_meta should not be null", KR(ret), K(micro_key), K(micro_meta_handle));
  }

  ret_ = ret;
  if (OB_FAIL(ret)) {
    b_ret = false;
  }
  return b_ret;
}

} /* namespace storage */
} /* namespace oceanbase */
