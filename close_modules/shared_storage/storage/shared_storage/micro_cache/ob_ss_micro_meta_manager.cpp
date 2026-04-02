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
#include "ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "share/ob_force_print_log.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::blocksstable;

/*-----------------------------------------SSReplayMicroCkptCtx------------------------------------------*/
void SSReplayMicroCkptCtx::reset()
{
  total_replay_cnt_ = 0;
  invalid_t1_micro_cnt_ = 0;
  invalid_t2_micro_cnt_ = 0;
  b1_micro_cnt_ = 0;
  b2_micro_cnt_ = 0;
}

void SSReplayMicroCkptCtx::inc_total_replay_cnt()
{
  ATOMIC_INC(&total_replay_cnt_);
}

void SSReplayMicroCkptCtx::inc_invalid_micro_cnt(const bool is_in_l1, const bool is_in_ghost)
{
  if (is_in_l1 && !is_in_ghost) {
    ATOMIC_INC(&invalid_t1_micro_cnt_);
  } else if (!is_in_l1 && !is_in_ghost) {
    ATOMIC_INC(&invalid_t2_micro_cnt_);
  }
}

void SSReplayMicroCkptCtx::inc_ghost_micro_cnt(const bool is_in_l1, const bool is_in_ghost)
{
  if (is_in_l1 && is_in_ghost) {
    ATOMIC_INC(&b1_micro_cnt_);
  } else if (!is_in_l1 && is_in_ghost) {
    ATOMIC_INC(&b2_micro_cnt_);
  }
}

/*-----------------------------------------ObSSMicroMetaManager------------------------------------------*/

ObSSMicroMetaManager::ObSSMicroMetaManager(ObSSMicroCacheStat &cache_stat)
    : is_inited_(false),
      is_mini_mode_(false),
      mem_limited_(false),
      block_size_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      micro_cnt_limit_(0),
      cache_mem_limit_(0),
      expiration_time_s_(0),
      allocator_(nullptr),
      micro_meta_map_(),
      bg_micro_iter_(micro_meta_map_),
      fg_micro_iter_(micro_meta_map_),
      arc_info_(),
      cache_stat_(cache_stat),
      replay_ctx_()
{}

int ObSSMicroMetaManager::init(
    const uint64_t tenant_id,
    const bool is_mini_mode,
    const int32_t block_size, 
    const int64_t cache_limit_size, 
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "SSMicroMap");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || block_size <= 0 || cache_limit_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(block_size), K(cache_limit_size));
  } else if (OB_FAIL(micro_meta_map_.init(attr))) {
    LOG_WARN("fail to init macro meta map", KR(ret), K(attr));
  } else {
    tenant_id_ = tenant_id;
    allocator_ = &allocator;
    block_size_ = block_size;
    if (OB_FAIL(inner_update_cache_expiration_time(tenant_id))) {
      LOG_WARN("fail to inner update cache expiration_time", KR(ret), K(tenant_id));
    } else if (OB_FAIL(inner_update_cache_mem_limit(tenant_id))) {
      LOG_WARN("fail to inner update cache mem_limit", KR(ret), K(tenant_id));
    } else if (OB_FAIL(init_arc_info(cache_limit_size, ATOMIC_LOAD(&micro_cnt_limit_)))) {
      LOG_WARN("fail to init arc_info", KR(ret), K(cache_limit_size));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObSSMicroMetaManager::destroy()
{
  micro_meta_map_.destroy();
  arc_info_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  allocator_ = nullptr;
  block_size_ = 0;
  micro_cnt_limit_ = 0;
  cache_mem_limit_ = 0;
  expiration_time_s_ = 0;
  mem_limited_ = false;
  is_inited_ = false;
}

void ObSSMicroMetaManager::clear_micro_meta_manager()
{
  micro_meta_map_.clear();
  arc_info_.reuse();
}

int ObSSMicroMetaManager::init_arc_info(const int64_t cache_limit_size, const int64_t micro_cnt_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cache_limit_size <= 0 || micro_cnt_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_limit_size), K(micro_cnt_limit));
  } else {
    inner_update_arc_limit(cache_limit_size);
    arc_info_.set_micro_cnt_limit(micro_cnt_limit);
  }
  return ret;
}

int ObSSMicroMetaManager::update_arc_limit(const int64_t new_cache_limit_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_cache_limit_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_cache_limit_size));
  } else {
    inner_update_arc_limit(new_cache_limit_size);
  }
  return ret;
}

OB_INLINE void ObSSMicroMetaManager::inner_update_arc_limit(const int64_t cache_limit_size)
{
  const int64_t arc_limit_pct = (is_mini_mode_ || cache_limit_size < SS_MINI_MODE_CACHE_FILE_SIZE)
                                ? SS_MINI_MODE_ARC_LIMIT_PCT
                                : SS_ARC_LIMIT_PCT;
  const int64_t arc_limit = cache_limit_size * arc_limit_pct / 100;
  arc_info_.update_arc_limit(arc_limit);
}

int ObSSMicroMetaManager::update_arc_work_limit_for_prewarm(const bool start_update)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (start_update) {
    arc_info_.dec_arc_work_limit_for_prewarm();
  } else {
    arc_info_.inc_arc_work_limit_for_prewarm();
  }
  return ret;
}

int ObSSMicroMetaManager::alloc_micro_block_meta(
    ObSSMicroBlockMeta *&micro_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_NOT_NULL(micro_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("micro_meta should be null", KR(ret), KP(micro_meta));
  } else if (check_cache_mem_limit()) {
    ret = OB_SS_CACHE_REACH_MEM_LIMIT;
    LOG_WARN("ss_micro_cache memory has reached limit", KR(ret));
  }

  if (OB_SUCC(ret)) {
    void *ptr = SSMicroMetaAlloc.alloc();
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc micro_meta mem", KR(ret));
    } else {
      micro_meta = new(ptr) ObSSMicroBlockMeta();
      SSMicroCacheStat.micro_stat().update_micro_pool_alloc_cnt(1);
      SSMicroCacheStat.micro_stat().update_micro_pool_info();
    }
  }
  return ret;
}

bool ObSSMicroMetaManager::check_cache_mem_limit()
{
  bool reach_limit = false;
  const int64_t cur_micro_cnt = cache_stat_.micro_stat().get_total_micro_cnt();
  const int64_t micro_cnt_limit = get_micro_cnt_limit();
  if (OB_UNLIKELY(cur_micro_cnt >= micro_cnt_limit)) {
    set_mem_limited(true);
    reach_limit = true;
  } else {
    set_mem_limited(false);
  }
  return reach_limit;
}

int ObSSMicroMetaManager::get_micro_block_meta_handle(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMicroBlockMetaHandle &micro_meta_handle,
    const bool update_arc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_meta_handle));
  } else {
    ObSSMemBlockHandle mem_handle;
    ObSSPhysicalBlockHandle phy_handle;
    SSMicroMapGetMicroHandleFunc get_func(micro_meta_handle, mem_handle, phy_handle, update_arc);
    int32_t micro_size = 0;
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, get_func))) {
      ret = ((OB_EAGAIN == ret) ? get_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get micro_meta", KR(ret), K(micro_key), K(update_arc));
      }
    } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_meta handle should be valid", KR(ret), K(micro_key));
    } else if (update_arc) {
      const bool is_in_l1 = get_func.micro_info_.is_in_l1_;
      const bool is_in_ghost = get_func.micro_info_.is_in_ghost_;
      const int32_t micro_size = get_func.micro_info_.size_;
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, get_func.arc_op_type_, micro_size, 1))) {
        LOG_WARN("fail to adjust arc seg_info", KR(ret), K(micro_key), K(get_func));
      }
    }
  }

  if (OB_FAIL(ret)) {
    micro_meta_handle.reset();
  }

  return ret;
}

int ObSSMicroMetaManager::do_add_or_update_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key, 
    const int32_t micro_size,
    const uint32_t micro_crc, 
    ObSSMemBlockHandle &mem_blk_handle, 
    bool &is_first_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid() || !mem_blk_handle.is_valid() || micro_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_size), K(mem_blk_handle));
  } else {
    is_first_add = false;
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSMicroBlockMeta *micro_meta = nullptr;

    // 1. alloc and init micro_meta
    if (OB_FAIL(alloc_micro_block_meta(micro_meta))) {
      LOG_WARN("fail to alloc micro_meta", KR(ret));
    } else if (OB_ISNULL(micro_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("micro_meta should not be null", KR(ret), KP(micro_meta));
    } else {
      micro_meta->reset();
      if (OB_FAIL(micro_meta->init(micro_key, mem_blk_handle, micro_size, micro_crc))) {
        LOG_WARN("fail to init micro_meta", KR(ret), K(micro_key), K(micro_size), K(micro_crc), K(mem_blk_handle));
      } else {
        micro_meta_handle.set_ptr(micro_meta);
      }
    }
    
    // 2. insert or update meta
    if (OB_SUCC(ret)) {
      bool is_in_l1 = true;
      bool is_in_ghost = false;
      ObSSARCOpType arc_op_type = ObSSARCOpType::SS_ARC_NEW_ADD;

      SSMicroMapUpdateMicroFunc update_func(micro_size, mem_blk_handle, micro_crc);
      const ObSSMicroBlockCacheKey &inner_micro_key = micro_meta_handle()->get_micro_key();
      // If micro_key exists, execute update_func on this micro_key. 
      // Otherwise insert this micro_key into hashmap and do not execute update_func.
      if (OB_FAIL(micro_meta_map_.insert_or_operate(&inner_micro_key, micro_meta_handle, update_func))) {
        LOG_WARN("fail to add_or_update meta", KR(ret), K_(update_func.ret), K(inner_micro_key), K(micro_size), 
                                               K(micro_crc), K(mem_blk_handle));
        ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
      } else {
        if (update_func.is_updated_) {
          is_in_l1 = update_func.is_in_l1_;
          is_in_ghost = update_func.is_in_ghost_;
          arc_op_type = update_func.arc_op_type_;
        } else {
          cache_stat_.micro_stat().update_micro_map_info(1, micro_size);
        }

        // insert new micro_meta or validate ghost micro_meta, means first add micro_block data into cache.
        if (!update_func.is_updated_ || update_func.validate_ghost_micro_) {
          is_first_add = true;
          mem_blk_handle()->add_valid_micro_block(micro_size);
        }

        if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, arc_op_type, micro_size, 1))) {
          LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(micro_key), K(micro_size));
        }
      }
    }
  }

  return ret;
}

int ObSSMicroMetaManager::add_or_update_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    const int32_t micro_size, 
    const uint32_t micro_crc, 
    ObSSMemBlockHandle &mem_blk_handle, 
    bool &is_first_add)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || !mem_blk_handle.is_valid() || micro_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_size), K(mem_blk_handle));
  } else {
    if (OB_UNLIKELY(ATOMIC_LOAD(&mem_limited_))) {
      ret = OB_SS_CACHE_REACH_MEM_LIMIT;
      LOG_WARN("ss_micro_cache memory has reached limit", KR(ret));
    } else if (OB_FAIL(do_add_or_update_micro_block_meta(micro_key, micro_size, micro_crc, mem_blk_handle, is_first_add))) {
      LOG_WARN("fail to add_or_update meta", KR(ret), K(micro_key), K(micro_size), K(micro_crc), K(mem_blk_handle));
    }

    // After trying to add or update the meta of this micro_block, we need to increase
    // handled_count to indicate that the meta of this micro_block has been handled.
    if (OB_LIKELY(mem_blk_handle()->is_valid())) {
      mem_blk_handle()->inc_handled_count();
    }
  }

  return ret;
}

int ObSSMicroMetaManager::try_evict_micro_block_meta(const ObSSMicroMetaSnapshot &cold_micro)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    const bool is_in_l1 = cold_micro.is_in_l1();
    const bool is_in_ghost = cold_micro.is_in_ghost();
    const int64_t phy_blk_idx = cold_micro.data_dest() / block_size_;
    SSMicroMapEvictMicroFunc evict_func(cold_micro);
    const ObSSMicroBlockCacheKey &micro_key = cold_micro.micro_key();
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, evict_func))) {
      ret = ((OB_EAGAIN == ret) ? evict_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to evict micro_block meta", KR(ret), K(micro_key));
      } else {
        LOG_TRACE("micro_key dose not exist", KR(ret), K(micro_key));
      }
    } else {
      const ObSSARCOpType op_type = ObSSARCOpType::SS_ARC_TASK_EVICT_OP;
      const int64_t micro_size = cold_micro.length();
      cache_stat_.task_stat().update_arc_evict_cnt(is_in_l1, is_in_ghost, 1);
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(micro_key), K(cold_micro));
      }
      LOG_TRACE("ss_cache: evict micro_meta by arc_task", KR(ret), K(micro_key), K(phy_blk_idx), K(is_in_l1), 
        K(is_in_ghost), K(cache_stat_.task_stat()));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_delete_micro_block_meta(const ObSSMicroMetaSnapshot &cold_micro)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    const bool is_in_l1 = cold_micro.is_in_l1();
    const bool is_in_ghost = cold_micro.is_in_ghost();
    const int64_t phy_blk_idx = cold_micro.data_dest() / block_size_;
    SSMicroMapDeleteMicroFunc delete_func(cold_micro);
    const ObSSMicroBlockCacheKey &micro_key = cold_micro.micro_key();
    if (OB_FAIL(micro_meta_map_.erase_if(&micro_key, delete_func))) {
      if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_TRACE("fail to try delete micro_block meta", KR(ret), K(cold_micro));
      } else {
        LOG_WARN("fail to try delete micro_block meta", KR(ret), K(cold_micro));
      }
    } else {
      const ObSSARCOpType op_type = ObSSARCOpType::SS_ARC_TASK_DELETE_OP;
      const int64_t micro_size = cold_micro.length();
      cache_stat_.task_stat().update_arc_delete_cnt(is_in_l1, is_in_ghost, 1);
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_ERROR("fail to adjust seg_info", KR(ret), K(micro_key), K(cold_micro));
      }
      LOG_TRACE("ss_cache: delete micro_meta by arc_task", KR(ret), K(micro_key), K(phy_blk_idx), K(is_in_l1),
        K(is_in_ghost), K(cache_stat_.task_stat()));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_delete_micro_block_meta(ObSSMemBlockHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handle));
  } else {
    const int64_t DEF_MICRO_CNT = 32;
    ObSEArray<ObSSMicroBlockCacheKey, DEF_MICRO_CNT> micro_keys; 
    if (OB_FAIL(mem_blk_handle()->get_all_micro_keys(micro_keys))) {
      LOG_WARN("fail to get all micro_keys", KR(ret), KPC(mem_blk_handle.get_ptr()));
    } else if (!micro_keys.empty()) {
      const int64_t micro_key_cnt = micro_keys.count();
      for (int64_t i = 0; OB_SUCC(ret) && (i < micro_key_cnt); ++i) {
        const ObSSMicroBlockCacheKey &micro_key = micro_keys.at(i);
        SSMicroMapDeleteUnpersistedMicroFunc delete_func(mem_blk_handle);
        if (OB_FAIL(micro_meta_map_.erase_if(&micro_key, delete_func))) {
          if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_TRACE("fail to try delete unpersisted micro_block meta", KR(ret), K(micro_key));
          } else {
            LOG_WARN("fail to try delete unpersisted micro_block meta", KR(ret), K(micro_key));
          }
        } else {
          const ObSSARCOpType op_type = ObSSARCOpType::SS_ARC_ABNORMAL_DELETE_OP;
          const bool is_in_l1 = delete_func.is_in_l1_;
          const bool is_in_ghost = delete_func.is_in_ghost_;
          const int64_t micro_size = delete_func.size_;
          if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
            LOG_WARN("fail to adjust arc seg_info", KR(ret), K(micro_key), K(mem_blk_handle));
          }
          LOG_INFO("ss_cache: delete micro_meta which fail to persist", KR(ret), K(micro_key), K(is_in_l1), 
            K(is_in_ghost));
        }
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_delete_invalid_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    SSMicroMapDeleteInvalidMicroFunc delete_func;
    if (OB_FAIL(micro_meta_map_.erase_if(&micro_key, delete_func))) {
      if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_TRACE("fail to try delete invalid micro_block meta", KR(ret), K(micro_key));
      } else {
        LOG_WARN("fail to try delete invalid micro_block meta", KR(ret), K(micro_key));
      }
    } else {
      const ObSSARCOpType op_type = ObSSARCOpType::SS_ARC_ABNORMAL_DELETE_OP;
      const bool is_in_l1 = delete_func.is_in_l1_;
      const bool is_in_ghost = delete_func.is_in_ghost_;
      const int64_t micro_size = delete_func.size_;
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_WARN("fail to adjust arc seg_info", KR(ret), K(micro_key));
      }
      LOG_INFO("ss_cache: delete micro_meta which is invalid", KR(ret), K(micro_key), K(is_in_l1), K(is_in_ghost));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_delete_expired_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMicroBaseInfo &expired_micro_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    const int64_t expiration_time = get_cache_expiration_time();
    SSMicroMapDeleteExpiredMicroFunc delete_func(expiration_time, expired_micro_info);
    if (OB_FAIL(micro_meta_map_.erase_if(&micro_key, delete_func))) {
      if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_TRACE("fail to try delete expired micro_block meta", KR(ret), K(micro_key));
      } else {
        LOG_WARN("fail to try delete expired micro_block meta", KR(ret), K(micro_key));
      }
    } else {
      const ObSSARCOpType op_type = ObSSARCOpType::SS_ARC_EXPIRED_DELETE_OP;
      const bool is_in_l1 = expired_micro_info.is_in_l1_;
      const bool is_in_ghost = expired_micro_info.is_in_ghost_;
      const int64_t micro_size = expired_micro_info.size_;
      cache_stat_.task_stat().update_expired_cnt(is_in_ghost, 1);
      if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
        LOG_WARN("fail to adjust arc seg_info", KR(ret), K(micro_key));
      }
      LOG_INFO("ss_cache: delete micro_meta which is expired", KR(ret), K(micro_key), K(expiration_time),
        K(expired_micro_info));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_invalidate_micro_block_meta(
    const ObSSMicroBlockCacheKey &micro_key,
    bool &succ_invalidate)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    succ_invalidate = false;
    ObSSMicroBaseInfo micro_info;
    SSMicroMapInvalidateMicroFunc invalidate_func(micro_info);
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, invalidate_func))) {
      ret = ((OB_EAGAIN == ret) ? invalidate_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to invalidate micro_block meta", KR(ret), K(micro_key));
      } else {
        LOG_TRACE("mirco_key does not exist", KR(ret), K(micro_key));
      }
    } else {
      succ_invalidate = invalidate_func.succ_invalidate_;
      if (succ_invalidate) {
        const bool is_in_l1 = micro_info.is_in_l1_;
        const bool is_in_ghost = micro_info.is_in_ghost_;
        const int64_t micro_size = micro_info.size_;
        const ObSSARCOpType op_type = ObSSARCOpType::SS_ARC_INVALIDATE_OP;
        if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, op_type, micro_size, 1))) {
          LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(micro_key), K(is_in_l1), K(is_in_ghost));
        } else if (micro_size > 0 && !micro_info.is_in_ghost_ && micro_info.is_persisted_) {
          const uint64_t data_dest = micro_info.data_dest_;
          const uint64_t reuse_version = micro_info.reuse_version_;
          const int32_t delta_size = micro_size * -1;
          int64_t phy_blk_idx = -1;
          if (OB_TMP_FAIL(SSPhyBlockMgr.update_block_valid_length(data_dest, reuse_version, delta_size, phy_blk_idx))) {
            if (OB_S2_REUSE_VERSION_MISMATCH == tmp_ret) {
              LOG_TRACE("fail to update block valid length", K(micro_key), K(micro_info));
            } else {
              LOG_ERROR("fail to update block valid length", KR(tmp_ret), K(micro_key), K(micro_info));
            }
          }
        }
      }
      LOG_TRACE("ss_cache: invalidate micro_meta", KR(ret), K(micro_key), K(succ_invalidate));
    }
  }
  return ret;
}

void ObSSMicroMetaManager::update_cache_expiration_time_by_config()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(inner_update_cache_expiration_time(tenant_id_))) {
    LOG_WARN("fail to inner update cache expiration_time", KR(ret), K_(tenant_id));
  }
}

int ObSSMicroMetaManager::inner_update_cache_expiration_time(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const int64_t cur_expiration_time = ObSSMicroCacheUtil::calc_ss_cache_expiration_time(tenant_id_);
    ATOMIC_STORE(&expiration_time_s_, cur_expiration_time);
  }
  return ret;
}

void ObSSMicroMetaManager::update_cache_mem_limit_by_config()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(inner_update_cache_mem_limit(tenant_id_))) {
    LOG_WARN("fail to inner update cache mem_limit", KR(ret), K_(tenant_id));
  }
}

int ObSSMicroMetaManager::inner_update_cache_mem_limit(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    int64_t cur_micro_cnt_limit = 0;
    const int64_t cur_mem_limit = ObSSMicroCacheUtil::calc_ss_cache_mem_limit_size(tenant_id);
    if (cur_mem_limit != cache_mem_limit_) {
      if (OB_FAIL(cal_micro_cnt_limit(cur_mem_limit, cur_micro_cnt_limit))) {
        LOG_WARN("fail to cal micro_cnt_limit", KR(ret), K(cur_mem_limit));
      } else {
        ATOMIC_STORE(&cache_mem_limit_, cur_mem_limit);
        ATOMIC_STORE(&micro_cnt_limit_, cur_micro_cnt_limit);
        cache_stat_.micro_stat().set_micro_cnt_limit(cur_micro_cnt_limit);
        arc_info_.set_micro_cnt_limit(cur_micro_cnt_limit);
        SSMicroMetaAlloc.set_total_limit(cur_mem_limit);
        check_cache_mem_limit();
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::get_micro_block_meta_handle(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMicroBaseInfo &micro_info,
    ObSSMicroBlockMetaHandle &micro_meta_handle, 
    ObSSMemBlockHandle &mem_blk_handle,
    ObSSPhysicalBlockHandle &phy_blk_handle,
    const bool update_arc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    SSMicroMapGetMicroHandleFunc get_func(micro_meta_handle, mem_blk_handle, phy_blk_handle, update_arc);
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, get_func))) {
      ret = ((OB_EAGAIN == ret) ? get_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get micro_block location", KR(ret), K(micro_key), K(get_func));
      }
    } else if (OB_UNLIKELY(!micro_meta_handle.is_valid() || !get_func.micro_info_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_meta_handle or micro_info should be valid", KR(ret), K(micro_key), K(get_func));
    } else if (FALSE_IT(micro_info = get_func.micro_info_)) {
    } else if (update_arc) {
      const bool is_in_l1 = micro_info.is_in_l1_;
      const bool is_in_ghost = micro_info.is_in_ghost_;
      const int32_t size = micro_info.size_;
      // Ghost micro must be invalid, directly return OB_ENTRY_NOT_EXIST
      // If micro_block is in T2, no need to adjust, cuz just need to update access_time.
      // So just need to adjust when it is in T1.
      if (is_in_l1 && !is_in_ghost) {
        if (OB_FAIL(inner_adjust_arc_seg_info(is_in_l1, is_in_ghost, ObSSARCOpType::SS_ARC_HIT_T1, size, 1))) {
          LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(get_func), KPC(micro_meta_handle.get_ptr()));
        }
      }
    }

    if (OB_FAIL(ret)) {
      micro_meta_handle.reset();
      mem_blk_handle.reset();
      phy_blk_handle.reset();
    }
  }
  return ret;
}

// When we persisted mem_block's data into phy_block, we should try to promise that:
// 'update_micro_block_meta' must succ. If some micro_blocks fail to update, these
// added micro_block cache will be invalid, cuz mem_block's reuse_version will be 
// increased.
int ObSSMicroMetaManager::update_micro_block_meta(
    ObSSMemBlockHandle &sealed_mem_blk_handle,
    const int64_t block_offset,
    const uint32_t reuse_version, 
    int64_t &updated_micro_size,
    int64_t &updated_micro_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(block_offset < 0 || !sealed_mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_offset), K(sealed_mem_blk_handle));
  } else if (OB_UNLIKELY(!(sealed_mem_blk_handle.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_offset), KPC(sealed_mem_blk_handle.get_ptr()));
  } else {
    updated_micro_size = 0;
    updated_micro_cnt = 0;

    const int64_t blk_valid_len = sealed_mem_blk_handle()->valid_val_;
    const int64_t valid_micro_cnt = sealed_mem_blk_handle()->valid_count_;
    const int64_t micro_cnt = sealed_mem_blk_handle()->micro_count_;
    const int32_t payload_offset = sealed_mem_blk_handle()->reserved_size_;
    const uint32_t micro_index_size = sealed_mem_blk_handle()->index_size_;
    int64_t data_dest = block_offset + payload_offset;
    int64_t pos = 0;
    
    ObSSMicroBlockIndex micro_index;
    for (int64_t i = 0; OB_SUCC(ret) && (pos < micro_index_size); ++i) {
      micro_index.reset();
      if (OB_FAIL(micro_index.deserialize(sealed_mem_blk_handle()->index_buf_, 
          sealed_mem_blk_handle()->index_buf_size_, pos))) {
        LOG_WARN("fail to deserialize micro index", KR(ret), KPC(sealed_mem_blk_handle.get_ptr()));
      } else {
        const int32_t size = micro_index.get_size();
        SSMicroMapUpdateMicroDestFunc update_func(data_dest, reuse_version, sealed_mem_blk_handle);
        if (OB_FAIL(micro_meta_map_.operate(&(micro_index.micro_key_), update_func))) {
          ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
          if (OB_ENTRY_NOT_EXIST == ret) {
            // 1. reach micro_cnt_limit; 2. TTL expired; 3. call clear_tablet_micro_meta()
            LOG_WARN("micro_meta not exist", KR(ret), K(micro_index), K(i), K(micro_cnt), K(valid_micro_cnt));
          } else {
            LOG_WARN("fail to update micro block location", KR(ret), K(micro_index), K(i), K(micro_cnt), 
              K(valid_micro_cnt));
          }
          // If fail to update data_dest, we can continue to handle next one.
          ret = OB_SUCCESS;
        } else if (OB_LIKELY(update_func.succ_update_)) {
          updated_micro_size += size;
          ++updated_micro_cnt;
        }
        data_dest += size;
      }
    }

    if (OB_SUCC(ret)) {
      if (updated_micro_cnt == valid_micro_cnt) {
        if (updated_micro_size != blk_valid_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("updated_micro_cnt or updated_micro_size mismatch", KR(ret), K(updated_micro_cnt),
              K(valid_micro_cnt), K(updated_micro_size), K(blk_valid_len), K(micro_cnt), 
              KPC(sealed_mem_blk_handle.get_ptr()));
        }
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::update_micro_block_meta_heat(
    const ObIArray<ObSSMicroBlockCacheKey> &micro_keys,
    const bool transfer_seg,
    const bool update_access_time,
    const int64_t time_delta_s)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(micro_keys.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_keys));
  } else {
    SSMicroMapUpdateMicroHeatFunc update_func(transfer_seg, update_access_time, time_delta_s);
    const int64_t total_micro_cnt = micro_keys.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < total_micro_cnt); ++i) {
      const ObSSMicroBlockCacheKey &micro_key = micro_keys.at(i);
      if (OB_UNLIKELY(!micro_key.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(i), K(total_micro_cnt), K(micro_key));
      } else if (OB_FAIL(micro_meta_map_.operate(&micro_key, update_func))) {
        ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
        if (OB_ENTRY_NOT_EXIST == ret) {
          // ignore error code, continue
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to update micro block meta heat", KR(ret), K(micro_key), K(transfer_seg), K(update_access_time),
                   K(time_delta_s));
        }
      } else if (update_func.arc_op_type_ == ObSSARCOpType::SS_ARC_HIT_T1) {
        if (OB_FAIL(inner_adjust_arc_seg_info(true, false, ObSSARCOpType::SS_ARC_HIT_T1, update_func.size_, 1))) {
          LOG_ERROR("fail to adjust arc seg_info", KR(ret), K(i), K(micro_key), "size", update_func.size_);
        }
      }
      // ignore ret, continue to handle next one
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_mark_reorganizing_if_exist(
    const ObSSMicroBlockCacheKey &micro_key, 
    const int64_t phy_blk_idx, 
    const int32_t blk_offset, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || phy_blk_idx < 0 || blk_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(phy_blk_idx), K(blk_offset));
  } else {
    const int64_t data_dest = phy_blk_idx * block_size_ + blk_offset;
    SSMicroMapTryReorganizeMicroFunc reorgan_func(data_dest, micro_meta_handle);
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, reorgan_func))) {
      ret = ((OB_EAGAIN == ret) ? reorgan_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to try mark micro_block reorganizing", KR(ret), K(micro_key), K(data_dest));
      } else {
        LOG_TRACE("micro key does not exist", KR(ret), K(micro_key));
      }
    }

    if (OB_FAIL(ret)) {
      micro_meta_handle.reset();
    }
  }
  return ret;
}

int ObSSMicroMetaManager::try_unmark_reorganizing_if_exist(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMemBlockHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || !mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(mem_blk_handle));
  } else if (OB_UNLIKELY(!(mem_blk_handle()->is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), KPC(mem_blk_handle.get_ptr()));
  } else {
    SSMicroMapCompleteReorganizingMicroFunc complete_func(mem_blk_handle);
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, complete_func))) {
      ret = ((OB_EAGAIN == ret) ? complete_func.ret_ : ret);
      LOG_WARN("fail to try unmark micro_block reorganizing", KR(ret), K(micro_key));
    }
  }
  return ret;
}

int ObSSMicroMetaManager::force_unmark_reorganizing(
    const ObSSMicroBlockCacheKey &micro_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key));
  } else {
    SSMicroMapUnmarkReorganizingMicroFunc unmark_func;
    if (OB_FAIL(micro_meta_map_.operate(&micro_key, unmark_func))) {
      ret = ((OB_EAGAIN == ret) ? unmark_func.ret_ : ret);
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to unmark micro_block reorganizing", KR(ret), K(micro_key));
      } else {
        LOG_TRACE("micro key does not exist", KR(ret), K(micro_key));
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

/*
 * Now, we consider that, arc_task and checkpoint_task will run in the same thread, thus, there
 * won't exist concurrent operation between these two task
 */
int ObSSMicroMetaManager::acquire_cold_micro_blocks(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_arc_seg_idx(seg_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(seg_idx), K(arc_iter_info));
  } else if (arc_iter_info.need_iterate_cold_micro(seg_idx)) {
    bg_micro_iter_.rewind_randomly();
    const int64_t op_cnt = arc_iter_info.get_op_micro_cnt(seg_idx);
    const int64_t exp_iter_cnt = arc_iter_info.get_expected_iter_micro_cnt(seg_idx);
    const int64_t micro_map_cnt = micro_meta_map_.count();
    const int64_t total_round = MIN(exp_iter_cnt + SS_MAX_ARC_HANDLE_OP_CNT, micro_map_cnt);
    // If micro_map has few meta, we need to iterate the entire map to prevent missing some meta.
    const int64_t early_exit_round = MAX(total_round / 2, micro_map_cnt);
    int64_t round = 0;
    while (OB_SUCC(ret) && (round < total_round) && (arc_iter_info.need_iterate_cold_micro(seg_idx))) {
      const ObSSMicroBlockCacheKey *micro_key = nullptr;
      ObSSMicroBlockMetaHandle cur_micro_handle;
      if (OB_FAIL(bg_micro_iter_.next(micro_key, cur_micro_handle))) {
        if (OB_ITER_END == ret) {
          bg_micro_iter_.rewind();
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to next micro_block meta", KR(ret), K(micro_key));
        }
      } else if (OB_FALSE_IT(++round)) {
      } else if (OB_UNLIKELY(!cur_micro_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta handle should be valid", KR(ret), K(cur_micro_handle));
      } else {
        ObSSMicroMetaSnapshot cur_micro_snap;
        if (OB_FAIL(cur_micro_snap.init(*(cur_micro_handle.get_ptr())))) {
          LOG_WARN("fail to init micro_meta snapshot", KR(ret), KPC(cur_micro_handle.get_ptr()));
        } else {
          const bool can_choose = ((seg_idx == ARC_T1 || seg_idx == ARC_T2)
                                  ? cur_micro_snap.can_evict() : cur_micro_snap.can_delete());
          if (can_choose) {
            const bool is_in_l1 = cur_micro_snap.is_in_l1();
            const bool is_in_ghost = cur_micro_snap.is_in_ghost();
            const int64_t cur_seg_idx = get_arc_seg_idx(is_in_l1, is_in_ghost);
            if (cur_seg_idx == seg_idx) {
              bool is_choosen = false;
              // avoid to choose the same micro_block twice.
              if (OB_FAIL(arc_iter_info.exist_cold_micro(*micro_key, is_choosen))) {
                LOG_WARN("fail to check cold_micro exist", KR(ret), KPC(micro_key), K(seg_idx), K(arc_iter_info));
              } else if (!is_choosen) {
                if (OB_FAIL(arc_iter_info.push_cold_micro(*micro_key, cur_micro_snap, seg_idx))) {
                  LOG_WARN("fail to push", KR(ret), K(seg_idx), KPC(micro_key), K(cur_micro_snap));
                }
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if ((round == early_exit_round) && (arc_iter_info.get_obtained_micro_cnt(seg_idx) < op_cnt)) {
          LOG_TRACE("it's hard to iterate cold micro in this seg", K(round), K(total_round), K(seg_idx), K(arc_iter_info));
          break;
        }
      }
    }
  }
  return ret;
}

// NOTICE: ckpt_task and arc_task are running in the same thread. If not, we need to have another macro_iter
//         to iterate micro_meta.
int ObSSMicroMetaManager::scan_micro_blocks_for_checkpoint(
    ObIArray<ObSSMicroBlockMetaHandle> &ckpt_micro_handles,
    ObIArray<ObSSMicroBlockCacheKey> &invalid_micro_keys,
    ObIArray<ObSSMicroBlockCacheKey> &expired_micro_keys,
    ObSSTabletCacheMap &tablet_cache_map,
    const bool is_first_scan)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (is_first_scan) {
    bg_micro_iter_.rewind();
  }
  
  if (OB_SUCC(ret)) {
    int64_t cnt = 0;
    while (OB_SUCC(ret) && (cnt < MAX_SCAN_MICRO_CKPT_CNT)) {
      const ObSSMicroBlockCacheKey *micro_key = nullptr;
      ObSSMicroBlockMetaHandle tmp_micro_handle;
      if (OB_FAIL(bg_micro_iter_.next(micro_key, tmp_micro_handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to next macro_block meta", KR(ret), K(micro_key));
        }
      } else if (OB_UNLIKELY(!tmp_micro_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta handle should be valid", KR(ret), K(tmp_micro_handle));
      } else {
        ++cnt;
        const bool is_in_l1 = tmp_micro_handle()->is_in_l1();
        const bool is_in_ghost = tmp_micro_handle()->is_in_ghost();
        const int64_t micro_size = tmp_micro_handle()->length();
        if (is_in_ghost || tmp_micro_handle()->is_valid_field()) {
          const int64_t cur_expiration_time = get_cache_expiration_time();
          if (tmp_micro_handle()->is_expired(cur_expiration_time)) {
            if (OB_FAIL(expired_micro_keys.push_back(tmp_micro_handle()->get_micro_key()))) {
              LOG_WARN("fail to push back", KR(ret), KPC(tmp_micro_handle.get_ptr()));
            }
          } else {
            ObTabletID tablet_id = tmp_micro_handle()->get_micro_key().get_major_macro_tablet_id();
            ObSSTabletCacheInfo tablet_cache_info;
            tablet_cache_info.tablet_id_ = tablet_id;
            if (OB_TMP_FAIL(tablet_cache_map.get_refactored(tablet_id, tablet_cache_info))) {
              if (OB_HASH_NOT_EXIST == tmp_ret) {
                tmp_ret = OB_SUCCESS;
                tablet_cache_info.add_micro_size(is_in_l1, is_in_ghost, micro_size);
                if (OB_TMP_FAIL(tablet_cache_map.set_refactored(tablet_id, tablet_cache_info))) {
                  LOG_WARN("fail to set refactored", KR(tmp_ret), K(tablet_id), K(tablet_cache_info));
                }
              } else {
                LOG_WARN("fail to get refactored", KR(tmp_ret), K(tablet_id), KPC(tmp_micro_handle.get_ptr()));
              }
            } else {
              tablet_cache_info.add_micro_size(is_in_l1, is_in_ghost, micro_size);
              if (OB_TMP_FAIL(tablet_cache_map.set_refactored(tablet_id, tablet_cache_info, true/*overwrite*/))) {
                LOG_WARN("fail to set refactored", KR(tmp_ret), K(tablet_id), K(tablet_cache_info));
              }
            }

            if (tmp_micro_handle()->is_persisted()) {
              // If not persisted, won't be saved into ckpt
              // invalid ghost micro_meta can also be saved into ckpt
              if (OB_FAIL(ckpt_micro_handles.push_back(tmp_micro_handle))) {
                LOG_WARN("fail to push back", KR(ret), KPC(tmp_micro_handle.get_ptr()));
              }
            }
          }
        } else if (OB_FAIL(invalid_micro_keys.push_back(tmp_micro_handle()->get_micro_key()))) {
          // Only consider T1/T2 invalid micro_meta
          LOG_WARN("fail to push back", KR(ret), KPC(tmp_micro_handle.get_ptr()));
        }
      }
    }
  }
  return ret;
}

int ObSSMicroMetaManager::read_micro_meta_checkpoint(
    ObSSLinkedPhyBlockItemReader &item_reader,
    const int64_t micro_ckpt_time_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!item_reader.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    const int64_t delta_time_us = MAX(ObTimeUtility::current_time() - micro_ckpt_time_us, 0);
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    cache_stat_.micro_stat().reset_dynamic_info();
    arc_info_.reset_seg_info();
    replay_ctx_.reset();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(item_reader.get_next_item(item_buf, item_buf_len))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next micro_meta item", KR(ret), K_(tenant_id), KP(item_buf), K(item_buf_len));
        }
        ret = OB_SUCCESS;
        break;
      } else {
        replay_ctx_.inc_total_replay_cnt();
        ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
        int64_t pos = 0;
        if (OB_FAIL(alloc_micro_block_meta(tmp_micro_meta))) {
          LOG_WARN("fail to alloc micro_block_meta", KR(ret));
        } else if (OB_ISNULL(tmp_micro_meta)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("allocated micro_meta from pool should not be null", KR(ret), KP(tmp_micro_meta));
        } else if (FALSE_IT(tmp_micro_meta->reset())) {
        } else {
          ObSSMicroBlockMetaHandle micro_meta_handle;
          micro_meta_handle.set_ptr(tmp_micro_meta);
          if (OB_FAIL(micro_meta_handle()->deserialize(item_buf, item_buf_len, pos))) {
            LOG_WARN("fail to deserialize", KR(ret), K(item_buf_len), KP(item_buf));
          } else {
            const bool is_in_l1 = micro_meta_handle()->is_in_l1();
            const bool is_in_ghost = micro_meta_handle()->is_in_ghost();
            const uint64_t data_dest = micro_meta_handle()->data_dest();
            const uint64_t reuse_version = micro_meta_handle()->reuse_version();
            const uint64_t micro_size = micro_meta_handle()->length();
            if (micro_meta_handle()->is_valid_for_ckpt(true/*print_log*/)) {
              micro_meta_handle()->update_access_time(delta_time_us / 1000000L);
              micro_meta_handle()->is_reorganizing_ = 0;
              int64_t blk_idx = -1;
              if (OB_FAIL(inner_add_ckpt_micro_block_meta(micro_meta_handle))) {
                LOG_WARN("fail to inner add ckpt micro_block meta", KR(ret), KPC(micro_meta_handle.get_ptr()));
              } else if (is_in_ghost) {
                micro_meta_handle()->mark_invalid();
                replay_ctx_.inc_ghost_micro_cnt(is_in_l1, is_in_ghost);
              } else if (OB_FAIL(SSPhyBlockMgr.update_block_valid_length(data_dest, reuse_version, micro_size, blk_idx))) {
                LOG_WARN("fail to update block valid length", KR(ret), KPC(micro_meta_handle.get_ptr()), K(blk_idx));
              }
            } else {
              micro_meta_handle()->mark_invalid();
              replay_ctx_.inc_invalid_micro_cnt(is_in_l1, is_in_ghost);
            }
          }
          ret = OB_SUCCESS; // continue to handle next micro_meta
        }
      }
    }

    FLOG_INFO("finish read micro_meta ckpt", KR(ret), K_(replay_ctx), K_(arc_info), "micro_stat", 
      cache_stat_.micro_stat());
  }
  return ret;
}

int ObSSMicroMetaManager::inner_add_ckpt_micro_block_meta(ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_meta_handle));
  } else if (OB_LIKELY(micro_meta_handle()->is_in_ghost() || micro_meta_handle()->is_valid_field())) { 
    const int32_t micro_size = micro_meta_handle()->length();
    const ObSSMicroBlockCacheKey &micro_key = micro_meta_handle()->get_micro_key();
    if (OB_FAIL(micro_meta_map_.insert(&micro_key, micro_meta_handle))) {
      if (OB_ENTRY_EXIST == ret) {
        LOG_INFO("micro_meta already exist, no need to insert again", KR(ret), K(micro_key), K(micro_meta_map_.count()));
      } else {
        LOG_WARN("fail to insert micro_map", KR(ret), K(micro_key), K(micro_meta_map_.count()));
      }
    } else {
      const bool is_in_l1 = micro_meta_handle()->is_in_l1();
      const bool is_in_ghost = micro_meta_handle()->is_in_ghost();
      const int32_t size = micro_meta_handle()->length();
      arc_info_.update_seg_info(is_in_l1, is_in_ghost, size, 1);

      const int64_t arc_valid_cnt = arc_info_.get_valid_count();
      const int64_t arc_valid_size = arc_info_.get_valid_size();
      cache_stat_.micro_stat().update_valid_micro_info(arc_valid_cnt, arc_valid_size);
      cache_stat_.micro_stat().update_micro_map_info(1, micro_size);
    }
  }

  return ret;
}

int ObSSMicroMetaManager::inner_adjust_arc_seg_info(
    const bool is_in_l1, 
    const bool is_in_ghost, 
    const ObSSARCOpType &op_type,
    const int32_t delta_size, 
    const int32_t delta_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(arc_info_.adjust_seg_info(is_in_l1, is_in_ghost, op_type, delta_size, delta_cnt))) {
    LOG_ERROR("fail to adjust seg_info", KR(ret), K(is_in_l1), K(is_in_ghost), K(op_type), K(delta_size),
      K(delta_cnt));
  }

  const int64_t arc_valid_cnt = arc_info_.get_valid_count();
  const int64_t arc_valid_size = arc_info_.get_valid_size();
  cache_stat_.micro_stat().update_valid_micro_info(arc_valid_cnt, arc_valid_size);

  // arc_info may fail to adjust, but we still need to update stat
  if (ObSSARCOpType::SS_ARC_TASK_DELETE_OP == op_type || 
      ObSSARCOpType::SS_ARC_ABNORMAL_DELETE_OP == op_type ||
      ObSSARCOpType::SS_ARC_EXPIRED_DELETE_OP == op_type) {
    cache_stat_.micro_stat().update_micro_map_info(-1, delta_size * -1);
  }
  return ret;
}

// For ob_admin
int ObSSMicroMetaManager::get_micro_meta_handle(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || micro_meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(micro_meta_handle));
  } else if (OB_FAIL(micro_meta_map_.get(&micro_key, micro_meta_handle))) {
    LOG_WARN("fail to get micro_meta", KR(ret), K(micro_key));
  }
  return ret;
}

int ObSSMicroMetaManager::clear_tablet_micro_meta(const ObTabletID &tablet_id, int64_t &remain_micro_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    bool finish_iter = false;
    remain_micro_cnt = 0;
    fg_micro_iter_.rewind();
    while (OB_SUCC(ret) && !finish_iter) {
      const ObSSMicroBlockCacheKey *micro_key = nullptr;
      ObSSMicroBlockMetaHandle micro_handle;
      bool succ_invalidate = false;
      if (OB_FAIL(fg_micro_iter_.next(micro_key, micro_handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to next macro_block meta", KR(ret), K(micro_key));
        } else {
          finish_iter = true;
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(micro_key) || OB_UNLIKELY(!micro_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_meta handle should be valid", KR(ret), KP(micro_key), K(micro_handle));
      } else if (micro_handle()->get_micro_key().get_major_macro_tablet_id() == tablet_id) {
        if (OB_FAIL(try_invalidate_micro_block_meta(*micro_key, succ_invalidate))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to try invalidate micro_block_meta", KR(ret), KPC(micro_key), K(succ_invalidate));
          }
        } else if (!succ_invalidate) {
          ++remain_micro_cnt;
        }
      } 
    }
  }
  return ret;
}

int ObSSMicroMetaManager::cal_micro_cnt_limit(const int64_t mem_limit, int64_t &micro_cnt_limit)
{
  int ret = OB_SUCCESS;
  micro_cnt_limit = 0;
  int64_t micro_cnt_per_block = 0;
  ObSmallAllocator *tmp_allocator = nullptr;
  void *mem_buffer = nullptr;
  ObArray<void*> slice_arr;
  const int64_t micro_meta_size = sizeof(ObSSMicroBlockMeta);
  const int64_t MAX_MICRO_CNT =  SS_META_ALLOCATOR_BLOCK_SIZE / micro_meta_size;
  ObMemAttr attr(tenant_id_, "CalMicroLimit");

  if (OB_UNLIKELY(mem_limit <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(mem_limit));
  } else if (OB_ISNULL(mem_buffer = ob_malloc(sizeof(ObSmallAllocator), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(sizeof(ObSmallAllocator)));
  } else if (OB_FALSE_IT(tmp_allocator = new (mem_buffer) ObSmallAllocator())) {
  } else if (OB_FAIL(tmp_allocator->init(micro_meta_size, "CalMicroLimit", tenant_id_, SS_META_ALLOCATOR_BLOCK_SIZE))) {
    LOG_WARN("fail to init ObSmallAllocator", KR(ret), K(micro_meta_size), K_(tenant_id));
  } else {
    tmp_allocator->set_total_limit(SS_META_ALLOCATOR_BLOCK_SIZE);
    int64_t micro_cnt = 0;
    while (OB_SUCC(ret) && micro_cnt <= MAX_MICRO_CNT) {
      void *ptr = tmp_allocator->alloc();
      if (OB_ISNULL(ptr)) {
        break;
      } else if (OB_FAIL(slice_arr.push_back(ptr))) {
        LOG_WARN("fail to push", KR(ret), KP(ptr));
      } else {
        micro_cnt++;
      }
    }
    
    if (OB_SUCC(ret)) {
      if (micro_cnt > 0 && micro_cnt <= MAX_MICRO_CNT) {
        micro_cnt_per_block = micro_cnt;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to test block capacity", KR(ret), K_(tenant_id), K(micro_cnt), K(MAX_MICRO_CNT));
      }
    }
  }

  if (OB_NOT_NULL(tmp_allocator)) {
    for (int64_t i = 0; i < slice_arr.count(); ++i) {
      void *ptr = slice_arr[i];
      if (OB_NOT_NULL(ptr)) {
        tmp_allocator->free(ptr);
      }
    }
    tmp_allocator->destroy();
    ob_free(tmp_allocator);
  }

  if (OB_SUCC(ret) && SS_META_ALLOCATOR_BLOCK_SIZE > 0) {
    micro_cnt_limit = (mem_limit / SS_META_ALLOCATOR_BLOCK_SIZE) * micro_cnt_per_block;
    LOG_INFO("finish calculate micro_cnt_limit", K(micro_cnt_per_block), K(micro_cnt_limit));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
