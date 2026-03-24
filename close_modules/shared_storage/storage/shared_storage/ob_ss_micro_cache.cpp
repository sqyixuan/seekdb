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

#include "ob_ss_micro_cache.h"
#include "storage/tx_storage/ob_ls_service.h"

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_handler.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_reader.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSMicroCacheConfig-----------------------------------------*/
ObSSMicroCacheConfig::ObSSMicroCacheConfig()
    : micro_ckpt_compressor_type_(ObCompressorType::NONE_COMPRESSOR),
      blk_ckpt_compressor_type_(ObCompressorType::NONE_COMPRESSOR)
{}

void ObSSMicroCacheConfig::reset()
{
  micro_ckpt_compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  blk_ckpt_compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
}
/*-----------------------------------------ObSSMicroCache-----------------------------------------*/
ERRSIM_POINT_DEF(EN_MICRO_CACHE_FULL);
ObSSMicroCache::ObSSMicroCache()
  : is_inited_(false), is_stopped_(false), is_mini_mode_(false), is_first_start_(true), is_enabled_(true), 
    phy_block_size_(0), tenant_id_(OB_INVALID_TENANT_ID), cache_file_size_(0), flying_req_cnt_(0), config_(), 
    cache_stat_(), allocator_(), phy_blk_mgr_(cache_stat_, allocator_), mem_data_mgr_(cache_stat_), 
    micro_meta_mgr_(cache_stat_), task_runner_(), latest_access_micro_key_mgr_(), micro_meta_allocator_()
{}

int ObSSMicroCache::mtl_init(ObSSMicroCache *&micro_cache)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_mgr = nullptr;
  
  if (OB_ISNULL(tnt_disk_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_mgr));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const int64_t cache_file_size = tnt_disk_mgr->get_micro_cache_reserved_size();
    if (OB_FAIL(micro_cache->init(tenant_id, cache_file_size))) {
      LOG_WARN("fail to init ss_micro_cache", KR(ret), K(tenant_id), K(cache_file_size));
    }
  }
  return ret;
}

int ObSSMicroCache::init(
    const uint64_t tenant_id, 
    const int64_t cache_file_size)
{
  int ret = OB_SUCCESS;
  const int32_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  int32_t phy_blk_size = (macro_block_size != 0) ? macro_block_size : DEFAULT_BLOCK_SIZE;
  const int64_t max_cache_mem_size = ObSSMicroCacheUtil::calc_ss_cache_mem_limit_size(tenant_id);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((cache_file_size < 0 || phy_blk_size < 1 || max_cache_mem_size <= 0) ||
             !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(cache_file_size), K(phy_blk_size), K(max_cache_mem_size));
  } else {
    tenant_id_ = tenant_id;
    cache_file_size_ = cache_file_size;
    phy_block_size_ = phy_blk_size;
    cache_stat_.set_tenant_id(tenant_id);
    // When create tenant, due to the hidden sys tenent, it may occur that, the cache_file_size is 
    // 0 for hidden_sys tenant at first. If the hidden_sys tenant converts to sys tenant, it needs
    // to resize cache_file_size. Thus, we need to handle this situation.
    if (cache_file_size > 0) {
      if (cache_file_size <= SS_MIN_CACHE_FILE_SIZE) {
        LOG_WARN("ss_micro_cache file size is too small, may exist some wrong state", K(cache_file_size));
      }
      
      is_mini_mode_ = MTL_IS_MINI_MODE();
      ObTenantFileManager *tnt_file_mgr = nullptr;
      if (OB_ISNULL(tnt_file_mgr = MTL(ObTenantFileManager*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant disk file manager is null", KR(ret), KP(tnt_file_mgr));
      } else if (OB_FAIL(allocator_.init(phy_blk_size, ObMemAttr(tenant_id, "SSMicroCache"), max_cache_mem_size))) {
        LOG_WARN("fail to init allocator", KR(ret), K(phy_blk_size), K(tenant_id), K(max_cache_mem_size));
      } else if (OB_FAIL(mem_data_mgr_.init(tenant_id, cache_file_size, phy_blk_size, is_mini_mode_))) {
        LOG_WARN("fail to init mem data manager", KR(ret), K(tenant_id), K(cache_file_size), K(phy_blk_size), K_(is_mini_mode));
      } else if (OB_FAIL(phy_blk_mgr_.init(tenant_id, cache_file_size, phy_blk_size))) {
        LOG_WARN("fail to init block manager", KR(ret), K(tenant_id), K(cache_file_size), K(phy_blk_size));
      } else if (OB_FAIL(init_micro_meta_pool())) {
        LOG_WARN("fail to init micro_meta_pool", KR(ret), K(tenant_id));
      } else if (OB_FAIL(init_micro_meta_manager(is_mini_mode_))) {
        LOG_WARN("fail to init micro meta manager", KR(ret), K(tenant_id), K_(is_mini_mode), K(phy_blk_size));
      } else if (OB_FAIL(latest_access_micro_key_mgr_.init())) {
        LOG_WARN("fail to init latest access micro key manager", KR(ret));
      } else if (OB_FAIL(task_runner_.init(tenant_id, allocator_, mem_data_mgr_, micro_meta_mgr_, phy_blk_mgr_, 
                 *tnt_file_mgr, cache_stat_, latest_access_micro_key_mgr_))) {
        LOG_WARN("fail to init micro_cache task runner", KR(ret), K(tenant_id));
      } else {
        flying_req_cnt_ = 0;
        is_inited_ = true;
        is_enabled_ = true;
        LOG_INFO("succ to init ss_micro_cache", K(tenant_id), K(cache_file_size), K(max_cache_mem_size), K_(is_mini_mode));
      }
    } else if (OB_UNLIKELY(OB_SYS_TENANT_ID != tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only for hidden_sys tenant, we can treat cache_file_size as 0", KR(ret), K(tenant_id), K(cache_file_size));
    }
  }
  return ret;
}

int ObSSMicroCache::init_micro_meta_pool()
{
  int ret = OB_SUCCESS;
  const int64_t micro_meta_size = sizeof(ObSSMicroBlockMeta);
  if (OB_FAIL(micro_meta_allocator_.init(micro_meta_size, "SSMetaAlloc", tenant_id_, 
             SS_META_ALLOCATOR_BLOCK_SIZE))) {
    LOG_WARN("fail to init micro_meta allocator", KR(ret), K(micro_meta_size), K_(tenant_id));
  }
  return ret;
}

int ObSSMicroCache::init_micro_meta_manager(const bool is_mini_mode)
{
  int ret = OB_SUCCESS;
  const int64_t cache_limit_size = phy_blk_mgr_.get_cache_limit_size();
  if (OB_UNLIKELY(cache_limit_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_limit_size));
  } else if (OB_FAIL(micro_meta_mgr_.init(tenant_id_, is_mini_mode, phy_block_size_, cache_limit_size, allocator_))) {
    LOG_WARN("fail to init micro meta manager", KR(ret), K_(tenant_id), K(is_mini_mode), K(cache_limit_size), 
      K_(phy_block_size));
  }
  return ret;
}

int ObSSMicroCache::start()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    FLOG_INFO("begin ss_micro_cache start func");
    ATOMIC_STORE(&is_stopped_, false);
    ObTenantFileManager *tnt_file_mgr = nullptr;
    if (OB_ISNULL(tnt_file_mgr = MTL(ObTenantFileManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant disk file manager is null", KR(ret), KP(tnt_file_mgr));
    } else if (FALSE_IT(is_first_start_ = (!tnt_file_mgr->is_micro_cache_file_exist()))) {
    } else if (OB_FAIL(read_or_format_super_block())) {
      LOG_WARN("fail to read or format ss_super_block", KR(ret), K_(is_first_start));
    } else if (OB_FAIL(task_runner_.start())) {
      LOG_WARN("fail to start micro_cache task runner", KR(ret));
    } else if (!is_cache_enabled()) {
      // if cache is disabled, that means we need to execute replay_ckpt_async. 
      // we can stop the other bg_tasks temporarily.
      task_runner_.stop_extra_task_for_ckpt();
      task_runner_.enable_replay_ckpt();
    }
    FLOG_INFO("finish ss_micro_cache start func", K(is_cache_enabled()));
  }
  return ret;
}

void ObSSMicroCache::stop()
{
  FLOG_INFO("begin ss_micro_cache stop func", KP(this));
  ATOMIC_STORE(&is_stopped_, true);
  task_runner_.stop();
  FLOG_INFO("finish ss_micro_cache stop func", KP(this));
}

int ObSSMicroCache::wait()
{
  FLOG_INFO("begin ss_micro_cache wait func", KP(this));
  int ret = OB_SUCCESS;
  do {
    if (ATOMIC_LOAD(&is_enabled_)) {
      task_runner_.wait();
      break;
    } else {
      ob_usleep(SLEEP_INTERVAL_US);
      if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_US)) {
        FLOG_WARN("still wait for clear_micro_cache finish");
      }
    }
  } while (OB_SUCC(ret));
  FLOG_INFO("begin ss_micro_cache wait func", KP(this));
  return ret;
}

void ObSSMicroCache::inner_check_micro_meta_cnt()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_INTERVAL_US = 1000;
  const int64_t PRINT_LOG_INTERVAL_US = 2 * 1000 * 1000;
  int64_t cur_alloc_cnt = cache_stat_.micro_stat().get_micro_pool_alloc_cnt();
  while (cur_alloc_cnt != 0) {
    ob_usleep(SLEEP_INTERVAL_US);
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_US)) {
      FLOG_WARN("still exist unfree micro_meta", KR(ret), K(cur_alloc_cnt));
    }
    cur_alloc_cnt = cache_stat_.micro_stat().get_micro_pool_alloc_cnt();
  }
}

void ObSSMicroCache::destroy()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("begin ss_micro_cache destroy func", KP(this));
  ATOMIC_STORE(&is_stopped_, true);

  do {
    if (IS_INIT && ATOMIC_LOAD(&is_enabled_)) {
      const int64_t SLEEP_INTERVAL_US = 1000;
      const int64_t PRINT_LOG_INTERVAL_US = 2 * 1000 * 1000;
      bool fin_destroy = false;
      do {
        const int64_t flying_req_cnt = ATOMIC_LOAD(&flying_req_cnt_);
        if (flying_req_cnt > 0) {
          ob_usleep(SLEEP_INTERVAL_US);
          if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_US)) {
            FLOG_WARN("still exist request which may access micro_meta_map", K(flying_req_cnt));
          }
        } else {
          latest_access_micro_key_mgr_.destroy();
          task_runner_.destroy();
          micro_meta_mgr_.destroy();
          mem_data_mgr_.destroy();
          phy_blk_mgr_.destroy();
          allocator_.reset();

          // must execute it at the last step
          inner_check_micro_meta_cnt();
          micro_meta_allocator_.destroy();
          fin_destroy = true;
        }
      } while (!fin_destroy);
      break;
    } else {
      ob_usleep(SLEEP_INTERVAL_US);
      if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_US)) {
        FLOG_WARN("still wait for clear_micro_cache finish");
      }
    }
  } while (OB_SUCC(ret) && IS_INIT);

  cache_stat_.reset();
  config_.reset();
  phy_block_size_ = 0;
  cache_file_size_ = 0;
  flying_req_cnt_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_mini_mode_ = false;
  is_inited_ = false;
  is_enabled_ = true;
  FLOG_INFO("finish ss_micro_cache destroy func", KP(this));
}

int ObSSMicroCache::is_ready_to_evict_cache(bool &prepare_eviction)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("fail to resize micro_cache_file", KR(ret), K_(is_enabled));
  } else {
    prepare_eviction = micro_meta_mgr_.get_arc_info().close_to_evict();
  }
  return ret;
}

int ObSSMicroCache::resize_micro_cache_file_size(const int64_t new_cache_file_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("fail to resize micro_cache_file", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(new_cache_file_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_cache_file_size));
  } else if (is_inited_) {
    if (new_cache_file_size > cache_file_size_) {
      // NOTICE: have no way to resize micro_meta_pool fixed count
      if (OB_FAIL(phy_blk_mgr_.resize_file_size(new_cache_file_size, phy_block_size_))) {
        LOG_WARN("fail to resize file size", KR(ret), K_(tenant_id), K(new_cache_file_size)); 
      } else {
        const int64_t new_cache_limit_size = phy_blk_mgr_.get_cache_limit_size();
        if (OB_FAIL(micro_meta_mgr_.update_arc_limit(new_cache_limit_size))) {
          LOG_WARN("fail to update arc_limit when resize", KR(ret), K(new_cache_file_size), K(new_cache_limit_size));
        } else {
          const int64_t ori_cache_file_size = cache_file_size_;
          cache_file_size_ = new_cache_file_size;
          FLOG_INFO("succ to resize micro_cache_file size", K(ori_cache_file_size), K(new_cache_file_size));
        }
      }
    }
  } else {
    // Why init() function related with resize() function ??
    // For sys_tenant, when execute init for bootstrap, the 'total_file_size' will be set as 0,
    // cuz the hidden_sys tenant won't be allocated space. Only for normal sys_tenant, will use
    // resize operation to change its 'total_file_size'.
    // Thus, we need to init and start when resize file_size for sys_tenant.
    if (OB_UNLIKELY(OB_SYS_TENANT_ID != tenant_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("must be sys tenant in this situation", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(init(tenant_id_, new_cache_file_size))) {
      LOG_WARN("fail to init", KR(ret), K_(tenant_id), K(new_cache_file_size));
    } else if (OB_FAIL(start())) {
      LOG_WARN("fail to start", KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObSSMicroCache::add_micro_block_cache(
    const ObSSMicroBlockCacheKey &micro_key,
    const char *micro_data,
    const int32_t size,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_req_cnt_);
  const int64_t start_us = ObTimeUtility::current_time();

  bool exist_valid_micro = false;
  bool is_first_add = false; // If this micro_block not exist in cache or is ghost state
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss_micro_cache not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_ISNULL(micro_data) || OB_UNLIKELY(!micro_key.is_valid() || (size <= 0) ||
             (ObSSMicroCacheAccessType::MAX_TYPE == access_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size), KP(micro_data), K(access_type));
  } else {
    const bool is_prewarm_io = (access_type != ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    cache_stat_.io_stat().update_io_stat_info(is_prewarm_io, false/*is_read*/, 1, size);

    ObSSMemBlockHandle mem_blk_handle;
    ObSSMicroBlockMetaHandle micro_meta_handle;
    if (OB_FAIL(micro_meta_mgr_.get_micro_block_meta_handle(micro_key, micro_meta_handle, true/*update_arc*/))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get micro_block meta handle", KR(ret), K(micro_key), K(access_type));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      exist_valid_micro = true;
    }
    
    if (OB_SUCC(ret) && !exist_valid_micro) {
      if (OB_UNLIKELY(micro_meta_mgr_.is_mem_limited())) {
        ret = OB_SS_CACHE_REACH_MEM_LIMIT;
        LOG_WARN("ss_micro_cache memory has reached limit", KR(ret));
      } else {
        uint32_t micro_crc = 0;
        if (OB_FAIL(mem_data_mgr_.add_micro_block_data(micro_key, micro_data, size, mem_blk_handle, micro_crc))) {
          LOG_WARN("fail to add micro_block data", KR(ret), K(micro_key), K(size), K(access_type));
          // no free mem_block, convert errno to OB_EAGAIN for upper module try again
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_EAGAIN;
          }
        } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mem_block handle should be valid", KR(ret), K(micro_key), K(size), K(mem_blk_handle));
        } else if (OB_FAIL(micro_meta_mgr_.add_or_update_micro_block_meta(
                   micro_key, size, micro_crc, mem_blk_handle, is_first_add))) {
          if (OB_SS_CACHE_REACH_MEM_LIMIT == ret) {
            LOG_WARN("micro_meta memory usage reachs limit", KR(ret), K(micro_key), K(size), K(micro_crc), 
              K(mem_blk_handle), K(access_type));
          } else {
            LOG_ERROR("fail to add or update micro block meta", KR(ret), K(micro_key), K(size), K(micro_crc), 
              K(mem_blk_handle), K(access_type));
          }
        } else if (is_prewarm_io) {
          cache_stat_.hit_stat().update_prewarm_info(access_type, 1, size);
        }

        if (OB_SUCC(ret)) {
          cache_stat_.hit_stat().update_add_info(is_first_add, is_prewarm_io, 1, size);
        }
      }
    }

    if (OB_FAIL(ret)) {
      cache_stat_.hit_stat().update_fail_add_cnt(1, size);
    }
  }

  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  LOG_TRACE("ss_cache: finish add micro_block cache", KR(ret), K(cost_us), K(exist_valid_micro), K(is_first_add), 
    K(size), K(access_type), K(micro_key));

  return ret;
}

int ObSSMicroCache::add_micro_block_cache_for_prewarm(
    const ObSSMicroBlockCacheKey &micro_key,
    const char *micro_data, const int32_t size,
    const ObSSMicroCacheAccessType access_type,
    const int64_t max_retry_times, const bool transfer_seg)
{
  int ret = OB_SUCCESS;
  bool is_add = false;
  int64_t retry_times = 0;
  while (OB_SUCC(ret) && !is_add && (retry_times < max_retry_times)) {
    if (OB_FAIL(add_micro_block_cache(micro_key, micro_data, size, access_type))) {
      LOG_WARN("fail to add micro block cache", KR(ret), K(micro_key), KP(micro_data),
              K(size), K(access_type), K(max_retry_times), K(retry_times));
    } else {
      is_add = true;
      // transfer from T1 to T2 if need
      if (transfer_seg) {
        int tmp_ret = OB_SUCCESS;
        ObSEArray<ObSSMicroBlockCacheKey, 1> micro_keys;
        micro_keys.set_attr(ObMemAttr(MTL_ID(), "SSMicroCache"));
        if (OB_TMP_FAIL(micro_keys.push_back(micro_key))) {
          LOG_WARN("fail to push back micro key", KR(tmp_ret), K(micro_key));
        } else if (OB_TMP_FAIL(update_micro_block_heat(micro_keys, true/*need to transfer T1 -> T2*/,
                                true/*need to update access_time to current_time*/))) {
          LOG_WARN("fail to update micro block heat", KR(tmp_ret), K(micro_keys));
        }
      }
    }
    // Note: OB_EAGAIN means space is not enough now and can retry again
    if (!is_add && (OB_EAGAIN == ret) && (retry_times < max_retry_times)) {
      ret = OB_SUCCESS; // ignore ret and retry again
      const int64_t retry_interval_ms = MIN(25 * (1 << retry_times), 1000); // [25ms, 1s]
      LOG_INFO("will retry add micro block cache", K(retry_times), K(retry_interval_ms),
                K(micro_key), K(access_type));
      ob_usleep(retry_interval_ms * 1000L);
    }
    retry_times++;
  }
  return ret;
}

int ObSSMicroCache::get_micro_block_cache(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMicroBlockId &phy_micro_id,
    const MicroCacheGetType get_type,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || !phy_micro_id.is_valid() ||
             (ObSSMicroCacheAccessType::MAX_TYPE == access_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(phy_micro_id), K(access_type));
  } else if (OB_FAIL(inner_get_micro_block_cache(micro_key, phy_micro_id, get_type, io_info,
             obj_handle, access_type))) {
    LOG_WARN("fail to inner get micro block cache", KR(ret), K(micro_key), K(phy_micro_id),
      K(get_type), K(io_info), K(obj_handle), K(access_type));
  }
  return ret;
}

int ObSSMicroCache::get_cached_micro_block(
    const ObSSMicroBlockCacheKey &micro_key,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_req_cnt_);
  const ObSSMicroBlockId tmp_phy_micro_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_UNLIKELY(!micro_key.is_valid() || (ObSSMicroCacheAccessType::MAX_TYPE == access_type) ||
            (ObSSMicroCacheAccessType::COMMON_IO_TYPE == access_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(access_type));
  } else if (OB_FAIL(inner_get_micro_block_cache(micro_key, tmp_phy_micro_id, MicroCacheGetType::GET_CACHE_HIT_DATA,
             io_info, obj_handle, access_type))) {
    LOG_WARN("fail to inner get micro block cache", KR(ret), K(micro_key), K(io_info), K(obj_handle), K(access_type));
  }
  return ret;
}

int ObSSMicroCache::inner_get_micro_block_cache(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMicroBlockId &phy_micro_id,
    const MicroCacheGetType get_type,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else {
    const MacroBlockId &macro_id = phy_micro_id.get_macro_id();
    ObSSMemBlockHandle mem_blk_handle;
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
    const bool is_common_io = (ObSSMicroCacheAccessType::COMMON_IO_TYPE == access_type);
    cache_stat_.io_stat().update_io_stat_info(!is_common_io, true/*is_read*/, 1, io_info.size_);
    const int32_t req_delta = (is_common_io ? 1 : 0);
    const int32_t req_size_delta = (is_common_io ? io_info.size_ : 0);
    const bool update_arc = is_common_io;

    // Detail: 
    ObSSMicroBaseInfo micro_info;
    if (OB_FAIL(inner_get_micro_block_handle(micro_key, micro_info, micro_meta_handle, mem_blk_handle, 
        io_info.phy_block_handle_, hit_type, update_arc))) {
      LOG_WARN("fail to inner get micro_block handle", KR(ret), K(micro_key), K(update_arc));
    } else {
      ObSSMicroCacheHandler handler;
      if (ObSSCacheHitType::SS_CACHE_MISS == hit_type) {
        if (MicroCacheGetType::GET_CACHE_HIT_DATA == get_type) {
          if (OB_FAIL(simulate_io_result(get_type, macro_id, io_info, obj_handle))) {
            LOG_WARN("fail to simulate io result", KR(ret), K(micro_key), K(phy_micro_id), K(io_info));
          }
        } else if (OB_FAIL(handler.handle_not_hit(micro_key, macro_id, io_info, obj_handle, access_type))) {
          LOG_WARN("fail to handle not hit", KR(ret), K(micro_key), K(phy_micro_id), K(access_type));
        } else if (OB_ISNULL(io_info.fd_.device_handle_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("device handle is null", KR(ret), K(micro_key), K(phy_micro_id), K(get_type),
                   K(access_type), K(io_info));
        } else if (io_info.fd_.device_handle_->is_object_device()) {
          // read private macro from disk should not update cache miss cnt,
          // only read from object storage should update cache miss cnt
          cache_stat_.hit_stat().update_cache_miss(req_delta, req_size_delta);
          LOG_INFO("ss_cache: not hit micro_cache", K(micro_key), "size", io_info.size_, K(access_type));
        }
      } else if (ObSSCacheHitType::SS_CACHE_HIT_MEM == hit_type) {
        char *io_buf = nullptr;
        // Due to the mem_block already inc_ref_cnt, thus we can promise when we get_micro_data, this mem_block is valid.
        if (MicroCacheGetType::GET_CACHE_MISS_DATA == get_type) {
          if (OB_FAIL(simulate_io_result(get_type, macro_id, io_info, obj_handle))) {
            LOG_WARN("fail to simulate io result", KR(ret), K(micro_key), K(phy_micro_id), K(io_info));
          }
        } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("mem_block handle is invalid", KR(ret), K(micro_key));
        } else if (OB_FAIL(mem_blk_handle()->get_micro_data(micro_key, io_info.size_, micro_info.crc_, io_buf))) {
          LOG_WARN("fail to get micro data from memory", KR(ret), K(micro_key), "size",
                   io_info.size_, K(micro_info), K(mem_blk_handle));
        } else if (OB_FAIL(handler.handle_hit_memory(get_type, macro_id, io_buf, io_info, obj_handle))) {
          LOG_WARN("fail to handle hit memory", KR(ret), K(get_type), K(micro_key), K(phy_micro_id),
                   K(micro_info), K(mem_blk_handle));
        } else {
          if (is_common_io && OB_TMP_FAIL(push_latest_access_micro_key(micro_key, hit_type, micro_info))) {
            LOG_WARN("fail to push latest access micro key", KR(tmp_ret), K(micro_key), K(hit_type), K(micro_info));
          }
          cache_stat_.hit_stat().update_cache_hit(req_delta, req_size_delta);
          LOG_INFO("ss_cache: get micro_cache from memory", K(micro_key), "size", io_info.size_,
                   K(micro_info), K(mem_blk_handle), K(access_type));
        }
      } else {
        const uint64_t data_dest = micro_info.data_dest_;
        const uint32_t crc = micro_info.crc_;
        const HandleHitDiskParam param(micro_key, macro_id, data_dest, crc, get_type, access_type);
        if (((MicroCacheGetType::GET_CACHE_MISS_DATA == get_type)
             && (ObSSCacheHitType::SS_CACHE_HIT_DISK == hit_type))) {
          if (OB_FAIL(simulate_io_result(get_type, macro_id, io_info, obj_handle))) {
            LOG_WARN("fail to simulate io result", KR(ret), K(micro_key), K(phy_micro_id), K(io_info));
          }
        } else if (OB_UNLIKELY(!io_info.phy_block_handle_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block handle in io_info is invalid", KR(ret), K(micro_key), K(data_dest));
        } else if (OB_FAIL(handler.handle_hit_disk(param, io_info, obj_handle))) {
          LOG_WARN("fail to handle hit disk", KR(ret), K(param));
        } else {
          if (is_common_io && OB_TMP_FAIL(push_latest_access_micro_key(micro_key, hit_type, micro_info))) {
            LOG_WARN("fail to push latest access micro key", KR(tmp_ret), K(micro_key), K(hit_type), K(micro_info));
          }
          cache_stat_.hit_stat().update_cache_hit(req_delta, req_size_delta);
          LOG_INFO("ss_cache: get micro_cache from disk", K(micro_key), K(phy_micro_id), K(micro_info), K(access_type));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    cache_stat_.hit_stat().update_fail_get_cnt(1, io_info.size_);
  }
  return ret;
}

int ObSSMicroCache::get_not_exist_micro_blocks(
    const ObIArray<ObSSMicroBlockCacheKeyMeta> &in_micro_block_key_metas,
    ObIArray<ObSSMicroBlockCacheKeyMeta> &out_micro_block_key_metas)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else {
    const int64_t micro_block_key_cnt = in_micro_block_key_metas.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_block_key_cnt); ++i) {
      const ObSSMicroBlockCacheKeyMeta &cur_key_meta = in_micro_block_key_metas.at(i);
      ObSSMicroBaseInfo micro_info;
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ObSSMemBlockHandle mem_blk_handle;
      ObSSPhysicalBlockHandle phy_blk_handle;
      ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
      if (OB_FAIL(inner_get_micro_block_handle(cur_key_meta.micro_key_, micro_info, micro_meta_handle,
          mem_blk_handle, phy_blk_handle, hit_type, false/*update_arc*/))) {
        LOG_WARN("fail to inner get micro_block handle", KR(ret), K(i), K(cur_key_meta));
      } else if ((ObSSCacheHitType::SS_CACHE_HIT_MEM == hit_type)
                || (ObSSCacheHitType::SS_CACHE_HIT_DISK == hit_type)) {
        // do nothing, already in micro cache
      } else if (ObSSCacheHitType::SS_CACHE_MISS == hit_type) {
        if (OB_FAIL(out_micro_block_key_metas.push_back(cur_key_meta))) {
          LOG_WARN("fail to push back", KR(ret), K(cur_key_meta));
        }
      }
    }
  }

  return ret;
}

int ObSSMicroCache::check_micro_block_exist(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroBaseInfo &micro_info,
    ObSSCacheHitType &hit_type)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ObSSMemBlockHandle mem_blk_handle;
    ObSSPhysicalBlockHandle phy_blk_handle;
    if (OB_FAIL(inner_get_micro_block_handle(micro_key, micro_info, micro_meta_handle, mem_blk_handle,
        phy_blk_handle, hit_type, false/*update_arc*/))) {
      LOG_WARN("fail to inner get micro_block handle", KR(ret), K(micro_key));
    }
  }
  return ret;
}

int ObSSMicroCache::inner_get_micro_block_handle(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMicroBaseInfo &micro_info,
    ObSSMicroBlockMetaHandle &micro_meta_handle,
    ObSSMemBlockHandle &mem_blk_handle,
    ObSSPhysicalBlockHandle &phy_blk_handle,
    ObSSCacheHitType &hit_type,
    const bool update_arc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else {
    hit_type = ObSSCacheHitType::SS_CACHE_MISS;
    if (OB_FAIL(micro_meta_mgr_.get_micro_block_meta_handle(micro_key, micro_info, micro_meta_handle, 
        mem_blk_handle, phy_blk_handle, update_arc))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get micro_block_meta handle", KR(ret), K(micro_key), K(update_arc));
      }
    } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_meta handle must be valid", KR(ret), K(micro_key), K(micro_meta_handle), K(update_arc));
    } else if (micro_info.is_in_ghost_) {  // Exist in B1/B2
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fetched micro_info should be in T1/T2", KR(ret), K(micro_key), K(micro_info));
    } else if (micro_info.is_persisted_) { // Exist in T1/T2, in phy_block
      if (OB_LIKELY(phy_blk_handle.is_valid())) {
        hit_type = ObSSCacheHitType::SS_CACHE_HIT_DISK;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("phy_block handle should be valid", KR(ret), K(micro_key), K(micro_info), K(update_arc));
      }
    } else {                                        // Exist in T1/T2, in mem_block
      if (OB_LIKELY(mem_blk_handle.is_valid())) {
        hit_type = ObSSCacheHitType::SS_CACHE_HIT_MEM;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("mem_block handle should be valid", KR(ret), K(micro_key), K(micro_info), K(update_arc));
      }
    }
  }

  if (OB_FAIL(ret) || (ObSSCacheHitType::SS_CACHE_MISS == hit_type)) {
    reset_handle(mem_blk_handle, phy_blk_handle, micro_meta_handle);
  }
  return ret;
}

int ObSSMicroCache::push_latest_access_micro_key(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSCacheHitType &hit_type,
    const ObSSMicroBaseInfo &micro_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if ((ObSSCacheHitType::SS_CACHE_HIT_MEM == hit_type) ||
             (ObSSCacheHitType::SS_CACHE_HIT_DISK == hit_type)) {
    ObSSMicroBlockCacheKeyMeta micro_meta(micro_key, micro_info.crc_, micro_info.size_, micro_info.is_in_l1_);
    if (OB_FAIL(latest_access_micro_key_mgr_.push_latest_access_micro_key_to_hashset(micro_meta))) {
      LOG_WARN("fail to push micro key", KR(ret), K(micro_meta));
    }
  }
  return ret;
}

void ObSSMicroCache::reset_handle(
    ObSSMemBlockHandle &mem_blk_handle,
    ObSSPhysicalBlockHandle &phy_blk_handle,
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  micro_meta_handle.reset();
  mem_blk_handle.reset();
  phy_blk_handle.reset();
}

int ObSSMicroCache::simulate_io_result(
    const MicroCacheGetType get_type,
    const MacroBlockId &macro_id,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle) const
{
  int ret = OB_SUCCESS;
  io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
  ObIOResult *io_result = nullptr;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(io_info.tenant_id_, tenant_holder))) {
    LOG_WARN("fail to get tenant io manager", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(tenant_holder.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant holder ptr is null", KR(ret));
  } else if (OB_FAIL(tenant_holder.get_ptr()->alloc_and_init_result(io_info, io_result))) {
    LOG_WARN("fail to alloc and init result", KR(ret), K(io_info));
  } else if (OB_ISNULL(io_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io result is null", KR(ret));
  } else if (OB_FAIL(obj_handle.get_io_handle().set_result(*io_result))) {
    LOG_WARN("fail to set result", KR(ret));
  } else if (FALSE_IT(io_result->finish_without_accumulate(ret))) {
  } else if (MicroCacheGetType::GET_CACHE_HIT_DATA == get_type) {
    // do nothing. there is no valid MacroBlockId when GET_CACHE_HIT_DATA (e.g., replica prewarm)
  } else if (OB_FAIL(obj_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
  }
  return ret;
}

int ObSSMicroCache::read_or_format_super_block()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (is_first_start_) {
    if (OB_FAIL(phy_blk_mgr_.format_ss_super_block())) {
      LOG_WARN("fail to format ss_super_block", KR(ret));
    }
  } else {
    ObSSMicroCacheSuperBlock super_blk;
    if (OB_FAIL(phy_blk_mgr_.read_ss_super_block(super_blk))) {
      LOG_WARN("fail to read ss_super_block", KR(ret));
    } else if (OB_UNLIKELY(!super_blk.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_super_block is invalid", KR(ret), K(super_blk));
    } else if (super_blk.is_valid_checkpoint()) {
      disable_cache();
      FLOG_INFO("ss_micro_cache checkpoint will execute async, thus disable cache firstly", 
        K_(is_enabled), K(super_blk));
    } else {
      super_blk.clear_ckpt_entry_list();
      if (OB_FAIL(phy_blk_mgr_.update_ss_super_block(super_blk))) {
        LOG_WARN("fail to update ss_super_block", KR(ret), K(super_blk));
      } else {
        FLOG_INFO("succ to update ss_super_block for invalid ckpt", K(super_blk));
      }
    }
  }
  return ret;
}

int ObSSMicroCache::read_checkpoint()
{
  int ret = OB_SUCCESS;
  ObSSMicroCacheSuperBlock super_blk;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_FAIL(phy_blk_mgr_.get_ss_super_block(super_blk))) {
    LOG_WARN("fail to get ss_super_blk", KR(ret), K_(is_inited), K_(is_enabled));
  } else if (OB_LIKELY(super_blk.is_valid())) {
    if (ObTimeUtility::current_time() - super_blk.micro_ckpt_time_us_ >= MAX_CKPT_EXPIRATION_TIME_US) {
      LOG_WARN("ss_micro_cache checkpoint is too old", K(super_blk));
    }

    if (super_blk.blk_ckpt_entry_list_.count() > 0) {
      const int64_t blk_entry = super_blk.blk_ckpt_entry_list_.at(0);
      if (OB_FAIL(read_phy_block_checkpoint(blk_entry))) {
        LOG_WARN("fail to read phy_block checkpoint", KR(ret), K(blk_entry));
        ret = OB_SUCCESS; // must succ, otherwise skip read micro_meta checkpoint
      } else if (super_blk.micro_ckpt_entry_list_.count() > 0) {
        const int64_t blk_entry = super_blk.micro_ckpt_entry_list_.at(0);
        const int64_t micro_ckpt_time_us = super_blk.micro_ckpt_time_us_;
        if (OB_FAIL(read_micro_meta_checkpoint(blk_entry, micro_ckpt_time_us))) {
          LOG_WARN("fail to read micro_meta checkpoint", KR(ret), K(blk_entry), K(micro_ckpt_time_us));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_blk_mgr_.update_block_state())) {
        LOG_ERROR("fail to update phy_block state", KR(ret), K_(tenant_id), K(super_blk));
      }
    }
  }
  return ret;
}

int ObSSMicroCache::read_phy_block_checkpoint(const int64_t blk_entry)
{
  int ret = OB_SUCCESS;
  ObSSLinkedPhyBlockItemReader item_reader;
  if (OB_FAIL(item_reader.init(blk_entry, tenant_id_, phy_blk_mgr_))) {
    LOG_WARN("fail to init ss_item_reader", KR(ret), K_(tenant_id), K(blk_entry));
  } else if (OB_FAIL(phy_blk_mgr_.read_phy_block_checkpoint(item_reader))) {
    LOG_WARN("fail to read phy_block checkpoint", KR(ret), K(blk_entry));
  }
  return ret;
}

int ObSSMicroCache::read_micro_meta_checkpoint(
    const int64_t blk_entry,
    const int64_t micro_ckpt_time_us)
{
  int ret = OB_SUCCESS;
  ObSSLinkedPhyBlockItemReader item_reader;
  if (OB_FAIL(item_reader.init(blk_entry, tenant_id_, phy_blk_mgr_))) {
    LOG_WARN("fail to init ss_item_reader", KR(ret), K_(tenant_id), K(blk_entry));
  } else if (OB_FAIL(micro_meta_mgr_.read_micro_meta_checkpoint(item_reader, micro_ckpt_time_us))) {
    LOG_WARN("fail to read micro_meta checkpoint", KR(ret), K_(tenant_id), K(micro_ckpt_time_us));
  }
  return ret;
}

int ObSSMicroCache::update_micro_block_heat(
    const ObIArray<ObSSMicroBlockCacheKey> &micro_keys,
    const bool transfer_seg,
    const bool update_access_time,
    const int64_t time_delta_s)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else {
    if (OB_FAIL(micro_meta_mgr_.update_micro_block_meta_heat(micro_keys, transfer_seg, update_access_time,
        time_delta_s))) {
      LOG_WARN("fail to update micro_block meta heat", KR(ret), K(micro_keys), K(transfer_seg), K(update_access_time),
        K(time_delta_s));
    } else {
      LOG_INFO("succ to update micro_block heat", K(micro_keys.count()), K(transfer_seg), K(update_access_time));
    }
  }
  return ret;
}

void ObSSMicroCache::update_hot_micro_lack_count(const bool full_scan, const int64_t lack_count)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (lack_count >= 0) {
    cache_stat_.hit_stat().update_hot_micro_lack_cnt_info(full_scan, lack_count);
  }
}

int ObSSMicroCache::get_available_space_for_prewarm(
    int64_t &available_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
#ifdef ERRSIM
  } else if (OB_SUCCESS != EN_MICRO_CACHE_FULL) {
    available_size = 0;
    LOG_INFO("[ERRSIM] fake EN_MICRO_CACHE_FULL", K(available_size));
#endif
  } else {
    available_size = 0;
    int64_t cache_limit_blk_cnt = 0;
    int64_t cache_data_blk_usage_pct = 0;
    phy_blk_mgr_.get_cache_data_block_info(cache_limit_blk_cnt, cache_data_blk_usage_pct);
    const int64_t cache_limit_size = cache_limit_blk_cnt * phy_block_size_;

    const ObSSARCInfo &arc_info = micro_meta_mgr_.get_arc_info();
    const int64_t arc_work_limit = arc_info.get_arc_work_limit();
    const int64_t valid_size = arc_info.get_valid_size();

    const int64_t available_pct = MAX(0, SS_ARC_LIMIT_MAX_PREWARM_PCT - cache_data_blk_usage_pct);
    available_size = cache_limit_size * available_pct / 100;
    available_size = MIN(available_size, arc_work_limit);
    // if throughput of common_io is high, we need to reduce available_size for prewarm
    // TODO: common_io_high_throughput is set 6MB/s temporarily
    if (cache_stat_.cache_load().is_common_io_high_throughput()) {
      available_size = MAX(SS_CLOSE_EVICTION_DIFF_SIZE, available_size - SS_CLOSE_EVICTION_DIFF_SIZE);
    }
    LOG_INFO("get ss_micro_cache space for prewarm", K(available_pct), K(available_size));
  }
  return ret;
}

void ObSSMicroCache::begin_free_space_for_prewarm()
{
  inner_update_free_space_for_prewarm(true/*is_start*/);
}

void ObSSMicroCache::finish_free_space_for_prewarm()
{
  inner_update_free_space_for_prewarm(false/*is_start*/);
}

int ObSSMicroCache::inner_update_free_space_for_prewarm(const bool is_start_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(micro_meta_mgr_.update_arc_work_limit_for_prewarm(is_start_op))) {
    LOG_WARN("fail to update arc_work_limit for prewarm", KR(ret), K(is_start_op));
  }
  return ret;
}

int ObSSMicroCache::get_batch_la_micro_keys(
    ObLS *ls, ObIArray<ObSSMicroBlockCacheKeyMeta> &keys)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(latest_access_micro_key_mgr_.get_batch_micro_keys(ls, keys))) {
    LOG_WARN("fail to get batch micro keys", KR(ret));
  }
  return ret;
}

int ObSSMicroCache::is_tablet_id_need_filter(
    ObLS *ls, const uint64_t tablet_id, bool &is_filter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(latest_access_micro_key_mgr_.is_tablet_id_need_filter(ls, tablet_id, is_filter))) {
    LOG_WARN("fail to judge is tablet id need filter", KR(ret));
  }
  return ret;
}

int ObSSMicroCache::get_phy_block_handle(
    const int64_t phy_blk_idx, 
    ObSSPhysicalBlockHandle &phy_blk_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(phy_blk_mgr_.get_block_handle(phy_blk_idx, phy_blk_handle))) {
    LOG_WARN("fail to get phy_block handle", KR(ret), K(phy_blk_idx));
  }
  return ret;
}

int ObSSMicroCache::get_phy_block_size(int64_t &phy_blk_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else {
    phy_blk_size = phy_block_size_;
  }
  return ret;
}

int ObSSMicroCache::get_total_micro_size(int64_t &total_micro_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else {
    total_micro_size = micro_meta_mgr_.get_arc_info().get_valid_size();
  }
  return ret;
}

int ObSSMicroCache::get_tablet_cache_info(
    const ObTabletID &tablet_id, 
    ObSSTabletCacheInfo &tablet_cache_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    ObSSMicroCacheSuperBlock super_blk;
    if (OB_FAIL(phy_blk_mgr_.get_ss_super_block(super_blk))) {
      LOG_WARN("fail to get ss_super_block", KR(ret));
    } else if (OB_LIKELY(super_blk.is_valid())) {
      if (super_blk.exist_tablet_cache_info_list()) {
        if (OB_FAIL(super_blk.get_tablet_cache_info(tablet_id, tablet_cache_info))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get tablet_cache_info", KR(ret), K(tablet_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMicroCache::get_ls_cache_info(const ObLSID &ls_id, ObSSLSCacheInfo &ls_cache_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    ObSSMicroCacheSuperBlock super_blk;
    if (OB_FAIL(phy_blk_mgr_.get_ss_super_block(super_blk))) {
      LOG_WARN("fail to get ss_super_block", KR(ret));
    } else if (OB_LIKELY(super_blk.is_valid())) {
      // first, try to get ls_cache info from super_block
      if (super_blk.exist_ls_cache_info_list()) {
        if (OB_FAIL(super_blk.get_ls_cache_info(ls_id, ls_cache_info))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get ls_cache_info", KR(ret), K(ls_id));
          }
        }
      } else {
        // if not exist, use average size
        ObLSService *ls_service = MTL(ObLSService *);
        if (OB_NOT_NULL(ls_service)) {
          ObSEArray<ObLSID, 8> ls_ids;
          if (OB_FAIL(ls_service->get_ls_ids(ls_ids))) {
            LOG_WARN("fail to get ls_ids", KR(ret), K(ls_id));
          } else if (ls_ids.count() > 0) {
            const int64_t valid_micro_size = micro_meta_mgr_.get_arc_info().get_valid_size();
            ls_cache_info.ls_id_ = ls_id;
            ls_cache_info.t1_size_ = valid_micro_size / 2 / ls_ids.count();
            ls_cache_info.t2_size_ = valid_micro_size / 2 / ls_ids.count();
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMicroCache::divide_phy_block_range(
    const ObLSID &ls_id, 
    const int64_t split_count,
    ObIArray<ObSSPhyBlockIdxRange> &block_ranges)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(phy_blk_mgr_.divide_cache_data_block_range(ls_id, split_count, block_ranges))) {
    LOG_WARN("fail to divide cache_data_block range", KR(ret), K(ls_id), K(split_count));
  }
  return ret;
}

void ObSSMicroCache::clear_micro_cache()
{
  int ret = OB_SUCCESS;
  if (ATOMIC_BCAS(&is_enabled_, true, false)) {
    FLOG_INFO("start clear micro_cache", K_(tenant_id), KP(this));
    bool do_clear = false;
    if (IS_INIT && !ATOMIC_LOAD(&is_stopped_)) {
      task_runner_.disable_task();

      bool fin_destroy = false;
      do {
        const int64_t flying_req_cnt = ATOMIC_LOAD(&flying_req_cnt_);
        const bool is_task_closed = task_runner_.is_task_closed();
        if (flying_req_cnt > 0 || !is_task_closed) {
          ob_usleep(SLEEP_INTERVAL_US);
          if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_US)) {
            FLOG_WARN("still exist request which may access micro_meta_map", K(flying_req_cnt), K(is_task_closed));
          }
        } else {
          micro_meta_mgr_.clear_micro_meta_manager();
          inner_check_micro_meta_cnt();
          fin_destroy = true;
        }
      } while (!fin_destroy);

      mem_data_mgr_.clear_mem_data_manager();
      phy_blk_mgr_.clear_phy_block_manager();
      cache_stat_.clear_cache_stat_dynamic_info();
      task_runner_.enable_task();
      do_clear = true;
    }
    enable_cache();
    FLOG_INFO("finish clear micro_cache", K_(tenant_id), K(do_clear), KP(this));
  } else {
    FLOG_INFO("already in clear_micro_cache process, please wait", KP(this));
  }
}

/*-----------------------------------------For ob_admin-----------------------------------------*/
void ObSSMicroCache::enable_cache()
{
  ATOMIC_STORE(&is_enabled_, true);
  FLOG_INFO("enable micro cache", K_(tenant_id), K_(is_enabled));
}

void ObSSMicroCache::disable_cache()
{
  ATOMIC_STORE(&is_enabled_, false);
  FLOG_INFO("disable micro cache", K_(tenant_id), K_(is_enabled));
}

bool ObSSMicroCache::is_cache_enabled() const
{
  return ATOMIC_LOAD(&is_enabled_);
}

int ObSSMicroCache::get_phy_block_info(const int64_t phy_blk_idx, ObSSPhysicalBlock& phy_blk_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(phy_blk_mgr_.get_phy_block_info(phy_blk_idx, phy_blk_info))) {
    LOG_WARN("fail to get phy_block info", KR(ret), K(phy_blk_idx));
  }
  return ret;
}

int ObSSMicroCache::get_micro_meta_handle(
    const ObSSMicroBlockCacheKey &micro_key, 
    ObSSMicroBlockMetaHandle &micro_meta_handle)
{
  int ret = OB_SUCCESS;
  SSMicroMetaReqGuard micro_req_guard(flying_req_cnt_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(micro_meta_mgr_.get_micro_meta_handle(micro_key, micro_meta_handle))) {
    LOG_WARN("fail to get micro_meta handle", KR(ret), K(micro_key), K(micro_meta_handle));
  }
  return ret;
}

int ObSSMicroCache::get_micro_cache_info(
    ObSSMicroCacheStat &cache_stat, 
    ObSSMicroCacheSuperBlock &super_blk, 
    ObSSARCInfo &arc_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(phy_blk_mgr_.get_ss_super_block(super_blk))) {
    LOG_WARN("fail to get super_block", KR(ret), K(super_blk));
  } else {
    cache_stat = cache_stat_;
    arc_info = micro_meta_mgr_.get_arc_info();
  }
  return ret;
}

int ObSSMicroCache::clear_micro_meta_by_tablet_id(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t remain_micro_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ss_micro_cache is stopped", KR(ret), K_(is_stopped));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_enabled_))) {
    ret = OB_SS_MICRO_CACHE_DISABLED;
    LOG_WARN("ss_micro_cache is disabled", KR(ret), K_(is_enabled));
  } else if (OB_FAIL(micro_meta_mgr_.clear_tablet_micro_meta(tablet_id, remain_micro_cnt))) {
    LOG_WARN("fail to clear tablet's micro_meta", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(remain_micro_cnt > 0)) {
    // If some micro_blocks are in reorganizing process, they can't be cleared.
    ret = OB_ENTRY_EXIST;
    LOG_WARN("still exists some micro_block_meta", KR(ret), K(remain_micro_cnt), K(tablet_id));
  }
  return ret;
}

void ObSSMicroCache::set_micro_ckpt_compressor_type(const ObCompressorType type)
{
  config_.set_micro_ckpt_compressor_type(type);
}

void ObSSMicroCache::set_blk_ckpt_compressor_type(const ObCompressorType type)
{
  config_.set_blk_ckpt_compressor_type(type);
}

ObCompressorType ObSSMicroCache::get_micro_ckpt_compressor_type() const
{
  return config_.get_micro_ckpt_compressor_type();
}

ObCompressorType ObSSMicroCache::get_blk_ckpt_compressor_type() const
{
  return config_.get_blk_ckpt_compressor_type();
}

} // namespace storage
} // namespace oceanbase
