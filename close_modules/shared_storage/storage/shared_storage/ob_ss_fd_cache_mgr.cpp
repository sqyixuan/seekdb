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

#include "storage/shared_storage/ob_ss_fd_cache_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_ss_file_util.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "lib/thread/thread_mgr.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::lib;
using namespace oceanbase::share;

/********************************** ObSSFdCacheEvictTask *********************************/
ObSSFdCacheEvictTask::ObSSFdCacheEvictTask(volatile bool &is_stop)
  : is_inited_(false), is_stop_(is_stop), fd_cache_mgr_(nullptr)
{
}

ObSSFdCacheEvictTask::~ObSSFdCacheEvictTask()
{
  destroy();
}

int ObSSFdCacheEvictTask::init(const int tg_id, ObSSFdCacheMgr *fd_cache_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init fd cache evict task twice", KR(ret));
  } else if (OB_UNLIKELY(-1 == tg_id) || OB_ISNULL(fd_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tg_id), KP(fd_cache_mgr));
  } else if (FALSE_IT(fd_cache_mgr_ = fd_cache_mgr)) {
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule fd cache evict task", KR(ret), K(tg_id));
  } else {
    is_inited_ = true;
  }
  LOG_INFO("finish to init fd cache evict task", KR(ret), K(tg_id));
  return ret;
}

void ObSSFdCacheEvictTask::destroy()
{
  fd_cache_mgr_ = nullptr;
  is_inited_ = false;
}

void ObSSFdCacheEvictTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fd cache evict task is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stop_)) {
    LOG_INFO("fd cache evict task is stopped");
  } else if (OB_ISNULL(fd_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fd cache mgr should not be null", KR(ret));
  } else if (OB_FAIL(fd_cache_mgr_->do_evict_work())) {
    LOG_WARN("fail to do evict work", KR(ret));
  }
}


/********************************** ObSSFdCacheMgr *********************************/
ObSSFdCacheMgr::ObSSFdCacheMgr()
  : is_inited_(false), is_stop_(false), fd_map_(), fd_cache_node_pool_(), evict_task_(is_stop_)
{
}

ObSSFdCacheMgr::~ObSSFdCacheMgr()
{
  destroy();
}

int ObSSFdCacheMgr::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "FdCacheMgr");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fd cache mgr is already inited", KR(ret));
  } else if (OB_FAIL(fd_map_.create(BUCKET_NUM, attr))) {
    LOG_WARN("fail to create map", KR(ret), LITERAL_K(BUCKET_NUM));
  } else if (OB_FAIL(fd_cache_node_pool_.init(MAX_FD_CACHE_CNT, "SharedFdPool", MTL_ID()))) {
    LOG_WARN("fail to init fd cache node pool", KR(ret), LITERAL_K(MAX_FD_CACHE_CNT));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSFdCacheMgr::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fd cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tg id", KR(ret), K(tg_id));
  } else if (OB_FAIL(evict_task_.init(tg_id, this))) {
    LOG_WARN("fail to init evict task", KR(ret), K(tg_id));
  }
  return ret;
}

void ObSSFdCacheMgr::stop()
{
  LOG_INFO("start to stop fd cache mgr");
  is_stop_ = true;
  LOG_INFO("finish to stop fd cache mgr");
}

void ObSSFdCacheMgr::wait()
{
  // do nothing
}

void ObSSFdCacheMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    const int64_t start_us = ObTimeUtility::fast_current_time();
    LOG_INFO("start to destroy fd cache mgr");
    fd_map_.destroy();
    // to destroy fd_cache_node_pool safely, wait all fd_cache_node memory are free
    while (fd_cache_node_pool_.get_alloc_count() != fd_cache_node_pool_.get_free_count()) {
      if (TC_REACH_TIME_INTERVAL(10 * 1000L * 1000L)) { // 10s
        LOG_INFO("wait fd cache node free", "alloc_count", fd_cache_node_pool_.get_alloc_count(),
                 "free_count", fd_cache_node_pool_.get_free_count());
      }
      ob_usleep(500 * 1000L); // 500ms
    }
    fd_cache_node_pool_.destroy();
    evict_task_.destroy();
    is_stop_ = true;
    is_inited_ = false;
    const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
    LOG_INFO("finish to destroy fd cache mgr", K(cost_us));
  }
}

int ObSSFdCacheMgr::get_fd(const MacroBlockId &macro_id, ObSSFdCacheHandle &fd_cache_handle)
{
  int ret = OB_SUCCESS;
  fd_cache_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fd cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("fd cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id));
  } else {
    ObSSFdCacheKey fd_cache_key(macro_id);
    bool is_hit = false;
    if (OB_FAIL(try_get_cache(fd_cache_key, fd_cache_handle, is_hit))) {
      LOG_WARN("fail to try get cache", KR(ret), K(fd_cache_key));
    } else if (!is_hit) {
      if (OB_FAIL(open_and_try_add_cache(fd_cache_key, fd_cache_handle))) {
        LOG_WARN("fail to try add cache", KR(ret), K(fd_cache_key));
      }
    }
  }
  return ret;
}

int ObSSFdCacheMgr::check_fd_cache_exist(const MacroBlockId &macro_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fd cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("fd cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id));
  } else {
    ObSSFdCacheKey fd_cache_key(macro_id);
    ObSSFdCacheHandle fd_cache_handle;
    if (OB_FAIL(try_get_cache(fd_cache_key, fd_cache_handle, is_exist))) {
      LOG_WARN("fail to try get cache", KR(ret), K(fd_cache_key));
    }
  }
  return ret;
}

int ObSSFdCacheMgr::erase_fd_cache_when_del_file(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fd cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("fd cache mgr is stopped", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(macro_id));
  } else {
    ObSSFdCacheKey fd_cache_key(macro_id);
    if (OB_FAIL(fd_map_.erase_refactored(fd_cache_key))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // not in fd cache, ignore ret
      } else {
        LOG_WARN("fail to erase refactored", KR(ret), K(fd_cache_key));
      }
    } else {
      LOG_TRACE("ss_fd_cache: evict fd cache when delete file", K(fd_cache_key));
    }
  }
  return ret;
}

int ObSSFdCacheMgr::try_get_cache(
    const ObSSFdCacheKey &fd_cache_key,
    ObSSFdCacheHandle &fd_cache_handle,
    bool &is_hit)
{
  int ret = OB_SUCCESS;
  is_hit = false;
  if (OB_UNLIKELY(!fd_cache_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fd cache key", KR(ret), K(fd_cache_key));
  } else if (OB_FAIL(fd_map_.get_refactored(fd_cache_key, fd_cache_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // cache miss, ignore ret
      LOG_TRACE("ss_fd_cache: fd cache miss", K(fd_cache_key));
    } else {
      LOG_WARN("fail to get refactored", KR(ret), K(fd_cache_key));
    }
  } else if (OB_UNLIKELY(!fd_cache_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fd cache handle is invalid", KR(ret), K(fd_cache_key));
  } else {
    is_hit = true;
    fd_cache_handle.update_timestamp_us();
    LOG_TRACE("ss_fd_cache: fd cache hit", K(fd_cache_key), "fd", fd_cache_handle.get_fd());
  }
  return ret;
}

int ObSSFdCacheMgr::open_and_try_add_cache(
    const ObSSFdCacheKey &fd_cache_key,
    ObSSFdCacheHandle &fd_cache_handle)
{
  int ret = OB_SUCCESS;
  fd_cache_handle.reset();
  ObSSFdCacheNode *fd_cache_node = nullptr;
  int fd = OB_INVALID_FD;
  if (OB_UNLIKELY(!fd_cache_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fd cache key", KR(ret), K(fd_cache_key));
  } else if (OB_FAIL(open_fd(fd_cache_key, fd))) {
    LOG_WARN("fail to open fd", KR(ret), K(fd_cache_key));
  } else if (OB_FAIL(fd_cache_node_pool_.alloc(fd_cache_node))) {
    LOG_WARN("fail to alloc fd cache node", KR(ret));
  } else if (OB_ISNULL(fd_cache_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fd cache node alloc from pool is null", KR(ret));
  } else if (FALSE_IT(fd_cache_node->reset())) {
  } else if (FALSE_IT(fd_cache_node->key_ = fd_cache_key)) {
  } else if (FALSE_IT(fd_cache_node->fd_ = fd)) {
  } else if (FALSE_IT(fd_cache_node->timestamp_us_ = ObTimeUtility::fast_current_time())) {
  } else if (OB_FAIL(fd_cache_handle.set_fd_cache_node(fd_cache_node, &fd_cache_node_pool_))) {
    LOG_WARN("fail to set fd cache node", KR(ret), KPC(fd_cache_node), KP_(&fd_cache_node_pool));
  } else if (fd_map_.size() > MAX_FD_CACHE_CNT) { // ignore concurrency, tollerate exceeds MAX_FD_CACHE_CNT
    LOG_TRACE("ss_fd_cache: reach fd cache max cnt", LITERAL_K(MAX_FD_CACHE_CNT));
  } else if (OB_FAIL(fd_map_.set_refactored(fd_cache_key, fd_cache_handle))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret, other thread already add cache
    } else {
      LOG_WARN("fail to set refactored", KR(ret), K(fd_cache_key), K(fd_cache_handle));
    }
  } else {
    LOG_TRACE("ss_fd_cache: add fd cache", K(fd_cache_key), K(fd));
  }

  // if has not set fd_cache_node into fd_cache_handle, need to close fd and free memory
  if (OB_FAIL(ret) && !fd_cache_handle.is_valid()) {
    if (OB_INVALID_FD != fd) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSFileUtil::close(fd))) {
        LOG_ERROR("fail to close fd", KR(tmp_ret), K(fd));
      }
    }
    if (OB_NOT_NULL(fd_cache_node)) {
      int tmp_ret = OB_SUCCESS;
      fd_cache_node->reset();
      if (OB_TMP_FAIL(fd_cache_node_pool_.free(fd_cache_node))) {
        LOG_ERROR("fail to free fd cache node", KR(tmp_ret), KPC(fd_cache_node));
      }
    }
  }
  return ret;
}

int ObSSFdCacheMgr::open_fd(const ObSSFdCacheKey &fd_cache_key, int &fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd_cache_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fd cache key", KR(ret), K(fd_cache_key));
  } else {
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(fd_cache_key.macro_id_, 0/*ls_epoch_id*/, true/*is_local_cache*/))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else {
      int open_flag = O_RDONLY | O_DIRECT;
      mode_t open_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
      if (OB_FAIL(ObSSFileUtil::open(ctx.get_path(), open_flag, open_mode, fd))) {
        LOG_WARN("fail to open", KR(ret), K(ctx));
      }
    }
  }
  return ret;
}

int ObSSFdCacheMgr::do_evict_work()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::fast_current_time();
  int64_t evict_from_high_to_low_cost_us = 0;
  int64_t evict_long_time_no_use_cost_us = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fd cache mgr is not inited", KR(ret));
  } else if (fd_map_.size() > EVICT_THRESHOLD_HIGH) {
    const int64_t batch_cnt = 3;
    ObSEArray<ObSSFdCacheHandle, batch_cnt> fd_cache_handles;
    int64_t loop_cnt = 0; // to avoid dead loop
    while (OB_SUCC(ret) && !is_stop_ && (fd_map_.size() > EVICT_THRESHOLD_LOW) &&
           (loop_cnt < (EVICT_THRESHOLD_HIGH - EVICT_THRESHOLD_LOW))) {
      fd_cache_handles.reuse();
      if (OB_FAIL(batch_get_handles_randomly(batch_cnt, fd_cache_handles))) {
        LOG_WARN("fail to batch get handles randomly", KR(ret), K(batch_cnt));
      } else if (fd_cache_handles.empty()) {
        LOG_TRACE("none sampled fd cache meets evict condition");
      } else if (OB_FAIL(evict_handle_with_min_timestamp(fd_cache_handles))) {
        LOG_WARN("fail to evict handle with min timestamp", KR(ret), K(fd_cache_handles));
      }
      loop_cnt++;
    }
    evict_from_high_to_low_cost_us = ObTimeUtility::fast_current_time() - start_us;
  } else if (TC_REACH_TIME_INTERVAL(MAX_RESERVED_TIME_US)) {
    if (OB_FAIL(evict_long_time_no_use_fd_cache())) {
      LOG_WARN("fail to evict long time no use fd cache", KR(ret));
    }
    evict_long_time_no_use_cost_us = ObTimeUtility::fast_current_time() - start_us;
  }
  const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
  if (OB_UNLIKELY(cost_us > 5 * 1000L * 1000L)) { // 5s
    LOG_WARN("do evict work cost too much time", K(cost_us), K(evict_from_high_to_low_cost_us),
             K(evict_long_time_no_use_cost_us));
  }
  return ret;
}

int ObSSFdCacheMgr::batch_get_handles_randomly(
    const int64_t batch_cnt,
    ObIArray<ObSSFdCacheHandle> &fd_cache_handles)
{
  int ret = OB_SUCCESS;
  int64_t handle_cnt = 0;
  int64_t sample_bucket_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fd cache mgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(batch_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(batch_cnt));
  } else {
    const int64_t bucket_count = fd_map_.bucket_count();
    while (OB_SUCC(ret) && (handle_cnt < batch_cnt) && (sample_bucket_cnt < batch_cnt * 2)) {
      // 1. sample one random bucket
      // Note: randomly sampled buckets may be the same, but this does not affect correctness
      FdMap::bucket_iterator bucket_iter;
      const int64_t bucket_pos = ObRandom::rand(0, bucket_count - 1);
      if (OB_FAIL(fd_map_.get_bucket_iterator(bucket_pos, bucket_iter))) {
        LOG_WARN("fail to get bucket iterator", KR(ret), K(bucket_pos));
      } else if (OB_UNLIKELY(bucket_iter == fd_map_.bucket_end())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bucket iter should not be bucket end", KR(ret));
      } else {
        // 2. get node from the random bucket
        FdMap::hashtable::bucket_lock_cond blk(*bucket_iter);
        FdMap::hashtable::readlocker locker(blk.lock());
        FdMap::hashtable::hashbucket::iterator node_iter = bucket_iter->node_begin();
        while (OB_SUCC(ret) && (handle_cnt < batch_cnt) && (node_iter != bucket_iter->node_end())) {
          if (OB_UNLIKELY(!node_iter->second.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fd cache handle should not be invalid", KR(ret));
          } else if (OB_FAIL(fd_cache_handles.push_back(node_iter->second))) {
            LOG_WARN("fail to push back fd cache handle", KR(ret));
          } else {
            handle_cnt++;
          }
          node_iter++;
        }
      }
      sample_bucket_cnt++;
    }
  }
  return ret;
}

int ObSSFdCacheMgr::evict_handle_with_min_timestamp(ObIArray<ObSSFdCacheHandle> &fd_cache_handles)
{
  int ret = OB_SUCCESS;
  ObSSFdCacheKey evict_key;
  int evict_fd = OB_INVALID_FD;
  if (OB_UNLIKELY(fd_cache_handles.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fd cache handles is empty", KR(ret));
  } else {
    // 1. find fd cache handle with min timestamp
    int64_t min_timestamp_handle_idx = 0;
    int64_t min_timestamp_us = fd_cache_handles.at(0).get_timestamp_us();
    const int64_t fd_cache_handle_cnt = fd_cache_handles.count();
    for (int64_t i = 1; (OB_SUCC(ret) && (i < fd_cache_handle_cnt)); ++i) {
      const int64_t cur_handle_timestamp_us = fd_cache_handles.at(i).get_timestamp_us();
      if (cur_handle_timestamp_us < min_timestamp_us) {
        min_timestamp_handle_idx = i;
        min_timestamp_us = cur_handle_timestamp_us;
      }
    }
    // 2. evict fd cache handle with min timestamp, if last access interval exceeds MIN_RESERVED_TIME_US
    ObSSFdCacheHandle &fd_cache_handle = fd_cache_handles.at(min_timestamp_handle_idx);
    const int64_t last_access_interval_us = ObTimeUtility::fast_current_time() - min_timestamp_us;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!fd_cache_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fd cache handle should not be invalid", KR(ret));
    } else if (OB_UNLIKELY(!fd_cache_handle.get_fd_cache_node()->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fd cache node should not be invalid", KR(ret));
    } else if (FALSE_IT(evict_key = fd_cache_handle.get_fd_cache_node()->key_)) {
    } else if (FALSE_IT(evict_fd = fd_cache_handle.get_fd_cache_node()->fd_)) {
    } else if (last_access_interval_us < MIN_RESERVED_TIME_US) {
      LOG_TRACE("ss_fd_cache: reserve fd cache within min reserved time", K(evict_key), K(evict_fd),
                K(last_access_interval_us), LITERAL_K(MIN_RESERVED_TIME_US));
    } else if (OB_FAIL(fd_map_.erase_refactored(evict_key))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret, already erased by foreground delete_local_file
      } else {
        LOG_WARN("fail to erase refactored", KR(ret), K(evict_key));
      }
    } else {
      LOG_TRACE("ss_fd_cache: evict fd cache when reach EVICT_THRESHOLD_HIGH", K(evict_key), K(evict_fd));
    }
  }
  return ret;
}

int ObSSFdCacheMgr::evict_long_time_no_use_fd_cache()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fd cache mgr is not inited", KR(ret));
  } else {
    ObSEArray<ObSSFdCacheKey, 4> evict_keys;
    evict_keys.set_attr(ObMemAttr(MTL_ID(), "FdCacheMgr"));
    FdMap::bucket_iterator bucket_iter = fd_map_.bucket_begin();
    while ((bucket_iter != fd_map_.bucket_end()) && OB_SUCC(ret) &&
           (fd_map_.size() > EVICT_THRESHOLD_RESERVED)) {
      evict_keys.reuse();
      { // bucket read lock begin
        FdMap::hashtable::bucket_lock_cond blk(*bucket_iter);
        FdMap::hashtable::readlocker locker(blk.lock());
        FdMap::hashtable::hashbucket::iterator node_iter = bucket_iter->node_begin();
        while (node_iter != bucket_iter->node_end()) {
          ObSSFdCacheHandle &cur_fd_cache_handle = node_iter->second;
          if (OB_UNLIKELY(!cur_fd_cache_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fd cache handle should not be invalid", KR(ret));
          } else if (OB_UNLIKELY(!cur_fd_cache_handle.get_fd_cache_node()->is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fd cache node should not be invalid", KR(ret));
          } else if ((ObTimeUtility::fast_current_time() - cur_fd_cache_handle.get_timestamp_us())
                     > MAX_RESERVED_TIME_US) {
            ObSSFdCacheKey &evict_key = cur_fd_cache_handle.get_fd_cache_node()->key_;
            if (OB_FAIL(evict_keys.push_back(evict_key))) {
              LOG_WARN("fail to push back", KR(ret), K(evict_key));
            }
          }
          node_iter++;
        }
      } // bucket read lock end

      for (int64_t i = 0; OB_SUCC(ret) && (i < evict_keys.count()); ++i) {
        if (OB_FAIL(fd_map_.erase_refactored(evict_keys.at(i)))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS; // ignore ret, already erased by foreground delete_local_file
          } else {
            LOG_WARN("fail to erase refactored", KR(ret), "fd_cache_key", evict_keys.at(i));
          }
        } else {
          LOG_TRACE("ss_fd_cache: evict fd cache which is long time no use", "evict_key", evict_keys.at(i));
        }
      }
      ++bucket_iter;
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase
