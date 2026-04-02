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

#include "storage/shared_storage/task/ob_batch_get_kvcache_key_task.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObBatchGetKVcacheKeyTask::ObBatchGetKVcacheKeyTask(volatile bool &is_stop)
  : is_inited_(false), is_stop_(is_stop), rwlock_(), block_cache_keys_(),
    default_once_batch_bucket_num_(0), batch_cnt_per_round_(0), produce_op_cnt_(0),
    consume_op_cnt_(0)
{
  block_cache_keys_.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "BlockCacheKeys"));
}

int ObBatchGetKVcacheKeyTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBatchGetKVcacheKeyTask has already been inited", KR(ret), K_(is_inited));
  } else {
    int64_t kvcache_bucket_num = ObKVGlobalCache::get_instance().get_bucket_num();
    // default_once_batch_bucket_num's max value is 50w
    default_once_batch_bucket_num_ = MIN(kvcache_bucket_num * DEFAULT_ONCE_BATCH_GET_BUCKET_NUM_FACTOR, OB_MAX_BATCH_GET_BUCKET_NUM);
    // default_once_batch_bucket_num's min value is 1w
    default_once_batch_bucket_num_ = MAX(default_once_batch_bucket_num_, ObKVGlobalCache::DEFAULT_ONCE_BATCH_GET_BUCKET_NUM);
    if (default_once_batch_bucket_num_ > 0) {
      batch_cnt_per_round_ = kvcache_bucket_num / default_once_batch_bucket_num_;
    }
    is_inited_ = true;
    LOG_INFO("succ to init sync hot micro key task", K(kvcache_bucket_num), K_(default_once_batch_bucket_num),
      K_(batch_cnt_per_round), K_(is_stop), K_(is_inited));
  }
  return ret;
}

void ObBatchGetKVcacheKeyTask::destroy()
{
  WLockGuard guard(rwlock_);
  is_inited_ = false;
  block_cache_keys_.destroy();
  default_once_batch_bucket_num_ = 0;
  batch_cnt_per_round_ = 0;
  produce_op_cnt_ = 0;
  consume_op_cnt_ = 0;
}

void ObBatchGetKVcacheKeyTask::runTimerTask()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (is_stop_) {
    // do nothing
    block_cache_keys_.reset();
  } else if (OB_FAIL(bacth_get_micro_keys_from_kvcache())) {
    LOG_WARN("fail to batch get micro keys from kvcache", KR(ret));
  }
  if (TC_REACH_TIME_INTERVAL(CHECK_LS_REPLICA_PREWARM_STOP_INTERVAL_US)) {
    if (OB_TMP_FAIL(check_ls_replica_prewarm_stop())) {
      LOG_WARN("fail to check ls replica prewarm stop", KR(tmp_ret));
    }
  }
}

int ObBatchGetKVcacheKeyTask::bacth_get_micro_keys_from_kvcache()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    const int64_t batch_get_count = default_once_batch_bucket_num_ / ObKVGlobalCache::DEFAULT_ONCE_BATCH_GET_BUCKET_NUM;
    ObSEArray<ObMicroBlockCacheKey, OB_DEFAULT_ARRAY_CAPACITY> block_cache_keys;
    block_cache_keys.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "BlockCacheKeys"));
    for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < batch_get_count; i++) {
      ObArray<ObMicroBlockCacheKey> batch_block_cache_keys;
      batch_block_cache_keys.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "BlockCacheKeys"));
      if (OB_FAIL(ObKVGlobalCache::get_instance().get_batch_data_block_cache_key(batch_block_cache_keys))) {
        LOG_WARN("fail to get batch data block cache key", KR(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && !is_stop_ && j < batch_block_cache_keys.count(); j++) {
        if (OB_FAIL(block_cache_keys.push_back(batch_block_cache_keys.at(j)))) {
          LOG_WARN("fail to push", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // finally lock block_cache_keys_ when block_cache_keys_ assign block_cache_keys
      // reduce lock block_cache_keys_ time
      WLockGuard guard(rwlock_);
      block_cache_keys_.reset();
      if (OB_FAIL(block_cache_keys_.assign(block_cache_keys))) {
        LOG_WARN("fail to assign", KR(ret), K(block_cache_keys.count()));
      } else if (produce_op_cnt_ < batch_cnt_per_round_) {
        ++produce_op_cnt_;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ to batch get micro keys from kvcache", K_(default_once_batch_bucket_num),
               "block_cache_keys_cnt", block_cache_keys_.count(),
               "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObBatchGetKVcacheKeyTask::get_micro_block_cache_keys(ObIArray<ObMicroBlockCacheKey> &keys, bool &full_scan)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (is_stop_) {
    // do nothing
  } else if (OB_FAIL(keys.assign(block_cache_keys_))) {
    LOG_WARN("fail to assign", KR(ret), "block_cache_keys_cnt", block_cache_keys_.count());
  } else {
    ATOMIC_INC(&consume_op_cnt_);
    if (produce_op_cnt_ >= batch_cnt_per_round_) {
      // consume_op_cnt should be close to batch_cnt_per_round * ls_count
      LOG_INFO("finish the entire round batch_get_keys", K_(batch_cnt_per_round), K_(consume_op_cnt));
      full_scan = true;
      produce_op_cnt_ = 0;
      ATOMIC_STORE(&consume_op_cnt_, 0);
    }
  }
  return ret;
}

int ObBatchGetKVcacheKeyTask::check_ls_replica_prewarm_stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    common::ObArray<uint64_t> tenant_ids;
    omt::ObMultiTenant *omt = GCTX.omt_;
    if (OB_ISNULL(omt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, omt is nullptr", KR(ret));
    } else if (OB_FAIL(omt->get_mtl_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get_mtl_tenant_ids", KR(ret));
    }
    bool is_ls_replica_prewarm_stop = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      MTL_SWITCH(tenant_id) {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
        if (OB_UNLIKELY(!tenant_config.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", KR(ret));
        } else if (tenant_config->_enable_ss_replica_prewarm) {
          is_ls_replica_prewarm_stop = false;
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_stop_ = is_ls_replica_prewarm_stop;
      LOG_INFO("succ to check ls prewarm stop", K_(is_stop));
    }
  }
  return ret;
}

} // storage
} // oceanbase
