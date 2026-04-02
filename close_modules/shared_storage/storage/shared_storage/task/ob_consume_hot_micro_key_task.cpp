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

#include "ob_consume_hot_micro_key_task.h"
#include "storage/shared_storage/prewarm/ob_ss_micro_cache_prewarm_service.h"

namespace oceanbase
{
namespace storage
{

ObConsumeHotMicroKeyTask::ObConsumeHotMicroKeyTask(volatile bool &is_stop)
  : is_inited_(false), is_stop_(is_stop),
    micro_cache_key_groups_(),
    fetch_micro_key_workers_(0)
{
}

int ObConsumeHotMicroKeyTask::init(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObConsumeHotMicroKeyTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    is_inited_ = true;
    LOG_INFO("succ to init consume hot micro key task", K(ls_id), K_(fetch_micro_key_workers), K_(is_stop), K_(is_inited));
  }
  return ret;
}

void ObConsumeHotMicroKeyTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  reset_group();
  ATOMIC_STORE(&fetch_micro_key_workers_, 0);
}

void ObConsumeHotMicroKeyTask::reset_group()
{
  int ret = OB_SUCCESS;
  const int64_t queue_size = micro_cache_key_groups_.size();
  ObLink *ptr = nullptr;
  ObSyncHotMicroKeyArgNode *node = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && (i < queue_size); i++) {
    if (OB_FAIL(micro_cache_key_groups_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to pop", KR(ret));
      }
    } else {
      node = static_cast<ObSyncHotMicroKeyArgNode *>(ptr);
      OB_DELETE(ObSyncHotMicroKeyArgNode, attr, node);
    }
  }
}

int ObConsumeHotMicroKeyTask::push_group(const obrpc::ObLSSyncHotMicroKeyArg &arg)
{
  int ret = OB_SUCCESS;
  ObSyncHotMicroKeyArgNode *node = nullptr;
  ObMemAttr attr(MTL_ID(), "MicroKeyGroup");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(node = OB_NEW(ObSyncHotMicroKeyArgNode, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else if (OB_FAIL(node->arg_.assign(arg))) {
    LOG_WARN("fail to assign", KR(ret));
  } else if (OB_FAIL(micro_cache_key_groups_.push(node))) {
    LOG_WARN("fail to ", KR(ret), K(arg));
  }
  // free memory on fail
  if (OB_FAIL(ret)) {
    OB_DELETE(ObSyncHotMicroKeyArgNode, attr, node);
  }
  return ret;
}

int ObConsumeHotMicroKeyTask::pop_group(obrpc::ObLSSyncHotMicroKeyArg &arg)
{
  int ret = OB_SUCCESS;
  ObLink *ptr = nullptr;
  ObSyncHotMicroKeyArgNode *node = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(micro_cache_key_groups_.pop(ptr))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to pop", KR(ret));
    }
  } else if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sync hot micro key is null", KR(ret));
  } else if (FALSE_IT(node = static_cast<ObSyncHotMicroKeyArgNode *>(ptr))) {
  } else if (OB_FAIL(arg.assign(node->arg_))) {
    LOG_WARN("fail to assign", KR(ret));
  }
  // free memory
  OB_DELETE(ObSyncHotMicroKeyArgNode, attr, node);
  return ret;
}

void ObConsumeHotMicroKeyTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret));
  } else if (is_stop_ || !tenant_config->_enable_ss_replica_prewarm) {
    // do nothing
    reset_group();
  } else if (OB_FAIL(consume_hot_micro_keys())) {
    LOG_WARN("fail to consume hot micro keys", KR(ret));
  }
}

int ObConsumeHotMicroKeyTask::consume_hot_micro_keys()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    ObSSMicroCachePrewarmService *prewarm_service = nullptr;
    while (OB_SUCC(ret) && !is_stop_ && micro_cache_key_groups_.size() > 0 &&
           (get_fetch_micro_key_workers() < MAX_MICRO_CACHE_KEY_GROUP_COUNT)) {
      obrpc::ObLSSyncHotMicroKeyArg arg;
      ObSEArray<ObSSMicroBlockCacheKeyMeta, OB_DEFAULT_ARRAY_CAPACITY> missed_micro_keys;
      missed_micro_keys.set_attr(ObMemAttr(MTL_ID(), "MissedKeys"));
      if (OB_FAIL(pop_group(arg))) {
        LOG_WARN("fail to pop group", KR(ret), K(arg));
      } else if (OB_FAIL(get_missed_micro_keys(arg.micro_keys_, missed_micro_keys))) {
        LOG_WARN("fail to get missed micro keys", KR(ret));
      } else if (missed_micro_keys.empty()) {
        // do nothing
      } else {
        ATOMIC_INC(&fetch_micro_key_workers_);
        if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ss micro cache prewarm service is null", KR(ret));
        } else if (OB_FAIL(prewarm_service->get_replica_prewarm_handler().push_task(is_stop_,
                           arg.leader_addr_, arg.tenant_id_, arg.ls_id_, missed_micro_keys,
                           fetch_micro_key_workers_))) {
          LOG_WARN("fail to push task", KR(ret), K(arg), K(missed_micro_keys), K_(fetch_micro_key_workers));
        } else {
          LOG_INFO("succ to push fetch micro keys task", K(arg),
                   "missed_micro_keys_cnt", missed_micro_keys.count(),
                   "micro_cache_key_groups_size", micro_cache_key_groups_.size());
        }
        if (OB_FAIL(ret)) {
          ATOMIC_DEC(&fetch_micro_key_workers_);
        }
      }
    }
  }
  return ret;
}

int ObConsumeHotMicroKeyTask::get_missed_micro_keys(const ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_keys,
                                                    ObIArray<ObSSMicroBlockCacheKeyMeta> &missed_micro_keys)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret));
  }
  ObArray<ObSSMicroBlockCacheKey> update_heat_micro_keys;
  update_heat_micro_keys.set_attr(ObMemAttr(MTL_ID(), "HeatCacheKeys"));
  for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < micro_keys.count(); i++) {
    const ObSSMicroBlockCacheKey &cur_key = micro_keys.at(i).micro_key_;
    ObSSMicroBaseInfo micro_info;
    ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
    if (OB_FAIL(micro_cache->check_micro_block_exist(cur_key, micro_info, hit_type))) {
      LOG_WARN("fail to check micro block exist", KR(ret), K(cur_key));
    } else if (ObSSCacheHitType::SS_CACHE_MISS == hit_type) { // ls follower only fetch missed micro cache key
      if (OB_FAIL(missed_micro_keys.push_back(micro_keys.at(i)))) {
        LOG_WARN("fail to push back", KR(ret), "micro_key", micro_keys.at(i));
      }
    } else if (OB_FAIL(update_heat_micro_keys.push_back(cur_key))) {  // if cache hit, need update micro key heat
       LOG_WARN("fail to push back", KR(ret), K(cur_key));
    }
  }
  // update follower's exist micro_key heat
  if (OB_FAIL(ret)) {
  } else if (update_heat_micro_keys.empty()) {
    // do nothing
  } else if (OB_FAIL(micro_cache->update_micro_block_heat(update_heat_micro_keys, true/*need to transfer T1 -> T2*/,
                                                          true/*need to update access_time to current_time*/))) {
    LOG_WARN("fail to update micro block heat", KR(ret));
  }
  return ret;
}

int ObConsumeHotMicroKeyTask::push_micro_cache_keys(const obrpc::ObLSSyncHotMicroKeyArg &arg)
{
  int ret = OB_SUCCESS;
  ObSyncHotMicroKeyArgNode *node = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (is_stop_) {
    //do nothing
  } else if (micro_cache_key_groups_.size() >= MAX_MICRO_CACHE_KEY_GROUP_COUNT) {
    // do nothing, micro_cache_key_groups_ has been reached max capacity, current micro_cache_keys throw away
    LOG_INFO("micro cache key group has been reached max capacity", K(arg),
             "micro_cache_key_groups_size", micro_cache_key_groups_.size());
  } else if (OB_FAIL(push_group(arg))) {
    LOG_WARN("fail to push group", KR(ret), K(arg));
  } else {
    LOG_INFO("succ to push micro cache keys", K(arg),
              "micro_cache_key_groups_size", micro_cache_key_groups_.size());
  }
  return ret;
}

} // storage
} // oceanbase
