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

#include "ob_sync_hot_micro_key_task.h"
#include "storage/ls/ob_ls.h"
#include "storage/shared_storage/prewarm/ob_ss_micro_cache_prewarm_service.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObSyncHotMicroKeyTask::ObSyncHotMicroKeyTask(volatile bool &is_stop)
  : is_inited_(false), is_stop_(is_stop), ls_(nullptr), ls_id_(ObLSID::INVALID_LS_ID)
{
}

int ObSyncHotMicroKeyTask::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSyncHotMicroKeyTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(ls));
  } else {
    ls_ = ls;
    ls_id_ = ls->get_ls_id().id();
    is_inited_ = true;
    LOG_INFO("succ to init sync hot micro key task", K_(is_stop), K_(ls_id), K_(is_inited));
  }
  return ret;
}

void ObSyncHotMicroKeyTask::destroy()
{
  is_inited_ = false;
  is_stop_ = true;
  ls_ = nullptr;
  ls_id_ = ObLSID::INVALID_LS_ID;
}

void ObSyncHotMicroKeyTask::runTimerTask()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret));
  } else if (is_stop_ || !tenant_config->_enable_ss_replica_prewarm) {
    // do nothing
  } else if (OB_FAIL(sync_hot_micro_key())) {
    LOG_WARN("fail to sync hot micro key", KR(ret));
  }
}

int ObSyncHotMicroKeyTask::sync_hot_micro_key()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    const uint64_t tenant_id = ls_->get_tenant_id();
    ObHashSet<ObSSMicroBlockCacheKeyMeta> micro_key_set;
    if (OB_FAIL(micro_key_set.create(ObSSMicroCachePrewarmService::OB_BATCH_MICRO_KEY_BUCKET_NUM, ObMemAttr(tenant_id, "MicroKeySet")))) {
      LOG_WARN("create block id set failed", KR(ret));
    } else if (OB_FAIL(get_batch_micro_block_cache_keys(micro_key_set))) {
      LOG_WARN("fail to get batch micro block cache keys", KR(ret));
    } else {
      obrpc::ObLSSyncHotMicroKeyArg arg;
      arg.leader_addr_ = GCTX.self_addr();
      arg.ls_id_ = ls_id_;
      arg.tenant_id_ = tenant_id;
      const int64_t reserved_serialize_size = arg.get_reserved_serialize_size();
      int64_t total_key_serialize_size = 0;
      // send micro_cache_keys in batches of 2MB
      ObHashSet<ObSSMicroBlockCacheKeyMeta>::const_iterator cur_iter = micro_key_set.begin();
      while (OB_SUCC(ret) && !is_stop_ && (cur_iter != micro_key_set.end())) {
        const int64_t cur_key_serialize_size = cur_iter->first.get_serialize_size();
        if ((reserved_serialize_size + total_key_serialize_size + cur_key_serialize_size) > OB_MALLOC_BIG_BLOCK_SIZE) {
          if (OB_FAIL(send_sync_hot_micro_key_rpc(arg))) {
            LOG_WARN("fail to send rpc", KR(ret), K(arg));
          } else {
            arg.micro_keys_.reset();
            total_key_serialize_size = 0;
          }
        } else if (OB_FAIL(arg.micro_keys_.push_back(cur_iter->first))) {
          LOG_WARN("fail to push back", KR(ret), "micro_key", cur_iter->first);
        } else {
          total_key_serialize_size += cur_key_serialize_size;
        }
        cur_iter++;
      }
      // process residual micro keys
      if (OB_SUCC(ret) && !arg.micro_keys_.empty()) {
        if (OB_FAIL(send_sync_hot_micro_key_rpc(arg))) {
          LOG_WARN("fail to send rpc", KR(ret), K(arg));
        }
      }
    }
  }
  return ret;
}

int ObSyncHotMicroKeyTask::get_batch_micro_block_cache_keys(ObHashSet<ObSSMicroBlockCacheKeyMeta> &micro_key_set)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_batch_micro_keys_from_kvcache(micro_key_set))) {
    LOG_WARN("fail to get batch micro keys from kvcache", KR(ret));
  } else if (OB_FAIL(get_batch_micro_keys_from_sscache(micro_key_set))) {
    LOG_WARN("fail to get batch micro keys from ss_micro_cache", KR(ret));
  } else {
    LOG_INFO("succ to get batch micro block cache keys", "micro_keys_set_size", micro_key_set.size(), K_(ls_id));
  }
  return ret;
}

int ObSyncHotMicroKeyTask::get_batch_micro_keys_from_kvcache(ObHashSet<ObSSMicroBlockCacheKeyMeta> &micro_key_set)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObSEArray<ObMicroBlockCacheKey, OB_DEFAULT_ARRAY_CAPACITY> block_cache_keys;
    block_cache_keys.set_attr(ObMemAttr(MTL_ID(), "BlockCacheKeys"));
    bool full_scan_block_cache = false;
    // step 1: get batch data block cache keys from KVCache
    if (OB_UNLIKELY(tenant_id != ls_->get_tenant_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_id is unexpected", KR(ret), K(tenant_id), "ls_tenant_id", ls_->get_tenant_id());
    } else if (OB_FAIL(OB_LS_PREWARM_MGR.get_micro_block_cache_keys(block_cache_keys, full_scan_block_cache))) {
      LOG_WARN("fail to get batch data block cache key", KR(ret));
    }
    // step 2: filter ObMicroBlockCacheKey by tenant_id/object_type/tablet_id
    ObArray<ObSSMicroBlockCacheKey> update_heat_micro_keys;
    update_heat_micro_keys.set_attr(ObMemAttr(MTL_ID(), "HeatCacheKeys"));
    int64_t lack_cnt = 0; // the count of micro_blocks which only exist in kvcache, not in ss_micro_cache
    for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < block_cache_keys.count(); i++) {
      const ObMicroBlockCacheKey &block_cache_key = block_cache_keys.at(i);
      if (tenant_id == block_cache_key.get_tenant_id()) { // filter by tenant id
        bool is_filter = false;
        ObSSMicroBlockCacheKeyMeta micro_meta;
        // filter ObMicroBlockCacheKey by object_type and tablet_id
        if (OB_FAIL(filter_micro_block_cache_key(block_cache_key, is_filter, micro_meta, lack_cnt))) {
          LOG_WARN("filter micro block cache key", KR(ret), K(block_cache_key));
        } else if (is_filter) {
          // do nothing, this micro_key need filter
        } else if (OB_UNLIKELY(!micro_meta.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", KR(ret), K(micro_meta));
        } else if (OB_FAIL(micro_key_set.set_refactored(micro_meta))) {
          LOG_WARN("add micro_key id to set failed", KR(ret), K(micro_meta));
        } else if (OB_FAIL(update_heat_micro_keys.push_back(micro_meta.micro_key_))) {
          LOG_WARN("fail to push back", KR(ret), K(micro_meta));
        }
      }
    }

    // step 3: update count of micro_blocks which exist in kvcache but not in ss_micro_cache
    if (OB_SUCC(ret)) {
      micro_cache->update_hot_micro_lack_count(full_scan_block_cache, lack_cnt);
    }

    // step 4: update micro block heat
    if (OB_FAIL(ret)) {
    } else if (update_heat_micro_keys.empty()) {
      // do nothing
    } else if (OB_FAIL(micro_cache->update_micro_block_heat(update_heat_micro_keys, true/*need to transfer T1 -> T2*/,
                                                            true/*need to update access_time to current_time*/))) {
      LOG_WARN("fail to update micro block heat", KR(ret));
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ to get batch micro keys from kvcache",
               "block_cache_keys_cnt", block_cache_keys.count(),
               "micro_keys_set_size", micro_key_set.size(), K_(ls_id),
               "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

/*
prewarm for KVCache micro key:
physical_key_mode: private_macro micro key -> no
physical_key_mode: shared_major_meta micro key -> yes
logical_key_mode: shared_major_data micro key -> yes
*/
int ObSyncHotMicroKeyTask::filter_micro_block_cache_key(const ObMicroBlockCacheKey &block_cache_key,
                                                        bool &is_filter,
                                                        ObSSMicroBlockCacheKeyMeta &micro_meta,
                                                        int64_t &lack_count)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  ObSSMicroBlockCacheKey micro_cache_key;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!block_cache_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_cache_key));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret));
  } else if (ObMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE == block_cache_key.get_mode()) {
    const ObMicroBlockId micro_block_id = block_cache_key.get_micro_block_id();
    const ObStorageObjectType object_type = micro_block_id.macro_id_.storage_object_type();
    const uint64_t tablet_id = static_cast<uint64_t>(micro_block_id.macro_id_.second_id());
    if (is_ls_replica_prewarm_filter_object_type(object_type)) {
      // do nothing, PRIVATE_MACRO need filter, only prewarm SHARED_MACRO
      is_filter = true;
    } else if (OB_FAIL(micro_cache->is_tablet_id_need_filter(ls_, tablet_id, is_filter))) {
      LOG_WARN("fail to judge is tablet id need filter", KR(ret), K(tablet_id));
    } else if (is_filter) {
      // do nothing, this micro_block_id's tablet_id need filter
    } else {
      // convert ObMicroBlockCacheKey to ObSSMicroBlockCacheKey
      micro_cache_key = ObSSMicroBlockCacheKey(ObSSMicroBlockId(micro_block_id.macro_id_, micro_block_id.offset_, micro_block_id.size_));
    }
  } else if (ObMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == block_cache_key.get_mode()) {
    const ObLogicMicroBlockId logic_macro_id = block_cache_key.get_logic_micro_id();
    const uint64_t tablet_id = static_cast<uint64_t>(logic_macro_id.logic_macro_id_.tablet_id_);
    if (OB_FAIL(micro_cache->is_tablet_id_need_filter(ls_, tablet_id, is_filter))) {
      LOG_WARN("fail to judge is tablet id need filter", KR(ret), K(tablet_id));
    } else if (is_filter) {
      // do nothing, this micro_block_id's tablet_id need filter
    } else {
      // convert ObMicroBlockCacheKey to ObSSMicroBlockCacheKey
      micro_cache_key = ObSSMicroBlockCacheKey(logic_macro_id, block_cache_key.get_data_checksum());
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ObMicroBlockCacheKeyMode", KR(ret), K(block_cache_key));
  }
  // if kvcache's micro_key does not exist in ss_micro_cache, need filter
  if (OB_FAIL(ret)) {
  } else if (is_filter) {
    // do nothing
  } else if (OB_FAIL(filter_micro_key_not_exist_in_sscache(micro_cache_key, is_filter, micro_meta))) {
    LOG_WARN("fail to filter micro key not exist in ss_micro_cache", KR(ret), K(micro_cache_key));
  } else if (is_filter) {
    ++lack_count;
  }
  return ret;
}

int ObSyncHotMicroKeyTask::filter_micro_key_not_exist_in_sscache(const ObSSMicroBlockCacheKey &micro_cache_key,
                                                                 bool &is_filter,
                                                                 ObSSMicroBlockCacheKeyMeta &micro_meta)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_cache_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_cache_key));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret));
  } else {
    ObSSMicroBaseInfo micro_info;
    ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
    if (OB_FAIL(micro_cache->check_micro_block_exist(micro_cache_key, micro_info, hit_type))) {
      LOG_WARN("fail to check micro block exist", KR(ret), K(micro_cache_key));
    } else if (ObSSCacheHitType::SS_CACHE_MISS == hit_type) {
      // if kvcache's micro_key does not exist in ss_micro_cache, need filter
      is_filter = true;
    } else {
      micro_meta = ObSSMicroBlockCacheKeyMeta(micro_cache_key, micro_info.crc_,
                                              micro_info.size_, micro_info.is_in_l1_);
    }
  }
  return ret;
}

int ObSyncHotMicroKeyTask::get_batch_micro_keys_from_sscache(ObHashSet<ObSSMicroBlockCacheKeyMeta> &micro_key_set)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    lib::ObMemAttr attr(MTL_ID(), "MicroCacheKeys");
    ObSEArray<ObSSMicroBlockCacheKeyMeta, OB_DEFAULT_ARRAY_CAPACITY> micro_cache_keys;
    micro_cache_keys.set_attr(attr);
    ObSSMicroCache *micro_cache = nullptr;
    if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_micro_cache is null", KR(ret));
    } else if (OB_FAIL(micro_cache->get_batch_la_micro_keys(ls_, micro_cache_keys))) {
      LOG_WARN("fail to get batch micro keys", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < micro_cache_keys.count(); i++) {
      if (OB_FAIL(micro_key_set.set_refactored(micro_cache_keys.at(i)))) {
        LOG_WARN("add micro_key to set failed", KR(ret), K(micro_cache_keys.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ to get batch micro keys from ss_micro_cache",
               "micro_keys_cnt", micro_cache_keys.count(),
               "micro_keys_set_size", micro_key_set.size(), K_(ls_id),
               "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObSyncHotMicroKeyTask::send_sync_hot_micro_key_rpc(const obrpc::ObLSSyncHotMicroKeyArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else {
    ObArray<ObAddr> addr_array;
    const int64_t rpc_timeout = GCONF.rpc_timeout;
    obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
    if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("srv rpc proxy is null", KR(ret), KP(srv_rpc_proxy));
    } else if (OB_FAIL(get_dest_addr(addr_array))) {
      LOG_WARN("fail to get dest addr", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < addr_array.count(); i++) {
      const ObAddr addr = addr_array.at(i);
      if (addr == GCTX.self_addr()) {
        // do nothing, do not send rpc to self
      } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_3_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("sync hot micro key is not supported", KR(ret));
      } else if (OB_FAIL(srv_rpc_proxy->to(addr)
                                       .by(MTL_ID())
                                       .timeout(rpc_timeout)
                                       .sync_hot_micro_key(arg))) {
        LOG_WARN("fail to send rpc", KR(ret), K(arg));
      } else {
        LOG_INFO("succ to send sync hot micro key rpc", K(arg));
      }
    }
  }
  return ret;
}

int ObSyncHotMicroKeyTask::get_dest_addr(ObIArray<ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    int64_t paxos_replica_num = 0;
    ObMemberList member_list;
    GlobalLearnerList learner_list;
    logservice::ObLogHandler *log_handler = nullptr;
    if (OB_ISNULL(ls_) || OB_ISNULL(log_handler = ls_->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls or log_handle is NULL", KR(ret), K_(ls_id), KP(ls_), KP(log_handler));
    } else if (OB_FAIL(log_handler->get_paxos_member_list_and_learner_list(member_list, paxos_replica_num, 
               learner_list))) {
      LOG_WARN("failed to get paxos_member_list_and_learner_list", KR(ret));
    } else if (OB_FAIL(member_list.get_addr_array(addrs))) {
      LOG_WARN("fail to get addr array", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && i < learner_list.get_member_number(); i++) {
      common::ObMember member;
      if (OB_FAIL(learner_list.get_member_by_index(i, member))) {
        LOG_WARN("get_member_by_index failed", KR(ret), K(learner_list), K(i));
      } else if (member.is_migrating()) { // is_migrating replica server need prewarm
        if (OB_FAIL(addrs.push_back(member.get_server()))) {
          LOG_WARN("fail to push back", KR(ret), K(i));
        }
      }
    }
  }
  return ret;
}

} // storage
} // oceanbase
