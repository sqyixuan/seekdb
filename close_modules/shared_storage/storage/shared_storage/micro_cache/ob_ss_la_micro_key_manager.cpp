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

#include "ob_ss_la_micro_key_manager.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common::hash;

ObSSLAMicroKeyManager::ObSSLAMicroKeyManager()
  : is_inited_(false), latest_access_micro_key_set_(),
    is_stop_record_la_micro_key_(false)
{
}

ObSSLAMicroKeyManager::~ObSSLAMicroKeyManager()
{
  destroy();
}

int ObSSLAMicroKeyManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret));
  } else if (OB_FAIL(latest_access_micro_key_set_.create(OB_LATEST_ACCESS_MICRO_KEY_BUCKET_NUM, ObMemAttr(MTL_ID(), "LSPrewarmSet")))) {
    LOG_WARN("fail to create set", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("succ to init ss latest access micro key manager", K_(is_inited));
  }
  return ret;
}

void ObSSLAMicroKeyManager::destroy()
{
  is_inited_ = false;
  latest_access_micro_key_set_.destroy();
  is_stop_record_la_micro_key_ = true;
}

int ObSSLAMicroKeyManager::push_latest_access_micro_key_to_hashset(const ObSSMicroBlockCacheKeyMeta &micro_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_filter = false;
  uint64_t tablet_id = 0;
  const uint64_t cur_us = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_meta));
  } else if (OB_UNLIKELY(is_stop_record_la_micro_key_)) {
    /// do nothing, stop record latest access micro key
  } else if (OB_FAIL(filter_micro_block_cache_key_by_object_type(micro_meta, is_filter))) {
    LOG_WARN("fail to filter ss micro block cache key by object type", KR(ret), K(micro_meta));
  } else if (is_filter) {
    // do nothing, PRIVATE_MACRO need filter, only prewarm SHARED_MACRO
  } else if (latest_access_micro_key_set_.size() > MAX_MICRO_BLOCK_KEY_SET_CAPACITY) {
    // do nothing, latest_access_micro_key_set can noly hold 10w elements, others throw away
    // TODO change to queue-based discard, new keys are added, old keys are discarded @xiaotao.ht
  } else if (OB_FAIL(latest_access_micro_key_set_.set_refactored(micro_meta, false/*overwrite*/))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to set refactored", KR(ret), K(micro_meta));
    }
  }
  LOG_TRACE("latest access micro key set size", KR(ret), "micro_keys_size", latest_access_micro_key_set_.size());
  return ret;
}

int ObSSLAMicroKeyManager::get_batch_micro_keys(ObLS *ls, ObIArray<ObSSMicroBlockCacheKeyMeta> &keys)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls));
  } else {
    ObGetMicroKeyOp get_micro_key_op(keys, ls, *this);
    if (OB_FAIL(latest_access_micro_key_set_.foreach_refactored(get_micro_key_op))) {
      LOG_WARN("fail to get micro keys", KR(ret));
    } else if (OB_FAIL(batch_erase_micro_keys(get_micro_key_op.keys_))) {
      LOG_WARN("fail to batch erase micro keys", KR(ret));
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::ObGetMicroKeyOp::operator()(
    HashSetTypes<ObSSMicroBlockCacheKeyMeta>::pair_type &pair)
{
  int ret = OB_SUCCESS;
  const ObSSMicroBlockCacheKeyMeta &micro_meta = pair.first;
  bool is_filtered = true;
  if (OB_FAIL(la_micro_key_manager_.filter_micro_block_cache_key_by_tablet_id(ls_, micro_meta, is_filtered))) {
    LOG_WARN("fail to filter", KR(ret), K(micro_meta));
  } else if (is_filtered) {
    // do nothing, this ObSSMicroBlockCacheKeyMeta do not belong to this ls
  } else if (OB_FAIL(keys_.push_back(micro_meta))) {
    LOG_WARN("fail to push back", KR(ret), K(micro_meta));
  }
  return ret;
}

int ObSSLAMicroKeyManager::batch_erase_micro_keys(const ObIArray<ObSSMicroBlockCacheKeyMeta> &keys)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    const int64_t key_cnt = keys.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < key_cnt); i++) {
      if (OB_FAIL(latest_access_micro_key_set_.erase_refactored(keys.at(i)))) {
        LOG_WARN("fail to erase", KR(ret), K(i), K(key_cnt), "key_meta", keys.at(i));
      }
      ret = OB_SUCCESS; // ignore ret, and go on erasing next key
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::filter_micro_block_cache_key_by_object_type(const ObSSMicroBlockCacheKeyMeta &micro_meta,
                                                                       bool &is_filter)
{
  int ret = OB_SUCCESS;
  is_filter = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_meta));
  } else if (ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE == micro_meta.micro_key_.mode_) {
    const ObStorageObjectType object_type = micro_meta.micro_key_.get_micro_id().macro_id_.storage_object_type();
    if (is_ls_replica_prewarm_filter_object_type(object_type)) {
      // PRIVATE_MACRO need filter, only prewarm SHARED_MACRO
      is_filter = true;
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::filter_micro_block_cache_key_by_tablet_id(ObLS *ls,
                                                                     const ObSSMicroBlockCacheKeyMeta &micro_meta,
                                                                     bool &is_filter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((nullptr == ls) || !micro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls), K(micro_meta));
  } else {
    uint64_t tablet_id = 0;
    if (ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE == micro_meta.micro_key_.mode_) {
      tablet_id = static_cast<uint64_t>(micro_meta.micro_key_.get_micro_id().macro_id_.second_id());
    } else if (ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE == micro_meta.micro_key_.mode_) {
      tablet_id = static_cast<uint64_t>(micro_meta.micro_key_.get_logic_micro_id().logic_macro_id_.tablet_id_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro key mode is invalid", KR(ret), K(micro_meta));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(is_tablet_id_need_filter(ls, tablet_id, is_filter))) {
      LOG_WARN("fail to judge is tablet id need filter", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::is_tablet_id_need_filter(ObLS *ls, const uint64_t tablet_id,
                                                    bool &is_filter)
{
  int ret = OB_SUCCESS;
  is_filter = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((nullptr == ls) || (ObTabletID::INVALID_TABLET_ID == tablet_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls), K(tablet_id));
  } else {
    bool is_exist = false;
    ObLSTabletService *ls_tablet_svr = nullptr;
    if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_tablet_svr is nullptr", KR(ret), KP(ls_tablet_svr));
    } else if (OB_FAIL(ls_tablet_svr->is_tablet_exist(ObTabletID(tablet_id), is_exist))) {
      LOG_WARN("fail to judge is tablet exist", KR(ret), K(tablet_id));
    } else if (is_exist) {
      is_filter = false;
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::check_stop_record_la_micro_key()
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret));
  } else {
    const bool ori_stop_flag = is_stop_record_la_micro_key_;
    is_stop_record_la_micro_key_ = !(tenant_config->_enable_ss_replica_prewarm);
    if (is_stop_record_la_micro_key_) {
      latest_access_micro_key_set_.clear();
    }
    if (ori_stop_flag != is_stop_record_la_micro_key_) {
      FLOG_INFO("_enable_ss_replica_prewarm config is changed", K_(is_stop_record_la_micro_key));
    }
  }
  return ret;
}


} /* namespace storage */
} /* namespace oceanbase */
