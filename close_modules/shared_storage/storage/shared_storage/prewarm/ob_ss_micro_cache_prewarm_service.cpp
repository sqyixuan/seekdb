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

#include "storage/shared_storage/prewarm/ob_ss_micro_cache_prewarm_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/compaction/ob_major_pre_warmer.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::compaction;

ObSSMicroCachePrewarmService::ObSSMicroCachePrewarmService()
  : is_inited_(false), is_stopped_(true), tenant_id_(OB_INVALID_TENANT_ID), replica_prewarm_handler_(),
    major_prewarm_percent_(-1), major_prewarm_level_(ObSSMajorPrewarmLevel::PREWARM_META_AND_DATA_LEVEL),
    tg_id_(INVALID_TG_ID), mc_prewarm_level_refresh_task_(), sync_hot_micro_key_task_map_(),
    consume_hot_micro_key_task_map_()
{
}

ObSSMicroCachePrewarmService::~ObSSMicroCachePrewarmService()
{
  destroy();
}

int ObSSMicroCachePrewarmService::mtl_init(ObSSMicroCachePrewarmService *&prewarm_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prewarm_service->init(MTL_ID()))) {
    LOG_WARN("fail to init micro cache prewarm service", KR(ret));
  }
  return ret;
}

int ObSSMicroCachePrewarmService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(replica_prewarm_handler_.init())) {
    LOG_WARN("fail to init replica prewarm handler", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReplicaPrewarmTimer, tg_id_))) {
    LOG_WARN("fail to create thread", KR(ret));
  } else if (OB_FAIL(mc_prewarm_level_refresh_task_.init(tenant_id))) {
    LOG_WARN("fail to init mc prewarm level refresh task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(sync_hot_micro_key_task_map_.create(LS_PREWARM_MAP_BUCKET_NUM, ObMemAttr(tenant_id, "LSPrewarmMap")))) {
    LOG_WARN("fail to create map", KR(ret));
  } else if (OB_FAIL(consume_hot_micro_key_task_map_.create(LS_PREWARM_MAP_BUCKET_NUM, ObMemAttr(tenant_id, "LSPrewarmMap")))) {
    LOG_WARN("fail to create map", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("succ to init tenant micro cache prewarm service", K(tenant_id), K_(is_inited), K_(tg_id));
  }
  return ret;
}

int ObSSMicroCachePrewarmService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("micro cache prewarm service is not init", KR(ret));
  } else if (OB_FAIL(replica_prewarm_handler_.start())) {
    LOG_WARN("fail to start replica prearm handler", KR(ret));
  } else if (FALSE_IT(is_stopped_ = false)) {
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer", KR(ret), K_(tg_id));
  } else if (OB_FAIL(mc_prewarm_level_refresh_task_.start(tg_id_))) {
    LOG_WARN("fail to start mc prewarm level refresh task", KR(ret), K_(tg_id));
  } else {
    LOG_INFO("succ to start micro cache prewarm service");
  }
  return ret;
}

void ObSSMicroCachePrewarmService::stop()
{
  LOG_INFO("start to stop micro cache prewarm service");
  is_stopped_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  replica_prewarm_handler_.stop();
  LOG_INFO("finish to stop micro cache prewarm service");
}

void ObSSMicroCachePrewarmService::wait()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to wait micro cache prewarm service");
  FOREACH(node_iter, consume_hot_micro_key_task_map_) {
    ObConsumeHotMicroKeyTask *task = node_iter->second;
    if (OB_NOT_NULL(task)) {
      while (task->get_fetch_micro_key_workers() > 0) {
        ob_usleep(ObConsumeHotMicroKeyTask::SLEEP_TIMEOUT); // 100ms
      }
    }
  }
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  replica_prewarm_handler_.wait();
  LOG_INFO("finish to wait micro cache prewarm service");
}

void ObSSMicroCachePrewarmService::destroy()
{
  LOG_INFO("start to destroy micro cache prewarm service");
  if (IS_INIT) {
    if (INVALID_TG_ID != tg_id_) {
      TG_DESTROY(tg_id_);
    }
    ObMemAttr attr(tenant_id_, "PrewarmTask");
    FOREACH(node_iter, sync_hot_micro_key_task_map_) {
      ObSyncHotMicroKeyTask *task = node_iter->second;
      OB_DELETE(ObSyncHotMicroKeyTask, attr, task);
    }
    sync_hot_micro_key_task_map_.destroy();
    FOREACH(node_iter, consume_hot_micro_key_task_map_) {
      ObConsumeHotMicroKeyTask *task = node_iter->second;
      OB_DELETE(ObConsumeHotMicroKeyTask, attr, task);
    }
    consume_hot_micro_key_task_map_.destroy();
  }
  major_prewarm_percent_ = -1;
  replica_prewarm_handler_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_stopped_ = true;
  is_inited_ = false;
  LOG_INFO("finish to destroy micro cache prewarm service");
}

ObReplicaPrewarmHandler &ObSSMicroCachePrewarmService::get_replica_prewarm_handler()
{
  return replica_prewarm_handler_;
}

int64_t ObSSMicroCachePrewarmService::get_major_prewarm_percent() const
{
  return ATOMIC_LOAD(&major_prewarm_percent_);
}

void ObSSMicroCachePrewarmService::set_major_prewarm_percent(const int64_t major_prewarm_percent)
{
  ATOMIC_STORE(&major_prewarm_percent_, major_prewarm_percent);
}

void ObSSMicroCachePrewarmService::reset_major_prewarm_percent()
{
  ATOMIC_STORE(&major_prewarm_percent_, -1);
}

ObSSMajorPrewarmLevel ObSSMicroCachePrewarmService::get_major_prewarm_level() const
{
  return major_prewarm_level_;
}

void ObSSMicroCachePrewarmService::set_major_prewarm_level(const ObSSMajorPrewarmLevel prewarm_level)
{
  major_prewarm_level_ = prewarm_level;
}

int ObSSMicroCachePrewarmService::init_sync_hot_micro_key_task(ObLS *ls, volatile bool &is_stop)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls));
  } else {
    const int64_t ls_id = ls->get_ls_id().id();
    ObMemAttr attr(tenant_id_, "PrewarmTask");
    ObSyncHotMicroKeyTask *sync_hot_micro_key_task = nullptr;
    if (OB_UNLIKELY(ls_id == ObLSID::INVALID_LS_ID)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_id is unexpected", KR(ret), K(ls_id), KP(ls));
    } else if (OB_ISNULL(sync_hot_micro_key_task = OB_NEW(ObSyncHotMicroKeyTask, attr, is_stop))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(sync_hot_micro_key_task->init(ls))) {
      LOG_WARN("fail to init sync hot micro key task", KR(ret));
    } else if (OB_FAIL(sync_hot_micro_key_task_map_.set_refactored(ls_id, sync_hot_micro_key_task))) {
      LOG_WARN("fail to set refactored", KR(ret), K(ls_id));
    } else {
      LOG_INFO("succ to init sync hot micro key task", KP(sync_hot_micro_key_task), K(ls_id), K_(tenant_id));
    }
    // if fail free memory
    if (OB_FAIL(ret)) {
      OB_DELETE(ObSyncHotMicroKeyTask, attr, sync_hot_micro_key_task);
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::init_consume_hot_micro_key_task(ObLS *ls, volatile bool &is_stop)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls));
  } else {
    const int64_t ls_id = ls->get_ls_id().id();
    ObMemAttr attr(tenant_id_, "PrewarmTask");
    ObConsumeHotMicroKeyTask *consume_hot_micro_key_task = nullptr;
    if (OB_UNLIKELY(ls_id == ObLSID::INVALID_LS_ID)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_id is unexpected", KR(ret), K(ls_id), KP(ls));
    } else if (OB_ISNULL(consume_hot_micro_key_task = OB_NEW(ObConsumeHotMicroKeyTask, attr, is_stop))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(consume_hot_micro_key_task->init(ls_id))) {
      LOG_WARN("fail to init consume hot micro key task", KR(ret));
    } else if (OB_FAIL(consume_hot_micro_key_task_map_.set_refactored(ls_id, consume_hot_micro_key_task))) {
      LOG_WARN("fail to set refactored", KR(ret), K(ls_id));
    } else {
      LOG_INFO("succ to init consume hot micro key task", KP(consume_hot_micro_key_task), K(ls_id), K_(tenant_id));
    }
    // if fail free memory
    if (OB_FAIL(ret)) {
      OB_DELETE(ObConsumeHotMicroKeyTask, attr, consume_hot_micro_key_task);
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::schedule_sync_hot_micro_key_task(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("prewarm thread is stopped", KR(ret));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    ObSyncHotMicroKeyTask *sync_hot_micro_key_task = nullptr;
    if (OB_FAIL(sync_hot_micro_key_task_map_.get_refactored(ls_id, sync_hot_micro_key_task))) {
      LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
    } else if (OB_ISNULL(sync_hot_micro_key_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sync_hot_micro_key_task is nullptr", KR(ret), K(ls_id));
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *sync_hot_micro_key_task, SYNC_TASK_SCHEDULE_INTERVAL_US, true/*repeat*/))) {
      LOG_WARN("fail to schedule sync hot micro key task", KR(ret), K_(tg_id), K(ls_id), K_(tenant_id));
    } else {
      LOG_INFO("succ to schedule sync hot micro key task", KP(sync_hot_micro_key_task), K_(tg_id), K(ls_id), K_(tenant_id));
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::schedule_consume_hot_micro_key_task(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("prewarm thread is stopped", KR(ret));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    ObConsumeHotMicroKeyTask *consume_hot_micro_key_task = nullptr;
    if (OB_FAIL(consume_hot_micro_key_task_map_.get_refactored(ls_id, consume_hot_micro_key_task))) {
      LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
    } else if (OB_ISNULL(consume_hot_micro_key_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("consume_hot_micro_key_task is nullptr", KR(ret), K(ls_id));
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *consume_hot_micro_key_task, CONSUME_TASK_SCHEDULE_INTERVAL_US, true/*repeat*/))) {
      LOG_WARN("fail to schedule consume hot micro key task", KR(ret), K_(tg_id), K(ls_id), K_(tenant_id));
    } else {
      LOG_INFO("succ to schedule consume hot micro key task", KP(consume_hot_micro_key_task), K_(tg_id), K(ls_id), K_(tenant_id));
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::stop_sync_hot_micro_key_task(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    ObSyncHotMicroKeyTask *sync_hot_micro_key_task = nullptr;
    if (OB_FAIL(sync_hot_micro_key_task_map_.get_refactored(ls_id, sync_hot_micro_key_task))) {
      LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
    } else if (OB_ISNULL(sync_hot_micro_key_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sync_hot_micro_key_task is nullptr", KR(ret), K(ls_id));
    } else {
      TG_CANCEL_TASK(tg_id_, *sync_hot_micro_key_task);
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ to stop sync hot micro key task", K_(tg_id), K(ls_id), K_(tenant_id));
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::stop_consume_hot_micro_key_task(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    ObConsumeHotMicroKeyTask *consume_hot_micro_key_task = nullptr;
    if (OB_FAIL(consume_hot_micro_key_task_map_.get_refactored(ls_id, consume_hot_micro_key_task))) {
      LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
    } else if (OB_ISNULL(consume_hot_micro_key_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("consume task is nullptr", KR(ret), K(ls_id));
    } else {
      TG_CANCEL_TASK(tg_id_, *consume_hot_micro_key_task);
      consume_hot_micro_key_task->reset_group();
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ to stop consume hot micro key task", K_(tg_id), K(ls_id), K_(tenant_id));
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::wait_sync_hot_micro_key_task(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObSyncHotMicroKeyTask *sync_hot_micro_key_task = nullptr;
    if (OB_FAIL(sync_hot_micro_key_task_map_.get_refactored(ls_id, sync_hot_micro_key_task))) {
      LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
    } else if (OB_ISNULL(sync_hot_micro_key_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sync_hot_micro_key_task is nullptr", KR(ret), K(ls_id));
    } else {
      TG_WAIT_TASK(tg_id_, *sync_hot_micro_key_task);
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ to wait sync hot micro key task", K_(tg_id), K(ls_id), K_(tenant_id),
               "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::wait_consume_hot_micro_key_task(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObConsumeHotMicroKeyTask *consume_hot_micro_key_task = nullptr;
    if (OB_FAIL(consume_hot_micro_key_task_map_.get_refactored(ls_id, consume_hot_micro_key_task))) {
      LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
    } else if (OB_ISNULL(consume_hot_micro_key_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("consume_hot_micro_key_task is nullptr", KR(ret), K(ls_id));
    } else {
      while (consume_hot_micro_key_task->get_fetch_micro_key_workers() > 0) {
        usleep(ObConsumeHotMicroKeyTask::SLEEP_TIMEOUT); // 100ms
      }
      TG_WAIT_TASK(tg_id_, *consume_hot_micro_key_task);
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ to wait consume hot micro key task", K_(tg_id), K(ls_id), K_(tenant_id),
               "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::destroy_sync_hot_micro_key_task(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObSyncHotMicroKeyTask *sync_hot_micro_key_task = nullptr;
    if (OB_FAIL(sync_hot_micro_key_task_map_.get_refactored(ls_id, sync_hot_micro_key_task))) {
      LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
    } else if (OB_ISNULL(sync_hot_micro_key_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sync_hot_micro_key_task is nullptr", KR(ret), K(ls_id));
    } else {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(tg_id_, *sync_hot_micro_key_task, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(tg_id_, *sync_hot_micro_key_task);
          TG_WAIT_TASK(tg_id_, *sync_hot_micro_key_task);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sync_hot_micro_key_task_map_.erase_refactored(ls_id))) {
        LOG_WARN("fail to erase refactored", KR(ret), K(ls_id));
      } else {
        ObMemAttr attr(tenant_id_, "PrewarmTask");
        OB_DELETE(ObSyncHotMicroKeyTask, attr, sync_hot_micro_key_task);
        LOG_INFO("succ to destroy sync hot micro key task", K_(tg_id), K(ls_id), K_(tenant_id),
                 "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
      }
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::destroy_consume_hot_micro_key_task(const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObConsumeHotMicroKeyTask *consume_hot_micro_key_task = nullptr;
    if (OB_FAIL(consume_hot_micro_key_task_map_.get_refactored(ls_id, consume_hot_micro_key_task))) {
      LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
    } else if (OB_ISNULL(consume_hot_micro_key_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("consume_hot_micro_key_task is nullptr", KR(ret), K(ls_id));
    } else {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(tg_id_, *consume_hot_micro_key_task, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(tg_id_, *consume_hot_micro_key_task);
          TG_WAIT_TASK(tg_id_, *consume_hot_micro_key_task);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(consume_hot_micro_key_task_map_.erase_refactored(ls_id))) {
        LOG_WARN("fail to erase refactored", KR(ret), K(ls_id));
      } else {
        ObMemAttr attr(tenant_id_, "PrewarmTask");
        OB_DELETE(ObConsumeHotMicroKeyTask, attr, consume_hot_micro_key_task);
        LOG_INFO("succ to destroy consume hot micro key task", K_(tg_id), K(ls_id), K_(tenant_id),
                 "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
      }
    }
  }
  return ret;
}

int ObSSMicroCachePrewarmService::push_micro_cache_keys_to_consume_task(
    const int64_t ls_id, 
    const obrpc::ObLSSyncHotMicroKeyArg &arg)
{
  int ret = OB_SUCCESS;
  ObConsumeHotMicroKeyTask *consume_hot_micro_key_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(ObLSID::INVALID_LS_ID == ls_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(consume_hot_micro_key_task_map_.get_refactored(ls_id, consume_hot_micro_key_task))) {
    LOG_WARN("fail to get refactored", KR(ret), K(ls_id));
  } else if (OB_ISNULL(consume_hot_micro_key_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("consume_hot_micro_key_task is nullptr", KR(ret), K(ls_id));
  } else if (OB_FAIL(consume_hot_micro_key_task->push_micro_cache_keys(arg))) {
    LOG_WARN("fail to push micro cache keys", KR(ret), K(ls_id), K(arg));
  }
  return ret;
}


} /* namespace storage */
} /* namespace oceanbase */
