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

#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::share;
using namespace oceanbase::common;

ObSSMicroCacheTaskRunner::ObSSMicroCacheTaskRunner()
    : is_inited_(false),
      is_stopped_(true),
      high_prio_tg_id_(INVALID_TG_ID),
      mid_prio_tg_id_(INVALID_TG_ID),
      low_prio_tg_id_(INVALID_TG_ID),
      task_ctx_(),
      persist_task_(*this),
      release_cache_task_(*this),
      micro_ckpt_task_(*this),
      blk_ckpt_task_(*this)
{}

int ObSSMicroCacheTaskRunner::init(
    const uint64_t tenant_id,
    ObIAllocator &allocator,
    ObSSMemDataManager &mem_data_mgr, 
    ObSSMicroMetaManager &micro_meta_mgr, 
    ObSSPhysicalBlockManager &phy_blk_mgr, 
    ObTenantFileManager &tnt_file_mgr,
    ObSSMicroCacheStat &cache_stat,
    ObSSLAMicroKeyManager &la_micro_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(task_ctx_.init(allocator, mem_data_mgr, micro_meta_mgr, phy_blk_mgr, tnt_file_mgr, cache_stat,
             la_micro_mgr))) {
    LOG_WARN("fail to init task_ctx", KR(ret));
  } else if (OB_FAIL(init_task(tenant_id))) {
    LOG_WARN("fail to init task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_timer_thread())) {
    LOG_WARN("fail to init timer thread", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::init_task(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persist_task_.init(tenant_id, DEFAULT_PERSIST_DATA_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init persist micro data task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(release_cache_task_.init(tenant_id, DEFAULT_ARC_CACHE_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init arc cache task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(micro_ckpt_task_.init(tenant_id, DEFAULT_DO_MICRO_CKPT_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init do micro_checkpoint task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(blk_ckpt_task_.init(tenant_id, DEFAULT_DO_BLK_CKPT_INTERVAL_US, task_ctx_))) {
    LOG_WARN("fail to init do blk_checkpoint task", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::init_timer_thread()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MicCacheHTimer, high_prio_tg_id_))) {
    LOG_WARN("fail to create high priority thread", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MicCacheMTimer, mid_prio_tg_id_))) {
    LOG_WARN("fail to create mid priority thread", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MicCacheLTimer, low_prio_tg_id_))) {
    LOG_WARN("fail to create low priority thread", KR(ret));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    is_stopped_ = false;
    if (OB_FAIL(start_timer_thread())) {
      LOG_WARN("fail to start timer thread", KR(ret));
    } else if (OB_FAIL(schedule_persist_data_task(DEFAULT_PERSIST_DATA_INTERVAL_US))) {
      LOG_WARN("fail to schedule persist_micro_data task", KR(ret), K_(high_prio_tg_id));
    } else if (OB_FAIL(schedule_do_blk_checkpoint_task(DEFAULT_DO_BLK_CKPT_INTERVAL_US))) {
      LOG_WARN("fail to schedule do_blk_ckpt task", KR(ret), K_(mid_prio_tg_id));
    } else if (OB_FAIL(schedule_arc_cache_task(DEFAULT_ARC_CACHE_INTERVAL_US))) {
      LOG_WARN("fail to schedule arc_cache task", KR(ret), K_(low_prio_tg_id));
    } else if (OB_FAIL(schedule_do_micro_checkpoint_task(DEFAULT_DO_MICRO_CKPT_INTERVAL_US))) {
      LOG_WARN("fail to schedule do_micro_ckpt task", KR(ret), K_(low_prio_tg_id));
    } else {
      LOG_INFO("ObSSMicroCacheTaskRunner start succ");
    }
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::start_timer_thread()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_START(high_prio_tg_id_))) {
    LOG_WARN("fail to start high priority timer", KR(ret), K_(high_prio_tg_id));
  } else if (OB_FAIL(TG_START(mid_prio_tg_id_))) {
    LOG_WARN("fail to start mid priority timer", KR(ret), K_(mid_prio_tg_id));
  } else if (OB_FAIL(TG_START(low_prio_tg_id_))) {
    LOG_WARN("fail to start low priority timer", KR(ret), K_(low_prio_tg_id));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_persist_data_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(high_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(high_prio_tg_id_, persist_task_, interval_us, false /*repeat*/))) {
    LOG_WARN("fail to schedule persist micro data task", KR(ret), K_(high_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule persist micro data task", K_(high_prio_tg_id), K(interval_us));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_arc_cache_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(low_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(low_prio_tg_id_, release_cache_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule arc cache task", KR(ret), K_(low_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule arc cache task", K_(low_prio_tg_id));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_do_micro_checkpoint_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(low_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(low_prio_tg_id_, micro_ckpt_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule do micro_checkpoint task", KR(ret), K_(low_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule do micro_checkpoint task", K_(low_prio_tg_id));
  }
  return ret;
}

int ObSSMicroCacheTaskRunner::schedule_do_blk_checkpoint_task(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ss_micro_cache task runner is stopped", KR(ret), K_(low_prio_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(mid_prio_tg_id_, blk_ckpt_task_, interval_us, false/*repeat*/))) {
    LOG_WARN("fail to schedule do blk_checkpoint task", KR(ret), K_(mid_prio_tg_id), K(interval_us));
  } else {
    LOG_TRACE("succ to schedule do blk_checkpoint task", K_(mid_prio_tg_id));
  }
  return ret;
}

void ObSSMicroCacheTaskRunner::stop()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("begin to stop task runner", KP(this));
  if (!ATOMIC_LOAD(&is_stopped_)) {
    ATOMIC_STORE(&is_stopped_, true);
    if (INVALID_TG_ID != low_prio_tg_id_) {
      TG_STOP(low_prio_tg_id_);
    }
    if (INVALID_TG_ID != mid_prio_tg_id_) {
      TG_STOP(mid_prio_tg_id_);
    }
    if (INVALID_TG_ID != high_prio_tg_id_) {
      TG_STOP(high_prio_tg_id_);
    }
    FLOG_INFO("finish to stop task runner", KP(this));
  }
}

void ObSSMicroCacheTaskRunner::wait()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("begin to wait task runner", KP(this));
  if (INVALID_TG_ID != low_prio_tg_id_) {
    TG_WAIT(low_prio_tg_id_);
  }
  if (INVALID_TG_ID != mid_prio_tg_id_) {
    TG_WAIT(mid_prio_tg_id_);
  }
  if (INVALID_TG_ID != high_prio_tg_id_) {
    TG_WAIT(high_prio_tg_id_);
  }
  FLOG_INFO("finish to wait task runner", KP(this));
}

void ObSSMicroCacheTaskRunner::destroy()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("begin to destroy task runner", KP(this));
  if (INVALID_TG_ID != low_prio_tg_id_) {
    TG_DESTROY(low_prio_tg_id_);
  }
  if (INVALID_TG_ID != mid_prio_tg_id_) {
    TG_DESTROY(mid_prio_tg_id_);
  }
  if (INVALID_TG_ID != high_prio_tg_id_) {
    TG_DESTROY(high_prio_tg_id_);
  }
  persist_task_.destroy();
  blk_ckpt_task_.destroy();
  release_cache_task_.destroy();
  micro_ckpt_task_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_stopped_ = true;
  is_inited_ = false;
  FLOG_INFO("finish to destroy task runner", KP(this));
}

} // storage
} // oceanbase
