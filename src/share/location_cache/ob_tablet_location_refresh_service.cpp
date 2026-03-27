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

#define USING_LOG_PREFIX SHARE_LOCATION
#include "ob_tablet_location_refresh_service.h"
#include "share/location_cache/ob_tablet_ls_service.h"
#include "src/rootserver/ob_root_utils.h"

namespace oceanbase
{
namespace share
{

ObTabletLocationRefreshMgr::ObTabletLocationRefreshMgr(
   const uint64_t tenant_id)
  : mutex_(),
    tenant_id_(tenant_id),
    tablet_ids_()
{
  tablet_ids_.set_attr(SET_USE_500("TbltRefIDS"));
}

ObTabletLocationRefreshMgr::~ObTabletLocationRefreshMgr()
{
}

int ObTabletLocationRefreshMgr::set_tablet_ids(
    const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("fail to assign tablet_ids", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObTabletLocationRefreshMgr::get_tablet_ids(
    common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(tablet_ids.assign(tablet_ids_))) {
    LOG_WARN("fail to get tablet_ids", KR(ret), K_(tenant_id));
  }
  return ret;
}

int64_t ObTabletLocationRefreshServiceIdling::get_idle_interval_us()
{
  int64_t idle_time = DEFAULT_TIMEOUT_US;
  if (0 != GCONF._auto_refresh_tablet_location_interval) {
    idle_time = GCONF._auto_refresh_tablet_location_interval;
  }
  return idle_time;
}

int ObTabletLocationRefreshServiceIdling::fast_idle()
{
  return idle(FAST_TIMEOUT_US);
}

ObTabletLocationRefreshService::ObTabletLocationRefreshService()
  : ObRsReentrantThread(true),
    inited_(false),
    has_task_(false),
    idling_(stop_),
    tablet_ls_service_(NULL),
    schema_service_(NULL),
    sql_proxy_(NULL),
    allocator_(SET_USE_500("TbltReSrv")),
    rwlock_(),
    tenant_mgr_map_()
{
}

ObTabletLocationRefreshService::~ObTabletLocationRefreshService()
{
  destroy();
}

int ObTabletLocationRefreshService::init(
    ObTabletLSService &tablet_ls_service,
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t THREAD_CNT = 1;
  const int64_t BUCKET_NUM = 1024;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (OB_FAIL(create(THREAD_CNT, "TbltRefreshSer"))) {
    LOG_WARN("create thread failed", KR(ret));
  } else if (OB_FAIL(tenant_mgr_map_.create(BUCKET_NUM,
             SET_USE_500("TbltRefreshMap"), SET_USE_500("TbltRefreshMap")))) {
    LOG_WARN("fail to create hash map", KR(ret));
  } else {
    tablet_ls_service_ = &tablet_ls_service;
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    has_task_ = false;
    inited_ = true;
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] init service", KR(ret));
  return ret;
}

int ObTabletLocationRefreshService::start()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[REFRESH_TABLET_LOCATION] start service begin");
  if (OB_FAIL(ObRsReentrantThread::start())) {
    LOG_WARN("fail to start thread", KR(ret));
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] start service end", KR(ret));
  return ret;
}

void ObTabletLocationRefreshService::stop()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] stop service begin");
  ObReentrantThread::stop();
  idling_.wakeup();
  FLOG_INFO("[REFRESH_TABLET_LOCATION] stop service end");
}

void ObTabletLocationRefreshService::wait()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] wait service begin");
  ObRsReentrantThread::wait();
  ObReentrantThread::wait();
  FLOG_INFO("[REFRESH_TABLET_LOCATION] wait service end");
}

void ObTabletLocationRefreshService::destroy()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] destroy service begin");
  (void) stop();
  (void) wait();
  SpinWLockGuard guard(rwlock_);
  if (inited_) {
    FOREACH(it, tenant_mgr_map_) {
      if (OB_NOT_NULL(it->second)) {
        (it->second)->~ObTabletLocationRefreshMgr();
        it->second = NULL;
      }
    }
    tablet_ls_service_ = NULL;
    schema_service_ = NULL;
    sql_proxy_ = NULL;
    tenant_mgr_map_.destroy();
    allocator_.reset();
    has_task_ = false;
    inited_ = false;
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] destroy service end");
}

void ObTabletLocationRefreshService::run3()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] run service begin");
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", KR(ret));
  } else {
    while(!stop_) {
      ObCurTraceId::init(GCTX.self_addr());
      if (OB_FAIL(refresh_cache_())) {
        LOG_WARN("fail to refresh tablet location", KR(ret));
      }
      // retry until stopped, reset ret to OB_SUCCESS
      ret = OB_SUCCESS;
      idle_();
    }
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] run service end");
}

void ObTabletLocationRefreshService::idle_()
{
  if (OB_UNLIKELY(stop_)) {
    // skip
  } else if (OB_UNLIKELY(has_task_ && 0 != GCONF._auto_refresh_tablet_location_interval)) {
    (void) idling_.fast_idle();
  } else {
    (void) idling_.idle();
  }
  has_task_ = false;
}

int ObTabletLocationRefreshService::check_stop_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", KR(ret));
  } else if (OB_UNLIKELY(stop_)) {
    ret = OB_CANCELED;
    LOG_WARN("thread has been stopped", KR(ret));
  } else if (OB_UNLIKELY(0 == GCONF._auto_refresh_tablet_location_interval)) {
    ret = OB_CANCELED;
    LOG_WARN("service is shut down by config", KR(ret));
  }
  return ret;
}

int ObTabletLocationRefreshService::try_init_base_point(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_user_tenant(tenant_id)) {
    bool should_init = false;
    {
      SpinRLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          should_init = true;
        } else {
          LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
        }
      }
    }
    if (OB_SUCC(ret) && should_init) {
      if (OB_FAIL(try_init_base_point_(tenant_id))) {
        LOG_WARN("fail to init base point", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

// 1. mgr is null means tenant has been dropped.
// 2. OB_HASH_NOT_EXIST means tenant has not been inited.
int ObTabletLocationRefreshService::inner_get_mgr_(
    const int64_t tenant_id,
    ObTabletLocationRefreshMgr *&mgr)
{
  int ret = OB_SUCCESS;
  mgr = NULL;
  int hash_ret = tenant_mgr_map_.get_refactored(tenant_id, mgr);
  if (OB_SUCCESS == hash_ret) {
    // success
  } else {
    mgr = NULL;
    ret = hash_ret;
    LOG_WARN("fail to get mgr from map", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTabletLocationRefreshService::get_tenant_ids_(
    common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_FAIL(tenant_ids.reserve(tenant_mgr_map_.size()))) {
    LOG_WARN("fail to reserved array", KR(ret));
  } else {
    FOREACH_X(it, tenant_mgr_map_, OB_SUCC(ret)) {
      const uint64_t tenant_id = it->first;
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("fail to push back tenant_id", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObTabletLocationRefreshService::try_clear_mgr_(const uint64_t tenant_id, bool &clear)
{
  int ret = OB_SUCCESS;
  bool has_been_dropped = false;
  clear = false;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", KR(ret));
  } else if (OB_FAIL(schema_service_->check_if_tenant_has_been_dropped(tenant_id, has_been_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  } else if (!has_been_dropped) {
    // skip
  } else {
    bool should_clear = false;
    {
      SpinRLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      } else if (OB_NOT_NULL(mgr)) {
        should_clear = true;
      } else {
        clear = true;
      }
    }

    if (OB_SUCC(ret) && should_clear) {
      SpinWLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      } else if (OB_NOT_NULL(mgr)) {
        ObTabletLocationRefreshMgr *tmp_mgr = NULL;
        int overwrite = 1;
        if (OB_FAIL(tenant_mgr_map_.set_refactored(tenant_id, tmp_mgr, overwrite))) {
          LOG_WARN("fail to overwrite mgr", KR(ret), K(tenant_id));
        } else {
          FLOG_INFO("[REFRESH_TABLET_LOCATION] destroy struct because tenant has been dropped", K(tenant_id));
          mgr->~ObTabletLocationRefreshMgr();
          mgr = NULL;
        }
      }
      if (OB_SUCC(ret)) {
        clear = true;
      }
    }
  }
  return ret;
}

int ObTabletLocationRefreshService::try_init_base_point_(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // try init struct
  if (OB_SUCC(ret)) {
    SpinWLockGuard guard(rwlock_);
    ObTabletLocationRefreshMgr *mgr = NULL;
    if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTabletLocationRefreshMgr)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (FALSE_IT(mgr = new (buf) ObTabletLocationRefreshMgr(tenant_id))) {
          LOG_WARN("fail to new ObTabletLocationRefreshMgr",
                   KR(ret), K(tenant_id));
        } else if (OB_FAIL(tenant_mgr_map_.set_refactored(tenant_id, mgr))) {
          LOG_WARN("fail to set ObTabletLocationRefreshMgr",
                   KR(ret), K(tenant_id));
        }
        FLOG_INFO("[REFRESH_TABLET_LOCATION] init struct",
                  KR(ret), K(tenant_id));
      } else {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObTabletLocationRefreshService::refresh_cache_()
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] refresh cache start");
  int64_t start_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(get_tenant_ids_(tenant_ids))) {
    LOG_WARN("fail to get tenant_ids", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;

    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(check_stop_())) {
        LOG_WARN("fail to check stop", KR(ret));
      } else { // ignore different tenant's failure
        bool clear = false; // will be true when tenant has been dropped
        if (OB_TMP_FAIL(try_clear_mgr_(tenant_id, clear))) {
          LOG_WARN("fail to clear mgr", KR(tmp_ret), K(tenant_id));
        }

        if (!clear && OB_TMP_FAIL(refresh_cache_(tenant_id))) {
          LOG_WARN("fail to refresh cache", KR(tmp_ret), K(tenant_id));
        }
      }
    } // end for
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] refresh cache end",
            KR(ret), "cost_us", ObTimeUtility::current_time() - start_time,
            "tenant_cnt", tenant_mgr_map_.size());
  return ret;
}

int ObTabletLocationRefreshService::refresh_cache_(const uint64_t tenant_id)
{
  FLOG_INFO("[REFRESH_TABLET_LOCATION] refresh cache start", K(tenant_id));
  int64_t start_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_stop_())) {
    LOG_WARN("fail to check stop", KR(ret));
  } else if (OB_FAIL(try_runs_for_compatibility_(tenant_id))) {
    LOG_WARN("fail to runs for compatibility", KR(ret), K(tenant_id));
  }
  FLOG_INFO("[REFRESH_TABLET_LOCATION] refresh cache end",
            KR(ret), K(tenant_id), "cost_us", ObTimeUtility::current_time() - start_time);
  return ret;
}

// try init base_task_id_ & get tablet_ids from local cache
int ObTabletLocationRefreshService::try_runs_for_compatibility_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
    ObArray<ObTabletID> tablet_ids;
    tablet_ids.set_attr(SET_USE_500("TbltRefIDS"));
    if (OB_FAIL(check_stop_())) {
      LOG_WARN("fail to check stop", KR(ret));
    } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(tablet_ls_service_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql_proxy_ or tablet_ls_service_ is null",
               KR(ret), KP_(sql_proxy), KP_(tablet_ls_service));
    } else if (OB_FAIL(tablet_ls_service_->get_tablet_ids_from_cache(tenant_id, tablet_ids))) {
      LOG_WARN("fail to get tablet_ids", KR(ret), K(tenant_id));
    } else {
      SpinWLockGuard guard(rwlock_);
      ObTabletLocationRefreshMgr *mgr = NULL;
      if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
        LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(mgr)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
      } else if (OB_FAIL(mgr->set_tablet_ids(tablet_ids))) {
        LOG_WARN("fail to set tablet_ids", KR(ret));
      }
    }

  // try reload tablet-ls caches according to tablet_ids_
  if (FAILEDx(try_reload_tablet_cache_(tenant_id))) {
    LOG_WARN("fail to reload tablet cache", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTabletLocationRefreshService::try_reload_tablet_cache_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> store_tablet_ids;
  store_tablet_ids.set_attr(SET_USE_500("TbltRefIDS"));
  {
    SpinRLockGuard guard(rwlock_);
    ObTabletLocationRefreshMgr *mgr = NULL;
    if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
      LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(mgr)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->get_tablet_ids(store_tablet_ids))) {
      LOG_WARN("fail to get tablet_ids", KR(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret) && store_tablet_ids.count() > 0) {

    const int64_t MAX_RELOAD_TABLET_NUM_IN_BATCH = 128;
    int64_t end_pos = min(store_tablet_ids.count(), MAX_RELOAD_TABLET_NUM_IN_BATCH);
    ObArenaAllocator allocator;
    ObList<ObTabletID, ObIAllocator> process_tablet_ids(allocator);
    for (int64_t i = 0; OB_SUCC(ret) && i < end_pos; i++) {
      if (OB_FAIL(check_stop_())) {
        LOG_WARN("fail to check stop", KR(ret));
      } else if (OB_FAIL(process_tablet_ids.push_back(store_tablet_ids.at(i)))) {
        LOG_WARN("fail to push back", KR(ret), K(store_tablet_ids.at(i)));
      }
    } // end for

    if (OB_SUCC(ret)) {
      ObArray<ObTabletLSCache> tablet_ls_caches; // not used
      if (OB_FAIL(check_stop_())) {
        LOG_WARN("fail to check stop", KR(ret));
      } else if (OB_ISNULL(tablet_ls_service_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("tablet_ls_service_ is null", KR(ret));
      } else if (OB_FAIL(tablet_ls_service_->batch_renew_tablet_ls_cache(
                 tenant_id, process_tablet_ids, tablet_ls_caches))) {
        LOG_WARN("fail to batch renew tablet ls cache",
                 KR(ret), K(tenant_id), "tablet_ids_cnt", process_tablet_ids.size());
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObTabletID> remain_tablet_ids;
      remain_tablet_ids.set_attr(SET_USE_500("TbltRefIDS"));
      if (OB_FAIL(rootserver::ObRootUtils::copy_array(store_tablet_ids,
          end_pos, store_tablet_ids.count(), remain_tablet_ids))) {
        LOG_WARN("fail to copy array", KR(ret), K(end_pos), K(store_tablet_ids.count()));
      } else {
        SpinRLockGuard guard(rwlock_);
        ObTabletLocationRefreshMgr *mgr = NULL;
        if (OB_FAIL(inner_get_mgr_(tenant_id, mgr))) {
          LOG_WARN("fail to get mgr", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(mgr)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("mgr is null, tenant has been dropped", KR(ret), K(tenant_id));
        } else if (OB_FAIL(mgr->set_tablet_ids(remain_tablet_ids))) {
          LOG_WARN("fail to set tablet_ids", KR(ret), K(tenant_id), K(remain_tablet_ids.count()));
        } else {
          has_task_ = (remain_tablet_ids.count() > 0);
        }
      }
    }

    FLOG_INFO("[REFRESH_TABLET_LOCATION] update tablet-ls caches for compatibility", KR(ret),
              K(tenant_id), "process_tablet_cnt", process_tablet_ids.size(),
              "remain_tablet_cnt", store_tablet_ids.count() - process_tablet_ids.size());
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
