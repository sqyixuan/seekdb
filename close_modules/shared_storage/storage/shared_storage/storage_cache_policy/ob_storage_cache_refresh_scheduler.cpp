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
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_refresh_scheduler.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"

namespace oceanbase
{
namespace storage
{

/*-----------------------------------------ObStorageCachePolicyRefreshType-----------------------------------------*/
bool is_valid_scp_refresh_type(const ObStorageCachePolicyRefreshType &type)
{
  return type >= 0 && type < ObStorageCachePolicyRefreshType::REFRESH_TYPE_MAX;
}

const char *get_scp_refresh_type_str(const ObStorageCachePolicyRefreshType &type)
{
  static const char *SCP_REFRESH_TYPE_STR[] = {
    "REFRESH_TYPE_NORMAL",
    "REFRESH_TYPE_CONFIG_CHANGE",
    "REFRESH_TYPE_FORCE_REFRESH"
  };
  STATIC_ASSERT(
      static_cast<int64_t>(ObStorageCachePolicyRefreshType::REFRESH_TYPE_MAX) == ARRAYSIZEOF(SCP_REFRESH_TYPE_STR),
      "ObStorageCachePolicyRefreshType count mismatch");
  
  const char *str_ret = "UNKNOWN";
  if (OB_LIKELY(type >= 0 && type < ObStorageCachePolicyRefreshType::REFRESH_TYPE_MAX)) {
    str_ret = SCP_REFRESH_TYPE_STR[static_cast<int64_t>(type)];
  }
  return str_ret;
}

bool is_need_refresh_type(const ObStorageCachePolicyRefreshType &type)
{
  return ObStorageCachePolicyRefreshType::REFRESH_TYPE_CONFIG_CHANGE == type
      || ObStorageCachePolicyRefreshType::REFRESH_TYPE_FORCE_REFRESH == type;
}

int get_default_storage_cache_policy(
    const uint64_t tenant_id,
    bool &enable_manual_storage_cache_policy,
    ObStorageCachePolicyType &default_storage_cache_policy_type)
{
  int ret = OB_SUCCESS;
  enable_manual_storage_cache_policy = false;
  default_storage_cache_policy_type = ObStorageCachePolicyType::MAX_POLICY;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only user tenant is supported", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    const char *cur_dafult_storage_cache_policy = nullptr;
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tenant config", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(cur_dafult_storage_cache_policy =
        tenant_config->default_storage_cache_policy)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("default_storage_cache_policy is NULL", KR(ret), K(tenant_id));
    } else {
      enable_manual_storage_cache_policy = tenant_config->enable_manual_storage_cache_policy;
      default_storage_cache_policy_type =
          ObStorageCacheGlobalPolicy::get_type(cur_dafult_storage_cache_policy);
    }
  }
  return ret;
}

/*-----------------------------------------ObSCPTaskBase-----------------------------------------*/
ObSCPTaskBase::ObSCPTaskBase()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      policy_service_(nullptr)
{
}

ObSCPTaskBase::~ObSCPTaskBase()
{
  destroy();
}

void ObSCPTaskBase::destroy()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  policy_service_ = nullptr;
}

int ObSCPTaskBase::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if(OB_ISNULL(policy_service_ = MTL(ObStorageCachePolicyService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("policy service is null", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

/*-----------------------------------------ObStorageRefreshCachePolicyTask-----------------------------------------*/
bool ObStorageRefreshCachePolicyTask::trigger_force_refresh_ = false;

ObStorageRefreshCachePolicyTask::ObStorageRefreshCachePolicyTask()
  : ObSCPTaskBase(),
    last_default_scp_type_(ObStorageCachePolicyType::MAX_POLICY),
    // Tenant Para `enable_manual_storage_cache_policy` default is true
    last_enable_manual_storage_cache_policy_(true),
    last_is_suspend_storage_cache_task_(false)
{
}

void ObStorageRefreshCachePolicyTask::destroy()
{
  last_default_scp_type_ = ObStorageCachePolicyType::MAX_POLICY;
  last_enable_manual_storage_cache_policy_ = true;
  last_is_suspend_storage_cache_task_ = false;
  ObSCPTaskBase::destroy();
}

int ObStorageRefreshCachePolicyTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSCPTaskBase::init(tenant_id))) {
    LOG_WARN("fail to init base", KR(ret), K(tenant_id));
  } else if (OB_FAIL(is_suspend_storage_cache_task(
      tenant_id, last_is_suspend_storage_cache_task_))) {
    LOG_WARN("fail to get is_suspend_storage_cache_task", KR(ret), KPC(this));
  } else {
    bool force_refresh_scp = false;
    sync_tenant_param_(force_refresh_scp);
  }
  return ret;
}

class ObSuspendTabletTaskFunc
{
public:
  ObSuspendTabletTaskFunc() {}

  int operator()(const hash::HashMapPair<int64_t, ObStorageCacheTabletTaskHandle> &entry)
  {
    int ret = OB_SUCCESS;
    ObStorageCacheTabletTaskHandle tmp_task_handle(entry.second);
    ObStorageCacheTabletTask *task = tmp_task_handle.get_ptr();
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObStorageCacheTabletTask is NULL", KR(ret), K(entry.first));
    } else if (OB_FAIL(task->set_status(
        ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED))) {
      LOG_WARN("fail to suspend task", KR(ret), KPC(task));
    }
    return ret;
  }
};

using SCPTabletTaskHandleArr = ObSEArray<ObStorageCacheTabletTaskHandle, 64>;
class ObCopySuspendedTabletTaskFunc
{
public:
  ObCopySuspendedTabletTaskFunc(SCPTabletTaskHandleArr &tablet_tasks)
      : tablet_tasks_(tablet_tasks)
  {}

  int operator()(const hash::HashMapPair<int64_t, ObStorageCacheTabletTaskHandle> &entry)
  {
    int ret = OB_SUCCESS;
    ObStorageCacheTabletTaskHandle tmp_task_handle(entry.second);
    if (OB_UNLIKELY(!tmp_task_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObStorageCacheTabletTaskHandle is invalid",
          KR(ret), K(entry.first), KPC(entry.second.get_ptr()));
    } else if (tmp_task_handle()->get_status() != ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED) {
      // skip
    } else if (OB_FAIL(tablet_tasks_.push_back(tmp_task_handle))) {
      LOG_WARN("fail to copy handle", KR(ret), K(tablet_tasks_.count()));
    }
    return ret;
  }

private:
  SCPTabletTaskHandleArr &tablet_tasks_;
};

void ObStorageRefreshCachePolicyTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard scp_trace_id_guard;
  bool is_suspend = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(is_suspend_storage_cache_task(tenant_id_, is_suspend))) {
    LOG_WARN("fail to get is_suspend_storage_cache_task", KR(ret), KPC(this));
  } else if (last_is_suspend_storage_cache_task_ != is_suspend) {
    last_is_suspend_storage_cache_task_ = is_suspend;
    SCPTabletTaskMap &tablet_tasks_map = policy_service_->get_tablet_tasks();
    if (is_suspend) {
      // suspend all task in map
      ObSuspendTabletTaskFunc func;
      if (OB_FAIL(tablet_tasks_map.foreach_refactored(func))) {
        LOG_WARN("fail to suspend tablets tasks", KR(ret), K(tablet_tasks_map.size()), KPC(this));
      }
    } else {
      // restore suspend tasks
      SCPTabletTaskHandleArr task_arr_;
      task_arr_.set_attr(ObMemAttr(tenant_id_, "SCPRefreshTask"));
      ObCopySuspendedTabletTaskFunc func(task_arr_);
      if (OB_FAIL(tablet_tasks_map.foreach_refactored(func))) {
        LOG_WARN("fail to copy suspend tablets tasks", KR(ret),
            K(tablet_tasks_map.size()), KPC(this));
      }

      const int64_t suspend_task_num = task_arr_.count();
      const ObStorageCacheTabletTask *task = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < suspend_task_num; i++) {
        task = task_arr_.at(i).get_ptr();
        if (OB_ISNULL(task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ObStorageCacheTabletTask is NULL",
              KR(ret), K(i), K(suspend_task_num), KPC(this), KPC(task));
        } else if (OB_FAIL(policy_service_->tablet_scheduler_.push_task(
            task->get_tenant_id(),
            task->get_ls_id(),
            task->get_tablet_id(),
            task->get_policy_status()))) {
          LOG_WARN("fail to reprocess suspended task",
              KR(ret), K(i), K(suspend_task_num), KPC(this), KPC(task));
        }
      } // end for
    }
  }
  
  if (OB_FAIL(ret)) {
  } else if (!is_suspend) {
    bool force_refresh_scp = false;
    sync_tenant_param_(force_refresh_scp);
    ObStorageCachePolicyRefreshType refresh_type = REFRESH_TYPE_NORMAL;
    if (get_trigger_force_refresh()) {
      refresh_type = REFRESH_TYPE_FORCE_REFRESH;
    } else if (force_refresh_scp) {
      refresh_type = REFRESH_TYPE_CONFIG_CHANGE;
    }

    if (OB_FAIL(policy_service_->refresh_schema_policy_map(refresh_type))) {
      LOG_WARN("fail to refresh schema policy map",
          KR(ret), KPC(this), K_SCP_REFRESH_TYPE(refresh_type));
    }
  }
  set_trigger_force_refresh(false);
  LOG_TRACE("[SCP]finish ObStorageRefreshCachePolicyTask::runTimerTask", KR(ret), KPC(this), K(is_suspend));
}

void ObStorageRefreshCachePolicyTask::sync_tenant_param_(bool &force_refresh_scp)
{
  int ret = OB_SUCCESS;
  // force_refresh_scp is used to indicate whether to force refresh the storage cache policy
  // scp means storage_cache_policy
  force_refresh_scp = false;
  if (IS_INIT) {
    ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
    bool cur_enable_manual_storage_cache_policy = false;
    if (OB_SUCC(get_default_storage_cache_policy(
        tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
      if (cur_enable_manual_storage_cache_policy != last_enable_manual_storage_cache_policy_) {
        force_refresh_scp = true;
      } else if (!cur_enable_manual_storage_cache_policy
          && cur_default_scp_type != last_default_scp_type_) {
        force_refresh_scp = true;
      }

      last_enable_manual_storage_cache_policy_ = cur_enable_manual_storage_cache_policy;
      last_default_scp_type_ = cur_default_scp_type;
    }
  }
}

/*-----------------------------------------ObStorageCacheCleanHistoryTask-----------------------------------------*/
ObStorageCacheCleanHistoryTask::ObStorageCacheCleanHistoryTask()
  : ObSCPTaskBase()
{
}

void ObStorageCacheCleanHistoryTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard scp_trace_id_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(clean_history_())) {
    LOG_WARN("fail to clean history", KR(ret), KPC(this));
  }
}

int ObStorageCacheCleanHistoryTask::clean_history_()
{
  int ret = OB_SUCCESS;
  int64_t before_tablet_task_cnt = 0;
  int64_t after_tablet_task_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const int64_t cur_time = ObTimeUtility::current_time();
    SCPTabletTaskMap &tablet_tasks = policy_service_->get_tablet_tasks();
    before_tablet_task_cnt = tablet_tasks.size();
    if (before_tablet_task_cnt > OB_STORAGE_CACHE_TABLET_TASK_SIZE) {
      LOG_INFO("tablet task count is too large, need to clean history", K(before_tablet_task_cnt));
      SCPTabletTaskMap::iterator iter = tablet_tasks.begin();
      ObSEArray<int64_t, 64> erase_tablet_ids;
      erase_tablet_ids.set_attr(ObMemAttr(tenant_id_, "ObStorageCache"));
      for (;iter != tablet_tasks.end(); iter++) {
        ObStorageCacheTabletTaskHandle task_handle = iter->second;
        if (OB_ISNULL(task_handle())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task is null", KR(ret), K(task_handle()));
        } else if (task_handle()->is_completed() && (task_handle()->get_status() != ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED)) {
          if (task_handle()->get_end_time() + OB_STORAGE_CACHE_TABLET_TASK_EXPIRE_TIME < cur_time) {
            if (OB_FAIL(erase_tablet_ids.push_back(task_handle()->get_tablet_id()))) {
              LOG_WARN("fail to push back tablet id", KR(ret), K(task_handle()->get_tablet_id()));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < erase_tablet_ids.count(); ++i) {
          if (OB_FAIL(tablet_tasks.erase_refactored(erase_tablet_ids.at(i)))) {
            LOG_WARN("fail to erase task", KR(ret), K(erase_tablet_ids.at(i)));
          }
        }
      }
    }
    after_tablet_task_cnt = tablet_tasks.size();
  }
  FLOG_INFO("[SCP]clean history finish", KR(ret), K(before_tablet_task_cnt), K(after_tablet_task_cnt));
  return ret;
}

/*-----------------------------------------ObStorageDailyRefreshCachePolicyTask-----------------------------------------*/
ObStorageDailyRefreshCachePolicyTask::ObStorageDailyRefreshCachePolicyTask()
  : ObSCPTaskBase()
{
}

void ObStorageDailyRefreshCachePolicyTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard scp_trace_id_guard;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(policy_service_->refresh_schema_policy_map(REFRESH_TYPE_FORCE_REFRESH))) {
    LOG_WARN("fail to daily refresh schema policy map", KR(ret), KPC(this));
  }
}

/*-----------------------------------------ObStorageCacheRefreshPolicyScheduler-----------------------------------------*/
ObStorageCacheRefreshPolicyScheduler::ObStorageCacheRefreshPolicyScheduler()
  : is_inited_(false),
    is_stopped_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    tg_id_(OB_INVALID_TG_ID),
    refresh_policy_task_(),
    daily_refresh_policy_task_(),
    clean_history_task_()
{
}


int ObStorageCacheRefreshPolicyScheduler::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StorageCachePolicyMgr, tg_id_))) {
    LOG_WARN("fail to init timer", KR(ret));
  } else if (OB_FAIL(refresh_policy_task_.init(tenant_id))) {
    LOG_WARN("fail to init policy task", KR(ret));
  } else if (OB_FAIL(daily_refresh_policy_task_.init(tenant_id))) {
    LOG_WARN("fail to init daily policy task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(clean_history_task_.init(tenant_id))) {
    LOG_WARN("fail to init clean history task", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObStorageCacheRefreshPolicyScheduler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TG_ID == tg_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tg id", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer", KR(ret));
  } else if (OB_FAIL(schedule_task_(refresh_policy_task_, OB_STORAGE_CACHE_POLICY_RUNNER_INTERVAL))) {
    LOG_WARN("fail to schedule refresh policy task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(schedule_task_(daily_refresh_policy_task_, OB_STORAGE_CACHE_DAY))) {
    LOG_WARN("fail to schedule refresh daily policy task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(schedule_task_(clean_history_task_, OB_STORAGE_CACHE_CLEAN_HISTORY_INTERVAL))) {
    LOG_WARN("fail to schedule clean history task", KR(ret), K_(tg_id));
  }
  return ret;
}

void ObStorageCacheRefreshPolicyScheduler::stop()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
    is_stopped_ = true;
  }
}

void ObStorageCacheRefreshPolicyScheduler::wait()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
}

void ObStorageCacheRefreshPolicyScheduler::destroy()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
    is_inited_ = false;
    is_stopped_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    tg_id_ = OB_INVALID_TG_ID;
    refresh_policy_task_.destroy();
    daily_refresh_policy_task_.destroy();
    clean_history_task_.destroy();
  }
}
int ObStorageCacheRefreshPolicyScheduler::schedule_task_(
    ObTimerTask &task, const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageCacheRefreshPolicyScheduler not init", KR(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObStorageCacheRefreshPolicyScheduler is stopped", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("interval_us invalid", KR(ret), KPC(this), K(interval_us));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task, interval_us, true/*repeat*/))) {
    LOG_WARN("fail to schedule refresh tablet svr task", KR(ret), KPC(this), K(interval_us));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
