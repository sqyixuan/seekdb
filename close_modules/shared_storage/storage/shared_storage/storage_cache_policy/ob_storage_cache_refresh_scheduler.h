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

#ifndef OCEANBASE_STORAGE_CACHE_REFRESH_SCHEDULER_H_
#define OCEANBASE_STORAGE_CACHE_REFRESH_SCHEDULER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/task/ob_timer.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls_tablet_service.h"

namespace oceanbase
{
namespace storage
{ 
class ObStorageCachePolicyService;

enum ObStorageCachePolicyRefreshType : uint8_t
{
  // Regular background periodic refresh
  // - Triggers tablet policy refresh when table schema version changes
  // - Generates prewarm tasks only when tablet status changes
  REFRESH_TYPE_NORMAL = 0,
  // Tenant configuration-driven refresh
  // - Forces refresh of all tablet policies
  // - Generates prewarm tasks when tablet status changes
  REFRESH_TYPE_CONFIG_CHANGE = 1,
  // Full enforcement refresh (currently daily refresh use only)
  // - Forces refresh of all tablet policies regardless of state
  // - Generates prewarm tasks unconditionally
  REFRESH_TYPE_FORCE_REFRESH = 2,
  REFRESH_TYPE_MAX
};

bool is_valid_scp_refresh_type(const ObStorageCachePolicyRefreshType &type);
const char *get_scp_refresh_type_str(const ObStorageCachePolicyRefreshType &type);
bool is_need_refresh_type(const ObStorageCachePolicyRefreshType &type);

#define K_SCP_REFRESH_TYPE(scp_refresh_type) K(scp_refresh_type),"scp_refresh_type_str",get_scp_refresh_type_str(scp_refresh_type)

int get_default_storage_cache_policy(
    const uint64_t tenant_id,
    bool &enable_manual_storage_cache_policy,
    ObStorageCachePolicyType &default_storage_cache_policy_type);

class ObSCPTaskBase : public common::ObTimerTask
{
public:
  ObSCPTaskBase();
  virtual ~ObSCPTaskBase();
  virtual void destroy();

  virtual int init(const uint64_t tenant_id);

  TO_STRING_KV(K_(tenant_id), K_(is_inited), KPC(policy_service_));

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  ObStorageCachePolicyService *policy_service_;
};

class ObStorageRefreshCachePolicyTask : public ObSCPTaskBase
{
public:
  ObStorageRefreshCachePolicyTask();
  virtual ~ObStorageRefreshCachePolicyTask() { destroy(); }
  virtual void destroy() override;

  virtual int init(const uint64_t tenant_id);
  virtual void runTimerTask() override;

  static bool get_trigger_force_refresh()
  {
    return ATOMIC_LOAD(&ObStorageRefreshCachePolicyTask::trigger_force_refresh_);
  }
  static void set_trigger_force_refresh(const bool trigger_force_refresh)
  {
    IGNORE_RETURN ATOMIC_SET(
      &ObStorageRefreshCachePolicyTask::trigger_force_refresh_, trigger_force_refresh);
  }

  INHERIT_TO_STRING_KV("ObSCPTaskBase", ObSCPTaskBase,
      K(last_default_scp_type_), K(last_enable_manual_storage_cache_policy_),
      K(last_is_suspend_storage_cache_task_), K(trigger_force_refresh_));

private:
  void sync_tenant_param_(bool &force_refresh_scp);

private:
  ObStorageCachePolicyType last_default_scp_type_;
  bool last_enable_manual_storage_cache_policy_;
  bool last_is_suspend_storage_cache_task_;
  static bool trigger_force_refresh_;
};

class ObStorageCacheCleanHistoryTask : public ObSCPTaskBase
{
public:
  ObStorageCacheCleanHistoryTask();
  virtual ~ObStorageCacheCleanHistoryTask() { destroy(); }
  virtual void runTimerTask() override;
public:
  const static int64_t OB_STORAGE_CACHE_TABLET_TASK_EXPIRE_TIME = 1 * 3600LL * 1000LL * 1000LL; // 1h
  const static int64_t OB_STORAGE_CACHE_TABLET_TASK_SIZE = 1000;
  
private:
  int clean_history_();
};

class ObStorageDailyRefreshCachePolicyTask : public ObSCPTaskBase
{
public:
  ObStorageDailyRefreshCachePolicyTask();
  virtual ~ObStorageDailyRefreshCachePolicyTask() { destroy(); }
  virtual void runTimerTask() override;
};

class ObStorageCacheRefreshPolicyScheduler
{
public:
  ObStorageCacheRefreshPolicyScheduler();
  ~ObStorageCacheRefreshPolicyScheduler() { destroy(); }
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
  TO_STRING_KV(K_(tenant_id), K_(is_inited), K_(is_stopped), 
      K_(tg_id), K_(refresh_policy_task));

private:
  int schedule_task_(common::ObTimerTask &task, const int64_t interval_us);

private:
  const static int64_t OB_STORAGE_CACHE_POLICY_RUNNER_INTERVAL = 30 * 1000 * 1000; // 30s
  const static int64_t OB_STORAGE_CACHE_DAY = 24LL * 3600LL * 1000LL * 1000LL; // 24h
  const static int64_t OB_STORAGE_CACHE_CLEAN_HISTORY_INTERVAL = 1 * 3600LL * 1000LL * 1000LL; // 1h

private:
  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  int tg_id_;
  ObStorageRefreshCachePolicyTask refresh_policy_task_;
  ObStorageDailyRefreshCachePolicyTask daily_refresh_policy_task_;
  ObStorageCacheCleanHistoryTask clean_history_task_;
};

} // namespace storage
} // namespace oceanbase

#endif
