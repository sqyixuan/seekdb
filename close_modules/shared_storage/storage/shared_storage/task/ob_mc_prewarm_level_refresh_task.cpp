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

#include "ob_mc_prewarm_level_refresh_task.h"
#include "storage/shared_storage/prewarm/ob_ss_micro_cache_prewarm_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::compaction;

ObMCPrewarmLevelRefreshTask::ObMCPrewarmLevelRefreshTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObMCPrewarmLevelRefreshTask::~ObMCPrewarmLevelRefreshTask()
{
  destroy();
}

int ObMCPrewarmLevelRefreshTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mc prewarm level refresh task has already been inited", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
  }
  LOG_INFO("finish to init mc prewarm level refresh task", KR(ret));
  return ret;
}

int ObMCPrewarmLevelRefreshTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mc prewarm level refresh task is not init", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL_US, true/*repeate*/))) {
    LOG_WARN("fail to schedule mc prewarm level refresh task", KR(ret), K_(tenant_id), K(tg_id));
  }
  LOG_INFO("finish to start mc prewarm level refresh task", KR(ret), K_(tenant_id), K(tg_id));
  return ret;
}

void ObMCPrewarmLevelRefreshTask::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObMCPrewarmLevelRefreshTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSSMicroCachePrewarmService *prewarm_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mc prewarm level refresh task is not init", KR(ret));
  } else if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache prewarm service is null", KR(ret));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (OB_LIKELY(tenant_config.is_valid())) {
      ObSSMajorPrewarmLevel prewarm_level = static_cast<ObSSMajorPrewarmLevel>(static_cast<uint8_t>(
                                            tenant_config->_ss_major_compaction_prewarm_level));
      prewarm_service->set_major_prewarm_level(prewarm_level);
    }
  }
}

} // storage
} // oceanbase
