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

#include "ob_check_expand_disk_size_task.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObCheckExpandDiskTask::ObCheckExpandDiskTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID),
    tg_id_(INVALID_TG_ID)
{
}

int ObCheckExpandDiskTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCheckExpandDiskTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("succ to init check expand disk size task", K(tenant_id));
  }
  return ret;
}

int ObCheckExpandDiskTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t INTERVAL_US = 3L * 1000L * 1000L; // 3s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("check expand disk size task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, INTERVAL_US, true/*schedule repeatly*/))) {
    LOG_WARN("fail to schedule task ObCheckExpandDiskTask", KR(ret), K_(tenant_id), K(tg_id));
  } else {
    LOG_INFO("succ to start check expand disk size task", K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObCheckExpandDiskTask::destroy()
{
  tg_id_ = INVALID_TG_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObCheckExpandDiskTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(check_expand_disk_size())) {
    LOG_WARN("fail to check expand disk size", KR(ret), K_(tenant_id));
  }
}

int ObCheckExpandDiskTask::check_expand_disk_size()
{
  int ret = OB_SUCCESS;
  return ret;
}

} // storage
} // oceanbase
