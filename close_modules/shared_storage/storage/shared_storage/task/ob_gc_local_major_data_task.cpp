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

#include "ob_gc_local_major_data_task.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObGCLocalMajorDataTask::ObGCLocalMajorDataTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID),
    tg_id_(INVALID_TG_ID)
{
}

int ObGCLocalMajorDataTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObGCLocalMajorDataTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("succ to init gc local major data task", K(tenant_id));
  }
  return ret;
}

int ObGCLocalMajorDataTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gc local major data task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, 0/*delay*/, false /*no schedule repeatly*/))) {
    LOG_WARN("fail to schedule task ObGCLocalMajorDataTask", KR(ret), K_(tenant_id), K(tg_id));
  } else {
    LOG_INFO("succ to start gc local major data task", K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObGCLocalMajorDataTask::destroy()
{
  tg_id_ = INVALID_TG_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObGCLocalMajorDataTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(gc_local_major_data())) {
    LOG_WARN("fail to gc local major data", KR(ret), K_(tenant_id));
  }

  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    // if gc major_data failed, schedule task again until gc succeeds
    if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, DELAY_US/*delay*/, false /*no schedule repeatly*/))) {
      LOG_WARN("fail to schedule task ObGCLocalMajorDataTask", KR(ret));
    }
  }
}

int ObGCLocalMajorDataTask::gc_local_major_data()
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_mgr = nullptr;
  int64_t delete_time_stamp_s = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), KP(file_mgr), K_(tenant_id));
  } else if (FALSE_IT(delete_time_stamp_s = file_mgr->get_start_server_time_s())) {
  } else if (OB_FAIL(file_mgr->delete_local_major_data_dir(delete_time_stamp_s))) {
    LOG_WARN("fail to delete local major data dir", KR(ret), K(delete_time_stamp_s));
  } else {
    LOG_INFO("succ to delete local major data dir", K_(tenant_id));
  }
  return ret;
}

} // storage
} // oceanbase
