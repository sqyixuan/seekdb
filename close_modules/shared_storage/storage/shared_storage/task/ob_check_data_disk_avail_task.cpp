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

#include "ob_check_data_disk_avail_task.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObCheckDataDiskAvailTask::ObCheckDataDiskAvailTask()
  : is_inited_(false)
{
}

int ObCheckDataDiskAvailTask::init(const int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t INTERVAL_US = 5 * 60 * 1000 * 1000L; // 5min
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCheckDataDiskAvailTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, INTERVAL_US, true/*schedule repeatly*/))) {
    LOG_WARN("fail to schedule check data disk available task", KR(ret), K(tg_id));
  } else {
    is_inited_ = true;
    LOG_INFO("succ to init check data disk available task", K(tg_id));
  }
  return ret;
}

void ObCheckDataDiskAvailTask::destroy()
{
  is_inited_ = false;
}

void ObCheckDataDiskAvailTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(check_data_disk_avail())) {
    LOG_WARN("fail to check data disk available", KR(ret));
  }
}

int ObCheckDataDiskAvailTask::check_data_disk_avail()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.check_disk_space_available())) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
      LOG_ERROR("the observer data disk is not available, maybe other files occupy the observer data disk space", KR(ret));
    } else {
      LOG_WARN("fail to check disk space available", KR(ret));
    }
  } else {
    LOG_INFO("succ to check data disk available");
  }
  return ret;
}

} // storage
} // oceanbase
