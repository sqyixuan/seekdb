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

#include "storage/shared_storage/task/ob_find_tmp_file_flush_task.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_segment_file_manager.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObFindTmpFileFlushTask::ObFindTmpFileFlushTask()
  : is_inited_(false)
{
}

int ObFindTmpFileFlushTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFindTmpFileFlushTask has already been inited", KR(ret), K_(is_inited));
  } else {
    is_inited_ = true;
    LOG_INFO("succ to init find unsealed tmp file flush task");
  }
  return ret;
}

int ObFindTmpFileFlushTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t INTERVAL_US = ObSegmentFileManager::UNSEALED_TMP_FILE_FLUSH_THRESHOLD / 2; // 30s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("find unsealed tmp file flush task is not init", KR(ret));
  } else if (OB_UNLIKELY(-1 == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule find unsealed tmp file flush task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to start find unsealed tmp file flush task");
  }
  return ret;
}

void ObFindTmpFileFlushTask::destroy()
{
  is_inited_ = false;
}

void ObFindTmpFileFlushTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_mgr = nullptr;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager*)) || OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager or disk space manager is null", KR(ret), KP(file_mgr), KP(disk_space_mgr));
  } else if (!disk_space_mgr->is_unsealed_tmp_file_need_flush()) {
    // do nothing, do not need flush unsealed tmp file
  } else {
    ObSEArray<TmpFileSegId, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> seg_files;
    seg_files.set_attr(ObMemAttr(MTL_ID(), "SegFiles"));
    if (OB_FAIL(file_mgr->get_segment_file_mgr().find_unsealed_tmp_file_to_flush(seg_files))) {
      LOG_WARN("fail to find unsealed tmp file to flush", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < seg_files.count()); ++i) {
      MacroBlockId file_id;
      file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
      file_id.set_second_id(seg_files.at(i).tmp_file_id_);
      file_id.set_third_id(seg_files.at(i).segment_id_);
      if (OB_FAIL(file_mgr->push_to_flush_queue(file_id, 0/*ls_epoch_id*/, false/*is_sealed*/))) {
        LOG_WARN("fail to push to flush queue", KR(ret), K(file_id));
      }
    }
  }
}

} // storage
} // oceanbase
