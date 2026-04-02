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

#include "ob_calibrate_disk_space_task.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_disk_space_meta.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObCalibrateDiskSpaceTask::ObCalibrateDiskSpaceTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), is_stop_(false)
{
}

int ObCalibrateDiskSpaceTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCalibrateDiskSpaceTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    LOG_INFO("succ to init calibrate disk space task", K(tenant_id), K_(is_stop));
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::start()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  const int64_t INTERVAL_US = 4 * 60 * 60 * 1000 * 1000L; // 4h
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("calibrate disk space task is not init", KR(ret));
  } else if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, INTERVAL_US, true/*schedule repeatly*/))) {
    LOG_WARN("fail to schedule task ObCalibrateDiskSpaceTask", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("succ to start calibrate disk space task", K_(tenant_id), K_(is_stop));
  }
  return ret;
}

void ObCalibrateDiskSpaceTask::stop()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else {
    TG_CANCEL_TASK(timer->get_tg_id(), *this);
    is_stop_ = true;
    LOG_INFO("ObCalibrateDiskSpaceTask stop finished", K_(is_stop));
  }
}

void ObCalibrateDiskSpaceTask::wait()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else {
    TG_WAIT_TASK(timer->get_tg_id(), *this);
    LOG_INFO("ObCalibrateDiskSpaceTask wait finished");
  }
}

void ObCalibrateDiskSpaceTask::destroy()
{
  stop();
  wait();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObCalibrateDiskSpaceTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  ObTenantStorageMetaService *tsms = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tsms = MTL(ObTenantStorageMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is null", KR(ret), KP(tsms), K_(tenant_id));
  } else if (!tsms->is_started()) {
    // if ObTenantStorageMetaService do not started, do not calibrate disk space, because it will pause gc when calibrate disk space,
    // tablet_current_version must delete success when ObTenantStorageMetaService is starting, otherwise server restart failed
    if (OB_FAIL(schedule_calibrate_disk_space())) {
      LOG_WARN("fail to schedule task ObCalibrateDiskSpaceTask", KR(ret));
    }
  } else if (OB_FAIL(calibrate_disk_space())) {
    LOG_WARN("fail to calibrate tenant disk space", KR(ret), K_(tenant_id));
  }
}

int ObCalibrateDiskSpaceTask::schedule_calibrate_disk_space()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = nullptr;
  const int64_t INTERVAL_US = 5 * 1000 * 1000L; // 5s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("calibrate disk space task is not init", KR(ret));
  } else if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSharedTimer is NULL", KR(ret), KP(timer));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, INTERVAL_US/*delay*/, false/*schedule no repeatly*/))) {
    LOG_WARN("fail to schedule task ObCalibrateDiskSpaceTask", KR(ret), K_(tenant_id));
  } else {
    LOG_INFO("succ to schedule calibrate disk space");
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::calibrate_disk_space()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  ObTenantFileManager *file_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*)) ||
             OB_ISNULL(file_mgr = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager or file manager is null", KR(ret), KP(tnt_disk_space_mgr), KP(file_mgr), K_(tenant_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    // if choose start_calc_size_time_s failed, retry 3 times
    int64_t retry_count = 3;
    int64_t start_calc_size_time_s = 0;
    do {
      if (OB_FAIL(choose_start_calc_size_time_s(start_calc_size_time_s))) {
        LOG_WARN("fail to choose start calculate size time", KR(ret));
        ob_usleep(10 * 1000L); // 10ms
      }
    } while (OB_FAIL(ret) && (--retry_count > 0));

    if (OB_FAIL(ret)) {
    } else {
      // when begin to calibrate disk space, pause gc
      file_mgr->set_pause_gc();
      file_mgr->set_tmp_file_cache_pause_gc();
      const int64_t initial_private_macro_alloc_size = tnt_disk_space_mgr->get_private_macro_alloc_size();
      const int64_t initial_meta_file_alloc_size = tnt_disk_space_mgr->get_meta_file_alloc_size();
      const int64_t initial_major_macro_read_cache_alloc_size = tnt_disk_space_mgr->get_major_macro_read_cache_alloc_size();
      const int64_t initial_tmp_file_write_cache_alloc_size = tnt_disk_space_mgr->get_tmp_file_write_cache_alloc_size();
      const int64_t initial_tmp_file_read_cache_alloc_size = tnt_disk_space_mgr->get_tmp_file_read_cache_alloc_size();
      LOG_INFO("begin to calibrate data disk space", K_(tenant_id),
               "private_macro_alloc_size(MB)", initial_private_macro_alloc_size/ObTenantFileManager::MB,
               "meta_file_alloc_size(MB)", initial_meta_file_alloc_size/ObTenantFileManager::MB,
               "tmp_file_write_cache_alloc_size(MB)", initial_tmp_file_write_cache_alloc_size/ObTenantFileManager::MB,
               "tmp_file_read_cache_alloc_size(MB)", initial_tmp_file_read_cache_alloc_size/ObTenantFileManager::MB,
               "major_macro_read_cache_alloc_size(MB)", initial_major_macro_read_cache_alloc_size/ObTenantFileManager::MB);

      // step 1: calibrate tmp_file_write_cache alloc size and tmp_file_read_cache alloc size
      if (!is_stop_ && OB_TMP_FAIL(calibrate_alloc_disk_size(ObStorageObjectType::TMP_FILE, start_calc_size_time_s,
                                                             initial_tmp_file_write_cache_alloc_size, 
                                                             initial_tmp_file_read_cache_alloc_size))) {
        LOG_WARN("fail to calibrate tmp data disk space", KR(tmp_ret));
      }
      file_mgr->set_tmp_file_cache_allow_gc();
      // step 2: calibrate private_macro alloc size
      if (!is_stop_ && OB_TMP_FAIL(calibrate_alloc_disk_size(ObStorageObjectType::PRIVATE_DATA_MACRO, start_calc_size_time_s, initial_private_macro_alloc_size))) {
        LOG_WARN("fail to calibrate private macro disk space", KR(tmp_ret));
      }
      // step 3: calibrate major_macro_read_cache alloc size
      if (!is_stop_ && OB_TMP_FAIL(calibrate_alloc_disk_size(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, start_calc_size_time_s, initial_major_macro_read_cache_alloc_size))) {
        LOG_WARN("fail to calibrate major data disk space", KR(tmp_ret));
      }
      // step 4: calibrate meta_file alloc size
      if (!is_stop_ && OB_TMP_FAIL(calibrate_alloc_disk_size(ObStorageObjectType::PRIVATE_TABLET_META, start_calc_size_time_s, initial_meta_file_alloc_size))) {
        LOG_WARN("fail to calibrate meta file disk space", KR(tmp_ret));
      }
    }

    const ObTenantAllDiskCacheInfo &all_disk_cache_info = tnt_disk_space_mgr->get_all_disk_cache_info();
    LOG_INFO("finish to calibrate data disk space", KR(tmp_ret), KR(ret), K_(tenant_id), "cache_alloc_info",
      all_disk_cache_info.alloc_info_, "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    // when finish to calibrate disk space, allow gc
    file_mgr->set_allow_gc();
    // delete tenant_id dir's .tmp.seq files, for example TENANT_DISK_SPACE_META,TENANT_SUPER_BLOCK,TENANT_UNIT_META
    ObDirCalcSizeOp del_tmp_seq_file_op(is_stop_);
    if (!is_stop_ && OB_TMP_FAIL(delete_tmp_seq_files(del_tmp_seq_file_op))) {
      LOG_WARN("fail to delete tmp seq files", KR(tmp_ret));
    }
    // delete tmp_data dir's .deleted files
    if (!is_stop_ && OB_TMP_FAIL(rm_logical_deleted_file())) {
      LOG_WARN("fail to rm logical deleted files", KR(tmp_ret));
    }
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::calibrate_alloc_disk_size(const ObStorageObjectType object_type,
                                                        const int64_t start_calc_size_time_s,
                                                        const int64_t initial_alloc_size,
                                                        const int64_t initial_tmp_file_read_cache_alloc_size)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  ObTenantFileManager *file_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((start_calc_size_time_s <= 0) || (initial_alloc_size < 0) ||
                         (initial_tmp_file_read_cache_alloc_size < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(initial_alloc_size), K(initial_tmp_file_read_cache_alloc_size));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K_(tenant_id));
  } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), KP(file_mgr), K_(tenant_id));
  } else {
    int64_t total_alloc_size = 0;   // calculate dir size by list
    int64_t cur_alloc_size = 0;     // cur alloc size when finish calculate dir size

    // because calculate tmp_data dir distinguish tmp_file_read_cache_alloc_size and tmp_file_write_cache_alloc_size at the same time,
    // especially using these variables for tmp_file_read_cache
    int64_t total_tmp_file_read_cache_alloc_size = 0;
    int64_t cur_tmp_file_read_cache_alloc_size = 0;
    if (tnt_disk_space_mgr->is_private_macro_objtype(object_type)) {
      if (OB_FAIL(file_mgr->calc_private_macro_disk_space(start_calc_size_time_s, total_alloc_size))) {
        LOG_WARN("fail to calc private macro disk space", KR(ret), K_(tenant_id), K(total_alloc_size));
      } else {
        cur_alloc_size = tnt_disk_space_mgr->get_private_macro_alloc_size();
      }
    } else if (tnt_disk_space_mgr->is_private_tablet_meta_objtype(object_type)) {
      if (OB_FAIL(file_mgr->calc_meta_file_disk_space(start_calc_size_time_s, total_alloc_size))) {
        LOG_WARN("fail to calc meta file disk space", KR(ret), K_(tenant_id), K(total_alloc_size));
      } else {
        cur_alloc_size = tnt_disk_space_mgr->get_meta_file_alloc_size();
      }
    } else if (tnt_disk_space_mgr->is_major_macro_objtype(object_type)) {
      if (OB_FAIL(file_mgr->calc_major_macro_disk_space(start_calc_size_time_s, total_alloc_size))) {
        LOG_WARN("fail to calc major macro disk space", KR(ret), K_(tenant_id), K(total_alloc_size));
      } else {
        cur_alloc_size = tnt_disk_space_mgr->get_major_macro_read_cache_alloc_size();
      }
    } else if (ObStorageObjectType::TMP_FILE == object_type) {
      if (OB_FAIL(file_mgr->calc_tmp_data_disk_space(start_calc_size_time_s, total_tmp_file_read_cache_alloc_size, total_alloc_size))) {
        LOG_WARN("fail to calc tmp data disk space", KR(ret), K_(tenant_id), K(total_tmp_file_read_cache_alloc_size), K(total_alloc_size));
      } else {
        cur_alloc_size = tnt_disk_space_mgr->get_tmp_file_write_cache_alloc_size();
        cur_tmp_file_read_cache_alloc_size = tnt_disk_space_mgr->get_tmp_file_read_cache_alloc_size();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected object type", KR(ret), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
    }
    if (OB_FAIL(ret)) {
    } else {
      // delta_alloc_size is the size of change during the calculation process
      const int64_t delta_alloc_size = cur_alloc_size - initial_alloc_size;
      total_alloc_size += delta_alloc_size;
      if (OB_FAIL(tnt_disk_space_mgr->calibrate_alloc_size(total_alloc_size, object_type))) {
        LOG_WARN("fail to calibrate alloc disk size", KR(ret), K_(tenant_id), K(total_alloc_size), K(object_type),
          "object_type_str", get_storage_objet_type_str(object_type));
      } else if (ObStorageObjectType::TMP_FILE == object_type) {
        // if calculate tmp_data dir, need check calibrate disk space for tmp_file_read_cache
        total_tmp_file_read_cache_alloc_size += (cur_tmp_file_read_cache_alloc_size - initial_tmp_file_read_cache_alloc_size);
        if (OB_FAIL(tnt_disk_space_mgr->calibrate_alloc_size(total_tmp_file_read_cache_alloc_size, object_type, true/*is_tmp_file_read_cache*/))) {
          LOG_WARN("fail to calibrate alloc disk size", KR(ret), K_(tenant_id), K(total_tmp_file_read_cache_alloc_size), K(object_type), 
            "object_type_str", get_storage_objet_type_str(object_type));
        }
      }
      const int64_t deviation_alloc_size = std::abs(total_alloc_size - cur_alloc_size);
      LOG_INFO("finish to calibrate alloc disk size", KR(ret), K_(tenant_id), K(object_type), "object_type_str",
        get_storage_objet_type_str(object_type), K(total_alloc_size), K(deviation_alloc_size),  K(cur_alloc_size), 
        K(initial_alloc_size), K(delta_alloc_size), K(total_tmp_file_read_cache_alloc_size),
        K(initial_tmp_file_read_cache_alloc_size), K(cur_tmp_file_read_cache_alloc_size));
    }
  }
  return ret;
}

int ObCalibrateDiskSpaceTask::choose_start_calc_size_time_s(int64_t &start_calc_size_time_s)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    // because statbuf.mtime_s_ is second level, maybe at start_calc_time_s has written some files,
    // ObDirCalcSizeOp statistics file size when statbuf.mtime_s_ < start_calc_size_time_s_
    // so when calibrate disk space, need sleep a while, let start_calc_time_s just pass current second
    const int64_t initial_time_us = ObTimeUtility::current_time();
    const int64_t initial_ms = (initial_time_us / 1000) % 1000;
    const int64_t need_sleep_us = (1000 - initial_ms + 1) * 1000; // just pass current second 2ms, so plus 1ms
    ob_usleep(need_sleep_us);
    const int64_t start_calc_size_time_us = ObTimeUtility::current_time();
    start_calc_size_time_s = start_calc_size_time_us / 1000000;
    const int64_t cur_ms = (start_calc_size_time_us / 1000) % 1000;
    // start_calc_size_time_s must equal initial_time_s plus 1, and start_calc_size_time_s's cur_ms less than 2ms
    if ((start_calc_size_time_s != (initial_time_us / 1000000 + 1)) || (cur_ms > 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected start_calc_size_time_s", KR(ret), K(start_calc_size_time_s), K(cur_ms), K(start_calc_size_time_us));
    }
    LOG_INFO("finish to choose start calc size time", KR(ret), K(initial_ms), K(need_sleep_us),
             K(initial_time_us), K(start_calc_size_time_us), K(start_calc_size_time_s));
  }
  return ret;
}

/*
 delete tenant_id dir's .tmp.seq files, for example TENANT_DISK_SPACE_META.tmp.seq, TENANT_SUPER_BLOCK.tmp.seq, TENANT_UNIT_META.tmp.seq,
 because the server is killed when writing the .tmp.seq file, the rename function will not be called, leaving the .tmp.seq files
 */
int ObCalibrateDiskSpaceTask::delete_tmp_seq_files(ObDirCalcSizeOp &del_tmp_seq_file_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    char tenant_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(tenant_path, sizeof(tenant_path), MTL_ID(), MTL_EPOCH_ID()))) {
      LOG_WARN("fail to get local tenant dir", KR(ret));
    } else if (OB_FAIL(del_tmp_seq_file_op.set_dir(tenant_path))) {
      LOG_WARN("fail to set dir", KR(ret), K(tenant_path));
    } else if (OB_FAIL(share::ObIODeviceLocalFileOp::scan_dir(tenant_path, del_tmp_seq_file_op))) {
      LOG_WARN("fail to scan dir", KR(ret), K(tenant_path));
    }
  }
  return ret;
}

/*
 delete tmp_data dir's .deleted files.
 because if it delete tmp file when calibrating disk space, rename the file to .deleted file.
 after calibrating disk space, list all tmp files and delete the .deleted file.
 */
int ObCalibrateDiskSpaceTask::rm_logical_deleted_file()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    ObRMLogicalDeletedFileOp rm_logical_deleted_file_op(is_stop_);
    ObDelTmpFileDirOp del_dir_op(is_stop_);
    char tmp_data_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_local_tmp_data_dir(tmp_data_path, sizeof(tmp_data_path), MTL_ID(), MTL_EPOCH_ID()))) {
      LOG_WARN("fail to get local tmp data dir", KR(ret));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir_rec(tmp_data_path, rm_logical_deleted_file_op, del_dir_op))) {
      LOG_WARN("fail to scan dir rec", KR(ret), K(tmp_data_path));
    } else {
      LOG_INFO("succ to rm logical deleted file", "total_file_count", rm_logical_deleted_file_op.get_total_file_count());
    }
  }
  return ret;
}

} // storage
} // oceanbase
