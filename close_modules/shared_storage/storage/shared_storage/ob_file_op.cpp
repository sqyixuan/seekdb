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
#include "ob_file_op.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_helper.h"


namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/**
 * --------------------------------ObDirCalcSizeOp------------------------------------
 */

ObDirCalcSizeOp::ObDirCalcSizeOp(volatile bool &is_stop, const char *dir)
  : ObScanDirOp(dir),
    is_stop_(is_stop),
    total_file_size_(0),
    start_calc_size_time_s_(0),
    is_tmp_file_calc_(false),
    total_tmp_file_read_cache_size_(0),
    tmp_seq_file_del_secy_time_s_(DEFAULT_TMP_SEQ_FILE_DELETE_SECURITY_TIME_S)
{
}

int ObDirCalcSizeOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  const char *dir = get_dir();
  if (OB_ISNULL(entry) || OB_ISNULL(dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret), KP(entry), KP(dir));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_CANCELED;
    LOG_WARN("dir calculate size is stop", KR(ret), K_(is_stop));
  } else {
    char full_path[common::MAX_PATH_SIZE] = { 0 };
    int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir, entry->d_name);
    if (p_ret <= 0 || p_ret >= sizeof(full_path)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("file name too long", KR(ret), K(dir), K(entry->d_name));
    } else {
      ObIODFileStat statbuf;
      // ::rename operation does not modify statbuf.mtime_s_
      if (OB_FAIL(ObIODeviceLocalFileOp::stat(full_path, statbuf))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to stat file", K(full_path), K(statbuf));
        }
      } else if (!S_ISREG(statbuf.mode_) && !S_ISDIR(statbuf.mode_)) {
        // do nothing, only print WARNING log, cannot report error code
        LOG_WARN("unexpcted file in current dir", K(dir), K(full_path), K(statbuf.mode_));
      } else if ((NULL != STRSTR(entry->d_name, DEFAULT_TMP_STR)) && S_ISREG(statbuf.mode_)) {
        // if .tmp.seq file has reached security deleting time(default 24h), need gc
        if (ObTimeUtility::current_time_s() > (statbuf.mtime_s_ + tmp_seq_file_del_secy_time_s_)) {
          if (OB_FAIL(ObIODeviceLocalFileOp::unlink(full_path))) {
            if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to del file", KR(ret), K(full_path));
            }
          }
        } else if (statbuf.mtime_s_ < start_calc_size_time_s_) {
          // .tmp.seq file has not reached security deleting time, need statistics file size
          total_file_size_ += statbuf.size_;
        }
      } else if (statbuf.mtime_s_ < start_calc_size_time_s_) { // only statistics the file before start_calc_size_time
        // if calculate tmp_file dir size, need check file_id if exist in lru,
        // if file_id exists in lru, this file_id is tmp_file_read_cache file;
        // otherwise this file_id is tmp_file_write_cache file
        if (is_tmp_file_calc_) {
          ObTenantFileManager *file_mgr = nullptr;
          bool is_exist = false;
          MacroBlockId segment_file_id;
          if (false == ObString(entry->d_name).is_numeric() && (NULL == STRSTR(entry->d_name, DEFAULT_DELETED_STR)) ) {
            // unexpected file path need calculate size, and print warning log
            total_file_size_ += statbuf.size_;
            // do nothing, only print log
            LOG_WARN("unexpected path", K(entry->d_name));
          } else if (OB_FAIL(ObFileHelper::tmpfile_path_to_macro_id(dir, entry->d_name, segment_file_id))) {
            LOG_WARN("fail to convert tmpfile path to macro id", KR(ret), K(dir), K(entry->d_name));
          } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager*))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant file manager is null", KR(ret), KP(file_mgr));
          } else if (OB_FAIL(file_mgr->get_preread_cache_mgr().is_exist_in_lru(segment_file_id, is_exist))) {
            LOG_WARN("fail to judge is exist in lru", KR(ret), K(segment_file_id));
          } else if (is_exist) {
            // if file_id exists in lru, this file_id is tmp_file_read_cache file
            total_tmp_file_read_cache_size_ += statbuf.size_;
          } else {
            // if file_id not exists in lru, this file_id is tmp_file_write_cache file
            total_file_size_ += statbuf.size_;
          }
        } else {
          total_file_size_ += statbuf.size_;
        }
      }
    }
  }
  return ret;
}

int ObDirCalcSizeOp::set_start_calc_size_time(const int64_t start_calc_size_time_s)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_calc_size_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_size_time_s));
  } else {
    start_calc_size_time_s_ = start_calc_size_time_s;
  }
  return ret;
}

int ObDirCalcSizeOp::set_tmp_seq_file_del_secy_time_s(const int64_t del_secy_time_s)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(del_secy_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(del_secy_time_s));
  } else {
    tmp_seq_file_del_secy_time_s_ = del_secy_time_s;
  }
  return ret;
}

/**
 * --------------------------------ObMajorDataDeleteOp------------------------------------
 */

ObMajorDataDeleteOp::ObMajorDataDeleteOp(const char *dir, const int64_t time_stamp)
  : dir_(dir),
    time_stamp_(time_stamp),
    total_file_count_(0)
{
}

int ObMajorDataDeleteOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else {
    char full_path[common::MAX_PATH_SIZE] = { 0 };
    ObIODFileStat statbuf;
    if (OB_FAIL(databuff_printf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name))) {
      LOG_WARN("fail to databuff printf", KR(ret), K_(dir), K(entry->d_name));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(full_path, statbuf))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to stat file", K(full_path), K(statbuf));
      }
    // if the file write time before observer restart time, need delete
    } else if (statbuf.mtime_s_ < time_stamp_) {
      ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
      if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant disk space manager is null", KR(ret), KP(disk_space_mgr));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(full_path))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to del file", KR(ret), K(full_path));
        }
      // because DATA_MACRO and META_MACRO sharing use alloc_size_, so do not need to parse d_name distinguish DATA_MACRO or META_MACRO
      } else if (OB_FAIL(disk_space_mgr->free_file_size(statbuf.size_, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, false/*is_tmp_file_read_cache*/))) {
        LOG_WARN("fail to free major macro size", KR(ret), K(statbuf.size_));
      } else {
        total_file_count_++;
      }
    }
  }
  return ret;
}

int ObMajorDataDeleteOp::set_dir(const char *dir)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dir == nullptr || strlen(dir) >= common::MAX_PATH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir));
  } else {
    dir_ = dir;
  }
  return ret;
}

int ObMajorDataDeleteOp::set_time_stamp(const int64_t time_stamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(time_stamp <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(time_stamp));
  } else {
    time_stamp_ = time_stamp;
  }
  return ret;
}

/**
 * --------------------------------ObSharedTenantDirListOp------------------------------------
 */

ObSharedTenantDirListOp::ObSharedTenantDirListOp(const uint64_t tenant_id) : file_list_()
{
  // tenant id is used to set ObMemAttr
  file_list_.set_attr(ObMemAttr(tenant_id, "ListSharedTen"));
}

int ObSharedTenantDirListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (1 != sscanf(entry->d_name, "tenant_%lu", &tenant_id)) {
    // do nothing, maybe other files
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(tenant_id));
  } else if (OB_FAIL(file_list_.push_back(tenant_id))) {
    LOG_WARN("failed to push back", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSharedTenantDirListOp::get_file_list(common::ObIArray<uint64_t> &files) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(files.assign(file_list_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}


/**
 * --------------------------------ObSingleNumFileListOp------------------------------------
 */

ObSingleNumFileListOp::ObSingleNumFileListOp() : file_list_()
{
  file_list_.set_attr(ObMemAttr(MTL_ID(), "ListSinFile"));
}

int ObSingleNumFileListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  int64_t file_id = -1;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (false == ObString(entry->d_name).is_numeric()) {
    // do nothing, only print log
    LOG_WARN("unexpected path", K(entry->d_name));
  } else if (OB_FAIL(ObFileHelper::parse_file_name(entry->d_name, file_id))) {
    LOG_WARN("fail to parse file name", KR(ret));
  } else if (OB_FAIL(file_list_.push_back(file_id))) {
    LOG_WARN("failed to push back", KR(ret), K(file_id));
  }
  return ret;
}

int ObSingleNumFileListOp::get_file_list(common::ObIArray<int64_t> &files) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(files.assign(file_list_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

/**
 * --------------------------------ObDoubleNumFileListOp------------------------------------
 */

ObDoubleNumFileListOp::ObDoubleNumFileListOp() : file_list_()
{
  file_list_.set_attr(ObMemAttr(MTL_ID(), "ListDouFile"));
}

int ObDoubleNumFileListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  int64_t first_id = -1;
  int64_t second_id = -1;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (2 != sscanf(entry->d_name, "%ld_%ld", &first_id, &second_id)) {
    // do nothing, only print log
    LOG_WARN("unexpected path", K(entry->d_name));
  } else if (OB_UNLIKELY(first_id < 0 || second_id < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(first_id), K(second_id));
  } else {
    std::pair<int64_t, int64_t> file_id;
    file_id.first = first_id;
    file_id.second = second_id;
    if (OB_FAIL(file_list_.push_back(file_id))) {
      LOG_WARN("failed to push back", KR(ret), K(first_id), K(second_id));
    }
  }
  return ret;
}

int ObDoubleNumFileListOp::get_file_list(common::ObIArray<std::pair<int64_t, int64_t>> &files) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(files.assign(file_list_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

/**
 * --------------------------------ObRMLogicalDeletedFileOp------------------------------------
 */

int ObRMLogicalDeletedFileOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  const char *dir = get_dir();
  if (OB_ISNULL(entry) || OB_ISNULL(dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret), KP(entry), KP(dir));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_CANCELED;
    LOG_WARN("del deleted file is stop", KR(ret), K_(is_stop));
  } else if (NULL != STRSTR(entry->d_name, DEFAULT_DELETED_STR)) { // only delete .deleted file
    MacroBlockId file_id;
    ObTenantFileManager *file_mgr = nullptr;
    if (OB_FAIL(ObFileHelper::tmpfile_path_to_macro_id(dir, entry->d_name, file_id))) {
      LOG_WARN("fail to convert tmpfile path to macro id", KR(ret), K(dir), K(entry->d_name));
    } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant file manager is null", KR(ret), KP(file_mgr));
    } else if (OB_FAIL(file_mgr->delete_local_file(file_id, 0/*ls_epoch_id*/,
                       true/*is_print_log*/, true/*is_del_seg_meta*/, true/*is_logical_delete*/))) {  // is_logical_delete is true
      LOG_WARN("fail to delete local file", KR(ret), K(file_id));
    } else {
      total_file_cnt_++;
    }
  }
  return ret;
}

/**
 * --------------------------------ObDelTmpFileDirOp------------------------------------
 */
int ObDelTmpFileDirOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  const char *dir = get_dir();
  int64_t tmp_file_id = -1;
  if (OB_ISNULL(entry) || OB_ISNULL(dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret), KP(entry), KP(dir));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_CANCELED;
    LOG_WARN("del dir is stop", KR(ret), K_(is_stop));
  } else if (false == ObString(entry->d_name).is_numeric()) {
    // do nothing, unexpected dir path, maybe is user created dir
  } else if (OB_FAIL(ObFileHelper::parse_tmp_file_name(entry->d_name, tmp_file_id))) {
    LOG_WARN("fail to parse tmp file name", KR(ret), K(entry->d_name), K(tmp_file_id));
  } else if (OB_FAIL(OB_DIR_MGR.delete_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id))) {
    LOG_WARN("fail to delete tmp file dir", KR(ret), K(tmp_file_id));
  } else {
    LOG_INFO("succ to delete tmp file dir", K(tmp_file_id));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
