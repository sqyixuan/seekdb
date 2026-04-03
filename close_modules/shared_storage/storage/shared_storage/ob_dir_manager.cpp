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

#include "storage/shared_storage/ob_dir_manager.h"
#include "common/ob_smart_call.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "share/ob_ss_file_util.h"
#ifdef _WIN32
#include <fcntl.h>
#endif

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObDirManager::ObDirManager()
{
  object_storage_root_dir_[0] = '\0';
}

ObDirManager::~ObDirManager()
{
  object_storage_root_dir_[0] = '\0';
}

ObDirManager &ObDirManager::get_instance()
{
  static ObDirManager instance_;
  return instance_;
}

int ObDirManager::set_object_storage_root_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(databuff_printf(object_storage_root_dir_, sizeof(object_storage_root_dir_), "%s", dir_path))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(dir_path));
  } else {
    LOG_INFO("succ to set object storage root dir", K_(object_storage_root_dir));
  }
  return ret;
}

int ObDirManager::fsync_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dir path is null", KR(ret));
  } else {
    // 1. open
    int fd = INVALID_FD;
    RETRY_ON_EINTR(fd, ::open(dir_path, O_DIRECTORY | O_RDONLY
#ifdef _WIN32
        | _O_BINARY
#endif
        ));

    // 2. fsync
    if (OB_UNLIKELY(fd < 0)) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_WARN("::open failed", KR(ret), K(dir_path), K(errno), KERRMSG);
    } else if (OB_UNLIKELY(0 != ::fsync(fd))) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_WARN("::fsync failed", KR(ret), K(dir_path), K(errno), KERRMSG);
    }

    // 3. close
    if (OB_LIKELY(fd >= 0)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSFileUtil::close(fd))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_ERROR("fail to close fd", KR(ret), KR(tmp_ret), K(fd));
      }
    }
  }
  return ret;
}

int ObDirManager::create_dir(const char *dir_path, const ObStorageObjectType object_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const mode_t mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
  // step 1: create directory
  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::mkdir(dir_path, mode))) {
    LOG_WARN("fail to mkdir", KR(ret), K(dir_path));
  } else if (OB_FAIL(ObDirManager::fsync_dir(dir_path))) {
    LOG_WARN("fail to fsync dir", KR(ret), K(dir_path));
  }
  // step 2: alloc directory size
  if (OB_SUCC(ret)) {
    // alloc_dir_size cannot affect mkdir, ignore alloc_dir_size failed, avoid aio_write failed
    int64_t dir_size = 0;
    ObIODFileStat statbuf;
    ObTenantFileManager *file_mgr = nullptr;
    if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager*))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObTenantFileManager is NULL", KR(tmp_ret));
    } else if (file_mgr->is_object_type_need_stat(object_type)) {
      if (ObStorageObjectType::PRIVATE_TABLET_META == object_type) {
        // because tablet_current_version file will overwrite, so alloc size when create tablet_id_dir
        dir_size += DEFAULT_DIR_SIZE;
      }
      if (OB_TMP_FAIL(ObIODeviceLocalFileOp::stat(dir_path, statbuf))) {
        LOG_WARN("fail to stat dir size", KR(tmp_ret), K(dir_path));
      } else if (FALSE_IT(dir_size += statbuf.size_)) {
      } else if (OB_TMP_FAIL(file_mgr->alloc_file_size(object_type, dir_size))) {
        LOG_WARN("fail to alloc dir size", KR(tmp_ret), K(dir_size), K(object_type),
                  "object_type_str", get_storage_objet_type_str(object_type));
      }
    }
  }
  return ret;
}

int ObDirManager::delete_dir(const char *dir_path, const ObStorageObjectType object_type)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_mgr = nullptr;
  ObIODFileStat statbuf;
  MacroBlockId mock_file_id;
  // because free_file_size() need MacroBlockId, so construct mock_file_id
  mock_file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  mock_file_id.set_storage_object_type(static_cast<uint64_t>(object_type));
  // step 1: delete directory
  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", KR(ret));
  } else if (((ObStorageObjectType::TMP_FILE == object_type) && file_mgr->is_tmp_file_cache_pause_gc()) ||
             ((ObStorageObjectType::TMP_FILE != object_type) && file_mgr->is_pause_gc())) {
    // for tmp_file use is_tmp_file_write_cache_pause_gc(), for other object use is_pause_gc()
    ret = OB_FILE_DELETE_FAILED;
    if (TC_REACH_TIME_INTERVAL(ObBaseFileManager::PRINT_LOG_INTERVAL)) {
      LOG_WARN("the tenant is calibrating data disk alloc size and pausing gc, so do not allow to delete directory",
               KR(ret), K(dir_path), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
    }
  } else if (file_mgr->is_object_type_need_stat(object_type) &&
             OB_FAIL(ObIODeviceLocalFileOp::stat(dir_path, statbuf))) {
    LOG_WARN("fail to stat dir size", KR(ret));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  } else if (OB_FAIL(file_mgr->free_file_size(mock_file_id, statbuf.size_))) {
    LOG_WARN("fail to free dir size", KR(ret), K(mock_file_id), K(statbuf.size_));
  }
  return ret;
}

int ObDirManager::delete_dir_rec(const char *path, int64_t &size)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is null", KR(ret));
  } else if (OB_ISNULL(dir = opendir(path))) {
    if (ENOENT == errno) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("dir does not exist", KR(ret), K(path), K(errno), KERRMSG);
    } else {
      ret = OB_FILE_NOT_OPENED;
      LOG_WARN("fail to open dir", KR(ret), K(path), K(errno), KERRMSG);
    }
  } else {
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    ObIODFileStat statbuf;
    while (((entry = readdir(dir)) != NULL) && OB_SUCC(ret)) {
      bool is_dir = false;
      MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if ((0 == strcmp(entry->d_name, ".")) || (0 == strcmp(entry->d_name, ".."))) {
        // do nothing
      } else if (OB_FAIL(databuff_printf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", path, entry->d_name))) {
        LOG_WARN("fail to databuff printf", KR(ret), K(current_file_path), K(path), K(entry->d_name));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(current_file_path, statbuf))) {
        LOG_WARN("fail to stat file", KR(ret), K(current_file_path), K(statbuf));
      } else if (S_ISDIR(statbuf.mode_)) { // 1. delete dir recursively
        if (OB_FAIL(SMART_CALL(delete_dir_rec(current_file_path, size)))) {
          LOG_WARN("delete_dir_rec failed", KR(ret), K(entry->d_name), K(path));
        }
      } else if (S_ISREG(statbuf.mode_)) { // 2. delete regular file
        if (OB_FAIL(ObIODeviceLocalFileOp::unlink(current_file_path))) {
          LOG_WARN("unlink failed", KR(ret), K(current_file_path));
        } else {
          size += statbuf.size_;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpcted file in current dir", KR(ret), K(current_file_path), K(statbuf.mode_));
      }
    }
  }

  ObIODFileStat statbuf;
  if (OB_FAIL(ret)) {
    LOG_WARN("delete directory rec failed", KR(ret), K(path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(path, statbuf))) {
    LOG_WARN("fail to stat", KR(ret), K(path), K(statbuf));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(path))) {
    LOG_WARN("rmdir failed", KR(ret), K(path));
  } else {
    size += statbuf.size_;
  }

  if (NULL != dir) {
    closedir(dir);
    dir = nullptr;
  }
  return ret;
}

int ObDirManager::create_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1: create tenant_id_epoch_id/ directory
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_local_tenant_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  } else if (OB_FAIL(databuff_printf(parent_dir_path, sizeof(parent_dir_path), "%s",
                                     get_local_cache_root_dir()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }

  dir_path[0] = '\0';
  // step 2: create tenant_id_epoch_id/ls/ directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_ls_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get ls dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  // step 3: create tenant_id_epoch_id/tablet_data/ directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_data_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  // step 4: create tenant_id_epoch_id/tmp_data/ directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tmp_data_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tmp data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  // step 5: create tenant_id_epoch_id/major_data/ directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_major_data_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get major data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  // setp 6: fsync parent dir
  parent_dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tenant_dir(parent_dir_path, sizeof(parent_dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

// tenant_id_epoch_id/ls_id_epoch_id/
int ObDirManager::create_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                   const int64_t ls_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1: create tenant_id_epoch_id/ls/ls_id_epoch_id/ directory
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_ls_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                   tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls id dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  parent_dir_path[0] = '\0';
  // step 2: create tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/ directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_meta_dir(dir_path, sizeof(dir_path), tenant_id,
                                         tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get tablet meta dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  } else if (OB_FAIL(get_ls_id_dir(parent_dir_path, sizeof(parent_dir_path), tenant_id,
                                   tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls id dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  } else if (FALSE_IT(parent_dir_path[0] = '\0')) {
  } else if (OB_FAIL(get_ls_dir(parent_dir_path, sizeof(parent_dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get ls dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/
int ObDirManager::create_tablet_meta_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                   const int64_t ls_id, const int64_t ls_epoch_id,
                                                   const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || tablet_id <= 0 ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id), K(tablet_id));
  } else if (OB_FAIL(get_tablet_meta_tablet_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                       tenant_epoch_id, ls_id, ls_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet meta tablet id dir", KR(ret), K(ls_id),
             K(ls_epoch_id), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  } else if (OB_FAIL(get_tablet_meta_dir(parent_dir_path, sizeof(parent_dir_path),
                                         tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get tablet meta dir", KR(ret), K(ls_id),
             K(ls_epoch_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/tablet_id/
int ObDirManager::create_tablet_data_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                   const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tablet_id <= 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                   tenant_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet data tablet id dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  } else if (OB_FAIL(get_local_tablet_data_dir(parent_dir_path, sizeof(parent_dir_path),
                                               tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

int ObDirManager::create_tablet_data_tablet_id_transfer_seq_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                                const int64_t tablet_id, const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1: create tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/ directory
  if (OB_UNLIKELY(tablet_id <= 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0 || transfer_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(get_tablet_data_tablet_id_transfer_seq_dir(dir_path, sizeof(dir_path), tenant_id,
                                                                tenant_epoch_id, tablet_id, transfer_seq))) {
    LOG_WARN("fail to get tablet data tablet id transfer seq dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      // if report error OB_NO_SUCH_FILE_OR_DIRECTORY, create tenant_id_epoch_id/tablet_data/tablet_id/ directory first
      if (OB_FAIL(create_tablet_data_tablet_id_dir(tenant_id, tenant_epoch_id, tablet_id))) {
        LOG_WARN("fail to create tablet data tablet id dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(tablet_id));
      } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
        LOG_WARN("fail to create dir", KR(ret), K(dir_path));
      }
    } else {
      LOG_WARN("fail to create dir", KR(ret), K(dir_path));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_data_dir(parent_dir_path, sizeof(parent_dir_path),
                                               tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }

  dir_path[0] = '\0';
  // step 2: create tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_id_macro_dir(
                     dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id, tablet_id, transfer_seq, ObMacroType::DATA_MACRO))) {
    LOG_WARN("fail to get tablet id data macro dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  // step 3: create tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/meta directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_id_macro_dir(
             dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id, tablet_id, transfer_seq, ObMacroType::META_MACRO))) {
    LOG_WARN("fail to get tablet id meta macro dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  // step 4: fsync data/ and meta/ parent dir
  parent_dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_data_tablet_id_transfer_seq_dir(parent_dir_path,
                     sizeof(parent_dir_path), tenant_id, tenant_epoch_id, tablet_id, transfer_seq))) {
    LOG_WARN("fail to get tablet data tablet id dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

// tenant_id_epoch_id/tmp_data/tmp_file_id
int ObDirManager::create_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                      const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_local_tmp_file_dir(dir_path, sizeof(dir_path), tenant_id,
                                            tenant_epoch_id, tmp_file_id))) {
    LOG_WARN("fail to get local tmp file dir", KR(ret), K(tmp_file_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::TMP_FILE))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1: delete tenant_id_epoch_id/ls/ directory
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_ls_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get ls dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  }
  // step 2: delete tenant_id_epoch_id/tablet_data/ directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_data_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tablet meta macro dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  }
  // step 3: delete tenant_id_epoch_id/tmp_data/ directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tmp_data_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tablet meta macro dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  }

  // step 4: delete tenant_id_epoch_id/ directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tenant_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                   const int64_t ls_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1:delete tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/ directory
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_tablet_meta_dir(dir_path, sizeof(dir_path),
                                   tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get tablet meta dir", KR(ret), K(ls_id), K(ls_epoch_id),
             K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  }
  // step 2:delete tenant_id_epoch_id/ls/ls_id_epoch_id/ directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_ls_id_dir(dir_path, sizeof(dir_path),
                                   tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls dir", KR(ret), K(ls_id), K(ls_epoch_id),
             K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tablet_meta_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                   const int64_t ls_id, const int64_t ls_epoch_id,
                                                   const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  // delete tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/ directory
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_tablet_meta_tablet_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                       tenant_epoch_id, ls_id, ls_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet meta tablet id dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tablet_id),
             K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tablet_data_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                   const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  // delete tenant_id_epoch_id/tablet_data/tablet_id directory
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                                 tenant_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tablet_data_tablet_id_transfer_seq_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                                const int64_t tablet_id, const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  // step 1: delete tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/meta directory
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || transfer_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(get_local_tablet_id_macro_dir(
               dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id, tablet_id, transfer_seq, ObMacroType::META_MACRO))) {
    LOG_WARN("fail to get tablet meta macro dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }

  // step 2: delete tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_id_macro_dir(
               dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id, tablet_id, transfer_seq, ObMacroType::DATA_MACRO))) {
    LOG_WARN("fail to get tablet data macro dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  // step 3: delete tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_data_tablet_id_transfer_seq_dir(dir_path, sizeof(dir_path), tenant_id,
                                                                tenant_epoch_id, tablet_id, transfer_seq))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                      const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::get_local_tmp_file_dir(dir_path, sizeof(dir_path),
                                           tenant_id, tenant_epoch_id, tmp_file_id))) {
    LOG_WARN("fail to get local tmp file dir", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::TMP_FILE))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
    } else if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      // due to the concurrency of tmp file preread and tmp file delete, maybe .tmp files exist in tmp_file_dir when delete tmp_file_dir
      // .tmp files will be gc by timertask
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
    }
  }
  return ret;
}

// cluster_id/
int ObDirManager::get_cluster_dir(char *path, const int64_t length)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld",
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(cluster_id));
  }
  return ret;
}

// tenant_id_epoch_id/
int ObDirManager::get_local_tenant_dir(char *path, const int64_t length,
                                       const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/
int ObDirManager::get_remote_tenant_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                        const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/tenant_id/
int ObDirManager::get_shared_tenant_dir(char *path, const int64_t length, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu",
                     object_storage_root_dir, CLUSTER_DIR_STR,
                     cluster_id, TENANT_DIR_STR, tenant_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(cluster_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/
int ObDirManager::get_ls_dir(char *path, const int64_t length,
                             const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, LS_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/
int ObDirManager::get_ls_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                const int64_t tenant_epoch_id, const int64_t ls_id,
                                const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || ls_id < 0 ||
                  ls_epoch_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id), K(ls_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld_%ld",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, LS_DIR_STR, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/
int ObDirManager::get_local_tablet_data_dir(char *path, const int64_t length,
                                            const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/tablet_data/
int ObDirManager::get_remote_tablet_data_dir(char *path, const int64_t length,
                                             const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                                     TABLET_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/tablet_id/
int ObDirManager::get_tablet_data_tablet_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                const int64_t tenant_epoch_id, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tablet_id <= 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, TABLET_DATA_DIR_STR, tablet_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/
int ObDirManager::get_tablet_data_tablet_id_transfer_seq_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                             const int64_t tenant_epoch_id, const int64_t tablet_id,
                                                             const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || transfer_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id), K(transfer_seq));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld/%ld",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                     TABLET_DATA_DIR_STR, tablet_id, transfer_seq))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(transfer_seq));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/ or meta/
int ObDirManager::get_local_tablet_id_macro_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                const int64_t tenant_epoch_id, const int64_t tablet_id,
                                                const int64_t transfer_seq, const ObMacroType macro_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || transfer_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length),
             K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld/%ld/%s",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                     TABLET_DATA_DIR_STR, tablet_id, transfer_seq, get_macro_type_str(macro_type)))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/ or meta/
int ObDirManager::get_remote_tablet_id_macro_dir(char *path, const int64_t length,
                                                 const uint64_t tenant_id,
                                                 const int64_t tenant_epoch_id,
                                                 const int64_t tablet_id,
                                                 const int64_t transfer_seq,
                                                 const ObMacroType macro_type)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || transfer_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(transfer_seq));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%ld/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                                     TABLET_DATA_DIR_STR, tablet_id, transfer_seq, get_macro_type_str(macro_type)))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/
int ObDirManager::get_tablet_meta_dir(char *path, const int64_t length,
                                      const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                      const int64_t ls_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  ls_id < 0 || ls_epoch_id < 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id),
             K(ls_epoch_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld_%ld/%s",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                     LS_DIR_STR, ls_id, ls_epoch_id, TABLET_META_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(ls_id), K(ls_epoch_id),
             K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/
int ObDirManager::get_tablet_meta_tablet_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                const int64_t tenant_epoch_id, const int64_t ls_id,
                                                const int64_t ls_epoch_id, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  ls_id < 0 || ls_epoch_id < 0 || tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id), K(ls_epoch_id),
             K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld_%ld/%s/%ld",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                     LS_DIR_STR, ls_id, ls_epoch_id, TABLET_META_DIR_STR, tablet_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(ls_id), K(ls_epoch_id),
             K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/tenant_id/tablet/tablet_id/major/meta/
int ObDirManager::get_shared_tablet_meta_dir(char *path, const int64_t length, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || tablet_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(), TABLET_DIR_STR, tablet_id,
                                     MAJOR_DIR_STR, SHARED_TABLET_META_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(MTL_ID()));
  }
  return ret;
}

// cluster_id/tenant_id/tablet/tablet_id/major/sstable/
int ObDirManager::get_shared_tablet_data_dir(char *path, const int64_t length, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || tablet_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(), TABLET_DIR_STR, tablet_id,
                                     MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(MTL_ID()));
  }
  return ret;
}

// cluster_id/tenant_id/tablet_ids/
int ObDirManager::get_shared_tablet_ids_dir(char *path, const int64_t length)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(), TABLET_IDS_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(MTL_ID()));
  }
  return ret;
}

// cluster_id/tenant_id/tablet/tablet_id/major
int ObDirManager::get_shared_tablet_dir(char *path, const int64_t length, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || tablet_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(),
                                     TABLET_DIR_STR, tablet_id, MAJOR_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(MTL_ID()));
  }
  return ret;
}

// tenant_id_epoch_id/tmp_data/
int ObDirManager::get_local_tmp_data_dir(char *path, const int64_t length,
                                         const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, TMP_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/tmp_data/
int ObDirManager::get_remote_tmp_data_dir(char *path, const int64_t length,
                                          const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s",
                     object_storage_root_dir,
                     CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
                     tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/tmp_data/tmp_file_id/
int ObDirManager::get_local_tmp_file_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                         const int64_t tenant_epoch_id, const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tmp_file_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld",
                                     get_local_cache_root_dir(), tenant_id,
                                     tenant_epoch_id, TMP_DATA_DIR_STR, tmp_file_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/
int ObDirManager::get_remote_tmp_file_dir(char *path, const int64_t length,
                                          const uint64_t tenant_id,
                                          const int64_t tenant_epoch_id,
                                          const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tmp_file_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld",
                     object_storage_root_dir, CLUSTER_DIR_STR,
                     cluster_id, SERVER_DIR_STR, server_id,
                     tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR, tmp_file_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/shared_major_macro_cache/
int ObDirManager::get_local_major_data_dir(char *path, const int64_t length,
                                           const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, MAJOR_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

int ObDirManager::get_object_storage_root_dir(char *&path)
{
  int ret = OB_SUCCESS;
  if (0 == STRLEN(object_storage_root_dir_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, object storage root dir has not been set", KR(ret), K_(object_storage_root_dir));
  } else {
    path = object_storage_root_dir_;
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
