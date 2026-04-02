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

#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_dir_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/**
 * --------------------------------ObPathContext------------------------------------
 */

ObPathContext::ObPathContext()
  : file_id_(), ls_epoch_id_(0), is_local_cache_(true),
    is_atomic_write_(false), write_seq_(OB_INVALID_ID),
    is_logical_delete_(false)
{
  path_[0] = '\0';
}

ObPathContext::~ObPathContext()
{
  reset();
}

ObPathContext& ObPathContext::operator =(const ObPathContext &other)
{
  MEMCPY(path_, other.path_, common::MAX_PATH_SIZE);
  file_id_ = other.file_id_;
  ls_epoch_id_ = other.ls_epoch_id_;
  is_local_cache_ = other.is_local_cache_;
  is_atomic_write_ = other.is_atomic_write_;
  write_seq_ = other.write_seq_;
  is_logical_delete_ = other.is_logical_delete_;
  return *this;
}

bool ObPathContext::is_valid() const
{
  bool is_valid = is_macro_block_id_valid() && (ls_epoch_id_ >= 0);
  if (is_valid && is_atomic_write_ && (OB_INVALID_ID == write_seq_)) {
    is_valid = false;  // if is_atomic_write true, write_seq must be valid
  }
  if (is_valid && (is_atomic_write_ || is_logical_delete_) && !is_local_cache_) {
    is_valid = false;  // .tmp.seq file and .deleted file must store in local
  }
  return is_valid;
}

void ObPathContext::reset()
{
  path_[0] = '\0';
  file_id_.reset();
  ls_epoch_id_ = 0;
  is_local_cache_ = true;
  is_atomic_write_ = false;
  write_seq_ = OB_INVALID_ID;
  is_logical_delete_ = false;
}

int ObPathContext::set_file_ctx(const MacroBlockId &file_id, const int64_t ls_epoch_id, const bool is_local_cache)
{
  int ret = OB_SUCCESS;
  path_[0] = '\0';
  file_id_ = file_id;
  ls_epoch_id_ = ls_epoch_id;
  is_local_cache_ = is_local_cache;
  is_atomic_write_ = false;
  write_seq_ = OB_INVALID_ID;
  is_logical_delete_ = false;
  if (OB_FAIL(to_path())) {
    LOG_WARN("fail to convert ctx to path", KR(ret), K(*this));
  }
  return ret;
}

// set for .deleted file
int ObPathContext::set_logical_delete_ctx(const MacroBlockId &file_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  path_[0] = '\0';
  file_id_ = file_id;
  ls_epoch_id_ = ls_epoch_id;
  is_local_cache_ = true;  // .deleted file is store in local
  is_atomic_write_ = false;
  write_seq_ = OB_INVALID_ID;
  is_logical_delete_ = true;  // logical_delete is true
  if (OB_FAIL(to_path())) {
    LOG_WARN("fail to convert ctx to path", KR(ret), K(*this));
  }
  return ret;
}

// set for .tmp.seq file
int ObPathContext::set_atomic_write_ctx(const MacroBlockId &file_id, const int64_t ls_epoch_id, const uint64_t write_seq)
{
  int ret = OB_SUCCESS;
  path_[0] = '\0';
  file_id_ = file_id;
  ls_epoch_id_ = ls_epoch_id;
  is_local_cache_ = true;  // .tmp.seq is store in lcoal
  is_atomic_write_ = true;  // atomic_write is true
  write_seq_ = write_seq;
  is_logical_delete_ = false;
  if (OB_FAIL(to_path())) {
    LOG_WARN("fail to convert ctx to path", KR(ret), K(*this));
  }
  return ret;
}

bool ObPathContext::is_macro_block_id_valid() const
{
  bool is_id_valid = (file_id_.is_valid() && (file_id_.id_mode() == (uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE));
  ObStorageObjectType object_type = file_id_.storage_object_type();
  if (is_id_valid) {
    switch (object_type) {
#define STORAGE_OBJECT_TYPE_INFO(obj_id, obj_str, is_pin_local, is_read_through, is_valid, to_local_path_format, to_remote_path_format, get_parent_dir, create_parent_dir) \
      case ObStorageObjectType::obj_id: { \
        is_id_valid = is_valid; \
        break; \
      }
      OB_STORAGE_OBJECT_TYPE_LIST
#undef STORAGE_OBJECT_TYPE_INFO
      default: {
        is_id_valid = false;
        break;
      }
    }
  }
  return is_id_valid;
}

int ObPathContext::to_path()
{
  int ret = OB_SUCCESS;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(*this));
  } else if (!is_local_cache_ && OB_FAIL(OB_DIR_MGR.get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else {
    int64_t pos = 0;
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    const int64_t cluster_id = GCONF.cluster_id;
    const uint64_t server_id = GCONF.observer_id;
    const ObStorageObjectType object_type = file_id_.storage_object_type();
    const int64_t length = sizeof(path_);
    switch (object_type) {
#define STORAGE_OBJECT_TYPE_INFO(obj_id, obj_str, is_pin_local, is_read_through, is_valid, to_local_path_format, to_remote_path_format, get_parent_dir, create_parent_dir) \
      case ObStorageObjectType::obj_id: { \
        if (is_local_cache_) { \
          if (OB_FAIL(to_local_path_format)) { \
            LOG_WARN("fail to databuff printf", KR(ret), K(*this)); \
          } \
        } else { \
          if (OB_FAIL(to_remote_path_format)) { \
            LOG_WARN("fail to databuff printf", KR(ret), K(*this)); \
          } \
        } \
        break; \
      }
      OB_STORAGE_OBJECT_TYPE_LIST
#undef STORAGE_OBJECT_TYPE_INFO
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected object type", KR(ret), K(*this));
        break;
      }
    }
    // for atomic write, use .tmp in file name
    if (OB_SUCC(ret) && is_atomic_write_) {
      if (OB_FAIL(databuff_printf(path_, length, pos, "%s%lu", DEFAULT_TMP_STR, write_seq_))) {
        LOG_WARN("fail to databuff printf", KR(ret), K(*this));
      }
    }
  }
  return ret;
}

/**
 * --------------------------------ObFileHelper------------------------------------
 */

int ObFileHelper::parse_file_name(const char *file_name, int64_t &file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_name == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_name));
  } else if (false == ObString(file_name).is_numeric()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file name, file name is not a number", KR(ret), K(file_name));
  } else {
    char *end_str = nullptr;
    file_id = strtoll(file_name, &end_str, 10);
    if (('\0' != *end_str) || (file_id < 0) || (INT64_MAX == file_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(file_id), K(file_name));
    }
  }
  return ret;
}

int ObFileHelper::parse_tmp_file_name(const char *file_name, int64_t &file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_name == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_name));
  } else if (true == ObString(file_name).is_numeric()) { // file name is a number
    if (OB_FAIL(parse_file_name(file_name, file_id))) {
      LOG_WARN("fail to parse file name", KR(ret), K(file_name));
    }
  } else if (NULL != STRSTR(file_name, DEFAULT_DELETED_STR)) { // file name is a number.deleted
    int rc = sscanf(file_name, "%ld.deleted", &file_id);
    if (1 != rc) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected file name", KR(ret), K(rc), K(file_name));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file name, file name is not a number or number.deleted", KR(ret), K(file_name));
  }
  return ret;
}

int ObFileHelper::tmpfile_path_to_macro_id(const char *dir_path, const char *file_name, MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  int64_t tmp_file_id = -1;
  int64_t segment_id = -1;
  ObString dir_str(dir_path);
  const char *tmp_file_id_str = nullptr;
  if (OB_UNLIKELY((file_name == nullptr) || (dir_path == nullptr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_name), KP(dir_path));
  } else if (OB_ISNULL(tmp_file_id_str = dir_str.reverse_find('/'))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpedted error", KR(ret), K(dir_str), K(dir_path));
  } else if (OB_FAIL(parse_file_name(tmp_file_id_str + 1, tmp_file_id))) { // remove '/'
    LOG_WARN("fail to parse file name", KR(ret), K(dir_path), K(tmp_file_id));
  } else if (OB_FAIL(parse_tmp_file_name(file_name, segment_id))) {
    LOG_WARN("fail to parse tmp file name", KR(ret), K(file_name), K(segment_id));
  } else {
    file_id.set_second_id(tmp_file_id);
    file_id.set_third_id(segment_id);
  }
  return ret;
}

int ObFileHelper::get_file_parent_dir(char *path, const int64_t length,
                                      const MacroBlockId &file_id,
                                      const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH ||
                 !file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(path), K(length), K(file_id), K(ls_epoch_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    int64_t pos = 0;
    ObStorageObjectType object_type = file_id.storage_object_type();
    switch (object_type) {
#define STORAGE_OBJECT_TYPE_INFO(obj_id, obj_str, is_pin_local, is_read_through, is_valid, to_local_path_format, to_remote_path_format, get_parent_dir, create_parent_dir) \
      case ObStorageObjectType::obj_id: { \
        if (OB_FAIL(get_parent_dir)) { \
          LOG_WARN("fail to get parent dir path", KR(ret), K(file_id), K(ls_epoch_id)); \
        } \
        break; \
      }
      OB_STORAGE_OBJECT_TYPE_LIST
#undef STORAGE_OBJECT_TYPE_INFO
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected object type", KR(ret), K(file_id));
        break;
      }
    }
  }
  return ret;
}

int ObFileHelper::create_file_parent_dir(const MacroBlockId &file_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    ObStorageObjectType object_type = file_id.storage_object_type();
    switch (object_type) {
#define STORAGE_OBJECT_TYPE_INFO(obj_id, obj_str, is_pin_local, is_read_through, is_valid, to_local_path_format, to_remote_path_format, get_parent_dir, create_parent_dir) \
      case ObStorageObjectType::obj_id: { \
        if (OB_FAIL(create_parent_dir)) { \
          LOG_WARN("fail to create parent dir path", KR(ret), K(file_id), K(ls_epoch_id)); \
        } \
        break; \
      }
      OB_STORAGE_OBJECT_TYPE_LIST
#undef STORAGE_OBJECT_TYPE_INFO
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected object type", KR(ret), K(file_id));
        break;
      }
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
