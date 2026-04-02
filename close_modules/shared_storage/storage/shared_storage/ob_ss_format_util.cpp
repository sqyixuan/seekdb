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

#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/backup/ob_backup_io_adapter.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

/**
 * --------------------------------ObSSFormatBody------------------------------------
 */

DEFINE_SERIALIZE(ObSSFormatBody)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0 || (buf_len - new_pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%ld\n", VERSION_ID_STR, version_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%lu\n", CLUSTER_VERSION_STR, cluster_version_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%ld\n", CREATE_TIMESTAMP_STR, create_timestamp_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSSFormatBody)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  } else {
    char tmp[META_BUFF_SIZE] = { 0 };
    char *token = NULL;
    char *saved_ptr = NULL;
    const int64_t str_len = strlen(buf + pos);
    MEMCPY(tmp, buf + pos, str_len);
    tmp[str_len] = '\0';
    token = tmp;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "\n", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(VERSION_ID_STR, token, strlen(VERSION_ID_STR))) {
        if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(VERSION_ID_STR), version_))) {
          LOG_WARN("fail to parse int value", KR(ret), K(token));
        }
      } else if (0 == strncmp(CLUSTER_VERSION_STR, token, strlen(CLUSTER_VERSION_STR))) {
        int64_t value = 0;
        if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(CLUSTER_VERSION_STR), value))) {
          LOG_WARN("fail to parse int value", KR(ret), K(token));
        } else {
          cluster_version_ = static_cast<uint64_t>(value);
        }
      } else if (0 == strncmp(CREATE_TIMESTAMP_STR, token, strlen(CREATE_TIMESTAMP_STR))) {
        if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(CREATE_TIMESTAMP_STR), create_timestamp_))) {
          LOG_WARN("fail to parse int value", KR(ret), K(token));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), K(token));
      }
    }
    if (OB_SUCC(ret)) {
      new_pos += str_len;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss format body", KR(ret), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSSFormatBody)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  char buf[META_BUFF_SIZE] = {0};
  int64_t buf_len = sizeof(buf);
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", VERSION_ID_STR, version_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%lu\n", CLUSTER_VERSION_STR, cluster_version_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", CREATE_TIMESTAMP_STR, create_timestamp_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else {
    len += pos;
  }
  return len;
}

ObSSFormatBody::ObSSFormatBody()
{
  reset();
}

bool ObSSFormatBody::is_valid() const
{
  bool is_valid = ((version_ >= SS_FORMAT_VERSION)
                  && (cluster_version_ != OB_INVALID_ID)
                  && (create_timestamp_ >= 0));
  return is_valid;
}

void ObSSFormatBody::reset()
{
  version_ = SS_FORMAT_VERSION;
  cluster_version_ = OB_INVALID_ID;
  create_timestamp_ = 0;
}

/**
 * --------------------------------ObSSFormat------------------------------------
 */

DEFINE_SERIALIZE(ObSSFormat)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0 || (buf_len - new_pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("failed to encode meta header", K(ret), K(buf_len), K(new_pos), K(*this));
  } else if (OB_FAIL(body_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("failed to encode meta body", K(ret), K(buf_len), K(new_pos), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSSFormat)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int32_t calc_crc = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize meta head", KR(ret));
  } else if (OB_UNLIKELY(header_.body_crc_ !=
      (calc_crc = static_cast<int32_t>(ob_crc64(buf + new_pos, header_.body_size_))))) {
    ret = OB_PHYSIC_CHECKSUM_ERROR;
    LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "failed to check crc", KR(ret), KP(buf), K(data_len), K(pos), K_(header), K(calc_crc));
  } else if (OB_FAIL(body_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize meta body", KR(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss format", KR(ret), K_(header), K_(body));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSSFormat)
{
  int64_t len = 0;
  len += header_.get_serialize_size();
  len += body_.get_serialize_size();
  return len;
}

ObSSFormat::ObSSFormat()
{
  reset();
}

void ObSSFormat::reset()
{
  header_.reset();
  body_.reset();
}

int ObSSFormat::init(const uint64_t cluster_version, int64_t create_timestamp)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!(cluster_version != OB_INVALID_ID && create_timestamp >= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cluster_version), K(create_timestamp));
  } else {
    body_.cluster_version_ = cluster_version;
    body_.create_timestamp_ = create_timestamp;
    if (OB_FAIL(header_.construct_header(body_))) {
      LOG_WARN("fail to construct header", KR(ret), K(body_));
    }
  }
  return ret;
}

bool ObSSFormat::is_valid() const
{
  bool is_valid = header_.is_valid()
                  && body_.is_valid();
  return is_valid;
}

/**
 * --------------------------------ObSSFormatUtil------------------------------------
 */

int ObSSFormatUtil::write_ss_format(const ObBackupDest &storage_dest,
                                    const ObSSFormat &ss_format)
{
  int ret = OB_SUCCESS;
  char ss_format_path[common::MAX_PATH_SIZE] = { 0 };
  if (OB_UNLIKELY(!storage_dest.is_valid() || !ss_format.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss_format", KR(ret), K(ss_format), K(storage_dest));
  } else if (OB_FAIL(get_ss_format_path(storage_dest, ss_format_path, sizeof(ss_format_path)))) {
    LOG_WARN("fail to get ss_format path", KR(ret), K(ss_format_path));
  } else {
    ObBackupIoAdapter io_adapter;
    DefaultPageAllocator allocator("SSFormat", OB_SERVER_TENANT_ID);
    const int64_t serialize_size = ss_format.get_serialize_size();
    const int64_t buf_len = upper_align(serialize_size, DIO_ALIGN_SIZE);
    int64_t pos = 0;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(buf_len));
    } else if (OB_FAIL(ss_format.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize ss_format", KR(ret), K(ss_format));
    } else if (OB_UNLIKELY(pos != serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pos", KR(ret), K(pos), K(serialize_size));
    } else if (FALSE_IT(memset(buf + pos, 0, buf_len - pos))) {
    } else if (OB_FAIL(io_adapter.write_single_file(ss_format_path, storage_dest.get_storage_info(),
                                                    buf, buf_len, common::ObStorageIdMod::get_default_id_mod()))) {
      LOG_WARN("fail to write single file", KR(ret), K(ss_format_path), K(storage_dest));
    } else {
      LOG_INFO("succ to write ss_format file", K(ss_format_path));
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObSSFormatUtil::read_ss_format(const ObBackupDest &storage_dest, ObSSFormat &ss_format)
{
  int ret = OB_SUCCESS;
  char ss_format_path[common::MAX_PATH_SIZE] = { 0 };
  if (OB_UNLIKELY(!storage_dest.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest));
  } else if (OB_FAIL(get_ss_format_path(storage_dest, ss_format_path, sizeof(ss_format_path)))) {
    LOG_WARN("fail to get ss_format path", KR(ret), K(ss_format_path));
  } else {
    ObBackupIoAdapter io_adapter;
    DefaultPageAllocator allocator("SSFormat", OB_SERVER_TENANT_ID);
    char *buf = nullptr;
    const int64_t file_len = DIO_ALIGN_SIZE;
    int64_t pos = 0;
    int64_t read_size = 0;
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(file_len));
    } else if (OB_FAIL(io_adapter.read_single_file(ss_format_path, storage_dest.get_storage_info(), buf,
                                            file_len, read_size, common::ObStorageIdMod::get_default_id_mod()))) {
      LOG_WARN("fail to read single file", KR(ret), K(ss_format_path), K(storage_dest));
    } else if (OB_FAIL(ss_format.deserialize(buf, read_size, pos))) {
      LOG_WARN("fail to deserialize", KR(ret), K(read_size));
    } else if (OB_UNLIKELY(!ss_format.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ss_format", KR(ret), K(ss_format));
    } else {
      LOG_INFO("succ to read ss_format file", KR(ret), K(ss_format), K(pos));
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObSSFormatUtil::is_exist_ss_format(const ObBackupDest &storage_dest,
                                       bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  char ss_format_path[common::MAX_PATH_SIZE] = { 0 };
  if (OB_UNLIKELY(!storage_dest.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest));
  } else if (OB_FAIL(get_ss_format_path(storage_dest, ss_format_path, sizeof(ss_format_path)))) {
    LOG_WARN("fail to get ss_format path", KR(ret), K(ss_format_path));
  } else if (OB_FAIL(io_adapter.is_exist(ss_format_path, storage_dest.get_storage_info(), is_exist))) {
    LOG_WARN("fail to list files", KR(ret), K(ss_format_path));
  }
  return ret;
}

int ObSSFormatUtil::get_ss_format_path(const ObBackupDest &storage_dest,
                                       char *path, const int64_t length)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_UNLIKELY(!storage_dest.is_valid()) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s",
             storage_dest.get_root_path().ptr(), CLUSTER_DIR_STR,
             cluster_id, SS_FORMAT_FILE_NAME))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(storage_dest));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
