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

#include "storage/shared_storage/ob_disk_space_meta.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/**
 * --------------------------------ObTenantDiskCacheInfo------------------------------------
 */
ObTenantDiskCacheInfo::ObTenantDiskCacheInfo(const int64_t space_percent)
  : reserved_size_(0), space_percent_(space_percent)
{}

void ObTenantDiskCacheInfo::reset()
{
  reserved_size_ = 0;
  space_percent_ = 0;
}

bool ObTenantDiskCacheInfo::is_valid() const
{
  return ((reserved_size_ > 0) && (space_percent_ > 0));
}

/**
 * --------------------------------ObTenantDiskCacheAllocInfo------------------------------------
 */
void ObTenantDiskCacheAllocInfo::reset()
{
  meta_file_alloc_size_ = 0;
  tmp_file_write_cache_alloc_size_ = 0;
  tmp_file_read_cache_alloc_size_ = 0;
  major_macro_read_cache_alloc_size_ = 0;
  private_macro_alloc_size_ = 0;
}

bool ObTenantDiskCacheAllocInfo::is_valid() const
{
  return (meta_file_alloc_size_ >= 0) &&
         (tmp_file_write_cache_alloc_size_ >= 0) &&
         (tmp_file_read_cache_alloc_size_ >= 0) &&
         (major_macro_read_cache_alloc_size_ >= 0) &&
         (private_macro_alloc_size_ >= 0);
}

/**
 * --------------------------------ObTenantAllDiskCacheInfo------------------------------------
 */
int ObTenantAllDiskCacheInfo::init(const int64_t total_disk_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(total_disk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(total_disk_size));
  } else {
    meta_file_cache_.space_percent_ = DEFAULT_META_FILE_SIZE_PCT;
    micro_cache_.space_percent_ = DEFAULT_MICRO_CACHE_SIZE_PCT;
    tmp_file_write_cache_.space_percent_ = DEFAULT_TMP_FILE_WRITE_CACHE_SIZE_PCT;
    preread_cache_.space_percent_ = DEFAULT_PREREAD_CACHE_SIZE_PCT;
    private_macro_cache_.space_percent_ = DEFAULT_PRIVATE_MACRO_SIZE_PCT;
    meta_file_cache_.update_reserved_size(total_disk_size);
    micro_cache_.update_reserved_size(total_disk_size);
    tmp_file_write_cache_.update_reserved_size(total_disk_size);
    preread_cache_.update_reserved_size(total_disk_size);
    private_macro_cache_.update_reserved_size(total_disk_size);
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected all cache disk info", KR(ret), K(*this));
    }
  }
  return ret;
}

int ObTenantAllDiskCacheInfo::update_reserved_size(const int64_t total_disk_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(total_disk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(total_disk_size));
  } else {
    meta_file_cache_.update_reserved_size(total_disk_size);
    micro_cache_.update_reserved_size(total_disk_size);
    tmp_file_write_cache_.update_reserved_size(total_disk_size);
    preread_cache_.update_reserved_size(total_disk_size);
    private_macro_cache_.update_reserved_size(total_disk_size);
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected all cache disk info", KR(ret), K(*this));
    }
  }
  return ret;
}

void ObTenantAllDiskCacheInfo::reset()
{
  meta_file_cache_.reset();
  micro_cache_.reset();
  tmp_file_write_cache_.reset();
  preread_cache_.reset();
  private_macro_cache_.reset();
  alloc_info_.reset();
}

bool ObTenantAllDiskCacheInfo::is_valid() const
{
  return (meta_file_cache_.is_valid()) && (micro_cache_.is_valid()) && (tmp_file_write_cache_.is_valid()) &&
         (preread_cache_.is_valid()) && (private_macro_cache_.is_valid() && (alloc_info_.is_valid()));
}

/**
 * --------------------------------ObTenantDiskCacheRatioInfo------------------------------------
 */
void ObTenantDiskCacheRatioInfo::reset()
{
  meta_file_size_pct_ = DEFAULT_META_FILE_SIZE_PCT;
  micro_cache_size_pct_ = DEFAULT_MICRO_CACHE_SIZE_PCT;
  tmp_file_write_cache_size_pct_ = DEFAULT_TMP_FILE_WRITE_CACHE_SIZE_PCT;
  preread_cache_size_pct_ = DEFAULT_PREREAD_CACHE_SIZE_PCT;
  private_macro_size_pct_ = DEFAULT_PRIVATE_MACRO_SIZE_PCT;
}

int64_t ObTenantDiskCacheRatioInfo::get_total_pct() const
{
  return (meta_file_size_pct_ + micro_cache_size_pct_ + tmp_file_write_cache_size_pct_ + 
          preread_cache_size_pct_ + private_macro_size_pct_);
}

bool ObTenantDiskCacheRatioInfo::is_valid() const
{
  return ((meta_file_size_pct_ > 0) && (micro_cache_size_pct_ > 0) && (tmp_file_write_cache_size_pct_ > 0) &&
          (preread_cache_size_pct_ > 0) && (private_macro_size_pct_ > 0) && (get_total_pct() == 100));
}

ObTenantDiskCacheRatioInfo& ObTenantDiskCacheRatioInfo::operator=(const ObTenantDiskCacheRatioInfo &other)
{
  if (this != &other) {
    meta_file_size_pct_ = other.meta_file_size_pct_;
    micro_cache_size_pct_ = other.micro_cache_size_pct_;
    tmp_file_write_cache_size_pct_ = other.tmp_file_write_cache_size_pct_;
    preread_cache_size_pct_ = other.preread_cache_size_pct_;
    private_macro_size_pct_ = other.private_macro_size_pct_;
  }
  return *this;
}

bool ObTenantDiskCacheRatioInfo::operator==(const ObTenantDiskCacheRatioInfo &other) const
{
  return (meta_file_size_pct_ == other.meta_file_size_pct_) &&
         (micro_cache_size_pct_ == other.micro_cache_size_pct_) &&
         (tmp_file_write_cache_size_pct_ == other.tmp_file_write_cache_size_pct_) &&
         (preread_cache_size_pct_ == other.preread_cache_size_pct_) &&
         (private_macro_size_pct_ == other.private_macro_size_pct_);
}

int ObTenantDiskCacheRatioInfo::encoded_str(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", META_FILE_SIZE_PCT_STR, meta_file_size_pct_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(meta_file_size_pct));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", MICRO_CACHE_SIZE_PCT_STR, micro_cache_size_pct_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(micro_cache_size_pct));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", TMP_FILE_WRITE_CACHE_SIZE_PCT_STR, tmp_file_write_cache_size_pct_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(tmp_file_write_cache_size_pct));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", PREREAD_CACHE_SIZE_PCT_STR, preread_cache_size_pct_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(preread_cache_size_pct));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", PRIVATE_MACRO_SIZE_PCT_STR, private_macro_size_pct_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(private_macro_size_pct));
  }
  return ret;
}

int ObTenantDiskCacheRatioInfo::decoded_str(char *token, bool &succ_decoded)
{
  int ret = OB_SUCCESS;
  succ_decoded = true;
  if (OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(token));
  } else if (0 == strncmp(META_FILE_SIZE_PCT_STR, token, strlen(META_FILE_SIZE_PCT_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(META_FILE_SIZE_PCT_STR), meta_file_size_pct_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(MICRO_CACHE_SIZE_PCT_STR, token, strlen(MICRO_CACHE_SIZE_PCT_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(MICRO_CACHE_SIZE_PCT_STR), micro_cache_size_pct_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(TMP_FILE_WRITE_CACHE_SIZE_PCT_STR, token, strlen(TMP_FILE_WRITE_CACHE_SIZE_PCT_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(TMP_FILE_WRITE_CACHE_SIZE_PCT_STR), tmp_file_write_cache_size_pct_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(PREREAD_CACHE_SIZE_PCT_STR, token, strlen(PREREAD_CACHE_SIZE_PCT_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(PREREAD_CACHE_SIZE_PCT_STR), preread_cache_size_pct_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(PRIVATE_MACRO_SIZE_PCT_STR, token, strlen(PRIVATE_MACRO_SIZE_PCT_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(PRIVATE_MACRO_SIZE_PCT_STR), private_macro_size_pct_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else {
    succ_decoded = false;
  }
  return ret;
}

DEFINE_SERIALIZE(ObTenantDiskCacheRatioInfo)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || (buf_len - new_pos) < ser_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_FAIL(encoded_str(buf, buf_len, new_pos))) {
    LOG_WARN("fail to encoded str", KR(ret), K(buf_len), K(new_pos), K(ser_len));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTenantDiskCacheRatioInfo)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(buf), K(data_len), K(pos), K(ret));
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
      bool succ_decoded = true;
      if (NULL == token) {
        break;
      } else if (OB_FAIL(decoded_str(token, succ_decoded))) {
        LOG_WARN("fail to decoded str", KR(ret), K(token));
      } else if (OB_UNLIKELY(!succ_decoded)) {
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
    LOG_WARN("invalid tenant cache disk info", KR(ret), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTenantDiskCacheRatioInfo)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  char buf[META_BUFF_SIZE] = {0};
  int64_t buf_len = sizeof(buf);
  int64_t pos = 0;
  if (OB_FAIL(encoded_str(buf, buf_len, pos))) {
    LOG_WARN("fail to encoded str", KR(ret), K(buf_len), K(pos));
  } else {
    len += pos;
  }
  return len;
}

/**
 * --------------------------------ObTenantDiskSpaceMetaBody------------------------------------
 */
int ObTenantDiskSpaceMetaBody::encoded_str(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%lu\n", TENANT_ID_STR, tenant_id_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(tenant_id));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", VERSION_ID_STR, version_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(version));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", ALLOC_META_FILE_SIZE_STR, meta_file_alloc_size_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(meta_file_alloc_size));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", ALLOC_PRIVATE_MACRO_SIZE_STR, private_macro_alloc_size_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(private_macro_alloc_size));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", ALLOC_TMP_FILE_WRITE_CACHE_SIZE_STR, tmp_file_write_cache_alloc_size_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(tmp_file_write_cache_alloc_size));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", ALLOC_TMP_FILE_READ_CACHE_SIZE_STR, tmp_file_read_cache_alloc_size_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(tmp_file_read_cache_alloc_size));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", ALLOC_MAJOR_MACRO_READ_CACHE_SIZE_STR, major_macro_read_cache_alloc_size_))) {
    LOG_WARN("write data buff failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(major_macro_read_cache_alloc_size));
  } else if (TENANT_DISK_SPACE_VERSION_2 == version_ && OB_FAIL(disk_cache_ratio_.encoded_str(buf, buf_len, pos))) {
    LOG_WARN("encode cache disk ratio info failed", KR(ret), KP(buf), K(buf_len), K(pos), K_(disk_cache_ratio));
  }
  return ret;
}

int ObTenantDiskSpaceMetaBody::decoded_str(char *token, bool &succ_decoded)
{
  int ret = OB_SUCCESS;
  succ_decoded = true;
  if (OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(token));
  } else if (0 == strncmp(TENANT_ID_STR, token, strlen(TENANT_ID_STR))) {
    int64_t value = 0;
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(TENANT_ID_STR), value))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    } else {
      tenant_id_ = static_cast<uint64_t>(value);
    }
  } else if (0 == strncmp(VERSION_ID_STR, token, strlen(VERSION_ID_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(VERSION_ID_STR), version_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(ALLOC_META_FILE_SIZE_STR, token, strlen(ALLOC_META_FILE_SIZE_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(ALLOC_META_FILE_SIZE_STR), meta_file_alloc_size_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(ALLOC_PRIVATE_MACRO_SIZE_STR, token, strlen(ALLOC_PRIVATE_MACRO_SIZE_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(ALLOC_PRIVATE_MACRO_SIZE_STR), private_macro_alloc_size_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(ALLOC_TMP_FILE_WRITE_CACHE_SIZE_STR, token, strlen(ALLOC_TMP_FILE_WRITE_CACHE_SIZE_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(ALLOC_TMP_FILE_WRITE_CACHE_SIZE_STR), tmp_file_write_cache_alloc_size_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(ALLOC_TMP_FILE_READ_CACHE_SIZE_STR, token, strlen(ALLOC_TMP_FILE_READ_CACHE_SIZE_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(ALLOC_TMP_FILE_READ_CACHE_SIZE_STR), tmp_file_read_cache_alloc_size_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else if (0 == strncmp(ALLOC_MAJOR_MACRO_READ_CACHE_SIZE_STR, token, strlen(ALLOC_MAJOR_MACRO_READ_CACHE_SIZE_STR))) {
    if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(ALLOC_MAJOR_MACRO_READ_CACHE_SIZE_STR), major_macro_read_cache_alloc_size_))) {
      LOG_WARN("fail to parse int value", KR(ret), K(token));
    }
  } else {
    succ_decoded = false;
  }
  return ret;
}

DEFINE_SERIALIZE(ObTenantDiskSpaceMetaBody)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || (buf_len - new_pos) < ser_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_FAIL(encoded_str(buf, buf_len, new_pos))) {
    LOG_WARN("fail to encoded str", KR(ret), K(buf_len), K(new_pos), K(ser_len));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTenantDiskSpaceMetaBody)
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
      bool succ_decoded = true;
      if (NULL == token) {
        break;
      } else if (OB_FAIL(decoded_str(token, succ_decoded))) {
        LOG_WARN("fail to decoded str", KR(ret), K(token));
      } else if (!succ_decoded && TENANT_DISK_SPACE_VERSION_2 == version_) {
        succ_decoded = true;
        if (OB_FAIL(disk_cache_ratio_.decoded_str(token, succ_decoded))) {
          LOG_WARN("fail to decoded cache disk ratio info str", KR(ret), K(token), K_(disk_cache_ratio));
        }
      }

      if (OB_SUCC(ret) && (!succ_decoded)) {
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
    LOG_WARN("invalid tenant disk space meta body", KR(ret), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTenantDiskSpaceMetaBody)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  char buf[META_BUFF_SIZE] = {0};
  int64_t buf_len = sizeof(buf);
  int64_t pos = 0;
  if (OB_FAIL(encoded_str(buf, buf_len, pos))) {
    LOG_WARN("fail to encoded str", KR(ret), K(buf_len), K(pos));
  } else {
    len += pos;
  }
  return len;
}

ObTenantDiskSpaceMetaBody::ObTenantDiskSpaceMetaBody()
{
  reset();
}

bool ObTenantDiskSpaceMetaBody::is_valid() const
{
  bool is_valid = (OB_INVALID_TENANT_ID != tenant_id_)
                  && (version_ >= TENANT_DISK_SPACE_VERSION)
                  && (meta_file_alloc_size_ >= 0)
                  && (private_macro_alloc_size_ >= 0)
                  && (tmp_file_write_cache_alloc_size_ >= 0)
                  && (tmp_file_read_cache_alloc_size_ >= 0)
                  && (major_macro_read_cache_alloc_size_ >= 0)
                  && (disk_cache_ratio_.is_valid());
  return is_valid;
}

void ObTenantDiskSpaceMetaBody::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  version_ = TENANT_DISK_SPACE_VERSION_2;
  meta_file_alloc_size_ = 0;
  private_macro_alloc_size_ = 0;
  tmp_file_write_cache_alloc_size_ = 0;
  tmp_file_read_cache_alloc_size_ = 0;
  major_macro_read_cache_alloc_size_ = 0;
  disk_cache_ratio_.reset();
}

int ObTenantDiskSpaceMetaBody::assign_by_disk_info(const ObTenantAllDiskCacheInfo &cache_disk_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cache_disk_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cache_disk_info));
  } else {
    meta_file_alloc_size_ = cache_disk_info.alloc_info_.meta_file_alloc_size_;
    private_macro_alloc_size_ = cache_disk_info.alloc_info_.private_macro_alloc_size_;
    tmp_file_write_cache_alloc_size_ = cache_disk_info.alloc_info_.tmp_file_write_cache_alloc_size_;
    tmp_file_read_cache_alloc_size_ = cache_disk_info.alloc_info_.tmp_file_read_cache_alloc_size_;
    major_macro_read_cache_alloc_size_ = cache_disk_info.alloc_info_.major_macro_read_cache_alloc_size_;
    if (TENANT_DISK_SPACE_VERSION_2 == version_) {
      disk_cache_ratio_.meta_file_size_pct_ = cache_disk_info.meta_file_cache_.get_space_percent();
      disk_cache_ratio_.micro_cache_size_pct_ = cache_disk_info.micro_cache_.get_space_percent();
      disk_cache_ratio_.tmp_file_write_cache_size_pct_ = cache_disk_info.tmp_file_write_cache_.get_space_percent();
      disk_cache_ratio_.preread_cache_size_pct_ = cache_disk_info.preread_cache_.get_space_percent();
      disk_cache_ratio_.private_macro_size_pct_ = cache_disk_info.private_macro_cache_.get_space_percent();
    }
  }
  return ret;
}

/**
 * --------------------------------ObTenantDiskSpaceMeta------------------------------------
 */

DEFINE_SERIALIZE(ObTenantDiskSpaceMeta)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || (buf_len - new_pos) < ser_len)) {
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

DEFINE_DESERIALIZE(ObTenantDiskSpaceMeta)
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
    LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "failed to check crc", KR(ret), KP(buf),
      K(data_len), K(pos), K_(header), K(calc_crc));
  } else if (OB_FAIL(body_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize meta body", KR(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant disk space meta", KR(ret), K_(header), K_(body));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTenantDiskSpaceMeta)
{
  int64_t len = 0;
  len += header_.get_serialize_size();
  len += body_.get_serialize_size();
  return len;
}

ObTenantDiskSpaceMeta::ObTenantDiskSpaceMeta()
{
  reset();
}

ObTenantDiskSpaceMeta::ObTenantDiskSpaceMeta(const uint64_t tenant_id)
{
  reset();
  body_.tenant_id_ = tenant_id;
}

void ObTenantDiskSpaceMeta::reset()
{
  header_.reset();
  body_.reset();
}

bool ObTenantDiskSpaceMeta::is_valid() const
{
  return (header_.is_valid() && body_.is_valid());
}

} // namespace storage
} // namespace oceanbase
