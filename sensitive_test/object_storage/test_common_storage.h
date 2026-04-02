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

#ifndef OCEANBASE_UNITTEST_COMMON_STORAGE_H_
#define OCEANBASE_UNITTEST_COMMON_STORAGE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "gtest/gtest.h"
#include "object_storage_authorization_info.h"
#include "test_object_storage.h"

/**
 * USER GUIDE
 *
 * This test case is for testing different storage media type, like oss, s3, fs, etc.
 * It will check reader/writer/util/appender/multipartupload primary interfaces correctness.
 * Besides, it will generate some abnormal situations to check the relative function correctness.
 *
 * If you want to run this test case, you just need to execute some simple steps.
 *
 * 1. For object storage, for example, s3, you just need to revise S3_BUCKET、S3_REGION、S3_ENDPOINT、S3_AK、
 *    S3_SK as correct info, so as to oss、cos
 *
 * 2. For NFS, you just need to revise FS_PATH as empty string(means "") or some other value, but not INVALID_STR. It will
 *    build a directory in the same directory as this test bin file. 
 *
 * NOTICE: consider the running time, you'd better check these media one by one, or it may be timeout.
 */

namespace oceanbase
{
namespace unittest
{

const char *INVALID_STR = "xxx";

// NFS CONFIG
const char *FS_PATH = ""; // if FS_PATH value not equals to invalid string, we will use current path as FS_PATH

enum class ObTestStorageType : uint8_t
{
  TEST_STORAGE_INVALID = 0,
  TEST_STORAGE_OSS = 1,
  TEST_STORAGE_S3 = 2,
  TEST_STORAGE_COS = 3,
  TEST_STORAGE_FS = 4,
  TEST_STORAGE_OBS = 5,
  TEST_STORAGE_GCS = 6,
  TEST_STORAGE_AZBLOB = 7,
  TEST_STORAGE_MAX = 8
};

const char test_storage_type_str_arr[8][8] = {"INVALID", "OSS", "S3", "COS", "NFS", "OBS", "GCS", "AZBLOB"};

struct ObTestStorageInfoConfig
{
public:
  const static int64_t CFG_BUF_LEN = 1024;

  char bucket_[CFG_BUF_LEN];
  char region_[CFG_BUF_LEN];
  char endpoint_[oceanbase::common::OB_MAX_BACKUP_ENDPOINT_LENGTH];
  char ak_[oceanbase::common::OB_MAX_BACKUP_ACCESSID_LENGTH];
  char sk_[oceanbase::common::OB_MAX_BACKUP_ACCESSKEY_LENGTH];
  union {
    char appid_[CFG_BUF_LEN];
    char fs_path_[CFG_BUF_LEN];
  };
  char extension_[oceanbase::common::OB_MAX_BACKUP_EXTENSION_LENGTH];

  bool is_valid_;

  ObTestStorageInfoConfig() : is_valid_(false)
  {
    MEMSET(this, 0, sizeof(*this));
  }
  ~ObTestStorageInfoConfig() {}

  #define SET_FIELD(field_name) \
    void set_##field_name(const char *value) \
    {  \
      if (nullptr != value) { \
        const int64_t val_len = strlen(value); \
        MEMCPY(field_name##_, value, val_len); \
        field_name##_[val_len] = '\0'; \
      } \
    } \
  
  SET_FIELD(bucket);
  SET_FIELD(region);
  SET_FIELD(endpoint);
  SET_FIELD(ak);
  SET_FIELD(sk);
  SET_FIELD(appid);
  SET_FIELD(fs_path);

  bool is_valid() const { return is_valid_; }

  TO_STRING_KV(K_(bucket), K_(region), K_(endpoint), K_(appid), K_(fs_path));
};

struct ObTestStorageMeta : public ObObjectStorageUnittestCommon
{
public:
  ObTestStorageType type_;
  ObTestStorageInfoConfig config_;

  ObTestStorageMeta() {}

  ~ObTestStorageMeta() {}

  int build_config(const ObTestStorageType type);
  bool is_valid() const;

  bool is_file_type() const { return type_ == ObTestStorageType::TEST_STORAGE_FS; }
  bool is_obj_type() const { return type_ == ObTestStorageType::TEST_STORAGE_S3 ||
                                    type_ == ObTestStorageType::TEST_STORAGE_COS ||
                                    type_ == ObTestStorageType::TEST_STORAGE_OSS ||
                                    type_ == ObTestStorageType::TEST_STORAGE_OBS ||
                                    type_ == ObTestStorageType::TEST_STORAGE_GCS ||
                                    type_ == ObTestStorageType::TEST_STORAGE_AZBLOB; }

  TO_STRING_KV(K_(type), K_(config));

private:
  bool is_valid_type(const ObTestStorageType type) const;
  void build_s3_cfg();
  void build_obs_cfg();
  void build_oss_cfg();
  void build_cos_cfg();
  void build_fs_cfg();
  void build_gcs_cfg();
  void build_azblob_cfg();

};

} // end of unittest
} // end of oceanbase

#endif
