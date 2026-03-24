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

#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/restore/ob_storage_info.h"
#include "lib/restore/ob_storage.h"
#include "lib/string/ob_sensitive_string.h"

using namespace oceanbase::common;

TEST(ObObjectStorageInfo, file)
{
  const char *uri = "file:///backup_dir/?&delete_mode=tagging";
  ObObjectStorageInfo info1;
  ASSERT_FALSE(info1.is_valid());

  // wrong storage info
  const char *storage_info = NULL;
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "access_key=xxx";    // nfs, endpoint/access_id/access_key should be empty
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "access_key=xxx&encrypt_key=xxx";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "delete_mode=wrong_mode";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "delete_mode=delete";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));

  ObObjectStorageInfo info2;
  ObObjectStorageInfo info3;
  storage_info = "";
  ObStorageType device_type = OB_STORAGE_FILE;
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, info2.set(device_type, storage_info));
  ASSERT_EQ(OB_SUCCESS, info3.assign(info1));

  ASSERT_EQ(info1, info2);
  ASSERT_EQ(info1, info3);

  // get
  ASSERT_EQ(device_type, info1.get_type());
  
  char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ("", buf);
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ("", buf);

  // clean
  ASSERT_TRUE(info1.is_valid());
  info1.reset();
  ASSERT_FALSE(info1.is_valid());
}

TEST(ObObjectStorageInfo, oss)
{
  const char *uri = "oss://backup_dir?host=xxx.com&access_id=111&access_key=222";
  ObObjectStorageInfo info1;
  
  const char *storage_info = "";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "access_id=111&access_key=222";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  
  storage_info = "host=xxx.com&access_id=111&access_key=222&checksum_type=md5";
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));

  char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info, buf);

  storage_info = "host=xxx.com&access_id=111&access_key=222&delete_mode=delete";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_NO_CHECKSUM_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&delete_mode=delete&checksum_type=no_checksum";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_NO_CHECKSUM_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&delete_mode=delete&checksum_type=md5";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_MD5_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&delete_mode=delete&checksum_type=crc32";
  info1.reset();
  ASSERT_EQ(OB_CHECKSUM_TYPE_NOT_SUPPORTED, info1.set(uri, storage_info));

  storage_info = "host=xxx.com&access_id=111&access_key=222&delete_mode=delete&checksum_type=";
  info1.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, info1.set(uri, storage_info));
}

TEST(ObObjectStorageInfo, cos)
{
  const char *uri = "cos://backup_dir?host=xxx.com&access_id=111&access_key=222&appid=333";
  ObObjectStorageInfo info1;
  
  const char *storage_info = "";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "host=xxx.com&access_id=111&access_key=222";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  
  storage_info = "host=xxx.com&access_id=111&access_key=222&appid=333&checksum_type=md5";
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));

  char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info, buf);

  storage_info = "host=xxx.com&access_id=111&access_key=222&appid=333&delete_mode=delete";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_NO_CHECKSUM_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&appid=333&delete_mode=delete&checksum_type=no_checksum";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_NO_CHECKSUM_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&appid=333&delete_mode=delete&checksum_type=md5";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_MD5_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&appid=333&delete_mode=delete&checksum_type=crc32";
  info1.reset();
  ASSERT_EQ(OB_CHECKSUM_TYPE_NOT_SUPPORTED, info1.set(uri, storage_info));

  storage_info = "host=xxx.com&access_id=111&access_key=222&appid=333&delete_mode=delete&checksum_type=";
  info1.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, info1.set(uri, storage_info));
}

TEST(ObObjectStorageInfo, s3)
{
  const char *uri = "s3://backup_dir?host=xxx.com&access_id=111&access_key=222&s3_region=333";
  ObObjectStorageInfo info1;
  
  const char *storage_info = "";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "host=xxx.com&access_id=111&access_key=222";
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  info1.reset();
  
  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&checksum_type=md5";
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(0, ::strcmp("s3_region=333&checksum_type=md5", info1.extension_));

  char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info, buf);

  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&delete_mode=delete&checksum_type=md5";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info, buf);
  ASSERT_EQ(OB_MD5_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&delete_mode=delete&checksum_type=no_checksum";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));

  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&delete_mode=delete&checksum_type=md5";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_MD5_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&delete_mode=delete&checksum_type=crc32";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_CRC32_ALGO, info1.checksum_type_);

  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&delete_mode=delete&checksum_type=";
  info1.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, info1.set(uri, storage_info));
}

TEST(ObObjectStorageInfo, s3_compatible)
{
  const char *uri = "s3://backup_dir?host=xxx.com&access_id=111&access_key=222&s3_region=333";
  ObObjectStorageInfo info1;
  
  const char *storage_info = "";
  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&delete_mode=delete&addressing_model=";
  info1.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, info1.set(uri, storage_info));
  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&delete_mode=delete&addressing_model=path_style";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&addressing_model=virtual_hosted_style";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  storage_info = "host=xxx.com&access_id=111&access_key=222&s3_region=333&addressing_model=xxx";
  info1.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, info1.set(uri, storage_info));
}

TEST(ObObjectStorageInfo, test_storage_info_str)
{
  const char *uri = "oss://test_bucket";
  const char *storage_info_str = "host=xxx.com&access_id=111&access_key=222&checksum_type=md5&delete_mode=delete&addressing_model=path_style";
  ObObjectStorageInfo ak_info;
  ASSERT_EQ(OB_SUCCESS, ak_info.set(uri, storage_info_str));
  char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, ak_info.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info_str, buf);
  
  uri = "s3://test_bucket";
  storage_info_str = "host=xxx.com&access_id=111&access_key=222&s3_region=333&checksum_type=md5&delete_mode=delete&addressing_model=path_style";
  ak_info.reset();
  ASSERT_EQ(OB_SUCCESS, ak_info.set(uri, storage_info_str));
  ASSERT_EQ(OB_SUCCESS, ak_info.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info_str, buf);
    
  uri = "cos://test_bucket";
  storage_info_str = "host=xxx.com&access_id=111&access_key=222&appid=333&checksum_type=md5&delete_mode=delete&addressing_model=path_style";
  ak_info.reset();
  ASSERT_EQ(OB_SUCCESS, ak_info.set(uri, storage_info_str));
  ASSERT_EQ(OB_SUCCESS, ak_info.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info_str, buf);
}

TEST(ObObjectStorageInfo, test_hidden_access_key)
{
  const int64_t BUFLEN = OB_MAX_URI_LENGTH;
  char buf[BUFLEN];
  int64_t pos = 0;
  const char *uri = "oss://error_uri&host=xxx.com&access_id=111&access_key=222&checksum_type=md5&delete_mode=delete&addressing_model";
  const char *res = "oss://error_uri&host=xxx.com&access_id=111&access_key={hidden_access_key}&checksum_type=md5&delete_mode=delete&addressing_model";
  pos = ObSensitiveString(uri).to_string(buf, BUFLEN);
  ASSERT_EQ(0, ObString(pos, buf).compare(res));

  uri = "oss://error_uri&host=xxx.com&access_id=111&access_key=222";
  res = "oss://error_uri&host=xxx.com&access_id=111&access_key={hidden_access_key}";
  pos = ObSensitiveString(uri).to_string(buf, BUFLEN);
  ASSERT_EQ(0, ObString(pos, buf).compare(res));

  uri = "oss://error_uri&host=xxx.com&access_id=111&AcceSS_Key=222";
  res = "oss://error_uri&host=xxx.com&access_id=111&access_key={hidden_access_key}";
  pos = ObSensitiveString(uri).to_string(buf, BUFLEN);
  ASSERT_EQ(0, ObString(pos, buf).compare(res));

  uri = "oss://error_uri&host=xxx.com&access_id=111  &  AcceSS_Key=   222&checksum_type";
  res = "oss://error_uri&host=xxx.com&access_id=111  &  access_key={hidden_access_key}&checksum_type";
  pos = ObSensitiveString(uri).to_string(buf, BUFLEN);
  ASSERT_EQ(0, ObString(pos, buf).compare(res));

  uri = nullptr;
  res = "NULL";
  pos = ObSensitiveString(uri).to_string(buf, BUFLEN);
  ASSERT_EQ(0, ObString(pos, buf).compare(res));

  uri = "";
  res = "";
  pos = ObSensitiveString(uri).to_string(buf, BUFLEN);
  ASSERT_EQ(0, ObString(pos, buf).compare(res));

  uri = "oss://error_uri&host=xxx.com&access_id=111  &  AcceSS_Key=   222&checksum_type";
  res = "oss://error_uri&host=xxx.com&access_id=111  &  access_key={hidden_access_key}&checksum_";
  ObString s(strlen(uri) - 4, uri);
  pos = ObSensitiveString(s).to_string(buf, BUFLEN);
  ASSERT_EQ(0, ObString(pos, buf).compare(res));

  uri = "";
  res = "";
  s = ObString(0, uri);
  pos = ObSensitiveString(s).to_string(buf, BUFLEN);
  ASSERT_EQ(0, ObString(pos, buf).compare(res));

  uri = "oss:/error_uri&host=xxx.com&access_id=111&access_key=222";
  ObStorageType type;
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, get_storage_type_from_path(uri, type));
}

TEST(utility, ob_atoll_overflow)
{
  int64_t val = 0;
  char *end_str = nullptr;
  // INT64_MIN(-9223372036854775808) - 1
  const char *INT64_MIN_SUB_1_STR = "-9223372036854775809";
  ASSERT_EQ(OB_SIZE_OVERFLOW, ob_atoll(INT64_MIN_SUB_1_STR, val));
  ASSERT_EQ(OB_SIZE_OVERFLOW, ob_strtoll(INT64_MIN_SUB_1_STR, end_str, val));
  
  // INT64_MIN
  const char *INT64_MIN_STR = "-9223372036854775808";
  ASSERT_EQ(OB_SUCCESS, ob_atoll(INT64_MIN_STR, val));
  ASSERT_EQ(INT64_MIN, val);
  ASSERT_EQ(OB_SUCCESS, ob_strtoll(INT64_MIN_STR, end_str, val));
  ASSERT_EQ(INT64_MIN, val);
  
  // INT64_MAX(9223372036854775807)
  const char *INT64_MAX_STR = "9223372036854775807";
  ASSERT_EQ(OB_SUCCESS, ob_atoll(INT64_MAX_STR, val));
  ASSERT_EQ(INT64_MAX, val);
  ASSERT_EQ(OB_SUCCESS, ob_strtoll(INT64_MAX_STR, end_str, val));
  ASSERT_EQ(INT64_MAX, val);
  
  // INT64_MAX + 1
  const char *INT64_MAX_ADD_1_STR = "9223372036854775808";
  ASSERT_EQ(OB_SIZE_OVERFLOW, ob_atoll(INT64_MAX_ADD_1_STR, val));
  ASSERT_EQ(OB_SIZE_OVERFLOW, ob_strtoll(INT64_MAX_ADD_1_STR, end_str, val));
  
  // empty str
  const char *EMPTY_STR = "";
  ASSERT_EQ(OB_INVALID_ARGUMENT, ob_atoll(EMPTY_STR, val));
  // ASSERT_EQ(OB_INVALID_ARGUMENT, ob_strtoll(EMPTY_STR, end_str, val));
  
  // invalid str
  const char *INVALID_STR = "12345abc";
  ASSERT_EQ(OB_INVALID_ARGUMENT, ob_atoll(INVALID_STR, val));
  // ASSERT_EQ(OB_INVALID_ARGUMENT, ob_strtoll(INVALID_STR, end_str, val));
}

TEST(utility, ob_atoull_overflow)
{
  uint64_t val = 0;
  char *end_str = nullptr;
  
  // 0
  ASSERT_EQ(OB_SUCCESS, ob_atoull("0", val));
  ASSERT_EQ(0, val);
  ASSERT_EQ(OB_SUCCESS, ob_strtoull("0", end_str, val));
  ASSERT_EQ(0, val);
  
  // UINT64_MAX(18446744073709551615)
  const char *UINT64_MAX_STR = "18446744073709551615";
  ASSERT_EQ(OB_SUCCESS, ob_atoull(UINT64_MAX_STR, val));
  ASSERT_EQ(UINT64_MAX, val);
  ASSERT_EQ(OB_SUCCESS, ob_strtoull(UINT64_MAX_STR, end_str, val));
  ASSERT_EQ(UINT64_MAX, val);
  
  // UINT64_MAX + 1
  const char *UINT64_MAX_ADD_1_STR = "18446744073709551616";
  ASSERT_EQ(OB_SIZE_OVERFLOW, ob_atoull(UINT64_MAX_ADD_1_STR, val));
  ASSERT_EQ(OB_SIZE_OVERFLOW, ob_strtoull(UINT64_MAX_ADD_1_STR, end_str, val));
  
  // empty str
  const char *EMPTY_STR = "";
  ASSERT_EQ(OB_INVALID_ARGUMENT, ob_atoull(EMPTY_STR, val));
  // ASSERT_EQ(OB_INVALID_ARGUMENT, ob_strtoull(EMPTY_STR, end_str, val));
  
  // invalid str
  const char *INVALID_STR = "12345abc";
  ASSERT_EQ(OB_INVALID_ARGUMENT, ob_atoull(INVALID_STR, val));
  // ASSERT_EQ(OB_INVALID_ARGUMENT, ob_strtoull(INVALID_STR, end_str, val));
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_info.log*");
  OB_LOGGER.set_file_name("test_storage_info.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
