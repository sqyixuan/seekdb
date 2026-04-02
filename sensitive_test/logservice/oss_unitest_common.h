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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_OSS_TEST_COMMON_
#define OCEANBASE_UNITTEST_LOGSERVICE_OSS_TEST_COMMON_
#include <gtest/gtest.h>
#include <string>
#include "lib/restore/ob_storage_info.h"
#include "share/ob_server_struct.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#define private public
#include "close_modules/shared_storage/log/ob_shared_log_utils.h"
#undef private
namespace oceanbase
{
namespace unittest
{
class ObLogUnittestOssInfo {
public:
  ObLogUnittestOssInfo()
  {
    bucket_ = "##OSS_BUC##";
    host_ = "##OSS_HOST##";
    access_id_ = "##OSS_ACCESS_ID##";
    access_key_ = "##OSS_KEY##";
    if (bucket_.back() != '/') {
      bucket_ = bucket_ + "/unittest/";
    } else {
      bucket_ = bucket_ + "unittest/";
    }
  }
  ~ObLogUnittestOssInfo() {}

public:
  int convert_to_object_storage_info(common::ObObjectStorageInfo &info)
  {
    int ret = OB_SUCCESS;
    std::string dest_str = "host=" + host_ + "&access_id=" + access_id_ + "&access_key=" + access_key_;
    std::string dest_uri = bucket_ + "0";
    if (OB_FAIL(info.set(dest_uri.c_str(), dest_str.c_str()))) {
      CLOG_LOG(WARN, "set object info failed", K(info));
    } else {
    }
    return ret;
  }

  const std::string& get_root_path()
  {
    return bucket_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogUnittestOssInfo);

private:
  // Must not print following fields.
  std::string bucket_;
  std::string host_;
  std::string access_id_;
  std::string access_key_;
};

class ObLogUnittestOssCommon : public ::testing::Test {
public:
  ObLogUnittestOssCommon() {}
  ~ObLogUnittestOssCommon() {}
  static void init_oss(const std::string &base_dir)
  {
    const char *used_for = share::ObStorageUsedType::get_str(share::ObStorageUsedType::TYPE::USED_TYPE_ALL);
    const char *state = "ADDED";
    //host=xxxx&access_mode=xxx&access_id=xxx&access_key=xxx
    share::ObDeviceConfig device_config;
    common::ObObjectStorageInfo *info = MTL_NEW(ObObjectStorageInfo, "unittest");
    EXPECT_EQ(OB_SUCCESS, get_object_storage_info(*info));
    std::string root_path = get_root_path() + base_dir; 
    MEMCPY(device_config.path_, root_path.c_str(), root_path.size());
    MEMCPY(device_config.used_for_, used_for, strlen(used_for));
    device_config.sub_op_id_ = 1;
    STRCPY(device_config.state_, state);
    //access_mode=access_by_id&access_id=xxx&access_key=xxx
    std::string access_info =  "access_mode=access_by_id&" + std::string(info->access_id_) + "&" + std::string(info->access_key_);
    MEMCPY(device_config.access_info_, access_info.c_str(), access_info.length());
    MEMCPY(device_config.endpoint_, info->endpoint_, strlen(info->endpoint_));
    device_config.op_id_ = 1;
    device_config.last_check_timestamp_ = 0;
    device_config.storage_id_ = 1;
    device_config.max_iops_ = 0;
    device_config.max_bandwidth_ = 0;
    
    std::string rmdir_str = "rm -rf " + base_dir;
    system(rmdir_str.c_str());
    std::string mkdir_str = "mkdir " + base_dir;
    system(mkdir_str.c_str());
    ASSERT_EQ(OB_SUCCESS, share::ObDeviceConfigMgr::get_instance().init(base_dir.c_str()));
    ASSERT_EQ(OB_SUCCESS, share::ObDeviceConfigMgr::get_instance().add_device_config(device_config));
    common::ObBackupIoAdapter io_adapter;
    share::ObBackupDest dest;
    logservice::ObSharedLogUtilsImpl impl;
    uint64_t storage_id;
    ASSERT_EQ(OB_SUCCESS, impl.get_storage_dest_and_id_(dest, storage_id));
    GCTX.startup_mode_ = share::ObServerMode::SHARED_STORAGE_MODE;
    MTL_DELETE(ObObjectStorageInfo, "unittest", info);
  }
  static void destroy()
  {
  }
  static int get_object_storage_info(common::ObObjectStorageInfo &info)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(oss_info_.convert_to_object_storage_info(info))) {
      CLOG_LOG(WARN, "convert_to_object_storage_info failed");
    }
    return ret;
  }
  static const std::string &get_root_path()
  {
    return oss_info_.get_root_path();
  }
private:
  static ObLogUnittestOssInfo oss_info_;
};

} // end of unittest
} // end of oceanbase

#endif
