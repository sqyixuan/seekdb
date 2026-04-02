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
#define USING_LOG_PREFIX STORAGETEST

#include <gtest/gtest.h>
#include <gtest/gtest-param-test.h>

#define protected public
#define private public
#include "lib/restore/ob_storage.h"

#include "lib/restore/ob_storage_info.h"
#include "object_storage/test_object_storage.h"
#include "object_storage/object_storage_authorization_info.h"
#include "share/ob_device_credential_task.h"
#include "lib/restore/hmac_signature.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/ob_device_manager.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
class TestAssumeRole : public ::testing::TestWithParam<Config>, public ObObjectStorageUnittestCommon
{
public:
  TestAssumeRole()
  {}
  virtual ~TestAssumeRole()
  {}
  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    const Config &config = GetParam();
    if (ObString(config.role_arn_).prefix_match("##")) {
      enable_test_ = false;
    } else {
      enable_test_ = true;
    }

    if (OB_ISNULL(config.ak_) || OB_ISNULL(config.sk_)) {
      switch_flag_ = false;
    } else if (ObString(config.ak_).prefix_match("##") || ObString(config.sk_).prefix_match("##") || !enable_test_) {
      switch_flag_ = false;
    } else {
      switch_flag_ = true;
    }
    if (enable_test_) {
      device_credential_mgr_ = &ObDeviceCredentialMgr::get_instance();
      ASSERT_EQ(OB_SUCCESS, set_storage_info_with_assume_role(config.bucket_, config.endpoint_, config.appid_, config.region_,
                                config.extension_, config.role_arn_, config.external_id_, info_base_));
      if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), "%s/%s/", config.bucket_, dir_name_));
      }
      OB_LOG(INFO, "show result of set_storage_info_with_assume_role", K(config), K_(info_base));
      ASSERT_EQ(OB_SUCCESS, credential_key_.init(info_base_));
    }
    OB_LOG(INFO, "start to test initiation", K(enable_test_), K(switch_flag_));
  }
  virtual void TearDown() override
  {
    if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
      ObStorageUtil util;
      bool is_exist = false;
      ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(dir_uri_, is_exist));

      if (is_exist) {
        std::string dir_path = (dir_uri_ + strlen(OB_FILE_PREFIX));
        if (dir_path.size() > strlen(get_current_dir_name()) + 1) {
          std::string s = "rm -rf ";
          s += dir_path;
          ASSERT_EQ(0, ::system(s.c_str()));
        }
      }
    }
    if (enable_test_) {
      info_base_.reset();
      device_credential_mgr_->credential_map_.clear();
    }
  }

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->sts_credential = oceanbase::unittest::STS_CREDENTIAL;
    ASSERT_EQ(OB_SUCCESS, ObObjectStorageInfo::register_cluster_version_mgr(
      &ObClusterVersionBaseMgr::get_instance()));
  }

  static void TearDownTestCase()
  {
    TG_STOP(lib::TGDefIDs::ServerGTimer);
    TG_WAIT(lib::TGDefIDs::ServerGTimer);
    TG_DESTROY(lib::TGDefIDs::ServerGTimer);
    MockTenantModuleEnv::get_instance().destroy();
  }

protected:
  static const char *dir_name_;
  static char uri[OB_MAX_URI_LENGTH];
  ObDeviceCredentialKey credential_key_;
  ObDeviceCredentialMgr *device_credential_mgr_;
  bool switch_flag_;
};

const char *TestAssumeRole::dir_name_ = "test-assume-role";
char TestAssumeRole::uri[OB_MAX_URI_LENGTH] = {0};
TEST_P(TestAssumeRole, test_assume_role)
{
  if (enable_test_) {
    ObObjectStorageCredential device_credential;
    // validate the availability
    ObStorageUtil util;
    bool is_exist = true;
    const char *content = "test";
    // TimeTask is not set to refresh the credential, so the credential will not be refreshed actively
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), 
                "%stest_assume_role_%ld", dir_uri_, ObTimeUtility::current_time()));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_FALSE(is_exist);

    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, content, strlen(content)));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_TRUE(is_exist);
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_FALSE(is_exist);
    util.close();
  }
}

TEST_P(TestAssumeRole, test_background_refresh)
// test auto refresh credential
{
  if (enable_test_) {
    ObStorageUtil util;
    share::ObDeviceCredentialTask device_credential_task_;

    bool is_exist = true;
    const char *content = "test";
    const int64_t interval_s = 3;
    ObObjectStorageCredential before_refresh_credential;
    ObObjectStorageCredential after_refresh_credential;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), 
                "%stest_background_refresh_%ld", dir_uri_, ObTimeUtility::current_time()));

    // First get the credential
    ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->get_credential(credential_key_, before_refresh_credential));

    // Set the refresh interval to 3s
    TG_START(lib::TGDefIDs::ServerGTimer);
    ASSERT_EQ(OB_SUCCESS, device_credential_task_.init(interval_s * 1000 * 1000L));
    sleep(interval_s + 1);
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_FALSE(is_exist);

    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, content, strlen(content)));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_TRUE(is_exist);

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_FALSE(is_exist);
    // Since the interval is short and the conditions for active refresh are not met,
    // active refresh will not be triggered at this time.
    ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->get_credential_from_map_(credential_key_, after_refresh_credential));
    // Different credentials prove triggering background refresh
    ASSERT_NE(before_refresh_credential.access_time_us_, after_refresh_credential.access_time_us_);

    TG_STOP(lib::TGDefIDs::ServerGTimer);
    TG_WAIT(lib::TGDefIDs::ServerGTimer);
    util.close();
  }
}

TEST_P(TestAssumeRole, test_active_refresh)
{
  if (enable_test_) {
    ObObjectStorageCredential device_credential;
    ObStorageUtil util;
    bool is_exist = true;
    const char *content = "test";
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    ObObjectStorageCredential before_refresh_credential;
    ObObjectStorageCredential after_refresh_credential;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), 
                "%stest_background_refresh_%ld", dir_uri_, ObTimeUtility::current_time()));

    ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->get_credential(credential_key_, before_refresh_credential));
    // Modify the temporary ak to assimulate the access_token is coming to expiration
    before_refresh_credential.born_time_us_ = ObTimeUtility::current_time() - before_refresh_credential.expiration_s_ * 1000 * 1000L
        + CREDENTIAL_TASK_SCHEDULE_INTERVAL_US / 3;
    ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->credential_map_.set_refactored(
            credential_key_, before_refresh_credential, true /*overwrite*/));
    // Test whether the active refresh mechanism is effective when the background refresh
    // mechanism fails
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_FALSE(is_exist);
    ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->get_credential(credential_key_, after_refresh_credential));
    ASSERT_NE(before_refresh_credential.access_id_, after_refresh_credential.access_id_);

    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, content, strlen(content)));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_TRUE(is_exist);
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_FALSE(is_exist);
    util.close();
  }
}

TEST_P(TestAssumeRole, test_credential_expired)
// test delete expired credentials
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    share::ObDeviceCredentialTask device_credential_task_;

    bool is_exist = true;
    const char *content = "test";
    const int64_t refresh_interval_s = 3;
    const int64_t duration_us = 1 * 1000 * 1000L;
    ObObjectStorageCredential before_delete_credential;
    ObObjectStorageCredential after_delete_credential;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), 
              "%stest_credential_expired_%ld", dir_uri_, ObTimeUtility::current_time()));
    // Set the expiration time of the credential to 1s
    device_credential_mgr_->set_credential_duration_us(duration_us);

    ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->get_credential(credential_key_, before_delete_credential));

    int64_t before_map_size = device_credential_mgr_->credential_map_.size();
    TG_START(lib::TGDefIDs::ServerGTimer);
    ASSERT_EQ(OB_SUCCESS, device_credential_task_.init(refresh_interval_s * 1000 * 1000L));
    sleep(refresh_interval_s + 1);
    TG_STOP(lib::TGDefIDs::ServerGTimer);
    TG_WAIT(lib::TGDefIDs::ServerGTimer);
    ASSERT_EQ(before_map_size - 1,  device_credential_mgr_->credential_map_.size());

    // Ensure the credential is still available after the expired credentials are deleted
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_FALSE(is_exist);
    ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->get_credential(credential_key_, after_delete_credential));
    ASSERT_NE(before_delete_credential.access_id_, after_delete_credential.access_id_);
    ASSERT_EQ(before_map_size, device_credential_mgr_->credential_map_.size());
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, content, strlen(content)));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    ASSERT_TRUE(is_exist);

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
    util.close();
  }
}

TEST_P(TestAssumeRole, switch_between_id_and_assume_role)
{
  if (enable_test_) {
    if (switch_flag_) {
      int64_t count = 2;
      ObStorageUtil util;
      bool is_exist = true;
      const char *content = "test";
      while (count > 0) {
        // switch to access_by_ak_sk mode, verify the availability of this mode
        const Config &config = GetParam();
        info_base_.reset();
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), 
                "%sswitch_ak_%ld_%ld", dir_uri_, count, ObTimeUtility::current_time()));
        ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_, config.endpoint_, config.ak_, config.sk_, config.appid_,
                                  config.region_, config.extension_, info_base_));

        ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
        ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
        ASSERT_FALSE(is_exist);

        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, content, strlen(content)));
        ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
        ASSERT_TRUE(is_exist);

        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
        ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
        util.close();
        // switch to access_by_assume_role, verify the availability of this mode
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), 
                "%sswitch_role_%ld_%ld", dir_uri_, count, ObTimeUtility::current_time()));
        info_base_.reset();
        ASSERT_EQ(OB_SUCCESS, set_storage_info_with_assume_role(config.bucket_, config.endpoint_, config.appid_, config.region_,
                                  config.extension_, config.role_arn_, config.external_id_, info_base_));
        ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
        ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
        ASSERT_FALSE(is_exist);

        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, content, strlen(content)));
        ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
        ASSERT_TRUE(is_exist);

        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
        ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
        util.close();
        count--;
      }
    }
  }
}

TEST_P(TestAssumeRole, test_passive_refresh_when_permission_denied) 
{
  if (enable_test_) {
      ObStorageUtil util;
      bool is_exist = true;
      const char *content = "test";
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), 
                "%stest_passive_refresh_%ld", dir_uri_, ObTimeUtility::current_time()));
      ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
      ObObjectStorageCredential before_refresh_credential;
      ObObjectStorageCredential after_refresh_credential;
      ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->get_credential(credential_key_, before_refresh_credential));
      // Modify the temporary ak to assimulate the access_token expiration
      before_refresh_credential.access_key_[strlen(before_refresh_credential.access_key_) - 2] = '\0';
      ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->credential_map_.set_refactored(credential_key_, before_refresh_credential, true /*overwrite*/));
      // If the bottom-up mechanism is effective, and the credential should been refreshed
      // after the permission is denied.
      ASSERT_EQ(OB_OBJECT_STORAGE_PERMISSION_DENIED, util.is_exist(uri, is_exist));
      ASSERT_FALSE(is_exist);
      ASSERT_EQ(OB_SUCCESS, device_credential_mgr_->get_credential(credential_key_, after_refresh_credential));
      ASSERT_NE(before_refresh_credential.access_id_, after_refresh_credential.access_id_);
      ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, content, strlen(content)));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
      ASSERT_TRUE(is_exist);
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_exist));
      ASSERT_FALSE(is_exist);
      util.close();
  }
}

TEST_P(TestAssumeRole, test_storage_info_str)
{
  if (enable_test_) {
    char storage_info_str1[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
    char storage_info_str2[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
    ASSERT_EQ(OB_SUCCESS, info_base_.get_storage_info_str(storage_info_str1, sizeof(storage_info_str1)));
    int64_t pos = strlen(storage_info_str1);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(storage_info_str1, sizeof(storage_info_str1), pos, "&checksum_type=md5&delete_mode=delete&addressing_model=path_style"));
    info_base_.reset();
    ASSERT_EQ(OB_SUCCESS, info_base_.set(dir_uri_, storage_info_str1));
    ASSERT_EQ(OB_SUCCESS, info_base_.get_storage_info_str(storage_info_str2, sizeof(storage_info_str2)));
    ASSERT_STREQ(storage_info_str1, storage_info_str2);
  }
}


TEST(TestSignature, test_hmac_signature)
{
  ObArray<std::pair<const char *, const char *>> params;
  char signature[1024] = {0};
  params.push_back(std::pair<const char *, const char *>("Action", "GetResourceSTSCredential"));
  params.push_back(std::pair<const char *, const char *>("RequestSource", "OBSERVER"));
  params.push_back(std::pair<const char *, const char *>("RequestId", "luyun5678"));
  params.push_back(std::pair<const char *, const char *>("RoleArn", "iam::ffae449e59374311b2f90fcd7017fa3c:agency:ObserverObsRoleForSts"));
  params.push_back(std::pair<const char *, const char *>("SignatureNonce", "17199251264788931433653971"));
  params.push_back(std::pair<const char *, const char *>("DurationSeconds", "900"));
  params.push_back(std::pair<const char *, const char *>("ObSignatureKey", "bcea7"));
  params.push_back(std::pair<const char *, const char *>("ObSignatureSecret", "bcea78e4888cec07892e447c85ec33f5"));

  ASSERT_EQ(OB_SUCCESS, sign_request(params, "POST", signature, sizeof(signature)));
  const char *tmp = "nkfdMADzsTkAVPYouuhUszHbfTU=";
  ASSERT_EQ(strcmp(signature, tmp), 0);

  params.reuse();
  params.push_back(std::pair<const char *, const char *>("Action", "GetResourceSTSCredential"));
  params.push_back(std::pair<const char *, const char *>("RequestSource", "OBSERVER"));
  params.push_back(std::pair<const char *, const char *>("RequestId", "luyun5678"));
  params.push_back(std::pair<const char *, const char *>("RoleArn", "iam::ffae449e59374311b2f90fcd7017fa3c:agency:ObserverObsRoleForSts"));
  params.push_back(std::pair<const char *, const char *>("SignatureNonce", "17199251264788931433653971"));
  params.push_back(std::pair<const char *, const char *>("DurationSeconds", "900"));
  params.push_back(std::pair<const char *, const char *>("ObSignatureKey", "58837"));
  params.push_back(std::pair<const char *, const char *>("ObSignatureSecret", "58837954a85a259f2b010fcb386bc548"));

  ASSERT_EQ(OB_SUCCESS, sign_request(params, "POST", signature, sizeof(signature)));
  tmp = "c1o8mIT5ZnE349fsjCgPPVGZl1U=";
  ASSERT_EQ(strcmp(signature, tmp), 0);

  params.reuse();
  params.push_back(std::pair<const char *, const char *>("Action", "GetResourceSTSCredential"));
  params.push_back(std::pair<const char *, const char *>("RequestSource", "OBSERVER"));
  params.push_back(std::pair<const char *, const char *>("RequestId", "luyun5678"));
  params.push_back(std::pair<const char *, const char *>("RoleArn", "iam::ffae449e59374311b2f90fcd7017fa3c:agency:ObserverObsRoleForSts"));
  params.push_back(std::pair<const char *, const char *>("SignatureNonce", "17199251264788931433653971"));
  params.push_back(std::pair<const char *, const char *>("DurationSeconds", "900"));
  params.push_back(std::pair<const char *, const char *>("ObSignatureKey", "e6259"));
  params.push_back(std::pair<const char *, const char *>("ObSignatureSecret", "e625912842015a4f022ff41642acac76"));
  ASSERT_EQ(OB_SUCCESS, sign_request(params, "POST", signature, sizeof(signature)));
  tmp = "lqoYtW9IUGS3wRYJ2S2a9KJe2LA=";
  ASSERT_EQ(strcmp(signature, tmp), 0);

  params.reuse();
  params.push_back(std::pair<const char *, const char *>("Action", "GetResourceSTSCredential"));
  params.push_back(std::pair<const char *, const char *>("RequestSource", "OBSERVER"));
  params.push_back(std::pair<const char *, const char *>("RequestId", "luyun5678"));
  params.push_back(std::pair<const char *, const char *>("RoleArn", "iam::ffae449e59374311b2f90fcd7017fa3c:agency:ObserverObsRoleForSts"));
  params.push_back(std::pair<const char *, const char *>("SignatureNonce", "17199251264788931433653971"));
  params.push_back(std::pair<const char *, const char *>("DurationSeconds", "900"));
  params.push_back(std::pair<const char *, const char *>("ObSignatureKey", "96fc8"));
  params.push_back(std::pair<const char *, const char *>("ObSignatureSecret", "96fc80ec6e92bd040c0dcf800e3b06e0"));
  ASSERT_EQ(OB_SUCCESS, sign_request(params, "POST", signature, sizeof(signature)));
  tmp = "7UyuMztOwNqWMuwQeO1j0anAsxk=";
  ASSERT_EQ(strcmp(signature, tmp), 0);

}
std::vector<unittest::Config> assume_configs;
INSTANTIATE_TEST_CASE_P(
  ConfigCombinations,
  TestAssumeRole,
  ::testing::ValuesIn(assume_configs),
  ObObjectStorageUnittestUtil::custom_test_name
);
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(oceanbase::unittest::assume_configs,
      oceanbase::unittest::Config{
          "S3",                             /*storage_type*/
          oceanbase::unittest::S3_BUCKET,   /*bucket*/
          oceanbase::unittest::S3_ENDPOINT, /*enpoint*/
          oceanbase::unittest::S3_AK,       /*ak*/
          oceanbase::unittest::S3_SK,       /*sk*/
          oceanbase::unittest::S3_REGION,   /*region*/
          nullptr,                          /*appid*/
          nullptr,                          /*entension*/
          oceanbase::unittest::S3_ROLE_ARN, /*role_arn*/
          nullptr                           /*external_id*/
      },
      {nullptr});
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(oceanbase::unittest::assume_configs,
      oceanbase::unittest::Config{
          "OBS",                             /*storage_type*/
          oceanbase::unittest::OBS_BUCKET,   /*bucket*/
          oceanbase::unittest::OBS_ENDPOINT, /*enpoint*/
          oceanbase::unittest::OBS_AK,       /*ak*/
          oceanbase::unittest::OBS_SK,       /*sk*/
          oceanbase::unittest::OBS_REGION,   /*region*/
          nullptr,                           /*appid*/
          nullptr,                           /*entension*/
          oceanbase::unittest::OBS_ROLE_ARN, /*role_arn*/
          nullptr                            /*external_id*/
      },
      {nullptr});

  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(oceanbase::unittest::assume_configs,
      oceanbase::unittest::Config{
          "OSS",                             /*storage_type*/
          oceanbase::unittest::OSS_BUCKET,   /*bucket*/
          oceanbase::unittest::OSS_ENDPOINT, /*enpoint*/
          oceanbase::unittest::OSS_AK,       /*ak*/
          oceanbase::unittest::OSS_SK,       /*sk*/
          nullptr,                           /*region*/
          nullptr,                           /*appid*/
          nullptr,                           /*entension*/
          oceanbase::unittest::OSS_ROLE_ARN, /*role_arn*/
          nullptr                            /*external_id*/
      },
      {nullptr});
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(oceanbase::unittest::assume_configs,
      oceanbase::unittest::Config{
          "COS",                             /*storage_type*/
          oceanbase::unittest::COS_BUCKET,   /*bucket*/
          oceanbase::unittest::COS_ENDPOINT, /*enpoint*/
          oceanbase::unittest::COS_AK,       /*ak*/
          oceanbase::unittest::COS_SK,       /*sk*/
          nullptr,                           /*region*/
          oceanbase::unittest::COS_APPID,    /*appid*/
          nullptr,                           /*entension*/
          oceanbase::unittest::COS_ROLE_ARN, /*role_arn*/
          nullptr                            /*external_id*/
      },
      {nullptr});

  system("rm -f ./test_assume_role.log*");
  OB_LOGGER.set_file_name("test_assume_role.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
