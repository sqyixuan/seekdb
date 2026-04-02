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

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#undef private
#undef protected
#include "test_object_storage.h"
#include "mittest/shared_storage/clean_residual_data.h"

#define private public
#undef private

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

class TestBackupIOAdapter: public ::testing::TestWithParam<Config>, public ObObjectStorageUnittestCommon
{
public:
  TestBackupIOAdapter() {}
  virtual ~TestBackupIOAdapter() {}
  virtual void SetUp()
  {
    const Config &config = GetParam();
    if (need_skip_test(config.ak_) || need_skip_test(config.sk_)) {
      enable_test_ = false;
    } else {
      enable_test_ = true;
    }
    // Since GCS does not support the batch_del_files and is_tagging interface,
    // Therefore, set a flag to mark GCS when performing those tests.
    if (0 == STRNCMP(config.storage_type_, "GCS", strlen("GCS"))) {
      is_gcs_ = true;
    }
    if (enable_test_) {
      ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_, config.endpoint_, config.ak_,
                                             config.sk_, config.appid_, config.region_,
                                             config.extension_, info_base_));
      if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), "%s%s/",
                                              OB_FILE_PREFIX, get_current_dir_name()));
      } else {
        ASSERT_EQ(OB_SUCCESS,
            databuff_printf(dir_uri_, sizeof(dir_uri_), "%s/%s/", config.bucket_, dir_name_));
      }
    }
    if (config.enable_obdal_) {
      cluster_enable_obdal_config = &ObClusterEnableObdalConfigBase::get_instance();
    } else {
      cluster_enable_obdal_config = nullptr;
    }
  }
  virtual void TearDown()
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
    info_base_.reset();
  }

  static void SetUpTestCase()
  {
    GCTX.startup_mode_  = observer::ObServerMode::SHARED_STORAGE_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    
    const int64_t MAX_IO_TIMEOUT_MS = 60LL * 1000LL; // 1min
    ObRefHolder<ObTenantIOManager> tenant_holder;
    OK(OB_IO_MANAGER.get_tenant_io_manager(OB_SERVER_TENANT_ID, tenant_holder));
    ASSERT_NE(nullptr, tenant_holder.get_ptr());
    
    ObTenantIOConfig::ParamConfig io_param_config(tenant_holder.get_ptr()->get_io_config().param_config_);
    io_param_config.object_storage_io_timeout_ms_ = MAX_IO_TIMEOUT_MS;
    OK(tenant_holder.get_ptr()->update_basic_io_param_config(io_param_config));
    ASSERT_EQ(OB_SUCCESS, ObObjectStorageInfo::register_cluster_version_mgr(
        &ObClusterVersionBaseMgr::get_instance()));
  }
  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
    }
    MockTenantModuleEnv::get_instance().destroy();
  }

protected:
  static const char *dir_name_;
  static char uri[OB_MAX_URI_LENGTH];
  // Override the parent class's info_base_
  ObBackupStorageInfo info_base_;
  bool is_gcs_;
};

const char *TestBackupIOAdapter::dir_name_ = "io_adapter_unittest_dir";
char TestBackupIOAdapter::uri[OB_MAX_URI_LENGTH] = { 0 };

TEST_P(TestBackupIOAdapter, test_io_manager_rw)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    const char *tmp_dir = "test_io_manager_rw";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ObBackupIoAdapter io_adapter;
    ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(dir_uri_, &info_base_));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_rw", dir_uri_));
    
    const int64_t size = 10;
    char buf[size] = "123456789";
    const uint64_t storage_id = 2;
    const ObStorageUsedMod mod = ObStorageUsedMod::STORAGE_USED_DATA;
    const ObStorageIdMod storage_id_mod(storage_id, mod);

    ASSERT_EQ(OB_SUCCESS,
        io_adapter.write_single_file(uri, &info_base_, buf, size, storage_id_mod));

    char read_buf[size];
    memset(read_buf, 0, size);
    int64_t read_size = -1;
    ASSERT_EQ(OB_SUCCESS,
        io_adapter.read_single_file(uri, &info_base_, read_buf, size, read_size, storage_id_mod));
    ASSERT_EQ(10, read_size);
    ASSERT_EQ(0, memcmp(buf, read_buf, read_size));
    // Verify the reading situation when offset is 0 and offset > 0 
    const int64_t tmp_buf_size = 5;
    for (int64_t i = 0; i < size - tmp_buf_size; i++) {
      memset(read_buf, 0, size);
      read_size = -1;
      ASSERT_EQ(OB_SUCCESS,
          io_adapter.read_part_file(uri, &info_base_, read_buf, tmp_buf_size, i, read_size, storage_id_mod));
      ASSERT_EQ(0, memcmp(buf + i, read_buf, tmp_buf_size));
      ASSERT_EQ(tmp_buf_size, read_size);
    }

    read_size = -1;
    ASSERT_EQ(OB_SUCCESS,
        io_adapter.pread(uri, &info_base_, read_buf, 5, 3, read_size, storage_id_mod));
    ASSERT_EQ(0, memcmp(buf + 3, read_buf, 5));
    ASSERT_EQ(5, read_size);

    ASSERT_EQ(OB_SUCCESS, io_adapter.del_file(uri, &info_base_));
  }
}

TEST_P(TestBackupIOAdapter, test_enable_worm)
{
  //If enable_worm=true, use put fragment instead of append for writes.
  int ret = OB_SUCCESS;
  if (enable_test_ && !is_use_obdal() && ObStorageType::OB_STORAGE_OSS == info_base_.get_type()
          && ObStorageChecksumType::OB_MD5_ALGO == info_base_.get_checksum_type()) {
    //init info_base_    
    info_base_.reset();
    const Config &config = GetParam();
    ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
      config.endpoint_,
      config.ak_, config.sk_,
      config.appid_, config.region_,
      "enable_worm=true&checksum_type=md5", info_base_));
    
    //init dir and uri
    const char *tmp_dir = "test_enable_worm";
    const char *file_name = "test_append";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld", tmp_dir, ts));
    ObBackupIoAdapter io_adapter;
    ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(dir_uri_, &info_base_));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s", dir_uri_, file_name));

    // init append content
    const int64_t size = 100;
    const int64_t first_fragment_size = size/4;
    const int64_t second_fragment_size = size - first_fragment_size;
    char write_buf[size] = {0};
    memset(write_buf, '1', first_fragment_size);
    memset(write_buf + first_fragment_size, '2', second_fragment_size);
    
    // append data
    ObIODevice *device_handle;
    ObIOFd write_fd;
    int64_t tmp_write_size = -1;
    
    ASSERT_EQ(OB_SUCCESS,
      io_adapter.open_with_access_type(device_handle, write_fd, &info_base_, uri,
                                       ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER,
                                       ObStorageIdMod::get_default_id_mod()));
    ASSERT_EQ(OB_SUCCESS, io_adapter.pwrite(
      *device_handle, write_fd,
      write_buf, 0, first_fragment_size, tmp_write_size, false/*is_can_seal*/));
    ASSERT_EQ(first_fragment_size, tmp_write_size);
    tmp_write_size = -1;
    ASSERT_EQ(OB_SUCCESS, io_adapter.pwrite(
      *device_handle, write_fd,
      write_buf + first_fragment_size, 
      first_fragment_size, second_fragment_size, tmp_write_size, false/*is_can_seal*/));
    ASSERT_EQ(second_fragment_size, tmp_write_size);
    ASSERT_EQ(OB_SUCCESS, io_adapter.seal_file(uri, &info_base_, ObStorageIdMod::get_default_id_mod()));

    char read_buf[size] = {0};
    int64_t tmp_read_size = -1;
    ASSERT_EQ(OB_SUCCESS, 
      io_adapter.adaptively_read_single_file(uri, &info_base_, read_buf, size, tmp_read_size,
                                             ObStorageIdMod::get_default_id_mod()));
    ASSERT_EQ(0, memcmp(write_buf, read_buf, size));
    TestObjectStorageListOp list_op;
    ASSERT_EQ(OB_SUCCESS, io_adapter.list_files(dir_uri_, &info_base_, list_op));
    ASSERT_EQ(4, list_op.object_list_.size());
  }
}

TEST_P(TestBackupIOAdapter, test_nohead_read)
{
  if (enable_test_) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_nohead_read";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ObBackupIoAdapter io_adapter;
    ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(dir_uri_, &info_base_));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_nohead_read", dir_uri_));

    const int64_t size = 10;
    char write_buf[size] = "123456789";
    const uint64_t storage_id = 2;
    const ObStorageUsedMod mod = ObStorageUsedMod::STORAGE_USED_DATA;
    const ObStorageIdMod storage_id_mod(storage_id, mod);
    const int64_t read_buf_size = size * 2;
    char read_buf[read_buf_size];
    memset(read_buf, 0, read_buf_size);

    ASSERT_EQ(OB_SUCCESS,
        io_adapter.write_single_file(uri, &info_base_, write_buf, size, storage_id_mod));
    
    {
      // invalid args
      ObBackupIoAdapter io_adapter;
      ObIODevice *device_handle;
      ObIOFd fd;
      ObIOHandle io_handle;
      ASSERT_EQ(OB_SUCCESS,
          io_adapter.open_with_access_type(device_handle, fd, &info_base_, uri,
                                           ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                           storage_id_mod));
      // buf_size == 0
      ASSERT_EQ(OB_SUCCESS, io_adapter.async_pread(*device_handle, fd, read_buf,
                                                   0, 0, io_handle));
      ASSERT_EQ(OB_INVALID_ARGUMENT, io_handle.wait());
      
      // offset < 0
      io_handle.reset();
      ASSERT_EQ(OB_INVALID_ARGUMENT, io_adapter.async_pread(*device_handle, fd, read_buf,
                                                            -1, 1, io_handle));
      
      ASSERT_EQ(OB_SUCCESS, io_adapter.close_device_and_fd(device_handle, fd));
    }
    {
      OB_LOG(INFO, "=================== Test read end > file length > start ===================");
      ObBackupIoAdapter io_adapter;
      ObIODevice *device_handle;
      ObIOFd fd;
      ObIOHandle io_handle;
      memset(read_buf, 0, read_buf_size);
      ASSERT_EQ(OB_SUCCESS,
          io_adapter.open_with_access_type(device_handle, fd, &info_base_, uri,
                                           ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                           storage_id_mod));
      ASSERT_EQ(OB_SUCCESS, io_adapter.async_pread(*device_handle, fd, read_buf,
                                                   5, size + 1, io_handle));
      ASSERT_EQ(OB_SUCCESS, io_handle.wait());
      
      const int64_t read_size = io_handle.get_data_size();
      if (is_use_obdal() == false && info_base_.get_type() == ObStorageType::OB_STORAGE_OSS) {
        ASSERT_EQ(read_size, size);
        ASSERT_EQ(0, STRCMP(read_buf, write_buf));
      } else {
        ASSERT_EQ(read_size, size - 5);
        ASSERT_EQ(0, STRCMP(read_buf, write_buf + 5));
      }
      ASSERT_EQ(OB_SUCCESS, io_adapter.close_device_and_fd(device_handle, fd));
    }
    {
      OB_LOG(INFO, "=================== Test read end > file length = start ===================");
      ObBackupIoAdapter io_adapter;
      ObIODevice *device_handle;
      ObIOFd fd;
      ObIOHandle io_handle;
      memset(read_buf, 0, read_buf_size);
      ASSERT_EQ(OB_SUCCESS,
          io_adapter.open_with_access_type(device_handle, fd, &info_base_, uri,
                                           ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                           storage_id_mod));
      ASSERT_EQ(OB_SUCCESS, io_adapter.async_pread(*device_handle, fd, read_buf,
                                                   size, size + 1, io_handle));
      
      if (is_use_obdal() == false && info_base_.get_type() == ObStorageType::OB_STORAGE_OSS) {
        ASSERT_EQ(OB_SUCCESS, io_handle.wait());
        const int64_t read_size = io_handle.get_data_size();
        ASSERT_EQ(read_size, size);
        ASSERT_EQ(0, STRCMP(read_buf, write_buf));
      } else if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
        ASSERT_EQ(OB_SUCCESS, io_handle.wait());
        ASSERT_EQ(0, io_handle.get_data_size());
      } else {
        ASSERT_NE(OB_SUCCESS, io_handle.wait());
      }
      ASSERT_EQ(OB_SUCCESS, io_adapter.close_device_and_fd(device_handle, fd));
    }
    {
      OB_LOG(INFO, "=================== Test read start > file length ===================");
      ObBackupIoAdapter io_adapter;
      ObIODevice *device_handle;
      ObIOFd fd;
      ObIOHandle io_handle;
      memset(read_buf, 0, read_buf_size);
      ASSERT_EQ(OB_SUCCESS,
          io_adapter.open_with_access_type(device_handle, fd, &info_base_, uri,
                                           ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                           storage_id_mod));
      ASSERT_EQ(OB_SUCCESS, io_adapter.async_pread(*device_handle, fd, read_buf,
                                                   size, size + 1, io_handle));

      if (is_use_obdal() == false && info_base_.get_type() == ObStorageType::OB_STORAGE_OSS) {
        ASSERT_EQ(OB_SUCCESS, io_handle.wait());
        const int64_t read_size = io_handle.get_data_size();
        ASSERT_EQ(read_size, size);
        ASSERT_EQ(0, STRCMP(read_buf, write_buf));
      } else if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
        ASSERT_EQ(OB_SUCCESS, io_handle.wait());
        ASSERT_EQ(0, io_handle.get_data_size());
      } else {
        ASSERT_NE(OB_SUCCESS, io_handle.wait());
      }
      ASSERT_EQ(OB_SUCCESS, io_adapter.close_device_and_fd(device_handle, fd));
    }

    ASSERT_EQ(OB_SUCCESS, io_adapter.del_file(uri, &info_base_));
  }
}

int parallel_read(
    const char *write_buf, const int64_t write_buf_size, const int64_t parallelism,
    const ObBackupStorageInfo &info, const ObString &uri,
    ObStorageAccessType access_type, const ObStorageIdMod &storage_id_mod)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_buf)
      || OB_UNLIKELY(parallelism <= 0 || write_buf_size < parallelism || !info.is_valid() || uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), KP(write_buf), K(parallelism),
        K(write_buf_size), K(uri), K(info), K(access_type), K(storage_id_mod));
  } else {
    char read_buf[write_buf_size];
    MEMSET(read_buf, 0, write_buf_size);
    ObIODevice *device_handle;
    ObIOFd fd;
    ObBackupIoAdapter io_adapter;
    std::vector<ObIOHandle> io_handle(parallelism);
    const int64_t split_size = write_buf_size / parallelism;

    if (OB_FAIL(io_adapter.open_with_access_type(
        device_handle, fd, &info, uri, access_type, storage_id_mod))) {
      OB_LOG(WARN, "fail to open adapter", KR(ret),
          K(info), K(uri), K(access_type), K(storage_id_mod));
    } 
    for (int64_t i = 0; OB_SUCC(ret) && i < parallelism; ++i) {
      const int64_t tmp_offset = i * split_size;
      const int64_t tmp_size = split_size;
      if (OB_FAIL(io_adapter.async_pread(*device_handle, fd, read_buf + tmp_offset,
                                         tmp_offset, tmp_size, io_handle[i]))) {
        OB_LOG(WARN, "fail to async_pread", KR(ret),
            K(info), K(uri), K(access_type), K(storage_id_mod), K(tmp_offset), K(tmp_size));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < parallelism; ++i) {
      if (OB_FAIL(io_handle[i].wait())) {
        OB_LOG(WARN, "fail to wait", KR(ret),
            K(info), K(uri), K(access_type), K(storage_id_mod), K(i));
      }
    }
    if (FAILEDx(io_adapter.close_device_and_fd(device_handle, fd))) {
      OB_LOG(WARN, "fail to close adapter", KR(ret),
          K(info), K(uri), K(access_type), K(storage_id_mod));
    } else if (OB_UNLIKELY(0 != MEMCMP(write_buf, read_buf, split_size * parallelism))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "data error", KR(ret), K(write_buf_size),
          K(info), K(uri), K(access_type), K(storage_id_mod), K(split_size), K(parallelism));
    }
  }
  return ret;
}

TEST_P(TestBackupIOAdapter, concurrent_pread)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base_));
    const char *tmp_dir = "test_concurrent_pread";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ObBackupIoAdapter io_adapter;
    ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(dir_uri_, &info_base_));
    
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_concurrent_pread", dir_uri_));
     
    const int64_t size = 100;
    char write_buf[size];
    memset(write_buf, 'a', size / 2);
    memset(write_buf + size / 2, 'b', size / 2);
    const uint64_t storage_id = 2;
    const ObStorageUsedMod mod = ObStorageUsedMod::STORAGE_USED_DATA;
    const ObStorageIdMod storage_id_mod(storage_id, mod);

    // append data
    const int64_t first_write_size = size / 2;
    const int64_t second_write_size = size / 4;
    ObIODevice *write_device_handle;
    ObIOFd write_fd;
    ASSERT_EQ(OB_SUCCESS,
        io_adapter.open_with_access_type(write_device_handle, write_fd, &info_base_, uri,
                                         ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER,
                                         storage_id_mod));
    
    int64_t tmp_write_size = -1;
    ASSERT_EQ(OB_SUCCESS, io_adapter.pwrite(
        *write_device_handle, write_fd,
        write_buf, 0, first_write_size, tmp_write_size, false/*is_can_seal*/));
    ASSERT_EQ(tmp_write_size, first_write_size);
    
    tmp_write_size = -1;
    ASSERT_EQ(OB_SUCCESS, io_adapter.pwrite(
        *write_device_handle, write_fd,
        write_buf + first_write_size, first_write_size, second_write_size,
        tmp_write_size, false/*is_can_seal*/));
    ASSERT_EQ(tmp_write_size, second_write_size);
    
    for (int64_t i = first_write_size + second_write_size; i < size - 1; i++) {
      tmp_write_size = -1;
      ASSERT_EQ(OB_SUCCESS, io_adapter.pwrite(
          *write_device_handle, write_fd,
          write_buf + i, i, 1,
          tmp_write_size, false/*is_can_seal*/));
      ASSERT_EQ(tmp_write_size, 1);
    }
    ASSERT_EQ(OB_SUCCESS, io_adapter.pwrite(
        *write_device_handle, write_fd,
        write_buf + size - 1, size - 1, 1,
        tmp_write_size, true/*is_can_seal*/));

    // write data
    // ASSERT_EQ(OB_SUCCESS, io_adapter.write_single_file(uri, &info_base_, write_buf, size, storage_id_mod));
    // appenable file needs to use ADAPTIVE_READER
    ASSERT_EQ(OB_SUCCESS, parallel_read(write_buf, size, 10,
                                        info_base_, uri,
                                        OB_STORAGE_ACCESS_ADAPTIVE_READER, storage_id_mod));
    ASSERT_EQ(OB_SUCCESS, io_adapter.adaptively_del_file(uri, &info_base_));

    // normal data
    ASSERT_EQ(OB_SUCCESS, io_adapter.write_single_file(uri, &info_base_, write_buf, size, storage_id_mod));
    // normal data can be read using any reader
    ASSERT_EQ(OB_SUCCESS, parallel_read(write_buf, size, 10,
                                        info_base_, uri,
                                        OB_STORAGE_ACCESS_ADAPTIVE_READER, storage_id_mod));
    ASSERT_EQ(OB_SUCCESS, parallel_read(write_buf, size, 10,
                                        info_base_, uri,
                                        OB_STORAGE_ACCESS_READER, storage_id_mod));
    // no head reader needs caller to control read range
    ASSERT_EQ(OB_SUCCESS, parallel_read(write_buf, size, 10,
                                        info_base_, uri,
                                        OB_STORAGE_ACCESS_NOHEAD_READER, storage_id_mod));
    ASSERT_EQ(OB_SUCCESS, io_adapter.del_file(uri, &info_base_));
  }
}

int random_upload(
    const ObString &uri,
    ObBackupStorageInfo *info,
    const ObStorageAccessType &access_type,
    const char *write_buf,
    const int64_t content_size,
    const int64_t part_size_threshold)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  ObIODevice *device_handle = nullptr;
  ObIOFd fd;

  if (OB_ISNULL(write_buf) || OB_ISNULL(info)
      || OB_UNLIKELY(uri.empty() || content_size <= 0 || part_size_threshold <= 0)
      || OB_UNLIKELY(!info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments",
        K(ret), K(uri), KP(write_buf), K(part_size_threshold), K(content_size), KPC(info));
  } else if (OB_FAIL(adapter.open_with_access_type(device_handle, fd, info, uri, access_type,
                             ObStorageIdMod::get_default_id_mod()))) {
    OB_LOG(WARN, "fail to open_with_access_type", K(ret), K(uri), KPC(info), K(access_type));
  } else {
    bool is_full = false;
    bool is_exist = false;
    int64_t part_id = -1;
    int64_t cur_offset = 0;
    int64_t task_num = content_size / part_size_threshold + 1;
    std::thread t[task_num];
    ObIOHandle io_handle[task_num];
    int i = 0;
    while (OB_SUCC(ret) && cur_offset < content_size) {
      int64_t part_size = MIN(part_size_threshold, content_size - cur_offset);
      if (OB_FAIL(adapter.async_upload_data(*device_handle, fd,
                                            write_buf + cur_offset, 0, part_size,
                                            io_handle[i]))) {
        OB_LOG(WARN, "fail to async_upload_data", K(ret), K(i));
      } else {
        i++;
        cur_offset += part_size;
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < i; j++) {
      if (OB_FAIL(io_handle[j].wait())) {
        OB_LOG(WARN, "fail to wait", K(ret), K(j), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(adapter.complete(*device_handle, fd))) {
        OB_LOG(WARN, "fail to complete", K(ret));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ret)) {
      if (OB_TMP_FAIL(adapter.abort(*device_handle, fd))) {
        OB_LOG(WARN, "fail to abort", K(ret), K(tmp_ret));
      }
    }
    if (OB_TMP_FAIL(adapter.close_device_and_fd(device_handle, fd))) {
      OB_LOG(WARN, "fail to close_device_and_fd", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int check_multipart_object(
    const ObString &uri, 
    ObBackupStorageInfo *info,
    const char *write_buf, 
    const int64_t content_size)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  int64_t file_length = -1;
  char *read_buf = nullptr;
  int64_t read_size = -1;
  ObArenaAllocator allocator;

  if (OB_ISNULL(write_buf) || OB_ISNULL(info)
      || OB_UNLIKELY(uri.empty() || content_size <= 0 || !info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments",
        K(ret), K(uri), KP(write_buf), K(content_size), KPC(info));
  } else if (OB_FAIL(adapter.get_file_length(uri, info, file_length))) {
    OB_LOG(WARN, "fail to get_file_length", K(uri), KPC(info));
  } else if (OB_UNLIKELY(file_length != content_size)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected length", K(ret), K(uri), K(file_length), K(content_size));
  } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(content_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc memory", K(ret), K(content_size));
  } else if (OB_FAIL(adapter.read_single_file(uri, info, read_buf, content_size, read_size,
                                              ObStorageIdMod::get_default_id_mod()))) {
    OB_LOG(WARN, "fail to read_single_file", K(ret), K(uri), KPC(info), K(content_size));
  } else if (OB_UNLIKELY(content_size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected read size", K(ret), K(uri), K(content_size), K(read_size));
  } else if (OB_UNLIKELY(0 != MEMCMP(write_buf, read_buf, content_size))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected data", K(ret), K(uri), K(content_size), K(content_size));
  }
  return ret;
}

TEST_P(TestBackupIOAdapter, test_parallel_multipart_write)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    const char *tmp_dir = "test_parallel_multipart_write";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ObBackupIoAdapter tmp_io_adapter;
    ASSERT_EQ(OB_SUCCESS, tmp_io_adapter.mkdir(dir_uri_, &info_base_));
    
    const int64_t content_size = 20 * 1024 * 1024L + 7;
    ObArenaAllocator allocator;
    char *write_buf = (char *)allocator.alloc(content_size);
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    for (int64_t i = 0; i < content_size - 1; i++) {
      write_buf[i] = alphanum[ObRandom::rand(0, sizeof(alphanum) - 2)];
    }
    write_buf[content_size - 1] = '\0';
    
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_multipart", dir_uri_));
    const int64_t part_size_threshold = ObStorageBufferedMultiPartWriter::PART_SIZE_THRESHOLD;
    {
      OB_LOG(INFO, "=================== Test complete and aobrt ===================");
      std::vector<ObStorageAccessType> access_type_lsit = 
          {OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER, OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER};
      for (auto &access_type : access_type_lsit) {
        ObBackupIoAdapter adapter;
        ObIODevice *device_handle = nullptr;
        ObIOFd fd;
        ObIOHandle io_handle;

        // abort an empty multipart upload
        ASSERT_EQ(OB_SUCCESS,
            adapter.open_with_access_type(device_handle, fd, &info_base_, uri, access_type,
                                          ObStorageIdMod::get_default_id_mod()));
        ASSERT_EQ(OB_SUCCESS, adapter.abort(*device_handle, fd));
        ASSERT_EQ(OB_SUCCESS, adapter.close_device_and_fd(device_handle, fd));

        // complete an empty multipart upload
        ASSERT_EQ(OB_SUCCESS,
            adapter.open_with_access_type(device_handle, fd, &info_base_, uri, access_type,
                                          ObStorageIdMod::get_default_id_mod()));
        ASSERT_EQ(OB_SUCCESS, adapter.complete(*device_handle, fd));
        ASSERT_EQ(OB_SUCCESS, adapter.close_device_and_fd(device_handle, fd));
        int64_t len = -1;
        ASSERT_EQ(OB_SUCCESS, adapter.get_file_length(uri, &info_base_, len));
        ASSERT_EQ(0, len);

        // abort
        ASSERT_EQ(OB_SUCCESS,
            adapter.open_with_access_type(device_handle, fd, &info_base_, uri, access_type,
                                          ObStorageIdMod::get_default_id_mod()));
        ASSERT_EQ(OB_SUCCESS,
            adapter.async_upload_data(*device_handle, fd, write_buf, 0, part_size_threshold, io_handle));
        ASSERT_EQ(OB_SUCCESS, io_handle.wait());
        ASSERT_EQ(OB_SUCCESS, adapter.abort(*device_handle, fd));
        ASSERT_EQ(OB_SUCCESS, adapter.close_device_and_fd(device_handle, fd));

        // write only one part
        bool is_exist = false;
        ASSERT_EQ(OB_SUCCESS,
            adapter.open_with_access_type(device_handle, fd, &info_base_, uri, access_type,
                                          ObStorageIdMod::get_default_id_mod()));
        ASSERT_EQ(OB_SUCCESS,
            adapter.async_upload_data(*device_handle, fd, "a", 0, 1, io_handle));
        ASSERT_EQ(OB_SUCCESS, io_handle.wait());
        ASSERT_EQ(OB_SUCCESS, adapter.complete(*device_handle, fd));
        ASSERT_EQ(OB_SUCCESS, adapter.close_device_and_fd(device_handle, fd));
        ASSERT_EQ(OB_SUCCESS, adapter.is_exist(uri, &info_base_, is_exist));
        ASSERT_TRUE(is_exist);
        ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &info_base_));
      }
    }

    {
      OB_LOG(INFO, "=================== Test OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER ===================");
      const ObStorageAccessType access_type = OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER;
      
      ASSERT_EQ(OB_SUCCESS, random_upload(uri, &info_base_, access_type, write_buf, 1, part_size_threshold));
      ASSERT_EQ(OB_SUCCESS, check_multipart_object(uri, &info_base_, write_buf, 1));

      ASSERT_EQ(OB_SUCCESS, random_upload(uri, &info_base_, access_type, write_buf, content_size, part_size_threshold));
      ASSERT_EQ(OB_SUCCESS, check_multipart_object(uri, &info_base_, write_buf, content_size));
    }

    {
      OB_LOG(INFO, "=================== Test OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER ===================");
      const ObStorageAccessType access_type = OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER;

      ASSERT_EQ(OB_SUCCESS, random_upload(uri, &info_base_, access_type, write_buf, 1, part_size_threshold));
      ASSERT_EQ(OB_SUCCESS, check_multipart_object(uri, &info_base_, write_buf, 1));

      ASSERT_EQ(OB_SUCCESS, random_upload(uri, &info_base_, access_type, write_buf, content_size, part_size_threshold - 1));
      ASSERT_EQ(OB_SUCCESS, check_multipart_object(uri, &info_base_, write_buf, content_size));

      ASSERT_EQ(OB_SUCCESS, random_upload(uri, &info_base_, access_type, write_buf, content_size, part_size_threshold));
      ASSERT_EQ(OB_SUCCESS, check_multipart_object(uri, &info_base_, write_buf, content_size));
    }
  }
}

int create_nested_directories_and_files(
    ObIAllocator &allocator,
    char *base_dir_uri,
    const int64_t base_dir_uri_buf_len,
    const share::ObBackupStorageInfo *storage_info,
    int64_t depth, int64_t breadth,
    ObIArray<ObString> &created_files,
    const int64_t expected_file_num = -1)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  if (OB_ISNULL(base_dir_uri) || OB_ISNULL(storage_info)
      || OB_UNLIKELY(base_dir_uri_buf_len <= 0 || breadth <= 0 || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret),
        KP(base_dir_uri), KPC(storage_info), K(breadth), K(base_dir_uri_buf_len));
  } else if (expected_file_num > 0 && created_files.count() >= expected_file_num) {
    // do nothing
  } else if (depth > 0) {
    const int64_t base_dir_uri_len = strlen(base_dir_uri);
    int64_t pos = base_dir_uri_len;
    char *tmp_uri = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < breadth && !(expected_file_num > 0 && created_files.count() >= expected_file_num); i++) {
      pos = base_dir_uri_len;
      if (OB_FAIL(databuff_printf(base_dir_uri, base_dir_uri_buf_len, pos, "/file_%d", i))) {
        OB_LOG(WARN, "fail to construct file name", K(ret),
            K(base_dir_uri), K(base_dir_uri_buf_len), K(i), K(depth));
      } else if (OB_FAIL(ob_dup_cstring(allocator, base_dir_uri, tmp_uri))) {
        OB_LOG(WARN, "fail to deep copy file name", K(ret), K(base_dir_uri), K(i), K(depth));
      } else if (OB_FAIL(created_files.push_back(tmp_uri))) {
        OB_LOG(WARN, "fail to push back file", K(ret),
            K(base_dir_uri), K(i), K(depth), K(tmp_uri), K(created_files.count()));
      } else if (OB_FAIL(io_adapter.write_single_file(base_dir_uri, storage_info, "a", 1,
                                    ObStorageIdMod::get_default_id_mod()))) {
        OB_LOG(WARN, "fail to write file", K(ret), K(base_dir_uri), K(i), K(depth));
      } else if (FALSE_IT(pos = base_dir_uri_len)) {
      } else if (OB_FAIL(databuff_printf(base_dir_uri, base_dir_uri_buf_len, pos, "/dir_%d", i))) {
        OB_LOG(WARN, "fail to construct dir name", K(ret), 
            K(base_dir_uri), K(base_dir_uri_buf_len), K(i), K(depth));
      } else if (OB_FAIL(io_adapter.mkdir(base_dir_uri, storage_info))) {
        OB_LOG(WARN, "fail to mkdir", K(ret), K(base_dir_uri), K(i), K(depth));
      } else if (OB_FAIL(create_nested_directories_and_files(
            allocator, base_dir_uri, base_dir_uri_buf_len,
            storage_info, depth - 1, breadth, created_files, expected_file_num))) {
        OB_LOG(WARN, "fail to create_nested_directories_and_files",
            K(ret), K(base_dir_uri), K(i), K(depth));
      }
    }
    base_dir_uri[base_dir_uri_len] = '\0';
  }
  return ret;
}

TEST_P(TestBackupIOAdapter, test_del_dir)
{
  if (enable_test_ && GetParam().extension_ == nullptr) {
    const char *tmp_dir = "test_del_dir";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ObBackupIoAdapter io_adapter;
    ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(dir_uri_, &info_base_));

    ObArenaAllocator allocator;
    ObArray<ObString> created_files;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/base_dir", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(uri, &info_base_));
    ASSERT_EQ(OB_SUCCESS, create_nested_directories_and_files(allocator, uri, sizeof(uri),
                                                              &info_base_, 8, 2, created_files));
    ASSERT_EQ(OB_SUCCESS, io_adapter.del_dir(uri, &info_base_, true/*recursive*/));
    bool is_empty_directory = false;
    ASSERT_EQ(OB_SUCCESS, io_adapter.is_empty_directory(dir_uri_, &info_base_, is_empty_directory));
    ASSERT_TRUE(is_empty_directory);
  }
}

int check_all_deleted(
    const share::ObBackupStorageInfo *storage_info,
    const ObIArray<ObString> &files_to_delete)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  if (OB_ISNULL(storage_info) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KPC(storage_info));
  } else {
    bool is_exist = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < files_to_delete.count(); i++) {
      is_exist = false;
      if (OB_FAIL(io_adapter.is_exist(files_to_delete.at(i), storage_info, is_exist))) {
        OB_LOG(WARN, "fail to check file is exist", K(ret), K(i), K(files_to_delete.at(i)));
      } else if (OB_UNLIKELY(is_exist)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "file is not deleted", K(ret), K(i), K(files_to_delete.at(i)));
      }
    }
  }
  return ret;
}

TEST_P(TestBackupIOAdapter, test_batch_del_files)
{
  if (enable_test_ && GetParam().extension_ == nullptr) {
    const char *tmp_dir = "test_batch_del_files";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ObBackupIoAdapter io_adapter;
    ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(dir_uri_, &info_base_));

    ObArenaAllocator allocator;
    char *tmp_uri;

    {
      OB_LOG(INFO, "=================== Test empty files_to_delete ===================");
      ObArray<ObString> files_to_delete;
      ObArray<int64_t> failed_files_idx;
      ASSERT_EQ(OB_INVALID_ARGUMENT, io_adapter.batch_del_files(&info_base_, files_to_delete,
                                                                failed_files_idx));
    }
    {
      OB_LOG(INFO, "=================== Test delete not exist objs ===================");
      ObArray<ObString> files_to_delete;
      ObArray<int64_t> failed_files_idx;

      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/not_exist_object", dir_uri_));
      ASSERT_EQ(OB_SUCCESS, files_to_delete.push_back(uri));
      ASSERT_EQ(OB_SUCCESS, io_adapter.batch_del_files(&info_base_, files_to_delete,
                                                       failed_files_idx));
      ASSERT_TRUE(failed_files_idx.empty());
    }
    {
      OB_LOG(INFO, "=================== Test delete little objs ===================");
      ObArray<ObString> files_to_delete;
      ObArray<int64_t> failed_files_idx;

      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/base_dir", dir_uri_));
      ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(uri, &info_base_));
      ASSERT_EQ(OB_SUCCESS, create_nested_directories_and_files(allocator,
                                                                uri, sizeof(uri),
                                                                &info_base_, 2, 2,
                                                                files_to_delete));
      ASSERT_EQ(OB_SUCCESS, io_adapter.batch_del_files(&info_base_, files_to_delete,
                                                       failed_files_idx));
      ASSERT_TRUE(failed_files_idx.empty());
      ASSERT_EQ(OB_SUCCESS, check_all_deleted(&info_base_, files_to_delete));

      if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
        ASSERT_EQ(OB_SUCCESS, io_adapter.del_dir(uri, &info_base_, true/*recursive*/));
      }
    }
    // GCS does not support is_tagging interface
    if (info_base_.get_type() != ObStorageType::OB_STORAGE_FILE && !is_gcs_) {
      OB_LOG(INFO, "=================== Test tag objs ===================");
      ObArray<ObString> files_to_delete;
      ObArray<int64_t> failed_files_idx;

      ObBackupStorageInfo info;
      const Config &config = GetParam();
      ASSERT_EQ(OB_SUCCESS, set_storage_info(config.bucket_,
                                             config.endpoint_,
                                             config.ak_, config.sk_,
                                             config.appid_, config.region_,
                                             "delete_mode=tagging", info));

      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tag_dir", dir_uri_));
      ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(uri, &info));
      ASSERT_EQ(OB_SUCCESS, create_nested_directories_and_files(allocator,
                                                                uri, sizeof(uri),
                                                                &info, 4, 2,
                                                                files_to_delete));
      ASSERT_EQ(OB_SUCCESS, io_adapter.batch_del_files(&info, files_to_delete,
                                                       failed_files_idx));
      ASSERT_TRUE(failed_files_idx.empty());

      bool is_tagging = false;
      for (int64_t i = 0; i < files_to_delete.count(); i++) {
        is_tagging = false;
        ASSERT_EQ(OB_SUCCESS, io_adapter.is_tagging(files_to_delete.at(i), &info, is_tagging));
        ASSERT_TRUE(is_tagging);
      }

      ASSERT_EQ(OB_SUCCESS, io_adapter.del_dir(uri, &info_base_, true/*recursive*/));
      bool is_empty_directory = false;
      ASSERT_EQ(OB_SUCCESS, io_adapter.is_empty_directory(dir_uri_, &info_base_, is_empty_directory));
      ASSERT_TRUE(is_empty_directory);
    }
    {
      OB_LOG(INFO, "=================== Test delete 1000 objs ===================");
      ObArray<ObString> files_to_delete;
      ObArray<int64_t> failed_files_idx;

      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/delete_1000_dir", dir_uri_));
      ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(uri, &info_base_));
      // GCS does not support batch_del_files interface, files will be deleted by single del_file
      // interface, so the number of files is limited to reduce the time of the test
      if (is_gcs_) {
        ASSERT_EQ(OB_SUCCESS, create_nested_directories_and_files(allocator,
                                                                  uri, sizeof(uri),
                                                                  &info_base_, 7, 2,
                                                                  files_to_delete));
      } else {
        ASSERT_EQ(OB_SUCCESS, create_nested_directories_and_files(allocator,
                                                                uri, sizeof(uri),
                                                                &info_base_, 8, 5,
                                                                files_to_delete,
                                                                OB_STORAGE_DEL_MAX_NUM));
      }
      ASSERT_EQ(OB_SUCCESS, io_adapter.batch_del_files(&info_base_, files_to_delete,
                                                       failed_files_idx));
      ASSERT_TRUE(failed_files_idx.empty());
      ASSERT_EQ(OB_SUCCESS, check_all_deleted(&info_base_, files_to_delete));

      if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
        ASSERT_EQ(OB_SUCCESS, io_adapter.del_dir(uri, &info_base_, true/*recursive*/));
      }
    }
    {
      OB_LOG(INFO, "=================== Test delete multiple objs ===================");
      ObArray<ObString> files_to_delete;
      ObArray<int64_t> failed_files_idx;

      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/delete_1000_dir", dir_uri_));
      ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(uri, &info_base_));
      if (is_gcs_) {
        ASSERT_EQ(OB_SUCCESS, create_nested_directories_and_files(allocator,
                                                                  uri, sizeof(uri),
                                                                  &info_base_, 7, 2,
                                                                  files_to_delete));
      } else {
        ASSERT_EQ(OB_SUCCESS, create_nested_directories_and_files(allocator,
                                                                  uri, sizeof(uri),
                                                                  &info_base_, 8, 5,
                                                                  files_to_delete,
                                                                  OB_STORAGE_DEL_MAX_NUM * 2 + 1));
      }
      ASSERT_EQ(OB_SUCCESS, io_adapter.batch_del_files(&info_base_, files_to_delete,
                                                       failed_files_idx));
      ASSERT_TRUE(failed_files_idx.empty());
      ASSERT_EQ(OB_SUCCESS, check_all_deleted(&info_base_, files_to_delete));

      if (info_base_.get_type() == ObStorageType::OB_STORAGE_FILE) {
        ASSERT_EQ(OB_SUCCESS, io_adapter.del_dir(uri, &info_base_, true/*recursive*/));
      }
    }
  }
}

TEST_P(TestBackupIOAdapter, test_switch_access_cos_with_diff_protocol)
{
  if (enable_test_ && info_base_.get_type() == ObStorageType::OB_STORAGE_COS) {
    int ret = OB_SUCCESS;
    const char *tmp_dir = "test_access_cos_with_s3_prefix";
    const int64_t ts = ObTimeUtility::current_time();
    const int64_t OB_MAX_BUCKET_LENGTH = 256;
    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld", tmp_dir, ts));
    // access cos with cos prefix
    ObBackupIoAdapter adapter;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_cos", dir_uri_));
    ASSERT_EQ(OB_SUCCESS, adapter.write_single_file(uri, &info_base_, "test", 4, ObStorageIdMod::get_default_id_mod()));
    ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &info_base_));

    ObBackupStorageInfo info;
    const Config &config = GetParam();
    char new_bucket[OB_MAX_BUCKET_LENGTH] = {0};
    ASSERT_EQ(OB_SUCCESS, databuff_printf(new_bucket, OB_MAX_BUCKET_LENGTH, "%s%s", OB_S3_PREFIX,
            config.bucket_ + strlen(OB_COS_PREFIX)));
    ASSERT_EQ(OB_SUCCESS, set_storage_info(new_bucket, config.endpoint_, config.ak_, 
        config.sk_, config.appid_, config.region_, config.extension_, info));
    ASSERT_EQ(info.device_type_, ObStorageType::OB_STORAGE_S3);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s/%s_%ld/test_s3", 
        new_bucket, dir_name_, tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, adapter.write_single_file(uri, &info, "test", 4, ObStorageIdMod::get_default_id_mod()));
    ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &info));
  }
}

TEST_P(TestBackupIOAdapter, test_max_device_key_amount)
{
  if (enable_test_ && info_base_.get_type() != ObStorageType::OB_STORAGE_FILE) {
    int ret = OB_SUCCESS;
    const int64_t OB_MAX_BUCKET_LENGTH = 256;
    const char *tmp_dir = "test_max_device_key_amount";
    const int64_t ts = ObTimeUtility::current_time();
    int64_t pos = strlen(dir_uri_);
    ObDeviceManager &manager = ObDeviceManager::get_instance();
    ObIODevice *tmp_dev_handle = nullptr;
    ObBackupIoAdapter adapter;
    ObObjectStorageInfo info;
    const Config &config = GetParam();
    ASSERT_EQ(OB_SUCCESS,
        databuff_printf(
            uri, sizeof(uri), "%s/%s/%s_%ld/test", config.bucket_, dir_name_, tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS,set_storage_info(config.bucket_, config.endpoint_, config.ak_, config.sk_, config.appid_,
         config.region_, config.extension_, info));
    ObDeviceManager::ObDeviceInsInfo *dev_info = nullptr;
    int64_t release_count = 0;
    for (auto it = manager.device_map_.begin(); OB_SUCC(ret) && it != manager.device_map_.end(); ++it) {
      dev_info = it->second;
      if (dev_info->device_->get_ref_cnt() == 0) {
        release_count++;
      }
    }

    const int64_t exist_count = manager.get_device_cnt() > 0 ? manager.get_device_cnt() : 0;
    const int64_t max_device_count
        = ObDeviceManager::MAX_DEVICE_INSTANCE - exist_count + release_count;
    ObIODevice *device_handle[max_device_count];
    for (int i = 0; i <= max_device_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(info.access_id_, sizeof(info.access_id_), 
          "access_id=%d", i));
      if (i < max_device_count - 1) {
        ASSERT_EQ(OB_SUCCESS, manager.get_device(config.bucket_, info, 
            ObStorageIdMod::get_default_id_mod(), device_handle[i]));
      } else if (i == max_device_count - 1) {
        ASSERT_EQ(OB_SUCCESS, adapter.write_single_file(uri, &info_base_, "test", 4, ObStorageIdMod::get_default_id_mod()));
        ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &info_base_));
        ASSERT_EQ(OB_SUCCESS, manager.get_device(config.bucket_, info, 
            ObStorageIdMod::get_default_id_mod(), device_handle[i]));
      } else {
        ASSERT_EQ(OB_OUT_OF_ELEMENT, adapter.write_single_file(uri, &info_base_, "test", 4, ObStorageIdMod::get_default_id_mod()));
      }
    }
    ASSERT_EQ(ObDeviceManager::MAX_DEVICE_INSTANCE, manager.get_device_cnt());
    
    // release added devices before next test
    for (int i = 0; i < max_device_count; i++) {
      ASSERT_EQ(OB_SUCCESS, manager.release_device(device_handle[i]));
    }
  }
}
// This test is used to test the behavior that will occur if you continue to append files after switching from the cos
// protocol to the s3 protocol during the append file process when accessing cos.
TEST_P(TestBackupIOAdapter, test_switch_cos_to_s3_when_append)
{
  if (enable_test_ && info_base_.get_type() == ObStorageType::OB_STORAGE_COS) {
    const char *tmp_dir = "test_switch_cos_to_s3_when_append";
    const char *file_name = "test_append";
    const int64_t ts = ObTimeUtility::current_time();
    ObBackupIoAdapter io_adapter;

    int64_t pos = strlen(dir_uri_);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), pos, "%s_%ld",
                                          tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, io_adapter.mkdir(dir_uri_, &info_base_));
  
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s", dir_uri_, file_name));
     
    const int64_t size = 100;
    const int64_t cos_size = size/4;
    const int64_t s3_size = size - cos_size;
    char write_buf[size] = {0};
    memset(write_buf, '1', cos_size);
    memset(write_buf + cos_size, '2', s3_size);
    // append data
    // disable obdal
    if (GetParam().enable_obdal_) {
      cluster_enable_obdal_config = nullptr;
    }
    ObIODevice *cos_device_handle;
    ObIOFd cos_write_fd;
    int64_t tmp_write_size = -1;

    ASSERT_EQ(OB_SUCCESS,
        io_adapter.open_with_access_type(cos_device_handle, cos_write_fd, &info_base_, uri,
                                         ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER,
                                         ObStorageIdMod::get_default_id_mod()));
    ASSERT_EQ(OB_SUCCESS, io_adapter.pwrite(
      *cos_device_handle, cos_write_fd,
      write_buf, 0, cos_size, tmp_write_size, false/*is_can_seal*/));
    ASSERT_EQ(cos_size, tmp_write_size);
    
    // switch to s3
    // enable obdal
    if (GetParam().enable_obdal_) {
      cluster_enable_obdal_config = &ObClusterEnableObdalConfigBase::get_instance();
    }
    ObBackupStorageInfo info;
    ObIODevice *s3_device_handle;
    Aws::Http::SetCompliantRfc3986Encoding(true);
    ObIOFd s3_write_fd;
    const Config &config = GetParam();
    const int64_t OB_MAX_BUCKET_LENGTH = 256;
    char new_bucket[OB_MAX_BUCKET_LENGTH] = {0};
    ASSERT_EQ(OB_SUCCESS, databuff_printf(new_bucket, OB_MAX_BUCKET_LENGTH, "%s%s", OB_S3_PREFIX,
                                          config.bucket_ + strlen(OB_COS_PREFIX)));
    ASSERT_EQ(OB_SUCCESS, set_storage_info(new_bucket, config.endpoint_, config.ak_, 
                                          config.sk_, config.appid_, config.region_, config.extension_, info));
    ASSERT_EQ(info.device_type_, ObStorageType::OB_STORAGE_S3);
    ASSERT_EQ(OB_SUCCESS, info.get_storage_info_str(uri, sizeof(uri)));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri_, sizeof(dir_uri_), "%s/%s/%s_%ld/", 
                                          new_bucket, dir_name_, tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s%s", dir_uri_, file_name));
    // Validate the behavior after switch
    // Append: After switching, the file part using s3 append actually simulates writing small
    // files.
    ASSERT_EQ(OB_SUCCESS,
        io_adapter.open_with_access_type(s3_device_handle, s3_write_fd, &info, uri,
                                         ObStorageAccessType::OB_STORAGE_ACCESS_APPENDER,
                                         ObStorageIdMod::get_default_id_mod()));
    ASSERT_EQ(OB_SUCCESS, io_adapter.pwrite(*s3_device_handle, s3_write_fd, write_buf, 
                                            0, s3_size, tmp_write_size, false/*is_can_seal*/));
    ASSERT_EQ(s3_size, tmp_write_size);

    // Read: After switching, files using s3 append will be ignored when reading, and only the part
    // appended using cos will be read.
    char read_buf[size] = {0};
    int64_t tmp_read_size = -1;
    ASSERT_EQ(OB_SUCCESS, 
        io_adapter.adaptively_read_single_file(uri, &info, read_buf, size, tmp_read_size,
                                               ObStorageIdMod::get_default_id_mod()));
    ASSERT_EQ(cos_size, tmp_read_size);

    // List: After switching, files using cos and s3 append will be listed.
    TestObjectStorageListOp list_adaptive_op;
    TestObjectStorageListOp list_op;
    ASSERT_EQ(OB_SUCCESS, io_adapter.adaptively_list_files(dir_uri_, &info, list_adaptive_op));
    
    ASSERT_EQ(OB_SUCCESS, io_adapter.list_files(dir_uri_, &info, list_op));
    for (auto &element : list_adaptive_op.object_names_) {
      ASSERT_EQ(file_name, element);
    }
    ASSERT_EQ(2, list_adaptive_op.object_list_.size());
    ASSERT_EQ(3, list_op.object_list_.size());
  }
}

std::vector<Config> all_configs;
INSTANTIATE_TEST_CASE_P(
  ConfigCombinations,
  TestBackupIOAdapter,
  ::testing::ValuesIn(all_configs),
  ObObjectStorageUnittestUtil::custom_test_name
);

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "S3",                               /*storage_type*/
      oceanbase::unittest::S3_BUCKET,     /*bucket*/
      oceanbase::unittest::S3_ENDPOINT,   /*enpoint*/
      oceanbase::unittest::S3_AK,         /*ak*/
      oceanbase::unittest::S3_SK,         /*sk*/
      oceanbase::unittest::S3_REGION,     /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr, "checksum_type=md5", "checksum_type=crc32"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "S3",                               /*storage_type*/
      oceanbase::unittest::S3_BUCKET,     /*bucket*/
      oceanbase::unittest::S3_ENDPOINT,   /*enpoint*/
      oceanbase::unittest::S3_AK,         /*ak*/
      oceanbase::unittest::S3_SK,         /*sk*/
      oceanbase::unittest::S3_REGION,     /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
      nullptr,                            /*role_arn*/
      nullptr,                            /*external_id*/
      true,                               /*enable_obdal*/
    },
    {nullptr, "checksum_type=md5", "checksum_type=crc32"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "OBS",                              /*storage_type*/
      oceanbase::unittest::OBS_BUCKET,    /*bucket*/
      oceanbase::unittest::OBS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::OBS_AK,        /*ak*/
      oceanbase::unittest::OBS_SK,        /*sk*/
      oceanbase::unittest::OBS_REGION,    /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "OBS",                              /*storage_type*/
      oceanbase::unittest::OBS_BUCKET,    /*bucket*/
      oceanbase::unittest::OBS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::OBS_AK,        /*ak*/
      oceanbase::unittest::OBS_SK,        /*sk*/
      oceanbase::unittest::OBS_REGION,    /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
      nullptr,                            /*role_arn*/
      nullptr,                            /*external_id*/
      true,                               /*enable_obdal*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "OSS",                              /*storage_type*/
      oceanbase::unittest::OSS_BUCKET,    /*bucket*/
      oceanbase::unittest::OSS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::OSS_AK,        /*ak*/
      oceanbase::unittest::OSS_SK,        /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "OSS",                              /*storage_type*/
      oceanbase::unittest::OSS_BUCKET,    /*bucket*/
      oceanbase::unittest::OSS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::OSS_AK,        /*ak*/
      oceanbase::unittest::OSS_SK,        /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
      nullptr,                            /*role_arn*/
      nullptr,                            /*external_id*/
      true,                               /*enable_obdal*/
    },
    {nullptr, "checksum_type=md5",}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "COS",                              /*storage_type*/
      oceanbase::unittest::COS_BUCKET,    /*bucket*/
      oceanbase::unittest::COS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::COS_AK,        /*ak*/
      oceanbase::unittest::COS_SK,        /*sk*/
      nullptr,                            /*region*/
      oceanbase::unittest::COS_APPID,     /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "COS",                              /*storage_type*/
      oceanbase::unittest::COS_BUCKET,    /*bucket*/
      oceanbase::unittest::COS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::COS_AK,        /*ak*/
      oceanbase::unittest::COS_SK,        /*sk*/
      nullptr,                            /*region*/
      oceanbase::unittest::COS_APPID,     /*appid*/
      nullptr,                            /*entension*/
      nullptr,                            /*role_arn*/
      nullptr,                            /*external_id*/
      true,                               /*enable_obdal*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "GCS",                              /*storage_type*/
      oceanbase::unittest::GCS_BUCKET,    /*bucket*/
      oceanbase::unittest::GCS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::GCS_AK,        /*ak*/
      oceanbase::unittest::GCS_SK,        /*sk*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "GCS",                              /*storage_type*/
      oceanbase::unittest::GCS_BUCKET,    /*bucket*/
      oceanbase::unittest::GCS_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::GCS_AK,        /*ak*/
      oceanbase::unittest::GCS_SK,        /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
      nullptr,                            /*role_arn*/
      nullptr,                            /*external_id*/
      true,                               /*enable_obdal*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "AZBLOB",                              /*storage_type*/
      oceanbase::unittest::AZBLOB_BUCKET,    /*bucket*/
      oceanbase::unittest::AZBLOB_ENDPOINT,  /*enpoint*/
      oceanbase::unittest::AZBLOB_AK,        /*ak*/
      oceanbase::unittest::AZBLOB_SK,        /*sk*/
    },
    {nullptr, "checksum_type=md5"}
  );
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "FILE",                             /*storage_type*/
      oceanbase::common::OB_FILE_PREFIX,  /*bucket*/
      nullptr,                            /*enpoint*/
      nullptr,                            /*ak*/
      nullptr,                            /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr}
  );

  system("rm -f ./test_backup_io_adapter.log*");
  OB_LOGGER.set_file_name("test_backup_io_adapter.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
