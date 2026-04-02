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
#include "oss_unitest_common.h"
#define private public
#include "logservice/ob_log_external_storage_handler.h"
#include "logservice/ob_log_external_storage_utils.h"
#include "share/io/ob_io_manager.h"
#undef private
#include "share/ob_device_manager.h"
namespace oceanbase
{
using namespace logservice;
using namespace share;
namespace unittest
{
ObLogUnittestOssInfo ObLogUnittestOssCommon::oss_info_;
class TestLogOss : public ObLogUnittestOssCommon {
public:
  TestLogOss(){}
  ~TestLogOss(){}
  static void SetUpTestCase()
  {
    ObLogUnittestOssCommon::init_oss("test_log_oss_dir");
    const int64_t test_memory = 6 * 1024 * 1024;
    ObTenantBase *tenant_base = new ObTenantBase(OB_SYS_TENANT_ID);
    auto malloc = ObMallocAllocator::get_instance();
    if (NULL == malloc->get_tenant_ctx_allocator(OB_SYS_TENANT_ID, 0)) {
      malloc->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
    }
    tenant_base->init();
    ObTenantEnv::set_tenant(tenant_base);
    ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init(test_memory));
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
    ObTenantIOManager *io_service = nullptr;
    EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
    EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
    EXPECT_EQ(OB_SUCCESS, io_service->start());
    tenant_base->set(io_service);
    ObTenantEnv::set_tenant(tenant_base);
  }
  static void TearDownTestCase()
  {
    ObIOManager::get_instance().stop();
    ObIOManager::get_instance().destroy();
  }
  void SetUp() final{}
  void TearDown() final{}
  int delete_oss_object(const common::ObString &uri,
                     const ObString &oss_info)
  {
    int ret = OB_SUCCESS;
    common::ObBackupIoAdapter io_adapter;
    share::ObBackupStorageInfo storage_info;
    if (OB_FAIL(io_adapter.del_file(uri, &storage_info))) {
      CLOG_LOG(WARN, "del dir failed");
    } else {}
    return ret;
  }
  
  int generate_oss_data(const common::ObString &uri,
                        const ObString &oss_info,
                        const char *buf,
                        const int64_t size)
  {
  
    common::ObBackupIoAdapter io_adapter;
    share::ObBackupStorageInfo storage_info;
    int ret = OB_SUCCESS;
    bool exist = false;
    if (OB_FAIL(io_adapter.is_exist(uri, &storage_info, exist))) {
      CLOG_LOG(WARN, "is_exist failed");
    } else if (!exist && OB_FAIL(io_adapter.write_single_file(uri, &storage_info, buf, size,
                                            ObStorageIdMod::get_default_id_mod()))) {
      CLOG_LOG(WARN, "ObBackupStorageInfo write_single_file failed");
    } else {
    }
    return ret;
  }

};

TEST_F(TestLogOss, hello_world)
{
  system("rm -rf hello_world.log*");
  OB_LOGGER.set_file_name("hello_world.log", true);
  common::ObObjectStorageInfo *info = MTL_NEW(ObObjectStorageInfo, "unittest");
  EXPECT_EQ(OB_SUCCESS, get_object_storage_info(*info));
  char oss_path_cstr[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {'\0'};
  EXPECT_EQ(OB_SUCCESS, info->get_storage_info_str(oss_path_cstr, sizeof(oss_path_cstr)));
  ObString oss_path(oss_path_cstr);
  std::string base_uri = get_root_path();
  std::string uri = base_uri + "xxx123hello_world_test123";
  logservice::ObLogExternalStorageHandler adapter;
  char read_buf[4096];
  int64_t out_read_size = 0;
  EXPECT_EQ(OB_SUCCESS, adapter.init());
  EXPECT_EQ(OB_SUCCESS, adapter.start(1));
  palf::LogIOContext io_ctx(palf::LogIOUser::DEFAULT);
  EXPECT_EQ(OB_OBJECT_NOT_EXIST,
            adapter.pread(uri.c_str(), oss_path, OB_INVALID_ID, 0, read_buf, sizeof(read_buf), out_read_size, io_ctx));
}
// test real oss object
TEST_F(TestLogOss, test_log_ext_handler_pread)
{
  system("rm -rf test_log_ext_handler_pread.log*");
  OB_LOGGER.set_file_name("test_log_ext_handler_pread.log", true);
  // Verify oss accessibility
  // Need to ensure that the following OSS can be continuously accessed
  {
    CLOG_LOG(INFO, "test oss can access");
    int ret = OB_SUCCESS;
    logservice::ObLogExternalStorageHandler adapter;
    EXPECT_EQ(OB_SUCCESS, adapter.init());
    EXPECT_EQ(OB_SUCCESS, adapter.start(1));
    std::vector<std::string> uris;
    std::vector<int64_t> object_checksums;
    std::string base_uri = get_root_path();
    common::ObObjectStorageInfo *info = MTL_NEW(ObObjectStorageInfo, "unittest");
    EXPECT_EQ(OB_SUCCESS, get_object_storage_info(*info));
    char oss_path_cstr[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {'\0'};
    EXPECT_EQ(OB_SUCCESS, info->get_storage_info_str(oss_path_cstr, sizeof(oss_path_cstr)));
    ObString oss_path(oss_path_cstr);
    const int64_t total_oss_object = 5;
    const uint64_t storage_id = OB_INVALID_ID;
    palf::LogIOContext io_ctx(palf::LogIOUser::DEFAULT);
    // There are 5 clog files in the test directory
    for (int i = 0; i < total_oss_object; i++) {
      uris.push_back(base_uri+std::to_string(i));
    }
    int exist_num = 0;
    const int64_t buf_len = 64*1024*1024;
    char *buf_self_pread = reinterpret_cast<char*>(ob_malloc(buf_len, "unittest"));
    memset(buf_self_pread, 'x', buf_len);
    // OSS address may change, when OSS is accessible, perform the following tests
    for (int i = 0; i < total_oss_object; i++) {
      bool exist = false;
      int64_t real_read_size = 0;
      if (OB_FAIL(generate_oss_data(uris[i].c_str(), oss_path,  buf_self_pread, buf_len))) {
        CLOG_LOG(ERROR, "oss can not access", K(uris[i].c_str()), K(exist));
      } else {
        int64_t checksum = ob_crc64(buf_self_pread, buf_len);
        object_checksums.push_back(checksum);
        exist_num++;
        memset(buf_self_pread, 'y', buf_len);
      }
    }
    if (exist_num == total_oss_object) 
    {
      int64_t real_read_size = 0;
      // The read offer is at the end of the file, read length is 0
      EXPECT_EQ(OB_SUCCESS, adapter.pread(uris[0].c_str(), oss_path, storage_id, buf_len, buf_self_pread, buf_len, real_read_size, io_ctx));
      EXPECT_EQ(0, real_read_size);
      CLOG_LOG(INFO, "read outof upper bound", K(ret), K(real_read_size));
      // The read offer is the end of file -1000, read length is 0
      EXPECT_EQ(OB_SUCCESS, adapter.pread(uris[0].c_str(), oss_path, storage_id, buf_len-1000, buf_self_pread, buf_len, real_read_size, io_ctx));
      EXPECT_EQ(1000, real_read_size);
      // The read offer is at the file end - 4*1000*1000, read length is 0
      EXPECT_EQ(OB_SUCCESS, adapter.pread(uris[0].c_str(), oss_path, storage_id, buf_len-1000*1000*4, buf_self_pread, buf_len, real_read_size, io_ctx));
      EXPECT_EQ(1000*1000*4, real_read_size);
      // The read offer is the end of file + 1000, read length is 0, return invalid argument
      // The underlying read interface will determine if the read offset exceeds the file length, and report an error invalid argument
      // OSS feature: if the specified offset exceeds the file length or offset + the length to be read is greater than the file length, it will return the entire file's data content without error
      EXPECT_EQ(OB_FILE_LENGTH_INVALID, adapter.pread(uris[0].c_str(), oss_path, storage_id, buf_len+1000, buf_self_pread, buf_len, real_read_size, io_ctx));
      // The read offer is the end of file + 1000 * 1000 * 4, read length is 0, return invalid argument
      EXPECT_EQ(OB_FILE_LENGTH_INVALID, adapter.pread(uris[0].c_str(), oss_path, storage_id, buf_len+1000*1000*4, buf_self_pread, buf_len, real_read_size, io_ctx));
      CLOG_LOG(INFO, "read outof upper bound", K(ret), K(real_read_size));
    }
    if (exist_num == total_oss_object) 
    // Validate performance
    {
      EXPECT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
      ObLogExternalStorageHandler handler;
      const int64_t concurrency = 16;
      EXPECT_EQ(OB_SUCCESS, handler.init());
      EXPECT_EQ(OB_SUCCESS, handler.start(concurrency));
      int64_t start_ts = ObTimeUtility::current_time();
      int64_t real_read_size = 0;
      for (int i = 0; i < total_oss_object; i++) {
        EXPECT_EQ(OB_SUCCESS, handler.pread(
          uris[i].c_str(), oss_path, storage_id, 0, buf_self_pread, buf_len, real_read_size, io_ctx
        ));
        EXPECT_EQ(real_read_size, buf_len);
      }
      int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      int64_t bw = total_oss_object * buf_len / cost_ts;
      CLOG_LOG(INFO, "band width", K(bw), K(cost_ts));
    }
    // Verify the correctness of the read
    if (exist_num == total_oss_object)
    {
      ObLogExternalStorageHandler handler;
      const int64_t concurrency = 16;
      EXPECT_EQ(OB_SUCCESS, handler.init());
      EXPECT_EQ(OB_SUCCESS, handler.start(concurrency));
      ASSERT_NE(nullptr, buf_self_pread);
      for (int i = 0; i < total_oss_object; i++) {
        int64_t real_read_size = 0;
        memset(buf_self_pread, '0', buf_len);
        EXPECT_EQ(OB_SUCCESS, handler.pread(
          uris[i].c_str(), oss_path, storage_id, 0, buf_self_pread, buf_len, real_read_size, io_ctx
        ));
        EXPECT_EQ(buf_len, real_read_size);
        int64_t checksum = ob_crc64(buf_self_pread, buf_len);
        EXPECT_EQ(checksum, object_checksums[i]);
      }
      if (NULL != buf_self_pread) {
        ob_free(buf_self_pread);
        buf_self_pread = NULL;
      }
      // Random position read
      {
        auto read_func = [&]() {
          for (int i = 0; i < 32; i++) {
            srandom(ObTimeUtility::current_time());
            const int64_t object_idx = random() % total_oss_object;
            const int64_t file_size = palf::PALF_PHY_BLOCK_SIZE;
            int64_t tmp_read_offset = random() %  file_size;
            const int64_t tmp_read_size = random() % file_size;
            char *tmp_read_buf1 = reinterpret_cast<char*>(ob_malloc(tmp_read_size, "unittest"));
            char *tmp_read_buf2 = reinterpret_cast<char*>(ob_malloc(tmp_read_size, "unittest"));
            int64_t tmp_real_read_size = 0;
            ASSERT_NE(nullptr, tmp_read_buf1);
            ASSERT_NE(nullptr, tmp_read_buf2);
            EXPECT_EQ(OB_SUCCESS, handler.pread(
              uris[object_idx].c_str(), oss_path, storage_id, tmp_read_offset, tmp_read_buf1, tmp_read_size, tmp_real_read_size, io_ctx
            ));
            EXPECT_EQ(std::min(file_size - tmp_read_offset, tmp_read_size),
                      tmp_real_read_size);
            tmp_real_read_size = 0;
            EXPECT_EQ(OB_SUCCESS, adapter.pread(
              uris[object_idx].c_str(), oss_path, storage_id, tmp_read_offset, tmp_read_buf2, tmp_read_size, tmp_real_read_size, io_ctx
            ));
            EXPECT_EQ(std::min(file_size - tmp_read_offset, tmp_read_size),
                      tmp_real_read_size);
            EXPECT_EQ(0, memcmp(tmp_read_buf1, tmp_read_buf2, tmp_real_read_size));
            if (NULL != tmp_read_buf1) {
              ob_free(tmp_read_buf1);
              tmp_read_buf1 = NULL;
            }
            if (NULL != tmp_read_buf2) {
              ob_free(tmp_read_buf2);
              tmp_read_buf2 = NULL;
            }
          }
        };
        std::vector<std::thread> tmp_pread_threads;
        for (int i = 0; i < 4; i++) {
          tmp_pread_threads.emplace_back(read_func);
        }
        auto tmp_resize_func = [&](){
          for (int i = 0; i < 4; i++) {
            srandom(ObTimeUtility::current_time());
            handler.resize(random()%4);
            usleep(1000 * 1000);
          }
        };
        std::thread tmp_resize_thread(tmp_resize_func);
        for (int i = 0; i < 4; i++) {
          tmp_pread_threads[i].join();
        }
        tmp_resize_thread.join();
      }
      for (auto uri : uris) {
        delete_oss_object(uri.c_str(), oss_path);
      }
      CLOG_LOG(INFO, "delete success");
    } else {
      CLOG_LOG(ERROR, "the object of oss is not correct", K(exist_num));
    }
  }
}

TEST_F(TestLogOss, test_log_ext_handler_upload)
{
  system("rm -rf test_log_ext_handler_upload.log*");
  OB_LOGGER.set_file_name("test_log_ext_handler_upload.log", true);
  // Verify oss accessibility
  // Need to ensure that the following OSS can be continuously accessed
  {
    CLOG_LOG(INFO, "test oss can access");
    int ret = OB_SUCCESS;
    logservice::ObLogExternalStorageHandler adapter;
    EXPECT_EQ(OB_SUCCESS, adapter.init());
    EXPECT_EQ(OB_SUCCESS, adapter.start(4));
    std::vector<std::string> uris;
    std::vector<int64_t> object_checksums;
    std::string base_uri = get_root_path();
    common::ObObjectStorageInfo *info = MTL_NEW(ObObjectStorageInfo, "unittest");
    EXPECT_EQ(OB_SUCCESS, get_object_storage_info(*info));
    char oss_path_cstr[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {'\0'};
    EXPECT_EQ(OB_SUCCESS, info->get_storage_info_str(oss_path_cstr, sizeof(oss_path_cstr)));
    ObString oss_path(oss_path_cstr);
    uint64_t tenant_id = 1002;
    ObLSID ls_id(1001);
    palf::block_id_t block_id = 0;
    const int64_t part_size = 1024 * 1024;
    const int64_t part_count = 4;
    ObLogExternalStorageCtx run_ctx;
    char *write_buf = reinterpret_cast<char *>(ob_malloc(part_size, "test"));
    EXPECT_EQ(OB_SUCCESS, adapter.init_multi_upload(tenant_id, ls_id.id(), block_id, part_size, run_ctx));
    for (int i = 0; i < part_count && OB_SUCC(ret); i++) {
      ObLogExternalStorageCtxItem *item = NULL;
      if (OB_FAIL(run_ctx.get_item(i, item))) {
        CLOG_LOG(WARN, "get_item failed", KR(ret), K(i), KP(item));
      } else if (OB_FAIL(adapter.upload_one_part(write_buf, part_size, part_size*i, i, run_ctx))) {
        CLOG_LOG(WARN, "upload_one_part failed", KR(ret), K(i), KP(item));
      } else {}
    }
    EXPECT_EQ(OB_SUCCESS, ret);
    int64_t out_pwrite_size = 0;
    EXPECT_EQ(OB_SUCCESS, run_ctx.wait(out_pwrite_size));
    EXPECT_EQ(out_pwrite_size, 4*part_size);
    if (OB_SUCC(ret)) {
      EXPECT_EQ(OB_SUCCESS, adapter.complete_multi_upload(run_ctx));
    } else {
      EXPECT_EQ(OB_SUCCESS, adapter.abort_multi_upload(run_ctx));
    }
  }
}
} // end namaspace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_log_oss.log*");
  OB_LOGGER.set_file_name("test_log_oss.log", true);
  OB_LOGGER.set_log_level("TRACE");
  srandom(ObTimeUtility::current_time());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
