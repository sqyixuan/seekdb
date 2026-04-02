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
#include "logservice/palf/log_block_header.h"
#include "share/io/ob_io_manager.h"
#ifdef OB_BUILD_SHARED_STORAGE
#endif
#undef private
#include "share/ob_device_manager.h"
namespace oceanbase
{
using namespace logservice;
using namespace share;
using namespace palf;
using namespace storage;
namespace unittest
{
const int64_t default_block_size = 4 * 1024;
ObLogExternalStorageHandler GLOBAL_EXT_HANDLER;
const int64_t MAX_BLOCK_COUNT= 10;
ObLogUnittestOssInfo ObLogUnittestOssCommon::oss_info_;
class TestSharedLogUtils : public ObLogUnittestOssCommon {
public:
  TestSharedLogUtils(){
  }
  ~TestSharedLogUtils(){}
  int upload_blocks(const uint64_t tenant_id,
                    const int64_t palf_id,
                    const palf::block_id_t start_block_id,
                    const std::vector<SCN> &scns)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(start_block_id)
        || scns.empty()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(start_block_id));
    } else {
      ret = upload_blocks_impl_(tenant_id, palf_id, start_block_id, scns);
    }
    return ret;
  }

  int upload_blocks(const uint64_t tenant_id,
                    const int64_t palf_id,
                    const palf::block_id_t start_block_id,
                    const int64_t block_count)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(start_block_id)
        || 0 >= block_count) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(start_block_id), K(block_count));
    } else {
      std::vector<SCN> scns(block_count, SCN::min_scn());
      int64_t start_ts = ObTimeUtility::current_time();
      (void)srand(start_ts);
      int64_t hour = 60 * 60;
      for (auto &scn : scns) {
        scn.convert_from_ts(start_ts);
        int64_t interval = random()%hour + 1;
        start_ts += interval;
      }
      ret = upload_blocks_impl_(tenant_id, palf_id, start_block_id, scns);
    }
    return ret;
  }

  int upload_block(const uint64_t tenant_id,
                   const int64_t palf_id,
                   const palf::block_id_t block_id,
                   const SCN &scn)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(block_id)
        || !scn.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(block_id), K(scn));
    } else {
      ret = upload_block_impl_(tenant_id, palf_id, block_id, scn);
    }
    return ret;
  }
  static void SetUpTestCase()
  {
    ObLogUnittestOssCommon::init_oss("test_shared_log_utils_dir");
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
private:
  int upload_blocks_impl_(const uint64_t tenant_id,
                          const int64_t palf_id,
                          const palf::block_id_t start_block_id,
                          const std::vector<SCN> &scns)
  {
    int ret = OB_SUCCESS;
    const int64_t block_count = scns.size();
    for (int64_t index = 0; index < block_count && OB_SUCC(ret); index++) {
      ret = upload_block_impl_(tenant_id, palf_id, start_block_id + index, scns[index]);
    }
    return ret;
  }
  virtual int upload_block_impl_(const uint64_t tenant_id,
                                 const int64_t palf_id,
                                 const palf::block_id_t block_id,
                                 const SCN &scn)
  {
    int ret = OB_SUCCESS;
    LogBlockHeader log_block_header;
    char *write_buf = reinterpret_cast<char*>(mtl_malloc(default_block_size, "test"));
    if (NULL == write_buf){
      return OB_ALLOCATE_MEMORY_FAILED;
    }
    memset(write_buf, 'c', default_block_size);
    LSN lsn(block_id*default_block_size);
    int64_t pos = 0;
    log_block_header.update_lsn_and_scn(lsn, scn);
    log_block_header.calc_checksum();
    if (OB_FAIL(log_block_header.serialize(write_buf, MAX_INFO_BLOCK_SIZE, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K(pos));
    } else if (OB_FAIL(GLOBAL_EXT_HANDLER.upload(tenant_id, palf_id, block_id, write_buf, default_block_size))) {
      CLOG_LOG(ERROR, "upload failed", K(tenant_id), K(palf_id), K(block_id));
    } else {
      CLOG_LOG(INFO, "upload_block success", K(tenant_id), K(palf_id), K(block_id), K(log_block_header));
    }
    if (NULL != write_buf) {
      mtl_free(write_buf);
    }
    return ret;
  }
};

TEST_F(TestSharedLogUtils, basic_interface)
{
  ASSERT_EQ(OB_SUCCESS, GLOBAL_EXT_HANDLER.init());
  ASSERT_EQ(OB_SUCCESS, GLOBAL_EXT_HANDLER.start(0));
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  ObSharedLogUtils::delete_tenant(tenant_id);
  // Precondition preparation
  // Generate 10 files, with the smallest block_id being 10
  std::vector<SCN> scns(10, SCN::min_scn());
  int64_t start_ts = ObTimeUtility::current_time();
  for (auto &scn : scns) {
    scn.convert_from_ts(start_ts++);
  }
  const block_id_t start_block_id = 10;
  const block_id_t end_block_id = 19;
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id, scns));
  
  uint64_t invalid_tenant_id = OB_INVALID_TENANT_ID;  
  uint64_t valid_tenant_id = 1;
  ObLSID invalid_ls_id;
  ObLSID valid_ls_id(1);
  block_id_t invalid_start_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_start_block_id = 1;
  block_id_t invalid_end_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_end_block_id = 1;
  block_id_t invalid_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_block_id = 1;
  char *invalid_uri = NULL;
  const int64_t invalid_uri_len = 0;
  char valid_uri[OB_MAX_URI_LENGTH] = {'\0'};
  const int64_t valid_uri_len = OB_MAX_URI_LENGTH;
  // case1: validate ObSharedLogUtils::get_oldest_block
  {
    CLOG_LOG(INFO, "begin case1 invalid argument");
    block_id_t oldest_block_id = LOG_INVALID_BLOCK_ID;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      invalid_tenant_id, ObLSID(invalid_ls_id), oldest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      valid_tenant_id, ObLSID(invalid_ls_id), oldest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      invalid_tenant_id, ObLSID(valid_ls_id), oldest_block_id));
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, oldest_block_id);

    CLOG_LOG(INFO, "begin case1 success");
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, ObLSID(ls_id), oldest_block_id));
    EXPECT_EQ(start_block_id, oldest_block_id);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR
    
    CLOG_LOG(INFO, "begin case1 not exist");
    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    ObLSID no_block_ls_id(500);
    // Log stream does not exist
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      not_exist_tenant_id, ls_id, oldest_block_id));
    // Log stream exists
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      tenant_id, no_block_ls_id, oldest_block_id));
  }
  // case2: validate get_newest_block
  {
    CLOG_LOG(INFO, "begin case2 invalid argument");
    block_id_t newest_block_id = LOG_INVALID_BLOCK_ID;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, invalid_ls_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      valid_tenant_id, invalid_ls_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, valid_ls_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, invalid_ls_id, valid_start_block_id, newest_block_id));
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, newest_block_id);
    {
      const block_id_t start_block_id = 0;
      const block_id_t cur_end_block_id = 0;
      EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id, 1));
      block_id_t tmp_start_block_id = 0;
      block_id_t newest_block_id = LOG_INVALID_BLOCK_ID;
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
        tenant_id, ls_id, tmp_start_block_id, newest_block_id));
      // TODO @runling: check the newest_block_id
      EXPECT_EQ(newest_block_id, end_block_id);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tenant_id, ls_id, start_block_id, cur_end_block_id+1));
    }
    CLOG_LOG(INFO, "begin case2 success");
    // OB_SUCCESS
    block_id_t tmp_start_block_id = 0;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    // Starting point file is the smallest file on oss
    tmp_start_block_id = start_block_id;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    tmp_start_block_id = start_block_id+1;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    tmp_start_block_id = start_block_id+2;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    tmp_start_block_id = start_block_id+3;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    // File has holes
    // 10 11 12 13...19  24 25 26
    {
      const block_id_t start_block_id_hole = 24;
      const block_id_t end_block_id_hole = 26;
      EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id_hole, 3));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
        tenant_id, ls_id, tmp_start_block_id, newest_block_id));
      EXPECT_EQ(newest_block_id, end_block_id_hole);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
        tenant_id, ls_id, start_block_id_hole, end_block_id_hole+1));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
        tenant_id, ls_id, tmp_start_block_id, newest_block_id));
      EXPECT_EQ(newest_block_id, end_block_id);
    }

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR
    
    CLOG_LOG(INFO, "begin case2 not exist");

    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    ObLSID no_block_ls_id(500);
    // Log stream does not exist
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_newest_block(
      not_exist_tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    // empty log stream
    tmp_start_block_id = LOG_INITIAL_BLOCK_ID;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_newest_block(
      tenant_id, no_block_ls_id, tmp_start_block_id, newest_block_id));
    // Starting file does not exist on oss
    tmp_start_block_id = end_block_id+1;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
  }
  // case3: validate locate_by_scn_corasely
  // case4: validate ObSharedLogUtils::get_block_min_scn
  {
    CLOG_LOG(INFO, "begin case4 invalid argument");
    SCN block_min_scn = SCN::min_scn();
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, invalid_ls_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      valid_tenant_id, invalid_ls_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, valid_ls_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, invalid_ls_id, valid_block_id, block_min_scn));

    CLOG_LOG(INFO, "begin case4 success");
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_block_min_scn(
      tenant_id, ls_id, start_block_id, block_min_scn));
    EXPECT_EQ(block_min_scn, scns[0]);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR
    
    CLOG_LOG(INFO, "begin case4 entry not exist");
    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    block_id_t not_exist_block_id = start_block_id + 100000;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_block_min_scn(
      tenant_id, ls_id, not_exist_block_id, block_min_scn));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_block_min_scn(
      not_exist_tenant_id, ls_id, start_block_id, block_min_scn));
  }
  // case5: validate ObSharedLogUtils::delete_blocks
  {
    CLOG_LOG(INFO, "begin case5");
    // OB_INVLAID_ARGUMENT
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_ls_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      valid_tenant_id, invalid_ls_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, valid_ls_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_ls_id, valid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_ls_id, invalid_start_block_id, valid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      valid_tenant_id, valid_ls_id, start_block_id, start_block_id));

    block_id_t oldest_block_id;
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, ls_id, start_block_id, start_block_id+2));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, ls_id, oldest_block_id));
    EXPECT_EQ(oldest_block_id, start_block_id+2);
    // Generate MAX_BLOCK_COUNT files
    {
      uint64_t tmp_tenant_id = 1004;
      ObLSID tmp_ls_id(1003);
      block_id_t tmp_start_block = 3;
      block_id_t tmp_end_block = tmp_start_block + MAX_BLOCK_COUNT;
      EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_ls_id.id(), tmp_start_block, MAX_BLOCK_COUNT));
      block_id_t tmp_oldest_block = 0;
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
        tmp_tenant_id, tmp_ls_id, tmp_oldest_block));
      EXPECT_EQ(tmp_start_block, tmp_oldest_block);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tmp_tenant_id, tmp_ls_id,
                                                            tmp_start_block, tmp_start_block+MAX_BLOCK_COUNT-5));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
        tmp_tenant_id, tmp_ls_id, tmp_oldest_block));
      EXPECT_EQ(tmp_start_block+MAX_BLOCK_COUNT - 5, tmp_oldest_block);
      bool tmp_palf_exist = false; 
      bool tmp_tenant_exist = false; 
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tmp_tenant_id, tmp_ls_id,
                                                            tmp_start_block, tmp_start_block+MAX_BLOCK_COUNT));
      CLOG_LOG(INFO, "runlin trace delete_palf", K(tmp_tenant_id), K(tmp_ls_id));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
        tmp_tenant_id, tmp_ls_id, tmp_palf_exist));
      EXPECT_EQ(false, tmp_palf_exist);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
        tmp_tenant_id, tmp_tenant_exist));
      // Although the tenant directory exists, there is no concept of directory on oss, therefore the tenant does not exist
      EXPECT_EQ(false, tmp_tenant_exist);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
        tmp_tenant_id, tmp_tenant_exist));
      EXPECT_EQ(false, tmp_tenant_exist);
    }
    // Ensure the same number of files still exist on OSS
    std::vector<SCN> tmp_scns{scns[0], scns[1]};
    EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id, tmp_scns));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, ls_id, oldest_block_id));
    EXPECT_EQ(oldest_block_id, start_block_id);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR
    // Delete non-existent file
    uint64_t not_exist_tenant_id = 500;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, ls_id, start_block_id+10000, start_block_id+10010));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      not_exist_tenant_id, ls_id, start_block_id, start_block_id+3));
  }
  // case6: validate ObSharedLogUtils::construct_external_storage_access_info
  {
    CLOG_LOG(INFO, "begin case6");
    ObBackupDest dest;
    uint64_t storage_id;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_ls_id, invalid_block_id, invalid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      valid_tenant_id, invalid_ls_id, invalid_block_id, invalid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, valid_ls_id, invalid_block_id, invalid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_ls_id, valid_block_id, invalid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_ls_id, invalid_block_id, valid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_ls_id, invalid_block_id, invalid_uri, valid_uri_len, dest, storage_id));

    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::construct_external_storage_access_info(
      tenant_id, ls_id, valid_block_id, valid_uri, valid_uri_len, dest, storage_id));
    EXPECT_EQ(0, STRNCMP(valid_uri, OB_OSS_PREFIX, strlen(OB_OSS_PREFIX)));
  }
  // case7: validate ObSharedLogUtils::delete_tenant and delete_palf
  // case7.1 validate ObSharedLogUtils::check_ls_exist
  // case7.2 validate ObSharedLogUtils::check_tenant_exist
  {
    CLOG_LOG(INFO, "begin case7");
    bool palf_exist = false;
    bool tenant_exist = false;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_tenant(
      invalid_tenant_id));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_ls(
      valid_tenant_id, invalid_ls_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_ls(
      invalid_tenant_id, valid_ls_id));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_tenant_exist(
      invalid_tenant_id, tenant_exist));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_ls_exist(
      invalid_tenant_id, invalid_ls_id, palf_exist));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_ls_exist(
      valid_tenant_id, invalid_ls_id, palf_exist));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_ls_exist(
      invalid_tenant_id, valid_ls_id, palf_exist));

    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
      tenant_id, ls_id, palf_exist));
    EXPECT_EQ(true, palf_exist);
    SHARED_LOG_GLOBAL_UTILS.delete_blocks(tenant_id, ls_id, start_block_id, end_block_id+1);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
      tenant_id, ls_id, palf_exist));
    EXPECT_EQ(false, palf_exist);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
      tenant_id, tenant_exist));
    EXPECT_EQ(false, tenant_exist);
    // Create empty log stream, an empty directory still exists on the local disk, but oss does not have the concept of directories, therefore check_ls_exist returns false
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
      tenant_id, ls_id, palf_exist));
    EXPECT_EQ(false, palf_exist);


    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
      tenant_id, ls_id, palf_exist));
    EXPECT_EQ(false, palf_exist);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
      tenant_id, tenant_exist));
    EXPECT_EQ(false, tenant_exist);

    block_id_t oldest_block_id = LOG_INVALID_BLOCK_ID;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      tenant_id, ls_id, oldest_block_id));
    // Validate multi-log stream scenario
    {
      uint64_t tmp_tenant_id = 1004;
      std::vector<int64_t> ls_ids = {1, 1001, 1002, 1003, 1004};
      block_id_t tmp_start_block_id = 0;
      block_id_t tmp_end_block_id = 0;
      // Each log stream prepares MAX_BLOCK_COUNT files
      for (auto tmp_ls_id: ls_ids) {
        EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_ls_id, tmp_start_block_id, MAX_BLOCK_COUNT));
      }
      block_id_t tmp_oldoest_block_id = 0;
      bool tmp_palf_exist = false;
      for (auto tmp_ls_id: ls_ids) {
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
          tmp_tenant_id, ObLSID(tmp_ls_id), tmp_oldoest_block_id));
        EXPECT_EQ(tmp_oldoest_block_id, tmp_start_block_id);
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
          tmp_tenant_id, ObLSID(tmp_ls_id), tmp_palf_exist));
        EXPECT_EQ(tmp_palf_exist, true);
      }
      for (auto tmp_ls_id : ls_ids) {
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tmp_tenant_id, ObLSID(tmp_ls_id), tmp_start_block_id, tmp_start_block_id+MAX_BLOCK_COUNT));
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
          tmp_tenant_id, ObLSID(tmp_ls_id), tmp_palf_exist));
        EXPECT_EQ(tmp_palf_exist, false);
      }
      bool tmp_tenant_exist = false;
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
        tmp_tenant_id, tmp_tenant_exist));
      EXPECT_EQ(false, tmp_tenant_exist);
    }
    // Validate multi-tenant scenario
    {
      std::vector<uint64_t> tenant_ids = {1, 1001, 1002, 1003, 1004};
      std::vector<int64_t> ls_ids = {1, 1001, 1002, 1003, 1004};
      block_id_t tmp_start_block_id = 0;
      block_id_t tmp_end_block_id = 0;
      block_id_t tmp_oldest_block_id = 0;
      bool tmp_tenant_exist = false;
      bool tmp_palf_exist = false;
      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_ls_id : ls_ids) {
          EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_ls_id, tmp_start_block_id, MAX_BLOCK_COUNT));
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_ls_id : ls_ids) {
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
            tmp_tenant_id, ObLSID(tmp_ls_id), tmp_oldest_block_id));
          EXPECT_EQ(tmp_oldest_block_id, tmp_start_block_id);
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_ls_id : ls_ids) {
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
            tmp_tenant_id, ObLSID(tmp_ls_id), tmp_oldest_block_id));
          EXPECT_EQ(tmp_oldest_block_id, tmp_start_block_id);
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_ls_id : ls_ids) {
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tmp_tenant_id, ObLSID(tmp_ls_id), tmp_start_block_id, tmp_start_block_id+MAX_BLOCK_COUNT));
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
            tmp_tenant_id, ObLSID(tmp_ls_id), tmp_palf_exist));
          EXPECT_EQ(false, tmp_palf_exist);
        }
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
            tmp_tenant_id, tmp_tenant_exist));
        EXPECT_EQ(false, tmp_tenant_exist);
      } // end 
      for (auto tmp_tenant_id : tenant_ids) {
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_tenant(tmp_tenant_id));
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_tenant(tenant_id));
}



TEST_F(TestSharedLogUtils, test_locate_by_scn_coarsely)
{
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  // Generate 5 files, the smallest block_id is 10
  std::vector<SCN> scns(5, SCN::min_scn());
  int64_t start_ts = ObTimeUtility::current_time();
  for (SCN &scn : scns) {
    scn.convert_from_ts(start_ts++);
  }
  const block_id_t start_block_id = 10;
  const block_id_t end_block_id = 15;
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id, scns));
  
  uint64_t invalid_tenant_id = OB_INVALID_TENANT_ID;  
  uint64_t valid_tenant_id = 1;
  ObLSID invalid_ls_id;
  ObLSID valid_ls_id(1001);
  block_id_t invalid_start_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_start_block_id = 1;
  block_id_t invalid_end_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_end_block_id = 1;
  block_id_t invalid_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_block_id = 1;
  SCN invalid_scn = SCN::invalid_scn();
  // case3: validate locate_by_scn_corasely
  {
    CLOG_LOG(INFO, "begin case3 invalid argument");
    block_id_t out_block_id = LOG_INVALID_BLOCK_ID;
    SCN out_block_min_scn;
    SCN target_scn = scns[0];
  
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      invalid_tenant_id, invalid_ls_id, invalid_start_block_id, invalid_end_block_id, 
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, invalid_ls_id, invalid_start_block_id, invalid_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, valid_ls_id, invalid_start_block_id, invalid_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, valid_ls_id, start_block_id, invalid_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, valid_ls_id, end_block_id, start_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, out_block_id);
    EXPECT_EQ(invalid_scn, out_block_min_scn);

    CLOG_LOG(INFO, "begin case3 abnormal situations");
    block_id_t temp_start_block_id, temp_end_block_id;
    // case 3.1: local log interval and OSS log interval do not intersect, return OB_ENTRY_NOT_EXIST
    // temp_end_block_id <= start_block_id
    temp_start_block_id = 1;
    temp_end_block_id = 10;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id, 
      target_scn, out_block_id, out_block_min_scn));
    // temp_start_block_id >= end_block_id
    temp_start_block_id = 15;
    temp_end_block_id = 20;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id, 
      target_scn, out_block_id, out_block_min_scn));
    
    // case 3.2: the min scn of each block is greater than target scn, return OB_ERR_OUT_OF_LOWER_BOUND
    temp_start_block_id = 11;
    temp_end_block_id = 16;
    target_scn = SCN::min_scn();
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id, 
      target_scn, out_block_id, out_block_min_scn));
    // case 3.3: log stream does not exist and empty log stream, return OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    ObLSID no_block_ls_id(500);

    // EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
    //   not_exist_tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id, 
    //   target_scn, out_block_id, out_block_min_scn));
    CLOG_LOG(INFO, "begin case3 empty logstream");
    temp_start_block_id = LOG_INITIAL_BLOCK_ID;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, no_block_ls_id, temp_start_block_id, temp_end_block_id, 
      target_scn, out_block_id, out_block_min_scn));
    
    CLOG_LOG(INFO, "begin case3 success");
    // case 3.4: success situations
    temp_start_block_id = 8;
    temp_end_block_id = 16;
    for (int i = 0; i < 5; i ++) {
      target_scn = scns[i];
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
        tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id,
        target_scn, out_block_id, out_block_min_scn));
      EXPECT_EQ(start_block_id + i, out_block_id);
      EXPECT_EQ(scns[i], out_block_min_scn);
    }

    // case 3.5: target scn is greater than the min scn of each block
    temp_start_block_id = 8;
    temp_end_block_id = 16;
    target_scn = SCN::max_scn();
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id, 
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(14, out_block_id);
    EXPECT_EQ(scns[4], out_block_min_scn);
  }
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_tenant(tenant_id));
}

TEST_F(TestSharedLogUtils, test_locate_by_scn_coarsely_with_holes)
{
  CLOG_LOG(INFO, "begin test_locate_by_scn_coarsely_with_holes");
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  // Precondition preparation
  // Generate 10 consecutive files, with the smallest block_id being 10, and the largest block_id being 19
  int block_count = 10;
  std::vector<SCN> scns(block_count, SCN::min_scn());
  int64_t start_ts = ObTimeUtility::current_time();
  (void)srand(start_ts);
  int64_t hour = 60 * 60;
  std::vector<SCN> target_scns(block_count, SCN::min_scn());
  for (int i = 0; i < block_count; i++) {
    scns[i].convert_from_ts(start_ts);
    target_scns[i].convert_from_ts(start_ts + 1);
    int64_t interval = random()%hour + 1;
    start_ts += interval;
  }
  const block_id_t start_block_id = 10;
  const block_id_t end_block_id = 20;
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), 10, scns));
  
  block_id_t temp_start_block_id = 7;
  block_id_t temp_end_block_id = 18;
  SCN target_scn = target_scns[5];
  SCN out_block_min_scn;
  block_id_t out_block_id = LOG_INVALID_BLOCK_ID;

  CLOG_LOG(INFO, "begin case1");
  // case 1: OSS does not have holes
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(15, out_block_id);
  EXPECT_EQ(scns[5], out_block_min_scn);
  CLOG_LOG(INFO, "begin case1", K(scns[5]), K(target_scn));
  // case 2: Assume the global checkpoint is at block 15, delete block 12, the first binary search block does not exist.
  // OSS logs: 10 11  13 14 15 16 17 18 19
  CLOG_LOG(INFO, "begin case2");
  block_id_t deleted_block_id = 12;
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, ls_id, deleted_block_id, deleted_block_id+1));
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(15, out_block_id);
  EXPECT_EQ(scns[5], out_block_min_scn);
  // case 3: continue to delete blocks with block_id 13, 14, and 15, the blocks do not exist in both the first and second binary searches
  // OSS logs: 10 11  16 17 18 19
  CLOG_LOG(INFO, "begin case3");
  target_scn = target_scns[6];
  deleted_block_id = 13;
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, ls_id, deleted_block_id, deleted_block_id+3));   
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(16, out_block_id);
  EXPECT_EQ(scns[6], out_block_min_scn);
  // case 4: test the first binary search block exists, but all blocks to its right that are before the global checkpoint do not exist
  CLOG_LOG(INFO, "begin case4");
  temp_start_block_id = 4; 
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(16, out_block_id);
  EXPECT_EQ(scns[6], out_block_min_scn);
  // case 5: test the first binary search when it falls to the right of the global checkpoint, and mid_block exists
  CLOG_LOG(INFO, "begin case5");
  temp_start_block_id = 12;
  temp_end_block_id = 25;
  target_scn = target_scns[9];
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
        tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
        target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(19, out_block_id);
  EXPECT_EQ(scns[9], out_block_min_scn);
  // case 6: test the first binary search falling to the right of the global checkpoint, and mid_block does not exist
  CLOG_LOG(INFO, "begin case6");
  temp_start_block_id = 15;
  temp_end_block_id = 26;
  target_scn = target_scns[8];
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
        tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
        target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(18, out_block_id);
  EXPECT_EQ(scns[8], out_block_min_scn);
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_tenant(tenant_id));
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_shared_log_utils.log*");
  OB_LOGGER.set_file_name("test_shared_log_utils.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  srandom(ObTimeUtility::current_time());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
