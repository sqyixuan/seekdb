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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include "gtest/gtest.h"

#define private public
#define protected public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "shared_storage/test_ss_common_util.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;
class TestSSPersistMicroPerf : public ::testing::Test 
{
public:
  TestSSPersistMicroPerf() {}
  virtual ~TestSSPersistMicroPerf() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  class TestSSPersistMicroPerfThread : public Threads 
  {
  public:
    TestSSPersistMicroPerfThread(ObTenantBase *tenant_base, ObSSMicroCache *micro_cache,
        const int64_t macro_block_cnt, const int64_t micro_block_cnt)
        : tenant_base_(tenant_base), micro_cache_(micro_cache), macro_block_cnt_(macro_block_cnt), 
          micro_block_cnt_(micro_block_cnt), fail_cnt_(0), total_add_cache_fail_cnt_(0) {}
    void run(int64_t idx) final;
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
    int64_t get_total_fail_add_cache_cnt() { return ATOMIC_LOAD(&total_add_cache_fail_cnt_); }
  private:
    int parallel_add_micro_block(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    ObSSMicroCache *micro_cache_;
    int64_t macro_block_cnt_;
    int64_t micro_block_cnt_;
    int64_t fail_cnt_;
    int64_t total_add_cache_fail_cnt_;
  };
};

void TestSSPersistMicroPerf::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSPersistMicroPerf::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSPersistMicroPerf::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32)));
  micro_cache->start();
}

void TestSSPersistMicroPerf::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSPersistMicroPerf::TestSSPersistMicroPerfThread::run(int64_t idx)
{
  ObTenantEnv::set_tenant(tenant_base_);
  parallel_add_micro_block(idx);
}

int TestSSPersistMicroPerf::TestSSPersistMicroPerfThread::parallel_add_micro_block(int64_t idx) 
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = micro_block_cnt_;
  const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  char *data_buf = nullptr;
  int64_t add_cache_fail_cnt = 0;
  if (OB_ISNULL(data_buf = static_cast<char *>(allocator.alloc(micro_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
  } else {
    MEMSET(data_buf, 'a', micro_size);
    const int64_t timeout_us = 10 * 1000 * 1000L;
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_cnt_; ++i) {
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(idx * macro_block_cnt_ + i + 1);
      for (int64_t j = 0; OB_SUCC(ret) && j < micro_cnt; ++j) {
        const int32_t offset = payload_offset + j * micro_size;
        const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        if (OB_FAIL(micro_cache_->add_micro_block_cache(micro_key, data_buf, micro_size, 
            ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
          ++add_cache_fail_cnt;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  ATOMIC_AAF(&total_add_cache_fail_cnt_, add_cache_fail_cnt);
  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

TEST_F(TestSSPersistMicroPerf, test_persist_micro_task_perf)
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  micro_ckpt_task.is_inited_ = false;
  ObSSExecuteBlkCheckpointTask &blk_ckpt_task = micro_cache->task_runner_.blk_ckpt_task_;
  blk_ckpt_task.is_inited_ = false;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;
  const int64_t TOTAL_MACRO_CNT = 1024;
  const int64_t micro_cnt = 128;
  const int64_t micro_size = DEFAULT_BLOCK_SIZE / micro_cnt;
  const int64_t thread_num = 8;
  const int64_t phy_block_cnt_per_thread = TOTAL_MACRO_CNT / thread_num;

  const int64_t start_us = ObTimeUtility::current_time();
  TestSSPersistMicroPerf::TestSSPersistMicroPerfThread threads(
      ObTenantEnv::get_tenant(), micro_cache, phy_block_cnt_per_thread, micro_cnt);
  threads.set_thread_count(thread_num);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  const int64_t end_us = ObTimeUtility::current_time();
  const int64_t used_time_ms = (end_us - start_us) / 1000;
  const int64_t total_fail_add_cache_cnt = threads.get_total_fail_add_cache_cnt();
  const double loss_ratio = total_fail_add_cache_cnt * 1.0 / (TOTAL_MACRO_CNT * micro_cnt);
  const int64_t iops =
      (micro_cnt * phy_block_cnt_per_thread * thread_num - total_fail_add_cache_cnt) / used_time_ms * 1000;
  const int64_t throughput_mb = iops * micro_size / (1024 * 1024);
  LOG_INFO("TEST: finish case", K(used_time_ms), K(total_fail_add_cache_cnt), K(loss_ratio), K(iops), K(throughput_mb));
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_persist_micro_perf.log*");
  OB_LOGGER.set_file_name("test_ss_persist_micro_perf.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
