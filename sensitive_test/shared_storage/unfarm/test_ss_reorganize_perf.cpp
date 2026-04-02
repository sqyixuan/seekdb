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
class TestSSReorganizePerf : public ::testing::Test 
{
public:
  TestSSReorganizePerf() {}
  virtual ~TestSSReorganizePerf() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  struct TestSSReorganizePerfCtx 
  {
  public:
    TestSSReorganizePerfCtx() : thread_num_(0), t2_pct_(0), macro_blk_cnt_(0), micro_size_(0) {}
    ~TestSSReorganizePerfCtx() { reset(); }
    void reset()
    {
      thread_num_ = 0;
      t2_pct_ = 0;
      macro_blk_cnt_ = 0;
      micro_size_ = 0;
    }
  public:
    int64_t thread_num_;
    int64_t t2_pct_; // percentage of T2 in total micro_block, range [0, 100].
    int64_t macro_blk_cnt_;
    int64_t micro_size_;
  };

  public : class TestSSReorganizePerfThread : public Threads 
  {
  public:
    TestSSReorganizePerfThread(ObTenantBase *tenant_base, TestSSReorganizePerfCtx &ctx)
        : tenant_base_(tenant_base), ctx_(ctx), fail_cnt_(0), stop_(false)
    {}
    void run(int64_t idx) final;
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
    void set_stop() { ATOMIC_STORE(&stop_, true); }
    bool is_stop() { return ATOMIC_LOAD(&stop_); }
  private:
    int parallel_add_micro_block(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    TestSSReorganizePerfCtx &ctx_;
    int64_t fail_cnt_;
    bool stop_;
  };
};

void TestSSReorganizePerf::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSReorganizePerf::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSReorganizePerf::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), 20L * (1L << 30)));
  micro_cache->start();
}

void TestSSReorganizePerf::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSReorganizePerf::TestSSReorganizePerfThread::run(int64_t idx)
{
  ObTenantEnv::set_tenant(tenant_base_);
  parallel_add_micro_block(idx);
}

int TestSSReorganizePerf::TestSSReorganizePerfThread::parallel_add_micro_block(int64_t idx) 
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_size = ctx_.micro_size_;
  const int32_t micro_cnt = (DEFAULT_BLOCK_SIZE - payload_offset) / (micro_size + micro_index_size);
  char *data_buf = nullptr;
  const int64_t time_delta_s = 10;
  if (OB_ISNULL(data_buf = static_cast<char *>(allocator.alloc(micro_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
  } else {
    MEMSET(data_buf, 'a', micro_size);
    for (int64_t i = 1; OB_SUCC(ret) && !is_stop() && i <= ctx_.macro_blk_cnt_; ++i) {
      ObArray<ObSSMicroBlockCacheKey> micro_keys;
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id((idx + 1) * ctx_.macro_blk_cnt_ + i);
      for (int64_t j = 0; OB_SUCC(ret) && !is_stop() && j < micro_cnt; ++j) {
        const int32_t offset = payload_offset + j * micro_size;
        const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        const bool transfer_T2 = (ObRandom::rand(0, 100) < ctx_.t2_pct_);
        if (OB_FAIL(micro_cache->add_micro_block_cache(
                micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
          if (OB_EAGAIN == ret || OB_SS_CACHE_REACH_MEM_LIMIT == ret) {
            ret = OB_SUCCESS;  // ignore fail to allocate mem_block, normal_phy_blk or micro_meta.
          } else {
            LOG_WARN("fail to add micro_block into cache", KR(ret), K(idx), K(micro_key));
          }
        } else if (transfer_T2 && OB_FAIL(micro_keys.push_back(micro_key))) {
          LOG_WARN("fail to push back micro key", KR(ret), K(micro_key));
        }
      }

      if (FAILEDx(micro_keys.count() > 0 &&
                  micro_cache->micro_meta_mgr_.update_micro_block_meta_heat(micro_keys, true, true, time_delta_s))) {
        LOG_WARN("fail to get micro_block meta handle", KR(ret), K(micro_keys));
      }
    }
  }
  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

/* Test reorganzie task performance is divided into two steps:
    Step1: stop arc_task and write data into micro_cache to make the usage of cache_data_blk reach limit
    Step2: start arc_task and write data into micro_cache in parallel, record the speed of reorganizing phy_block 
*/
TEST_F(TestSSReorganizePerf, test_reorganize_task_perf)
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroCacheTaskStat &task_stat = micro_cache->cache_stat_.task_stat_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  const int64_t micro_size = 16 * 1024;
  const int64_t thread_num = 4;
  const int64_t macro_block_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt() / thread_num;

  // Step 1
  TestSSReorganizePerfCtx ctx;
  ctx.thread_num_ = thread_num;
  ctx.macro_blk_cnt_ = macro_block_cnt;
  ctx.micro_size_ = micro_size;
  ctx.t2_pct_ = 50;
  TestSSReorganizePerf::TestSSReorganizePerfThread threads(ObTenantEnv::get_tenant(), ctx);
  threads.set_thread_count(thread_num);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());

  arc_task.is_inited_ = true;
  ctx.macro_blk_cnt_ = 100000000L;
  TestSSReorganizePerf::TestSSReorganizePerfThread threads2(ObTenantEnv::get_tenant(), ctx);
  threads2.set_thread_count(thread_num);
  threads2.start();

  // Step 2
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t duration_us = 300L * 1000 * 1000;
  const int64_t resize_time_us = 100L *1000 * 1000;
  bool do_resize = false;
  const int64_t PRINT_LOG_INTERVAL_US = 5 * 1000 * 1000;
  int64_t reorgan_task_cnt = 0;
  int64_t reorgan_free_blk_cnt = 0;
  int64_t reorgan_free_blk_cnt_speed = 0;
  while (ObTimeUtility::current_time() - start_us < duration_us) {
    if (!do_resize && (ObTimeUtility::current_time() - start_us > resize_time_us)) {
      do_resize = true;
      const int64_t new_cache_file_size = micro_cache->cache_file_size_ * 2;
      ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(new_cache_file_size)); 
    }
    ob_usleep(3 * 1000 * 1000);
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL_US)) {
      const int64_t cost_s = (ObTimeUtility::current_time() - start_us) / (1000L * 1000);
      reorgan_task_cnt = ATOMIC_LOAD(&task_stat.reorgan_cnt_);
      reorgan_free_blk_cnt = ATOMIC_LOAD(&task_stat.reorgan_free_blk_cnt_);
      reorgan_free_blk_cnt_speed = ATOMIC_LOAD(&task_stat.reorgan_free_blk_cnt_) / cost_s;
      LOG_INFO("reorganize perf",
          K(reorgan_task_cnt),
          K(reorgan_free_blk_cnt),
          K(reorgan_free_blk_cnt_speed),
          K(phy_blk_mgr.get_sparse_block_cnt()),
          K_(phy_blk_mgr.blk_cnt_info));
    }
  }
  threads2.set_stop();
  threads2.wait();
  ASSERT_EQ(0, threads2.get_fail_cnt());  
  // reorganize_free_blk speed needs to exceed 100, otherwise it will be considered as performance regression.
  ASSERT_LT(100, reorgan_free_blk_cnt_speed);

  // wait sparse_block in sparse_blk_map to be reorganized, 
  // finally, sparse_block_cnt needs to be less than MIN_REORGAN_PHY_BLK_CNT.
  start_us = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - start_us < duration_us) {
    const int64_t remain_reorgan_cnt = phy_blk_mgr.get_sparse_block_cnt();
    if (remain_reorgan_cnt < MIN_REORGAN_BLK_CNT) {
      break;
    } else {
      LOG_INFO("reorganziable phy_block cnt", K(remain_reorgan_cnt));
    }
    ob_usleep(1 * 1000 * 1000);
  }
  ASSERT_LT(phy_blk_mgr.get_sparse_block_cnt(), MIN_REORGAN_BLK_CNT);
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_reorganize_perf.log*");
  OB_LOGGER.set_file_name("test_ss_reorganize_perf.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
