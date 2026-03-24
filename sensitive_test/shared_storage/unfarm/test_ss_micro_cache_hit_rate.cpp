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
#include <gtest/gtest.h>

#define protected public
#define private public
#include "shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::hash;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheHitRate : public ::testing::Test
{
public:
  TestSSMicroCacheHitRate() {}
  virtual ~TestSSMicroCacheHitRate() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void set_basic_read_io_info(ObIOInfo &io_info);
  int check_micro_cache_hit_rate(ObSSMicroCache *micro_cache, const uint64_t tenant_id);
  // calculate total micro cnt of fg_mem_block and bg_mem_block.
  int64_t cal_unpersisted_micro_cnt();
};

void TestSSMicroCacheHitRate::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheHitRate::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheHitRate::SetUp()
{
}

void TestSSMicroCacheHitRate::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCacheHitRate::set_basic_read_io_info(ObIOInfo &io_info)
{
  io_info.tenant_id_ = MTL_ID();
  io_info.timeout_us_ = 5 * 1000 * 1000L; // 5s
  io_info.flag_.set_read();
  io_info.flag_.set_resource_group_id(0);
  io_info.flag_.set_wait_event(1);
}

int TestSSMicroCacheHitRate::check_micro_cache_hit_rate(ObSSMicroCache *micro_cache, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_cache) || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(micro_cache), K(tenant_id));
  } else {
    ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
    ObSSMemDataManager &mem_data_mgr = micro_cache->mem_data_mgr_;
    ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
    ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
    ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

    ObArenaAllocator allocator;
    const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
    const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
    const int32_t micro_size = 8 * 1024;
    const int32_t micro_cnt = (DEFAULT_BLOCK_SIZE - payload_offset) / (micro_size + micro_index_size);
    char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
    if (OB_ISNULL(data_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
    } else if (OB_UNLIKELY(micro_meta_mgr.micro_meta_map_.count() > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_cache is not empty", KR(ret));
    }

    const int64_t data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
    const int64_t t2_blk_cnt = data_blk_cnt / 2;
    // build some T2 micro_blocks
    for (int64_t i = 0; OB_SUCC(ret) && (i < t2_blk_cnt); ++i) {
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1000 + i);
      for (int64_t j = 0; OB_SUCC(ret) && (j < micro_cnt); ++j) {
        const int32_t offset = payload_offset + j * micro_size;
        const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        ret = micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
        if (OB_SUCC(ret)) {
          ObSSMicroBlockMetaHandle micro_meta_handle;
          if (OB_FAIL(micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, true))) {
            LOG_WARN("fail to get micro_meta handle", KR(ret), K(i), K(j));
          } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("micro_meta handle should be valid", KR(ret), K(i), K(j));
          } else if (OB_UNLIKELY(micro_meta_handle()->is_in_l1() || micro_meta_handle()->is_in_ghost())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("micro_meta is unexpected", KR(ret), KPC(micro_meta_handle.get_ptr()));
          } else {
            micro_meta_handle()->access_time_ += (i * micro_cnt + j);
          }
        } else if (OB_EAGAIN == ret) {
          ob_usleep(5 * 1000);
          --j;
        }
      }
      if (FAILEDx(TestSSCommonUtil::wait_for_persist_task())) {
        LOG_WARN("fail to wait for persist task", KR(ret));
      }
    }

    // build some T1 micro_blocks
    for (int64_t i = t2_blk_cnt; OB_SUCC(ret) && (i < data_blk_cnt); ++i) {
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1000 + i);
      for (int64_t j = 0; OB_SUCC(ret) && (j < micro_cnt); ++j) {
        const int32_t offset = payload_offset + j * micro_size;
        const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        ret = micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
        if (OB_SUCC(ret)) {
          ObSSMicroBlockMetaHandle micro_meta_handle;
          if (OB_FAIL(micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_meta_handle))) {
            LOG_WARN("fail to get micro_meta handle", KR(ret), K(i), K(j));
          } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("micro_meta handle should be valid", KR(ret), K(i), K(j));
          } else {
            micro_meta_handle()->access_time_ += (i * micro_cnt + j);
          }
        } else if (OB_EAGAIN == ret) {
          ob_usleep(5 * 1000);
          --j;
        }
      }
      if (FAILEDx(TestSSCommonUtil::wait_for_persist_task())) {
        LOG_WARN("fail to wait for persist task", KR(ret));
      }
    }

    {
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1000 + data_blk_cnt);
      const int32_t offset = payload_offset;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      if (FAILEDx(micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
        LOG_WARN("fail to add micro_block cache", KR(ret), K(macro_id));
      }
    }

    // wait until the eviction finished
    ObSSARCIterInfo arc_iter_info;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(arc_iter_info.init(tenant_id))) {
        LOG_WARN("fail to init arc_iter_info", KR(ret), K(tenant_id));
      } else {
        bool finish_evict = true;
        do {
          finish_evict = true;
          for (int64_t i = 0; finish_evict && i < SS_ARC_SEG_COUNT; ++i) {
            arc_info.calc_arc_iter_info(arc_iter_info, i);
            if (arc_iter_info.need_handle_arc_seg(i)) {
              finish_evict = false;
            }
            arc_iter_info.reuse(i);
          }
          if (!finish_evict) {
            ob_usleep(100 * 1000);
          }
        } while (!finish_evict);
      }
    }

    // get the evicted micro_count
    const int64_t t1_total_cnt = (data_blk_cnt - t2_blk_cnt) * micro_cnt + 1;
    const int64_t t2_total_cnt = t2_blk_cnt * micro_cnt;
    const int64_t t1_evict_cnt = cache_stat.task_stat().t1_evict_cnt_;
    const int64_t t2_evict_cnt = cache_stat.task_stat().t2_evict_cnt_;
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(t1_evict_cnt >= t1_total_cnt || t2_evict_cnt >= t2_total_cnt)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }

    // calc the micro_cache hit rate
    int64_t t1_hit_cnt = 0;
    bool finish_check = false;
    for (int64_t i = t2_blk_cnt; OB_SUCC(ret) && (i < data_blk_cnt) && !finish_check; ++i) {
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1000 + i);
      for (int64_t j = 0; OB_SUCC(ret) && (j < micro_cnt) && !finish_check; ++j) {
        if ((i - t2_blk_cnt) * micro_cnt + j < t1_evict_cnt) {
          const int32_t offset = payload_offset + j * micro_size;
          const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
          ObSSMicroBlockMetaHandle micro_meta_handle;
          if (OB_FAIL(micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_meta_handle))) {
            LOG_WARN("fail to get micro_meta handle", KR(ret), K(i), K(j));
          } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("micro_meta handle should be valid", KR(ret), K(i), K(j));
          } else if (!micro_meta_handle()->is_valid_field()) {
            ++t1_hit_cnt;
          }
        } else {
          finish_check = true;
        }
      }
    }
    int64_t t2_hit_cnt = 0;
    finish_check = false;
    for (int64_t i = 0; OB_SUCC(ret) && (i < t2_blk_cnt) && !finish_check; ++i) {
      const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1000 + i);
      for (int64_t j = 0; OB_SUCC(ret) && (j < micro_cnt) && !finish_check; ++j) {
        if (i * micro_cnt + j < t2_evict_cnt) {
          const int32_t offset = payload_offset + j * micro_size;
          const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
          ObSSMicroBlockMetaHandle micro_meta_handle;
          if (OB_FAIL(micro_meta_mgr.micro_meta_map_.get(&micro_key, micro_meta_handle))) {
            LOG_WARN("fail to get micro_meta handle", KR(ret), K(i), K(j));
          } else if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("micro_meta handle should be valid", KR(ret), K(i), K(j));
          } else if (!micro_meta_handle()->is_valid_field()) {
            ++t2_hit_cnt;
          }
        } else {
          finish_check = true;
        }
      }
    }

    // check the micro_cache hit rate
    const double t1_hit_rate = (t1_total_cnt - 2 * t1_evict_cnt + 2 * t1_hit_cnt) / double(t1_total_cnt);
    const int64_t t1_exp_hit_cnt = t1_evict_cnt / 3;
    const double t1_ref_rate = (t1_total_cnt - 2 * t1_evict_cnt + 2 * t1_exp_hit_cnt) / double(t1_total_cnt);
    const double t2_hit_rate = (t2_total_cnt - 2 * t2_evict_cnt + 2 * t2_hit_cnt) / double(t2_total_cnt);
    const int64_t t2_exp_hit_cnt = t2_evict_cnt / 3;
    const double t2_ref_rate = (t2_total_cnt - 2 * t2_evict_cnt + 2 * t2_exp_hit_cnt) / double(t2_total_cnt);
    // t1_total_cnt=245664, t2_total_cnt=245663, t1_evict_cnt=50780, t2_evict_cnt=42824, t1_hit_cnt=20036, t2_hit_cnt=20325, 
    // t1_hit_rate=0.74, t1_ref_rate=0.72, t2_hit_rate=0.81, t2_ref_rate=0.76
    LOG_INFO("evict info", K(t1_total_cnt), K(t2_total_cnt), K(t1_evict_cnt), K(t2_evict_cnt), K(t1_hit_cnt), K(t2_hit_cnt),
      K(t1_hit_rate), K(t1_ref_rate), K(t2_hit_rate), K(t2_ref_rate));
    if (OB_SUCC(ret)) {
      if (t1_hit_rate < t1_ref_rate && t2_hit_rate < t2_ref_rate) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hit rate is unexpected", KR(ret));
      }
    }
  }
  return ret;
}

int64_t TestSSMicroCacheHitRate::cal_unpersisted_micro_cnt()
{
  ObSSMemDataManager &mem_data_mgr = MTL(ObSSMicroCache *)->mem_data_mgr_;
  int64_t total_unpersisted_micro_cnt = 0;
  if (nullptr != mem_data_mgr.fg_mem_block_) {
    total_unpersisted_micro_cnt += mem_data_mgr.fg_mem_block_->micro_count_;
  }
  if (nullptr != mem_data_mgr.bg_mem_block_) {
    total_unpersisted_micro_cnt += mem_data_mgr.bg_mem_block_->micro_count_;
  }
  return total_unpersisted_micro_cnt;
}

TEST_F(TestSSMicroCacheHitRate, test_micro_cache_hit_rate)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_micro_cache_hit_rate");
  const uint64_t tenant_id = MTL_ID();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  bool succ_check = false;
  for (int64_t i = 0; i < 10 && !succ_check; ++i) {
    if (OB_FAIL(check_micro_cache_hit_rate(micro_cache, tenant_id))) {
      LOG_WARN("fail to check micro_cache hit_rate", KR(ret));
    } else {
      succ_check = true;
    }
    ret = OB_SUCCESS;
    micro_cache->clear_micro_cache();
  }
  ASSERT_EQ(true, succ_check);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_hit_rate.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_hit_rate.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
