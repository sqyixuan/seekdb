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
#define private public
#define protected public

#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "test_ss_local_cache_util.h"
#include "test_ss_local_cache_op.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::storage;
using namespace oceanbase::common;

class TestSSLocalCache : public ::testing::Test
{
public:
  TestSSLocalCache() 
    : is_inited_(false), module_ctx_(), space_ctx_(), env_ctx_(), shared_major_macro_ids_()
  {}
  virtual ~TestSSLocalCache() {}
  virtual void SetUp() override { init_ctx(); }
  virtual void TearDown() override {}
  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
    }
    MockTenantModuleEnv::get_instance().destroy();
  }

private:
  void init_ctx();
  int init_module_ctx();
  int init_space_ctx();
  void init_thread_op_ctx(TestSSLocalCacheOpCtx &op_ctx, const TestSSLocalCacheOpType &op_type, 
      const ObStorageObjectType &obj_type);
  int init_env_ctx();

  int batch_write_private_tablet_meta(const int64_t file_size, const int64_t file_cnt, const int64_t thread_num);
  int batch_write_private_data_macro(const int64_t file_size, const int64_t file_cnt, const int64_t thread_num);
  int batch_write_tmp_file(const int64_t segment_size, const int64_t tmp_file_size, const int64_t tmp_file_cnt,
      const bool seal_last_chunk, const int64_t , const int64_t thread_num);
  int batch_read_cache_tmp_file(const int64_t read_size, const int64_t tmp_file_seg_cnt, const int64_t tmp_file_cnt, 
      const bool is_random_offset, const int64_t base_id, const int64_t thread_num);
  int batch_write_shared_major_data_macro(const int64_t macro_size, const int64_t macro_cnt, const int64_t thread_num,
      ObArray<MacroBlockId> &macro_id_arr);
  int batch_read_micro_block(const int64_t macro_start_idx, const int64_t macro_end_idx, const int64_t first_micro_offset, 
      const int64_t micro_blk_size, const int64_t micro_cnt_of_macro, const int64_t thread_num, 
      ObArray<MacroBlockId> &shared_major_macro_ids, const bool check_data, const int64_t delay_time_us);
  
  TestSSLocalCacheMode gen_mode_randomly();

  int test_local_meta_file();
  
  int test_local_increamental_data();

  int test_local_tmpfile();
  int64_t calc_sealed_tmpfile_size() const;
  int64_t calc_write_cache_tmpfile_size() const;
  int64_t calc_read_cache_tmpfile_size() const;
  int write_sealed_tmpfile();
  int read_sealed_tmpfile();
  int write_unsealed_tmpfile();
  int read_unsealed_tmpfile();

  int test_micro_cache();
  int write_shared_major_data();
  int add_micro_cache_firstly();
  int get_cached_micro_data();
  int add_micro_cache_secondly();
  int decrease_arc_limit();
  int add_micro_cache(const int64_t macro_start_idx, const int64_t macro_cnt, int64_t &add_micro_cnt);

private:
  bool is_inited_;
  TestSSLocalCacheModuleCtx module_ctx_;
  TestSSLocalCacheSpaceCtx space_ctx_;
  TestSSLocalCacheEnvCtx env_ctx_;
  ObArray<MacroBlockId> shared_major_macro_ids_;
};

void TestSSLocalCache::init_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_module_ctx())) {
    LOG_WARN("fail to init module ctx", KR(ret));
  } else if (OB_FAIL(init_space_ctx())) {
    LOG_WARN("fail to init space ctx", KR(ret));
  } else if (OB_FAIL(init_env_ctx())) {
    LOG_WARN("fail to init env_ctx", KR(ret));
  } else {
    shared_major_macro_ids_.reset();
    is_inited_ = true;
  }
}

int TestSSLocalCache::init_module_ctx()
{
  int ret = OB_SUCCESS;
  module_ctx_.disk_space_mgr_ = MTL(ObTenantDiskSpaceManager *);
  module_ctx_.file_mgr_ = MTL(ObTenantFileManager *);
  module_ctx_.micro_cache_ = MTL(ObSSMicroCache *);
  if (!module_ctx_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("module_ctx is invalid", KR(ret));
  }
  return ret;
}

int TestSSLocalCache::init_space_ctx()
{
  int ret = OB_SUCCESS;
  space_ctx_.total_size_ = module_ctx_.disk_space_mgr_->get_total_disk_size();
  space_ctx_.total_meta_size_ = module_ctx_.disk_space_mgr_->get_meta_file_free_disk_size();
  space_ctx_.total_tmp_rcache_size_ = module_ctx_.disk_space_mgr_->get_preread_free_disk_size();
  space_ctx_.total_tmp_wcache_size_ = module_ctx_.disk_space_mgr_->get_tmp_file_write_free_disk_size();
  space_ctx_.total_private_macro_size_ = module_ctx_.disk_space_mgr_->get_private_macro_free_disk_size();
  space_ctx_.total_micro_cache_size_ = module_ctx_.disk_space_mgr_->get_micro_cache_reserved_size();
  space_ctx_.arc_limit_ = module_ctx_.micro_cache_->micro_meta_mgr_.get_arc_info().limit_;
  if (!space_ctx_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("space_ctx is invalid", KR(ret), K_(space_ctx));
  } else {
    LOG_INFO("tenant disk space info", K_(space_ctx)); 
  }
  return ret;
}

void TestSSLocalCache::init_thread_op_ctx(
    TestSSLocalCacheOpCtx &op_ctx,
    const TestSSLocalCacheOpType &op_type, 
    const ObStorageObjectType &obj_type)
{
  op_ctx.tenant_id_ = MTL_ID();
  op_ctx.tenant_epoch_id_ = MTL_EPOCH_ID();
  op_ctx.op_type_ = op_type;
  op_ctx.obj_type_ = obj_type;
}

int TestSSLocalCache::init_env_ctx()
{
  int ret = OB_SUCCESS;
  env_ctx_.thread_num_ = 6;
  TestSSLocalCacheMode mode = gen_mode_randomly();
  env_ctx_.set_test_mode(mode);
  switch (mode) {
    case TestSSLocalCacheMode::FAST_CHECK: printf("This round is FAST_CHECK mode\n"); break;
    case TestSSLocalCacheMode::SMALL_DATA: printf("This round is SMALL_DATA mode\n"); break;
    case TestSSLocalCacheMode::NORMAL_DATA: printf("This round is NORMAL_DATA mode\n"); break;
    case TestSSLocalCacheMode::HUGE_DATA: printf("This round is HUGE_DATA mode\n"); break;
    default: printf("This round mode is invalid\n"); break;
  }

  if (!env_ctx_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("env_ctx is invalid", KR(ret), K_(env_ctx));
  } else {
    LOG_INFO("env_ctx info", K_(env_ctx));
  }
  return ret;
}

int TestSSLocalCache::batch_write_private_tablet_meta(
    const int64_t file_size, 
    const int64_t file_cnt, 
    const int64_t thread_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_size <= 0 || file_cnt <= 0 || thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_size), K(file_cnt), K(thread_num));
  } else {
    TestSSLocalCacheOpCtx op_ctx;
    init_thread_op_ctx(op_ctx, TestSSLocalCacheOpType::PARALLEL_WRITE, ObStorageObjectType::PRIVATE_TABLET_META);
    TestSSParaWritePrivTabletMetaCtx cur_ctx(file_size, file_cnt / thread_num);
    op_ctx.ctx_group_.write_priv_tablet_meta_ = &cur_ctx;
    TestSSLocalCacheOpThread op_threads(ObTenantEnv::get_tenant(), op_ctx, module_ctx_);
    op_threads.set_thread_count(thread_num);
    op_threads.start();
    op_threads.wait();
    const int64_t fail_cnt = op_threads.get_fail_count();
    if (fail_cnt > 0) {
      ret = op_threads.get_op_ret();
      LOG_WARN("exist failure when batch write private tablet meta", KR(ret), K(fail_cnt));
    }
  }
  return ret;
}

int TestSSLocalCache::batch_write_private_data_macro(
    const int64_t file_size, 
    const int64_t file_cnt, 
    const int64_t thread_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_size <= 0 || file_cnt <= 0 || thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_size), K(file_cnt), K(thread_num));
  } else {
    TestSSLocalCacheOpCtx op_ctx;
    init_thread_op_ctx(op_ctx, TestSSLocalCacheOpType::PARALLEL_WRITE, ObStorageObjectType::PRIVATE_DATA_MACRO);
    TestSSParaWritePrivDataMacroCtx cur_ctx(file_size, file_cnt / thread_num);
    op_ctx.ctx_group_.write_priv_data_macro_ = &cur_ctx;
    TestSSLocalCacheOpThread op_threads(ObTenantEnv::get_tenant(), op_ctx, module_ctx_);
    op_threads.set_thread_count(thread_num);
    op_threads.start();
    op_threads.wait();
    const int64_t fail_cnt = op_threads.get_fail_count();
    if (fail_cnt > 0) {
      ret = op_threads.get_op_ret();
      LOG_WARN("exist failure when batch write private macro data", KR(ret), K(fail_cnt));
    }
  }
  return ret;
}

int TestSSLocalCache::batch_write_tmp_file(
    const int64_t segment_size, 
    const int64_t tmp_file_size, 
    const int64_t tmp_file_cnt,
    const bool seal_last_chunk,
    const int64_t base_id,
    const int64_t thread_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(segment_size <= 0 || tmp_file_size <= 0 || tmp_file_cnt <= 0 || base_id < 0 ||
                  thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(segment_size), K(tmp_file_size), K(tmp_file_cnt), K(base_id), K(thread_num));
  } else {
    TestSSLocalCacheOpCtx op_ctx;
    init_thread_op_ctx(op_ctx, TestSSLocalCacheOpType::PARALLEL_WRITE, ObStorageObjectType::TMP_FILE);
    TestSSParaWriteTmpFileCtx cur_ctx(segment_size, tmp_file_size, tmp_file_cnt / thread_num, base_id, seal_last_chunk);
    op_ctx.ctx_group_.write_tmp_file_ = &cur_ctx;
    TestSSLocalCacheOpThread op_threads(ObTenantEnv::get_tenant(), op_ctx, module_ctx_);
    op_threads.set_thread_count(thread_num);
    op_threads.start();
    op_threads.wait();
    const int64_t fail_cnt = op_threads.get_fail_count();
    if (fail_cnt > 0) {
      ret = op_threads.get_op_ret();
      LOG_WARN("exist failure when batch write tmp_file", KR(ret), K(fail_cnt));
    }
  }
  return ret;
}

int TestSSLocalCache::batch_read_cache_tmp_file(
    const int64_t read_size,
    const int64_t tmp_file_seg_cnt,
    const int64_t tmp_file_cnt,
    const bool is_random_offset,
    const int64_t base_id,
    const int64_t thread_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(read_size <= 0 || tmp_file_seg_cnt <= 0 || tmp_file_cnt <= 0 || base_id < 0 || thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_size), K(tmp_file_seg_cnt), K(tmp_file_cnt), K(base_id), K(thread_num));
  } else {
    TestSSLocalCacheOpCtx op_ctx;
    init_thread_op_ctx(op_ctx, TestSSLocalCacheOpType::PARALLEL_READ, ObStorageObjectType::TMP_FILE);
    int64_t offset = 0;
    if (is_random_offset) {
      offset = ObRandom::rand(ObPrereadCacheManager::NOT_PREREAD_IO_SIZE / 2, ObPrereadCacheManager::NOT_PREREAD_IO_SIZE);
    }
    TestSSParaReadCacheTmpFileCtx cur_ctx(offset, read_size, tmp_file_seg_cnt, tmp_file_cnt / thread_num, base_id);
    op_ctx.ctx_group_.read_cache_tmp_file_ = &cur_ctx;
    TestSSLocalCacheOpThread op_threads(ObTenantEnv::get_tenant(), op_ctx, module_ctx_);
    op_threads.set_thread_count(thread_num);
    op_threads.start();
    op_threads.wait();
    const int64_t fail_cnt = op_threads.get_fail_count();
    if (fail_cnt > 0) {
      ret = op_threads.get_op_ret();
      LOG_WARN("exist failure when batch read tmp_file", KR(ret), K(fail_cnt));
    }
  }
  return ret;
}

int TestSSLocalCache::batch_write_shared_major_data_macro(
    const int64_t macro_size, 
    const int64_t macro_cnt, 
    const int64_t thread_num,
    ObArray<MacroBlockId> &macro_id_arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_size <= 0 || macro_cnt <= 0 || thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_size), K(macro_cnt), K(thread_num));
  } else {
    TestSSLocalCacheOpCtx op_ctx;
    init_thread_op_ctx(op_ctx, TestSSLocalCacheOpType::PARALLEL_WRITE, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
    TestSSParaWriteSharedMajorDataMacroCtx cur_ctx(macro_size, macro_cnt / thread_num, macro_id_arr);
    op_ctx.ctx_group_.write_shared_major_macro_ = &cur_ctx;
    TestSSLocalCacheOpThread op_threads(ObTenantEnv::get_tenant(), op_ctx, module_ctx_);
    op_threads.set_thread_count(thread_num);
    op_threads.start();
    op_threads.wait();
    const int64_t fail_cnt = op_threads.get_fail_count();
    if (fail_cnt > 0) {
      ret = op_threads.get_op_ret();
      LOG_WARN("exist failure when batch write private macro data", KR(ret), K(fail_cnt));
    }
  }
  return ret;
}

int TestSSLocalCache::batch_read_micro_block(
    const int64_t macro_start_idx, 
    const int64_t macro_cnt, 
    const int64_t first_micro_offset, 
    const int64_t micro_blk_size, 
    const int64_t micro_cnt_of_macro, 
    const int64_t thread_num, 
    ObArray<MacroBlockId> &shared_major_macro_ids,
    const bool check_data,
    const int64_t delay_time_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_start_idx < 0 || macro_cnt <= 0 || first_micro_offset < 0 ||
      micro_blk_size <= 0 || micro_cnt_of_macro <= 0 || thread_num <= 0 || shared_major_macro_ids.empty() || 
      (macro_start_idx + macro_cnt - 1) > shared_major_macro_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_start_idx), K(macro_cnt), K(first_micro_offset), K(micro_blk_size),
      K(micro_cnt_of_macro), K(thread_num), K(shared_major_macro_ids.count()));
  } else {
    TestSSLocalCacheOpCtx op_ctx;
    init_thread_op_ctx(op_ctx, TestSSLocalCacheOpType::PARALLEL_READ, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
    TestSSParaReadMicroBlockCtx cur_ctx(macro_start_idx, macro_cnt / thread_num, first_micro_offset, micro_blk_size, micro_cnt_of_macro,
      shared_major_macro_ids, check_data, delay_time_us);
    op_ctx.ctx_group_.read_micro_ = &cur_ctx;
    TestSSLocalCacheOpThread op_threads(ObTenantEnv::get_tenant(), op_ctx, module_ctx_);
    op_threads.set_thread_count(thread_num);
    op_threads.start();
    op_threads.wait();
    const int64_t fail_cnt = op_threads.get_fail_count();
    if (fail_cnt > 0) {
      ret = op_threads.get_op_ret();
      LOG_WARN("exist failure when batch read micro_block", KR(ret), K(fail_cnt));
    }
  }
  return ret;
}

TestSSLocalCacheMode TestSSLocalCache::gen_mode_randomly()
{
  TestSSLocalCacheMode mode = TestSSLocalCacheMode::MAX_TYPE;
  const int64_t cur_time_us = ObTimeUtility::current_time();
  const int64_t tmp_val = cur_time_us % 10;
  if (tmp_val == 0) {
    mode = TestSSLocalCacheMode::FAST_CHECK;
  } else if (tmp_val <= 2) {
    mode = TestSSLocalCacheMode::SMALL_DATA;
  } else if (tmp_val <= 5) {
    mode = TestSSLocalCacheMode::NORMAL_DATA;
  } else {
    mode = TestSSLocalCacheMode::HUGE_DATA;
  }
  return mode;
}

int TestSSLocalCache::test_local_meta_file()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t meta_size = 512 * 1024;
  const double data_multiple = MIN(1.0, env_ctx_.data_multiple_);
  const int64_t tmp_val = static_cast<int64_t>((static_cast<double>(space_ctx_.total_meta_size_) * data_multiple));
  const int64_t total_meta_size = tmp_val / meta_size * meta_size;
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t total_meta_cnt = total_meta_size / meta_size / thread_num * thread_num;
  printf("start write private tablet meta, cnt=%ld, size=%ld\n", total_meta_cnt, total_meta_size);
  if (OB_FAIL(batch_write_private_tablet_meta(meta_size, total_meta_cnt, thread_num))) {
    LOG_WARN("fail to batch write private_tablet_meta", KR(ret), K(meta_size), K(total_meta_cnt), 
      K_(env_ctx), K_(space_ctx));
  }
  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  printf("finish write private tablet meta, cost_ms=%ld\n", cost_ms);
  return ret;
}

int TestSSLocalCache::test_local_increamental_data()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t priv_macro_data_size = 2 * 1024 * 1024;
  const double data_multiple = env_ctx_.data_multiple_;
  const int64_t tmp_val = static_cast<int64_t>((static_cast<double>(space_ctx_.total_private_macro_size_) * data_multiple));
  const int64_t total_data_size = tmp_val / priv_macro_data_size * priv_macro_data_size;
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t total_data_cnt = total_data_size / priv_macro_data_size / thread_num * thread_num;
  printf("start write private data macro, cnt=%ld, size=%ld\n", total_data_cnt, total_data_size);
  if (OB_FAIL(batch_write_private_data_macro(priv_macro_data_size, total_data_cnt, thread_num))) {
    LOG_WARN("fail to batch write private_data_macro", KR(ret), K(priv_macro_data_size), K(total_data_cnt), 
      K_(env_ctx), K_(space_ctx));
  }
  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  printf("finish write private data macro, cost_ms=%ld\n", cost_ms);
  return ret;
}

int TestSSLocalCache::test_local_tmpfile()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_sealed_tmpfile())) {
    LOG_WARN("fail to write sealed tmp_file", KR(ret));
  } else if (OB_FAIL(read_sealed_tmpfile())) {
    LOG_WARN("fail to read sealed cache tmp_file", KR(ret));
  } else if (OB_FAIL(write_unsealed_tmpfile())) {
    LOG_WARN("fail to write unsealed tmp_file", KR(ret));
  } else if (OB_FAIL(read_unsealed_tmpfile())) {
    LOG_WARN("fail to read unsealed tmpfile", KR(ret));
  }
  return ret;
}

int64_t TestSSLocalCache::calc_sealed_tmpfile_size() const
{
  const int64_t tmp_file_cache_size = (space_ctx_.total_tmp_rcache_size_ + space_ctx_.total_tmp_wcache_size_) * 2;
  return static_cast<int64_t>((static_cast<double>((tmp_file_cache_size) * env_ctx_.data_multiple_)));
}

int TestSSLocalCache::write_sealed_tmpfile()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t base_id = 100000;
  const int64_t segment_size = 2 * 1024 * 1024; // each segment file is 2MB
  const int64_t tmp_file_seg_cnt = 8;
  const int64_t tmp_file_size = segment_size * tmp_file_seg_cnt;
  const int64_t tmp_val = calc_sealed_tmpfile_size();
  const int64_t total_tmp_file_size = tmp_val / tmp_file_size * tmp_file_size;
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t total_tmp_file_cnt = total_tmp_file_size / tmp_file_size / thread_num * thread_num;
  const bool seal_last_chunk = true;
  printf("start write sealed tmp_file, tmp_file_cnt=%ld, size=%ld\n", total_tmp_file_cnt, total_tmp_file_size);
  if (OB_FAIL(batch_write_tmp_file(
          segment_size, tmp_file_size, total_tmp_file_cnt, seal_last_chunk, base_id, thread_num))) {
    LOG_WARN("fail to batch write sealed tmp_file", KR(ret), K(segment_size), K(tmp_file_size), K(total_tmp_file_cnt),
                                                    K(seal_last_chunk), K(base_id), K(thread_num));
  }
  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  printf("finish write sealed tmp_file, cost_ms=%ld\n", cost_ms);
  return ret;
}

int64_t TestSSLocalCache::calc_read_cache_tmpfile_size() const
{
  const int64_t read_cache_size = space_ctx_.total_tmp_rcache_size_ * 2;
  return static_cast<int64_t>((static_cast<double>((read_cache_size) * env_ctx_.data_multiple_)));
}

int TestSSLocalCache::read_sealed_tmpfile()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t base_id = 100000;
  const int64_t ori_segment_size = 2 * 1024 * 1024;
  const int64_t read_size = ObPrereadCacheManager::NOT_PREREAD_IO_SIZE / 2;
  const int64_t tmp_file_seg_cnt = 8;
  const int64_t tmp_file_size = ori_segment_size * tmp_file_seg_cnt;
  const int64_t tmp_val = calc_read_cache_tmpfile_size();
  const int64_t total_tmp_file_size = tmp_val / tmp_file_size * tmp_file_size;
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t total_tmp_file_cnt = total_tmp_file_size / tmp_file_size / thread_num * thread_num;
  const bool is_random_offset = false;
  printf("start read tmp_file to cache, tmp_file_cnt=%ld, size=%ld\n", total_tmp_file_cnt, total_tmp_file_size);
  if (OB_FAIL(batch_read_cache_tmp_file(
          read_size, tmp_file_seg_cnt, total_tmp_file_cnt, is_random_offset, base_id, thread_num))) {
    LOG_WARN("fail to batch read tmp_file", KR(ret), K(read_size), K(tmp_file_seg_cnt), K(total_tmp_file_cnt), 
                                            K(is_random_offset), K(base_id), K(thread_num));
  } else {
    const int64_t read_cache_size = space_ctx_.total_tmp_rcache_size_;
    const int64_t rcached_tmpfile_size = module_ctx_.disk_space_mgr_->get_tmp_file_read_cache_alloc_size();
    if (rcached_tmpfile_size > read_cache_size || rcached_tmpfile_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read cache tmp_file unexpected", KR(ret), K(rcached_tmpfile_size), K(read_cache_size), K(total_tmp_file_cnt));
    }
  }
  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  printf("finish read tmp_file to cache, cost_ms=%ld\n", cost_ms);
  return ret;
}

int64_t TestSSLocalCache::calc_write_cache_tmpfile_size() const
{
  const int64_t write_cache_size = space_ctx_.total_tmp_wcache_size_ * 2;
  return static_cast<int64_t>((static_cast<double>((write_cache_size) * env_ctx_.data_multiple_)));
}

int TestSSLocalCache::write_unsealed_tmpfile()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t base_id = 10000000;
  const int64_t segment_size = 1024 * 1024; // each unsealed segment is 1MB
  const int64_t tmp_file_seg_cnt = 8;
  const int64_t tmp_file_size = segment_size * tmp_file_seg_cnt;
  const int64_t tmp_val = calc_sealed_tmpfile_size();
  const int64_t total_tmp_file_size = tmp_val / tmp_file_size * tmp_file_size;
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t total_tmp_file_cnt = total_tmp_file_size / tmp_file_size / thread_num * thread_num;
  const bool seal_last_chunk = false;
  printf("start write unsealed tmp_file, tmp_file_cnt=%ld, size=%ld\n", total_tmp_file_cnt, total_tmp_file_size);
  if (OB_FAIL(batch_write_tmp_file(
          segment_size, tmp_file_size, total_tmp_file_cnt, seal_last_chunk, base_id, thread_num))) {
    LOG_WARN("fail to batch write sealed tmp_file", KR(ret), K(segment_size), K(tmp_file_size), K(total_tmp_file_cnt),
                                                    K(seal_last_chunk), K(base_id), K(thread_num));
  }
  const int64_t read_cache_size = space_ctx_.total_tmp_rcache_size_; // 10% * total_size
  const int64_t write_cache_size = space_ctx_.total_tmp_wcache_size_; // 15% * total_size
  const int64_t wcached_tmpfile_size = module_ctx_.disk_space_mgr_->get_tmp_file_write_cache_alloc_size();
  const int64_t rcached_tmpfile_size = module_ctx_.disk_space_mgr_->get_tmp_file_read_cache_alloc_size();

  if (OB_SUCC(ret)) {
    if (rcached_tmpfile_size > read_cache_size || wcached_tmpfile_size <= 0 ||
        (wcached_tmpfile_size + rcached_tmpfile_size) > write_cache_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write unsealed tmp_file unexpected", KR(ret), K(wcached_tmpfile_size), K(rcached_tmpfile_size), 
        K(write_cache_size), K(read_cache_size));
    }
  }
  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  printf("finish write unsealed tmp_file to cache, cost_ms=%ld\n", cost_ms);
  return ret;
}

int TestSSLocalCache::read_unsealed_tmpfile()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t base_id = 10000000;
  const int64_t ori_segment_size = 1024 * 1024;
  const int64_t read_size = ObPrereadCacheManager::NOT_PREREAD_IO_SIZE / 2; 
  const int64_t tmp_file_seg_cnt = 8;
  const int64_t tmp_file_size = ori_segment_size * tmp_file_seg_cnt;
  const int64_t tmp_val = calc_read_cache_tmpfile_size();
  const int64_t total_tmp_file_size = tmp_val / tmp_file_size * tmp_file_size;
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t total_tmp_file_cnt = total_tmp_file_size / tmp_file_size / thread_num * thread_num;
  const bool is_random_offset = true;
  printf("start read unsealed tmp_file, tmp_file_cnt=%ld, size=%ld\n", total_tmp_file_cnt, total_tmp_file_size);

  if (OB_FAIL(batch_read_cache_tmp_file(
          read_size, tmp_file_seg_cnt, total_tmp_file_cnt, is_random_offset, base_id, thread_num))) {
    LOG_WARN("fail to batch read unsealed tmp_file", KR(ret), K(read_size), K(tmp_file_seg_cnt), K(total_tmp_file_cnt), 
                                                     K(is_random_offset), K(base_id), K(thread_num));
  }
  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  printf("finish read unsealed tmp_file, cost_ms=%ld\n", cost_ms);
  return ret;
}

int TestSSLocalCache::test_micro_cache()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_shared_major_data())) {
    LOG_WARN("fail to write shared major data", KR(ret));
  } else if (OB_FAIL(add_micro_cache_firstly())) {
    LOG_WARN("fail to add micro cache firstly", KR(ret));
  } else if (OB_FAIL(get_cached_micro_data())) {
    LOG_WARN("fail to get cached micro data", KR(ret));
  } else if (OB_FAIL(add_micro_cache_secondly())) {
    LOG_WARN("fail to add micro cache secondly", KR(ret));
  } else if (OB_FAIL(decrease_arc_limit())) {
    LOG_WARN("fail to decrease arc_limit", KR(ret));
  }
  return ret;
}

int TestSSLocalCache::write_shared_major_data()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t block_size = 2 * 1024 * 1024;
  const double data_multiple = env_ctx_.data_multiple_;
  const int64_t tmp_val = static_cast<int64_t>((static_cast<double>(space_ctx_.total_micro_cache_size_) * data_multiple));
  const int64_t total_data_size = tmp_val / block_size * block_size;
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t total_macro_cnt = total_data_size / block_size / thread_num * thread_num;
  shared_major_macro_ids_.reset();
  printf("start write shared_major_macro, macro_cnt=%ld\n", total_macro_cnt);
  if (OB_FAIL(batch_write_shared_major_data_macro(block_size, total_macro_cnt, thread_num, shared_major_macro_ids_))) {
    LOG_WARN("fail to batch write shared_major_macro", KR(ret), K(block_size), K(total_macro_cnt), K(thread_num), 
      K_(space_ctx));
  } else if (OB_UNLIKELY(shared_major_macro_ids_.count() != total_macro_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(total_macro_cnt), K(shared_major_macro_ids_.count()));
  }
  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  printf("finish write shared_major_macro, cost_ms=%ld\n", cost_ms);
  return ret;
}

int TestSSLocalCache::add_micro_cache(const int64_t macro_start_idx, const int64_t macro_cnt, int64_t &add_micro_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t block_size = 2 * 1024 * 1024;
  const int64_t thread_num = env_ctx_.thread_num_;
  int64_t delay_time_us = 0;
  const int64_t first_micro_offset = 8 * 1024;
  const int64_t micro_blk_size = 48 * 1024; // 48KB
  const int64_t micro_cnt_of_macro = (block_size - first_micro_offset) / micro_blk_size;
  int64_t total_micro_cnt = macro_cnt * micro_cnt_of_macro;
  const bool check_data = env_ctx_.is_normal_data_mode();
  printf("start load micro_cache, macro_cnt=%ld, micro_cnt=%ld\n", macro_cnt, total_micro_cnt);
  if (OB_FAIL(batch_read_micro_block(macro_start_idx, macro_cnt, first_micro_offset, micro_blk_size, 
      micro_cnt_of_macro, thread_num, shared_major_macro_ids_, check_data, delay_time_us))) {
    LOG_WARN("fail to batch read micro_block and add into cache", KR(ret), K(macro_start_idx), K(macro_cnt), K(thread_num), 
      K(check_data), K(delay_time_us), K_(space_ctx));
  } else {
    add_micro_cnt = total_micro_cnt;
  }
  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  printf("finish load micro_cache, cost_ms=%ld\n", cost_ms);
  return ret;
}

int TestSSLocalCache::add_micro_cache_firstly()
{
  int ret = OB_SUCCESS;
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t total_macro_cnt = shared_major_macro_ids_.count() / 5;
  const int64_t macro_start_idx = 0;
  const int64_t macro_cnt = total_macro_cnt / thread_num * thread_num;
  int64_t total_micro_cnt = 0;
  if (OB_FAIL(add_micro_cache(macro_start_idx, macro_cnt, total_micro_cnt))) {
    LOG_WARN("fail to add micro_cache", KR(ret), K(macro_start_idx), K(macro_cnt));
  } else {
    ObSSARCInfo &arc_info = module_ctx_.micro_cache_->micro_meta_mgr_.arc_info_;
    ObSSMicroCacheStat &cache_stat = module_ctx_.micro_cache_->cache_stat_;
    if (OB_UNLIKELY(cache_stat.hit_stat_.add_cnt_ + cache_stat.hit_stat_.fail_add_cnt_ != total_micro_cnt ||
                    cache_stat.hit_stat_.cache_miss_cnt_ != total_micro_cnt ||
                    arc_info.seg_info_arr_[ARC_T1].cnt_ != cache_stat.hit_stat_.add_cnt_ ||
                    arc_info.seg_info_arr_[ARC_T2].cnt_ != 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(cache_stat), K(arc_info), K(total_micro_cnt));
    }
  }
  return ret;
}

int TestSSLocalCache::get_cached_micro_data()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!module_ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(module_ctx));
  } else {
    ObSSARCInfo &arc_info = module_ctx_.micro_cache_->micro_meta_mgr_.arc_info_;
    ObSSMicroCacheStat &cache_stat = module_ctx_.micro_cache_->cache_stat_;
    const int64_t ori_micro_cnt = arc_info.seg_info_arr_[ARC_T1].cnt_;
    const int64_t ori_add_cnt = cache_stat.hit_stat_.add_cnt_;

    const int64_t thread_num = env_ctx_.thread_num_;
    const int64_t total_macro_cnt = shared_major_macro_ids_.count() / 5;
    const int64_t macro_start_idx = 0;
    const int64_t macro_cnt = total_macro_cnt / thread_num * thread_num;
    int64_t total_micro_cnt = 0;
    if (OB_FAIL(add_micro_cache(macro_start_idx, macro_cnt, total_micro_cnt))) {
      LOG_WARN("fail to add micro_cache", KR(ret), K(macro_start_idx), K(macro_cnt));
    } else if (OB_UNLIKELY(cache_stat.hit_stat_.cache_hit_cnt_ != ori_add_cnt ||
                           arc_info.seg_info_arr_[ARC_T1].cnt_ != 0 ||
                           arc_info.seg_info_arr_[ARC_T2].cnt_ != ori_micro_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(arc_info), K(cache_stat), K(ori_add_cnt), K(ori_micro_cnt));                        
    }
  }
  return ret;
}

int TestSSLocalCache::add_micro_cache_secondly()
{
  int ret = OB_SUCCESS;
  ObSSARCInfo &arc_info = module_ctx_.micro_cache_->micro_meta_mgr_.arc_info_;
  ObSSMicroCacheStat &cache_stat = module_ctx_.micro_cache_->cache_stat_;
  const int64_t ori_t1_cnt = arc_info.seg_info_arr_[ARC_T1].cnt_;
  const int64_t ori_t2_cnt = arc_info.seg_info_arr_[ARC_T2].cnt_;
  
  const int64_t thread_num = env_ctx_.thread_num_;
  const int64_t prev_total_macro_cnt = shared_major_macro_ids_.count() / 5;
  const int64_t prev_macro_cnt = prev_total_macro_cnt / thread_num;
  const int64_t macro_start_idx = prev_macro_cnt;
  const int64_t macro_cnt = shared_major_macro_ids_.count() - prev_total_macro_cnt;
  int64_t total_micro_cnt = 0;
  if (OB_FAIL(add_micro_cache(macro_start_idx, macro_cnt, total_micro_cnt))) {
    LOG_WARN("fail to add micro_cache", KR(ret), K(macro_start_idx), K(macro_cnt));
  } else if (OB_UNLIKELY(arc_info.seg_info_arr_[ARC_T1].cnt_ == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(arc_info), K(cache_stat), K(ori_t1_cnt), K(ori_t2_cnt));                  
  }
  return ret;
}

int TestSSLocalCache::decrease_arc_limit()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!module_ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(module_ctx), K_(space_ctx));
  } else {
    module_ctx_.micro_cache_->task_runner_.micro_ckpt_task_.ckpt_op_.enable_update_arc_limit_ = false;
    ob_usleep(1000 * 1000);
    module_ctx_.micro_cache_->micro_meta_mgr_.arc_info_.work_limit_ = 0;
    ObSSARCInfo &arc_info = module_ctx_.micro_cache_->micro_meta_mgr_.arc_info_;
    int64_t ori_t1_size = arc_info.seg_info_arr_[ARC_T1].size_;
    int64_t ori_t2_size = arc_info.seg_info_arr_[ARC_T2].size_;
    const int64_t MAX_WAIT_ROUND = 60;
    int64_t round = 0;
    while (OB_SUCC(ret) && (round < MAX_WAIT_ROUND)) {
      ob_usleep(1000 * 1000);
      int64_t cur_t1_size = arc_info.seg_info_arr_[ARC_T1].size_;
      int64_t cur_t2_size = arc_info.seg_info_arr_[ARC_T2].size_;
      if (cur_t1_size < ori_t1_size || cur_t2_size < ori_t2_size) {
        ori_t1_size = cur_t1_size;
        ori_t2_size = cur_t2_size;
      } else if (cur_t1_size == ori_t1_size) {
        if (cur_t1_size >= arc_info.limit_ / 10) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("micro_cache evict unexpected", KR(ret), K(cur_t1_size), K(ori_t1_size), K(arc_info));
        }
      } else if (cur_t2_size == ori_t2_size) {
        if (cur_t2_size >= arc_info.limit_ / 10) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("micro_cache evict unexpected", KR(ret), K(cur_t2_size), K(ori_t2_size), K(arc_info));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("micro_cache evict unexpected", KR(ret), K(cur_t1_size), K(ori_t1_size), K(cur_t2_size), 
          K(ori_t2_size), K(arc_info));
      }
      ++round;
    } 
  }
  return ret;
}

TEST_F(TestSSLocalCache, ss_local_cache_core_case)
{
  int ret = OB_SUCCESS;
  // 1. Check local meta file logic
  if (OB_FAIL(test_local_meta_file())) {
    LOG_WARN("fail to test_local_meta_file", KR(ret));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2. Check incremental dump data logic
  if (OB_FAIL(test_local_increamental_data())) {
    LOG_WARN("fail to test_local_increamental_data", KR(ret));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3. Check the logic related to temporary files
  if (OB_FAIL(test_local_tmpfile())) {
    LOG_WARN("fail to test_local_tmpfile", KR(ret));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  // 4. Check micro-block cache related logic
  if (OB_FAIL(test_micro_cache())) {
    LOG_WARN("fail to test_micro_cache", KR(ret));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_local_cache.log*");
  OB_LOGGER.set_file_name("test_ss_local_cache.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
