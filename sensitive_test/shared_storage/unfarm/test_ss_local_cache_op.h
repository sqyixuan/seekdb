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
#include "lib/ob_errno.h"
#include "lib/random/ob_random.h"
#include "lib/thread/threads.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "share/rc/ob_tenant_base.h"
#include "test_ss_local_cache_struct.h"

namespace oceanbase 
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

enum class TestSSLocalCacheOpType : uint8_t
{
  PARALLEL_WRITE = 0,
  PARALLEL_READ = 1,
  MAX,
};

struct TestSSParaWritePrivTabletMetaCtx
{
public:
  int64_t file_size_;
  int64_t thread_file_cnt_;
  TestSSParaWritePrivTabletMetaCtx(const int64_t file_size, const int64_t thread_file_cnt)
    : file_size_(file_size), thread_file_cnt_(thread_file_cnt)
  {}

  bool is_valid() const { return file_size_ > 0 && thread_file_cnt_ > 0; }
};

struct TestSSParaWritePrivDataMacroCtx
{
public:
  int64_t file_size_;
  int64_t thread_file_cnt_;
  TestSSParaWritePrivDataMacroCtx(const int64_t file_size, const int64_t thread_file_cnt)
    : file_size_(file_size), thread_file_cnt_(thread_file_cnt)
  {}

  bool is_valid() const { return file_size_ > 0 && thread_file_cnt_ > 0; }
};

struct TestSSParaWriteTmpFileCtx
{
public:
  int64_t segment_size_;
  int64_t tmp_file_size_;
  int64_t thread_tmp_file_cnt_;
  int64_t base_id_;
  bool seal_last_chunk_;

  TestSSParaWriteTmpFileCtx(const int64_t segment_size, const int64_t tmp_file_size, const int64_t thread_tmp_file_cnt,
      const int64_t base_id, const bool seal_last_chunk)
      : segment_size_(segment_size),
        tmp_file_size_(tmp_file_size),
        thread_tmp_file_cnt_(thread_tmp_file_cnt),
        base_id_(base_id),
        seal_last_chunk_(seal_last_chunk)
  {}

  int64_t get_file_seg_cnt() const { return tmp_file_size_ / segment_size_; }
  bool is_valid() const
  {
    return segment_size_ > 0 && tmp_file_size_ > 0 && thread_tmp_file_cnt_ > 0 && base_id_ >= 0;
  }
};

struct TestSSParaReadCacheTmpFileCtx
{
public:
  int64_t read_offset_;
  int64_t read_size_;
  int64_t tmp_file_seg_cnt_;
  int64_t thread_tmp_file_cnt_;
  int64_t base_id_;

  TestSSParaReadCacheTmpFileCtx(const int64_t read_offset, const int64_t read_size, const int64_t tmp_file_seg_cnt,
      const int64_t thread_tmp_file_cnt, const int64_t base_id)
      : read_offset_(read_offset),
        read_size_(read_size),
        tmp_file_seg_cnt_(tmp_file_seg_cnt),
        thread_tmp_file_cnt_(thread_tmp_file_cnt),
        base_id_(base_id)
  {}

  bool is_valid() const
  {
    return read_offset_ >= 0 && read_size_ > 0 && tmp_file_seg_cnt_ > 0 && thread_tmp_file_cnt_ > 0 && base_id_ >= 0;
  }
};

struct TestSSParaWriteSharedMajorDataMacroCtx
{
public:
  int64_t macro_size_;
  int64_t thread_macro_cnt_;
  ObArray<MacroBlockId> &macro_id_arr_;
  TestSSParaWriteSharedMajorDataMacroCtx(const int64_t macro_size, const int64_t thread_macro_cnt,
      ObArray<MacroBlockId> &macro_id_arr)
    : macro_size_(macro_size), thread_macro_cnt_(thread_macro_cnt), macro_id_arr_(macro_id_arr)
  {}

  bool is_valid() const { return macro_size_ > 0 && thread_macro_cnt_ > 0; }
};

struct TestSSParaReadMicroBlockCtx
{
public:
  int64_t macro_read_start_idx_;  
  int64_t macro_read_cnt_;
  int64_t first_micro_offset_;
  int64_t micro_blk_size_;
  int64_t micro_cnt_of_macro_; 
  ObArray<MacroBlockId> &shared_major_macro_ids_;
  bool check_data_;
  int64_t delay_time_us_;

  TestSSParaReadMicroBlockCtx(const int64_t macro_read_start_idx, const int64_t macro_read_cnt,
      const int64_t first_micro_offset, const int64_t micro_blk_size, const int64_t micro_cnt_of_macro,
      ObArray<MacroBlockId> &shared_major_macro_ids, const bool check_data = false, const int64_t delay_time_us = 0)
    : macro_read_start_idx_(macro_read_start_idx), macro_read_cnt_(macro_read_cnt), first_micro_offset_(first_micro_offset),
      micro_blk_size_(micro_blk_size), micro_cnt_of_macro_(micro_cnt_of_macro), shared_major_macro_ids_(shared_major_macro_ids),
      check_data_(check_data), delay_time_us_(delay_time_us)
  {}

  bool is_valid() const { return macro_read_start_idx_ >= 0 && macro_read_cnt_ > 0 &&
                          first_micro_offset_ >= 0 && micro_blk_size_ > 0 && micro_cnt_of_macro_ > 0 && 
                          !shared_major_macro_ids_.empty(); }
};

struct TestSSLocalCacheCtxGroup
{
public:
  TestSSParaWritePrivTabletMetaCtx *write_priv_tablet_meta_;
  TestSSParaWriteTmpFileCtx *write_tmp_file_;
  TestSSParaReadCacheTmpFileCtx *read_cache_tmp_file_; // Read temporary file 0~512KB, will read the entire segment (2MB)
  TestSSParaWritePrivDataMacroCtx *write_priv_data_macro_;
  TestSSParaWriteSharedMajorDataMacroCtx *write_shared_major_macro_;
  TestSSParaReadMicroBlockCtx *read_micro_;

  TO_STRING_KV(KP_(write_priv_tablet_meta), KP_(write_tmp_file), KP_(read_cache_tmp_file), KP_(write_priv_data_macro), 
    KP_(write_shared_major_macro), KP_(read_micro));
};

struct TestSSLocalCacheOpCtx
{
public:
  uint64_t tenant_id_;
  int64_t tenant_epoch_id_;
  TestSSLocalCacheOpType op_type_;
  ObStorageObjectType obj_type_;
  int64_t fail_cnt_;
  int op_ret_;
  TestSSLocalCacheCtxGroup ctx_group_;

  TestSSLocalCacheOpCtx() 
    : tenant_id_(OB_INVALID_TENANT_ID), tenant_epoch_id_(0), op_type_(TestSSLocalCacheOpType::MAX), 
      obj_type_(ObStorageObjectType::MAX), fail_cnt_(0), op_ret_(common::OB_SUCCESS), ctx_group_()
  {}

  TO_STRING_KV(K_(tenant_id), K_(tenant_epoch_id), K_(op_type), K_(obj_type), K_(fail_cnt), K_(op_ret), K_(ctx_group));
};

/*-----------------------------------------TestSSLocalCacheOpThread-----------------------------------------*/
class TestSSLocalCacheOpThread : public Threads
{
public:
  TestSSLocalCacheOpThread(ObTenantBase *tenant_base, TestSSLocalCacheOpCtx &op_ctx, TestSSLocalCacheModuleCtx &module_ctx)
    : tenant_base_(tenant_base), op_ctx_(op_ctx), module_ctx_(module_ctx), lock_()
  {}

  virtual void run(int64_t idx) final;

  int64_t get_fail_count() const { return ATOMIC_LOAD(&op_ctx_.fail_cnt_); }
  int get_op_ret() const { return ATOMIC_LOAD(&op_ctx_.op_ret_); }

private:
  int parallel_write_private_tablet_meta(const int64_t idx);
  int parallel_write_tmp_file(const int64_t idx);
  int parallel_read_cache_tmp_file(const int64_t idx);
  int parallel_write_private_data_macro(const int64_t idx);
  int parallel_write_shared_major_data_macro(const int64_t idx);
  int parallel_read_micro_block(const int64_t idx);

private:
  ObTenantBase *tenant_base_;
  TestSSLocalCacheOpCtx &op_ctx_;
  TestSSLocalCacheModuleCtx &module_ctx_;
  common::SpinRWLock lock_;
};

void TestSSLocalCacheOpThread::run(int64_t idx)
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(tenant_base_);
  if (op_ctx_.op_type_ == TestSSLocalCacheOpType::PARALLEL_WRITE) {
    if (op_ctx_.obj_type_ == ObStorageObjectType::PRIVATE_TABLET_META) {
      parallel_write_private_tablet_meta(idx);
    } else if (op_ctx_.obj_type_ == ObStorageObjectType::TMP_FILE) {
      parallel_write_tmp_file(idx);
    } else if (op_ctx_.obj_type_ == ObStorageObjectType::PRIVATE_DATA_MACRO) {
      parallel_write_private_data_macro(idx);
    } else if (op_ctx_.obj_type_ == ObStorageObjectType::SHARED_MAJOR_DATA_MACRO) {
      parallel_write_shared_major_data_macro(idx);
    }
  } else if (op_ctx_.op_type_ == TestSSLocalCacheOpType::PARALLEL_READ) {
    if (op_ctx_.obj_type_ == ObStorageObjectType::TMP_FILE) {
      parallel_read_cache_tmp_file(idx);
    } else if (op_ctx_.obj_type_ == ObStorageObjectType::SHARED_MAJOR_DATA_MACRO) {
      parallel_read_micro_block(idx); // actually read micro_block(part of the macro_block)
    }
  }
}

int TestSSLocalCacheOpThread::parallel_write_private_tablet_meta(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_ctx_.ctx_group_.write_priv_tablet_meta_) || !op_ctx_.ctx_group_.write_priv_tablet_meta_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(op_ctx));
  } else {
    const int64_t ls_id = idx + 1;
    const int64_t ls_epoch_id = 1;
    const int64_t tablet_id = 100;
    ObArenaAllocator allocator;
    const int64_t file_size = op_ctx_.ctx_group_.write_priv_tablet_meta_->file_size_;
    const int64_t thread_file_cnt = op_ctx_.ctx_group_.write_priv_tablet_meta_->thread_file_cnt_;
    char *write_buf = static_cast<char*>(allocator.alloc(file_size));
    LOG_INFO("start parallel write private tablet meta", K(file_size), K(thread_file_cnt), K(idx));
    if (OB_ISNULL(write_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K_(op_ctx), K(file_size));
    } else if (OB_FAIL(OB_DIR_MGR.create_ls_id_dir(op_ctx_.tenant_id_, op_ctx_.tenant_epoch_id_, ls_id, ls_epoch_id))) {
      LOG_WARN("fail to create ls_id dir", KR(ret), K_(op_ctx), K(ls_id), K(ls_epoch_id));
    } else if (OB_FAIL(OB_DIR_MGR.create_tablet_meta_tablet_id_dir(op_ctx_.tenant_id_, op_ctx_.tenant_epoch_id_, 
               ls_id, ls_epoch_id, tablet_id))) {
      LOG_WARN("fail to create tablet_meta tablet_id dir", KR(ret), K_(op_ctx), K(ls_id), K(ls_epoch_id), K(tablet_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < thread_file_cnt); ++i) {
        MacroBlockId macro_id = TestSSLocalCacheUtil::gen_macro_block_id(ObStorageObjectType::PRIVATE_TABLET_META, ls_id, 
          tablet_id, 100 + i);
        if (OB_FAIL(TestSSLocalCacheUtil::write_private_tablet_meta(module_ctx_.file_mgr_, op_ctx_.tenant_id_, macro_id, 
            file_size, ls_epoch_id, write_buf))) {
          LOG_WARN("fail to write private tablet meta", KR(ret), K_(op_ctx), K(macro_id), K(file_size), K(ls_epoch_id), KP(write_buf));
        }
      }
    }
    allocator.clear();
  }

  if (OB_FAIL(ret)) {
    ATOMIC_AAF(&op_ctx_.fail_cnt_, 1);
    ATOMIC_STORE(&op_ctx_.op_ret_, ret);
  }
  return ret;
}

int TestSSLocalCacheOpThread::parallel_write_tmp_file(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_ctx_.ctx_group_.write_tmp_file_) || !op_ctx_.ctx_group_.write_tmp_file_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(op_ctx), K_(module_ctx));
  } else {
    ObArenaAllocator allocator;
    const int64_t file_seg_cnt = op_ctx_.ctx_group_.write_tmp_file_->get_file_seg_cnt();
    const int64_t tmp_file_cnt = op_ctx_.ctx_group_.write_tmp_file_->thread_tmp_file_cnt_;
    const int64_t segment_size = op_ctx_.ctx_group_.write_tmp_file_->segment_size_;
    const int64_t base_id = op_ctx_.ctx_group_.write_tmp_file_->base_id_;
    const bool seal_last_chunk = op_ctx_.ctx_group_.write_tmp_file_->seal_last_chunk_;
    char *write_buf = static_cast<char*>(allocator.alloc(segment_size));
    LOG_INFO("start parallel write tmp_file", K(idx), K(file_seg_cnt), K(tmp_file_cnt), K(segment_size));
    if (OB_ISNULL(write_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K_(op_ctx), K(segment_size));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_file_cnt; ++i) {
        const int64_t tmp_file_id = (idx + 1) * base_id + i;
        if (OB_FAIL(OB_DIR_MGR.create_tmp_file_dir(op_ctx_.tenant_id_, op_ctx_.tenant_epoch_id_, tmp_file_id))) {
          LOG_WARN("fail to create tmp_file dir", KR(ret), K_(op_ctx), K(tmp_file_id), K(i));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < file_seg_cnt; ++j) {
            const int64_t segment_id = j + 1;
            // each segment file is split into some chunks
            const int64_t chunk_cnt = 1 << ObRandom::rand(0, 2);
            const int64_t chunk_size = segment_size / chunk_cnt;
            MacroBlockId macro_id =
                TestSSLocalCacheUtil::gen_macro_block_id(ObStorageObjectType::TMP_FILE, tmp_file_id, segment_id, 1);
            for (int64_t k = 0; OB_SUCC(ret) && k < chunk_cnt; ++k) {
              const bool need_sealed = ((k == chunk_cnt - 1) && seal_last_chunk);
              int64_t offset = k * chunk_size;
              if (OB_FAIL(TestSSLocalCacheUtil::write_tmp_file(
                      op_ctx_.tenant_id_, macro_id, offset, chunk_size, need_sealed, write_buf))) {
                LOG_WARN("fail to write tmp_file", KR(ret), K(idx), K(i), K(j), K(k), K_(op_ctx), K(macro_id), K(offset), 
                                                   K(chunk_size), K(need_sealed), K(segment_size), KP(write_buf));
              }

              if (OB_IO_LIMIT == ret) {
                ret = OB_SUCCESS;
                --k;
              }
            }
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    ATOMIC_AAF(&op_ctx_.fail_cnt_, 1);
    ATOMIC_STORE(&op_ctx_.op_ret_, ret);
  }
  return ret;
}

int TestSSLocalCacheOpThread::parallel_read_cache_tmp_file(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_ctx_.ctx_group_.read_cache_tmp_file_) || !op_ctx_.ctx_group_.read_cache_tmp_file_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(op_ctx), K_(module_ctx));
  } else {
    ObArenaAllocator allocator;
    const int64_t base_id = op_ctx_.ctx_group_.read_cache_tmp_file_->base_id_;
    const int64_t file_seg_cnt = op_ctx_.ctx_group_.read_cache_tmp_file_->tmp_file_seg_cnt_;
    const int64_t tmp_file_cnt = op_ctx_.ctx_group_.read_cache_tmp_file_->thread_tmp_file_cnt_;
    const int64_t read_size = op_ctx_.ctx_group_.read_cache_tmp_file_->read_size_;
    const int64_t read_offset = op_ctx_.ctx_group_.read_cache_tmp_file_->read_offset_;
    char *read_buf = static_cast<char*>(allocator.alloc(read_size));
    LOG_INFO("start parallel read cache tmp_file", K(idx), K(file_seg_cnt), K(tmp_file_cnt), K(read_size), K(read_offset));
    if (OB_ISNULL(read_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K_(op_ctx));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_file_cnt; ++i) {
        const int64_t tmp_file_id = (idx + 1) * base_id + i;
        for (int64_t j = 0; OB_SUCC(ret) && j < file_seg_cnt; ++j) {
          const int64_t segment_id = j + 1;
          MacroBlockId macro_id =
              TestSSLocalCacheUtil::gen_macro_block_id(ObStorageObjectType::TMP_FILE, tmp_file_id, segment_id, 1);
          if (OB_FAIL(TestSSLocalCacheUtil::read_tmp_file(
                  op_ctx_.tenant_id_, macro_id, read_offset, read_size, read_buf))) {
            LOG_WARN("fail to read cache tmp_file", KR(ret), K(idx), K(i), K(j), K_(op_ctx), K(macro_id), K(read_offset), 
              K(read_size), KP(read_buf));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    ATOMIC_AAF(&op_ctx_.fail_cnt_, 1);
    ATOMIC_STORE(&op_ctx_.op_ret_, ret);
  }
  return ret;
}

int TestSSLocalCacheOpThread::parallel_write_private_data_macro(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_ctx_.ctx_group_.write_priv_data_macro_) || !op_ctx_.ctx_group_.write_priv_data_macro_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(op_ctx));
  } else {
    const int64_t seq_id = 10000;
    const int64_t tablet_id = 1000 + idx;
    const uint64_t server_id = 1;
    ObArenaAllocator allocator;
    const int64_t file_size = op_ctx_.ctx_group_.write_priv_data_macro_->file_size_;
    const int64_t thread_file_cnt = op_ctx_.ctx_group_.write_priv_data_macro_->thread_file_cnt_;
    char *write_buf = static_cast<char*>(allocator.alloc(file_size));
    LOG_INFO("start parallel write private data macro", K(file_size), K(thread_file_cnt), K(idx));
    if (OB_ISNULL(write_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K_(op_ctx), K(file_size));
    } else if (OB_FAIL(OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(op_ctx_.tenant_id_, op_ctx_.tenant_epoch_id_, tablet_id, 0))) {
      LOG_WARN("fail to create tablet data tablet_id transfer_seq dir", KR(ret), K_(op_ctx), K(tablet_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < thread_file_cnt); ++i) {
        MacroBlockId macro_id = TestSSLocalCacheUtil::gen_macro_block_id(ObStorageObjectType::PRIVATE_DATA_MACRO, tablet_id, 
          seq_id + i, server_id);
        if (OB_FAIL(TestSSLocalCacheUtil::write_data_macro(module_ctx_.file_mgr_, op_ctx_.tenant_id_, macro_id, 
            file_size, write_buf))) {
          LOG_WARN("fail to write private data macro", KR(ret), K_(op_ctx), K(macro_id), K(file_size), KP(write_buf));
        }
      }
    }
    allocator.clear();
  }

  if (OB_FAIL(ret)) {
    ATOMIC_AAF(&op_ctx_.fail_cnt_, 1);
    ATOMIC_STORE(&op_ctx_.op_ret_, ret);
  }
  return ret;
}

int TestSSLocalCacheOpThread::parallel_write_shared_major_data_macro(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_ctx_.ctx_group_.write_shared_major_macro_) || !op_ctx_.ctx_group_.write_shared_major_macro_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(op_ctx));
  } else {
    const int64_t tablet_id = 500 + idx;
    const int64_t data_seq_start = 2000;
    ObArenaAllocator allocator;
    const int64_t macro_size = op_ctx_.ctx_group_.write_shared_major_macro_->macro_size_;
    const int64_t thread_macro_cnt = op_ctx_.ctx_group_.write_shared_major_macro_->thread_macro_cnt_;
    char *write_buf = static_cast<char*>(allocator.alloc(macro_size));
    LOG_INFO("start parallel write shared major data mcro", K(macro_size), K(thread_macro_cnt), K(idx));
    if (OB_ISNULL(write_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K_(op_ctx), K(macro_size));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < thread_macro_cnt); ++i) {
        MacroBlockId macro_id = TestSSLocalCacheUtil::gen_macro_block_id(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, tablet_id, 
          data_seq_start + i, 1);
        if (OB_FAIL(TestSSLocalCacheUtil::write_data_macro(module_ctx_.file_mgr_, op_ctx_.tenant_id_, macro_id, 
            macro_size, write_buf))) {
          LOG_WARN("fail to write shared major data macro", KR(ret), K_(op_ctx), K(macro_id), K(macro_size), KP(write_buf));
        } else {
          SpinWLockGuard guard(lock_);
          if (OB_FAIL(op_ctx_.ctx_group_.write_shared_major_macro_->macro_id_arr_.push_back(macro_id))) {
            LOG_WARN("fail to push back", KR(ret), K(macro_id), K(i), K(idx));
          }
        }
      }
    }
    allocator.clear();
  }

  if (OB_FAIL(ret)) {
    ATOMIC_AAF(&op_ctx_.fail_cnt_, 1);
    ATOMIC_STORE(&op_ctx_.op_ret_, ret);
  }
  return ret;
}

int TestSSLocalCacheOpThread::parallel_read_micro_block(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op_ctx_.ctx_group_.read_micro_) || !op_ctx_.ctx_group_.read_micro_->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(op_ctx));
  } else {
    TestSSParaReadMicroBlockCtx *read_ctx = op_ctx_.ctx_group_.read_micro_;
    const int64_t macro_start_idx = read_ctx->macro_read_start_idx_ + idx * read_ctx->macro_read_cnt_;
    const int64_t macro_end_idx = macro_start_idx + read_ctx->macro_read_cnt_;
    const int64_t micro_size = read_ctx->micro_blk_size_;
    const int64_t micro_cnt = read_ctx->micro_cnt_of_macro_;
    const bool check_data = read_ctx->check_data_;
    const int64_t delay_time_us = read_ctx->delay_time_us_;
    ObArenaAllocator allocator;
    char *read_buf = static_cast<char*>(allocator.alloc(micro_size));
    LOG_INFO("start parallel read micro_block", K(macro_start_idx), K(macro_end_idx), K(idx));
    if (OB_ISNULL(read_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K_(op_ctx), K(micro_size));
    } else {
      for (int64_t i = macro_start_idx; OB_SUCC(ret) && (i < macro_end_idx); ++i) {
        MacroBlockId macro_id = read_ctx->shared_major_macro_ids_.at(i);
        int64_t micro_offset = read_ctx->first_micro_offset_;
        for (int64_t j = 0; OB_SUCC(ret) && (j < micro_cnt); ++j) {
          if (OB_FAIL(TestSSLocalCacheUtil::read_micro_block(module_ctx_.file_mgr_, op_ctx_.tenant_id_, macro_id, 
              micro_offset, micro_size, read_buf, check_data))) {
            LOG_WARN("fail to read micro_block", KR(ret), K_(op_ctx), K(macro_id), K(i), K(j), KP(read_buf), K(check_data));
          } else {
            micro_offset += micro_size;
            if (delay_time_us > 0) {
              ob_usleep(delay_time_us);
            } 
          }
        }
      }
    }
    allocator.clear();
  }

  if (OB_FAIL(ret)) {
    ATOMIC_AAF(&op_ctx_.fail_cnt_, 1);
    ATOMIC_STORE(&op_ctx_.op_ret_, ret);
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
