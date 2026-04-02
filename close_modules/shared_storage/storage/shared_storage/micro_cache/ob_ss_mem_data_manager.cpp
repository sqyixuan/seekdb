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
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_mem_data_manager.h"
namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSMemDataManager------------------------------------------*/
ObSSMemDataManager::ObSSMemDataManager(ObSSMicroCacheStat &cache_stat)
  : is_inited_(false), block_size_(0), tenant_id_(OB_INVALID_TENANT_ID), lock_(), cache_stat_(cache_stat), 
    mem_block_pool_(cache_stat), fg_mem_block_(nullptr), bg_mem_block_(nullptr), fg_sealed_mem_blocks_(),
    bg_sealed_mem_blocks_()
{}

void ObSSMemDataManager::destroy()
{
  free_remained_mem_block();
  fg_sealed_mem_blocks_.destroy();
  bg_sealed_mem_blocks_.destroy();
  uncomplete_sealed_mem_blocks_.destroy();
  mem_block_pool_.destroy();
  block_size_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObSSMemDataManager::clear_mem_data_manager()
{
  free_remained_mem_block();
}

int ObSSMemDataManager::free_remained_mem_block()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    // ignore ret here.
    if (OB_SUCCESS != (ret = inner_free_mem_blocks(fg_sealed_mem_blocks_))) {
      LOG_WARN("fail to free sealed fg_mem_blocks", KR(ret));
    }
    if (OB_SUCCESS != (ret = inner_free_mem_blocks(bg_sealed_mem_blocks_))) {
      LOG_WARN("fail to free sealed bg_mem_blocks", KR(ret));
    }
    if (OB_SUCCESS != (ret = inner_free_mem_blocks(uncomplete_sealed_mem_blocks_))) {
      LOG_WARN("fail to free sealed uncomplete sealed mem_blocks", KR(ret));
    }
    if (nullptr != fg_mem_block_) {
      do_free_mem_block(fg_mem_block_);
      fg_mem_block_ = nullptr;
    }
    if (nullptr != bg_mem_block_) {
      do_free_mem_block(bg_mem_block_);
      bg_mem_block_ = nullptr;
    }
  }
  return ret;
}

int ObSSMemDataManager::inner_free_mem_blocks(ObFixedQueue<ObSSMemBlock> &mem_blocks)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ObSSMemBlock *tmp_mem_blk = nullptr;
    bool has_more = true;
    int64_t total_cnt = 0;
    int64_t succ_cnt = 0;
    while (OB_SUCC(ret) && has_more) {
      if (OB_FAIL(mem_blocks.pop(tmp_mem_blk))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          has_more = false;
        } else {
          LOG_WARN("fail to pop from fixed_queue", KR(ret));
        }
      } else if (nullptr != tmp_mem_blk) {
        ++total_cnt;
        if (OB_FAIL(do_free_mem_block(tmp_mem_blk))) {
          LOG_WARN("fail to do free mem_block", KR(ret), K(tmp_mem_blk));
        } else {
          ++succ_cnt;
        }
      }
      ret = OB_SUCCESS; // continue to pop next one
    }

    if (OB_UNLIKELY(total_cnt != succ_cnt)) {
      LOG_WARN("occur error when free mem_blocks", K(total_cnt), K(succ_cnt));
    }
  }
  return ret;
}

// 1. For already persisted mem_block, we will try to free/delete it.
// 2. When destroy mem_data_manager, we need to free these sealed_mem_blocks which still not persist.
int ObSSMemDataManager::do_free_mem_block(ObSSMemBlock *mem_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(mem_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mem_block));
  } else if (OB_FAIL(mem_block->try_free())) {
    LOG_WARN("fail to try free mem_block", KR(ret), KP(mem_block), KPC(mem_block));
  }
  return ret;
}

void ObSSMemDataManager::cal_mem_blk_cnt(
    const bool is_mini_mode, 
    const uint64_t tenant_id, 
    const int64_t cache_file_size, 
    int64_t &def_cnt, 
    int64_t &max_cnt, 
    int64_t &max_bg_cnt)
{
  const bool is_mini_mem = (is_mini_mode || is_meta_tenant(tenant_id));

  if (is_mini_mem) {
    def_cnt = MINI_MODE_BASE_MEM_BLK_CNT;
    const int64_t dynamic_cnt = MIN_DYNAMIC_MEM_BLK_CNT;
    max_cnt = def_cnt + dynamic_cnt;
    max_bg_cnt = MINI_MODE_MAX_BG_MEM_BLK_CNT;
  } else {
    const int64_t memory_limit = lib::get_tenant_memory_limit(tenant_id);
    const int64_t base_cnt = BASE_MEM_BLK_CNT;
    const int64_t scale_cnt_by_disk = cache_file_size / DISK_SIZE_PER_MEM_BLK;
    const int64_t scale_cnt_by_mem = memory_limit / MEMORY_SIZE_PER_MEM_BLK;
    def_cnt = MIN(base_cnt + MIN(scale_cnt_by_disk, scale_cnt_by_mem), MAX_MEM_BLK_CNT / 2);

    const int64_t dynamic_cnt =
        MIN(MIN_DYNAMIC_MEM_BLK_CNT + memory_limit / MEMORY_SIZE_PER_MEM_BLK, MAX(0, MAX_MEM_BLK_CNT - def_cnt));

    max_cnt = def_cnt + dynamic_cnt;
    max_bg_cnt = MAX_BG_MEM_BLK_CNT;
  }
}

int ObSSMemDataManager::init(
    const uint64_t tenant_id, 
    const int64_t cache_file_size, 
    const uint32_t block_size,
    const bool is_mini_mode)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(block_size));
  } else {
    int64_t def_pool_cnt = 0;
    int64_t max_pool_cnt = 0;
    int64_t max_bg_mem_blk_cnt = 0;
    cal_mem_blk_cnt(is_mini_mode, tenant_id, cache_file_size, def_pool_cnt, max_pool_cnt, max_bg_mem_blk_cnt);
    const int64_t max_fg_mem_blk_cnt = max_pool_cnt - max_bg_mem_blk_cnt;

    if (OB_FAIL(mem_block_pool_.init(tenant_id, block_size, def_pool_cnt, max_pool_cnt, max_bg_mem_blk_cnt))) {
      LOG_WARN("fail to init mem_block_pool", KR(ret), K(tenant_id), K(block_size), K(def_pool_cnt), 
        K(max_pool_cnt), K(max_bg_mem_blk_cnt));
    } else if (OB_FAIL(fg_sealed_mem_blocks_.init(max_fg_mem_blk_cnt))) {
      LOG_WARN("fail to init fg_sealed_mem_blocks", KR(ret), K(max_fg_mem_blk_cnt));
    } else if (OB_FAIL(bg_sealed_mem_blocks_.init(max_bg_mem_blk_cnt))) {
      LOG_WARN("fail to init bg_sealed_mem_blocks", KR(ret), K(max_bg_mem_blk_cnt));
    } else if (OB_FAIL(uncomplete_sealed_mem_blocks_.init(max_pool_cnt))) {
      LOG_WARN("fail to init uncomplete_sealed_mem_blocks", KR(ret), K(max_pool_cnt));
    } else {
      block_size_ = block_size;
      tenant_id_ = tenant_id;
      is_inited_ = true;
      LOG_INFO("succ to init ss_mem_blk_mgr", K_(tenant_id), K(is_mini_mode), K(def_pool_cnt), K(max_pool_cnt),
        K(max_fg_mem_blk_cnt), K(max_bg_mem_blk_cnt));
    }
  }
  return ret;
}

int ObSSMemDataManager::add_micro_block_data(
    const ObSSMicroBlockCacheKey &micro_key, 
    const char *content, 
    const int32_t size, 
    ObSSMemBlockHandle &mem_blk_handle,
    uint32_t &crc)
{
  int ret = OB_SUCCESS;

  int32_t data_offset = -1; // start pos for writing micro_block data
  int32_t idx_offset = -1;  // start pos for writing micro_block_index
  mem_blk_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), K(micro_key), K(size));
  } else if (OB_ISNULL(content) || OB_UNLIKELY(!micro_key.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size), KP(content));
  } else {
    SpinWLockGuard guard(lock_);
    bool need_alloc_blk = (nullptr == fg_mem_block_);
    if (nullptr != fg_mem_block_) {
      if (OB_FAIL(fg_mem_block_->calc_write_location(micro_key, size, data_offset, idx_offset))) {
        if (OB_SIZE_OVERFLOW == ret) {
          need_alloc_blk = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to calc location for fg_mem_blk", KR(ret), K(micro_key), K(size), K_(fg_mem_block));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (need_alloc_blk) {
        if (OB_FAIL(inner_seal_and_alloc_fg_mem_block())) {
          LOG_WARN("fail to seal and alloc new fg_mem_blk", KR(ret), K(micro_key), K(size), K_(fg_mem_block));
        } else if (OB_FAIL(fg_mem_block_->calc_write_location(micro_key, size, data_offset, idx_offset))) {
          LOG_WARN("fail to calc location for fg_mem_blk", KR(ret), K(micro_key), K(size), K_(fg_mem_block));
        }
      }

      if (OB_SUCC(ret)) {
        mem_blk_handle.set_ptr(fg_mem_block_);
      }
    }
  }

  if (OB_FAIL(ret)) {
    mem_blk_handle.reset();
  } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mem_block handle should be valid", KR(ret), K(micro_key), K(size), K(mem_blk_handle));
  } else if (OB_FAIL(mem_blk_handle()->write_micro_data(micro_key, content, size, data_offset, 
             idx_offset, crc))) {
    LOG_WARN("fail to write micro_block data", KR(ret), K(micro_key), K(size), K(data_offset), K(idx_offset),
      K(mem_blk_handle), KPC(mem_blk_handle.get_ptr()));
  }
  return ret;
}

// Differ from 'add_micro_block_data', it creates new mem_block only when bg_mem_block is null.
// It doesn't need to 'seal-and-create'. So it may return OB_SIZE_OVERFLOW.
int ObSSMemDataManager::add_bg_micro_block_data(
    const ObSSMicroBlockCacheKey &micro_key, 
    const char *content, 
    const int32_t size, 
    ObSSMemBlockHandle &mem_blk_handle,
    uint32_t &crc)
{
  int ret = OB_SUCCESS;

  mem_blk_handle.reset();
  int32_t data_offset = -1;
  int32_t idx_offset = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), K(micro_key), K(size));
  } else if (OB_ISNULL(content) || OB_UNLIKELY(!micro_key.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(size), KP(content));
  } else {
    // Now, add_bg_micro_block_data will only be used in reorganize_task, it runs in a single thread, thus
    // we don't use lock
    if (OB_FAIL(inner_alloc_bg_mem_block_if_need())) {
      LOG_WARN("fail to alloc bg_mem_block if need", KR(ret), K(micro_key), K(size), K_(bg_mem_block));
    } else if (OB_FAIL(bg_mem_block_->calc_write_location(micro_key, size, data_offset, idx_offset))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("fail to calc location for bg_mem_blk", KR(ret), K(micro_key), K(size), K_(bg_mem_block));
      }
    } else {
      mem_blk_handle.set_ptr(bg_mem_block_);
    }
  }

  if (OB_FAIL(ret)) {
    mem_blk_handle.reset();
  } else if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mem_block handle should be valid", KR(ret), K(micro_key), K(size), K(mem_blk_handle));
  } else if (OB_FAIL(mem_blk_handle()->write_micro_data(micro_key, content, size, data_offset, idx_offset, crc))) {
    LOG_WARN("fail to write micro_block data", KR(ret), K(micro_key), K(size), K(data_offset), K(idx_offset), 
      K(mem_blk_handle), KPC(mem_blk_handle.get_ptr()));
  }
  return ret;
}

int ObSSMemDataManager::inner_seal_and_alloc_fg_mem_block()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_NOT_NULL(fg_mem_block_)) {
    if (OB_FAIL(fg_sealed_mem_blocks_.push(fg_mem_block_))) {
      LOG_WARN("fail to push sealed fg_mem_block into queue", KR(ret), KPC_(fg_mem_block));
    } else {
      fg_mem_block_ = nullptr;
    }
  }
  if (FAILEDx(do_alloc_mem_block(fg_mem_block_, true/*is_fg*/))) {
    LOG_WARN("fail to alloc fg_mem_block", KR(ret));
  }
  return ret;
}

int ObSSMemDataManager::inner_alloc_bg_mem_block_if_need()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_LIKELY(nullptr == bg_mem_block_)) {
    if (OB_FAIL(do_alloc_mem_block(bg_mem_block_, false/*is_fg*/))) {
      LOG_WARN("fail to alloc bg_mem_block", KR(ret));
    }
  }
  return ret;
}

int ObSSMemDataManager::seal_and_alloc_new_bg_mem_blk()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_NOT_NULL(bg_mem_block_)) {
    if (OB_FAIL(bg_sealed_mem_blocks_.push(bg_mem_block_))) {
      LOG_WARN("fail to push sealed bg_mem_block into queue", KR(ret), KPC_(bg_mem_block));
    } else {
      bg_mem_block_ = nullptr;
    }
  }
  if (FAILEDx(do_alloc_mem_block(bg_mem_block_, false/*is_fg*/))) {
    LOG_WARN("fail to alloc bg_mem_block", KR(ret));
  }
  return ret;
}

int ObSSMemDataManager::do_alloc_mem_block(ObSSMemBlock *&mem_blk, const bool is_fg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(mem_block_pool_.alloc(mem_blk, is_fg))) {
    LOG_WARN("fail to alloc mem_block", KR(ret), K(is_fg));
  } else if (OB_ISNULL(mem_blk)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_block should not be null", KR(ret));
  }
  return ret;
}

int ObSSMemDataManager::get_micro_block_data(
    const ObSSMicroBlockCacheKey &micro_key,
    ObSSMemBlockHandle &mem_blk_handle, 
    char *buf, 
    const int32_t size,
    const uint32_t crc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(!micro_key.is_valid() || !mem_blk_handle.is_valid() || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(mem_blk_handle), KP(buf), K(size));
  } else if (OB_FAIL(mem_blk_handle()->get_micro_data(micro_key, buf, size, crc))) {
    LOG_WARN("fail to get micro block data from mem_block", KR(ret), K(micro_key), K(size), K(crc));
  }
  return ret;
}

int ObSSMemDataManager::get_sealed_mem_blocks(
    ObIArray<ObSSMemBlockHandle> &sealed_mem_blk_arr, 
    const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    if (OB_FAIL(pop_sealed_mem_blocks(false, sealed_mem_blk_arr, max_cnt))) {
      LOG_WARN("fail to pop sealed bg_mem_blocks", KR(ret), K(sealed_mem_blk_arr), K(max_cnt));
    } else if (OB_FAIL(pop_sealed_mem_blocks(true, sealed_mem_blk_arr, max_cnt))) {
      LOG_WARN("fail to pop sealed fg_mem_blocks", KR(ret), K(sealed_mem_blk_arr), K(max_cnt));
    } else if (OB_FAIL(handle_uncomplete_sealed_mem_block())) {
      LOG_WARN("fail to handle uncomplete sealed mem_blocks", KR(ret));
    }
  }
  return ret;
}

int ObSSMemDataManager::pop_sealed_mem_blocks(
    const bool is_fg, 
    ObIArray<ObSSMemBlockHandle> &sealed_mem_blk_arr,
    const int64_t max_cnt)
{
  int ret= OB_SUCCESS;
  ObFixedQueue<ObSSMemBlock> &sealed_mem_blocks = is_fg ? fg_sealed_mem_blocks_ : bg_sealed_mem_blocks_;
  bool pop_finish = false;
  while (OB_SUCC(ret) && !pop_finish && (sealed_mem_blk_arr.count() < max_cnt)) {
    ObSSMemBlock *mem_block = nullptr;
    if (OB_FAIL(sealed_mem_blocks.pop(mem_block))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        pop_finish = true;
      } else {
        LOG_WARN("fail to pop sealed mem_blk", KR(ret), K(is_fg));
      }
    } else if (OB_NOT_NULL(mem_block)) {
      if (OB_UNLIKELY(is_fg != mem_block->is_fg_mem_blk())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mem_block attribute mismatch", KR(ret), K(is_fg), KPC(mem_block));
      } else if (OB_LIKELY(mem_block->is_completed())) {
        ObSSMemBlockHandle mem_blk_handle;
        mem_blk_handle.set_ptr(mem_block);
        if (OB_FAIL(sealed_mem_blk_arr.push_back(mem_blk_handle))) {
          LOG_WARN("fail to push back", KR(ret), KP(mem_block), KPC(mem_block));
        }
      } else if (OB_FAIL(uncomplete_sealed_mem_blocks_.push(mem_block))) {
        LOG_ERROR("fail to add into uncomplete mem_block list", KR(ret), KP(mem_block), KPC(mem_block));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sealed mem_block should not be null", KR(ret), K(is_fg));
    }
  }
  return ret;
}

int ObSSMemDataManager::handle_uncomplete_sealed_mem_block()
{
  int ret = OB_SUCCESS;
  bool pop_finish = false;
  do {
    ObSSMemBlock *mem_block = nullptr;
    if (OB_FAIL(uncomplete_sealed_mem_blocks_.pop(mem_block))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        pop_finish = true;
      } else {
        LOG_WARN("fail to pop uncomplete sealed mem_blk", KR(ret));
      }
    } else if (OB_NOT_NULL(mem_block)) {
      const bool is_fg = mem_block->is_fg_mem_blk();
      ObFixedQueue<ObSSMemBlock> &sealed_mem_blocks = is_fg ? fg_sealed_mem_blocks_ : bg_sealed_mem_blocks_;
      if (OB_FAIL(sealed_mem_blocks.push(mem_block))) {
        LOG_WARN("fail to push", KR(ret), K(is_fg), KP(mem_block), KPC(mem_block));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("uncomplete sealed mem_block should not be null", KR(ret));
    }
  } while (OB_SUCC(ret) && !pop_finish);
  return ret;
}

int ObSSMemDataManager::add_into_sealed_block_list(ObSSMemBlockHandle &sealed_mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!sealed_mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sealed_mem_blk_handle));
  } else {
    const bool is_fg = sealed_mem_blk_handle()->is_fg_mem_blk();
    ObFixedQueue<ObSSMemBlock> &sealed_mem_blocks = is_fg ? fg_sealed_mem_blocks_ : bg_sealed_mem_blocks_;
    if (OB_FAIL(sealed_mem_blocks.push(sealed_mem_blk_handle.get_ptr()))) {
      LOG_WARN("fail to push into fixed queue", KR(ret), K(sealed_mem_blk_handle), K(sealed_mem_blocks.get_total()),
        K(sealed_mem_blocks.get_free()), K(sealed_mem_blocks.get_curr_total()), K(sealed_mem_blocks.capacity()));
    }
  }
  return ret;
}

int ObSSMemDataManager::get_max_mem_blk_count(int64_t &max_cnt) 
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else {
    max_cnt = mem_block_pool_.get_max_count();
  }
  return ret;
}

int64_t ObSSMemDataManager::get_sealed_mem_block_cnt() const
{
  return fg_sealed_mem_blocks_.get_curr_total() + bg_sealed_mem_blocks_.get_curr_total();
}

int64_t ObSSMemDataManager::get_free_mem_blk_cnt(const bool is_fg) const
{
  return mem_block_pool_.get_free_mem_blk_cnt(is_fg);
}

} // namespace storage
} // namespace oceanbase
