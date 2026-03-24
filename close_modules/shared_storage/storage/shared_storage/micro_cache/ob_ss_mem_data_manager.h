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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_DATA_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_DATA_MANAGER_H_

#include <stdint.h>
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{

/* To manage cached micro block data which is stored in memory temporarily.
 *
 * When these data is persisted into physical block, these data can be droped.
 */
class ObSSMemDataManager
{
public:
  ObSSMemDataManager(ObSSMicroCacheStat &cache_stat);
  virtual ~ObSSMemDataManager() {}

  bool is_inited() const { return is_inited_; }
  int init(const uint64_t tenant_id, const int64_t cache_file_size, const uint32_t block_size, const bool is_mini_mode);
  int add_micro_block_data(const ObSSMicroBlockCacheKey &micro_key, const char *content, const int32_t size, 
                           ObSSMemBlockHandle &mem_blk_handle, uint32_t &crc);
  int get_sealed_mem_blocks(common::ObIArray<ObSSMemBlockHandle> &sealed_mem_blk_arr, const int64_t max_cnt);
  int add_into_sealed_block_list(ObSSMemBlockHandle &sealed_mem_blk_handle);
  int64_t get_sealed_mem_block_cnt() const;
  int get_max_mem_blk_count(int64_t &max_cnt);
  int64_t get_free_mem_blk_cnt(const bool is_fg) const;
  void destroy();
  void clear_mem_data_manager();

  // for reorganize_task
  int add_bg_micro_block_data(const ObSSMicroBlockCacheKey &micro_key, const char *content, const int32_t size, 
                              ObSSMemBlockHandle &mem_blk_handle, uint32_t &crc);
  int seal_and_alloc_new_bg_mem_blk();// if bg_mem_blk is null, create new; otherwise, seal old and create new

  // for test
  int get_micro_block_data(const ObSSMicroBlockCacheKey &micro_key, ObSSMemBlockHandle &mem_blk_handle, char *buf, 
                           const int32_t size, const uint32_t crc);

private:
  void cal_mem_blk_cnt(const bool is_mini_mode, const uint64_t tenant_id, const int64_t cache_file_size, 
                       int64_t &def_cnt, int64_t &max_cnt, int64_t &max_bg_cnt);
  int inner_seal_and_alloc_fg_mem_block();  // if fg_mem_blk is null, create new; otherwise, seal old and create new
  int inner_alloc_bg_mem_block_if_need(); // if bg_mem_blk is null, create new
  int do_alloc_mem_block(ObSSMemBlock *&mem_blk, const bool is_fg);
  int free_remained_mem_block();
  int inner_free_mem_blocks(common::ObFixedQueue<ObSSMemBlock> &mem_blocks);
  int do_free_mem_block(ObSSMemBlock *mem_block);
  int pop_sealed_mem_blocks(const bool is_fg, common::ObIArray<ObSSMemBlockHandle> &sealed_mem_blk_arr, 
                            const int64_t max_cnt);
  int handle_uncomplete_sealed_mem_block();

private:
  bool is_inited_;
  uint32_t block_size_;
  uint64_t tenant_id_;
  common::SpinRWLock lock_;
  ObSSMicroCacheStat &cache_stat_;
  ObSSMemBlockPool mem_block_pool_; // used for foreground & background IO to write micro_block
  // TODO @donglou.zl Consider a more space-efficient and write-efficient usage of mem_blk
  ObSSMemBlock *fg_mem_block_; // used for storing new added micro_block's data of foreground request.
  ObSSMemBlock *bg_mem_block_;  // used for background reorganize task to store reaggregated micro_blocks' data
  common::ObFixedQueue<ObSSMemBlock> fg_sealed_mem_blocks_;
  common::ObFixedQueue<ObSSMemBlock> bg_sealed_mem_blocks_;
  common::ObFixedQueue<ObSSMemBlock> uncomplete_sealed_mem_blocks_; // if not finish updating all micro_meta, add into this one
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_DATA_MANAGER_H_ */
