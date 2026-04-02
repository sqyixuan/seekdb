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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_OP_H_

#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_op_base.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace storage
{
struct SSPersistOPEntry
{
public:
  int64_t phy_blk_idx_;
  int64_t updated_micro_cnt_;
  ObSSPhysicalBlockHandle phy_blk_handle_;
  ObSSMemBlockHandle mem_blk_handle_;
  common::ObIOHandle io_handle_;

  SSPersistOPEntry();
  ~SSPersistOPEntry() { reset(); }
  int init(const int64_t phy_blk_idx, ObSSPhysicalBlock *phy_blk, ObSSMemBlock *mem_blk);
  bool is_valid() const
  {
    return (phy_blk_idx_ >= 0) && phy_blk_handle_.is_valid() && mem_blk_handle_.is_valid();
  }
  void reset();
  TO_STRING_KV(K_(phy_blk_idx), K_(updated_micro_cnt), K_(phy_blk_handle), K_(mem_blk_handle));
};

class ObSSPersistMicroOp : public ObSSMicroCacheOpBase
{
public:
  ObSSPersistMicroOp();
  virtual ~ObSSPersistMicroOp() { destroy(); }
  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();
  int execute_persist_micro();
  int64_t get_alloc_blk_fail_cnt() const { return alloc_blk_fail_cnt_; }

private:
  int pre_alloc_persist_entry();
  int persist_sealed_mem_blocks();
  int async_write_sealed_mem_blocks(ObArray<ObSSMemBlockHandle> &sealed_mem_blk_handles);
  int do_async_write_sealed_mem_block(SSPersistOPEntry &persist_entry, const ObSSMemBlockHandle &sealed_mem_blk_handle);
  int update_micro_meta_of_phy_blocks();
  int do_update_micro_meta_of_phy_block(SSPersistOPEntry &persist_entry);
  int handle_sealed_mem_block(const int64_t updated_micro_cnt, const bool valid_phy_blk, ObSSMemBlockHandle &mem_blk_handle);

private:
  static constexpr int64_t PER_ROUND_WATI_US = 10; // 10us

private:
  int64_t max_mem_blk_cnt_;
  int64_t alloc_blk_fail_cnt_;
  SSPersistOPEntry *persist_entry_arr_;
  common::ObFixedQueue<SSPersistOPEntry> free_list_;
  common::ObFixedQueue<SSPersistOPEntry> in_use_list_; // sealed_mem_blocks that succeed to async write
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_OP_H_ */
