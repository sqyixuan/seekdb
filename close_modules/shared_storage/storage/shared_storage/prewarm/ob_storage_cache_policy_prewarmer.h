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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_STORAGE_CACHE_POLICY_PREWARMER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_STORAGE_CACHE_POLICY_PREWARMER_H_

#include "storage/tx_storage/ob_ls_service.h"
#include "storage/blocksstable/index_block/ob_index_block_bare_iterator.h"

namespace oceanbase
{
namespace storage
{

class ObStorageCacheTabletTask;

int is_suspend_storage_cache_task(const uint64_t tenant_id, bool &is_suspend);

class ObMacroBlockAsyncReader
{
public:
  ObMacroBlockAsyncReader(const int64_t parallelism);
  virtual ~ObMacroBlockAsyncReader();
  void reset_io_resources();

  int async_read_from_object_storage(const blocksstable::MacroBlockId &macro_id);
  int batch_read_from_object_storage_with_retry(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      const int64_t start_idx,
      const int64_t batch_size);
  int batch_wait();

protected:
  static const int64_t MAX_PARALLELISM = 64;
  static const int64_t MIN_PARALLELISM = 1;

  common::ObArenaAllocator allocator_;
  int64_t read_handle_idx_;
  const int64_t used_parallelism_;

  blocksstable::ObStorageObjectHandle read_handles_[MAX_PARALLELISM];
};

struct TabletMajorPrewarmStat
{
public:
  TabletMajorPrewarmStat() { reset(); }

  void reset()
  {
    macro_block_num_ = 0;
    macro_block_bytes_ = 0;
    macro_block_fail_cnt_ = 0;
    micro_block_num_ = 0;
    micro_block_bytes_ = 0;
    micro_block_hit_cnt_ = 0;
    micro_block_fail_cnt_ = 0;
    micro_block_add_cnt_ = 0;
    micro_cache_max_available_size_ = 0;
  }

  void update_macro_block_num_and_bytes(const int64_t delta_cnt, const int64_t delta_size)
  {
    macro_block_num_ += delta_cnt;
    macro_block_bytes_ += delta_size;
  }
  int64_t get_macro_block_num() const { return macro_block_num_; }
  int64_t get_macro_block_bytes() const { return macro_block_bytes_; }

  void inc_macro_block_fail_cnt()
  {
    macro_block_fail_cnt_++;
  }

  void update_micro_block_num_and_bytes(const int64_t delta_cnt, const int64_t delta_size)
  {
    micro_block_num_ += delta_cnt;
    micro_block_bytes_ += delta_size;
  }
  int64_t get_micro_block_num() const { return micro_block_num_; }
  int64_t get_micro_block_bytes() const { return micro_block_bytes_; }

  void inc_micro_block_hit_cnt()
  {
    micro_block_hit_cnt_++;
  }

  void inc_micro_block_fail_cnt()
  {
    micro_block_fail_cnt_++;
  }

  void inc_micro_block_add_cnt()
  {
    micro_block_add_cnt_++;
  }
  
  void set_max_micro_cache_size(const int64_t micro_cache_max_available_size)
  {
    micro_cache_max_available_size_ = micro_cache_max_available_size;
  }

  int64_t get_micro_cache_max_available_size() const
  {
    return micro_cache_max_available_size_;
  }
  TO_STRING_KV(K(macro_block_num_), K(macro_block_bytes_), K(macro_block_fail_cnt_),
      K(micro_block_num_), K(micro_block_bytes_), K(micro_block_hit_cnt_), 
      K(micro_block_fail_cnt_), K(micro_block_add_cnt_), K(micro_cache_max_available_size_));

private:
  int64_t macro_block_num_;
  int64_t macro_block_bytes_;
  int64_t macro_block_fail_cnt_;
  int64_t micro_block_num_;
  int64_t micro_block_bytes_;
  int64_t micro_block_hit_cnt_;
  int64_t micro_block_fail_cnt_;
  int64_t micro_block_add_cnt_;
  int64_t micro_cache_max_available_size_;
};

// Parallelism not supported
// Usage:
//    ObStorageCachePolicyPrewarmer prewarmer;
//    OK(prewarmer.prewarm_hot_tablet(ls_id, tablet_id));
//    TabletMajorPrewarmStat stat = prewarmer.get_tablet_major_prewarm_stat();
class ObStorageCachePolicyPrewarmer : public ObMacroBlockAsyncReader
{
public:
  ObStorageCachePolicyPrewarmer(const int64_t parallelism = MAX_PARALLELISM);
  virtual ~ObStorageCachePolicyPrewarmer();

  int prewarm_hot_tablet(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      ObStorageCacheTabletTask *task);
  const TabletMajorPrewarmStat &get_tablet_major_prewarm_stat() { return stat_; }
  
private:
  // The SHARED_MAJOR_META_MACRO block may store tablet meta,
  // and in this case, its structure differs from that of a standard macro block.
  // It is not organized into micro blocks
  // and uses ObSharedObjectHeader as the header instead of ObMacroBlockCommonHeader.
  // This func is used to check whether the meta macro block is a shared object
  static bool is_shared_object_meta_macro_(
      const blocksstable::MacroBlockId &macro_id,
      const char *macro_block_buf,
      const int64_t macro_block_size);

  static int get_major_blocks_(
      const common::ObTabletID &tablet_id,
      ObIArray<blocksstable::MacroBlockId> &block_ids);
  
  int prewarm_major_macro_(
      const blocksstable::MacroBlockId &macro_id,
      const char *macro_block_buf,
      const int64_t macro_block_size);
  
  int prewarm_major_micro_(
      const ObSSMicroBlockCacheKey &micro_key,
      const blocksstable::ObMicroBlockData &micro_data);
  
  int open_iterator_(
      const MacroBlockId &macro_id,
      const char *macro_block_buf,
      const int64_t macro_block_size,
      blocksstable::ObMicroBlockBareIterator &micro_iter,
      blocksstable::ObIndexBlockBareIterator &idx_micro_iter);

private:
  TabletMajorPrewarmStat stat_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_STORAGE_CACHE_POLICY_PREWARMER_H_ */
