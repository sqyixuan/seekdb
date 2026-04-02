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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_MANAGER_H_

#include <stdint.h>
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/shared_storage/micro_cache/ob_ss_arc_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_basic_op.h"

namespace oceanbase
{
namespace obrpc
{
struct ObSSMicroMetaInfo;
}
namespace storage
{
struct ObSSMicroCacheStat;
class ObSSLinkedPhyBlockItemReader;

struct SSReplayMicroCkptCtx
{
public:
  int64_t total_replay_cnt_;
  int64_t invalid_t1_micro_cnt_;
  int64_t invalid_t2_micro_cnt_;
  int64_t b1_micro_cnt_;
  int64_t b2_micro_cnt_;

  SSReplayMicroCkptCtx() { reset(); }
  void reset();

  void inc_total_replay_cnt();
  void inc_invalid_micro_cnt(const bool is_in_l1, const bool is_in_ghost);
  void inc_ghost_micro_cnt(const bool is_in_l1, const bool is_in_ghost);

  TO_STRING_KV(K_(total_replay_cnt), K_(invalid_t1_micro_cnt), K_(invalid_t2_micro_cnt),
    K_(b1_micro_cnt), K_(b2_micro_cnt));
};

/*
 * To manage all macro_blocks' meta and micro_blocks' meta
 */
class ObSSMicroMetaManager
{
public:
  typedef common::ObLinearHashMap<const ObSSMicroBlockCacheKey *, ObSSMicroBlockMetaHandle> SSMicroMap;

  ObSSMicroMetaManager(ObSSMicroCacheStat &cache_stat);
  virtual ~ObSSMicroMetaManager() {}

  bool is_inited() const { return is_inited_; }
  bool is_mem_limited() const { return ATOMIC_LOAD(&mem_limited_); }
  void set_mem_limited(const bool mem_limited) { ATOMIC_STORE(&mem_limited_, mem_limited); }
  int init(const uint64_t tenant_id, const bool is_mini_mode, const int32_t block_size,
           const int64_t cache_limit_size, common::ObIAllocator &allocator);
  void destroy();
  void clear_micro_meta_manager();
  const ObSSARCInfo &get_arc_info() const { return arc_info_; }
  int update_arc_limit(const int64_t new_cache_limit_size);
  int update_arc_work_limit_for_prewarm(const bool start_update);
  void update_cache_mem_limit_by_config();
  void update_cache_expiration_time_by_config();
  bool check_cache_mem_limit();
  int64_t get_micro_cnt_limit() const { return ATOMIC_LOAD(&micro_cnt_limit_); }
  int64_t get_cache_expiration_time() const { return ATOMIC_LOAD(&expiration_time_s_); }

  int alloc_micro_block_meta(ObSSMicroBlockMeta *&micro_meta);
  int add_or_update_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, const int32_t micro_size,
      const uint32_t micro_crc, ObSSMemBlockHandle &mem_blk_handle, bool &is_first_add);
  /*
   * To evict micro_meta based on arc algorithm
   */
  int try_evict_micro_block_meta(const ObSSMicroMetaSnapshot &cold_micro);
  /*
   * To delete micro_meta based on arc algorithm
   */
  int try_delete_micro_block_meta(const ObSSMicroMetaSnapshot &cold_micro);
  /*
   * When execute ckpt, delete T1/T2 invalid micro_meta
   */
  int try_delete_invalid_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key);
  /*
   * When execute ckpt, delete expired micro_meta
   */
  int try_delete_expired_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBaseInfo &expired_micro_info);
  /*
   * If some micro_meta of this mem_blk failed to persist, need to delete them
   */
  int try_delete_micro_block_meta(ObSSMemBlockHandle &sealed_mem_blk_handle);
  /*
   * To mark this micro_block_meta as invalid
   */
  int try_invalidate_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, bool &succ_invalidate);
  /*
   * If the micro_meta exists and is valid, return the micro_meta_handle
   */
  int get_micro_block_meta_handle(const ObSSMicroBlockCacheKey &micro_key,
                                  ObSSMicroBlockMetaHandle &micro_meta_handle,
                                  const bool update_arc);
  /*
   * Get the micro_block_meta handle if exists in micro_cache. Else return OB_ENTRY_NOT_EXIST
   * Cuz the upper layer needs to read micro_block data later, thus we need to hold the mem_blk
   * or phy_blk handle. A micro_block's data will be cached in a mem_block or a phy_block.
   */
  int get_micro_block_meta_handle(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBaseInfo &micro_info,
                                  ObSSMicroBlockMetaHandle &micro_meta_handle, ObSSMemBlockHandle &mem_blk_handle,
                                  ObSSPhysicalBlockHandle &phy_blk_handle, const bool update_arc);
  /*
   * After persist micro_block data, need to update micro_blocks' meta: location, is_persisted, etc.
   */
  int update_micro_block_meta(ObSSMemBlockHandle &sealed_mem_blk_handle, const int64_t block_offset,
                              const uint32_t reuse_version, int64_t &updated_micro_size, int64_t &update_micro_cnt);
  /*
   * To support prewarm, update micro_meta's arc_seg and access_time
   */
  int update_micro_block_meta_heat(const common::ObIArray<ObSSMicroBlockCacheKey> &micro_keys, const bool transfer_seg,
                                   const bool update_access_time, const int64_t time_delta_s);
  /*
   * Used for reorganize_task, when start reorganizing one micro_block, mark its 'is_reorganizing' as true
   */
  int try_mark_reorganizing_if_exist(const ObSSMicroBlockCacheKey &micro_key, const int64_t phy_blk_idx,
                                     const int32_t blk_offset, ObSSMicroBlockMetaHandle &micro_meta_handle);
  /*
   * Used for reorganize_task, when finish reorganizing one micro_block, mark its 'is_reorganizing' as false
   * Also need to update its new meta
   */
  int try_unmark_reorganizing_if_exist(const ObSSMicroBlockCacheKey &micro_key, ObSSMemBlockHandle &mem_blk_handle);
  /*
   * If we choose some micro_blocks to start reorgan_task, but not finish them. Then we need to mark its
   * 'is_reorganizing' as false
   */
  int force_unmark_reorganizing(const ObSSMicroBlockCacheKey &micro_key);
  /*
   * For one arc_seg(T1/T2/B1/B2), choose some cold micro_meta to evict or delete
   */
  int acquire_cold_micro_blocks(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx);
  /*
   * Scan all micro_meta for checkpoint
   */
  int scan_micro_blocks_for_checkpoint(common::ObIArray<ObSSMicroBlockMetaHandle> &ckpt_micro_handles,
                                       common::ObIArray<ObSSMicroBlockCacheKey> &invalid_micro_keys,
                                       common::ObIArray<ObSSMicroBlockCacheKey> &expired_micro_keys,
                                       ObSSTabletCacheMap &tablet_cache_map,
                                       const bool is_first_scan);
  /*
   * When restart observer, if exists valid micro_meta ckpt, need to replay it to rebuild micro_meta_map
   */
  int read_micro_meta_checkpoint(ObSSLinkedPhyBlockItemReader &item_reader, const int64_t micro_ckpt_time_us);

  // below for ob_admin tool
  int get_micro_meta_handle(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
  int clear_tablet_micro_meta(const common::ObTabletID &tablet_id, int64_t &remain_micro_cnt);

private:
  int do_add_or_update_micro_block_meta(const ObSSMicroBlockCacheKey &micro_key, const int32_t micro_size,
      const uint32_t micro_crc, ObSSMemBlockHandle &mem_blk_handle, bool &is_first_add);
  int init_arc_info(const int64_t cache_limit_size, const int64_t micro_cnt_limit);
  void inner_update_arc_limit(const int64_t cache_limit_size);
  int inner_update_cache_mem_limit(const uint64_t tenant_id);
  int inner_update_cache_expiration_time(const uint64_t tenant_id);
  int inner_add_ckpt_micro_block_meta(ObSSMicroBlockMetaHandle &micro_meta_handle);
  int inner_adjust_arc_seg_info(const bool is_in_l1, const bool is_in_ghost, const ObSSARCOpType &op_type,
                                const int32_t delta_size, const int32_t delta_cnt);
  // Calculate the maximum count of micro_meta that can be allocated by SSMicroMetaAlloc
  int cal_micro_cnt_limit(const int64_t mem_limit, int64_t &micro_cnt_limit);

private:
  static const int64_t PRINT_LOG_INTERVAL = 5 * 60 * 1000 * 1000;
  static const int32_t MAX_SCAN_MICRO_CKPT_CNT = 1000;

  bool is_inited_;
  bool is_mini_mode_;
  bool mem_limited_;
  int32_t block_size_;
  uint64_t tenant_id_;
  int64_t micro_cnt_limit_;
  int64_t cache_mem_limit_;
  int64_t expiration_time_s_;
  common::ObIAllocator *allocator_;
  SSMicroMap micro_meta_map_;
  SSMicroMap::BlurredIterator bg_micro_iter_;
  SSMicroMap::BlurredIterator fg_micro_iter_;
  ObSSARCInfo arc_info_;
  ObSSMicroCacheStat &cache_stat_;
  SSReplayMicroCkptCtx replay_ctx_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_META_MANAGER_H_ */
