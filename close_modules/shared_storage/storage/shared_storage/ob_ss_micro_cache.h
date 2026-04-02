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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_H_

#include <stdint.h>
#include "lib/allocator/ob_small_allocator.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_basic_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_data_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_runner.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_struct.h"
#include "storage/shared_storage/micro_cache/ob_ss_la_micro_key_manager.h"

namespace oceanbase
{
namespace common
{
struct ObIOInfo;
}
namespace blocksstable
{
class ObStorageObjectHandle;
}
namespace storage
{
class ObSSPhysicalBlockManager;
class ObTenantFileManager;

struct ObSSMicroCacheConfig
{
public:
  ObSSMicroCacheConfig();
  ~ObSSMicroCacheConfig() { reset(); }
  void reset();
  void set_micro_ckpt_compressor_type(const common::ObCompressorType type) { micro_ckpt_compressor_type_ = type; }
  void set_blk_ckpt_compressor_type(const common::ObCompressorType type) { blk_ckpt_compressor_type_ = type; }
  common::ObCompressorType get_micro_ckpt_compressor_type() const { return micro_ckpt_compressor_type_; }
  common::ObCompressorType get_blk_ckpt_compressor_type() const { return blk_ckpt_compressor_type_; }

  TO_STRING_KV(K_(micro_ckpt_compressor_type), K_(blk_ckpt_compressor_type));
private:
  common::ObCompressorType micro_ckpt_compressor_type_;
  common::ObCompressorType blk_ckpt_compressor_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMicroCacheConfig);
};

enum class MicroCacheGetType : uint8_t
{
  // when cache hit, get data from ss_micro_cache
  // when cache miss, do not get data.
  // for replica_prewarm
  GET_CACHE_HIT_DATA = 0,
  // when cache hit, do not get data.
  // when cache miss, get data from object storage and add it into ss_micro_cache.
  // for major_compaction_prewarm
  GET_CACHE_MISS_DATA = 1,
  // when cache hit, get data from micro_cache
  // when cache miss, get data from object storage and add it into ss_micro_cache.
  // for common io
  FORCE_GET_DATA = 2,
};

struct SSMicroMetaReqGuard
{
public:
  SSMicroMetaReqGuard(int64_t &flying_req_cnt) 
    : flying_req_cnt_(flying_req_cnt)
  { 
    ATOMIC_INC(&flying_req_cnt_); 
  }
  ~SSMicroMetaReqGuard() 
  {
    ATOMIC_DEC(&flying_req_cnt_);
  }

public:
  int64_t &flying_req_cnt_;
};

/* The basic entry of the whole shared_storage micro_block data cache.
 *
 * 1. include a micro_meta_manager, which owns a map(key is macro_block_id, value is
 *    a hashmap which will store this macro_block's micro_blocks' meta)
 * 
 * 2. include a physical_block_manager, which will manage all phy_blocks of cache_file.
 *    We can allocate or free block through this manager.
 *
 * 3. include a mem_data_manager, which will store micro_blocks' data into memory util
 *    the data_size is more than block_size. Then we will persist this data into phy_block.
 * 
 * 4. include a task_runner, which will run some background tasks, like persist_micro_data_task.
 */
struct ObSSARCInfo;
class ObSSMicroCache
{
public:
  ObSSMicroCache();
  virtual ~ObSSMicroCache() {}
  static int mtl_init(ObSSMicroCache *&micro_cache);

  int init(const uint64_t tenant_id, const int64_t cache_file_size);
  int start();
  void stop();
  int wait();
  void destroy();
  // these code can be just used in micro_cache, not use it in any other place.
  common::ObConcurrentFIFOAllocator &get_allocator() { return allocator_; }
  common::ObSmallAllocator &get_micro_meta_allocator() { return micro_meta_allocator_; }
  ObSSMicroCacheStat &get_micro_cache_stat() { return cache_stat_; }
  ObSSMicroMetaManager &get_micro_meta_mgr() { return micro_meta_mgr_; }
  ObSSPhysicalBlockManager &get_phy_block_mgr() { return phy_blk_mgr_; }
  ObSSMicroCacheTaskRunner &get_task_runner() { return task_runner_; }

  // Check whether micro_cache is close to evict data
  int is_ready_to_evict_cache(bool &prepare_eviction);
  // To change micro_cache file size, now only support increase it.
  int resize_micro_cache_file_size(const int64_t new_cache_file_size);
  // Add new micro_block data into micro_cache.
  // Cuz it will be invoked in io_callback, to avoid cause io failure, we set the return type as 'void'
  int add_micro_block_cache(const ObSSMicroBlockCacheKey &micro_key, const char *micro_data, const int32_t size,
                            const ObSSMicroCacheAccessType access_type);
  // this function is designed for prewarm, which can specify max_retry_times and whether need to
  // transfer micro_block cache from T1 to T2.
  int add_micro_block_cache_for_prewarm(const ObSSMicroBlockCacheKey &micro_key,
                                        const char *micro_data, const int32_t size,
                                        const ObSSMicroCacheAccessType access_type,
                                        const int64_t max_retry_times,
                                        const bool transfer_seg);

  // get micro_block data if it existed in cache. If cache miss, get it from object storage and add it into cache.
  int get_micro_block_cache(const ObSSMicroBlockCacheKey &micro_key, const ObSSMicroBlockId &phy_micro_id,
                            const MicroCacheGetType get_type, common::ObIOInfo &io_info,
                            blocksstable::ObStorageObjectHandle &obj_handle,
                            const ObSSMicroCacheAccessType access_type);
  // only get micro_block data from cache, if cache miss, won't get micro_block data.
  int get_cached_micro_block(const ObSSMicroBlockCacheKey &micro_key, common::ObIOInfo &io_info, 
                             blocksstable::ObStorageObjectHandle &obj_handle,
                             const ObSSMicroCacheAccessType access_type);
  // check the micro_block exist in micro_cache or not. If not exist, return hit_type=CACHE_MISS
  int check_micro_block_exist(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBaseInfo &micro_info,
                              ObSSCacheHitType &hit_type);
  int get_not_exist_micro_blocks(const common::ObIArray<ObSSMicroBlockCacheKeyMeta> &in_micro_block_key_metas,
                                 common::ObIArray<ObSSMicroBlockCacheKeyMeta> &out_micro_block_key_metas);
  // update a batch of micro_blocks' arc_seg/heat
  int update_micro_block_heat(const common::ObIArray<ObSSMicroBlockCacheKey> &micro_keys,
                              const bool transfer_seg/*need to transfer T1 -> T2*/,
                              const bool update_access_time/*need to update access_time to current_time*/,
                              const int64_t time_delta_s = 120/*default 120s*/);
  void update_hot_micro_lack_count(const bool full_scan, const int64_t lack_count);
  // get available disk space for prewarm to add micro_cache.
  int get_available_space_for_prewarm(int64_t &available_size);
  void begin_free_space_for_prewarm();
  void finish_free_space_for_prewarm();
  int get_batch_la_micro_keys(ObLS *ls, ObIArray<ObSSMicroBlockCacheKeyMeta> &keys);
  int is_tablet_id_need_filter(ObLS *ls, const uint64_t tablet_id, bool &is_filter);

  int get_phy_block_handle(const int64_t phy_blk_idx, ObSSPhysicalBlockHandle &phy_blk_handle);
  int get_phy_block_size(int64_t &phy_blk_size);
  int get_total_micro_size(int64_t &total_micro_size);
  int get_tablet_cache_info(const common::ObTabletID &tablet_id, ObSSTabletCacheInfo &tablet_cache_info);
  int get_ls_cache_info(const share::ObLSID &ls_id, ObSSLSCacheInfo &ls_cache_info);
  int divide_phy_block_range(const share::ObLSID &ls_id, const int64_t split_count,
                             ObIArray<ObSSPhyBlockIdxRange> &block_ranges);
  void clear_micro_cache();
  int read_checkpoint();

  // For ob_admin
  void enable_cache();
  void disable_cache();
  bool is_cache_enabled() const;
  int get_phy_block_info(const int64_t phy_blk_idx, ObSSPhysicalBlock& phy_blk_info);
  int get_micro_meta_handle(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
  int get_micro_cache_info(ObSSMicroCacheStat &cache_stat, ObSSMicroCacheSuperBlock &super_blk, ObSSARCInfo &arc_info);
  int clear_micro_meta_by_tablet_id(const common::ObTabletID &tablet_id);

  // For micro_cache config
  void set_micro_ckpt_compressor_type(const common::ObCompressorType type);
  void set_blk_ckpt_compressor_type(const common::ObCompressorType type);
  common::ObCompressorType get_micro_ckpt_compressor_type() const;
  common::ObCompressorType get_blk_ckpt_compressor_type() const;
private:
  int init_micro_meta_pool();
  int init_micro_meta_manager(const bool is_mini_mode);

  int read_or_format_super_block();
  int read_phy_block_checkpoint(const int64_t blk_entry);
  int read_micro_meta_checkpoint(const int64_t blk_entry, const int64_t micro_ckpt_time_us);
  void reset_handle(ObSSMemBlockHandle &mem_blk_handle, ObSSPhysicalBlockHandle &phy_blk_handle,
                    ObSSMicroBlockMetaHandle &micro_meta_handle);
  int simulate_io_result(const MicroCacheGetType get_type,
                         const blocksstable::MacroBlockId &macro_id,
                         common::ObIOInfo &io_info,
                         blocksstable::ObStorageObjectHandle &obj_handle) const;
  // Get micro_block data from micro_cache(in memory or in disk) if cache hit.
  // If cache miss, it will send io_request to fetch data from remote and resave back to micro_cache.
  int inner_get_micro_block_cache(const ObSSMicroBlockCacheKey &micro_key, const ObSSMicroBlockId &phy_micro_id,
                                  const MicroCacheGetType get_type, common::ObIOInfo &io_info,
                                  blocksstable::ObStorageObjectHandle &obj_handle,
                                  const ObSSMicroCacheAccessType access_type);
  int inner_get_micro_block_handle(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroBaseInfo &micro_info,
                                   ObSSMicroBlockMetaHandle &micro_meta_handle, ObSSMemBlockHandle &mem_blk_handle, 
                                   ObSSPhysicalBlockHandle &phy_blk_handle, ObSSCacheHitType &hit_type, 
                                   const bool update_arc);
  int inner_update_free_space_for_prewarm(const bool is_start_op);
  int push_latest_access_micro_key(const ObSSMicroBlockCacheKey &micro_key,
                                   const ObSSCacheHitType &hit_type,
                                   const ObSSMicroBaseInfo &micro_info);
  // used when destory or clear micro_cache.
  void inner_check_micro_meta_cnt();

private:
  static const uint32_t DEFAULT_BLOCK_SIZE = 2 * 1024 * 1024;
  static const uint64_t MAX_CKPT_EXPIRATION_TIME_US = 24 * 60 * 60 * 1000 * 1000L;
  static const int64_t SLEEP_INTERVAL_US = 1000;
  static const int64_t PRINT_LOG_INTERVAL_US = 2 * 1000 * 1000;

private:
  bool is_inited_;
  bool is_stopped_;
  bool is_mini_mode_;
  bool is_first_start_; // first start or restart
  bool is_enabled_; // enable/disable micro_cache
  int32_t phy_block_size_;
  uint64_t tenant_id_;
  int64_t cache_file_size_;
  int64_t flying_req_cnt_; // the count of flying_request which will access micro_meta_map
  ObSSMicroCacheConfig config_;
  ObSSMicroCacheStat cache_stat_;
  common::ObConcurrentFIFOAllocator allocator_;
  ObSSPhysicalBlockManager phy_blk_mgr_;
  ObSSMemDataManager mem_data_mgr_;
  ObSSMicroMetaManager micro_meta_mgr_;
  ObSSMicroCacheTaskRunner task_runner_;
  ObSSLAMicroKeyManager latest_access_micro_key_mgr_;
  common::ObSmallAllocator micro_meta_allocator_;
};

#define SSPhyBlockMgr (MTL(ObSSMicroCache *)->get_phy_block_mgr())
#define SSMicroMetaMgr (MTL(ObSSMicroCache *)->get_micro_meta_mgr())
#define SSMicroMetaAlloc (MTL(ObSSMicroCache *)->get_micro_meta_allocator())
#define SSMicroCacheStat (MTL(ObSSMicroCache *)->get_micro_cache_stat())

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_MICRO_CACHE_H_ */
