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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASIC_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASIC_OP_H_

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase 
{
namespace storage 
{
/*
 * To get a micro_block meta handle. A micro_block may be stored in mem_block or phy_block.
 * When is_persisted_ = false, that means stored in mem_block; Otherwise, stored in phy_block.
 * If this micro_block's meta is invalid, we treat this situation as NOT_EXIST.
 */
struct SSMicroMapGetMicroHandleFunc 
{
public:
  int ret_;
  bool update_arc_;       // true: transfer arc_seg
  ObSSARCOpType arc_op_type_;
  ObSSMicroBaseInfo micro_info_;
  ObSSMicroBlockMetaHandle &micro_meta_handle_;
  ObSSMemBlockHandle &mem_blk_handle_;
  ObSSPhysicalBlockHandle &phy_blk_handle_;

  SSMicroMapGetMicroHandleFunc(ObSSMicroBlockMetaHandle &micro_meta_handle, ObSSMemBlockHandle &mem_blk_handle, 
                               ObSSPhysicalBlockHandle &phy_blk_handle, const bool update_arc = true);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);

  TO_STRING_KV(K_(update_arc), K_(arc_op_type), K_(micro_info));
};

/*
 * When write micro_block into mem_block, if this micro_meta already exist, we should update it:
 * If micro_block exist but invalid, update micro_block meta to valid;
 * If micro_block exist but valid, just need to update T1/T2/B1/B2;
 */
struct SSMicroMapUpdateMicroFunc
{
public:
  int ret_;
  bool is_updated_;
  bool validate_ghost_micro_;
  bool is_in_l1_;
  bool is_in_ghost_;
  int32_t size_;
  uint32_t crc_;
  const ObSSMemBlockHandle &mem_blk_handle_;
  ObSSARCOpType arc_op_type_;

  SSMicroMapUpdateMicroFunc(const int32_t size, const ObSSMemBlockHandle &mem_blk_handle, const uint32_t crc);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
  TO_STRING_KV(K_(ret), K_(is_updated), K_(validate_ghost_micro), K_(is_in_l1), K_(is_in_ghost), K_(size), K_(crc),
      K_(mem_blk_handle), K_(arc_op_type));
};

struct SSMicroMapUpdateMicroHeatFunc
{
public:
  int ret_;
  bool transfer_seg_;
  bool update_access_time_;
  int64_t time_delta_s_;
  int32_t size_;
  ObSSARCOpType arc_op_type_;

  SSMicroMapUpdateMicroHeatFunc(const bool transfer_seg, const bool update_access_time, const int64_t heat_delta);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * When a mem_block is sealed and persisted into cache_data_file, we need to update
 * all relative micro_blocks' meta, to change its location from mem_block to disk_offset.
 * It should be successful, otherwise, it is a logical error!!!
 */
struct SSMicroMapUpdateMicroDestFunc 
{
public:
  int ret_;
  int64_t data_dest_;   // micro_block data persisted location in cache_data_file
  uint32_t reuse_version_; // reuse_version of persisted phy_block
  ObSSMemBlockHandle &mem_blk_handle_;
  bool succ_update_;

  SSMicroMapUpdateMicroDestFunc(const int64_t data_dest, const uint32_t reuse_version, 
                                ObSSMemBlockHandle &mem_blk_handle);

  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle); 
};

/*
 * Evict one micro_block, change its meta(update is_in_ghost and mark invalid) and return its 
 * offset & reuse_version, cuz we need to update its relative phy_block's valid_length
 */
struct SSMicroMapEvictMicroFunc
{
public:
  int ret_;
  const ObSSMicroMetaSnapshot &cold_micro_;

  explicit SSMicroMapEvictMicroFunc(const ObSSMicroMetaSnapshot &cold_micro);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * Delete one micro_block, before delete its meta, return its offset & reuse_version, cuz we need to
 * update its relative phy_block's valid_length
 */
struct SSMicroMapDeleteMicroFunc 
{
public:
  const ObSSMicroMetaSnapshot &cold_micro_;

  explicit SSMicroMapDeleteMicroFunc(const ObSSMicroMetaSnapshot &cold_micro);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * Invalidate one micro_block. If it was already invalid, return OB_ENTRY_NOT_EXIST
 */
struct SSMicroMapInvalidateMicroFunc
{
public:
  int ret_;
  bool succ_invalidate_;
  ObSSMicroBaseInfo &micro_info_;

  SSMicroMapInvalidateMicroFunc(ObSSMicroBaseInfo &micro_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * If a sealed mem_block(which is dynamiclly created) is being persisted, but part fail to 
 * update micro_block's meta from mem_address to a disk address, we need to delete all of 
 * its unpersisted micro_block before free this mem_block.
 */
struct SSMicroMapDeleteUnpersistedMicroFunc
{
public:
  bool is_in_l1_;
  bool is_in_ghost_;
  int32_t size_;
  ObSSMemBlockHandle &mem_blk_handle_;

  explicit SSMicroMapDeleteUnpersistedMicroFunc(ObSSMemBlockHandle &mem_blk_handle);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * When execute ckpt, we can iterate all invalid micro_meta and try to delete them
 */
struct SSMicroMapDeleteInvalidMicroFunc
{
public:
  bool is_in_l1_;
  bool is_in_ghost_;
  int32_t size_;

  SSMicroMapDeleteInvalidMicroFunc();
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * When execute ckpt, we can iterate all expired micro_meta and try to delete them
 */
struct SSMicroMapDeleteExpiredMicroFunc
{
public:
  int64_t expiration_time_;
  ObSSMicroBaseInfo &expired_micro_info_;

  SSMicroMapDeleteExpiredMicroFunc(const int64_t expiration_time, ObSSMicroBaseInfo &expired_micro_info);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * Try to set this micro_block meta's 'is_reorganizing' as true.
 */
struct SSMicroMapTryReorganizeMicroFunc
{
public:
  int ret_;
  int64_t data_dest_;
  ObSSMicroBlockMetaHandle &micro_meta_handle_;

  SSMicroMapTryReorganizeMicroFunc(const int64_t data_dest, ObSSMicroBlockMetaHandle &micro_meta_handle);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle);
};

/*
 * Try to set this micro_block meta's 'is_reorganizing' as false.
 *
 * When reorganize phy_block's micro_blocks, we need to move these micro_blocks which meet the requirement
 * from disk to mem_block firstly. That means, 'is_persisted': true -> false
 */
struct SSMicroMapCompleteReorganizingMicroFunc
{
public:
  int ret_;
  ObSSMemBlockHandle &mem_blk_handle_;

  explicit SSMicroMapCompleteReorganizingMicroFunc(ObSSMemBlockHandle &mem_blk_handle);
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle); 
};

/*
 * Directly set this micro_block meta's 'is_reorganizing' as false.
 */
struct SSMicroMapUnmarkReorganizingMicroFunc
{
public:
  int ret_;

  SSMicroMapUnmarkReorganizingMicroFunc();
  bool operator()(const ObSSMicroBlockCacheKey *micro_key, ObSSMicroBlockMetaHandle &micro_meta_handle); 
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_BASIC_OP_H_ */
