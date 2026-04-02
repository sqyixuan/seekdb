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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_STRUCT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array_serialization.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/high_availability/ob_migration_warmup_struct.h"

namespace oceanbase
{
namespace blocksstable
{
  class ObBufferReader;
}
namespace storage
{

class ObSSMicroCache;
struct ObMigrationCacheJobInfo;
class ObLSTabletService;

class ObCopyMicroBlockKeySetProducer
{
public:
  ObCopyMicroBlockKeySetProducer();
  virtual ~ObCopyMicroBlockKeySetProducer();
  int init(const ObMigrationCacheJobInfo &job_info,
           const share::ObLSID &ls_id);
  int get_next_micro_block_key_set(ObCopyMicroBlockKeySet &key_set);

private:
  int read_header(const int64_t phy_blk_size,
                  ObSSPhysicalBlockHandle &phy_block_handle,
                  ObSSNormalPhyBlockHeader &header);
  int read_micro_block_key_metas(const ObSSNormalPhyBlockHeader &header,
                                 const int64_t phy_blk_size,
                                 ObSSPhysicalBlockHandle &phy_block_handle,
                                 common::ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_block_key_metas);
  int filter_ls_major_macro(const ObSSMicroBlockCacheKey &micro_block_key, bool &is_filtered);

private:
  bool is_inited_;
  int64_t start_blk_idx_;
  int64_t end_blk_idx_;
  int64_t blk_idx_;
  ObLSHandle ls_handle_; // in order to ensure the safety of @tablet_service_
  ObLSTabletService *tablet_service_;
};


struct ObSSMicroBlockInfo
{
public:
  ObSSMicroBlockInfo();
  ObSSMicroBlockInfo(const ObSSMicroBlockCacheKeyMeta &key_meta, const uint64_t offset);
  virtual ~ObSSMicroBlockInfo();
  TO_STRING_KV(K_(key_meta), K_(offset));

public:
  ObSSMicroBlockCacheKeyMeta key_meta_;
  uint64_t offset_;
};


class ObCopyMicroBlockDataProducer
{
public:
  ObCopyMicroBlockDataProducer();
  virtual ~ObCopyMicroBlockDataProducer();
  int init(const common::ObIArray<ObCopyMicroBlockKeySet> &key_sets);
  int get_next_micro_block_data(common::ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_block_key_metas,
                                blocksstable::ObBufferReader &data);

private:
  int filter_moved_micro_blocks(const ObCopyMicroBlockKeySet &key_set,
                                const int64_t phy_blk_size,
                                ObSSMicroCache &micro_cache,
                                common::ObIArray<ObSSMicroBlockInfo> &micro_block_infos);
  int read_micro_blocks(const common::ObIArray<ObSSMicroBlockInfo> &micro_block_infos,
                        ObSSPhysicalBlockHandle &phy_block_handle,
                        common::ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_block_key_metas,
                        blocksstable::ObBufferReader &data);
  void calc_offset_and_size(const common::ObIArray<ObSSMicroBlockInfo> &micro_block_infos,
                            uint64_t &offset,
                            uint32_t &size);

private:
  bool is_inited_;
  common::ObArray<ObCopyMicroBlockKeySet> key_sets_;
  int64_t key_set_idx_;
  ObArenaAllocator allocator_;
  int64_t buf_size_;
  char *read_buf_;
  char *data_buf_;
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_STRUCT_H_ */
