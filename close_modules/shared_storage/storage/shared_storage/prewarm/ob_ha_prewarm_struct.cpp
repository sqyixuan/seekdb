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

#include "ob_ha_prewarm_struct.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;

/***************************ObCopyMicroBlockKeySetProducer**********************/
ObCopyMicroBlockKeySetProducer::ObCopyMicroBlockKeySetProducer()
  : is_inited_(false), start_blk_idx_(-1), end_blk_idx_(-1), blk_idx_(-1), ls_handle_(),
    tablet_service_(nullptr)
{
}

ObCopyMicroBlockKeySetProducer::~ObCopyMicroBlockKeySetProducer()
{
}

int ObCopyMicroBlockKeySetProducer::init(
    const ObMigrationCacheJobInfo &job_info,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_BUCKET_CNT = 100000; // 10w
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy micro block key set producer init twice", KR(ret));
  } else {
    // job_info is left-open and right-closed interval: (start_blk_idx_, end_blk_idx_]
    // hence, set start_blk_idx to (job_info.start_blk_idx + 1)
    start_blk_idx_ = job_info.start_blk_idx_ + 1;
    end_blk_idx_ = job_info.end_blk_idx_;
    blk_idx_ = start_blk_idx_ - 1;

    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle_, ObLSGetMod::SS_PREWARM_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(ls_id), KPC(ls_service));
    } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", KR(ret), K_(ls_handle));
    } else if (OB_ISNULL(tablet_service_ = ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet service is null", KR(ret), KPC(ls));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCopyMicroBlockKeySetProducer::get_next_micro_block_key_set(ObCopyMicroBlockKeySet &key_set)
{
  int ret = OB_SUCCESS;
  ++blk_idx_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy micro block key set producer not init", KR(ret));
  } else if (OB_UNLIKELY((blk_idx_ < start_blk_idx_) || (blk_idx_ > (end_blk_idx_ + 1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected blk idx", KR(ret), K_(blk_idx), K_(start_blk_idx), K_(end_blk_idx));
  } else if (blk_idx_ == (end_blk_idx_ + 1)) {
    ret = OB_ITER_END;
    LOG_INFO("get next micro block key set end");
  } else { // blk_idx_ < (end_blk_idx_ + 1)
    key_set.blk_idx_ = blk_idx_;
    ObSSMicroCache *micro_cache = nullptr;
    if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_micro_cache is null", KR(ret));
    } else {
      ObSSPhysicalBlockHandle phy_block_handle;
      // hold phy block handle to avoid reorganize and reuse
      if (OB_FAIL(micro_cache->get_phy_block_handle(blk_idx_, phy_block_handle))) {
        LOG_WARN("fail to get block handle", KR(ret), K_(blk_idx));
      } else if (OB_UNLIKELY(!phy_block_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy block handle is invalid", KR(ret), K_(blk_idx));
      } else if (phy_block_handle()->is_cache_data_block()) {
        int64_t phy_blk_size = 0;
        ObSSNormalPhyBlockHeader header;
        if (OB_FAIL(micro_cache->get_phy_block_size(phy_blk_size))) {
          LOG_WARN("fail to get phy_block size", KR(ret));
        } else if (OB_FAIL(read_header(phy_blk_size, phy_block_handle, header))) {
          LOG_WARN("fail to read header", KR(ret), K_(blk_idx), K(phy_blk_size));
        } else if (OB_FAIL(read_micro_block_key_metas(header, phy_blk_size, phy_block_handle,
                                                      key_set.micro_block_key_metas_))) {
          LOG_WARN("fail to read micro block key metas", KR(ret), K(header), K_(blk_idx), K(phy_blk_size));
        }
      }
    }
  }
  return ret;
}

int ObCopyMicroBlockKeySetProducer::read_header(
    const int64_t phy_blk_size,
    ObSSPhysicalBlockHandle &phy_block_handle,
    ObSSNormalPhyBlockHeader &header)
{
  int ret = OB_SUCCESS;
  ObSSPhyBlockCommonHeader common_header;
  int64_t pos = 0;
  const int64_t header_offset = blk_idx_ * phy_blk_size;
  const int64_t header_size = common_header.header_size_ + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSHAPWKeySetPrd"));
  char *header_buf = nullptr;
  if (OB_ISNULL(header_buf = static_cast<char *>(allocator.alloc(header_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for read buf", KR(ret), K(header_size));
  } else if (OB_FAIL(ObSSMicroCacheIOHelper::read_block(header_offset, header_size, header_buf, phy_block_handle))) {
    LOG_WARN("fail to read block", KR(ret), K(header_offset), K(header_size), K(phy_block_handle));
  } else if (OB_FAIL(common_header.deserialize(header_buf, header_size, pos))) {
    LOG_WARN("fail to deserialize common header", KR(ret), KP(header_buf), K(header_size), K(pos));
  } else if (OB_UNLIKELY(pos != common_header.header_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized pos is unexpected", KR(ret), K(common_header), K(pos));
  } else if (OB_UNLIKELY(!common_header.is_valid() || !common_header.is_cache_data_blk())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common header is invalid or wrong type", KR(ret), K(common_header));
  } else if (OB_FAIL(header.deserialize(header_buf, header_size, pos))) {
    LOG_WARN("fail to deserialize header", KR(ret), KP(header_buf), K(header_size), K(pos));
  } else if (OB_UNLIKELY(!header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_blk header is invalid", KR(ret), K(header), K(pos));
  }
  return ret;
}

int ObCopyMicroBlockKeySetProducer::read_micro_block_key_metas(
    const ObSSNormalPhyBlockHeader &header,
    const int64_t phy_blk_size,
    ObSSPhysicalBlockHandle &phy_block_handle,
    ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_block_key_metas)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  const int64_t index_size = header.micro_index_size_;
  const int64_t index_offset = blk_idx_ * phy_blk_size + header.micro_index_offset_;
  const int64_t micro_cnt = header.micro_count_;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSHAPWKeySetPrd"));
  char *index_buf = nullptr;
  ObSEArray<ObSSMicroBlockIndex, 16> micro_indexs;
  if (OB_ISNULL(index_buf = static_cast<char *>(allocator.alloc(index_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for read buf", KR(ret), K(index_size));
  } else if (OB_FAIL(ObSSMicroCacheIOHelper::read_block(index_offset, index_size, index_buf, phy_block_handle))) {
    LOG_WARN("fail to read block", KR(ret), K(index_offset), K(index_size), K(phy_block_handle));
  } else if (OB_FAIL(ObSSMicroCacheUtil::parse_micro_block_indexs(index_buf, index_size, micro_indexs))) {
    LOG_WARN("fail to parse micro_block indexs", KR(ret), KP(index_buf), K(index_size), K(header));
  } else if (OB_UNLIKELY(micro_cnt != micro_indexs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_block count not mismatch", KR(ret), K(micro_cnt), "micro_idx_cnt", micro_indexs.count(), K(header));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("s2 micro cache is null", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt); ++i) {
      const ObSSMicroBlockCacheKey &micro_key = micro_indexs.at(i).get_micro_key();
      bool is_filtered = false;
      if (OB_FAIL(filter_ls_major_macro(micro_key, is_filtered))) {
        LOG_WARN("fail to filter ls major macro", KR(ret), K(micro_key));
      } else if (!is_filtered) {
        ObSSMicroBaseInfo micro_info;
        ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
        if (OB_FAIL(micro_cache->check_micro_block_exist(micro_key, micro_info, hit_type))) {
          LOG_WARN("fail to check micro block exist", KR(ret), K(micro_key));
        } else if (ObSSCacheHitType::SS_CACHE_HIT_DISK == hit_type) {
          ObSSMicroBlockCacheKeyMeta micro_key_meta(micro_key, micro_info.crc_, micro_info.size_, micro_info.is_in_l1_);
          if (OB_FAIL(micro_block_key_metas.push_back(micro_key_meta))) {
            LOG_WARN("fail to push back micro key meta", KR(ret), K(micro_key_meta));
          }
        }
      }
    }
  }
  return ret;
}

int ObCopyMicroBlockKeySetProducer::filter_ls_major_macro(
    const ObSSMicroBlockCacheKey &micro_block_key,
    bool &is_filtered)
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  if (micro_block_key.is_major_macro_key()) {
    ObTabletID tablet_id = micro_block_key.get_major_macro_tablet_id();
    bool is_exist = false;
    if (OB_ISNULL(tablet_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet service is null", KR(ret));
    } else if (OB_FAIL(tablet_service_->is_tablet_exist(tablet_id, is_exist))) {
      LOG_WARN("fail to check is tablet exist", KR(ret), K(tablet_id));
    } else if (!is_exist) {
      is_filtered = true;
    }
  } else {
    is_filtered = true;
  }
  return ret;
}


/***************************ObSSMicroBlockInfo**********************/
ObSSMicroBlockInfo::ObSSMicroBlockInfo() : key_meta_(), offset_(0)
{
}

ObSSMicroBlockInfo::ObSSMicroBlockInfo(
    const ObSSMicroBlockCacheKeyMeta &key_meta,
    const uint64_t offset)
  : key_meta_(key_meta), offset_(offset)
{
}

ObSSMicroBlockInfo::~ObSSMicroBlockInfo()
{
}


/***************************ObCopyMicroBlockDataProducer**********************/
ObCopyMicroBlockDataProducer::ObCopyMicroBlockDataProducer()
  : is_inited_(false), key_sets_(), key_set_idx_(-1), allocator_(), buf_size_(0),
    read_buf_(nullptr), data_buf_(nullptr)
{
}

ObCopyMicroBlockDataProducer::~ObCopyMicroBlockDataProducer()
{
}

int ObCopyMicroBlockDataProducer::init(const ObIArray<ObCopyMicroBlockKeySet> &key_sets)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy micro block data producer init twice", KR(ret));
  } else {
    ObMemAttr mem_attr(MTL_ID(), "SSHAPWDataPrd");
    key_sets_.set_attr(mem_attr);
    allocator_.set_attr(mem_attr);
    ObSSMicroCache *micro_cache = nullptr;
    if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_micro_cache is null", KR(ret));
    } else {
      if (OB_FAIL(micro_cache->get_phy_block_size(buf_size_))) {
        LOG_WARN("fail to get phy_block size", KR(ret));
      } else if (OB_FAIL(key_sets_.assign(key_sets))) {
        LOG_WARN("fail to assign", KR(ret));
      } else if (OB_ISNULL(read_buf_ = reinterpret_cast<char *>(allocator_.alloc(buf_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for read buf", KR(ret), K_(buf_size));
      } else if (OB_ISNULL(data_buf_ = reinterpret_cast<char *>(allocator_.alloc(buf_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for data buf", KR(ret), K_(buf_size));
      } else {
        key_set_idx_ = -1;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObCopyMicroBlockDataProducer::get_next_micro_block_data(
    ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_block_key_metas,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  ++key_set_idx_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy micro block data producer not init", KR(ret));
  } else if (OB_UNLIKELY((key_set_idx_ < 0) || (key_set_idx_ > key_sets_.count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected key set idx", KR(ret), K_(key_set_idx), "key_set_count", key_sets_.count());
  } else if (key_set_idx_ == key_sets_.count()) {
    ret = OB_ITER_END;
    LOG_INFO("get next micro block data end");
  } else { // key_set_idx_ < key_sets_.count()
    ObSSMicroCache *micro_cache = nullptr;
    if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_micro_cache is null", KR(ret));
    } else {
      ObSEArray<ObSSMicroBlockInfo, 16> micro_block_infos;
      micro_block_infos.set_attr(ObMemAttr(MTL_ID(), "SSHAPWKeySetPrd"));
      // hold phy block handle to avoid reorganize and reuse
      int64_t phy_blk_size = 0;
      ObSSPhysicalBlockHandle phy_block_handle;
      const ObCopyMicroBlockKeySet &cur_key_set = key_sets_.at(key_set_idx_);
      if (OB_FAIL(micro_cache->get_phy_block_handle(cur_key_set.blk_idx_, phy_block_handle))) {
        LOG_WARN("fail to get block handle", KR(ret), "phy_blk_idx", cur_key_set.blk_idx_);
      } else if (OB_UNLIKELY(!phy_block_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy block handle is invalid", KR(ret), "phy_blk_idx", cur_key_set.blk_idx_);
      } else if (OB_FAIL(micro_cache->get_phy_block_size(phy_blk_size))) {
        LOG_WARN("fail to get phy_block size", KR(ret));
      } else if (OB_FAIL(filter_moved_micro_blocks(cur_key_set, phy_blk_size, *micro_cache,
                                                   micro_block_infos))) {
        LOG_WARN("fail to filter moved micro blocks", KR(ret), K(phy_blk_size));
      } else if (!micro_block_infos.empty()) {
        if (OB_FAIL(read_micro_blocks(micro_block_infos, phy_block_handle, micro_block_key_metas, data))) {
          LOG_WARN("fail to read micro blocks", KR(ret));
        }
      }
    }
  }
  return ret;
}

// filter and read micro blocks, which are still in @key_set.blk_idx_ phy_block
int ObCopyMicroBlockDataProducer::filter_moved_micro_blocks(
    const ObCopyMicroBlockKeySet &key_set,
    const int64_t phy_blk_size,
    ObSSMicroCache &micro_cache,
    ObIArray<ObSSMicroBlockInfo> &micro_block_infos)
{
  int ret = OB_SUCCESS;
  const int64_t micro_block_key_cnt = key_set.micro_block_key_metas_.count();
  if (OB_UNLIKELY(0 == phy_blk_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_size));
  }

  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_block_key_cnt); ++i) {
    const ObSSMicroBlockCacheKeyMeta &cur_key_meta = key_set.micro_block_key_metas_.at(i);
    ObSSMicroBaseInfo micro_info;
    ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
    if (OB_FAIL(micro_cache.check_micro_block_exist(cur_key_meta.micro_key_, micro_info, hit_type))) {
      LOG_WARN("fail to check micro block exist", KR(ret), "micro_key", cur_key_meta.micro_key_);
    } else if (ObSSCacheHitType::SS_CACHE_HIT_DISK == hit_type) {
      if (micro_info.data_dest_ / phy_blk_size == key_set.blk_idx_) {
        ObSSMicroBlockInfo micro_block_info(cur_key_meta, micro_info.data_dest_);
        if (OB_UNLIKELY((cur_key_meta.data_size_ != micro_info.size_) ||
                        (cur_key_meta.data_crc_ != micro_info.crc_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected data size or data crc", KR(ret), K(cur_key_meta), K(micro_info));
        } else if (OB_FAIL(micro_block_infos.push_back(micro_block_info))) {
          LOG_WARN("fail to push back", KR(ret), K(micro_block_info));
        }
      }
    }
  }
  return ret;
}

int ObCopyMicroBlockDataProducer::read_micro_blocks(
    const ObIArray<ObSSMicroBlockInfo> &micro_block_infos,
    ObSSPhysicalBlockHandle &phy_block_handle,
    ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_block_key_metas,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  uint64_t offset = 0;
  uint32_t size = 0;
  calc_offset_and_size(micro_block_infos, offset, size);
  if (size > buf_size_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("read size exceeds buf size", KR(ret), K(size), K_(buf_size));
  } else if (OB_FAIL(ObSSMicroCacheIOHelper::read_block(offset, size, read_buf_, phy_block_handle))) {
    LOG_WARN("fail to read block", KR(ret), K(offset), K(size), K(phy_block_handle));
  } else {
    uint64_t next_offset = 0;
    const int64_t micro_block_cnt = micro_block_infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_block_cnt); ++i) {
      const ObSSMicroBlockInfo &micro_block_info = micro_block_infos.at(i);
      uint64_t read_buf_offset = micro_block_info.offset_ - offset;
      const int32_t cur_micro_size = micro_block_info.key_meta_.data_size_;
      if (OB_UNLIKELY(((read_buf_offset + cur_micro_size) > buf_size_)
                      || ((next_offset + cur_micro_size) > buf_size_))) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("unexpected offset and size", KR(ret), K(read_buf_offset), K(cur_micro_size),
                 K_(buf_size), K(next_offset));
      } else {
        MEMCPY(data_buf_ + next_offset, read_buf_ + read_buf_offset, cur_micro_size);
        next_offset += cur_micro_size;
        if (OB_FAIL(micro_block_key_metas.push_back(micro_block_info.key_meta_))) {
          LOG_WARN("fail to push back", KR(ret), K(micro_block_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      data.assign(data_buf_, next_offset);
    }
  }
  return ret;
}

void ObCopyMicroBlockDataProducer::calc_offset_and_size(
    const ObIArray<ObSSMicroBlockInfo> &micro_block_infos,
    uint64_t &offset,
    uint32_t &size)
{
  uint64_t min_offset = UINT64_MAX;
  uint64_t max_offset = 0;
  uint32_t last_size = 0;
  const int64_t micro_block_info_cnt = micro_block_infos.count();
  for (int64_t i = 0; i < micro_block_info_cnt; ++i) {
    const ObSSMicroBlockInfo &micro_block_info = micro_block_infos.at(i);
    // update min_offset
    if (micro_block_info.offset_ < min_offset) {
      min_offset = micro_block_info.offset_;
    }
    // update max_offset and related size
    if (micro_block_info.offset_ > max_offset) {
      max_offset = micro_block_info.offset_;
      last_size = micro_block_info.key_meta_.data_size_;
    }
  }

  // obtain final offset and size
  offset = min_offset;
  size = (max_offset - min_offset + last_size);
}


} /* namespace storage */
} /* namespace oceanbase */
