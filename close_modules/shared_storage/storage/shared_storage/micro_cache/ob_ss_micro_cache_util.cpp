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
#include "common/storage/ob_io_device.h"
#include "share/ob_io_device_helper.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_reader.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObSSMicroCacheUtil::parse_micro_block_indexs(
    const char *buf, 
    const int64_t buf_len, 
    ObIArray<ObSSMicroBlockIndex> &micro_indexs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else {
    int64_t deserialized_size = 0;
    while (OB_SUCC(ret) && (deserialized_size < buf_len)) {
      ObSSMicroBlockIndex micro_index;
      int64_t pos = 0;
      if (OB_FAIL(micro_index.deserialize(buf + deserialized_size, buf_len - deserialized_size, pos))) {
        LOG_WARN("fail to deserialize micro_index", KR(ret), K(deserialized_size), K(buf_len), K(pos));
      } else if (OB_FAIL(micro_indexs.push_back(micro_index))) {
        LOG_WARN("fail to push back micro_index", KR(ret), K(micro_index));
      } else {
        deserialized_size += pos;
      }
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_phy_block_common_header(
    const char *buf, 
    const int64_t buf_len, 
    ObSSPhyBlockCommonHeader &common_header)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < common_header.header_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(common_header));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(common_header.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize common header", KR(ret), KP(buf), K(buf_len), K(common_header));
    } else if (OB_UNLIKELY(!common_header.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block common header is invalid", KR(ret), K(common_header));
    } else if (OB_FAIL(common_header.check_payload_checksum(buf + common_header.header_size_,
               common_header.payload_size_))) {
      LOG_WARN("fail to check payload checksum", KR(ret), K(common_header), KP(buf));
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_normal_phy_block_header(
    const char *buf, 
    const int64_t buf_len, 
    ObSSNormalPhyBlockHeader &header)
{
  int ret = OB_SUCCESS;
  const int64_t header_size = ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < header_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(header_size));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(header.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize phy_block header", KR(ret), KP(buf), K(buf_len));
    } else if (OB_UNLIKELY(!header.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block_header is invalid", KR(ret), K(header));
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::dump_phy_block(
    char *buf, 
    const int64_t buf_len, 
    const char *file_path, 
    const bool only_index,
    const bool is_micro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(file_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(file_path));
  } else {
    ObArenaAllocator allocator;
    int64_t pos = 0;
    char *dest_buf = nullptr;
    const int64_t dest_len = buf_len * 12; // reserve enough space for parsing phy_block.
    ObSSPhyBlockCommonHeader common_hdr;
    if (OB_ISNULL(dest_buf = static_cast<char *>(allocator.alloc(dest_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(dest_len));
    } else if (OB_FAIL(ObSSMicroCacheUtil::parse_phy_block_common_header(buf, buf_len, common_hdr))) {
      LOG_WARN("fail to parse phy_blk common header", KR(ret), KP(buf), K(buf_len));
    } else if (OB_UNLIKELY(!common_hdr.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_blk common header is invalid", KR(ret), K(common_hdr));
    } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, "ObSSPhyBlockCommonHeader: ", 
               common_hdr, "\n"))) {
      LOG_WARN("fail to write phy_blk common header", KR(ret), KP(dest_buf), K(dest_len), K(pos), K(common_hdr));
    } else {
      if (common_hdr.is_super_blk()) {
        if (OB_FAIL(parse_ss_super_block(dest_buf, dest_len, buf, buf_len, pos))) {
          LOG_WARN("fail to parse ss_super_block", KR(ret), KP(dest_buf), K(dest_len), KP(buf), K(buf_len), K(pos));
        }
      } else if (common_hdr.is_cache_data_blk()) {
        if (OB_FAIL(parse_normal_phy_block(dest_buf, dest_len, buf, buf_len, pos, only_index))) {
          LOG_WARN("fail to parse normal_block", KR(ret), KP(dest_buf), K(dest_len), KP(buf), K(buf_len), K(pos));
        }
      } else if (common_hdr.is_ckpt_blk()) {
        if (OB_FAIL(parse_checkpoint_block(dest_buf, dest_len, buf, buf_len, pos, is_micro_meta))) {
          LOG_WARN("fail to parse checkpoint_block", KR(ret), KP(dest_buf), K(dest_len), KP(buf), K(buf_len), K(pos), 
            K(is_micro_meta));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unknow phy_block type", KR(ret), K(common_hdr));
      }
    }

    ObIOFd fd;
    int64_t written_size = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObIODeviceLocalFileOp::open(file_path, O_RDWR | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR, fd))) {
      LOG_WARN("fail to open file", KR(ret), K(file_path));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::write(fd, dest_buf, pos, written_size))) {
      LOG_WARN("fail to write phy_block content", KR(ret), K(fd), KP(dest_buf), K(pos), K(written_size));
    } else if (OB_UNLIKELY(written_size != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write_size is wrong", KR(ret), K(written_size), K(pos));
    }
    int tmp_ret = OB_SUCCESS;
    if (fd.is_valid() && OB_TMP_FAIL(ObIODeviceLocalFileOp::close(fd))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      LOG_WARN("fail to close file", KR(ret), KR(tmp_ret), K(fd));
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_checkpoint_block(
    char *dest_buf, 
    const int64_t dest_len, 
    char *src_buf,
    const int64_t src_len, 
    int64_t &pos,
    const bool is_micro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_buf) || OB_ISNULL(src_buf) || OB_UNLIKELY((dest_len <= 0) || (src_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dest_buf), KP(src_buf), K(dest_len), K(src_len));
  } else {
    int64_t src_buf_pos = 0;
    ObSSLinkedPhyBlockHeader linked_hdr;
    const int64_t common_hdr_size = ObSSPhyBlockCommonHeader::get_serialize_size();
    const int64_t linked_hdr_size = ObSSLinkedPhyBlockHeader::get_fixed_serialize_size();
    if (OB_FAIL(linked_hdr.deserialize(src_buf + common_hdr_size, src_len - common_hdr_size, src_buf_pos))) {
      LOG_WARN("fail to deserialize ObSSLinkedPhyBlockHeader", KR(ret), KP(src_buf), K(src_len), K(src_buf_pos));
    } else if (OB_UNLIKELY(!linked_hdr.is_valid() || linked_hdr.get_serialize_size() != src_buf_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("linked_hdr is invalid or deserialized pos unexpected", KR(ret), K(linked_hdr), K(src_buf_pos), 
        K(linked_hdr.get_serialize_size()));
    } else {
      ObArenaAllocator allocator;
      const int64_t def_blk_size = 2 * 1024 * 1024L;
      const int64_t item_cnt = linked_hdr.item_count_;
      const int64_t payload_size = linked_hdr.get_payload_size();
      const int64_t payload_offset = common_hdr_size + linked_hdr_size;
      char *data_buf = src_buf + payload_offset;
      ObSSLinkedPhyBlockItemReader item_reader;
      ObSSMicroBlockMeta *micro_meta = nullptr;
      if (OB_FAIL(item_reader.init(data_buf, payload_size, def_blk_size))) {
        LOG_WARN("fail to init item_reader", KR(ret), KP(data_buf), K(payload_size), K(def_blk_size));
      } else if (OB_ISNULL(micro_meta = static_cast<ObSSMicroBlockMeta *>(allocator.alloc(sizeof(ObSSMicroBlockMeta))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret), K(sizeof(ObSSMicroBlockMeta)));
      }
      for (int64_t i = 0; OB_SUCC(ret) && (i < item_cnt); ++i) {
        char *item_buf = nullptr;
        int64_t item_buf_len = 0;
        int64_t item_buf_pos = 0;
        if (OB_FAIL(item_reader.get_next_item(item_buf, item_buf_len))) {
          LOG_WARN("fail to get next item", KR(ret), K(i), K(item_cnt));
        } else if (is_micro_meta) {
          micro_meta->reset();
          if (OB_FAIL(micro_meta->deserialize(item_buf, item_buf_len, item_buf_pos))) {
            LOG_WARN("fail to deserialize micro_meta", KR(ret), K(item_buf_len), KP(item_buf));
          } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, *micro_meta, "\n"))) {
            LOG_WARN("fail to write micro_meta", KR(ret), KP(dest_buf), K(dest_len), K(pos), K(*micro_meta));
          }
        } else {
          ObSSPhyBlockPersistInfo persist_info;
          if (OB_FAIL(persist_info.deserialize(item_buf, item_buf_len, item_buf_pos))) {
            LOG_WARN("fail to deserialize persist_info", KR(ret), K(item_buf_len), KP(item_buf));
          } else if (OB_UNLIKELY(!persist_info.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("phy_block persist_info should be valid", KR(ret), K(persist_info));
          } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, persist_info, "\n"))) {
            LOG_WARN("fail to write persist_info", KR(ret), KP(dest_buf), K(dest_len), K(pos), K(persist_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_ss_super_block(
    char *dest_buf, 
    const int64_t dest_len, 
    const char *src_buf, 
    const int64_t src_len, 
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_buf) || OB_ISNULL(src_buf) || OB_UNLIKELY((dest_len <= 0) || (src_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dest_buf), KP(src_buf), K(dest_len), K(src_len));
  } else {
    ObSSMicroCacheSuperBlock ss_super_blk;
    int64_t src_buf_pos = 0;
    const int64_t common_hdr_size = ObSSPhyBlockCommonHeader::get_serialize_size();
    if (OB_FAIL(ss_super_blk.deserialize(src_buf + common_hdr_size, src_len - common_hdr_size, src_buf_pos))) {
      LOG_WARN("fail to deserialize ss_super_blk", KR(ret), KP(src_buf), K(src_len), K(src_buf_pos));
    } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, ss_super_blk, "\n"))) {
      LOG_WARN("fail to write ss_super_blk", KR(ret), KP(dest_buf), K(dest_len), K(pos), K(ss_super_blk));
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_normal_phy_block(
    char *dest_buf, 
    const int64_t dest_len, 
    const char *src_buf, 
    const int64_t src_len, 
    int64_t &pos,
    const bool only_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_buf) || OB_ISNULL(src_buf) || OB_UNLIKELY(dest_len <= 0) || OB_UNLIKELY(src_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dest_buf), KP(src_buf), K(dest_len), K(src_len));
  } else {
    ObSSNormalPhyBlockHeader header;
    const int64_t common_hdr_size = ObSSPhyBlockCommonHeader::get_serialize_size();
    const int64_t blk_hdr_size = ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
    ObSSPhysicalBlockHandle phy_blk_handle;
    ObSEArray<ObSSMicroBlockIndex, 64> micro_indexs;
    if (OB_FAIL(ObSSMicroCacheUtil::parse_normal_phy_block_header(src_buf + common_hdr_size, blk_hdr_size, header))) {
      LOG_WARN("fail to parse phy_block header", KR(ret), KP(src_buf), K(src_len));
    } else if (OB_UNLIKELY(!header.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_blk header is invalid", KR(ret), K(header));
    } else if (OB_FAIL(ObSSMicroCacheUtil::parse_micro_block_indexs(src_buf + header.micro_index_offset_,
               header.micro_index_size_, micro_indexs))) {
      LOG_WARN("fail to parse micro_block indexs", KR(ret), K_(header.micro_index_size), K(header));
    } else if (OB_UNLIKELY(header.micro_count_ != micro_indexs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_block count mismatch", KR(ret), K_(header.micro_count), "micro_idx_cnt", micro_indexs.count());
    } else {
      int32_t micro_data_offset = header.payload_offset_;
      if (OB_FAIL(databuff_printf(dest_buf, dest_len, pos, "micro_blk_cnt=%ld\n", micro_indexs.count()))) {
        LOG_WARN("fail to write mciro_blk_cnt", KR(ret), K(dest_len), K(pos));
      }
      for (int64_t i = 0; OB_SUCC(ret) && (i < micro_indexs.count()); ++i) {
        const ObSSMicroBlockIndex &micro_index = micro_indexs.at(i);
        if (OB_UNLIKELY(!micro_index.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block micro index should be valid", KR(ret), K(micro_index));
        } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, "\nmicro_key=", micro_index.get_micro_key(),
                ", size=", micro_index.get_size(), "\n"))) {
          LOG_WARN("fail to write micro index", KR(ret), K(dest_len), K(pos), K(micro_index));
        } else if (!only_index && OB_FAIL(databuff_memcpy(dest_buf, dest_len, pos, micro_index.get_size(), 
                   src_buf + micro_data_offset))) {
          LOG_WARN("fail to write micro data", KR(ret), K(dest_len), K(pos), K(micro_index.get_size()));
        }
        micro_data_offset += micro_index.get_size();
      }
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::calc_ls_tablet_cache_info(
    const uint64_t tenant_id,
    const ObSSTabletCacheMap &tablet_cache_info_map,
    ObIArray<ObSSLSCacheInfo> &ls_info_list,
    ObIArray<ObSSTabletCacheInfo> &tablet_info_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  }

  ObSSTabletCacheMap::const_iterator iter = tablet_cache_info_map.begin();
  for (; OB_SUCC(ret) && (iter != tablet_cache_info_map.end()); ++iter) {
    if (OB_FAIL(tablet_info_list.push_back(iter->second))) {
      LOG_WARN("fail to push back", KR(ret), "tablet_cache_info", iter->second);
    } else if (OB_NOT_NULL(GCTX.location_service_)) {
      bool is_cache_hit = false;
      ObLSID ls_id;
      if (OB_FAIL(GCTX.location_service_->get(tenant_id, iter->first, INT64_MAX, is_cache_hit, ls_id))) {
        if (OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST != ret) {
          LOG_WARN("fail to get", KR(ret), K(tenant_id), "tablet_id", iter->first);
        }
      } else {
        int64_t ls_idx = -1;
        const int64_t ls_cnt = ls_info_list.count();
        for (int64_t i = 0; OB_SUCC(ret) && (i < ls_cnt) && (-1 == ls_idx); ++i) {
          if (ls_id == ls_info_list.at(i).ls_id_) {
            ls_idx = i;
          }
        }

        if (-1 == ls_idx) {
          ObSSLSCacheInfo ls_cache_info;
          ls_cache_info.ls_id_ = ls_id;
          ls_cache_info.t1_size_ = iter->second.t1_size_;
          ls_cache_info.t2_size_ = iter->second.t2_size_;
          ls_cache_info.tablet_cnt_ = 1;
          if (OB_FAIL(ls_info_list.push_back(ls_cache_info))) {
            LOG_WARN("fail to push back", KR(ret), K(ls_cache_info), "tablet_cache_info", iter->second);
          }
        } else {
          ObSSLSCacheInfo &ls_cache_info = ls_info_list.at(ls_idx);
          ls_cache_info.t1_size_ += iter->second.t1_size_;
          ls_cache_info.t2_size_ += iter->second.t2_size_;
          ls_cache_info.tablet_cnt_ += 1;
        }
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int64_t ObSSMicroCacheUtil::calc_ss_cache_mem_limit_size(const uint64_t tenant_id)
{
  const int64_t DEFAULT_CACHE_MEM_PCT = 20;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  const int64_t micro_meta_mem_pct = tenant_config.is_valid() ? 
                                     tenant_config->_ss_micro_cache_memory_percentage : 
                                     DEFAULT_CACHE_MEM_PCT;
  const int64_t tenant_mem = MTL_MEM_SIZE();
  return tenant_mem * (micro_meta_mem_pct / 100.0);
}

int64_t ObSSMicroCacheUtil::calc_ss_cache_expiration_time(const uint64_t tenant_id)
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t expiration_time_s = tenant_config.is_valid() 
                              ? (tenant_config->_ss_local_cache_expiration_time / 1000000L)
                              : 0L;
  if (0L == expiration_time_s) {
    expiration_time_s = SS_DEF_CACHE_EXPIRATION_TIME;
  }
  return expiration_time_s;
}

} // namespace storage
} // namespace oceanbase
