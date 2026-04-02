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

#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_util.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"

namespace oceanbase 
{
namespace storage 
{
/*-----------------------------------------ObSSLinkedPhyBlockWriter-----------------------------------------*/
ObSSLinkedPhyBlockWriter::ObSSLinkedPhyBlockWriter()
    : is_inited_(false),
      block_type_(ObSSPhyBlockType::SS_INVALID_BLK_TYPE),
      phy_blk_mgr_(nullptr),
      phy_block_size_(0)
{
}

int ObSSLinkedPhyBlockWriter::init(ObSSPhysicalBlockManager &phy_blk_mgr, const ObSSPhyBlockType block_type)
{
  int ret = OB_SUCCESS;
  phy_block_size_ = phy_blk_mgr.get_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSLinkedPhyBlockWriter has been inited", KR(ret));
  } else if (OB_UNLIKELY(!phy_blk_mgr.is_inited() || phy_block_size_ <= 0 || !is_ckpt_block_type(block_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_mgr.is_inited()), K_(phy_block_size), K(block_type));
  } else {
    phy_blk_mgr_ = &phy_blk_mgr;
    block_type_ = block_type;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLinkedPhyBlockWriter::write_block(
    const char *buf, 
    const int64_t buf_len, 
    PhyBlockId &written_block_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLinkedPhyBlockWriter is not inited", KR(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else {
    ObSSPhysicalBlockHandle phy_blk_handle;
    if (OB_FAIL(phy_blk_mgr_->alloc_block(written_block_id, phy_blk_handle, block_type_))) {
      LOG_WARN("fail to alloc block", KR(ret), K_(block_type));
    } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block handle must be valid", KR(ret), K(written_block_id));
    } else if (OB_FAIL(ObSSMicroCacheIOHelper::write_block(written_block_id * phy_block_size_, buf_len, buf, 
               phy_blk_handle))) {
      LOG_WARN("fail to write micro_cache block", KR(ret), K(written_block_id), K(buf_len), K_(phy_block_size));
    } else {
      phy_blk_handle()->set_sealed(buf_len);
    }
  }
  return ret;
}

void ObSSLinkedPhyBlockWriter::reset()
{
  is_inited_ = false;
  block_type_ = ObSSPhyBlockType::SS_INVALID_BLK_TYPE;
  phy_blk_mgr_ = nullptr;
  phy_block_size_ = 0;
}

/*-----------------------------------------ObSSLinkedPhyBlockItemWriter-----------------------------------------*/
ObSSLinkedPhyBlockItemWriter::ObSSLinkedPhyBlockItemWriter()
    : is_inited_(false), is_closed_(false), block_type_(ObSSPhyBlockType::SS_INVALID_BLK_TYPE), phy_blk_mgr_(nullptr),
      seg_items_cnt_(0), written_items_cnt_(0), block_writer_(), io_buf_(nullptr), io_buf_size_(0),
      io_buf_pos_(0), io_seg_buf_(nullptr), io_seg_buf_size_(0), io_seg_buf_pos_(0), io_seg_comp_buf_(nullptr),
      io_seg_comp_buf_size_(0), common_header_(), linked_header_(), written_block_id_list_(0), 
      compressor_(nullptr), compressor_type_(ObCompressorType::NONE_COMPRESSOR)
{
}

int64_t ObSSLinkedPhyBlockItemWriter::get_blk_payload_offset() const
{ 
  return common_header_.header_size_ + linked_header_.get_fixed_serialize_size(); 
}

void ObSSLinkedPhyBlockItemWriter::reuse_for_next_write_(const PhyBlockId &pre_block_id)
{
  written_items_cnt_ = 0;
  io_buf_pos_ = get_blk_payload_offset();
  common_header_.reset();
  linked_header_.reset();
  linked_header_.set_previous_block_id(pre_block_id);
}

int ObSSLinkedPhyBlockItemWriter::init(
    const uint64_t tenant_id,
    ObSSPhysicalBlockManager &phy_blk_mgr,
    const ObSSPhyBlockType block_type,
    const ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  ObMemAttr io_mem_attr(tenant_id, "SSWCkptIOBuf");
  ObMemAttr seg_mem_attr(tenant_id, "SSWCkptSegBuf");
  const int64_t phy_block_size = phy_blk_mgr.get_block_size();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSLinkedPhyBlockItemWriter has already been inited", KR(ret));
  } else if (OB_UNLIKELY(!phy_blk_mgr.is_inited() || phy_block_size <= 0 || !is_valid_tenant_id(tenant_id) ||
                         !is_ckpt_block_type(block_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        KR(ret), K(phy_blk_mgr.is_inited()), K(phy_block_size), K(block_type), K(tenant_id));
  } else if (OB_FAIL(block_writer_.init(phy_blk_mgr, block_type))) {
    LOG_WARN("fail to init meta block writer", K(block_type), KR(ret));
  } else if (OB_ISNULL(io_buf_ = static_cast<char *>(
      ob_malloc_align(SS_MEM_BUF_ALIGNMENT, phy_block_size, io_mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(phy_block_size), K(io_mem_attr));
  } else if (OB_ISNULL(io_seg_buf_ = static_cast<char *>(ob_malloc(SS_SEG_BUF_SIZE, seg_mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(seg_mem_attr));
  } else if (OB_FAIL(init_compressor_info_(tenant_id, compressor_type))) {
    LOG_WARN("fail to init compressor info", KR(ret), K(tenant_id), K(compressor_type));
  } else {
    block_type_ = block_type;
    phy_blk_mgr_ = &phy_blk_mgr;
    io_buf_size_ = phy_block_size;
    io_seg_buf_size_ = SS_SEG_BUF_SIZE;
    seg_items_cnt_ = 0;
    reuse_for_next_write_(EMPTY_PHY_BLOCK_ID);
    is_inited_ = true;
    is_closed_ = false;
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::init_compressor_info_(
    const uint64_t tenant_id, 
    const ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  if (ObSSLinkedPhyBlockUtil::need_compress(compressor_type)) {
    int64_t max_overflow_size = 0;
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_type, compressor_))) {
      LOG_WARN("fail to get compressor", K(ret), K(compressor_type));
    } else if (OB_ISNULL(compressor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("compressor should not be null", KR(ret), KP_(compressor), K(compressor_type));
    } else if (OB_FAIL(compressor_->get_max_overflow_size(SS_SEG_BUF_SIZE, max_overflow_size))) {
      LOG_WARN("fail to get_max_overflow_size", K(ret), K(compressor_type));
    } else {
      ObMemAttr mem_attr(tenant_id, "SSCkptSegCBuf");
      io_seg_comp_buf_size_ = SS_SEG_BUF_SIZE + max_overflow_size;
      if (OB_ISNULL(io_seg_comp_buf_ = static_cast<char *>(ob_malloc(io_seg_comp_buf_size_, mem_attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(tenant_id), K_(io_seg_comp_buf_size));
      }
    }
  }

  if (OB_SUCC(ret)) {
    compressor_type_ = compressor_type;
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::write_item(const char *item_buf, const int64_t item_buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("item_writer not init", KR(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item_writer has been closed", KR(ret));
  } else if (OB_ISNULL(item_buf) || OB_UNLIKELY(item_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(item_buf), K(item_buf_len));
  } else {
    const int64_t payload_offset = get_blk_payload_offset();
    ObSSLinkedPhyBlockItemHeader item_header;
    item_header.payload_size_ = item_buf_len;
    const int64_t real_item_size = item_header.get_serialize_size() + item_buf_len;
    const int64_t seg_header_size = ObSSLinkedPhyBlockSegmentHeader::get_max_serialize_size();

    if (OB_UNLIKELY(real_item_size + payload_offset + seg_header_size > io_buf_size_ || real_item_size > io_seg_buf_size_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("large item is not supported now", KR(ret), K(real_item_size), K(payload_offset), K(seg_header_size),
        K_(io_buf_size), K(item_buf_len), K_(io_seg_buf_size));
    } else if ((real_item_size > get_seg_remaining_size()) && OB_FAIL(write_segment_())) {
      LOG_WARN("fail to write segment", KR(ret), K(real_item_size), K_(io_seg_buf_size), K_(io_seg_buf_pos));
    }

    if (FAILEDx(write_item_(item_header, item_buf, item_buf_len))) {
      LOG_WARN("fail to write item", KR(ret), K(item_header), KP(item_buf), K(item_buf_len));
    } else {
      ++seg_items_cnt_;
    }
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::write_item_(
    const ObSSLinkedPhyBlockItemHeader &item_header,
    const char *item_buf, 
    const int64_t item_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t remaining_size = get_seg_remaining_size();
  if (OB_UNLIKELY(item_buf_len <= 0 || item_header.get_serialize_size() + item_buf_len > remaining_size) ||
      OB_ISNULL(item_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(item_buf), K(item_header), K(item_buf_len));
  } else if (OB_FAIL(item_header.serialize(io_seg_buf_ + io_seg_buf_pos_, io_seg_buf_size_ - io_seg_buf_pos_, pos))) {
    LOG_WARN("fail to serialize item header", KR(ret), K_(io_seg_buf_pos), K_(io_seg_buf_size), KP_(io_seg_buf));
  } else if (OB_UNLIKELY(item_header.get_serialize_size() != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("position 'pos' does not match the expected serialized size of item header", KR(ret), K(pos), 
      K(item_header.get_serialize_size()), K(item_header));
  } else {
    io_seg_buf_pos_ += pos;
    MEMCPY(io_seg_buf_ + io_seg_buf_pos_, item_buf, item_buf_len);
    io_seg_buf_pos_ += item_buf_len;
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::write_segment_()
{
  int ret = OB_SUCCESS;
  ObSSLinkedPhyBlockSegmentHeader seg_header;
  if (OB_FAIL(build_seg_header_(seg_header))) {
    LOG_WARN("fail to build segment header", KR(ret), K(seg_header));
  } else {
    const ObCompressorType cur_comp_type = static_cast<ObCompressorType>(seg_header.compressor_type_);
    const bool is_compressed = ObSSLinkedPhyBlockUtil::need_compress(cur_comp_type);
    const char *seg_buf = is_compressed ? io_seg_comp_buf_ : io_seg_buf_;
    const int64_t seg_buf_len = seg_header.get_real_payload_size();
    const int64_t total_seg_len = seg_header.get_serialize_size() + seg_buf_len;
    if (total_seg_len > get_remaining_size()) {
      if (OB_FAIL(write_block_())) {
        LOG_WARN("fail to write block", KR(ret));
      }
    }

    if (FAILEDx(do_write_segment_(seg_header, seg_buf, seg_buf_len))) {
      LOG_WARN("fail to do_write_segment", KR(ret), K(seg_header), KP(seg_buf), K(seg_buf_len));
    }
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::build_seg_header_(ObSSLinkedPhyBlockSegmentHeader &seg_header)
{
  int ret = OB_SUCCESS;
  int64_t payload_zsize = 0;
  int64_t payload_size = io_seg_buf_pos_;
  if (ObSSLinkedPhyBlockUtil::need_compress(compressor_type_)) {
    if (OB_ISNULL(compressor_) || OB_ISNULL(io_seg_comp_buf_) || (io_seg_comp_buf_size_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected compress info", KR(ret), KP_(compressor), KP_(io_seg_comp_buf), K_(io_seg_comp_buf_size));
    } else if (OB_FAIL(compressor_->compress(io_seg_buf_, io_seg_buf_pos_, io_seg_comp_buf_, io_seg_comp_buf_size_, 
               payload_zsize))) {
      LOG_WARN("fail to compress", K(ret), K_(compressor_type), K_(io_seg_buf_pos), K_(io_seg_comp_buf_size));
    }
  }

  if (OB_SUCC(ret)) {
    seg_header.payload_size_ = io_seg_buf_pos_;
    seg_header.payload_crc_ = static_cast<uint32_t>(ob_crc64(io_seg_buf_, io_seg_buf_pos_));
    if (payload_zsize > 0 && payload_zsize < io_seg_buf_pos_) {
      seg_header.payload_zsize_ = payload_zsize;
      seg_header.compressor_type_ = compressor_type_;
    }
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::do_write_segment_(
    const ObSSLinkedPhyBlockSegmentHeader &seg_header,
    const char *seg_buf, 
    const int64_t seg_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t remaining_size = get_remaining_size();
  if (OB_ISNULL(seg_buf) || OB_UNLIKELY(!seg_header.is_valid()) || 
      OB_UNLIKELY(seg_buf_len <= 0 || seg_header.get_serialize_size() + seg_buf_len > remaining_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(seg_buf), K(seg_buf_len), K(remaining_size), K(seg_header), 
      K(seg_header.get_serialize_size()));
  } else if (OB_FAIL(seg_header.serialize(io_buf_ + io_buf_pos_, io_buf_size_ - io_buf_pos_, pos))) {
    LOG_WARN("fail to serialize segment header", KR(ret), K_(io_buf_pos), K_(io_buf_size),
      KP_(io_buf));
  } else if (OB_UNLIKELY(seg_header.get_serialize_size() != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("position 'pos' does not match the expected serialized size of segment header", KR(ret), K(pos),
      K(seg_header.get_serialize_size()), K(seg_header));
  } else {
    io_buf_pos_ += pos;
    MEMCPY(io_buf_ + io_buf_pos_, seg_buf, seg_buf_len);
    io_buf_pos_ += seg_buf_len;
    io_seg_buf_pos_ = 0;
    written_items_cnt_ += seg_items_cnt_;
    seg_items_cnt_ = 0;
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::write_block_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  linked_header_.item_count_ = written_items_cnt_;
  if (OB_UNLIKELY(linked_header_.item_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item count is unexpected", KR(ret), K_(linked_header));
  } else if (OB_ISNULL(phy_blk_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KP_(phy_blk_mgr), K_(is_inited), K_(is_closed));
  } else if (OB_UNLIKELY(!phy_blk_mgr_->exist_free_block(block_type_))) {
    ret = OB_EAGAIN;
    LOG_WARN("not exist free phy_block to save ckpt", KR(ret), K_(block_type), K(phy_blk_mgr_->get_blk_cnt_info()));
  } else {
    PhyBlockId pre_block_id = EMPTY_PHY_BLOCK_ID;
    const int32_t cm_header_size = common_header_.header_size_;
    const int32_t payload_offset = get_blk_payload_offset();
    linked_header_.set_payload_size(io_buf_pos_ - payload_offset);
    const int64_t upper_align_size = upper_align(io_buf_pos_, DIO_ALIGN_SIZE);
    
    int64_t pos = cm_header_size;
    if (OB_FAIL(linked_header_.serialize(io_buf_, io_buf_size_, pos))) {
      LOG_WARN("fail to serialize linked header", KR(ret), K(pos), KP_(io_buf), K_(io_buf_size));
    } else if (OB_UNLIKELY(linked_header_.get_serialize_size() > linked_header_.get_fixed_serialize_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("must reserve enough space for linked header", KR(ret), K(linked_header_.get_serialize_size()),
        K(linked_header_.get_fixed_serialize_size()));
    } else if (OB_UNLIKELY(linked_header_.get_serialize_size() != (pos - cm_header_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("position 'pos' does not match the expected serialized size of linked_header", KR(ret), K(pos),
        K(linked_header_.get_serialize_size()), K_(linked_header), K(cm_header_size));
    } else if (FALSE_IT(common_header_.set_payload_size(static_cast<int32_t>(io_buf_pos_ - cm_header_size)))) {
    } else if (FALSE_IT(common_header_.set_block_type(block_type_))) {
    } else if (FALSE_IT(common_header_.calc_payload_checksum(io_buf_ + cm_header_size, common_header_.payload_size_))) {
    } else if (FALSE_IT(pos = 0)) {
    } else if (OB_FAIL(common_header_.serialize(io_buf_, io_buf_size_, pos))) {
      LOG_WARN("fail to serialize common header", KR(ret), K(pos), KP_(io_buf), K_(io_buf_size));
    } else if (OB_UNLIKELY(cm_header_size != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("position 'pos' does not match the expected serialized size of common_header", KR(ret), K(pos),
        K(linked_header_.get_serialize_size()), K_(linked_header), K(cm_header_size));
    } else if (OB_FAIL(block_writer_.write_block(io_buf_, upper_align_size, pre_block_id))) {
      LOG_WARN("fail to write block", KR(ret), KP_(io_buf), K(upper_align_size), K_(linked_header));
    }

    // If we succ to alloc a phy_block, but fail to write_block, we need to push_back this block_id into list.
    // Then we can reuse these phy_blocks in phy_blk_ckpt function.
    if ((EMPTY_PHY_BLOCK_ID != pre_block_id) && OB_TMP_FAIL(written_block_id_list_.push_back(pre_block_id))) {
      LOG_ERROR("fail to record block_id", KR(ret), KR(tmp_ret), K_(linked_header), K(pre_block_id), "block_cnt",
        written_block_id_list_.count(), K_(written_block_id_list));
    }
    
    if (OB_SUCC(ret)) {
      reuse_for_next_write_(pre_block_id);
    }
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::get_entry_block(PhyBlockId &entry_block) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSLinkedPhyBlockItemWriter is not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSLinkedPhyBlockItemWriter must be closed when get entry block", KR(ret));
  } else {
    if (0 == written_block_id_list_.count()) {
      LOG_INFO("no block items has been write");
      entry_block = EMPTY_PHY_BLOCK_ID;
    } else {
      entry_block = linked_header_.get_previous_block_id();
    }
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::get_block_id_list(common::ObIArray<PhyBlockId> &block_id_list) const
{
  int ret = OB_SUCCESS;
  block_id_list.reset();
  block_id_list.reserve(written_block_id_list_.count());
  for (int64_t i = written_block_id_list_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    if (OB_FAIL(block_id_list.push_back(written_block_id_list_[i]))) {
      OB_LOG(WARN, "fail to get_block_id_list", K(ret), K(i), K(written_block_id_list_.count()));
    }
  }
  return ret;
}

int ObSSLinkedPhyBlockItemWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSLinkedPhyBlockItemWriter is not inited", KR(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSLinkedPhyBlockItemWriter has been closed", KR(ret));
  } else if ((io_seg_buf_pos_ > 0) && OB_FAIL(write_segment_())) {
    LOG_WARN("fail to write segment", KR(ret), K_(io_seg_buf_pos));
  }
  
  if (OB_SUCC(ret)) {
    const int64_t payload_offset = get_blk_payload_offset();
    if ((io_buf_pos_ > payload_offset) && OB_FAIL(write_block_())) {
      LOG_WARN("fail to write block", KR(ret), K_(io_buf_pos));
    }
  }

  if (OB_SUCC(ret)) {
    is_closed_ = true;
  }
  return ret;
}

void ObSSLinkedPhyBlockItemWriter::reset()
{
  is_inited_ = false;
  is_closed_ = false;
  block_type_ = ObSSPhyBlockType::SS_INVALID_BLK_TYPE;
  seg_items_cnt_ = 0;
  written_items_cnt_ = 0;
  if (OB_NOT_NULL(io_buf_)) {
    ob_free_align(io_buf_);
    io_buf_ = nullptr;
  }
  io_buf_size_ = 0;
  io_buf_pos_ = 0;
  if (OB_NOT_NULL(io_seg_buf_)) {
    ob_free(io_seg_buf_);
    io_seg_buf_ = nullptr;
  }
  io_seg_buf_size_ = 0;
  io_seg_buf_pos_ = 0;
  if (OB_NOT_NULL(io_seg_comp_buf_)) {
    ob_free(io_seg_comp_buf_);
    io_seg_comp_buf_ = nullptr;
  }
  io_seg_comp_buf_size_ = 0;
  common_header_.reset();
  linked_header_.reset();
  block_writer_.reset();
  written_block_id_list_.reset();
  phy_blk_mgr_ = nullptr;
  compressor_ = nullptr;
  compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
}

} /* namespace storage */
} /* namespace oceanbase */
