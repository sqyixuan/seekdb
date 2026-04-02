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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_WRITER_H_

#include "share/io/ob_io_define.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_struct.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase
{
namespace storage
{

/*
 * Usage: ObSSLinkedPhyBlockItemWriter -> init -> write_item -> close -> get_entry_block
 *        ObSSLinkedPhyBlockItemReader -> init -> get_next_item (until get error code OB_ITER_END)
 * 
 * The ObSSLinkedPhyBlockWriter/Reader classes are designed to
 * handle the writing/reading of data blocks to/from the disk.
 * The Writer class is responsible for writing data to the disk
 * once the in-memory block is filled with items,
 * while the Reader class facilitates the retrieval of these blocks from the disk into memory.
 * These classes are intended for internal operation and should not be directly manipulated by the user.
 * 
 */
class ObSSLinkedPhyBlockWriter final
{
public:
  ObSSLinkedPhyBlockWriter();
  ~ObSSLinkedPhyBlockWriter() { reset(); }
  ObSSLinkedPhyBlockWriter(const ObSSLinkedPhyBlockWriter &) = delete;
  ObSSLinkedPhyBlockWriter &operator=(const ObSSLinkedPhyBlockWriter &) = delete;

  int init(ObSSPhysicalBlockManager &phy_blk_mgr, const ObSSPhyBlockType block_type);
  int write_block(const char *buf, const int64_t buf_len, PhyBlockId &written_block_id);
  void reset();

private:
  bool is_inited_;
  ObSSPhyBlockType block_type_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  int64_t phy_block_size_;
};

class ObSSLinkedPhyBlockItemWriter final
{
public:
  ObSSLinkedPhyBlockItemWriter();
  ~ObSSLinkedPhyBlockItemWriter() { reset(); }
  ObSSLinkedPhyBlockItemWriter(const ObSSLinkedPhyBlockItemWriter &) = delete;
  ObSSLinkedPhyBlockItemWriter &operator=(const ObSSLinkedPhyBlockItemWriter &) = delete;

  int init(const uint64_t tenant_id, ObSSPhysicalBlockManager &phy_blk_mgr, const ObSSPhyBlockType block_type,
           const common::ObCompressorType compressor_type = ObCompressorType::NONE_COMPRESSOR);
  int write_item(const char *item_buf, const int64_t item_buf_len);
  int close();
  void reset();

  // must be called after closed
  int get_entry_block(PhyBlockId &entry_block) const;
  int get_block_id_list(common::ObIArray<PhyBlockId> &block_id_list) const;
  int64_t get_remaining_size() const { return io_buf_size_ - io_buf_pos_; }
  int64_t get_seg_remaining_size() const { return io_seg_buf_size_ - io_seg_buf_pos_; }

private:
  int init_compressor_info_(const uint64_t tenant_id, const common::ObCompressorType compressor_type);
  // exclude common_header and block header, and start from this position.
  int64_t get_blk_payload_offset() const;
  void reuse_for_next_write_(const PhyBlockId &pre_block_id);
  int write_block_();
  int write_item_(const ObSSLinkedPhyBlockItemHeader &item_header, const char *item_buf, const int64_t item_buf_len);
  int write_segment_();
  int build_seg_header_(ObSSLinkedPhyBlockSegmentHeader &seg_header);
  int do_write_segment_(const ObSSLinkedPhyBlockSegmentHeader &seg_header, 
                        const char *item_buf, 
                        const int64_t item_buf_len);

private:
  bool is_inited_;
  bool is_closed_;
  ObSSPhyBlockType block_type_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  int64_t seg_items_cnt_;    // count of items which were written into segment
  int64_t written_items_cnt_;// count of items which were written into whole io_buf

  ObSSLinkedPhyBlockWriter block_writer_;

  // buf for write io
  char *io_buf_;
  int64_t io_buf_size_;
  int64_t io_buf_pos_;
  char *io_seg_buf_;
  int64_t io_seg_buf_size_;
  int64_t io_seg_buf_pos_;
  char *io_seg_comp_buf_;
  int64_t io_seg_comp_buf_size_;
  ObSSPhyBlockCommonHeader common_header_;
  ObSSLinkedPhyBlockHeader linked_header_;
  ObArray<PhyBlockId> written_block_id_list_;
  common::ObCompressor *compressor_;
  common::ObCompressorType compressor_type_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_WRITER_H_ */
