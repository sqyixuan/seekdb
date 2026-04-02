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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_READER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_READER_H_

#include "share/io/ob_io_define.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_struct.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase
{
namespace storage
{

class ObSSLinkedPhyBlockReader final
{
public:
  ObSSLinkedPhyBlockReader();
  ~ObSSLinkedPhyBlockReader() { reset(); };
  ObSSLinkedPhyBlockReader(const ObSSLinkedPhyBlockReader &) = delete;
  ObSSLinkedPhyBlockReader &operator=(const ObSSLinkedPhyBlockReader &) = delete;

  int init(const PhyBlockId &entry_block, const uint64_t tenant_id, ObSSPhysicalBlockManager &phy_blk_mgr);
  int read_next_block(char *&buf, int64_t &buf_data_len);
  void reset();
  
  static bool is_valid_entry_block(const PhyBlockId &entry_block);

private:
  int inner_read_block_(const PhyBlockId &block_id);

private:
  bool is_inited_;
  char *io_buf_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  int64_t phy_block_size_;
  PhyBlockId cur_phy_block_id_;
  ObSSPhyBlockCommonHeader common_header_;
  ObSSLinkedPhyBlockHeader linked_header_;
  ObSSLinkedPhyBlockSegmentHeader seg_header_;
};

class ObSSLinkedPhyBlockItemReader final
{
public:
  ObSSLinkedPhyBlockItemReader();
  ~ObSSLinkedPhyBlockItemReader() { reset(); };
  ObSSLinkedPhyBlockItemReader(const ObSSLinkedPhyBlockItemReader &) = delete;
  ObSSLinkedPhyBlockItemReader &operator=(const ObSSLinkedPhyBlockItemReader &) = delete;

  int init(const PhyBlockId &entry_block, const uint64_t tenant_id, ObSSPhysicalBlockManager &phy_blk_mgr);
  int init(char *buf, const int64_t buf_data_len, const int64_t phy_blk_size); // only used for ob_admin parse single ckpt block.
  int get_next_item(char *&item_buf, int64_t &item_buf_len);
  void reset();

  bool is_inited() const { return is_inited_; }

private:
  int read_item_block_();
  int parse_item_(char *&item_buf, int64_t &item_buf_len);
  int read_item_segment_();
  int alloc_segment_buf_(const uint64_t tenant_id, const int64_t seg_buf_size);
  int parse_segment_();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObSSLinkedPhyBlockReader block_reader_;
  char *buf_;
  int64_t buf_pos_;
  int64_t buf_data_len_;
  ObSSLinkedPhyBlockSegmentHeader seg_header_;
  char *seg_buf_;
  int64_t seg_buf_pos_;
  int64_t seg_buf_size_; // capacity of this buffer
  int64_t seg_data_len_; // valid data length
  common::ObCompressor *compressor_;
  common::ObCompressorType compressor_type_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_READER_H_ */
