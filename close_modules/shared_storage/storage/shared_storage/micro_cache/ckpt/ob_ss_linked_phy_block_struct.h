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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_STRUCT_H_

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace storage
{
// Temporary replacement
using PhyBlockId = int64_t;
const PhyBlockId EMPTY_PHY_BLOCK_ID = -1;

// The purpose of implementing custom serialization and deserialization functions for the ObSSLinkedPhyBlockHeader class
// is to address a specific requirement that the serialization result must be of a fixed length. This fixed-length constraint
// is necessary because the actual content of the header, including payload size and checksum, can only be determined after
// the block is fully written. The 'OB_SERIALIZE_MEMBER' functions produce variable-length results,
// which change with the values of the member variables. However, to reserve a fixed length of buffer for the header
// serialization before writing, and to ensure the integrity and predictability of the header's format in our persistent block
// objects, a tailored approach is required. This custom implementation will allow us to predetermine the buffer size needed
// for the header, accommodating the eventual payload size and checksum information, thereby adhering to the specifications 
// for persistent storage of block objects.
struct ObSSLinkedPhyBlockHeader final
{
public:
  static const int32_t SS_LINKED_PHY_BLOCK_HEADER_MAGIC = 12345;

  ObSSLinkedPhyBlockHeader()
  {
    reset();
  }
  ~ObSSLinkedPhyBlockHeader() = default;
  void reset();
  bool is_valid() const;
  const PhyBlockId &get_previous_block_id() const
  {
    return previous_phy_block_id_;
  }
  void set_previous_block_id(const PhyBlockId &block_id)
  {
    previous_phy_block_id_ = block_id;
  }
  void set_payload_checksum(const uint32_t payload_checksum)
  {
    payload_checksum_ = payload_checksum;
  }
  uint32_t get_payload_checksum() const
  {
    return payload_checksum_;
  }
  int32_t get_payload_size() const
  {
    return payload_size_;
  }
  void set_payload_size(const int32_t payload_size)
  {
    payload_size_ = payload_size;
  }
  static int64_t get_fixed_serialize_size();

  TO_STRING_KV(K_(magic), K_(item_count), K_(payload_size), K_(payload_checksum), K_(previous_phy_block_id));

  int32_t magic_;
  int32_t item_count_;
  int32_t payload_size_;
  uint32_t payload_checksum_;
  PhyBlockId previous_phy_block_id_;

public:
  static const int64_t OB_SS_LINKED_PHY_BLOCK_HEADER_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_LINKED_PHY_BLOCK_HEADER_VERSION);
};

/*
 * Each segment contains several items; We can compress segment and save the meta into its header.
 */
constexpr int64_t SS_SEG_BUF_SIZE = 32 * 1024;
struct ObSSLinkedPhyBlockSegmentHeader final
{
public:
  ObSSLinkedPhyBlockSegmentHeader() { reset(); }
  ~ObSSLinkedPhyBlockSegmentHeader() = default;
  void reset();
  bool need_compress() const;
  uint8_t get_compressor_type() const;
  bool is_valid() const { return payload_size_ > 0; }
  int32_t get_real_payload_size() const;
  static int64_t get_max_serialize_size();

  TO_STRING_KV(K_(payload_size), K_(payload_zsize), K_(payload_crc), K_(attr));

  int32_t payload_size_;
  int32_t payload_zsize_;
  uint32_t payload_crc_;
  union {
    uint32_t attr_;
    struct {
      uint32_t compressor_type_ : 8;
      uint32_t reserved_ : 24;
    };
  };

public:
  static const int64_t OB_SS_LINKED_PHY_BLOCK_SEGMENT_HEADER_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_LINKED_PHY_BLOCK_SEGMENT_HEADER_VERSION);
};

struct ObSSLinkedPhyBlockItemHeader final
{
public:
  ObSSLinkedPhyBlockItemHeader()
    : payload_size_(0), payload_crc_(0)
  {}
  ~ObSSLinkedPhyBlockItemHeader() = default;
  static int64_t get_max_serialize_size() { return (sizeof(int32_t) + 1) * 2 + SS_SERIALIZE_EXTRA_BUF_LEN; }

  TO_STRING_KV(K_(payload_size), K_(payload_crc));

  int32_t payload_size_;
  uint32_t payload_crc_;

public:
  static const int64_t OB_SS_LINKED_PHY_BLOCK_ITEM_HEADER_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_LINKED_PHY_BLOCK_ITEM_HEADER_VERSION);
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_LINKED_PHY_BLOCK_STRUCT_H_ */
