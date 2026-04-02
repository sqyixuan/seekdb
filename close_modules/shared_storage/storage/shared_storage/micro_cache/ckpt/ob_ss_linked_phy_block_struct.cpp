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

#include "lib/compress/ob_compress_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_struct.h"

namespace oceanbase 
{
namespace storage 
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSLinkedPhyBlockHeader-----------------------------------------*/
bool ObSSLinkedPhyBlockHeader::is_valid() const
{
  return (SS_LINKED_PHY_BLOCK_HEADER_MAGIC == magic_) && (payload_size_ > 0);
}

OB_SERIALIZE_MEMBER(ObSSLinkedPhyBlockHeader, magic_, item_count_, payload_size_, 
  payload_checksum_, previous_phy_block_id_);

int64_t ObSSLinkedPhyBlockHeader::get_fixed_serialize_size()
{
  // cuz 'version' and 'len' will be added, so need to add 'SS_SERIALIZE_EXTRA_BUF_LEN'
  // If we want to add fields in the future, 
  //   in the one hand, we should consider 'compat' here. It should be like:
  //    if (version == 1) { return X; }
  //    else if (version == 2) { return Y; }
  //
  //   in the other hand, we must ensure that, the returned value must not be less than the
  //   variable-encoded size, cuz we need to notice that, a int32_t may occupy 5 bytes.
  return sizeof(ObSSLinkedPhyBlockHeader) + SS_SERIALIZE_EXTRA_BUF_LEN;
}

void ObSSLinkedPhyBlockHeader::reset()
{
  magic_ = SS_LINKED_PHY_BLOCK_HEADER_MAGIC;
  item_count_ = 0;
  payload_size_ = 0;
  payload_checksum_ = 0;
  previous_phy_block_id_ = EMPTY_PHY_BLOCK_ID;
}

/*-----------------------------------------ObSSLinkedPhyBlockSegmentHeader-----------------------------------------*/
bool ObSSLinkedPhyBlockSegmentHeader::need_compress() const
{
  ObCompressorType comp_type = static_cast<ObCompressorType>(compressor_type_);
  return ObSSLinkedPhyBlockUtil::need_compress(comp_type);
}

uint8_t ObSSLinkedPhyBlockSegmentHeader::get_compressor_type() const
{
  return static_cast<uint8_t>(compressor_type_);
}

int32_t ObSSLinkedPhyBlockSegmentHeader::get_real_payload_size() const
{
  int32_t real_size = 0;
  if (need_compress()) {
    real_size = payload_zsize_;
  } else {
    real_size = payload_size_;
  }
  return real_size;
}

int64_t ObSSLinkedPhyBlockSegmentHeader::get_max_serialize_size()
{
  return sizeof(ObSSLinkedPhyBlockSegmentHeader) + sizeof(uint32_t) + SS_SERIALIZE_EXTRA_BUF_LEN;
}

OB_SERIALIZE_MEMBER(ObSSLinkedPhyBlockSegmentHeader, payload_size_, payload_zsize_, payload_crc_, attr_);

void ObSSLinkedPhyBlockSegmentHeader::reset()
{
  payload_size_ = 0;
  payload_zsize_ = 0;
  payload_crc_ = 0;
  compressor_type_ = static_cast<uint32_t>(ObCompressorType::NONE_COMPRESSOR);
  reserved_ = 0;
}

/*-----------------------------------------ObSSLinkedPhyBlockItemHeader-----------------------------------------*/
OB_SERIALIZE_MEMBER(ObSSLinkedPhyBlockItemHeader, payload_size_, payload_crc_);

} /* namespace storage */
} /* namespace oceanbase */
