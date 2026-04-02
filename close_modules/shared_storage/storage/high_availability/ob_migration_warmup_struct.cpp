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
#include "ob_migration_warmup_struct.h"

namespace oceanbase
{
namespace storage
{
/******************ObMigrationCacheJobInfo*********************/
ObMigrationCacheJobInfo::ObMigrationCacheJobInfo()
  : start_blk_idx_(0),
    end_blk_idx_(0)
{
}

ObMigrationCacheJobInfo::ObMigrationCacheJobInfo(
    const int64_t start_blk_idx,
    const int64_t end_blk_idx)
  : start_blk_idx_(start_blk_idx), end_blk_idx_(end_blk_idx)
{
}

int ObMigrationCacheJobInfo::assign(const ObMigrationCacheJobInfo &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // do nothing
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    start_blk_idx_ = other.start_blk_idx_;
    end_blk_idx_ = other.end_blk_idx_;
  }
  return ret;
}

bool ObMigrationCacheJobInfo::is_valid() const
{
  return start_blk_idx_ >= 0 && end_blk_idx_ >= start_blk_idx_ && end_blk_idx_ != 0;
}

void ObMigrationCacheJobInfo::reset()
{
  start_blk_idx_ = 0;
  end_blk_idx_ = 0;
}

int ObMigrationCacheJobInfo::convert_from(const ObSSPhyBlockIdxRange &block_range)
{
  int ret = OB_SUCCESS;
  if (!block_range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_range));
  } else {
    start_blk_idx_ = block_range.start_blk_idx_;
    end_blk_idx_ = block_range.end_blk_idx_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMigrationCacheJobInfo, start_blk_idx_, end_blk_idx_);
/******************ObCopyMicroBlockKeySet*********************/
ObCopyMicroBlockKeySet::ObCopyMicroBlockKeySet()
  : blk_idx_(0),
    micro_block_key_metas_()
{
}

ObCopyMicroBlockKeySet::~ObCopyMicroBlockKeySet()
{
}

int ObCopyMicroBlockKeySet::assign(const ObCopyMicroBlockKeySet &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // do nothing
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    blk_idx_ = other.blk_idx_;
    if (OB_FAIL(micro_block_key_metas_.assign(other.micro_block_key_metas_))) {
      LOG_WARN("fail to assign keys", KR(ret), K(other));
    }
  }
  return ret;
}

bool ObCopyMicroBlockKeySet::is_valid() const
{
  return blk_idx_ > 0 && !micro_block_key_metas_.empty();
}

void ObCopyMicroBlockKeySet::reset()
{
  blk_idx_ = 0;
  micro_block_key_metas_.reset();
}

OB_SERIALIZE_MEMBER(ObCopyMicroBlockKeySet, blk_idx_, micro_block_key_metas_);
/******************ObCopyMicroBlockKeySetRpcHeader*********************/
ObCopyMicroBlockKeySetRpcHeader::ObCopyMicroBlockKeySetRpcHeader()
  : object_count_(0),
    end_blk_idx_(0),
    connect_status_(MAX_STATUS)
{
}

ObCopyMicroBlockKeySetRpcHeader::~ObCopyMicroBlockKeySetRpcHeader()
{
}

bool ObCopyMicroBlockKeySetRpcHeader::is_valid() const
{
  return object_count_ >= 0
      && connect_status_ >= RECONNECT
      && connect_status_ < MAX_STATUS
      && end_blk_idx_ > 0;
}

void ObCopyMicroBlockKeySetRpcHeader::reset()
{
  object_count_ = 0;
  end_blk_idx_ = 0;
  connect_status_ = MAX_STATUS;
}
OB_SERIALIZE_MEMBER(ObCopyMicroBlockKeySetRpcHeader, object_count_, end_blk_idx_, connect_status_);
}
}
