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

#ifndef OCEANBASE_SHARED_STORAGE_HIGH_AVAILABILITY_OB_MIGRATION_WARMUP_STRUCT_H_
#define OCEANBASE_SHARED_STORAGE_HIGH_AVAILABILITY_OB_MIGRATION_WARMUP_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"

namespace oceanbase
{
namespace storage
{
// migration warmup job info
struct ObMigrationCacheJobInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObMigrationCacheJobInfo();
  ObMigrationCacheJobInfo(const int64_t start_blk_idx, const int64_t end_blk_idx);
  ~ObMigrationCacheJobInfo() {}
  bool is_valid() const;
  void reset();
  int assign(const ObMigrationCacheJobInfo &other);
  int convert_from(const ObSSPhyBlockIdxRange &block_range);
  TO_STRING_KV(
      K_(start_blk_idx),
      K_(end_blk_idx));
public:
  int64_t start_blk_idx_;
  int64_t end_blk_idx_;
};

// migration warmup related : micro block key set
struct ObCopyMicroBlockKeySet final
{
  OB_UNIS_VERSION(1);
public:
  ObCopyMicroBlockKeySet();
  ~ObCopyMicroBlockKeySet();
  bool is_valid() const;
  void reset();
  int assign(const ObCopyMicroBlockKeySet &other);
  TO_STRING_KV(
      K_(blk_idx),
      K_(micro_block_key_metas));
public:
  int64_t blk_idx_;
  common::ObSArray<ObSSMicroBlockCacheKeyMeta> micro_block_key_metas_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCopyMicroBlockKeySet);
};

// migration warmup key set header
struct ObCopyMicroBlockKeySetRpcHeader final
{
  OB_UNIS_VERSION(1);
public:
  enum ConnectStatus
  {
    RECONNECT = 0,
    ENDCONNECT = 1,
    MAX_STATUS
  };
  ObCopyMicroBlockKeySetRpcHeader();
  ~ObCopyMicroBlockKeySetRpcHeader();
  bool is_valid() const;
  void reset();
  bool need_reconnect() { return RECONNECT == connect_status_; }
  bool end_connect() { return ENDCONNECT == connect_status_; }
  TO_STRING_KV(K_(object_count), K_(end_blk_idx), K_(connect_status));
public:
  int32_t object_count_;
  int64_t end_blk_idx_;
  ConnectStatus connect_status_;
};
}
}
#endif
