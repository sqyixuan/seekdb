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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_META_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_META_H_

#include "storage/shared_storage/ob_ss_common_header.h"

namespace oceanbase
{
namespace storage
{

constexpr int64_t DEFAULT_META_FILE_SIZE_PCT = 7;
constexpr int64_t DEFAULT_MICRO_CACHE_SIZE_PCT = 40;
constexpr int64_t DEFAULT_TMP_FILE_WRITE_CACHE_SIZE_PCT = 5;
constexpr int64_t DEFAULT_PREREAD_CACHE_SIZE_PCT = 10;
constexpr int64_t DEFAULT_PRIVATE_MACRO_SIZE_PCT = 38;
constexpr int64_t DEFAULT_MB = 1024 * 1024L;

struct ObTenantDiskCacheInfo
{
public:
  int64_t reserved_size_;
  int64_t space_percent_;

  ObTenantDiskCacheInfo() { reset(); }
  explicit ObTenantDiskCacheInfo(const int64_t space_percent);
  ~ObTenantDiskCacheInfo() { reset(); }
  void reset();
  bool is_valid() const;
  OB_INLINE int64_t get_reserved_size() const { return reserved_size_; }
  OB_INLINE void set_reserved_size (const int64_t reserved_size) { reserved_size_ = reserved_size; }
  void update_reserved_size(const int64_t total_size) { reserved_size_ = total_size * space_percent_ / 100; }
  OB_INLINE int64_t get_space_percent() const { return space_percent_; }
  OB_INLINE void set_space_percent(const int64_t space_pct) { space_percent_ = space_pct; }

  TO_STRING_KV(K_(reserved_size), "reserved_size_MB", (reserved_size_ / DEFAULT_MB), K_(space_percent));
};

struct ObTenantDiskCacheAllocInfo
{
public:
  int64_t meta_file_alloc_size_;
  int64_t tmp_file_write_cache_alloc_size_;
  int64_t tmp_file_read_cache_alloc_size_;
  int64_t major_macro_read_cache_alloc_size_;
  int64_t private_macro_alloc_size_;

  ObTenantDiskCacheAllocInfo() { reset(); }
  ~ObTenantDiskCacheAllocInfo() { reset(); }
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(meta_file_alloc_size), "meta_file_alloc_MB", (meta_file_alloc_size_ / DEFAULT_MB),
    K_(tmp_file_write_cache_alloc_size), "tmp_file_write_alloc_MB", (tmp_file_write_cache_alloc_size_ / DEFAULT_MB),
    K_(tmp_file_read_cache_alloc_size), "tmp_file_read_alloc_MB", (tmp_file_read_cache_alloc_size_ / DEFAULT_MB),
    K_(major_macro_read_cache_alloc_size), "major_macro_read_alloc_MB", (major_macro_read_cache_alloc_size_ / DEFAULT_MB),
    K_(private_macro_alloc_size), "private_macro_alloc_MB", (private_macro_alloc_size_ / DEFAULT_MB));
};

struct ObTenantAllDiskCacheInfo
{
public:
  ObTenantDiskCacheInfo meta_file_cache_;
  ObTenantDiskCacheInfo micro_cache_;
  ObTenantDiskCacheInfo tmp_file_write_cache_;
  ObTenantDiskCacheInfo preread_cache_;
  ObTenantDiskCacheInfo private_macro_cache_;
  ObTenantDiskCacheAllocInfo alloc_info_;

  ObTenantAllDiskCacheInfo() { reset(); }
  ~ObTenantAllDiskCacheInfo() { reset(); }

  int init(const int64_t total_disk_size);
  int update_reserved_size(const int64_t total_disk_size);
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(meta_file_cache), K_(micro_cache), K_(tmp_file_write_cache), K_(preread_cache), 
    K_(private_macro_cache), K_(alloc_info));
};

struct ObTenantDiskCacheRatioInfo final
{
public:
  int64_t meta_file_size_pct_;
  int64_t micro_cache_size_pct_;
  int64_t tmp_file_write_cache_size_pct_;
  int64_t preread_cache_size_pct_;
  int64_t private_macro_size_pct_;

  ObTenantDiskCacheRatioInfo() { reset(); }
  ~ObTenantDiskCacheRatioInfo() { reset(); }

  void reset();
  bool is_valid() const;
  int encoded_str(char* buf, const int64_t buf_len, int64_t& pos) const;
  int decoded_str(char *token, bool &succ_decoded);

  ObTenantDiskCacheRatioInfo &operator=(const ObTenantDiskCacheRatioInfo &other);
  bool operator==(const ObTenantDiskCacheRatioInfo &other) const;

  TO_STRING_KV(K_(meta_file_size_pct), K_(micro_cache_size_pct), K_(tmp_file_write_cache_size_pct), 
    K_(preread_cache_size_pct), K_(private_macro_size_pct));

private:
  int64_t get_total_pct() const;

public:
  static const int64_t META_BUFF_SIZE = 1024;
  static const int64_t OB_TENANT_CACHE_DISK_RATIO_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_TENANT_CACHE_DISK_RATIO_INFO_VERSION);
public:
  const char *const META_FILE_SIZE_PCT_STR = "meta_file_size_pct=";
  const char *const MICRO_CACHE_SIZE_PCT_STR = "micro_cache_size_pct=";
  const char *const TMP_FILE_WRITE_CACHE_SIZE_PCT_STR = "tmp_file_write_cache_size_pct=";
  const char *const PREREAD_CACHE_SIZE_PCT_STR = "preread_cache_size_pct=";
  const char *const PRIVATE_MACRO_SIZE_PCT_STR = "private_macro_size_pct=";
};

struct ObTenantDiskSpaceMetaBody final
{
public:
  static const int32_t TENANT_DISK_SPACE_VERSION = 1;
  static const int32_t TENANT_DISK_SPACE_VERSION_2 = 2;
  static const int64_t META_BUFF_SIZE = 4096;
  static const int64_t META_ITEM_SIZE = 128;
  const char *const TENANT_ID_STR = "tenant_id=";
  const char *const VERSION_ID_STR = "version_id=";
  const char *const ALLOC_META_FILE_SIZE_STR = "meta_file_alloc_size=";
  const char *const ALLOC_PRIVATE_MACRO_SIZE_STR = "private_macro_alloc_size=";
  const char *const ALLOC_TMP_FILE_WRITE_CACHE_SIZE_STR = "tmp_file_write_cache_alloc_size=";
  const char *const ALLOC_TMP_FILE_READ_CACHE_SIZE_STR = "tmp_file_read_cache_alloc_size=";
  const char *const ALLOC_MAJOR_MACRO_READ_CACHE_SIZE_STR = "major_macro_read_cache_alloc_size=";
  ObTenantDiskSpaceMetaBody();
  ~ObTenantDiskSpaceMetaBody() = default;
  bool is_valid() const;
  void reset();
  int encoded_str(char* buf, const int64_t buf_len, int64_t& pos) const;
  int decoded_str(char *token, bool &succ_decoded);
  int assign_by_disk_info(const ObTenantAllDiskCacheInfo &cache_disk_info);

  TO_STRING_KV(K_(tenant_id),
               K_(version),
               K_(meta_file_alloc_size),
               K_(private_macro_alloc_size),
               K_(tmp_file_write_cache_alloc_size),
               K_(tmp_file_read_cache_alloc_size),
               K_(major_macro_read_cache_alloc_size),
               K_(disk_cache_ratio));
  
  OB_UNIS_VERSION(TENANT_DISK_SPACE_VERSION_2);
public:
  uint64_t tenant_id_;
  int64_t version_;
  int64_t meta_file_alloc_size_;
  int64_t private_macro_alloc_size_;
  int64_t tmp_file_write_cache_alloc_size_;
  int64_t tmp_file_read_cache_alloc_size_;
  int64_t major_macro_read_cache_alloc_size_;
  ObTenantDiskCacheRatioInfo disk_cache_ratio_;
};

struct ObTenantDiskSpaceMeta final
{
public:
  ObTenantDiskSpaceMeta();
  ObTenantDiskSpaceMeta(const uint64_t tenant_id);
  ~ObTenantDiskSpaceMeta() = default;
  ObTenantDiskSpaceMeta &operator==(const ObTenantDiskSpaceMeta &other) = delete;
  ObTenantDiskSpaceMeta &operator!=(const ObTenantDiskSpaceMeta &other) = delete;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(header),
               K_(body));
  OB_UNIS_VERSION(1);
public:
  ObSSCommonHeader header_;
  ObTenantDiskSpaceMetaBody body_;
};

struct ObLocalCacheHitStat
{
public:
  int64_t cache_hit_cnt_;
  int64_t cache_hit_bytes_;
  int64_t cache_miss_cnt_;
  int64_t cache_miss_bytes_;
  ObLocalCacheHitStat() { reset(); }
  ~ObLocalCacheHitStat() = default;
  void reset ()
  {
    cache_hit_cnt_ = 0;
    cache_hit_bytes_ = 0;
    cache_miss_cnt_ = 0;
    cache_miss_bytes_ = 0;
  }
  void update_cache_hit(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&cache_hit_cnt_, delta_cnt);
    ATOMIC_AAF(&cache_hit_bytes_, delta_size);
  }
  void update_cache_miss(const int64_t delta_cnt, const int64_t delta_size)
  {
    ATOMIC_AAF(&cache_miss_cnt_, delta_cnt);
    ATOMIC_AAF(&cache_miss_bytes_, delta_size);
  }
  TO_STRING_KV(K_(cache_hit_cnt), K_(cache_miss_cnt), K_(cache_hit_bytes), K_(cache_miss_bytes));
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_META_H_ */
