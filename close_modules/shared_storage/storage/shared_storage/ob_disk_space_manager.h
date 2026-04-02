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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_MANAGER_H_

#include "deps/oblib/src/common/storage/ob_io_device.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "share/config/ob_server_config.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "storage/shared_storage/ob_ss_common_header.h"
#include "storage/shared_storage/common/ob_ss_common_info.h"
#include "storage/shared_storage/ob_disk_space_meta.h"
#include "share/ob_lease_struct.h"
#include "storage/high_availability/ob_storage_ha_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDiskSpaceManager final
{
public:
  static const int64_t DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH = 1024L * 1024L * 1024L; // tenant data_disk_size expand step length is 1GB
  int init(const int64_t total_disk_size);
  void destroy();
  static ObDiskSpaceManager &get_instance();
  int resize(const int64_t new_total_disk_size);
  int alloc(const int64_t alloc_size);
  int free(const int64_t free_size);
  int auto_expand_data_disk_size(const uint64_t tenant_id, const int64_t expand_size);
  int reload_hidden_sys_data_disk_config(const ObServerConfig &server_config);
  int reload_ss_cache_max_percentage_config(const ObServerConfig &server_config);
  int reload_ss_cache_maxsize_percpu_config(const ObServerConfig &server_config);
  int update_hidden_sys_data_disk_config_size(const int64_t new_hidden_sys_data_disk_config_size);
  int update_hidden_sys_data_disk_size(const int64_t new_hidden_sys_data_disk_size);
  OB_INLINE int64_t get_total_disk_size() const { return total_disk_size_; }
  OB_INLINE int64_t get_reserved_disk_size() const { return reserved_disk_size_; }
  OB_INLINE int64_t get_disk_size_capacity() const { return total_disk_size_ - reserved_disk_size_ - hidden_sys_data_disk_size_; }  // __all_virtual_server's DATA_DISK_CAPACITY
  OB_INLINE int64_t get_alloc_disk_size() const { return alloc_disk_size_ - hidden_sys_data_disk_size_; } // __all_virtual_server's DATA_DISK_ASSIGNED
  OB_INLINE int64_t get_free_disk_size() const
  {
    return total_disk_size_ - alloc_disk_size_ - reserved_disk_size_;
  }
  OB_INLINE int64_t get_hidden_sys_data_disk_config_size() const { return hidden_sys_data_disk_config_size_; }
  OB_INLINE int64_t get_hidden_sys_data_disk_size() const { return hidden_sys_data_disk_size_; }
  int gen_hidden_sys_data_disk_size(int64_t &hidden_sys_data_disk_size);
  static int get_used_disk_size(int64_t &used_disk_size);  // __all_virtual_server's DATA_DISK_IN_USE
  OB_INLINE int64_t get_ss_cache_max_size() const { return ss_cache_maxsize_percpu_ * common::get_cpu_count(); };
  OB_INLINE int64_t get_data_disk_suggested_size() const { return data_disk_suggested_size_; }
  OB_INLINE share::DataDiskSuggestedOperationType::TYPE get_data_disk_suggested_operation() const { return data_disk_suggested_operation_; }
  int get_tenant_object_storage_used_size(const uint64_t tenant_id, int64_t &total_size);
  int check_expand_tenant_data_disk_size(const uint64_t tenant_id, bool &is_need_expand);
  int check_ls_migration_space_full(const ObMigrationOpArg &arg, const int64_t required_private_macro_size);
  int check_if_tenant_support_auto_expand_disk(const uint64_t tenant_id, bool &is_auto_expand);

private:
  enum DiskSizeOpType : uint8_t
  {
    ALLOC = 0,
    FREE = 1,
    RESIZE = 2,
  };
  void modify_datadisk_suggested_opertion_type(const DiskSizeOpType type,
                                               const int64_t data_disk_suggested_size,
                                               const int64_t old_total_disk_size,
                                               const int64_t old_alloc_disk_size);
  int get_data_disk_capacity(int64_t &disk_capacity); // disk_capacity = total_disk_size - cpu*2GB
  int get_free_disk_size_for_auto_expand(int64_t &free_disk_size); // free_disk_size = disk_capacity - alloc_disk_size
  int is_ss_cache_need_expand(bool &is_need_expand);

private:
  static const int64_t DEFAULT_SERVER_TENANT_ID_DISK_SIZE = 0; // reserved disk size for server tenant id, currently 0
  static const int64_t MEMORY_TO_DATA_DISK_FACTOR = 2;
  static const int64_t DEFAULT_SS_CACHE_MAX_PERCENTAGE = 30;  // 30%
  static const int64_t DEFAULT_SS_CACHE_MAXSIZE_PERCPU = 128L * 1024L * 1024L * 1024L;  // 128GB
  static constexpr const double DEFAULT_DISK_SIZE_EXPAND_THRESHOLD_PERCENT = 0.9;
  static const int64_t DEFAULT_DISK_SIZE_RESERVED_PERCPU = 2L * 1024L * 1024L * 1024L;  // 2GB
  ObDiskSpaceManager();
  ~ObDiskSpaceManager();

private:
  bool is_inited_;
  common::ObRecursiveMutex disk_size_lock_;
  int64_t total_disk_size_;
  int64_t reserved_disk_size_;
  int64_t alloc_disk_size_;
  int64_t hidden_sys_data_disk_config_size_;  // hidden sys's initial default value
  int64_t hidden_sys_data_disk_size_;  // hidden sys's actual value
  int64_t ss_cache_max_percentage_;  // default value is 30
  int64_t ss_cache_maxsize_percpu_;  // default value is 128GB percpu
  int64_t data_disk_suggested_size_;  // 0 means cloud platform auto expands data disk; none-zero means cloud platform expands more than this value.
  share::DataDiskSuggestedOperationType::TYPE data_disk_suggested_operation_; // none, expand, shrink
};

class ObTenantDiskSpaceManager final
{
public:
  static const int64_t PRINT_LOG_INTERVAL = 60L * 1000L * 1000L; // 1min
  ObTenantDiskSpaceManager();
  ~ObTenantDiskSpaceManager();
  int init(const uint64_t tenant_id, const int64_t data_disk_size);
  int start();
  void stop();
  void wait();
  void destroy();
  static int mtl_init(ObTenantDiskSpaceManager *&tenant_disk_space_manager);
  int alloc_file_size(const int64_t alloc_size, const blocksstable::ObStorageObjectType object_type, 
                      const bool is_tmp_file_read_cache = false);
  int free_file_size(const int64_t free_size, const blocksstable::ObStorageObjectType object_type, 
                     const bool is_tmp_file_read_cache = false);
  int falloc_micro_cache_file(const int64_t offset, const int64_t size);
  int resize_total_disk_size(const int64_t new_data_disk_size);
  int calibrate_alloc_size(const int64_t alloc_disk_size, const blocksstable::ObStorageObjectType object_type,
                           const bool is_tmp_file_read_cache = false);
  int check_micro_cache_file_size(const int64_t micro_cache_file_size);
  int update_cache_disk_ratio(const ObTenantDiskCacheRatioInfo &new_disk_cache_ratio);
  int update_cache_disk_ratio(const int64_t new_micro_size_pct, const int64_t new_macro_size_pct, bool &succ_adjust,
                              int64_t &ori_micro_cache_reserved_size);
  // To get mini and minor macro's free disk size
  int64_t get_private_macro_free_disk_size() const
  {
    const int64_t reserved_size = get_private_macro_reserved_size();
    return MAX(0, reserved_size - get_dir_reserved_disk_size(reserved_size) - get_private_macro_alloc_size());
  }
  OB_INLINE static bool is_private_macro_objtype(const blocksstable::ObStorageObjectType object_type)
  {
    return (blocksstable::ObStorageObjectType::PRIVATE_DATA_MACRO == object_type) ||
           (blocksstable::ObStorageObjectType::PRIVATE_META_MACRO == object_type);
  }
  OB_INLINE static bool is_major_macro_objtype(const blocksstable::ObStorageObjectType object_type)
  {
    return (blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == object_type) ||
           (blocksstable::ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type);
  }
  OB_INLINE static bool is_private_tablet_meta_objtype(const blocksstable::ObStorageObjectType object_type)
  {
    return (blocksstable::ObStorageObjectType::PRIVATE_TABLET_META == object_type) ||
           (blocksstable::ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION == object_type);
  }
  void update_local_cache_hit_stat(const blocksstable::ObStorageObjectType object_type, const int64_t delta_cnt, const int64_t delta_size);
  void update_local_cache_miss_stat(const blocksstable::ObStorageObjectType object_type, const int64_t delta_cnt, const int64_t delta_size);
  OB_INLINE ObLocalCacheHitStat get_tmp_file_cache_stat() const { return tmp_file_cache_stat_; }
  OB_INLINE ObLocalCacheHitStat get_major_macro_cache_stat() const { return major_macro_cache_stat_; }
  OB_INLINE ObLocalCacheHitStat get_private_macro_cache_stat() const { return private_macro_cache_stat_; }
  // NOTICE: there is no need to add lock when execute below functions. FROM: @xiaotao
  OB_INLINE int64_t get_total_disk_size() const { return total_disk_size_; }
  OB_INLINE int64_t get_micro_cache_reserved_size() const { return get_micro_cache_info().get_reserved_size(); }
  OB_INLINE int64_t get_tmp_file_write_cache_reserved_size() const { return get_tmp_file_write_cache_info().get_reserved_size(); }
  OB_INLINE int64_t get_preread_cache_reserved_size() const { return get_preread_cache_info().get_reserved_size(); }
  OB_INLINE int64_t get_meta_file_reserved_size() const { return get_meta_file_cache_info().get_reserved_size(); }
  OB_INLINE int64_t get_private_macro_reserved_size() const { return get_private_macro_cache_info().get_reserved_size(); }
  OB_INLINE int64_t get_private_macro_alloc_size() const { return get_alloc_info().private_macro_alloc_size_; }
  OB_INLINE int64_t get_major_macro_read_cache_alloc_size() const { return get_alloc_info().major_macro_read_cache_alloc_size_; }
  OB_INLINE int64_t get_meta_file_alloc_size() const { return get_alloc_info().meta_file_alloc_size_; }
  OB_INLINE int64_t get_tmp_file_write_cache_alloc_size() const { return get_alloc_info().tmp_file_write_cache_alloc_size_; }
  OB_INLINE int64_t get_tmp_file_read_cache_alloc_size() const { return get_alloc_info().tmp_file_read_cache_alloc_size_; }
  const ObTenantAllDiskCacheInfo &get_all_disk_cache_info() const { return all_disk_cache_info_; }
  bool need_expand_disk_size() const;
  int get_used_disk_size(int64_t &used_disk_size);
  OB_INLINE bool is_unsealed_tmp_file_flush_reach_low_water_mark(const int64_t total_flush_size) const
  {
    return (get_tmp_file_write_cache_alloc_size() - total_flush_size) < (total_disk_size_ * UNSEALED_TMP_FILE_FLUSH_LOW_WATER_MARK_PCT);
  }
  OB_INLINE bool is_tenant_disk_size_need_expand(const int64_t alloc_size, const int64_t total_size) const 
  { 
    return alloc_size >= total_size * DEFAULT_TENANT_DISK_SIZE_EXPAND_THRESHOLD_PCT; 
  }
  OB_INLINE bool is_unsealed_tmp_file_need_flush() const 
  { 
    return get_tmp_file_write_cache_alloc_size() > get_tmp_file_write_cache_reserved_size(); 
  }
  // In order to ensure that the local cache data_disk_size can hold private_macro, when the local cache disk size of private_macro is exhausted, need expand data_disk_size
  OB_INLINE bool is_need_expand_for_private_macro() const
  {
    return is_tenant_disk_size_need_expand(get_private_macro_alloc_size(), get_private_macro_reserved_size()) &&
           (get_private_macro_reserved_size() < MAX_RATIO_EXPANSION_DISK_TO_MEMORY * MTL_MEM_SIZE());
  }
  // In order to ensure that the local cache data_disk_size can hold meta_file, when the local cache disk size of meta_file is exhausted, need expand data_disk_size
  OB_INLINE bool is_need_expand_for_meta_file() const
  {
    return is_tenant_disk_size_need_expand(get_meta_file_alloc_size(), get_meta_file_reserved_size()) &&
           (get_meta_file_reserved_size() < MAX_RATIO_EXPANSION_DISK_TO_MEMORY * MTL_MEM_SIZE());
  }
  OB_INLINE bool is_meta_tenant_need_expand() const
  {
    return total_disk_size_ < MAX_RATIO_EXPANSION_DISK_TO_MEMORY * MTL_MEM_SIZE();
  }

private:
  OB_INLINE const ObTenantDiskCacheAllocInfo &get_alloc_info() const { return all_disk_cache_info_.alloc_info_; }
  OB_INLINE ObTenantDiskCacheAllocInfo &alloc_info() { return all_disk_cache_info_.alloc_info_; }
  OB_INLINE const ObTenantDiskCacheInfo &get_meta_file_cache_info() const { return all_disk_cache_info_.meta_file_cache_; }
  OB_INLINE ObTenantDiskCacheInfo &meta_file_cache_info() { return all_disk_cache_info_.meta_file_cache_; }
  OB_INLINE const ObTenantDiskCacheInfo &get_micro_cache_info() const { return all_disk_cache_info_.micro_cache_; }
  OB_INLINE ObTenantDiskCacheInfo &micro_cache_info() { return all_disk_cache_info_.micro_cache_; }
  OB_INLINE const ObTenantDiskCacheInfo &get_tmp_file_write_cache_info() const { return all_disk_cache_info_.tmp_file_write_cache_; }
  OB_INLINE ObTenantDiskCacheInfo &tmp_file_write_cache_info() { return all_disk_cache_info_.tmp_file_write_cache_; }
  OB_INLINE const ObTenantDiskCacheInfo &get_preread_cache_info() const { return all_disk_cache_info_.preread_cache_; }
  OB_INLINE ObTenantDiskCacheInfo &preread_cache_info() { return all_disk_cache_info_.preread_cache_; }
  OB_INLINE const ObTenantDiskCacheInfo &get_private_macro_cache_info() const { return all_disk_cache_info_.private_macro_cache_; }
  OB_INLINE ObTenantDiskCacheInfo &private_macro_cache_info() { return all_disk_cache_info_.private_macro_cache_; }
  
  OB_INLINE void set_meta_file_alloc_size(const int64_t alloc_size) { alloc_info().meta_file_alloc_size_ = alloc_size; }
  OB_INLINE void add_meta_file_alloc_size(const int64_t delta_size) { alloc_info().meta_file_alloc_size_ += delta_size; }
  OB_INLINE void set_tmp_file_write_cache_alloc_size(const int64_t alloc_size) { alloc_info().tmp_file_write_cache_alloc_size_ = alloc_size; }
  OB_INLINE void add_tmp_file_write_cache_alloc_size(const int64_t delta_size) { alloc_info().tmp_file_write_cache_alloc_size_ += delta_size; }
  OB_INLINE void set_tmp_file_read_cache_alloc_size(const int64_t alloc_size) { alloc_info().tmp_file_read_cache_alloc_size_ = alloc_size; }
  OB_INLINE void add_tmp_file_read_cache_alloc_size(const int64_t delta_size) { alloc_info().tmp_file_read_cache_alloc_size_ += delta_size; }
  OB_INLINE void set_major_macro_read_cache_alloc_size(const int64_t alloc_size) { alloc_info().major_macro_read_cache_alloc_size_ = alloc_size; }
  OB_INLINE void add_major_macro_read_cache_alloc_size(const int64_t delta_size) { alloc_info().major_macro_read_cache_alloc_size_ += delta_size; }
  OB_INLINE void set_private_macro_alloc_size(const int64_t alloc_size) { alloc_info().private_macro_alloc_size_ = alloc_size; }
  OB_INLINE void add_private_macro_alloc_size(const int64_t delta_size) { alloc_info().private_macro_alloc_size_ += delta_size; }
  
  OB_INLINE void set_meta_file_reserved_size(const int64_t reserved_size) { meta_file_cache_info().set_reserved_size(reserved_size); }
  OB_INLINE void set_tmp_file_write_cache_reserved_size(const int64_t reserved_size) { tmp_file_write_cache_info().set_reserved_size(reserved_size); }
  OB_INLINE void set_preread_cache_reserved_size(const int64_t reserved_size) { preread_cache_info().set_reserved_size(reserved_size); }
  OB_INLINE void set_micro_cache_reserved_size(const int64_t reserved_size) { micro_cache_info().set_reserved_size(reserved_size); }
  OB_INLINE void set_private_macro_reserved_size(const int64_t reserved_size) { private_macro_cache_info().set_reserved_size(reserved_size); }

  OB_INLINE void set_meta_file_space_percent(const int64_t space_pct) { meta_file_cache_info().set_space_percent(space_pct); }
  OB_INLINE void set_tmp_file_write_cache_space_percent(const int64_t space_pct) { tmp_file_write_cache_info().set_space_percent(space_pct); }
  OB_INLINE void set_preread_cache_space_percent(const int64_t space_pct) { preread_cache_info().set_space_percent(space_pct); }
  OB_INLINE void set_micro_cache_space_percent(const int64_t space_pct) { micro_cache_info().set_space_percent(space_pct); }
  OB_INLINE void set_private_macro_space_percent(const int64_t space_pct) { private_macro_cache_info().set_space_percent(space_pct); }

  OB_INLINE int64_t get_dir_reserved_disk_size(const int64_t disk_size) const
  {
    return MIN(disk_size * DEFAULT_DIR_RESERVED_SIZE_PCT, MAX_RESERVED_DISK_SIZE); 
  }
  // To get meta file's free disk size
  int64_t get_meta_file_free_disk_size() const
  {
    const int64_t reserved_size = get_meta_file_reserved_size();
    return MAX(0, reserved_size - get_dir_reserved_disk_size(reserved_size) - get_meta_file_alloc_size());
  }
  // To get tmp file's write cache free disk size
  int64_t get_tmp_file_write_free_disk_size() const
  {
    const int64_t tmpfile_reserved_size = get_tmp_file_write_cache_reserved_size();
    const int64_t preread_reserved_size = get_preread_cache_reserved_size();
    return MAX(0, tmpfile_reserved_size - get_dir_reserved_disk_size(tmpfile_reserved_size) + preread_reserved_size - 
                  get_tmp_file_write_cache_alloc_size() - get_tmp_file_read_cache_alloc_size() - get_major_macro_read_cache_alloc_size());
  }
  // To get preread cache free disk size
  // tmp_file_write_cache disk size can occupy preread_cache disk size
  // major_macro_read and tmp_file_read shared use of preread_cache_reserved_size
  int64_t get_preread_free_disk_size() const
  {
    const int64_t tmpfile_reserved_size = get_tmp_file_write_cache_reserved_size();
    const int64_t preread_reserved_size = get_preread_cache_reserved_size();
    const int64_t tmpfile_w_alloc_size = get_tmp_file_write_cache_alloc_size();
    const int64_t tmpfile_r_alloc_size = get_tmp_file_read_cache_alloc_size();
    const int64_t major_macro_alloc_size = get_major_macro_read_cache_alloc_size();
    return MAX(0, preread_reserved_size - tmpfile_r_alloc_size - major_macro_alloc_size - MAX(tmpfile_w_alloc_size - tmpfile_reserved_size, 0));
  }

  int alloc_file_size(const int64_t alloc_size, const int64_t avail_size, int64_t &used_size);
  int free_file_size(const int64_t free_size, const blocksstable::ObStorageObjectType object_type, 
                     const bool is_tmp_file_read_cache, int64_t &used_size);
  int do_alloc_micro_cache_file(const int64_t offset, const int64_t size);

private:
/**
 * Tenant_Disk_Space
 * |--------------|-----------------|-------------------|-------------------|-------------------|
 * | meta_file 7% | micro_cache 40% | tmp_file_write 5% | preread_cache 10% | private_macro 38% |
 * |--------------|-----------------|-------------------|-------------------|-------------------|
 */
  // unsealed tmp file write cache low water mark percent
  static constexpr const double UNSEALED_TMP_FILE_FLUSH_LOW_WATER_MARK_PCT = 0.04;
  // reserved 1% for each part disk_size, for example meta_file and private_macro, because as more files are written, 
  // the dir size increases, especially tablet_data dir and tablet_meta dir, prevent current tenant's disk_size from 
  // occupying other tenants' disk_size
  static constexpr const double DEFAULT_DIR_RESERVED_SIZE_PCT = 0.01;
  // max reserved size is 512MB for each part disk_size
  static const int64_t MAX_RESERVED_DISK_SIZE = 512L * 1024L * 1024L;
  static constexpr const double META_FILE_ALLOC_SIZE_WARNING_PCT = 0.95;
  static constexpr const double DEFAULT_TENANT_DISK_SIZE_EXPAND_THRESHOLD_PCT = 0.9;
  static const int64_t MAX_RATIO_EXPANSION_DISK_TO_MEMORY = 10; // inremental data max disk size is 10 times of memory size

private:
  bool is_inited_;
  uint64_t tenant_id_;
  lib::ObMutex disk_size_lock_;
  int64_t total_disk_size_;
  ObTenantAllDiskCacheInfo all_disk_cache_info_;
  ObLocalCacheHitStat tmp_file_cache_stat_;
  ObLocalCacheHitStat major_macro_cache_stat_;
  ObLocalCacheHitStat private_macro_cache_stat_;
};

} // namespace storage
} // namespace oceanbase

#define OB_SERVER_DISK_SPACE_MGR (oceanbase::storage::ObDiskSpaceManager::get_instance())

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_MANAGER_H_ */
