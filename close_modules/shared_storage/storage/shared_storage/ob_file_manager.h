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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_MANAGER_H_

#include "lib/queue/ob_link_queue.h"
#include "common/storage/ob_io_device.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_ss_preread_cache_manager.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/task/ob_ss_tmp_file_flush_task.h"
#include "storage/shared_storage/task/ob_find_tmp_file_flush_task.h"
#include "storage/shared_storage/task/ob_persist_disk_space_task.h"
#include "storage/shared_storage/task/ob_calibrate_disk_space_task.h"
#include "storage/shared_storage/task/ob_gc_local_major_data_task.h"
#include "storage/shared_storage/task/ob_check_expand_disk_size_task.h"
#include "storage/shared_storage/ob_ss_fd_cache_mgr.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/task/ob_check_data_disk_avail_task.h"
#include "storage/shared_storage/ob_segment_file_manager.h"
#include "share/ob_io_device_helper.h"

namespace oceanbase
{
namespace share
{
  class ObBackupDest;
  class ObBackupStorageInfo;
}
namespace storage
{

struct ObSegmentFileInfo : public common::ObLink
{
public:
  ObSegmentFileInfo() : file_id_(), func_type_(0) {}
  explicit ObSegmentFileInfo(const blocksstable::MacroBlockId file_id, const uint8_t func_type, const bool is_sealed)
      : file_id_(file_id), func_type_(func_type), is_sealed_(is_sealed) {}
  ~ObSegmentFileInfo() {}
  void reset();
  bool is_valid() const { return file_id_.is_valid(); }
  TO_STRING_KV(K_(file_id), K_(func_type), K_(is_sealed));

public:
  blocksstable::MacroBlockId file_id_;
  uint8_t func_type_;
  bool is_sealed_;
};

class ObBaseFileManager
{
public:
  static const int64_t OB_MAX_FILE_PATH_LENGTH = 1024;
  static const int64_t OB_DEFAULT_BUCKET_NUM = 10001;
  static const int64_t MB = 1024L * 1024L;
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 64;
  static const int64_t MAX_RETRY_ALLOC_CACHE_SPACE_CNT = 5;
  static const int64_t PRINT_LOG_INTERVAL = 30L * 1000L * 1000L; //30s
  ObBaseFileManager();
  virtual ~ObBaseFileManager();
  bool is_inited() const { return is_inited_; }
  virtual int init(const uint64_t tenant_id);
  virtual int start();
  virtual void destroy();
  int get_storage_dest(share::ObBackupDest &storage_dest);
  int append_file(const blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &object_handle);
  virtual int async_append_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                                blocksstable::ObStorageObjectHandle &object_handle) = 0;
  int write_file(const blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &object_handle);
  virtual int async_write_file(const blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &object_handle) = 0;
  int pread_file(const blocksstable::ObStorageObjectReadInfo &read_info, blocksstable::ObStorageObjectHandle &object_handle);
  virtual int async_pread_file(const blocksstable::ObStorageObjectReadInfo &read_info, blocksstable::ObStorageObjectHandle &object_handle);
  int fsync_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0);
  int get_file_length(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, int64_t &file_length);
  int get_local_file_length(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, int64_t &file_length);
  int get_remote_file_length(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, int64_t &file_length);
  int is_exist_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist);
  int is_exist_local_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist);
  int is_exist_remote_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist);
  OB_INLINE common::ObIAllocator &get_io_callback_allocator() { return io_callback_allocator_; }
  OB_INLINE common::ObSmallObjPool<ObSSFdCacheNode> &get_fd_cache_node_pool() { return fd_cache_node_pool_; }
  int64_t get_start_server_time_s() { return start_server_time_s_; }

protected:
  static const int64_t TIME_GUARD_WARN_THRESHOLD = 10L * 1000L * 1000L; // 10s
  int delete_local_dir(const char *dir_path);
  int delete_remote_dir(const char *dir_path);
  int do_aio_write(const blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &object_handle);
  int do_aio_read(const blocksstable::ObStorageObjectReadInfo &read_info, blocksstable::ObStorageObjectHandle &object_handle);

protected:
  uint64_t tenant_id_;
  common::ObConcurrentFIFOAllocator io_callback_allocator_;
  bool is_inited_;
  bool is_started_;
  int64_t start_server_time_s_;
  common::ObSmallObjPool<ObSSFdCacheNode> fd_cache_node_pool_;
};

class ObTenantFileManager : public ObBaseFileManager
{
public:
  friend class ObSSTmpFileFlushTask;
  ObTenantFileManager();
  virtual ~ObTenantFileManager();
  static int mtl_init(ObTenantFileManager *&tenant_file_manager);
  virtual int init(const uint64_t tenant_id) override;
  virtual int start() override;
  int start_timer_task();
  virtual void destroy() override;
  void stop();
  void wait();
  int pwrite_cache_block(const int64_t offset,const int64_t size, const char *buf, int64_t &write_size);
  int pread_cache_block(const int64_t offset, const int64_t size, char *buf, int64_t &read_size);
  int fsync_cache_file();
  int read_tenant_disk_space_meta(ObTenantDiskSpaceMeta &disk_space_meta);
  // Get current disk_space_meta and persist it into disk
  int write_tenant_disk_space_meta();
  // Get current disk_space_meta and update micro/macro ratio and persist it into disk
  int adjust_cache_disk_ratio(const int64_t new_micro_cache_pct, const int64_t new_macro_cache_pct, 
                              bool &succ_adjust, int64_t &micro_cache_reserved_size);
  int resize_total_disk_space(const int64_t new_disk_size);
  int calc_disk_space(const char *dir_path, const int64_t start_calc_time_s, int64_t &total_file_size);
  int calc_private_macro_disk_space(const int64_t start_calc_time_s, int64_t &total_file_size);
  int calc_tmp_data_disk_space(const int64_t start_calc_time_s,
                               int64_t &total_read_cache_size,
                               int64_t &total_write_cache_size);
  int calc_major_macro_disk_space(const int64_t start_calc_time_s, int64_t &total_file_size);
  int calc_meta_file_disk_space(const int64_t start_calc_time_s, int64_t &total_file_size);
  virtual int async_append_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                                blocksstable::ObStorageObjectHandle &object_handle) override;
  // normal tenant async_write_file need alloc_file_size and if OB_SERVER_OUTOF_DISK_SPACE write through
  virtual int async_write_file(const blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &object_handle) override;
  virtual int async_pread_file(const blocksstable::ObStorageObjectReadInfo &read_info, blocksstable::ObStorageObjectHandle &object_handle) override;
  int push_to_flush_queue(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0, const bool is_sealed = true);
  int delete_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0);
  int delete_local_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0,
                        const bool is_print_log = true, const bool is_del_seg_meta = true,
                        const bool is_logical_delete = false);
  int delete_remote_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0);
  int delete_tmp_file(const blocksstable::MacroBlockId &file_id);
  int delete_local_tmp_file(const blocksstable::MacroBlockId &file_id, const bool is_only_delete_read_cache = false); // used for ObAdmin delete local tmp file read cache
  int delete_tmp_file(const blocksstable::MacroBlockId &file_id, const int64_t length);
  int delete_ls_dir(const int64_t ls_id, const int64_t ls_epoch_id); // delete ls dir files
  int delete_files(const ObIArray<blocksstable::MacroBlockId> &block_ids); // delete files
  int delete_local_files(const ObIArray<blocksstable::MacroBlockId> &block_ids); // delete local files
  int delete_remote_files(const ObIArray<blocksstable::MacroBlockId> &block_ids); // delete remote files
  int delete_shared_tablet_data_dir(const int64_t tablet_id); // delete shared tablet data dir files
  int delete_local_major_data_dir(const int64_t delete_time_stamp_s); // when restart observer, need gc local major_data dir's major macro file
  int list_tmp_file(ObIArray<blocksstable::MacroBlockId> &tmp_file_ids); // No guarantee of order
  int list_tablet_data_dir(ObIArray<int64_t> &tablet_ids); // No guarantee of order
  int list_shared_tablet_ids(ObIArray<int64_t> &tablet_ids); // No guarantee of order
  int list_tablet_meta_dir(const int64_t ls_id, const int64_t ls_epoch_id,
                           ObIArray<int64_t> &tablet_ids);  // Support transfer restart, requires modifications from Yongle group, too much work. Last meeting said Lingchuan's side would work around it, temporarily provided interface, to be deleted later @xiaotao.ht
  int list_private_macro_file(const int64_t tablet_id, const int64_t transfer_seq,
                              ObIArray<blocksstable::MacroBlockId> &block_ids); // No guarantee of order
  int get_micro_cache_file_path(char *path, const int64_t length,
                                const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int alloc_file_size(const blocksstable::ObStorageObjectType object_type, const int64_t size);
  int free_file_size(const blocksstable::MacroBlockId &file_id, const int64_t size);
  int calibrate_disk_space(); // used for ObAdmin calibrate local data disk space
  OB_INLINE int get_micro_cache_file_fd() { return micro_cache_file_fd_; }
  OB_INLINE bool is_micro_cache_file_exist() const { return is_cache_file_exist_; }
  OB_INLINE ObPrereadCacheManager &get_preread_cache_mgr() { return preread_cache_mgr_; }
  OB_INLINE ObSegmentFileManager &get_segment_file_mgr() { return segment_file_mgr_; }
  OB_INLINE ObSSFdCacheMgr &get_fd_cache_mgr() { return fd_cache_mgr_;}
  void set_pause_gc() { ATOMIC_STORE(&is_pause_gc_, true); }
  void set_allow_gc() { ATOMIC_STORE(&is_pause_gc_, false); }
  void set_tmp_file_cache_pause_gc() { ATOMIC_STORE(&is_tmp_file_cache_pause_gc_, true); }
  void set_tmp_file_cache_allow_gc() { ATOMIC_STORE(&is_tmp_file_cache_pause_gc_, false); }
  OB_INLINE bool is_pause_gc() const { return ATOMIC_LOAD(&is_pause_gc_); }
  OB_INLINE bool is_tmp_file_cache_pause_gc() const { return ATOMIC_LOAD(&is_tmp_file_cache_pause_gc_); }
  bool is_object_type_need_stat(const blocksstable::ObStorageObjectType object_type);
  int aio_write_with_create_parent_dir(const blocksstable::ObStorageObjectWriteInfo &write_info,
                                       const blocksstable::ObStorageObjectHandle &ori_object_handle,
                                       const bool need_free_file_size,
                                       const int64_t free_size,
                                       blocksstable::ObStorageObjectHandle &object_handle);

private:
  int list_local_private_macro_file(const int64_t tablet_id, const int64_t transfer_seq,
                                    const ObMacroType macro_type,
                                    ObIArray<blocksstable::MacroBlockId> &block_ids);
  int list_remote_private_macro_file(const int64_t tablet_id, const int64_t transfer_seq,
                                     const ObMacroType macro_type,
                                     ObIArray<blocksstable::MacroBlockId> &block_ids);
  int list_local_files(const char *dir_path, ObBaseDirEntryOperator &list_op);
  int list_local_files_rec(const char *dir_path, share::ObScanDirOp &list_file_op, share::ObScanDirOp &list_dir_op);
  int list_remote_files(const char *dir_path, ObBaseDirEntryOperator &list_op);
  // when writing PRIVATE_TABLET_META, try fsync PRIVATE_DATA_MACRO/PRIVATE_META_MACRO parent dir
  int try_fsync_private_macro_parent_dir(const blocksstable::ObStorageObjectType object_type,
                                         const blocksstable::MacroBlockId &file_id,
                                         const int64_t ls_epoch_id);
  int fsync_private_tablet_dir(const blocksstable::ObStorageObjectType object_type, const int64_t tablet_id);
  void batch_print_deleted_file_ids(const ObIArray<blocksstable::MacroBlockId> &block_ids, const bool is_local);
  int try_alloc_tmp_file_write_cache_size(const int64_t size);
  int try_delete_last_unsealed_segment(const TmpFileSegId &seg_id);
  int logical_delete_local_file(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0); // logical delete local_file is renamed file to .deleted file
  int is_delete_read_cache_tmp_file(const blocksstable::MacroBlockId &file_id, bool &is_need_delete);
  int do_build_tenant_disk_space_meta(ObTenantDiskSpaceMeta &disk_space_meta);
  int do_write_tenant_disk_space_meta(const ObTenantDiskSpaceMeta &disk_space_meta);

private:
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t BATCH_DELETE_FILE_COUNT = 1000;
  static const int64_t BATCH_PRINT_FILE_PATH_COUNT = 64;
private:
  bool is_cache_file_exist_;
  int micro_cache_file_fd_;
  common::ObLinkQueue flush_queue_; // store sealed TmpFile sub-file
  ObPrereadCacheManager preread_cache_mgr_;
  ObSegmentFileManager segment_file_mgr_;
  int tg_id_;
  ObSSTmpFileFlushTask tmp_file_flush_task_;
  ObFindTmpFileFlushTask find_tmp_file_flush_task_;
  ObPersistDiskSpaceTask persist_disk_space_task_;
  ObCalibrateDiskSpaceTask calibrate_disk_space_task_;
  ObGCLocalMajorDataTask gc_local_major_data_task_;
  ObCheckExpandDiskTask check_expand_disk_task_;
  ObSSFdCacheMgr fd_cache_mgr_;
  bool is_pause_gc_; // when calibrate disk alloc size pause gc
  bool is_tmp_file_cache_pause_gc_; // when calibrate tmp_file_cache disk space, if delete tmp_file, rename tmp_file to .deleted file
  volatile bool is_stop_;
  lib::ObMutex disk_meta_lock_; // used to control write/read tenant_disk_space_meta
};

class ObServerFileManager : public ObBaseFileManager
{
public:
  static ObServerFileManager &get_instance();
  int start(const int64_t reserved_size);
  virtual void destroy() override;
  int resize_device_size(const int64_t new_device_size, const int64_t new_device_disk_percentage,
                         const int64_t reserved_size, storage::ObServerSuperBlock &super_block);
  virtual int async_append_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                                blocksstable::ObStorageObjectHandle &object_handle) override;
  // 500 tenant async_write_file do not need alloc_file_size and only write SERVER_META/TENANT_SUPER_BLOCK/TENANT_UNIT_META
  virtual int async_write_file(const blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &object_handle) override;
  int delete_local_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id); // delete local tenant dir files
  int delete_remote_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id); // delete remote tenant dir files
  int delete_shared_tenant_dir(const uint64_t tenant_id); // delete shared tenant dir files
  int list_shared_tenant_ids(const uint64_t tenant_id, ObIArray<uint64_t> &tenant_ids); // tenant id is used to set ObMemAttr
  int check_disk_space_available();

  int create_io_calibration_file(const int64_t block_count);
  int delete_io_calibration_file();
  int get_io_calibration_fd() const { return io_calibration_fd_; }

private:
  ObServerFileManager();
  virtual ~ObServerFileManager();
  int aio_write_with_create_parent_dir(const blocksstable::ObStorageObjectWriteInfo &write_info,
                                       const blocksstable::ObStorageObjectHandle &ori_object_handle,
                                       blocksstable::ObStorageObjectHandle &object_handle);

private:
  static constexpr const char *IO_CALIBRATION_FILE = "ss_io_calibration_file";

  lib::ObMutex resize_device_size_lock_;
  int io_calibration_fd_;
  ObCheckDataDiskAvailTask check_data_disk_avail_task_;
};

#define OB_SERVER_FILE_MGR (oceanbase::storage::ObServerFileManager::get_instance())

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_MANAGER_H_ */
