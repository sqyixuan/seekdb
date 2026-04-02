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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_MANAGER_H_

#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_shared_storage_tmp_file.h"
#include "storage/tmp_file/ob_i_tmp_file_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_flush_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_remove_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_shrink_manager.h"

namespace oceanbase
{
namespace tmp_file
{

class ObSSTenantTmpFileManager : public ObITenantTmpFileManager
{
public:
  ObSSTenantTmpFileManager();
  ~ObSSTenantTmpFileManager();

  virtual int alloc_dir(int64_t &dir_id) override;
  virtual int open(int64_t &fd, const int64_t &dir_id, const char* const label) override;
  int get_tmp_file(const int64_t fd, ObSSTmpFileHandle &handle) const;
  virtual int get_tmp_file_disk_usage(int64_t &disk_data_size, int64_t &occupied_disk_size) override;

  // TODO: wanyue.wy
  // remove this function when ObSSTmpFileAsyncFlushWaitTaskHandle has been removed
  OB_INLINE ObIAllocator * get_wait_task_allocator() { return &flush_mgr_.get_wait_task_allocator(); }

private:
  virtual int init_sub_module_();
  virtual int start_sub_module_();
  virtual int stop_sub_module_();
  virtual int wait_sub_module_();
  virtual int destroy_sub_module_();

private:
  class ObTmpFileDiskUsageCalculator final
  {
  public:
    ObTmpFileDiskUsageCalculator() : disk_data_size_(0), occupied_disk_size_(0) {}
    bool operator()(const ObTmpFileKey &key,
                    ObITmpFileHandle &tmp_file_handle);
    OB_INLINE int64_t get_disk_data_size() const { return disk_data_size_; }
    OB_INLINE int64_t get_occupied_disk_size() const { return occupied_disk_size_; }

  private:
    int64_t disk_data_size_;
    int64_t occupied_disk_size_;
  };

private:
  ObTmpWriteBufferPool wbp_;
  ObSSTmpFileFlushManager flush_mgr_;
  ObSSTmpFileRemoveManager remove_mgr_;
  ObSSTmpFileShrinkManager shrink_mgr_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_MANAGER_H_
