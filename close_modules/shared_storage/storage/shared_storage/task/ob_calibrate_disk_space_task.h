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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CALIBRATE_DISK_SPACE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CALIBRATE_DISK_SPACE_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_file_op.h"

namespace oceanbase
{
namespace storage
{

/*
 * ObCalibrateDiskSpaceTask is used to calibrate disk space intervally(4h),
 * because of write failure or observer restart, the tenant disk space usage statistics is inaccurate
 */
class ObCalibrateDiskSpaceTask : public common::ObTimerTask
{
public:
  ObCalibrateDiskSpaceTask();
  virtual ~ObCalibrateDiskSpaceTask() { destroy(); }

  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();

  virtual void runTimerTask() override;
  int schedule_calibrate_disk_space();

private:
  int calibrate_disk_space();
  int calibrate_alloc_disk_size(const blocksstable::ObStorageObjectType object_type,
                                const int64_t start_calc_size_time_s,
                                const int64_t initial_alloc_size,
                                const int64_t initial_tmp_file_read_cache_alloc_size = 0);
  int choose_start_calc_size_time_s(int64_t &start_calc_size_time_s);
  int delete_tmp_seq_files(ObDirCalcSizeOp &del_tmp_seq_file_op); // delete tenant_id dir's .tmp.seq files, for example TENANT_DISK_SPACE_META,TENANT_SUPER_BLOCK,TENANT_UNIT_META
  int rm_logical_deleted_file(); // delete tmp_data dir's .deleted files

private:
  bool is_inited_;
  uint64_t tenant_id_;
  volatile bool is_stop_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CALIBRATE_DISK_SPACE_TASK_H_ */
