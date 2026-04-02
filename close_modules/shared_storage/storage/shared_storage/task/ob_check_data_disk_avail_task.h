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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_DATA_DISK_AVAIL_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_DATA_DISK_AVAIL_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

/*
 * Unlike shared-nothing, which occupies disk space with large blockfile,
 * shared-storage occupies disk space with small files of 2MB,
 * so need check data disk available periodically
 */
class ObCheckDataDiskAvailTask : public common::ObTimerTask
{
public:
  ObCheckDataDiskAvailTask();
  virtual ~ObCheckDataDiskAvailTask() { destroy(); }
  int init(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int check_data_disk_avail();

private:
  bool is_inited_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_DATA_DISK_AVAIL_TASK_H_ */
