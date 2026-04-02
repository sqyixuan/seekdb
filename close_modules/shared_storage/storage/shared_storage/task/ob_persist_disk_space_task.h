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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_DISK_SPACE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_DISK_SPACE_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

/*
 * ObPersistDiskSpaceTask is used to intervally(1s) persist the tenant disk space usage by writing tenant_disk_space_meta file,
 * so that the tenant disk space usage can be replay when oberver restart.
 */
class ObPersistDiskSpaceTask : public common::ObTimerTask
{
public:
  ObPersistDiskSpaceTask();
  virtual ~ObPersistDiskSpaceTask() { destroy(); }

  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();

  virtual void runTimerTask() override;

private:
  static const int64_t MB = 1024L * 1024L;
  static const int64_t PRINT_LOG_INTERVAL = 60L * 1000L * 1000L; // 1min
  int persist_disk_space_meta();

private:
  bool is_inited_;
  uint64_t tenant_id_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_DISK_SPACE_TASK_H_ */
