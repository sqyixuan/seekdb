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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_NEED_EXPAND_DISK_SIZE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_NEED_EXPAND_DISK_SIZE_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{
/*
 * ObCheckExpandDiskTask is to check whether the tenant needs to expand data disk size
 */
class ObCheckExpandDiskTask : public common::ObTimerTask
{
public:
  ObCheckExpandDiskTask();
  virtual ~ObCheckExpandDiskTask() { destroy(); }
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int64_t INVALID_TG_ID = -1;
  int check_expand_disk_size();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int tg_id_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_NEED_EXPAND_DISK_SIZE_TASK_H_ */
