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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_FIND_TMP_FILE_FLUSH_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_FIND_TMP_FILE_FLUSH_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{
struct TmpFileSegId;
/*
 * ObFindTmpFileFlushTask is used for find unsealed segment file to flush object storage
 */
class ObFindTmpFileFlushTask : public common::ObTimerTask
{
public:
  ObFindTmpFileFlushTask();
  virtual ~ObFindTmpFileFlushTask() { destroy(); }

  int init();
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  bool is_inited_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_FIND_TMP_FILE_FLUSH_TASK_H_ */
