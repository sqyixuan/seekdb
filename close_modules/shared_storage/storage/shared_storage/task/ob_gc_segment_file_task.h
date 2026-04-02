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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_SEGMENT_FILE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_SEGMENT_FILE_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

class ObSegmentFileManager;

/*
 * ObGCSegmentFileTask is used for async deleting unsealed segment file
 */
class ObGCSegmentFileTask : public common::ObTimerTask
{
public:
  ObGCSegmentFileTask();
  virtual ~ObGCSegmentFileTask() { destroy(); }

  int init(ObSegmentFileManager *segment_file_mgr);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask() override;

private:
  bool is_inited_;
  ObSegmentFileManager *segment_file_mgr_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_SEGMENT_FILE_TASK_H_ */
