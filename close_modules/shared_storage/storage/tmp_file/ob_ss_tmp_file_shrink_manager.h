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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_SHRINK_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_SHRINK_MANAGER_H_

#include "lib/task/ob_timer.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_ss_tmp_file_flush_manager.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpFileShrinkWBPTask : public common::ObTimerTask
{
public:
  ObTmpFileShrinkWBPTask() : is_inited_(false),
                             wbp_(nullptr),
                             flush_mgr_(nullptr) {}
  virtual ~ObTmpFileShrinkWBPTask() {}
  int init(ObTmpWriteBufferPool *wbp, ObSSTmpFileFlushManager *flush_mgr);
  virtual void runTimerTask() override;
private:
  bool is_inited_;
  ObTmpWriteBufferPool *wbp_;
  ObSSTmpFileFlushManager *flush_mgr_;
};

class ObSSTmpFileShrinkManager
{
private:
public:
  ObSSTmpFileShrinkManager() : is_inited_(false),
                               tg_id_(-1),
                               shrink_wbp_task_() {}
  virtual ~ObSSTmpFileShrinkManager() {}
  int init(ObTmpWriteBufferPool *wbp, ObSSTmpFileFlushManager *flush_mgr_);
  int start();
  void stop();
  int wait();
  void destroy();
  TO_STRING_KV(K(is_inited_));

private:
  bool is_inited_;
  int tg_id_;
  ObTmpFileShrinkWBPTask shrink_wbp_task_;
  ObTmpWriteBufferPool *wbp_;
  ObSSTmpFileFlushManager *flush_mgr_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_SHRINK_MANAGER_H_
