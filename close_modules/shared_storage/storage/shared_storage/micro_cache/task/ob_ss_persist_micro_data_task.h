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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_base.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_persist_micro_op.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace storage
{
class ObSSMicroCacheTaskRunner;

class ObSSPersistMicroDataTask : public common::ObTimerTask, public ObSSMicroCacheTaskBase
{
public:
  ObSSPersistMicroDataTask(ObSSMicroCacheTaskRunner &runner);
  virtual ~ObSSPersistMicroDataTask() {}
  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);
  virtual void runTimerTask() override;
  void destroy();

  bool is_persist_data_op_closed() const { return persist_op_.is_closed(); }
  void enable_persist_data_op() { persist_op_.enable_op(); }
  void disable_persist_data_op() { persist_op_.disable_op(); }

private:
  void adjust_persist_task_interval(const int64_t alloc_phy_blk_fail_cnt);

private:
  static constexpr int64_t FAST_SCHEDULE_PERSIST_INTERVAL_US = 10; // 10us

private:
  ObSSMicroCacheTaskRunner &runner_;
  ObSSPersistMicroOp persist_op_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PERSIST_MICRO_DATA_TASK_H_ */
