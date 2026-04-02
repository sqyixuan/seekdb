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
#ifndef OCEANBASE_STORAGE_SHARE_STORAGE_OB_RELEASE_CACHE_TASK_H_
#define OCEANBASE_STORAGE_SHARE_STORAGE_OB_RELEASE_CACHE_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_base.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_evict_micro_op.h"
#include "storage/shared_storage/micro_cache/task/op/ob_ss_reorganize_micro_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_basic_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_arc_info.h"

namespace oceanbase
{
namespace storage
{
class ObSSMicroCacheTaskRunner;

struct SSUpdateArcCtx
{
public:
  bool start_update_;
  int64_t exe_round_;

  SSUpdateArcCtx() : start_update_(false), exe_round_(0) {}

  void reset() { start_update_ = false; exe_round_ = 0; }
  void reset_round() { exe_round_ = 0; }
  bool is_started() const { return start_update_; }
  bool try_to_execute() { ++exe_round_; return exe_round_ > DEFAULT_EXE_ROUND_CNT; }
  void set_started()
  {
    start_update_ = true;
  }
  void set_finished()
  {
    start_update_ = false;
  }

  TO_STRING_KV(K_(start_update), K_(exe_round));

public:
  const int64_t DEFAULT_EXE_ROUND_CNT = 100;
};

class ObSSReleaseCacheTask : public common::ObTimerTask, public ObSSMicroCacheTaskBase
{
public:
  ObSSReleaseCacheTask(ObSSMicroCacheTaskRunner &runner);
  virtual ~ObSSReleaseCacheTask() { destroy(); }

  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);
  virtual void runTimerTask() override;
  void destroy();

  bool is_evict_op_closed() const { return evict_op_.is_closed(); }
  void enable_evict_op() { evict_op_.enable_op(); }
  void disable_evict_op() { evict_op_.disable_op(); }
  bool is_reorganize_op_closed() const { return reorganize_op_.is_closed(); }
  void enable_reorganize_op() { reorganize_op_.enable_op(); }
  void disable_reorganize_op() { reorganize_op_.disable_op(); }

private:
  void clear_for_next_round();
  void update_arc_limit_for_prewarm(const bool need_trigger = false);
  void adjust_arc_task_interval();

private:
  static constexpr int64_t FAST_SCHEDULE_ARC_INTERVAL_US = 1000; // 1ms

private:
  ObSSMicroCacheTaskRunner &runner_;
  SSUpdateArcCtx update_arc_ctx_;
  ObSSEvictMicroOp evict_op_;
  ObSSReorganizeMicroOp reorganize_op_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARE_STORAGE_OB_RELEASE_CACHE_TASK_H_ */
