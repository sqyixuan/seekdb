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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_RUNNER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_RUNNER_H_

#include "storage/shared_storage/micro_cache/task/ob_ss_persist_micro_data_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_release_cache_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_execute_micro_checkpoint_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_execute_blk_checkpoint_task.h"

namespace oceanbase
{
namespace storage
{
class ObTenantFileManager;
class ObSSPhysicalBlockManager;
class ObSSMemDataManager;
class ObSSMicroMetaManager;
class ObSSLAMicroKeyManager;

class ObSSMicroCacheTaskRunner
{
public:
  ObSSMicroCacheTaskRunner();
  virtual ~ObSSMicroCacheTaskRunner() {}

  int init(const uint64_t tenant_id, common::ObIAllocator &allocator, ObSSMemDataManager &mem_data_mgr, 
           ObSSMicroMetaManager &micro_meta_mgr, ObSSPhysicalBlockManager &phy_blk_mgr, 
           ObTenantFileManager &tnt_file_mgr, ObSSMicroCacheStat &cache_stat, ObSSLAMicroKeyManager &la_micro_mgr);
  int start();
  void stop();
  void wait();
  void destroy();

  int schedule_persist_data_task(const int64_t interval_us);
  int schedule_arc_cache_task(const int64_t interval_us);
  int schedule_do_micro_checkpoint_task(const int64_t interval_us);
  int schedule_do_blk_checkpoint_task(const int64_t interval_us);

  void enable_task() { enable_micro_ckpt(); enable_blk_ckpt(); enable_persist_data(); enable_release_cache(); }
  void disable_task() { disable_micro_ckpt(); disable_blk_ckpt(); disable_persist_data(); disable_release_cache(); }
  bool is_task_closed() const 
  { 
    return is_micro_ckpt_closed() && is_blk_ckpt_closed() && is_persist_data_closed() && is_release_cache_closed(); 
  }
  void stop_extra_task_for_ckpt() { disable_blk_ckpt(); disable_persist_data(); disable_release_cache(); }
  void resume_extra_task_for_ckpt() { enable_blk_ckpt(); enable_persist_data(); enable_release_cache(); }
  void enable_replay_ckpt() { micro_ckpt_task_.enable_replay_ckpt(); }

private:
  int init_task(const uint64_t tenant_id);
  int init_timer_thread();
  int start_timer_thread();

  OB_INLINE void enable_micro_ckpt() { micro_ckpt_task_.enable_micro_ckpt_op(); }
  OB_INLINE void disable_micro_ckpt() { micro_ckpt_task_.disable_micro_ckpt_op(); }
  OB_INLINE bool is_micro_ckpt_closed() const { return micro_ckpt_task_.is_micro_ckpt_op_closed(); }
  OB_INLINE void enable_blk_ckpt() { blk_ckpt_task_.enable_blk_ckpt_op(); }
  OB_INLINE void disable_blk_ckpt() { blk_ckpt_task_.disable_blk_ckpt_op(); }
  OB_INLINE bool is_blk_ckpt_closed() const { return blk_ckpt_task_.is_blk_ckpt_op_closed(); }
  OB_INLINE void enable_persist_data() { persist_task_.enable_persist_data_op(); }
  OB_INLINE void disable_persist_data() { persist_task_.disable_persist_data_op(); }
  OB_INLINE bool is_persist_data_closed() const { return persist_task_.is_persist_data_op_closed(); }
  OB_INLINE void enable_release_cache() 
  { 
    release_cache_task_.enable_evict_op(); 
    release_cache_task_.enable_reorganize_op(); 
  }
  OB_INLINE void disable_release_cache() 
  { 
    release_cache_task_.disable_evict_op(); 
    release_cache_task_.disable_reorganize_op(); 
  }
  OB_INLINE bool is_release_cache_closed() const 
  { 
    return release_cache_task_.is_evict_op_closed() && release_cache_task_.is_reorganize_op_closed(); 
  }

private:
  const static int64_t DEFAULT_PERSIST_DATA_INTERVAL_US = 2 * 1000; // 2ms
  const static int64_t DEFAULT_ARC_CACHE_INTERVAL_US = 10 * 1000; // 10ms
  const static int64_t DEFAULT_DO_MICRO_CKPT_INTERVAL_US = 200 * 1000; // 200ms
  const static int64_t DEFAULT_DO_BLK_CKPT_INTERVAL_US = 10 * 1000; // 10ms
  const static int64_t INVALID_TG_ID = -1;

private:
  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  int high_prio_tg_id_; // high priority timer thread, execute persist_task
  int mid_prio_tg_id_;  // mid priority timer thread, execute blk_ckpt_task
  int low_prio_tg_id_;  // low priority timer thread, execute release_cache_task, micro_ckpt_task
  ObSSMicroCacheTaskCtx task_ctx_;
  ObSSPersistMicroDataTask persist_task_;
  ObSSReleaseCacheTask release_cache_task_;
  ObSSExecuteMicroCheckpointTask micro_ckpt_task_;
  ObSSExecuteBlkCheckpointTask blk_ckpt_task_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_RUNNER_H_ */
