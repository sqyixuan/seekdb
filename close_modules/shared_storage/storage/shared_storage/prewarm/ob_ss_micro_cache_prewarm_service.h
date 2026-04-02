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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_SS_MICRO_CACHE_PREWARM_SERVICE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_SS_MICRO_CACHE_PREWARM_SERVICE_H_

#include <stdint.h>
#include "storage/shared_storage/prewarm/ob_replica_prewarm_struct.h"
#include "storage/shared_storage/task/ob_sync_hot_micro_key_task.h"
#include "storage/shared_storage/task/ob_consume_hot_micro_key_task.h"
#include "storage/shared_storage/task/ob_mc_prewarm_level_refresh_task.h"

namespace oceanbase
{
namespace compaction
{
enum class ObSSMajorPrewarmLevel : uint8;
}
namespace storage
{

class ObSSMicroCachePrewarmService
{
public:
  static const int64_t OB_BATCH_MICRO_KEY_BUCKET_NUM = 1001;
  ObSSMicroCachePrewarmService();
  virtual ~ObSSMicroCachePrewarmService();
  static int mtl_init(ObSSMicroCachePrewarmService *&prewarm_service);
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
  ObReplicaPrewarmHandler &get_replica_prewarm_handler();
  int64_t get_major_prewarm_percent() const;
  void set_major_prewarm_percent(const int64_t major_prewarm_percent);
  void reset_major_prewarm_percent();
  compaction::ObSSMajorPrewarmLevel get_major_prewarm_level() const;
  void set_major_prewarm_level(const compaction::ObSSMajorPrewarmLevel prewarm_level);
  int init_sync_hot_micro_key_task(ObLS *ls, volatile bool &is_stop);
  int init_consume_hot_micro_key_task(ObLS *ls, volatile bool &is_stop);
  int schedule_sync_hot_micro_key_task(const int64_t ls_id);
  int schedule_consume_hot_micro_key_task(const int64_t ls_id);
  int stop_sync_hot_micro_key_task(const int64_t ls_id);
  int stop_consume_hot_micro_key_task(const int64_t ls_id);
  int wait_sync_hot_micro_key_task(const int64_t ls_id);
  int wait_consume_hot_micro_key_task(const int64_t ls_id);
  int destroy_sync_hot_micro_key_task(const int64_t ls_id);
  int destroy_consume_hot_micro_key_task(const int64_t ls_id);
  int push_micro_cache_keys_to_consume_task(const int64_t ls_id, const obrpc::ObLSSyncHotMicroKeyArg &arg);

private:
  const static int64_t INVALID_TG_ID = -1;
  static const int64_t LS_PREWARM_MAP_BUCKET_NUM = 7;
  static const int64_t SYNC_TASK_SCHEDULE_INTERVAL_US = 5L * 1000L * 1000L; // 5s
  static const int64_t CONSUME_TASK_SCHEDULE_INTERVAL_US = 1L * 1000L * 1000L; // 1s
  typedef int64_t ObLSId;

private:
  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  ObReplicaPrewarmHandler replica_prewarm_handler_;
  int64_t major_prewarm_percent_;
  compaction::ObSSMajorPrewarmLevel major_prewarm_level_;
  int tg_id_;
  ObMCPrewarmLevelRefreshTask mc_prewarm_level_refresh_task_;
  hash::ObHashMap<ObLSId, ObSyncHotMicroKeyTask*> sync_hot_micro_key_task_map_;
  hash::ObHashMap<ObLSId, ObConsumeHotMicroKeyTask*> consume_hot_micro_key_task_map_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_SS_MICRO_CACHE_PREWARM_SERVICE_H_ */
