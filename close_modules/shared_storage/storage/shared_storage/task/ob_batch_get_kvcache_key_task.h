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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_BATCH_GET_KVCACHE_KEY_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_BATCH_GET_KVCACHE_KEY_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_micro_block_cache.h"

namespace oceanbase
{
namespace storage
{

class ObBatchGetKVcacheKeyTask : public common::ObTimerTask
{
public:
  ObBatchGetKVcacheKeyTask(volatile bool &is_stop);
  virtual ~ObBatchGetKVcacheKeyTask() { destroy(); }
  int init();
  void destroy();
  virtual void runTimerTask() override;
  int bacth_get_micro_keys_from_kvcache();
  int get_micro_block_cache_keys(ObIArray<blocksstable::ObMicroBlockCacheKey> &keys, bool &full_scan);

private:
  int check_ls_replica_prewarm_stop();

private:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 64;
  static constexpr const double DEFAULT_ONCE_BATCH_GET_BUCKET_NUM_FACTOR = 0.01; // 1%
  static const int64_t OB_MAX_BATCH_GET_BUCKET_NUM = 500000; // 50w
  static const int64_t CHECK_LS_REPLICA_PREWARM_STOP_INTERVAL_US = 3L * 60L * 1000L * 1000L; // 3min
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;

private:
  bool is_inited_;
  volatile bool &is_stop_;
  RWLock rwlock_;
  ObSEArray<blocksstable::ObMicroBlockCacheKey, OB_DEFAULT_ARRAY_CAPACITY> block_cache_keys_;
  int64_t default_once_batch_bucket_num_; // total_bucket_num * 1%, min value is 1w, max value is 50w
  int64_t batch_cnt_per_round_; // total_bucket_num / default_once_batch_bucket_num
  int64_t produce_op_cnt_; // When execute bacth_get_micro_keys_from_kvcache function, need to inc this value
  int64_t consume_op_cnt_; // when execute get_micro_block_cache_keys function, need to inc this value
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_BATCH_GET_KVCACHE_KEY_TASK_H_ */
