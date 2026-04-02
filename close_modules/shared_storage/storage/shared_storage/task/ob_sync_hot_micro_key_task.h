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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SYNC_HOT_MICRO_KEY_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SYNC_HOT_MICRO_KEY_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{
class ObSSLSPreWarmHandler;
class ObLS;

class ObSyncHotMicroKeyTask : public common::ObTimerTask
{
public:
  friend class ObSSLSPreWarmHandler;
  ObSyncHotMicroKeyTask(volatile bool &is_stop);
  virtual ~ObSyncHotMicroKeyTask() { destroy(); }
  int init(ObLS *ls);
  void destroy();
  virtual void runTimerTask() override;
  int sync_hot_micro_key();
  OB_INLINE bool is_inited() const { return is_inited_; }

private:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 64;
  int get_batch_micro_block_cache_keys(hash::ObHashSet<ObSSMicroBlockCacheKeyMeta> &micro_key_set);
  int get_batch_micro_keys_from_kvcache(hash::ObHashSet<ObSSMicroBlockCacheKeyMeta> &micro_key_set);
  int filter_micro_block_cache_key(const blocksstable::ObMicroBlockCacheKey &block_cache_key,
                                   bool &is_filter,
                                   ObSSMicroBlockCacheKeyMeta &micro_meta,
                                   int64_t &lack_count);
  int filter_micro_key_not_exist_in_sscache(const ObSSMicroBlockCacheKey &micro_cache_key,
                                            bool &is_filter,
                                            ObSSMicroBlockCacheKeyMeta &micro_meta);
  int get_batch_micro_keys_from_sscache(hash::ObHashSet<ObSSMicroBlockCacheKeyMeta> &micro_key_set);
  int send_sync_hot_micro_key_rpc(const obrpc::ObLSSyncHotMicroKeyArg &arg);
  int get_dest_addr(ObIArray<ObAddr> &addrs);

private:
  bool is_inited_;
  volatile bool &is_stop_;
  ObSSLSPreWarmHandler *ls_prewarm_handler_;
  ObLS *ls_;
  int64_t ls_id_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SYNC_HOT_MICRO_KEY_TASK_H_ */
