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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CONSUME_HOT_MICRO_KEY_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CONSUME_HOT_MICRO_KEY_TASK_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/ob_rpc_struct.h"
#include "lib/queue/ob_link_queue.h"

namespace oceanbase
{
namespace storage
{
class ObSSLSPreWarmHandler;

struct ObSyncHotMicroKeyArgNode : public common::ObLink
{
public:
  ObSyncHotMicroKeyArgNode() {}
  ~ObSyncHotMicroKeyArgNode() {}
  TO_STRING_KV(K_(arg));

public:
  obrpc::ObLSSyncHotMicroKeyArg arg_;
};

class ObConsumeHotMicroKeyTask : public common::ObTimerTask
{
public:
  friend class ObSSLSPreWarmHandler;
  ObConsumeHotMicroKeyTask(volatile bool &is_stop);
  virtual ~ObConsumeHotMicroKeyTask() { destroy(); }
  int init(const int64_t ls_id);
  void destroy();
  void reset_group();
  int push_group(const obrpc::ObLSSyncHotMicroKeyArg &arg);
  int pop_group(obrpc::ObLSSyncHotMicroKeyArg &arg);
  virtual void runTimerTask() override;
  int consume_hot_micro_keys();
  int get_missed_micro_keys(const ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_keys,
                            ObIArray<ObSSMicroBlockCacheKeyMeta> &missed_micro_keys);
  int push_micro_cache_keys(const obrpc::ObLSSyncHotMicroKeyArg &arg);
  OB_INLINE int64_t get_fetch_micro_key_workers() const { return ATOMIC_LOAD(&fetch_micro_key_workers_); }
  OB_INLINE bool is_inited() const { return is_inited_; }

public:
  static const int64_t SLEEP_TIMEOUT = 100L * 1000L; // 100ms

private:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 64;
  static const int64_t MAX_MICRO_CACHE_KEY_GROUP_COUNT = 2;

private:
  bool is_inited_;
  volatile bool &is_stop_;
  common::ObLinkQueue micro_cache_key_groups_;
  int64_t fetch_micro_key_workers_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CONSUME_HOT_MICRO_KEY_TASK_H_ */
