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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_REPLICA_PREWARM_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_REPLICA_PREWARM_STRUCT_H_

#include "rpc/obrpc/ob_rpc_packet.h"
#include "storage/ob_storage_rpc.h"
#include "storage/shared_storage/task/ob_batch_get_kvcache_key_task.h"

namespace oceanbase
{
namespace storage
{

class ObReplicaPrewarmMicroBlockProducer
{
public:
  ObReplicaPrewarmMicroBlockProducer();
  virtual ~ObReplicaPrewarmMicroBlockProducer();
  int init(const common::ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_metas);
  int get_next_micro_block(ObSSMicroBlockCacheKeyMeta &micro_meta, blocksstable::ObBufferReader &data);

private:
  int inner_get_next_micro_block(ObSSMicroBlockCacheKeyMeta &micro_meta,
                                 blocksstable::ObBufferReader &data,
                                 bool &is_get);
  int alloc_small_io_bufs();
  int get_io_buf(const int64_t buf_size, common::ObArenaAllocator &allocator, char *&io_buf);
  int prefetch_if_need();
  void reset_io_resources();

private:
  static const int64_t PREFETCH_PARALLELISM = 50;
  bool is_inited_;
  common::ObArray<ObSSMicroBlockCacheKeyMeta> micro_metas_;
  int64_t process_idx_; // indicate idx of micro_metas_ that is processed
  int64_t prefetch_idx_; // indicate idx of micro_metas_ that is prefetched
  common::ObArenaAllocator small_buf_allocator_; // allocator of io bufs with OB_DEFAULT_SSTABLE_BLOCK_SIZE
  common::ObArenaAllocator big_buf_allocator_; // allocator of io bufs exceed OB_DEFAULT_SSTABLE_BLOCK_SIZE
  char *small_io_bufs_[PREFETCH_PARALLELISM] = {nullptr}; // io bufs with OB_DEFAULT_SSTABLE_BLOCK_SIZE
  blocksstable::ObStorageObjectHandle obj_handles_[PREFETCH_PARALLELISM];
};


class ObReplicaPrewarmMicroBlockReader
{
public:
  ObReplicaPrewarmMicroBlockReader();
  virtual ~ObReplicaPrewarmMicroBlockReader();
  int init(const common::ObAddr &addr,
           const uint64_t tenant_id,
           const common::ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_metas);
  int get_next_micro_block(ObSSMicroBlockCacheKeyMeta &micro_meta, blocksstable::ObBufferReader &data);

private:
  int alloc_buffers();
  int fetch_next_buffer_if_need();
  int fetch_next_buffer();

private:
  bool is_inited_;
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_REPLICA_PREWARM_FETCH_MICRO_BLOCK> handle_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  blocksstable::ObBufferReader data_buffer_; // Data used to assemble micro blocks
  common::ObDataBuffer rpc_buffer_;
  int64_t rpc_buffer_parse_pos_;
  common::ObArenaAllocator allocator_;
  int64_t last_send_time_;
  int64_t data_size_;
  DISALLOW_COPY_AND_ASSIGN(ObReplicaPrewarmMicroBlockReader);
};


class ObReplicaPrewarmMicroBlockWriter
{
public:
  ObReplicaPrewarmMicroBlockWriter(volatile bool &is_stop);
  virtual ~ObReplicaPrewarmMicroBlockWriter();
  int init(const uint64_t tenant_id, ObReplicaPrewarmMicroBlockReader *reader);
  int process();

private:
  int write_micro_block_cache(const ObSSMicroBlockCacheKeyMeta &micro_meta,
                              const blocksstable::ObBufferReader &data);

private:
  bool is_inited_;
  volatile bool &is_stop_;
  uint64_t tenant_id_;
  ObReplicaPrewarmMicroBlockReader *reader_;
  DISALLOW_COPY_AND_ASSIGN(ObReplicaPrewarmMicroBlockWriter);
};


class ObReplicaPrewarmHandler : public lib::TGTaskHandler
{
public:
  ObReplicaPrewarmHandler();
  virtual ~ObReplicaPrewarmHandler();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  int push_task(volatile bool &is_stop,
                const common::ObAddr &addr,
                const uint64_t tenant_id,
                const int64_t ls_id,
                const common::ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_metas,
                int64_t &fetch_micro_key_workers);
  virtual void handle(void *task) override;
  virtual void handle_drop(void *task) override;

public:
  static const int64_t MIN_THREAD_COUNT = 1;
  static const int64_t MAX_THREAD_COUNT = 5;
  static const int64_t MAX_TASK_NUM = 128;

private:
  static const int64_t INVALID_TG_ID = -1;
  bool is_inited_;
  int tg_id_;
};

class ObReplicaPrewarmTask
{
public:
  ObReplicaPrewarmTask(volatile bool &is_stop, int64_t &fetch_micro_block_workers);
  virtual ~ObReplicaPrewarmTask();
  int init(const common::ObAddr &addr,
           const uint64_t tenant_id,
           const int64_t ls_id,
           const common::ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_metas);
  int do_task();
  void dec_fetch_micro_block_workers();
  TO_STRING_KV(K_(is_inited), K_(is_stop), K_(addr), K_(tenant_id), K_(ls_id), K_(micro_metas),
               K_(trace_id));

private:
  int get_micro_block_reader(ObReplicaPrewarmMicroBlockReader *&reader);
  int get_micro_block_writer(ObReplicaPrewarmMicroBlockReader *reader,
                             ObReplicaPrewarmMicroBlockWriter *&writer);
  void free_micro_block_reader(ObReplicaPrewarmMicroBlockReader *&reader);
  void free_micro_block_writer(ObReplicaPrewarmMicroBlockWriter *&writer);

private:
  bool is_inited_;
  volatile bool &is_stop_;
  common::ObAddr addr_;
  uint64_t tenant_id_;
  int64_t ls_id_;
  common::ObArray<ObSSMicroBlockCacheKeyMeta> micro_metas_;
  int64_t &fetch_micro_block_workers_;
  common::ObCurTraceId::TraceId trace_id_;
  DISALLOW_COPY_AND_ASSIGN(ObReplicaPrewarmTask);
};

class ObLSPrewarmManager
{
public:
  static ObLSPrewarmManager &get_instance();
  int init();
  void stop();
  void wait();
  void destroy();
  int get_micro_block_cache_keys(ObIArray<blocksstable::ObMicroBlockCacheKey> &keys, bool &full_scan);

private:
  int check_ls_replica_prewarm_stop();

private:
  ObLSPrewarmManager();
  ~ObLSPrewarmManager();
  static const int64_t SCHEDULE_INTERVAL_US = 5L * 1000L * 1000L; // 5s

private:
  bool is_inited_;
  volatile bool is_stop_;
  ObBatchGetKVcacheKeyTask batch_get_kvcache_key_task_;
};

#define OB_LS_PREWARM_MGR (oceanbase::storage::ObLSPrewarmManager::get_instance())

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_REPLICA_PREWARM_STRUCT_H_ */
