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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_PART_TRANS_RESOLVER_FACTORY_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_PART_TRANS_RESOLVER_FACTORY_H__

#include "ob_cdc_part_trans_resolver.h"           // IObLogPartTransResolver, PartTransTaskMap

#include "lib/allocator/ob_small_allocator.h"     // ObSmallAllocator

namespace oceanbase
{
namespace libobcdc
{

class IObLogPartTransResolverFactory
{
public:
  virtual ~IObLogPartTransResolverFactory() {}

public:
  virtual int alloc(const char *tls_id_str, IObCDCPartTransResolver *&ptr) = 0;
  virtual void free(IObCDCPartTransResolver *ptr) = 0;
};

/////////////////////////////////////////////////////////////////////

typedef ObLogTransTaskPool<PartTransTask> TaskPool;
class IObLogEntryTaskPool;
class IObLogFetcherDispatcher;
class IObLogClusterIDFilter;

class ObLogPartTransResolverFactory : public IObLogPartTransResolverFactory
{
  static const int64_t DEFAULT_BLOCK_SIZE = (1L << 24);

public:
  ObLogPartTransResolverFactory();
  virtual ~ObLogPartTransResolverFactory();

public:
  int init(TaskPool &task_pool,
      IObLogEntryTaskPool &log_entry_task_pool,
      IObLogFetcherDispatcher &dispatcher,
      IObLogClusterIDFilter &cluster_id_filter);
  void destroy();

public:
  virtual int alloc(const char *tls_id_str, IObCDCPartTransResolver *&ptr);
  virtual void free(IObCDCPartTransResolver *ptr);

  struct TransInfoClearerByCheckpoint
  {
    int64_t checkpoint_;
    int64_t purge_count_;

    explicit TransInfoClearerByCheckpoint(const int64_t checkpoint) : checkpoint_(checkpoint), purge_count_(0)
    {}
    bool operator()(const PartTransID &part_trans_id, TransCommitInfo &trans_commit_info);
  };

private:
  bool                      inited_;
  TaskPool                  *task_pool_;
  IObLogEntryTaskPool       *log_entry_task_pool_;
  IObLogFetcherDispatcher   *dispatcher_;
  IObLogClusterIDFilter     *cluster_id_filter_;

  common::ObSmallAllocator  allocator_;
  PartTransTaskMap          task_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogPartTransResolverFactory);
};

}
}

#endif
