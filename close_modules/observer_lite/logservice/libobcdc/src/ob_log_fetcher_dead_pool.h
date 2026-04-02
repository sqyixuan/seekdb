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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_DEAD_POOL_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_DEAD_POOL_H__

#include "lib/utility/ob_macro_utils.h"   // DISALLOW_COPY_AND_ASSIGN

#include "ob_log_config.h"                // ObLogConfig
#include "ob_map_queue_thread.h"          // ObMapQueueThread
#include "ob_log_ls_fetch_ctx.h"          // FetchTaskList, LSFetchCtx

namespace oceanbase
{
namespace libobcdc
{

class IObLogFetcherDeadPool
{
public:
  static const int64_t MAX_THREAD_NUM = ObLogConfig::max_dead_pool_thread_num;

public:
  virtual ~IObLogFetcherDeadPool() {}

public:
  virtual int push(LSFetchCtx *task) = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

/////////////////////////////////////////////////////////////////

typedef common::ObMapQueueThread<IObLogFetcherDeadPool::MAX_THREAD_NUM> DeadPoolThread;

class IObLogErrHandler;
class IObLogLSFetchMgr;

class ObLogFetcherDeadPool : public IObLogFetcherDeadPool, public DeadPoolThread
{
  static const int64_t IDLE_WAIT_TIME = 100 * 1000;

public:
  ObLogFetcherDeadPool();
  virtual ~ObLogFetcherDeadPool();

public:
  int init(const int64_t thread_num,
      void *fetcher_host_host,
      IObLogLSFetchMgr &ls_fetch_mgr,
      IObLogErrHandler &err_handler);
  void destroy();

public:
  // Implement the IObLogFetcherDeadPool virtual function
  virtual int push(LSFetchCtx *task);
  virtual int start();
  virtual void stop();
  virtual void mark_stop_flag();

public:
  // Implement the ObMapQueueThread virtual function
  // Overloading thread handling functions
  virtual void run(const int64_t thread_index);

private:
  void reset_task_list_array_();
  int retrieve_task_list_(const int64_t thread_index, FetchTaskList &list);
  int handle_task_list_(const int64_t thread_index, FetchTaskList &list);

private:
  bool                      inited_;
  void                      *fetcher_host_;
  IObLogErrHandler          *err_handler_;
  IObLogLSFetchMgr          *ls_fetch_mgr_;

  // One task array per thread
  FetchTaskList             task_list_array_[MAX_THREAD_NUM];

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcherDeadPool);
};


}
}

#endif
