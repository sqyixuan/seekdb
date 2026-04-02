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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FD_CACHE_MGR_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FD_CACHE_MGR_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_mutex.h"
#include "lib/task/ob_timer.h"
#include "storage/shared_storage/ob_ss_fd_cache_struct.h"

namespace oceanbase
{
namespace storage
{

class ObSSFdCacheMgr;

class ObSSFdCacheEvictTask : public common::ObTimerTask
{
public:
  ObSSFdCacheEvictTask(volatile bool &is_stop);
  virtual ~ObSSFdCacheEvictTask();
  int init(const int tg_id, ObSSFdCacheMgr *fd_cache_mgr);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int64_t SCHEDULE_INTERVAL_US = 10 * 1000L; // 10ms
  bool is_inited_;
  volatile bool &is_stop_;
  ObSSFdCacheMgr *fd_cache_mgr_;
};


class ObSSFdCacheMgr
{
public:
  ObSSFdCacheMgr();
  virtual ~ObSSFdCacheMgr();
  int init();
  int start(const int tg_id);
  void stop();
  void wait();
  void destroy();
  int get_fd(const blocksstable::MacroBlockId &macro_id, ObSSFdCacheHandle &fd_cache_handle);
  int check_fd_cache_exist(const blocksstable::MacroBlockId &macro_id, bool &is_exist);
  int erase_fd_cache_when_del_file(const blocksstable::MacroBlockId &macro_id);
  int do_evict_work();

private:
  int try_get_cache(const ObSSFdCacheKey &fd_cache_key,
                    ObSSFdCacheHandle &fd_cache_handle,
                    bool &is_hit);
  int open_and_try_add_cache(const ObSSFdCacheKey &fd_cache_key,
                             ObSSFdCacheHandle &fd_cache_handle);
  int open_fd(const ObSSFdCacheKey &fd_cache_key, int &fd);
  int batch_get_handles_randomly(const int64_t batch_cnt,
                                 common::ObIArray<ObSSFdCacheHandle> &fd_cache_handles);
  int evict_handle_with_min_timestamp(common::ObIArray<ObSSFdCacheHandle> &fd_cache_handles);
  int evict_long_time_no_use_fd_cache();

private:
  typedef common::hash::ObHashMap<ObSSFdCacheKey, ObSSFdCacheHandle> FdMap;
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t MAX_FD_CACHE_CNT = 10000; // 1w
  static const int64_t EVICT_THRESHOLD_HIGH = MAX_FD_CACHE_CNT * 85 / 100; // 85%
  static const int64_t EVICT_THRESHOLD_LOW = MAX_FD_CACHE_CNT * 70 / 100; // 70%
  static const int64_t EVICT_THRESHOLD_RESERVED = MAX_FD_CACHE_CNT * 30 / 100; // 30%
  static const int64_t BUCKET_NUM = MAX_FD_CACHE_CNT;
  static const int64_t MIN_RESERVED_TIME_US = 100 * 1000L; // 100ms
  static const int64_t MAX_RESERVED_TIME_US = 30 * 60 * 1000 * 1000L; // 30min
  bool is_inited_;
  volatile bool is_stop_;
  FdMap fd_map_;
  common::ObSmallObjPool<ObSSFdCacheNode> fd_cache_node_pool_;
  ObSSFdCacheEvictTask evict_task_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FD_CACHE_MGR_H_ */
