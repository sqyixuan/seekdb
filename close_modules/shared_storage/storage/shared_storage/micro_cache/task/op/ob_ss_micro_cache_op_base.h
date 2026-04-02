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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_OP_BASE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_OP_BASE_H_

#include <stdint.h>
#include "lib/ob_define.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_ctx.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace storage
{
class ObSSMemDataManager;
class ObSSMicroMetaManager;
class ObSSPhysicalBlockManager;
class ObTenantFileManager;
class ObSSLAMicroKeyManager;

class ObSSMicroCacheOpBase
{
public:
  ObSSMicroCacheOpBase() 
    : is_inited_(false), is_enabled_(false), is_closed_(false), tenant_id_(OB_INVALID_TENANT_ID), 
      start_time_us_(0), task_ctx_(nullptr)
  {}
  virtual ~ObSSMicroCacheOpBase() { destroy(); }
  int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();

  bool is_closed() const { return is_closed_; }
  bool is_op_enabled() const { return is_enabled_; }
  void enable_op() { is_enabled_ = true; }
  void disable_op() { is_enabled_ = false; }

protected:
  int64_t get_cost_us() const;

  ObSSMemDataManager* mem_data_mgr();
  ObSSMicroMetaManager* micro_meta_mgr();
  ObSSPhysicalBlockManager* phy_blk_mgr();
  ObTenantFileManager* tnt_file_mgr();
  ObSSMicroCacheStat* cache_stat();
  ObSSLAMicroKeyManager* la_micro_mgr();

protected:
  bool is_inited_;
  volatile bool is_enabled_; // If set as true, the task_op main processing logic won't be invoked in next round
  bool is_closed_; // When is_enabled=false and finish current round execution, is_closed will be set as true
  uint64_t tenant_id_;
  int64_t start_time_us_; // the start time of task in each round
  ObSSMicroCacheTaskCtx *task_ctx_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_OP_BASE_H_ */
