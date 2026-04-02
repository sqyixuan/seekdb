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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_BASE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_BASE_H_

#include <stdint.h>
#include "lib/ob_define.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
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

class ObSSMicroCacheTaskBase
{
public:
  ObSSMicroCacheTaskBase() 
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), interval_us_(0), adjusted_interval_us_(0),
      task_ctx_(nullptr)
  {}
  virtual ~ObSSMicroCacheTaskBase() { destroy(); }
  void destroy();
  int init(const uint64_t tenant_id, const int64_t interval_us, ObSSMicroCacheTaskCtx &task_ctx);

protected:
  ObSSMemDataManager* mem_data_mgr();
  ObSSMicroMetaManager* micro_meta_mgr();
  ObSSPhysicalBlockManager* phy_blk_mgr();
  ObTenantFileManager* tnt_file_mgr();
  ObSSMicroCacheStat* cache_stat();
  ObSSLAMicroKeyManager* la_micro_mgr();

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t interval_us_;
  int64_t adjusted_interval_us_;
  ObSSMicroCacheTaskCtx *task_ctx_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_BASE_H_ */
