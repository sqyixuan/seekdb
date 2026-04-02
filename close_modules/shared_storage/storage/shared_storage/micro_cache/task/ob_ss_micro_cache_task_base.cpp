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
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_base.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_data_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_la_micro_key_manager.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------ObSSMicroCacheTaskBase------------------------------------------*/
int ObSSMicroCacheTaskBase::init(
    const uint64_t tenant_id, 
    const int64_t interval_us, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (interval_us < 0) || (!task_ctx.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(interval_us), "valid_task_ctx", task_ctx.is_valid());
  } else {
    tenant_id_ = tenant_id;
    interval_us_ = interval_us;
    adjusted_interval_us_ = interval_us;
    task_ctx_ = &task_ctx;
  }
  return ret;
}

void ObSSMicroCacheTaskBase::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  interval_us_ = 0;
  adjusted_interval_us_ = 0;
  task_ctx_ = nullptr;
  is_inited_ = false;
}

// Upper level should promise task_ctx is valid
ObSSMemDataManager* ObSSMicroCacheTaskBase::mem_data_mgr() { return task_ctx_->mem_data_mgr_; }
ObSSMicroMetaManager* ObSSMicroCacheTaskBase::micro_meta_mgr() { return task_ctx_->micro_meta_mgr_; }
ObSSPhysicalBlockManager* ObSSMicroCacheTaskBase::phy_blk_mgr() { return task_ctx_->phy_blk_mgr_; }
ObTenantFileManager* ObSSMicroCacheTaskBase::tnt_file_mgr() { return task_ctx_->tnt_file_mgr_; }
ObSSMicroCacheStat* ObSSMicroCacheTaskBase::cache_stat() { return task_ctx_->cache_stat_; }
ObSSLAMicroKeyManager* ObSSMicroCacheTaskBase::la_micro_mgr() { return task_ctx_->la_micro_mgr_; }

} // storage
} // oceanbase
