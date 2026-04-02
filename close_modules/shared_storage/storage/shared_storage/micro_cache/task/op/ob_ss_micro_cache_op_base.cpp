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

#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_op_base.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

void ObSSMicroCacheOpBase::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  start_time_us_ = 0;
  task_ctx_ = nullptr;
  is_enabled_ = false;
  is_closed_ = false;
  is_inited_ = false;
}

int ObSSMicroCacheOpBase::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (!task_ctx.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "valid_task_ctx", task_ctx.is_valid());
  } else {
    tenant_id_ = tenant_id;
    task_ctx_ = &task_ctx;
    is_enabled_ = true;
    is_closed_ = false;
  }
  return ret;
}

int64_t ObSSMicroCacheOpBase::get_cost_us() const
{
  int ret = OB_SUCCESS;
  int64_t cost_us = 0;
  if (start_time_us_ > 0) {
    cost_us = ObTimeUtility::current_time() - start_time_us_;
    if (cost_us >= SS_SLOW_TASK_COST_US) {
      LOG_WARN("slow micro_cache task", K(cost_us), K_(start_time_us));
    }
  }
  return cost_us;
}

// Upper level should promise task_ctx is valid
ObSSMemDataManager* ObSSMicroCacheOpBase::mem_data_mgr() { return task_ctx_->mem_data_mgr_; }
ObSSMicroMetaManager* ObSSMicroCacheOpBase::micro_meta_mgr() { return task_ctx_->micro_meta_mgr_; }
ObSSPhysicalBlockManager* ObSSMicroCacheOpBase::phy_blk_mgr() { return task_ctx_->phy_blk_mgr_; }
ObTenantFileManager* ObSSMicroCacheOpBase::tnt_file_mgr() { return task_ctx_->tnt_file_mgr_; }
ObSSMicroCacheStat* ObSSMicroCacheOpBase::cache_stat() { return task_ctx_->cache_stat_; }
ObSSLAMicroKeyManager* ObSSMicroCacheOpBase::la_micro_mgr() { return task_ctx_->la_micro_mgr_; }

} // storage
} // oceanbase
