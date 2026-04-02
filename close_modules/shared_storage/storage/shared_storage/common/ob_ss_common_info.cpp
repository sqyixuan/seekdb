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

#include "ob_ss_common_info.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"

namespace oceanbase
{
namespace storage
{

static int set_add_stat_value(common::ObDiagnoseTenantInfo &diag_info, const uint32_t stat_id, const int64_t value)
{
  int ret = OB_SUCCESS;
  ObStatEventAddStat *stat = nullptr;
  if (OB_UNLIKELY(stat_id >= ObStatEventIds::STAT_EVENT_ADD_END)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "stat id invalid", KR(ret), K(stat_id));
  } else if (OB_ISNULL(stat = diag_info.get_add_stat_stats().get(stat_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "stat id invalid", KR(ret), K(stat_id), KP(stat));
  } else {
    stat->stat_value_ = value;
  }
  return ret;
}

// NOTICE: Cuz update_stat() won't get correct previous stat_value, thus we set the stat_value of
//         add_event_stat directly.
static int set_ss_micro_cache_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else {
    ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
    if (OB_ISNULL(micro_cache)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "micro cache is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_ALLOC_DISK_SIZE,
        micro_cache->get_micro_cache_stat().phy_blk_stat_.cache_file_size_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_ALLOC_DISK_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_USED_MEM_SIZE,
        micro_cache->get_micro_cache_stat().micro_stat_.micro_total_mem_size_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_USED_MEM_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_HIT,
        micro_cache->get_micro_cache_stat().hit_stat_.cache_hit_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_HIT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_MISS,
        micro_cache->get_micro_cache_stat().hit_stat_.cache_miss_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_MISS", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_FAIL_ADD,
        micro_cache->get_micro_cache_stat().hit_stat_.fail_add_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_FAIL_ADD", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_FAIL_GET,
        micro_cache->get_micro_cache_stat().hit_stat_.fail_get_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_FAIL_GET", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_MC_PREWARM,
        micro_cache->get_micro_cache_stat().hit_stat_.major_compaction_prewarm_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_MC_PREWARM", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_HA_PREWARM,
        micro_cache->get_micro_cache_stat().hit_stat_.migration_prewarm_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_HA_PREWARM", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_REPLICA_PREWARM,
        micro_cache->get_micro_cache_stat().hit_stat_.replica_prewarm_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_REPLICA_PREWARM", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_DDL_PREWARM,
        micro_cache->get_micro_cache_stat().hit_stat_.ddl_prewarm_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_DDL_PREWARM", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_HOLD_COUNT,
        micro_cache->get_micro_cache_stat().micro_stat_.total_micro_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_HOLD_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_HOLD_SIZE,
        micro_cache->get_micro_cache_stat().micro_stat_.total_micro_size_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_HOLD_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_HIT_BYTES,
        micro_cache->get_micro_cache_stat().hit_stat_.cache_hit_bytes_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_HIT_BYTES", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_MISS_BYTES,
        micro_cache->get_micro_cache_stat().hit_stat_.cache_miss_bytes_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_MISS_BYTES", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_MC_PREWARM_BYTES,
        micro_cache->get_micro_cache_stat().hit_stat_.major_compaction_prewarm_bytes_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_MC_PREWARM_BYTES", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_HA_PREWARM_BYTES,
        micro_cache->get_micro_cache_stat().hit_stat_.migration_prewarm_bytes_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_HA_PREWARM_BYTES", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_REPLICA_PREWARM_BYTES,
        micro_cache->get_micro_cache_stat().hit_stat_.replica_prewarm_bytes_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_REPLICA_PREWARM_BYTES", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_DDL_PREWARM_BYTES,
        micro_cache->get_micro_cache_stat().hit_stat_.ddl_prewarm_bytes_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_DDL_PREWARM_BYTES", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_USED_DISK_SIZE,
        micro_cache->get_micro_cache_stat().phy_blk_stat_.get_used_disk_size()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_USED_DISK_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_COMMON_ADD_COUNT,
        micro_cache->get_micro_cache_stat().io_stat_.common_io_param_.add_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_COMMON_ADD_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_COMMON_GET_COUNT,
        micro_cache->get_micro_cache_stat().io_stat_.common_io_param_.get_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_COMMON_GET_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_NEW_ADD_COUNT,
        micro_cache->get_micro_cache_stat().hit_stat_.new_add_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_NEW_ADD_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_COMMON_NEW_ADD_COUNT,
        micro_cache->get_micro_cache_stat().hit_stat_.get_common_new_add_cnt()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_COMMON_NEW_ADD_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_EVICT_COUNT,
        micro_cache->get_micro_cache_stat().task_stat_.get_total_evict_cnt()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_EVICT_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_DELETE_COUNT,
        micro_cache->get_micro_cache_stat().task_stat_.get_total_delete_cnt()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_DELETE_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MICRO_CACHE_REORGAN_FREE_BLK_COUNT,
        micro_cache->get_micro_cache_stat().task_stat_.reorgan_free_blk_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_REORGAN_FREE_BLK_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_HOT_MICRO_LACK_COUNT,
        micro_cache->get_micro_cache_stat().hit_stat_.hot_micro_lack_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_HOT_MICRO_LACK_COUNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_COMMON_READ_IOPS,
        micro_cache->get_micro_cache_stat().cache_load_.common_io_load_.read_load_.get_iops()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_COMMON_READ_IOPS", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_COMMON_READ_THROUGHPUT,
        micro_cache->get_micro_cache_stat().cache_load_.common_io_load_.read_load_.get_throughput()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_COMMON_READ_THROUGHPUT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_COMMON_WRITE_IOPS,
        micro_cache->get_micro_cache_stat().cache_load_.common_io_load_.write_load_.get_iops()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_COMMON_WRITE_IOPS", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_COMMON_WRITE_THROUGHPUT,
        micro_cache->get_micro_cache_stat().cache_load_.common_io_load_.write_load_.get_throughput()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_COMMON_WRITE_THROUGHPUT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_PREWARM_READ_IOPS,
        micro_cache->get_micro_cache_stat().cache_load_.prewarm_io_load_.read_load_.get_iops()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_PREWARM_READ_IOPS", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_PREWARM_READ_THROUGHPUT,
        micro_cache->get_micro_cache_stat().cache_load_.prewarm_io_load_.read_load_.get_throughput()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_PREWARM_READ_THROUGHPUT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_PREWARM_WRITE_IOPS,
        micro_cache->get_micro_cache_stat().cache_load_.prewarm_io_load_.write_load_.get_iops()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_PREWARM_WRITE_IOPS", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_PREWARM_WRITE_THROUGHPUT,
        micro_cache->get_micro_cache_stat().cache_load_.prewarm_io_load_.write_load_.get_throughput()))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_PREWARM_WRITE_THROUGHPUT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_VALID_CNT,
        micro_cache->get_micro_cache_stat().micro_stat_.valid_micro_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_VALID_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_MICRO_CACHE_VALID_SIZE,
        micro_cache->get_micro_cache_stat().micro_stat_.valid_micro_size_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_VALID_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info,
        ObStatEventIds::SS_MICRO_CACHE_STORAGE_CACHE_POLICY_PREWARM,
        micro_cache->get_micro_cache_stat().hit_stat_.storage_cache_policy_prewarm_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_STORAGE_CACHE_POLICY_PREWARM",
          KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info,
        ObStatEventIds::SS_MICRO_CACHE_STORAGE_CACHE_POLICY_PREWARM_BYTES,
        micro_cache->get_micro_cache_stat().hit_stat_.storage_cache_policy_prewarm_bytes_))) {
      OB_LOG(WARN, "fail to set SS_MICRO_CACHE_STORAGE_CACHE_POLICY_PREWARM_BYTES",
          KR(ret), K(tenant_id));
    } 
  }
  return ret;
}

static int set_ss_local_cache_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else {
    ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
    if (OB_ISNULL(tnt_disk_space_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "disk space manager is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_LOCAL_CACHE_META_ALLOC_SIZE,
        tnt_disk_space_mgr->get_meta_file_reserved_size()))) {
      OB_LOG(WARN, "fail to set SS_LOCAL_CACHE_META_ALLOC_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_ALLOC_SIZE,
        tnt_disk_space_mgr->get_preread_cache_reserved_size()
        + tnt_disk_space_mgr->get_tmp_file_write_cache_reserved_size()))) {
      OB_LOG(WARN, "fail to set SS_LOCAL_CACHE_TMPFILE_ALLOC_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_LOCAL_CACHE_INCREMENTAL_DATA_ALLOC_SIZE,
        tnt_disk_space_mgr->get_private_macro_reserved_size()))) {
      OB_LOG(WARN, "fail to set SS_LOCAL_CACHE_INCREMENTAL_DATA_ALLOC_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_LOCAL_CACHE_INCREMENTAL_DATA_USED_DISK_SIZE,
        tnt_disk_space_mgr->get_private_macro_alloc_size()))) {
      OB_LOG(WARN, "fail to set SS_LOCAL_CACHE_INCREMENTAL_DATA_USED_DISK_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_LOCAL_CACHE_META_USED_DISK_SIZE,
        tnt_disk_space_mgr->get_meta_file_alloc_size()))) {
      OB_LOG(WARN, "fail to set SS_LOCAL_CACHE_META_USED_DISK_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_W,
        tnt_disk_space_mgr->get_tmp_file_write_cache_alloc_size()))) {
      OB_LOG(WARN, "fail to set SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_W", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_R,
        tnt_disk_space_mgr->get_tmp_file_read_cache_alloc_size()))) {
      OB_LOG(WARN, "fail to set SS_LOCAL_CACHE_TMPFILE_USED_DISK_SIZE_R", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::SS_LOCAL_CACHE_MAJOR_MACRO_USED_DISK_SIZE,
        tnt_disk_space_mgr->get_major_macro_read_cache_alloc_size()))) {
      OB_LOG(WARN, "fail to set SS_LOCAL_CACHE_MAJOR_MACRO_USED_DISK_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_TMPFILE_CACHE_HIT,
        tnt_disk_space_mgr->get_tmp_file_cache_stat().cache_hit_cnt_))) {
      OB_LOG(WARN, "fail to set SS_TMPFILE_CACHE_HIT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_TMPFILE_CACHE_MISS,
        tnt_disk_space_mgr->get_tmp_file_cache_stat().cache_miss_cnt_))) {
      OB_LOG(WARN, "fail to set SS_TMPFILE_CACHE_MISS", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MAJOR_MACRO_CACHE_HIT,
        tnt_disk_space_mgr->get_major_macro_cache_stat().cache_hit_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MAJOR_MACRO_CACHE_HIT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(set_add_stat_value(diag_info, ObStatEventIds::SS_MAJOR_MACRO_CACHE_MISS,
        tnt_disk_space_mgr->get_major_macro_cache_stat().cache_miss_cnt_))) {
      OB_LOG(WARN, "fail to set SS_MAJOR_MACRO_CACHE_MISS", KR(ret), K(tenant_id));
    }
  }

  return ret;
}

int set_ss_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(set_ss_micro_cache_stats(tenant_id, diag_info))) {
    OB_LOG(WARN, "fail to set ss micro cache stats", KR(ret), K(tenant_id));
  } else if (OB_FAIL(set_ss_local_cache_stats(tenant_id, diag_info))) {
    OB_LOG(WARN, "fail to set ss local cache stats", KR(ret), K(tenant_id));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
