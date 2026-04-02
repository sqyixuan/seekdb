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
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/resolver/ddl/ob_storage_cache_ddl_util.h"
namespace oceanbase
{
namespace storage
{

/*-----------------------------------------ObTablePolicyInfo-----------------------------------------*/
int ObTablePolicyInfo::init(const int64_t table_id, const ObStorageCachePolicy &table_policy, const int64_t table_schema_version)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(table_id), K(table_policy), K(table_schema_version));
  } else {
    table_id_ = table_id;
    table_policy_ = table_policy;
    table_schema_version_ = table_schema_version;
    is_inited_ = true;
  }
  return ret;
}

void ObTablePolicyInfo::reset()
{
  table_id_ = OB_INVALID_ID;
  table_policy_.reset();
  table_schema_version_ = OB_INVALID_VERSION;
  is_inited_ = false;
}

ObTablePolicyInfo &ObTablePolicyInfo::operator=(const ObTablePolicyInfo &other)
{
  table_id_ = other.table_id_;
  table_policy_ = other.table_policy_;
  table_schema_version_ = other.table_schema_version_;
  is_inited_ = other.is_inited_;
  return *this;
}
bool ObTablePolicyInfo::operator==(const ObTablePolicyInfo &other) const
{
  return table_id_ == other.table_id_ 
         && table_policy_ == other.table_policy_ 
         && table_schema_version_ == other.table_schema_version_
         && is_inited_ == other.is_inited_;
}

bool ObTablePolicyInfo::operator!=(const ObTablePolicyInfo &other) const
{
  return !(*this == other);
}

/*-----------------------------------------ObTabletPolicyInfo-----------------------------------------*/
int ObTabletPolicyInfo::init(const ObPartition *partition)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else {
    part_id_ = partition->get_part_id();
    tablet_policy_ = partition->get_part_storage_cache_policy_type();
    tablet_schema_version_ = partition->get_schema_version();
    tablet_policy_status_ = PolicyStatus::MAX_STATUS;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletPolicyInfo::init(const ObSubPartition *sub_partition)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(sub_partition)) {
    ret = OB_INVALID_ARGUMENT;  
    LOG_WARN("invalid argument", KR(ret), K(sub_partition));
  } else {
    part_id_ = sub_partition->get_sub_part_id();
    tablet_policy_ = sub_partition->get_part_storage_cache_policy_type();
    tablet_schema_version_ = sub_partition->get_schema_version();
    tablet_policy_status_ = PolicyStatus::MAX_STATUS;
    is_inited_ = true;
  }
  return ret;
}

void ObTabletPolicyInfo::reset()
{
  part_id_ = OB_INVALID_ID;
  tablet_policy_ = ObStorageCachePolicyType::MAX_POLICY;
  tablet_schema_version_ = OB_INVALID_VERSION;
  tablet_policy_status_ = PolicyStatus::MAX_STATUS;
  is_inited_ = false;
}

ObTabletPolicyInfo &ObTabletPolicyInfo::operator=(const ObTabletPolicyInfo &other)
{
  if (&other != this) {
    is_inited_ = other.is_inited_;
    part_id_ = other.part_id_;
    tablet_policy_ = other.tablet_policy_;
    tablet_schema_version_ = other.tablet_schema_version_;
    tablet_policy_status_ = other.tablet_policy_status_;
  }
  return *this;
}

bool ObTabletPolicyInfo::operator==(const ObTabletPolicyInfo &other) const    
{
  return is_inited_ == other.is_inited_
         && part_id_ == other.part_id_ 
         && tablet_policy_ == other.tablet_policy_ 
         && tablet_schema_version_ == other.tablet_schema_version_
         && tablet_policy_status_ == other.tablet_policy_status_;
}   

bool ObTabletPolicyInfo::operator!=(const ObTabletPolicyInfo &other) const
{
  return !(*this == other);
}

/*-----------------------------------------ObStorageCachePolicyService-----------------------------------------*/

int ObStorageCachePolicyService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  schema_service_ = &(schema::ObMultiVersionSchemaService::get_instance());
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(table_policy_map_.create(DEFAULT_TABLE_POLICY_MAP_SIZE, ObMemAttr(MTL_ID(), "TablePolicyMap")))) {
    LOG_WARN("fail to create table policy map", KR(ret));
  } else if (OB_FAIL(part_policy_map_.create(DEFAULT_PART_POLICY_MAP_SIZE, ObMemAttr(MTL_ID(),"TabletPolicyMap")))) {
    LOG_WARN("fail to create tablet policy map", KR(ret));
  } else if (OB_FAIL(tablet_status_map_.create(DEFAULT_TABLET_STATUS_MAP_SIZE, ObMemAttr(MTL_ID(),"TabletStatusMap")))) {
    LOG_WARN("fail to create tablet status map", KR(ret));
  } else if (OB_FAIL(refresh_policy_scheduler_.init(tenant_id))) {
    LOG_WARN("fail to init refresh scheduler", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_scheduler_.init(tenant_id))) {
    LOG_WARN("fail to init tablet scheduler", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
   if (OB_FAIL(ret)) {
    LOG_WARN("fail to init storage cache policy mgr", KR(ret), K(tenant_id));
    destroy();
  }
  LOG_INFO("storage cache policy service init success", KR(ret));
  return ret;
}

int ObStorageCachePolicyService::mtl_init(ObStorageCachePolicyService *&service)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("mtl_init storage cache policy service", KR(ret), K(MTL_ID()));
  int64_t tenant_id = MTL_ID();
  if (is_user_tenant(tenant_id)) {
    if (OB_FAIL(service->init(tenant_id))) {
      LOG_WARN("fail to init storage cache policy mgr", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

ObStorageCachePolicyService &ObStorageCachePolicyService::get_instance()
{
  static ObStorageCachePolicyService instance_;
  return instance_;
}

int ObStorageCachePolicyService::start()
{
  int ret = OB_SUCCESS;
  if (is_user_tenant(MTL_ID())) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("storage cache policy mgr not init", KR(ret));
    } else if (OB_FAIL(refresh_policy_scheduler_.start())) {
      LOG_WARN("fail to start refresh scheduler", KR(ret));
    } else if (OB_FAIL(tablet_scheduler_.start())) {
      LOG_WARN("fail to start tablet scheduler", KR(ret));
    }
  }
  LOG_INFO("storage cache policy service start success", KR(ret));
  return ret;
}

void ObStorageCachePolicyService::stop()
{
  if (is_user_tenant(MTL_ID())) {
    if (IS_INIT) {
      is_stopped_ = true;
      tablet_scheduler_.stop();
      refresh_policy_scheduler_.stop();
    }
  }
  LOG_INFO("storage cache policy service stop success");
}

void ObStorageCachePolicyService::wait()
{
  if (is_user_tenant(MTL_ID())) {
    if (IS_INIT) {
      tablet_scheduler_.wait();
      refresh_policy_scheduler_.wait();
    }
  }
  LOG_INFO("storage cache policy service wait success");
}

void ObStorageCachePolicyService::destroy()
{
  int ret = OB_SUCCESS;
  if (is_user_tenant(MTL_ID())) {
    if (IS_INIT) {
      tenant_id_ = OB_INVALID_TENANT_ID;
      is_inited_ = false;
      is_stopped_ = false;
      is_triggered_ = false;
      local_schema_version_ = OB_INVALID_VERSION;
      schema_service_ = nullptr;
      tablet_scheduler_.destroy();
      refresh_policy_scheduler_.destroy();
      lock_.destroy();
      tablet_status_map_.destroy();
      part_policy_map_.destroy();
      table_policy_map_.destroy();
    }
  }
  LOG_INFO("storage cache policy service destroy success");
}
// check table schema version changed
int ObStorageCachePolicyService::check_table_schema_version_changed(const ObSimpleTableSchemaV2 *table_schema, 
                                                                    bool &table_schema_version_changed)
{
  int ret = OB_SUCCESS;
  table_schema_version_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema));
  } else {
    const int64_t table_id = table_schema->get_table_id();
    ObTablePolicyInfo ori_table_policy_info;
    int64_t ori_schema_version = 0;
    const int64_t new_schema_version = table_schema->get_schema_version();
    if (OB_FAIL(table_policy_map_.get_refactored(table_id, ori_table_policy_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        table_schema_version_changed = true;
      } else {
        LOG_WARN("fail to get table schema version", KR(ret), K(table_id));
      }
    } else if (FALSE_IT(ori_schema_version = ori_table_policy_info.get_table_schema_version())) {
    } else if (ori_schema_version < new_schema_version) {
      table_schema_version_changed = true;
    } else if (ori_schema_version == new_schema_version) {
      // skip when table schema version not changed
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("local table schema version should not be greater than current table schema version", 
          KR(ret), K(ori_schema_version), K(new_schema_version));
    }
  }
  return ret;
}



int ObStorageCachePolicyService::check_table_policy_need_recaculate(
    const ObSimpleTableSchemaV2 *table_schema,
    const ObStorageCachePolicyRefreshType &refresh_type,
    bool &table_policy_need_calc)
{
  int ret = OB_SUCCESS;
  table_policy_need_calc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(table_schema) || OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(table_schema), K_SCP_REFRESH_TYPE(refresh_type));
  } else {
    const int64_t table_id = table_schema->get_table_id();
    ObTablePolicyInfo ori_table_policy_info;
    ObTablePolicyInfo new_table_policy_info;
    int64_t ori_schema_version = 0;
    const int64_t new_schema_version = table_schema->get_schema_version();
    if (OB_FAIL(retrive_table_policy(table_schema, new_table_policy_info))) {
      LOG_WARN("fail to retrive table policy", KR(ret), K(table_schema));
    } else if (OB_FAIL(table_policy_map_.get_refactored(table_id, ori_table_policy_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(table_policy_map_.set_refactored(table_id, new_table_policy_info, true/*overwrite*/))) {
          LOG_WARN("fail to set table schema version", KR(ret), K(table_id), K(new_schema_version));
        } else {
          table_policy_need_calc = true;
        }
      } else {
        LOG_WARN("fail to get table schema version", KR(ret), K(table_id));
      }
    } else if (FALSE_IT(ori_schema_version = ori_table_policy_info.get_table_schema_version())){
    } else if (ori_schema_version < new_schema_version) {
      const ObStorageCachePolicy new_table_policy = new_table_policy_info.get_table_policy();
      const ObStorageCachePolicy ori_table_policy = ori_table_policy_info.get_table_policy();
      if (new_table_policy != ori_table_policy || !new_table_policy.is_global_policy()) {
        if (OB_FAIL(table_policy_map_.set_refactored(table_id, new_table_policy_info, true/*overwrite*/))) {
          LOG_WARN("fail to set table policy", KR(ret), K(table_id), K(new_table_policy));
        } else {
          table_policy_need_calc = true;
        }
      }
    } else if (ori_schema_version == new_schema_version) {
      // skip when table schema version not changed
    } else if (ori_schema_version > new_schema_version) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("local table schema version should not be greater than current table schema version", 
          KR(ret), K(ori_schema_version), K(new_schema_version));
    } 
  }

  if (OB_SUCC(ret)) {
    table_policy_need_calc |= is_need_refresh_type(refresh_type);
  }
  return ret;
}

int ObStorageCachePolicyService::check_part_schema_version_changed(const ObBasePartition *partition, 
                                                                   const int64_t part_id,
                                                                   bool &partition_schema_version_changed)
{
  int ret = OB_SUCCESS;
  partition_schema_version_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(partition) || OB_INVALID_PARTITION_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition), K(part_id));
  } else {
    ObTabletPolicyInfo ori_partition_policy_info;
    int64_t ori_schema_version = 0;
    const int64_t new_schema_version = partition->get_schema_version();
    if (OB_FAIL(part_policy_map_.get_refactored(part_id, ori_partition_policy_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        partition_schema_version_changed = true;
      } else {
        LOG_WARN("fail to get partition policy", KR(ret), K(part_id));
      }
    } else if (FALSE_IT(ori_schema_version = ori_partition_policy_info.get_tablet_schema_version())){
    } else if (ori_schema_version < new_schema_version) {
      partition_schema_version_changed = true;
    } else if (ori_schema_version == new_schema_version) {
      // skip 
    } else if (ori_schema_version > new_schema_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current schema version should be greater than the partition schema version", KR(ret), 
          K(part_id), K(partition->get_schema_version()), K(ori_partition_policy_info.get_tablet_schema_version()));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::check_part_policy_need_recaculate(const ObBasePartition *partition, 
                                                                   const int64_t part_id, 
                                                                   bool &partition_policy_need_calc)
{
  int ret = OB_SUCCESS;
  partition_policy_need_calc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(partition) || OB_INVALID_PARTITION_ID == part_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else {
    ObTabletPolicyInfo ori_partition_policy_info;
    int64_t ori_schema_version = 0;
    const int64_t new_schema_version = partition->get_schema_version();
    if (OB_FAIL(part_policy_map_.get_refactored(part_id, ori_partition_policy_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        partition_policy_need_calc = true;
      } else {
        LOG_WARN("fail to get partition policy", KR(ret), K(part_id));
      }
    } else if (FALSE_IT(ori_schema_version = ori_partition_policy_info.get_tablet_schema_version())){
    } else if (ori_schema_version < new_schema_version) {
      ObStorageCachePolicyType new_partition_policy;
      ObStorageCachePolicyType ori_partition_policy;
      new_partition_policy = partition->get_part_storage_cache_policy_type();
      ori_partition_policy = ori_partition_policy_info.get_tablet_policy();
      if (new_partition_policy != ori_partition_policy) {
        partition_policy_need_calc = true;
      }
    } else if (ori_schema_version == new_schema_version) {
      // skip
    } else if (ori_schema_version > new_schema_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current schema version should be greater than the partition schema version", KR(ret), 
          K(part_id), K(partition->get_schema_version()), K(ori_partition_policy_info.get_tablet_schema_version()));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_partition_policy_status(const ObPartition *partition, 
                                                             const ObStorageCachePolicy &table_policy, 
                                                             PolicyStatus &partition_policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType partition_policy;
  ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
  bool cur_enable_manual_storage_cache_policy = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else if (partition->get_part_storage_cache_policy_type() == ObStorageCachePolicyType::MAX_POLICY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition policy is empty", KR(ret), K(partition));
  } else if (OB_FAIL(get_default_storage_cache_policy(
      tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
    LOG_WARN("fail to get_default_storage_cache_policy", KR(ret), KPC(this));
  } else if (!cur_enable_manual_storage_cache_policy) {
    // disable manual storage_cache_policy, use default global storage cache policy
    if (OB_FAIL(ObStorageCacheGlobalPolicy::policy_type_to_status(
        cur_default_scp_type, partition_policy_status))) {
      LOG_WARN("fail to cal policy status", K(ret), K(cur_default_scp_type));
    }
  } else {
    partition_policy = partition->get_part_storage_cache_policy_type();
    switch (partition_policy) {
      case ObStorageCachePolicyType::HOT_POLICY: {
        partition_policy_status = PolicyStatus::HOT;
        break;
      }
      case ObStorageCachePolicyType::AUTO_POLICY: {
        partition_policy_status = PolicyStatus::AUTO;
        break;
      }
      case ObStorageCachePolicyType::NONE_POLICY: {
        if (OB_FAIL(cal_partition_policy_status_from_table(partition, table_policy, partition_policy_status))) {
          LOG_WARN("fail to cal partition policy status from table", KR(ret), K(table_policy));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(partition_policy));
        break;
      }
    }
  }
  LOG_TRACE("[SCP]cal partition policy status finish", KR(ret), K(partition_policy_status), KPC(partition));
  return ret;
}

int ObStorageCachePolicyService::get_table_schema(const uint64_t table_id,
                                                  const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("fail to get simple table schema", KR(ret), K(tenant_id_), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", KR(ret), K(table_id));
  }
  return ret;
}

int ObStorageCachePolicyService::cal_index_partition_policy_status(const ObBasePartition *partition, 
                                                                   PolicyStatus &index_partition_policy_status,
                                                                   const bool is_part)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_table_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  uint64_t index_table_id = OB_INVALID_ID;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition is null", KR(ret), K(partition));
  } else if (FALSE_IT(index_table_id = partition->get_table_id())) {
  } else if (OB_FAIL(get_table_schema(index_table_id, index_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(index_table_id));
  } else if (OB_ISNULL(index_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", KR(ret), K(index_table_id));
  } else if (!index_table_schema->is_index_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is not index table", KR(ret), K(index_table_id));
  } else {
    const uint64_t data_table_id = index_table_schema->get_data_table_id();
    PolicyStatus data_table_policy_status = PolicyStatus::MAX_STATUS;
    ObStorageCachePolicy data_table_policy;
    if (OB_FAIL(get_table_schema(data_table_id, data_table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(index_table_id), K(data_table_id), K_(tenant_id));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", KR(ret), K(data_table_id));
    } else if (OB_FAIL(data_table_policy.load_from_string(data_table_schema->get_storage_cache_policy()))) {
      LOG_WARN("fail to get table policy", KR(ret), K(data_table_id));
    } else if (OB_FAIL(cal_table_policy_status(data_table_policy, data_table_policy_status))) {
      LOG_WARN("fail to cal table policy status", KR(ret), K(data_table_policy), KPC(partition));
    } else if (PolicyStatus::MAX_STATUS == data_table_policy_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table policy status can not be max status", KR(ret), K(data_table_policy_status), K(data_table_policy), KPC(partition));
    } else {
      ObPartition **part_array = data_table_schema->get_part_array();
      const int64_t part_idx = partition->get_part_idx();
      ObPartition *data_partition = nullptr;
      int64_t data_part_id = OB_INVALID_ID;
      ObTabletPolicyInfo data_part_policy_info;
      if (OB_ISNULL(part_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part array is null", KR(ret), K(data_table_schema));
      } 
      if (OB_FAIL(ret)) {
      } else if (is_part) {
        // The policy status of partition of the index needs to follow the corresponding partition status in the data table.
        if (FALSE_IT(data_partition = part_array[part_idx])) {
        } else if (OB_ISNULL(data_partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data partition is null", KR(ret), K(data_partition), K(part_idx), KPC(partition));
        } else if (FALSE_IT(data_part_id = data_partition->get_part_id())) {
        } else if (OB_FAIL(part_policy_map_.get_refactored(data_part_id, data_part_policy_info))) {
          LOG_WARN("fail to get tablet status", KR(ret), K(data_part_id));
        } else {
          index_partition_policy_status = data_part_policy_info.get_tablet_policy_status();
          if (PolicyStatus::MAX_STATUS == index_partition_policy_status) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet policy status can not be max status", KR(ret), K(data_part_policy_info), KPC(partition), KPC(data_partition));
          }
        }
      } else { // !is_part
        int64_t part_idx = OB_INVALID_ID;
        int64_t subpart_idx = OB_INVALID_ID;
        ObObjectID tmp_object_id = OB_INVALID_ID;
        ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
        ObTabletID real_tablet_id;
        PolicyStatus subpart_policy_status = PolicyStatus::MAX_STATUS;
        // Since the tablet_id of the secondary partition must be legal, you can directly use the tablet_id to get part_idx here.
        if (OB_FAIL(index_table_schema->get_part_idx_by_tablet(partition->get_tablet_id(), part_idx, subpart_idx))) {
          LOG_WARN("fail to get part idx by tablet", KR(ret), KPC(partition));
        } else if (OB_FAIL(data_table_schema->get_part_id_and_tablet_id_by_idx(part_idx,
                                                                               subpart_idx,
                                                                               tmp_object_id,
                                                                               tmp_first_level_part_id,
                                                                               real_tablet_id))) {
          LOG_WARN("get part by idx failed", K(ret), K(part_idx), K(subpart_idx), KPC(data_table_schema));
        } else if (OB_FAIL(tablet_status_map_.get_refactored(real_tablet_id.id(), subpart_policy_status))) {
          // Since the tablet_id is obtained, the value is directly taken from tablet_status_map_, 
          // which is actually the same value as the policy_status in part_policy_map_.
          LOG_WARN("fail to get tablet status", KR(ret), K(real_tablet_id));
        } else if (PolicyStatus::MAX_STATUS == subpart_policy_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet policy status can not be max status", KR(ret), K(subpart_policy_status), K(real_tablet_id));
        } else {
          index_partition_policy_status = subpart_policy_status;
        }
      }
      LOG_TRACE("[SCP]cal index partition policy status", KR(ret), K(index_partition_policy_status), K(data_part_policy_info), K(partition->get_part_id()), K(data_part_id));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_partition_policy_status_from_table(const ObPartition *partition,
                                                                        const ObStorageCachePolicy &table_policy, 
                                                                        PolicyStatus &partition_policy_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(partition));
  } else if (table_policy.is_global_policy()) {
    if (ObStorageCacheGlobalPolicy::HOT_POLICY == table_policy.get_global_policy()) {
      partition_policy_status = PolicyStatus::HOT;
    } else if (ObStorageCacheGlobalPolicy::AUTO_POLICY == table_policy.get_global_policy()) { 
      partition_policy_status = PolicyStatus::AUTO;
    } else if (ObStorageCacheGlobalPolicy::NONE_POLICY == table_policy.get_global_policy()) {
      // The policy status of local index table can be NONE.
      if (OB_FAIL(cal_index_partition_policy_status(partition, partition_policy_status, true/*is_part*/))) {
        LOG_WARN("fail to cal index partition policy status", KR(ret), KPC(partition));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(table_policy.get_global_policy()));
    }
  } else if (table_policy.is_time_policy()) {
    // If the boundary column is a partition column of the first-level partition, then part_level is the first-level partition. 
    // The same is true for second-level partitions.
    if (share::schema::PARTITION_LEVEL_ONE == table_policy.get_part_level()) {
      const ObObj *tmp_obj = nullptr;
      tmp_obj = partition->get_high_bound_val().get_obj_ptr();
      if (OB_ISNULL(tmp_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition high bound value obj is null", KR(ret), KPC(partition));
      } else {
        ObObj &obj = const_cast<ObObj&>(tmp_obj[0]);
        if (OB_FAIL(cal_part_policy_status(obj, table_policy, partition_policy_status))) {
          LOG_WARN("fail to cal part policy status", KR(ret), K(obj), K(table_policy));
        }
      }
    } else if (share::schema::PARTITION_LEVEL_TWO == table_policy.get_part_level()) {
      partition_policy_status = PolicyStatus::NONE;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument when storage cache policy is time policy", KR(ret), K(table_policy.get_part_level()));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_sub_partition_policy_status(const ObSubPartition *sub_partition, 
                                                                 const PolicyStatus &partition_policy_status,
                                                                 const ObStorageCachePolicy &table_policy,
                                                                 PolicyStatus &sub_part_policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
  bool cur_enable_manual_storage_cache_policy = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_ISNULL(sub_partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sub_partition));
  } else if (sub_partition->get_part_storage_cache_policy_type() == ObStorageCachePolicyType::MAX_POLICY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub partition policy is empty", KR(ret), K(sub_partition));
  } else if (OB_FAIL(get_default_storage_cache_policy(
      tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
    LOG_WARN("fail to get_default_storage_cache_policy", KR(ret), KPC(this));
  } else if (!cur_enable_manual_storage_cache_policy) {
    // disable manual storage_cache_policy, use default global storage cache policy
    if (OB_FAIL(ObStorageCacheGlobalPolicy::policy_type_to_status(
        cur_default_scp_type, sub_part_policy_status))) {
      LOG_WARN("fail to cal policy status", K(ret), K(cur_default_scp_type));
    }
  } else {
    ObStorageCachePolicyType sub_part_policy;
    sub_part_policy = sub_partition->get_part_storage_cache_policy_type();
    if (table_policy.is_none_policy()) {
      // When table policy is NONE, it means that it must be a local index 
      // and the status value of the sub partition is calculated directly.
      if (OB_FAIL(cal_index_partition_policy_status(sub_partition, sub_part_policy_status, false/*is_part*/))) {
        LOG_WARN("fail to cal index partition policy status", KR(ret), KPC(sub_partition));
      }
    } else {
      switch (sub_part_policy) {
        case ObStorageCachePolicyType::HOT_POLICY: {
          sub_part_policy_status = PolicyStatus::HOT;
          break;
        }
        case ObStorageCachePolicyType::AUTO_POLICY: {
          sub_part_policy_status = PolicyStatus::AUTO;
          break;
        }
        case ObStorageCachePolicyType::NONE_POLICY: {
          switch (partition_policy_status) {
            case PolicyStatus::HOT: {
              sub_part_policy_status = PolicyStatus::HOT;
                break;
              }
              case PolicyStatus::AUTO: {  
                sub_part_policy_status = PolicyStatus::AUTO;
                break;
              }
              case PolicyStatus::NONE: {
                const ObObj *tmp_obj = nullptr;
                tmp_obj = sub_partition->get_high_bound_val().get_obj_ptr();
                if (OB_ISNULL(tmp_obj)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("partition high bound value obj is null", KR(ret), KPC(sub_partition));
                } else {
                  ObObj &obj = const_cast<ObObj&>(tmp_obj[0]);
                  if (OB_FAIL(cal_part_policy_status(obj, table_policy, sub_part_policy_status))) {
                    LOG_WARN("fail to cal part policy status", KR(ret), K(obj), K(table_policy));
                  }
                }
                break;
              }
              default: {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected partition policy status", KR(ret), K(partition_policy_status));
                break;
            }
          }
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(sub_part_policy));
          break;
        }
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_part_policy_status(const ObObj &obj, 
                                                        const ObStorageCachePolicy &table_policy,
                                                        PolicyStatus &part_policy_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else {
    int64_t partition_bound_val_int = 0;
    int32_t partition_bound_val_int32 = 0;
    uint8_t partition_bound_val_uint8 = 0;
    ObMySQLDate partition_bound_val_mysql_date;
    ObMySQLDateTime partition_bound_val_mysql_datetime;
    const int64_t current_time = ObTimeUtility::current_time(); // UTC time
    int64_t part_time = 0;
    int64_t hot_retention_usecs = 0;
    bool is_hot = false;

    if (OB_FAIL(table_policy.cal_hot_rentention_time(hot_retention_usecs))) {
      LOG_WARN("fail to cal hot retention time", KR(ret), K(table_policy));
    } else {
      ObTimeConvertCtx cvrt_ctx(NULL/*timezone info*/, true/*is_timestamp*/, false/*need_truncate*/);
      switch (obj.get_type()) {
        case ObInt32Type: {
          // When the partition boundary value is int32 type, the column boundary type must be int type
          partition_bound_val_int32 = obj.get_int32();
          if (table_policy.get_column_unit() == ObBoundaryColumnUnit::ColumnUnitType::SECOND) {
            // Due to the limitation of the number of bits, the int type can only represent timestamps at the second level.
            part_time = partition_bound_val_int32 * USECS_PER_SEC;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("When the boundary column type is int, only column units of level second are supported.", KR(ret), K(table_policy.get_column_unit()));
          }
          break;
        }
        case ObIntType: {
          // When the column boundary value is int type, the partition type may be ObIntType(aka bigint) if the partition boudary use function expr
          // e.g. unix_timestamp("2021-01-01 00:00:00") -> bigint
          // When the column boundary value is bigint type, the partition type must be ObIntType
          part_time = obj.get_int();
          if (table_policy.get_column_unit() == ObBoundaryColumnUnit::ColumnUnitType::SECOND) {
            part_time *= USECS_PER_SEC; // int type use second as unit, convert to usec
          } else if (table_policy.get_column_unit() == ObBoundaryColumnUnit::ColumnUnitType::MILLISECOND) {
            part_time *= USECS_PER_MSEC; // int type use millisecond as unit, convert to usec
          } else if (table_policy.get_column_unit () == ObBoundaryColumnUnit::ColumnUnitType::MICROSECOND) {
            // int type use microsecond as unit, no need to convert
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", KR(ret), K(table_policy.get_column_unit()));
          }
          break;
        }
        case ObYearType: {
          partition_bound_val_uint8 = obj.get_year();
          int64_t part_year_val = 0;
          if (OB_FAIL(ObTimeConverter::year_to_int(partition_bound_val_uint8, part_year_val))) {
            LOG_WARN("fail to convert year to int", KR(ret), K(partition_bound_val_uint8));
          } else {
            const int64_t month = 1;
            const int64_t day = 1;
            const int64_t year_date = ObTimeConverter::calc_date(part_year_val, month, day);
            if (OB_FAIL(ObTimeConverter::date_to_datetime(year_date, cvrt_ctx, part_time))) {
              LOG_WARN("fail to convert date to datetime", KR(ret), K(year_date));
            }
          }
          break;
        }
        case ObDateType: {
          partition_bound_val_int32 = obj.get_date();
          if (OB_FAIL(ObTimeConverter::date_to_datetime(partition_bound_val_int32, cvrt_ctx, part_time))) {
            LOG_WARN("fail to convert date to datetime", KR(ret), K(partition_bound_val_int32));
          }
          break;
        }
        // not support time type
        // todo(baonian.wcx): modify check to not support this type in resolver
        case ObTimeType: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("storage cache policy not supported time type", KR(ret), K(partition_bound_val_int));
          break;
        }
        case ObMySQLDateType: {
          partition_bound_val_mysql_date = obj.get_mysql_date();
          ObDateSqlMode date_sql_mode;
          if (OB_FAIL(ObTimeConverter::mdate_to_datetime(partition_bound_val_mysql_date,
                cvrt_ctx, part_time, date_sql_mode))) {
            LOG_WARN("fail to convert mysql date to datetime", KR(ret), K(partition_bound_val_mysql_date));
          }
          break;
        }
        case ObMySQLDateTimeType: {
          partition_bound_val_mysql_datetime = obj.get_mysql_datetime();
          ObDateSqlMode date_sql_mode;
          if (OB_FAIL(ObTimeConverter::mdatetime_to_datetime(partition_bound_val_mysql_datetime,
              part_time, date_sql_mode))) {
            LOG_WARN("fail to convert mysql datetime to datetime", KR(ret), K(partition_bound_val_mysql_datetime));
          }
          break;
        }
        case ObTimestampType:
        case ObDateTimeType: {
          part_time = obj.get_timestamp();
          break;
        }
        case ObExtendType: {
          // Max or min type for partition, the partition must be hot
          // In partitioned table, only MAXVALUE partition will be this type
          is_hot = true;
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(obj.get_type()));
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_hot = (is_hot || ((part_time + hot_retention_usecs) > current_time));
      part_policy_status = (is_hot ? PolicyStatus::HOT : PolicyStatus::AUTO);
    }
    LOG_TRACE("[SCP]cal part policy status finish", KR(ret), K(part_policy_status), 
        K(part_time), K(current_time), K(hot_retention_usecs), K(is_hot), K(obj.get_type()));
  }
  return ret;
}

int ObStorageCachePolicyService::cal_index_table_policy_status(const ObSimpleTableSchemaV2 *simple_table_schema,
                                                               PolicyStatus &index_table_policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
  bool cur_enable_manual_storage_cache_policy = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_FAIL(get_default_storage_cache_policy(
      tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
    LOG_WARN("fail to get_default_storage_cache_policy", KR(ret), KPC(this));
  } else if (!cur_enable_manual_storage_cache_policy) {
    // disable manual storage_cache_policy, use default global storage cache policy
    if (OB_FAIL(ObStorageCacheGlobalPolicy::policy_type_to_status(
        cur_default_scp_type, index_table_policy_status))) {
      LOG_WARN("fail to cal policy status", K(ret), K(cur_default_scp_type));
    }
  } else { // cur_enable_manual_storage_cache_policy
    if (OB_ISNULL(simple_table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(simple_table_schema));
    } else if (!simple_table_schema->is_index_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is not index table", KR(ret), KPC(simple_table_schema));
    } else {
      const ObTableSchema *data_table_schema = nullptr;
      ObStorageCachePolicy data_table_policy;
      PolicyStatus data_table_status = PolicyStatus::MAX_STATUS;
      const uint64_t data_table_id = simple_table_schema->get_data_table_id();
      if (OB_FAIL(get_table_schema(data_table_id, data_table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(simple_table_schema->get_table_id()), K(data_table_id), K_(tenant_id));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret), K(data_table_id));
      } else if (OB_FAIL(data_table_policy.load_from_string(data_table_schema->get_storage_cache_policy()))) {
        LOG_WARN("fail to get table policy", KR(ret), K(data_table_id));
      } else if (OB_FAIL(cal_table_policy_status(data_table_policy, data_table_status))) {
        LOG_WARN("fail to cal table policy status", KR(ret), K(data_table_policy), KPC(simple_table_schema));
      } else if ((PolicyStatus::MAX_STATUS == data_table_status) || 
                 (PolicyStatus::NONE == data_table_status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table policy status can not be max or none status", KR(ret), K(data_table_policy), KPC(simple_table_schema));
      } else {
        index_table_policy_status = data_table_status;
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::cal_table_policy_status(const ObStorageCachePolicy &table_policy, 
                                                         PolicyStatus &table_policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyType cur_default_scp_type = ObStorageCachePolicyType::MAX_POLICY;
  bool cur_enable_manual_storage_cache_policy = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_FAIL(get_default_storage_cache_policy(
      tenant_id_, cur_enable_manual_storage_cache_policy, cur_default_scp_type))) {
    LOG_WARN("fail to get_default_storage_cache_policy", KR(ret), KPC(this));
  } else if (!cur_enable_manual_storage_cache_policy) {
    // disable manual storage_cache_policy, use default global storage cache policy
    if (OB_FAIL(ObStorageCacheGlobalPolicy::policy_type_to_status(
        cur_default_scp_type, table_policy_status))) {
      LOG_WARN("fail to cal policy status", K(ret), K(cur_default_scp_type));
    }
  } else {
    if (table_policy.is_global_policy()) {
      switch (table_policy.get_global_policy()) {
        case ObStorageCacheGlobalPolicy::HOT_POLICY: {
          table_policy_status = PolicyStatus::HOT;
          break;
        }
        case ObStorageCacheGlobalPolicy::AUTO_POLICY: {
          table_policy_status = PolicyStatus::AUTO;
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected global policy", K(ret), K(table_policy.get_global_policy()));
          break;
        }
      }
    } else if (table_policy.is_time_policy()) {
      // todo(baonian.wcx): maybe we would remove status of table later
      table_policy_status = PolicyStatus::NONE;
    }
  }

  LOG_TRACE("[SCP]finish handle non-partitioned table", KR(ret),
      K(table_policy), KPC(this), K(cur_default_scp_type), K(cur_enable_manual_storage_cache_policy));
  return ret;
}


int ObStorageCachePolicyService::tenant_need_refresh(bool &need_refresh)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  need_refresh = false;
  const ObTenantSchema *tenant_schema = nullptr;
  int64_t schema_version = 0;
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K_(tenant_id));
  } else if (tenant_schema->is_oracle_tenant()) {
    // oracle tenant, no need to refresh
    need_refresh = false;
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret), K_(tenant_id));
  } else if (!ObSchemaService::is_formal_version(schema_version)) {
    ret = OB_EAGAIN;
    LOG_INFO("is not a formal_schema_version", KR(ret), K(schema_version));
  } else if (local_schema_version_ == OB_INVALID_VERSION ||  local_schema_version_ < schema_version) {
    LOG_TRACE("schema changed", KR(ret), K_(local_schema_version), K(schema_version)); 
    local_schema_version_ = schema_version;
    need_refresh = true;
  }
  return ret;
}


int ObStorageCachePolicyService::get_tablet_ls_id(const uint64_t tablet_id, int64_t &ls_id) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_UNLIKELY(tablet_id == common::ObTabletID::INVALID_TABLET_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    common::ObSEArray<share::ObLSID, DEFAULT_LS_SIZE> ls_ids;
    if (OB_FAIL(MTL(ObLSService *)->get_ls_ids(ls_ids))) {
      LOG_WARN("fail to get ls ids", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
        ObLSTabletService *tablet_svr = nullptr;
        ObLSHandle ls_handle;
        if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_ids[i], ls_handle, ObLSGetMod::STORAGE_CACHE_POLICY_MOD))) {
          LOG_WARN("failed to get ls", KR(ret), K(ls_ids[i].id()));
        } else if (OB_ISNULL(tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get ls", KR(ret), K(ls_ids[i].id()));
        } else if (OB_FAIL(tablet_svr->is_tablet_exist(ObTabletID(tablet_id), is_exist))) {
          LOG_WARN("fail to judge is tablet exist", KR(ret), K(tablet_id), K(ls_ids[i].id()));
        } else if (is_exist) {
          ls_id = ls_ids[i].id();
          break;
        }
      }
    }
  }
  return ret;
}

int ObStorageCachePolicyService::retrive_table_policy(const ObSimpleTableSchemaV2 *simple_table_schema,
                                                      ObTablePolicyInfo &table_policy_info)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicy table_policy;
  int64_t table_id = simple_table_schema->get_table_id();
  int32_t part_level = share::schema::PARTITION_LEVEL_MAX;

  if (OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(simple_table_schema));
  } else if (table_policy_info.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table policy info is inited", KR(ret), K(table_policy_info));
  } else {
    ObStorageCachePolicyType table_policy_type;
    table_policy_type = simple_table_schema->get_storage_cache_policy_type();
    if (table_policy_type == ObStorageCachePolicyType::TIME_POLICY) {
      // If the table policy is time policy, we need to get the table schema from the full table schema
      const ObTableSchema *full_table_schema = nullptr;
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard, local_schema_version_))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, full_table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(table_id));
      } else if (OB_ISNULL(full_table_schema)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("table schema is null", KR(ret), K(table_id));
      } else if (OB_FAIL(table_policy.load_from_string(full_table_schema->get_storage_cache_policy()))) {
        LOG_WARN("fail to load table policy from string", KR(ret), K(table_id), K(full_table_schema->get_storage_cache_policy()));
      } else if (OB_FAIL(sql::ObStorageCacheUtil::get_range_part_level(*full_table_schema, table_policy, part_level))) {
        LOG_WARN("fail to get range part level", KR(ret), K(table_policy));
      } else if (FALSE_IT(table_policy.set_part_level(part_level))) {
        LOG_WARN("fail to set part level", KR(ret), K(table_policy));
      }
    } else if (table_policy_type == ObStorageCachePolicyType::HOT_POLICY ||
               table_policy_type == ObStorageCachePolicyType::AUTO_POLICY ||
               (table_policy_type == ObStorageCachePolicyType::NONE_POLICY && 
               ObTableType::USER_INDEX == simple_table_schema->get_table_type())) {
      table_policy.set_global_policy(table_policy_type);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table policy", KR(ret), KPC(simple_table_schema), K(table_id), K(table_policy));
    }
  }
  if (OB_SUCC(ret)) {
    if (!simple_table_schema->is_partitioned_table() && !table_policy.is_global_policy()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table policy", KR(ret), K(simple_table_schema));
    } else if (OB_FAIL(table_policy_info.init(table_id, table_policy, simple_table_schema->get_schema_version()))) {
      LOG_WARN("fail to init table policy info", KR(ret), K(table_id), K(table_policy), K(simple_table_schema->get_schema_version()));
    }
  }
  return ret;
}

int ObStorageCachePolicyService::check_and_generate_tablet_tasks(
    const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObStorageCachePolicyService::check_and_generate_tablet_tasks", POLICY_SCHEMA_REFRESH_TIME_THRESHOLD);
  ObArray<uint64_t> table_id_array;
  ObSchemaGetterGuard schema_guard;
  int64_t task_num = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache policy mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid refresh_type", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
  } else if (OB_FAIL(schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                tenant_id_, schema_guard, local_schema_version_))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id_, table_id_array))) {
    LOG_WARN("fail to get table ids in tenant", KR(ret), K(tenant_id_));
  } else {
    int64_t idx = 0;
    bool skip_table = false;
    time_guard.click("start_check_and_generate_tablet_tasks");
    // TODO(baonian.wcx): need to add the judgement of enable_manual_storage_cache_policy configuration item
    while (OB_SUCC(ret) && idx < table_id_array.count()) {
      // temp schema guard to loop table ids
      bool table_policy_need_calc = false;
      skip_table = false;
      const int64_t table_id = table_id_array.at(idx);
      if (table_id < OB_MIN_USER_OBJECT_ID) {
        // Skip non-user table
        skip_table = true;
      } else {
        const ObSimpleTableSchemaV2 *simple_table_schema = nullptr;
        bool table_schema_version_changed = false;
        ObTablePolicyInfo table_policy_info;
        if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, simple_table_schema))) {
          LOG_WARN("failed to get simple schema", KR(ret), K(table_id));
        } else if (OB_ISNULL(simple_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table schema is null", KR(ret), K(table_id), K_(tenant_id));
        } else if (!(simple_table_schema->is_user_table() || simple_table_schema->is_index_table())) {
          skip_table = true;
        } else if (OB_FAIL(check_table_schema_version_changed(simple_table_schema, table_schema_version_changed))) {
          LOG_WARN("fail to check table schema version", KR(ret), K(table_id));
        } else if (OB_FAIL(retrive_table_policy(simple_table_schema, table_policy_info))) {
          LOG_WARN("fail to retrive table policy", KR(ret), K(table_id));
        } else if (OB_FAIL(check_table_policy_need_recaculate(
            simple_table_schema, refresh_type, table_policy_need_calc))) {
          LOG_WARN("fail to check table policy need recaculate", 
              KR(ret), K(table_id), K_SCP_REFRESH_TYPE(refresh_type));
        } else if (!(table_schema_version_changed || table_policy_need_calc)) {
          // If the table schema version are not changed and the  table policy do not need to be recaculated, 
          // skip the table
          skip_table = true;
        } 
        // Situation 1: table schema version did not change, and table policy do not need to be recaculated
        // (this situation has been skipped in the above code)
        // Situation 2: table schema version changed, but table policy do not need to be recaculated
        // (need to check the partition status or sub partition status)
        // Situation 3: table schema version did not change, but table policy need to be recaculated
        // (table policy type is time policy and the table is exist in table_policy_map)
        // Situation 4: table schema version changed, and table policy need to be recaculated
        // Situation 2, 3, and 4 all need to scan all partitions and sub partitions
        if (!skip_table) {
          if (OB_SUCC(ret) && (table_schema_version_changed || table_policy_need_calc)) {
            const ObStorageCachePolicy table_policy = table_policy_info.get_table_policy();
            // Handle the partitioned table
            // table_schema_version_changed or table_policy_need_calc is true 
            if (simple_table_schema->is_partitioned_table()) {
              if (OB_FAIL(handle_partitioned_table(
                  simple_table_schema, table_policy, table_policy_need_calc, refresh_type))) {
                LOG_WARN("fail to handle partitioned table", KR(ret),
                    K(simple_table_schema), K_SCP_REFRESH_TYPE(refresh_type));
              }
            } else if (OB_FAIL(handle_non_partitioned_table(simple_table_schema, table_policy_info, table_policy_need_calc,
                          table_schema_version_changed, refresh_type))) {
              LOG_WARN("fail to handle non-partitioned table", KR(ret),
                  K(simple_table_schema), K_SCP_REFRESH_TYPE(refresh_type));
            }
            task_num++;
          } // do nothing if the table schema version not changed
          if (OB_FAIL(ret)) {
            LOG_WARN("fail to check and generate tablet tasks for table", KR(ret), K(table_id_array[idx]), K_(tenant_id), 
                KPC(simple_table_schema), K_SCP_REFRESH_TYPE(refresh_type), K(table_schema_version_changed), K(table_policy_need_calc), K(table_policy_info), K(lbt()));
            // When a single table fails, only an error is reported and processing of other tables continues
            ret = OB_SUCCESS;
          }
        } // end of iterate tables
      }
      idx++;
    }
    time_guard.click("finish_check_and_generate_tablet_tasks");
  }
  LOG_TRACE("finish check and generate tablet tasks", KR(ret), K_(tenant_id), K(time_guard), K(task_num));
  return ret;
}

int ObStorageCachePolicyService::handle_non_partitioned_table(
    const ObSimpleTableSchemaV2 *simple_table_schema,
    const ObTablePolicyInfo &table_policy_info,
    const bool &table_policy_need_calc,
    const bool &table_schema_version_changed,
    const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  const int64_t table_id = simple_table_schema->get_table_id();
  const ObStorageCachePolicy table_policy = table_policy_info.get_table_policy();
  if (OB_ISNULL(simple_table_schema) || OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(simple_table_schema), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (table_policy_need_calc) {
    // Handle the non-partitioned table
    // If the table policy need to be recaculated, 
    // need to generate a tablet task to tablet_scheduler_
    // and update the table policy info in table_policy_map
    int64_t ls_id = 0;
    PolicyStatus table_policy_status;
    bool tablet_status_changed = false;
    int64_t tablet_id = simple_table_schema->get_tablet_id().id();
    if (OB_FAIL(get_tablet_ls_id(tablet_id, ls_id))) {
      LOG_WARN("fail to get tablet ls id", KR(ret), K(tablet_id));
    } else if (table_policy.is_none_policy() && simple_table_schema->is_index_table()) {
      if (OB_FAIL(cal_index_table_policy_status(simple_table_schema, table_policy_status))) {
        LOG_WARN("fail to cal index table policy status", KR(ret), K(tablet_id), K(table_policy));
      }
    } else if (OB_FAIL(cal_table_policy_status(table_policy, table_policy_status))) {
      LOG_WARN("fail to cal table policy status", KR(ret), K(table_policy));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_tablet_status_changed(
        tablet_id, table_policy_status, refresh_type, tablet_status_changed))) {
      LOG_WARN("fail to check tablet status changed", KR(ret),
          K(tablet_id), K(table_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
    } else if (tablet_status_changed
        && OB_FAIL(tablet_scheduler_.push_task(tenant_id_, ls_id, tablet_id, table_policy_status))) {
      LOG_WARN("fail to push task", KR(ret), K(tablet_id), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
    } else if (tablet_status_changed
        && OB_FAIL(update_tablet_status(tablet_id, table_policy_status))) {
      LOG_WARN("fail to update tablet status", KR(ret), K(tablet_id), K(table_policy_status));
    } else if (OB_FAIL(table_policy_map_.set_refactored(
        table_id, table_policy_info, true/*overwrite*/))) {
      LOG_WARN("fail to set table policy", KR(ret), K(table_id));
    }
  } else if (table_schema_version_changed) {
    // If the table schema version changed, but no need to recaculate the table policy
    // Only update the table schema version in table_policy_map
    ObTablePolicyInfo ori_table_policy_info;
    if (OB_FAIL(table_policy_map_.get_refactored(table_id, ori_table_policy_info))) {
      LOG_WARN("fail to get table policy", KR(ret), K(table_id));
    } else if (OB_FAIL(ori_table_policy_info.set_table_schema_version(simple_table_schema->get_schema_version()))) {
      LOG_WARN("fail to set table schema version", KR(ret), K(table_id));
    } else if (OB_FAIL(table_policy_map_.set_refactored(table_id, ori_table_policy_info, true/*overwrite*/))) {
      LOG_WARN("fail to set table policy", KR(ret), K(table_id));
    }
  }
  LOG_TRACE("[SCP]finish handle non-partitioned table", KR(ret),
      K(table_id), K(simple_table_schema), K_SCP_REFRESH_TYPE(refresh_type));
  return ret;
}

int ObStorageCachePolicyService::handle_partitioned_table(const ObSimpleTableSchemaV2 *table_schema, 
                                                          const ObStorageCachePolicy &table_policy,
                                                          const bool &table_policy_need_calc,
                                                          const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  ObPartition **part_array = nullptr;

  if (OB_ISNULL(table_schema) || OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (FALSE_IT(part_array = table_schema->get_part_array())) {
  } else if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part array is null", KR(ret), K(table_schema));
  } else {
    ObPartitionLevel table_part_level = table_schema->get_part_level();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_part_option().get_part_num(); ++i) {
      PolicyStatus partition_policy_status = PolicyStatus::MAX_STATUS;
      bool partition_policy_need_calc = false;
      ObPartition *partition = part_array[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", KR(ret), K(table_schema), K(i), K(table_schema->get_part_option().get_part_num()));
      } else if (OB_FAIL(check_part_policy_need_recaculate(partition, partition->get_part_id(), partition_policy_need_calc))) {
        LOG_WARN("fail to check partition policy need recaculate", KR(ret), K(partition), K(table_policy));  
      } else if (FALSE_IT(partition_policy_need_calc = partition_policy_need_calc || table_policy_need_calc)) {
      } else if (OB_FAIL(handle_partition(
          partition, table_policy, partition_policy_need_calc,
          table_part_level, refresh_type, partition_policy_status))) {
        LOG_WARN("failed to handle partition", KR(ret), KPC(partition), K(table_policy), K(partition_policy_need_calc),
            K(table_part_level), K(partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
      }
      LOG_TRACE("[SCP]finish handle partition in handle partitioned table", KR(ret), K(table_schema->get_table_id()), K(i), K(partition_policy_need_calc), K(partition_policy_status));
      // scan level two partitions
      if (OB_FAIL(ret)) {
      } else if (table_part_level == PARTITION_LEVEL_TWO) {
        // If partition level is two, and partition policy did not recaculate,
        // we need to calculate the partition policy status for sub partitions
        if (!partition_policy_need_calc) {
          if (OB_FAIL(cal_partition_policy_status(partition, table_policy, partition_policy_status))) {
            LOG_WARN("fail to cal and update partition policy status", KR(ret), K(partition), K(table_policy));
          } else if (PolicyStatus::MAX_STATUS == partition_policy_status) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected partition policy status", KR(ret), K(partition_policy_status));
          }
        } 
        if (OB_FAIL(ret)) {
        } else {
          ObSubPartition **sub_part_array = partition->get_subpart_array();
          bool sub_partition_policy_need_calc = false;
          if (OB_ISNULL(sub_part_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub part array is null", KR(ret), K(table_schema), K(partition));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < partition->get_sub_part_num(); ++j) {
              ObSubPartition *sub_partition = sub_part_array[j];
              if (OB_ISNULL(sub_partition)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("sub partition is null", KR(ret), K(table_schema), K(partition), K(j));
              } else if (OB_FAIL(check_part_policy_need_recaculate(sub_partition, sub_partition->get_sub_part_id(), sub_partition_policy_need_calc))) {
                LOG_WARN("fail to check sub partition policy need recaculate", KR(ret), K(sub_partition));
              } else if (FALSE_IT(sub_partition_policy_need_calc = sub_partition_policy_need_calc || partition_policy_need_calc)) {
              } else if (OB_FAIL(handle_subpartitions(
                  sub_partition, table_policy, partition_policy_status,
                  refresh_type, sub_partition_policy_need_calc))) {
                LOG_WARN("failed to handle subpartition", KR(ret),
                    KPC(sub_partition), KPC(partition), K(partition_policy_status),
                    K(table_policy), K_SCP_REFRESH_TYPE(refresh_type));
              }
              LOG_TRACE("[SCP]finish handle sub partitions in handle partitioned table", KR(ret), 
                  K(table_schema->get_table_id()), K(i), K(j), K(sub_partition_policy_need_calc),
                  K(partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
            } // end of iterate sub partitions
          }
        }
      }
    }
  }
  
  LOG_TRACE("[SCP]finish handle partitioned table", KR(ret), K(table_schema->get_table_id()),
      K(table_policy), K(table_policy_need_calc), K_SCP_REFRESH_TYPE(refresh_type));
  return ret;
}

int ObStorageCachePolicyService::handle_partition(const ObPartition *partition, 
                                                  const ObStorageCachePolicy table_policy,
                                                  const bool &partition_policy_need_calc,
                                                  const ObPartitionLevel &table_part_level,
                                                  const ObStorageCachePolicyRefreshType &refresh_type,
                                                  PolicyStatus &partition_policy_status)
{
  int ret = OB_SUCCESS;
  bool partition_schema_version_changed = false;
  ObTabletPolicyInfo tablet_policy_info;
  if (OB_ISNULL(partition) || OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(partition), K(table_policy), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (PARTITION_LEVEL_MAX == table_part_level || PARTITION_LEVEL_ZERO == table_part_level) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_part_level), KPC(partition), K(table_policy));
  } else if (OB_FAIL(check_part_schema_version_changed(partition, partition->get_part_id(), partition_schema_version_changed))) {
    LOG_WARN("fail to check partition schema version changed", KR(ret), K(partition), K(table_policy));
  }
  // Scan level one partitions
  // Situation 1: no need to recalculate partition, and partition schema version did not change
  // (Skip, do nothing)
  // Situation 2: no need to recalculate partition, but partition schema version changed
  // (Only update the related map)
  // Situation 3: need to recalculate partition, and partition schema version changed
  // (Recalculate partition status. If the status changed, update the related maps
  // and generate new tablet_task if it is level-one partitioned table
  // Situation 4: need to recalculate partition, but partition schema version did not change
  // (Time storage cache policy. Same operation to situation 3)
  if (OB_FAIL(ret)) {
  } else if (partition_policy_need_calc) {
    // Need to recaculate tablet policy status
    // Calculate partition policy status
    if (OB_FAIL(cal_partition_policy_status(partition, table_policy, partition_policy_status))) {
      LOG_WARN("fail to cal and update partition policy status", KR(ret), K(partition), K(table_policy));
    } else if (PolicyStatus::MAX_STATUS == partition_policy_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition policy status", KR(ret), K(partition_policy_status), KPC(partition));
    } else if (table_part_level == PARTITION_LEVEL_ONE && PolicyStatus::NONE == partition_policy_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition policy status is time to live", KR(ret), K(partition), K(table_policy));
    } else if (table_part_level == PARTITION_LEVEL_ONE) {
      // Only the level one partition need to generate tablet task
      // Update the tablet status map and part policy map after the tablet task is generated
      int64_t ls_id = 0;
      int64_t tablet_id = partition->get_tablet_id().id();
      bool tablet_status_changed = false;
      if (OB_FAIL(get_tablet_ls_id(tablet_id, ls_id))) {
        LOG_WARN("fail to get tablet ls id", KR(ret), K(tablet_id));
      } else if (OB_FAIL(check_tablet_status_changed(
          tablet_id, partition_policy_status, refresh_type, tablet_status_changed))) {
        LOG_WARN("fail to check tablet status changed", KR(ret),
            K(tablet_id), K(partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
      } else if (tablet_status_changed && OB_FAIL(tablet_scheduler_.push_task(tenant_id_, ls_id, tablet_id, partition_policy_status))) {
        LOG_WARN("fail to push task", KR(ret), K(tablet_id), K(partition_policy_status));
      } else if (tablet_status_changed && OB_FAIL(update_tablet_status(tablet_id, partition_policy_status))) {
        LOG_WARN("fail to update tablet status", KR(ret), K(tablet_id), K(partition_policy_status));
      }
      LOG_TRACE("[SCP]finish cal partition", KR(ret), K(partition_policy_status), K(tablet_status_changed), K(tablet_policy_info), K(tablet_id), K(ls_id));
    }
    // Regardless of whether the table is a primary partition table or a secondary partition table, 
    // the status of its primary partition is stored in the part_policy_map_.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_policy_info.init(partition))) {
      LOG_WARN("fail to init partition policy info", KR(ret), K(partition));
    } else {
      tablet_policy_info.set_tablet_policy_status(partition_policy_status);
    }
  } else if (partition_schema_version_changed) {
    // No need to recaculate tablet policy status, but schema version changed
    // Only update part_policy_map_, not update tablet_status_map_
    if (OB_FAIL(part_policy_map_.get_refactored(partition->get_part_id(), tablet_policy_info))) {
      LOG_WARN("fail to get tablet policy", KR(ret), K(partition->get_part_id()), K(partition));
    } else {
      tablet_policy_info.set_tablet_schema_version(partition->get_schema_version());
    }
  }
  if (partition_policy_need_calc || partition_schema_version_changed) {
    LOG_TRACE("[SCP]partition policy need recaculate or schema version changed", KR(ret),
        K(partition_policy_need_calc), K(partition_schema_version_changed), K(partition->get_part_id()), K(tablet_policy_info));
    if (FAILEDx(part_policy_map_.set_refactored(partition->get_part_id(), tablet_policy_info, true/*overwrite*/))) {
      LOG_WARN("fail to set tablet policy", KR(ret), K(tablet_policy_info), K(partition));
    }
  }
  LOG_TRACE("[SCP]finish handle partition", KR(ret), KPC(partition));
  return ret;
}

int ObStorageCachePolicyService::handle_subpartitions(const ObSubPartition *sub_partition, 
                                                      const ObStorageCachePolicy &table_policy,
                                                      const PolicyStatus &partition_policy_status, 
                                                      const ObStorageCachePolicyRefreshType &refresh_type,
                                                      const bool &sub_partition_policy_need_calc)
{
  int ret = OB_SUCCESS;
  ObTabletPolicyInfo tablet_policy_info;
  if (OB_ISNULL(sub_partition)
      || OB_UNLIKELY(PolicyStatus::MAX_STATUS == partition_policy_status
          || !is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(sub_partition), K(partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
  } else {
    PolicyStatus sub_partition_policy_status = PolicyStatus::MAX_STATUS;
    bool sub_partition_schema_version_changed = false;
    int64_t ls_id = 0;
    int64_t tablet_id = sub_partition->get_tablet_id().id();
    bool tablet_status_changed = false;
    if (OB_FAIL(check_part_schema_version_changed(sub_partition, sub_partition->get_sub_part_id(), sub_partition_schema_version_changed))) {
      LOG_WARN("fail to check sub partition schema version changed", KR(ret), K(sub_partition));
    } else if (sub_partition_policy_need_calc) {
      if (OB_FAIL(cal_sub_partition_policy_status(sub_partition, partition_policy_status, table_policy, sub_partition_policy_status))) {
        LOG_WARN("fail to calculate sub partition policy status", KR(ret), K(partition_policy_status), K(sub_partition));
      } else if (PolicyStatus::NONE == sub_partition_policy_status || PolicyStatus::MAX_STATUS == sub_partition_policy_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected sub partition policy status", KR(ret), K(sub_partition_policy_status));
      } else if (OB_FAIL(get_tablet_ls_id(tablet_id, ls_id))) {
        LOG_WARN("fail to get tablet ls id", KR(ret), K(tablet_id));
      } else if (OB_FAIL(check_tablet_status_changed(
          tablet_id, sub_partition_policy_status, refresh_type, tablet_status_changed))) {
        LOG_WARN("fail to check tablet status changed", KR(ret),
            K(tablet_id), K(sub_partition_policy_status), K_SCP_REFRESH_TYPE(refresh_type));
      } else if (tablet_status_changed && OB_FAIL(tablet_scheduler_.push_task(tenant_id_, ls_id, tablet_id, sub_partition_policy_status))) {
        LOG_WARN("fail to push task", KR(ret), K(tablet_id), K(sub_partition_policy_status));
      } else if (tablet_status_changed && OB_FAIL(update_tablet_status(tablet_id, sub_partition_policy_status))) {
        LOG_WARN("fail to update tablet status", KR(ret), K(tablet_id), K(sub_partition_policy_status));
      } else if (OB_FAIL(tablet_policy_info.init(sub_partition))) {
        LOG_WARN("fail to init sub partition policy info", KR(ret), K(sub_partition));
      } else {
        tablet_policy_info.set_tablet_policy_status(sub_partition_policy_status);
      }
      LOG_TRACE("[SCP]finish cal subpartition", KR(ret), K(sub_partition_policy_status), K(tablet_status_changed), K(tablet_id), K(ls_id));
    } else if (sub_partition_schema_version_changed) {
      if (OB_FAIL(part_policy_map_.get_refactored(sub_partition->get_sub_part_id(), tablet_policy_info))) {
        LOG_WARN("fail to get tablet policy", KR(ret), K(sub_partition->get_sub_part_id()), K(sub_partition));
      } else {
        tablet_policy_info.set_tablet_schema_version(sub_partition->get_schema_version());
      }
    }
    if (sub_partition_policy_need_calc || sub_partition_schema_version_changed) {
      if (FAILEDx(part_policy_map_.set_refactored(sub_partition->get_sub_part_id(), tablet_policy_info, true/*overwrite*/))) {
        LOG_WARN("fail to set tablet policy", KR(ret), K(tablet_policy_info), K(sub_partition));
      }
    }
    LOG_TRACE("[SCP]finish handle subpartition", KR(ret), K(tablet_policy_info), K(sub_partition), 
        K(sub_partition_policy_status), K(sub_partition_schema_version_changed), K(sub_partition_policy_need_calc));
  }
  return ret;
}

int ObStorageCachePolicyService::check_tablet_status_changed(const uint64_t tablet_id, 
                                                             const PolicyStatus new_tablet_status,
                                                             const ObStorageCachePolicyRefreshType &refresh_type,
                                                             bool &tablet_status_changed)
{
  int ret = OB_SUCCESS;
  PolicyStatus ori_tablet_status;
  tablet_status_changed = false;
  if (OB_UNLIKELY(common::ObTabletID::INVALID_TABLET_ID == tablet_id
      || !is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id or refresh_type", KR(ret),
        K(tablet_id), K(new_tablet_status), K_SCP_REFRESH_TYPE(refresh_type));
  } else if (OB_FAIL(tablet_status_map_.get_refactored(tablet_id, ori_tablet_status))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      tablet_status_changed = true;
    } else {
      LOG_WARN("fail to get tablet status", KR(ret), K(tablet_id));
    }
  } else if (ori_tablet_status != new_tablet_status) {
    tablet_status_changed = true;
  } else if (ObStorageCachePolicyRefreshType::REFRESH_TYPE_FORCE_REFRESH == refresh_type) {
    tablet_status_changed = true;
  }
  return ret;
}

int ObStorageCachePolicyService::update_tablet_status(const uint64_t tablet_id, const PolicyStatus new_tablet_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(common::ObTabletID::INVALID_TABLET_ID == tablet_id || 
                  PolicyStatus::MAX_STATUS == new_tablet_status ||
                  PolicyStatus::NONE == new_tablet_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id or new_tablet_status", KR(ret), K(tablet_id), K(new_tablet_status));
  } else if (OB_FAIL(tablet_status_map_.set_refactored(tablet_id, new_tablet_status, true/*overwrite*/))) {
    LOG_WARN("fail to set tablet status", KR(ret), K(tablet_id), K(new_tablet_status));
  }
  return ret;
}

void ObStorageCachePolicyService::set_trigger_status(const bool is_triggered)
{
  SpinWLockGuard guard(lock_);
  is_triggered_ = is_triggered;
}

bool ObStorageCachePolicyService::get_trigger_status() const
{
  SpinRLockGuard guard(lock_);
  return is_triggered_;
}

SCPTabletTaskMap &ObStorageCachePolicyService::get_tablet_tasks()
{
  return tablet_scheduler_.tablet_task_map_;
}

const hash::ObHashMap<int64_t, PolicyStatus> &ObStorageCachePolicyService::get_tablet_status_map() const
{
  return tablet_status_map_;
}

int ObStorageCachePolicyService::refresh_schema_policy_map(const ObStorageCachePolicyRefreshType &refresh_type)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("ObStorageCachePolicyService::check_and_handle_event", POLICY_SCHEMA_REFRESH_TIME_THRESHOLD);
  ObStorageCachePolicyRefreshType real_refresh_type = refresh_type;
  LOG_TRACE("[SCP]start to refresh schema policy map", K_(tenant_id), K_SCP_REFRESH_TYPE(refresh_type));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet ttl manager not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_scp_refresh_type(refresh_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid refresh_type", KR(ret), K_SCP_REFRESH_TYPE(refresh_type), KPC(this));
  } else {
    // Check the trigger status, if it is true means user is tryint to trigger the refresh
    // Change the refresh type to force refresh to do the refresh
    if (!is_need_refresh_type(real_refresh_type) && is_triggered_) {
      real_refresh_type = ObStorageCachePolicyRefreshType::REFRESH_TYPE_FORCE_REFRESH;
      set_trigger_status(false);
      FLOG_INFO("[SCP]trigger status is true, change refresh type to force refresh", K_(tenant_id), K_SCP_REFRESH_TYPE(real_refresh_type));
    }
    bool need_refresh = is_need_refresh_type(real_refresh_type);
    if (!need_refresh) {
      if (OB_FAIL(tenant_need_refresh(need_refresh))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to check schema version", KR(ret));
        }
      }
    }
    
    if (OB_SUCC(ret) && need_refresh) {
      if (OB_FAIL(check_and_generate_tablet_tasks(real_refresh_type))) {
        LOG_WARN("fail to generate tablet tasks", KR(ret),
            KPC(this), K(need_refresh), K_SCP_REFRESH_TYPE(real_refresh_type));
      }
      guard.click("check_and_generate_tablet_tasks");
    }
  }
  if (ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL != real_refresh_type) {
      FLOG_INFO("[SCP]finish to refresh schema policy map", K_(tenant_id), K_SCP_REFRESH_TYPE(real_refresh_type), K_SCP_REFRESH_TYPE(refresh_type), K(guard), K_(is_triggered));
  }
  return ret;
}
} // end namespace storage
} // end namespace oceanbase
