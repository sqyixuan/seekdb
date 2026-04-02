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

#ifndef OCEANBASE_STORAGE_CACHE_SERVICE_H_
#define OCEANBASE_STORAGE_CACHE_SERVICE_H_

#include "lib/task/ob_timer.h"
#include "share/tablet/ob_tablet_info.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_force_print_log.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/table/ob_table_service.h"
#include "share/table/ob_table_config_util.h"
#include "storage/ls/ob_ls.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_refresh_scheduler.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
class ObTablePolicyInfo
{
public:
  ObTablePolicyInfo():
    is_inited_(false),
    table_id_(OB_INVALID_ID),
    table_policy_(),
    table_schema_version_(OB_INVALID_VERSION)
  {
  }
  ~ObTablePolicyInfo()
  {
    reset();
  }
  int init(const int64_t table_id, const ObStorageCachePolicy &table_policy, const int64_t table_schema_version);
  void reset();

  ObTablePolicyInfo &operator=(const ObTablePolicyInfo &other);
  bool operator==(const ObTablePolicyInfo &other) const;
  bool operator!=(const ObTablePolicyInfo &other) const;
  const ObStorageCachePolicy &get_table_policy() const { return table_policy_; }
  int64_t get_table_schema_version() const { return table_schema_version_; }
  int64_t get_table_id() const { return table_id_; }
  int set_table_policy(const ObStorageCachePolicy &table_policy) { table_policy_ = table_policy; return OB_SUCCESS; }
  int set_table_schema_version(int64_t table_schema_version) { table_schema_version_ = table_schema_version; return OB_SUCCESS; }
  int set_table_id(int64_t table_id) { table_id_ = table_id; return OB_SUCCESS; }
  bool is_inited() const { return is_inited_; }
  TO_STRING_KV(K_(table_id), K_(table_policy), K_(table_schema_version));
private:
  bool is_inited_;
  int64_t table_id_;
  ObStorageCachePolicy table_policy_;
  int64_t table_schema_version_;
};

class ObTabletPolicyInfo      
{
public:
  ObTabletPolicyInfo():
    is_inited_(false), 
    part_id_(OB_INVALID_ID),
    tablet_policy_(ObStorageCachePolicyType::MAX_POLICY),
    tablet_schema_version_(OB_INVALID_VERSION),
    tablet_policy_status_(PolicyStatus::MAX_STATUS)
  {
  }
  ~ObTabletPolicyInfo()
  {
    reset();
  }
  int init(const ObPartition *partition); 
  int init(const ObSubPartition *sub_partition);
  void reset();
  ObTabletPolicyInfo &operator=(const ObTabletPolicyInfo &other); 
  bool operator==(const ObTabletPolicyInfo &other) const;
  bool operator!=(const ObTabletPolicyInfo &other) const;
  ObStorageCachePolicyType &get_tablet_policy() { return tablet_policy_; }
  int64_t get_tablet_schema_version() const { return tablet_schema_version_; }
  int64_t get_part_id() const { return part_id_; }
  PolicyStatus &get_tablet_policy_status() { return tablet_policy_status_; }
  int set_tablet_policy(const ObStorageCachePolicyType &tablet_policy) { tablet_policy_ = tablet_policy; return OB_SUCCESS; }
  int set_tablet_schema_version(int64_t tablet_schema_version) { tablet_schema_version_ = tablet_schema_version; return OB_SUCCESS; }
  int set_part_id(int64_t part_id) { part_id_ = part_id; return OB_SUCCESS; }   
  int set_tablet_policy_status(const PolicyStatus &tablet_policy_status) { tablet_policy_status_ = tablet_policy_status; return OB_SUCCESS; }
  TO_STRING_KV(K_(part_id), K_(tablet_policy), K_(tablet_schema_version), K_(tablet_policy_status));    

private:
  bool is_inited_;
  int64_t part_id_;
  ObStorageCachePolicyType tablet_policy_;
  int64_t tablet_schema_version_;
  PolicyStatus tablet_policy_status_; 
};

class ObStorageCachePolicyService
{
public:
  ObStorageCachePolicyService():
    tablet_scheduler_(),
    is_inited_(false),
    is_stopped_(false),
    is_triggered_(false),
    local_schema_version_(OB_INVALID_VERSION),
    tenant_id_(OB_INVALID_TENANT_ID),
    lock_(common::ObLatchIds::STORAGE_CACHE_POLICY_MGR_LOCK),
    schema_service_(nullptr),
    table_policy_map_(),
    part_policy_map_(),
    refresh_policy_scheduler_(),
    tablet_status_map_()
  {
  }
  virtual ~ObStorageCachePolicyService()
  {
    destroy();
  }
  int init(const uint64_t tenant_id);
  static int mtl_init(ObStorageCachePolicyService *&service);
  static ObStorageCachePolicyService &get_instance();


  uint64_t get_tenant_id() const { return tenant_id_; }
  int start();
  void stop();
  void wait();
  void destroy();
  int refresh_schema_policy_map(const ObStorageCachePolicyRefreshType &refresh_type);
  int tenant_need_refresh(bool &need_refresh);
  int get_tenant_schema_guard(share::schema::ObSchemaGetterGuard &schema_guard);
  int check_and_generate_tablet_tasks(const ObStorageCachePolicyRefreshType &refresh_type);
  int check_table_schema_version_changed(const ObSimpleTableSchemaV2 *table_schema, bool &table_schema_version_changed);
  int check_table_policy_need_recaculate(
      const ObSimpleTableSchemaV2 *table_schema,
      const ObStorageCachePolicyRefreshType &refresh_type,
      bool &table_policy_need_calc);
  int check_part_schema_version_changed(const ObBasePartition *partition, const int64_t part_id, bool &partition_schema_version_changed);
  //int check_partition_schema_version_changed(const ObPartition *partition, bool &partition_schema_version_changed);
  //int check_sub_partition_schema_version_changed(const ObSubPartition *sub_partition, bool &sub_partition_schema_version_changed);
  int check_part_policy_need_recaculate(const ObBasePartition *partition, const int64_t part_id, bool &partition_policy_need_calc);
  //int check_partition_policy_need_recaculate(const ObPartition *partition, bool &partition_policy_need_calc);
  //int check_sub_partition_policy_need_recaculate(const ObSubPartition *sub_partition, bool &sub_partition_policy_need_calc);
  int cal_index_table_policy_status(const ObSimpleTableSchemaV2 *simple_table_schema, PolicyStatus &index_table_policy_status);
  int cal_table_policy_status(const ObStorageCachePolicy &table_policy, PolicyStatus &table_policy_status);
  int cal_partition_policy_status(const ObPartition *partition, const ObStorageCachePolicy &table_policy, 
                                  PolicyStatus &partition_policy_status);
  int get_table_schema(const uint64_t table_id, const ObTableSchema *&table_schema);
  // is_part is used to distinguish between partition and sub_partition
  int cal_index_partition_policy_status(const ObBasePartition *partition, PolicyStatus &data_partition_policy_status, const bool is_part);

  int cal_partition_policy_status_from_table(const ObPartition *partition, const ObStorageCachePolicy &table_policy, PolicyStatus &partition_policy_status);
  int cal_sub_partition_policy_status(const ObSubPartition *sub_partition, const PolicyStatus &partition_policy_status,
                                      const ObStorageCachePolicy &table_policy, PolicyStatus &sub_partition_policy_status);
  int cal_part_policy_status(const ObObj &obj, const ObStorageCachePolicy &table_policy, 
                              PolicyStatus &partition_policy_status);
  int handle_partitioned_table(const ObSimpleTableSchemaV2 *table_schema, const ObStorageCachePolicy &table_policy, 
                               const bool &table_policy_need_calc,
                               const ObStorageCachePolicyRefreshType &refresh_type);     
  int handle_non_partitioned_table(const ObSimpleTableSchemaV2 *simple_table_schema, const ObTablePolicyInfo &table_policy_info,
                                   const bool &table_policy_need_calc, const bool &table_schema_version_changed,
                                   const ObStorageCachePolicyRefreshType &refresh_type);  
  int handle_partition(
      const ObPartition *partition, const ObStorageCachePolicy table_policy,
      const bool &partition_policy_need_calc,
      const ObPartitionLevel &table_part_level,
      const ObStorageCachePolicyRefreshType &refresh_type,
      PolicyStatus &partition_policy_status);
  int handle_subpartitions(const ObSubPartition *sub_partition,  const ObStorageCachePolicy &table_policy, 
                           const PolicyStatus &partition_policy_status,
                           const ObStorageCachePolicyRefreshType &refresh_type,
                           const bool &sub_partition_policy_need_calc);
  int get_tablet_ls_id(const uint64_t tablet_id, int64_t &ls_id) const;
  int retrive_table_policy(const ObSimpleTableSchemaV2 *simple_table_schema, ObTablePolicyInfo &table_policy_info);
  int check_tablet_status_changed(const uint64_t tablet_id, const PolicyStatus tablet_policy_status, 
                                  const ObStorageCachePolicyRefreshType &refresh_type,
                                  bool &tablet_status_changed);
  int update_tablet_status(const uint64_t tablet_id, const PolicyStatus new_tablet_status);
  void set_trigger_status(const bool is_triggered);
  bool get_trigger_status() const;
  SCPTabletTaskMap &get_tablet_tasks();
  const hash::ObHashMap<int64_t, PolicyStatus> &get_tablet_status_map() const;
  TO_STRING_KV(K_(tenant_id), K_(is_inited), K_(is_stopped), K_(local_schema_version));

public:
  static const int64_t POLICY_SCHEMA_REFRESH_TIME_THRESHOLD = 30*1000*1000; // 30s
  static const int64_t DEFAULT_LS_SIZE = 8; // Used to obtain log stream service
  static const int64_t DEFAULT_TABLE_POLICY_MAP_SIZE = 10000; // Used for policy and schema_version information related to the table in memory
  static const int64_t DEFAULT_PART_POLICY_MAP_SIZE = 100000; // Used for policy and schema_version information related to partitions and sub_partitions in memory
  static const int64_t DEFAULT_TABLET_STATUS_MAP_SIZE = 100000; // Tablet size
public:
  ObStorageCacheTabletScheduler tablet_scheduler_;

private:
  bool is_inited_;
  bool is_stopped_;
  bool is_triggered_; // If true, it means user has triggered the full refresh command
  int64_t local_schema_version_;
  uint64_t tenant_id_;
  common::SpinRWLock lock_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  hash::ObHashMap<int64_t, ObTablePolicyInfo> table_policy_map_;
  // For the second-level partition, the first-level partition is a virtual partition 
  // and does not have a real tablet_id, but we need to save the information of the 
  // virtual first-level partition to calculate the attributes of the second-level partition. 
  // Therefore we need a map to restore informationwith partition_id as the key.
  hash::ObHashMap<int64_t, ObTabletPolicyInfo> part_policy_map_;
  // It is provided for virtual tables to view the tablet information.
  ObStorageCacheRefreshPolicyScheduler refresh_policy_scheduler_;
  hash::ObHashMap<int64_t, PolicyStatus> tablet_status_map_;
};

#define OB_STORAGE_CACHE_SERVICE (oceanbase::storage::ObStorageCachePolicyService::get_instance())
} // end namespace storage
} // end namespace oceanbase
#endif
