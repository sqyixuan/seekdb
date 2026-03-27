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

#ifndef OCEANBASE_ROOTSERVER_OB_UNIT_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_UNIT_MANAGER_H_

#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_unit_table_operator.h"
#include "lib/worker.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_unit_placement_strategy.h"
namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObServerConfig;
class ObMySQLTransaction;
}
namespace share
{
struct ObServerStatus;
struct ObUnitStat;
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace rootserver
{
class ObZoneManager;
class ObServerManager;
class ObUnitManager
{
public:
  typedef common::hash::ObHashMap<uint64_t, share::ObResourcePool *> IdPoolMap;
  typedef common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> TenantPoolsMap;
  struct UnitZoneOrderCmp
  {
    bool operator()(const share::ObUnit *left, const share::ObUnit *right) {
      bool bool_ret = true;
      if (nullptr == left && nullptr == right) {
        bool_ret = false;
      } else if (nullptr == left && nullptr != right) {
        bool_ret = true;
      } else if (nullptr != left && nullptr == right) {
        bool_ret = false;
      } else if (left->zone_ < right->zone_) {
        bool_ret = true;
      } else if (left->zone_ > right->zone_) {
        bool_ret = false;
      } else {
        bool_ret = left->unit_id_ < right->unit_id_;
      }
      return bool_ret;
    }
  };
  struct ZoneUnit
  {
    ZoneUnit() : zone_(), unit_infos_() {}
    ~ZoneUnit() {}

    bool is_valid() const { return !zone_.is_empty() && unit_infos_.count() > 0; }
    void reset() { zone_.reset(); unit_infos_.reset(); }
    int assign(const ZoneUnit &other);

    TO_STRING_KV(K_(zone), K_(unit_infos));
  public:
    common::ObZone zone_;
    common::ObArray<share::ObUnitInfo> unit_infos_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ZoneUnit);
  };

  struct ObUnitLoad: public ObIServerResourceDemand
  {
    ObUnitLoad() : unit_(NULL), unit_config_(NULL), pool_(NULL) {}
    ~ObUnitLoad() {}

    inline bool is_valid() const {
      return NULL != pool_ && NULL != unit_ && NULL != unit_config_;
    }
    // return -1 if resource_type is invalid
    virtual double get_demand(ObResourceType resource_type) const override;
    inline bool is_sys_unit() const { return NULL != pool_ && common::OB_SYS_TENANT_ID == pool_->tenant_id_; }
    uint64_t get_tenant_id() const { return NULL != pool_ ? pool_->tenant_id_ : common::OB_INVALID_TENANT_ID; }
    uint64_t get_unit_id() const { return NULL != unit_ ? unit_->unit_id_ : common::OB_INVALID_ID; }
    uint64_t get_resource_pool_id() const { return NULL != pool_ ? pool_->resource_pool_id_ : common::OB_INVALID_ID; }
    TO_STRING_KV(
        "unit_id", get_unit_id(),
        "tenant_id", get_tenant_id(),
        "resource_pool_id", get_resource_pool_id(),
        KPC_(unit_config),
        KP_(unit), KP_(unit_config), KP_(pool));
  public:
    share::ObUnit *unit_;
    share::ObUnitConfig *unit_config_;
    share::ObResourcePool *pool_;
  };

  enum EndMigrateOp {
    COMMIT,
    ABORT,
    REVERSE,
  };

  const char *end_migrate_op_type_to_str(const EndMigrateOp &t);

public:
  ObUnitManager(ObServerManager &server_mgr, ObZoneManager &zone_mgr);
  virtual ~ObUnitManager();

  int init(common::ObMySQLProxy &proxy,
           common::ObServerConfig &server_config,
           obrpc::ObSrvRpcProxy &srv_rpc_proxy,
           share::schema::ObMultiVersionSchemaService &schema_service,
           ObRootService &root_service);
  virtual int load();
  common::SpinRWLock& get_lock() { return lock_; }
  common::ObMySQLProxy &get_sql_proxy() { return *proxy_; }

  // unit config related
  virtual int create_unit_config(const share::ObUnitConfig &unit_config,
                                 const bool if_not_exist);

  // resource pool related
  virtual int check_tenant_pools_in_shrinking(const uint64_t tenant_id, bool &is_shrinking);
  virtual int check_pool_in_shrinking(const uint64_t pool_id, bool &is_shrinking);
  virtual int check_resource_pool_exist(const share::ObResourcePoolName &resource_pool_name,
                                        bool &is_exist);
  virtual int create_resource_pool(share::ObResourcePool &resource_pool,
                                   const share::ObUnitConfigName &config_name,
                                   const bool if_not_exist);
  virtual int get_zone_pools_unit_num(
      const common::ObZone &zone,
      const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
      int64_t &total_unit_num,
      int64_t &full_unit_num,
      int64_t &logonly_unit_num);
  //Delete the resource pool and associated unit in memory, and other memory structures
  virtual int grant_pools(
      common::ObMySQLTransaction &trans,
      common::ObIArray<uint64_t> &new_ug_id_array,
      const lib::Worker::CompatMode compat_mode,
      const common::ObIArray<share::ObResourcePoolName> &pool_names,
      const uint64_t tenant_id,
      const bool is_bootstrap,
      const bool check_data_version);
  virtual int revoke_pools(
      common::ObMySQLTransaction &trans,
      common::ObIArray<uint64_t> &new_ug_id_array,
      const common::ObIArray<share::ObResourcePoolName> &pool_names,
      const uint64_t tenant_id);
  virtual int get_tenant_alive_servers_non_block(const uint64_t tenant_id,
                                           common::ObIArray<common::ObAddr> &servers);
  virtual int get_pool_ids_of_tenant(const uint64_t tenant_id,
                                     common::ObIArray<uint64_t> &pool_ids) const;
  virtual int get_pool_names_of_tenant(const uint64_t tenant_id,
      common::ObIArray<share::ObResourcePoolName> &pool_names) const;
  virtual int get_unit_config_by_pool_name(
      const common::ObString &pool_name,
      share::ObUnitConfig &unit_config) const;
  virtual int get_zones_of_pools(const common::ObIArray<share::ObResourcePoolName> &pool_names,
                                 common::ObIArray<common::ObZone> &zones) const;
  virtual int get_pools(common::ObIArray<share::ObResourcePool> &pools) const;
  virtual int create_sys_units(const common::ObIArray<share::ObUnit> &sys_units);
  virtual int get_tenant_pool_zone_list(const uint64_t tenant_id,
                                        common::ObIArray<common::ObZone> &zone_list) const;
  virtual int cancel_migrate_out_units(const common::ObAddr &server);
  virtual int check_server_empty(const common::ObAddr &server,
                                 bool &is_empty) const;

  int get_unit_group(
      const uint64_t tenant_id,
      const uint64_t unit_group_id,
      common::ObIArray<share::ObUnitInfo> &unit_info_array);


  virtual int get_unit_infos_of_pool(const uint64_t resource_pool_id,
                                     common::ObIArray<share::ObUnitInfo> &unit_infos) const;
  virtual int get_deleting_units_of_pool(const uint64_t resource_pool_id,
                                         common::ObIArray<share::ObUnit> &units) const;
  virtual int get_unit_infos(const common::ObIArray<share::ObResourcePoolName> &pools,
                             common::ObIArray<share::ObUnitInfo> &unit_infos);
  virtual int get_servers_by_pools(const common::ObIArray<share::ObResourcePoolName> &pools,
                                   common::ObIArray<ObAddr> &addrs);

  virtual int get_unit_ids(common::ObIArray<uint64_t> &unit_ids) const;
  virtual int get_logonly_unit_by_tenant(const int64_t tenant_id,
                                         common::ObIArray<share::ObUnitInfo> &logonly_unit_infos);
  virtual int get_logonly_unit_by_tenant(share::schema::ObSchemaGetterGuard &schema_guard,
                                         const int64_t tenant_id,
                                         common::ObIArray<share::ObUnitInfo> &logonly_unit_infos);
  virtual int get_tenants_of_server(const common::ObAddr &server,
      common::hash::ObHashSet<uint64_t> &tenant_id_set) const;
  virtual int check_tenant_on_server(const uint64_t tenant_id,
      const common::ObAddr &server, bool &on_server) const;
  int get_tenant_unit_servers(
      const uint64_t tenant_id,
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &server_array) const;

  template <typename SCHEMA>
  int check_schema_zone_unit_enough(
      const common::ObZone &zone,
      const int64_t total_unit_num,
      const int64_t full_unit_num,
      const int64_t logonly_unit_num,
      const SCHEMA &schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &enough);
  static int calc_sum_load(const common::ObArray<ObUnitLoad> *unit_loads,
                           share::ObUnitConfig &sum_load,
                           const bool include_ungranted_unit = true);
  // get hard limit
  int get_hard_limit(double &hard_limit) const;

private:
  enum AlterUnitNumType
  {
    AUN_SHRINK = 0,
    AUN_ROLLBACK_SHRINK,
    AUN_EXPAND,
    AUN_NOP,
    AUN_MAX,
  };

  enum AlterResourceErr
  {
    MIN_CPU = 0,
    MAX_CPU,
    MEMORY,
    LOG_DISK,
    DATA_DISK,
    ALT_ERR
  };

  struct ZoneUnitPtr
  {
    ZoneUnitPtr() : zone_(), unit_ptrs_() {}
    ~ZoneUnitPtr() {}

    bool is_valid() const { return !zone_.is_empty() && unit_ptrs_.count() > 0; }
    void reset() { zone_.reset(); unit_ptrs_.reset(); }
    int assign(const ZoneUnitPtr &other);
    int sort_by_unit_id_desc();

    TO_STRING_KV(K_(zone), K_(unit_ptrs));
  public:
    common::ObZone zone_;
    common::ObSEArray<share::ObUnit *, 16> unit_ptrs_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ZoneUnitPtr);
  };

  class UnitGroupIdCmp
  {
  public:
    UnitGroupIdCmp() : ret_(common::OB_SUCCESS) {}
    ~UnitGroupIdCmp() {}
    bool operator()(const share::ObUnit *left, const share::ObUnit *right) {
      bool bool_ret = false;
      if (common::OB_SUCCESS != ret_) {
      } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
        ret_ = common::OB_ERR_UNEXPECTED;
      } else if (left->unit_id_ > right->unit_id_) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
      return bool_ret;
    }
    int get_ret() const { return ret_; }
  private:
    int ret_;
  };

  struct UnitNum
  {
    UnitNum() : full_unit_num_(0), logonly_unit_num_(0) {}
    UnitNum(const int64_t full_unit_num, const int64_t logonly_unit_num)
      : full_unit_num_(full_unit_num), logonly_unit_num_(logonly_unit_num) {}
    TO_STRING_KV(K_(full_unit_num), K_(logonly_unit_num));
    void reset() { full_unit_num_ = 0; logonly_unit_num_ = 0; }
    int64_t full_unit_num_;
    int64_t logonly_unit_num_;
  };

  static const int64_t UNIT_MAP_BUCKET_NUM = 32;
  static const int64_t CONFIG_MAP_BUCKET_NUM = 32;
  static const int64_t CONFIG_REF_COUNT_MAP_BUCKET_NUM = 32;
  static const int64_t CONFIG_POOLS_MAP_BUCKET_NUM = 32;
  static const int64_t POOL_MAP_BUCKET_NUM = 32;
  static const int64_t UNITLOAD_MAP_BUCKET_NUM = 32;
  static const int64_t TENANT_POOLS_MAP_BUCKET_NUM = 32;
  static const int64_t SERVER_MIGRATE_UNITS_MAP_BUCKET_NUM = 32;
  static const int64_t SERVER_REF_COUNT_MAP_BUCKET_NUM = 32;
  static const int64_t NOTIFY_RESOURCE_RPC_TIMEOUT = 9 * 1000000; // 9 second

private:
  // make sure lock_ is held when calling this method
  int check_inner_stat_() const;
  // for ObServerBalancer
  IdPoolMap& get_id_pool_map() { return id_pool_map_; }
  TenantPoolsMap& get_tenant_pools_map() { return tenant_pools_map_; }
  int get_zone_units(const common::ObArray<share::ObResourcePool *> &pools,
                     common::ObArray<ZoneUnit> &zone_units) const;
  virtual int end_migrate_unit(const uint64_t unit_id, const EndMigrateOp end_migrate_op = COMMIT);

  int check_expand_zone_resource_allowed_by_old_unit_stat_(
      const uint64_t tenant_id,
      bool &is_allowed);
  int check_expand_zone_resource_allowed_by_new_unit_stat_(
      const common::ObIArray<share::ObResourcePoolName> &pool_names);
  int check_expand_zone_resource_allowed_by_data_disk_size_(
      const uint64_t tenant_id,
      const common::ObIArray<share::ObResourcePoolName> &pool_names);
  int check_tenant_pools_unit_num_legal_(
      const uint64_t tenant_id,
      const common::ObIArray<share::ObResourcePoolName> &pool_names,
      bool &unit_num_legal,
      int64_t &legal_unit_num);
  int get_tenant_pool_unit_group_id_(
      const bool is_bootstrap,
      const bool grant,
      const uint64_t tenant_id,
      const int64_t unit_group_num,
      common::ObIArray<uint64_t> &new_unit_group_id_array);
  int get_migrate_units_by_server(const ObAddr &server,
                                  common::ObIArray<uint64_t> &migrate_units) const;
  //////end of server_balance

  static int check_bootstrap_pool(const share::ObResourcePool &pool);
  int have_enough_resource(const obrpc::ObGetServerResourceInfoResult &report_server_resource_info,
                           const share::ObUnitResource &unit_resource,
                           const double limit,
                           bool &is_enough,
                           AlterResourceErr &err_index) const;
  int inner_get_unit_info_by_id(const uint64_t unit_id, share::ObUnitInfo &unit) const;
  int check_server_enough(const uint64_t tenant_id,
                          const common::ObIArray<share::ObResourcePoolName> &pool_names,
                          bool &enough);

  int inner_get_active_unit_infos_of_tenant(const share::schema::ObTenantSchema &tenant_schema,
                                            common::ObIArray<share::ObUnitInfo> &unit_info);

  int inner_get_unit_infos_of_pool_(const uint64_t resource_pool_id,
                                   common::ObIArray<share::ObUnitInfo> &unit_infos) const;

  int inner_get_zone_alive_unit_infos_by_tenant(
      const uint64_t tenant_id,
      const common::ObZone &zone,
      common::ObIArray<share::ObUnitInfo> &unit_infos) const;

  int inner_get_pool_ids_of_tenant(const uint64_t tenant_id,
                                   ObIArray<uint64_t> &pool_ids) const;

  int check_resource_pool(share::ObResourcePool &resource_pool) const;
  int get_pool_servers(const uint64_t resource_pool_id,
                       const common::ObZone &zone,
                       common::ObIArray<common::ObAddr> &servers) const;
  int get_pools_servers(const common::ObIArray<share::ObResourcePool *> &pools,
      common::hash::ObHashMap<common::ObAddr, int64_t> &server_ref_count_map) const;
  int add_unit(common::ObISQLClient &client, const share::ObUnit &unit);
  // load balance related
  int inner_get_unit_ids(common::ObIArray<uint64_t> &unit_ids) const;
  int inner_get_tenant_zone_full_unit_num(
      const int64_t tenant_id,
      const common::ObZone &zone,
      int64_t &unit_num);
  int inner_get_tenant_pool_zone_list(
      const uint64_t tenant_id,
      common::ObIArray<common::ObZone> &zone_list) const;
  int get_tenant_zone_all_unit_loads(
      const int64_t tenant_id,
      const common::ObZone &zone,
      common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads);
  int get_tenant_zone_unit_loads(
      const int64_t tenant_id,
      const common::ObZone &zone,
      const common::ObReplicaType replica_type,
      common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads);

  // alter pool related
  int build_sorted_zone_unit_ptr_array(
      share::ObResourcePool *pool,
      common::ObIArray<ZoneUnitPtr> &zone_unit_ptrs);
  int check_shrink_unit_num_zone_condition(
      share::ObResourcePool *pool,
      const int64_t alter_unit_num,
      const common::ObIArray<uint64_t> &delete_unit_id_array);
  int fill_delete_unit_ptr_array(
      share::ObResourcePool *pool,
      const common::ObIArray<uint64_t> &delete_unit_id_array,
      const int64_t alter_unit_num,
      common::ObIArray<share::ObUnit *> &output_delete_unit_ptr_array);
  int shrink_not_granted_pool(
      share::ObResourcePool *pool,
      const int64_t unit_num,
      const common::ObIArray<uint64_t> &delete_unit_id_array);
  template <typename SCHEMA>
  int inner_check_schema_zone_unit_enough(
      const common::ObZone &zone,
      const int64_t total_unit_num,
      const int64_t full_unit_num,
      const int64_t logonly_unit_num,
      const SCHEMA &schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &enough);
  int inner_get_zone_pools_unit_num(const common::ObZone &zone,
                                    const common::ObIArray<share::ObResourcePool *> &pool_list,
                                    int64_t &total_unit_num,
                                    int64_t &full_unit_num,
                                    int64_t &logonly_unit_num);
  int check_pool_intersect_(const uint64_t tenant_id,
                           const common::ObIArray<share::ObResourcePoolName> &pool_names,
                           bool &intersect);
  int check_pool_ownership_(const uint64_t tenant_id,
                           const common::ObIArray<share::ObResourcePoolName> &pool_names,
                           const bool grant);

  int construct_pool_units_to_grant_(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const share::ObResourcePool &new_pool,
      const common::ObIArray<share::ObUnit *> &zone_sorted_unit_array,
      const common::ObIArray<uint64_t> &new_ug_ids,
      const lib::Worker::CompatMode &compat_mode,
      ObNotifyTenantServerResourceProxy &notify_proxy,
      ObIArray<share::ObUnit> &pool_units,
      const bool check_data_version);

  int construct_unit_group_id_for_unit_(
      const share::ObUnit &target_unit,
      const int64_t unit_index,
      const int64_t unit_num,
      const common::ObIArray<uint64_t> &new_ug_ids,
      uint64_t &unit_group_id);

  int do_grant_pools_(common::ObMySQLTransaction &trans,
                     const common::ObIArray<uint64_t> &new_unit_group_id_array,
                     const lib::Worker::CompatMode compat_mode,
                     const common::ObIArray<share::ObResourcePoolName> &pool_names,
                     const uint64_t tenant_id,
                     const bool is_bootstrap,
                     const bool check_data_version);

  int do_revoke_pools_(common::ObMySQLTransaction &trans,
                      const common::ObIArray<uint64_t> &new_unit_group_id_array,
                      const common::ObIArray<share::ObResourcePoolName> &pool_names,
                      const uint64_t tenant_id);

  int build_zone_sorted_unit_array_(const share::ObResourcePool *pool,
                                common::ObArray<share::ObUnit*> &units);

  // build hashmaps
  int build_unit_map(const common::ObIArray<share::ObUnit> &units);
  int build_config_map(const common::ObIArray<share::ObUnitConfig> &configs);
  int build_pool_map(const common::ObIArray<share::ObResourcePool> &pools);

  // insert into memory
  int insert_unit_config(share::ObUnitConfig *config);
  int inc_config_ref_count(const uint64_t config_id);
  int dec_config_ref_count(const uint64_t config_id);
  int update_pool_map(share::ObResourcePool *resource_pool);
  int insert_unit(share::ObUnit *unit);
  int insert_unit_loads(share::ObUnit *unit);
  int insert_unit_load(const common::ObAddr &server, const ObUnitLoad &load);
  int insert_load_array(const common::ObAddr &addr, common::ObArray<ObUnitLoad> *load);
  int update_pool_load(share::ObResourcePool *pool, share::ObUnitConfig *new_config);
  int update_unit_load(share::ObUnit *unit, share::ObResourcePool *new_pool);
  int gen_unit_load(share::ObUnit *unit, ObUnitLoad &load) const;
  int insert_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *resource_pool);
  int insert_config_pool(const uint64_t config_id, share::ObResourcePool *resource_pool);
  int insert_migrate_unit(const common::ObAddr &src_server, const uint64_t unit_id);

  // delete from memory
  int delete_unit_config(const uint64_t config_id,
                         const share::ObUnitConfigName &config_name);
  int delete_resource_pool(const uint64_t pool_id,
                           const share::ObResourcePoolName &pool_name);
  int delete_units_of_pool(const uint64_t resource_pool_id);
  int delete_inmemory_units(const uint64_t resource_pool_id,
                            const common::ObIArray<uint64_t> &unit_ids);
  int delete_unit_loads(const share::ObUnit &unit);
  int delete_unit_load(const common::ObAddr &server, const uint64_t unit_id);
  int delete_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *pool);
  int delete_migrate_unit(const common::ObAddr &src_server, const uint64_t unit_id);

  int get_unit_config_by_name(const share::ObUnitConfigName &name,
                              share::ObUnitConfig *&config) const;
  int get_unit_config_by_id(const uint64_t config_id, share::ObUnitConfig *&config) const;
  // if not exist, return OB_ENTRY_NOT_EXIST
  int get_config_ref_count(const uint64_t config_id, int64_t &ref_count) const;
  int get_server_ref_count(common::hash::ObHashMap<common::ObAddr, int64_t> &map,
      const common::ObAddr &server, int64_t &server_ref_count) const;
  int set_server_ref_count(common::hash::ObHashMap<common::ObAddr, int64_t> &map,
      const common::ObAddr &server, const int64_t server_ref_count) const;
  int inner_get_resource_pool_by_name(
      const share::ObResourcePoolName &name,
      share::ObResourcePool *&pool) const;
  int get_resource_pool_by_id(const uint64_t pool_id,
                              share::ObResourcePool *&pool) const;
  int get_units_by_pool(const uint64_t pood_id, common::ObArray<share::ObUnit *> *&units) const;
  int get_unit_by_id(const uint64_t unit_id, share::ObUnit *&unit) const;
  int get_loads_by_server(const common::ObAddr &server, common::ObArray<ObUnitLoad> *&loads) const;
  int get_pools_by_tenant_(const uint64_t tenant_id,
                          common::ObArray<share::ObResourcePool *> *&pools) const;
  int get_pools_by_config(const uint64_t tenant_id,
                          common::ObArray<share::ObResourcePool *> *&pools) const;
  int get_migrate_units_by_server(const common::ObAddr &server,
                                  common::ObArray<uint64_t> *&migrate_units) const;
  int fetch_new_unit_config_id(uint64_t &unit_config_id);
  int fetch_new_resource_pool_id(uint64_t &resource_pool_id);
  int fetch_new_unit_id(uint64_t &unit_id);
  int fetch_new_unit_group_id(uint64_t &unit_group_id);
  int extract_unit_ids(const common::ObIArray<share::ObUnit *> &units,
                       common::ObIArray<uint64_t> &unit_ids);
  int try_notify_tenant_server_unit_resource_(
      const uint64_t tenant_id,
      const bool is_delete, /*Expansion of semantics, possibly deleting resources*/
      ObNotifyTenantServerResourceProxy &notify_proxy,
      const uint64_t unit_config_id,
      const lib::Worker::CompatMode compat_mode,
      const share::ObUnit &unit,
      const bool if_not_grant,
      const bool skip_offline_server,
      const bool check_data_version);
  int build_notify_create_unit_resource_rpc_arg_(
      const uint64_t tenant_id,
      const share::ObUnit &unit,
      const lib::Worker::CompatMode compat_mode,
      const uint64_t unit_config_id,
      const bool if_not_grant,
      obrpc::TenantServerUnitConfig &rpc_arg) const;
  int do_notify_unit_resource_(
    const common::ObAddr server,
    const obrpc::TenantServerUnitConfig &notify_arg,
    ObNotifyTenantServerResourceProxy &notify_proxy);
  int rollback_persistent_units_(
      const common::ObArray<share::ObUnit> &units,
      const share::ObResourcePool &pool,
      ObNotifyTenantServerResourceProxy &notify_proxy);
  int get_pools_by_id(
      const common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
      const uint64_t id, common::ObArray<share::ObResourcePool *> *&pools) const;
  int insert_id_pool(
      common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
      common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > &allocator,
      const uint64_t id,
      share::ObResourcePool *resource_pool);
  int insert_id_pool_array(
      common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
      const uint64_t id,
      common::ObArray<share::ObResourcePool *> *pools);
  int delete_id_pool(
      common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
      common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > &allocator,
      const uint64_t id,
      share::ObResourcePool *resource_pool);
  int cancel_migrate_unit(
      const share::ObUnit &unit,
      const bool migrate_from_server_can_migrate_in);
  int inner_create_unit_config_(
      const share::ObUnitConfig &unit_config,
      const bool if_not_exist);
  int inner_create_resource_pool_(
      share::ObResourcePool &resource_pool,
      const share::ObUnitConfigName &config_name,
      const bool if_not_exist);
  int inner_try_delete_migrate_unit_resource(
      const uint64_t unit_id,
      const common::ObAddr &migrate_from_server);
  int inner_get_all_unit_group_id(
      const uint64_t tenant_id,
      const bool is_active,
      common::ObIArray<uint64_t> &unit_group_id_array);
  int get_servers_resource_info_via_rpc(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &report_server_resource_info) const;
  int get_server_resource_info_via_rpc(
    const share::ObServerInfoInTable &server_info,
    obrpc::ObGetServerResourceInfoResult &report_servers_resource_info) const ;
  int inner_check_pool_in_shrinking_(
      const uint64_t pool_id,
      bool &is_shrinking);
  int inner_commit_shrink_tenant_resource_pool_(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const common::ObArray<share::ObResourcePool*> &pools);


  // arrange unit related
  int get_servers_resource_info_via_rpc_(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info);
  static int order_report_servers_resource_info_(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &ordered_report_servers_resource_info);

  int compute_server_resource_(
    const obrpc::ObGetServerResourceInfoResult &report_server_resource_info,
    ObUnitPlacementStrategy::ObServerResource &server_resource) const;
  bool check_resource_enough_for_unit_(
      const ObUnitPlacementStrategy::ObServerResource &r,
      const share::ObUnitResource &u,
      const double hard_limit,
      ObResourceType &not_enough_resource,
      AlterResourceErr &not_enough_resource_config) const;
  //LOCK IN
  int commit_shrink_resource_pool_in_trans_(
    const common::ObIArray<share::ObResourcePool *> &pools,
    common::ObMySQLTransaction &trans,
    common::ObIArray<common::ObArray<uint64_t>> &resource_units);
  // tools
  const char *alter_resource_err_to_str(AlterResourceErr err) const
  {
    const char *str = "UNKNOWN";
    switch (err) {
      case MIN_CPU: { str = "MIN_CPU"; break; }
      case MAX_CPU: { str = "MAX_CPU"; break; }
      case MEMORY: { str = "MEMORY_SIZE"; break; }
      case LOG_DISK: { str = "LOG_DISK_SIZE"; break; }
      case DATA_DISK: { str = "DATA_DISK_SIZE"; break; }
      default: { str = "UNKNOWN"; break; }
    }
    return str;
  }
  void print_user_error_(const uint64_t tenant_id);

private:
  bool inited_;
  bool loaded_;
  common::ObMySQLProxy *proxy_;
  common::ObServerConfig *server_config_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  ObServerManager &server_mgr_;
  ObZoneManager &zone_mgr_;
  share::ObUnitTableOperator ut_operator_;
  common::hash::ObHashMap<uint64_t, share::ObUnitConfig *> id_config_map_;
  common::hash::ObHashMap<share::ObUnitConfigName, share::ObUnitConfig *> name_config_map_;
  common::hash::ObHashMap<uint64_t, int64_t> config_ref_count_map_;
  common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> config_pools_map_;
  common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > config_pools_allocator_;
  common::ObPooledAllocator<share::ObUnitConfig> config_allocator_;
  IdPoolMap id_pool_map_;
  common::hash::ObHashMap<share::ObResourcePoolName, share::ObResourcePool *> name_pool_map_;
  common::ObPooledAllocator<share::ObResourcePool> pool_allocator_;
  common::hash::ObHashMap<uint64_t, common::ObArray<share::ObUnit *> *> pool_unit_map_;
  common::ObPooledAllocator<common::ObArray<share::ObUnit *> > pool_unit_allocator_;
  common::hash::ObHashMap<uint64_t, share::ObUnit *> id_unit_map_;
  common::ObPooledAllocator<share::ObUnit> allocator_;
  common::hash::ObHashMap<common::ObAddr, common::ObArray<ObUnitLoad> *> server_loads_;
  common::ObPooledAllocator<common::ObArray<ObUnitLoad> > load_allocator_;
  TenantPoolsMap tenant_pools_map_;
  common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > tenant_pools_allocator_;
  common::hash::ObHashMap<common::ObAddr, common::ObArray<uint64_t> *> server_migrate_units_map_;
  common::ObPooledAllocator<common::ObArray<uint64_t> > migrate_units_allocator_;
  common::SpinRWLock lock_;
  ObRootService *root_service_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  DISALLOW_COPY_AND_ASSIGN(ObUnitManager);
};

template<typename SCHEMA>
int ObUnitManager::check_schema_zone_unit_enough(
    const common::ObZone &zone,
    const int64_t total_unit_num,
    const int64_t full_unit_num,
    const int64_t logonly_unit_num,
    const SCHEMA &schema,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &enough)
{
  int ret = OB_SUCCESS;
  enough = true;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    RS_LOG(WARN, "variable is not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(inner_check_schema_zone_unit_enough(
          zone, total_unit_num, full_unit_num, logonly_unit_num,
          schema, schema_guard, enough))) {
    RS_LOG(WARN, "fail to inner check schema zone unit enough", K(ret));
  }
  return ret;
}

template <typename SCHEMA>
int ObUnitManager::inner_check_schema_zone_unit_enough(
    const common::ObZone &zone,
    const int64_t total_unit_num,
    const int64_t full_unit_num,
    const int64_t logonly_unit_num,
    const SCHEMA &schema,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &enough)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaNumSet> zone_locality_array;
  enough = true;
  UNUSED(logonly_unit_num);
  if (OB_FAIL(check_inner_stat_())) {
    RS_LOG(WARN, "variable is not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality_array))) {
    RS_LOG(WARN, "fail to get zone replica num array", K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < zone_locality_array.count(); ++i) {
      share::ObZoneReplicaNumSet &num_set = zone_locality_array.at(i);
      if (zone != num_set.zone_) {
        // go on next
      } else {
        find = true;
        int64_t full_and_readonly_num
            = num_set.get_full_replica_num()
              + (num_set.get_readonly_replica_num() == ObLocalityDistribution::ALL_SERVER_CNT
                  ? 0 : num_set.get_readonly_replica_num());
        if (total_unit_num < num_set.get_specific_replica_num()) {
          // The total number of unit num is less than the number of specific replica num,
          // which is not enough.
          enough = false;
        } else if (full_unit_num < full_and_readonly_num) {
          enough = false;
        }
        break;
      }
    }
    if (OB_FAIL(ret)) {
      // bypass
    } else if (!find) { // no zone locality exist, this is enough
      enough = true;
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_UNIT_MANAGER_H_
