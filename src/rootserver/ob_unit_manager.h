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
  virtual int create_resource_pool(share::ObResourcePool &resource_pool,
                                   const share::ObUnitConfigName &config_name,
                                   const bool if_not_exist);
  //Delete the resource pool and associated unit in memory, and other memory structures
  virtual int grant_pools(
      common::ObMySQLTransaction &trans,
      common::ObIArray<uint64_t> &new_ug_id_array,
      const lib::Worker::CompatMode compat_mode,
      const common::ObIArray<share::ObResourcePoolName> &pool_names,
      const uint64_t tenant_id,
      const bool is_bootstrap,
      const bool check_data_version);
  virtual int get_tenant_alive_servers_non_block(const uint64_t tenant_id,
                                           common::ObIArray<common::ObAddr> &servers);
  virtual int get_unit_config_by_pool_name(
      const common::ObString &pool_name,
      share::ObUnitConfig &unit_config) const;
  virtual int create_sys_units(const common::ObIArray<share::ObUnit> &sys_units);
  virtual int check_server_empty(const common::ObAddr &server,
                                 bool &is_empty) const;

private:
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
  //////end of server_balance

  static int check_bootstrap_pool(const share::ObResourcePool &pool);
  int inner_get_unit_info_by_id(const uint64_t unit_id, share::ObUnitInfo &unit) const;

  int add_unit(common::ObISQLClient &client, const share::ObUnit &unit);

  // alter pool related
  int check_pool_intersect_(const uint64_t tenant_id,
                           const common::ObIArray<share::ObResourcePoolName> &pool_names,
                           bool &intersect);
  int check_pool_ownership_(const uint64_t tenant_id,
                           const common::ObIArray<share::ObResourcePoolName> &pool_names,
                           const bool grant);

  int do_grant_pools_(common::ObMySQLTransaction &trans,
                     const common::ObIArray<uint64_t> &new_unit_group_id_array,
                     const lib::Worker::CompatMode compat_mode,
                     const common::ObIArray<share::ObResourcePoolName> &pool_names,
                     const uint64_t tenant_id,
                     const bool is_bootstrap,
                     const bool check_data_version);

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
  int gen_unit_load(share::ObUnit *unit, ObUnitLoad &load) const;
  int insert_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *resource_pool);
  int insert_config_pool(const uint64_t config_id, share::ObResourcePool *resource_pool);

  // delete from memory
  int delete_resource_pool(const uint64_t pool_id,
                           const share::ObResourcePoolName &pool_name);
  int delete_units_of_pool(const uint64_t resource_pool_id);
  int delete_unit_loads(const share::ObUnit &unit);
  int delete_unit_load(const common::ObAddr &server, const uint64_t unit_id);

  int get_unit_config_by_name(const share::ObUnitConfigName &name,
                              share::ObUnitConfig *&config) const;
  int get_unit_config_by_id(const uint64_t config_id, share::ObUnitConfig *&config) const;
  // if not exist, return OB_ENTRY_NOT_EXIST
  int get_config_ref_count(const uint64_t config_id, int64_t &ref_count) const;
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
  int fetch_new_unit_config_id(uint64_t &unit_config_id);
  int fetch_new_unit_id(uint64_t &unit_id);
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
  int inner_create_unit_config_(
      const share::ObUnitConfig &unit_config,
      const bool if_not_exist);
  int inner_create_resource_pool_(
      share::ObResourcePool &resource_pool,
      const share::ObUnitConfigName &config_name,
      const bool if_not_exist);

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

}//end namespace share
}//end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_UNIT_MANAGER_H_
