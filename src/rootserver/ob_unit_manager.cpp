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

#define USING_LOG_PREFIX RS



#include "ob_unit_manager.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_tenant_memstore_info_operator.h"
#include "rootserver/ddl_task/ob_sys_ddl_util.h"  // for ObSysDDLServiceUtil
#include "rootserver/ob_root_service.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace common::hash;
using namespace share;
using namespace share::schema;
namespace rootserver
{
////////////////////////////////////////////////////////////////
double ObUnitManager::ObUnitLoad::get_demand(ObResourceType resource_type) const
{
  double ret = -1;
  switch (resource_type) {
    case RES_CPU:
      ret = unit_config_->min_cpu();
      break;
    case RES_MEM:
      ret = static_cast<double>(unit_config_->memory_size());
      break;
    case RES_LOG_DISK:
      ret = static_cast<double>(unit_config_->log_disk_size());
      break;
    case RES_DATA_DISK:
      ret = static_cast<double>(unit_config_->data_disk_size());
      break;
    default:
      ret = -1;
      break;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObUnitManager::ObUnitManager(ObZoneManager &zone_mgr)
: inited_(false), loaded_(false), proxy_(NULL), server_config_(NULL),
    srv_rpc_proxy_(NULL),
    zone_mgr_(zone_mgr), ut_operator_(), id_config_map_(),
    name_config_map_(), config_ref_count_map_(), config_pools_map_(),
    config_pools_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    config_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    id_pool_map_(), name_pool_map_(),
    pool_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    pool_unit_map_(),
    pool_unit_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    id_unit_map_(),
    allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    server_loads_(),
    load_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    tenant_pools_map_(),
    tenant_pools_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    server_migrate_units_map_(),
    migrate_units_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(ObModIds::OB_RS_UNIT_MANAGER)),
    lock_(ObLatchIds::UNIT_MANAGER_LOCK),
    schema_service_(NULL)
{
}

ObUnitManager::~ObUnitManager()
{
  ObHashMap<uint64_t, ObArray<share::ObResourcePool *> *>::iterator iter1;
  for (iter1 = config_pools_map_.begin(); iter1 != config_pools_map_.end(); ++iter1) {
    ObArray<share::ObResourcePool *> *ptr = iter1->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
  ObHashMap<uint64_t, ObArray<ObUnit *> *>::iterator iter2;
  for (iter2 = pool_unit_map_.begin(); iter2 != pool_unit_map_.end(); ++iter2) {
    ObArray<share::ObUnit *> *ptr = iter2->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
  ObHashMap<ObAddr, ObArray<ObUnitLoad> *>::iterator iter3;
  for (iter3 = server_loads_.begin(); iter3 != server_loads_.end(); ++iter3) {
    ObArray<ObUnitLoad> *ptr = iter3->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
  TenantPoolsMap::iterator iter4;
  for (iter4 = tenant_pools_map_.begin(); iter4 != tenant_pools_map_.end(); ++iter4) {
    common::ObArray<share::ObResourcePool *> *ptr = iter4->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
  ObHashMap<ObAddr, ObArray<uint64_t> *>::iterator iter5;
  for (iter5 = server_migrate_units_map_.begin();
       iter5 != server_migrate_units_map_.end();
       ++iter5) {
    common::ObArray<uint64_t> *ptr = iter5->second;
    if (NULL != ptr) {
      ptr->reset();
      ptr = NULL;
    }
  }
}

int ObUnitManager::init(ObMySQLProxy &proxy,
                        ObServerConfig &server_config,
                        obrpc::ObSrvRpcProxy &srv_rpc_proxy,
                        share::schema::ObMultiVersionSchemaService &schema_service,
                        ObRootService &root_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ut_operator_.init(proxy))) {
    LOG_WARN("init unit table operator failed", K(ret));
  } else if (OB_FAIL(pool_unit_map_.create(
              POOL_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_POOL_UNIT_MAP))) {
    LOG_WARN("pool_unit_map_ create failed", LITERAL_K(POOL_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(id_unit_map_.create(
              UNIT_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_ID_UNIT_MAP))) {
    LOG_WARN("id_unit_map_ create failed",
             LITERAL_K(UNIT_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(id_config_map_.create(
              CONFIG_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_ID_CONFIG_MAP))) {
    LOG_WARN("id_config_map_ create failed",
             LITERAL_K(CONFIG_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(name_config_map_.create(
              CONFIG_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_NAME_CONFIG_MAP))) {
    LOG_WARN("name_config_map_ create failed",
             LITERAL_K(CONFIG_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(config_ref_count_map_.create(
              CONFIG_REF_COUNT_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_CONFIG_REF_COUNT_MAP))) {
    LOG_WARN("config_ref_count_map_ create failed",
             LITERAL_K(CONFIG_REF_COUNT_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(config_pools_map_.create(CONFIG_POOLS_MAP_BUCKET_NUM,
                                              ObModIds::OB_HASH_BUCKET_CONFIG_POOLS_MAP))) {
    LOG_WARN("create config_pools_map failed",
             LITERAL_K(CONFIG_POOLS_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(id_pool_map_.create(
              POOL_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_ID_POOL_MAP))) {
    LOG_WARN("id_pool_map_ create failed",
             LITERAL_K(POOL_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(name_pool_map_.create(
              POOL_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_NAME_POOL_MAP))) {
    LOG_WARN("name_pool_map_ create failed",
             LITERAL_K(POOL_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(server_loads_.create(
              UNITLOAD_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_SERVER_UNITLOAD_MAP))) {
    LOG_WARN("server_loads_ create failed",
             LITERAL_K(UNITLOAD_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(tenant_pools_map_.create(
              TENANT_POOLS_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_TENANT_POOLS_MAP))) {
    LOG_WARN("tenant_pools_map_ create failed",
             LITERAL_K(TENANT_POOLS_MAP_BUCKET_NUM), K(ret));
  } else if (OB_FAIL(server_migrate_units_map_.create(
              SERVER_MIGRATE_UNITS_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_SERVER_MIGRATE_UNIT_MAP))) {
    LOG_WARN("server_migrate_units_map_ create failed",
             LITERAL_K(SERVER_MIGRATE_UNITS_MAP_BUCKET_NUM), K(ret));
  } else {
    proxy_ = &proxy;
    server_config_ = &server_config;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    root_service_ = &root_service;
    schema_service_ = &schema_service;
    loaded_ = false;
    inited_ = true;
  }
  return ret;
}

// make sure lock_ is held when calling this method
int ObUnitManager::check_inner_stat_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!loaded_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("not loaded", KR(ret));
    // need to reload
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(root_service_->submit_reload_unit_manager_task())) {
      if (OB_CANCELED != tmp_ret) {
        LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit', please try 'alter system reload unit'", K(tmp_ret));
      }
    }
  } else {
    // stat is normal
  }
  return ret;
}

int ObUnitManager::load()
{
  DEBUG_SYNC(BEFORE_RELOAD_UNIT);
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  LOG_INFO("unit manager load start", K(ret));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    loaded_ = false;
    ObHashMap<uint64_t, ObArray<share::ObResourcePool *> *>::iterator iter1;
    for (iter1 = config_pools_map_.begin(); iter1 != config_pools_map_.end(); ++iter1) {
      ObArray<share::ObResourcePool *> *ptr = iter1->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    ObHashMap<uint64_t, ObArray<ObUnit *> *>::iterator iter2;
    for (iter2 = pool_unit_map_.begin(); iter2 != pool_unit_map_.end(); ++iter2) {
      ObArray<share::ObUnit *> *ptr = iter2->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    ObHashMap<ObAddr, ObArray<ObUnitLoad> *>::iterator iter3;
    for (iter3 = server_loads_.begin(); iter3 != server_loads_.end(); ++iter3) {
      ObArray<ObUnitLoad> *ptr = iter3->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    TenantPoolsMap::iterator iter4;
    for (iter4 = tenant_pools_map_.begin(); iter4 != tenant_pools_map_.end(); ++iter4) {
      common::ObArray<share::ObResourcePool *> *ptr = iter4->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    ObHashMap<ObAddr, ObArray<uint64_t> *>::iterator iter5;
    for (iter5 = server_migrate_units_map_.begin();
         iter5 != server_migrate_units_map_.end();
         ++iter5) {
      common::ObArray<uint64_t> *ptr = iter5->second;
      if (NULL != ptr) {
        ptr->reset();
        ptr = NULL;
      }
    }
    if (OB_FAIL(id_config_map_.clear())) {
      LOG_WARN("id_config_map_ clear failed", K(ret));
    } else if (OB_FAIL(name_config_map_.clear())) {
      LOG_WARN("name_pool_map_  clear failed", K(ret));
    } else if (OB_FAIL(config_ref_count_map_.clear())) {
      LOG_WARN("config_ref_count_map_ clear failed", K(ret));
    } else if (OB_FAIL(config_pools_map_.clear())) {
      LOG_WARN("config_pools_map_ clear failed", K(ret));
    } else if (OB_FAIL(id_pool_map_.clear())) {
      LOG_WARN("id_pool_map_ clear failed", K(ret));
    } else if (OB_FAIL(name_pool_map_.clear())) {
      LOG_WARN("name_pool_map_ clear failed", K(ret));
    } else if (OB_FAIL(pool_unit_map_.clear())) {
      LOG_WARN("pool_unit_map_ clear failed", K(ret));
    } else if (OB_FAIL(id_unit_map_.clear())) {
      LOG_WARN("id_unit_map_ clear failed", K(ret));
    } else if (OB_FAIL(server_loads_.clear())) {
      LOG_WARN("server_loads_ clear failed", K(ret));
    } else if (OB_FAIL(tenant_pools_map_.clear())) {
      LOG_WARN("tenant_pools_map_ clear failed", K(ret));
    } else if (OB_FAIL(server_migrate_units_map_.clear())) {
      LOG_WARN("server_migrate_units_map_ clear failed", K(ret));
    }

    // free all memory
    if (OB_SUCC(ret)) {
      config_allocator_.reset();
      config_pools_allocator_.reset();
      pool_allocator_.reset();
      pool_unit_allocator_.reset();
      allocator_.reset();
      load_allocator_.reset();
      tenant_pools_allocator_.reset();
      migrate_units_allocator_.reset();
    }

    // load unit config
    ObArray<ObUnitConfig> configs;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ut_operator_.get_unit_configs(configs))) {
        LOG_WARN("get_unit_configs failed", K(ret));
      } else if (OB_FAIL(build_config_map(configs))) {
        LOG_WARN("build_config_map failed", K(ret));
      }
    }

    // load resource pool
    ObArray<share::ObResourcePool> pools;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ut_operator_.get_resource_pools(pools))) {
        LOG_WARN("get_resource_pools failed", K(ret));
      } else if (OB_FAIL(build_pool_map(pools))) {
        LOG_WARN("build_pool_map failed", K(ret));
      }
    }

    // load unit
    ObArray<ObUnit> units;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ut_operator_.get_units(units))) {
        LOG_WARN("get_units failed", K(ret));
      } else if (OB_FAIL(build_unit_map(units))) {
        LOG_WARN("build_unit_map failed", K(ret));
      }
    }

    // build tenant pools
    if (OB_SUCC(ret)) {
      for (ObHashMap<uint64_t, share::ObResourcePool *>::iterator it = id_pool_map_.begin();
           OB_SUCCESS == ret && it != id_pool_map_.end(); ++it) {
        // pool not grant to tenant don't add to tenant_pools_map
        if (NULL == it->second) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("it->second is null", KP(it->second), K(ret));
        } else if (it->second->is_granted_to_tenant()) {
          if (OB_FAIL(insert_tenant_pool(it->second->tenant_id_, it->second))) {
            LOG_WARN("insert_tenant_pool failed", "tenant_id", it->second->tenant_id_,
                     "pool", *(it->second), K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      loaded_ = true;
    }
  }
  LOG_INFO("unit manager load finish", K(ret));
  return ret;
}

int ObUnitManager::create_unit_config(const ObUnitConfig &unit_config, const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(inner_create_unit_config_(unit_config, if_not_exist))) {
    LOG_WARN("fail to create unit config", KR(ret), K(unit_config), K(if_not_exist));
  }
  return ret;
}

int ObUnitManager::inner_create_unit_config_(const ObUnitConfig &unit_config, const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  ObUnitConfig *temp_config = NULL;
  ObUnitConfig *new_config = NULL;
  uint64_t unit_config_id = unit_config.unit_config_id();
  const ObUnitConfigName &name = unit_config.name();
  const ObUnitResource &rpc_ur = unit_config.unit_resource();
  ObUnitResource ur = rpc_ur;

  LOG_INFO("start create unit config", K(name), K(rpc_ur), K(if_not_exist));

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (name.is_empty()) {
    ret = OB_MISS_ARGUMENT;
    LOG_WARN("miss 'name' argument", KR(ret), K(name));
    LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource unit name");
  } else if (OB_FAIL(ur.init_and_check_valid_for_unit(rpc_ur))) {
    LOG_WARN("init from user specified unit resource and check valid fail", KR(ret), K(rpc_ur));
  } else if (OB_SUCCESS == (ret = get_unit_config_by_name(name, temp_config))) {
    if (NULL == temp_config) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("temp_config is null", KP(temp_config), K(ret));
    } else if (if_not_exist) {
      ObCStringHelper helper;
      LOG_USER_NOTE(OB_RESOURCE_UNIT_EXIST, helper.convert(name));
      LOG_INFO("unit config already exist", K(name));
    } else {
      ret = OB_RESOURCE_UNIT_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_EXIST, helper.convert(name));
      LOG_WARN("unit config already exist", K(name), KR(ret));
    }
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("get_unit_config_by_name failed", "config_name", name, KR(ret));
  }
  // allocate new unit config id
  else if (OB_INVALID_ID == unit_config_id && OB_FAIL(fetch_new_unit_config_id(unit_config_id))) {
    LOG_WARN("fetch_new_unit_config_id failed", KR(ret), K(unit_config));
  } else {
    if (OB_ISNULL(new_config = config_allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", KR(ret));
    } else if (OB_FAIL(new_config->init(unit_config_id, name, ur))) {
      LOG_WARN("init unit config fail", KR(ret), K(unit_config_id), K(name), K(ur));
    } else if (OB_FAIL(ut_operator_.update_unit_config(*proxy_, *new_config))) {
      LOG_WARN("update_unit_config failed", "unit config", *new_config, K(ret));
    } else if (OB_FAIL(insert_unit_config(new_config))) {
      LOG_WARN("insert_unit_config failed", "unit config", *new_config,  K(ret));
    }

    // avoid memory leak
    if (OB_FAIL(ret) && NULL != new_config) {
      config_allocator_.free(new_config);
      new_config = NULL;
    }
  }
  LOG_INFO("finish create unit config", KR(ret), K(name), K(rpc_ur), K(if_not_exist), KPC(new_config));
  return ret;
}

int ObUnitManager::create_resource_pool(
    share::ObResourcePool &resource_pool,
    const ObUnitConfigName &config_name,
    const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(inner_create_resource_pool_(resource_pool, config_name, if_not_exist))) {
    LOG_WARN("fail to inner create resource pool", KR(ret), K(resource_pool), K(config_name), K(if_not_exist));
  }
  return ret;
}

int ObUnitManager::inner_create_resource_pool_(
    share::ObResourcePool &resource_pool,
    const ObUnitConfigName &config_name,
    const bool if_not_exist)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start inner create resource pool", K(resource_pool), K(config_name),
           K(if_not_exist));
  ObUnitConfig *config = NULL;
  share::ObResourcePool *pool = NULL;
  bool is_bootstrap_pool = (ObUnitConfig::SYS_UNIT_CONFIG_ID == resource_pool.unit_config_id_);
  const char *module = "CREATE_RESOURCE_POOL";

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (is_bootstrap_pool && OB_FAIL(check_bootstrap_pool(resource_pool))) {
    LOG_WARN("check bootstrap pool failed", K(resource_pool), K(ret));
  } else if (config_name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource unit name");
    LOG_WARN("invalid config_name", K(config_name), K(ret));
  } else if (OB_FAIL(get_unit_config_by_name(config_name, config))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_unit_config_by_name failed", K(config_name), K(ret));
    } else {
      ret = OB_RESOURCE_UNIT_NOT_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_NOT_EXIST, helper.convert(config_name));
      LOG_WARN("config not exist", K(config_name), K(ret));
    }
  } else if (NULL == config) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config is null", KP(config), K(ret));
  } else if (OB_SUCCESS == (ret = inner_get_resource_pool_by_name(resource_pool.name_, pool))) {
    if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", K_(resource_pool.name), KP(pool), K(ret));
    } else if (if_not_exist) {
      ObCStringHelper helper;
      LOG_USER_NOTE(OB_RESOURCE_POOL_EXIST, helper.convert(resource_pool.name_));
      LOG_INFO("resource_pool already exist, no need to create", K(resource_pool.name_));
    } else {
      ret = OB_RESOURCE_POOL_EXIST;
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_POOL_EXIST, helper.convert(resource_pool.name_));
      LOG_WARN("resource_pool already exist", "name", resource_pool.name_, K(ret));
    }
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("get resource pool by name failed", "name", resource_pool.name_, K(ret));
  } else {
    ret = OB_SUCCESS;
    common::ObMySQLTransaction trans;
    share::ObResourcePool *new_pool = NULL;
    if (NULL == (new_pool = pool_allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else {
      if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("start transaction failed", K(ret));
      } else {
        if (OB_FAIL(new_pool->assign(resource_pool))) {
          LOG_WARN("failed to assign new_pool", K(ret));
        } else {
          new_pool->unit_config_id_ = config->unit_config_id();
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, *new_pool))) {
          LOG_WARN("update_resource_pool failed", "resource_pool", *new_pool, K(ret));
        } else if (OB_FAIL(update_pool_map(new_pool))) {
          LOG_WARN("update pool map failed", "resource_pool", *new_pool, K(ret));
        }
      }

      if (trans.is_started()) {
        const bool commit = (OB_SUCC(ret));
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
          LOG_WARN("trans end failed", K(commit), K(temp_ret));
          ret = (OB_SUCCESS == ret) ? temp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_UNIT_MANAGER) OB_SUCCESS;
        DEBUG_SYNC(UNIT_MANAGER_WAIT_FOR_TIMEOUT);
      }

      if (OB_FAIL(ret)) {
        if (OB_INVALID_ID == new_pool->resource_pool_id_) {
          // do nothing, fetch new resource pool id failed
        } else {
          int temp_ret = OB_SUCCESS; // avoid ret overwritten
          // some error occur during doing the transaction, rollback change occur in memory
          ObArray<ObUnit *> *units = NULL;
          if (OB_SUCCESS == (temp_ret = get_units_by_pool(new_pool->resource_pool_id_, units))) {
            if (NULL == units) {
              temp_ret = OB_ERR_UNEXPECTED;
              LOG_WARN("units is null", KP(units), K(temp_ret));
            } else if (OB_SUCCESS != (temp_ret = delete_units_of_pool(
                        new_pool->resource_pool_id_))) {
              LOG_WARN("delete_units_of_pool failed", "resource_pool_id",
                       new_pool->resource_pool_id_, K(temp_ret));
            }
          } else if (OB_ENTRY_NOT_EXIST != temp_ret) {
            LOG_WARN("get_units_by_pool failed",
                     "resource_pool_id", new_pool->resource_pool_id_, K(temp_ret));
          } else {
            temp_ret = OB_SUCCESS;
          }

          share::ObResourcePool *temp_pool = NULL;
          if (OB_SUCCESS != temp_ret) {
          } else if (OB_SUCCESS != (temp_ret = get_resource_pool_by_id(
                      new_pool->resource_pool_id_, temp_pool))) {
            if (OB_ENTRY_NOT_EXIST != temp_ret) {
              LOG_WARN("get_resource_pool_by_id failed", "pool_id", new_pool->resource_pool_id_,
                       K(temp_ret));
            } else {
              temp_ret = OB_SUCCESS;
              // do nothing, no need to delete from id_map and name_map
            }
          } else if (NULL == temp_pool) {
            temp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("temp_pool is null", KP(temp_pool), K(temp_ret));
          } else if (OB_SUCCESS != (temp_ret = delete_resource_pool(
                      new_pool->resource_pool_id_, new_pool->name_))) {
            LOG_WARN("delete_resource_pool failed", "new pool", *new_pool, K(temp_ret));
          }
        }
        // avoid memory leak
        pool_allocator_.free(new_pool);
        new_pool = NULL;
      } else {
        // inc unit config ref count at last
        if (OB_FAIL(inc_config_ref_count(config->unit_config_id()))) {
          LOG_WARN("inc_config_ref_count failed", "config id", config->unit_config_id(), K(ret));
        } else if (OB_FAIL(insert_config_pool(config->unit_config_id(), new_pool))) {
          LOG_WARN("insert config pool failed", "config id", config->unit_config_id(), K(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("unit", "create_resource_pool",
                                "name", new_pool->name_,
                                "unit", config_name,
                                "zone_list", new_pool->zone_list_);
        }
      }
    }
  }
  LOG_INFO("finish inner create resource pool", KR(ret), K(resource_pool), K(config_name));
  return ret;
}

/* when expand zone resource for tenant this func is invoked,
 * we need to check whether the tenant units are in deleting.
 * if any tenant unit is in deleting,
 * @is_allowed returns false
 */
int ObUnitManager::check_expand_zone_resource_allowed_by_old_unit_stat_(
    const uint64_t tenant_id,
    bool &is_allowed)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *cur_pool_array = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    int tmp_ret = get_pools_by_tenant_(tenant_id, cur_pool_array);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      is_allowed = true;
    } else if (OB_UNLIKELY(nullptr == cur_pool_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_pool is null", KR(ret), K(tenant_id));
    } else {
      is_allowed = true;
      for (int64_t i = 0; is_allowed && OB_SUCC(ret) && i < cur_pool_array->count(); ++i) {
        share::ObResourcePool *cur_pool = cur_pool_array->at(i);
        ObArray<share::ObUnit *> *units = nullptr;
        if (OB_UNLIKELY(nullptr == cur_pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur pool is null", KR(ret));
        } else if (OB_FAIL(get_units_by_pool(cur_pool->resource_pool_id_, units))) {
          LOG_WARN("fail to get units by pool", KR(ret),
                   "pool_id", cur_pool->resource_pool_id_);
        } else if (OB_UNLIKELY(nullptr == units)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("units ptrs is null", KR(ret), K(tenant_id), KPC(cur_pool));
        } else {
          for (int64_t j = 0; is_allowed && OB_SUCC(ret) && j < units->count(); ++j) {
            if (OB_UNLIKELY(nullptr == units->at(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("units ptrs is null", KR(ret), K(tenant_id), KPC(cur_pool));
            } else {
              is_allowed = ObUnit::UNIT_STATUS_ACTIVE == units->at(j)->status_;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_expand_zone_resource_allowed_by_new_unit_stat_(
    const common::ObIArray<share::ObResourcePoolName> &pool_names)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
    share::ObResourcePool *pool = NULL;
    ObArray<ObUnit *> *units = nullptr;
    if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
      LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), K(ret));
    } else if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", KP(pool), K(ret));
    } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
      LOG_WARN("fail to get units by pool", K(ret));
    } else if (OB_UNLIKELY(nullptr == units)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units ptr is null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < units->count(); ++j) {
        ObUnit *unit = units->at(j);
        if (OB_UNLIKELY(nullptr == unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", KR(ret), KPC(pool));
        } else if (unit->status_ != ObUnit::UNIT_STATUS_ACTIVE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected unit status", KR(ret), KPC(pool), KPC(unit));
        } else {/* good */}
      }
    }
  }
  return ret;
}

// check if data_disk_size of tenant's resource pools are all zero or all non-zero
int ObUnitManager::check_expand_zone_resource_allowed_by_data_disk_size_(
    const uint64_t tenant_id,
    const common::ObIArray<share::ObResourcePoolName> &pool_names)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || pool_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(pool_names));
  } else if (!GCTX.is_shared_storage_mode()) {
    // check pass
  } else {
    bool is_data_disk_size_zero = false;
    // check new pools
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      share::ObResourcePool *pool = NULL;
      ObUnitConfig *unit_config = nullptr;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), KR(ret));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KR(ret));
      } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, unit_config))) {
        LOG_WARN("fail to get unit config by pool", KR(ret));
      } else if (OB_ISNULL(unit_config)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_config ptr is null", KR(ret));
      } else if (0 == i) {
        is_data_disk_size_zero = (0 == unit_config->data_disk_size());
      } else if (is_data_disk_size_zero != (0 == unit_config->data_disk_size())) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("data_disk_size of tenant should be all-zero or all-nonzero", KR(ret),
                 K(is_data_disk_size_zero), KPC(pool), KPC(unit_config));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The DATA_DISK_SIZE for all resource pools of a tenant must be consistently "
                       "set to either zero or non-zero values. Mixed configurations are");
      } else { /*good*/ }
    }
    // check curr pools
    ObArray<share::ObResourcePool *> *curr_pools = nullptr;
    if (FAILEDx(get_pools_by_tenant_(tenant_id, curr_pools))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get pools by tenant", KR(ret), K(tenant_id));
      }
    } else if (OB_ISNULL(curr_pools)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("curr_pools is null", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < curr_pools->count(); ++i) {
        share::ObResourcePool *pool = curr_pools->at(i);
        ObUnitConfig *unit_config = nullptr;
        if (OB_ISNULL(pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool is null ptr", KR(ret));
        } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, unit_config))) {
          LOG_WARN("fail to get unit config by pool", KR(ret));
        } else if (OB_ISNULL(unit_config)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit_config ptr is null", KR(ret));
        } else if (is_data_disk_size_zero != (0 == unit_config->data_disk_size())) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("data_disk_size of tenant should be all-zero or all-nonzero", KR(ret),
                  K(is_data_disk_size_zero), KPC(pool), KPC(unit_config));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The DATA_DISK_SIZE for all resource pools of a tenant must be consistently "
                        "set to either zero or non-zero values. Mixed configurations are");
        } else { /*good*/ }
      }
    }
  }
  return ret;
}

/* 1 when this is a tenant being created:
 *   check the input pools, each input pool unit num shall be equal, otherwise illegal
 * 2 when this is a tenant which exists:
 *   check the input pools, each input pool unit num shall be equal to the pools already granted to the tenant.
 */
int ObUnitManager::check_tenant_pools_unit_num_legal_(
    const uint64_t tenant_id,
    const common::ObIArray<share::ObResourcePoolName> &input_pool_names,
    bool &unit_num_legal,
    int64_t &sample_unit_num)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *cur_pool_array = nullptr;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                        || input_pool_names.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    unit_num_legal = true;
    sample_unit_num = -1;
    int tmp_ret = get_pools_by_tenant_(tenant_id, cur_pool_array);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // when create tenant pools belong to this tenant is empty
    } else if (OB_UNLIKELY(nullptr == cur_pool_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_pool is null", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(cur_pool_array->count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool_array", KR(ret));
    } else if (OB_UNLIKELY(nullptr == cur_pool_array->at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool ptr is null", KR(ret));
    } else {
      sample_unit_num = cur_pool_array->at(0)->unit_count_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && unit_num_legal && i < input_pool_names.count(); ++i) {
      share::ObResourcePool *pool = NULL;
      if (OB_FAIL(inner_get_resource_pool_by_name(input_pool_names.at(i), pool))) {
        LOG_WARN("fail to get pool by name", KR(ret), "pool_name", input_pool_names.at(i));
      } else if (OB_UNLIKELY(nullptr == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", KR(ret), "pool_name", input_pool_names.at(i));
      } else if (-1 == sample_unit_num) {
        sample_unit_num = pool->unit_count_;
      } else if (sample_unit_num == pool->unit_count_) {
        // this is good, unit num matched
      } else {
        unit_num_legal = false;
      }
    }
  }
  return ret;
}

/* get unit group id for a tenant
 * 1 when bootstrap:
 *   generate unit group id for sys unit group, assign 0 directly
 * 2 when revoke pools for tenant:
 *   after revokes pools, unit group id shall be set to 0 which representing these units
 *   is not belong to any unit group
 * 3 when grant pools for tenant:
 *   3.1 when this is a tenant being created: fetch unit group id from inner table
 *   3.2 when this is a tenant which exists: get unit group id from units granted to this tenant
 */
int ObUnitManager::get_tenant_pool_unit_group_id_(
    const bool is_bootstrap,
    const bool grant,
    const uint64_t tenant_id,
    const int64_t unit_group_num,
    common::ObIArray<uint64_t> &new_unit_group_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || unit_group_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(unit_group_num));
  } else if (is_bootstrap) {
    if (OB_FAIL(new_unit_group_id_array.push_back(OB_SYS_UNIT_GROUP_ID))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

// TODO(cangming.zl): need a new function to abstract the process of getting pools by pool_names
// TODO: disable resource pools intersect for one tenant
//       NEED to disable logics to handle resource pool intersect in server_balancer
int ObUnitManager::grant_pools(common::ObMySQLTransaction &trans,
                               common::ObIArray<uint64_t> &new_unit_group_id_array,
                               const lib::Worker::CompatMode compat_mode,
                               const ObIArray<ObResourcePoolName> &pool_names,
                               const uint64_t tenant_id,
                               const bool is_bootstrap,
                               /*arg "const bool skip_offline_server" is no longer supported*/
                               const bool check_data_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const bool grant = true;
  bool intersect = false;
  bool is_grant_pool_allowed = false;
  bool unit_num_legal = false;
  int64_t legal_unit_num = -1;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), KR(ret));
  } else if (pool_names.count() <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_names), K(tenant_id), K(ret));
  } else if (OB_FAIL(check_pool_ownership_(tenant_id, pool_names, true/*is_grant*/))) {
    LOG_WARN("check pool ownership failed", KR(ret), K(pool_names));
  } else if (OB_FAIL(check_pool_intersect_(tenant_id, pool_names, intersect))) {
    LOG_WARN("check pool intersect failed", K(pool_names), KR(ret));
  } else if (intersect) {
    ret = OB_POOL_SERVER_INTERSECT;
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_POOL_SERVER_INTERSECT, helper.convert(pool_names));
    LOG_WARN("resource pool unit server intersect", K(pool_names), KR(ret));
  } else if (OB_FAIL(check_expand_zone_resource_allowed_by_old_unit_stat_(
          tenant_id, is_grant_pool_allowed))) {
    LOG_WARN("fail to check grant pools allowed by unit stat", KR(ret), K(tenant_id));
  } else if (!is_grant_pool_allowed) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "grant pool when pools in shrinking");
  } else if (OB_FAIL(check_expand_zone_resource_allowed_by_new_unit_stat_(pool_names))) {
    LOG_WARN("fail to check grant pools allowed by unit stat", KR(ret));
  } else if (OB_FAIL(check_expand_zone_resource_allowed_by_data_disk_size_(tenant_id, pool_names))) {
    LOG_WARN("fail to check grant pools allowed by data_disk_size", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_pools_unit_num_legal_(
          tenant_id, pool_names, unit_num_legal, legal_unit_num))) {
    LOG_WARN("fail to check pools unit num legal", KR(ret), K(tenant_id), K(pool_names));
  } else if (!unit_num_legal) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("pools belong to one tenant with different unit num are not allowed",
             KR(ret), K(tenant_id), K(pool_names));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "pools belong to one tenant with different unit num are");
  } else if (OB_FAIL(get_tenant_pool_unit_group_id_(
          is_bootstrap, grant, tenant_id, legal_unit_num, new_unit_group_id_array))) {
    LOG_WARN("fail to generate new unit group id", KR(ret), K(tenant_id), K(legal_unit_num));
  } else if (OB_FAIL(do_grant_pools_(
             trans, new_unit_group_id_array, compat_mode,
             pool_names, tenant_id, is_bootstrap, check_data_version))) {
    LOG_WARN("do grant pools failed", KR(ret), K(grant), K(pool_names), K(tenant_id),
                                         K(compat_mode), K(is_bootstrap));
  }
  LOG_INFO("grant resource pools to tenant", KR(ret), K(pool_names), K(tenant_id), K(is_bootstrap));
  return ret;
}

int ObUnitManager::get_tenant_alive_servers_non_block(const uint64_t tenant_id,
                                                      common::ObIArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObArray<ObUnit> tenant_units;
    uint64_t valid_tnt_id = is_meta_tenant(tenant_id) ? gen_user_tenant_id(tenant_id) : tenant_id;
    // Try lock and get units from inmemory data. If lock failed, get from inner table.
    if (lock_.try_rdlock()) {
      // Get from inmemory data
      ObArray<share::ObResourcePool *> *pools = nullptr;
      if (OB_FAIL(check_inner_stat_())) {
        LOG_WARN("check_inner_stat failed", KR(ret), K(inited_), K(loaded_));
      } else if (OB_FAIL(get_pools_by_tenant_(valid_tnt_id, pools))) {
        LOG_WARN("failed to get pools by tenant", KR(ret), K(valid_tnt_id));
      } else if (OB_ISNULL(pools)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pools is nullptr", KR(ret), KP(pools));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
          const share::ObResourcePool *pool = pools->at(i);
          ObArray<ObUnit *> *pool_units;
          if (OB_ISNULL(pool)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pool is nullptr", KR(ret), KP(pool));
          } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, pool_units))) {
            LOG_WARN("fail to get_units_by_pool", KR(ret),
                    "pool_id", pool->resource_pool_id_);
          } else if (OB_ISNULL(pool_units)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pool_units is nullptr", KR(ret), KP(pool_units));
          } else {
            ARRAY_FOREACH_X(*pool_units, idx, cnt, OB_SUCC(ret)) {
              const ObUnit *unit = pool_units->at(idx);
              if (OB_ISNULL(unit)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unit is nullptr", KR(ret), KP(unit));
              } else if (OB_FAIL(tenant_units.push_back(*unit))) {
                LOG_WARN("failed to push_back unit", KR(ret), KPC(unit));
              }
            }
          }
        }
      }
      lock_.unlock();
    } else {
      // Get from inner_table
      tenant_units.reuse();
      if (OB_FAIL(ut_operator_.get_units_by_tenant(valid_tnt_id, tenant_units))) {
        LOG_WARN("fail to get_units_by_tenant from inner_table",
                KR(ret), K(tenant_id), K(valid_tnt_id));
      }
    }

    // Filter alive servers
    if (OB_SUCC(ret)) {
      servers.reuse();
      FOREACH_X(unit, tenant_units, OB_SUCC(ret)) {
        if (has_exist_in_array(servers, unit->server_)) {
          // server exist
        } else if (OB_FAIL(servers.push_back(unit->server_))) {
          LOG_WARN("push_back failed", KR(ret), K(unit->server_));
        }
        if (OB_FAIL(ret) || !unit->migrate_from_server_.is_valid()) {
          // skip
        } else {
          if (has_exist_in_array(servers, unit->migrate_from_server_)) {
            // server exist
          } else if (OB_FAIL(servers.push_back(unit->migrate_from_server_))) {
            LOG_WARN("push_back failed", KR(ret), K(unit->migrate_from_server_));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_unit_config_by_pool_name(
    const ObString &pool_name,
    share::ObUnitConfig &unit_config) const
{
  int ret = OB_SUCCESS;
  share::ObResourcePool *pool = NULL;
  ObUnitConfig *config = NULL;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_FAIL(inner_get_resource_pool_by_name(pool_name, pool))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get resource pool by name failed", K(pool_name), K(ret));
    } else {
      ret = OB_RESOURCE_POOL_NOT_EXIST;
      LOG_WARN("pool not exist", K(ret), K(pool_name));
    }
  } else if (NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is null", KP(pool), K(ret));
  } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, config))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_RESOURCE_UNIT_NOT_EXIST;
    }
    LOG_WARN("can not find config for unit",
             "unit_config_id", pool->unit_config_id_,
             K(ret));
  } else if (NULL == config) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config is null", KP(config), K(ret));
  } else {
    unit_config = *config;
  }
  return ret;
}

int ObUnitManager::create_sys_units(const ObIArray<ObUnit> &sys_units)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (sys_units.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sys_units is empty", K(sys_units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_units.count(); ++i) {
      if (OB_FAIL(add_unit(*proxy_, sys_units.at(i)))) {
        LOG_WARN("add_unit failed", "unit", sys_units.at(i), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::check_server_empty(const ObAddr &server, bool &empty) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  ObArray<ObUnitLoad> *loads = NULL;
  empty = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(get_loads_by_server(server, loads))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_loads_by_server failed", K(server), K(ret));
    } else {
      ret = OB_SUCCESS;
      empty = true;
    }
  } else if (NULL == loads) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("loads is null", KP(loads), K(ret));
  } else {
    empty = false;
  }
  return ret;
}

int ObUnitManager::inner_get_unit_info_by_id(const uint64_t unit_id, ObUnitInfo &unit_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(ret));
  } else {
    ObUnit *unit = NULL;
    ObUnitConfig *config = NULL;
    share::ObResourcePool  *pool = NULL;
    if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
      LOG_WARN("get_unit_by_id failed", K(unit_id), K(ret));
    } else if (NULL == unit) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit is null", KP(unit), K(ret));
    } else if (OB_FAIL(get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_RESOURCE_POOL_NOT_EXIST;
      }
      LOG_WARN("get_resource_pool_by_id failed", "pool id", unit->resource_pool_id_, K(ret));
    } else if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", KP(pool), K(ret));
    } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, config))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_RESOURCE_UNIT_NOT_EXIST;
      }
      LOG_WARN("get_unit_config_by_id failed", "unit config id", pool->unit_config_id_, K(ret));
    } else if (NULL == config) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config is null", KP(config), K(ret));
    } else {
      if (OB_FAIL(unit_info.pool_.assign(*pool))) {
        LOG_WARN("failed to assign unit_info.pool_", K(ret));
      } else {
        unit_info.unit_ = *unit;
        unit_info.config_ = *config;
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_UNIT_PERSISTENCE_ERROR);

int ObUnitManager::add_unit(ObISQLClient &client, const ObUnit &unit)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit), K(ret));
  } else {
    ObUnit *new_unit = NULL;
    if (OB_SUCCESS == (ret = get_unit_by_id(unit.unit_id_, new_unit))) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("unit already exist, can't add", K(unit), K(ret));
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_unit_by_id failed", "unit_id", unit.unit_id_, K(ret));
    } else {
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(ret)) {
    } else if (NULL == (new_unit = allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc unit failed", K(ret));
    } else {
      if (OB_FAIL(ut_operator_.update_unit(client, unit))) {
        LOG_WARN("update_unit failed", K(unit), K(ret));
      } else {
        *new_unit = unit;
        if (OB_FAIL(insert_unit(new_unit))) {
          LOG_WARN("insert_unit failed", "new unit", *new_unit, K(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("unit", "create_unit",
              "unit_id", unit.unit_id_,
              "server", unit.server_);
        }
      }
      if (OB_FAIL(ret)) {
        //avoid memory leak
        allocator_.free(new_unit);
        new_unit = NULL;
      }
    }
  }
  return ret;
}

// The zones of multiple pools have no intersection
// 14x new semantics. If it is the source_pool used to store the copy of L, it can be compared
int ObUnitManager::check_pool_intersect_(
    const uint64_t tenant_id,
    const ObIArray<ObResourcePoolName> &pool_names,
    bool &intersect)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, OB_DEFAULT_REPLICA_NUM> zones;
  common::ObArray<share::ObResourcePool *> *pools = NULL;;
  intersect = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), KR(ret));
  } else if (pool_names.count() <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool_names is empty", K(pool_names), K(tenant_id), KR(ret));
  } else {
    FOREACH_CNT_X(pool_name, pool_names, OB_SUCCESS == ret && !intersect) {
      share::ObResourcePool *pool = NULL;
      if (OB_FAIL(inner_get_resource_pool_by_name(*pool_name, pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", *pool_name, KR(ret));
      } else if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), KR(ret));
      } else {
        FOREACH_CNT_X(zone, pool->zone_list_, OB_SUCCESS == ret && !intersect) {
          if (NULL == zone) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit is null", KR(ret));
          } else {
            ObString zone_str;
            zone_str.assign_ptr(zone->ptr(), static_cast<int32_t>(zone->size()));
            if (has_exist_in_array(zones, zone_str)) {
              intersect = true;
            } else if (OB_FAIL(zones.push_back(zone_str))) {
              LOG_WARN("push_back failed", KR(ret));
            }
          }
        }  // end foreach zone
      }
    }  // end foreach pool
    if (OB_FAIL(ret)) {
    } else if (intersect) {
    } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // a new tenant, without resource pool already granted
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get pools by tenant", KR(ret), K(tenant_id));
      }
    } else if (OB_UNLIKELY(NULL == pools)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KR(ret), KP(pools));
    } else {
      for (int64_t i = 0; !intersect && OB_SUCC(ret) && i < pools->count(); ++i) {
        const share::ObResourcePool *pool = pools->at(i);
        if (OB_UNLIKELY(NULL == pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool ptr is null", KR(ret), KP(pool));
        } else {
          for (int64_t j = 0; !intersect && OB_SUCC(ret) && j < zones.count(); ++j) {
            common::ObZone zone;
            if (OB_FAIL(zone.assign(zones.at(j).ptr()))) {
              LOG_WARN("fail to assign zone", KR(ret));
            } else if (has_exist_in_array(pool->zone_list_, zone)) {
              intersect = true;
            } else {} // good
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_pool_ownership_(const uint64_t tenant_id,
                                        const common::ObIArray<share::ObResourcePoolName> &pool_names,
                                        const bool grant)
{
  int ret = OB_SUCCESS;
  share::ObResourcePool *pool = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
    if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
      LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), KR(ret));
    } else if (OB_ISNULL(pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool is null", KP(pool), KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (grant) {
      if (pool->is_granted_to_tenant()) {
        ret = OB_RESOURCE_POOL_ALREADY_GRANTED;
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_POOL_ALREADY_GRANTED, helper.convert(pool_names.at(i)));
        LOG_WARN("pool has already granted to other tenant, can't grant again",
                  KR(ret), K(tenant_id), "pool", *pool);
      } else {/*good*/}
    } else {
      if (!pool->is_granted_to_tenant()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find pool not granted to any tenant, can not revoke",
            "pool", *pool, K(tenant_id), KR(ret));
      } else if (pool->tenant_id_ != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find pool already granted to other tenant, can not revoke",
            "pool", *pool, K(tenant_id), KR(ret));
      } else {/*good*/}
    }
  }
  return ret;
}

int ObUnitManager::do_grant_pools_(
    ObMySQLTransaction &trans,
    const common::ObIArray<uint64_t> &new_ug_ids,
    const lib::Worker::CompatMode compat_mode,
    const ObIArray<ObResourcePoolName> &pool_names,
    const uint64_t tenant_id,
    const bool is_bootstrap,
    const bool check_data_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || pool_names.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(pool_names), KR(ret));
  } else if (OB_UNLIKELY(nullptr == srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ ptr is null", KR(ret));
  } else {
    ObNotifyTenantServerResourceProxy notify_proxy(
                                      *srv_rpc_proxy_,
                                      &obrpc::ObSrvRpcProxy::notify_tenant_server_unit_resource);
    share::ObResourcePool new_pool;
    ObArray<ObUnit> pool_units;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      share::ObResourcePool *pool = NULL;
      pool_units.reset();
      common::ObArray<ObUnit *> zone_sorted_unit_array;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        LOG_WARN("get resource pool by name failed", "pool_name", pool_names.at(i), KR(ret));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), KR(ret));
      } else if (pool->unit_count_ != new_ug_ids.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count not match", KR(ret), K(tenant_id),
                 "pool", new_pool, K(new_ug_ids));
      } else if (OB_FAIL(new_pool.assign(*pool))) {
        LOG_WARN("failed to assign new_pool", KR(ret));
      } else if (FALSE_IT(new_pool.tenant_id_ = tenant_id)) {
        // shall never be here
      } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
        LOG_WARN("update_resource_pool failed", K(new_pool), KR(ret));
      } else if (is_bootstrap) {
        //no need to notify unit and modify unit group id
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", KR(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      // arg/dest/result can be used here.
    }
    ret = ERRSIM_UNIT_PERSISTENCE_ERROR ? : ret;
  }
  return ret;
}

#define INSERT_ITEM_TO_MAP(map, key, pvalue) \
  do { \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(map.set_refactored(key, pvalue))) { \
      if (OB_HASH_EXIST == ret) { \
        LOG_WARN("key already exist", K(key), K(ret)); \
      } else { \
        LOG_WARN("map set failed", K(ret)); \
      } \
    } else { \
    } \
  } while (false)

#define SET_ITEM_TO_MAP(map, key, value) \
  do { \
    const int overwrite = 1; \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(map.set_refactored(key, value, overwrite))) { \
      LOG_WARN("map set failed", K(ret)); \
    } else { \
    } \
  } while (false)

#define INSERT_ARRAY_TO_MAP(map, key, array) \
  do { \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(map.set_refactored(key, array))) { \
      if (OB_HASH_EXIST == ret) { \
        LOG_WARN("key already exist", K(key), K(ret)); \
      } else { \
        LOG_WARN("map set failed", K(ret)); \
      } \
    } else { \
    } \
  } while (false)

int ObUnitManager::build_unit_map(const ObIArray<ObUnit> &units)
{
  int ret = OB_SUCCESS;
  // units is empty if invoked during bootstrap
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(id_unit_map_.clear())) {
    LOG_WARN("id_unit_map_ clear failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
      ObUnit *unit = NULL;
      if (NULL == (unit = allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc unit failed", K(ret));
      } else {
        *unit = units.at(i);
        if (OB_FAIL(insert_unit(unit))) {
          LOG_WARN("insert_unit failed", "unit", *unit, K(ret));
        }

        if (OB_FAIL(ret)) {
          //avoid memory leak
          allocator_.free(unit);
          unit = NULL;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::build_config_map(const ObIArray<ObUnitConfig> &configs)
{
  int ret = OB_SUCCESS;
  // configs is empty if invoked during bootstrap
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(id_config_map_.clear())) {
    LOG_WARN("id_config_map_ clear failed", K(ret));
  } else if (OB_FAIL(name_config_map_.clear())) {
    LOG_WARN("name_config_map_ clear failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < configs.count(); ++i) {
      ObUnitConfig *config = NULL;
      if (NULL == (config = config_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc unit config failed", K(ret));
      } else {
        *config = configs.at(i);
        if (OB_FAIL(insert_unit_config(config))) {
          LOG_WARN("insert_unit_config failed", KP(config), K(ret));
        }

        if (OB_FAIL(ret)) {
          config_allocator_.free(config);
          config = NULL;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::build_pool_map(const ObIArray<share::ObResourcePool> &pools)
{
  int ret = OB_SUCCESS;
  // pools is empty if invoked during bootstrap
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(id_pool_map_.clear())) {
    LOG_WARN("id_pool_map_ clear failed", K(ret));
  } else if (OB_FAIL(name_pool_map_.clear())) {
    LOG_WARN("name_pool_map_ clear failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools.count(); ++i) {
      share::ObResourcePool *pool = NULL;
      if (NULL == (pool = pool_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc resource pool failed", K(ret));
      } else {
        if (OB_FAIL(pool->assign(pools.at(i)))) {
          LOG_WARN("failed to assign pool", K(ret));
        } else {
          INSERT_ITEM_TO_MAP(id_pool_map_, pool->resource_pool_id_, pool);
          INSERT_ITEM_TO_MAP(name_pool_map_, pool->name_, pool);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(inc_config_ref_count(pool->unit_config_id_))) {
            LOG_WARN("inc_config_ref_count failed", "config id", pool->unit_config_id_, K(ret));
          } else if (OB_FAIL(insert_config_pool(pool->unit_config_id_, pool))) {
            LOG_WARN("insert config pool failed", "config id", pool->unit_config_id_, K(ret));
          }
        }

        if (OB_FAIL(ret)) {
          pool_allocator_.free(pool);
          pool = NULL;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::insert_unit_config(ObUnitConfig *config)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == config) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config is null", KP(config), K(ret));
  } else if (!config->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config", "config", *config, K(ret));
  } else {
    INSERT_ITEM_TO_MAP(id_config_map_, config->unit_config_id(), config);
    INSERT_ITEM_TO_MAP(name_config_map_, config->name(), config);
    int64_t ref_count = 0;
    SET_ITEM_TO_MAP(config_ref_count_map_, config->unit_config_id(), ref_count);
  }
  return ret;
}

int ObUnitManager::inc_config_ref_count(const uint64_t config_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config_id", K(config_id), K(ret));
  } else {
    int64_t ref_count = 0;
    if (OB_FAIL(get_config_ref_count(config_id, ref_count))) {
      LOG_WARN("get_config_ref_count failed", K(config_id), K(ret));
    } else {
      ++ref_count;
      SET_ITEM_TO_MAP(config_ref_count_map_, config_id, ref_count);
    }
  }
  return ret;
}

int ObUnitManager::dec_config_ref_count(const uint64_t config_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config_id", K(config_id), K(ret));
  } else {
    int64_t ref_count = 0;
    if (OB_FAIL(get_config_ref_count(config_id, ref_count))) {
      LOG_WARN("get_config_ref_count failed", K(config_id), K(ret));
    } else {
      --ref_count;
      SET_ITEM_TO_MAP(config_ref_count_map_, config_id, ref_count);
    }
  }
  return ret;
}

int ObUnitManager::update_pool_map(share::ObResourcePool *resource_pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == resource_pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resource_pool is null", KP(resource_pool), K(ret));
  } else if (!resource_pool->is_valid() || OB_INVALID_ID == resource_pool->resource_pool_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource_pool", "resource_pool", *resource_pool, K(ret));
  } else {
    INSERT_ITEM_TO_MAP(id_pool_map_, resource_pool->resource_pool_id_, resource_pool);
    INSERT_ITEM_TO_MAP(name_pool_map_, resource_pool->name_, resource_pool);
  }
  return ret;
}

int ObUnitManager::insert_unit(ObUnit *unit)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_INFO("not init", K(ret));
  } else if (NULL == unit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (!unit->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", "unit", *unit, K(ret));
  } else {
    ObArray<ObUnit *> *units = NULL;
    if (OB_FAIL(get_units_by_pool(unit->resource_pool_id_, units))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_units_by_pool failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (NULL == (units = pool_unit_allocator_.alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc ObArray<ObUnit *> failed", K(ret));
        } else {
          if (OB_FAIL(units->push_back(unit))) {
            LOG_WARN("push_back failed", K(ret));
          } else {
            INSERT_ARRAY_TO_MAP(pool_unit_map_, unit->resource_pool_id_, units);
          }
          if (OB_FAIL(ret)) {
            pool_unit_allocator_.free(units);
            units = NULL;
          }
        }
      }
    } else if (NULL == units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units is null", KP(units), K(ret));
    } else if (OB_FAIL(units->push_back(unit))) {
      LOG_WARN("push_back failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      INSERT_ITEM_TO_MAP(id_unit_map_, unit->unit_id_, unit);

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(insert_unit_loads(unit))) {
        LOG_WARN("insert_unit_loads failed", "unit", *unit, K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::insert_unit_loads(ObUnit *unit)
{
  int ret = OB_SUCCESS;
  ObUnitLoad load;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == unit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (!unit->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", "unit", *unit, K(ret));
  } else if (OB_FAIL(gen_unit_load(unit, load))) {
    LOG_WARN("gen_unit_load failed", "unit", *unit, K(ret));
  } else if (OB_FAIL(insert_unit_load(unit->server_, load))) {
    LOG_WARN("insert_unit_load failed", "server", unit->server_, K(ret));
  } else {
    if (OB_SUCCESS == ret && unit->migrate_from_server_.is_valid()) {
      if (OB_FAIL(insert_unit_load(unit->migrate_from_server_, load))) {
        LOG_WARN("insert_unit_load failed", "server", unit->migrate_from_server_, K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::insert_unit_load(const ObAddr &server, const ObUnitLoad &load)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitLoad> *loads = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || !load.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(load), K(ret));
  } else if (OB_FAIL(get_loads_by_server(server, loads))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_loads_by_server failed", K(server), K(ret));
    } else {
      ret = OB_SUCCESS;
      // not exist, alloc new array, add to hash map
      if (NULL == (loads = load_allocator_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc ObArray<ObUnitLoad> failed", K(ret));
      } else {
        if (OB_FAIL(loads->push_back(load))) {
          LOG_WARN("push_back failed", K(ret));
        } else if (OB_FAIL(insert_load_array(server, loads))) {
          LOG_WARN("insert_unit_load failed", K(server), K(ret));
        }
        if (OB_FAIL(ret)) {
          // avoid memory leak
          load_allocator_.free(loads);
          loads = NULL;
        }
      }
    }
  } else if (NULL == loads) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("loads is null", KP(loads), K(ret));
  } else {
    if (OB_FAIL(loads->push_back(load))) {
      LOG_WARN("push_back failed", K(ret));
    }
  }
  return ret;
}

int ObUnitManager::insert_load_array(const ObAddr &addr, ObArray<ObUnitLoad> *loads)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid() || NULL == loads) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr), KP(loads), K(ret));
  } else if (loads->count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("loads is empty", "loads count", loads->count(), K(ret));
  } else {
    if (OB_FAIL(server_loads_.set_refactored(addr, loads))) {
      if (OB_HASH_EXIST == ret) {
        LOG_WARN("load array is not expect to exist", K(addr), K(ret));
      } else {
        LOG_WARN("set failed", K(addr), K(ret));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObUnitManager::gen_unit_load(ObUnit *unit, ObUnitLoad &load) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == unit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is null", KP(unit), K(ret));
  } else if (!unit->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is invalid", "unit", *unit, K(ret));
  } else {
    ObUnitConfig *config = NULL;
    share::ObResourcePool *pool = NULL;
    if (OB_FAIL(get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
      LOG_WARN("get_resource_pool_by_id failed", "pool id", unit->resource_pool_id_, K(ret));
    } else if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pool), K(ret));
    } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, config))) {
      LOG_WARN("get_unit_config_by_id failed", "unit config id", pool->unit_config_id_, K(ret));
    } else if (NULL == config) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config is null", KP(config), K(ret));
    } else {
      load.unit_ = unit;
      load.pool_ = pool;
      load.unit_config_ = config;
    }
  }
  return ret;
}


int ObUnitManager::insert_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KP(pool), K(ret));
  } else if (OB_FAIL(insert_id_pool(tenant_pools_map_,
      tenant_pools_allocator_, tenant_id, pool))) {
    LOG_WARN("insert tenant pool failed", K(tenant_id), KP(pool), K(ret));
  }

  return ret;
}

int ObUnitManager::insert_config_pool(const uint64_t config_id, share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), KP(pool), K(ret));
  } else if (OB_FAIL(insert_id_pool(config_pools_map_,
      config_pools_allocator_, config_id, pool))) {
    LOG_WARN("insert config pool failed", K(config_id), KP(pool), K(ret));
  }

  return ret;
}

int ObUnitManager::insert_id_pool(
    common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
    common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > &allocator,
    const uint64_t id,
    share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || OB_INVALID_ID == id || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(id), KP(pool), K(ret));
  } else {
    ObArray<share::ObResourcePool *> *pools = NULL;
    if (OB_FAIL(get_pools_by_id(map, id, pools))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_pools_by_id failed", K(id), K(ret));
      } else {
        ret = OB_SUCCESS;
        if (NULL == (pools = allocator.alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc pools failed", K(ret));
        } else if (OB_FAIL(pools->push_back(pool))) {
          LOG_WARN("push_back failed", K(ret));
        } else if (OB_FAIL(insert_id_pool_array(map, id, pools))) {
          LOG_WARN("insert_id_pool_array failed", K(id), K(ret));
        }

        // avoid memory leak
        if (OB_SUCCESS != ret && NULL != pools) {
          allocator.free(pools);
          pools = NULL;
        }
      }
    } else if (NULL == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pools), K(ret));
    } else if (!has_exist_in_array(*pools, pool)) {
      if (OB_FAIL(pools->push_back(pool))) {
        LOG_WARN("push_back failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("resource pool already exists", K(ret), K(id), K(pools), K(pool));
    }
  }
  return ret;
}

int ObUnitManager::insert_id_pool_array(
    common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
    const uint64_t id,
    ObArray<share::ObResourcePool *> *pools)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || OB_INVALID_ID == id || NULL == pools) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(id), KP(pools), K(ret));
  } else {
    if (OB_FAIL(map.set_refactored(id, pools))) {
      if (OB_HASH_EXIST == ret) {
        LOG_WARN("pools is not expect to exist", K(id), K(ret));
      } else {
        LOG_WARN("set failed", K(id), K(ret));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

#undef INSERT_ITEM_TO_MAP
#undef SET_ITEM_TO_MAP
#undef INSERT_ARRAY_TO_MAP

#define DELETE_ITEM_FROM_MAP(map, key) \
  do { \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(map.erase_refactored(key))) { \
      LOG_WARN("map erase failed", K(key), K(ret)); \
    } else { \
    } \
  } while (false)

int ObUnitManager::delete_resource_pool(const uint64_t pool_id,
                                        const ObResourcePoolName &name)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pool_id || name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_id), K(name), K(ret));
  } else {
    DELETE_ITEM_FROM_MAP(id_pool_map_, pool_id);
    DELETE_ITEM_FROM_MAP(name_pool_map_, name);
  }
  return ret;
}

int ObUnitManager::delete_units_of_pool(const uint64_t resource_pool_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
    LOG_WARN("fail to get unit by pool", K(resource_pool_id), K(ret));
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units is null", KP(units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      const ObUnit *unit = units->at(i);
      if (OB_UNLIKELY(NULL == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit should not be null", K(ret));
      } else if (OB_FAIL(delete_unit_loads(*unit))) {
        LOG_WARN("fail to delete unit load", K(ret));
      } else {
        DELETE_ITEM_FROM_MAP(id_unit_map_, unit->unit_id_);
        if (OB_SUCC(ret)) {
          ROOTSERVICE_EVENT_ADD("unit", "drop_unit",
                                "unit_id", units->at(i)->unit_id_);
          allocator_.free(units->at(i));
          units->at(i) = NULL;
        }
      }
    }
    if (OB_SUCC(ret)) {
      DELETE_ITEM_FROM_MAP(pool_unit_map_, resource_pool_id);
      if (OB_SUCC(ret)) {
        pool_unit_allocator_.free(units);
        units = NULL;
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_unit_loads(const ObUnit &unit)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit", K(unit), K(ret));
  } else {
    const uint64_t unit_id = unit.unit_id_;
    if (OB_FAIL(delete_unit_load(unit.server_, unit_id))) {
      LOG_WARN("delete_unit_load failed", "server", unit.server_, K(unit_id), K(ret));
    } else {
      if (unit.migrate_from_server_.is_valid()) {
        if (OB_FAIL(delete_unit_load(unit.migrate_from_server_, unit_id))) {
          LOG_WARN("delete_unit_load failed", "server", unit.migrate_from_server_,
              K(unit_id), K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_unit_load(const ObAddr &server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(unit_id), K(ret));
  } else {
    ObArray<ObUnitLoad> *loads = NULL;
    if (OB_FAIL(get_loads_by_server(server, loads))) {
      LOG_WARN("get_loads_by_server failed", K(server), K(ret));
    } else if (NULL == loads) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("loads is null", KP(loads), K(ret));
    } else {
      int64_t index = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < loads->count(); ++i) {
        if (loads->at(i).unit_->unit_id_ == unit_id) {
          index = i;
          break;
        }
      }
      if (-1 == index) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("unit load not exist", K(server), K(unit_id), K(ret));
      } else {
        for (int64_t i = index; i < loads->count() - 1; ++i) {
          loads->at(i) = loads->at(i+1);
        }
        loads->pop_back();
        if (0 == loads->count()) {
          load_allocator_.free(loads);
          loads = NULL;
          DELETE_ITEM_FROM_MAP(server_loads_, server);
        }
      }
    }
  }
  return ret;
}

#undef DELETE_ITEM_FROM_MAP

#define GET_ITEM_FROM_MAP(map, key, value) \
  do { \
    if (OB_FAIL(map.get_refactored(key, value))) { \
      if (OB_HASH_NOT_EXIST == ret) { \
        ret = OB_ENTRY_NOT_EXIST; \
      } else { \
        LOG_WARN("map get failed", K(key), K(ret)); \
      } \
    } else { \
    } \
  } while (false)

int ObUnitManager::get_unit_config_by_name(const ObUnitConfigName &name,
                                           ObUnitConfig *&config) const
{
  int ret = OB_SUCCESS;
  config = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(name), K(ret));
  } else {
    GET_ITEM_FROM_MAP(name_config_map_, name, config);
  }
  return ret;
}

int ObUnitManager::get_unit_config_by_id(const uint64_t config_id, ObUnitConfig *&config) const
{
  int ret = OB_SUCCESS;
  config = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(id_config_map_, config_id, config);
  }
  return ret;
}

int ObUnitManager::get_config_ref_count(const uint64_t config_id, int64_t &ref_count) const
{
  int ret = OB_SUCCESS;
  ref_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(config_ref_count_map_, config_id, ref_count);
    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
      } else {
        LOG_WARN("GET_ITEM_FROM_MAP failed", K(config_id), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_get_resource_pool_by_name(
    const ObResourcePoolName &name,
    share::ObResourcePool *&pool) const
{
  int ret = OB_SUCCESS;
  pool = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(name), K(ret));
  } else {
    GET_ITEM_FROM_MAP(name_pool_map_, name, pool);
  }
  return ret;
}

int ObUnitManager::get_resource_pool_by_id(const uint64_t pool_id, share::ObResourcePool *&pool) const
{
  int ret = OB_SUCCESS;
  pool = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(id_pool_map_, pool_id, pool);
  }
  return ret;
}

int ObUnitManager::get_units_by_pool(const uint64_t pool_id, ObArray<ObUnit *> *&units) const
{
  int ret = OB_SUCCESS;
  units = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(pool_unit_map_, pool_id, units);
  }
  return ret;
}

int ObUnitManager::get_unit_by_id(const uint64_t unit_id, ObUnit *&unit) const
{
  int ret = OB_SUCCESS;
  unit = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(id_unit_map_, unit_id, unit);
  }
  return ret;
}

int ObUnitManager::get_loads_by_server(const ObAddr &addr,
                                       ObArray<ObUnitManager::ObUnitLoad> *&loads) const
{
  int ret = OB_SUCCESS;
  loads = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(addr), K(ret));
  } else {
    GET_ITEM_FROM_MAP(server_loads_, addr, loads);
  }
  return ret;
}

int ObUnitManager::get_pools_by_tenant_(const uint64_t tenant_id,
                                       ObArray<share::ObResourcePool *> *&pools) const
{
  int ret = OB_SUCCESS;
  // meta tenant has no self resource pool, here return user tenant resource pool
  uint64_t valid_tnt_id = is_meta_tenant(tenant_id) ? gen_user_tenant_id(tenant_id) : tenant_id;

  pools = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_pools_by_id(tenant_pools_map_, valid_tnt_id, pools))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_pools_by_id failed", K(valid_tnt_id), K(ret));
    }
  }
  return ret;
}

int ObUnitManager::get_pools_by_id(
    const common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
    const uint64_t id, ObArray<share::ObResourcePool *> *&pools) const
{
  int ret = OB_SUCCESS;
  pools = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || OB_INVALID_ID == id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(id), K(ret));
  } else {
    GET_ITEM_FROM_MAP(map, id, pools);
  }
  return ret;
}

#undef DELETE_ITEM_FROM_MAP

int ObUnitManager::fetch_new_unit_config_id(uint64_t &config_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObMaxIdFetcher id_fetcher(*proxy_);
    uint64_t combine_id = OB_INVALID_ID;
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        OB_SYS_TENANT_ID, OB_MAX_USED_UNIT_CONFIG_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_max_id failed", "id_type", OB_MAX_USED_UNIT_CONFIG_ID_TYPE, K(ret));
    } else {
      config_id = combine_id;
    }
  }
  return ret;
}

int ObUnitManager::fetch_new_unit_id(uint64_t &unit_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    uint64_t combine_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID,
        OB_MAX_USED_UNIT_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_new_max_id failed", "id_type", OB_MAX_USED_UNIT_ID_TYPE, K(ret));
    } else {
      unit_id = combine_id;
    }
  }
  return ret;
}

int ObUnitManager::check_bootstrap_pool(const share::ObResourcePool &pool)
{
  int ret = OB_SUCCESS;
  if (!pool.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pool", K(pool), K(ret));
  } else if (ObUnitConfig::SYS_UNIT_CONFIG_ID == pool.unit_config_id_) {
    // good
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool not sys pool", K(pool), K(ret));
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
