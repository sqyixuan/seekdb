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

int ObUnitManager::ZoneUnitPtr::assign(const ZoneUnitPtr &other)
{
  int ret = OB_SUCCESS;
  zone_ = other.zone_;
  if (OB_FAIL(copy_assign(unit_ptrs_, other.unit_ptrs_))) {
    LOG_WARN("fail to assign unit_ptrs", K(ret));
  }
  return ret;
}

int ObUnitManager::ZoneUnitPtr::sort_by_unit_id_desc()
{
  UnitGroupIdCmp cmp;
  lib::ob_sort(unit_ptrs_.begin(), unit_ptrs_.end(), cmp);
  return cmp.get_ret();
}

int ObUnitManager::ZoneUnit::assign(const ZoneUnit &other)
{
  int ret = OB_SUCCESS;
  zone_ = other.zone_;
  if (OB_FAIL(copy_assign(unit_infos_, other.unit_infos_))) {
    LOG_WARN("failed to assign unit_infos_", K(ret));
  }
  return ret;
}
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

const char *ObUnitManager::end_migrate_op_type_to_str(const ObUnitManager::EndMigrateOp &t)
{
  const char* str = "UNKNOWN";
  if (EndMigrateOp::COMMIT == t) {
    str = "COMMIT";
  } else if (EndMigrateOp::ABORT == t) {
    str = "ABORT";
  } else if (EndMigrateOp::REVERSE == t) {
    str = "REVERSE";
  } else {
    str = "NONE";
  }
  return str;
}
////////////////////////////////////////////////////////////////
ObUnitManager::ObUnitManager(ObServerManager &server_mgr, ObZoneManager &zone_mgr)
: inited_(false), loaded_(false), proxy_(NULL), server_config_(NULL),
    srv_rpc_proxy_(NULL), server_mgr_(server_mgr),
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

    // build server migrate units
    if (OB_SUCC(ret)) {
      FOREACH_CNT_X(unit, units, OB_SUCCESS == ret) {
        if (unit->migrate_from_server_.is_valid()) {
          if (OB_FAIL(insert_migrate_unit(unit->migrate_from_server_, unit->unit_id_))) {
            LOG_WARN("insert_migrate_unit failed", "server", unit->migrate_from_server_,
                     "unit_id", unit->unit_id_, K(ret));
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

int ObUnitManager::check_tenant_pools_in_shrinking(
    const uint64_t tenant_id,
    bool &is_shrinking)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObResourcePool *> *pools = NULL;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools ptr is null", K(ret), KP(pools));
  } else {
    is_shrinking = false;
    for (int64_t i = 0; !is_shrinking && OB_SUCC(ret) && i < pools->count(); ++i) {
      common::ObArray<share::ObUnit *> *units = NULL;
      const share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
        LOG_WARN("fail to get units by pool", K(ret), "pool_id", pool->resource_pool_id_);
      } else if (NULL == units) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("units ptrs is null", K(ret), KP(units));
      } else {
        is_shrinking = false;
        for (int64_t j = 0; !is_shrinking && OB_SUCC(ret) && j < units->count(); ++j) {
          const ObUnit *unit = units->at(j);
          if (OB_UNLIKELY(NULL == unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr si null", K(ret));
          } else if (ObUnit::UNIT_STATUS_DELETING == unit->status_) {
            is_shrinking = true;
          } else if (ObUnit::UNIT_STATUS_ACTIVE == unit->status_) {
            // a normal unit, go on and check next
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit status unexpected", K(ret), "unit_status", unit->status_);
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_pool_in_shrinking(
    const uint64_t pool_id,
    bool &is_shrinking)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(inner_check_pool_in_shrinking_(pool_id, is_shrinking))) {
    LOG_WARN("inner check pool in shrinking failed", KR(ret), K(pool_id), K(is_shrinking));
  }
  return ret;
}

int ObUnitManager::inner_check_pool_in_shrinking_(
    const uint64_t pool_id,
    bool &is_shrinking)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pool_id));
  } else {
    common::ObArray<share::ObUnit *> *units = NULL;
    if (OB_FAIL(get_units_by_pool(pool_id, units))) {
      LOG_WARN("fail to get units by pool", K(ret), K(pool_id));
    } else if (NULL == units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units ptr is null", K(ret), KP(units));
    } else {
      is_shrinking = false;
      for (int64_t i = 0; !is_shrinking && OB_SUCC(ret) && i < units->count(); ++i) {
        const ObUnit *unit = units->at(i);
        if (OB_UNLIKELY(NULL == unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", K(ret));
        } else if (ObUnit::UNIT_STATUS_DELETING == unit->status_) {
          is_shrinking = true;
        } else if (ObUnit::UNIT_STATUS_ACTIVE == unit->status_) {
          // a normal unit, go on and check next
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit status unexpected", K(ret), "unit_status", unit->status_);
        }
      }
    }
  }
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
  } else if (!is_bootstrap_pool && OB_FAIL(check_resource_pool(resource_pool))) {
    LOG_WARN("check_resource_pool failed", K(resource_pool), KR(ret));
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
          if (OB_INVALID_ID == new_pool->resource_pool_id_) {
            if (OB_FAIL(fetch_new_resource_pool_id(new_pool->resource_pool_id_))) {
              LOG_WARN("fetch_new_resource_pool_id failed", K(ret));
            }
          } else {
            // sys resource pool with pool_id set
          }
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

void ObUnitManager::print_user_error_(const uint64_t tenant_id)
{
  const int64_t ERR_MSG_LEN = 256;
  char err_msg[ERR_MSG_LEN] = {'\0'};
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(err_msg, ERR_MSG_LEN, pos,
      "Tenant (%lu) 'enable_rebalance' is disabled, alter tenant unit num", tenant_id))) {
    if (OB_SIZE_OVERFLOW == ret) {
      LOG_WARN("format to buff size overflow", KR(ret));
    } else {
      LOG_WARN("format new unit num failed", KR(ret));
    }
  } else {
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
  }
}

//After the 14x version,
//the same tenant is allowed to have multiple unit specifications in a zone,
//but it is necessary to ensure that these units can be scattered on each server in the zone,
//and multiple units of the same tenant cannot be located on the same machine;
int ObUnitManager::check_server_enough(const uint64_t tenant_id,
                                       const ObIArray<ObResourcePoolName> &pool_names,
                                       bool &enough)
{
  int ret = OB_SUCCESS;
  enough = true;
  share::ObResourcePool *pool = NULL;
  ObArray<ObUnitInfo> unit_infos;
  ObArray<ObUnitInfo> total_unit_infos;
  common::ObArray<share::ObResourcePool *> *pools = NULL;;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (pool_names.count() <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_names), K(tenant_id), K(ret));
  } else {
    //Count the number of newly added units
    for (int64_t i = 0; i < pool_names.count() && OB_SUCC(ret); i++) {
      unit_infos.reset();
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        LOG_WARN("fail to get resource pool by name", K(ret), "pool_name", pool_names.at(i));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid pool", K(ret), K(pool));
      } else if (OB_FAIL(inner_get_unit_infos_of_pool_(pool->resource_pool_id_, unit_infos))) {
        LOG_WARN("fail to get unit infos", K(ret), K(*pool));
      } else {
        for (int64_t j = 0; j < unit_infos.count() && OB_SUCC(ret); j++) {
          if (OB_FAIL(total_unit_infos.push_back(unit_infos.at(j)))) {
            LOG_WARN("fail to push back unit", K(ret), K(total_unit_infos), K(j), K(unit_infos));
          } else {
            LOG_DEBUG("add unit infos", K(ret), K(total_unit_infos), K(unit_infos));
          }
        }
      } //end else
    } // end for
  }
  //Count the number of existing units
  if (FAILEDx(get_pools_by_tenant_(tenant_id, pools))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // a new tenant, without resource pool already granted
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools is null", K(ret), KP(pools));
  } else {
    for (int64_t i = 0; i < pools->count() && OB_SUCC(ret); i++) {
      unit_infos.reset();
      const share::ObResourcePool *pool = pools->at(i);
      if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (OB_FAIL(inner_get_unit_infos_of_pool_(pool->resource_pool_id_, unit_infos))) {
        LOG_WARN("fail to get unit infos", K(ret), K(*pool));
      } else {
        for (int64_t j = 0; j < unit_infos.count() && OB_SUCC(ret); j++) {
          if (OB_FAIL(total_unit_infos.push_back(unit_infos.at(j)))) {
            LOG_WARN("fail to push back unit", K(ret), K(total_unit_infos), K(j), K(unit_infos));
          } else {
            LOG_WARN("add unit infos", K(ret), K(total_unit_infos), K(unit_infos));
          }
        }
      }
    }
  }
  ObArray<ObZoneInfo> zone_infos;
  if (FAILEDx(zone_mgr_.get_zone(zone_infos))) {
    LOG_WARN("fail to get zone infos", K(ret));
  } else {
    //Count the number of units in zone
    for (int64_t i = 0; i < zone_infos.count() && OB_SUCC(ret) && enough; i++) {
      const ObZone &zone = zone_infos.at(i).zone_;
      int64_t unit_count = 0;
      int64_t alive_server_count = 0;
      for (int64_t j = 0; j < total_unit_infos.count() && OB_SUCC(ret); j++) {
        if (total_unit_infos.at(j).unit_.zone_ == zone) {
          unit_count ++;
        }
      }
      if (unit_count > 0) {
        if (OB_FAIL(SVR_TRACER.get_alive_servers_count(zone, alive_server_count))) {
          LOG_WARN("fail to get alive server count", KR(ret), K(zone));
        } else if (alive_server_count < unit_count) {
          //ret = OB_UNIT_NUM_OVER_SERVER_COUNT;
          enough = false;
          LOG_WARN("resource pool unit num over zone server count", K(ret), K(unit_count), K(alive_server_count),
                   K(total_unit_infos));
        }
      }
    }
  }
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
  } else {
    if (!grant) {
      // when revoke pools, unit_group_id in units shall be modified to 0
      for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_num; ++i) {
        if (OB_FAIL(new_unit_group_id_array.push_back(
                0/* 0 means this unit doesn't belong to any unit group*/))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    } else {
      // when grant pools, an unit group id greater than 0 is needed for every unit
      ObArray<share::ObResourcePool *> *tenant_pool_array = nullptr;
      int tmp_ret = get_pools_by_tenant_(tenant_id, tenant_pool_array);
      if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        // need to fetch unit group from inner table, since this is invoked by create tenant
        for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_num; ++i) {
          uint64_t unit_group_id = OB_INVALID_ID;
          if (OB_FAIL(fetch_new_unit_group_id(unit_group_id))) {
            LOG_WARN("fail to fetch new unit group id", KR(ret));
          } else if (OB_FAIL(new_unit_group_id_array.push_back(unit_group_id))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      } else {
        const bool is_active = false;
        if (OB_FAIL(inner_get_all_unit_group_id(tenant_id, is_active, new_unit_group_id_array))) {
          LOG_WARN("fail to get all unit group id array", KR(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::inner_get_all_unit_group_id(
    const uint64_t tenant_id,
    const bool is_active,
    common::ObIArray<uint64_t> &unit_group_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObArray<share::ObResourcePool *> *cur_pool_array = nullptr;
    if (OB_FAIL(get_pools_by_tenant_(tenant_id, cur_pool_array))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // bypass
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get pools by tenant", KR(ret), K(tenant_id));
      }
    } else if (OB_ISNULL(cur_pool_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur pool array ptr is null", KR(ret), K(tenant_id));
    } else if (cur_pool_array->count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_pool_array is empty", KR(ret), K(tenant_id));
    } else {
      share::ObResourcePool *cur_pool = cur_pool_array->at(0);
      common::ObArray<ObUnit *> zone_sorted_unit_array;
      if (OB_ISNULL(cur_pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_pool ptr is null", KR(ret), KP(cur_pool));
      } else if (OB_FAIL(build_zone_sorted_unit_array_(cur_pool, zone_sorted_unit_array))) {
        LOG_WARN("fail to build zone_sorted_unit_array", KR(ret), K(cur_pool));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < zone_sorted_unit_array.count(); ++j) {
          ObUnit *unit = zone_sorted_unit_array.at(j);
          if (OB_ISNULL(unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", KR(ret), K(tenant_id), KPC(cur_pool), K(j));
          } else if (has_exist_in_array(unit_group_array, unit->unit_group_id_)) {
            //unit_count_ is small than unit group count while some unit is in deleting
            break;
          } else if (is_active && ObUnit::UNIT_STATUS_ACTIVE != unit->status_) {
            //need active unit group
            continue;
          } else if (OB_FAIL(unit_group_array.push_back(unit->unit_group_id_))) {
            LOG_WARN("fail to push back", KR(ret), K(unit));
          }
        }
        if (OB_SUCC(ret) && OB_UNLIKELY(unit_group_array.count() < cur_pool->unit_count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool unit count unexpected", KR(ret), KPC(cur_pool),
                   K(unit_group_array));
        }
      }
    }
  }
  return ret;
}


int ObUnitManager::get_unit_group(
    const uint64_t tenant_id,
    const uint64_t unit_group_id,
    common::ObIArray<share::ObUnitInfo> &unit_info_array)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                         || 0 == unit_group_id
                         || OB_INVALID_ID == unit_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(unit_group_id));
  } else {
    ObArray<share::ObResourcePool *> *cur_pool_array = nullptr;
    ret = get_pools_by_tenant_(tenant_id, cur_pool_array);
    if (OB_ENTRY_NOT_EXIST == ret) {
      // bypass
    } else if (OB_SUCCESS != ret) {
      LOG_WARN("fail to get unit group", KR(ret), K(tenant_id), K(unit_group_id));
    } else if (OB_UNLIKELY(nullptr == cur_pool_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur pool array ptr is null", KR(ret), K(tenant_id), K(unit_group_id));
    } else {
      ObUnitInfo unit_info;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_pool_array->count(); ++i) {
        share::ObResourcePool *cur_pool = cur_pool_array->at(i);
        ObArray<share::ObUnit *> *units = nullptr;
        ObUnitConfig *config = nullptr;
        if (OB_UNLIKELY(nullptr == cur_pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur pool is null", KR(ret));
        } else if (OB_FAIL(get_unit_config_by_id(cur_pool->unit_config_id_, config))) {
          LOG_WARN("fail to get pool unit config", KR(ret), K(tenant_id), KPC(cur_pool));
        } else if (OB_FAIL(get_units_by_pool(cur_pool->resource_pool_id_, units))) {
          LOG_WARN("fail to get units by pool", KR(ret),
                   "pool_id", cur_pool->resource_pool_id_);
        } else if (OB_UNLIKELY(nullptr == units)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("units ptrs is null", KR(ret), K(tenant_id), KPC(cur_pool));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < units->count(); ++j) {
            if (OB_UNLIKELY(nullptr == units->at(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("units ptrs is null", KR(ret), K(tenant_id), KPC(cur_pool));
            } else if (units->at(j)->unit_group_id_ != unit_group_id) {
              // unit group id not match
            } else {
              unit_info.reset();
              unit_info.config_ = *config;
              unit_info.unit_ = *(units->at(j));
              if (OB_FAIL(unit_info.pool_.assign(*cur_pool))) {
                LOG_WARN("fail to assign", KR(ret), KPC(cur_pool));
              } else if (OB_FAIL(unit_info_array.push_back(unit_info))) {
                LOG_WARN("fail to push back", K(unit_info));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (unit_info_array.count() <= 0) {
          ret = OB_ENTRY_NOT_EXIST;
        }
      }
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
  bool server_enough = true;
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
  } else if (!is_bootstrap
      && OB_FAIL(check_server_enough(tenant_id, pool_names, server_enough))) {
    LOG_WARN("fail to check server enough", KR(ret), K(tenant_id), K(pool_names));
  } else if (!server_enough) {
    ret = OB_UNIT_NUM_OVER_SERVER_COUNT;
    LOG_WARN("resource pool unit num over zone server count", K(ret), K(pool_names), K(tenant_id));
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

int ObUnitManager::revoke_pools(common::ObMySQLTransaction &trans,
                                common::ObIArray<uint64_t> &new_unit_group_id_array,
                                const ObIArray<ObResourcePoolName> &pool_names,
                                const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const bool grant = false;
  bool unit_num_legal = false;
  int64_t legal_unit_num = -1;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (pool_names.count() <= 0 || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pool_names), K(tenant_id), K(ret));
  } else if (OB_FAIL(check_pool_ownership_(tenant_id, pool_names, false/*is_grant*/))) {
    LOG_WARN("check pool ownership failed", KR(ret), K(pool_names));
  } else if (OB_FAIL(check_tenant_pools_unit_num_legal_(
          tenant_id, pool_names, unit_num_legal, legal_unit_num))) {
    LOG_WARN("fail to check pools unit num legal", KR(ret), K(tenant_id), K(pool_names));
  } else if (!unit_num_legal) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("pools belong to one tenant with different unit num are not allowed",
             KR(ret), K(tenant_id), K(pool_names));
  } else if (OB_FAIL(get_tenant_pool_unit_group_id_(
          false /* is_bootstrap */, grant, tenant_id,
          legal_unit_num, new_unit_group_id_array))) {
    LOG_WARN("fail to generate new unit group id", KR(ret), K(tenant_id), K(legal_unit_num));
  } else if (OB_FAIL(do_revoke_pools_(trans, new_unit_group_id_array, pool_names, tenant_id))) {
    LOG_WARN("do revoke pools failed", KR(ret), K(grant), K(pool_names), K(tenant_id));
  }
  LOG_INFO("revoke resource pools from tenant", K(pool_names), K(ret));
  return ret;
}

int ObUnitManager::inner_get_pool_ids_of_tenant(const uint64_t tenant_id,
                                                ObIArray<uint64_t> &pool_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else {
    pool_ids.reuse();
    ObArray<share::ObResourcePool  *> *pools = NULL;
    if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_DEBUG("get_pools_by_tenant failed", K(tenant_id), K(ret));
      } else {
        // just return empty pool_ids
        LOG_INFO("tenant doesn't own any pool", K(tenant_id), KR(ret));
        ret = OB_SUCCESS;
      }
    } else if (NULL == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pools), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
        if (NULL == pools->at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool is null", "pool", OB_P(pools->at(i)), K(ret));
        } else if (OB_FAIL(pool_ids.push_back(pools->at(i)->resource_pool_id_))) {
          LOG_WARN("push_back failed", K(ret));
        }
      }
    }
  }
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
        bool is_alive = false;
        if (OB_FAIL(SVR_TRACER.check_server_alive(unit->server_, is_alive))) {
          LOG_WARN("check_server_alive failed", KR(ret), K(unit->server_));
        } else if (is_alive) {
          if (has_exist_in_array(servers, unit->server_)) {
            // server exist
          } else if (OB_FAIL(servers.push_back(unit->server_))) {
            LOG_WARN("push_back failed", KR(ret), K(unit->server_));
          }
        }
        if (OB_FAIL(ret) || !unit->migrate_from_server_.is_valid()) {
          // skip
        } else if (OB_FAIL(SVR_TRACER.check_server_alive(unit->migrate_from_server_, is_alive))) {
          LOG_WARN("check_server_alive failed", KR(ret), K(unit->migrate_from_server_));
        } else if (is_alive) {
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

int ObUnitManager::get_pool_ids_of_tenant(const uint64_t tenant_id,
                                          ObIArray<uint64_t> &pool_ids) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else if (OB_FAIL(inner_get_pool_ids_of_tenant(tenant_id, pool_ids))) {
    LOG_WARN("fail to inner get pool ids of tenant", K(ret));
  }
  return ret;
}

int ObUnitManager::get_pool_names_of_tenant(const uint64_t tenant_id,
                                            ObIArray<ObResourcePoolName> &pool_names) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ret));
  } else {
    ObArray<share::ObResourcePool  *> *pools = NULL;
    if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_pools_by_tenant failed", K(tenant_id), K(ret));
      } else {
        // just return empty pool_ids
        ret = OB_SUCCESS;
        LOG_WARN("tenant doesn't own any pool", K(tenant_id), K(ret));
      }
    } else if (NULL == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pools), K(ret));
    } else {
      pool_names.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
        if (NULL == pools->at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool is null", "pool", OB_P(pools->at(i)), K(ret));
        } else if (OB_FAIL(pool_names.push_back(pools->at(i)->name_))) {
          LOG_WARN("push_back failed", K(ret));
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


int ObUnitManager::get_zones_of_pools(const ObIArray<ObResourcePoolName> &pool_names,
                                      ObIArray<ObZone> &zones) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (pool_names.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool_names is empty", K(pool_names), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      share::ObResourcePool  *pool = NULL;
      if (pool_names.at(i).is_empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool name");
        LOG_WARN("invalid pool name", "pool name", pool_names.at(i), K(ret));
      } else if (OB_FAIL(inner_get_resource_pool_by_name(pool_names.at(i), pool))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get resource pool by name failed", "name", pool_names.at(i), K(ret));
        } else {
          ret = OB_RESOURCE_POOL_NOT_EXIST;
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_RESOURCE_POOL_NOT_EXIST, helper.convert(pool_names.at(i)));
          LOG_WARN("pool not exist", "pool_name", pool_names.at(i), K(ret));
        }
      } else if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), K(ret));
      } else if (OB_FAIL(append(zones, pool->zone_list_))) {
        LOG_WARN("append failed", "zone_list", pool->zone_list_, K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::get_pools(common::ObIArray<share::ObResourcePool> &pools) const
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else {
    ObHashMap<uint64_t, share::ObResourcePool  *>::const_iterator iter = id_pool_map_.begin();
    for ( ; OB_SUCC(ret) && iter != id_pool_map_.end(); ++iter) {
      if (NULL == iter->second) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter->second is null", KP(iter->second), K(ret));
      } else if (OB_FAIL(pools.push_back(*(iter->second)))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
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

int ObUnitManager::inner_get_tenant_pool_zone_list(
    const uint64_t tenant_id,
    common::ObIArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools ptr is null", K(ret), K(tenant_id));
  } else if (pools->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools array is empty", K(ret), K(*pools));
  } else {
    zone_list.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
      const share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", K(ret), KP(pool));
      } else if (OB_FAIL(append(zone_list, pool->zone_list_))) {
        LOG_WARN("fail to append", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObUnitManager::get_tenant_pool_zone_list(
    const uint64_t tenant_id,
    common::ObIArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(inner_get_tenant_pool_zone_list(tenant_id, zone_list))) {
    LOG_WARN("fail to inner get tenant pool zone list", K(ret));
  }
  return ret;
}

// cancel migrate units on server to other servers
int ObUnitManager::cancel_migrate_out_units(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  ObArray<uint64_t> migrate_units;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server), K(ret));
  } else if (OB_FAIL(get_migrate_units_by_server(server, migrate_units))) {
    LOG_WARN("get_migrate_units_by_server failed", K(server), K(ret));
  } else {
    const EndMigrateOp op = REVERSE;
    for (int64_t i = 0; OB_SUCC(ret) && i < migrate_units.count(); ++i) {
      if (OB_FAIL(end_migrate_unit(migrate_units.at(i), op))) {
        LOG_WARN("end_migrate_unit failed", "unit_id", migrate_units.at(i), K(op), K(ret));
      }
    }
  }
  LOG_INFO("cancel migrate out units", K(server), K(ret));
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

int ObUnitManager::inner_get_zone_alive_unit_infos_by_tenant(
    const uint64_t tenant_id,
    const common::ObZone &zone,
    common::ObIArray<share::ObUnitInfo> &unit_infos) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> rs_pool;
  ObArray<ObUnitInfo> unit_array;
  unit_infos.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(zone));
  } else if (OB_FAIL(inner_get_pool_ids_of_tenant(tenant_id, rs_pool))) {
    LOG_WARN("fail to get pool ids by tennat", K(ret), K(tenant_id));
  } else {
    FOREACH_X(pool, rs_pool, OB_SUCCESS == ret) {
      unit_array.reuse();
      if (OB_FAIL(check_inner_stat_())) {
        LOG_WARN("check inner stat failed", K(ret), K(inited_), K(loaded_));
      } else if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", K(ret));
      } else if (OB_FAIL(inner_get_unit_infos_of_pool_(*pool, unit_array))) {
        LOG_WARN("fail to get unit infos of pool", K(ret));
      } else if (unit_array.count() > 0) {
        FOREACH_X(u, unit_array, OB_SUCCESS == ret) {
          bool is_alive = false;
          bool is_in_service = false;
          if (OB_UNLIKELY(NULL == u)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit is empty", K(ret));
          } else if (zone != u->unit_.zone_) {
            // do not belong to this zone
            } else if (OB_FAIL(SVR_TRACER.check_server_alive(u->unit_.server_, is_alive))) {
            LOG_WARN("check_server_alive failed", "server", u->unit_.server_, K(ret));
          } else if (OB_FAIL(SVR_TRACER.check_in_service(u->unit_.server_, is_in_service))) {
            LOG_WARN("check server in service failed", "server", u->unit_.server_, K(ret));
          } else if (!is_alive || !is_in_service) {
            // ignore unit on not-alive server
          } else if (ObUnit::UNIT_STATUS_ACTIVE != u->unit_.status_) {
            // ignore the unit which is in deleting status
          } else if (OB_FAIL(unit_infos.push_back(*u))) {
            LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
      } else {} // empty array
    }
  }
  return ret;
}

int ObUnitManager::commit_shrink_resource_pool_in_trans_(
  const common::ObIArray<share::ObResourcePool *> &pools,
  common::ObMySQLTransaction &trans,
  common::ObIArray<common::ObArray<uint64_t>> &resource_units)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret), K(loaded_), K(inited_));
  } else if (OB_UNLIKELY(0 == pools.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool ptr is null", KR(ret), K(pools));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < pools.count(); ++index) {
      // Commit shrink resource pool only needs to change the state of the unit,
      // the state of the resource pool itself has been changed before,
      // and there is no need to adjust it again.
      units = NULL;
      share::ObResourcePool *pool = pools.at(index);
      if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KR(ret), K(index), K(pools));
      } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
        LOG_WARN("failed to get pool unit", KR(ret), KPC(pool));
      } else if (OB_ISNULL(units)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("units is null of the pool", KR(ret), KPC(pool));
      } else {
        common::ObArray<uint64_t> unit_ids;
        const int64_t unit_count = pool->unit_count_;
        if (unit_count <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit count unexpected", KR(ret), K(unit_count));
        } else if (units->count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone unit ptrs has no element", KR(ret), "unit_cnt",
                   units->count());
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
            const ObUnit *unit = units->at(i);
            if (OB_ISNULL(unit)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unit ptr is null", KR(ret), KP(unit), K(i), KPC(units));
            } else if (ObUnit::UNIT_STATUS_DELETING == unit->status_) {
              if (OB_FAIL(ut_operator_.remove_unit(trans, *unit))) {
                LOG_WARN("fail to remove unit", KR(ret), "unit", *unit);
              } else if (OB_FAIL(unit_ids.push_back(unit->unit_id_))) {
                LOG_WARN("fail to push back", KR(ret), KPC(unit));
              } else {} // no more to do
            } else {} // active unit, do nothing
          }// end for each unit
          if (FAILEDx(resource_units.push_back(unit_ids))) {
            LOG_WARN("failed to push back units", KR(ret), K(unit_ids));
          }
        }
      }
    }//end for each pool
  }
  return ret;
}

int ObUnitManager::get_deleting_units_of_pool(
    const uint64_t resource_pool_id,
    ObIArray<share::ObUnit> &units) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  ObArray<ObUnit *> *inner_units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(loaded_), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == resource_pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(resource_pool_id));
  } else if (OB_FAIL(get_units_by_pool(resource_pool_id, inner_units))) {
      LOG_WARN("fail to get units by pool", K(ret), K(resource_pool_id));
  } else if (NULL == inner_units) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_units ptr is null", K(ret), KP(inner_units));
  } else {
    units.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < inner_units->count(); ++i) {
      const share::ObUnit *this_unit = inner_units->at(i);
      if (NULL == this_unit) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret), KP(this_unit));
      } else if (ObUnit::UNIT_STATUS_DELETING == this_unit->status_) {
        if (OB_FAIL(units.push_back(*this_unit))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      } else if (ObUnit::UNIT_STATUS_ACTIVE == this_unit->status_) {
        // a normal unit, ignore
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected unit status", K(ret), "unit_status", this_unit->status_);
      }
    }
    LOG_INFO("get deleting units of pool", K(ret), K(resource_pool_id), K(units));
  }
  return ret;
}

int ObUnitManager::get_unit_infos_of_pool(const uint64_t resource_pool_id,
                                          ObIArray<ObUnitInfo> &unit_infos) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else if (OB_FAIL(inner_get_unit_infos_of_pool_(resource_pool_id, unit_infos))) {
    LOG_WARN("inner_get_unit_infos_of_pool failed", K(resource_pool_id), K(ret));
  }
  return ret;
}

int ObUnitManager::inner_get_unit_infos_of_pool_(const uint64_t resource_pool_id,
                                                ObIArray<ObUnitInfo> &unit_infos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else {
    unit_infos.reuse();
    ObUnitInfo unit_info;
    share::ObResourcePool  *pool = NULL;
    ObUnitConfig *config = NULL;
    if (OB_FAIL(get_resource_pool_by_id(resource_pool_id, pool))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_RESOURCE_POOL_NOT_EXIST;
      }
      LOG_WARN("get_resource_pool_by_id failed", K(resource_pool_id), K(ret));
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
      unit_info.reset();
      if (OB_FAIL(unit_info.pool_.assign(*pool))) {
        LOG_WARN("failed to assign unit_info.pool_", K(ret));
      } else {
        unit_info.config_ = *config;
        ObArray<ObUnit *> *units = NULL;
        if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
          LOG_WARN("get_units_by_pool failed", K(resource_pool_id), K(ret));
        } else if (NULL == units) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("units is null", KP(units), K(ret));
        } else if (units->count() <= 0) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("units of resource pool not exist", K(resource_pool_id), K(ret));
        } else if (OB_FAIL(unit_infos.reserve(units->count()))) {
          LOG_WARN("failed to reserve for unit_infos", KR(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
            if (NULL == units->at(i)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unit is null", "unit", OB_P(units->at(i)), K(ret));
            } else {
              unit_info.unit_ = *units->at(i);
              if (OB_FAIL(unit_infos.push_back(unit_info))) {
                LOG_WARN("push_back failed", K(ret));
              }
            }
          }
        }
      }
    }
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

int ObUnitManager::extract_unit_ids(
    const common::ObIArray<share::ObUnit *> &units,
    common::ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  unit_ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
    if (OB_UNLIKELY(NULL == units.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit ptr is null", K(ret));
    } else if (OB_FAIL(unit_ids.push_back(units.at(i)->unit_id_))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObUnitManager::inner_get_unit_ids(ObIArray<uint64_t> &unit_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  }
  for (ObHashMap<uint64_t, ObArray<ObUnit *> *>::const_iterator it = pool_unit_map_.begin();
      OB_SUCCESS == ret && it != pool_unit_map_.end(); ++it) {
    if (NULL == it->second) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("pointer of ObArray<ObUnit *> is null", "pool_id", it->first, K(ret));
    } else if (it->second->count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("array of unit is empty", "pool_id", it->first, K(ret));
    } else {
      const ObArray<ObUnit *> units = *it->second;
      for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
        uint64_t unit_id = units.at(i)->unit_id_;
        if (OB_FAIL(unit_ids.push_back(unit_id))) {
          LOG_WARN("fail push back it", K(unit_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_unit_ids(ObIArray<uint64_t> &unit_ids) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(inner_get_unit_ids(unit_ids))) {
    LOG_WARN("fail to inner get unit ids", K(ret));
  }
  return ret;
}

int ObUnitManager::calc_sum_load(const ObArray<ObUnitLoad> *unit_loads,
                                 ObUnitConfig &sum_load,
                                 const bool include_ungranted_unit)
{
  int ret = OB_SUCCESS;
  sum_load.reset();
  if (NULL == unit_loads) {
    // all be zero
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_loads->count(); ++i) {
      if (!unit_loads->at(i).is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid unit_load", "unit_load", unit_loads->at(i), K(ret));
      } else if (!is_valid_tenant_id(unit_loads->at(i).get_tenant_id())
                 && !include_ungranted_unit) {
        // skip this unit_load
      } else {
        sum_load += *unit_loads->at(i).unit_config_;
      }
    }
  }
  return ret;
}

int ObUnitManager::check_resource_pool(
    share::ObResourcePool &resource_pool) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (resource_pool.name_.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "resource pool name");
    LOG_WARN("invalid resource pool name", "resource pool name", resource_pool.name_, K(ret));
  } else if (resource_pool.unit_count_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "unit num");
    LOG_WARN("invalid resource unit num", "unit num", resource_pool.unit_count_, K(ret));
  } else {
    // check zones in zone_list not intersected
    for (int64_t i = 0; OB_SUCC(ret) && i < resource_pool.zone_list_.count(); ++i) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < resource_pool.zone_list_.count(); ++j) {
        if (resource_pool.zone_list_[i] == resource_pool.zone_list_[j]) {
          ret = OB_ZONE_DUPLICATED;
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_ZONE_DUPLICATED, helper.convert(resource_pool.zone_list_[i]),
              helper.convert(resource_pool.zone_list_));
          LOG_WARN("duplicate zone in zone list", "zone_list", resource_pool.zone_list_, K(ret));
        }
      }
    }
    // check zones in zone_list all existed
    if (OB_SUCC(ret)) {
      FOREACH_CNT_X(zone, resource_pool.zone_list_, OB_SUCCESS == ret) {
        bool zone_exist = false;
        if (OB_FAIL(zone_mgr_.check_zone_exist(*zone, zone_exist))) {
          LOG_WARN("check_zone_exist failed", KPC(zone), K(ret));
        } else if (!zone_exist) {
          ret = OB_ZONE_INFO_NOT_EXIST;
          ObCStringHelper helper;
          LOG_USER_ERROR(OB_ZONE_INFO_NOT_EXIST, helper.convert(*zone));
          LOG_WARN("zone not exist", "zone", *zone, K(ret));
        }
      }
    }
    // construct all zone as zone_list if zone_list is null
    if (OB_SUCCESS == ret && 0 == resource_pool.zone_list_.count()) {
      ObArray<ObZoneInfo> zone_infos;
      if (OB_FAIL(zone_mgr_.get_zone(zone_infos))) {
        LOG_WARN("get_zone failed", K(ret));
      } else {
        FOREACH_CNT_X(zone_info, zone_infos, OB_SUCCESS == ret) {
          if (OB_FAIL(resource_pool.zone_list_.push_back(zone_info->zone_))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
        if (OB_SUCCESS == ret && resource_pool.zone_list_.count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not active zone found", K(ret));
        }
      }
    }
    // check zones in zone_list has enough alive servers to support unit_num
    if (OB_SUCC(ret)) {
      FOREACH_CNT_X(zone, resource_pool.zone_list_, OB_SUCCESS == ret) {
        int64_t alive_server_count = 0;
        if (OB_FAIL(SVR_TRACER.get_alive_servers_count(*zone, alive_server_count))) {
          LOG_WARN("get_alive_servers failed", KR(ret), KPC(zone));
        } else if (alive_server_count < resource_pool.unit_count_) {
          ret = OB_UNIT_NUM_OVER_SERVER_COUNT;
          LOG_WARN("resource pool unit num over zone server count", "unit_count",
              resource_pool.unit_count_, K(alive_server_count), K(ret), "zone", *zone);
        }
      }
    }
  }
  return ret;
}

/* Notify creating or dropping unit on ObServer.
 * 1. Specify @is_delete as true when dropping unit,
 *    and only @tenant_id and @unit will be used in this case.
 * 2. This function merely sends RPC call, so executor should make sure waiting is called later.
 *    But there is one exception that when @unit is on this server where RS Leader locates,
 *    notification will be locally executed without RPC, which means no need to wait proxy.
 */
int ObUnitManager::try_notify_tenant_server_unit_resource_(
    const uint64_t tenant_id,
    const bool is_delete,
    ObNotifyTenantServerResourceProxy &notify_proxy,
    const uint64_t unit_config_id,
    const lib::Worker::CompatMode compat_mode,
    const share::ObUnit &unit,
    const bool if_not_grant,
    const bool skip_offline_server,
    const bool check_data_version)
{
  int ret = OB_SUCCESS;
  bool is_alive = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret), K(inited_), K(loaded_));
  } else if (OB_FAIL(SVR_TRACER.check_server_alive(unit.server_, is_alive))) {
    LOG_WARN("fail to get server_info", KR(ret), K(unit.server_));
  } else if (!is_alive && (is_delete || skip_offline_server)) {
    // do nothing
    LOG_INFO("ignore not alive server when is_delete or skip_offline_server is true",
              "server", unit.server_, K(is_delete), K(skip_offline_server));
  } else if (!is_valid_tenant_id(tenant_id)) {
    // do nothing, unit not granted
  } else {
    // STEP 1: Get and init notifying arg
    obrpc::TenantServerUnitConfig tenant_unit_server_config;
    if (!is_delete) {
      const bool should_check_data_version = check_data_version && is_user_tenant(tenant_id);
      if (OB_FAIL(build_notify_create_unit_resource_rpc_arg_(
                    tenant_id, unit, compat_mode, unit_config_id, if_not_grant,
                    tenant_unit_server_config))) {
        LOG_WARN("fail to init tenant_unit_server_config", KR(ret), K(tenant_id), K(is_delete));
      }
    } else {
      if (OB_FAIL(tenant_unit_server_config.init_for_dropping(tenant_id, is_delete))) {
        LOG_WARN("fail to init tenant_unit_server_config", KR(ret), K(tenant_id), K(is_delete));
      }
    }

    // STEP 2: Do notification
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_notify_unit_resource_(
                unit.server_, tenant_unit_server_config, notify_proxy))) {
      LOG_WARN("failed to do_notify_unit_resource", "dst", unit.server_, K(tenant_unit_server_config));
      if (OB_TENANT_EXIST == ret) {
        ret = OB_TENANT_RESOURCE_UNIT_EXIST;
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_TENANT_RESOURCE_UNIT_EXIST, tenant_id, helper.convert(unit.server_));
      }
    }
  }
  return ret;
}

int ObUnitManager::build_notify_create_unit_resource_rpc_arg_(
    const uint64_t tenant_id,
    const share::ObUnit &unit,
    const lib::Worker::CompatMode compat_mode,
    const uint64_t unit_config_id,
    const bool if_not_grant,
    obrpc::TenantServerUnitConfig &rpc_arg) const
{
  int ret = OB_SUCCESS;
  // get unit_config
  share::ObUnitConfig *unit_config = nullptr;
  if (OB_FAIL(get_unit_config_by_id(unit_config_id, unit_config))) {
    LOG_WARN("fail to get unit config by id", KR(ret));
  } else if (OB_ISNULL(unit_config)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit config is null", KR(ret), "unit_config_id", unit_config_id);
  }

  // init rpc_arg
  const bool is_delete = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rpc_arg.init(tenant_id,
                                  unit.unit_id_,
                                  compat_mode,
                                  *unit_config,
                                  ObReplicaType::REPLICA_TYPE_FULL,
                                  if_not_grant,
                                  is_delete))) {
    LOG_WARN("fail to init rpc_arg", KR(ret), K(tenant_id), K(is_delete));
  }
  return ret;
}

int ObUnitManager::do_notify_unit_resource_(
  const common::ObAddr server,
  const obrpc::TenantServerUnitConfig &notify_arg,
  ObNotifyTenantServerResourceProxy &notify_proxy)
{
  int ret = OB_SUCCESS;
  if (GCONF.self_addr_ == server) {
    // Directly call local interface without using RPC
    if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().handle_notify_unit_resource(notify_arg))) {
      LOG_WARN("fail to handle_notify_unit_resource", K(ret), K(notify_arg));
    } else {
      LOG_INFO("call notify resource to server (locally)", KR(ret), "dst", server, K(notify_arg));
    }
  } else {
    int64_t start = ObTimeUtility::current_time();
    int64_t rpc_timeout = NOTIFY_RESOURCE_RPC_TIMEOUT;
    if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
      rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
    }
    if (OB_FAIL(notify_proxy.call(server, rpc_timeout, notify_arg))) {
      LOG_WARN("fail to call notify resource to server", KR(ret), K(rpc_timeout),
               "dst", server, "cost", ObTimeUtility::current_time() - start);
    } else {
      LOG_INFO("call notify resource to server", KR(ret), "dst", server, K(notify_arg),
               "cost", ObTimeUtility::current_time() - start);
    }
  }
  return ret;
}

int ObUnitManager::rollback_persistent_units_(
      const common::ObArray<share::ObUnit> &units,
      const share::ObResourcePool &pool,
      ObNotifyTenantServerResourceProxy &notify_proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool is_delete = true;
  ObArray<int> return_ret_array;
  notify_proxy.reuse();
  for (int64_t i = 0; i < units.count(); i++) {
    const ObUnit & unit = units.at(i);
    const lib::Worker::CompatMode dummy_mode = lib::Worker::CompatMode::INVALID;
    if (OB_TMP_FAIL(try_notify_tenant_server_unit_resource_(
        pool.tenant_id_, is_delete, notify_proxy,
        pool.unit_config_id_, dummy_mode, unit,
        false/*if_not_grant*/, false/*skip_offline_server*/,
        false /*check_data_version*/))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("fail to try notify server unit resource", KR(ret), KR(tmp_ret),
               K(is_delete), K(pool), K(dummy_mode), K(unit));
    }
  }
  if (OB_TMP_FAIL(notify_proxy.wait_all(return_ret_array))) {
    LOG_WARN("fail to wait notify resource", KR(ret), K(tmp_ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  } else if (OB_FAIL(ret)) {
  } else {
    // don't use arg/dest/result here because call() may has failure.
    ObAddr invalid_addr;
    for (int64_t i = 0; OB_SUCC(ret) && i < return_ret_array.count(); i++) {
      const int ret_i = return_ret_array.at(i);
      const ObAddr &addr = return_ret_array.count() != notify_proxy.get_dests().count() ?
                           invalid_addr : notify_proxy.get_dests().at(i);
      // if (OB_SUCCESS != ret_i && OB_TENANT_NOT_IN_SERVER != ret_i) {
      if (OB_SUCCESS != ret_i && OB_TENANT_NOT_IN_SERVER != ret_i) {
        ret = ret_i;
        LOG_WARN("fail to mark tenant removed", KR(ret), KR(ret_i), K(addr));
      }
    }
  }
  LOG_WARN("rollback persistent unit", KR(ret), K(pool), K(units));
  return ret;
}


int ObUnitManager::get_tenant_unit_servers(
    const uint64_t tenant_id,
    const common::ObZone &zone,
    common::ObIArray<common::ObAddr> &server_array) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    server_array.reset();
    int tmp_ret = get_pools_by_tenant_(tenant_id, pools);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // pass, and return empty server array
    } else if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
    } else if (nullptr == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools ptr is null, unexpected", K(ret), K(tenant_id));
    } else {
      common::ObArray<common::ObAddr> this_server_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
        this_server_array.reset();
        const share::ObResourcePool *pool = pools->at(i);
        if (OB_UNLIKELY(nullptr == pool)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pool ptr is null", K(ret), K(tenant_id));
        } else if (OB_FAIL(get_pool_servers(
                pool->resource_pool_id_, zone, this_server_array))) {
          LOG_WARN("fail to get pool server", K(ret),
                   "pool_id", pool->resource_pool_id_, K(zone));
        } else if (OB_FAIL(append(server_array, this_server_array))) {
          LOG_WARN("fail to append", K(ret));
        }
      }
    }
  }
  return ret;
}
ERRSIM_POINT_DEF(ERRSIM_UNIT_PERSISTENCE_ERROR);

int ObUnitManager::get_pools_servers(const common::ObIArray<share::ObResourcePool  *> &pools,
    common::hash::ObHashMap<common::ObAddr, int64_t> &server_ref_count_map) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (pools.count() <= 0 || !server_ref_count_map.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", K(pools),
        "server_ref_count_map created", server_ref_count_map.created(), K(ret));
  } else {
    ObArray<ObAddr> servers;
    const ObZone all_zone;
    FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret) {
      servers.reuse();
      if (NULL == *pool) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("pool is null", "pool", *pool, K(ret));
      } else if (OB_FAIL(get_pool_servers((*pool)->resource_pool_id_, all_zone, servers))) {
        LOG_WARN("get pool servers failed",
            "pool id", (*pool)->resource_pool_id_, K(all_zone), K(ret));
      } else {
        FOREACH_CNT_X(server, servers, OB_SUCCESS == ret) {
          int64_t server_ref_count = 0;
          if (OB_FAIL(get_server_ref_count(server_ref_count_map, *server, server_ref_count))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              server_ref_count = 1;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("get server ref count failed", "server", *server, K(ret));
            }
          } else {
            ++server_ref_count;
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(set_server_ref_count(server_ref_count_map, *server, server_ref_count))) {
              LOG_WARN("set server ref count failed",
                  "server", *server, K(server_ref_count), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_pool_servers(const uint64_t resource_pool_id,
                                    const ObZone &zone,
                                    ObIArray<ObAddr> &servers) const
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit *> *units = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    // don't need to check zone, can be empty
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource_pool_id", K(resource_pool_id), K(ret));
  } else if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
    LOG_WARN("get_units_by_pool failed", K(resource_pool_id), K(ret));
  } else if (NULL == units) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units is null", KP(units), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      if (NULL == units->at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit is null", "unit", OB_P(units->at(i)), K(ret));
      } else if (!zone.is_empty() && zone != units->at(i)->zone_) {
        continue;
      } else {
        if (OB_SUCCESS == ret && units->at(i)->migrate_from_server_.is_valid()) {
          if (OB_FAIL(servers.push_back(units->at(i)->migrate_from_server_))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }

        if (OB_SUCCESS == ret && units->at(i)->server_.is_valid()) {
          if (OB_FAIL(servers.push_back(units->at(i)->server_))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::compute_server_resource_(
    const obrpc::ObGetServerResourceInfoResult &report_server_resource_info,
    ObUnitPlacementStrategy::ObServerResource &server_resource) const
{
  int ret = OB_SUCCESS;
  ObUnitConfig sum_load;
  ObArray<ObUnitLoad> *unit_loads = NULL;
  const ObAddr &server = report_server_resource_info.get_server();
  const ObServerResourceInfo &report_resource = report_server_resource_info.get_resource_info();
  if (OB_UNLIKELY(!report_server_resource_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(report_server_resource_info));
  } else if (OB_FAIL(get_loads_by_server(server, unit_loads))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_loads_by_server failed", "server", server, KR(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(unit_loads)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_loads is null", KR(ret), KP(unit_loads));
  } else if (OB_FAIL(calc_sum_load(unit_loads, sum_load))) {
    LOG_WARN("calc_sum_load failed", KR(ret), KP(unit_loads));
  }

  if (OB_SUCC(ret)) {
    // Unit resource information is persisted on the observer side,
    // The unit_load seen on the rs side is only the resource view managed by rs itself,
    // It is actually inaccurate to judge whether the resources are sufficient based on the resource view of rs itself,
    // Need to consider the persistent resources of the unit resource information on the observer side.
    // The persistent information of the unit on the observer side is regularly reported to rs by the observer through the heartbeat.
    // When performing allocation, rs reports the maximum value of resource information from its own resource view
    // and observer side as a reference for unit resource allocation
    server_resource.addr_ = server;
    // RES_CPU
    server_resource.assigned_[RES_CPU] = sum_load.min_cpu() > report_resource.report_cpu_assigned_
                                         ? sum_load.min_cpu() : report_resource.report_cpu_assigned_;
    server_resource.max_assigned_[RES_CPU] = sum_load.max_cpu() > report_resource.report_cpu_max_assigned_
                                         ? sum_load.max_cpu() : report_resource.report_cpu_max_assigned_;
    server_resource.capacity_[RES_CPU] = report_resource.cpu_;
    // RES_MEM
    server_resource.assigned_[RES_MEM] = sum_load.memory_size() > report_resource.report_mem_assigned_
                                         ? static_cast<double>(sum_load.memory_size())
                                         : static_cast<double>(report_resource.report_mem_assigned_);
    server_resource.max_assigned_[RES_MEM] = server_resource.assigned_[RES_MEM];
    server_resource.capacity_[RES_MEM] = static_cast<double>(report_resource.mem_total_);
    // RES_LOG_DISK
    server_resource.assigned_[RES_LOG_DISK] = static_cast<double>(sum_load.log_disk_size());
    server_resource.max_assigned_[RES_LOG_DISK] = server_resource.assigned_[RES_LOG_DISK];
    server_resource.capacity_[RES_LOG_DISK] = static_cast<double>(report_resource.log_disk_total_);
    // RES_DATA_DISK
    server_resource.assigned_[RES_DATA_DISK] = static_cast<double>(MAX(sum_load.data_disk_size(),
                                               report_resource.report_data_disk_assigned_));
    server_resource.max_assigned_[RES_DATA_DISK] = server_resource.assigned_[RES_DATA_DISK];
    server_resource.capacity_[RES_DATA_DISK] = static_cast<double>(report_resource.data_disk_total_);
  }

  LOG_INFO("compute server resource", KR(ret),
            "server", server,
            K(server_resource),
            "report_resource_info", report_resource,
            "valid_unit_sum", sum_load,
            "valid_unit_count", unit_loads != NULL ? unit_loads->count(): 0);
  return ret;
}

// check resource enough for unit
//
// @param [in] u                              demands resource that may have some invalid items, need not check valid
// @param [out] not_enough_resource           returned resource type that is not enough
// @param [out] not_enough_resource_config    returned resource config type that is not enough
bool ObUnitManager::check_resource_enough_for_unit_(
    const ObUnitPlacementStrategy::ObServerResource &r,
    const ObUnitResource &u,
    const double hard_limit,
    ObResourceType &not_enough_resource,
    AlterResourceErr &not_enough_resource_config) const
{
  bool is_enough = false; // default is false

  if (u.is_max_cpu_valid() &&
      r.capacity_[RES_CPU] * hard_limit < r.max_assigned_[RES_CPU] + u.max_cpu()) {
    not_enough_resource = RES_CPU;
    not_enough_resource_config = MAX_CPU;
  } else if (u.is_min_cpu_valid() &&
             r.capacity_[RES_CPU] < r.assigned_[RES_CPU] + u.min_cpu()) {
    not_enough_resource = RES_CPU;
    not_enough_resource_config = MIN_CPU;
  } else if (u.is_memory_size_valid() &&
             r.capacity_[RES_MEM] < r.assigned_[RES_MEM] + u.memory_size()) {
    not_enough_resource = RES_MEM;
    not_enough_resource_config = MEMORY;
  } else if (u.is_log_disk_size_valid() &&
             r.capacity_[RES_LOG_DISK] < r.assigned_[RES_LOG_DISK] + u.log_disk_size()) {
    not_enough_resource = RES_LOG_DISK;
    not_enough_resource_config = LOG_DISK;
  } else if (u.is_data_disk_size_valid() &&
             r.capacity_[RES_DATA_DISK] < r.assigned_[RES_DATA_DISK] + u.data_disk_size()) {
    not_enough_resource = RES_DATA_DISK;
    not_enough_resource_config = DATA_DISK;
  } else {
    is_enough = true;
    not_enough_resource = RES_MAX;
    not_enough_resource_config = ALT_ERR;
  }

  if (! is_enough) {
    ObCStringHelper helper;
    _LOG_INFO("server %s resource '%s' is not enough for unit. hard_limit=%.6g, server_resource=%s, "
        "demands=%s",
        resource_type_to_str(not_enough_resource),
        alter_resource_err_to_str(not_enough_resource_config),
        hard_limit,
        helper.convert(r),
        helper.convert(u));
  }
  return is_enough;
}


// demand_resource may have some invalid items, need not check valid for demand_resource
int ObUnitManager::have_enough_resource(const obrpc::ObGetServerResourceInfoResult &report_server_resource_info,
                                        const ObUnitResource &demand_resource,
                                        const double hard_limit,
                                        bool &is_enough,
                                        AlterResourceErr &err_index) const
{
  int ret = OB_SUCCESS;
  ObResourceType not_enough_resource = RES_MAX;
  ObUnitPlacementStrategy::ObServerResource server_resource;
  err_index = ALT_ERR;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!report_server_resource_info.is_valid() || hard_limit <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(report_server_resource_info), K(hard_limit), K(ret));
  } else if (OB_FAIL(compute_server_resource_(report_server_resource_info, server_resource))) {
    LOG_WARN("compute server resource fail", KR(ret), K(report_server_resource_info));
  } else {
    is_enough = check_resource_enough_for_unit_(server_resource, demand_resource, hard_limit,
        not_enough_resource, err_index);
  }
  return ret;
}
int ObUnitManager::get_servers_resource_info_via_rpc(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  obrpc::ObGetServerResourceInfoArg arg;
  ObArray<obrpc::ObGetServerResourceInfoResult> tmp_report_servers_resource_info;
  report_servers_resource_info.reset();
  if (OB_UNLIKELY(servers_info.count() < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("servers_info.count() should be >= 0", KR(ret), K(servers_info.count()));
  } else if (0 == servers_info.count()) {
    // do nothing
  } else {
    if (OB_ISNULL(srv_rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("srv_rpc_proxy_ is null", KR(ret), KP(srv_rpc_proxy_));
    } else if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else {
      ObGetServerResourceInfoProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_server_resource_info);
      for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
        const ObServerInfoInTable & server_info = servers_info.at(i);
        arg.reset();
        if (OB_UNLIKELY(!server_info.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid server_info", KR(ret), K(server_info));
        } else {
          const ObAddr &server = server_info.get_server();
          const int64_t time_out = ctx.get_timeout();
          if (OB_FAIL(arg.init(GCTX.self_addr()))) {
            LOG_WARN("fail to init arg", KR(ret), K(GCTX.self_addr()));
          } else if (OB_FAIL(proxy.call(
              server,
              time_out,
              GCONF.cluster_id,
              OB_SYS_TENANT_ID,
              arg))) {
            LOG_WARN("fail to send get_server_resource_info rpc",  KR(ret), KR(tmp_ret), K(server),
                K(time_out), K(arg));
          }
        }
      }
      if (OB_TMP_FAIL(proxy.wait())) {
        LOG_WARN("fail to wait all batch result", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_SUCC(ret)) {
        ARRAY_FOREACH_X(proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
          const obrpc::ObGetServerResourceInfoResult *rpc_result = proxy.get_results().at(idx);
          if (OB_ISNULL(rpc_result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rpc_result is null", KR(ret), KP(rpc_result));
          } else if (OB_UNLIKELY(!rpc_result->is_valid())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("rpc_result is invalid", KR(ret), KPC(rpc_result));
          } else if (OB_FAIL(tmp_report_servers_resource_info.push_back(*rpc_result))) {
            LOG_WARN("fail to push an element into tmp_report_servers_resource_info", KR(ret), KPC(rpc_result));
          }
        }
      }
    }
    // get ordered report_servers_resource_info: since when processing resource_info,
    // we assume servers_info.at(i).get_server() = report_servers_resource_info.at(i).get_server()
    if (FAILEDx(order_report_servers_resource_info_(
        servers_info,
        tmp_report_servers_resource_info,
        report_servers_resource_info ))) {
      LOG_WARN("fail to order report_servers_resource_info", KR(ret),
          K(servers_info.count()), K(tmp_report_servers_resource_info.count()),
          K(servers_info), K(tmp_report_servers_resource_info));
    }
  }
  return ret;
}

int ObUnitManager::order_report_servers_resource_info_(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &ordered_report_servers_resource_info)
{
  // target: servers_info.at(i).get_server() = ordered_report_servers_resource_info.at(i).get_server()
  int ret = OB_SUCCESS;
  ordered_report_servers_resource_info.reset();
  if (servers_info.count() != report_servers_resource_info.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the size of servers_info should be equal to the size of report_servers_resource_info",
        KR(ret), K(servers_info.count()), K(report_servers_resource_info.count()),
        K(servers_info), K(report_servers_resource_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
      bool find_server = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find_server && j < report_servers_resource_info.count(); j++) {
        const obrpc::ObGetServerResourceInfoResult &server_resource_info = report_servers_resource_info.at(j);
        if (servers_info.at(i).get_server() == server_resource_info.get_server()) {
          find_server = true;
          if (OB_FAIL(ordered_report_servers_resource_info.push_back(server_resource_info))) {
            LOG_WARN("fail to push an element into ordered_report_servers_resource_info",
                KR(ret), K(server_resource_info));
          }
        }
      }
      if(OB_SUCC(ret) && !find_server) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server not exists in report_servers_resource_info",
            K(servers_info.at(i)), K(report_servers_resource_info));
      }
    }
  }
  return ret;
}
int ObUnitManager::get_server_resource_info_via_rpc(
    const share::ObServerInfoInTable &server_info,
    obrpc::ObGetServerResourceInfoResult &report_server_resource_info) const
{
  int ret = OB_SUCCESS;
  ObArray<share::ObServerInfoInTable> servers_info;
  ObArray<obrpc::ObGetServerResourceInfoResult> report_resource_info_array;
  report_server_resource_info.reset();
  if (OB_UNLIKELY(!server_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_info is invalid", KR(ret), K(server_info));
  } else if (OB_FAIL(servers_info.push_back(server_info))) {
    LOG_WARN("fail to push an element into servers_info", KR(ret), K(server_info));
  } else if (OB_FAIL(get_servers_resource_info_via_rpc(servers_info, report_resource_info_array))) {
    LOG_WARN("fail to execute get_servers_resource_info_via_rpc", KR(ret), K(servers_info));
  } else if (OB_UNLIKELY(1 != report_resource_info_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("report_resource_info_array.count() should be one", KR(ret), K(report_resource_info_array.count()));
  } else if (OB_FAIL(report_server_resource_info.assign(report_resource_info_array.at(0)))) {
    LOG_WARN("fail to assign report_server_resource_info", KR(ret), K(report_resource_info_array.at(0)));
  }
  return ret;
}

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

int ObUnitManager::build_sorted_zone_unit_ptr_array(
    share::ObResourcePool *pool,
    common::ObIArray<ZoneUnitPtr> &zone_unit_ptrs)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit *> *units = NULL;
  zone_unit_ptrs.reset();
  if (OB_UNLIKELY(NULL == pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool));
  } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", K(ret), "pool_id", pool->resource_pool_id_);
  } else if (OB_UNLIKELY(NULL == units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units ptr is null", K(ret), KP(units));
  } else {
    // traverse all unit in units
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      ObUnit *this_unit = units->at(i);
      if (OB_UNLIKELY(NULL == this_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret), KP(this_unit));
      } else {
        // aggregate units with the same zone
        int64_t index = 0;
        for (; OB_SUCC(ret) && index < zone_unit_ptrs.count(); ++index) {
          if (this_unit->zone_ == zone_unit_ptrs.at(index).zone_) {
            break;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (index >= zone_unit_ptrs.count()) {
          ZoneUnitPtr zone_unit_ptr;
          zone_unit_ptr.zone_ = this_unit->zone_;
          if (OB_FAIL(zone_unit_ptrs.push_back(zone_unit_ptr))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (index >= zone_unit_ptrs.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", K(ret), K(index),
                   "zone_unit_count", zone_unit_ptrs.count());
        } else if (this_unit->zone_ != zone_unit_ptrs.at(index).zone_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone not match", K(ret), "left_zone", this_unit->zone_,
                   "right_zone", zone_unit_ptrs.at(index).zone_);
        } else if (OB_FAIL(zone_unit_ptrs.at(index).unit_ptrs_.push_back(this_unit))) {
          LOG_WARN("fail to push back", K(ret), K(index));
        } else {} // good, no more to do
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_unit_ptrs.count(); ++i) {
      // sort each zone unit ptr using group id
      ZoneUnitPtr &zone_unit_ptr = zone_unit_ptrs.at(i);
      if (OB_FAIL(zone_unit_ptr.sort_by_unit_id_desc())) {
        LOG_WARN("fail to sort unit", K(ret));
      } else {} // good, unit num match
    }
  }
  return ret;
}

int ObUnitManager::check_shrink_unit_num_zone_condition(
    share::ObResourcePool *pool,
    const int64_t alter_unit_num,
    const common::ObIArray<uint64_t> &delete_unit_id_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == pool || alter_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(alter_unit_num));
  } else if (delete_unit_id_array.count() <= 0) {
    // good, we choose deleting set all by ourselves
  } else if (pool->unit_count_ <= alter_unit_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be a shrink pool operation", K(ret),
             "cur_unit_num", pool->unit_count_,
             "new_unit_num", alter_unit_num);
  } else {
    int64_t delta = pool->unit_count_ - alter_unit_num;
    const common::ObIArray<common::ObZone> &zone_list = pool->zone_list_;
    common::ObArray<ZoneUnitPtr> delete_zone_unit_ptrs;
    for (int64_t i = 0; OB_SUCC(ret) && i < delete_unit_id_array.count(); ++i) {
      ObUnit *this_unit = NULL;
      if (OB_FAIL(get_unit_by_id(delete_unit_id_array.at(i), this_unit))) {
        LOG_WARN("fail to get unit by id", K(ret));
      } else if (OB_UNLIKELY(NULL == this_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret), KP(this_unit));
      } else if (this_unit->resource_pool_id_ != pool->resource_pool_id_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("do not support shrink unit belonging to other pool");
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "shrink unit belonging to other pool");
      } else if (!has_exist_in_array(zone_list, this_unit->zone_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit zone not match", K(ret), K(zone_list), "unit_zone", this_unit->zone_);
      } else {
        int64_t index = 0;
        for (; OB_SUCC(ret) && index < delete_zone_unit_ptrs.count(); ++index) {
          if (this_unit->zone_ == delete_zone_unit_ptrs.at(index).zone_) {
            break;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (index >= delete_zone_unit_ptrs.count()) {
          ZoneUnitPtr delete_zone_unit_ptr;
          delete_zone_unit_ptr.zone_ = this_unit->zone_;
          if (OB_FAIL(delete_zone_unit_ptrs.push_back(delete_zone_unit_ptr))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (index >= delete_zone_unit_ptrs.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", K(ret), K(index),
                   "delete_zone_unit_count", delete_zone_unit_ptrs.count());
        } else if (this_unit->zone_ != delete_zone_unit_ptrs.at(index).zone_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("zone not match", K(ret), "left_zone", this_unit->zone_,
                   "right_zone", delete_zone_unit_ptrs.at(index).zone_);
        } else if (OB_FAIL(delete_zone_unit_ptrs.at(index).unit_ptrs_.push_back(this_unit))) {
          LOG_WARN("fail to push back", K(ret), K(index));
        } else {} // good, no more to do
      }
    }
    if (OB_SUCC(ret)) {
      if (delete_zone_unit_ptrs.count() != zone_list.count()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("do not support shrink unit num to different value on different zone", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "shrink unit num to different value on different zone");
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < delete_zone_unit_ptrs.count(); ++i) {
        if (delta != delete_zone_unit_ptrs.at(i).unit_ptrs_.count()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("shrink mismatching unit num and unit id list not support", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "shrink mismatching unit num and unit id list");
        } else {} // good, go on to check next
      }
    }
  }
  return ret;
}

int ObUnitManager::fill_delete_unit_ptr_array(
    share::ObResourcePool *pool,
    const common::ObIArray<uint64_t> &delete_unit_id_array,
    const int64_t alter_unit_num,
    common::ObIArray<ObUnit *> &output_delete_unit_ptr_array)
{
  int ret = OB_SUCCESS;
  output_delete_unit_ptr_array.reset();
  if (delete_unit_id_array.count() > 0) {
    // The alter resource pool shrinkage specifies the deleted unit, just fill it in directly
    for (int64_t i = 0; OB_SUCC(ret) && i < delete_unit_id_array.count(); ++i) {
      ObUnit *unit = NULL;
      if (OB_FAIL(get_unit_by_id(delete_unit_id_array.at(i), unit))) {
        LOG_WARN("fail to get unit by id", K(ret));
      } else if (NULL == unit) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret), KP(unit));
      } else if (OB_FAIL(output_delete_unit_ptr_array.push_back(unit))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
  } else {
    common::ObArray<ZoneUnitPtr> zone_unit_ptrs;
    if (OB_UNLIKELY(NULL == pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(ret), KP(pool));
    } else if (OB_FAIL(build_sorted_zone_unit_ptr_array(pool, zone_unit_ptrs))) {
      LOG_WARN("fail to build sorted zone unit ptr array", K(ret));
    } else if (zone_unit_ptrs.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone unit ptrs has no element", K(ret), "zone_unit_cnt", zone_unit_ptrs.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_unit_ptrs.count(); ++i) {
        const ZoneUnitPtr &zone_unit_ptr = zone_unit_ptrs.at(i);
        for (int64_t j = alter_unit_num; OB_SUCC(ret) && j < zone_unit_ptr.unit_ptrs_.count(); ++j) {
          ObUnit *unit = zone_unit_ptr.unit_ptrs_.at(j);
          if (OB_UNLIKELY(NULL == unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", K(ret), KP(unit), K(i), K(j));
          } else if (OB_FAIL(output_delete_unit_ptr_array.push_back(unit))) {
            LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
      }
    }
  }
  return ret;
}


// the resource pool don't grant any tenants shrink directly.
// step:
// 1. clear __all_unit, change the unit num in __all_resouce_pool
// 2. clear the unit info in memory structure, change the unit_num in memroy structure resource pool
int ObUnitManager::shrink_not_granted_pool(
    share::ObResourcePool *pool,
    const int64_t alter_unit_num,
    const common::ObIArray<uint64_t> &delete_unit_id_array)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  if (OB_UNLIKELY(NULL == pool || alter_unit_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), K(alter_unit_num));
  } else if (is_valid_tenant_id(pool->tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool already grant to some tenant", K(ret), "tenant_id", pool->tenant_id_);
  } else if (pool->unit_count_ <= alter_unit_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be a shrink pool operation", K(ret),
             "cur_unit_num", pool->unit_count_,
             "new_unit_num", alter_unit_num);
  } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    common::ObArray<ObUnit *> output_delete_unit_ptr_array;
    common::ObArray<uint64_t> output_delete_unit_id_array;
    share::ObResourcePool new_pool;
    if (OB_FAIL(new_pool.assign(*pool))) {
      LOG_WARN("fail to assign new pool", K(ret));
    } else {
      new_pool.unit_count_ = alter_unit_num;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
      LOG_WARN("fail to update resource pool", K(ret));
    } else if (OB_FAIL(check_shrink_unit_num_zone_condition(pool, alter_unit_num, delete_unit_id_array))) {
      LOG_WARN("fail to check shrink unit num zone condition", K(ret));
    } else if (OB_FAIL(fill_delete_unit_ptr_array(
            pool, delete_unit_id_array, alter_unit_num, output_delete_unit_ptr_array))) {
      LOG_WARN("fail to fill delete unit id array", K(ret));
    } else if (output_delete_unit_ptr_array.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone unit ptrs has no element", K(ret),
               "zone_unit_cnt", output_delete_unit_ptr_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_delete_unit_ptr_array.count(); ++i) {
        const ObUnit *unit = output_delete_unit_ptr_array.at(i);
        if (OB_UNLIKELY(NULL == unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", K(ret), KP(unit));
        } else if (OB_FAIL(ut_operator_.remove_unit(trans, *unit))) {
          LOG_WARN("fail to remove unit", K(ret), "unit", *unit);
        } else if (OB_FAIL(output_delete_unit_id_array.push_back(unit->unit_id_))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
    // however, we need to end this transaction
    const bool commit = (OB_SUCCESS == ret);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
      LOG_WARN("trans end failed", K(tmp_ret), K(commit));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(delete_inmemory_units(pool->resource_pool_id_, output_delete_unit_id_array))) {
      LOG_WARN("fail to delete unit groups", K(ret));
    } else {
      pool->unit_count_ = alter_unit_num;
    }
  }
  return ret;
}

int ObUnitManager::get_zone_pools_unit_num(
    const common::ObZone &zone,
    const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
    int64_t &total_unit_num,
    int64_t &full_unit_num,
    int64_t &logonly_unit_num)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    common::ObArray<share::ObResourcePool *> pool_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_pool_name_list.count(); ++i) {
      const share::ObResourcePoolName &pool_name = new_pool_name_list.at(i);
      share::ObResourcePool *pool = NULL;
      if (OB_FAIL(inner_get_resource_pool_by_name(pool_name, pool))) {
        LOG_WARN("fail to get resource pool by name", K(ret));
      } else if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), K(pool_name));
      } else if (OB_FAIL(pool_list.push_back(pool))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inner_get_zone_pools_unit_num(
            zone, pool_list, total_unit_num, full_unit_num, logonly_unit_num))) {
      LOG_WARN("fail to inner get pools unit num", K(ret));
    }
  }
  return ret;
}

int ObUnitManager::inner_get_zone_pools_unit_num(
    const common::ObZone &zone,
    const common::ObIArray<share::ObResourcePool *> &pool_list,
    int64_t &total_unit_num,
    int64_t &full_unit_num,
    int64_t &logonly_unit_num)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    total_unit_num = 0;
    full_unit_num = 0;
    logonly_unit_num = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_list.count(); ++i) {
      share::ObResourcePool *pool = pool_list.at(i);
      if (OB_UNLIKELY(NULL == pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret));
      } else if (!has_exist_in_array(pool->zone_list_, zone)) {
        //ignore
      } else {
        total_unit_num += pool->unit_count_;
        if (common::REPLICA_TYPE_FULL == pool->replica_type_) {
          full_unit_num += pool->unit_count_;
        } else if (common::REPLICA_TYPE_LOGONLY == pool->replica_type_) {
          logonly_unit_num += pool->unit_count_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("do not support this pool type", K(ret), K(*pool));
        }
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

int ObUnitManager::construct_pool_units_to_grant_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const share::ObResourcePool &new_pool,
    const common::ObIArray<share::ObUnit *> &zone_sorted_unit_array,
    const common::ObIArray<uint64_t> &new_ug_ids,
    const lib::Worker::CompatMode &compat_mode,
    ObNotifyTenantServerResourceProxy &notify_proxy,
    ObIArray<share::ObUnit> &pool_units,
    const bool check_data_version)
{
  int ret = OB_SUCCESS;
  pool_units.reset();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret), K_(inited), K_(loaded));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || OB_UNLIKELY(0 >= zone_sorted_unit_array.count())
             || OB_UNLIKELY(0 >= new_ug_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(zone_sorted_unit_array),
             K(new_ug_ids));
  } else {
    ObUnit new_unit;
    for (int64_t j = 0; OB_SUCC(ret) && j < zone_sorted_unit_array.count(); ++j) {
      ObUnit *unit = zone_sorted_unit_array.at(j);
      uint64_t unit_group_id_to_set = 0;
      new_unit.reset();
      if (OB_ISNULL(unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", KR(ret));
      } else if (OB_FAIL(try_notify_tenant_server_unit_resource_(
                             tenant_id, false /*is_delete*/, notify_proxy,
                             new_pool.unit_config_id_, compat_mode, *unit,
                             false/*if_not_grant*/, false/*skip_offline_server*/,
                             check_data_version))) {
        LOG_WARN("fail to try notify server unit resource", KR(ret),
                 K(tenant_id), K(compat_mode), KPC(unit));
      } else if (FALSE_IT(new_unit = *unit)) {
        // shall never be here
      } else if (OB_FAIL(construct_unit_group_id_for_unit_(
                             *unit,
                             j/*unit_index*/,
                             new_pool.unit_count_,
                             new_ug_ids,
                             unit_group_id_to_set))) {
        LOG_WARN("fail to construct unit group id for unit", KR(ret),
                 KPC(unit), K(j), K(new_pool), K(new_ug_ids));
      } else if (FALSE_IT(new_unit.unit_group_id_ = unit_group_id_to_set)) {
        // shall never be here
      } else if (OB_FAIL(ut_operator_.update_unit(trans, new_unit))) {
        LOG_WARN("fail to update unit", KR(ret), K(new_unit));
      } else if (OB_FAIL(pool_units.push_back(*unit))) {
        LOG_WARN("fail to push an element into pool_units", KR(ret), KPC(unit));
      }
    }
  }
  return ret;
}

int ObUnitManager::construct_unit_group_id_for_unit_(
    const ObUnit &target_unit,
    const int64_t unit_index,
    const int64_t unit_num,
    const common::ObIArray<uint64_t> &new_ug_ids,
    uint64_t &unit_group_id)
{
  int ret = OB_SUCCESS;
  unit_group_id = OB_INVALID_ID;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret), K_(inited), K_(loaded));
  } else if (OB_UNLIKELY(0 >= unit_num)
             || OB_UNLIKELY(0 > unit_index)
             || OB_UNLIKELY(new_ug_ids.count() != unit_num)
             || OB_UNLIKELY(0 >= new_ug_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_index),
             K(unit_num), K(new_ug_ids));
  } else {
    // for tenant not clone, just allocate new unit group ids by order
    unit_group_id = new_ug_ids.at(unit_index % (unit_num));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_INVALID_ID == unit_group_id)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can not find a valid unit group id", KR(ret), K(unit_group_id), K(target_unit));
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
    ObArray<share::ObResourcePool> pools;
    ObArray<ObArray<ObUnit>> all_pool_units;
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
      } else if (OB_FAIL(build_zone_sorted_unit_array_(pool, zone_sorted_unit_array))) {
        LOG_WARN("failed to generate zone_sorted_unit_array", KR(ret), KPC(pool));
      } else if (OB_FAIL(construct_pool_units_to_grant_(
                             trans,
                             tenant_id,
                             new_pool,
                             zone_sorted_unit_array,
                             new_ug_ids,
                             compat_mode,
                             notify_proxy,
                             pool_units,
                             check_data_version))) {
        LOG_WARN("fail to construct pool units to grant", KR(ret), K(tenant_id), K(new_pool),
                 K(zone_sorted_unit_array), K(new_ug_ids), K(compat_mode));
      } else if (OB_FAIL(all_pool_units.push_back(pool_units))) {
        LOG_WARN("fail to push an element into all_pool_units", KR(ret), K(pool_units));
      } else if (OB_FAIL(pools.push_back(new_pool))) {
        LOG_WARN("fail to push an element into pools", KR(ret), K(new_pool));
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
    if (OB_FAIL(ret) && pools.count() == all_pool_units.count()) {
      LOG_WARN("start to rollback unit persistence", KR(ret), K(pools), K(tenant_id));
      for (int64_t i = 0; i < pools.count(); ++i) {
        if (OB_TMP_FAIL(rollback_persistent_units_(all_pool_units.at(i), pools.at(i), notify_proxy))) {
          LOG_WARN("fail to rollback unit persistence", KR(ret), KR(tmp_ret), K(all_pool_units.at(i)),
              K(pools.at(i)), K(compat_mode));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::do_revoke_pools_(
    ObMySQLTransaction &trans,
    const common::ObIArray<uint64_t> &new_ug_ids,
    const ObIArray<ObResourcePoolName> &pool_names,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const lib::Worker::CompatMode dummy_mode = lib::Worker::CompatMode::INVALID;
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
    ObArray<share::ObResourcePool *> shrinking_pools;
    ObArray<share::ObResourcePool> pools;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_names.count(); ++i) {
      share::ObResourcePool *pool = NULL;
      bool is_shrinking = false;
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
      } else if (OB_FAIL(inner_check_pool_in_shrinking_(pool->resource_pool_id_, is_shrinking))) {   // TODO(cangming.zl): maybe need to move ahead
        LOG_WARN("inner check pool in shrinking failed", KR(ret), "pool", *pool, K(is_shrinking));
      } else if (is_shrinking && OB_FAIL(shrinking_pools.push_back(pool))) {
        LOG_WARN("fail to push back shrinking resource pool before revoked", KR(ret), K(tenant_id), "pool", *pool);
      } else if (OB_FAIL(new_pool.assign(*pool))) {
        LOG_WARN("failed to assign new_pool", KR(ret));
      } else if (FALSE_IT(new_pool.tenant_id_ = OB_INVALID_ID)) {
        // shall never be here
      } else if (OB_FAIL(ut_operator_.update_resource_pool(trans, new_pool))) {
        LOG_WARN("update_resource_pool failed", K(new_pool), KR(ret));
      } else if (OB_FAIL(build_zone_sorted_unit_array_(pool, zone_sorted_unit_array))) {
        LOG_WARN("failed to generate zone_sorted_unit_array", KR(ret), KPC(pool));
      } else {
        ObUnit new_unit;
        for (int64_t j = 0; OB_SUCC(ret) && j < zone_sorted_unit_array.count(); ++j) {
          ObUnit *unit = zone_sorted_unit_array.at(j);
          new_unit.reset();
          if (OB_ISNULL(unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", KR(ret));
          } else if (OB_FAIL(try_notify_tenant_server_unit_resource_(
                  tenant_id, true /*is_delete*/, notify_proxy,
                  new_pool.unit_config_id_, dummy_mode, *unit,
                  false/*if_not_grant*/, false/*skip_offline_server*/,
                  false /*check_data_version*/))) {
            LOG_WARN("fail to try notify server unit resource", KR(ret));
          } else if (FALSE_IT(new_unit = *unit)) {
            // shall never be here
          } else if (FALSE_IT(new_unit.unit_group_id_ = new_ug_ids.at(j % (pool->unit_count_)))) {
            // shall never be here
          } else if (OB_FAIL(ut_operator_.update_unit(trans, new_unit))) {
            LOG_WARN("fail to update unit", KR(ret));
          }
        }
      }
    }
    // If some of the pools are shrinking, commit these shrinking pools now.
    if (OB_SUCCESS != ret) {
    } else if (shrinking_pools.count() > 0 && OB_FAIL(inner_commit_shrink_tenant_resource_pool_(trans, tenant_id, shrinking_pools))) {
      LOG_WARN("failed to commit shrinking pools in revoking", KR(ret), K(tenant_id));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", KR(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      // arg/dest/result can be used here.
    }
  }
  return ret;
}

int ObUnitManager::build_zone_sorted_unit_array_(const share::ObResourcePool *pool,
                                             common::ObArray<share::ObUnit*> &zone_sorted_units)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObUnit*> *units;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), KR(ret));
  } else if (OB_ISNULL(pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is nullptr", KR(ret));
  } else if (OB_FAIL(get_units_by_pool(pool->resource_pool_id_, units))) {
    LOG_WARN("fail to get units by pool", KR(ret));
  } else if (OB_ISNULL(units)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units ptr is null", KR(ret));
  } else if (OB_FAIL(zone_sorted_units.assign(*units))) {
    LOG_WARN("fail to assign zone unit array", KR(ret));
  } else {
    UnitZoneOrderCmp cmp_operator;
    lib::ob_sort(zone_sorted_units.begin(), zone_sorted_units.end(), cmp_operator);
    // check unit count in each zone
    ObUnit *curr_unit = NULL;
    ObUnit *prev_unit = NULL;
    int64_t unit_count_per_zone = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < zone_sorted_units.count(); ++j) {
      prev_unit = curr_unit;
      curr_unit = zone_sorted_units.at(j);
      if (OB_ISNULL(curr_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", KR(ret));
      } else if (0 == j || curr_unit->zone_ == prev_unit->zone_) {
        unit_count_per_zone += ObUnit::UNIT_STATUS_ACTIVE == curr_unit->status_ ? 1 : 0;
      } else if (unit_count_per_zone != pool->unit_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone unit num doesn't match resource pool's unit_count",
                 KR(ret), "zone", prev_unit->zone_, K(unit_count_per_zone), K(pool->unit_count_));
      } else {
        unit_count_per_zone = ObUnit::UNIT_STATUS_ACTIVE == curr_unit->status_ ? 1 : 0;
      }
    }
    if (OB_SUCC(ret) && unit_count_per_zone != pool->unit_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone unit num doesn't match resource pool's unit_count",
                KR(ret), "zone", curr_unit->zone_, K(unit_count_per_zone), K(pool->unit_count_));
    }
  }
  return ret;
}

// this function is only used to commit part of shrinking pools of a tenant when these pools are revoked.
int ObUnitManager::inner_commit_shrink_tenant_resource_pool_(
  common::ObMySQLTransaction &trans, const uint64_t tenant_id, const common::ObArray<share::ObResourcePool *> &pools)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret), K(loaded_), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", KR(ret), K(pools));
  } else {
    // first check that pools are all owned by specified tenant
    FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret) {
      if (OB_ISNULL(*pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null pointer", KR(ret), KP(*pool));
      } else if (tenant_id != (*pool)->tenant_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is not owned by specified tenant", KR(ret), K(tenant_id), KPC(*pool));
      }
    }
    ObArray<ObArray<uint64_t>> resource_units;
    if (FAILEDx(commit_shrink_resource_pool_in_trans_(pools, trans, resource_units))) {
      LOG_WARN("failed to shrink in trans", KR(ret), K(pools));
    } else if (OB_UNLIKELY(resource_units.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("resource units is empty", KR(ret), K(resource_units));
    } else {/* good */}
  }
  return ret;
}

int ObUnitManager::get_zone_units(const ObArray<share::ObResourcePool *> &pools,
                                  ObArray<ZoneUnit> &zone_units) const
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zones;
  zone_units.reuse();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K_(inited), K_(loaded), K(ret));
  } else if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pools is empty", K(pools), K(ret));
  } else {
    FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret) {
      if (NULL == *pool) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("pool is null", "pool", OB_P(*pool), K(ret));
      } else {
        FOREACH_CNT_X(pool_zone, (*pool)->zone_list_, OB_SUCCESS == ret) {
          bool find = false;
          FOREACH_CNT_X(zone, zones, !find) {
            if (*zone == *pool_zone) {
              find = true;
            }
          }
          if (!find) {
            if (OB_FAIL(zones.push_back(*pool_zone))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
        }
      }
    }

    ZoneUnit zone_unit;
    ObArray<ObUnitInfo> unit_infos;
    FOREACH_CNT_X(zone, zones, OB_SUCCESS == ret) {
      zone_unit.reset();
      zone_unit.zone_ = *zone;
      FOREACH_CNT_X(pool, pools, OB_SUCCESS == ret) {
        unit_infos.reuse();
        if (NULL == *pool) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("pool is null", "pool", OB_P(*pool), K(ret));
        } else if (OB_FAIL(inner_get_unit_infos_of_pool_((*pool)->resource_pool_id_, unit_infos))) {
          LOG_WARN("inner_get_unit_infos_of_pool failed",
              "pool id", (*pool)->resource_pool_id_, K(ret));
        } else {
          FOREACH_CNT_X(unit_info, unit_infos, OB_SUCCESS == ret) {
            if (unit_info->unit_.zone_ == *zone) {
              if (OB_FAIL(zone_unit.unit_infos_.push_back(*unit_info))) {
                LOG_WARN("push_back failed", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(zone_units.push_back(zone_unit))) {
          LOG_WARN("push_back failed", K(zone_unit), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_tenants_of_server(const common::ObAddr &server,
    common::hash::ObHashSet<uint64_t> &tenant_id_set) const
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitLoad> *unit_loads = NULL;
  {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(check_inner_stat_())) {
      LOG_WARN("check inner stat failed", K_(inited), K_(loaded), K(ret));
    } else if (!server.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("server is invalid", K(server), K(ret));
    } else if (OB_FAIL(get_loads_by_server(server, unit_loads))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_loads_by_server failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        // just return empty set
      }
    } else if (NULL == unit_loads) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_loads is null", KP(unit_loads), K(ret));
    }
    if (OB_SUCC(ret) && !OB_ISNULL(unit_loads)) {
      FOREACH_CNT_X(unit_load, *unit_loads, OB_SUCCESS == ret) {
        if (!unit_load->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid unit_load", "unit_load", *unit_load, K(ret));
        } else {
          const uint64_t tenant_id = unit_load->pool_->tenant_id_;
          if (!is_valid_tenant_id(tenant_id)) {
            //do nothing
          } else if (OB_FAIL(tenant_id_set.set_refactored(tenant_id))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("set tenant id failed", K(tenant_id), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::check_tenant_on_server(const uint64_t tenant_id,
    const ObAddr &server, bool &on_server) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K_(inited), K_(loaded), K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or invalid server", K(tenant_id), K(server), K(ret));
  } else {
    ObArray<uint64_t> pool_ids;
    ObZone zone;
    ObArray<ObAddr> servers;
    if (OB_FAIL(get_pool_ids_of_tenant(tenant_id, pool_ids))) {
      LOG_WARN("get_pool_ids_of_tenant failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(SVR_TRACER.get_server_zone(server, zone))) {
      LOG_WARN("get_server_zone failed", K(server), K(ret));
    } else {
      SpinRLockGuard guard(lock_);
      FOREACH_CNT_X(pool_id, pool_ids, OB_SUCCESS == ret && !on_server) {
        if (OB_FAIL(get_pool_servers(*pool_id, zone, servers))) {
          LOG_WARN("get_pool_servers failed", "pool_id", *pool_id, K(zone), K(ret));
        } else if (has_exist_in_array(servers, server)) {
          on_server = true;
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_hard_limit(double &hard_limit) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", K_(inited), K_(loaded), K(ret));
  } else {
    hard_limit = static_cast<double>(server_config_->resource_hard_limit) / 100;
  }
  return ret;
}

int ObUnitManager::inner_try_delete_migrate_unit_resource(
    const uint64_t unit_id,
    const common::ObAddr &migrate_from_server)
{
  int ret = OB_SUCCESS;
  ObUnit *unit = NULL;
  share::ObResourcePool *pool = NULL;
  share::ObUnitConfig *unit_config = nullptr;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  bool is_alive = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat unexpected", K(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_id));
  } else if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
    LOG_WARN("fail to get unit by id", K(ret), K(unit_id));
  } else if (OB_ISNULL(unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit ptr is null", K(ret), KP(unit));
  } else if (!migrate_from_server.is_valid()) {
    LOG_INFO("unit not in migrating, no need to delete src resource", K(unit_id));
  } else if (OB_FAIL(SVR_TRACER.check_server_alive(migrate_from_server, is_alive))) {
    LOG_WARN("fail to check server alive", K(ret), "server", migrate_from_server);
  } else if (!is_alive) {
    LOG_INFO("src server not alive, ignore notify",
             K(unit_id), "server", migrate_from_server);
  } else if (OB_FAIL(get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
    LOG_WARN("failed to get pool", K(ret), K(unit));
  } else if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", K(ret), KP(pool));
  } else if (OB_FAIL(get_unit_config_by_id(pool->unit_config_id_, unit_config))) {
    LOG_WARN("fail to get unit config by id", K(ret));
  } else if (OB_UNLIKELY(nullptr == unit_config)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit config is null", K(ret), "unit_config_id", pool->unit_config_id_);
  } else if (!pool->is_granted_to_tenant()) {
    LOG_INFO("unit is not granted to any tenant", K(ret), "tenant_id", pool->tenant_id_);
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(pool->tenant_id_, compat_mode))) {
    LOG_WARN("fail to get tenant compat mode", K(ret),
             "tenant_id", pool->tenant_id_, K(unit_id), "pool", *pool);
  } else {
    const int64_t rpc_timeout = NOTIFY_RESOURCE_RPC_TIMEOUT;
    obrpc::TenantServerUnitConfig tenant_unit_server_config;
    ObNotifyTenantServerResourceProxy notify_proxy(
                                      *srv_rpc_proxy_,
                                      &obrpc::ObSrvRpcProxy::notify_tenant_server_unit_resource);
    if (OB_FAIL(tenant_unit_server_config.init_for_dropping(pool->tenant_id_, true/*delete*/))) {
      LOG_WARN("fail to init tenant server unit config", K(ret), "tenant_id", pool->tenant_id_);
    } else if (OB_FAIL(notify_proxy.call(
            migrate_from_server, rpc_timeout, tenant_unit_server_config))) {
      LOG_WARN("fail to call notify resource to server",
               K(ret), K(rpc_timeout), "unit", *unit, "dest", migrate_from_server);
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      // arg/dest/result can be used here.
      LOG_INFO("notify resource to server succeed", "unit", *unit, "dest", migrate_from_server);
    }
  }
  return ret;
}

int ObUnitManager::end_migrate_unit(const uint64_t unit_id, const EndMigrateOp end_migrate_op)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_id), K(ret));
  } else if (end_migrate_op < COMMIT || end_migrate_op > REVERSE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid end_migrate_op", K(end_migrate_op), K(ret));
  } else {
    ObUnit *unit = NULL;
    common::ObMySQLTransaction trans;
    if (OB_FAIL(get_unit_by_id(unit_id, unit))) {
      LOG_WARN("get_unit_by_id failed", K(unit_id), K(ret));
    } else if (NULL == unit) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit is null", KP(unit), K(ret));
    } else if (!unit->migrate_from_server_.is_valid()) {
      // FIXME(jingqian): when can this happened, figure it out
      ret = OB_SUCCESS;
      LOG_WARN("unit is not in migrating status, maybe end_migrate_unit has ever called",
               "unit", *unit, K(ret));
    } else {
      const ObAddr migrate_from_server = unit->migrate_from_server_;
      const ObAddr unit_server = unit->server_;
      const bool is_manual = unit->is_manual_migrate();
      ObUnit new_unit = *unit;
      new_unit.is_manual_migrate_ = false;  // clear manual_migrate
      // generate new unit
      if (COMMIT == end_migrate_op) {
        new_unit.migrate_from_server_.reset();
      } else if (ABORT == end_migrate_op) {
        new_unit.server_ = unit->migrate_from_server_;
        new_unit.migrate_from_server_.reset();
      } else {
        new_unit.server_ = unit->migrate_from_server_;
        new_unit.migrate_from_server_ = unit->server_;
      }

      // update unit in sys_table and in memory
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to start transaction ", K(ret));
      } else if (OB_FAIL(ut_operator_.update_unit(trans, new_unit))) {
        LOG_WARN("ut_operator update unit failed", K(new_unit), K(ret));
      } else {
        if (ABORT == end_migrate_op) {
          if (OB_FAIL(delete_unit_load(unit->server_, unit->unit_id_))) {
            LOG_WARN("delete_unit_load failed", "unit", *unit, K(ret));
          }
        } else if (COMMIT == end_migrate_op) {
          if (OB_FAIL(delete_unit_load(unit->migrate_from_server_, unit->unit_id_))) {
            LOG_WARN("delete_unit_load failed", "unit", *unit, K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          *unit = new_unit;
        }
      }

      // delete migrating unit from migrate_units of migrate_from_server,
      // if REVERSE == op, add migrating unit to migrate_units of unit_server
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(delete_migrate_unit(migrate_from_server, unit->unit_id_))) {
        LOG_WARN("delete_migrate_unit failed", K(migrate_from_server),
                 "unit_id", unit->unit_id_, K(ret));
      } else if (REVERSE == end_migrate_op) {
        if (OB_FAIL(insert_migrate_unit(unit_server, unit->unit_id_))) {
          LOG_WARN("insert_migrate_unit failed", K(unit_server), "unit_id",
                   unit->unit_id_, K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        share::ObResourcePool *pool = NULL;
        if (OB_SUCCESS != (tmp_ret = get_resource_pool_by_id(unit->resource_pool_id_, pool))) {
          LOG_WARN("failed to get pool", K(tmp_ret), K(unit));
        } else {
          tenant_id = pool->tenant_id_;
        }
        ROOTSERVICE_EVENT_ADD("unit", "finish_migrate_unit",
                              "unit_id", unit_id,
                              "end_op", end_migrate_op_type_to_str(end_migrate_op),
                              "migrate_from_server", migrate_from_server,
                              "server", unit_server,
                              "tenant_id", tenant_id,
                              "manual_migrate", is_manual ? "YES" : "NO");

        // complete the job if exists
        char ip_buf[common::MAX_IP_ADDR_LENGTH];
        (void)unit_server.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
        int64_t job_id = 0;
        tmp_ret = RS_JOB_FIND(MIGRATE_UNIT, job_id, trans, "unit_id", unit_id,
                              "svr_ip", ip_buf, "svr_port", unit_server.get_port());
        if (OB_SUCCESS == tmp_ret && job_id > 0) {
          tmp_ret = (end_migrate_op == COMMIT) ? OB_SUCCESS :
              (end_migrate_op == REVERSE ? OB_CANCELED : OB_TIMEOUT);
          if (OB_FAIL(RS_JOB_COMPLETE(job_id, tmp_ret, trans))) {
            LOG_WARN("all_rootservice_job update failed", K(ret), K(job_id));
          }
        } else {
          //Can not find the situation, only the user manually opened will write rs_job
          LOG_WARN("no rs job", K(ret), K(tmp_ret), K(unit_id));
        }
      }
      const bool commit = OB_SUCC(ret) ? true:false;
      int tmp_ret = OB_SUCCESS ;
      if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
        LOG_WARN("tran commit failed", K(tmp_ret));
      }
      ret = OB_SUCC(ret) ? tmp_ret : ret;

      if (OB_SUCC(ret) && COMMIT == end_migrate_op && OB_INVALID_ID != tenant_id) {
        (void)inner_try_delete_migrate_unit_resource(unit_id, migrate_from_server);
      }
    }
  }

  LOG_INFO("end migrate unit", K(unit_id), K(end_migrate_op), K(ret));
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

int ObUnitManager::update_pool_load(share::ObResourcePool *pool,
    share::ObUnitConfig *new_config)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == pool || NULL == new_config) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(pool), KP(new_config));
  } else {
    FOREACH_X(sl, server_loads_, OB_SUCC(ret)) {
      if (NULL == sl->second) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL value", K(ret));
      }
      FOREACH_X(l, *sl->second, OB_SUCC(ret)) {
        if (l->pool_ == pool) {
          l->unit_config_ = new_config;
        }
      }
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

int ObUnitManager::insert_migrate_unit(const ObAddr &src_server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!src_server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(src_server), K(unit_id), K(ret));
  } else {
    ObArray<uint64_t> *migrate_units = NULL;
    if (OB_FAIL(get_migrate_units_by_server(src_server, migrate_units))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get_migrate_units_by_server failed", K(src_server), K(ret));
      } else {
        ret= OB_SUCCESS;
        if (NULL == (migrate_units = migrate_units_allocator_.alloc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc  failed", K(ret));
        } else if (OB_FAIL(migrate_units->push_back(unit_id))) {
          LOG_WARN("push_back failed", K(ret));
        } else {
          INSERT_ARRAY_TO_MAP(server_migrate_units_map_, src_server, migrate_units);
        }

        // avoid memory leak
        if (OB_SUCCESS != ret && NULL != migrate_units) {
          migrate_units_allocator_.free(migrate_units);
          migrate_units = NULL;
        }
      }
    } else if (NULL == migrate_units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("migrate_units is null", KP(migrate_units), K(ret));
    } else {
      if (OB_FAIL(migrate_units->push_back(unit_id))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::set_server_ref_count(common::hash::ObHashMap<ObAddr, int64_t> &map,
    const ObAddr &server, const int64_t ref_count) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || !server.is_valid() || ref_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map_created", map.created(), K(server), K(ref_count), K(ret));
  } else {
    SET_ITEM_TO_MAP(map, server, ref_count);
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
          if (unit->migrate_from_server_.is_valid()) {
            if (OB_FAIL(delete_migrate_unit(unit->migrate_from_server_,
                                            unit->unit_id_))) {
              LOG_WARN("failed to delete migrate unit", K(ret), "unit", *unit);
            }
          }

        }

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

int ObUnitManager::delete_inmemory_units(
    const uint64_t resource_pool_id,
    const common::ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(resource_pool_id), K(ret));
  } else {
    ObArray<ObUnit *> *units = NULL;
    ObArray<ObUnit *> left_units;
    if (OB_FAIL(get_units_by_pool(resource_pool_id, units))) {
      LOG_WARN("get_units_by_pool failed", K(resource_pool_id), K(ret));
    } else if (NULL == units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units is null", KP(units), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
        if (NULL == units->at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit should not be null", "unit", OB_P(units->at(i)), K(ret));
        } else if (!has_exist_in_array(unit_ids, units->at(i)->unit_id_)) {
          if (OB_FAIL(left_units.push_back(units->at(i)))) {
            LOG_WARN("push_back failed", K(ret));
          }
        } else {
          if (OB_FAIL(delete_unit_loads(*units->at(i)))) {
            LOG_WARN("delete_unit_load failed", "unit", *units->at(i), K(ret));
          } else {
            DELETE_ITEM_FROM_MAP(id_unit_map_, units->at(i)->unit_id_);
            if (OB_SUCC(ret)) {
              ROOTSERVICE_EVENT_ADD("unit", "drop_unit",
                  "unit_id", units->at(i)->unit_id_);
              allocator_.free(units->at(i));
              units->at(i) = NULL;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        // if all units of pool are deleted, delete item from hashmap
        if (left_units.count() <= 0) {
          DELETE_ITEM_FROM_MAP(pool_unit_map_, resource_pool_id);
          if (OB_SUCC(ret)) {
            pool_unit_allocator_.free(units);
            units = NULL;
          }
        } else {
          if (OB_FAIL(units->assign(left_units))) {
            LOG_WARN("assign failed", K(ret));
          }
        }
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

int ObUnitManager::delete_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id) || NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KP(pool), K(ret));
  } else if (OB_FAIL(delete_id_pool(tenant_pools_map_,
      tenant_pools_allocator_, tenant_id, pool))) {
    LOG_WARN("delete tenant pool failed", K(ret), K(tenant_id), "pool", *pool);
  }
  return ret;
}

int ObUnitManager::delete_id_pool(
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
      LOG_WARN("get_pools_by_id failed", K(id), K(ret));
    } else if (NULL == pools) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pools is null", KP(pools), K(ret));
    } else {
      int64_t index = -1;
      for (int64_t i = 0; i < pools->count(); ++i) {
        if (pools->at(i) == pool) {
          index = i;
          break;
        }
      }
      if (-1 == index) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("pool not exist", K(id), K(ret));
      } else if (OB_FAIL(pools->remove(index))) {
        LOG_WARN("remove failed", K(index), K(ret));
      } else if (0 == pools->count()) {
        allocator.free(pools);
        pools = NULL;
        DELETE_ITEM_FROM_MAP(map, id);
      }
    }
  }
  return ret;
}

int ObUnitManager::delete_migrate_unit(const ObAddr &src_server,
                                       const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!src_server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(src_server), K(unit_id), K(ret));
  } else {
    ObArray<uint64_t> *migrate_units = NULL;
    if (OB_FAIL(get_migrate_units_by_server(src_server, migrate_units))) {
      LOG_WARN("get_migrate_units_by_server failed", K(src_server), K(ret));
    } else if (NULL == migrate_units) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("migrate_units is null", KP(migrate_units), K(ret));
    } else {
      int64_t index = -1;
      for (int64_t i = 0; i < migrate_units->count(); ++i) {
        if (migrate_units->at(i) == unit_id) {
          index = i;
          break;
        }
      }
      if (-1 == index) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("migrate_unit not exist", K(unit_id), K(ret));
      } else if (OB_FAIL(migrate_units->remove(index))) {
        LOG_WARN("remove failed", K(index), K(ret));
      } else if (0 == migrate_units->count()) {
        migrate_units_allocator_.free(migrate_units);
        migrate_units = NULL;
        DELETE_ITEM_FROM_MAP(server_migrate_units_map_, src_server);
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

int ObUnitManager::update_unit_load(
    share::ObUnit *unit,
    share::ObResourcePool *new_pool)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == unit || NULL == new_pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(unit), KP(new_pool));
  } else {
    ObArray<ObUnitManager::ObUnitLoad> *server_load = NULL;
    GET_ITEM_FROM_MAP(server_loads_, unit->server_, server_load);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get server load", K(ret), "server", unit->server_);
    } else if (NULL == server_load) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("loads ptr is null", K(ret));
    } else {
      FOREACH_X(l, *server_load, OB_SUCC(ret)) {
        if (l->unit_ == unit) {
          l->pool_ = new_pool;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (unit->migrate_from_server_.is_valid()) {
      ObArray<ObUnitManager::ObUnitLoad> *migrate_from_load = NULL;
      GET_ITEM_FROM_MAP(server_loads_, unit->migrate_from_server_, migrate_from_load);
      if (OB_FAIL(ret)) {
         LOG_WARN("fail to get server load", K(ret),
                  "migrate_from_server", unit->migrate_from_server_);
      } else if (NULL == migrate_from_load) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("loads ptr is null", K(ret));
      } else {
        FOREACH_X(l, *migrate_from_load, OB_SUCC(ret)) {
          if (l->unit_ == unit) {
            l->pool_ = new_pool;
          }
        }
      }
    }
  }
  return ret;
}

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

int ObUnitManager::get_server_ref_count(common::hash::ObHashMap<ObAddr, int64_t> &map,
    const ObAddr &server, int64_t &ref_count) const
{
  int ret = OB_SUCCESS;
  ref_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!map.created() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "map created", map.created(), K(server), K(ret));
  } else {
    GET_ITEM_FROM_MAP(map, server, ref_count);
    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
      } else {
        LOG_WARN("GET_ITEM_FROM_MAP failed", K(server), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::check_resource_pool_exist(const share::ObResourcePoolName &resource_pool_name,
                                             bool &is_exist)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  share::ObResourcePool *pool = NULL;
  is_exist = false;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret), K(inited_), K(loaded_));
  } else if (resource_pool_name.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(resource_pool_name));
  } else if (OB_FAIL(inner_get_resource_pool_by_name(resource_pool_name, pool))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("inner_get_resource_pool_by_name failed", KR(ret), K(resource_pool_name));
    } else {
      ret = OB_SUCCESS;
      // is_exist = false
    }
  } else if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), KP(pool), K(resource_pool_name));
  } else {
    is_exist = true;
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

int ObUnitManager::get_pools_by_config(const uint64_t config_id,
                                       ObArray<share::ObResourcePool *> *&pools) const
{
  int ret = OB_SUCCESS;
  pools = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == config_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config_id), K(ret));
  } else if (OB_FAIL(get_pools_by_id(config_pools_map_, config_id, pools))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_pools_by_id failed", K(config_id), K(ret));
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

int ObUnitManager::get_migrate_units_by_server(const ObAddr &server,
                                               common::ObIArray<uint64_t> &migrate_units) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> *units= NULL;
  if (OB_FAIL(get_migrate_units_by_server(server, units))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // no migrating units
    } else {
      LOG_WARN("fail get migrate units by server", K(server), K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
      const uint64_t unit_id = units->at(i);
      if (OB_FAIL(migrate_units.push_back(unit_id))) {
        LOG_WARN("fail push back unit id to array", K(unit_id), K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitManager::get_migrate_units_by_server(const ObAddr &server,
                                               common::ObArray<uint64_t> *&migrate_units) const
{
  int ret = OB_SUCCESS;
  migrate_units = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    GET_ITEM_FROM_MAP(server_migrate_units_map_, server, migrate_units);
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

int ObUnitManager::fetch_new_resource_pool_id(uint64_t &resource_pool_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    uint64_t combine_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID,
        OB_MAX_USED_RESOURCE_POOL_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_new_max_id failed", "id_type", OB_MAX_USED_RESOURCE_POOL_ID_TYPE, K(ret));
    } else {
      resource_pool_id = combine_id;
    }
  }
  return ret;
}

int ObUnitManager::fetch_new_unit_group_id(uint64_t &unit_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    uint64_t combine_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID,
        OB_MAX_USED_UNIT_GROUP_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_new_max_id failed", "id_type", OB_MAX_USED_UNIT_ID_TYPE, K(ret));
    } else {
      unit_group_id = combine_id;
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

int ObUnitManager::inner_get_tenant_zone_full_unit_num(
    const int64_t tenant_id,
    const common::ObZone &zone,
    int64_t &unit_num)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools ptr is null", K(ret), KP(pools));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < pools->count(); ++i) {
      share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (!has_exist_in_array(pool->zone_list_, zone)) {
        // not in this pool
      } else if (REPLICA_TYPE_FULL == pool->replica_type_) {
        unit_num = pool->unit_count_;
        find = true;
      } else {} // not a full replica type resource pool, go on to check next
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      unit_num = 0;
    }
  }
  return ret;
}

int ObUnitManager::get_tenant_zone_unit_loads(
    const int64_t tenant_id,
    const common::ObZone &zone,
    const common::ObReplicaType replica_type,
    common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = NULL;
  unit_loads.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                         || zone.is_empty()
                         || !ObReplicaTypeCheck::is_replica_type_valid(replica_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(zone), K(replica_type));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", K(ret), KP(pools));
  } else {
    unit_loads.reset();
    share::ObResourcePool *target_pool = NULL;
    for (int64_t i = 0; NULL == target_pool && OB_SUCC(ret) && i < pools->count(); ++i) {
      share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (!has_exist_in_array(pool->zone_list_, zone)) {
        // not in this pool
      } else if (replica_type == pool->replica_type_) {
        target_pool = pool;
      } else {} // not a full replica type resource pool, go on to check next
    }
    if (OB_FAIL(ret)) {
    } else if (NULL == target_pool) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ObArray<share::ObUnit *> *units = NULL;
      if (OB_FAIL(get_units_by_pool(target_pool->resource_pool_id_, units))) {
        LOG_WARN("fail to get units by pool", K(ret),
                 "pool_id", target_pool->resource_pool_id_);
      } else if (OB_UNLIKELY(NULL == units)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("units ptr is null", K(ret), KP(units));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
          ObUnitLoad unit_load;
          ObUnit *unit = units->at(i);
          if (NULL == unit) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", K(ret));
          } else if (unit->zone_ != zone) {
            // not this zone, ignore
          } else if (OB_FAIL(gen_unit_load(unit, unit_load))) {
            LOG_WARN("fail to gen unit load", K(ret));
          } else if (OB_FAIL(unit_loads.push_back(unit_load))) {
            LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_tenant_zone_all_unit_loads(
    const int64_t tenant_id,
    const common::ObZone &zone,
    common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> *pools = NULL;
  unit_loads.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(zone));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(NULL == pools)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool ptr is null", K(ret), KP(pools));
  } else {
    unit_loads.reset();
    common::ObArray<share::ObResourcePool *> target_pools;
    for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
      share::ObResourcePool *pool = pools->at(i);
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool ptr is null", K(ret), KP(pool));
      } else if (!has_exist_in_array(pool->zone_list_, zone)) {
        // not in this pool
      } else if (OB_FAIL(target_pools.push_back(pool))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_pools.count(); ++i) {
      ObArray<share::ObUnit *> *units = NULL;
      share::ObResourcePool *target_pool = target_pools.at(i);
      if (OB_UNLIKELY(NULL == target_pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("target pool ptr is null", K(ret), KP(target_pool));
      } else if (OB_FAIL(get_units_by_pool(target_pool->resource_pool_id_, units))) {
        LOG_WARN("fail to get units by pool", K(ret),
                 "pool_id", target_pool->resource_pool_id_);
      } else if (OB_UNLIKELY(NULL == units)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("units ptr is null", K(ret), KP(units));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < units->count(); ++i) {
          ObUnitLoad unit_load;
          ObUnit *unit = units->at(i);
          if (NULL == unit) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit ptr is null", K(ret));
          } else if (unit->zone_ != zone) {
            // not this zone, ignore
          } else if (OB_FAIL(gen_unit_load(unit, unit_load))) {
            LOG_WARN("fail to gen unit load", K(ret));
          } else if (OB_FAIL(unit_loads.push_back(unit_load))) {
            LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (unit_loads.count() <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObUnitManager::get_logonly_unit_by_tenant(const int64_t tenant_id,
                                              ObIArray<ObUnitInfo> &logonly_unit_infos)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema service is null", K(schema_service_), KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_logonly_unit_by_tenant(schema_guard, tenant_id, logonly_unit_infos))) {
    LOG_WARN("get logonly unit by tenant fail", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObUnitManager::get_logonly_unit_by_tenant(share::schema::ObSchemaGetterGuard &schema_guard,
                                              const int64_t tenant_id,
                                              ObIArray<ObUnitInfo> &logonly_unit_infos)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  logonly_unit_infos.reset();
  ObArray<ObUnitInfo> unit_infos;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tenant_schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(inner_get_active_unit_infos_of_tenant(*tenant_schema, unit_infos))) {
    LOG_WARN("fail to get active unit", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < unit_infos.count() && OB_SUCC(ret); i++) {
      if (REPLICA_TYPE_LOGONLY != unit_infos.at(i).unit_.replica_type_) {
        //nothing todo
      } else if (OB_FAIL(logonly_unit_infos.push_back(unit_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(unit_infos));
      }
    }
  }
  return ret;
}

int ObUnitManager::get_unit_infos(const common::ObIArray<share::ObResourcePoolName> &pools,
                                  ObIArray<ObUnitInfo> &unit_infos)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  unit_infos.reset();
  if (pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pools));
  } else {
    ObArray<ObUnitInfo> pool_units;
    share::ObResourcePool *pool = NULL;
    for (int64_t i = 0; i < pools.count() && OB_SUCC(ret); i++) {
      pool_units.reset();
      if (OB_FAIL(inner_get_resource_pool_by_name(pools.at(i), pool))) {
        LOG_WARN("fail to get resource pool", K(ret), K(i), K(pools));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid pool", K(ret), K(pools));
      } else if (OB_FAIL(inner_get_unit_infos_of_pool_(pool->resource_pool_id_, pool_units))) {
        LOG_WARN("fail to get unit infos", K(ret), K(*pool));
      } else {
        for (int64_t j = 0; j < pool_units.count() && OB_SUCC(ret); j++) {
          if (OB_FAIL(unit_infos.push_back(pool_units.at(j)))) {
            LOG_WARN("fail to push back", K(ret), K(pool_units));
          }
        }
      }
    }
  }
  return ret;
}

int ObUnitManager::get_servers_by_pools(
    const common::ObIArray<share::ObResourcePoolName> &pools,
    common::ObIArray<ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  addrs.reset();
  ObArray<share::ObUnitInfo> unit_infos;
  if (OB_FAIL(get_unit_infos(pools, unit_infos))) {
    LOG_WARN("fail to get unit infos", KR(ret), K(pools));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_infos.count(); i++) {
    const share::ObUnitInfo &unit_info = unit_infos.at(i);
    if (OB_FAIL(addrs.push_back(unit_info.unit_.server_))) {
      LOG_WARN("fail to push back addr", KR(ret), K(unit_info));
    }
  } // end for
  return ret;
}

int ObUnitManager::inner_get_active_unit_infos_of_tenant(const ObTenantSchema &tenant_schema,
                                                  ObIArray<ObUnitInfo> &unit_info)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> pool_ids;
  uint64_t tenant_id = tenant_schema.get_tenant_id();
  common::ObArray<common::ObZone> tenant_zone_list;
  ObArray<share::ObResourcePool  *> *pools = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", K(inited_), K(loaded_), K(ret));
  } else if (OB_FAIL(tenant_schema.get_zone_list(tenant_zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_FAIL(get_pools_by_tenant_(tenant_id, pools))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_pools_by_tenant failed", K(tenant_id), K(ret));
    } else {
      // just return empty pool_ids
      ret = OB_SUCCESS;
      LOG_WARN("tenant doesn't own any pool", K(tenant_id), K(ret));
    }
  } else if (NULL == pools) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pools is null", KP(pools), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pools->count(); ++i) {
      if (NULL == pools->at(i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", "pool", OB_P(pools->at(i)), K(ret));
      } else if (OB_FAIL(pool_ids.push_back(pools->at(i)->resource_pool_id_))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObArray<ObUnitInfo> unit_in_pool;
    for (int64_t i = 0; i < pool_ids.count() && OB_SUCC(ret); i++) {
      uint64_t pool_id = pool_ids.at(i);
      unit_in_pool.reset();
      if (OB_FAIL(inner_get_unit_infos_of_pool_(pool_id, unit_in_pool))) {
        LOG_WARN("fail to inner get unit infos", K(ret), K(pool_id));
      } else {
        for (int64_t j = 0; j < unit_in_pool.count() && OB_SUCC(ret); j++) {
          if (ObUnit::UNIT_STATUS_ACTIVE != unit_in_pool.at(j).unit_.status_) {
            //nothing todo
          } else if (!has_exist_in_array(tenant_zone_list, unit_in_pool.at(j).unit_.zone_)) {
            //nothing todo
          } else if (OB_FAIL(unit_info.push_back(unit_in_pool.at(j)))) {
            LOG_WARN("fail to push back", K(ret), K(unit_in_pool), K(j));
          }
        }
      }
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
