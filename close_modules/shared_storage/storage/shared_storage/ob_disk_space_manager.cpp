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

#include "ob_disk_space_manager.h"
#include "observer/omt/ob_tenant.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "share/object_storage/ob_zone_storage_table_operation.h"
#include "share/ob_all_server_tracer.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
/**
 * --------------------------------ObDiskSpaceManager------------------------------------
 */
ObDiskSpaceManager::ObDiskSpaceManager()
  : is_inited_(false),
    disk_size_lock_(ObLatchIds::DISK_SPACE_MANAGER_LOCK),
    total_disk_size_(0),
    reserved_disk_size_(0),
    alloc_disk_size_(0),
    hidden_sys_data_disk_config_size_(0),
    hidden_sys_data_disk_size_(0),
    ss_cache_max_percentage_(0),
    ss_cache_maxsize_percpu_(0),
    data_disk_suggested_size_(0),
    data_disk_suggested_operation_(DataDiskSuggestedOperationType::TYPE::NONE)
{
}

ObDiskSpaceManager::~ObDiskSpaceManager() { destroy(); }

int ObDiskSpaceManager::init(const int64_t total_disk_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("disk space manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(total_disk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(total_disk_size));
  } else {
    ObRecursiveMutexGuard guard(disk_size_lock_);
    total_disk_size_ = total_disk_size;
    reserved_disk_size_ = DEFAULT_SERVER_TENANT_ID_DISK_SIZE;
    alloc_disk_size_ = 0;
    hidden_sys_data_disk_config_size_ = GMEMCONF.get_server_memory_limit() * MEMORY_TO_DATA_DISK_FACTOR;
    hidden_sys_data_disk_size_ = 0; // when create hidden sys, then init this value
    ss_cache_max_percentage_ = DEFAULT_SS_CACHE_MAX_PERCENTAGE;
    ss_cache_maxsize_percpu_ = DEFAULT_SS_CACHE_MAXSIZE_PERCPU;
    data_disk_suggested_size_ = 0;
    data_disk_suggested_operation_ = DataDiskSuggestedOperationType::TYPE::NONE;
    is_inited_ = true;
    LOG_INFO("succ to init disk space manager", K_(total_disk_size), K_(alloc_disk_size),
             K_(reserved_disk_size), K_(hidden_sys_data_disk_config_size), K_(hidden_sys_data_disk_size),
             K_(ss_cache_max_percentage), K_(ss_cache_maxsize_percpu), K_(data_disk_suggested_size),
             K_(data_disk_suggested_operation));
  }
  return ret;
}

void ObDiskSpaceManager::destroy()
{
  ObRecursiveMutexGuard guard(disk_size_lock_);
  is_inited_ = false;
  total_disk_size_ = 0;
  reserved_disk_size_ = 0;
  alloc_disk_size_ = 0;
  hidden_sys_data_disk_config_size_ = 0;
  hidden_sys_data_disk_size_ = 0;
}

ObDiskSpaceManager &ObDiskSpaceManager::get_instance()
{
  static ObDiskSpaceManager instance;
  return instance;
}

int ObDiskSpaceManager::resize(const int64_t new_total_disk_size)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_total_disk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid total disk size to resize", KR(ret), K(new_total_disk_size));
  } else if (new_total_disk_size < total_disk_size_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not make total disk size smaller", KR(ret), K(new_total_disk_size), K_(total_disk_size));
  } else {
    const int64_t old_total_disk_size = total_disk_size_;
    total_disk_size_ = new_total_disk_size;
    // succ to expand server datafile_size, modify data_disk_suggested_operation from EXPAND to NONE
    modify_datadisk_suggested_opertion_type(DiskSizeOpType::RESIZE, 0/*data_disk_suggeted_size*/, old_total_disk_size, alloc_disk_size_);
    LOG_INFO("succ to resize total disk size", K_(total_disk_size), K_(alloc_disk_size), K_(reserved_disk_size),
             K_(hidden_sys_data_disk_config_size), K_(hidden_sys_data_disk_size), K_(data_disk_suggested_size),
             K_(data_disk_suggested_operation));
  }
  return ret;
}

int ObDiskSpaceManager::alloc(const int64_t alloc_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t data_disk_suggested_size = 0;
  ObRecursiveMutexGuard guard(disk_size_lock_);
  bool is_need_expand = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(alloc_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk size to alloc", KR(ret), K(alloc_size));
  // create tenant or alter unit data_disk_size do not need confider cpu*2G reserved_size, only auto expand need to consider
  } else if (alloc_size > get_free_disk_size()) {
    // ss_cache free_size is not enough, need expand
    data_disk_suggested_size = total_disk_size_ + alloc_size - get_free_disk_size();
    modify_datadisk_suggested_opertion_type(DiskSizeOpType::ALLOC, data_disk_suggested_size, total_disk_size_, alloc_disk_size_);
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_WARN("fail to alloc disk size", KR(ret), K_(alloc_disk_size), K_(total_disk_size),
              K_(reserved_disk_size), K(alloc_size), K_(data_disk_suggested_size),
              K_(data_disk_suggested_operation));
  } else {
    const int64_t old_alloc_disk_size = alloc_disk_size_;
    alloc_disk_size_ += alloc_size;
    // ss_cache alloc_size has reached 90% of total_datafile_size, need expand
    if (OB_TMP_FAIL(is_ss_cache_need_expand(is_need_expand))) { // use tmp_ret, cannot affect alloc disk size
      LOG_WARN("fail to check ss cache need expand", KR(tmp_ret));
    } else if (is_need_expand) {
      modify_datadisk_suggested_opertion_type(DiskSizeOpType::ALLOC, data_disk_suggested_size, total_disk_size_, old_alloc_disk_size);
    }
    LOG_INFO("succ to alloc disk size", K_(alloc_disk_size), K_(total_disk_size), K_(reserved_disk_size),
             K(alloc_size), K_(hidden_sys_data_disk_config_size), K_(hidden_sys_data_disk_size),
             K_(data_disk_suggested_size),K_(data_disk_suggested_operation), K(is_need_expand));
  }
  return ret;
}

int ObDiskSpaceManager::free(const int64_t free_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(disk_size_lock_);
  bool is_need_expand = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(free_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk size to free", KR(ret), K(free_size));
  } else if (free_size > alloc_disk_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error, alloc disk size less than 0", KR(ret), K_(alloc_disk_size),
              K_(total_disk_size), K(free_size));
  } else {
    const int64_t old_alloc_disk_size = alloc_disk_size_;
    alloc_disk_size_ -= free_size;
    // new_free_size and suggested_need_free_size will be subtracted, so do not need to consider cpu*2G reserved size
    const int64_t new_free_size = total_disk_size_ - alloc_disk_size_;
    const int64_t suggested_need_free_size = data_disk_suggested_size_ - old_alloc_disk_size;
    // succ to free disk size, if data_disk_suggested_operation is expand and alloc_size has dropped to 90%, need set operation to NONE
    if (OB_TMP_FAIL(is_ss_cache_need_expand(is_need_expand))) { // use tmp_ret, cannot affect free disk size
      LOG_WARN("fail to check ss cache need expand", KR(tmp_ret));
    // if !is_need_expand and new_free_size is larger than suggested_need_free_size, do not need EXPAND
    } else if (!is_need_expand && ((0 == data_disk_suggested_size_) || (new_free_size >= suggested_need_free_size))) {
      modify_datadisk_suggested_opertion_type(DiskSizeOpType::FREE, 0/*data_disk_suggested_size*/, total_disk_size_, old_alloc_disk_size);
    }
    LOG_INFO("succ to free disk size", K_(alloc_disk_size), K_(total_disk_size), K_(reserved_disk_size),
             K(free_size), K_(hidden_sys_data_disk_config_size), K_(hidden_sys_data_disk_size),
             K_(data_disk_suggested_size),K_(data_disk_suggested_operation), K(is_need_expand));
  }
  return ret;
}

void ObDiskSpaceManager::modify_datadisk_suggested_opertion_type(const DiskSizeOpType type,
                                                                 const int64_t data_disk_suggested_size,
                                                                 const int64_t old_total_disk_size,
                                                                 const int64_t old_alloc_disk_size)
{
  const DataDiskSuggestedOperationType::TYPE old_data_disk_suggested_operation = data_disk_suggested_operation_;
  bool is_suggested_operation_changed = false;
  switch (type) {
    case DiskSizeOpType::ALLOC: {
      if ((DataDiskSuggestedOperationType::TYPE::NONE == data_disk_suggested_operation_) &&
          (total_disk_size_ < get_ss_cache_max_size())) {
        is_suggested_operation_changed = true;
        data_disk_suggested_operation_ = DataDiskSuggestedOperationType::TYPE::EXPAND;
        data_disk_suggested_size_ = data_disk_suggested_size;
      }
      break;
    }
    case DiskSizeOpType::FREE: {
      if (DataDiskSuggestedOperationType::TYPE::EXPAND == data_disk_suggested_operation_) {
        is_suggested_operation_changed = true;
        data_disk_suggested_operation_ = DataDiskSuggestedOperationType::TYPE::NONE;
        data_disk_suggested_size_ = 0;
      }
      break;
    }
    case DiskSizeOpType::RESIZE: {
      // succ to expand server datafile_size, modify data_disk_suggested_operation from EXPAND to NONE
      if ((DataDiskSuggestedOperationType::TYPE::EXPAND == data_disk_suggested_operation_) &&
          (total_disk_size_ >= data_disk_suggested_size_)) {
        is_suggested_operation_changed = true;
        data_disk_suggested_operation_ = DataDiskSuggestedOperationType::TYPE::NONE;
        data_disk_suggested_size_ = 0;
      }
      break;
    }
    default: {
      // do nothing
      break;
    }
  }
  if (is_suggested_operation_changed) {
    SERVER_EVENT_ADD("shared_storage", "data disk suggested operation changed",
      "old_data_disk_suggested_operation", DataDiskSuggestedOperationType::get_str(old_data_disk_suggested_operation),
      "old_total_disk_size", old_total_disk_size,
      "old_alloc_disk_size", old_alloc_disk_size,
      "new_data_disk_suggested_operation", DataDiskSuggestedOperationType::get_str(data_disk_suggested_operation_),
      "new_total_disk_size", total_disk_size_,
      "new_alloc_disk_size", alloc_disk_size_);
  }
}

int ObDiskSpaceManager::auto_expand_data_disk_size(const uint64_t tenant_id, const int64_t expand_size)
{
  int ret = OB_SUCCESS;
  share::ObUnitInfoGetter::ObTenantConfig new_unit;
  omt::ObTenant *tenant = nullptr;
  ObRecursiveMutexGuard guard(disk_size_lock_);
  int64_t free_disk_size =0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY((expand_size <= 0) || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(expand_size), K(tenant_id));
  } else if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpacted error, tenant is null", KR(ret), KP(tenant));
  } else if (OB_FAIL(new_unit.assign(tenant->get_unit()))) {
    LOG_WARN("fail to assign old unit failed", KR(ret), K(tenant_id), K(new_unit));
  } else if (OB_FAIL(get_free_disk_size_for_auto_expand(free_disk_size))) {
    LOG_WARN("fail to get free disk size", KR(ret));
  } else {
    new_unit.actual_data_disk_size_ += expand_size;
    const int64_t new_data_disk_size = new_unit.actual_data_disk_size_;
    // step 1: check free disk size is enough for expansion, consider cpu*2GB reserved size, free_disk_size = total_disk_size_ - alloc_disk_size_ - cpu*2GB
    if (expand_size > free_disk_size) {
      const int64_t data_disk_suggested_size = total_disk_size_ + expand_size - free_disk_size;
      modify_datadisk_suggested_opertion_type(DiskSizeOpType::ALLOC, data_disk_suggested_size, total_disk_size_, alloc_disk_size_);
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      if (TC_REACH_TIME_INTERVAL(ObTenantDiskSpaceManager::PRINT_LOG_INTERVAL)) {
        LOG_WARN("fail to auto expand data disk size", KR(ret), K_(alloc_disk_size), K_(total_disk_size), K(expand_size),
                  K_(reserved_disk_size), K_(data_disk_suggested_size), K_(data_disk_suggested_operation), K(free_disk_size));
      }
    } else if (OB_FAIL(GCTX.omt_->update_tenant_unit(new_unit))) {  // step 2: persist tenant_config_unit
      LOG_WARN("fail to update tenant unit", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GCTX.omt_->update_tenant_data_disk_size(tenant_id, new_data_disk_size))) {  // step 3: expand tenant data disk space size
      LOG_WARN("fail to update tenant data disk size", KR(ret), K(tenant_id), K(new_data_disk_size));
    } else {
      SERVER_EVENT_ADD("shared_storage", "auto expand data disk size",
        "tenant_id", MTL_ID(),
        "ret", ret,
        "expand_size", expand_size,
        "new_data_disk_size", new_data_disk_size);
      LOG_INFO("succ to auto expand data disk size", K(tenant_id), K(new_unit), K(new_data_disk_size), K(expand_size));
    }
  }
  return ret;
}

int ObDiskSpaceManager::reload_hidden_sys_data_disk_config(const ObServerConfig &server_config)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(disk_size_lock_);
  int64_t delta_data_disk_size = 0;
  int64_t hidden_sys_data_disk_config_size = server_config._ss_hidden_sys_tenant_data_disk_size;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if ((0 == hidden_sys_data_disk_config_size) || (hidden_sys_data_disk_config_size == hidden_sys_data_disk_config_size_)) {
    // do nothing
  } else if (hidden_sys_data_disk_config_size < hidden_sys_data_disk_config_size_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data disk size can not support shrink", KR(ret),
            K(hidden_sys_data_disk_config_size), K_(hidden_sys_data_disk_config_size));
  } else if (FALSE_IT(delta_data_disk_size = hidden_sys_data_disk_config_size - hidden_sys_data_disk_config_size_)) {
  } else if (delta_data_disk_size > get_free_disk_size()) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_WARN("the server is outof disk space", KR(ret), K(delta_data_disk_size), K(get_free_disk_size()));
  } else {
    const uint64_t tenant_id = OB_SYS_TENANT_ID;
    share::ObUnitInfoGetter::ObTenantConfig new_unit;
    omt::ObTenant *tenant = nullptr;
    if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
      LOG_WARN("fail to get tenant", KR(ret), K(tenant_id));
    } else if (OB_FAIL(new_unit.assign(tenant->get_unit()))) {
      LOG_WARN("fail to assign old unit failed", K(tenant_id), K(new_unit));
    } else if (tenant->is_hidden()) { // if current sys tenant is hidden
      // if config value is more than hidden sys actual data disk size, update hidden sys data_disk_size as new_data_disk_size
      int64_t new_data_disk_size = MAX(hidden_sys_data_disk_config_size, hidden_sys_data_disk_size_);
      new_unit.config_.set_data_disk_size(new_data_disk_size);
      new_unit.hidden_sys_data_disk_config_size_ = hidden_sys_data_disk_config_size;
      if (OB_FAIL(GCTX.omt_->update_tenant_unit(new_unit))) {
        LOG_WARN("fail to update tenant unit", K(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.omt_->update_tenant_data_disk_size(tenant_id, new_data_disk_size))) {
        LOG_WARN("fail to update tenant data disk size", K(ret), K(tenant_id), K(new_data_disk_size));
      } else {
        hidden_sys_data_disk_size_ = new_data_disk_size;
        hidden_sys_data_disk_config_size_ = hidden_sys_data_disk_config_size;
      }
    } else if (!tenant->is_hidden()) { // if current sys tenant is real
      int64_t new_data_disk_size = hidden_sys_data_disk_config_size;
      // real_sys_data_disk_size = sys_unit_config + hidden_sys_data_disk_size
      new_data_disk_size += tenant->get_unit().config_.data_disk_size();
      new_unit.hidden_sys_data_disk_config_size_ = hidden_sys_data_disk_config_size;
      if (OB_FAIL(GCTX.omt_->update_tenant_unit(new_unit))) {
        LOG_WARN("fail to update tenant unit", K(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.omt_->update_tenant_data_disk_size(tenant_id, new_data_disk_size))) {
        LOG_WARN("fail to update tenant data disk size", K(ret), K(tenant_id), K(new_data_disk_size));
      } else {
        hidden_sys_data_disk_size_ = hidden_sys_data_disk_config_size;
        hidden_sys_data_disk_config_size_ = hidden_sys_data_disk_config_size;
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to update hidden sys tenant data disk size", K_(hidden_sys_data_disk_config_size), K_(hidden_sys_data_disk_size));
  }
  return ret;
}

int ObDiskSpaceManager::reload_ss_cache_max_percentage_config(const ObServerConfig &server_config)
{
  int ret = OB_SUCCESS;
  const int64_t new_ss_cache_max_percentage = server_config.ss_cache_max_percentage;
  int64_t total_shared_size = 0;
  int64_t total_disk_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(ObStorageInfoOperator::get_total_shared_data_size(total_shared_size))) {
    LOG_WARN("fail to get total shared data size", KR(ret));
  } else if (OB_FAIL(ObStorageInfoOperator::get_total_disk_size(total_disk_size))) {
    LOG_WARN("fail to get total disk data size", KR(ret));
  } else if ((new_ss_cache_max_percentage <= 0) ||
             (new_ss_cache_max_percentage > 100) ||
             ((new_ss_cache_max_percentage < ss_cache_max_percentage_) &&
              (total_disk_size > total_shared_size * new_ss_cache_max_percentage / 100))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ss_cache_max_percentage value modification is not supported", KR(ret),
             K(new_ss_cache_max_percentage), K_(ss_cache_max_percentage),
             K(total_disk_size), K(total_shared_size));
  } else {
    ss_cache_max_percentage_ = new_ss_cache_max_percentage;
    LOG_INFO("succ to update ss_cache_max_percentage", K_(ss_cache_max_percentage));
  }
  return ret;
}

int ObDiskSpaceManager::reload_ss_cache_maxsize_percpu_config(const ObServerConfig &server_config)
{
  int ret = OB_SUCCESS;
  const int64_t new_ss_cache_maxsize_percpu = server_config.ss_cache_maxsize_percpu;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if ((new_ss_cache_maxsize_percpu <= 0) || (new_ss_cache_maxsize_percpu * common::get_cpu_count() < total_disk_size_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ss_cache_maxsize_percpu value modification is not supported", KR(ret),
             K(new_ss_cache_maxsize_percpu), K_(ss_cache_maxsize_percpu), K_(total_disk_size));
  } else {
    ss_cache_maxsize_percpu_ = new_ss_cache_maxsize_percpu;
    LOG_INFO("succ to update ss_cache_maxsize_percpu", K_(ss_cache_maxsize_percpu));
  }
  return ret;
}

int ObDiskSpaceManager::update_hidden_sys_data_disk_config_size(const int64_t new_hidden_sys_data_disk_config_size)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_hidden_sys_data_disk_config_size <= 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid hidden sys default data disk size", KR(ret), K(new_hidden_sys_data_disk_config_size));
  } else {
    hidden_sys_data_disk_config_size_ = new_hidden_sys_data_disk_config_size;
    LOG_INFO("succ to update hidden sys default data_disk_size", K_(hidden_sys_data_disk_config_size));
  }
  return ret;
}

int ObDiskSpaceManager::update_hidden_sys_data_disk_size(const int64_t new_hidden_sys_data_disk_size)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_hidden_sys_data_disk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hidden sys data disk size", KR(ret), K(new_hidden_sys_data_disk_size));
  } else {
    hidden_sys_data_disk_size_ = new_hidden_sys_data_disk_size;
    LOG_INFO("succ to update hidden sys data_disk_size", K_(hidden_sys_data_disk_size));
  }
  return ret;
}

int ObDiskSpaceManager::gen_hidden_sys_data_disk_size(int64_t &hidden_sys_data_disk_size)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (0 == hidden_sys_data_disk_size_) {
    hidden_sys_data_disk_size = hidden_sys_data_disk_config_size_;
  } else {
    hidden_sys_data_disk_size = hidden_sys_data_disk_size_;
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to generate hidden sys data_disk_size", K(hidden_sys_data_disk_size));
  }
  return ret;
}

int ObDiskSpaceManager::get_used_disk_size(int64_t &used_disk_size)
{
  int ret = OB_SUCCESS;
  used_disk_size = 0;
  common::ObArray<uint64_t> tenant_ids;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, omt is nullptr", KR(ret));
  } else if (OB_FAIL(omt->get_mtl_tenant_ids(tenant_ids))) {
    STORAGE_LOG(WARN, "fail to get_mtl_tenant_ids", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); i++) {
    const uint64_t tenant_id = tenant_ids.at(i);
    MTL_SWITCH(tenant_id) {
      int64_t tenant_used_disk_size = 0;
      ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
      if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr));
      } else if (OB_FAIL(tnt_disk_space_mgr->get_used_disk_size(tenant_used_disk_size))) {
        LOG_WARN("fail to get used disk size", KR(ret), K(tenant_used_disk_size));
      } else {
        used_disk_size += tenant_used_disk_size;
      }
    } else {
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("tenant is stopped, ingnore", K(tenant_id));
      } else {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObDiskSpaceManager::get_tenant_object_storage_used_size(const uint64_t tenant_id, int64_t &total_size)
{
  int ret = OB_SUCCESS;
  total_size = 0;
  int64_t tmp_file_used_size = 0;
  int64_t total_data_size = 0;
  const ObAddr self_addr = GCTX.self_addr();
  tmp_file::ObTenantTmpFileManager *tmp_file_manager = MTL(tmp_file::ObTenantTmpFileManager*);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tmp_file_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret));
  } else if (OB_FAIL(ObStorageInfoOperator::get_tmp_file_data_size(tenant_id, self_addr, tmp_file_used_size))) {
    LOG_WARN("fail to get tmp data size", KR(ret), K(tenant_id), K(self_addr));
  // total_data_size is the data written by current tenant on current server, include incremental_data and shared_data
  } else if (OB_FAIL(ObStorageInfoOperator::get_table_total_data_size(tenant_id, self_addr, total_data_size))) {
    LOG_WARN("fail to get table data size", KR(ret), K(tenant_id), K(self_addr));
  } else {
    // get current tenant object storage used_size written on current server, include tmp_file size and total_data size, take the maximum of the two
    total_size = MAX(total_data_size, tmp_file_used_size * 0.5);
  }
  return ret;
}

// check tenant expansion disk_size, which must be less than 30% of tenant object storage size
int ObDiskSpaceManager::check_expand_tenant_data_disk_size(const uint64_t tenant_id, bool &is_need_expand)
{
  int ret = OB_SUCCESS;
  int64_t total_object_storage_size = 0;
  int64_t tnt_total_disk_size = 0;
  is_need_expand = false;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  ObSSMicroCache *micro_cache = nullptr;
  int64_t zone_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K(tenant_id));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_if_tenant_support_auto_expand_disk(tenant_id, is_need_expand))) {
    LOG_WARN("fail to check if tenant support auto expand disk", KR(ret));
  } else if (!is_need_expand) {
    // do nothing, auto expand tenant disk size only when data_disk_size is 0
  } else if (OB_FAIL(micro_cache->is_ready_to_evict_cache(is_need_expand))) {
    LOG_WARN("fail to check ready to evict cache", KR(ret));
  // if each module's alloc_size has reached 90% of the total_size, need auto expand tenant data disk size
  } else if (FALSE_IT(is_need_expand = (is_need_expand || tnt_disk_space_mgr->need_expand_disk_size()))) {
  } else if (!is_need_expand) { // do nothing, do not need expand disk size
  // check tenant disk_size must be less than 30% of tenant object storage size, this situation can expand tenant data_disk_size
  } else if (is_meta_tenant(tenant_id)) {
    // meta tenant get_table_total_data_size will fail, because CDB_OB_SERVER_SPACE_USAGE do not record meta tenant used size
    // meta tenant max expand size is 10 times of memory size
    is_need_expand = tnt_disk_space_mgr->is_meta_tenant_need_expand();
    LOG_INFO("succ to check expand tenant data disk size, beacause meta tenant need expand disk size", K(tenant_id), K(is_need_expand));
  } else if (OB_FAIL(get_tenant_object_storage_used_size(tenant_id, total_object_storage_size))) {
    LOG_WARN("fail to get tenant object storage size", KR(ret), K(tenant_id));
  } else if (FALSE_IT(tnt_total_disk_size = tnt_disk_space_mgr->get_total_disk_size())) {
  } else if (OB_FAIL(SVR_TRACER.get_zones_count(zone_cnt))) {
    LOG_WARN("fail to get zones count", KR(ret));
  // tenant disk size must be less than 30% of tenant object storage size, this situation can expand tenant data_disk_size
  } else if (tnt_total_disk_size < (total_object_storage_size * ss_cache_max_percentage_ / 100 / zone_cnt)) {
    is_need_expand = true;
    LOG_INFO("succ to check expand tenant data disk size, because tenant disk size is less than ss_cache_max_percentage of tenant object storage size",
             KR(ret), K(tenant_id), K(is_need_expand), K(tnt_total_disk_size), K(total_object_storage_size), K_(ss_cache_max_percentage), K(zone_cnt));
  // In order to ensure that the local cache data_disk_size can hold private_macro, when the local cache disk size of private_macro is exhausted, need expand data_disk_size
  } else if (tnt_disk_space_mgr->is_need_expand_for_private_macro()) {
    is_need_expand = true;
    const int64_t private_macro_alloc_size = tnt_disk_space_mgr->get_private_macro_alloc_size();
    const int64_t private_macro_reserved_size = tnt_disk_space_mgr->get_private_macro_reserved_size();
    LOG_INFO("succ to check expand tenant data disk size, because tenant incremental data local cache size is less than 10 times of tenant memory",
             KR(ret), K(tenant_id), K(is_need_expand), K(private_macro_alloc_size), K(private_macro_reserved_size), K(MTL_MEM_SIZE()));
  // In order to ensure that the local cache data_disk_size can hold meta_file, when the local cache disk size of meta_file is exhausted, need expand data_disk_size
  } else if (tnt_disk_space_mgr->is_need_expand_for_meta_file()) {
    is_need_expand = true;
    const int64_t meta_file_alloc_size = tnt_disk_space_mgr->get_meta_file_alloc_size();
    const int64_t meta_file_reserved_size = tnt_disk_space_mgr->get_meta_file_reserved_size();
    LOG_INFO("succ to check expand tenant data disk size, because tenant meta file local cache size is less than 10 times of tenant memory",
             KR(ret), K(tenant_id), K(is_need_expand), K(meta_file_alloc_size), K(meta_file_reserved_size), K(MTL_MEM_SIZE()));
  } else {
    // All situations are not satisfied, cannot expand
    is_need_expand = false;
  }
  return ret;
}

int ObDiskSpaceManager::check_ls_migration_space_full(const ObMigrationOpArg &arg, const int64_t required_private_macro_size)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  bool is_support_auto_expand = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(required_private_macro_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, required_private_macro_size is invalid", KR(ret), K(required_private_macro_size));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    // do nothing, sys tenant and meta tenant's ls migration do not need check space full
  } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.check_if_tenant_support_auto_expand_disk(tenant_id, is_support_auto_expand))) {
    LOG_WARN("fail to check if tenant support auto expand disk", KR(ret));
  } else if (!is_support_auto_expand) {
    // do nothing, check ls migration space full only when data_disk_size is 0
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr));
  } else {
    int64_t ls_total_size = 0;
    int64_t tenant_private_macro_free_size = tnt_disk_space_mgr->get_private_macro_free_disk_size();
    int64_t src_unit_disk_size = 0;
    int64_t dest_unit_disk_size = tnt_disk_space_mgr->get_total_disk_size();
    int64_t src_table_total_size = 0;
    int64_t dest_table_total_size = 0;
    ObAddr src_server;
    // because ls Copy-Migration arg's src_server is invalid, need get ls leader addr as src addr
    if (OB_FAIL(ObStorageInfoOperator::get_ls_leader_addr(tenant_id, arg.ls_id_.id(), src_server))) {
      // maybe ls has no leader, report OB_ENTRY_NOT_EXIST, reset ret to 4184, retry ls migration after 10 seconds
      // Error codes that inner sql may return:
      // 1. If sql performs aggregation such as SUM, a row will be returned even if the row content cannot be found, but the content in this row value is NULL, and an error OB_ERR_NULL_VALUE will be reported when the value is extracted.
      // 2. If sql selects the corresponding column normally, no row will be returned if the row content cannot be found, and an error OB_ENTRY_NOT_EXIST will be reported.
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SERVER_OUTOF_DISK_SPACE;
      }
      LOG_WARN("fail to get ls leader addr", KR(ret), K(tenant_id), K(arg), K(src_server));
    } else if (OB_FAIL(ObStorageInfoOperator::get_unit_data_disk_size(tenant_id, src_server, src_unit_disk_size))) {
      LOG_WARN("fail to get unit data disk size", KR(ret), K(tenant_id), K(arg), K(src_server));
    } else if (OB_FAIL(ObStorageInfoOperator::get_table_total_data_size(tenant_id, src_server, src_table_total_size))) {
      LOG_WARN("fail to get table data size", KR(ret), K(tenant_id), K(arg), K(src_server));
    } else if (OB_FAIL(ObStorageInfoOperator::get_table_total_data_size(tenant_id, arg.dst_.get_server(), dest_table_total_size))) {
      LOG_WARN("fail to get table data size", KR(ret), K(tenant_id), K(arg));
    } else if (OB_FAIL(ObStorageInfoOperator::get_ls_total_disk_size(tenant_id, arg.ls_id_.id(), src_server, ls_total_size))) {
      LOG_WARN("fail to get ls total disk size", KR(ret), K(tenant_id), K(arg), K(src_server));
    } else {
      // maybe required_private_macro_size is smaller than tenant_private_macro_free_size
      const int64_t private_macro_cache_pct = tnt_disk_space_mgr->get_all_disk_cache_info().private_macro_cache_.get_space_percent();
      const int64_t expand_size_depend_private_size = MAX(0, required_private_macro_size - tenant_private_macro_free_size) / private_macro_cache_pct * 100;
      int64_t expand_size_depand_ls_size = 0;
      int64_t expand_size = 0;
      double src_cache_ratio = 0;
      int64_t dest_total_size = 0;
      if (0 != src_table_total_size) {
        // dest_local_cache = src_local_cache / src_table_total_size * (dest_table_total_size + ls_total_size)
        src_cache_ratio = double(src_unit_disk_size) / double(src_table_total_size);
        // maybe src_unit_disk_size is 2GB, but src_table_total_size only 100MB, this situation cannot calculate expand_size, dest_server can hold ls_size
        if (src_cache_ratio < 1.0) {
          dest_total_size = dest_table_total_size + ls_total_size;
          expand_size_depand_ls_size = MAX(src_cache_ratio * dest_total_size - dest_unit_disk_size, 0);
        }
      }
      expand_size = MAX(expand_size_depend_private_size, expand_size_depand_ls_size);
      // expand_size needs to be 1GB aligned
      expand_size = upper_align(expand_size, DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH);
      if (0 == expand_size) {
        // do nothing, do not need expand
      } else if (OB_FAIL(auto_expand_data_disk_size(tenant_id, expand_size))) {
        LOG_WARN("fail to auto expand data disk size", KR(ret), K(tenant_id), K(expand_size), K(expand_size_depend_private_size), K(expand_size_depand_ls_size),
                 K(src_cache_ratio), K(dest_total_size), K(ls_total_size), K(required_private_macro_size), K(tenant_private_macro_free_size),
                 K(src_unit_disk_size), K(dest_unit_disk_size), K(src_table_total_size), K(dest_table_total_size), K(src_server), K(arg));
      } else {
        LOG_INFO("succ to check ls migration space full", K(tenant_id), K(expand_size), K(arg), K(expand_size_depand_ls_size), K(src_server),
                 K(expand_size_depend_private_size), K(src_cache_ratio), K(dest_total_size), K(ls_total_size), K(required_private_macro_size),
                 K(tenant_private_macro_free_size), K(src_unit_disk_size), K(dest_unit_disk_size), K(src_table_total_size), K(dest_table_total_size));
      }
    }
  }
  return ret;
}

int ObDiskSpaceManager::check_if_tenant_support_auto_expand_disk(const uint64_t tenant_id, bool &is_auto_expand)
{
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  omt::ObTenant *tenant = nullptr;
  is_auto_expand = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpacted error, omt is null", KR(ret), KP(omt));
  } else if (OB_FAIL(omt->get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpacted error, tenant is null", KR(ret), KP(tenant));
  } else if (0 == tenant->get_unit().config_.data_disk_size()) {
    is_auto_expand = true;
  } else {
    is_auto_expand = false;
  }
  return ret;
}

// disk_capacity = total_disk_size - cpu*2GB
int ObDiskSpaceManager::get_data_disk_capacity(int64_t &disk_capacity)
{
  int ret = OB_SUCCESS;
  disk_capacity = 0;
  omt::ObTenantNodeBalancer::ServerResource svr_res_assigned;
  if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().get_server_allocated_resource(svr_res_assigned))) {
    LOG_WARN("fail to get server allocated resource", KR(ret));
  } else {
    double free_cpu_count = MAX(0, common::get_cpu_count() - svr_res_assigned.max_cpu_);
    // maybe cpu*2GB is larger than datafile_size, so take max(0,value)
    disk_capacity = MAX(0, total_disk_size_ - reserved_disk_size_ - free_cpu_count * DEFAULT_DISK_SIZE_RESERVED_PERCPU);
  }
  return ret;
}

// if auto expand, need to consider cpu*2GB reserved size, so free_disk_size = total_disk_size - cpu*2GB - alloc_disk_size = disk_capacity - alloc_disk_size
int ObDiskSpaceManager::get_free_disk_size_for_auto_expand(int64_t &free_disk_size)
{
  int ret = OB_SUCCESS;
  free_disk_size = 0;
  int64_t data_disk_capacity = 0;
  if (OB_FAIL(get_data_disk_capacity(data_disk_capacity))) {
    LOG_WARN("fail to get res disk size", KR(ret));
  } else {
    free_disk_size = MAX(0, data_disk_capacity - alloc_disk_size_);
  }
  return ret;
}

int ObDiskSpaceManager::is_ss_cache_need_expand(bool &is_need_expand)
{
  int ret = OB_SUCCESS;
  is_need_expand = false;
  int64_t data_disk_capacity = 0;
  if (OB_FAIL(get_data_disk_capacity(data_disk_capacity))) {
    LOG_WARN("fail to get data disk capacity", KR(ret));
  } else if (alloc_disk_size_ >= data_disk_capacity * DEFAULT_DISK_SIZE_EXPAND_THRESHOLD_PERCENT) {
    is_need_expand = true;
  } else {
    is_need_expand = false;
  }
  LOG_INFO("finish to check ss cache need expand", KR(ret), K_(alloc_disk_size), K(data_disk_capacity), K_(total_disk_size), K(is_need_expand));
  return ret;
}

/**
 * --------------------------------ObTenantDiskSpaceManager------------------------------------
 */

ObTenantDiskSpaceManager::ObTenantDiskSpaceManager()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    disk_size_lock_(ObLatchIds::DISK_SPACE_MANAGER_LOCK),
    total_disk_size_(0),
    all_disk_cache_info_(),
    tmp_file_cache_stat_(),
    major_macro_cache_stat_(),
    private_macro_cache_stat_()
{}

ObTenantDiskSpaceManager::~ObTenantDiskSpaceManager() { destroy(); }

int ObTenantDiskSpaceManager::init(const uint64_t tenant_id, const int64_t data_disk_size)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant disk space manager has been inited.", KR(ret));
  } else if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id)) || (tenant == nullptr) ||
             (data_disk_size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(data_disk_size), KP(tenant));
  } else {
    lib::ObMutexGuard guard(disk_size_lock_);
    const bool is_hidden_tenant = tenant->is_hidden();
    total_disk_size_ = data_disk_size;
    // when restart observer, real_sys_tenant's data_disk_size = sys_unit_config + hidden_sys_data_disk_size
    if (is_sys_tenant(tenant_id) && !is_hidden_tenant) {
      total_disk_size_ += OB_SERVER_DISK_SPACE_MGR.get_hidden_sys_data_disk_config_size();
    }
    tenant_id_ = tenant_id;
    tmp_file_cache_stat_.reset();
    major_macro_cache_stat_.reset();
    private_macro_cache_stat_.reset();
    if (OB_FAIL(all_disk_cache_info_.init(total_disk_size_))) {
      LOG_WARN("fail to init all cache disk info", KR(ret), K_(total_disk_size));
    } else {
      is_inited_ = true;
      LOG_INFO("succ to init tenant disk space manager", K_(tenant_id), K(is_hidden_tenant), K_(total_disk_size),
        K_(all_disk_cache_info));
    }
  }
  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

int ObTenantDiskSpaceManager::start()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObTenantDiskSpaceManager::stop()
{}

void ObTenantDiskSpaceManager::wait()
{}

void ObTenantDiskSpaceManager::destroy()
{
  lib::ObMutexGuard guard(disk_size_lock_);
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tmp_file_cache_stat_.reset();
  major_macro_cache_stat_.reset();
  private_macro_cache_stat_.reset();
  all_disk_cache_info_.reset();
  total_disk_size_ = 0;
}

int ObTenantDiskSpaceManager::mtl_init(ObTenantDiskSpaceManager *&tenant_disk_space_manager)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_disk_space_manager->init(MTL_ID(), MTL_INIT_CTX()->init_data_disk_size_))) {
    LOG_WARN("fail to init", KR(ret));
  }
  return ret;
}

int ObTenantDiskSpaceManager::alloc_file_size(const int64_t alloc_size,
                                              const ObStorageObjectType object_type,
                                              const bool is_tmp_file_read_cache)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(alloc_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk size to alloc", KR(ret), K(alloc_size));
  } else {
    lib::ObMutexGuard guard(disk_size_lock_);
    switch (object_type) {
      case ObStorageObjectType::PRIVATE_DATA_MACRO:
      case ObStorageObjectType::PRIVATE_META_MACRO: {
        if (alloc_size > get_private_macro_free_disk_size()) {
          ret = OB_SERVER_OUTOF_DISK_SPACE;
          if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
            LOG_ERROR("local cache of incremental data is exhausted, will affect performance, please expand DATA_DISK_SIZE",
              KR(ret), K_(tenant_id), K_(total_disk_size), K(alloc_size), K_(all_disk_cache_info));
          }
        } else {
          add_private_macro_alloc_size(alloc_size);
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION: {
        // do nothing, cannot alloc file size for PRIVATE_TABLET_CURRENT_VERSION, because create tablet_id dir has been alloced size
        break;
      }
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        // meta_file_disk_size cannot be oversold
        if (alloc_size > get_meta_file_free_disk_size()) {
          ret = OB_SERVER_OUTOF_DISK_SPACE;
          if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
            LOG_ERROR("fail to alloc meta file disk size, please expand unit DATA_DISK_SIZE", KR(ret), K_(tenant_id),
              K(alloc_size), K_(total_disk_size), K_(all_disk_cache_info));
          }
        } else {
          add_meta_file_alloc_size(alloc_size);
        }
        // when meta_file_alloc_size reaches 95% of meta_file_reserved_size, report error log
        if (get_meta_file_alloc_size() > (get_meta_file_reserved_size() * META_FILE_ALLOC_SIZE_WARNING_PCT)) {
          if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
            LOG_ERROR("meta file disk size is exhausted, please expand DATA_DISK_SIZE", K_(tenant_id),
              K_(total_disk_size), K(alloc_size), K_(all_disk_cache_info));
          }
        }
        break;
      }
      case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
        const int64_t avail_size = get_preread_free_disk_size();
        if (OB_FAIL(alloc_file_size(alloc_size, avail_size, alloc_info().major_macro_read_cache_alloc_size_))) {
          if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
            LOG_WARN("fail to alloc file size", KR(ret), K(alloc_size), K(avail_size), K_(all_disk_cache_info));
          }
        }
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        if (is_tmp_file_read_cache) {
          const int64_t avail_size = get_preread_free_disk_size();
          if (OB_FAIL(alloc_file_size(alloc_size, avail_size, alloc_info().tmp_file_read_cache_alloc_size_))) {
            if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
              LOG_WARN("fail to alloc file size", KR(ret), K(alloc_size), K(avail_size), K_(all_disk_cache_info));
            }
          }
        } else {
          const int64_t avail_size = get_tmp_file_write_free_disk_size();
          if (OB_FAIL(alloc_file_size(alloc_size, avail_size, alloc_info().tmp_file_write_cache_alloc_size_))) {
            if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
              LOG_WARN("fail to alloc file size", KR(ret), K(alloc_size), K(avail_size), K_(all_disk_cache_info));
            }
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(object_type), "object_type_str", 
          get_storage_objet_type_str(object_type));
        break;
      }
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::alloc_file_size(const int64_t alloc_size, const int64_t avail_size, int64_t &used_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(alloc_size <= 0 || avail_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk size to alloc", KR(ret), K(alloc_size), K(avail_size));
  } else if (alloc_size > avail_size) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_WARN("fail to alloc disk size", KR(ret), K_(tenant_id), K(alloc_size), K_(all_disk_cache_info),
        K_(total_disk_size));
    }
  } else {
    used_size += alloc_size;
  }
  return ret;
}

int ObTenantDiskSpaceManager::free_file_size(const int64_t free_size,
                                             const ObStorageObjectType object_type,
                                             const bool is_tmp_file_read_cache)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(free_size <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid disk size to free", KR(ret), K(free_size));
  } else {
    lib::ObMutexGuard guard(disk_size_lock_);
    switch (object_type) {
      case ObStorageObjectType::PRIVATE_DATA_MACRO:
      case ObStorageObjectType::PRIVATE_META_MACRO: {
        if (OB_FAIL(free_file_size(free_size, object_type, is_tmp_file_read_cache, 
            alloc_info().private_macro_alloc_size_))) {
          LOG_WARN("fail to free file size", KR(ret), K(free_size), K_(all_disk_cache_info));
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION:
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        if (OB_FAIL(free_file_size(free_size, object_type, is_tmp_file_read_cache, 
            alloc_info().meta_file_alloc_size_))) {
          LOG_WARN("fail to free file size", KR(ret), K(free_size), K_(all_disk_cache_info));
        }
        break;
      }
      case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
        if (OB_FAIL(free_file_size(free_size, object_type, is_tmp_file_read_cache, 
            alloc_info().major_macro_read_cache_alloc_size_))) {
          LOG_WARN("fail to free file size", KR(ret), K(free_size), K_(all_disk_cache_info));
        }
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        if (is_tmp_file_read_cache) {
          if (OB_FAIL(free_file_size(free_size, object_type, is_tmp_file_read_cache, 
              alloc_info().tmp_file_read_cache_alloc_size_))) {
            LOG_WARN("fail to free file size", KR(ret), K(free_size), K_(all_disk_cache_info));
          }
        } else {
          if (OB_FAIL(free_file_size(free_size, object_type, is_tmp_file_read_cache, 
              alloc_info().tmp_file_write_cache_alloc_size_))) {
            LOG_WARN("fail to free file size", KR(ret), K(free_size), K_(all_disk_cache_info));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(object_type), "object_type_str", 
          get_storage_objet_type_str(object_type));
        break;
      }
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::free_file_size(const int64_t free_size,
                                             const ObStorageObjectType object_type,
                                             const bool is_tmp_file_read_cache,
                                             int64_t &used_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(free_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk size to free", KR(ret), K(free_size));
  } else if (free_size > used_size) {
    used_size = 0;
    LOG_WARN("alloc size is inaccurate", K_(tenant_id), K(free_size), K(object_type), "object_type_str",
      get_storage_objet_type_str(object_type), K(is_tmp_file_read_cache), K(used_size));
  } else {
    used_size -= free_size;
  }
  return ret;
}

int ObTenantDiskSpaceManager::resize_total_disk_size(const int64_t new_data_disk_size)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K_(is_inited));
  } else if (new_data_disk_size == total_disk_size_) {
    // do nothing, tenant data disk size does not changed
  } else if (new_data_disk_size < total_disk_size_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not make the tenant data disk size smaller", KR(ret), K_(total_disk_size), K(new_data_disk_size));
  } else if (new_data_disk_size > total_disk_size_) {
    int64_t delta_size = new_data_disk_size - total_disk_size_;
    if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.alloc(delta_size))) {
      LOG_WARN("fail to alloc disk size", KR(ret), K(delta_size));
    } else {
      int64_t delta_micro_cache_file_size = delta_size * get_micro_cache_info().get_space_percent() / 100;
      if (OB_FAIL(do_alloc_micro_cache_file(get_micro_cache_reserved_size(), delta_micro_cache_file_size))) {
        LOG_WARN("fail to alloc micro_cache file", KR(ret), K_(all_disk_cache_info), K(delta_micro_cache_file_size));
      } else {
        total_disk_size_ += delta_size;
        if (OB_FAIL(all_disk_cache_info_.update_reserved_size(total_disk_size_))) {
          LOG_WARN("fail to update cache reserved size", KR(ret), K_(total_disk_size), K_(all_disk_cache_info));
        } else {
          LOG_INFO("succ to resize tenant disk size", K_(tenant_id), K_(total_disk_size), K_(all_disk_cache_info),
            K(delta_size), K(delta_micro_cache_file_size));
        }
      }
      if (OB_FAIL(ret)) { // if fail, need free disk size
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = OB_SERVER_DISK_SPACE_MGR.free(delta_size))) {
          LOG_ERROR("fail to free disk size", KR(tmp_ret));
        }
      }
    }
  }
  return ret;
}

bool ObTenantDiskSpaceManager::need_expand_disk_size() const
{
  bool is_need_expand = false;
  // if alloc_size has reached 90% of total_size, need auto expand tenant data disk size
  is_need_expand = (is_tenant_disk_size_need_expand(get_private_macro_alloc_size(), get_private_macro_reserved_size()) ||
                    is_tenant_disk_size_need_expand(get_meta_file_alloc_size(), get_meta_file_reserved_size()) ||
                    is_tenant_disk_size_need_expand(get_preread_cache_reserved_size() - get_preread_free_disk_size(), 
                                                    get_preread_cache_reserved_size()));
  return is_need_expand;
}

int ObTenantDiskSpaceManager::calibrate_alloc_size(const int64_t alloc_disk_size,
                                                   const ObStorageObjectType object_type,
                                                   const bool is_tmp_file_read_cache)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(alloc_disk_size < 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid alloc disk size", KR(ret), K(alloc_disk_size));
  } else {
    LOG_INFO("begin to calibrate alloc size", K_(tenant_id), K_(total_disk_size), K(is_tmp_file_read_cache),
      K(object_type), "object_type_str", get_storage_objet_type_str(object_type), K(alloc_disk_size), 
      K_(all_disk_cache_info));
    lib::ObMutexGuard guard(disk_size_lock_);
    switch (object_type) {
      case ObStorageObjectType::PRIVATE_DATA_MACRO:
      case ObStorageObjectType::PRIVATE_META_MACRO: {
        if (alloc_disk_size > get_private_macro_reserved_size()) {
          LOG_ERROR("unexpected private macro alloc disk size", K(object_type), K(alloc_disk_size), 
            K_(all_disk_cache_info));
        }
        set_private_macro_alloc_size(alloc_disk_size);
        break;
      }
      case ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION:
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        if (alloc_disk_size > get_meta_file_reserved_size()) {
          LOG_ERROR("unexpected meta file alloc disk size", K(object_type), K(alloc_disk_size),
            K_(all_disk_cache_info));
        }
        set_meta_file_alloc_size(alloc_disk_size);
        break;
      }
      case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
        if ((alloc_disk_size + get_tmp_file_read_cache_alloc_size()) > get_preread_cache_reserved_size()) {
          LOG_ERROR("unexpected major macro preread cache alloc disk size", K(object_type), K(alloc_disk_size),
            K_(all_disk_cache_info));
        }
        set_major_macro_read_cache_alloc_size(alloc_disk_size);
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        if (is_tmp_file_read_cache) {
          if ((alloc_disk_size + get_major_macro_read_cache_alloc_size()) > get_preread_cache_reserved_size()) {
            LOG_ERROR("unexpected tmp file read cache alloc disk size", K(object_type), K(alloc_disk_size),
              K_(all_disk_cache_info));
          }
          set_tmp_file_read_cache_alloc_size(alloc_disk_size);
        } else {
          if (alloc_disk_size > (get_preread_cache_reserved_size() + get_tmp_file_write_cache_reserved_size())) {
            LOG_ERROR("unexpected tmp file write cache alloc disk size", K(object_type), K(alloc_disk_size),
              K_(all_disk_cache_info));
          }
          set_tmp_file_write_cache_alloc_size(alloc_disk_size);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
        break;
      }
    }
    LOG_INFO("finish to calibrate alloc size", KR(ret), K_(tenant_id), K_(total_disk_size), K(is_tmp_file_read_cache),
      K(object_type), "object_type_str", get_storage_objet_type_str(object_type), K(alloc_disk_size), K_(all_disk_cache_info));
  }
  return ret;
}

int ObTenantDiskSpaceManager::check_micro_cache_file_size(const int64_t exp_micro_cache_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else {
    lib::ObMutexGuard guard(disk_size_lock_);
    // when restart observer, need to check micro cache file size.
    // Because user maybe resize data disk size, but resize_total_disk_size has not enough time to be called yet,
    // the server has been restarted
    if (get_micro_cache_reserved_size() > exp_micro_cache_size) {
      int64_t delta_size = get_micro_cache_reserved_size() - exp_micro_cache_size;
      if (OB_FAIL(do_alloc_micro_cache_file(exp_micro_cache_size, delta_size))) {
        LOG_WARN("fail to alloc micro_cache file", KR(ret), K(exp_micro_cache_size), K(delta_size));
      } else {
        FLOG_INFO("succ to resize micro cache file size", K_(tenant_id), K_(total_disk_size), K(exp_micro_cache_size),
          K(delta_size), K_(all_disk_cache_info));
      }
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::update_cache_disk_ratio(const ObTenantDiskCacheRatioInfo &new_disk_cache_ratio)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!new_disk_cache_ratio.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_disk_cache_ratio));
  } else {
    lib::ObMutexGuard guard(disk_size_lock_);
    // NOTICE: currently, not support 'shrink micro_cache'
    const int64_t new_micro_cache_pct = new_disk_cache_ratio.micro_cache_size_pct_;
    const int64_t new_private_macro_pct = new_disk_cache_ratio.private_macro_size_pct_;
    const int64_t cur_micro_cache_pct = micro_cache_info().get_space_percent();
    const int64_t cur_private_macro_pct = private_macro_cache_info().get_space_percent();
    if (OB_UNLIKELY(new_micro_cache_pct < cur_micro_cache_pct)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(new_micro_cache_pct), K(cur_micro_cache_pct));
    } else if ((new_micro_cache_pct != cur_micro_cache_pct) && (new_private_macro_pct != cur_private_macro_pct)) {
      micro_cache_info().space_percent_ = new_micro_cache_pct;
      private_macro_cache_info().space_percent_ = new_private_macro_pct;
      if (OB_FAIL(all_disk_cache_info_.update_reserved_size(total_disk_size_))) {
        LOG_WARN("fail to update cache reserved size", KR(ret), K(new_disk_cache_ratio), K_(all_disk_cache_info));
      }
    }
  } 
  return ret;
}

int ObTenantDiskSpaceManager::update_cache_disk_ratio(
    const int64_t new_micro_size_pct, 
    const int64_t new_macro_size_pct,
    bool &succ_adjust,
    int64_t &ori_micro_cache_reserved_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K_(is_inited));
  } else {
    succ_adjust = false;
    lib::ObMutexGuard guard(disk_size_lock_);
    const int64_t new_macro_reserved_size = total_disk_size_ * new_macro_size_pct / 100;
    const int64_t new_micro_cache_size = total_disk_size_ * new_micro_size_pct / 100;
    ori_micro_cache_reserved_size = get_micro_cache_reserved_size();
    const int64_t ori_private_macro_reserved_size = get_private_macro_reserved_size();
    const int64_t ori_total_size = ori_micro_cache_reserved_size + ori_private_macro_reserved_size;
    if (OB_UNLIKELY(new_micro_cache_size + new_macro_reserved_size != ori_total_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(new_micro_cache_size), K(new_macro_reserved_size), K(ori_total_size));
    // Now, only support 'increase micro_cache and decrease macro_cache'
    } else if (new_macro_reserved_size >= get_private_macro_alloc_size() &&
               new_macro_reserved_size < ori_private_macro_reserved_size &&
               new_micro_cache_size > ori_micro_cache_reserved_size) {
      set_micro_cache_space_percent(new_micro_size_pct);
      set_private_macro_space_percent(new_macro_size_pct);
      set_micro_cache_reserved_size(new_micro_cache_size);
      set_private_macro_reserved_size(new_macro_reserved_size);
      succ_adjust = true;
      LOG_INFO("succ to update macro/micro file ratio", K(new_micro_cache_size), K(new_macro_reserved_size),
        K_(all_disk_cache_info));
    } else {
      LOG_INFO("can't update macro/micro file ratio", K(new_macro_reserved_size), K(new_micro_cache_size), 
        K_(all_disk_cache_info));
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::falloc_micro_cache_file(const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K_(is_inited));
  } else {
    lib::ObMutexGuard guard(disk_size_lock_);
    if (OB_FAIL(do_alloc_micro_cache_file(offset, size))) {
      LOG_WARN("fail to alloc micro_cache file", KR(ret), K(offset), K(size));
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::do_alloc_micro_cache_file(const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  int sys_ret = 0;
  int file_fd = 0;
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  if (OB_UNLIKELY(offset < 0 || size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(offset), K(size));
  } else if (OB_ISNULL(tnt_file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager should not be null", KR(ret), K(tnt_file_mgr));
  } else if (FALSE_IT(file_fd = tnt_file_mgr->get_micro_cache_file_fd())) {
  } else if (0 != (sys_ret = ::fallocate(file_fd, 0/*MODE*/, offset, size))) {
    ret = share::ObIODeviceLocalFileOp::convert_sys_errno();
    LOG_WARN("fail to alloc micro_cache file size", KR(ret), K(sys_ret), K_(total_disk_size),
      K(offset), K(size), K(errno));
  }
  return ret;
}

int ObTenantDiskSpaceManager::get_used_disk_size(int64_t &used_disk_size)
{
  int ret = OB_SUCCESS;
  share::ObUnitInfoGetter::ObTenantConfig unit;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_unit(tenant_id_, unit))) {
    LOG_WARN("fail to get tenant unit config", KR(ret), K(tenant_id_));
  } else {
    int64_t tenant_config_disk_size = unit.get_effective_actual_data_disk_size();
    int64_t tenant_used_disk_size = get_private_macro_alloc_size() +
                                    get_meta_file_alloc_size() +
                                    get_micro_cache_reserved_size() +
                                    get_tmp_file_write_cache_alloc_size() +
                                    get_tmp_file_read_cache_alloc_size() +
                                    get_major_macro_read_cache_alloc_size();
    // because real_sys_tenant's data_disk_size = sys_unit_config + hidden_sys_data_disk_config_size
    // get tenant_used_disk_size must be less than sys_unit_config
    if (tenant_used_disk_size > tenant_config_disk_size) {
      if (OB_SYS_TENANT_ID == tenant_id_) {
        tenant_used_disk_size = tenant_config_disk_size;
      } else {
        // ignore error code, otherwise ALTER RESOURCE UNIT DATA_DISK_SIZE failed
        LOG_ERROR("the tenant's data disk used size exceeds data disk total size", K_(tenant_id), K(tenant_used_disk_size), 
          K(tenant_config_disk_size), K_(all_disk_cache_info));
      }
    }
    used_disk_size = tenant_used_disk_size;
  }
  return ret;
}

void ObTenantDiskSpaceManager::update_local_cache_hit_stat(const ObStorageObjectType object_type,
                                                           const int64_t delta_cnt, const int64_t delta_size)
{
  int ret = OB_SUCCESS;
  if (ObStorageObjectType::TMP_FILE == object_type) {
    tmp_file_cache_stat_.update_cache_hit(delta_cnt, delta_size);
  } else if (is_major_macro_objtype(object_type)) {
    major_macro_cache_stat_.update_cache_hit(delta_cnt, delta_size);
  } else if (is_private_macro_objtype(object_type)) {
    private_macro_cache_stat_.update_cache_hit(delta_cnt, delta_size);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected object type", KR(ret), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
  }
}

void ObTenantDiskSpaceManager::update_local_cache_miss_stat(const ObStorageObjectType object_type,
                                                            const int64_t delta_cnt, const int64_t delta_size)
{
  int ret = OB_SUCCESS;
  if (ObStorageObjectType::TMP_FILE == object_type) {
    tmp_file_cache_stat_.update_cache_miss(delta_cnt, delta_size);
  } else if (is_major_macro_objtype(object_type)) {
    major_macro_cache_stat_.update_cache_miss(delta_cnt, delta_size);
  } else if (is_private_macro_objtype(object_type)) {
    private_macro_cache_stat_.update_cache_miss(delta_cnt, delta_size);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected object type", KR(ret), K(object_type), "object_type_str", get_storage_objet_type_str(object_type));
  }
}

} // namespace storage
} // namespace oceanbase
