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


#include "ob_zone_manager.h"
#include "share/ob_zone_table_operation.h"
#include "src/share/ob_common_rpc_proxy.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace observer;

namespace rootserver
{

ObZoneManagerBase::ObZoneManagerBase()
  : lock_(ObLatchIds::ZONE_INFO_RW_LOCK), inited_(false), loaded_(false),
    zone_infos_(), zone_count_(0),
    global_info_(), proxy_(NULL)
{
}

ObZoneManagerBase::~ObZoneManagerBase()
{
}

void ObZoneManagerBase::reset()
{
  zone_count_ = 0;
  global_info_.reset();
}

int ObZoneManagerBase::init(ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    proxy_ = &proxy;
    inited_ = true;
    loaded_ = false;
  }
  return ret;
}

int ObZoneManagerBase::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_ || !loaded_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner_stat_error", K_(inited), K_(loaded), K(ret));
  }
  return ret;
}

int ObZoneManagerBase::get_zone_count(int64_t &zone_count) const
{
  int ret = OB_SUCCESS;
  zone_count = 0;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    zone_count = zone_count_;
  }
  return ret;
}

int ObZoneManagerBase::get_zone(const int64_t idx, ObZoneInfo &info) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  info.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(idx), K(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    if (idx >= 0 && idx < zone_count_) {
      info = zone_infos_[idx];
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(ObZoneInfo &info) const
{
  return get_zone(info.zone_, info);
}

int ObZoneManagerBase::get_zone(const common::ObZone &zone, ObZoneInfo &info) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(zone), K(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < zone_count_; ++i) {
      if (zone_infos_[i].zone_ == zone) {
        ret = OB_SUCCESS;
        info = zone_infos_[i];
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_all_region(common::ObIArray<common::ObRegion> &region_list) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else {
    region_list.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      common::ObRegion region;
      if (OB_FAIL(zone_infos_[i].get_region(region))) {
        LOG_WARN("fail to get region", K(ret));
      } else if (has_exist_in_array(region_list, region)) {
        // bypass
      } else if (OB_FAIL(region_list.push_back(region))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_region(const common::ObZone &zone, ObRegion &region) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(zone), K(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < zone_count_; ++i) {
      if (zone_infos_[i].zone_ == zone) {
        if (OB_FAIL(zone_infos_[i].get_region(region))) {
          LOG_WARN("fail get region", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone_count(const ObRegion &region, int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(region), K(ret));
  } else {
    for (int64_t i = 0; i < zone_count_; ++i) {
      ObRegion zone_region(zone_infos_[i].region_.info_.ptr());
      if (zone_region == region) {
        count ++;
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(const ObRegion &region, ObIArray<ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(region), K(ret));
  } else {
    for (int64_t i = 0; i < zone_count_ && OB_SUCC(ret); ++i) {
      ObRegion zone_region(zone_infos_[i].region_.info_.ptr());
      if (zone_region == region) {
        if (OB_FAIL(zone_list.push_back(zone_infos_[i].zone_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(const share::ObZoneStatus::Status &status,
                            common::ObIArray<ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (status < ObZoneStatus::INACTIVE || status >= ObZoneStatus::UNKNOWN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(status), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      if (zone_infos_[i].status_ == status) {
        if (OB_FAIL(zone_list.push_back(zone_infos_[i].zone_))) {
          LOG_WARN("failed to add zone list", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(ObIArray<ObZoneInfo> &infos) const
{
  int ret = OB_SUCCESS;
  infos.reset();
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      if (OB_FAIL(infos.push_back(zone_infos_[i]))) {
        LOG_WARN("failed to add zone info list", K(ret));
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(ObIArray<ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      if (OB_FAIL(zone_list.push_back(zone_infos_[i].zone_))) {
        LOG_WARN("push back zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_active_zone(ObZoneInfo &info) const
{
  int ret = OB_SUCCESS;
  ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
  ObPtrGuard<ObZoneInfo> tmp_info_guard(alloc);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid info", K(ret), K(info));
  } else if (OB_FAIL(tmp_info_guard.init())) {
    LOG_WARN("init temporary variable failed", K(ret));
  } else {
    ObZoneInfo &tmp_info = *tmp_info_guard.ptr();
    tmp_info.zone_ = info.zone_;
    ret = get_zone(tmp_info);
    if (OB_FAIL(get_zone(tmp_info))) {
      LOG_WARN("get_zone failed", "zone", tmp_info.zone_, K(ret));
    } else {
      if (ObZoneStatus::ACTIVE == tmp_info.status_) {
        info = tmp_info;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::check_zone_exist(const common::ObZone &zone, bool &zone_exist) const
{
  int ret = OB_SUCCESS;
  zone_exist = false;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; i < zone_count_; ++i) {
      if (zone == zone_infos_[i].zone_) {
        zone_exist = true;
        break;
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::check_zone_active(const common::ObZone &zone, bool &zone_active) const
{
  int ret = OB_SUCCESS;
  zone_active = false;
  bool find_zone = false;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; i < zone_count_; ++i) {
      if (zone == zone_infos_[i].zone_) {
        find_zone = true;
        if (ObZoneStatus::ACTIVE == zone_infos_[i].status_) {
          zone_active = true;
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret) && !find_zone) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find zone", KR(ret), K(zone));
  }
  return ret;
}

int ObZoneManagerBase::reload()
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zone_list;
  HEAP_VAR(ObGlobalInfo, global_info) {
    ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
    ObPtrGuard<ObZoneInfo, common::MAX_ZONE_NUM> tmp_zone_infos(alloc);
    // use last value default

    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(tmp_zone_infos.init())) {
      LOG_WARN("failed to alloc temp zone infos", K(ret));
    } else if (OB_FAIL(ObZoneTableOperation::load_global_info(*proxy_, global_info))) {
      LOG_WARN("failed to get global info", K(ret));
    } else if (OB_FAIL(ObZoneTableOperation::get_zone_list(*proxy_, zone_list))) {
      LOG_WARN("failed to get zone list", K(ret));
    } else if (zone_list.count() > common::MAX_ZONE_NUM) {
      ret = OB_ERR_SYS;
      LOG_ERROR("the count of zone on __all_zone table is more than limit, cannot reload",
                "zone count", zone_list.count(),
                "zone count limit", common::MAX_ZONE_NUM, K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
        ObZoneInfo &info = tmp_zone_infos.ptr()[i];
        info.zone_ = zone_list[i];
        if (OB_FAIL(ObZoneTableOperation::load_zone_info(*proxy_, info))) {
          LOG_WARN("failed reload zone", "zone", zone_list[i], K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      reset();
      global_info_ = global_info;

      for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
        zone_infos_[zone_count_] = tmp_zone_infos.ptr()[i];
        ++zone_count_;
      }
    }

    if (OB_SUCC(ret)) {
      loaded_ = true;
      LOG_INFO("succeed to reload zone manager", "zone_manager_info", this);
    } else {
      LOG_WARN("failed to reload zone manager", KR(ret));
    }
  }
  return ret;
}

int ObZoneManagerBase::update_privilege_version(const int64_t privilege_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (privilege_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid privilege_version", K(privilege_version), K(ret));
  } else if (OB_FAIL(update_value_with_lease(global_info_.zone_,
      global_info_.privilege_version_, privilege_version))) {
    LOG_WARN("update privilege version failed", K(ret), K(privilege_version));
  }
  return ret;
}

int ObZoneManagerBase::update_config_version(const int64_t config_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (config_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config_version", K(config_version), K(ret));
  } else if (OB_FAIL(update_value_with_lease(global_info_.zone_,
      global_info_.config_version_, config_version))) {
    LOG_WARN("update config version failed", K(ret), K(config_version));
  }
  return ret;
}

int ObZoneManagerBase::update_recovery_status(
    const common::ObZone &zone,
    const share::ObZoneInfo::RecoveryStatus status)
{
  return OB_OP_NOT_ALLOW; // discarded.
}

int ObZoneManagerBase::set_cluster_name(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (cluster_name.empty() || cluster_name.length() > common::OB_MAX_CLUSTER_NAME_LENGTH)  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster_name invalid", KR(ret), K(cluster_name));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", KR(ret));
  } else {
    const ObZone &zone = global_info_.zone_;

    if (OB_FAIL(global_info_.cluster_.info_.assign(cluster_name))) {
      LOG_WARN("fail assign cluster name", KR(ret), K(cluster_name), K(global_info_.cluster_));
    } else if (OB_FAIL(global_info_.cluster_.update(updater, zone, 0, global_info_.cluster_.info_))) {
      LOG_WARN("update cluster_name failed", KR(ret), K(zone), K(cluster_name), K(global_info_.cluster_));
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  DEBUG_SYNC(AFTER_SET_ALL_ZONE_CLUSTER_NAME);
  return ret;
}

int ObZoneManagerBase::get_cluster(ObFixedLengthString<MAX_ZONE_INFO_LENGTH> &cluster) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    cluster = global_info_.cluster_.info_;
  }
  return ret;
}

int ObZoneManagerBase::get_config_version(int64_t &config_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    config_version =  global_info_.config_version_;
  }
  return ret;
}

int ObZoneManagerBase::get_time_zone_info_version(int64_t &time_zone_info_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    time_zone_info_version =  global_info_.time_zone_info_version_;
  }
  return ret;
}

int ObZoneManagerBase::get_lease_info_version(int64_t &lease_info_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    lease_info_version = global_info_.lease_info_version_;
  }
  return ret;
}

ObClusterRole ObZoneManagerBase::get_cluster_role()
{
  ObClusterRole cluster_role = PRIMARY_CLUSTER;
  return cluster_role;
}

int ObZoneManagerBase::check_encryption_zone(const common::ObZone &zone, bool &encryption)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(zone), KR(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < zone_count_ && OB_SUCC(ret); ++i) {
      if (zone_infos_[i].zone_ == zone) {
        found = true;
        encryption = zone_infos_[i].is_encryption();
      } else {
        // go on check
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("zone not exist", KR(ret), K(zone));
    }
  }
  return ret;
}

int ObZoneManagerBase::update_value_with_lease(
    const common::ObZone &zone, share::ObZoneInfoItem &item,
    int64_t value, const char *info /* = NULL */)
{
  int ret = OB_SUCCESS;
  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t lease_info_version = std::max(global_info_.lease_info_version_ + 1, cur_time);
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (!item.is_valid() || value < 0) {
    // empty zone means all zone, info can be null
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(item), K(value), K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    if (OB_FAIL(item.update(updater, zone, value, info))) {
      LOG_WARN("update item failed", K(ret), K(zone), K(value), K(info));
    } else if (OB_FAIL(global_info_.lease_info_version_.update(
        updater, global_info_.zone_, lease_info_version))) {
      LOG_WARN("update lease info version failed", K(ret), K(lease_info_version));
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      (void)ATOMIC_SET(&global_info_.lease_info_version_.value_, lease_info_version);
    }
  }
  return ret;
}

int ObZoneManagerBase::find_zone(const common::ObZone &zone, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; i < zone_count_; ++i) {
      if (zone == zone_infos_[i].zone_) {
        idx = i;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObZoneManagerBase)
{
  SpinRLockGuard guard(lock_);
  int64_t pos = 0;
  J_KV(K_(global_info),
       K_(zone_count));
  J_ARRAY_START();
  for(int64_t i = 0; i < zone_count_; ++i) {
    if (0 != i) {
      J_COMMA();
    }
    BUF_PRINTO(zone_infos_[i]);
  }
  J_ARRAY_END();
  return pos;
}

// only used for copying data to/from shadow_
int ObZoneManagerBase::copy_infos(ObZoneManagerBase &dest, const ObZoneManagerBase &src)
{
  int ret = OB_SUCCESS;
  const int64_t count = src.zone_count_;
  if (0 > count || common::MAX_ZONE_NUM < count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid zone count", K(count), K(ret));
  } else {
    for (int64_t idx = 0; idx < count; ++idx) {
      dest.zone_infos_[idx] = src.zone_infos_[idx];
    }
    dest.global_info_ = src.global_info_;
    dest.zone_count_ = count;
    dest.inited_ = src.inited_;
    dest.loaded_ = src.loaded_;
  }
  return ret;
}

///////////////////////////
///////////////////////////
ObZoneManager::ObZoneManager()
  : write_lock_(ObLatchIds::ZONE_MANAGER_LOCK), shadow_(), random_(71)
{
}

ObZoneManager::~ObZoneManager()
{
}

ObZoneManager::ObZoneManagerShadowGuard::ObZoneManagerShadowGuard(const common::SpinRWLock &lock,
                                                                  ObZoneManagerBase &zone_mgr,
                                                                  ObZoneManagerBase &shadow,
                                                                  int &ret)
    : lock_(const_cast<SpinRWLock &>(lock)),
    zone_mgr_(zone_mgr), shadow_(shadow), ret_(ret)
{
  SpinRLockGuard copy_guard(lock_);
  if (OB_UNLIKELY(OB_SUCCESS !=
      (ret_ = ObZoneManager::copy_infos(shadow_, zone_mgr_)))) {
    LOG_WARN("copy to shadow_ failed", K(ret_));
  }
}

ObZoneManager::ObZoneManagerShadowGuard::~ObZoneManagerShadowGuard()
{
  SpinWLockGuard copy_guard(lock_);
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
  } else if (OB_UNLIKELY(OB_SUCCESS !=
      (ret_ = ObZoneManager::copy_infos(zone_mgr_, shadow_)))) {
    LOG_WARN_RET(ret_, "copy from shadow_ failed", K(ret_));
  }
}

int ObZoneManager::init(ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    // uninit proxy is used to prevent somebody from
    // trying to directly use the set-interfaces in ObZoneManagerBase
    // actually, all update operations are performed in shadow_
    if (OB_FAIL(ObZoneManagerBase::init(uninit_proxy_))) {
      LOG_WARN("init zone manager failed", K(ret));
    } else if (OB_FAIL(shadow_.init(proxy))) {
      LOG_WARN("init shadow_ failed", K(ret));
    }
  }
  return ret;
}

int ObZoneManager::reload()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_,
        *(static_cast<ObZoneManagerBase *> (this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.reload();
    }
  }
  return ret;
}

int ObZoneManager::update_privilege_version(const int64_t privilege_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_,
        *(static_cast<ObZoneManagerBase *> (this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.update_privilege_version(privilege_version);
    }
  }
  return ret;
}

int ObZoneManager::update_config_version(const int64_t config_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_,
        *(static_cast<ObZoneManagerBase *> (this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.update_config_version(config_version);
    }
  }
  return ret;
}

int ObZoneManager::update_recovery_status(
    const common::ObZone &zone,
    const share::ObZoneInfo::RecoveryStatus status)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_,
        *(static_cast<ObZoneManagerBase *> (this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.update_recovery_status(zone, status);
    }
  }
  return ret;
}

int ObZoneManager::set_cluster_name(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_,
        *(static_cast<ObZoneManagerBase *> (this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_cluster_name(cluster_name);
    }
  }
  return ret;
}
int ObZoneManager::set_storage_format_version(const int64_t version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_,
        *(static_cast<ObZoneManagerBase *> (this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_storage_format_version(version);
    }
  }
  return ret;
}


int ObZoneManagerBase::check_all_zone_regions_the_same_(const common::ObRegion &region)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    bool same = true;
    for (int64_t i = 0; OB_SUCC(ret) && same && i < zone_count_; ++i) {
      common::ObRegion zone_region;
      if (OB_UNLIKELY(!zone_infos_[i].is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid zone_info", KR(ret), "zone_info", zone_infos_[i]);
      } else if (OB_FAIL(zone_infos_[i].get_region(zone_region))) {
        LOG_WARN("fail to get region", K(ret));
      } else if (zone_region != region) {
        same = false;
      }
    }
    if (OB_SUCC(ret) && !same) {
      // For now in shared_storage mode, all zones must keep the same region.
      // This constraint will be canceled in later verison.
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("new region is different with current zone regions", KR(ret), K(region));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "more than one region in shared-storage mode");
    }
  }
  return ret;
}

int ObZoneManagerBase::get_storage_format_version(int64_t &version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    version = global_info_.storage_format_version_;
    if (version < OB_STORAGE_FORMAT_VERSION_V3) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage format version should greater than or equal to 3", K(ret), K(version));
    }
  }
  return ret;
}

int ObZoneManagerBase::set_storage_format_version(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  const int64_t old_version = global_info_.storage_format_version_;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (version <= OB_STORAGE_FORMAT_VERSION_INVALID
              || version >= OB_STORAGE_FORMAT_VERSION_MAX
              || old_version < OB_STORAGE_FORMAT_VERSION_V3
              || version != old_version + 1 ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(version), K(old_version), K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    if (OB_FAIL(global_info_.storage_format_version_.update(updater, global_info_.zone_, version))) {
      LOG_WARN("set storage format version failed", K(ret), "zone", global_info_.zone_, K(version));
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("set format version succeed", K(ret), K(version), K(global_info_.storage_format_version_));
    }
  }
  return ret;
}
} // rootserver
} // oceanbase
