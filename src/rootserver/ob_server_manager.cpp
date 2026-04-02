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

// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!
// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!
// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!

#define USING_LOG_PREFIX RS


#include "ob_server_manager.h"
#include "observer/ob_server.h"
#include "storage/ob_file_system_router.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver
{
ObServerManager::ObServerManager()
  : inited_(false), has_build_(false),
    server_status_rwlock_(ObLatchIds::SERVER_STATUS_LOCK),
    maintaince_lock_(ObLatchIds::SERVER_MAINTAINCE_LOCK),
    status_change_callback_(NULL), server_change_callback_(NULL), config_(NULL),
    unit_mgr_(NULL), zone_mgr_(NULL),
    rpc_proxy_(NULL),
    st_operator_(), rs_addr_(), server_statuses_()
{
  reset();
}

ObServerManager::~ObServerManager()
{
}

int ObServerManager::init(ObIStatusChangeCallback &status_change_callback,
                          ObIServerChangeCallback &server_change_callback,
                          ObMySQLProxy &proxy,
                          ObUnitManager &unit_mgr,
                          ObZoneManager &zone_mgr,
                          common::ObServerConfig &config,
                          const common::ObAddr &rs_addr,
                          obrpc::ObSrvRpcProxy &rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("server manager have already init", K(ret));
  } else if (!rs_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rs_addr", K(rs_addr), K(ret));
  } else if (OB_FAIL(st_operator_.init(&proxy))) {
    LOG_WARN("server table operator init failed", K(ret));
  } else {
    has_build_ = false;
    status_change_callback_ = &status_change_callback;
    server_change_callback_ = &server_change_callback;
    config_ = &config;
    unit_mgr_ = &unit_mgr;
    zone_mgr_ = &zone_mgr;
    rs_addr_ = rs_addr;
    rpc_proxy_ = &rpc_proxy;
    server_statuses_.reset();
    inited_ = true;
  }
  return ret;
}

int ObServerManager::add_server(const common::ObAddr &server, const ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObServerStatus *server_status = NULL;
  uint64_t server_id = OB_INVALID_ID;
  // avoid maintain operation run concurrently
  SpinWLockGuard guard(maintaince_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (!zone.is_empty()) {
    bool zone_active = false;
    if (OB_FAIL(zone_mgr_->check_zone_active(zone, zone_active))) {
      LOG_WARN("check_zone_active failed", K(zone), K(ret));
    } else if (!zone_active) {
      ret = OB_ZONE_NOT_ACTIVE;
      LOG_WARN("zone not active", K(zone), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    {
      SpinRLockGuard guard(server_status_rwlock_);
      if (OB_SUCC(find(server, server_status))) {
        if (NULL == server_status) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server_status is null", "server_status ptr", OB_P(server_status), K(ret));
        } else if (!server_status->is_status_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status not valid", K(ret), "status", *server_status);
        } else {
          ret = OB_ENTRY_EXIST;
          LOG_WARN("server already added", K(server), K(ret));
        }
      } else if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fetch_new_server_id(server_id))) {
      LOG_WARN("fetch_new_server_id failed", K(ret));
    } else {
      const int64_t now = common::ObTimeUtility::current_time();
      ObServerStatus new_server_status;
      new_server_status.id_ = server_id;
      new_server_status.server_ = server;
      new_server_status.zone_ = zone;
      new_server_status.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_NORMAL;
      new_server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
      new_server_status.last_hb_time_ = now;
      new_server_status.lease_expire_time_ = now + ObLeaseRequest::SERVICE_LEASE;

      if (OB_SUCC(ret)) {
        if (OB_FAIL(st_operator_.update(new_server_status))) {
          LOG_WARN("st_operator update failed", K(new_server_status), K(ret));
        } else {
          SpinWLockGuard inner_guard(server_status_rwlock_);
          if (OB_FAIL(server_statuses_.push_back(new_server_status))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ROOTSERVICE_EVENT_ADD("server", "add_server", K(server));
    LOG_INFO("add new server", K(server), K(zone));
    int tmp_ret = server_change_callback_->on_server_change();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to callback on server change", KR(ret), K(tmp_ret));
    } else {
      LOG_WARN("callback on add server success");
    }
    if (OB_TMP_FAIL(SVR_TRACER.refresh())) {
      LOG_WARN("fail to refresh all server tracer", KR(ret), KR(tmp_ret));
    }
  }
  return ret;
}

int ObServerManager::is_server_stopped(const ObAddr &server, bool &is_stopped) const
{
  int ret = OB_SUCCESS;
  is_stopped = false;
  bool zone_active = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else if (OB_FAIL(zone_mgr_->check_zone_active(status_ptr->zone_, zone_active))) {
      LOG_WARN("fail to check zone active", K(ret), K(server));
    } else {
      is_stopped = status_ptr->is_stopped() || !zone_active;
    }
  }
  return ret;
}

int ObServerManager::receive_hb(
    const ObLeaseRequest &lease_request,
    uint64_t &server_id,
    bool &to_alive)
{
  int ret = OB_SUCCESS;
  bool zone_exist = true;
  to_alive = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!lease_request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease_request", K(lease_request), K(ret));
  } else if (OB_UNLIKELY(nullptr == GCTX.root_service_ || nullptr == zone_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice ptr is null", KR(ret));
  } else if (GCTX.root_service_->is_full_service()
      && OB_FAIL(zone_mgr_->check_zone_exist(lease_request.zone_, zone_exist))) {
    LOG_WARN("fail to check zone exist", KR(ret));
  } else if (!zone_exist) {
    ret = OB_ZONE_INFO_NOT_EXIST;
    LOG_WARN("zone info not exist", KR(ret), K(lease_request));
  } else {
    SpinWLockGuard guard(server_status_rwlock_);
    ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(lease_request.server_, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", "server", lease_request.server_, K(ret));
      } else {
        // overwrite ret on purpose
        ret = OB_SERVER_NOT_IN_WHITE_LIST;
        LOG_WARN("server not in white list", "server", lease_request.server_, K(ret));
        if (!has_build_) {
          LOG_INFO("accept server heartbeat before white list loaded",
              "server", lease_request.server_);
          ret = OB_SUCCESS;
        }
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else { // the new logic
      if (status_ptr->resource_info_ != lease_request.resource_info_) {
        LOG_INFO("server resource changed", "old_resource_info", status_ptr->resource_info_,
            "new_resource_info", lease_request.resource_info_);
        status_ptr->resource_info_ = lease_request.resource_info_;
      }
      status_ptr->last_hb_time_ = ::oceanbase::common::ObTimeUtility::current_time();
      server_id = status_ptr->id_;
    }
  }
  return ret;
}

int ObServerManager::set_server_status(const ObLeaseRequest &lease_request,
                                       const int64_t hb_timestamp,
                                       const bool with_rootserver,
                                       ObServerStatus &server_status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!lease_request.is_valid() || hb_timestamp <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease_request or invalid hb_timestamp",
        K(lease_request), K(hb_timestamp), K(ret));
  } else if (OB_UNLIKELY(nullptr == GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice ptr is null", KR(ret));
  } else {
    if (GCTX.root_service_->is_full_service()) {
      server_status.zone_ = lease_request.zone_;
    }
    MEMCPY(server_status.build_version_, lease_request.build_version_, OB_SERVER_VERSION_LENGTH);
    server_status.server_ = lease_request.server_;
    server_status.sql_port_ = lease_request.sql_port_;
    server_status.last_hb_time_ = hb_timestamp;
    server_status.hb_status_ = ObServerStatus::OB_HEARTBEAT_ALIVE;
    server_status.with_rootserver_ = with_rootserver;
    server_status.resource_info_ = lease_request.resource_info_;
    server_status.start_service_time_ = lease_request.start_service_time_;
    server_status.ssl_key_expired_time_ = lease_request.ssl_key_expired_time_;
  }
  return ret;
}

int ObServerManager::update_admin_status(const ObAddr &server,
    const ObServerStatus::ServerAdminStatus status, const bool remove)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard inner_guard(server_status_rwlock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (status < ObServerStatus::OB_SERVER_ADMIN_NORMAL
      || status >= ObServerStatus::OB_SERVER_ADMIN_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(status), K(ret));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_INVALID_INDEX == idx && i < server_statuses_.count(); ++i) {
      if (server_statuses_.at(i).server_ == server) {
        idx = i;
      }
    }
    if (OB_INVALID_INDEX == idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server is expected to exist", K(server), K(ret));
    } else {
      if (!remove) {
        if (ObServerStatus::OB_SERVER_ADMIN_DELETING == server_statuses_[idx].admin_status_
            && ObServerStatus::OB_SERVER_ADMIN_NORMAL == status) {
          server_statuses_[idx].force_stop_hb_ = false;
        }
        server_statuses_[idx].admin_status_ = status;
      } else {
        if (OB_FAIL(server_statuses_.remove(idx))) {
          LOG_WARN("remove status failed", K(ret), "size", server_statuses_.count(), K(idx));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::check_server_permanent_offline(const ObAddr &server, bool &is_offline) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_offline = false;
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        is_offline = false;
        LOG_DEBUG("treat not exist server as not alive", K(server));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      is_offline = status_ptr->is_permanent_offline();
    }
  }
  return ret;
}

int ObServerManager::check_server_alive(const ObAddr &server, bool &is_alive) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_alive = false;
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        is_alive = false;
        LOG_DEBUG("treat not exist server as not alive", K(server));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      is_alive = status_ptr->is_alive();
    }
  }
  return ret;
}

int ObServerManager::check_server_active(const ObAddr &server, bool &is_active) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_active = false;
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        is_active = false;
        LOG_INFO("treat not exist server as not active", K(server));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      is_active = status_ptr->is_active();
    }
  }
  return ret;
}

int ObServerManager::check_server_stopped(const common::ObAddr &server, bool &is_stopped) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    is_stopped = true;
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        ret = OB_SUCCESS;
        is_stopped = true;
        LOG_INFO("treat server that is not exist as stopped", K(server));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      is_stopped = status_ptr->is_stopped();
    }
  }
  return ret;
}


int ObServerManager::get_servers_of_zone(
    const common::ObZone &zone,
    common::ObIArray<common::ObAddr> &server_list,
    common::ObIArray<uint64_t> &server_id_list) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", KR(ret));
  } else {
    server_list.reuse();
    server_id_list.reuse();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if (server_statuses_[i].zone_ == zone || zone.is_empty()) {
        if (OB_FAIL(server_list.push_back(server_statuses_[i].server_))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(server_id_list.push_back(server_statuses_[i].id_))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::get_servers_of_zone(const common::ObZone &zone,
                                         ObServerArray &server_list) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    server_list.reuse();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if ((server_statuses_[i].zone_ == zone || zone.is_empty())) {
        ret = server_list.push_back(server_statuses_[i].server_);
        if (OB_FAIL(ret)) {
          LOG_WARN("push back to server_list failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerManager::is_server_exist(const ObAddr &server, bool &exist) const
{
  int ret = OB_SUCCESS;
  exist = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
      ret = OB_SUCCESS;
      exist = false;
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      exist = true;
    }
  }
  return ret;
}



int ObServerManager::get_server_status(const ObAddr &server,
                                       ObServerStatus &server_status) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(server, status_ptr))) {
      LOG_WARN("find failed", K(server), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      server_status = *status_ptr;
    }
  }
  return ret;
}

int ObServerManager::load_server_manager()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObServerStatus> statuses;
  SpinWLockGuard guard(maintaince_lock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(st_operator_.get(statuses))) {
    LOG_WARN("get server statuses from all server failed", K(ret));
  } else if (OB_FAIL(load_server_statuses(statuses))) {
    LOG_WARN("server manager build from all server failed", K(ret), K(statuses));
  } else {} // no more to do
  return ret;
}

int ObServerManager::load_server_statuses(const ObServerStatusArray &server_statuses)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (server_statuses.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_statuses is empty", K(server_statuses), K(ret));
  } else {
    // to protect from executing concurrently with add_server(),
    // see 
    SpinWLockGuard guard2(server_status_rwlock_);
    for (int64_t idx = server_statuses_.count() - 1; OB_SUCC(ret) && idx >= 0; --idx) {
      bool found = false;
      FOREACH_X(s, server_statuses, !found) {
        if (s->server_ == server_statuses_.at(idx).server_) {
          found = true;
        }
      }
      if (!found) {
        if (OB_FAIL(server_statuses_.remove(idx))) {
          LOG_WARN("remove server status failed", K(ret), K(idx));
        }
      }
    }

    FOREACH_X(s, server_statuses, OB_SUCCESS == ret) {
      ObServerStatus status = *s;
      ObServerStatus *exist_status = NULL;
      if (OB_FAIL(find(status.server_, exist_status))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("find failed", "server", status.server_, K(ret));
        } else {
          ret = OB_SUCCESS;
          // import servers can not in alive heartbeat status.
          if (ObServerStatus::OB_HEARTBEAT_ALIVE == status.hb_status_) {
            status.hb_status_ = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
          }
          LOG_INFO("import server", K(status));
          if (OB_FAIL(server_statuses_.push_back(status))) {
            LOG_WARN("push back to server_statuses failed", K(ret));
          }
        }
      } else if (NULL == exist_status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exist_status is null", "server_status ptr", OB_P(exist_status), K(ret));
      } else {
        LOG_INFO("update server admin status, before update",
            "server", status.server_, "status", *exist_status);
        ObServerStatus bak = *exist_status;
        *exist_status = status;
        exist_status->last_hb_time_ = bak.last_hb_time_;
        exist_status->with_rootserver_ = bak.with_rootserver_;
        exist_status->hb_status_ = bak.hb_status_;
        exist_status->register_time_ = bak.register_time_;
        exist_status->merged_version_ = bak.merged_version_;
        exist_status->resource_info_ = bak.resource_info_;
        exist_status->lease_expire_time_ = bak.lease_expire_time_;
        LOG_INFO("update server admin status, after update",
            "server", status.server_, "status", *exist_status);
        if (OB_SUCCESS != (ret = status_change_callback_->on_server_status_change(status.server_))) {
          LOG_WARN("submit server status update task failed",
              K(ret), "status", *exist_status);
        }
      }
    }
    if (OB_SUCC(ret)) {
      has_build_ = true;
    }
  }
  ROOTSERVICE_EVENT_ADD("server", "load_servers", K(ret), K_(has_build));
  return ret;
}

bool ObServerManager::has_build() const
{
  return has_build_;
}

int ObServerManager::get_server_zone(const ObAddr &addr, ObZone &zone) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else {
    ObServerStatus server_status;
    if (OB_FAIL(get_server_status(addr, server_status))) {
      LOG_WARN("get_server_status failed", K(addr), K(ret));
    } else {
      zone = server_status.zone_;
    }
  }
  return ret;
}

int ObServerManager::get_server_statuses(const ObZone &zone,
                                         ObServerStatusIArray &server_statuses,
                                         bool include_permanent_offline) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else {
    server_statuses.reset();
    SpinRLockGuard guard(server_status_rwlock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < server_statuses_.count(); ++i) {
      if (server_statuses_[i].zone_ == zone || zone.is_empty()) {
        if (include_permanent_offline || !server_statuses_[i].is_permanent_offline()) {
          if (!server_statuses_[i].is_valid()) {
            ret = OB_INVALID_SERVER_STATUS;
            LOG_WARN("server status is not valid", "server status", server_statuses_[i], K(ret));
          } else if (OB_SUCCESS != (ret = server_statuses.push_back(server_statuses_[i]))) {
            LOG_WARN("push back to server_statuses failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObServerManager::get_server_statuses(const ObServerArray &servers,
                                         ObServerStatusArray &server_statuses) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    server_statuses.reset();
    SpinRLockGuard guard(server_status_rwlock_);
    FOREACH_CNT_X(server, servers, OB_SUCC(ret)) {
      bool find = false;
      FOREACH_CNT_X(status, server_statuses_, !find && OB_SUCC(ret)) {
        if (!status->is_valid()) {
          ret = OB_INVALID_SERVER_STATUS;
          LOG_WARN("server status is not valid", "server status", *status, K(ret));
        } else if (*server == status->server_) {
          if (OB_FAIL(server_statuses.push_back(*status))) {
            LOG_WARN("push_back failed", K(ret));
          } else {
            find = true;
          }
        }
      }
      if (OB_SUCC(ret) && !find) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("server not found", "server", *server, K(ret));
      }
    }
  }
  return ret;
}

void ObServerManager::reset()
{
  SpinWLockGuard guard(server_status_rwlock_);
  has_build_ = false;
  server_statuses_.reset();
}


int ObServerManager::find(const ObAddr &server, const ObServerStatus *&status) const
{
  int ret = OB_SUCCESS;
  status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; i < server_statuses_.count() && !find; ++i) {
      if (server_statuses_[i].server_ == server) {
        status = &server_statuses_[i];
        find = true;
      }
    }
    if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      // we print info log here, because sometime this is normal(such as add server)
      LOG_INFO("server not exist", K(server), K(ret));
    }
  }
  return ret;
}

int ObServerManager::find(const ObAddr &server, ObServerStatus *&status)
{
  int ret = OB_SUCCESS;
  status = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    const ObServerStatus *temp_status = NULL;
    if (OB_FAIL(static_cast<const ObServerManager &>(*this).find(server, temp_status))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(server), K(ret));
      } else {
        // don't print log here, invoked function "find" already print
      }
    } else if (NULL == temp_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("temp_status is null", "temp_status ptr", OB_P(temp_status), K(ret));
    } else {
      status = const_cast<ObServerStatus *>(temp_status);
    }
  }
  return ret;
}

int ObServerManager::fetch_new_server_id(uint64_t &server_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid sql proxy", KR(ret));
  } else {
    uint64_t combine_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*GCTX.sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(OB_SYS_TENANT_ID,
        OB_MAX_USED_SERVER_ID_TYPE, combine_id))) {
      LOG_WARN("fetch_new_max_id failed", K(ret));
    } else {
      server_id = combine_id;
    }
  }
  return ret;
}


int ObServerManager::check_migrate_in_blocked(const common::ObAddr &addr, bool &blocked) const
{
  int ret = OB_SUCCESS;
  blocked = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(addr, status_ptr))) {
      LOG_WARN("find failed", K(addr), K(ret));
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      blocked = status_ptr->is_migrate_in_blocked();
    }
  }
  return ret;
}

int ObServerManager::check_in_service(const common::ObAddr &addr, bool &in_service) const
{
  int ret = OB_SUCCESS;
  in_service = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server manager has not inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(addr), K(ret));
  } else {
    SpinRLockGuard guard(server_status_rwlock_);
    const ObServerStatus *status_ptr = NULL;
    if (OB_FAIL(find(addr, status_ptr))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("find failed", K(addr), K(ret));
      } else {
        ret = OB_SUCCESS;
        in_service = false;
        LOG_INFO("treat server that not exist as not in service", K(addr));
      }
    } else if (NULL == status_ptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("status_ptr is null", "status_ptr", OB_P(status_ptr), K(ret));
    } else {
      in_service = status_ptr->in_service();
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
