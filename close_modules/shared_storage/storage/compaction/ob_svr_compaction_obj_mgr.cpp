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
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_svr_compaction_obj_mgr.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
namespace oceanbase
{
using namespace share;
using namespace lib;
namespace compaction
{
/**
 * -------------------------------------------------------------------ObSvrIDCache-------------------------------------------------------------------
 */
ObSvrIDCache::ObSvrIDCache()
  : is_inited_(false),
    lock_(),
    last_refresh_ts_(0)
{}

ObSvrIDCache::~ObSvrIDCache()
{
  destroy();
}

int ObSvrIDCache::refresh(const bool force_refresh)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSvrIDCache has not been inited", K(ret));
  } else if (force_refresh
      || (last_refresh_ts_ + REFRESH_SVR_ID_INTERVAL < ObTimeUtility::fast_current_time())) {
    ret = inner_refresh();
  }
  return ret;
}

int ObSvrIDCache::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSvrIDCache has been inited", K(ret));
  } else {
    svr_infos_.set_attr(ObMemAttr(MTL_ID(), "SvrIDCache"));
    is_inited_ = true;
  }
  return ret;
}

void ObSvrIDCache::destroy()
{
  if (is_inited_) {
    ObMutexGuard guard(lock_);
    is_inited_ = false;
    last_refresh_ts_ = 0;
    svr_infos_.reset();
  }
}

int ObSvrIDCache::inner_refresh()
{
  int ret = OB_SUCCESS;
  ObServerTableOperator table_operator;
  ObSEArray<ObServerStatus, DEFAULT_ARRAY_SIZE> server_status;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is unexpected null", KR(ret));
  } else if (OB_FAIL(table_operator.init(GCTX.sql_proxy_))) {
    LOG_WARN("failed to init operator", KR(ret));
  } else if (table_operator.get(*GCTX.sql_proxy_, ObZone()/*empty zone*/, server_status)) {
    LOG_WARN("failed to get server status", KR(ret));
  } else if (!server_status.empty()) {
    ObSEArray<ObSvrPair, DEFAULT_ARRAY_SIZE> alive_svr_infos;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < server_status.count(); ++idx) {
      const ObServerStatus &status = server_status.at(idx);
      if (status.is_alive() && OB_FAIL(alive_svr_infos.push_back(ObSvrPair(status.id_, status.server_)))) {
        LOG_WARN("failed to push server id", KR(ret), K(status));
      }
    }

    if (OB_SUCC(ret)) {
      ObMutexGuard guard(lock_);
      if (OB_FAIL(svr_infos_.assign(alive_svr_infos))) {
        LOG_WARN("failed to assign svr id", KR(ret), K(alive_svr_infos));
      } else {
        LOG_INFO("[SS_MERGE] success to refresh server id cache", KR(ret), K_(svr_infos), K_(last_refresh_ts));
        last_refresh_ts_ = ObTimeUtility::fast_current_time();
      }
    }
  }
  return ret;
}

int ObSvrIDCache::get_svr_ids(ObIArray<uint64_t> &input_svr_ids)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  const int64_t info_cnt = svr_infos_.count();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSvrIDCache has not been inited", K(ret));
  } else if (info_cnt > 0 && OB_FAIL(input_svr_ids.reserve(info_cnt))) {
    LOG_WARN("failed to reserve input svr ids", K(ret));
  }

  for (int64_t idx = 0; OB_SUCC(ret) && idx < info_cnt; ++idx) {
    const ObSvrPair &svr_pair = svr_infos_.at(idx);
    if (OB_FAIL(input_svr_ids.push_back(svr_pair.svr_id_))) {
      LOG_WARN("failed to add svr id", K(ret), K(svr_pair));
    }
  }
  return ret;
}

int ObSvrIDCache::get_svr_infos(ObIArray<ObSvrPair> &input_svr_infos)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSvrIDCache has not been inited", K(ret));
  } else if (OB_FAIL(input_svr_infos.assign(svr_infos_))) {
    LOG_WARN("failed to assign svr infos", K(ret), K(svr_infos_));
  }
  return ret;
}

int ObSvrIDCache::get_svr_info(const uint64_t svr_id, ObSvrPair &svr_info)
{
  int ret = OB_SUCCESS;
  svr_info.reset();
  ObMutexGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSvrIDCache has not been inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < svr_infos_.count(); ++i) {
    const ObSvrPair &cur_info = svr_infos_.at(i);
    if (svr_id == cur_info.svr_id_) {
      svr_info = cur_info;
      break;
    }
  }
  if (OB_SUCC(ret) && !svr_info.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObSvrIDCache::get_svr_id_by_addr(
    const ObIArray<ObSvrPair> &svr_infos,
    const common::ObAddr &addr,
    uint64_t &svr_id)
{
  int ret = OB_SUCCESS;
  svr_id = 0;
  for (int64_t i = 0; i < svr_infos.count(); ++i) {
    const ObSvrPair &cur_info = svr_infos.at(i);
    if (addr == cur_info.svr_addr_) {
      svr_id = cur_info.svr_id_;
      break;
    }
  }
  if (0 == svr_id) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}
/**
 * -------------------------------------------------------------------ObSvrCompactionObjMgr-------------------------------------------------------------------
 */
int ObSvrCompactionObjMgr::init_obj(const uint64_t &key, ObCompactionReportObj &value)
{
  int ret = OB_SUCCESS;
  if (0 == GCTX.get_server_id()) {
    ret = OB_EAGAIN;
    LOG_INFO("[ShareStorage]server id is invalid", KR(ret));
  } else {
    ret = value.init(key, key == GCTX.get_server_id() ? OBJ_EXEC_MODE_WRITE_ONLY : OBJ_EXEC_MODE_READ_ONLY);
  }
  return ret;
}

int ObSvrCompactionObjMgr::get_valid_key_array(ObIArray<uint64_t> &keys)
{
  int ret = OB_SUCCESS;
  if (exist_ls_leader_flag_) {
    if (OB_FAIL(MTL(ObTenantCompactionObjMgr*)->get_svr_id_cache().get_svr_ids(keys))) {
      LOG_WARN("failed to get svr ids", KR(ret));
    }
  } else if (GCTX.get_server_id() > 0) {
    if (OB_FAIL(keys.push_back(GCTX.get_server_id()))) {
      LOG_WARN("failed to set keys", KR(ret));
    }
    LOG_TRACE("not exist ls leader, no need to refresh all svr compaction status obj",
      KR(ret), K_(exist_ls_leader_flag));
  }
  return ret;
}

int ObSvrCompactionObjMgr::check_obj_valid(
  const uint64_t &key,
  bool &is_valid,
  bool &is_leader)
{
  UNUSED(key);
  int ret = OB_SUCCESS;
  is_valid = true; // all svr_id is valid
  is_leader = false; // useless for svr_id
  return ret;
}

int ObSvrCompactionObjMgr::refresh(
  const bool exist_ls_leader_flag,
  ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  exist_ls_leader_flag_ = exist_ls_leader_flag;
  ret = ObBasicCompactionObjMgr::refresh(obj_buf);
  return ret;
}

} // namespace compaction
} // namespace oceanbase
