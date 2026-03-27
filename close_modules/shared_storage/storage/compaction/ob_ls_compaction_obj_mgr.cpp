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
#include "ob_ls_compaction_obj_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/compaction/ob_tenant_ls_merge_scheduler.h"

namespace oceanbase
{
using namespace lib;
namespace compaction
{
/**
 * -------------------------------------------------------------------ObLSObj-------------------------------------------------------------------
 */
ObLSObj::ObLSObj()
  : ObBasicObj(),
    lock_(),
    is_leader_(false),
    switch_to_leader_ts_(0),
    ls_compaction_status_(),
    cur_svr_ls_compaction_status_(),
    compaction_svrs_(),
    ls_compaction_list_()
{}

ObLSObj::~ObLSObj()
{
  destroy();
}

void ObLSObj::destroy()
{
  ObBasicObj::destroy();
  is_leader_ = false;
  switch_to_leader_ts_ = 0;
  ls_compaction_status_.destroy();
  cur_svr_ls_compaction_status_.destroy();
  compaction_svrs_.reset();
}

int ObLSObj::init(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool is_leader = false;
  if (OB_FAIL(ObLSCompactionObjMgr::get_ls_role(ls_id, is_valid, is_leader))) {
    if (OB_LS_NOT_EXIST == ret) {
      LOG_WARN("failed to get ls role", KR(ret), K(ls_id));
    }
  } else if (OB_UNLIKELY(!is_valid)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected invalid", KR(ret), K(ls_id), K(is_valid), K(is_leader));
  } else if (FALSE_IT(is_leader_ = is_leader)) {
  } else if (OB_FAIL(ls_compaction_status_.init(ls_id))) {
    LOG_WARN("failed to init compaction status", KR(ret), K(ls_id));
  } else if (OB_FAIL(cur_svr_ls_compaction_status_.init(GCTX.get_server_id(), ls_id, OBJ_EXEC_MODE_WRITE_ONLY))) {
    LOG_WARN("failed to init svr compaction status", KR(ret), "svr_id", GCTX.get_server_id(), K(ls_id));
  } else if (OB_FAIL(compaction_svrs_.init(ls_id))) {
    LOG_WARN("failed to init compaction tasks", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_compaction_list_.init(ls_id))) {
    LOG_WARN("failed to init ls compaction list", KR(ret), K(ls_id));
  } else {
    LOG_INFO("[SharedStorage] success to init ls obj", KR(ret), K(ls_id), K(is_leader));
  }
  return ret;
}

void ObLSObj::get_broadcast_info(ObLSBroadcastInfo &info)
{
  ls_compaction_status_.fill_info(info); // get merge version && merge state on ls leader
  compaction_svrs_.fill_info(info);      // get ls exec svr id
  {
    lib::ObMutexGuard guard(lock_);
    info.is_leader_ = is_leader_;
  }
}

void ObLSObj::set_role_with_delay_overwrite(
    const ObLSID &ls_id,
    const bool input_is_leader)
{
  ObTenantLSMergeScheduler *scheduler = MTL(ObTenantLSMergeScheduler *);
  const int64_t delay_write_time = nullptr == scheduler
                                 ? ObTenantLSMergeScheduler::DEFAULT_DELAY_OVERWRITE_INTERVAL
                                 : scheduler->get_dealy_overwrite_interval();
  bool is_leader = is_leader_;

  if (input_is_leader) {
    if (is_leader) {
      // do nothing
    } else if (0 == switch_to_leader_ts_) {
      // if cur state is not leader, just set ts, is_leader_ = false
      switch_to_leader_ts_ = ObTimeUtility::fast_current_time();
      LOG_INFO("[SS_MERGE] new ls leader, set switch ts first", K(ls_id), K(input_is_leader), K(is_leader), K(delay_write_time));
    } else if (switch_to_leader_ts_ + delay_write_time < ObTimeUtility::fast_current_time()) {
      is_leader = true;
      switch_to_leader_ts_ = 0;
    }
  } else {
    is_leader = false;
  }
  if (is_leader != is_leader_) {
    is_leader_ = is_leader;
    LOG_INFO("[SS_MERGE] ls set role", K(ls_id), K(input_is_leader), K_(is_leader), K_(switch_to_leader_ts));
  }
}

int ObLSObj::refresh(
    const bool is_leader,
    ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  bool is_leader_role = false;
  {
    lib::ObMutexGuard guard(lock_);
    set_role_with_delay_overwrite(ls_compaction_status_.get_ls_id(), is_leader);
    is_leader_role = is_leader_;
  }

  ObLSBroadcastInfo info;
  (void) get_broadcast_info(info);

  if (FAILEDx(ls_compaction_status_.refresh(is_leader_role, cur_svr_ls_compaction_status_, info, obj_buf))) { // info update state
    LOG_WARN("failed to refresh ls finish tablet", KR(ret));
  } else if (OB_FAIL(refresh_exec_svr(is_leader_role, info))) { // info update exec_svr
    LOG_WARN("failed to refresh exec svr", KR(ret));
  } else if (OB_FAIL(cur_svr_ls_compaction_status_.refresh(info, obj_buf))) {
    LOG_WARN("failed to refresh svr finish tablet", KR(ret));
  } else if (OB_FAIL(compaction_svrs_.refresh(is_leader_role, obj_buf))) {
    LOG_WARN("failed to refresh compaction tasks", KR(ret));
  } else if (OB_FAIL(ls_compaction_list_.refresh(is_leader_role, obj_buf))) {
    LOG_WARN("failed to refresh ls compaction list", KR(ret));
  }
  return ret;
}

int ObLSObj::refresh_exec_svr(
    const bool is_leader_role,
    ObLSBroadcastInfo &info)
{
  int ret = OB_SUCCESS;
  bool has_exec_svr = false;
  ObSvrPair svr_info;

  if (!is_leader_role || ObLSCompactionState::COMPACTION_STATE_IDLE == info.state_) {
    has_exec_svr = true; // ls follower do nothing
  } else if (0 == info.exec_svr_id_) {
    // no exec svr, need to choose one
  } else if (OB_FAIL(MTL_SVR_ID_CACHE.get_svr_info(info.exec_svr_id_, svr_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) { // server is offline, can't find in svr_id_cache
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check svr id", KR(ret), K(info));
    }
  }

  ObSEArray<uint64_t, 8> candidates;
  if (OB_FAIL(ret) || has_exec_svr) {
  } else if (OB_FAIL(get_candidate_svrs(candidates))) {
    LOG_WARN("failed to get candidate svr ids", K(ret));
  } else if (OB_FAIL(get_new_exec_svr(candidates, info))) {
    LOG_WARN("failed to choose exec server for ls", K(ret), K(info));
  } else if (is_leader_role && OB_FAIL(compaction_svrs_.update(info.exec_svr_id_, info.compaction_scn_))) {
    LOG_WARN("failed to update compaction svrs", K(ret), K(info));
  } else {
    LOG_INFO("[SS_MERGE] succ to choose the new exec server", K(info));
  }
  return ret;
}

// It's OK to get an invalid input ls info
int ObLSObj::get_candidate_svrs(
    ObIArray<uint64_t> &candidates)
{
  int ret = OB_SUCCESS;
  candidates.reset();
  ObSEArray<ObSvrPair, 8> svr_infos;

  if (OB_FAIL(MTL_SVR_ID_CACHE.get_svr_infos(svr_infos))) {
    LOG_WARN("failed to get candidate svr ids", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < svr_infos.count(); ++i) {
    const ObSvrPair &cur_info = svr_infos.at(i);
    bool is_exist = false;
    if (is_exist && OB_FAIL(candidates.push_back(cur_info.svr_id_))) {
      LOG_WARN("failed to add svr id to candidates", K(ret), K(cur_info), K(svr_infos));
    }
  }
  return ret;
}

int ObLSObj::get_new_exec_svr(
    const ObIArray<uint64_t> &candidates,
    ObLSBroadcastInfo &info)
{
  int ret = OB_SUCCESS;
  info.exec_svr_id_ = 0;
  int64_t min_unfinish_tablet_cnt = INT64_MAX;

  // choose the server with the minimum count of unfinished tablets as exec server
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates.count(); ++i) {
    uint64_t svr_id = candidates.at(i);
    ObBasicObjHandle<ObCompactionReportObj> report_obj_hdl;
    int64_t unfinish_tablet_cnt = 0;

    if (OB_FAIL(MTL_SVR_OBJ_MGR.get_obj_handle(svr_id, report_obj_hdl))) {
      LOG_WARN("failed to get report obj handle", K(ret), K(svr_id));
    } else if (FALSE_IT(unfinish_tablet_cnt = report_obj_hdl.get_obj()->get_unfinish_tablet_count())) {
    } else if (report_obj_hdl.get_obj()->is_empty() || 0 == unfinish_tablet_cnt) {
      info.exec_svr_id_ = svr_id;
      break;
    } else if (min_unfinish_tablet_cnt > unfinish_tablet_cnt) {
      info.exec_svr_id_ = svr_id;
      min_unfinish_tablet_cnt = unfinish_tablet_cnt;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == info.exec_svr_id_) { // choose ls leader's svr
    info.exec_svr_id_ = GCTX.get_server_id();
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_SHARED_STORAGE_COMPACTION_CHOOSE_EXEC_SVR) ret;
    if (OB_FAIL(ret)) {
      if (candidates.count() == 3) {
        info.exec_svr_id_ = 3;
      }
      ret = OB_SUCCESS;
      LOG_INFO("ERRSIM EN_SHARED_STORAGE_COMPACTION_CHOOSE_EXEC_SVR", KR(ret),
        "ls_id", ls_compaction_status_.get_ls_id(), K(info), K(candidates));
    }
  }

  #ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_SHARED_STORAGE_IGNORE_REPLICA) ret;
      if (OB_FAIL(ret)) {
        info.exec_svr_id_ = 1; // force choose svr 1 as exec svr
        ret = OB_SUCCESS;
        LOG_INFO("ERRSIM EN_SHARED_STORAGE_IGNORE_REPLICA", K(ret));
      }
    }
#endif
#endif
  return ret;
}

int ObLSObj::get_tablet_state(
  const ObTabletID &tablet_id,
  ObTabletCompactionState &tablet_state)
{
  return cur_svr_ls_compaction_status_.get_tablet_state(tablet_id, tablet_state);
}

int ObLSObj::start_merge(
    const int64_t broadcast_version,
    const bool is_leader)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_compaction_status_.get_ls_id();
  bool obj_is_leader = false;
  {
    lib::ObMutexGuard guard(lock_);
    set_role_with_delay_overwrite(ls_id, is_leader);
    obj_is_leader = is_leader_;
  }

  if (!is_leader && !obj_is_leader) {
    // do nothing
  } else if (!obj_is_leader) {
    // new leader, but delay write obj
  } else if (OB_FAIL(ls_compaction_status_.refresh_compaction_scn(broadcast_version))) {
    LOG_WARN("failed to refresh compaction scn", KR(ret), K(broadcast_version), KPC(this));
    if (OB_HASH_NOT_EXIST != ret) {
      (void) ADD_SUSPECT_LS_INFO(MAJOR_MERGE,
                               ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                               ls_id,
                               ObSuspectInfoType::SUSPECT_SS_START_MERGE,
                               broadcast_version,
                               (int64_t) ret/*error_code*/);
    }
  } else {
    LOG_INFO("[SS_MERGE] ls success to start merge", K(broadcast_version), K(is_leader), KPC(this));
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObLSCompactionObjMgr-------------------------------------------------------------------
 */
int ObLSCompactionObjMgr::get_valid_key_array(ObIArray<share::ObLSID> &keys)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObLSService *)->get_ls_ids(keys))) {
    LOG_WARN("failed to get all ls id", K(ret));
  }
  return ret;
}

int ObLSCompactionObjMgr::refresh(
  bool &exist_ls_leader_flag,
  ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  exist_ls_leader_flag = false;
  if (OB_SUCC(ObBasicCompactionObjMgr::refresh(obj_buf))) {
    exist_ls_leader_flag = exist_ls_leader_flag_;
  }
  return ret;
}

int ObLSCompactionObjMgr::check_obj_valid(
    const share::ObLSID &ls_id,
    bool &is_valid,
    bool &is_leader)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(get_ls_role(ls_id, is_valid, is_leader)) && is_leader) {
    LOG_TRACE("exist ls leader", KR(ret), K(ls_id));
    exist_ls_leader_flag_ = true;
  } else if (OB_LS_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    is_valid = false;
  }
  return ret;
}

int ObLSCompactionObjMgr::get_ls_role(const share::ObLSID &ls_id, bool &is_valid, bool &is_leader)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  is_leader = false;
  ObLSHandle ls_handle;
  ObRole role = INVALID_ROLE;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, storage::ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle is invalid", KR(ret), K(ls_handle));
  } else if (ls_handle.get_ls()->is_deleted() || ls_handle.get_ls()->is_offline()) {
    ret = OB_LS_NOT_EXIST;
    if (REACH_THREAD_TIME_INTERVAL(30_s)) {
      LOG_INFO("ls is in abnormal status", KR(ret), K(ls_id), K(ls_handle),
        "deleted", ls_handle.get_ls()->is_deleted(),
        "offline", ls_handle.get_ls()->is_offline());
    }
  } else if (OB_FAIL(ls_handle.get_ls()->get_ls_role(role))) {
    LOG_WARN("failed to get ls role", K(ret), K(ls_id));
  } else {
    is_leader = (LEADER == role);
    is_valid = true;
    LOG_TRACE("get ls role", KR(ret), K(is_leader), K(role));
  }
  return ret;
}

int ObLSCompactionObjMgr::start_merge(const int64_t broadcast_version)
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(lock_);
  typename ObjMap::iterator iter = obj_map_.begin();

  for ( ; OB_SUCC(ret) && iter != obj_map_.end(); ++iter) {
    const ObLSID &ls_id = iter->first;
    ObLSObj *ls_obj = iter->second;
    bool is_valid = false;
    bool is_leader = false;

    if (OB_ISNULL(ls_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls obj is unexpected null", KR(ret), K(ls_id));
    } else if (OB_FAIL(get_ls_role(ls_id, is_valid, is_leader))) {
      if (OB_LS_NOT_EXIST != ret) {
        LOG_WARN("failed to get ls role", KR(ret), K(ls_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(!is_valid)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is unexpected invalid", KR(ret), K(broadcast_version), K(ls_id), KPC(ls_obj));
    } else if (OB_FAIL(ls_obj->start_merge(broadcast_version, is_leader))) {
      LOG_WARN("[SS_MERGE] failed to start ls merge", KR(ret), K(ls_id), KPC(ls_obj));
    }
  }
  return ret;
}

bool ObLSCompactionObjMgr::exist_compacting_obj()
{
  bool bret = false;
  obsys::ObRLockGuard guard(lock_);
  typename ObjMap::iterator iter = obj_map_.begin();

  for ( ; iter != obj_map_.end(); ++iter) {
    const ObLSID &ls_id = iter->first;
    ObLSObj *ls_obj = iter->second;

    if (OB_ISNULL(ls_obj)) {
    } else if (COMPACTION_STATE_IDLE != ls_obj->cur_svr_ls_compaction_status_.get_state()) {
      bret = true;
      break;
    }
  }
  return bret;
}


} // namespace compaction
} // namespace oceanbase
