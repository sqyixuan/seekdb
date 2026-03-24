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

#include "storage/compaction/ob_tenant_ls_merge_checker.h"
#include "storage/compaction/ob_tenant_ls_merge_scheduler.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/sys_table/ob_all_tablet_checksum_error_info_operator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace lib;

namespace compaction
{


/************************************************ ObTenantLSMergeChecker ************************************************/
ObTenantLSMergeChecker::ObTenantLSMergeChecker()
  : is_inited_(false),
    compaction_scn_(ObTenantLSMergeScheduler::INIT_COMPACTION_SCN),
    verified_scn_(compaction_scn_),
    check_state_(CHECK_STATE_IDLE),
    validate_type_(ObLSVerifyCkmType::VERIFY_INVALID),
    sql_proxy_(nullptr),
    validate_ls_ids_(),
    tablet_ls_pair_cache_(),
    ckm_validator_(MTL_ID(), validate_ls_ids_, tablet_ls_pair_cache_, *GCTX.sql_proxy_)
{
}

ObTenantLSMergeChecker::~ObTenantLSMergeChecker()
{
  destroy();
}

int ObTenantLSMergeChecker::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantLSMergeChecker init twice", K(ret));
  } else if (OB_FAIL(validate_ls_ids_.create(DEFAULT_MAP_BUCKET, "MrgCkmLSSet", "MrgCkmLSNode", MTL_ID()))) {
    LOG_WARN("failed to create validate ls set", K(ret));
  } else if (OB_FAIL(ckm_validator_.init())) {
    LOG_WARN("failed to init ckm validator", KR(ret));
  } else {
    tablet_ls_pair_cache_.set_tenant_id(MTL_ID());
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

void ObTenantLSMergeChecker::destroy()
{
  is_inited_ = false;
  sql_proxy_ = nullptr;
  if (validate_ls_ids_.created()) {
    validate_ls_ids_.destroy();
  }
}

ERRSIM_POINT_DEF(SKIP_LS_STATE_UPDATE);
int ObTenantLSMergeChecker::process()
{
  int ret = OB_SUCCESS;
  bool skip_ls_state_update = false;
  const int64_t new_compaction_scn = MERGE_SCHEDULER_PTR->get_frozen_version();
  ObFreezeInfo freeze_info;
  LOG_INFO("start to check ls merge", K(new_compaction_scn), KPC(this));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantLSMergeChecker not inited", K(ret));
  } else if (OB_UNLIKELY(CHECK_STATE_CKM_ERROR == get_state())) {
    if (REACH_THREAD_TIME_INTERVAL(20_s)) {
      // ignore ret
      LOG_ERROR("[SS_MERGE] Checksum Error, the validation checksum cannot continue", KPC(this));
    }
    // TODO(@DanLing) set merge status here
  } else if (OB_UNLIKELY(compaction_scn_ > new_compaction_scn
                     || (CHECK_STATE_IDLE != get_state() && compaction_scn_ != new_compaction_scn))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected compaction scn", K(ret), K(new_compaction_scn), KPC(this));
  } else if (FALSE_IT(compaction_scn_ = new_compaction_scn)) {
  } else if (OB_FAIL(check_can_validate())) {
    LOG_WARN("failed to check can validate", K(ret), K(new_compaction_scn), KPC(this));
  } else if (CHECK_STATE_VERIFY != get_state()) {
    // do nothing
  } else if (OB_FAIL(get_validate_type())) {
    LOG_WARN("failed to get verify type info", K(ret), KPC(this));
  } else if (OB_FAIL(tablet_ls_pair_cache_.try_refresh(true/*force_refresh*/))) {
    LOG_WARN("failed to refresh tablet ls pair", K(ret));
  } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr*)->get_freeze_info_by_snapshot_version(new_compaction_scn, freeze_info))) {
    LOG_WARN("failed to get freeze info", K(ret), K(new_compaction_scn));
  } else if (FALSE_IT(ckm_validator_.clear_cached_info())) {
  } else if (OB_FAIL(ckm_validator_.do_work(freeze_info, validate_type_))) {
    LOG_WARN("failed to validate checksum", K(ret));
  } else {
    update_state(CHECK_STATE_REPORT);
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_SHARED_STORAGE_DONT_UPDATE_LS_STATE) ret;
    if (OB_FAIL(ret)) {
      if (COMPACTION_STATE_INDEX_VERIFY == -ret) {
        skip_ls_state_update = true;
        LOG_INFO("ERRSIM EN_SHARED_STORAGE_DONT_UPDATE_LS_STATE", K(ret));
      }
      ret = OB_SUCCESS;
    }
    if (OB_UNLIKELY(SKIP_LS_STATE_UPDATE)) {
      skip_ls_state_update = true;
      LOG_INFO("ERRSIM SKIP_LS_STATE_UPDATE", K(ret));
    }
  }
#endif
  if (OB_SUCC(ret) && !skip_ls_state_update && CHECK_STATE_REPORT == get_state()) {
    if (OB_FAIL(update_ls_compaction_state())) {
      LOG_WARN("failed to update ls compaction state", K(ret));
    } else {
      verified_scn_ = compaction_scn_;
      update_state(CHECK_STATE_IDLE);
    }
  }

  if (OB_UNLIKELY(OB_CHECKSUM_ERROR == ret)) {
    update_state(CHECK_STATE_CKM_ERROR);
  }
  return ret;
}

void ObTenantLSMergeChecker::update_state(const LSMergeCheckState state)
{
  ATOMIC_STORE(&check_state_, state);
  FLOG_INFO("[SS_MERGE] LSMergeChecker update state", K(state), KPC(this));
}

int ObTenantLSMergeChecker::check_can_validate()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<share::ObLSID, 4> ls_ids;

  if (CHECK_STATE_IDLE != get_state()) {
    if (REACH_THREAD_TIME_INTERVAL(30_s)) {
      LOG_INFO("[SS_MERGE] LSMergeChecker is checking now", KPC(this));
    }
  } else if (verified_scn_ == compaction_scn_ && !REACH_THREAD_TIME_INTERVAL(10_min)) { // deal with the scene that ls has no leader
    if (REACH_THREAD_TIME_INTERVAL(30_s)) {
      LOG_INFO("[SS_MERGE] LSMergeChecker has verified current round of merge", KPC(this));
    }
  } else if (OB_FAIL(MTL(storage::ObLSService *)->get_ls_ids(ls_ids))) {
    LOG_WARN("failed to get ls ids", K(ret));
  } else {
    validate_ls_ids_.reuse();
    bool allow_verify = true;

    for (int64_t idx = 0; OB_SUCC(ret) && allow_verify && idx < ls_ids.count(); ++idx) {
      const ObLSID &ls_id = ls_ids.at(idx);
      bool is_valid = false;
      bool is_leader = false;
      ObBasicObjHandle<ObLSObj> ls_obj_hdl;
      ObLSBroadcastInfo ls_info;

      if (OB_FAIL(MTL_LS_OBJ_MGR.get_ls_role(ls_id, is_valid, is_leader))) {
        LOG_WARN("failed to get ls role", K(ret), K(ls_id));
      } else if (!is_valid) {
        LOG_INFO("[SS_MERGE] cur ls is not valid, cannot check checksum", K(ls_id), KPC(this));
      } else if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_id, ls_obj_hdl))) {
        LOG_WARN("failed to get ls obj handle", K(ret), K(ls_id));
      } else if (FALSE_IT(ls_obj_hdl.get_obj()->ls_compaction_status_.fill_info(ls_info))) {
      } else if (ls_info.compaction_scn_ <= ObTenantLSMergeScheduler::INIT_COMPACTION_SCN) {
        if (REACH_THREAD_TIME_INTERVAL(60_s)) {
          LOG_INFO("[SS_MERGE] get empty ls info", K(ls_id), K(ls_info));
        }
      } else if (OB_UNLIKELY(compaction_scn_ != ls_info.compaction_scn_ || !is_valid_compaction_state(ls_info.state_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpecetd ls info", K(ret), K(ls_id), K(ls_info), KPC(this));
      } else if (ObLSCompactionState::COMPACTION_STATE_IDLE == ls_info.state_
              || ObLSCompactionState::COMPACTION_STATE_INDEX_VERIFY < ls_info.state_) {
        // ignore cur ls
      } else if (ls_info.state_ == ObLSCompactionState::COMPACTION_STATE_INDEX_VERIFY) {
        if (is_leader && OB_FAIL(validate_ls_ids_.set_refactored(ls_id))) {
          LOG_WARN("failed to add ls id", K(ret), K(ls_id));  
        }
      } else { // ls merge not finished, cannot verify checksum
        validate_ls_ids_.reuse();
        allow_verify = false;
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (allow_verify) {
      if (validate_ls_ids_.empty()) {
        LOG_INFO("[SS_MERGE] Cur svr has no ls leader, no need to verify checksum", K(allow_verify), KPC(this), K(ls_ids));
      } else {
        update_state(CHECK_STATE_VERIFY);
        verified_scn_ = ObTenantLSMergeScheduler::INIT_COMPACTION_SCN;
      }
    } else {
      LOG_INFO("[SS_MERGE] Cannot verify now, wait for all ls merge to finish", K(allow_verify), KPC(this), K(ls_ids));
    }
  }
  return ret;
}

int ObTenantLSMergeChecker::get_validate_type()
{
  int ret = OB_SUCCESS;
  share::SCN broadcast_scn;
  bool is_exist = false;

  if (OB_UNLIKELY(MTL_TENANT_ROLE_CACHE_IS_INVALID())) {
    validate_type_ = ObLSVerifyCkmType::VERIFY_NONE;
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[SS_MERGE] current tenant is invalid, try later", K(ret), KPC(this));
  } else if (OB_FAIL(broadcast_scn.convert_for_inner_table_field(compaction_scn_))) {
    LOG_WARN("failed to convert int64_t to SCN", K(ret), K(compaction_scn_));
  } else if (OB_FAIL(ObTabletChecksumOperator::is_first_tablet_in_sys_ls_exist(*GCTX.sql_proxy_,
                                                                               MTL_ID(),
                                                                               broadcast_scn,
                                                                               is_exist))) {
    LOG_WARN("failed to check first tablet is exist", K(ret), K(broadcast_scn), KPC(this));
  } else if (is_exist) {
    validate_type_ = ObLSVerifyCkmType::VERIFY_CROSS_CLUSTER_CKM;
  } else if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
    validate_type_ = ObLSVerifyCkmType::VERIFY_INDEX_CKM;
  } else {
    validate_type_ = ObLSVerifyCkmType::VERIFY_CROSS_CLUSTER_CKM;
  }
  return ret;
}

int ObTenantLSMergeChecker::update_ls_compaction_state()
{
  int ret = OB_SUCCESS;

  for (LSIDIter iter = validate_ls_ids_.begin(); OB_SUCC(ret) && iter != validate_ls_ids_.end(); ++iter) {
    const ObLSID &ls_id = iter->first;
    ObBasicObjHandle<ObLSObj> ls_obj_hdl;
    ObLSBroadcastInfo ls_info;

    if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_id, ls_obj_hdl))) {
      LOG_WARN("failed to get ls obj handle", K(ret), K(ls_id));
    } else if (FALSE_IT(ls_obj_hdl.get_obj()->ls_compaction_status_.fill_info(ls_info))) {
    } else if (OB_UNLIKELY(ls_info.compaction_scn_ != compaction_scn_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected ls info", K(ret), K(ls_info), KPC(this));
    } else if (OB_FAIL(ls_obj_hdl.get_obj()->cur_svr_ls_compaction_status_.report_ls_index_verified(compaction_scn_, true/*is_svr_obj*/))) {
      LOG_WARN("failed to mark ls index verify finished on cur svr", K(ret));
    } else {
      // update ls state quickly
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ls_obj_hdl.get_obj()->ls_compaction_status_.report_ls_index_verified(compaction_scn_, false/*is_svr_obj*/))) {
        LOG_WARN_RET(tmp_ret, "failed to report ls state", K(compaction_scn_));
        if (REACH_THREAD_TIME_INTERVAL(5_min)) {
          ADD_SUSPECT_LS_INFO(MAJOR_MERGE,
                              share::ObDiagnoseTabletType::TYPE_REPORT,
                              ls_id,
                              ObSuspectInfoType::SUSPECT_COMPACTION_REPORT_ADD_FAILED,
                              tmp_ret);

        }
      }
    }
  }
  return ret;
}

int ObTenantLSMergeChecker::clear_merge_error()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletCkmErrorInfoOperator::delete_ckm_error_info(MTL_ID(), compaction_scn_))) {
    LOG_WARN("failed to delete replica ckm error info", K(ret), K(MTL_ID()), K(compaction_scn_));
  } else if (OB_FAIL(ObColumnChecksumErrorOperator::delete_column_checksum_err_info_by_scn(*GCTX.sql_proxy_, MTL_ID(), compaction_scn_))) {
    LOG_WARN("failed to delete column ckm error info", K(ret), K(MTL_ID()), K(compaction_scn_));
  } {
    update_state(CHECK_STATE_IDLE);
    FLOG_INFO("ls merge checker clear merge error", K(ret), K(MTL_ID()), K(compaction_scn_));
  }
  return ret;
}


} // compaction
} // oceanbase
