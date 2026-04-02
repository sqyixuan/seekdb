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

#define USING_LOG_PREFIX RS_COMPACTION
#include "rootserver/ob_ls_merge_progress_checker.h"
#include "storage/compaction/ob_tenant_ls_merge_checker.h"
#include "observer/sys_table/ob_all_tablet_checksum_error_info_operator.h"

namespace oceanbase
{
using namespace compaction;
using namespace share;
using namespace common;

namespace rootserver
{

ObLSMergeProgressChecker::ObLSMergeProgressChecker(
    const uint64_t tenant_id,
    volatile bool &stop)
  : is_inited_(false),
    stop_(stop),
    is_primary_service_(false),
    tenant_id_(tenant_id),
    freeze_info_(),
    expected_epoch_(OB_INVALID_ID),
    sql_proxy_(nullptr),
    schema_service_(nullptr),
    merge_progress_(),
    tablet_ls_pair_cache_(),
    ls_ids_()
{
}

int ObLSMergeProgressChecker::init(
    const bool is_primary_service,
    ObMySQLProxy &sql_proxy,
    schema::ObMultiVersionSchemaService &schema_service,
    ObIServerTrace &server_trace,
    ObMajorMergeInfoManager &merge_info_mgr)
{
  int ret = OB_SUCCESS;
  UNUSEDx(server_trace, merge_info_mgr);

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    tablet_ls_pair_cache_.set_tenant_id(tenant_id_),
    is_primary_service_ = is_primary_service;
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    is_inited_ = true;
  }
  return ret;
}

int ObLSMergeProgressChecker::set_basic_info(
    const share::ObFreezeInfo &freeze_info,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!freeze_info.is_valid() || expected_epoch < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(freeze_info), K(expected_epoch));
  } else if (OB_FAIL(clear_cached_info())) {
    LOG_WARN("fail to clear cached info", KR(ret));
  } else {
    freeze_info_ = freeze_info;
    expected_epoch_ = expected_epoch;
    LOG_INFO("success to set basic info", KR(ret), K_(freeze_info), K_(expected_epoch));
  }
  return ret;
}

int ObLSMergeProgressChecker::clear_cached_info()
{
  int ret = OB_SUCCESS;
  LOG_INFO("success to clear cached info", KR(ret), K_(tenant_id), "compaction_scn", get_compaction_scn());
  freeze_info_.reset();
  expected_epoch_ = OB_INVALID_ID;
  merge_progress_.reset();
  ls_ids_.reuse();
  return ret;
}

int ObLSMergeProgressChecker::refresh_ls_infos()
{
  int ret = OB_SUCCESS;
  ObArray<ObLSInfo> ls_infos;
  ls_infos.set_attr(ObMemAttr(tenant_id_, "RefLSInfos"));
  bool need_refresh = true;

  if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ls operator", K(ret));
  } else if (ls_ids_.empty() || REACH_THREAD_TIME_INTERVAL(5_min)) {
    ls_ids_.reuse(); // should refresh ls infos
  } else {
    need_refresh = false;
  }

  if (OB_FAIL(ret) || !need_refresh) {
  } else if (OB_FAIL(GCTX.lst_operator_->get_by_tenant(tenant_id_, false/*inner_table_only*/, ls_infos))) {
    LOG_WARN("failed to get ls infos", K(ret));
  } else if (OB_FAIL(ls_ids_.reserve(ls_infos.count()))) {
    LOG_WARN("failed to reserve ls status infos", K(ret), K(tenant_id_), K(ls_infos), K(ls_ids_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_infos.count(); ++i) {
      if (OB_FAIL(ls_ids_.push_back(ls_infos.at(i).get_ls_id()))) {
        LOG_WARN("failed to add ls ids", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      ls_ids_.reuse();
    }
  }
  return ret;
}

int ObLSMergeProgressChecker::check_exist_ckm_error()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool exist = false;
  MTL_SWITCH(tenant_id_) {
    compaction::ObTenantLSMergeChecker *tenant_ls_merge_checker = nullptr;
    if (OB_ISNULL(tenant_ls_merge_checker = MTL(compaction::ObTenantLSMergeChecker *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls merge checker is unexpected null", K(ret), K_(tenant_id));
    } else if (tenant_ls_merge_checker->check_exist_ckm_error()) {
      ret= OB_CHECKSUM_ERROR;
      LOG_ERROR("exist checksum error, check __all_tablet_checksum_error_info", KR(ret), K_(tenant_id), "compaction_scn", get_compaction_scn());
    } else if (OB_TMP_FAIL(ObTabletCkmErrorInfoOperator::check_exist_ckm_error_tablet(tenant_id_, get_compaction_scn_val(), exist))) {
      LOG_WARN("failed to check exist ckm error tablet", KR(tmp_ret), K_(tenant_id), "compaction_scn", get_compaction_scn());
    } else if (!exist && OB_TMP_FAIL(ObColumnChecksumErrorOperator::check_exist_ckm_error_table(tenant_id_, get_compaction_scn_val(), exist))) {
      LOG_WARN("failed to check exist ckm error tablet", KR(tmp_ret), K_(tenant_id), "compaction_scn", get_compaction_scn());
    } else if (exist) {
      ret= OB_CHECKSUM_ERROR;
      LOG_ERROR("exist checksum error, check __all_tablet_checksum_error_info/__all_column_checksum_error_info", KR(ret), K_(tenant_id), "compaction_scn", get_compaction_scn());
    }
  } else {
    LOG_WARN("fail to switch tenant", K(ret), K_(tenant_id));
  }
  return ret;
}

int ObLSMergeProgressChecker::check_progress()
{
  int ret = OB_SUCCESS;
  int64_t last_epoch_check_us = 0;
  common::ObSEArray<ObLSCompactionStatusInfo, 4> ls_infos;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSMergeProgressChecker not init", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(expected_epoch_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected epoch", K(ret), K(expected_epoch_));
  } else if (OB_FAIL(ObMajorFreezeUtil::check_epoch_periodically(*sql_proxy_, tenant_id_, expected_epoch_, last_epoch_check_us))) {
    LOG_WARN("failed to check freeze service epoch", KR(ret), K(tenant_id_), K(expected_epoch_));
  } else if (OB_FAIL(check_exist_ckm_error())) {
    LOG_WARN("failed to check exist checksum error", KR(ret), K(tenant_id_), K(expected_epoch_));
  } else if (OB_FAIL(refresh_ls_infos())) {
    LOG_WARN("failed to refresh ls ids", K(ret));
  } else if (FALSE_IT(merge_progress_.reset())) {
  } else if (FALSE_IT(merge_progress_.ls_total_cnt_ = ls_ids_.count())) {
  } else if (OB_FAIL(ls_infos.reserve(merge_progress_.ls_total_cnt_))) {
    LOG_WARN("failed to reserve ls status infos", K(ret), K(tenant_id_), K(ls_ids_));
  } else if (OB_FAIL(ObLSCompactionStatusObjLoader::load_ls_compaction_status_array(tenant_id_, ls_ids_, ls_infos))) {
    LOG_WARN("failed to load ls compaction status", K(ret), K(tenant_id_), K(ls_ids_));
  } else if (merge_progress_.ls_total_cnt_ != ls_infos.count()) {
    LOG_INFO("wait ls leader write ls obj, check later", K(merge_progress_), K(ls_infos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_infos.count(); ++i) {
      const ObLSCompactionStatusInfo &ls_info = ls_infos.at(i);

      // no need to check ls_info.refreshed_scn_, which will be updated after finishing RS validation
      if (OB_UNLIKELY(!ls_info.is_valid()
                   || ls_info.compaction_scn_ > get_compaction_scn_val()
                   || ObLSCompactionState::COMPACTION_STATE_MAX == ls_info.state_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected invalid ls status info", K(ret), "compaction_scn", get_compaction_scn(), K(ls_info));
      } else if (ls_info.compaction_scn_ < get_compaction_scn_val()) {
        // ls is merging
        ++merge_progress_.ls_merging_cnt_;
      } else if (ObLSCompactionState::COMPACTION_STATE_IDLE == ls_info.state_) {
        ++merge_progress_.ls_refreshed_cnt_;
      } else if (ObLSCompactionState::COMPACTION_STATE_RS_VERIFY == ls_info.state_) {
        ++merge_progress_.ls_verified_cnt_;
      } else if (ObLSCompactionState::COMPACTION_STATE_REFRESH == ls_info.state_) {
        // ls is refreshing, wait for ls state to update IDLE
        ++merge_progress_.ls_refreshing_cnt_;
      } else {
        ++merge_progress_.ls_merging_cnt_;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (merge_progress_.is_merge_finished()) {
      LOG_INFO("[SS_MERGE] RS check ls merge finished", K(tenant_id_), "compaction_scn", get_compaction_scn(),
          K(merge_progress_), K(ls_ids_), K(ls_infos));
    } else if (!merge_progress_.is_verify_finished()) {
      LOG_INFO("[SS_MERGE] ls index checksum validation not finished", K(tenant_id_), "compaction_scn", get_compaction_scn(),
          K(merge_progress_), K(ls_ids_), K(ls_infos));
    } else if (OB_FAIL(verify_special_table())) {
      LOG_WARN("failed to verify special table", K(ret), K(tenant_id_), "compaction_scn", get_compaction_scn(), K(ls_ids_), K(merge_progress_), K(ls_infos));
    } else {
      FLOG_INFO("[SS_MERGE] RS finish the validation of special table", K(ret), K(ls_infos));
      // update ls state
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_infos.count(); ++i) {
        if (ObLSCompactionState::COMPACTION_STATE_IDLE == ls_infos.at(i).state_
          && get_compaction_scn_val() == ls_infos.at(i).refreshed_scn_) {
          LOG_INFO("skip IDLE ls, no need to update", KR(ret), "ls_info", ls_infos.at(i), "compaction_scn", get_compaction_scn());
        } else if (ObLSCompactionState::COMPACTION_STATE_RS_VERIFY == ls_infos.at(i).state_) {
          ls_infos.at(i).rs_update_state_ = true;
          ls_infos.at(i).state_ = ObLSCompactionState::COMPACTION_STATE_REFRESH;
        }
      }

      if (FAILEDx(ObLSCompactionStatusObjLoader::batch_update_ls_compaction_status(tenant_id_, ls_infos))) {
        LOG_WARN("failed to batch write ls compaction status", K(ret));
      } else {
        FLOG_INFO("[SS_MERGE] RS advance the ls state to REFRESH in shared storage", K(ret));
      }
    }
  }
  return ret;
}

int ObLSMergeProgressChecker::verify_special_table()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  compaction::ObLSVerifyCkmType verify_type = ObLSVerifyCkmType::VERIFY_INVALID;

  if (OB_FAIL(ObTabletChecksumOperator::is_first_tablet_in_sys_ls_exist(*sql_proxy_,
                                                                        tenant_id_,
                                                                        get_compaction_scn(),
                                                                        is_exist))) {
    LOG_WARN("failed to check first tablet is exist", K(ret), K(tenant_id_), "compaction_scn", get_compaction_scn());
  } else {
    verify_type = (is_exist || !is_primary_service_)
                ? ObLSVerifyCkmType::VERIFY_CROSS_CLUSTER_CKM
                : ObLSVerifyCkmType::VERIFY_INDEX_CKM;
    FLOG_INFO("[SS_MERGE] RS start to verify special table",
        K(tenant_id_), "compaction_scn", get_compaction_scn(), K(is_exist), K(is_primary_service_), K(verify_type));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_ls_pair_cache_.try_refresh(true))) {
    LOG_WARN("failed to refresh tablet ls pair cache", K(ret));
  } else {
    LSIDSet placeholder; // useless for verifying special table
    SMART_VAR(ObLSChecksumValidator, ckm_validator, tenant_id_, placeholder, tablet_ls_pair_cache_, *sql_proxy_) {
      if (OB_FAIL(ckm_validator.set_basic_info(freeze_info_, verify_type, schema_service_))) {
        LOG_WARN("failed to set basic info for validator", K(ret), K(tenant_id_),
          "compaction_scn", get_compaction_scn(), K(verify_type));
      } else if (OB_FAIL(ckm_validator.validate_special_table())) {
        LOG_WARN("failed to validate special table", K(ret), K(tenant_id_),
          "compaction_scn", get_compaction_scn(), K(verify_type));
      }
    }
  }
  return ret;
}


} // rootserver
} // oceanbase
