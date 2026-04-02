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
#include "storage/compaction/ob_tablet_refresh_dag.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/compaction/ob_refresh_tablet_util.h"
#include "storage/compaction/ob_merge_ctx_func.h"
#include "close_modules/shared_storage/storage/compaction/ob_tenant_ls_merge_scheduler.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;

namespace compaction
{

ObTabletsRefreshSSTableParam::ObTabletsRefreshSSTableParam()
  : compaction_scn_(0),
    ls_id_(),
    tablet_id_()
{
}

ObTabletsRefreshSSTableParam::ObTabletsRefreshSSTableParam(
  const share::ObLSID &ls_id,
  const int64_t compaction_scn)
  : compaction_scn_(compaction_scn),
    ls_id_(ls_id),
    tablet_id_()
{
}

int ObTabletsRefreshSSTableParam::assign(
    const ObTabletsRefreshSSTableParam &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // do nothing
  } else {
    ls_id_ = other.ls_id_;
    compaction_scn_ = other.compaction_scn_;
    tablet_id_ = other.tablet_id_;
  }
  return ret;
}

bool ObTabletsRefreshSSTableParam::operator == (
    const ObTabletsRefreshSSTableParam &other) const
{
  bool is_same = true;
  if (this == &other) {
    // do nothing
  } else if ((ls_id_ != other.ls_id_) || (tablet_id_ != other.tablet_id_)) {
    is_same = false;
  }
  return is_same;
}

int64_t ObTabletsRefreshSSTableParam::get_hash() const
{
  int64_t hash_val = 0;

  hash_val = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);

  return hash_val;
}


ObTabletsRefreshSSTableDag::ObTabletsRefreshSSTableDag()
  : ObIDag(share::ObDagType::DAG_TYPE_REFRESH_SSTABLES),
    is_inited_(false),
    arena_("RefreshSSTDag", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::MERGE_NORMAL_CTX_ID),
    param_()
{
}

ObTabletsRefreshSSTableDag::~ObTabletsRefreshSSTableDag()
{
}

int ObTabletsRefreshSSTableDag::init_by_param(
    const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTabletsRefreshSSTableParam *refresh_param = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletsRefreshSSTableDag has been inited", K(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == param || !param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(param));
  } else if (FALSE_IT(refresh_param = static_cast<const ObTabletsRefreshSSTableParam *>(param))) {
  } else if (OB_FAIL(param_.assign(*refresh_param))) {
    LOG_WARN("failed to assign cache param", K(ret), KPC(refresh_param));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTabletsRefreshSSTableDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletsRefreshSSTableTask *task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletsRefreshSSTableDag has not inited", K(ret));
  } else if (OB_FAIL(create_task(nullptr/*parent*/, task))) {
    LOG_WARN("failed to create cache sstable task", K(ret));
  }
  return ret;
}

bool ObTabletsRefreshSSTableDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else if (param_ != static_cast<const ObTabletsRefreshSSTableDag &>(other).param_) {
    is_same = false;
  }
  return is_same;
}

int64_t ObTabletsRefreshSSTableDag::hash() const
{
  return param_.get_hash();
}

int ObTabletsRefreshSSTableDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablets cache sstable dag not inited", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  param_.ls_id_.id(),
                                  static_cast<int64_t>(param_.tablet_id_.id()),
                                  param_.compaction_scn_))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObTabletsRefreshSSTableDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%ld tablet_id=%ld", param_.ls_id_.id(), param_.tablet_id_.id()))) {
    LOG_WARN("failed to fill dag key", K(ret), K(param_));
  }
  return ret;
}


ObTabletsRefreshSSTableTask::ObTabletsRefreshSSTableTask()
  : ObITask(ObITask::TASK_TYPE_REFRESH_SSTABLES),
    is_inited_(false),
    compaction_scn_(0),
    ls_id_(),
    base_dag_(nullptr)
{
}

ObTabletsRefreshSSTableTask::~ObTabletsRefreshSSTableTask()
{
}

int ObTabletsRefreshSSTableTask::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTbletsCacheSSTableTask init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null dag", K(ret));
  } else if (OB_UNLIKELY(ObDagType::ObDagTypeEnum::DAG_TYPE_REFRESH_SSTABLES != dag_->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dag type", K(ret));
  } else if (FALSE_IT(base_dag_ = static_cast<ObTabletsRefreshSSTableDag *>(dag_))) {
  } else if (OB_UNLIKELY(!base_dag_->get_param().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected not valid param", K(ret), K(base_dag_->get_param()));
  } else {
    ls_id_ = base_dag_->get_param().ls_id_;
    compaction_scn_ = base_dag_->get_param().compaction_scn_;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletsRefreshSSTableTask::process()
{
  int ret = OB_SUCCESS;
  const ObTabletsRefreshSSTableParam &param = base_dag_->get_param();
  ObLSHandle ls_handle;
  ObBasicObjHandle<ObLSObj> ls_obj_hdl;
  const ObTabletID &tablet_id = param.tablet_id_;
  ObTabletHandle tablet_handle;
  bool update_flag = false;
  int64_t last_major_snapshot = 0;
  int64_t refresh_scn = compaction_scn_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE;
  ObDownloadTabletMetaParam download_tablet_meta_param(refresh_scn/*snapshot_version*/,
                                                       false/*allow_dup_major*/,
                                                       false/*init_major_ckm_info*/,
                                                       true/*need_prewarm*/);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletsRefreshSSTableTask not init", K(ret));
  } else if (!ObBasicMergeScheduler::get_merge_scheduler()->could_major_merge_start()) {
    // do nothing
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_id_, ls_obj_hdl))) {
    LOG_WARN("failed to get ls obj handle", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
          tablet_id,
          tablet_handle,
          0 /*timeout_us*/,
          storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet handle", K(ret), K(param));
    } else {
      ret = OB_SUCCESS;
      update_flag = true;
      LOG_INFO("tablet not exist, just update tablet state", K(param));
    }
  } else if (FALSE_IT(last_major_snapshot = tablet_handle.get_obj()->get_last_major_snapshot_version())) {
  } else if (last_major_snapshot >= compaction_scn_) {
    // tablet has corresponding major sstable or state is too old
    update_flag = true;
    LOG_INFO("tablet has been refreshed, just update tablet state", K(param));
  } else if (last_major_snapshot < MERGE_SCHEDULER_PTR->get_inner_table_merged_scn()
      && OB_FAIL(decide_refreshed_scn_by_meta_list(tablet_id, last_major_snapshot, refresh_scn))) {
    // should read meta list to decide refreshed_scn
    LOG_WARN("failed to decide refreshed scn by meta list obj", KR(ret), K(tablet_id), K(last_major_snapshot));
  } else if (refresh_scn <= last_major_snapshot) {
    // do nothing
    update_flag = true;
  } else if (OB_UNLIKELY(!ObLSStatusCache::check_weak_read_ts_ready(refresh_scn, *ls_handle.get_ls()))) {
    ret = OB_EAGAIN;
    LOG_INFO("ls weak read ts not ready, cannot schedule merge", K_(compaction_scn), K_(ls_id));
  } else if (OB_FAIL(MTL(ObTenantLSMergeScheduler *)->get_tablet_merge_reason(refresh_scn, ls_id_, *tablet_handle.get_obj(), merge_reason))) {
    LOG_WARN("failed to get tablet merge reason", K(ret), K(refresh_scn), K_(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(tablet_handle.get_obj()->get_snapshot_version() < refresh_scn
                      && ObAdaptiveMergePolicy::NO_INC_DATA != merge_reason
                      && ObAdaptiveMergePolicy::DURING_DDL != merge_reason)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet snapshot is unexpected smaller than compaction scn, should not refresh", K(ret), K(param),
             "tablet snapshot", tablet_handle.get_obj()->get_snapshot_version());
  } else if (ObAdaptiveMergePolicy::DURING_DDL == merge_reason) {
    update_flag = true; // it's OK to set either skip scn or refreshed scn to tablet state
  } else if (FALSE_IT(download_tablet_meta_param.snapshot_version_ = refresh_scn)) {
  } else if (OB_FAIL(ObRefreshTabletUtil::download_major_compaction_tablet_meta(*ls_handle.get_ls(), tablet_id, download_tablet_meta_param))) {
    LOG_WARN("failed to download tablet meta", K(ret), K(param), K(download_tablet_meta_param));
  } else {
    FLOG_INFO("success to refresh tablet", K(param), K(download_tablet_meta_param));

    ObTabletCompactionState tablet_state;
    if (OB_FAIL(ls_obj_hdl.get_obj()->cur_svr_ls_compaction_status_.get_tablet_state(tablet_id, tablet_state))) {
      LOG_WARN("failed to get tablet state on cur server", K(ret), K(ls_id_), K(tablet_id));
    } else if (OB_UNLIKELY(compaction_scn_ < tablet_state.get_refreshed_scn())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected tablet state", K(ret), K(tablet_id), K_(compaction_scn), K(tablet_state));
    } else {
      update_flag = true;
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret) || !update_flag) {
    // do nothing
  } else {
    ObTabletCompactionState tmp_state;
    tmp_state.set_refreshed_scn(refresh_scn);
    if (OB_TMP_FAIL(ObMergeCtxFunc::update_tablet_state(ls_id_, tablet_id, tmp_state))) {
      ADD_SUSPECT_INFO(MAJOR_MERGE,
                       ObDiagnoseTabletType::TYPE_REPORT,
                       ls_id_,
                       tablet_id,
                       ObSuspectInfoType::SUSPECT_COMPACTION_REPORT_ADD_FAILED,
                       tmp_ret);
    }
  }
  LOG_INFO("Tablet refresh finish", K(ret), K(tmp_ret), K(param), K(update_flag));

  return ret;
}

int ObTabletsRefreshSSTableTask::decide_refreshed_scn_by_meta_list(
  const ObTabletID &tablet_id,
  const int64_t last_major_snapshot,
  int64_t &refresh_scn)
{
  int ret = OB_SUCCESS;
  int64_t latest_upload_major_scn = 0;
  if (OB_FAIL(ObMergeCtxFunc::get_latest_upload_major_scn(tablet_id, latest_upload_major_scn))) {
    LOG_WARN("failed to get latest upload major scn", KR(ret), K(tablet_id));
  } else if (latest_upload_major_scn > last_major_snapshot) {
    refresh_scn = latest_upload_major_scn;
    // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
    LOG_INFO("refresh scn from meta_list in IDLE state", KR(ret), K(tablet_id), K(refresh_scn));
  } else {
    refresh_scn = 0;
  }
  if (OB_UNLIKELY(refresh_scn > compaction_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("refresh scn should not larger than compaction_scn", KR(ret), K(refresh_scn), K_(compaction_scn));
  }
  return ret;
}

} // compaction
} // oceanbase
