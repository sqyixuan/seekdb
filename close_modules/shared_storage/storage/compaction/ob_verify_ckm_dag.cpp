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
#include "storage/compaction/ob_verify_ckm_dag.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_merge_ctx_func.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/compaction/ob_tenant_ls_merge_checker.h"

namespace oceanbase
{
using namespace share;
using namespace blocksstable;
namespace compaction
{
ERRSIM_POINT_DEF(EN_VERIFY_CKM_DAG_FAILED);
/**
 * -------------------------------------------------------------------ObVerifyCkmTask-------------------------------------------------------------------
 */
ObVerifyCkmTask::ObVerifyCkmTask()
  : ObBatchExecTask(ObITask::TASK_TYPE_VERIFY_CKM)
{
}

bool ObVerifyCkmTask::need_batch_loop(
    const int64_t start_idx,
    const int64_t end_idx) const
{
  return end_idx - start_idx >= get_batch_size();
}

// [start_idx, end_idx)
int ObVerifyCkmTask::get_replica_ckms(
    const ObVerifyCkmParam &param,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null sql proxy", KR(ret));
  } else if (OB_UNLIKELY(start_idx < 0 || end_idx < 0 || start_idx >= end_idx || end_idx > param.tablet_info_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(start_idx), K(end_idx), K(param));
  } else if (need_batch_loop(start_idx, end_idx)) {
    if (OB_FAIL(ObTabletReplicaChecksumOperator::range_get(MTL_ID(),
                                                           param.tablet_info_array_.at(start_idx),
                                                           param.tablet_info_array_.at(end_idx - 1),
                                                           param.compaction_scn_,
                                                           *GCTX.sql_proxy_,
                                                           items))) {
      LOG_WARN("fail to range get by operator", KR(ret), "start_tablet_id", param.tablet_info_array_.at(get_start_idx()),
        "end_tablet_id", param.tablet_info_array_.at(end_idx));
    }
  } else if (OB_FAIL(ObTabletReplicaChecksumOperator::multi_get(MTL_ID(),
                                                                param.tablet_info_array_,
                                                                param.compaction_scn_,
                                                                *GCTX.sql_proxy_,
                                                                items))) {
    LOG_WARN("failed to get replica checksum", KR(ret), K(param));
  }
  return ret;
}

int ObVerifyCkmTask::get_tablet_and_verify(
  storage::ObLS &ls,
  const ObTabletID &tablet_id,
  const int64_t compaction_scn,
  const ObTabletReplicaChecksumItem &ckm_item)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  const ObTabletTableStore *table_store = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  #define PRINT_WARN_LOG(str) LOG_WARN(str, K(ret), K(ls_id), K(tablet_id), K(compaction_scn))
  if (!ObBasicMergeScheduler::get_merge_scheduler()->could_major_merge_start()) {
    // merge is suspended
  } else if (OB_FAIL(ls.get_tablet_svr()->get_tablet(
                 tablet_id, tablet_handle, 0 /*timeout_us*/,
                 storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      PRINT_WARN_LOG("failed to get tablet");
    }
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    PRINT_WARN_LOG("failed to fetch table store");
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    PRINT_WARN_LOG("failed to get table store");
  } else if (table_store->get_major_ckm_info().is_empty()) {
    ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type;
    if (OB_FAIL(ObTenantTabletScheduler::get_co_merge_type_for_compaction(compaction_scn, *tablet, co_major_merge_type))) {
      LOG_WARN("fail to get co merge type from medium info", K(ret), K(compaction_scn), KPC(tablet));
    } else if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_merge_dag(ls.get_ls_id(), 
                                                                       *tablet, 
                                                                       MEDIUM_MERGE, 
                                                                       compaction_scn, 
                                                                       EXEC_MODE_CALC_CKM,
                                                                       nullptr /*dag_net_id*/,
                                                                       co_major_merge_type))) {
      if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to schedule medium merge dag", K(ret), K(ls_id),
                 K(tablet_id));
      }
    } else {
      LOG_INFO("success to schedule tablet validate dag", KR(ret), K(ls_id), K(tablet_id), K(compaction_scn));
    }
  } else if (OB_FAIL(ObMergeCtxFunc::validate_sstable_ckm_info(
      ls_id, tablet_id, compaction_scn, table_store->get_major_ckm_info(), ckm_item))) {
    PRINT_WARN_LOG("failed to validate ckm");
  } else {
    LOG_TRACE("success to validate sstable ckm", KR(ret), K(ls_id), K(tablet_id), K(compaction_scn));
  }
  #undef PRINT_WARN_LOG
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_VERIFY_CKM_DAG_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "ERRSIM EN_VERIFY_CKM_DAG_FAILED", K(ret));
    }
  }
#endif
  return ret;
}

int ObVerifyCkmTask::inner_process()
{
  int ret = OB_SUCCESS;
  const ObVerifyCkmParam &param = base_dag_->get_param();
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObLSHandle ls_handle;
  ObBasicObjHandle<ObLSObj> ls_obj_hdl;

  if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", KR(ret), K(param));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ls", K(param));
  } else if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(param.ls_id_, ls_obj_hdl))) {
    LOG_WARN("failed to get ls obj handle", KR(ret), K(param));
  } else {
    const int64_t start_idx = get_start_idx();
    const int64_t end_idx = MIN(param.tablet_info_array_.count(), get_end_idx());
    ObArray<ObTabletReplicaChecksumItem> items;
    items.set_attr(ObMemAttr(MTL_ID(), "VerifyTask", ObCtxIds::MERGE_NORMAL_CTX_ID));

    if (OB_FAIL(items.reserve(end_idx - start_idx))) {
      LOG_WARN("failed to reserve array", KR(ret), "cnt", end_idx - start_idx);
    } else if (OB_FAIL(get_replica_ckms(param, start_idx, end_idx, items))) {
      LOG_WARN("failed to replica ckm", KR(ret), K(param));
    } else if (items.empty()) {
      // truncated tablet, update tablet state directly
      if (OB_FAIL(batch_update_tablet_state(param, start_idx, end_idx, *ls_obj_hdl.get_obj()))) {
        LOG_WARN("failed to batch update tablet state", K(ret), K(start_idx), K(end_idx));
      }
    } else if (OB_FAIL(loop_validate(*ls_handle.get_ls(), *ls_obj_hdl.get_obj(), items))) {
      LOG_WARN("failed to validate ckm", KR(ret), K(param));
    } else {
      LOG_INFO("verify ckm finished", KR(ret), K(param), K(start_idx), K(end_idx),
               "cost_ts", ObTimeUtility::fast_current_time() - cost_ts);
    }
  }
  return ret;
}

int ObVerifyCkmTask::batch_update_tablet_state(
    const ObVerifyCkmParam &param,
    const int64_t start_idx,
    const int64_t end_idx,
    compaction::ObLSObj &ls_obj)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletCompactionState tmp_state;
  tmp_state.set_calc_ckm_scn(param.compaction_scn_);
  tmp_state.set_verified_scn(param.compaction_scn_);
  int64_t left_idx = start_idx;
  int64_t right_idx = end_idx;

  if (OB_UNLIKELY(start_idx < 0 || end_idx < 0 || start_idx >= end_idx || end_idx > param.tablet_info_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tmp_state), K(param));
  } else if (!need_batch_loop(start_idx, end_idx)) {
    left_idx = 0;
    right_idx = param.tablet_info_array_.count();
  }

  for (int64_t idx = left_idx; idx < right_idx; ++idx) {
    const ObTabletID &tablet_id = param.tablet_info_array_.at(idx);
    if (OB_TMP_FAIL(ls_obj.cur_svr_ls_compaction_status_.update_tablet_state(
                          tablet_id, tmp_state))) {
      LOG_WARN("failed to update tablet state on cur server", KR(tmp_ret), K(tablet_id), K(param));
    }
  }
  return ret;
}

int ObVerifyCkmTask::loop_validate(
  ObLS &ls,
  ObLSObj &ls_obj,
  const ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t ckm_idx = 0;
  int64_t i = get_start_idx();
  const ObVerifyCkmParam &param = base_dag_->get_param();
  const int64_t end_idx = MIN(param.tablet_info_array_.count(), get_end_idx());
  ObTabletCompactionState tmp_state;
  tmp_state.set_calc_ckm_scn(param.compaction_scn_);
  tmp_state.set_verified_scn(param.compaction_scn_);
  while (OB_SUCC(ret) && i < end_idx && ckm_idx < items.count()) {
    const ObTabletID &tablet_id = param.tablet_info_array_.at(i);
    const ObTabletReplicaChecksumItem &item = items.at(ckm_idx);
    if (tablet_id == item.tablet_id_) {
      if (OB_TMP_FAIL(get_tablet_and_verify(ls, tablet_id, param.compaction_scn_, item))) {
        cnt_.failure_cnt_++;
      } else if (OB_TMP_FAIL(ls_obj.cur_svr_ls_compaction_status_.update_tablet_state(tablet_id, tmp_state))) {
        LOG_WARN("failed to update tablet state on cur server", KR(tmp_ret), K(param));
        cnt_.failure_cnt_++;
      } else {
        cnt_.success_cnt_++;
      }
      ++i;
      ++ckm_idx;
    } else if (tablet_id < item.tablet_id_) {
      ++i;
    } else {
      ++ckm_idx;
    }
    if (OB_CHECKSUM_ERROR == tmp_ret) {
      ret = OB_CHECKSUM_ERROR;
      MTL(ObTenantLSMergeChecker *)->set_merge_error();
      LOG_ERROR("Found Replica Checksum ERROR!", K(ret), K(tablet_id), "ls_id", ls.get_ls_id(), K(item));
    } else if (OB_FAIL(share::dag_yield())) {
      LOG_WARN("failed to dag yield", KR(ret));
    }
  } // while
  LOG_INFO("ObVerifyCkmTask finish verify", KR(ret), K_(cnt));
  return ret;
}

} // namespace compaction
} // namespace oceanbase
