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
#ifndef OB_SHARE_STORAGE_STORAGE_COMPACTION_MERGE_CTX_FUNC_H_
#define OB_SHARE_STORAGE_STORAGE_COMPACTION_MERGE_CTX_FUNC_H_
#include "storage/ob_i_table.h"
#include "storage/compaction/ob_tablet_compaction_status.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_storage_struct.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/compaction/ob_ls_compaction_status.h"
namespace oceanbase
{
namespace share
{
struct ObTabletReplicaChecksumItem;
}
namespace blocksstable
{
class ObMajorChecksumInfo;
}
namespace compaction
{
class ObMajorTaskCheckpointMgr;
struct ObTabletCompactionState;
struct ObNewMicroInfo;
struct ObMediumCompactionInfo;

struct ObMergeCtxFunc final
{
public:
  static int get_old_root_macro_seq(
    const storage::ObTablesHandleArray &tables_handle,
    int64_t &root_macro_seq);
  static int init_major_task_checkpoint_mgr(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t parallel_cnt,
    const int64_t cg_cnt,
    const storage::ObTablesHandleArray &tables_handle,
    const ObExecMode exec_mode,
    storage::ObLS &ls,
    ObIAllocator &allocator,
    ObMajorTaskCheckpointMgr &task_ckp_mgr);
  static int validate_sstable_ckm_info(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const blocksstable::ObMajorChecksumInfo &major_ckm_info,
    const share::ObTabletReplicaChecksumItem &item);
  static int update_tablet_state(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObTabletCompactionState &tmp_state,
    const compaction::ObNewMicroInfo *input_new_micro_info = NULL);
  static int write_obj_with_retry(
    ObCompactionObjInterface &obj,
    const bool force_write);
  template<typename CTX>
  static int upload_tablet_and_write_major_ckm_info(
    CTX &ctx,
    const blocksstable::ObSSTable &new_sstable,
    storage::ObTabletHandle &new_tablet_handle);
  static int check_medium_info(
    storage::ObLS &ls,
    const compaction::ObMediumCompactionInfo &next_medium_info,
    const int64_t last_major_snapshot,
    const ObTabletID &tablet_id);
  static int check_major_tablet_exist(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t ls_epoch,
    bool &is_exist);
  static int get_latest_upload_major_scn(const ObTabletID &tablet_id, int64_t &latest_upload_major_scn);
private:
  static const int64_t UPDATE_INNER_TABLE_MAX_RETRY_TIMES = 10;
  static int validate_sstable_ckm_info(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const blocksstable::ObMajorChecksumInfo &major_ckm_info,
    const share::ObTabletReplicaChecksumItem &replica_ckm);

};

template<typename CTX>
int ObMergeCtxFunc::upload_tablet_and_write_major_ckm_info(
    CTX &ctx,
    const blocksstable::ObSSTable &new_sstable,
    storage::ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = ctx.get_tablet_id();
  const share::ObLSID &ls_id = ctx.get_ls_id();
  const int64_t compaction_scn = ctx.get_merge_version();
  storage::ObUpdateTableStoreParam param;
  int64_t macro_start_seq = 0;
#define LOG STORAGE_COMPACTION_LOG
  if (OB_FAIL(ctx.build_update_table_store_param(&new_sstable, param))) {
    LOG(WARN, "failed to build update table store param", KR(ret), K(param));
  } else if (OB_FAIL(ctx.get_macro_seq_by_stage(BUILD_TABLET_META, macro_start_seq))) {
    LOG(WARN, "failed to get macro seq", K(ret), K(ctx));
  } else if (OB_FAIL(ctx.get_ls()->upload_major_compaction_tablet_meta(tablet_id,
                                                                   param,
                                                                   macro_start_seq))) {
    LOG(WARN, "failed to upload compaction tablet meta", KR(ret), K(macro_start_seq));
  } else if (OB_FAIL(MTL(ObTenantStorageMetaService *)->update_shared_tablet_meta_list(
      tablet_id, compaction_scn))) {
    LOG(WARN, "failed to update shared tablet meta list", KR(ret));
  } else {
    // write major ckm info to tablet store
    if (OB_FAIL(ctx.build_update_table_store_param(nullptr/* new_sstable */, param))) {
      LOG(WARN, "failed to build update table store param", KR(ret), K(param));
    } else if (OB_FAIL(param.compaction_info_.major_ckm_info_.init_from_sstable(ctx.mem_ctx_.get_allocator(), ctx.get_exec_mode(), *param.storage_schema_, new_sstable))) {
      LOG(WARN, "failed to init major ckm info from sstable", KR(ret), K(new_sstable));
    } else if (OB_FAIL(ctx.get_ls()->update_tablet_table_store(tablet_id, param, new_tablet_handle))) {
      LOG(WARN, "failed to update tablet table store", K(ret), K(param), K(new_tablet_handle));
    } else {
      ObTabletCompactionState tmp_state;
      tmp_state.set_output_scn(compaction_scn);
      int tmp_ret = OB_SUCCESS;
      (void) ObMergeCtxFunc::update_tablet_state(ls_id, tablet_id, tmp_state, &ctx.get_merge_history().get_new_micro_info());
      if (OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id, true/*need_diagnose*/))) {
        LOG(WARN, "failed to submit tablet update task to report", K(tmp_ret), K(ls_id), K(tablet_id));
      } else if (OB_TMP_FAIL(ctx.get_ls()->get_tablet_svr()->update_tablet_report_status(tablet_id))) {
        LOG(WARN, "failed to update tablet report status", K(tmp_ret), K(ls_id), K(tablet_id));
      }
    }
  }
#undef LOG
  return ret;
}

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_STORAGE_COMPACTION_MERGE_CTX_FUNC_H_
