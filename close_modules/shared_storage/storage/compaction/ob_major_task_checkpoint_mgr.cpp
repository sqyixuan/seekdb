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
#include "storage/compaction/ob_major_task_checkpoint_mgr.h"
#include "storage/compaction/ob_merge_ctx_func.h"
namespace oceanbase
{
using namespace share;
using namespace common;
using namespace blocksstable;
namespace compaction
{

int ObSimpleTaskCheckpointMgr::calc_macro_seq(
    const int64_t calc_task_idx,
    const int64_t root_macro_seq,
    int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  macro_start_seq = root_macro_seq + calc_task_idx * MACRO_STEP_SIZE;
  if (OB_UNLIKELY(macro_start_seq >= INT64_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("macro start seq is size overflow", K(ret), K(calc_task_idx), K(root_macro_seq));
  }
  return ret;
}

int ObSimpleTaskCheckpointMgr::get_all_macro_start_seq(
    const ObTabletID &tablet_id,
    const int64_t parallel_cnt,
    const int64_t column_group_cnt,
    const int64_t last_major_root_macro_seq,
    ObIArray<blocksstable::MacroBlockId> &seq_id_array)
{
  int ret = OB_SUCCESS;
  int64_t macro_start_seq = 0;
  ObStorageObjectOpt data_opt;
  ObStorageObjectOpt meta_opt;
  MacroBlockId block_id;
  if (OB_UNLIKELY(!tablet_id.is_valid() || parallel_cnt < 1 || column_group_cnt < 1
    || last_major_root_macro_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(parallel_cnt), K(column_group_cnt),
      K(last_major_root_macro_seq));
  } else if (OB_FAIL(calc_macro_seq(0, last_major_root_macro_seq, macro_start_seq))) {
    LOG_WARN("failed to cal seq", KR(ret), K(macro_start_seq));
  }

#define PUSH_BLOCK_ID_AND_INC(opt)                                             \
  if (FAILEDx(ObObjectManager::ss_get_object_id(opt, block_id))) {             \
    LOG_WARN("failed to generate obj id", KR(ret), K(opt));                    \
  } else if (OB_FAIL(seq_id_array.push_back(block_id))) {                      \
    LOG_WARN("failed to push macro seq", KR(ret), K(block_id));                \
  } else {                                                                     \
    opt.ss_share_opt_.data_seq_ += MACRO_STEP_SIZE;                            \
  }
  for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < column_group_cnt; ++cg_idx) {
    data_opt.set_ss_share_data_macro_object_opt(tablet_id.id(), macro_start_seq, cg_idx);
    meta_opt.set_ss_share_meta_macro_object_opt(tablet_id.id(), macro_start_seq, cg_idx);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < parallel_cnt; ++idx) {
      PUSH_BLOCK_ID_AND_INC(data_opt);
      PUSH_BLOCK_ID_AND_INC(meta_opt);
    } // for

    for (int64_t stage = 0; OB_SUCC(ret) && stage < GET_NEW_ROOT_MACRO_SEQ; ++stage) {
      PUSH_BLOCK_ID_AND_INC(meta_opt);
    } // for
  } // for
#undef PUSH_BLOCK_ID_AND_INC
  return ret;
}

ObMajorTaskCheckpointMgr::ObMajorTaskCheckpointMgr()
  : lock_(),
    finish_flags_(NULL),
    task_cnt_(0),
    max_continue_finish_idx_(INVALID_TASK_IDX),
    root_macro_seq_(0),
    exec_mode_(EXEC_MODE_MAX),
    is_inited_(false)
{}

ObMajorTaskCheckpointMgr::~ObMajorTaskCheckpointMgr()
{
  if (is_inited_ && nullptr != finish_flags_) {
    LOG_ERROR_RET(OB_STATE_NOT_MATCH, "not call destroy with allocator, may have memory leak");
  }
}

int ObMajorTaskCheckpointMgr::init(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t parallel_cnt,
    const int64_t cg_cnt, // unused for row store
    const int64_t root_macro_seq_on_sstable,
    const ObExecMode exec_mode,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != cg_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(parallel_cnt), K(cg_cnt));
  } else if (OB_FAIL(inner_init(tablet_id, compaction_scn, parallel_cnt, root_macro_seq_on_sstable, exec_mode, allocator))) {
    LOG_WARN("failed to init", KR(ret), K(tablet_id), K(compaction_scn), K(cg_cnt), K(root_macro_seq_on_sstable));
  }
  return ret;
}

int ObMajorTaskCheckpointMgr::inner_init(
    const ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t task_cnt,
    const int64_t root_macro_seq_on_sstable,
    const ObExecMode exec_mode,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tablet_id), KP(finish_flags_), K_(task_cnt));
  } else if (OB_UNLIKELY(task_cnt <= 0 || !tablet_id.is_valid() || compaction_scn <= 0 || !is_valid_exec_mode(exec_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_cnt), K(tablet_id), K(compaction_scn), K(exec_mode));
  } else if (OB_FAIL(init_start_task_idx(tablet_id, compaction_scn, task_cnt, root_macro_seq_on_sstable))) {
    // will init max_continue_finish_idx_ & seq
    LOG_WARN("failed to init start task idx", KR(ret), K(tablet_id), K(task_cnt));
  } else if (OB_ISNULL(finish_flags_ = (bool *)(allocator.alloc(sizeof(bool) * task_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", KR(ret), K(task_cnt));
  } else {
    MEMSET(finish_flags_, false, task_cnt * sizeof(bool));
    if (max_continue_finish_idx_ >= 0) {
      MEMSET(finish_flags_, true, (max_continue_finish_idx_ + 1) * sizeof(bool));
    }
  }

  if (OB_SUCC(ret)) {
    task_cnt_ = task_cnt;
    exec_mode_ = exec_mode;
    is_inited_ = true;
  }
  return ret;
}

void ObMajorTaskCheckpointMgr::destroy(ObIAllocator &allocator)
{
  if (IS_INIT) {
    is_inited_ = false;
    task_cnt_ = 0;
    max_continue_finish_idx_ = INVALID_TASK_IDX;
    root_macro_seq_ = 0;
    if (OB_NOT_NULL(finish_flags_)) {
      allocator.free(finish_flags_);
      finish_flags_ = NULL;
    }
  }
}

int ObMajorTaskCheckpointMgr::mark_task_finish(
    const ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t finish_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMajorTaskCheckpointMgr not inited", K(ret), KP(finish_flags_), K_(task_cnt));
  } else if (OB_UNLIKELY(finish_idx >= task_cnt_ || finish_idx < 0
      || !tablet_id.is_valid() || compaction_scn <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(finish_idx), K(tablet_id), K(compaction_scn), KPC(this));
  } else {
    lib::ObMutexGuard guard(lock_);
    finish_flags_[finish_idx] = true;
    if (finish_idx == max_continue_finish_idx_ + 1) {
      // update finish idx only when next idx finish
      update_continue_finish_idx_();

      if (task_cnt_ > 1 && OB_FAIL(upload_tablet_compaction_status_(tablet_id, compaction_scn))) {
        LOG_WARN("failed to update seq to inner table", KR(ret), K(tablet_id), K(compaction_scn));
      }
    }
  }
  return ret;
}

int ObMajorTaskCheckpointMgr::upload_tablet_compaction_status_(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn) const
{
  int ret = OB_SUCCESS;
  if (!is_output_exec_mode(exec_mode_)) {
    LOG_INFO("skip upload tablet status for cur exec mode", KR(ret), "exec_mode", exec_mode_to_str(exec_mode_));
  } else {
    ObTabletCompactionStatus status_info(tablet_id, compaction_scn, get_ckp_type());
    status_info.task_idx_ = max_continue_finish_idx_;
    status_info.root_macro_seq_ = root_macro_seq_;
    if (OB_FAIL(ObMergeCtxFunc::write_obj_with_retry(status_info, false/*force_write*/))) {
      LOG_WARN("failed to write obj", KR(ret), K(status_info));
    }
  }
  return ret;
}

int ObMajorTaskCheckpointMgr::init_start_task_idx(
    const ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t task_cnt,
    const int64_t root_macro_seq_on_sstable)
{
  int ret = OB_SUCCESS;
  ObTabletCompactionStatus status_info(tablet_id, compaction_scn, ObTabletCompactionStatus::CKM_TYPE_MAX);
  bool is_exist = false;
  if (OB_FAIL(status_info.check_exist(is_exist))) {
    LOG_WARN("failed to check exist tablet compaction seq", KR(ret), K(status_info));
  } else if (!is_exist) { // obj not exist, use macro seq on sstable
    status_info.root_macro_seq_ = root_macro_seq_on_sstable;
  } else {
    ObCompactionObjBuffer obj_buf;
    if (OB_FAIL(obj_buf.init())) {
      LOG_WARN("failed to init obj buf", KR(ret));
    } else if (OB_FAIL(status_info.read_object(obj_buf))) {
      LOG_WARN("failed to read obj", KR(ret), K(status_info));
    } else if (OB_UNLIKELY(status_info.task_idx_ >= task_cnt || status_info.ckp_type_ != get_ckp_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task idx is invalid", KR(ret), K(task_cnt), K(status_info));
    } else {
      max_continue_finish_idx_ = status_info.task_idx_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(INT64_MAX - status_info.root_macro_seq_
      <= (task_cnt - max_continue_finish_idx_ - 1) * MACRO_STEP_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro seq is overflow", KR(ret), K(status_info), KPC(this));
  } else {
    // root_macro_seq_ = status_info.root_macro_seq_;
    // TODO do not use TabletStatus obj now
    max_continue_finish_idx_ = INVALID_TASK_IDX;
    root_macro_seq_ = root_macro_seq_on_sstable;
    LOG_INFO("success to read tablet compaction seq", KR(ret), K(status_info), KPC(this),
      "use_checkpoint", max_continue_finish_idx_ > INVALID_TASK_IDX, K(root_macro_seq_on_sstable));
  }
  return ret;
}

void ObMajorTaskCheckpointMgr::update_continue_finish_idx_()
{
  int64_t idx = max_continue_finish_idx_ + 1;
  for (; idx < task_cnt_; ++idx) {
    if (!finish_flags_[idx]) {
      break;
    }
  }
  // idx = first unfinish idx or task_cnt_
  max_continue_finish_idx_ = idx - 1;
}

int ObMajorTaskCheckpointMgr::inner_get_macro_start_seq(
    const int64_t task_idx,
    const int64_t task_cnt,
    int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  macro_start_seq = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMajorTaskCheckpointMgr not inited", K(ret), KP(finish_flags_), K(task_cnt));
  } else if (OB_UNLIKELY(task_idx < 0 || task_idx >= task_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(task_idx), KPC(this));
  } else if (OB_SUCC(calc_macro_seq(task_idx, root_macro_seq_, macro_start_seq))) {
    LOG_INFO("success to get start seq", KR(ret), K(task_idx), K(macro_start_seq), KPC(this));
  }
  return ret;
}

int ObMajorTaskCheckpointMgr::inner_get_macro_seq_by_stage(
  const ObGetMacroSeqStage stage,
  const int64_t input_task_cnt,
  int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  macro_start_seq = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMajorTaskCheckpointMgr not inited", K(ret), KP(finish_flags_), K(input_task_cnt));
  } else if (OB_UNLIKELY(!is_valid_get_macro_seq_stage(stage))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(input_task_cnt), K(stage), KPC(this));
  } else if (OB_SUCC(calc_macro_seq(int64_t(stage) + input_task_cnt, root_macro_seq_, macro_start_seq))) {
    LOG_INFO("success to get start seq", KR(ret), K(input_task_cnt), K(stage), K(macro_start_seq), KPC(this));
  }
  return ret;
}

int ObCOMajorTaskCheckpointMgr::init(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t parallel_cnt,
    const int64_t cg_cnt,
    const int64_t root_macro_seq_on_sstable,
    const ObExecMode exec_mode,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(parallel_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parallel cnt", KR(ret), K(parallel_cnt));
  } else if (OB_FAIL(inner_init(tablet_id, compaction_scn, cg_cnt, root_macro_seq_on_sstable, exec_mode, allocator))) {
    LOG_WARN("failed to init", KR(ret), K(tablet_id), K(compaction_scn), K(cg_cnt), K(root_macro_seq_on_sstable));
  } else {
    parallel_cnt_in_cg_ = parallel_cnt;
  }
  return ret;
}

int ObCOMajorTaskCheckpointMgr::mark_cg_finish(
    const ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCOMajorTaskCheckpointMgr not inited", K(ret), KP(finish_flags_), K_(task_cnt));
  } else if (OB_UNLIKELY(start_cg_idx < 0 || start_cg_idx >= end_cg_idx || end_cg_idx > cg_cnt_
      || !tablet_id.is_valid() || compaction_scn <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_cg_idx), K(end_cg_idx), K(tablet_id), K(compaction_scn), KPC(this));
  } else {
    lib::ObMutexGuard guard(lock_);
    for (int64_t idx = start_cg_idx; idx < end_cg_idx; ++idx) {
      finish_flags_[idx] = true;
    }
    if (start_cg_idx == max_continue_finish_idx_ + 1) {
      // update finish idx only when next idx finish
      update_continue_finish_idx_();

      if (OB_FAIL(upload_tablet_compaction_status_(tablet_id, compaction_scn))) {
        LOG_WARN("failed to update seq to inner table", KR(ret), K(tablet_id), K(compaction_scn));
      }
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
