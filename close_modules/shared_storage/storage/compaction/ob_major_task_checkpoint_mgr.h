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
#ifndef OB_STORAGE_COMPACTION_MAJOR_TASK_CHECKPOINT_MGR_H_
#define OB_STORAGE_COMPACTION_MAJOR_TASK_CHECKPOINT_MGR_H_
#include "lib/lock/ob_mutex.h"
#include "common/ob_tablet_id.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_tablet_compaction_status.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
struct ObTabletCompactionSeqInfo;
}
namespace blocksstable
{
class MacroBlockId;
}
namespace compaction
{
/*
 *        | MACRO_STEP_SIZE | MACRO_STEP_SIZE | MACRO_STEP_SIZE | MACRO_STEP_SIZE |
 *        |  task_idx = 0   |  task_idx = 1   |   index tree    |   tablet meta   |
 *        |                                                                       |
 * root_macro_seq_on_sstable                                               new_root_seq
*/

// used for GC
class ObSimpleTaskCheckpointMgr
{
public:
  ObSimpleTaskCheckpointMgr() = default;
  ~ObSimpleTaskCheckpointMgr() = default;
  int get_all_macro_start_seq(
    const common::ObTabletID &tablet_id,
    const int64_t parallel_cnt,
    const int64_t column_group_cnt,
    const int64_t last_major_root_macro_seq,
    ObIArray<blocksstable::MacroBlockId> &seq_id_array);
protected:
  int calc_macro_seq(
    const int64_t calc_task_idx,
    const int64_t root_macro_seq,
    int64_t &macro_seq) const;
};

class ObMajorTaskCheckpointMgr : public ObSimpleTaskCheckpointMgr
{
public:
  ObMajorTaskCheckpointMgr();
  ~ObMajorTaskCheckpointMgr();
  virtual int init(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t parallel_cnt,
    const int64_t cg_cnt, // unused for row store
    const int64_t root_macro_seq_on_sstable,
    const ObExecMode exec_mode,
    common::ObIAllocator &allocator);
  void destroy(common::ObIAllocator &allocator);
  int mark_task_finish(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t task_idx);
  int64_t get_start_schedule_task_idx() const
  {
    return max_continue_finish_idx_ + 1;
  }
  virtual int get_macro_start_seq(const int64_t task_idx,
                                  int64_t &macro_start_seq) const {
    return inner_get_macro_start_seq(task_idx, task_cnt_, macro_start_seq);
  }
  virtual int get_macro_seq_by_stage(const ObGetMacroSeqStage stage,
                                     int64_t &macro_start_seq) const {
    return inner_get_macro_seq_by_stage(stage, task_cnt_, macro_start_seq);
  }
  TO_STRING_KV(K_(is_inited), K_(task_cnt), K_(max_continue_finish_idx), K_(root_macro_seq),
    "exec_mode", exec_mode_to_str(exec_mode_));
protected:
  int inner_init(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t task_cnt,
    const int64_t root_macro_seq_on_sstable,
    const ObExecMode exec_mode,
    common::ObIAllocator &allocator);
  virtual ObTabletCompactionStatus::ObMajorCkpType get_ckp_type() const
  { return ObTabletCompactionStatus::PARALLEL_TASK_IDX; }
  int init_start_task_idx(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t task_cnt,
    const int64_t root_macro_seq_on_sstable);
  void update_continue_finish_idx_();
  int upload_tablet_compaction_status_(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn) const;
  int inner_get_macro_start_seq(
      const int64_t task_idx,
      const int64_t task_cnt,
      int64_t &macro_start_seq) const;
  int inner_get_macro_seq_by_stage(
      const ObGetMacroSeqStage stage,
      const int64_t task_cnt,
      int64_t &macro_start_seq) const;
protected:
  lib::ObMutex lock_;
  bool *finish_flags_;
  union {
    int64_t task_cnt_;
    int64_t cg_cnt_;
  };
  int64_t max_continue_finish_idx_;
  int64_t root_macro_seq_; // record max used seq in last major
  ObExecMode exec_mode_;
  bool is_inited_;
};

// for co major, task_cnt_ = cg_cnt_
class ObCOMajorTaskCheckpointMgr : public ObMajorTaskCheckpointMgr
{
public:
  ObCOMajorTaskCheckpointMgr()
    : ObMajorTaskCheckpointMgr(),
      parallel_cnt_in_cg_(0)
  {}
  virtual ~ObCOMajorTaskCheckpointMgr() = default;
  virtual int init(
    const common::ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t parallel_cnt,
    const int64_t cg_cnt,
    const int64_t root_macro_seq_on_sstable,
    const ObExecMode exec_mode,
    common::ObIAllocator &allocator) override;
  /*
  * parallel_idx = parallel index in cg parallel major merge
  */
  virtual int get_macro_start_seq(const int64_t task_idx,
                                  int64_t &macro_start_seq) const  override {
    return inner_get_macro_start_seq(task_idx, parallel_cnt_in_cg_, macro_start_seq);
  }
  virtual int get_macro_seq_by_stage(const ObGetMacroSeqStage stage,
                                     int64_t &macro_start_seq) const override {
    return inner_get_macro_seq_by_stage(stage, parallel_cnt_in_cg_, macro_start_seq);
  }
  int mark_cg_finish(
    const ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx);
  INHERIT_TO_STRING_KV("ObCOMajorTaskCheckpointMgr", ObMajorTaskCheckpointMgr, K_(parallel_cnt_in_cg));
protected:
  virtual ObTabletCompactionStatus::ObMajorCkpType get_ckp_type() const
  { return ObTabletCompactionStatus::CG_IDX; }
  int64_t parallel_cnt_in_cg_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MAJOR_TASK_CHECKPOINT_MGR_H_
