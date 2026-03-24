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
#ifndef OB_STORAGE_COMPACTION_UPDATE_SKIP_MAJOR_TABLET_DAG_H_
#define OB_STORAGE_COMPACTION_UPDATE_SKIP_MAJOR_TABLET_DAG_H_
#include "share/compaction/ob_batch_exec_dag.h"
namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObLS;
class ObTableHandleV2;
class ObCOSSTableV2;
}
namespace blocksstable
{
class ObSSTable;
}
namespace compaction
{
class ObLSObj;

struct ObUpdateSkipMajorParam : public ObBatchExecParam<ObTabletID>
{
  ObUpdateSkipMajorParam()
    : ObBatchExecParam(UPDATE_SKIP_MAJOR_TABLET)
  {}
  ObUpdateSkipMajorParam(
    const share::ObLSID &ls_id,
    const int64_t merge_version)
    : ObBatchExecParam(UPDATE_SKIP_MAJOR_TABLET, ls_id, merge_version, DEFAULT_BATCH_SIZE)
  {}
  virtual ~ObUpdateSkipMajorParam() {}
  static const int64_t DEFAULT_BATCH_SIZE = 32;
};

struct ObUpdateTabletCnt
{
  ObUpdateTabletCnt()
    : ss_cnt_(0),
      local_cnt_(0)
  {}
  TO_STRING_KV(K_(ss_cnt), K_(local_cnt));
  int64_t ss_cnt_;
  int64_t local_cnt_;
};

class ObUpdateSkipMajorTabletTask : public ObBatchExecTask<ObUpdateSkipMajorTabletTask, ObUpdateSkipMajorParam>
{
public:
  ObUpdateSkipMajorTabletTask();
  virtual int inner_process() override;
private:
  int update_skip_major_tablet_meta(
    common::ObArenaAllocator &allocator,
    storage::ObLS &ls,
    ObLSObj &ls_obj,
    const int64_t compaction_scn,
    const ObTabletID &tablet_id,
    int64_t &batch_finished_data_size);
  int load_sstable_and_deep_copy(
    common::ObArenaAllocator &allocator,
    blocksstable::ObSSTable &last_major,
    storage::ObTableHandleV2 &new_sstable_handle);
  int update_tablet(
    common::ObArenaAllocator &allocator,
    const int64_t macro_seq,
    storage::ObLS &ls,
    const storage::ObTablet &old_tablet,
    const blocksstable::ObSSTable &new_major);
  int generate_new_co_sstable(
    const int64_t compaction_scn,
    storage::ObCOSSTableV2 &last_major);
  int update_sstable(
    const int64_t compaction_scn,
    blocksstable::ObSSTable &new_major,
    int64_t &update_tablet_macro_seq);
  int try_refresh_major_sstable(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t last_major_snapshot_version,
    bool &scheduled_refresh_dag);
private:
  ObUpdateTabletCnt update_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObUpdateSkipMajorTabletTask);
};

class ObUpdateSkipMajorTabletDag : public ObBatchExecDag<ObUpdateSkipMajorTabletTask, ObUpdateSkipMajorParam>
{
public:
  ObUpdateSkipMajorTabletDag()
    : ObBatchExecDag(share::ObDagType::DAG_TYPE_UPDATE_SKIP_MAJOR)
  {}
  virtual ~ObUpdateSkipMajorTabletDag() = default;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUpdateSkipMajorTabletDag);
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_UPDATE_SKIP_MAJOR_TABLET_DAG_H_
