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

#ifndef OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_H
#define OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_H

#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"

namespace oceanbase
{

namespace share
{
struct ObTabletCacheInterval;
}

namespace storage
{

class ObTabletFullDirectLoadMgrV2 final : public ObTabletDirectLoadMgr
{
public:
  ObTabletFullDirectLoadMgrV2()
    : ObTabletDirectLoadMgr(), last_data_seq_(0), last_meta_seq_(0), last_lob_id_(0), slice_group_(), start_scn_(), total_slice_cnt_(-1)
  { }
  ~ObTabletFullDirectLoadMgrV2()
  {
    last_data_seq_ = 0;
    last_meta_seq_ = 0;
    last_lob_id_ = 0;
  }
  virtual int update(
      ObTabletDirectLoadMgr *lob_tablet_mgr,
      const ObTabletDirectLoadInsertParam &build_param) override;
  int open(const int64_t current_execution_id, share::SCN &start_scn) override;
  int close(const int64_t current_execution_id, const share::SCN &start_scn) override
  {
    return close(current_execution_id);
  }
  share::SCN get_start_scn() override
  {
    return start_scn_;
  }
  share::SCN get_commit_scn(const ObTabletMeta &tablet_meta) override { return share::SCN::invalid_scn(); }
  int64_t get_last_lob_id() const { return last_lob_id_; }

  int init(
      ObTabletDirectLoadMgr *lob_tablet_mgr,
      const ObTabletDirectLoadInsertParam &build_param); // init mgr and set lob mgr handle on demand.
  
  int close(const int64_t current_execution_id, bool need_report_checksum = true); // create major sstable, and callback write finish log.
  int open_sstable_slice(
      const bool is_data_tablet_process_for_lob,
      const blocksstable::ObMacroDataSeq &start_seq,
      const ObDirectLoadSliceInfo &slice_info) override;
  int fill_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info,
      const share::SCN &start_scn,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows,
      ObInsertMonitor *insert_monitor = nullptr) override;
  int fill_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info,
      const share::SCN &start_scn,
      const blocksstable::ObBatchDatumRows &datum_rows,
      ObInsertMonitor *insert_monitor = nullptr) override;
  int fill_lob_sstable_slice(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      const share::SCN &start_scn,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObDatumRow &datum_row) override;
  int fill_lob_sstable_slice(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      const share::SCN &start_scn,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObBatchDatumRows &datum_rows) override;
  int close_sstable_slice(
      const bool is_data_tablet_process_for_lob,
      const ObDirectLoadSliceInfo &slice_info,
      const share::SCN &start_scn,
      const int64_t execution_id,
      ObInsertMonitor *insert_monitor,
      blocksstable::ObMacroDataSeq &next_seq) override;
  int fill_column_group(const int64_t thread_cnt, const int64_t thread_id) override;
  int wait_notify(
      const ObDirectLoadSliceWriter *slice_writer);
  int update_max_lob_id(const int64_t lob_id) override;
  int update_max_macro_seq(const int64_t macro_seq);
  int set_total_slice_cnt(const int64_t total_slice_cnt) override;
  int64_t get_dir_id() const { return dir_id_; }
  ObMacroMetaStoreManager &get_macro_meta_store_manager() { return macro_meta_store_mgr_; }
  INHERIT_TO_STRING_KV("ObTabletDirectLoadMgr", ObTabletDirectLoadMgr, KPC(this), K(last_data_seq_), K(last_meta_seq_), K(last_lob_id_), K(slice_group_), K(total_slice_cnt_));

private:
  inline void update_last_meta_seq(const ObITable::TableKey &table_key, const int64_t new_seq);
  int64_t calc_root_macro_seq();
  int build_ddl_sstable_res(
    blocksstable::ObSSTableIndexBuilder &sstable_index_builder,
    ObITable::TableKey &table_key,
    blocksstable::ObSSTableMergeRes &res);
  int create_ddl_ro_sstable(
    ObTablet &tablet,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &sstable_handle);
  int create_ddl_co_sstable(
    ObTablet &tablet,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &co_sstable_handle);
  int create_ddl_cg_sstable(
    const int64_t cg_idx,
    const int64_t create_schema_version_on_tablet,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObITable::TableKey &cg_table_key,
    ObTableHandleV2 &cg_sstable_handle);
  int update_table_store(
    const blocksstable::ObSSTable *sstable,
    const ObStorageSchema *storage_schema,
    ObITable::TableKey &table_key,
    ObTablet &tablet);
  int fill_aggregated_column_group(
    const int64_t parallel_idx,
    const int64_t start_idx,
    const int64_t last_idx,
    const ObStorageSchema *storage_schema,
    ObCOSliceWriter *cur_writer,
    int64_t &fill_cg_finish_count,
    int64_t &fill_row_cnt,
    int64_t &last_seq);
  int fill_batch_slice_cg(
    const int64_t context_id,
    const ObArray<int64_t> &slice_id_array,
    const ObStorageSchema *storage_schema,
    const share::SCN &start_scn,
    int64_t &last_seq,
    ObInsertMonitor *insert_monitor);
  int get_tablet_handle(ObTabletHandle &tablet_handle);
DISALLOW_COPY_AND_ASSIGN(ObTabletFullDirectLoadMgrV2);

private:
  int64_t last_data_seq_;
  int64_t last_meta_seq_;
  int64_t last_lob_id_;
  ObTabletDirectLoadSliceGroup slice_group_;
  share::SCN start_scn_;
  int64_t total_slice_cnt_;
  ObMacroMetaStoreManager macro_meta_store_mgr_;
};

}// namespace storage
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_H
