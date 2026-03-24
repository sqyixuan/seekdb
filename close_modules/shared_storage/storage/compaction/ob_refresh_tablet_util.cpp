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
#include "storage/compaction/ob_refresh_tablet_util.h"
#include "src/storage/ls/ob_ls.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/compaction/ob_merge_ctx_func.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/compaction/ob_compaction_schedule_util.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace compaction
{

ObDownloadTabletMetaParam::ObDownloadTabletMetaParam()
  : snapshot_version_(0), allow_dup_major_(false), init_major_ckm_info_(false), need_prewarm_(false)
{
}

ObDownloadTabletMetaParam::ObDownloadTabletMetaParam(
    const int64_t snapshot_version,
    const bool allow_dup_major,
    const bool init_major_ckm_info,
    const bool need_prewarm)
  : snapshot_version_(snapshot_version), allow_dup_major_(allow_dup_major),
    init_major_ckm_info_(init_major_ckm_info), need_prewarm_(need_prewarm)
{
}

ObDownloadTabletMetaParam::~ObDownloadTabletMetaParam()
{
}


ObUpdateTabletMetaParam::ObUpdateTabletMetaParam()
  : pre_warm_snapshot_version_(0), allow_dup_major_(false), init_major_ckm_info_(false)
{
}

ObUpdateTabletMetaParam::ObUpdateTabletMetaParam(
    const int64_t pre_warm_snapshot_version,
    const bool allow_dup_major,
    const bool init_major_ckm_info)
  : pre_warm_snapshot_version_(pre_warm_snapshot_version),
    allow_dup_major_(allow_dup_major),
    init_major_ckm_info_(init_major_ckm_info)
{
}

ObUpdateTabletMetaParam::~ObUpdateTabletMetaParam()
{
}

bool ObUpdateTabletMetaParam::is_valid() const
{
  return (pre_warm_snapshot_version_ >= 0);
}


int ObRefreshTabletUtil::get_shared_tablet_meta(
  common::ObArenaAllocator &allocator,
  const ObTabletID &tablet_id,
  const int64_t snapshot_version,
  ObTablet &shared_tablet)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_share_tablet_meta_object_opt(tablet_id.id(), snapshot_version);
  MacroBlockId tablet_meta_obj_id;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObMetaDiskAddr disk_addr;
  const int64_t tablet_meta_size = OB_DEFAULT_MACRO_BLOCK_SIZE;

  if (OB_UNLIKELY(!tablet_id.is_valid() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(snapshot_version));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, tablet_meta_obj_id))) {
    LOG_WARN("fail to set macro id", K(ret), K(opt));
  } else if (FALSE_IT(disk_addr.set_block_addr(tablet_meta_obj_id, 0/*offset*/, tablet_meta_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
  } else if (OB_FAIL(MTL(ObTenantStorageMetaService*)->read_from_disk(disk_addr, 0/*ls_epoch*/, allocator, buf, buf_len))) {
    LOG_WARN("fail to read tablet buf from disk", K(ret), K(disk_addr));
  } else if (FALSE_IT(shared_tablet.set_tablet_addr(disk_addr))) {
  } else if (OB_FAIL(shared_tablet.deserialize_for_replay(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize tablet", K(ret), KP(buf), K(buf_len), K(disk_addr));
  }
  return ret;
}

/*
 CAUTION!!!!
 The downloaded tablet_meta only has limited valid info like table_store and storage_schema.
 If requirements need more info, 
 make sure the info has been assigned correctly in ObTablet::init_for_shared_merge.
*/
int ObRefreshTabletUtil::download_major_compaction_tablet_meta(
  storage::ObLS &ls,
  const ObTabletID &tablet_id,
  const ObDownloadTabletMetaParam &param)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DldTabletMeta"));
  ObTablet shared_tablet;
  const ObUpdateTabletMetaParam update_tablet_meta_param(
        param.need_prewarm_ ? param.snapshot_version_ : 0/*pre_warm_snapshot_version*/,
        param.allow_dup_major_/*allow_dup_major_*/,
        param.init_major_ckm_info_/*init_major_ckm_info_*/);
  if (OB_FAIL(get_shared_tablet_meta(allocator, tablet_id, param.snapshot_version_, shared_tablet))) {
    LOG_WARN("fail to get shared tablet", K(ret), K(tablet_id), "snapshot_version", param.snapshot_version_);
  } else if (OB_FAIL(update_tablet_meta(ls, shared_tablet, update_tablet_meta_param, ObVersionRange::MIN_VERSION))) {
    LOG_WARN("failed to update tablet meta", K(ret), K(update_tablet_meta_param));
  }
  return ret;
}

/*
 CAUTION!!!!
 The shared_tablet maybe downloaded_tablet_meta, 
 which only has limited valid info like table_store and storage_schema.
 If requirements need more info, 
 make sure the info has been assigned correctly in ObTablet::init_for_shared_merge.
*/
int ObRefreshTabletUtil::update_tablet_meta(
    storage::ObLS &ls,
    const storage::ObTablet &shared_tablet,
    const ObUpdateTabletMetaParam &param, 
    const int64_t table_multi_version_start)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DldUpTabMeta"));
  ObSSTable *sstable = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  ObTabletHandle new_tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletID &tablet_id = shared_tablet.get_tablet_id();
  if (OB_UNLIKELY(!tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invlaid argument", K(ret), K(tablet_id), "pre_warm_snapshot_version", param.pre_warm_snapshot_version_);
  } else if (OB_FAIL(fetch_sstable(shared_tablet, table_store_wrapper, sstable))) {
    LOG_WARN("fail to fetch last major sstable", K(ret));
  } else if (OB_FAIL(shared_tablet.load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K(shared_tablet));
  } else {
    if ((param.pre_warm_snapshot_version_ > 0) && !param.init_major_ckm_info_) {
      int64_t prewarm_percent = 0;
      if (param.pre_warm_snapshot_version_ <= MERGE_SCHEDULER_PTR->get_inner_table_merged_scn()) {
        // refresh one old compaction_scn under migration and major merge orthogonal scenes
        prewarm_percent = ObMCPrewarmPercentUtil::DEFAULT_PREWARM_PERCENT;
      } else if (OB_FAIL(ObMCPrewarmPercentUtil::get_prewarm_percent(prewarm_percent))) {
        LOG_WARN("fail to get prewarm percent", KR(ret));
      }
      ObHotTabletInfoReader reader(tablet_id.id(), param.pre_warm_snapshot_version_, prewarm_percent);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(reader.load_hot_macro_infos())) {
        // TODO use tmp ret later
        LOG_WARN("failed to load hot macro info", K(ret), K(tablet_id), "pre_warm_snapshot_version",
                 param.pre_warm_snapshot_version_);
      }
    }

    ObUpdateTableStoreParam update_table_store_param(
        sstable->get_key().get_snapshot_version(),
        table_multi_version_start,
        storage_schema,
        ls.get_rebuild_seq(),
        param.init_major_ckm_info_ ? NULL : sstable,
        param.allow_dup_major_);
    update_table_store_param.ddl_info_.update_with_major_flag_ = true; /* download only used for update major*/
    if (FAILEDx(update_table_store_param.init_with_compaction_info(
      ObCompactionTableStoreParam(MAJOR_MERGE, SCN::min_scn(), true/*need_report*/, shared_tablet.has_truncate_info())))) {
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else if (param.init_major_ckm_info_
      && OB_FAIL(update_table_store_param.compaction_info_.major_ckm_info_.init_from_sstable(
                 allocator, EXEC_MODE_OUTPUT, *storage_schema, *sstable))) {
      LOG_WARN("failed to major ckm info", KR(ret), KPC(sstable));
    } else if (OB_FAIL(ls.update_tablet_table_store(tablet_id, update_table_store_param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(update_table_store_param), K(new_tablet_handle));
    } else {
      FLOG_INFO("update tablet", K(ret), K(tablet_id), KPC(new_tablet_handle.get_obj()), K(update_table_store_param));
    }
  }
  ObStorageSchemaUtil::free_storage_schema(allocator, storage_schema);
  return ret;
}

int ObRefreshTabletUtil::download_major_ckm_info(
      const common::ObTabletID &tablet_id,
      const int64_t compaction_scn,
      const ObNewMicroInfo *new_micro_info,
      storage::ObLS &ls)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  const ObDownloadTabletMetaParam download_tablet_meta_param(compaction_scn/*snapshot_version*/,
                                                             false/*allow_dup_major*/,
                                                             true/*init_major_ckm_info*/,
                                                             false/*need_prewarm*/);
  if (OB_FAIL(download_major_compaction_tablet_meta(ls, tablet_id, download_tablet_meta_param))) {
    LOG_WARN("failed to download tablet meta", KR(ret), K(tablet_id), K(download_tablet_meta_param));
  } else {
    ObTabletCompactionState tmp_state;
    tmp_state.set_output_scn(compaction_scn);
    (void) ObMergeCtxFunc::update_tablet_state(ls_id, tablet_id, tmp_state, new_micro_info);
    if (OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id, true/*need_diagnose*/))) {
      LOG_WARN_RET(tmp_ret, "failed to submit tablet update task to report", K(ls_id), K(tablet_id));
    } else if (OB_TMP_FAIL(ls.get_tablet_svr()->update_tablet_report_status(tablet_id))) {
      LOG_WARN_RET(tmp_ret, "failed to update tablet report status", K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObRefreshTabletUtil::fetch_sstable(
  const storage::ObTablet &shared_tablet,
  ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
  ObSSTable *&last_major)
{
  int ret = OB_SUCCESS;
  last_major = NULL;
  if (OB_FAIL(shared_tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is null", K(ret), K(shared_tablet));
  } else if (OB_UNLIKELY(1 != table_store_wrapper.get_member()->get_major_sstables().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorret major sstable", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (OB_ISNULL(last_major = static_cast<ObSSTable *>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable is null", K(ret), K(shared_tablet));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
