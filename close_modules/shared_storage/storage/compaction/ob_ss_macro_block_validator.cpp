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
#include "storage/compaction/ob_ss_macro_block_validator.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/compaction/ob_refresh_tablet_util.h"
namespace oceanbase
{
using namespace blocksstable;
using namespace storage;
using namespace common;
namespace compaction
{

void ObSSMacroBlockValidator::validate_and_dump(const ObMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  bool dump_flag = false;
  ObDataMacroBlockMeta ss_macro_meta;
  if (OB_FAIL(sec_meta_iter_.get_next(ss_macro_meta))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Failed to get next macro block", K(ret));
    } else {
      ret = OB_SUCCESS;
      // no macro in major sstable, cur macro should dump
      dump_flag = true;
    }
  } else if (ss_macro_meta.val_.data_checksum_ != macro_block.get_data_checksum()) {
    dump_flag = true;
  } else {
    LOG_INFO("ss_macro and validate_macro have same data checksum", KR(ret),
      "validate_macro_data_checksum", macro_block.get_data_checksum(),
      "ss_macro_data_checksum", ss_macro_meta.val_.data_checksum_);
  }
  if (OB_SUCC(ret) && dump_flag) {
    int tmp_ret = OB_SUCCESS;
    MacroBlockId dump_macro_id;
    if (dump_macro_rest_cnt_ <= 0) {
      LOG_WARN("ObSSMacroBlockValidator print checksum error macro", KR(ret), K(macro_block.get_row_count()),
        K(macro_block.get_data_checksum()), K(ss_macro_meta.val_.data_checksum_));
    } else if (OB_TMP_FAIL(ObSSMacroBlockDumper::dump_macro_block(ss_macro_meta.get_macro_id(), macro_block, dump_macro_id))) {
      LOG_WARN("failed to dump macro block", KR(tmp_ret), K(ss_macro_meta));
    } else {
      --dump_macro_rest_cnt_;
      // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
      FLOG_WARN("ObSSMacroBlockValidator dump checksum error macro", KR(ret), K(macro_block.get_row_count()),
        "validate_macro_data_checksum", macro_block.get_data_checksum(),
        "ss_macro_data_checksum", ss_macro_meta.val_.data_checksum_, K(dump_macro_id));
    }
  }
}

void ObSSMacroBlockValidator::close()
{
  ObDataMacroBlockMeta macro_meta;
  int ret = sec_meta_iter_.get_next(macro_meta);
  if (OB_ITER_END != ret) {
    if (OB_SUCCESS == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exist macro in shared_storage major sstable, but iter end in local major", KR(ret), K(macro_meta));
    } else {
      LOG_WARN("failed to get next for meta iter", KR(ret), K(sec_meta_iter_));
    }
  }
}

int ObSSMacroBlockValidatorMgr::init(
  const ObStaticMergeParam &static_param)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DldUpTabMeta"));
  ObTablet shared_tablet;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTable *sstable = nullptr;
  if (OB_UNLIKELY(!static_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(static_param));
  } else if (OB_FAIL(ObRefreshTabletUtil::get_shared_tablet_meta(
          allocator, static_param.get_tablet_id(),
          static_param.get_compaction_scn(), shared_tablet))) {
    LOG_WARN("fail to get shared tablet", K(ret), K(static_param));
  } else if (OB_FAIL(ObRefreshTabletUtil::fetch_sstable(shared_tablet, table_store_wrapper, sstable))) {
    LOG_WARN("fail to fetch last major sstable", K(ret));
  } else if (OB_FAIL(table_handle_.set_sstable(sstable, table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("failed to set table handle", K(ret));
  }
  return ret;
}

int ObSSMacroBlockValidatorMgr::alloc_validator(
    const ObMergeParameter &merge_param,
    ObArenaAllocator &allocator,
    ObIMacroBlockValidator *&validator)
{
  validator = NULL;

  int ret = OB_SUCCESS;
  void *buf = NULL;
  ObSSMacroBlockValidator *alloc_ptr = NULL;
  // for concurrent merge, only dump one wrong block in one parallel range
  const int64_t block_cnt = (1 == merge_param.static_param_.concurrent_cnt_) ? DUMP_MACRO_CNT_THREASHOLD : 1;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSSMacroBlockValidator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc validator", K(ret), KPC(this), "alloc_size", sizeof(ObSSMacroBlockValidator));
  } else if (FALSE_IT(alloc_ptr = new(buf) ObSSMacroBlockValidator(block_cnt))) {
  } else if (OB_FAIL(open_meta_iter(*static_cast<ObSSTable *>(table_handle_.get_table()), merge_param, allocator, *alloc_ptr))) {
    LOG_WARN("failed to open meta iter", K(ret), KPC(this));
  } else {
    validator = alloc_ptr;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(alloc_ptr)) {
    alloc_ptr->~ObSSMacroBlockValidator();
    allocator.free(alloc_ptr);
  }
  return ret;
}

int ObSSMacroBlockValidatorMgr::open_meta_iter(
  const ObSSTable &other_major_sstable,
  const ObMergeParameter &merge_param,
  ObIAllocator &allocator,
  ObSSMacroBlockValidator &validator)
{
  int ret = OB_SUCCESS;
  const storage::ObITableReadInfo *index_read_info = nullptr;
  const ObDatumRange &merge_range = other_major_sstable.is_normal_cg_sstable() ? merge_param.merge_rowid_range_ : merge_param.merge_range_;
  if (other_major_sstable.is_normal_cg_sstable()) {
    if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
      LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
    }
  } else {
    index_read_info = merge_param.static_param_.rowkey_read_info_;
  }

  if (FAILEDx(validator.sec_meta_iter_.open(merge_range, DATA_BLOCK_META, other_major_sstable, *index_read_info, allocator))) {
    LOG_WARN("failed to open secondary meta iter", KR(ret));
  }
  return ret;
}

lib::ObMutex ObSSMacroBlockDumper::lock(common::ObLatchIds::MERGER_DUMP_LOCK);
int ObSSMacroBlockDumper::dump_macro_block(
  const MacroBlockId &block_id,
  const ObMacroBlock &macro_block,
  MacroBlockId &dump_macro_id)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  lib::ObMutexGuard guard(ObSSMacroBlockDumper::lock); // lock to avoid several thread dump to same dir
  if (OB_FAIL(generate_dump_macro_opt(block_id, macro_block.get_compaction_scn(), opt))) {
    LOG_WARN("failed to generate dump file name", K(ret), K(block_id));
  } else if (OB_FAIL(dump(macro_block, opt))) {
    LOG_WARN("failed to dump macro", K(ret), K(block_id), K(dump_macro_id));
  } else if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, dump_macro_id))) {
    LOG_WARN("failed to get macro id from opt", KR(ret), K(opt));
  }
  return ret;
}

int ObSSMacroBlockDumper::generate_dump_macro_opt(
      const MacroBlockId &block_id,
      const int64_t compaction_scn,
      ObStorageObjectOpt &opt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro_meta is invalid", K(ret), K(block_id));
  } else {
    opt.set_ss_checksum_error_dump_macro_opt(
      ObStorageObjectType::CHECKSUM_ERROR_DUMP_MACRO,
      block_id.second_id(), // tablet_id
      block_id.fourth_id(), // column_group_id
      compaction_scn,
      block_id.third_id() // data_seq
    );
  }
  return ret;
}

int ObSSMacroBlockDumper::dump(
  const ObMacroBlock &macro_block,
  const ObStorageObjectOpt &opt)
{
  int ret = OB_SUCCESS;
  const ObCompactionBufferBlock &mb_buf = macro_block.get_block_buffer();
  ObStorageObjectWriteInfo write_info;
  ObStorageObjectHandle handle;
  write_info.buffer_ = static_cast<const char *>(mb_buf.get_buffer());
  write_info.size_ = mb_buf.get_buffer_size();
  write_info.offset_ = 0;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  write_info.io_desc_.set_sealed();
  write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  write_info.mtl_tenant_id_ = MTL_ID();
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.write_object(opt, write_info, handle))) {
    LOG_WARN("failed to write obj", KR(ret), K(opt), K(write_info));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
