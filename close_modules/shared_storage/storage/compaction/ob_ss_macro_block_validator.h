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
#ifndef OB_STORAGE_COMPACTION_MACRO_BLOCK_VALIDATOR_H_
#define OB_STORAGE_COMPACTION_MACRO_BLOCK_VALIDATOR_H_
#include "/usr/include/stdint.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
namespace oceanbase
{
namespace blocksstable
{
class ObMacroBlock;
}
namespace compaction
{

struct ObSSMacroBlockValidator : public blocksstable::ObIMacroBlockValidator
{
public:
  ObSSMacroBlockValidator(const int64_t block_cnt)
    : dump_macro_rest_cnt_(block_cnt),
      sec_meta_iter_()
  {}
  virtual void validate_and_dump(const blocksstable::ObMacroBlock &macro_block) override;
  virtual void close() override;
  TO_STRING_KV(K_(dump_macro_rest_cnt), K_(sec_meta_iter));
  int64_t dump_macro_rest_cnt_;
  blocksstable::ObSSTableSecMetaIterator sec_meta_iter_;
};

struct ObSSMacroBlockValidatorMgr
{
public:
  int init(const ObStaticMergeParam &static_param);
  int alloc_validator(
    const ObMergeParameter &merge_param,
    ObArenaAllocator &allocator,
    ObIMacroBlockValidator *&validator);
  TO_STRING_KV(K_(table_handle));
private:
  int open_meta_iter(
    const ObSSTable &other_major_sstable,
    const ObMergeParameter &merge_param,
    ObIAllocator &allocator,
    ObSSMacroBlockValidator &validator);
  static const int64_t DUMP_MACRO_CNT_THREASHOLD = 3;
  ObTableHandleV2 table_handle_;
};

struct ObSSMacroBlockDumper
{
public:
  static int dump_macro_block(const blocksstable::MacroBlockId &block_id,
                              const blocksstable::ObMacroBlock &macro_block,
                              MacroBlockId &dump_macro_id);
  static lib::ObMutex lock;
private:
  static int generate_dump_macro_opt(
      const blocksstable::MacroBlockId &block_id,
      const int64_t compaction_scn,
      ObStorageObjectOpt &opt);
  static int dump(
    const blocksstable::ObMacroBlock &macro_block,
    const ObStorageObjectOpt &opt);
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MACRO_BLOCK_VALIDATOR_H_
