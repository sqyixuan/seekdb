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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_BLOCK_GC_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_BLOCK_GC_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "deps/oblib/src/common/ob_tablet_id.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObPendingFreeTabletItem;

typedef common::hash::ObHashSet<blocksstable::MacroBlockId, common::hash::NoPthreadDefendMode> BlockIDSet;

struct GCBaseOP
{
public:
  GCBaseOP(
    const BlockIDSet *exist_block_set)
    : block_id_set_(exist_block_set)
  {}
  virtual ~GCBaseOP() { result_block_id_set_.destroy(); }
  GCBaseOP(GCBaseOP&) = delete;
  GCBaseOP& operator=(const GCBaseOP&) = delete;
  const BlockIDSet &get_result_block_id_set() { return result_block_id_set_; }
  virtual int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> &result_block_ids)
  { return OB_NOT_SUPPORTED; }

protected:
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> *result_block_ids);

  virtual void add_or_print_debug_block(const blocksstable::MacroBlockId &block_id) {}
private:
  const BlockIDSet *block_id_set_;
  BlockIDSet result_block_id_set_;
};

struct BlockCollectOP : public GCBaseOP
{
  BlockCollectOP()
    : GCBaseOP(NULL)
  {}
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids)
  { return GCBaseOP::operator()(block_ids, NULL); }
  virtual ~BlockCollectOP() {}
};

struct GCTabletMetaVersionOP : public GCBaseOP
{
public:
  GCTabletMetaVersionOP(
    const BlockIDSet &exist_block_set,
    const BlockIDSet &blocks_for_error_check)
    : GCBaseOP(&exist_block_set),
      blocks_for_error_check_(blocks_for_error_check)
  {}
  virtual ~GCTabletMetaVersionOP() 
  { print_debug_block(); }
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> &result_block_ids);
  void print_debug_block();
  void add_or_print_debug_block(const blocksstable::MacroBlockId &block_id);

private:
  const BlockIDSet &blocks_for_error_check_;
  ObArray<blocksstable::MacroBlockId> debug_block_ids_;
};

struct GCTabletOP : public GCBaseOP
{
  GCTabletOP()
    : GCBaseOP(NULL)
  {}
  virtual ~GCTabletOP() {}
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> &result_block_ids)
  { return GCBaseOP::operator()(block_ids, &result_block_ids); }
};

struct MacroBlockCheckOP
{
public:
  MacroBlockCheckOP(
    const BlockIDSet &exist_block_set,
    const uint64_t min_block_id,
    const uint64_t max_block_id)
    : block_id_set_(exist_block_set),
      min_block_id_(min_block_id),
      max_block_id_(max_block_id)
  {}
  MacroBlockCheckOP(MacroBlockCheckOP&) = delete;
  MacroBlockCheckOP& operator=(const MacroBlockCheckOP&) = delete;

  // result_block_ids = block_ids - block_id_set
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> &result_block_ids) const;
  TO_STRING_KV(K_(max_block_id), K_(min_block_id));
private:
  const BlockIDSet &block_id_set_;
  // for macro block check in private dir
  const uint64_t min_block_id_;
  const uint64_t max_block_id_;
};

int delete_macro_blocks(
    ObIArray<blocksstable::MacroBlockId> &block_ids);

class ObBlockGCHandler
{
public:
  ObBlockGCHandler(
      const ObTabletID &tablet_id)
    : tablet_id_(tablet_id),
      gc_macro_block_cnt_(0)
  {}
  virtual ~ObBlockGCHandler() {}
  bool is_valid()
  { return tablet_id_.is_valid(); }

  // gc tablet operation
  int gc_tablet_meta_versions(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version);
  int gc_tablet(
      const ObIArray<int64_t> &tablet_versions,
      // for macro block check in private dir
      const uint64_t min_block_id = 0,
      const uint64_t max_block_id = INT64_MAX);

  // virutal interface
  virtual int get_blocks_for_tablet(
    int64_t tablet_meta_version,
    ObIArray<blocksstable::MacroBlockId> &block_ids) = 0;
  virtual int delete_tablet_meta_version(
    int64_t tablet_meta_version) = 0;
  virtual int try_delete_tablet_meta_dir() = 0;
  virtual int try_delete_tablet_data_dir() = 0;
  virtual int get_block_ids_from_dir(
    ObIArray<blocksstable::MacroBlockId> &block_ids) = 0;
  virtual int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &collect_check_block_op) = 0;
  virtual int delete_macro_blocks(
      ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    return storage::delete_macro_blocks(block_ids);
  }

  int macro_block_check(
      const BlockIDSet &block_set,
      const uint64_t min_block_id,
      const uint64_t max_block_id);

  int build_macro_block(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &get_gc_blocks_op);


  VIRTUAL_TO_STRING_KV(K_(tablet_id));
private:
  int delete_tablet_meta_versions_(
      const ObIArray<int64_t> &tablet_versions,
      int64_t gc_tablet_meta_ts);
  int gc_blocks_(
    const ObIArray<int64_t> &tablet_versions,
    int64_t gc_tablet_meta_ts,
    GCBaseOP &op);

  int64_t get_min_retain_tablet_meta_version_(
    const ObIArray<int64_t> &tablet_versions,
    int64_t min_retain_tablet_meta_ts)
  {
    int64_t ret = INT64_MAX;
    for (int64_t i = 0; i < tablet_versions.count(); i++) {
      const int64_t tablet_version = tablet_versions.at(i);
      if (tablet_version >= min_retain_tablet_meta_ts
          && tablet_version < ret) {
        ret = tablet_version;
      }
    }
    if (INT64_MAX == ret) {
      // if all version is less min_retain_tablet_meta_ts, 
      // min_retain_tablet_meta_version is max version of all version.
      ret = -1;
      for (int64_t i = 0; i < tablet_versions.count(); i++) {
        const int64_t tablet_version = tablet_versions.at(i);
        if (tablet_version > ret) {
          ret = tablet_version;
        }
      }
    }
    return ret;
  }

protected:
  const common::ObTabletID tablet_id_;
  // for statistics
public:
  int64_t gc_macro_block_cnt_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_BLOCK_GC_H_ */
