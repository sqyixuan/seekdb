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
#ifndef OB_SHARE_STORAGE_COMPACTION_LS_MERGE_SCHEDULE_ITERATOR_H_
#define OB_SHARE_STORAGE_COMPACTION_LS_MERGE_SCHEDULE_ITERATOR_H_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/compaction/ob_compaction_schedule_iterator.h"
#include "storage/compaction/ob_ls_compaction_obj_mgr.h"
namespace oceanbase
{
namespace storage
{
class ObLS;
class ObLSHandle;
class ObTabletHandle;
class ObLSTabletService;
}

namespace compaction
{
struct ObLSBroadcastInfo;

class ObLSMergeIterator
{
public:
  ObLSMergeIterator();
  ~ObLSMergeIterator() { reset(); }
  void reset();
  int build_iter(const int64_t merge_version);
  int get_next_ls(storage::ObLSHandle &ls_handle);
  OB_INLINE void skip_cur_ls() { ++ls_idx_; }
  TO_STRING_KV(K_(merge_version), K_(ls_idx), K_(ls_ids));
private:
  int64_t merge_version_;
  int64_t ls_idx_;
  common::ObSEArray<share::ObLSID, 10> ls_ids_;
};

} // compaction
} // oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_LS_MERGE_SCHEDULE_ITERATOR_H_
