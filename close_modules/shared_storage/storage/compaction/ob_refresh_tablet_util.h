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
#ifndef STORAGE_COMPACTION_OB_REFRESH_TABLET_UTIL_H_
#define STORAGE_COMPACTION_OB_REFRESH_TABLET_UTIL_H_
#include "/usr/include/stdint.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
namespace oceanbase
{
namespace common
{
class ObTabletID;
class ObArenaAllocator;
}
namespace storage
{
class ObLS;
class ObTablet;
class ObTabletTableStore;
class ObStorageSchema;
}
namespace blocksstable
{
class ObSSTable;
}
namespace compaction
{
struct ObNewMicroInfo;

struct ObDownloadTabletMetaParam final
{
public:
  ObDownloadTabletMetaParam();
  ObDownloadTabletMetaParam(const int64_t snapshot_version,
                            const bool allow_dup_major,
                            const bool init_major_ckm_info,
                            const bool need_prewarm);
  ~ObDownloadTabletMetaParam();
  TO_STRING_KV(K_(snapshot_version), K_(allow_dup_major), K_(init_major_ckm_info), K_(need_prewarm));
public:
  int64_t snapshot_version_;
  bool allow_dup_major_;
  bool init_major_ckm_info_;
  bool need_prewarm_;
};

struct ObUpdateTabletMetaParam final
{
public:
  ObUpdateTabletMetaParam();
  ObUpdateTabletMetaParam(const int64_t pre_warm_snapshot_version,
                          const bool allow_dup_major,
                          const bool init_major_ckm_info);
  ~ObUpdateTabletMetaParam();
  bool is_valid() const;
  TO_STRING_KV(K_(pre_warm_snapshot_version), K_(allow_dup_major), K_(init_major_ckm_info));
public:
  int64_t pre_warm_snapshot_version_;
  bool allow_dup_major_;
  bool init_major_ckm_info_;
};

struct ObRefreshTabletUtil
{
  /**
  * init_major_ckm_info = true, will only serialize major ckm info to local tablet
  * init_major_ckm_info = false, will serialize major sstable to local tablet
  */
  static int download_major_compaction_tablet_meta(
      storage::ObLS &ls,
      const common::ObTabletID &tablet_id,
      const ObDownloadTabletMetaParam &param);

  /*
  * 1) download tablet meta
  * 2) update tablet_state in cur_svr_ls_compaction_status_
  * 3) add ckm-report job
  */
  static int download_major_ckm_info(
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const compaction::ObNewMicroInfo *new_micro_info,
      storage::ObLS &ls);
  static int get_shared_tablet_meta(
      common::ObArenaAllocator &allocator,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      storage::ObTablet &shared_tablet);
  static int fetch_sstable(
      const storage::ObTablet &shared_tablet,
      storage::ObTabletMemberWrapper<storage::ObTabletTableStore> &table_store_wrapper,
      blocksstable::ObSSTable *&last_major);
  static int update_tablet_meta(
      storage::ObLS &ls,
      const storage::ObTablet &shared_tablet,
      const ObUpdateTabletMetaParam &param, 
      const int64_t table_multi_version_start);
};

} // namespace compaction
} // namespace oceanbase

#endif // STORAGE_COMPACTION_OB_REFRESH_TABLET_UTIL_H_
