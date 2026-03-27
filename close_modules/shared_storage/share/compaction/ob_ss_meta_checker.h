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

#ifndef OB_SHARE_STORAGE_SHARE_COMPACTION_SS_META_CHECKER_H_
#define OB_SHARE_STORAGE_SHARE_COMPACTION_SS_META_CHECKER_H_

#include "share/tablet/ob_tablet_info.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
}

namespace compaction
{

/* In shared storage mode:
 * 1. __all_tablet_meta_table no longer needs to be reported,
 *    therefore, MetaChecker doesn't need to check the status of __all_tablet_meta_table any more.
 * 2. For tenant merge, the exec replica needs to report __all_tablet_replica_checksum,
 *    which will be used to validate ckm between replicas later.
 *    Therefore, we need to deal with the case where the ckm table fails to report
 * In summary, in shared storge mode:
 * it is necessary to use MetaChecker to check the status of __all_tablet_replica_checksum with replicas.
 */
class ObTenantSSMetaChecker
{
public:
  static int check_tablet_table();

private:
  static const int64_t TABLET_REPLICA_MAP_BUCKET_NUM = 64 * 1024;
  typedef common::hash::ObHashMap<share::ObTabletLSPair, share::ObTabletReplica> ObTabletReplicaMap;
  static int build_replica_map(ObTabletReplicaMap &replica_map);
  static int check_inner_table_replicas(ObTabletReplicaMap &replica_map);
  static int check_local_tablet_exist(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      bool &not_exist);
  static int check_local_ls_replicas(ObTabletReplicaMap &replica_map, int64_t &report_count);
  static int check_local_tablet_replicas(storage::ObLS *ls, ObTabletReplicaMap &replica_map, int64_t &report_count);
  static void check_local_tablet(
      const share::ObLSID &ls_id,
      storage::ObTablet *tablet,
      ObTabletReplicaMap &replica_map,
      int64_t &report_count);
};


} // compaction
} // oceanbase

#endif // OB_SHARE_STORAGE_SHARE_COMPACTION_SS_META_CHECKER_H_
