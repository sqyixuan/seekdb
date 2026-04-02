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
 
#ifndef OCEANBASE_SHARED_STORAGE_OBSERVER_SYS_TABLE_OB_ALL_TABLET_CHECKSUM_ERROR_INFO_OPERATOR_
#define OCEANBASE_SHARED_STORAGE_OBSERVER_SYS_TABLE_OB_ALL_TABLET_CHECKSUM_ERROR_INFO_OPERATOR_

#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "share/ob_ls_id.h"
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
namespace share
{
struct ObTabletCkmErrorInfo
{
public:
  ObTabletCkmErrorInfo()
   : tenant_id_(OB_INVALID_TENANT_ID),
     ls_id_(0),
     tablet_id_(0),
     compaction_scn_(0),
     check_error_info_()
   {}
  ObTabletCkmErrorInfo(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t compaction_scn)
   : tenant_id_(tenant_id),
     ls_id_(ls_id.id()),
     tablet_id_(tablet_id.id()),
     compaction_scn_(compaction_scn),
     check_error_info_()
   {}
  void set_info(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t compaction_scn)
  {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id.id();
    tablet_id_ = tablet_id.id();
    compaction_scn_ = compaction_scn;
  }
  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_) && ls_id_ > 0 && tablet_id_ > 0 && compaction_scn_ > 0;
  }
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_id_ = 0;
    tablet_id_ = 0;
    compaction_scn_ = 0;
    MEMSET(check_error_info_, '\0', sizeof(char) * OB_CKM_ERROR_INFO_STR_LENGTH);
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(compaction_scn), K_(check_error_info));
public:
  uint64_t tenant_id_;
  int64_t ls_id_;
  int64_t tablet_id_;
  int64_t compaction_scn_;
  char check_error_info_[OB_CKM_ERROR_INFO_STR_LENGTH];
};

class ObTabletLSPair;
// CRUD operation to __all_tablet_checksum_error_info
class ObTabletCkmErrorInfoOperator
{
public:
  static int write_tablet_ckm_error_info(const ObTabletCkmErrorInfo &error_info);
  static int check_exist_ckm_error_tablet(const uint64_t tenant_id, const int64_t compaction_scn, bool &exist);
  static int get_ckm_error_tablets(const uint64_t tenant_id, const int64_t compaction_scn, ObIArray<share::ObTabletLSPair> &error_tablet);
  static int delete_ckm_error_info(const uint64_t tenant_id, const int64_t compaction_scn);
private:
  static int construct_ls_tablet_pair_array(
      sqlclient::ObMySQLResult &result,
      common::ObIArray<share::ObTabletLSPair> &ls_tablet_array);
  static int basic_check();
};

} // end namespace share
} // end namespace oceanbase

#endif  // OCEANBASE_SHARED_STORAGE_OBSERVER_SYS_TABLE_OB_ALL_TABLET_CHECKSUM_ERROR_INFO_OPERATOR_
