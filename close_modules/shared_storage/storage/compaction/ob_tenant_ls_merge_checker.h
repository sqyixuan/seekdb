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
#ifndef OB_SHARE_STORAGE_COMPACTION_TENANT_LS_MERGE_CHECKER_H_
#define OB_SHARE_STORAGE_COMPACTION_TENANT_LS_MERGE_CHECKER_H_
#include "lib/literals/ob_literals.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashset.h"
#include "storage/compaction/ob_ls_merge_schedule_iterator.h"
#include "rootserver/freeze/ob_major_merge_progress_util.h"
#include "storage/compaction/ob_ls_merge_checksum_validator.h"


namespace oceanbase
{
namespace compaction
{

class ObTenantLSMergeChecker final
{
public:
  enum LSMergeCheckState : uint16_t
  {
    CHECK_STATE_IDLE = 0,
    CHECK_STATE_VERIFY = 1,
    CHECK_STATE_REPORT = 2,
    CHECK_STATE_CKM_ERROR = 3
  };

  ObTenantLSMergeChecker();
  virtual ~ObTenantLSMergeChecker();
  static int mtl_init(ObTenantLSMergeChecker *&checker) { return checker->init(); }
  int init();
  void destroy();
  int process();
  void set_merge_error() { update_state(CHECK_STATE_CKM_ERROR); }
  int clear_merge_error();
  bool check_exist_ckm_error() { return CHECK_STATE_CKM_ERROR == get_state(); }

  TO_STRING_KV(K_(is_inited), K_(compaction_scn), K_(verified_scn),
               K_(check_state), K_(validate_type), K_(validate_ls_ids));
private:
  int check_can_validate();
  int get_validate_type();
  int update_ls_compaction_state();
  void update_state(const LSMergeCheckState state);
  LSMergeCheckState get_state() { return ATOMIC_LOAD(&check_state_); }
public:
  static const int64_t DEFAULT_MAP_BUCKET = 8;
  static const int64_t TABLET_ID_BUCKET_SIZE = 10000;
private:
  bool is_inited_;
  int64_t compaction_scn_;
  int64_t verified_scn_;
  LSMergeCheckState check_state_;
  ObLSVerifyCkmType validate_type_;
  common::ObMySQLProxy *sql_proxy_;
  // record ls id which should be verified on current server
  LSIDSet validate_ls_ids_;
  compaction::ObTabletLSPairCache tablet_ls_pair_cache_;
  ObLSChecksumValidator ckm_validator_;
};


} // compaction
} // oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_TENANT_LS_MERGE_CHECKER_H_
