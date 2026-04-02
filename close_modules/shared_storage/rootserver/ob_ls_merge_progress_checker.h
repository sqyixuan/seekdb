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
#ifndef OB_SHARE_STORAGE_ROOTSERVER_LS_MERGE_PROGRESS_CHECKER_H_
#define OB_SHARE_STORAGE_ROOTSERVER_LS_MERGE_PROGRESS_CHECKER_H_

#include "rootserver/freeze/ob_major_merge_progress_checker.h"
#include "rootserver/freeze/ob_major_merge_progress_util.h"

namespace oceanbase
{
namespace rootserver
{

class ObLSMergeProgressChecker : public ObBasicMergeProgressChecker
{
public:
  ObLSMergeProgressChecker(
    const uint64_t tenant_id,
    volatile bool &stop);
  virtual ~ObLSMergeProgressChecker() {}
  virtual const compaction::ObBasicMergeProgress &get_merge_progress() const override
  {
    return merge_progress_;
  }
  virtual int init(
      const bool is_primary_service,
      common::ObMySQLProxy &sql_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service,
      share::ObIServerTrace &server_trace,
      ObMajorMergeInfoManager &merge_info_mgr) override;
  virtual int set_basic_info(
      const share::ObFreezeInfo &freeze_info,
      const int64_t expected_epoch) override;
  virtual int clear_cached_info() override;
  virtual int check_progress() override;
private:
  share::SCN get_compaction_scn() const { return freeze_info_.frozen_scn_; }
  int64_t get_compaction_scn_val() const { return get_compaction_scn().get_val_for_tx(); }
  int refresh_ls_infos();
  int verify_special_table();
  int check_exist_ckm_error();
private:
  bool is_inited_;
  volatile bool &stop_;
  bool is_primary_service_;
  uint64_t tenant_id_;
  share::ObFreezeInfo freeze_info_;
  uint64_t expected_epoch_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  compaction::ObLSMergeProgress merge_progress_;
  compaction::ObTabletLSPairCache tablet_ls_pair_cache_;
  common::ObSEArray<share::ObLSID, 4> ls_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObLSMergeProgressChecker);
};


} // rootserver
} // oceanbase

#endif // OB_SHARE_STORAGE_ROOTSERVER_LS_MERGE_PROGRESS_CHECKER_H_
