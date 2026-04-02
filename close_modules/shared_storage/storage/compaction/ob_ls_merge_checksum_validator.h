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
#ifndef OB_SHARE_STORAGE_COMPACTION_LS_MERGE_CHECKSUM_CHECKER_H_
#define OB_SHARE_STORAGE_COMPACTION_LS_MERGE_CHECKSUM_CHECKER_H_
#include "lib/literals/ob_literals.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashset.h"
#include "share/scn.h"
#include "rootserver/freeze/ob_checksum_validator.h"
#include "storage/compaction/ob_ls_compaction_list.h"


namespace oceanbase
{
namespace share
{
class ObLSID;
class ObTabletLSPair;
struct ObTabletChecksumItem;
struct ObTabletReplicaChecksumItem;
namespace schema
{
class ObSchemaGetterGuard;
class ObSimpleTableSchemaV2;
}
}
namespace common
{
class ObTabletID;
}

namespace compaction
{
struct ObTableCkmItems;
struct ObTabletLSPairCache;
class ObLSCompactionListObj;
typedef typename common::hash::ObHashSet<share::ObLSID, hash::NoPthreadDefendMode> LSIDSet;
typedef typename LSIDSet::const_iterator LSIDIter;


enum ObLSVerifyCkmType : uint8_t
{
  VERIFY_INVALID = 0,
  VERIFY_INDEX_CKM = 1,
  VERIFY_CROSS_CLUSTER_CKM = 2,
  VERIFY_NONE = 3
};


struct ObLSChecksumVerifyStat
{
public:
  ObLSChecksumVerifyStat() { reset(); }
  ~ObLSChecksumVerifyStat() = default;
  OB_INLINE void reset()
  {
    tenant_table_cnt_ = 0;
    data_table_cnt_ = 0;
    check_table_cnt_ = 0;
    verified_table_cnt_ = 0;
    skip_table_cnt_ = 0;
  }

  TO_STRING_KV(K_(tenant_table_cnt), K_(data_table_cnt), K_(check_table_cnt), K_(verified_table_cnt), K_(skip_table_cnt));
public:
  uint32_t tenant_table_cnt_;
  uint32_t data_table_cnt_;
  uint32_t check_table_cnt_;
  uint32_t verified_table_cnt_;
  uint32_t skip_table_cnt_;
};


class ObLSChecksumValidator final
{
public:
  ObLSChecksumValidator(
    const uint64_t tenant_id,
    const LSIDSet &ls_ids,
    const compaction::ObTabletLSPairCache &tablet_ls_pair_cache,
    common::ObMySQLProxy &sql_proxy);
  ~ObLSChecksumValidator();
  void clear_cached_info();
  int do_work(
      const share::ObFreezeInfo &freeze_info,
      const ObLSVerifyCkmType &verify_type);
  int init();
  int set_basic_info(
      const share::ObFreezeInfo &freeze_info,
      const ObLSVerifyCkmType &verify_type,
      share::schema::ObMultiVersionSchemaService *schema_service);
  int validate_special_table();
  TO_STRING_KV(K_(tenant_id), K_(ls_ids), K_(verify_type), K_(freeze_info), K_(verify_stat));
private:
  share::SCN get_compaction_scn() const { return freeze_info_.frozen_scn_; }
  int validate_index_checksum();
  int validate_cross_cluster_checksum();
  int prepare_validate_tables();
  int check_table_need_verify(
      const uint64_t table_id,
      const share::schema::ObSimpleTableSchemaV2 &simple_schema,
      bool &need_verify);
  int validate_table_index_checksum(const uint64_t table_id);
  int validate_table_crossed_cluster_checksum(const uint64_t table_id);
  int verify_table_index(
      const int64_t data_table_id,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int check_crossed_cluster_column_checksum(
      const share::ObReplicaCkmArray &standby_ckm_items,
      const ObIArray<share::ObTabletChecksumItem> &primary_ckm_items);
  int deal_with_the_rest_table();
  int get_tenant_schema_guard(share::schema::ObSchemaGetterGuard &schema_guard);
  int check_schema_version();
  int get_tablet_ls_pairs(
      const uint64_t table_id,
      const share::schema::ObSimpleTableSchemaV2 &simple_schema);
  int get_table_first_tablet_id(
      const uint64_t table_id,
      const share::schema::ObSimpleTableSchemaV2 &simple_schema,
      common::ObTabletID &tablet_id,
      common::ObIArray<ObTabletID> &tablet_ids);
  int push_tablet_ckm_items_with_update(
      const common::ObIArray<share::ObTabletReplicaChecksumItem> &replica_ckm_items);
  int batch_write_tablet_ckm();
  int check_need_verify(
    const uint64_t table_id,
    const share::SCN &compaction_scn,
    const ObTableCkmItems &table_items,
    bool &need_verify);
  int filter_skip_merge_table(
    const int64_t table_id,
    const common::ObIArray<ObTabletID> &tablet_list,
    bool &need_verify);
  int get_obj_from_cache(
    const share::ObLSID &ls_id,
    const compaction::ObLSCompactionListObj *&list_obj);
private:
  static const int64_t SPECIAL_TABLE_ID = 1;
  static const int64_t MAX_BATCH_INSERT_COUNT = 1500;
  static const int64_t TABLE_ID_BATCH_CHECK_SIZE = 200;
  static const int64_t MAX_RETRY_CNT = 5;
  static const int64_t DEFAULT_TABLET_CNT = 32;
  static const int64_t DEFAULT_BUCKET_CNT = 7;
  const uint64_t tenant_id_;
  const LSIDSet &ls_ids_;
  const compaction::ObTabletLSPairCache &tablet_ls_pair_cache_;
  common::ObMySQLProxy &sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObLSVerifyCkmType verify_type_;
  share::ObFreezeInfo freeze_info_;
  share::ObReplicaCkmArray replica_ckm_items_;
  common::ObArray<uint64_t> validate_tables_;
  common::ObArray<uint64_t> rest_validate_tables_;
  common::ObArray<const share::schema::ObSimpleTableSchemaV2 *> index_schemas_;
  common::ObArray<share::ObTabletLSPair> cur_tablet_ls_pair_array_;
  common::ObSEArray<share::ObTabletChecksumItem, 128> finish_tablet_ckm_array_;
  ObLSChecksumVerifyStat verify_stat_;
  common::ObArenaAllocator obj_allocator_;
  hash::ObHashMap<share::ObLSID, compaction::ObLSCompactionListObj *> merge_list_cache_;
};



} //compaction
} // oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_LS_MERGE_CHECKSUM_CHECKER_H_
