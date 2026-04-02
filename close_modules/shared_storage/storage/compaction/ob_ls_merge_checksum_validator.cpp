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

#include "storage/compaction/ob_ls_merge_checksum_validator.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "observer/sys_table/ob_all_tablet_checksum_error_info_operator.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"


namespace oceanbase
{
using namespace common;
using namespace share;
using namespace lib;

namespace compaction
{

ObLSChecksumValidator::ObLSChecksumValidator(
    const uint64_t tenant_id,
    const LSIDSet &ls_ids,
    const compaction::ObTabletLSPairCache &tablet_ls_pair_cache,
    common::ObMySQLProxy &sql_proxy)
  : tenant_id_(tenant_id),
    ls_ids_(ls_ids),
    tablet_ls_pair_cache_(tablet_ls_pair_cache),
    sql_proxy_(sql_proxy),
    schema_service_(nullptr),
    verify_type_(ObLSVerifyCkmType::VERIFY_NONE),
    freeze_info_(),
    replica_ckm_items_(),
    validate_tables_(),
    rest_validate_tables_(),
    index_schemas_(),
    cur_tablet_ls_pair_array_(),
    finish_tablet_ckm_array_(),
    verify_stat_(),
    obj_allocator_("ListObjAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
    merge_list_cache_()
{
}

ObLSChecksumValidator::~ObLSChecksumValidator()
{
  clear_cached_info();
}

void ObLSChecksumValidator::clear_cached_info()
{
  verify_type_ = ObLSVerifyCkmType::VERIFY_NONE;
  freeze_info_.reset();
  replica_ckm_items_.reset();
  validate_tables_.reuse();
  rest_validate_tables_.reuse();
  index_schemas_.reuse();
  cur_tablet_ls_pair_array_.reuse();
  finish_tablet_ckm_array_.reuse();
  verify_stat_.reset();

  hash::ObHashMap<share::ObLSID, ObLSCompactionListObj*>::iterator iter = merge_list_cache_.begin();
  for ( ; iter != merge_list_cache_.end(); ++iter) {
    ObLSCompactionListObj *obj_ptr = iter->second;
    if (OB_NOT_NULL(obj_ptr)) {
      obj_ptr->~ObLSCompactionListObj();
      obj_allocator_.free(obj_ptr);
      obj_ptr = nullptr;
    }
  }
  merge_list_cache_.destroy();
  obj_allocator_.clear();
}

int ObLSChecksumValidator::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replica_ckm_items_.init(tenant_id_, DEFAULT_TABLET_CNT))) {
    LOG_WARN("failed to init ckm array", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObLSChecksumValidator::set_basic_info(
    const ObFreezeInfo &freeze_info,
    const ObLSVerifyCkmType &verify_type,
    share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!freeze_info.is_valid() || nullptr == schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(freeze_info), K(schema_service));
  } else {
    freeze_info_ = freeze_info;
    schema_service_ = schema_service;
    verify_type_ = verify_type;
  }
  return ret;
}

int ObLSChecksumValidator::do_work(
    const ObFreezeInfo &freeze_info,
    const ObLSVerifyCkmType &verify_type)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = nullptr;

  if (ObLSVerifyCkmType::VERIFY_NONE == verify_type) {
    // do nothing
  } else if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  } else if (OB_FAIL(set_basic_info(freeze_info, verify_type, schema_service))) {
    LOG_WARN("failed to set basic info", K(ret), K(freeze_info), K(verify_type));
  } else if (OB_FAIL(prepare_validate_tables())) {
    LOG_WARN("failed to prepare validate tables", K(ret), "compaction_scn", get_compaction_scn());
  } else if (ObLSVerifyCkmType::VERIFY_INDEX_CKM == verify_type_) {
    if (OB_FAIL(validate_index_checksum())) {
      LOG_WARN("failed to validate index checksum", K(ret));
    }
  } else if (ObLSVerifyCkmType::VERIFY_CROSS_CLUSTER_CKM == verify_type_) {
    if (OB_FAIL(validate_cross_cluster_checksum())) {
      LOG_WARN("failed to validate cross cluster checksum", K(ret));
    }
  }
  return ret;
}

int ObLSChecksumValidator::validate_special_table()
{
  int ret = OB_SUCCESS;

  if (ObLSVerifyCkmType::VERIFY_INDEX_CKM == verify_type_) {
    if (OB_FAIL(validate_table_index_checksum(SPECIAL_TABLE_ID))) {
      LOG_WARN("failed to validate special table index checksum", K(ret));
    }
  } else if (ObLSVerifyCkmType::VERIFY_CROSS_CLUSTER_CKM == verify_type_) {
    if (OB_FAIL(validate_table_crossed_cluster_checksum(SPECIAL_TABLE_ID))) {
      LOG_WARN("failed to validate special table index checksum", K(ret));
    }
  }
  return ret;
}

int ObLSChecksumValidator::prepare_validate_tables()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObLSVerifyCkmType::VERIFY_NONE == verify_type_)) {
    // do nothing
  } else if (OB_FAIL(check_schema_version())) {
    LOG_WARN("failed to check schema version", K(ret));
  } else {
    SMART_VAR(ObArray<uint64_t>, table_list) {
      // get tenant table list
      {
        ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK);

        if (OB_FAIL(get_tenant_schema_guard(schema_guard))) {
          LOG_WARN("failed to get tenant schema guard", K(ret));
        } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id_, table_list))) {
          LOG_WARN("failed to get table ids in tenant", KR(ret));
        } else if (OB_FAIL(validate_tables_.reserve(table_list.count()))) {
          LOG_WARN("failed to reserve index table id array", K(ret));
        } else {
          verify_stat_.tenant_table_cnt_ = table_list.count();
        }
      }

      // get tables that need to be verified
      int64_t start_idx = 0;
      int64_t end_idx = 0;
      while (OB_SUCC(ret) && end_idx < table_list.count()) {
        ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK); // temp schema guard to loop table_id array
        start_idx = end_idx;
        end_idx = MIN(table_list.count(), start_idx + TABLE_ID_BATCH_CHECK_SIZE);
        if (OB_FAIL(get_tenant_schema_guard(schema_guard))) {
          LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
        }

        for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
          const uint64_t table_id = table_list.at(i);
          const ObSimpleTableSchemaV2 *simple_schema = nullptr;
          bool can_validate = false;

          if (SPECIAL_TABLE_ID == table_id) {
            // do nothing, deal with special table in RS
          } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, simple_schema))) {
            LOG_WARN("failed to get table schema", K(ret), K(table_id));
          } else if (OB_ISNULL(simple_schema) || !simple_schema->should_check_major_merge_progress()) {
            // should ignore cur table
          } else if (!simple_schema->has_tablet()) {
            // tablet has no tablet, no need to check
          } else if (simple_schema->is_index_table() && !simple_schema->can_read_index()) {
            // no need to validate not-ready index table
          } else if (simple_schema->is_index_table() && ObLSVerifyCkmType::VERIFY_INDEX_CKM == verify_type_) {
            // index table should be verified by data table
          } else if (FALSE_IT(++verify_stat_.data_table_cnt_)) {
          } else if (OB_FAIL(check_table_need_verify(table_id, *simple_schema, can_validate))) {
            LOG_WARN("failed to check table can validate", K(ret), K(table_id), KPC(simple_schema));
          } else if (!can_validate) {
            // do nothing
          } else if (OB_FAIL(validate_tables_.push_back(table_id))) {
            LOG_WARN("failed to add index table id", K(ret), K(table_id));
          }
        } // end for
      } // end while

      if (OB_SUCC(ret)) {
        verify_stat_.check_table_cnt_ = validate_tables_.count();
        LOG_INFO("[SS_MERGE] Succ to prepare verify tables", K(ret), KPC(this));
      }
    }
  }
  return ret;
}

int ObLSChecksumValidator::check_table_need_verify(
    const uint64_t table_id,
    const ObSimpleTableSchemaV2 &simple_schema,
    bool &need_verify)
{
  int ret = OB_SUCCESS;
  int exist_ret = OB_SUCCESS;
  share::ObLSID first_tablet_ls_id;
  ObTabletID first_tablet_id;
  need_verify = false;

  SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
    if (OB_FAIL(get_table_first_tablet_id(table_id, simple_schema, first_tablet_id, tablet_ids))) {
      LOG_WARN("failed to get first tablet id for table", K(ret), K(table_id));
    } else if (OB_FAIL(tablet_ls_pair_cache_.get_tablet_ls_id(table_id, first_tablet_id, first_tablet_ls_id))) {
      LOG_WARN("failed to get tablet ls pair array", K(ret));
    } else if (FALSE_IT(exist_ret = ls_ids_.exist_refactored(first_tablet_ls_id))) {
    } else if (OB_HASH_NOT_EXIST == exist_ret) {
      LOG_DEBUG("ls is not leader on curr svr, should be verified on other svr", K(table_id), K(first_tablet_id), K(first_tablet_ls_id));
    } else if (OB_HASH_EXIST != exist_ret) {
      ret = exist_ret;
      LOG_WARN("failed to check ls id exists", K(ret), K(table_id), K(first_tablet_ls_id));
    } else if (OB_FAIL(filter_skip_merge_table(table_id, tablet_ids, need_verify))) {
      LOG_WARN("failed to filter skip merge table", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObLSChecksumValidator::filter_skip_merge_table(
    const int64_t table_id,
    const ObIArray<ObTabletID> &tablet_list,
    bool &need_verify)
{
  int ret = OB_SUCCESS;
  need_verify = true;
  ObArray<share::ObTabletLSPair> ls_tablet_pairs;

  if (OB_FAIL(tablet_ls_pair_cache_.get_tablet_ls_pairs(table_id, tablet_list, ls_tablet_pairs))) {
    LOG_WARN("failed to get tablet ls tablet pairs", K(ret), K(table_id));
  } else if (OB_UNLIKELY(ls_tablet_pairs.empty())) {
    // do nothing
  } else {
    std::sort(ls_tablet_pairs.begin(), ls_tablet_pairs.end(),
              [](const share::ObTabletLSPair &a, const share::ObTabletLSPair &b) {
                  return a.get_ls_id() < b.get_ls_id(); });
  }

  ObLSID prev_ls_id = ObLSID(ObLSID::INVALID_LS_ID);
  ObBasicObjHandle<ObLSObj> ls_obj_hdl;
  const ObLSCompactionListObj *ls_list_obj = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < ls_tablet_pairs.count(); ++idx) {
    const ObLSID &ls_id = ls_tablet_pairs.at(idx).get_ls_id();
    const ObTabletID &tablet_id = ls_tablet_pairs.at(idx).get_tablet_id();

    if (ls_id == prev_ls_id && nullptr != ls_list_obj) {
      // no need to get list obj again
    } else if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_id, ls_obj_hdl))) {
      if (OB_LS_NOT_EXIST != ret) {
        LOG_WARN("failed to get ls obj handle", K(ret), K(ls_id));
      } else if (OB_FAIL(get_obj_from_cache(ls_id, ls_list_obj))) { // ls not exists on cur svr, read ss instead
        LOG_WARN("failed to get list obj from cache", K(ret), K(ls_id));
      }
    } else {
      ls_list_obj = &ls_obj_hdl.get_obj()->get_ls_compaction_list();
    }

    bool need_skip = false;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ls_list_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null ls list obj", K(ret), K(ls_id));
    } else if (FALSE_IT(prev_ls_id = ls_id)) {
    } else if (OB_FAIL(ls_list_obj->tablet_need_skip(get_compaction_scn().get_val_for_tx(),
                                                     tablet_id,
                                                     need_skip))) {
      LOG_WARN("failed to check tablet exist", K(ret), K(ls_id), K(tablet_id));
    } else if (need_skip) {
      need_verify = false;
      FLOG_INFO("table skip to verify", K(table_id), K(ls_id), K(tablet_id));
      break;
    }
  } // end for
  return ret;
}

int ObLSChecksumValidator::get_obj_from_cache(
    const share::ObLSID &ls_id,
    const compaction::ObLSCompactionListObj *&list_obj)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  compaction::ObLSCompactionListObj *tmp_obj_ptr = nullptr;

  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(ls_id));
  } else if (OB_UNLIKELY(!merge_list_cache_.created())) {
    if (OB_FAIL(merge_list_cache_.create(DEFAULT_BUCKET_CNT, lib::ObMemAttr(tenant_id_, "MrgListCache")))) {
      LOG_WARN("failed to create merge list cache", K(ret), K_(tenant_id));
    } else {
      ret = OB_HASH_NOT_EXIST;
    }
  } else {
    ret = merge_list_cache_.get_refactored(ls_id, tmp_obj_ptr);
  }

  if (OB_SUCC(ret)) {
    // do nothing
  } else if (OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("failed to get list obj from cache", K(ret), K(ls_id));
  } else if (OB_ISNULL(buf = obj_allocator_.alloc(sizeof(ObLSCompactionListObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem for list obj", K(ret), K(ls_id));
  } else if (FALSE_IT(tmp_obj_ptr = new (buf) ObLSCompactionListObj())) {
  } else if (OB_FAIL(ObLSCompactionStatusObjLoader::load_ls_compaction_list(tenant_id_, ls_id, *tmp_obj_ptr))) {
    LOG_WARN("failed to load ls list obj", K(ret), K(ls_id));
  } else if (OB_FAIL(merge_list_cache_.set_refactored(ls_id, tmp_obj_ptr))) {
    LOG_WARN("failed to add list obj to cache", K(ret), K(ls_id));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_obj_ptr)) {
    tmp_obj_ptr->destroy();
    obj_allocator_.free(tmp_obj_ptr);
    tmp_obj_ptr = nullptr;
  } else {
    list_obj = tmp_obj_ptr;
  }
  return ret;
}

int ObLSChecksumValidator::validate_index_checksum()
{
  int ret = OB_SUCCESS;
  index_schemas_.reuse();
  rest_validate_tables_.reuse();

  for (int64_t i = 0; OB_SUCC(ret) && i < validate_tables_.count(); ++i) {
    const uint64_t data_table_id = validate_tables_.at(i);
    if (OB_FAIL(validate_table_index_checksum(data_table_id))) {
      LOG_WARN("failed to validate table index checksum", K(ret), K(data_table_id));
    } else if (REACH_THREAD_TIME_INTERVAL(60_s)) {
      LOG_INFO("[SS_MERGE] check ls checksum verify progress", KPC(this));
    }
  }

  if (FAILEDx(deal_with_the_rest_table())) {
    LOG_WARN("failed to deal with the rest table", K(ret));
  } else if (finish_tablet_ckm_array_.empty()) {
    // no need to write tablet ckm
  } else if (OB_FAIL(batch_write_tablet_ckm())) {
    LOG_WARN("failed to batch write tablet ckm", K(ret));
  }
  return ret;
}

int ObLSChecksumValidator::validate_table_index_checksum(
    const uint64_t data_table_id)
{
  int ret = OB_SUCCESS;
  index_schemas_.reuse();

  ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK);
  ObSEArray<const ObSimpleTableSchemaV2 *, 32> index_table_schemas;
  const ObSimpleTableSchemaV2 *data_table_schema = nullptr;

  if (OB_FAIL(get_tenant_schema_guard(schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, data_table_id, data_table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ++verify_stat_.skip_table_cnt_; // table not exist, no need to validate
  } else if (OB_UNLIKELY(data_table_schema->is_index_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected index table", K(ret), K(data_table_id), KPC(data_table_schema), K(validate_tables_));
  } else if (OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(tenant_id_, data_table_id, index_table_schemas))) {
    LOG_WARN("failed to get index schemas for data table", K(ret));
  } else if (OB_FAIL(index_schemas_.reserve(index_table_schemas.count()))) {
    LOG_WARN("failed to reserve index schemas", K(ret));
  } else {
    // prepare index tables that should be validated
    for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schemas.count(); ++j) {
      const ObSimpleTableSchemaV2 *index_table_schema = index_table_schemas.at(j);
      const uint64_t index_table_id = index_table_schema->get_table_id();
      bool need_verify = true;

      if (OB_ISNULL(index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpeceted null index schema", K(ret), K(data_table_id));
      } else if (!index_table_schema->has_tablet()
              || !index_table_schema->can_read_index()) {
        // index table no need to validate
      } else if (OB_FAIL(check_table_need_verify(index_table_id, *index_table_schema, need_verify))) {
        LOG_WARN("failed to check whether table can validate", K(ret), K(index_table_id));
      } else if (!need_verify) {
        LOG_INFO("index table no need to validate ckm", K(ret), K(index_table_id));
      } else if (index_table_schema->should_not_validate_data_index_ckm()) {
        if (SPECIAL_TABLE_ID == data_table_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("special table has unexpected spatial index table", K(ret), K(data_table_id), K(index_table_id), KPC(index_table_schema));
        } else {
          ++verify_stat_.skip_table_cnt_;
          LOG_INFO("no need to validate full text index", K(index_table_id));
        }
      } else if (OB_FAIL(index_schemas_.push_back(index_table_schema))) {
        LOG_WARN("failed to add index schema", K(ret), KPC(index_table_schema));
      }
    }

    // validate checksum between main table and index tables
    if (FAILEDx(verify_table_index(data_table_id, schema_guard))) {
      LOG_WARN("failed to validate table index checksums", K(ret), K(data_table_id), KPC(data_table_schema));
    } else {
      ++verify_stat_.verified_table_cnt_;
    }
  }
  return ret;
}

int ObLSChecksumValidator::deal_with_the_rest_table()
{
  int ret = OB_SUCCESS;
  const int64_t rest_table_cnt = rest_validate_tables_.count();
  ObSEArray<ObTabletID, 64> tablet_id_array;

  SMART_VARS_2((ObArray<share::ObTabletLSPair>, tablet_pairs), (ObReplicaCkmArray, ckm_items, false/*need_map*/)) {
    if (OB_FAIL(ckm_items.init(tenant_id_, DEFAULT_TABLET_CNT))) {
      LOG_WARN("failed to build ckm array", KR(ret));
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < rest_table_cnt; ++idx) {
      tablet_id_array.reuse();

      ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK);
      const uint64_t table_id = rest_validate_tables_.at(idx);
      const share::schema::ObTableSchema *table_schema = nullptr;

      if (OB_FAIL(get_tenant_schema_guard(schema_guard))) {
        LOG_WARN("failed to get tenant schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
        LOG_WARN("failed to get table schema", K(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        // table schemas are changed, and index_table or data_table does not exist
        // in new table schemas. no need to check index column checksum.
        LOG_WARN("table schema is null", KR(ret), K(table_id), KP(table_schema));
      } else if (table_schema->is_index_table() && !table_schema->should_not_validate_data_index_ckm()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected index table", K(ret), K(table_id), KPC(table_schema), K(rest_validate_tables_));
      } else if (OB_FAIL(table_schema->get_tablet_ids(tablet_id_array))) {
        LOG_WARN("fail to get tablet_ids from table schema", KR(ret), KPC(table_schema));
      } else if (OB_FAIL(tablet_ls_pair_cache_.get_tablet_ls_pairs(table_id, tablet_id_array, tablet_pairs))) {
        LOG_WARN("failed to get tablet ls pairs", KR(ret), K_(tenant_id), K(table_id), K(tablet_id_array));
      } else if ((rest_table_cnt > idx + 1) && MAX_BATCH_INSERT_COUNT > tablet_pairs.count()) {
        // do nothing
      } else if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablet_replica_checksum_items(tenant_id_,
                                                                                            sql_proxy_,
                                                                                            get_compaction_scn(),
                                                                                            tablet_pairs,
                                                                                            ckm_items))) {
        LOG_WARN("failed to get table column checksum items", K(ret), K(tablet_pairs));
      } else if (OB_FAIL(push_tablet_ckm_items_with_update(ckm_items.get_array()))) {
        LOG_WARN("failed to push tablet ckm items", K(ret));
      } else {
        tablet_pairs.reuse();
        ckm_items.reset();
      }
    }
  }

  rest_validate_tables_.reuse();
  return ret;
}

int ObLSChecksumValidator::verify_table_index(
    const int64_t data_table_id,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  ObTableCkmItems data_table_ckm(tenant_id_);
  finish_tablet_ckm_array_.reuse();
  bool need_verify = true;

  if (OB_FAIL(data_table_ckm.build_for_s2(data_table_id,
                                          get_compaction_scn(),
                                          sql_proxy_,
                                          schema_guard,
                                          tablet_ls_pair_cache_))) {
    LOG_WARN("failed to get checksum items", K(ret), K(data_table_id), "compaction_scn", get_compaction_scn());
  } else if (OB_FAIL(check_need_verify(data_table_id, get_compaction_scn(), data_table_ckm, need_verify))) {
    LOG_WARN("failed to check need verify", K(ret), K(data_table_id), "compaction_scn", get_compaction_scn(), K(data_table_ckm));
  } else if (!need_verify) {
    LOG_INFO("curr data table no need to verify", K(ret), K(data_table_id));
  } else {
    // when verifying the rest tables, index schemas should be empty
    for (int64_t i = 0; OB_SUCC(ret) && i < index_schemas_.count(); ++i) {
      const ObSimpleTableSchemaV2 *index_schema = index_schemas_.at(i);
      const uint64_t index_table_id = index_schema->get_table_id();
      ObTableCkmItems index_table_ckm(tenant_id_);
      bool index_need_verify = true;

      if (OB_UNLIKELY(!index_schema->is_index_table() || data_table_id != index_schema->get_data_table_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is must be index", K(ret), K(data_table_id), KPC(index_schema));
      } else if (!index_schema->has_tablet()
              || !index_schema->can_read_index()
              || index_schema->should_not_validate_data_index_ckm()) {
        // do nothing
      } else if (OB_FAIL(get_tablet_ls_pairs(index_table_id, *index_schema))) {
        LOG_WARN("failed to get tablet ls pairs", K(ret), K(index_table_id), KPC(index_schema));
      } else if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_get(tenant_id_,
                                                                    cur_tablet_ls_pair_array_,
                                                                    get_compaction_scn(),
                                                                    sql_proxy_,
                                                                    replica_ckm_items_,
                                                                    true/*larger_than*/,
                                                                    share::OBCG_DEFAULT))) {
        LOG_WARN("failed to get replica ckm items", K(ret), "compaction_scn", get_compaction_scn(), K(index_table_id));
      } else if (OB_FAIL(index_table_ckm.build(schema_guard, *index_schema, cur_tablet_ls_pair_array_, replica_ckm_items_))) {
        LOG_WARN("failed to build index table ckm", K(ret), K(index_table_id));
      } else if (OB_FAIL(check_need_verify(index_table_id, get_compaction_scn(), index_table_ckm, index_need_verify))) {
        LOG_WARN("failed to check need verify", K(ret), K(data_table_id), "compaction_scn", get_compaction_scn(), K(data_table_ckm));
      } else if (!index_need_verify) {
        LOG_INFO("curr index table no need to verify", K(ret), K(data_table_id), K(index_table_id));
      } else if (OB_UNLIKELY(replica_ckm_items_.get_tablet_cnt() < cur_tablet_ls_pair_array_.count())) {
        ret = OB_ITEM_NOT_MATCH;
        LOG_WARN("failed to get tablet replica checksum items", K(ret), "compaction_scn", get_compaction_scn(), K(index_table_id),
            K(data_table_id), K(cur_tablet_ls_pair_array_), K(replica_ckm_items_));
      } else {
        const bool is_global_index = index_schema->is_global_index_table();
        if (OB_FAIL(ObTableCkmItems::validate_ckm_func[is_global_index](freeze_info_,
                                                                        sql_proxy_,
                                                                        data_table_ckm,
                                                                        index_table_ckm))) {
          LOG_WARN("failed to validate checksum", K(ret), K(data_table_id), K(index_table_id), K(is_global_index),
              K(data_table_ckm), K(index_table_ckm), K(replica_ckm_items_), K(cur_tablet_ls_pair_array_));
        } else if (OB_FAIL(push_tablet_ckm_items_with_update(index_table_ckm.get_ckm_items()))) {
          LOG_WARN("failed to push index table ckm items", K(ret), K(index_table_ckm));
        }
      }
    } // end for

    if (FAILEDx(push_tablet_ckm_items_with_update(data_table_ckm.get_ckm_items()))) {
      LOG_WARN("failed to push tablet ckm items", K(ret), K(data_table_ckm));
    }
  }
  return ret;
}

int ObLSChecksumValidator::check_need_verify(
    const uint64_t table_id,
    const share::SCN &compaction_scn,
    const ObTableCkmItems &table_items,
    bool &need_verify)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<share::ObTabletReplicaChecksumItem> &ckm_items = table_items.get_ckm_items();
  const common::ObIArray<share::ObTabletLSPair> &tablet_pairs = table_items.get_tablet_ls_pairs();
  need_verify = true;

  hash::ObHashSet<uint64_t> tablet_id_set;  // record all tablet_ids in @items
  if (ckm_items.empty()) {
    need_verify = false;
    LOG_INFO("All tablet in table has skipped merge, skip to verify cur table", K(table_id), K(compaction_scn), K(table_items));
  } else if (OB_FAIL(tablet_id_set.create(ckm_items.count()))) {
    LOG_WARN("fail to create tablet_id set", KR(ret), K(ckm_items));
  } else {
    SCN min_compaction_scn = SCN::max_scn();
    SCN max_compaction_scn = SCN::min_scn();
    // step 1. obtain min_compaction_scn/max_compaction_scn and tablet_ids of checksum_items
    for (int64_t i = 0; OB_SUCC(ret) && i < ckm_items.count(); ++i) {
      const SCN &cur_compaction_scn = ckm_items.at(i).compaction_scn_;
      if (cur_compaction_scn < min_compaction_scn) {
        min_compaction_scn = cur_compaction_scn;
      }
      if (cur_compaction_scn > max_compaction_scn) {
        max_compaction_scn = cur_compaction_scn;
      }

      const ObTabletID &cur_tablet_id = ckm_items.at(i).tablet_id_;
      if (OB_FAIL(tablet_id_set.set_refactored(cur_tablet_id.id()))) {
        LOG_WARN("fail to set refactored", KR(ret), K(cur_tablet_id));
      }
    }

    // step 2. check if exists checksum_items with compaction_scn larger than compaction_scn of major merge
    if (OB_SUCC(ret)) {
      if ((min_compaction_scn == compaction_scn) && (max_compaction_scn == compaction_scn)) {
        // do nothing
      } else if ((min_compaction_scn >= compaction_scn) && (max_compaction_scn > compaction_scn)) {
        // This means another medium compaction is launched. Thus, no need to verify checksum.
        need_verify = false;
        LOG_INFO("no need to verify checksum, cuz max_compaction_scn of checksum_items is larger "
                  "than compaction_scn of major compaction", K(min_compaction_scn),
                  K(max_compaction_scn), K(compaction_scn));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected compaction_scn of tablet_replica_checksum_items", KR(ret),
                  K(min_compaction_scn), K(max_compaction_scn), K(compaction_scn));
      }
    }

    // step 3. check if tablet_cnt in table_schema is equal to tablet_cnt in checksum_items
    if (OB_SUCC(ret) && need_verify) {
      int64_t ckm_tablet_cnt = tablet_id_set.size();
      if (tablet_pairs.count() != ckm_tablet_cnt) { // may be caused by truncate table
        need_verify = false;
        LOG_INFO("no need to verify checksum, cuz tablet_cnt in table_schema is not equal to "
          "tablet_cnt in checksum_items", "tablet_id_in_table_schema", tablet_pairs,
          "tablet_id_in_checksum_items", tablet_id_set);
      }
    }

    // step 4. check if each tablet in table_schema has tablet_replica_checksum_items
    FOREACH_CNT_X(tablet_pair, tablet_pairs, (OB_SUCCESS == ret) && need_verify) {
      if (OB_ISNULL(tablet_pair)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_id is null", KR(ret), K(tablet_pairs));
      } else if (OB_FAIL(tablet_id_set.exist_refactored(tablet_pair->get_tablet_id().id()))) {
        if (OB_HASH_NOT_EXIST == ret) { // may be caused by truncate table
          need_verify = false;
          ret = OB_SUCCESS;
          LOG_INFO("no need to verify checksum, cuz tablet in table_schema has no checksum_item",
                K(table_id), KPC(tablet_pair), K(ckm_items));
        } else if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to check tablet_id exist", KR(ret), KPC(tablet_pair));
        }
      }
    }
  }
  return ret;
}

int ObLSChecksumValidator::push_tablet_ckm_items_with_update(
    const ObIArray<ObTabletReplicaChecksumItem> &replica_ckm_items)
{
  int ret = OB_SUCCESS;
  ObTabletChecksumItem tmp_checksum_item;

  for (int64_t i = 0; OB_SUCC(ret) && i < replica_ckm_items.count(); ++i) {
    const ObTabletReplicaChecksumItem &curr_replica_item = replica_ckm_items.at(i);

    if (OB_UNLIKELY(!curr_replica_item.is_key_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet replica checksum is not valid", KR(ret), K(curr_replica_item));
    } else if (OB_FAIL(tmp_checksum_item.assign(curr_replica_item))) { // ObTabletReplicaChecksumItem->ObTabletChecksumItem
      LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(curr_replica_item));
    } else if (OB_FAIL(finish_tablet_ckm_array_.push_back(tmp_checksum_item))) {
      LOG_WARN("fail to push back tablet checksum item", KR(ret), K(curr_replica_item), K(tmp_checksum_item));
    }
  } // end of for

  if (OB_FAIL(ret)) {
  } else if (finish_tablet_ckm_array_.count() >= MAX_BATCH_INSERT_COUNT) {
    (void) batch_write_tablet_ckm();
  }
  return ret;
}

int ObLSChecksumValidator::validate_cross_cluster_checksum()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < validate_tables_.count(); ++i) {
    // validate table with __all_tablet_checksum
    const uint64_t table_id = validate_tables_.at(i);
    if (OB_FAIL(validate_table_crossed_cluster_checksum(table_id))) {
      LOG_WARN("failed to validate table crossed cluster checksum", K(ret), K(table_id));
    } else if (REACH_THREAD_TIME_INTERVAL(60_s)) {
      LOG_INFO("[SS_MERGE] check ls checksum verify progress", KPC(this));
    }
  }
  return ret;
}

int ObLSChecksumValidator::validate_table_crossed_cluster_checksum(
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  replica_ckm_items_.reset();

  SMART_VAR(common::ObArray<ObTabletChecksumItem>, primary_ckm_items) {
    ObSchemaGetterGuard schema_guard(ObSchemaMgrItem::MOD_RS_MAJOR_CHECK);
    const ObSimpleTableSchemaV2 *simple_schema = nullptr;

    if (OB_FAIL(get_tenant_schema_guard(schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id_, table_id, simple_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(table_id));
    } else if (OB_ISNULL(simple_schema)) {
      ++verify_stat_.skip_table_cnt_; // table not exist, no need to validate
    } else if (OB_FAIL(get_tablet_ls_pairs(table_id, *simple_schema))) {
      LOG_WARN("failed to get tablet ls pairs", K(ret), K(table_id));
    } else if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_get(tenant_id_,
                                                                  cur_tablet_ls_pair_array_,
                                                                  get_compaction_scn(),
                                                                  sql_proxy_,
                                                                  replica_ckm_items_,
                                                                  false/*larger_than*/,
                                                                  share::OBCG_DEFAULT))) {
      LOG_WARN("failed to get replica ckm items", K(ret), "compaction_scn", get_compaction_scn(), K(table_id));
    } else if (OB_FAIL(ObTabletChecksumOperator::load_tablet_checksum_items(sql_proxy_,
                                                                            cur_tablet_ls_pair_array_,
                                                                            tenant_id_,
                                                                            get_compaction_scn(),
                                                                            primary_ckm_items))) {
      LOG_WARN("fail to batch get tablet checksum items", KR(ret), "compaction_scn", get_compaction_scn());
    } else if (replica_ckm_items_.get_tablet_cnt() != primary_ckm_items.count()) {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("fail to get checksum items", KR(ret), "compaction_scn", get_compaction_scn(), K(replica_ckm_items_), K(primary_ckm_items));
    } else if (OB_FAIL(check_crossed_cluster_column_checksum(replica_ckm_items_, primary_ckm_items))) {
      if (OB_CHECKSUM_ERROR == ret) {
        LOG_ERROR("ERROR! ERROR! ERROR! checksum error in cross-cluster checksum", KR(ret),
          "compaction_scn", get_compaction_scn());
      } else {
        LOG_WARN("fail to check cross-cluster checksum", KR(ret), "compaction_scn", get_compaction_scn());
      }
    } else {
      ++verify_stat_.verified_table_cnt_;
    }
  }

  replica_ckm_items_.reset();
  return ret;
}

int ObLSChecksumValidator::check_crossed_cluster_column_checksum(
    const ObReplicaCkmArray &standby_ckm_items,
    const ObIArray<share::ObTabletChecksumItem> &primary_ckm_items)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tablet_cnt = 0;

  // there should be only one record for one tablet in share storage mode
  if (OB_UNLIKELY(primary_ckm_items.count() != standby_ckm_items.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), "primary item cnt", primary_ckm_items.count(),
             "standby item cnt", standby_ckm_items.count());
  } else {
    tablet_cnt = primary_ckm_items.count();
  }
  const ObTabletReplicaChecksumItem *replica_ckm_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
    const ObTabletChecksumItem &tablet_ckm_item = primary_ckm_items.at(i);
    if (OB_FAIL(standby_ckm_items.get(tablet_ckm_item.get_tablet_id(), replica_ckm_item))) {
      LOG_WARN("failed to get replica ckm item", KR(ret));
    } else {
      if (OB_FAIL(tablet_ckm_item.verify_tablet_column_checksum(*replica_ckm_item))) {
        if (OB_CHECKSUM_ERROR == ret) {
          LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "ERROR! ERROR! ERROR! checksum error in "
            "cross-cluster checksum", K(tablet_ckm_item), KPC(replica_ckm_item));
          ObTabletCkmErrorInfo error_info;
          error_info.set_info(
              replica_ckm_item->tenant_id_,
              replica_ckm_item->ls_id_,
              replica_ckm_item->tablet_id_,
              replica_ckm_item->compaction_scn_.get_val_for_tx());
          if (OB_TMP_FAIL(ObTabletCkmErrorInfoOperator::write_tablet_ckm_error_info(error_info))) {
            LOG_WARN_RET(tmp_ret, "failed to write ckm error info", K(ret), K(error_info));
          }
        } else {
          LOG_WARN("unexpected error in cross-cluster checksum", KR(ret), 
            K(tablet_ckm_item), KPC(replica_ckm_item));
        }
      }
    }
  }
  return ret;
}

int ObLSChecksumValidator::check_schema_version()
{
  int ret = OB_SUCCESS;
  share::ObFreezeInfo freeze_info;
  int64_t local_schema_version = OB_INVALID_VERSION;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_freeze_info_by_snapshot_version(get_compaction_scn().get_val_for_tx(),
                                                                                       freeze_info))) {
    LOG_WARN("failed to get freeze info", K(ret), "compaction_scn", get_compaction_scn());
  } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id_, local_schema_version))) {
    LOG_WARN("fail to get tenant local schema version", KR(ret));
  } else if (!ObSchemaService::is_formal_version(local_schema_version)) {
    ret = OB_EAGAIN;
    LOG_WARN("is not a formal_schema_version", KR(ret), K(local_schema_version));
  } else if (local_schema_version < freeze_info.schema_version_) {
    ret = OB_EAGAIN;
    LOG_WARN("schema is not new enough", KR(ret), K(freeze_info), K(local_schema_version));
  }
  return ret;
}

int ObLSChecksumValidator::get_tenant_schema_guard(
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  int64_t tenant_schema_version = 0;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id_, tenant_schema_version))) {
    LOG_WARN("failed to get schema version", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_,
                                                             schema_guard,
                                                             tenant_schema_version,
                                                             OB_INVALID_VERSION,
                                                             ObMultiVersionSchemaService::FORCE_LAZY))) {
    LOG_WARN("failed to get schema guard", KR(ret));
  }
  return ret;
}

int ObLSChecksumValidator::get_table_first_tablet_id(
    const uint64_t table_id,
    const ObSimpleTableSchemaV2 &simple_schema,
    ObTabletID &first_tablet_id,
    common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(simple_schema.get_tablet_ids(tablet_ids))) {
    LOG_WARN("failed to get tablet ids", K(ret), K(table_id));
  } else if (tablet_ids.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty tablet id array", K(ret), K(table_id));
  } else if (FALSE_IT(first_tablet_id = tablet_ids.at(0))) {
  } else if (tablet_ids.count() > 1) {
    for (int64_t i = 1; i < tablet_ids.count(); ++i) {
      if (tablet_ids.at(i) < first_tablet_id) {
        first_tablet_id = tablet_ids.at(i);
      }
    }
  }
  return ret;
}

int ObLSChecksumValidator::get_tablet_ls_pairs(
    const uint64_t table_id,
    const share::schema::ObSimpleTableSchemaV2 &simple_schema)
{
  int ret = OB_SUCCESS;
  cur_tablet_ls_pair_array_.reuse();

  SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
    if (OB_UNLIKELY(!simple_schema.has_tablet())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet schema should have tablet", K(ret), K(simple_schema));
    } else if (OB_FAIL(simple_schema.get_tablet_ids(tablet_ids))) {
      LOG_WARN("fail to get tablet_ids from simple schema", KR(ret), K(simple_schema));
    } else if (OB_UNLIKELY(tablet_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tablet_ids of current table schema", KR(ret), K(simple_schema));
    } else if (OB_FAIL(cur_tablet_ls_pair_array_.reserve(tablet_ids.count()))) {
      LOG_WARN("failed to reserve tablet array", KR(ret), K(tablet_ids.count()));
    } else if (OB_FAIL(tablet_ls_pair_cache_.get_tablet_ls_pairs(table_id, tablet_ids, cur_tablet_ls_pair_array_))) {
      LOG_WARN("failed to tablet ls pair", KR(ret), K(tablet_ids));
    } else {
      LOG_TRACE("success to get tablet ls pairs", KR(ret), K(cur_tablet_ls_pair_array_));
    }
  }
  return ret;
}

int ObLSChecksumValidator::batch_write_tablet_ckm()
{
  int ret = OB_SUCCESS;

  if (!finish_tablet_ckm_array_.empty()) {
    int64_t retry_cnt = 0;
    int64_t sleep_time_us = 200 * 1000; // 200 ms

    while (OB_SUCC(ret) && retry_cnt < MAX_RETRY_CNT) {
      if (OB_SUCC(ObTabletChecksumOperator::update_tablet_checksum_items(sql_proxy_,
                                                                         tenant_id_,
                                                                         finish_tablet_ckm_array_))) {
        break;
      } else {
        ++retry_cnt;
        LOG_WARN("failed to write tablet checksum items", KR(ret), K(retry_cnt), K(sleep_time_us));
        USLEEP(sleep_time_us);
        sleep_time_us *= 2;
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret)) {
      finish_tablet_ckm_array_.reuse();
    }
  }
  return ret;
}


} // compaction
} // oceanbase
