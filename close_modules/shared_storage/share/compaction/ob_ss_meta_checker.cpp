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

#define USING_LOG_PREFIX SERVER

#include "ob_ss_meta_checker.h"
#include "share/tablet/ob_tablet_replica_checksum_iterator.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_tablet_replica_checksum_operator.h"


namespace oceanbase
{
using namespace share;
using namespace common;

namespace compaction
{

int ObTenantSSMetaChecker::check_tablet_table()
{
  int ret = OB_SUCCESS;
  int64_t report_count = 0;
  const int64_t start_time = ObTimeUtility::current_time();
  ObTabletReplicaMap replica_map;

  if (OB_FAIL(build_replica_map(replica_map))) {
    LOG_WARN("failed to build checksum replica map", K(ret));
  } else if (OB_FAIL(check_inner_table_replicas(replica_map))) {
    LOG_WARN("failed to check replica in inner table", K(ret));
  } else if (OB_FAIL(check_local_ls_replicas(replica_map, report_count))) {
    LOG_WARN("check replicas exist in tablet table but not in local failed", K(ret));
  } else if (report_count != 0) {
    LOG_INFO("checker found and corrected replicas for tablet replica checksum table",
      K(ret), K(report_count));
  }
  LOG_TRACE("finish checking tablet table", K(ret), K(report_count),
            K(start_time), "cost_time", ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObTenantSSMetaChecker::build_replica_map(ObTabletReplicaMap &replica_map)
{
  int ret = OB_SUCCESS;
  ObTenantChecksumTableIterator ckm_iter(MTL_ID());
  ObTabletReplicaChecksumItem item;
  ObTabletReplica replica;

  if (OB_FAIL(replica_map.create(TABLET_REPLICA_MAP_BUCKET_NUM,
                                        "TabletCheckMap",
                                        ObModIds::OB_HASH_NODE,
                                        MTL_ID()))) {
    LOG_WARN("fail to create replica map", K(ret));
  }

  while (OB_SUCC(ret)) {
    item.reset();
    replica.reset();
    if (OB_FAIL(ckm_iter.next(item))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("tablet table iterator next failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(replica.init(MTL_ID(),
                                    item.tablet_id_,
                                    item.ls_id_,
                                    GCTX.self_addr(),
                                    item.compaction_scn_.get_val_for_tx(),
                                    0/*data_size*/,
                                    0/*required_size*/,
                                    0/*report_scn*/,
                                    ObTabletReplica::SCN_STATUS_IDLE))) {
      LOG_WARN("failed to init tablet replica", K(ret), K(item));
    } else if (OB_FAIL(replica_map.set_refactored(ObTabletLSPair(item.tablet_id_, item.ls_id_), replica))) {
      LOG_WARN("fail to set_refactored", K(ret), K(replica), K(item));
    }
  }
  return ret;
}

int ObTenantSSMetaChecker::check_inner_table_replicas(ObTabletReplicaMap &replica_map)
{
  int ret = OB_SUCCESS;
  bool not_exist = false;
  int tmp_ret = OB_SUCCESS;
  FOREACH_X(it, replica_map, OB_SUCC(ret)) {
    const ObLSID &ls_id = it->first.get_ls_id();
    const ObTabletID &tablet_id = it->first.get_tablet_id();
    if (OB_TMP_FAIL(check_local_tablet_exist(ls_id, tablet_id, not_exist))) {
      LOG_WARN_RET(tmp_ret, "fail to check tablet whether exist in local", K(ls_id), K(tablet_id));
    } else if (!not_exist) {
      // do nothing
    } else if (OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id))) {
      LOG_WARN_RET(tmp_ret, "fail to submit tablet update task", K(ls_id), K(tablet_id));
    } else {
      LOG_INFO("add async task to remove replica from inner table", "replica", it->second);
    }
  } // end for
  return ret;
}

int ObTenantSSMetaChecker::check_local_tablet_exist(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    bool &not_exist)
{
  int ret = OB_SUCCESS;
  not_exist = false;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObRole ls_role;

  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id), K(tablet_id));
  } else if (tablet_id.is_reserved_tablet()) {
    // skip reserved tablet
  } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls() || nullptr == ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected ls handle", KR(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_ls_role(ls_role))) {
    LOG_WARN("failed to get ls role", K(ret), K(ls_id));
  } else if (ObRole::LEADER != ls_role) {
    // ls follower should skip check
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      not_exist = true;
    } else {
      LOG_WARN("fail to get tablet", KR(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObTenantSSMetaChecker::check_local_ls_replicas(
    ObTabletReplicaMap &replica_map,
    int64_t &report_count)
{
  int ret = OB_SUCCESS;
  report_count = 0;
  ObSharedGuard<ObLSIterator> ls_iter;

  if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ob_service is null", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter, ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("failed to get ls iter", K(ret));
  }

  storage::ObLS *ls = nullptr;
  while(OB_SUCC(ret)) {
    if (OB_FAIL(ls_iter->get_next(ls))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("scan next ls failed.", K(ret));
      }
    } else if (OB_UNLIKELY(nullptr == ls || nullptr == ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get ls or tablet svr", K(ret), KP(ls));
    } else if (OB_FAIL(check_local_tablet_replicas(ls, replica_map, report_count))) {
      LOG_WARN("failed to check local tablet replicas", K(ret));
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTenantSSMetaChecker::check_local_tablet_replicas(
    storage::ObLS *ls,
    ObTabletReplicaMap &replica_map,
    int64_t &report_count)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_ALL_COMMITED);

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret));
  } else {
    ls_id = ls->get_ls_id();
  }

  ObTabletHandle tablet_hdl;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_iter.get_next_tablet(tablet_hdl))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next tablet", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_UNLIKELY(!tablet_hdl.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), K(tablet_hdl));
    } else {
      check_local_tablet(ls_id, tablet_hdl.get_obj(), replica_map, report_count);
    }
  }
  return ret;
}

void ObTenantSSMetaChecker::check_local_tablet(
    const share::ObLSID &ls_id,
    storage::ObTablet *tablet,
    ObTabletReplicaMap &replica_map,
    int64_t &report_count)
{
  int ret = OB_SUCCESS;
  bool need_report_flag = false;
  ObTabletID tablet_id;
  ObTabletReplica table_replica; // replica from replica ckm table

  if (OB_ISNULL(tablet)) {
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
  } else if (OB_FAIL(replica_map.get_refactored(ObTabletLSPair(tablet_id, ls_id), table_replica))) {
    if (OB_HASH_NOT_EXIST == ret) { // not exist in table while exist in local
      ret = OB_SUCCESS;
      need_report_flag = true;
    } else {
      LOG_WARN("get replica from hashmap failed", K(ret), K(ls_id), K(tablet_id));
    }
  } else {
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    const ObTabletTableStore *table_store = nullptr;
    if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
      LOG_WARN("fail to get table store", K(ret), K(table_store_wrapper));
    } else {
      int64_t report_major_version = table_store->get_major_ckm_info().get_report_compaction_scn();
      report_major_version = MAX(report_major_version, tablet->get_last_major_snapshot_version());
      if (table_replica.get_snapshot_version() < report_major_version) {
        need_report_flag = true;
      }
    }
  }

  if (OB_SUCC(ret) && need_report_flag) {
    if (OB_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id))) {
      LOG_WARN("fail to submit tablet update task", K(ret), K(ls_id), K(tablet_id));
    } else {
      ++report_count;
      LOG_INFO("modify replica success", K(ret), K(table_replica));
    }
  }
}


} // compaction
} // oceanbase
