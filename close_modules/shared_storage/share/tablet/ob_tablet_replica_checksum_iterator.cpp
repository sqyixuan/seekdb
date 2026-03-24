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

#define USING_LOG_PREFIX SHARE
#include "share/tablet/ob_tablet_replica_checksum_iterator.h"
#include "share/ob_tablet_replica_checksum_operator.h"


namespace oceanbase
{
namespace share
{

ObTenantChecksumTableIterator::ObTenantChecksumTableIterator(
    const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    inner_idx_(0),
    inner_tablet_items_()
{
}

int ObTenantChecksumTableIterator::next(
    ObTabletReplicaChecksumItem &tablet_item)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inner_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_idx_ can't be smaller than 0", KR(ret), K_(inner_idx));
  } else {
    tablet_item.reset();
    if (inner_idx_ >= inner_tablet_items_.count()) {
      if (OB_FAIL(prefetch_())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to prfetch", KR(ret));
        }
      } else {
        inner_idx_ = 0;
      }
    }

    if (FAILEDx(tablet_item.assign(inner_tablet_items_[inner_idx_]))) {
      LOG_WARN("failed to assign tablet item", KR(ret), K_(inner_idx), K(inner_tablet_items_));
    } else {
      ++inner_idx_;
    }
  }
  return ret;
}

int ObTenantChecksumTableIterator::prefetch_()
{
  int ret = OB_SUCCESS;
  ObTabletID last_tablet_id; // start with INVALID_TABLET_ID = 0

  if (inner_tablet_items_.count() > 0) {
    const int64_t last_idx = inner_tablet_items_.count() - 1;
    last_tablet_id = inner_tablet_items_.at(last_idx).tablet_id_;
  }
  inner_tablet_items_.reset();
  const int64_t range_size = GCONF.tablet_meta_table_scan_batch_count;
  int64_t tablet_cnt = 0;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null sql proxy", K(ret));
  } else if (OB_FAIL(ObTabletReplicaChecksumOperator::range_get(tenant_id_,
                                                                last_tablet_id,
                                                                range_size,
                                                                0/*OBCG_DEFAULT*/,
                                                                *GCTX.sql_proxy_,
                                                                inner_tablet_items_,
                                                                tablet_cnt))) {
    LOG_WARN("fail to range get by operator", KR(ret), K_(tenant_id),
              K(last_tablet_id), K(range_size), K(inner_tablet_items_));
  } else if (inner_tablet_items_.count() <= 0) {
    ret = OB_ITER_END;
  } else if (tablet_cnt != inner_tablet_items_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet count should be equal with tablet items count", K(ret), K(tablet_cnt));
  }
  return ret;
}


} // share
} // oceanbase
