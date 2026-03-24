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

#ifndef OCEANBASE_SHARE_TABLET_OB_TABLET_REPLICA_CHECKSUM_ITERATOR_H_
#define OCEANBASE_SHARE_TABLET_OB_TABLET_REPLICA_CHECKSUM_ITERATOR_H_

#include "share/tablet/ob_tablet_filter.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "share/ob_tablet_replica_checksum_operator.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}

namespace share
{

class ObTenantChecksumTableIterator
{
public:
  ObTenantChecksumTableIterator(const uint64_t tenant_id);
  virtual ~ObTenantChecksumTableIterator() {}
  int next(ObTabletReplicaChecksumItem &tablet_item);
private:
  int prefetch_();
private:
  uint64_t tenant_id_;
  int64_t inner_idx_;
  common::ObArray<ObTabletReplicaChecksumItem> inner_tablet_items_;
};


} // share
} // oceanbase

#endif // OCEANBASE_SHARE_TABLET_OB_TABLET_REPLICA_CHECKSUM_ITERATOR_H_
