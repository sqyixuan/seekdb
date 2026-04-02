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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_PROXY_SERVER_STAT_H_
#define OCEANBASE_OBSERVER_VIRTUAL_PROXY_SERVER_STAT_H_

#include "share/ob_virtual_table_projector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
}
}
namespace common
{
class ObMySQLProxy;
}
namespace observer
{
class ObVirtualProxyServerStat : public common::ObVirtualTableProjector
{
public:
  ObVirtualProxyServerStat();
  virtual ~ObVirtualProxyServerStat();

  int init(share::schema::ObMultiVersionSchemaService &schema_service);

  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  int get_full_row(const share::schema::ObTableSchema *table,
                   common::ObIArray<Column> &columns);

  bool is_inited_;
  int64_t server_idx_;
  const share::schema::ObTableSchema *table_schema_;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualProxyServerStat);
};

}//end namespace observer
}//end namespace oceanbase
#endif  /*OCEANBASE_OBSERVER_VIRTUAL_PROXY_SERVER_STAT_H_*/
