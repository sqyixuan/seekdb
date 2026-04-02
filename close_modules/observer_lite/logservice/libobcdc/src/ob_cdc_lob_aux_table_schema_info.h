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

#ifndef OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_SCHEMA_INFO_H_
#define OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_SCHEMA_INFO_H_

#include "share/schema/ob_table_schema.h"           // TableSchema
#include "share/schema/ob_table_param.h"            // ObColDesc

namespace oceanbase
{
namespace libobcdc
{
class ObCDCLobAuxTableSchemaInfo
{
public:
  ObCDCLobAuxTableSchemaInfo();
  ~ObCDCLobAuxTableSchemaInfo() { reset (); }
  int init();
  void reset();
  const share::schema::ObTableSchema &get_table_schema() const { return table_schema_; }
  const ObArray<share::schema::ObColDesc> &get_cols_des_array() const { return col_des_array_; }

  // For test or debug
  void print();

private:
  share::schema::ObTableSchema table_schema_;
  ObArray<share::schema::ObColDesc> col_des_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCLobAuxTableSchemaInfo);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
