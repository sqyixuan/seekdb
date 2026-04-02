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

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_aux_table_schema_info.h"
#include "share/inner_table/ob_inner_table_schema.h"                // ObInnerTableSchema
#include "share/schema/ob_column_schema.h"                          // ObColumnSchemaV2

namespace oceanbase
{
namespace libobcdc
{
ObCDCLobAuxTableSchemaInfo::ObCDCLobAuxTableSchemaInfo() :
  table_schema_(),
  col_des_array_()
{
}

int ObCDCLobAuxTableSchemaInfo::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_meta_schema(table_schema_))) {
    LOG_ERROR("ObInnerTableSchema all_column_aux_lob_meta_schema failed", KR(ret));
  } else if (OB_FAIL(table_schema_.get_column_ids(col_des_array_))) {
    LOG_ERROR("table_schema_ get_column_ids failed", KR(ret));
  } else {}

  return ret;
}

void ObCDCLobAuxTableSchemaInfo::reset()
{
  table_schema_.reset();
  col_des_array_.reset();
}

void ObCDCLobAuxTableSchemaInfo::print()
{
  int ret = OB_SUCCESS;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < col_des_array_.count(); ++idx) {
    const uint64_t column_id = col_des_array_[idx].col_id_;
    const share::schema::ObColumnSchemaV2 *column_table_schema = NULL;

    if (OB_ISNULL(column_table_schema = table_schema_.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("column_table_schema is null", KR(ret), K(column_id), KPC(column_table_schema));
    } else {
      LOG_INFO("LOB_AUX_META_TABLE", K(idx), K(column_id), "name", column_table_schema->get_column_name());
    }
  } // for
}

} // namespace libobcdc
} // namespace oceanbase
