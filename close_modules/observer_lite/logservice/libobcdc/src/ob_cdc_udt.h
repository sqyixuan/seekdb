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

#ifndef OCEANBASE_LIBOBCDC_UDT_H_
#define OCEANBASE_LIBOBCDC_UDT_H_

#include "ob_log_schema_cache_info.h"
#include "ob_log_part_trans_task.h"

namespace oceanbase
{
namespace libobcdc
{

class ObCDCUdtSchemaInfo
{
public:
  ObCDCUdtSchemaInfo() :
      main_column_(nullptr),
      hidden_columns_()
  {}
  ~ObCDCUdtSchemaInfo() {}

  int set_main_column(ColumnSchemaInfo *column_info);
  int get_main_column(ColumnSchemaInfo *&column_info);
  int add_hidden_column(ColumnSchemaInfo *column_info);

private:
  // the main column info for display to user, with type info , such as udt id
  ColumnSchemaInfo *main_column_;
  // generated hidden column, with real data
  common::ObArray<ColumnSchemaInfo*> hidden_columns_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCUdtSchemaInfo);
};

// just wrapper of ColValueList, used for map udt_set_id to ColValue
// new allocated ColValue must append to ColValueList cols
// and will init when first invoked
class ObCDCUdtValueMap {
public:
  ObCDCUdtValueMap(
      common::ObIAllocator &allocator,
      const TableSchemaInfo *tb_schema_info,
      ColValueList &column_values) :
    is_inited_(false),
    allocator_(allocator),
    tb_schema_info_(tb_schema_info),
    column_values_(column_values),
    udt_value_map_() {}

  ~ObCDCUdtValueMap() {}
  int init();

private:
  int create_udt_value_(uint64_t udt_set_id, ColValue *&cv_node);
  int get_udt_value_(uint64_t udt_set_id, ColValue*& val);

private:
  bool is_inited_;
  // allocator_ used alloc ColValue to append cols_
  // currentluy must ensure this is same MutatorRow::add_column_
  common::ObIAllocator &allocator_;
  //used for get udt column schema info according to udt_set_id
  const TableSchemaInfo *tb_schema_info_;
  // new alloc udt value will append to cols_
  // get from MutatorRow::parse_columns_ cols parammeter
  ColValueList &column_values_;
  // udt_set_id map to udt value
  // this map is just used to fast look up udt value in cols_ with udt_set_id
  common::hash::ObHashMap<uint64_t, ColValue*> udt_value_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCUdtValueMap);
};

class ObCDCUdtValueBuilder {
public:

  // then entry for building udt value, such xmltype, to get real data of udt
  // and this function is called just before ObLogFormatter fill row value.
  // param column_schema_info is the main column of udt , used to get udt column meta info
  // the final result will store to param cv
  static int build(
      const ColumnSchemaInfo &column_schema_info,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      const bool is_new_value,
      DmlStmtTask &dml_stmt_task,
      ObObj2strHelper &obj2str_helper,
      ObLobDataOutRowCtxList &lob_ctx_cols,
      ColValue &cv);

private:
  // used to build_xmltype
  static int build_xmltype_(
      const ColumnSchemaInfo &column_schema_info,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      const bool is_new_value,
      DmlStmtTask &dml_stmt_task,
      ObObj2strHelper &obj2str_helper,
      ObLobDataOutRowCtxList &lob_ctx_cols,
      ColValue &cv);

};

} // namespace libobcdc
} // namespace oceanbase

#endif
