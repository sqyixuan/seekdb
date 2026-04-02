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

#define USING_LOG_PREFIX OBLOG_FORMATTER

#include "ob_cdc_udt.h"
#include "ob_log_utils.h"

namespace oceanbase
{
namespace libobcdc
{

int ObCDCUdtSchemaInfo::set_main_column(ColumnSchemaInfo *column_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("set null main column info", KR(ret));
  } else {
    main_column_ = column_info;
  }
  return ret;
}

int ObCDCUdtSchemaInfo::get_main_column(ColumnSchemaInfo *&column_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(main_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("main_column_ is null", KR(ret));
  } else {
    column_info = main_column_;
  }
  return ret;
}

int ObCDCUdtSchemaInfo::add_hidden_column(ColumnSchemaInfo *column_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("add null hidden column info", KR(ret));
  } else if (OB_FAIL(hidden_columns_.push_back(column_info))) {
    LOG_ERROR("push_back hidden_columns fail", KR(ret));
  }
  return ret;
}

int ObCDCUdtValueMap::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
  } else if (OB_ISNULL(tb_schema_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tb_schema_info is null", KR(ret));
  } else if (OB_FAIL(udt_value_map_.create(4, "UDT"))) {
    LOG_ERROR("create map fail", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCDCUdtValueMap::get_udt_value_(uint64_t udt_set_id, ColValue *&cv_node)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(udt_value_map_.get_refactored(udt_set_id, cv_node))) {
    // get value success, do nothing
  } else if (OB_HASH_NOT_EXIST == ret) {
    // not found , so create and set
    if (OB_FAIL(create_udt_value_(udt_set_id, cv_node))) {
      LOG_ERROR("create_udt_value_ fail", KR(ret), K(udt_set_id));
    } else if (OB_FAIL(udt_value_map_.set_refactored(udt_set_id, cv_node))) {
      LOG_ERROR("set udt_value_map_ fail", KR(ret), K(udt_set_id));
    }
  } else {
    LOG_ERROR("get_refactored fail", KR(ret), K(udt_set_id));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(cv_node)) {
    allocator_.free(cv_node);
    cv_node = nullptr;
  }
  return ret;
}

int ObCDCUdtValueMap::create_udt_value_(uint64_t udt_set_id, ColValue *&cv_node)
{
  int ret = OB_SUCCESS;
  ColumnSchemaInfo *column_schema_info = nullptr;
  if (OB_FAIL(tb_schema_info_->get_main_column_of_udt(udt_set_id, column_schema_info))) {
    LOG_ERROR("get_main_column_of_udt fail", KR(ret), K(udt_set_id));
  } else if (OB_ISNULL(cv_node = static_cast<ColValue *>(allocator_.alloc(sizeof(ColValue))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for ColValue fail", KR(ret), "size", sizeof(ColValue));
  } else if (OB_FAIL(column_values_.add(cv_node))) {
    LOG_ERROR("add column to column_values fail", KR(ret), K(udt_set_id));
  } else {
    cv_node->reset();
    cv_node->column_id_ = column_schema_info->get_column_id();
    cv_node->is_out_row_ = 0;
    column_cast(cv_node->value_, *column_schema_info);
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(cv_node)) {
    allocator_.free(cv_node);
    cv_node = nullptr;
  }
  return ret;
}

int ObCDCUdtValueBuilder::build(
    const ColumnSchemaInfo &column_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool is_new_value,
    DmlStmtTask &dml_stmt_task,
    ObObj2strHelper &obj2str_helper,
    ObLobDataOutRowCtxList &lob_ctx_cols,
    ColValue &cv)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cv.is_col_nop_)) {
    LOG_DEBUG("ignore nop col",
        "tls_id", dml_stmt_task.get_tls_id(),
        "table_id", dml_stmt_task.get_table_id(),
        "column_id", column_schema_info.get_column_id());
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR(
        "not supported column type, udt value builder only support xmltype currently",
        KR(ret),
        K(column_schema_info));
  }
  return ret;
}

int ObCDCUdtValueBuilder::build_xmltype_(
    const ColumnSchemaInfo &column_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool is_new_value,
    DmlStmtTask &dml_stmt_task,
    ObObj2strHelper &obj2str_helper,
    ObLobDataOutRowCtxList &lob_ctx_cols,
    ColValue &cv)
{
  int ret = OB_SUCCESS;
  ColValue *value = cv.children_.head_;
  ObString *col_str = nullptr;
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("input is null", KR(ret), K(cv));
  } else {
    if (! value->is_out_row_) {
      // ColValue not called obj2str before, so used origin obj value
      // and because xmltype only have one hidden blob cloumn
      // so call ObObj::get_string is fine, but other type hidden column cannot
      if (OB_FAIL(value->value_.get_string(value->string_value_))) {
        LOG_WARN("get_string from col_value", KR(ret), K(*value), K(cv));
      } else {
        col_str =  &value->string_value_;
      }
    } else {
      if (OB_FAIL(lob_ctx_cols.get_lob_column_value(value->column_id_, is_new_value, col_str))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("get_lob_column_value fail", KR(ret), K(value->column_id_));
        } else {
          LOG_DEBUG("get_lob_column_value not exist", KR(ret), K(value->column_id_));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(col_str)) {
      LOG_INFO("col_str is null", K(is_new_value), K(value->is_out_row_), K(value->column_id_));
    } else if (OB_FALSE_IT(cv.value_.set_sql_udt(col_str->ptr(),
                                                 static_cast<int32_t>(col_str->length()),
                                                 ObXMLSqlType))) {
    } else if (OB_FAIL(dml_stmt_task.parse_col(
        dml_stmt_task.get_tenant_id(),
        cv.column_id_,
        column_schema_info,
        tz_info_wrap,
        obj2str_helper,
        cv))) {
      LOG_ERROR("dml_stmt_task parse_col fail", KR(ret), K(dml_stmt_task), K(cv));
    }
  }
  LOG_DEBUG("build_xmltype", KR(ret), K(cv.string_value_), K(value->string_value_));
  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
