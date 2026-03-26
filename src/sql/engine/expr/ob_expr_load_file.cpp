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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_load_file.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/utility.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{

ObExprLoadFile::ObExprLoadFile(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_LOAD_FILE_EXPRESSION, N_LOAD_FILE, 2,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLoadFile::~ObExprLoadFile()
{
}

int ObExprLoadFile::calc_result_type2(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprResType &type2,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  // result type is BLOB
  type.set_type(ObLongTextType);
  type.set_collation_type(CS_TYPE_BINARY);
  type.set_length(OB_MAX_LONGTEXT_LENGTH);
  // first parameter: location_name (VARCHAR)
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());
  // second parameter: filename (VARCHAR)
  type2.set_calc_type(ObVarcharType);
  type2.set_calc_collation_type(ObCharset::get_system_collation());
  return ret;
}

int ObExprLoadFile::eval_load_file(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *location_name_datum = nullptr;
  ObDatum *filename_datum = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, location_name_datum, filename_datum))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (location_name_datum->is_null() || filename_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("LOAD_FILE parameters cannot be NULL", K(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "LOAD_FILE");
  } else {
    const ObString location_name = location_name_datum->get_string();
    const ObString filename = filename_datum->get_string();
    const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      const uint64_t tenant_id = session->get_effective_tenant_id();
      ObString file_data;
      ObExprStrResAlloc expr_res_alloc(expr, ctx);
      if (OB_FAIL(read_file_from_location(location_name, filename, tenant_id, ctx.exec_ctx_, expr_res_alloc, file_data))) {
        LOG_WARN("fail to read file from location", K(ret), K(location_name), K(filename));
        expr_datum.set_null();
      } else {
        ObTextStringDatumResult blob_res(ObLongTextType, &expr, &ctx, &expr_datum);
        if (OB_FAIL(blob_res.init(file_data.length()))) {
          LOG_WARN("fail to init blob result", K(ret), K(file_data.length()));
        } else if (OB_FAIL(blob_res.append(file_data))) {
          LOG_WARN("fail to append file data", K(ret), K(file_data.length()));
        } else {
          blob_res.set_result();
        }
      }
    }
  }
  return ret;
}

int ObExprLoadFile::read_file_from_location(const ObString &location_name,
                                            const ObString &filename,
                                            const uint64_t tenant_id,
                                            ObExecContext &exec_ctx,
                                            ObIAllocator &alloc,
                                            ObString &file_data)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObLocationSchema *location_schema = nullptr;
  ObString file_url;
  ObString access_info;
  // 1. get schema_guard and find location schema
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_location_schema_by_name(tenant_id, location_name, location_schema))) {
    LOG_WARN("fail to get location schema by name", K(ret), K(location_name), K(tenant_id));
  } else if (OB_ISNULL(location_schema)) {
    ret = OB_LOCATION_OBJ_NOT_EXIST;
    LOG_WARN("location object does't exist", K(ret), K(tenant_id), K(location_name));
  } else {
    // 2. get URL and access_info
    const ObString &location_url = location_schema->get_location_url_str();
    access_info = location_schema->get_location_access_info_str();
    // 3. build full file path: location_url + "/" + filename
    if (OB_FAIL(build_file_path(location_url, filename, alloc, file_url))) {
      LOG_WARN("fail to build full file path", K(ret), K(location_url), K(filename));
    } else {
      // 4. read file
      ObExternalDataAccessDriver driver;
      int64_t file_size = 0;
      char *buffer = nullptr;
      ObArenaAllocator tmp_alloc;
      ObString file_url_cstring;
      // initialize driver (need to pass location_url and access_info)
      if (OB_FAIL(driver.init(location_url, access_info))) {
        LOG_WARN("fail to init external data access driver", K(ret), K(location_url));
      } else if (OB_FAIL(ob_write_string(tmp_alloc, file_url, file_url_cstring, true))) {
        LOG_WARN("fail to write file url string", K(ret), K(file_url));
      } else if (OB_FAIL(driver.get_file_size(file_url_cstring, file_size))) {
        LOG_WARN("fail to get file size", K(ret), K(file_url_cstring));
      } else if (file_size < 0) {
        ret = OB_OBJECT_NAME_NOT_EXIST;
        LOG_WARN("file does not exist", K(ret), K(file_url_cstring));
      } else if (file_size == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid file size (empty file)", K(ret), K(file_size));
      } else if (file_size > 100 * 1024 * 1024) { // limit 100MB
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "file size exceeds 100MB limit");
      } else if (OB_ISNULL(buffer = static_cast<char *>(alloc.alloc(file_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(file_size));
      } else {
        // open file
        if (OB_FAIL(driver.open(file_url_cstring.ptr()))) {
          LOG_WARN("fail to open file", K(ret), K(file_url_cstring));
        } else {
          // read file content
          int64_t offset = 0;
          while (OB_SUCC(ret) && offset < file_size) {
            int64_t read_bytes = 0;
            int64_t batch_size = min(static_cast<int64_t>(2 * 1024 * 1024), file_size - offset); // 2MB batch read
            if (OB_FAIL(driver.pread(buffer + offset, batch_size, offset, read_bytes))) {
              LOG_WARN("fail to read file", K(ret), K(offset), K(batch_size));
            } else if (read_bytes <= 0) {
              break;
            } else {
              offset += read_bytes;
            }
          }
          driver.close();
          if (OB_SUCC(ret)) {
            file_data.assign_ptr(buffer, static_cast<int32_t>(offset));
          }
        }
      }
    }
  }
  return ret;
}

int ObExprLoadFile::build_file_path(const ObString &location_url, const ObString &filename, ObIAllocator &alloc, ObString &full_path)
{
  int ret = OB_SUCCESS;
  ObSqlString path_builder;
  if (OB_FAIL(path_builder.append(location_url))) {
    LOG_WARN("fail to append location url", K(ret), K(location_url));
  } else {
    const char *url_ptr = location_url.ptr();
    int64_t url_len = location_url.length();
    bool need_separator = true;
    ObString path_str;
    char *buf = nullptr;
    if ((url_len > 0 && url_ptr[url_len - 1] == '/') || (filename.length() > 0 && filename.ptr()[0] == '/')) {
      need_separator = false;
    }
    if (need_separator && OB_FAIL(path_builder.append("/"))) {
      LOG_WARN("fail to append separator", K(ret));
    } else if (OB_FAIL(path_builder.append(filename))) {
      LOG_WARN("fail to append filename", K(ret), K(filename));
    } else if (OB_FALSE_IT(path_str = path_builder.string())) {
    } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(path_str.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(path_str.length()));
    } else {
      MEMCPY(buf, path_str.ptr(), path_str.length());
      full_path.assign_ptr(buf, path_str.length());
    }
  }
  return ret;
}

int ObExprLoadFile::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = eval_load_file;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase

