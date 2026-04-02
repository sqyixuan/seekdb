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

#include "ob_config_storage.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/time/ob_time_utility.h"
#include "share/config/ob_system_config.h"
#include "share/config/ob_config.h"
#include <sqlite/sqlite3.h>
#include <string.h>

namespace oceanbase
{
namespace common
{

ObConfigStorage::ObConfigStorage()
  : db_(nullptr), is_inited_(false)
{
  db_path_[0] = '\0';
}

ObConfigStorage::~ObConfigStorage()
{
  destroy();
}

int ObConfigStorage::init(const char *db_path)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sqlite config storage already inited", K(ret));
  } else if (OB_ISNULL(db_path) || strlen(db_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid db_path", K(ret), KP(db_path));
  } else {
    int sqlite_ret = sqlite3_open(db_path, &db_);
    if (SQLITE_OK != sqlite_ret) {
      ret = OB_ERROR;
      LOG_ERROR("failed to open sqlite database", K(ret), K(db_path),
                "sqlite_err", sqlite3_errmsg(db_));
      if (nullptr != db_) {
        sqlite3_close(db_);
        db_ = nullptr;
      }
    } else {
      snprintf(db_path_, OB_MAX_FILE_NAME_LENGTH, "%s", db_path);
      if (OB_FAIL(create_table_if_not_exists())) {
        LOG_WARN("failed to create table", K(ret));
        sqlite3_close(db_);
        db_ = nullptr;
      } else {
        is_inited_ = true;
        LOG_INFO("sqlite config storage init success", K(db_path));
      }
    }
  }
  return ret;
}

void ObConfigStorage::destroy()
{
  if (nullptr != db_) {
    sqlite3_close(db_);
    db_ = nullptr;
  }
  db_path_[0] = '\0';
  is_inited_ = false;
}

int ObConfigStorage::create_table_if_not_exists()
{
  int ret = OB_SUCCESS;
  const char *create_table_sql =
    "CREATE TABLE IF NOT EXISTS __all_sys_parameter ("
    "  gmt_create INTEGER,"
    "  gmt_modified INTEGER,"
    "  name TEXT,"
    "  data_type TEXT,"
    "  value TEXT NOT NULL,"
    "  value_strict TEXT,"
    "  info TEXT,"
    "  need_reboot INTEGER,"
    "  section TEXT,"
    "  visible_level TEXT,"
    "  config_version INTEGER NOT NULL,"
    "  scope TEXT,"
    "  source TEXT,"
    "  edit_level TEXT,"
    "  PRIMARY KEY (name)"
    ");";

  if (OB_FAIL(execute_sql(create_table_sql))) {
    LOG_WARN("failed to create table", K(ret));
  }
  return ret;
}

int ObConfigStorage::execute_sql(const char *sql)
{
  int ret = OB_SUCCESS;
  char *err_msg = nullptr;
  int sqlite_ret = sqlite3_exec(db_, sql, nullptr, nullptr, &err_msg);
  if (SQLITE_OK != sqlite_ret) {
    ret = OB_ERROR;
    LOG_WARN("failed to execute sql", K(ret), K(sql), "sqlite_err", err_msg);
    if (nullptr != err_msg) {
      sqlite3_free(err_msg);
    }
  }
  return ret;
}

int ObConfigStorage::load_all_configs(ObSystemConfig &system_config)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || OB_ISNULL(db_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_), KP(db_));
  } else {
    const char *select_sql =
      "SELECT name, data_type, value, info, section, scope, source, edit_level "
      "FROM __all_sys_parameter;";

    sqlite3_stmt *stmt = nullptr;
    int sqlite_ret = sqlite3_prepare_v2(db_, select_sql, -1, &stmt, nullptr);
    if (SQLITE_OK != sqlite_ret) {
      ret = OB_ERROR;
      LOG_WARN("failed to prepare statement", K(ret), "sqlite_err", sqlite3_errmsg(db_));
    } else {
      while (OB_SUCC(ret) && SQLITE_ROW == (sqlite_ret = sqlite3_step(stmt))) {
        ObSystemConfigKey key;
        ObSystemConfigValue value;

        int col_idx = 0;
        const unsigned char *name_str = sqlite3_column_text(stmt, col_idx++);
        if (OB_SUCC(ret) && nullptr != name_str) {
          ObString name_val((int32_t)strlen((const char*)name_str), (const char*)name_str);
          if (FALSE_IT(key.set_name(name_val))) {
            LOG_WARN("failed to set name", K(ret));
          }
        }

        const unsigned char *data_type_str = sqlite3_column_text(stmt, col_idx++);
        const unsigned char *value_str = sqlite3_column_text(stmt, col_idx++);
        if (OB_SUCC(ret) && nullptr != value_str) {
          ObString value_val((int32_t)strlen((const char*)value_str), (const char*)value_str);
          if (FALSE_IT(value.set_value(value_val))) {
            LOG_WARN("failed to set value", K(ret));
          }
        }

        const unsigned char *info_str = sqlite3_column_text(stmt, col_idx++);
        if (OB_SUCC(ret) && nullptr != info_str) {
          ObString info_val((int32_t)strlen((const char*)info_str), (const char*)info_str);
          if (FALSE_IT(value.set_info(info_val))) {
            LOG_WARN("failed to set info", K(ret));
          }
        }

        const unsigned char *section_str = sqlite3_column_text(stmt, col_idx++);
        if (OB_SUCC(ret) && nullptr != section_str) {
          ObString section_val((int32_t)strlen((const char*)section_str), (const char*)section_str);
          if (FALSE_IT(value.set_section(section_val))) {
            LOG_WARN("failed to set section", K(ret));
          }
        }

        const unsigned char *scope_str = sqlite3_column_text(stmt, col_idx++);
        if (OB_SUCC(ret) && nullptr != scope_str) {
          ObString scope_val((int32_t)strlen((const char*)scope_str), (const char*)scope_str);
          if (FALSE_IT(value.set_scope(scope_val))) {
            LOG_WARN("failed to set scope", K(ret));
          }
        }

        const unsigned char *source_str = sqlite3_column_text(stmt, col_idx++);
        if (OB_SUCC(ret) && nullptr != source_str) {
          ObString source_val((int32_t)strlen((const char*)source_str), (const char*)source_str);
          if (FALSE_IT(value.set_source(source_val))) {
          }
        }

        const unsigned char *edit_level_str = sqlite3_column_text(stmt, col_idx++);
        if (OB_SUCC(ret) && nullptr != edit_level_str) {
          ObString edit_level_val((int32_t)strlen((const char*)edit_level_str), (const char*)edit_level_str);
          if (FALSE_IT(value.set_edit_level(edit_level_val))) {
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(system_config.update_value(key, value))) {
            LOG_WARN("failed to update system config", K(ret));
          }
        }
      }

      if (SQLITE_DONE != sqlite_ret && SQLITE_ROW != sqlite_ret) {
        ret = OB_ERROR;
        LOG_WARN("failed to step statement", K(ret), "sqlite_err", sqlite3_errmsg(db_));
      } else {
        LOG_INFO("load all configs from sqlite success");
      }

      sqlite3_finalize(stmt);
    }
  }
  return ret;
}

int ObConfigStorage::upsert_config(
    const ObString &name,
    const ObString &data_type,
    const ObString &value,
    const ObString &info,
    const ObString &section,
    const ObString &scope,
    const ObString &source,
    const ObString &edit_level)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || OB_ISNULL(db_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_), KP(db_));
  } else {
    const char *upsert_sql =
      "INSERT OR REPLACE INTO __all_sys_parameter "
      "(name, data_type, value, info, "
      " section, scope, source, edit_level, config_version, gmt_modified) "
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?);";

    sqlite3_stmt *stmt = nullptr;
    int sqlite_ret = sqlite3_prepare_v2(db_, upsert_sql, -1, &stmt, nullptr);
    if (SQLITE_OK != sqlite_ret) {
      ret = OB_ERROR;
      LOG_WARN("failed to prepare statement", K(ret), "sqlite_err", sqlite3_errmsg(db_));
    } else {
      int param_idx = 1;

      sqlite3_bind_text(stmt, param_idx++, name.ptr(), name.length(), SQLITE_STATIC);
      sqlite3_bind_text(stmt, param_idx++, data_type.ptr(), data_type.length(), SQLITE_STATIC);
      sqlite3_bind_text(stmt, param_idx++, value.ptr(), value.length(), SQLITE_STATIC);
      sqlite3_bind_text(stmt, param_idx++, info.ptr(), info.length(), SQLITE_STATIC);
      sqlite3_bind_text(stmt, param_idx++, section.ptr(), section.length(), SQLITE_STATIC);
      sqlite3_bind_text(stmt, param_idx++, scope.ptr(), scope.length(), SQLITE_STATIC);
      sqlite3_bind_text(stmt, param_idx++, source.ptr(), source.length(), SQLITE_STATIC);
      sqlite3_bind_text(stmt, param_idx++, edit_level.ptr(), edit_level.length(), SQLITE_STATIC);
      param_idx++;

      int64_t current_time = ObTimeUtility::current_time();
      sqlite3_bind_int64(stmt, param_idx++, current_time);

      sqlite_ret = sqlite3_step(stmt);
      if (SQLITE_DONE != sqlite_ret) {
        ret = OB_ERROR;
        LOG_WARN("failed to execute statement", K(ret), "sqlite_err", sqlite3_errmsg(db_));
      } else {
        LOG_INFO("upsert config to sqlite success", K(name));
      }

      sqlite3_finalize(stmt);
    }
  }
  return ret;
}


} // namespace common
} // namespace oceanbase
