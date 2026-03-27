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

#ifndef OCEANBASE_SHARE_CONFIG_OB_CONFIG_STORAGE_H_
#define OCEANBASE_SHARE_CONFIG_OB_CONFIG_STORAGE_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/config/ob_system_config.h"
#include "share/config/ob_system_config_key.h"
#include "share/config/ob_system_config_value.h"

struct sqlite3;
struct sqlite3_stmt;
namespace oceanbase
{
namespace common
{

class ObConfigStorage
{
public:
  ObConfigStorage();
  virtual ~ObConfigStorage();

  int init(const char *db_path);
  void destroy();
  int load_all_configs(ObSystemConfig &system_config);
  int upsert_config(
      const ObString &name,
      const ObString &data_type,
      const ObString &value,
      const ObString &info,
      const ObString &section,
      const ObString &scope,
      const ObString &source,
      const ObString &edit_level);
  bool is_inited() const { return is_inited_; }

private:
  int create_table_if_not_exists();
  int execute_sql(const char *sql);
  int execute_prepared_statement(sqlite3_stmt *stmt);
  int read_config_from_result(sqlite3_stmt *stmt, ObSystemConfig &system_config);

private:
  sqlite3 *db_;
  char db_path_[OB_MAX_FILE_NAME_LENGTH];
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObConfigStorage);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CONFIG_OB_CONFIG_STORAGE_H_
