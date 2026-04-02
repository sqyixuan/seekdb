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

#include "share/storage/ob_reserved_snapshot_table_storage.h"
#include "share/storage/ob_sqlite_connection.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/net/ob_addr.h"

#include "share/storage/ob_sqlite_table_schema.h"

namespace oceanbase
{
namespace share
{

ObReservedSnapshotTableStorage::ObReservedSnapshotTableStorage()
  : pool_(nullptr)
{
}

ObReservedSnapshotTableStorage::~ObReservedSnapshotTableStorage()
{
}

int ObReservedSnapshotTableStorage::init(ObSQLiteConnectionPool *pool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pool_ = pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pool", K(ret));
  } else if (OB_FAIL(create_table_if_not_exists())) {
    LOG_WARN("failed to create table", K(ret));
  }
  if (OB_FAIL(ret)) {
    pool_ = NULL;
  }
  return ret;
}

int ObReservedSnapshotTableStorage::create_table_if_not_exists()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pool_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pool not set", K(ret));
  } else {
    ObSQLiteConnectionGuard guard(pool_);
    if (!guard) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to acquire connection", K(ret));
    } else if (OB_FAIL(guard->execute(SQLITE_CREATE_TABLE_RESERVED_SNAPSHOT, nullptr))) {
      LOG_WARN("failed to create table", K(ret));
    }
  }
  return ret;
}

int ObReservedSnapshotTableStorage::insert_or_update(
    const uint64_t tenant_id,
    const ObIArray<ObReservedSnapshotEntry> &entries)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (entries.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("entries is empty", K(ret));
  } else {
    ObSQLiteConnectionGuard guard(pool_);
    if (!guard) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to acquire connection", K(ret));
    } else {
      // Prepare batch execute
      const char *insert_sql =
        "INSERT INTO __all_reserved_snapshot "
        "(tenant_id, snapshot_type, svr_ip, svr_port, create_time, snapshot_version, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(tenant_id, snapshot_type, svr_ip, svr_port) DO UPDATE SET "
        "create_time = excluded.create_time, "
        "snapshot_version = excluded.snapshot_version;";
      
      // Begin transaction for batch insert
      if (OB_FAIL(guard->begin_transaction())) {
        LOG_WARN("failed to begin transaction", K(ret));
      } else {
        ObSQLiteStmt *stmt = nullptr;
        if (OB_FAIL(guard->prepare_execute(insert_sql, stmt))) {
          LOG_WARN("failed to prepare execute", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
            const ObReservedSnapshotEntry &entry = entries.at(i);
            char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
            if (OB_UNLIKELY(!entry.svr_addr_.ip_to_string(ip, sizeof(ip)))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("ip to string failed", K(ret), K(entry.svr_addr_));
            } else {
              auto binder = [&entry, &ip](ObSQLiteBinder &b) -> int {
                b.bind_int64(entry.tenant_id_);
                b.bind_int64(entry.snapshot_type_);
                b.bind_text(ip, static_cast<int>(strlen(ip)));
                b.bind_int64(entry.svr_addr_.get_port());
                b.bind_int64(entry.create_time_);
                b.bind_int64(entry.snapshot_version_);
                b.bind_int64(entry.status_);
                return OB_SUCCESS;
              };
              
              int64_t affected_rows = 0;
              if (OB_FAIL(guard->step_execute(stmt, binder, &affected_rows))) {
                LOG_WARN("failed to step execute", K(ret), K(i));
              }
            }
          }
          
          // Finalize statement
          guard->finalize_execute(stmt);
          
          // Commit or rollback transaction
          if (OB_FAIL(ret)) {
            int rollback_ret = guard->rollback();
            if (OB_SUCCESS != rollback_ret) {
              LOG_WARN("failed to rollback", K(rollback_ret));
            }
          } else {
            if (OB_FAIL(guard->commit())) {
              LOG_WARN("failed to commit", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObReservedSnapshotTableStorage::update_status(
    const uint64_t tenant_id,
    const common::ObAddr &svr_addr,
    const uint64_t status)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (OB_UNLIKELY(!svr_addr.ip_to_string(ip, sizeof(ip)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ip to string failed", K(ret), K(svr_addr));
    } else {
      ObSQLiteConnectionGuard guard(pool_);
      if (!guard) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to acquire connection", K(ret));
      } else {
        const char *update_sql =
          "UPDATE __all_reserved_snapshot "
          "SET status = ? "
          "WHERE tenant_id = ? AND svr_ip = ? AND svr_port = ?;";
        
        auto binder = [tenant_id, &ip, &svr_addr, status](ObSQLiteBinder &b) -> int {
          b.bind_int64(status);
          b.bind_int64(tenant_id);
          b.bind_text(ip, static_cast<int>(strlen(ip)));
          b.bind_int64(svr_addr.get_port());
          return OB_SUCCESS;
        };
        
        int64_t affected_rows = 0;
        if (OB_FAIL(guard->execute(update_sql, binder, &affected_rows))) {
          LOG_WARN("failed to execute update", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObReservedSnapshotTableStorage::get(
    const uint64_t tenant_id,
    const uint64_t snapshot_type,
    const common::ObAddr &svr_addr,
    ObReservedSnapshotEntry &entry)
{
  int ret = OB_SUCCESS;
  entry.reset();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (OB_UNLIKELY(!svr_addr.ip_to_string(ip, sizeof(ip)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ip to string failed", K(ret), K(svr_addr));
    } else {
      ObSQLiteConnectionGuard guard(pool_);
      if (!guard) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to acquire connection", K(ret));
      } else {
        const char *select_sql =
          "SELECT tenant_id, snapshot_type, svr_ip, svr_port, "
          "       create_time, snapshot_version, status "
          "FROM __all_reserved_snapshot "
          "WHERE tenant_id = ? AND snapshot_type = ? AND svr_ip = ? AND svr_port = ?;";
        
        auto binder = [tenant_id, snapshot_type, &ip, &svr_addr](ObSQLiteBinder &b) -> int {
          b.bind_int64(tenant_id);
          b.bind_int64(snapshot_type);
          b.bind_text(ip, static_cast<int>(strlen(ip)));
          b.bind_int64(svr_addr.get_port());
          return OB_SUCCESS;
        };
        
        auto row_processor = [&entry](ObSQLiteRowReader &reader) -> int {
          int ret = OB_SUCCESS;
          entry.tenant_id_ = reader.get_int64();
          entry.snapshot_type_ = reader.get_int64();
          int ip_len = 0;
          const char *ip_str = reader.get_text(&ip_len);
          int64_t port = reader.get_int64();
          entry.create_time_ = reader.get_int64();
          entry.snapshot_version_ = reader.get_int64();
          entry.status_ = reader.get_int64();
          
          if (OB_ISNULL(ip_str)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ip_str is null", K(ret));
          } else if (ip_len > 0) {
            // Use ObString version to avoid strlen() issue with binary data
            common::ObString ip_obstr(ip_len, ip_str);
            if (!entry.svr_addr_.set_ip_addr(ip_obstr, static_cast<int32_t>(port))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("failed to set ip addr", K(ret), K(ip_obstr), K(port));
            }
          }
          return ret;
        };
        
        if (OB_FAIL(guard->query(select_sql, binder, row_processor))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to query", K(ret));
          } else {
            ret = OB_ENTRY_NOT_EXIST;
          }
        }
      }
    }
  }
  return ret;
}

int ObReservedSnapshotTableStorage::get_all(
    const uint64_t tenant_id,
    ObIArray<ObReservedSnapshotEntry> &entries)
{
  int ret = OB_SUCCESS;
  entries.reset();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSQLiteConnectionGuard guard(pool_);
    if (!guard) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to acquire connection", K(ret));
    } else {
      const char *select_sql =
        "SELECT tenant_id, snapshot_type, svr_ip, svr_port, "
        "       create_time, snapshot_version, status "
        "FROM __all_reserved_snapshot "
        "WHERE tenant_id = ? "
        "ORDER BY snapshot_type, svr_ip, svr_port;";
      
      auto binder = [tenant_id](ObSQLiteBinder &b) -> int {
        b.bind_int64(tenant_id);
        return OB_SUCCESS;
      };
      
      ObSQLiteStmt *stmt = nullptr;
      if (OB_FAIL(guard->prepare_query(select_sql, binder, stmt))) {
        LOG_WARN("failed to prepare query", K(ret));
      } else {
        ObSQLiteRowReader reader;
        while (OB_SUCC(ret)) {
          ret = guard->step_query(stmt, reader);
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else if (OB_FAIL(ret)) {
            LOG_WARN("failed to step query", K(ret));
          } else {
            ObReservedSnapshotEntry entry;
            entry.tenant_id_ = reader.get_int64();
            entry.snapshot_type_ = reader.get_int64();
            int ip_len = 0;
            const char *ip_str = reader.get_text(&ip_len);
            int64_t port = reader.get_int64();
            entry.create_time_ = reader.get_int64();
            entry.snapshot_version_ = reader.get_int64();
            entry.status_ = reader.get_int64();
            
            if (OB_ISNULL(ip_str)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("ip_str is null", K(ret));
            } else if (ip_len > 0) {
              // Use ObString version to avoid strlen() issue with binary data
              common::ObString ip_obstr(ip_len, ip_str);
              if (!entry.svr_addr_.set_ip_addr(ip_obstr, static_cast<int32_t>(port))) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("failed to set ip addr", K(ret), K(ip_obstr), K(port));
              } else if (OB_FAIL(entries.push_back(entry))) {
                LOG_WARN("failed to push back entry", K(ret));
              }
            } else if (OB_FAIL(entries.push_back(entry))) {
              LOG_WARN("failed to push back entry", K(ret));
            }
          }
        }
        
        if (stmt) {
          guard->finalize_query(stmt);
        }
      }
    }
  }
  return ret;
}

int ObReservedSnapshotTableStorage::delete_expired(
    const uint64_t tenant_id,
    const common::ObAddr &svr_addr)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (OB_UNLIKELY(!svr_addr.ip_to_string(ip, sizeof(ip)))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ip to string failed", K(ret), K(svr_addr));
    } else {
      ObSQLiteConnectionGuard guard(pool_);
      if (!guard) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to acquire connection", K(ret));
      } else {
        const char *delete_sql =
          "DELETE FROM __all_reserved_snapshot "
          "WHERE tenant_id = ? AND svr_ip = ? AND svr_port = ?;";
        
        auto binder = [tenant_id, &ip, &svr_addr](ObSQLiteBinder &b) -> int {
          b.bind_int64(tenant_id);
          b.bind_text(ip, static_cast<int>(strlen(ip)));
          b.bind_int64(svr_addr.get_port());
          return OB_SUCCESS;
        };
        
        int64_t affected_rows = 0;
        if (OB_FAIL(guard->execute(delete_sql, binder, &affected_rows))) {
          LOG_WARN("failed to execute delete", K(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

