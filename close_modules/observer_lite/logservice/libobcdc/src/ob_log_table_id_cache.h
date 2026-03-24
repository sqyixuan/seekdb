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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_TABLE_ID_CACHE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_TABLE_ID_CACHE_H_

#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap

namespace oceanbase
{
namespace libobcdc
{
// Cache of Global General Index
struct TableID
{
  uint64_t table_id_;

  TableID(const uint64_t table_id) : table_id_(table_id) {}

  int64_t hash() const
  {
    return static_cast<int64_t>(table_id_);
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  bool operator== (const TableID &other) const
  {
    return table_id_ == other.table_id_;
  }

  TO_STRING_KV(K_(table_id));
};

// Record table_id
// 1. for the primary table, record itself
// 2. For an index table, record table_id of its primary table
struct TableInfo
{
  uint64_t table_id_;

  TableInfo() { reset(); }
  ~TableInfo() { reset(); }

  void reset();
  int init(const uint64_t table_id);

  TO_STRING_KV(K_(table_id));
};
struct TableInfoEraserByTenant
{
  uint64_t tenant_id_;
  bool is_global_normal_index_;

  explicit TableInfoEraserByTenant(const uint64_t id, const bool is_global_normal_index)
    : tenant_id_(id), is_global_normal_index_(is_global_normal_index) {}
  bool operator()(const TableID &table_id_key, TableInfo &tb_info);
};

struct TableInfoEraserByDatabase
{
  uint64_t database_id_;
  
  explicit TableInfoEraserByDatabase(const uint64_t database_id)
    : database_id_(database_id) {}
  
  bool operator()(const TableID &table_id_key, uint64_t &val);
};

// Global General Index Cache
typedef common::ObLinearHashMap<TableID, TableInfo> GIndexCache;
// TableIDCache, contains the table_id in tb_white_list and not in tb_black_list
// The kv pair is <table_id, database_id>
typedef common::ObLinearHashMap<TableID, uint64_t> TableIDCache;
}
}

#endif
