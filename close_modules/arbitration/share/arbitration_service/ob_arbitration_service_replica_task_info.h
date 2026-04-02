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

#ifndef OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_TASK_INFO_H_
#define OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_TASK_INFO_H_

#include "share/ob_dml_sql_splicer.h"  // for ObDMLSqlSplicer
#include "lib/net/ob_addr.h"           // for ObAddr
#include "share/arbitration_service/ob_arbitration_service_info.h" // for ObArbitrationServiceInfo
#include "share/ob_ls_id.h"            // for ObLSID
#include "share/ob_define.h"           // for ObTaskId

namespace oceanbase
{
namespace share
{
class ObArbitrationServiceReplicaTaskType
{
  OB_UNIS_VERSION(1);
public:
  enum ArbitrationServiceReplicaTaskType
  {
    INVALID_TYPE = -1,
    ADD_REPLICA = 0,
    REMOVE_REPLICA = 1,
    MAX_TYPE
  };
public:
  ObArbitrationServiceReplicaTaskType() : type_(INVALID_TYPE) {}
  explicit ObArbitrationServiceReplicaTaskType(ArbitrationServiceReplicaTaskType type) : type_(type) {}

  ObArbitrationServiceReplicaTaskType &operator=(const ArbitrationServiceReplicaTaskType type) { type_ = type; return *this; }
  ObArbitrationServiceReplicaTaskType &operator=(const ObArbitrationServiceReplicaTaskType &other) { type_ = other.type_; return *this; }
  bool operator==(const ObArbitrationServiceReplicaTaskType &other) const { return other.type_ == type_; }
  bool operator!=(const ObArbitrationServiceReplicaTaskType &other) const { return other.type_ != type_; }
  void reset() { type_ = INVALID_TYPE; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void assign(const ObArbitrationServiceReplicaTaskType &other) { type_ = other.type_; }
  bool is_valid() const { return INVALID_TYPE < type_ && MAX_TYPE > type_; }
  bool is_add_task() const { return ADD_REPLICA == type_; }
  bool is_remove_task() const { return REMOVE_REPLICA == type_; }
  int parse_from_string(const ObString &type);
  const ArbitrationServiceReplicaTaskType &get_type() const { return type_; }
  const char* get_type_str() const;
private:
  ArbitrationServiceReplicaTaskType type_;
};

// [class_full_name] ObArbitrationServiceReplicaTaskInfo
// [class_functions] Use this class to build info in __all_arbitration_service_replica_task
// [class_attention] None
class ObArbitrationServiceReplicaTaskInfo
{
  OB_UNIS_VERSION(1);
public:
  ObArbitrationServiceReplicaTaskInfo()
    : create_time_us_(0),
      tenant_id_(common::OB_INVALID_ID),
      ls_id_(),
      task_id_(0),
      task_type_(),
      trace_id_(),
      arbitration_service_("ArbTaskInfo"),
      arbitration_service_type_(),
      comment_("ArbTaskInfo") {}
  ~ObArbitrationServiceReplicaTaskInfo() {}

  int build(
      const int64_t create_time_us,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t task_id,
      const ObArbitrationServiceReplicaTaskType &task_type,
      const share::ObTaskId &trace_id,
      const ObString &arbitration_service,
      const ObArbitrationServiceType &arbitration_service_type,
      const ObString &comment);
  void reset();
  int assign(const ObArbitrationServiceReplicaTaskInfo &other);
  bool is_equal(const ObArbitrationServiceReplicaTaskInfo &other) const;
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int fill_dml_splicer(ObDMLSqlSplicer &dml_splicer) const;

  // functions to get members
  inline int64_t get_create_time_us() const { return create_time_us_; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  inline int64_t get_task_id() const { return task_id_; }
  const share::ObTaskId &get_trace_id() const { return trace_id_; }
  const ObString get_comment() const { return comment_.string(); }

  const ObArbitrationServiceReplicaTaskType &get_task_type() const { return task_type_; }
  const char* get_task_type_str() const { return task_type_.get_type_str(); }

  const ObString get_arbitration_service_string() const { return arbitration_service_.string(); }
  int get_arbitration_service_addr(ObAddr &arbitration_service) const;

  const ObArbitrationServiceType &get_arbitration_service_type() const { return arbitration_service_type_; }
  const char* get_arbitration_service_type_str() const { return arbitration_service_type_.get_type_str(); }

private:
  int64_t create_time_us_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t task_id_;
  ObArbitrationServiceReplicaTaskType task_type_;
  share::ObTaskId trace_id_;
  ObSqlString arbitration_service_;
  ObArbitrationServiceType arbitration_service_type_;
  ObSqlString comment_;
};
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_TASK_INFO_H_
