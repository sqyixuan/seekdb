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

#define USING_LOG_PREFIX SERVER

#include "ob_arbitration_service_replica_task_info.h"

namespace oceanbase
{
namespace share
{

static const char* arbitration_service_replica_task_type_strs[] = {
  "ADD REPLICA",
  "REMOVE REPLICA"
};

const char* ObArbitrationServiceReplicaTaskType::get_type_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(arbitration_service_replica_task_type_strs) == (int64_t)MAX_TYPE,
                "arbitration_service_replica_task_type string array size mismatch enum ArbitrationServiceReplicaTaskType count");
  const char *str = NULL;
  if (type_ > INVALID_TYPE && type_ < MAX_TYPE) {
    str = arbitration_service_replica_task_type_strs[static_cast<int64_t>(type_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ArbitrationServiceReplicaTaskType", K_(type));
  }
  return str;
}

int64_t ObArbitrationServiceReplicaTaskType::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), "type", get_type_str());
  J_OBJ_END();
  return pos;
}

int ObArbitrationServiceReplicaTaskType::parse_from_string(const ObString &type)
{
  int ret = OB_SUCCESS;
  bool found = false;
  STATIC_ASSERT(ARRAYSIZEOF(arbitration_service_replica_task_type_strs) == (int64_t)MAX_TYPE,
                "arbitration_service_replica_task_type string array size mismatch enum ArbitrationServiceReplicaTaskType count");
  for (int i = 0; i < ARRAYSIZEOF(arbitration_service_replica_task_type_strs) && !found; i++) {
    if (type.case_compare(ObString::make_string(arbitration_service_replica_task_type_strs[i])) == 0) {
      type_ = static_cast<ArbitrationServiceReplicaTaskType>(i);
      found = true;
      break;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse type from string", KR(ret), K(type), K_(type));
  }
  return ret;
}

int ObArbitrationServiceReplicaTaskInfo::build(
    const int64_t create_time_us,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t task_id,
    const ObArbitrationServiceReplicaTaskType &task_type,
    const share::ObTaskId &trace_id,
    const ObString &arbitration_service,
    const ObArbitrationServiceType &arbitration_service_type,
    const ObString &comment)
{
  int ret = OB_SUCCESS;
  reset();
  ObArbitrationServiceType arbitration_service_type_to_set;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
        || !ls_id.is_valid_with_tenant(tenant_id)
        || task_id <= 0
        || !task_type.is_valid()
        || trace_id.is_invalid()
        || nullptr == arbitration_service
        || !arbitration_service_type.is_valid()
        || nullptr == comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(task_id), K(task_type),
             K(trace_id), K(arbitration_service), K(arbitration_service_type),
             K(comment));
  } else if (OB_FAIL(arbitration_service_.assign(arbitration_service))) {
    LOG_WARN("fail to assign arbitration_service", KR(ret), K(arbitration_service));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("fail to assign comment", KR(ret), K(comment));
  } else if (OB_FAIL(ObArbitrationServiceInfo::parse_arbitration_service_with_type(
                     arbitration_service, arbitration_service_type_to_set))) {
    LOG_WARN("fail to parse target arbitration service type", KR(ret), K(arbitration_service));
  } else if (arbitration_service_type_to_set != arbitration_service_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("target arbitration service has different type with the given type", KR(ret),
             K(arbitration_service_type_to_set), K(arbitration_service_type));
  } else {
    create_time_us_ = create_time_us;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    task_id_ = task_id;
    task_type_ = task_type;
    trace_id_ = trace_id;
    arbitration_service_type_ = arbitration_service_type;
  }
  return ret;
}

void ObArbitrationServiceReplicaTaskInfo::reset()
{
  create_time_us_ = 0;
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  task_id_ = 0;
  task_type_.reset();
  trace_id_.reset();
  arbitration_service_.reset();
  arbitration_service_type_.reset();
  comment_.reset();
}

int ObArbitrationServiceReplicaTaskInfo::assign(const ObArbitrationServiceReplicaTaskInfo &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (OB_FAIL(arbitration_service_.assign(other.arbitration_service_))) {
    LOG_WARN("fail to assign target arbitration service", KR(ret), K(other));
  } else if (OB_FAIL(comment_.assign(other.comment_))) {
    LOG_WARN("fail to assign comment", KR(ret), K(other));
  } else {
    create_time_us_ = other.create_time_us_;
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    task_id_ = other.task_id_;
    trace_id_ = other.trace_id_;
    task_type_ = other.task_type_;
    arbitration_service_type_ = other.arbitration_service_type_;
  }
  return ret;
}

bool ObArbitrationServiceReplicaTaskInfo::is_equal(const ObArbitrationServiceReplicaTaskInfo &other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else if (tenant_id_ == other.tenant_id_
             && ls_id_ == other.ls_id_
             && task_id_ == other.task_id_) {
    is_equal = true;
  }
  return is_equal;
}

bool ObArbitrationServiceReplicaTaskInfo::is_valid() const
{
  return ls_id_.is_valid_with_tenant(tenant_id_)
         && task_id_ > 0
         && task_type_.is_valid()
         && !trace_id_.is_invalid()
         && !arbitration_service_.empty()
         && arbitration_service_type_.is_valid()
         && !comment_.empty();
}

int ObArbitrationServiceReplicaTaskInfo::get_arbitration_service_addr(ObAddr &arbitration_service) const 
{
  int ret = OB_SUCCESS;
  if (!arbitration_service_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arbitration_service_ is invalid", KR(ret), K_(arbitration_service));
  } else if (!arbitration_service_type_.is_addr()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected type", KR(ret), K_(arbitration_service_type));
  } else if (OB_FAIL(arbitration_service.parse_from_string(arbitration_service_.string()))) {
    LOG_WARN("fail to parse arbitration_service_ to addr", KR(ret), K_(arbitration_service));
  }
  return ret;
}

int64_t ObArbitrationServiceReplicaTaskInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(
      K_(create_time_us),
      K_(tenant_id),
      K_(ls_id),
      K_(task_id),
      K_(task_type),
      K_(trace_id),
      K_(arbitration_service),
      K_(arbitration_service_type),
      K_(comment));
  J_OBJ_END();
  return pos;
}

int ObArbitrationServiceReplicaTaskInfo::fill_dml_splicer(
    ObDMLSqlSplicer &dml_splicer) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", tenant_id_))
      || OB_FAIL(dml_splicer.add_pk_column("ls_id", ls_id_.id()))
      || OB_FAIL(dml_splicer.add_column("task_id", task_id_))
      || OB_FAIL(dml_splicer.add_column("trace_id", trace_id_))
      || OB_FAIL(dml_splicer.add_column("task_type", get_task_type_str()))
      || OB_FAIL(dml_splicer.add_column("arbitration_service", get_arbitration_service_string()))
      || OB_FAIL(dml_splicer.add_column("arbitration_service_type", get_arbitration_service_type_str()))
      || OB_FAIL(dml_splicer.add_column("comment", comment_.string()))) {
    LOG_WARN("add column failed", KR(ret), K_(tenant_id), K_(ls_id), K_(task_id), K_(task_type), K_(trace_id),
             K_(arbitration_service), K_(arbitration_service_type), K_(comment));
  }
  LOG_INFO("success to fill dml splicer for ObArbitrationServiceReplicaTaskInfo");
  return ret;
}
} // end namespace share
} // end namespace oceanbase
