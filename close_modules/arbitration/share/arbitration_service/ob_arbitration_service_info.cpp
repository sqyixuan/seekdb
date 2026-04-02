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

#include "ob_arbitration_service_info.h"

namespace oceanbase
{
namespace share
{

static const char* arbitration_service_type_strs[] = {
  "ADDR",
  "URL"
};

const char* ObArbitrationServiceType::get_type_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(arbitration_service_type_strs) == (int64_t)MAX_TYPE,
                "arbitration_service_type string array size mismatch enum ArbitrationServiceType count");
  const char *str = NULL;
  if (type_ > INVALID_ARBITRATION_SERVICE_TYPE
      && type_ < MAX_TYPE) {
    str = arbitration_service_type_strs[static_cast<int64_t>(type_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ArbitrationServiceType", K_(type));
  }
  return str;
}

int64_t ObArbitrationServiceType::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type));
  J_OBJ_END();
  return pos;
}

int ObArbitrationServiceType::parse_from_string(const ObString &type)
{
  int ret = OB_SUCCESS;
  STATIC_ASSERT(ARRAYSIZEOF(arbitration_service_type_strs) == (int64_t)MAX_TYPE,
                "arbitration_service_type string array size mismatch enum ArbitrationServiceType count");
  ObString addr_type_in_string = ObString::make_string(
      arbitration_service_type_strs[static_cast<int64_t>(ADDR)]);
  ObString url_type_in_string = ObString::make_string(
      arbitration_service_type_strs[static_cast<int64_t>(URL)]);
  if (OB_UNLIKELY(type.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(type));
  } else if (type.case_compare(addr_type_in_string) == 0) {
    type_ = ADDR;
  } else if (type.case_compare(url_type_in_string) == 0) {
    type_ = URL;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to parse type from string", KR(ret), K(type), K_(type));
  }
  return ret;
}

int ObArbitrationServiceInfo::init(
    const ObString &arbitration_service_key,
    const ObString &arbitration_service,
    const ObString &previous_arbitration_service,
    const ObArbitrationServiceType &type)
{
  int ret = OB_SUCCESS;
  reset();
  arbitration_service_key_.set_label(lib::ObLabel("ArbSrv"));
  arbitration_service_.set_label(lib::ObLabel("ArbSrv"));
  previous_arbitration_service_.set_label(lib::ObLabel("ArbSrv"));
  if (OB_ISNULL(arbitration_service_key)
      || OB_ISNULL(arbitration_service)
      || !type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arbitration_service_key), K(arbitration_service), K(type));
  } else if (OB_FAIL(arbitration_service_key_.assign(arbitration_service_key))) {
    LOG_WARN("fail to assign arbitration_service_key", KR(ret), K(arbitration_service_key));
  } else if (OB_FAIL(arbitration_service_.assign(arbitration_service))) {
    LOG_WARN("fail to assign arbitration_service", KR(ret), K(arbitration_service));
  } else if (OB_FAIL(previous_arbitration_service_.assign(previous_arbitration_service))) {
    LOG_WARN("fail to assign previous_arbitration_service", KR(ret), K(previous_arbitration_service));
  } else {
    type_.assign(type);
  }
  return ret;
}


void ObArbitrationServiceInfo::reset()
{
  arbitration_service_key_.reset();
  arbitration_service_.reset();
  previous_arbitration_service_.reset();
  type_.reset();
}

int ObArbitrationServiceInfo::assign(const ObArbitrationServiceInfo &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (OB_FAIL(arbitration_service_key_.assign(other.arbitration_service_key_))) {
    LOG_WARN("fail to assign arbitration_service_key_", KR(ret), K(other));
  } else if (OB_FAIL(arbitration_service_.assign(other.arbitration_service_))) {
    LOG_WARN("fail to assign arbitration_service_", KR(ret), K(other));
  } else if (OB_FAIL(previous_arbitration_service_.assign(other.previous_arbitration_service_))) {
    LOG_WARN("fail to assign previous_arbitration_service_", KR(ret), K(other));
  } else {
    type_.assign(other.type_);
  }
  return ret;
}

int ObArbitrationServiceInfo::parse_arbitration_service_with_type(
    const ObString &arbitration_service,
    ObArbitrationServiceType &type)
{
  int ret = OB_SUCCESS;
  // TODO: only support ADDR type now, will support URL in the future
  ObAddr arbitration_service_addr;
  type = ObArbitrationServiceType::INVALID_ARBITRATION_SERVICE_TYPE;
  if (OB_FAIL(arbitration_service_addr.parse_from_string(arbitration_service))) {
    LOG_WARN("fail to parse from string", KR(ret), K(arbitration_service));
  } else {
    type = ObArbitrationServiceType::ADDR;
  }
  return ret;
}

bool ObArbitrationServiceInfo::is_equal(const ObArbitrationServiceInfo &other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else if (arbitration_service_key_.string() == other.arbitration_service_key_.string()
      && arbitration_service_.string() == other.arbitration_service_.string()
      && previous_arbitration_service_.string() == other.previous_arbitration_service_.string()
      && type_ == other.type_) {
    is_equal = true;
  }
  return is_equal;
}

bool ObArbitrationServiceInfo::is_valid() const
{
  return arbitration_service_key_.is_valid()
         && arbitration_service_.is_valid()
         && type_.is_valid();
}

int ObArbitrationServiceInfo::get_arbitration_service_addr(ObAddr &arbitration_service) const
{
  int ret = OB_SUCCESS;
  if (!arbitration_service_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arbitration_service_ is invalid", KR(ret), K_(arbitration_service));
  } else if (!type_.is_addr()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected type", KR(ret), K_(type));
  } else if (OB_FAIL(arbitration_service.parse_from_string(arbitration_service_.string()))) {
    LOG_WARN("fail to parse arbitration_service_ to addr", KR(ret), K_(arbitration_service));
  }
  return ret;
}

int ObArbitrationServiceInfo::get_previous_arbitration_service_addr(ObAddr &previous_arbitration_service) const
{
  int ret = OB_SUCCESS;
  if (!previous_arbitration_service_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arbitration_service_ is invalid", KR(ret), K_(arbitration_service));
  } else if (!type_.is_addr()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected type", KR(ret), K_(type));
  } else if (OB_FAIL(previous_arbitration_service.parse_from_string(previous_arbitration_service_.string()))) {
    LOG_WARN("fail to parse arbitration_service_ to addr", KR(ret), K_(previous_arbitration_service));
  }
  return ret;
}

int64_t ObArbitrationServiceInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(
      K_(arbitration_service_key),
      K_(arbitration_service),
      K_(previous_arbitration_service),
      K_(type));
  J_OBJ_END();
  return pos;
}

int ObArbitrationServiceInfo::fill_dml_splicer(
    ObDMLSqlSplicer &dml_splicer) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret));
  } else if (OB_FAIL(dml_splicer.add_pk_column("arbitration_service_key", arbitration_service_key_))
      || OB_FAIL(dml_splicer.add_column("arbitration_service", arbitration_service_))
      || OB_FAIL(dml_splicer.add_column("previous_arbitration_service", previous_arbitration_service_))
      || OB_FAIL(dml_splicer.add_column("type", type_.get_type_str()))) {
    LOG_WARN("add column failed", KR(ret), K_(arbitration_service_key), K_(arbitration_service), K_(previous_arbitration_service), K_(type));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
