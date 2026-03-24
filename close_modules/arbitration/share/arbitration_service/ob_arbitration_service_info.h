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

#ifndef OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_INFO_H_
#define OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_INFO_H_

#include "share/ob_arbitration_service_status.h" // for ObArbitrationServiceStatus
#include "share/ob_dml_sql_splicer.h"  // for ObDMLSqlSplicer
#include "lib/net/ob_addr.h"           // for ObAddr

namespace oceanbase
{
namespace share
{
class ObArbitrationServiceType
{
  OB_UNIS_VERSION(1);
public:
  // only support addr now
  enum ArbitrationServiceType
  {
    INVALID_ARBITRATION_SERVICE_TYPE = -1,
    ADDR = 0,
    URL = 1,
    MAX_TYPE
  };
public:
  ObArbitrationServiceType() : type_(INVALID_ARBITRATION_SERVICE_TYPE) {}
  explicit ObArbitrationServiceType(ArbitrationServiceType type) : type_(type) {}

  ObArbitrationServiceType &operator=(const ArbitrationServiceType type) { type_ = type; return *this; }
  ObArbitrationServiceType &operator=(const ObArbitrationServiceType &other) { type_ = other.type_; return *this; }
  void reset() { type_ = INVALID_ARBITRATION_SERVICE_TYPE; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void assign(const ObArbitrationServiceType &other) { type_ = other.type_; }
  bool operator==(const ObArbitrationServiceType &other) const { return other.type_ == type_; }
  bool operator!=(const ObArbitrationServiceType &other) const { return other.type_ != type_; }
  bool is_valid() const { return INVALID_ARBITRATION_SERVICE_TYPE < type_ && MAX_TYPE > type_; }
  bool is_addr() const { return ADDR == type_; }
  bool is_url() const { return URL == type_; }
  int parse_from_string(const ObString &type);
  const ArbitrationServiceType &get_type() const { return type_; }
  const char* get_type_str() const;
private:
  ArbitrationServiceType type_;
};

// [class_full_name] ObArbitrationServiceInfo
// [class_functions] Use this class to build info in __all_arbitration_service
// [class_attention] None
class ObArbitrationServiceInfo
{
  OB_UNIS_VERSION(1);
public:
  ObArbitrationServiceInfo() : arbitration_service_key_("ArbInfo"),
                               arbitration_service_("ArbInfo"),
                               previous_arbitration_service_("ArbInfo"),
                               type_() {}
  ~ObArbitrationServiceInfo() {}
  static int parse_arbitration_service_with_type(const ObString &arbitration_service, ObArbitrationServiceType &type);
  int init(
      const ObString &arbitration_service_key,
      const ObString &arbitration_service,
      const ObString &previous_arbitration_service,
      const ObArbitrationServiceType &type);
  void reset();
  int assign(const ObArbitrationServiceInfo &other);
  bool is_equal(const ObArbitrationServiceInfo &other) const;
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int fill_dml_splicer(ObDMLSqlSplicer &dml_splicer) const;
  bool arbitration_service_is_equal_to_string(const ObString &arbitration_service) const
  { return arbitration_service_.string().case_compare(arbitration_service) == 0; }
  bool previous_arbitration_service_is_equal_to_string(const ObString &previous_arbitration_service) const
  { return previous_arbitration_service_.string().case_compare(previous_arbitration_service) == 0; }

  // functions to operate members
  const ObString get_arbitration_service_key() const { return arbitration_service_key_.string(); }
  const ObString get_arbitration_service_string() const { return arbitration_service_.string(); }
  const ObString get_previous_arbitration_service_string() const { return previous_arbitration_service_.string(); }
  int get_arbitration_service_addr(ObAddr &arbitration_service) const;
  int get_previous_arbitration_service_addr(ObAddr &previous_arbitration_service) const;
  const ObArbitrationServiceType &get_type() const { return type_; }
  const char* get_type_str() const { return type_.get_type_str(); }
private:
  ObSqlString arbitration_service_key_;
  ObSqlString arbitration_service_;
  ObSqlString previous_arbitration_service_;
  ObArbitrationServiceType type_;
};
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_ARBITRATION_SERVICE_INFO_H_
