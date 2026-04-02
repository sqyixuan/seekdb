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

#ifndef OCEANBASE_LIBOBCDC_START_SCHEMA_MATCHER_H__
#define OCEANBASE_LIBOBCDC_START_SCHEMA_MATCHER_H__

#include "lib/utility/ob_print_utils.h"          // TO_STRING_KV
#include "lib/container/ob_array.h"              // ObArray

namespace oceanbase
{
namespace libobcdc
{
class IObLogStartSchemaMatcher
{
public:
  virtual ~IObLogStartSchemaMatcher() {}

public:
  // match function
  virtual int match_data_start_schema_version(const uint64_t tenant_id,
      bool &match,
      int64_t &schema_version) = 0;
};


/*
 * Impl.
 *
 */
class ObLogStartSchemaMatcher : public IObLogStartSchemaMatcher
{
  const char* DEFAULT_START_SCHEMA_VERSION_STR = "|";
public:
  ObLogStartSchemaMatcher();
  virtual ~ObLogStartSchemaMatcher();

public:
  int init(const char *schema_version_str);
  int destroy();

  // Matches a tenant and returns the schema_version set according to the profile
  int match_data_start_schema_version(const uint64_t tenant_id, bool &match, int64_t &schema_version);

private:
  // Initialising the configuration according to the configuration file
  int set_pattern_(const char *schema_version_str);

  int build_tenant_schema_version_();

private:
  struct TenantSchema
  {
    uint64_t tenant_id_;
    int64_t schema_version_;

    void reset();

    TO_STRING_KV(K(tenant_id_), K(schema_version_));
  };
  typedef common::ObArray<TenantSchema> TenantSchemaArray;

private:
  char *buf_;
  int64_t buf_size_;

  TenantSchemaArray tenant_schema_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStartSchemaMatcher);
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_START_SCHEMA_MATCHER_H__ */
