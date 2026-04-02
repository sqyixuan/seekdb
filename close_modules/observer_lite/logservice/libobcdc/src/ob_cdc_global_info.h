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

#ifndef OCEANBASE_LIBOBCDC_GLOBAL_INFO_H_
#define OCEANBASE_LIBOBCDC_GLOBAL_INFO_H_

#include "ob_cdc_lob_aux_table_schema_info.h"

namespace oceanbase
{
namespace libobcdc
{
class ObCDCGlobalInfo
{
public:
  ObCDCGlobalInfo();
  ~ObCDCGlobalInfo() { reset(); }
  void reset();
  int init();

  OB_INLINE const ObCDCLobAuxTableSchemaInfo &get_lob_aux_table_schema_info() const { return lob_aux_table_schema_info_; }

  OB_INLINE uint64_t get_min_cluster_version() const { return min_cluster_version_; }
  OB_INLINE void update_min_cluster_version(const uint64_t min_cluster_version) { min_cluster_version_ = min_cluster_version; }

private:
  ObCDCLobAuxTableSchemaInfo lob_aux_table_schema_info_;
  uint64_t min_cluster_version_;

  DISALLOW_COPY_AND_ASSIGN(ObCDCGlobalInfo);
};
} // namespace libobcdc
} // namespace oceanbase

#endif
