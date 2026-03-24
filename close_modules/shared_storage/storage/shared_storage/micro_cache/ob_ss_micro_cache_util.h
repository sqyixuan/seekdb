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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_UTIL_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_UTIL_H_

#include <stdint.h>
#include "share/ob_define.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{

class ObSSMicroCacheUtil
{
public:
  static int parse_micro_block_indexs(const char *buf, const int64_t buf_len, 
      common::ObIArray<ObSSMicroBlockIndex> &micro_indexs);
  static int parse_phy_block_common_header(const char *buf, const int64_t buf_len, 
      ObSSPhyBlockCommonHeader &common_header);
  static int parse_normal_phy_block_header(const char *buf, const int64_t buf_len, 
      ObSSNormalPhyBlockHeader &header);
  static int dump_phy_block(char *buf, const int64_t buf_len, const char *file_path, 
      const bool only_index = false, const bool is_micro_meta = false);
  static int parse_normal_phy_block(char *dest_buf, const int64_t dest_len, const char *src_buf, 
      const int64_t src_len, int64_t &pos, const bool only_index);
  static int parse_ss_super_block(char *dest_buf, const int64_t dest_len, const char *src_buf, 
      const int64_t src_len, int64_t &pos);
  static int parse_checkpoint_block(char *dest_buf, const int64_t dest_len, char *src_buf, 
      const int64_t src_len, int64_t &pos, const bool is_micro_meta);
  static int calc_ls_tablet_cache_info(const uint64_t tenant_id,
                                       const ObSSTabletCacheMap &tablet_cache_info_map,
                                       common::ObIArray<ObSSLSCacheInfo> &ls_info_list,
                                       common::ObIArray<ObSSTabletCacheInfo> &tablet_info_list);
  static int64_t calc_ss_cache_mem_limit_size(const uint64_t tenant_id);
  static int64_t calc_ss_cache_expiration_time(const uint64_t tenant_id);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_UTIL_H_ */
