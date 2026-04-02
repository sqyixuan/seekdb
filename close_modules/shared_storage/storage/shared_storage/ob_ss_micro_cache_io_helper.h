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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_IO_HELPER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_IO_HELPER_H_

#include <stdint.h>
#include "share/io/ob_io_define.h"

namespace oceanbase
{
namespace storage
{

class ObSSMicroCacheIOHelper
{
public:
  static int async_read_block(const int64_t offset,
                              const int64_t size,
                              char *buf,
                              ObSSPhysicalBlockHandle &phy_block_handle,
                              common::ObIOHandle &io_handle);
  static int async_write_block(const int64_t offset,
                               const int64_t size,
                               const char *buf,
                               ObSSPhysicalBlockHandle &phy_block_handle,
                               common::ObIOHandle &io_handle);
  static int read_block(const int64_t offset,
                        const int64_t size,
                        char *buf,
                        ObSSPhysicalBlockHandle &phy_block_handle);
  static int write_block(const int64_t offset,
                         const int64_t size,
                         const char *buf,
                         ObSSPhysicalBlockHandle &phy_block_handle);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_IO_HELPER_H_ */
