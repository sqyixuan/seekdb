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

#ifndef OCEANBASE_SHARE_SHARED_STORAGE_IO_OB_IO_REQUEST_H_
#define OCEANBASE_SHARE_SHARED_STORAGE_IO_OB_IO_REQUEST_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/io/ob_io_define.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace common
{

class ObSSIORequest : public ObIORequest
{
public:
  ObSSIORequest();
  virtual ~ObSSIORequest();
  virtual void destroy() override;
  virtual void reset() override;
  TO_STRING_KV(K_(is_inited), K_(tenant_id), KP_(control_block), K_(ref_cnt), KP_(raw_buf), K_(fd),
               K_(trace_id), K_(retry_count), K_(tenant_io_mgr), KPC_(io_result),
               K_(phy_block_handle), K_(fd_cache_handle));

private:
  void try_close_device_and_fd();
  virtual int set_block_handle(const ObIOInfo &info) override;
  virtual int set_fd_cache_handle(const ObIOInfo &info) override;

private:
  storage::ObSSPhysicalBlockHandle phy_block_handle_; // hold ref_cnt until this io finished
  storage::ObSSFdCacheHandle fd_cache_handle_; // avoid fd cache to be evicted when io has not finished
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OCEANBASE_SHARE_SHARED_STORAGE_IO_OB_IO_REQUEST_H_ */
