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

#ifndef OCEANBASE_SHARE_SHARED_STORAGE_OB_IO_DEVICE_WRAPPER_H_
#define OCEANBASE_SHARE_SHARED_STORAGE_OB_IO_DEVICE_WRAPPER_H_

#include <stdint.h>
#include "share/config/ob_server_config.h"
#include "share/ob_local_device.h"
#include "storage/shared_storage/ob_local_cache_device.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace share
{

class ObSSIODeviceWrapper final
{
public:
  static ObSSIODeviceWrapper &get_instance();

  int init(const char *data_dir,
           const char *sstable_dir,
           const int64_t block_size,
           const int64_t data_disk_percentage,
           const int64_t data_disk_size);
  void destroy();

  ObIODevice &get_local_device()
  {
    if (GCTX.is_shared_storage_mode()) {
      abort_unless(NULL != local_cache_device_);
      return *local_cache_device_;
    } else {
      abort_unless(NULL != local_device_);
      return *local_device_;
    }
  }

  // just for unittest (mock_tenant_module_env)
  void set_local_device(ObLocalDevice *local_device)
  {
    local_device_ = local_device;
  }

  // just for unittest (mock_tenant_module_env)
  void set_local_cache_device(storage::ObLocalCacheDevice *local_cache_device)
  {
    local_cache_device_ = local_cache_device;
  }

private:
  ObSSIODeviceWrapper();
  ~ObSSIODeviceWrapper();
  int get_local_device_from_mgr(share::ObLocalDevice *&local_device);
  int get_local_cache_device_from_mgr(storage::ObLocalCacheDevice *&local_cache_device);

private:
  ObLocalDevice *local_device_; // for share nothing mode
  storage::ObLocalCacheDevice *local_cache_device_; // for share storage mode
  bool is_inited_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SHARED_STORAGE_OB_IO_DEVICE_WRAPPER_H_
