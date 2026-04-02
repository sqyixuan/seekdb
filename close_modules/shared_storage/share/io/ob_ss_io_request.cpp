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

#define USING_LOG_PREFIX COMMON

#include "share/io/ob_ss_io_request.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_file_manager.h"
#include "share/backup/ob_backup_io_adapter.h"

namespace oceanbase
{
namespace common
{
using namespace oceanbase::share;

ObSSIORequest::ObSSIORequest() : phy_block_handle_(), fd_cache_handle_()
{
}

ObSSIORequest::~ObSSIORequest()
{
  destroy();
}

void ObSSIORequest::reset() //only for test, not dec resut_ref
{
  try_close_device_and_fd();
  phy_block_handle_.reset();
  fd_cache_handle_.reset();
  ObIORequest::reset();
}

void ObSSIORequest::destroy()
{
  try_close_device_and_fd();
  phy_block_handle_.reset();
  fd_cache_handle_.reset();
  ObIORequest::destroy();
}
// TODO(renju.rj) the logic of try_close_device_and_fd should be encapsulated in ObIOFd
void ObSSIORequest::try_close_device_and_fd()
{
  int ret = OB_SUCCESS;
  if ((nullptr != io_result_) && io_result_->flag_.is_need_close_dev_and_fd()) {
    ObIODevice *device_handle = nullptr;
    if (OB_ISNULL(device_handle = fd_.device_handle_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("device handle is null", KR(ret), K(*this));
    } else if (device_handle->is_object_device()) {
      ObBackupIoAdapter io_adapter;
      if (OB_FAIL(io_adapter.close_device_and_fd(device_handle, fd_))) {
        LOG_WARN("fail to close device and fd", KR(ret), K(*this));
      }
    }
  }
}

int ObSSIORequest::set_block_handle(const ObIOInfo &info)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode() && info.phy_block_handle_.is_valid()) {
    if (OB_FAIL(phy_block_handle_.assign(info.phy_block_handle_))) {
      LOG_WARN("fail to assign physical block handle", KR(ret), "physcial_block_handle", info.phy_block_handle_);
    }
  }
  return ret;
}

int ObSSIORequest::set_fd_cache_handle(const ObIOInfo &info)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode() && info.fd_cache_handle_.is_valid()) {
    if (OB_FAIL(fd_cache_handle_.assign(info.fd_cache_handle_))) {
      LOG_WARN("fail to assign fd cache handle", KR(ret), "fd_cache_handle", info.fd_cache_handle_);
    }
  }
  return ret;
}

} /* namespace common */
} /* namespace oceanbase */
