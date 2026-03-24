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

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"
#include "share/io/ob_io_manager.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;

int ObSSMicroCacheIOHelper::async_read_block(
    const int64_t offset,
    const int64_t size,
    char *buf,
    ObSSPhysicalBlockHandle &phy_block_handle,
    ObIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_UNLIKELY((offset < 0) || (size <= 0) || !phy_block_handle.is_valid()) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(offset), K(size), K(phy_block_handle), KP(buf));
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), "tenant_id", MTL_ID());
  } else {
    io_info.tenant_id_ = MTL_ID();
    io_info.fd_.first_id_ = ObIOFd::NORMAL_FILE_ID; // first_id is not used in shared storage mode
    io_info.fd_.second_id_ = file_manager->get_micro_cache_file_fd();
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    io_info.offset_ = offset;
    io_info.size_ = size;
    io_info.timeout_us_ = GCONF._data_storage_io_timeout;
    io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    io_info.flag_.set_read();
    io_info.user_data_buf_ = buf;
    if (OB_FAIL(io_info.phy_block_handle_.assign(phy_block_handle))) {
      LOG_WARN("fail to assign phy block handle", KR(ret), K(phy_block_handle));
    } else if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, io_handle))) {
      LOG_WARN("fail to aio_read", KR(ret), K(io_info));
    }
  }
  return ret;
}

int ObSSMicroCacheIOHelper::async_write_block(
    const int64_t offset,
    const int64_t size,
    const char *buf,
    ObSSPhysicalBlockHandle &phy_block_handle,
    ObIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_UNLIKELY((offset < 0) || (size <= 0) || !phy_block_handle.is_valid()) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(offset), K(size), K(phy_block_handle), KP(buf));
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), "tenant_id", MTL_ID());
  } else {
    io_info.tenant_id_ = MTL_ID();
    io_info.fd_.first_id_ = ObIOFd::NORMAL_FILE_ID; // first_id is not used in shared storage mode
    io_info.fd_.second_id_ = file_manager->get_micro_cache_file_fd();
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    io_info.offset_ = offset;
    io_info.size_ = size;
    io_info.timeout_us_ = GCONF._data_storage_io_timeout;
    io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    io_info.flag_.set_write();
    io_info.buf_ = buf;

    if (OB_FAIL(io_info.phy_block_handle_.assign(phy_block_handle))) {
      LOG_WARN("fail to assign phy block handle", KR(ret), K(phy_block_handle));
    } else if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, io_handle))) {
      LOG_WARN("fail to aio_write", KR(ret), K(io_info));
    } else {
      ret = EventTable::EN_SHARED_STORAGE_MICRO_CACHE_WRITE_DISK_ERR;
    }
  }
  return ret;
}

int ObSSMicroCacheIOHelper::read_block(
    const int64_t offset,
    const int64_t size,
    char *buf,
    ObSSPhysicalBlockHandle &phy_block_handle)
{
  int ret = OB_SUCCESS;
  ObIOHandle io_handle;
  if (OB_FAIL(async_read_block(offset, size, buf, phy_block_handle, io_handle))) {
    LOG_WARN("fail to async read block", KR(ret), K(offset), K(size), KP(buf), K(phy_block_handle));
  } else if (OB_FAIL(io_handle.wait())) {
    LOG_WARN("fail to wait", KR(ret), K(offset), K(size), KP(buf), K(phy_block_handle));
  }
  return ret;
}

int ObSSMicroCacheIOHelper::write_block(
    const int64_t offset,
    const int64_t size,
    const char *buf,
    ObSSPhysicalBlockHandle &phy_block_handle)
{
  int ret = OB_SUCCESS;
  ObIOHandle io_handle;
  if (OB_FAIL(async_write_block(offset, size, buf, phy_block_handle, io_handle))) {
    LOG_WARN("fail to async write block", KR(ret), K(offset), K(size), KP(buf), K(phy_block_handle));
  } else if (OB_FAIL(io_handle.wait())) {
    LOG_WARN("fail to wait", KR(ret), K(offset), K(size), KP(buf), K(phy_block_handle));
  }
  return ret;
}


} /* namespace storage */
} /* namespace oceanbase */
