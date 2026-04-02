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

#define USING_LOG_PREFIX SHARE

#include "share/ob_ss_io_device_wrapper.h"
#include "share/ob_device_manager.h"

namespace oceanbase
{
namespace share
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

ObSSIODeviceWrapper &ObSSIODeviceWrapper::get_instance()
{
  static ObSSIODeviceWrapper instance;
  return instance;
}

ObSSIODeviceWrapper::ObSSIODeviceWrapper()
  : local_device_(NULL), local_cache_device_(NULL), is_inited_(false)
{
}

ObSSIODeviceWrapper::~ObSSIODeviceWrapper()
{
  destroy();
}

// in share nothing mode, io device helper hold one local cache device, and never free
int ObSSIODeviceWrapper::get_local_device_from_mgr(share::ObLocalDevice *&local_device)
{
  int ret = OB_SUCCESS;

  ObIODevice* device = nullptr;
  ObString storage_type_prefix(OB_LOCAL_PREFIX);
  const ObStorageIdMod storage_id_mod(0, ObStorageUsedMod::STORAGE_USED_DATA);
  if (OB_FAIL(ObDeviceManager::get_local_device(storage_type_prefix, storage_id_mod, device))) {
    LOG_WARN("fail to get local device", K(ret));
  } else if (OB_ISNULL(local_device = static_cast<share::ObLocalDevice *>(device))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get local device", K(ret));
  }

  if (OB_FAIL(ret)) {
    //if fail, release the resource
    if (OB_NOT_NULL(local_device)) {
      ObDeviceManager::get_instance().release_device((ObIODevice *&)local_device);
      local_device = nullptr;
    }
  }
  return ret;
}

// in share storage mode, io device helper hold one local cache device, and never free
int ObSSIODeviceWrapper::get_local_cache_device_from_mgr(ObLocalCacheDevice *&local_cache_device)
{
  int ret = OB_SUCCESS;

  ObIODevice* device = nullptr;
  ObString storage_type_prefix(OB_LOCAL_CACHE_PREFIX);
  const ObStorageIdMod storage_id_mod(0, ObStorageUsedMod::STORAGE_USED_DATA);
  if (OB_FAIL(ObDeviceManager::get_local_device(storage_type_prefix, storage_id_mod, device))) {
    LOG_WARN("fail to get local cache device", KR(ret));
  } else if (OB_ISNULL(local_cache_device = static_cast<ObLocalCacheDevice *>(device))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to cast local cache device", KR(ret));
  }

  if (OB_FAIL(ret)) {
    //if fail, release the resource
    if (OB_NOT_NULL(local_cache_device)) {
      ObDeviceManager::get_instance().release_device((ObIODevice *&)local_cache_device);
      local_cache_device = nullptr;
    }
  }
  return ret;
}

int ObSSIODeviceWrapper::init(
    const char *data_dir,
    const char *sstable_dir,
    const int64_t block_size,
    const int64_t data_disk_percentage,
    const int64_t data_disk_size)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_IOD_OPT_CNT = 5;
  ObIODOpt iod_opt_array[MAX_IOD_OPT_CNT];
  ObIODOpts iod_opts;
  iod_opts.opts_ = iod_opt_array;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (OB_ISNULL(data_dir) || OB_ISNULL(sstable_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(data_dir), KP(sstable_dir));
  } else if ('/' != data_dir[0] && '.' != data_dir[0]) {
    ret = OB_IO_ERROR;
    LOG_ERROR("unknown storage type, not support", KR(ret), K(data_dir));
  } else {
    if (GCTX.is_shared_storage_mode()) { // shared storage mode
      if (OB_FAIL(get_local_cache_device_from_mgr(local_cache_device_))) {
        LOG_WARN("fail to get the tiered device", KR(ret));
      }
    } else { // shared nothing mode
      if (OB_FAIL(get_local_device_from_mgr(local_device_))) {
        LOG_WARN("fail to get the local device", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      iod_opt_array[0].set("data_dir", data_dir);
      iod_opt_array[1].set("sstable_dir", sstable_dir);
      iod_opt_array[2].set("block_size", block_size);
      iod_opt_array[3].set("datafile_disk_percentage", data_disk_percentage);
      iod_opt_array[4].set("datafile_size", data_disk_size);
      iod_opts.opt_cnt_ = MAX_IOD_OPT_CNT;
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY((nullptr != local_device_) || (nullptr != local_cache_device_))) {
    if (OB_UNLIKELY((nullptr != local_device_) && (nullptr != local_cache_device_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("both local device and local cache device are not null", KR(ret));
    } else if ((nullptr != local_device_) && OB_FAIL(local_device_->init(iod_opts))) {
      LOG_WARN("fail to init local device", KR(ret), K(data_dir), K(sstable_dir), K(block_size),
               K(data_disk_percentage), K(data_disk_size));
    } else if ((nullptr != local_cache_device_) && OB_FAIL(local_cache_device_->init(iod_opts))) {
      LOG_WARN("fail to init local cache device", KR(ret), K(data_dir), K(sstable_dir),
               K(block_size), K(data_disk_percentage), K(data_disk_size));
    } else {
      is_inited_ = true;
      LOG_INFO("finish to init io device", KR(ret), K(data_dir), K(sstable_dir), K(block_size),
               K(data_disk_percentage), K(data_disk_size));
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

void ObSSIODeviceWrapper::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    if (NULL != local_device_) {
      local_device_->destroy();
      ObDeviceManager::get_instance().release_device((ObIODevice *&)local_device_);
      local_device_ = NULL;
    }
    if (NULL != local_cache_device_) {
      local_cache_device_->destroy();
      ObDeviceManager::get_instance().release_device((ObIODevice *&)local_cache_device_);
      local_cache_device_ = NULL;
    }
    is_inited_ = false;
    LOG_INFO("io device wrapper destroy");
  }
}

} // namespace share
} // namespace oceanbase
