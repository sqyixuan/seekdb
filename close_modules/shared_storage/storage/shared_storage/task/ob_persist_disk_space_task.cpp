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

#include "ob_persist_disk_space_task.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObPersistDiskSpaceTask::ObPersistDiskSpaceTask()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID)
{
}

int ObPersistDiskSpaceTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPersistDiskSpaceTask has already been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    LOG_INFO("succ to init persist disk space task", K(tenant_id));
  }
  return ret;
}

int ObPersistDiskSpaceTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t INTERVAL_US = 1L * 1000L * 1000L; // 1s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("persist disk space task is not init", KR(ret));
  } else if (OB_UNLIKELY(-1 == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, INTERVAL_US, true /*schedule repeatly*/))) {
    LOG_WARN("fail to schedule task ObPersistDiskSpaceTask", KR(ret), K_(tenant_id), K(tg_id));
  } else {
    LOG_INFO("succ to start persist disk space task", K_(tenant_id), K(tg_id));
  }
  return ret;
}

void ObPersistDiskSpaceTask::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObPersistDiskSpaceTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObTenantStorageMetaService *tsms = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tsms = MTL(ObTenantStorageMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is null", KR(ret), KP(tsms), K_(tenant_id));
  } else if (!tsms->is_started()) {
    // because ObTenantStorageMetaService maybe has not been started, seq_generator is not inited, report error code OB_NOT_INIT
    // do nothing, wait for ObTenantStorageMetaService started
  } else if (OB_FAIL(persist_disk_space_meta())) {
    LOG_WARN("fail to persist tenant disk space meta", KR(ret), K_(tenant_id));
  }
}

int ObPersistDiskSpaceTask::persist_disk_space_meta()
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  ObTenantFileManager *file_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), KP(tnt_disk_space_mgr), K_(tenant_id));
  } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), KP(file_mgr), K_(tenant_id));
  } else if (OB_FAIL(file_mgr->write_tenant_disk_space_meta())) {
    LOG_WARN("fail to write tenant disk space meta", KR(ret));
  } else {
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("succ to write tenant disk space meta", K_(tenant_id),
                "total_disk_size(MB)", (tnt_disk_space_mgr->get_total_disk_size())/MB,
                "all_disk_cache_info", (tnt_disk_space_mgr->get_all_disk_cache_info()));
    }
  }
  return ret;
}

} // storage
} // oceanbase
