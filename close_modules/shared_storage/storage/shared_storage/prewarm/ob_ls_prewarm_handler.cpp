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

#include "ob_ls_prewarm_handler.h"
#include "logservice/ob_log_service.h"
#include "storage/shared_storage/prewarm/ob_ss_micro_cache_prewarm_service.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

ObSSLSPreWarmHandler::ObSSLSPreWarmHandler()
  : is_inited_(false),
    is_ls_stop_(false),
    is_sync_hot_micro_key_task_stop_(true),
    is_consume_hot_micro_key_task_stop_(false),
    is_leader_(false),
    ls_(nullptr),
    ls_id_(ObLSID::INVALID_LS_ID)
{
}

ObSSLSPreWarmHandler::~ObSSLSPreWarmHandler()
{
}

int ObSSLSPreWarmHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObSSMicroCachePrewarmService *prewarm_service = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("prewarm handler init twice", KR(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init prewarm handler get inavlid argument", KR(ret), KP(ls));
  } else if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache prewarm service is null", KR(ret));
  } else if (OB_FAIL(prewarm_service->init_sync_hot_micro_key_task(ls, is_sync_hot_micro_key_task_stop_))) {
    LOG_WARN("fail to init sync hot micro key task", KR(ret), KP(ls));
  } else if (OB_FAIL(prewarm_service->init_consume_hot_micro_key_task(ls, is_consume_hot_micro_key_task_stop_))) {
    LOG_WARN("fail to init consume hot micro keys task", KR(ret), KP(ls));
  } else if (FALSE_IT(ls_id_ = ls->get_ls_id().id())) {
  } else if (OB_FAIL(prewarm_service->schedule_consume_hot_micro_key_task(ls_id_))) {
    LOG_WARN("fail to schedule consume hot micro key task", K(ret));
  } else {
    ls_ = ls;
    is_inited_ = true;
    LOG_INFO("succ to init ls prewarm handler", K_(ls_id));
  }
  return ret;
}

void ObSSLSPreWarmHandler::stop()
{
  int ret = OB_SUCCESS;
  is_ls_stop_ = true;
  is_sync_hot_micro_key_task_stop_ = true;
  is_consume_hot_micro_key_task_stop_ = true;
  ObSSMicroCachePrewarmService *prewarm_service = nullptr;
  if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache prewarm service is null", KR(ret));
  } else if (OB_FAIL(prewarm_service->stop_sync_hot_micro_key_task(ls_id_))) {
    LOG_WARN("fail to stop sync hot micro key task", KR(ret));
  } else if (OB_FAIL(prewarm_service->stop_consume_hot_micro_key_task(ls_id_))) {
    LOG_WARN("fail to stop consume hot micro key task", KR(ret));
  }
}

void ObSSLSPreWarmHandler::wait()
{
  int ret = OB_SUCCESS;
  ObSSMicroCachePrewarmService *prewarm_service = nullptr;
  if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache prewarm service is null", KR(ret));
  } else if (OB_FAIL(prewarm_service->wait_sync_hot_micro_key_task(ls_id_))) {
    LOG_WARN("fail to wait sync hot micro key task", KR(ret));
  } else if (OB_FAIL(prewarm_service->wait_consume_hot_micro_key_task(ls_id_))) {
    LOG_WARN("fail to wait consume hot micro key task", KR(ret));
  }
}

void ObSSLSPreWarmHandler::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    is_sync_hot_micro_key_task_stop_ = true;
    is_consume_hot_micro_key_task_stop_ = true;
    ObSSMicroCachePrewarmService *prewarm_service = nullptr;
    if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro cache prewarm service is null", KR(ret));
    } else if (OB_FAIL(prewarm_service->destroy_sync_hot_micro_key_task(ls_id_))) {
      LOG_WARN("fail to destroy sync hot micro key task", KR(ret));
    } else if (OB_FAIL(prewarm_service->destroy_consume_hot_micro_key_task(ls_id_))) {
      LOG_WARN("fail to destroy consume hot micro key task", KR(ret));
    }
    ls_id_ = ObLSID::INVALID_LS_ID;
    ls_ = nullptr;
    is_leader_ = false;
    is_ls_stop_ = false;
    is_inited_ = false;
  }
}

void ObSSLSPreWarmHandler::switch_to_follower_forcedly()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(switch_role_to(is_leader))) {
    LOG_WARN("fail to switch role", KR(ret), K(is_leader), K_(is_ls_stop));
  }
  LOG_INFO("ls prewarm handler switch to follower finish", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
}

int ObSSLSPreWarmHandler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  bool is_leader = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(switch_role_to(is_leader))) {
    LOG_WARN("fail to switch role", KR(ret), K(is_leader), K_(is_ls_stop));
  }
  LOG_INFO("ls prewarm handler switch to leader finish", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
  return ret;
}

int ObSSLSPreWarmHandler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(switch_role_to(is_leader))) {
    LOG_WARN("fail to switch role", KR(ret), K(is_leader), K_(is_ls_stop));
  }
  LOG_INFO("ls prewarm handler switch to follower gracefully", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
  return ret;
}

int ObSSLSPreWarmHandler::resume_leader()
{
  int ret = OB_SUCCESS;
  bool is_leader = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(switch_role_to(is_leader))) {
    LOG_WARN("fail to switch role", KR(ret), K(is_leader), K_(is_ls_stop));
  }
  LOG_INFO("ls prewarm handler resume leader finish", KR(ret), K_(ls_id), K_(is_leader), K_(is_ls_stop));
  return ret;
}

int ObSSLSPreWarmHandler::replay(
    const void *buffer,
    const int64_t nbytes,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  UNUSED(buffer);
  UNUSED(nbytes);
  UNUSED(lsn);
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObSSLSPreWarmHandler::flush(share::SCN &scn)
{
  UNUSED(scn);
  return OB_SUCCESS;
}

int ObSSLSPreWarmHandler::offline()
{
  int ret = OB_SUCCESS;
  is_sync_hot_micro_key_task_stop_ = true;
  is_consume_hot_micro_key_task_stop_ = true;
  LOG_INFO("ls prewarm handler offline", KR(ret), K_(ls_id), K_(is_leader));
  return ret;
}

int ObSSLSPreWarmHandler::online()
{
  int ret = OB_SUCCESS;
  is_sync_hot_micro_key_task_stop_ = true;
  is_consume_hot_micro_key_task_stop_ = false;
  LOG_INFO("ls prewarm handler online", KR(ret), K_(ls_id), K_(is_leader));
  return ret;
}

int ObSSLSPreWarmHandler::push_micro_cache_keys(const obrpc::ObLSSyncHotMicroKeyArg &arg)
{
  int ret = OB_SUCCESS;
  ObSSMicroCachePrewarmService *prewarm_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (is_consume_hot_micro_key_task_stop_) {
    // do nothing
  } else if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache prewarm service is null", KR(ret));
  } else if (OB_FAIL(prewarm_service->push_micro_cache_keys_to_consume_task(ls_id_, arg))) {
    LOG_WARN("fail to push micro cache keys to consume task", KR(ret), K(arg));
  }
  return ret;
}

int ObSSLSPreWarmHandler::switch_role_to(const bool is_leader)
{
  int ret = OB_SUCCESS;
  ObSSMicroCachePrewarmService *prewarm_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (FALSE_IT(is_leader_ = is_leader)) {
  } else if (OB_UNLIKELY(is_ls_stop_)) {
    LOG_INFO("ls is already stopped", K_(ls_id), K_(is_leader));
  } else if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache prewarm service is null", KR(ret));
  } else {
    LOG_INFO("ls prewarm handler start to switch role", K_(ls_id), K_(is_leader),
             K_(is_sync_hot_micro_key_task_stop), K_(is_consume_hot_micro_key_task_stop), K_(is_ls_stop));
    // when switch role to leader, stop consume_hot_micro_key_task, shedule sync_hot_micro_key_task
    if (is_leader) {
      is_sync_hot_micro_key_task_stop_ = false;
      is_consume_hot_micro_key_task_stop_ = true;
      if (OB_FAIL(prewarm_service->schedule_sync_hot_micro_key_task(ls_id_))) {
        LOG_WARN("fail to schedule sync hot micro key task", KR(ret));
      } else if (OB_FAIL(prewarm_service->stop_consume_hot_micro_key_task(ls_id_))) {
        LOG_WARN("fail to stop consume hot micro key task", KR(ret));
      }
    // when switch role to follower, stop sync_hot_micro_key_task, shedule consume_hot_micro_key_task
    } else {
      is_sync_hot_micro_key_task_stop_ = true;
      is_consume_hot_micro_key_task_stop_ = false;
      if (OB_FAIL(prewarm_service->schedule_consume_hot_micro_key_task(ls_id_))) {
        LOG_WARN("fail to schedule consume hot micro key task", KR(ret));
      } else if (OB_FAIL(prewarm_service->stop_sync_hot_micro_key_task(ls_id_))) {
        LOG_WARN("fail to stop sync hot micro key task", KR(ret));
      }
    }
    LOG_INFO("ls prewarm handler finish to switch role", KR(ret), K_(ls_id), K_(is_leader),
             K_(is_sync_hot_micro_key_task_stop), K_(is_consume_hot_micro_key_task_stop), K_(is_ls_stop));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
