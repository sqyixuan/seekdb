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


#include "ob_shared_log_service.h"
#include "logservice/ob_log_service.h"


namespace oceanbase
{
using namespace palf;
using namespace common;
namespace logservice
{
ObSharedLogUploadHandler *ObLogSSHandlerFactory::alloc()
{
  return MTL_NEW(ObSharedLogUploadHandler, "SharedLogHandle");
}

void ObLogSSHandlerFactory::free(ObSharedLogUploadHandler *shared_log_handler) {
  MTL_DELETE(ObSharedLogUploadHandler, "SharedLogHandle", shared_log_handler);
}

ObSharedLogUploadHandler *ObLogSSHandlerAlloc::alloc_value()
{
  return NULL;
}

void ObLogSSHandlerAlloc::free_value(ObSharedLogUploadHandler *shared_log_handler)
{
  ObLogSSHandlerFactory::free(shared_log_handler);
  shared_log_handler = NULL;
}

ObLogSSHandlerAlloc::Node *ObLogSSHandlerAlloc::alloc_node(ObSharedLogUploadHandler *shared_log_handler)
{
  UNUSED(shared_log_handler);
  return op_reclaim_alloc(Node);
}

void ObLogSSHandlerAlloc::free_node(ObLogSSHandlerAlloc::Node *node)
{
  op_reclaim_free(node);
  node = NULL;
}

ObSharedLogService::ObSharedLogService()
: is_inited_(false),
    log_service_(NULL),
    log_ss_handle_map_(64),//min_size = 64
    ext_handler_(),
    shared_log_gc_(),
    file_upload_mgr_(),
    log_fast_rebuild_engine_()
{
}

ObSharedLogService::~ObSharedLogService()
{
  destroy();
}

int ObSharedLogService::init(const uint64_t tenant_id,
                             const ObAddr &addr,
                             ObLogService *log_service,
                             common::ObMySQLProxy *sql_proxy,
                             obrpc::ObLogServiceRpcProxy *rpc_proxy,
                             ObLocationAdapter *location_adapter,
                             ObILogAllocator *alloc_mgr,
                             palf::LogSharedQueueTh *log_shared_queue_th)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "init twice", K(tenant_id), K(addr));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!addr.is_valid()) 
            || OB_ISNULL(log_service) || OB_ISNULL(sql_proxy) || OB_ISNULL(rpc_proxy)
            || OB_ISNULL(location_adapter) || OB_ISNULL(alloc_mgr) || OB_ISNULL(log_shared_queue_th)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(addr), KP(log_service), KP(sql_proxy),
        KP(rpc_proxy), KP(location_adapter), KP(alloc_mgr), KP(log_shared_queue_th));
  } else if (OB_FAIL(log_ss_handle_map_.init("LOG_SS_MAP", tenant_id))) {
    CLOG_LOG(WARN, "failed to init log_ss_handle_map_", K(tenant_id));
  } else if (OB_FAIL(ext_handler_.init())) {
    CLOG_LOG(WARN, "failed to init shared_storage_handler", K(tenant_id));
  } else if (OB_FAIL(shared_log_gc_.init(addr, is_sys_tenant(tenant_id) /*handle_tenant_drop*/,
                                         sql_proxy, rpc_proxy, location_adapter))) {
  } else if (OB_FAIL(file_upload_mgr_.init(addr, log_service, this, &ext_handler_))) {
    CLOG_LOG(WARN, "failed to init file_upload_mgr_", K(tenant_id));
  } else if (OB_FAIL(log_fast_rebuild_engine_.init(alloc_mgr, log_shared_queue_th))) {
    CLOG_LOG(WARN, "failed to init log_fast_rebuild_engine_", K(tenant_id));
  } else {
    log_service_ = log_service;
    is_inited_ = true;
    CLOG_LOG(INFO, "success to init ObSharedLogService", K(tenant_id));
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    log_service_ = NULL;
    (void)log_ss_handle_map_.destroy();
    (void)ext_handler_.destroy();
    (void)shared_log_gc_.destroy();
    (void)file_upload_mgr_.destroy();
    (void)log_fast_rebuild_engine_.destroy();
  }
  return ret;
}

void ObSharedLogService::destroy()
{
  CLOG_LOG_RET(INFO, OB_SUCCESS, "ObSharedLogService destroy");
  is_inited_ = false;
  log_service_ = NULL;
  log_ss_handle_map_.destroy();
  ext_handler_.destroy();
  shared_log_gc_.destroy();
  file_upload_mgr_.destroy();
  log_fast_rebuild_engine_.destroy();
}
int ObSharedLogService::start()
{
  int ret = OB_SUCCESS;
  constexpr int64_t concurrency = 64;
  if (IS_NOT_INIT){
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSharedLogService is not inited");
  } else if (OB_FAIL(ext_handler_.start(concurrency))) {
    CLOG_LOG(WARN, "failed to start ext_handler_");
  } else if (OB_FAIL(shared_log_gc_.start())) {
    CLOG_LOG(WARN, "failed to start shared_log_gc_");
  } else if (OB_FAIL(file_upload_mgr_.start())) {
    CLOG_LOG(WARN, "failed to start file_upload_mgr_");
  } else {
    CLOG_LOG(INFO, "ObSharedLogService started");
  }
  return ret;
}

void ObSharedLogService::stop()
{
  CLOG_LOG(INFO, "begin to stop ObSharedLogService");
  (void)shared_log_gc_.stop();
  (void)file_upload_mgr_.stop();
  CLOG_LOG(INFO, "ObSharedLogService is stopped");
}

void ObSharedLogService::wait()
{
  (void)shared_log_gc_.wait();
  (void)file_upload_mgr_.wait();
  (void)ext_handler_.stop();
  (void)ext_handler_.wait();
  CLOG_LOG(INFO, "ObSharedLogService finished waiting");
}

int ObSharedLogService::add_ls(const share::ObLSID &id)

{
  int ret = OB_SUCCESS;
  ObSharedLogUploadHandler *handler = NULL;
  LSKey hash_map_key(id.id());
  if (IS_NOT_INIT){
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSharedLogService is not inited");
  } else if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id));
  } else if (OB_ISNULL(handler = ObLogSSHandlerFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to alloc ObSharedLogUploadHandler", K(id));
  } else if (OB_FAIL(handler->init(id))) {
    CLOG_LOG(WARN, "failed to init ObSharedLogUploadHandler");
  } else if (OB_FAIL(log_ss_handle_map_.insert_and_get(hash_map_key, handler))) {
    CLOG_LOG(WARN, "log_ss_handle_map_ insert_and_get failed", K(id));
  } else {
    log_ss_handle_map_.revert(handler);
    CLOG_LOG(INFO, "add_ls finished", K(id));
  }
  if (OB_FAIL(ret) && NULL != handler) {
    ObLogSSHandlerFactory::free(handler);
    handler = NULL;
  }

  return ret;
}

int ObSharedLogService::remove_ls(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  ObSharedLogUploadHandler *handler = NULL;
  LSKey hash_map_key(id.id());
  if (IS_NOT_INIT){
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSharedLogService is not inited");
  } else if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id));
  } else if (OB_FAIL(log_ss_handle_map_.del(hash_map_key))) {
    CLOG_LOG(WARN, "log_ss_handle_map_ del failed", K(id));
  } else {
    CLOG_LOG(INFO, "remove_ls finished", K(id));
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

ObSharedLogGarbageCollector *ObSharedLogService::get_shared_log_gc()
{
  ObSharedLogGarbageCollector *log_gc = NULL;
  if (IS_INIT) {
    log_gc = &shared_log_gc_;
  }
  return log_gc;
}

ObLogExternalStorageHandler *ObSharedLogService::get_log_ext_handler()
{
  ObLogExternalStorageHandler *ext_handler = NULL;
  //TODO(yaoying.yyy):uncommit this after runlin's commit
  //if (IS_INIT) {
    ext_handler = &ext_handler_;
  //}
  return ext_handler;
}

ObLogFastRebuildEngine *ObSharedLogService::get_log_fast_rebuild_engine()
{
  // Note: do not need to check if the ObSharedLogService has been inited
  ObLogFastRebuildEngine *rebuild_engine = &log_fast_rebuild_engine_;
  return rebuild_engine;
}

int ObSharedLogService::get_log_ss_handler(const ObLSID &id, ObSharedLogUploadHandler *&shared_log_handler)
{
  int ret = OB_SUCCESS;
  LSKey hash_map_key(id.id());
  ObSharedLogUploadHandler *handler = NULL;
  if (IS_NOT_INIT){
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObSharedLogService is not inited");
  } else if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(id));
  } else if (OB_FAIL(log_ss_handle_map_.get(hash_map_key, handler))) {
    PALF_LOG(TRACE, "get from map failed", K(id));
  } else {
    shared_log_handler = handler;
  }

  if (OB_FAIL(ret) && NULL != handler) {
    revert_log_ss_handler(handler);
  }
  return ret;
}

void ObSharedLogService::revert_log_ss_handler(ObSharedLogUploadHandler *handler)
{
  if (NULL != handler) {
    log_ss_handle_map_.revert(handler);
    handler = NULL;
  }
}

} // namespace logservice
} // namespace oceanbase
