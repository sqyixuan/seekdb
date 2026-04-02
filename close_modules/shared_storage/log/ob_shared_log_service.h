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

#ifndef OCEANBSE_LOG_SHARED_STORAGE_SERVICE_
#define OCEANBSE_LOG_SHARED_STORAGE_SERVICE_

#include "lib/hash/ob_link_hashmap.h"

#include "logservice/ob_log_external_storage_handler.h"
#include "log/ob_shared_log_upload_handler.h"
#include "log/ob_shared_log_garbage_collector.h"    // ObSharedLogGarbageCollector
#include "log/ob_file_upload_mgr.h"
#include "log/ob_log_fast_rebuild_engine.h"         // ObLogFastRebuildEngine


namespace oceanbase
{
namespace logservice
{
class ObLogService;
class ObLocationAdapter;

class ObLogSSHandlerFactory {
public:
  static ObSharedLogUploadHandler *alloc();
  static void free(ObSharedLogUploadHandler *shared_log_handler);
};
class ObLogSSHandlerAlloc
{
public:
  typedef common::LinkHashNode<palf::LSKey> Node;

  static ObSharedLogUploadHandler *alloc_value();

  static void free_value(ObSharedLogUploadHandler *handler);

  static Node *alloc_node(ObSharedLogUploadHandler *handler);

  static void free_node(Node *node);
};

typedef common::ObLinkHashMap<palf::LSKey, ObSharedLogUploadHandler, ObLogSSHandlerAlloc> LogSSHandlerMap;
class ObSharedLogService
{
public:
  ObSharedLogService();
  ~ObSharedLogService();
public:
  int init(const uint64_t tenant_id,
           const ObAddr &addr,
           ObLogService *log_service,
           common::ObMySQLProxy *sql_proxy,
           obrpc::ObLogServiceRpcProxy *rpc_proxy,
           ObLocationAdapter *location_adapter,
           ObILogAllocator *alloc_mgr,
           palf::LogSharedQueueTh *log_shared_queue_th);
  void destroy();
  int start();
  void stop();
  void wait();

  int add_ls(const share::ObLSID &id);
  int remove_ls(const share::ObLSID &id);

  ObSharedLogGarbageCollector *get_shared_log_gc();
  ObLogExternalStorageHandler *get_log_ext_handler();
  ObLogFastRebuildEngine *get_log_fast_rebuild_engine();
  template <class Functor>
  int for_each(Functor &func)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT){
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "ObSharedLogService is not inited");
    } else if (OB_FAIL(log_ss_handle_map_.for_each(func))) {
      CLOG_LOG(WARN, "iterate log_ss_hanle_map failed", K(ret));
    }
    return ret;
  }
  int get_log_ss_handler(const share::ObLSID &id, ObSharedLogUploadHandler *&handler);
  void revert_log_ss_handler(ObSharedLogUploadHandler *handler);
private:
  bool is_inited_;
  ObLogService *log_service_;
  LogSSHandlerMap log_ss_handle_map_;
  // TODO by runlin: delete it after use ObIOManager
  ObLogExternalStorageHandler ext_handler_;
  ObSharedLogGarbageCollector shared_log_gc_;
  ObFileUploadMgr file_upload_mgr_;
  ObLogFastRebuildEngine log_fast_rebuild_engine_;
};

}
} // namespace oceanbase
#endif
