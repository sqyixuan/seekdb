/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this software except in compliance with the License.
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

#include "storage/ob_storage_grpc.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/time/ob_time_utility.h"
#include "storage/high_availability/ob_storage_ha_reader.h"
#include <string>

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::obgrpc;
using namespace storageservice;

namespace oceanbase
{
namespace storage
{

// ==================== ObStorageGrpcServiceImpl ====================

grpc::Status ObStorageGrpcServiceImpl::copy_ls_info(
    grpc::ServerContext* context,
    const CopyLSInfoReq* request,
    CopyLSInfoRes* response)
{
  int ret = OB_SUCCESS;

  ObCopyLSInfoArg arg;
  ObCopyLSInfo result;

  if (OB_FAIL(deserialize_proto_to_ob(*request, arg))) {
    LOG_WARN("failed to deserialize ObCopyLSInfoArg", K(ret));
  }

  MTL_SWITCH(OB_SYS_TENANT_ID) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    logservice::ObLogHandler *log_handler = nullptr;
    ObRole role;
    int64_t proposal_id = 0;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;
    ObMigrationStatus migration_status;
    ObLSMetaPackage ls_meta_package;
    bool is_need_rebuild = false;
    bool is_log_sync = false;
    const bool check_archive = true;
    const bool need_sorted_tablet_id = false;

    LOG_INFO("start to fetch log stream info", K(arg.ls_id_), K(arg));

    if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot be migrate src", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("faield to get log stream", K(ret), K(arg));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret), KP(log_handler), K(arg));
    } else if (OB_FAIL(ls->get_ls_meta_package_and_tablet_metas(check_archive,
                                                              [&](const ObLSMetaPackage &meta_package)
                                                              { result.ls_meta_package_ = meta_package;
                                                                return OB_SUCCESS;
                                                              },
                                                              need_sorted_tablet_id,
                                                              [&]
                                                              (const obrpc::ObCopyTabletInfo &tablet_info, const ObTabletHandle &tablet_handle)
                                                              {
                                                                result.tablet_id_array_.push_back(tablet_info.tablet_id_);
                                                                return OB_SUCCESS;
                                                              }
                         ))) {
      LOG_WARN("failed to get ls meta package and tablet ids", K(ret));
    } else if (OB_FAIL(result.ls_meta_package_.ls_meta_.get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), K(result));
    } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status) || ls->is_stopped()
        || ls->is_offline()) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      STORAGE_LOG(WARN, "src migration status do not allow to migrate out", K(ret), "src migration status",
          migration_status, KPC(ls));
    } else if (OB_FAIL(ObStorageHAUtils::get_server_version(result.version_))) {
      LOG_WARN("failed to get server version", K(ret), K(arg));
    } else if (OB_FAIL(log_handler->get_role(role, proposal_id))) {
      LOG_WARN("failed to get role", K(ret), K(arg));
    } else if (is_strong_leader(role)) {
      result.is_log_sync_ = true;
    } else if (OB_FAIL(log_handler->is_in_sync(is_log_sync, is_need_rebuild))) {
      LOG_WARN("failed to check is in sync", K(ret), K(arg));
    } else if (!is_log_sync || is_need_rebuild) {
      result.is_log_sync_ = false;
    } else {
      result.is_log_sync_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get ls info in MTL_SWITCH", K(ret), K(arg));
    } else {
      if (OB_FAIL(serialize_ob_to_proto(result, response))) {
        LOG_WARN("failed to serialize ObCopyLSInfo", K(ret));
      } else {
        LOG_INFO("copy_ls_info RPC handled successfully", K(arg.tenant_id_), K(arg.ls_id_), K(result.tablet_id_array_.size()));
      }
    }
  }

  return obgrpc::ob_error_to_grpc_status(ret);
}

grpc::Status ObStorageGrpcServiceImpl::fetch_tablet_info(
    grpc::ServerContext* context,
    const FetchTabletInfoReq* request,
    grpc::ServerWriter<FetchTabletInfoRes>* writer)
{
  int ret = OB_SUCCESS;

  const std::string& req_buf = request->buf();
  const uint64_t req_size = request->size();

  ObCopyTabletInfoArg arg;

  if (OB_FAIL(deserialize_proto_to_ob(*request, arg))) {
    LOG_WARN("failed to deserialize ObCopyTabletInfoArg", K(ret));
  }

  MTL_SWITCH(arg.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    ObCopyTabletInfoObProducer producer;
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(arg));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg));
    } else if (OB_FAIL(producer.init(arg.tenant_id_, arg.ls_id_, arg.tablet_id_list_))) {
      LOG_WARN("failed to init copy tablet info producer", K(ret), K(arg));
    } else {
      ObCopyTabletInfo tablet_info;
      while (OB_SUCC(ret)) {
        tablet_info.reset();

        if (context->IsCancelled()) {
          ret = OB_CANCELED;
          LOG_WARN("client cancelled the request", K(ret));
          break;
        }

        if (OB_FAIL(producer.get_next_tablet_info(tablet_info))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            STORAGE_LOG(WARN, "failed to get next tablet meta info", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          FetchTabletInfoRes response;
          if (OB_FAIL(serialize_ob_to_proto(tablet_info, &response))) {
            LOG_WARN("failed to serialize ObCopyTabletInfo", K(ret));
          } else {
            if (!writer->Write(response)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to write tablet info to stream", K(ret));
            }
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fetch_tablet_info stream failed", K(ret), K(arg));
  }

  return obgrpc::ob_error_to_grpc_status(ret);
}

// ==================== ObStorageGrpcClient ====================

ObStorageGrpcClient::ObStorageGrpcClient()
  : is_inited_(false),
    grpc_client_()
{
}

ObStorageGrpcClient::~ObStorageGrpcClient()
{
}

int ObStorageGrpcClient::init(const common::ObAddr& addr, int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObStorageGrpcClient already inited", K(ret));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(ret), K(addr));
  } else {
    ret = grpc_client_.init(addr, timeout);
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      LOG_INFO("ObStorageGrpcClient init success", K(addr), K(timeout));
    } else {
      LOG_WARN("failed to init grpc client", K(ret), K(addr));
    }
  }

  return ret;
}

int ObStorageGrpcClient::copy_ls_info(const obrpc::ObCopyLSInfoArg& arg, obrpc::ObCopyLSInfo& result)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageGrpcClient not inited", K(ret));
  } else {
    CopyLSInfoReq req;
    CopyLSInfoRes res;
    if (OB_FAIL(serialize_ob_to_proto(arg, &req))) {
      LOG_WARN("failed to serialize ObCopyLSInfoArg", K(ret));
    } else {
      grpc::ClientContext context;
      GRPC_SET_CONTEXT(grpc_client_, context);
      auto status = grpc_client_.stub_->copy_ls_info(&context, req, &res);
      if (OB_FAIL(grpc_client_.translate_error(status))) {
        LOG_WARN("failed to call copy_ls_info RPC", K(ret));
      } else if (OB_FAIL(deserialize_proto_to_ob(res, result))) {
        LOG_WARN("failed to deserialize ObCopyLSInfo, may be connected to wrong server", K(ret));
      }
    }
  }

  return ret;
}

int ObStorageGrpcClient::fetch_tablet_info(
    const obrpc::ObCopyTabletInfoArg& arg,
    std::function<int(const obrpc::ObCopyTabletInfo&)> callback)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageGrpcClient not inited", K(ret));
  } else {
    FetchTabletInfoReq req;
    if (OB_FAIL(serialize_ob_to_proto(arg, &req))) {
      LOG_WARN("failed to serialize ObCopyTabletInfoArg", K(ret));
    } else {
      grpc::ClientContext context;
      GRPC_SET_CONTEXT(grpc_client_, context);
      auto reader = grpc_client_.stub_->fetch_tablet_info(&context, req);

      if (OB_ISNULL(reader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create stream reader", K(ret));
      } else {
        FetchTabletInfoRes response;
        while (reader->Read(&response)) {
          const std::string& res_buf = response.buf();
          const uint64_t res_size = response.size();

          if (res_size > 0) {
            ObCopyTabletInfo tablet_info;
            if (OB_FAIL(deserialize_proto_to_ob(response, tablet_info))) {
              LOG_WARN("failed to deserialize ObCopyTabletInfo", K(ret), K(res_size));
              break;
            } else {
              if (OB_FAIL(callback(tablet_info))) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  LOG_INFO("callback requested to stop");
                } else {
                  LOG_WARN("callback failed", K(ret));
                }
                break;
              }
            }
          }
        }

        grpc::Status status = reader->Finish();
        int grpc_ret = grpc_client_.translate_error(status);

        if (OB_FAIL(grpc_ret)) {
          ret = grpc_ret;
          LOG_WARN("fetch_tablet_info stream failed", K(ret));
        } else {
          LOG_INFO("fetch_tablet_info stream completed", K(arg));
        }
      }
    }
  }

  return ret;
}

} // storage
} // oceanbase
