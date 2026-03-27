/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include <stdio.h>
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "lib/ob_running_mode.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/serialization.h"
#include "lib/net/ob_net_util.h"
#include "share/config/ob_server_config.h"
#include "grpc/ob_grpc_context.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
//#include "grpc/newlogstorepb.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using grpc::StatusCode;

namespace oceanbase {
namespace obgrpc {

grpc::Status ob_error_to_grpc_status(int ob_ret)
{
  if (OB_SUCCESS == ob_ret) {
    return grpc::Status::OK;
  }
  char buf[64];
  snprintf(buf, sizeof(buf), "OB_ERROR:%d", ob_ret);
  return grpc::Status(grpc::StatusCode::INTERNAL, buf);
}

int extract_error_from_grpc_status(const grpc::Status& status, bool* is_ob_error)
{
  if (status.ok()) {
    if (is_ob_error) {
      *is_ob_error = false;
    }
    return OB_SUCCESS;
  }
  const std::string& msg = status.error_message();
  if (msg.find("OB_ERROR:") == 0) {
    if (is_ob_error) {
      *is_ob_error = true;
    }
    int ob_ret = OB_RPC_SEND_ERROR;
    size_t pos = strlen("OB_ERROR:");
    if (pos < msg.length()) {
      char* endptr = nullptr;
      long parsed = strtol(msg.c_str() + pos, &endptr, 10);
      if (endptr != msg.c_str() + pos) {
        ob_ret = static_cast<int>(parsed);
      }
    }
    return ob_ret;
  }

  if (is_ob_error) {
    *is_ob_error = false;
  }
  StatusCode err_code = status.error_code();
  switch (err_code) {
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      return OB_TIMEOUT;
    case grpc::StatusCode::UNIMPLEMENTED:
      return OB_NOT_SUPPORTED;
    case grpc::StatusCode::CANCELLED:
      return OB_CANCELED;
    case grpc::StatusCode::INVALID_ARGUMENT:
      return OB_INVALID_ARGUMENT;
    case grpc::StatusCode::NOT_FOUND:
      return OB_ENTRY_NOT_EXIST;
    default:
      return OB_RPC_SEND_ERROR;
  }
}

ObGrpcContext::ObGrpcContext() : dst_(), timeout_(MAX_RPC_TIMEOUT)
{}

int ObGrpcContext::init(const ObAddr &addr, int64_t timeout) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid())) {
    RPC_LOG(WARN, "invalid addr", K(addr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    dst_ = addr;
    timeout_ = timeout;
  }
  return ret;
}

void ObGrpcContext::set_grpc_context(ClientContext &context)
{
  set_grpc_context_(context, timeout_);
}

void ObGrpcContext::set_grpc_context(ClientContext &context, const int64_t timeout)
{
  set_grpc_context_(context, timeout);
}

int ObGrpcContext::translate_error(const Status &status)
{
  int ret = OB_SUCCESS;
  StatusCode err_code = status.error_code();
  int64_t send_cnt = ATOMIC_AAF(&statistics_info.send_cnt_, 1);
  if (OB_UNLIKELY(StatusCode::OK != err_code)) {
    bool is_ob_error = false;
    ret = extract_error_from_grpc_status(status, &is_ob_error);

    ATOMIC_INC(&statistics_info.failed_cnt_);
    RPC_LOG(WARN, "grpc call failed", K(err_code), K(ret),
        K(is_ob_error), "error_msg", status.error_message().c_str(), K(dst_), K(timeout_));
  }
  if (OB_UNLIKELY(send_cnt % REPORT_COUNT_INTERVAL == 0)) {
    int64_t failed_cnt = ATOMIC_LOAD(&statistics_info.failed_cnt_);
    RPC_LOG(INFO, "[grpc report]", KP(this), K(dst_), K(send_cnt), K(failed_cnt));
  }
  return ret;
}

void ObGrpcContext::set_grpc_context_(ClientContext &context, const int64_t timeout)
{
  char str_buf[512] = {0};
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
  const char* trace_id_str = NULL;
  const uint64_t* trace_id = common::ObCurTraceId::get();
  if (0 == trace_id[0]) {
    common::ObCurTraceId::TraceId temp;
    temp.init(obrpc::ObRpcProxy::myaddr_);
    temp.to_string(trace_id_buf, sizeof(trace_id_buf));
    trace_id_str = trace_id_buf;
  } else {
    trace_id_str = common::ObCurTraceId::get_trace_id_str(trace_id_buf, sizeof(trace_id_buf));
  }
  snprintf(str_buf, sizeof(str_buf), "%X,%s", VERSION, trace_id_str);
  context.AddMetadata("custom-header", str_buf);
  int64_t abs_timeout_us = oceanbase::ObTimeUtility::current_time() + timeout;
  std::chrono::microseconds ts(abs_timeout_us);
  std::chrono::time_point<std::chrono::system_clock> tp(ts);
  context.set_deadline(tp);
}

} // end of namespace obgrpc
} // end of namespace oceanbase
