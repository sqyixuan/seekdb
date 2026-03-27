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

ObGrpcContext::ObGrpcContext() : dst_(), timeout_(MAX_RPC_TIMEOUT),
        tenant_id_(common::OB_SERVER_TENANT_ID), src_cluster_id_(common::OB_INVALID_CLUSTER_ID) {
}

int ObGrpcContext::init(const ObAddr &addr, int64_t timeout, const int64_t src_cluster_id, const uint64_t tenant_id) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid())) {
    RPC_LOG(WARN, "invalid addr", K(addr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    dst_ = addr;
    timeout_ = timeout;
    tenant_id_ = tenant_id;
    src_cluster_id_ = src_cluster_id;
  }
  return ret;
}

int ObGrpcContext::set_grpc_context(ClientContext &context, const int64_t start_ts)
{
  return set_grpc_context_(context, start_ts, timeout_);
}

int ObGrpcContext::set_grpc_context(ClientContext &context, const int64_t start_ts, const int64_t timeout)
{
  return set_grpc_context_(context, start_ts, timeout);
}

int ObGrpcContext::translate_error(const Status &status, const int64_t start_ts, const char *func_name)
{
  int ret = OB_SUCCESS;
  StatusCode err_code = status.error_code();
  int64_t send_cnt = ATOMIC_AAF(&statistics_info.send_cnt_, 1);
  if (OB_UNLIKELY(StatusCode::OK != err_code)) {
    if (StatusCode::DEADLINE_EXCEEDED == err_code) {
      ret = OB_TIMEOUT;
    } else if (StatusCode::NOT_FOUND == err_code) {
      ret = OB_NOT_SUPPORTED;
    } else if (StatusCode::CANCELLED == err_code) {
      ret = OB_CANCELED;
    } else {
      ret = OB_RPC_SEND_ERROR;
    }
    ATOMIC_INC(&statistics_info.failed_cnt_);
    char msg[64];
    std::snprintf(msg, sizeof(msg), "%s", status.error_message().c_str());
    RPC_LOG(WARN, "grpc call faild", K(func_name), K(err_code), K(msg), K(dst_), K(timeout_));
  }
  int64_t end_ts = oceanbase::ObTimeUtility::current_time();
  int64_t wait_time = ATOMIC_FAA(&statistics_info.wait_time_, end_ts - start_ts);
  if (OB_UNLIKELY(send_cnt % REPORT_COUNT_INTERVAL == 0)) {
    int64_t failed_cnt = ATOMIC_LOAD(&statistics_info.failed_cnt_);
    int64_t avg_cost = wait_time / REPORT_COUNT_INTERVAL;
    RPC_LOG(INFO, "[grpc report]", KP(this), K(dst_), K(send_cnt), K(failed_cnt), K(wait_time), K(avg_cost));
    ATOMIC_FAA(&statistics_info.wait_time_, -wait_time);
  }
  return ret;
}

int ObGrpcContext::set_grpc_context_(ClientContext &context, const int64_t start_ts, const int64_t timeout)
{
  int ret = OB_SUCCESS;
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
  snprintf(str_buf, sizeof(str_buf), "%X,%s,%lX,%lX,%lX", VERSION, trace_id_str,
            tenant_id_, src_cluster_id_, start_ts);
  context.AddMetadata("custom-header", str_buf);
  int64_t abs_timeout_us = start_ts + timeout;
  std::chrono::microseconds ts(abs_timeout_us);
  std::chrono::time_point<std::chrono::system_clock> tp(ts);
  context.set_deadline(tp);
  return ret;
}

} // end of namespace obgrpc
} // end of namespace oceanbase
