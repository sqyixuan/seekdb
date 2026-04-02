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

#include "ob_file_upload_mgr.h"
#include "log/ob_shared_log_utils.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_log_external_storage_utils.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;

namespace logservice
{
ObFileUploadMgr::ObFileUploadMgr()
  : is_inited_(false),
    self_(),
    last_sync_base_lsn_ts_(OB_INVALID_TIMESTAMP),
    last_uploaded_size_(0),
    read_buf_(),
    ext_storage_handler_(NULL),
    log_shared_storage_service_(NULL),
    log_service_(NULL) {}

ObFileUploadMgr::~ObFileUploadMgr()
{
  destroy();
}

int ObFileUploadMgr::init(const ObAddr &addr,
                          ObLogService *log_service,
                          ObSharedLogService *shared_log,
                          ObLogExternalStorageHandler *ext_storage_handler) {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "FileUploadMgr has been inited");
  } else if (OB_UNLIKELY(!addr.is_valid()) || OB_ISNULL(log_service)
             || OB_ISNULL(shared_log) || OB_ISNULL(ext_storage_handler)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(addr), KP(log_service),
             KP(shared_log), KP(ext_storage_handler));
  } else {
    self_ = addr;
    read_buf_.reset();
    ext_storage_handler_ = ext_storage_handler;
    log_service_ = log_service;
    log_shared_storage_service_ = shared_log;
    last_sync_base_lsn_ts_ = OB_INVALID_TIMESTAMP;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObFileUploadMgr init success");
  }
  return ret;
}

void ObFileUploadMgr::destroy()
{
  is_inited_ = false;
  last_sync_base_lsn_ts_ = OB_INVALID_TIMESTAMP;
  last_uploaded_size_ = 0;
  ext_storage_handler_ = NULL;
  log_shared_storage_service_ = NULL;
  log_service_ = NULL;
  try_destroy_read_buf_();
}

int ObFileUploadMgr::start()
{
  int ret = OB_SUCCESS;
  ThreadPool::set_run_wrapper(MTL_CTX());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObFileUploadMgr is not inited");
  } else if (OB_FAIL(ObThreadPool::start())) {
    CLOG_LOG(ERROR, "ObFileUploadMgr thread failed to start");
  } else {
    CLOG_LOG(INFO, "ObFileUploadMgr thread started");
  }

  return ret;
}

void ObFileUploadMgr::run1()
{
  CLOG_LOG(INFO, "FileUploadMgr start to run");
  lib::set_thread_name("LogUpMgr");
  while (!has_set_stop()) {
    ObCurTraceId::TraceId trace_id;
    trace_id.init(MYADDR);
    ObTraceIdGuard trace_id_guard(trace_id);
    const int64_t start_ts = ObClockGenerator::getClock();
    if (IS_NOT_INIT) {
      CLOG_LOG_RET(ERROR, OB_NOT_INIT, "not inited");
    } else {
      (void)upload_();
    }
    const int64_t round_cost_time = ObClockGenerator::getClock() - start_ts;
    const int64_t sleep_ts_us = MAX(0, UPLOAD_INTERVAL - round_cost_time);
    ob_usleep(sleep_ts_us);
    if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      int ret = OB_SUCCESS;
      StatUploadProcessFunctor functor;
      int64_t uploaded_size = 0;
      int64_t unuploaded_size = 0;
      if (OB_FAIL(log_shared_storage_service_->for_each(functor))) {
        CLOG_LOG(WARN, "for_each failed", KR(ret));
      } else {
        constexpr int64_t MB = 1024 * 1024;
        unuploaded_size = functor.get_unuploaded_size();
        uploaded_size = functor.get_uploaded_size();
        CLOG_LOG(INFO, "dump tenant upload process", K(sleep_ts_us), K(last_sync_base_lsn_ts_),
                "last_uploaded_size(MB)", last_uploaded_size_/MB,
                 "uploaded_size(MB)", uploaded_size/MB,
                 "unuploaded_size(MB)", unuploaded_size/MB);
        last_uploaded_size_ = uploaded_size;
      }
    }
  }
}

int ObFileUploadMgr::upload_ls(ObSharedLogUploadHandler *handle, bool &has_file_to_upload)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  has_file_to_upload = false;
  ObLSID ls_id = handle->get_ls_id();
  palf::LogIOContext io_ctx(tenant_id, ls_id.id(), palf::LogIOUser::SHARED_UPLOAD);
  CONSUMER_GROUP_FUNC_GUARD(io_ctx.get_function_type());
  ObRole role = FOLLOWER;
  LogUploadCtx new_log_upload_ctx;
  bool is_leader = false;
  LSN begin_lsn;
  LSN base_lsn;
  LSN end_lsn;
  {
    PalfHandleGuard palf_handle_guard;
    ObAddr leader;
    if (OB_FAIL(log_service_->open_palf(ls_id, palf_handle_guard))) {
      if (OB_LS_NOT_EXIST == ret) {
        CLOG_LOG(INFO, "ls may has been removed", K(ls_id));
        ret = OB_SUCCESS;
      } else {
        CLOG_LOG(WARN, "failed to open_palf", K(ls_id));
      }
    } else if (OB_FAIL(palf_handle_guard.get_election_leader(leader))) {
      CLOG_LOG(WARN, "failed to get_election_leader", K(ls_id));
    } else if (self_ != leader) {
      CLOG_LOG(INFO, "only election leader needs to upload block", K(ls_id), K(leader), K(self_));
    } else if (OB_FAIL(palf_handle_guard.get_begin_lsn(begin_lsn))) {
      CLOG_LOG(WARN, "failed to get_begin_lsn", K(ls_id));
    } else if (OB_FAIL(palf_handle_guard.get_base_lsn(base_lsn))) {
      CLOG_LOG(WARN, "failed to get_base_lsn", K(ls_id));
    } else if (OB_FAIL(palf_handle_guard.get_end_lsn(end_lsn))) {
      CLOG_LOG(WARN, "failed to get_end_lsn", K(ls_id));
    } else {
      is_leader = true;
    }
  }

  if (OB_SUCC(ret)) {
    block_id_t to_upload_block_id = LOG_INVALID_BLOCK_ID;
    if (!is_leader) {
      //reset update_ctx
      handle->reset_upload_ctx();
      try_destroy_read_buf_();
    } else if (OB_FAIL(locate_upload_range_(begin_lsn, base_lsn, end_lsn, handle, new_log_upload_ctx))) {
      CLOG_LOG(WARN, "failed to locate_upload_range_", K(ls_id));
    } else if (OB_FAIL(new_log_upload_ctx.get_next_block_to_upload(to_upload_block_id))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        CLOG_LOG(INFO, "there is no log file to be uploaded", K(ls_id), K(new_log_upload_ctx));
        ret = OB_SUCCESS;
      } else {
        CLOG_LOG(WARN, "failed to get_next_block_to_upload", K(ls_id), K(new_log_upload_ctx));
      }
    } else if (OB_FAIL(try_alloc_read_buf_())) {
      CLOG_LOG(WARN, "failed to alloc read_buf_", KR(ret), K(ls_id), K(new_log_upload_ctx));
    } else if (OB_FAIL(do_upload_file_(tenant_id, handle->get_ls_id(), to_upload_block_id))) {
      CLOG_LOG(WARN, "failed to upload_file", K(tenant_id), K(ls_id), K(new_log_upload_ctx),
               K(to_upload_block_id));
    } else if (OB_FAIL(handle->after_upload_file(to_upload_block_id))) {
      CLOG_LOG(WARN, "failed to after_upload_file", K(tenant_id), K(ls_id), K(to_upload_block_id));
    } else if (OB_FAIL(update_base_lsn_(ls_id, LSN((to_upload_block_id+1) * PALF_BLOCK_SIZE)))) {
      CLOG_LOG(WARN, "failed to update_base_lsn_", K(tenant_id), K(ls_id), K(to_upload_block_id));
    } else {
      CLOG_LOG(INFO, "success to upload_file", K(ls_id), K(new_log_upload_ctx), K(to_upload_block_id));
      if (new_log_upload_ctx.end_block_id_ > to_upload_block_id + 1) {
        has_file_to_upload = true;
      }
    }
  }

  return ret;
}

int ObFileUploadMgr::upload_()
{
  int ret = OB_SUCCESS;
  bool has_file_to_upload = true;

  while (has_file_to_upload) {
    ObFileUploadFunctor upload_functor(*this);
    if (OB_FAIL(log_shared_storage_service_->for_each(upload_functor))) {
      CLOG_LOG(WARN, "ObLogService for_each failed");
    } else if (!(has_file_to_upload = upload_functor.has_file_to_upload())) {
      CLOG_LOG(INFO, "no need to upload file this round");
    }
  }
  return ret;
}

int ObFileUploadMgr::locate_upload_range_(const LSN &begin_lsn,
                                          const LSN &base_lsn,
                                          const LSN &end_lsn,
                                          ObSharedLogUploadHandler *handle,
                                          LogUploadCtx &new_log_upload_ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  bool need_update_base_lsn = false;
  block_id_t max_block_id_on_ss = LOG_INVALID_BLOCK_ID;
  LogUploadCtx log_upload_ctx;
  ObLSID ls_id = handle->get_ls_id();
  if (OB_FAIL(handle->get_log_upload_ctx(log_upload_ctx))) {
    CLOG_LOG(WARN, "failed to get_log_upload_ctx", K(tenant_id), K(ls_id));
  } else if (!log_upload_ctx.need_locate_upload_range()) {
    //no need to locate upload_range
    new_log_upload_ctx = log_upload_ctx;
  } else {
    block_id_t base_block_id = lsn_2_block(base_lsn, PALF_BLOCK_SIZE);
    block_id_t considable_base_block_id = (LOG_INITIAL_BLOCK_ID == base_block_id ? base_block_id : base_block_id - 1);
    if (OB_FAIL(get_max_block_id_on_ss_(tenant_id, ls_id, log_upload_ctx, considable_base_block_id, max_block_id_on_ss,
                                        need_update_base_lsn))
      && OB_ENTRY_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "failed to get_max_block_id_on_ss", K(tenant_id), K(ls_id), K(base_block_id), K(considable_base_block_id));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      if (begin_lsn != base_lsn) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "log files are missing, may be in status of tenant dropping", KR(ret), K(tenant_id), K(ls_id), K(base_block_id), K(considable_base_block_id));
      } else {
        //GC logical of block on shared storage ensures that at least one file is retained
        //no file exists on shared storage means this log stream has never uploaded files to shared storage before
        new_log_upload_ctx.has_file_on_ss_ = false;
        new_log_upload_ctx.max_block_id_on_ss_ = LOG_INVALID_BLOCK_ID;
        new_log_upload_ctx.start_block_id_ = base_block_id;
        if (OB_FAIL(locate_end_block_id_(ls_id, new_log_upload_ctx.start_block_id_,
                                         new_log_upload_ctx.end_block_id_))) {
          CLOG_LOG(WARN, "failed to locate_end_block_id_", K(tenant_id), K(ls_id), K(new_log_upload_ctx));
        }
      }
    } else {
      LSN new_base_lsn(max_block_id_on_ss * PALF_BLOCK_SIZE);
      block_id_t start_block_id = max_block_id_on_ss + 1;
      if (new_base_lsn > end_lsn) {
        //during reconfirm, end_lsn may be smaller than new_base_lsn
        new_log_upload_ctx.has_file_on_ss_ = true;
        new_log_upload_ctx.max_block_id_on_ss_ = max_block_id_on_ss;
        new_log_upload_ctx.start_block_id_ = start_block_id;
        new_log_upload_ctx.end_block_id_ = start_block_id;
        // need update base_lsn with end_lsn is advancing
        need_update_base_lsn = true;
      } else {
        new_log_upload_ctx.has_file_on_ss_ = true;
        new_log_upload_ctx.max_block_id_on_ss_ = max_block_id_on_ss;
        new_log_upload_ctx.start_block_id_ = max_block_id_on_ss + 1;
        if (OB_FAIL(locate_end_block_id_(ls_id, new_log_upload_ctx.start_block_id_,
                                         new_log_upload_ctx.end_block_id_))) {
          CLOG_LOG(WARN, "failed to locate_end_block_id_", K(tenant_id), K(ls_id), K(log_upload_ctx));
        }
      }
    }

    if (OB_SUCC(ret)) {
      //advance base_lsn
      const block_id_t start_block_id = new_log_upload_ctx.start_block_id_;
      const LSN base_lsn = is_valid_block_id(start_block_id) ? LSN(start_block_id * PALF_BLOCK_SIZE) : LSN(0);
      if (need_update_base_lsn && OB_FAIL(update_base_lsn_(ls_id, base_lsn))) {
        CLOG_LOG(WARN, "failed to update_base_lsn", K(ls_id), K(base_lsn));
      } else if (OB_FAIL(handle->update_log_upload_ctx(new_log_upload_ctx))) {
        CLOG_LOG(WARN, "failed to update_log_upload_ctx_", K(tenant_id), K(ls_id), K(new_log_upload_ctx));
      } else {
        CLOG_LOG(TRACE, "after update_log_upload_ctx_", K(tenant_id), K(ls_id), K(new_log_upload_ctx));
      }
    }
  }
  return ret;
}

int ObFileUploadMgr::get_max_block_id_on_ss_(const uint64_t tenant_id,
                                             const ObLSID &ls_id,
                                             const LogUploadCtx &log_upload_ctx,
                                             const block_id_t &base_block_id,
                                             block_id_t &max_block_id_on_ss,
                                             bool &need_update_base_lsn)
{
  int ret = OB_SUCCESS;
  if (!(log_upload_ctx.is_valid())) {
    if (OB_FAIL(ObSharedLogUtils::get_newest_block(tenant_id, ls_id, base_block_id, max_block_id_on_ss))) {
      CLOG_LOG(WARN, "failed to get_newest_block", K(tenant_id), K(ls_id), K(base_block_id));
    } else {
      CLOG_LOG(INFO, "success to get_newest_block", K(tenant_id), K(ls_id), K(base_block_id), K(max_block_id_on_ss));
      need_update_base_lsn = true;
    }
  } else if (log_upload_ctx.has_file_on_ss_) {
    //log files have been successfully uploaded to the shared storage before
    max_block_id_on_ss = log_upload_ctx.max_block_id_on_ss_;
  } else {
    //files have never been successfully uploaded to the shared storage before
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObFileUploadMgr::locate_end_block_id_(const ObLSID &ls_id,
                                          const block_id_t start_block_id,
                                          block_id_t &end_block_id)
{
  int ret = OB_SUCCESS;
  PalfHandleGuard palf_handle_guard;
  if (OB_FAIL(log_service_->open_palf(ls_id, palf_handle_guard))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      end_block_id = start_block_id;
      CLOG_LOG(INFO, "ls may has been removed", K(ls_id));
    } else {
      CLOG_LOG(WARN, "failed to get_role", K(ls_id));
    }
  } else {
    LSN start_lsn;
    AccessMode access_mode;
    LSN end_lsn;
    LSN max_lsn;
    block_id_t block_id = LOG_INVALID_BLOCK_ID;
    if (OB_FAIL(palf_handle_guard.get_readable_end_lsn(end_lsn))) {
      CLOG_LOG(WARN, "failed to get_end_lsn", K(palf_handle_guard));
    } else if (OB_FAIL(palf_handle_guard.get_access_mode(access_mode))) {
      CLOG_LOG(WARN, "failed to get_access_mode", K(palf_handle_guard));
    } else if (AccessMode::APPEND == access_mode) {
      // primary tenant
      block_id_t block_id = lsn_2_block(end_lsn, PALF_BLOCK_SIZE);
      end_block_id = MAX(block_id, start_block_id);
    } else {
      // standby tenant
      SCN replayable_point;
      LSN replayable_lsn;
      if (OB_FAIL(log_service_->get_replayable_point(replayable_point))) {
        CLOG_LOG(WARN, "failed to get_replayable_point", K(palf_handle_guard));
      } else if (OB_FAIL(palf_handle_guard.locate_by_scn_coarsely(replayable_point, replayable_lsn))) {
        CLOG_LOG(WARN, "failed to locate_by_scn_coarsely", K(replayable_point), K(palf_handle_guard));
        // in scene with restart, replayable_point may fallback, just ignore and
        // set end_block_id with value of start_block_id
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          ret = OB_SUCCESS;
          end_block_id = start_block_id;
        }
      } else {
        const LSN end_block_lsn = MIN(end_lsn, replayable_lsn);
        const block_id_t block_id = lsn_2_block(end_block_lsn, PALF_BLOCK_SIZE);
        end_block_id = MAX(block_id, start_block_id);
      }
    }
  }
  return ret;
}

int ObFileUploadMgr::update_base_lsn_(const share::ObLSID &ls_id,
                                      const LSN &base_lsn)
{
  // TODO by runlin: nowdays, update_base_lsn_ after upload each file to object storage successfully.
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObMemberList member_list;
  common::GlobalLearnerList learner_list;
  int64_t paxos_replica_num = 0;
  PalfHandleGuard palf_handle_guard;
  if (OB_FAIL(log_service_->open_palf(ls_id, palf_handle_guard))) {
    if (OB_LS_NOT_EXIST == ret) {
      CLOG_LOG(INFO, "ls may has been removed", K(ls_id));
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(WARN, "failed to open_palf", K(ls_id));
    }
  } else if (OB_FAIL(palf_handle_guard.advance_base_lsn(base_lsn))) {
    CLOG_LOG(WARN, "failed to update_base_lsn", K(ls_id));
  } else if (OB_FAIL(palf_handle_guard.get_paxos_member_list_and_learner_list(member_list,
                                                                              paxos_replica_num,
                                                                              learner_list))) {
    CLOG_LOG(WARN, "failed to get_paxos_member_list_and_learner_list", K(ls_id), K(base_lsn));
  } else if (OB_FAIL(member_list.remove_server(self_))) {
    CLOG_LOG(WARN, "failed to remove_server", K(ls_id), K(self_), K(base_lsn));
  } else if (OB_SUCCESS != (tmp_ret = (sync_base_lsn_(ls_id, base_lsn, member_list)))) {
    CLOG_LOG(WARN, "failed to sync_base_lsn with member_list", K(ls_id), K(base_lsn));
  } else if (OB_SUCCESS != (tmp_ret = (sync_base_lsn_(ls_id, base_lsn, learner_list)))) {
    CLOG_LOG(WARN, "failed to sync_base_lsn with learner_list", K(ls_id), K(base_lsn));
  }
  return ret;
}

bool ObFileUploadMgr::ObFileUploadFunctor::operator()(palf::LSKey &key, ObSharedLogUploadHandler *handler)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  bool has_file_to_upload = false;
  if (OB_FAIL(upload_mgr_.upload_ls(handler, has_file_to_upload))) {
    CLOG_LOG(WARN, "failed to open_palf", K(handler));
  } else if (has_file_to_upload) {
    has_file_to_upload_ = true;
  }
  return true;//need to handle all ls
}

ObFileUploadMgr::StatUploadProcessFunctor::StatUploadProcessFunctor()
  : unuploaded_size_(0), uploaded_size_(0) {}

ObFileUploadMgr::StatUploadProcessFunctor::~StatUploadProcessFunctor()
{
  unuploaded_size_ = 0;
  uploaded_size_ = 0;
}

bool ObFileUploadMgr::StatUploadProcessFunctor::operator()(palf::LSKey &key, ObSharedLogUploadHandler *handler)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  LogUploadCtx ctx; 
  if (OB_FAIL(handler->get_log_upload_ctx(ctx))) {
    CLOG_LOG(WARN, "get_log_upload_ctx failed", KR(ret), K(key));
  } else if (!ctx.is_valid()) {
  } else {
    uploaded_size_ = (ctx.max_block_id_on_ss_ + 1) * palf::PALF_BLOCK_SIZE;
    unuploaded_size_ = (ctx.end_block_id_ + 1) * palf::PALF_BLOCK_SIZE;
  }
  return true;//need to handle all ls
}

int64_t ObFileUploadMgr::StatUploadProcessFunctor::get_unuploaded_size() const
{
  return unuploaded_size_;
}

int64_t ObFileUploadMgr::StatUploadProcessFunctor::get_uploaded_size() const
{
  return uploaded_size_;
}

template <typename LIST>
int ObFileUploadMgr::sync_base_lsn_(const ObLSID &id,
                                    const LSN &base_lsn,
                                    const LIST &list)
{
  int ret = OB_SUCCESS;
  const int64_t CONN_TIMEOUT_US = GCONF.rpc_timeout;
  LogSyncBaseLSNReq sync_base_req(self_, id, base_lsn);
  obrpc::ObLogServiceRpcProxy *rpc_proxy = log_service_->get_rpc_proxy();
  for (int i = 0; i < list.get_member_number(); i++) {
    int tmp_ret = OB_SUCCESS;
    common::ObAddr server;
    if (OB_SUCCESS != (tmp_ret = list.get_server_by_index(i, server))) {
      CLOG_LOG(WARN, "failed to get_server_by_index", K(tmp_ret), K(id), K(base_lsn));
    } else if (OB_SUCCESS != (tmp_ret = rpc_proxy->to(server)
                              .timeout(CONN_TIMEOUT_US)
                              .trace_time(true)
                              .max_process_handler_time(static_cast<int32_t>(CONN_TIMEOUT_US))
                              .by(MTL_ID())
                              .sync_base_lsn(sync_base_req, NULL))) {
      CLOG_LOG(WARN, "sync base lsn failed", K(tmp_ret), K(id), K(base_lsn), K(server));
    } else {
      CLOG_LOG(TRACE, "sync base lsn", K(id), K(base_lsn), K(server));
    }
  }
  return ret;
}

int ObFileUploadMgr::do_upload_file_(const uint64_t tenant_id,
                                     const share::ObLSID &ls_id,
                                     const palf::block_id_t block_id)
{
  static_assert(SINGLE_PART_SIZE > palf::MAX_LOG_BUFFER_SIZE, "unexpected upload size");
  static_assert(SINGLE_PART_SIZE + palf::MAX_LOG_BUFFER_SIZE == SINGLE_READ_SIZE, "unexpected upload size");
  int ret = OB_SUCCESS;
  int64_t remained_upload_size = palf::PALF_PHY_BLOCK_SIZE;
  int64_t remained_read_size = palf::PALF_BLOCK_SIZE;
  const int64_t part_count = (remained_upload_size + SINGLE_PART_SIZE) / SINGLE_PART_SIZE;
  LSN read_lsn(block_id * palf::PALF_BLOCK_SIZE);
  PalfHandleGuard palf_handle_guard;
  ObIOFd io_fd;
  palf::LogIOContext io_ctx(tenant_id, ls_id.id(), palf::LogIOUser::SHARED_UPLOAD);
  CONSUMER_GROUP_FUNC_GUARD(io_ctx.get_function_type());
  ObLogExternalStorageCtx run_ctx;
  if (OB_FAIL(log_service_->open_palf(ls_id, palf_handle_guard))) {
    CLOG_LOG(WARN, "open_palf failed", KR(ret), K(tenant_id), K(ls_id), K(block_id));
  } else if (OB_FAIL(ext_storage_handler_->init_multi_upload(tenant_id, ls_id.id(),
                                                             block_id, part_count, run_ctx))) {
    CLOG_LOG(WARN, "init_multi_upload failed", KR(ret), K(tenant_id), K(ls_id), K(block_id));
  } else {
    bool first_round = true;
    char *read_buf = read_buf_.buf_;
    int64_t in_read_size = SINGLE_READ_SIZE;
    // three steps:
    // 1. raw read from palf.
    // 2. check integrity of raw read data.
    // 3. upload above data to object storage.
    // the layout of raw read data in each step.
    // after raw read        | SINGLE_PART_SIZE |   MAX_LOG_BUFFER_SIZE   |
    // after check integrity |         has check intergrity            |xx|
    // after upload          |    has upload    | has check intergrity |xx|
    // after memove          |  has check intergrity |xx|    next pread   |
    //       read_buf_last_log_end_pos_______________|  | SINGLE_PART_SIZE|
    //       read_buf_raw_read_pos______________________|_________________|
    //                       |                          |   in_read_size  |
    // NB: to ensure that the data uploaded each time has passed integrity verification, 
    //      one more log needs to be read for the first time.

    int64_t read_buf_raw_read_pos = MAX_INFO_BLOCK_SIZE;
    int64_t read_buf_last_log_end_pos = MAX_INFO_BLOCK_SIZE;
    SCN first_log_scn;
    LSN last_log_lsn(block_id * PALF_BLOCK_SIZE);
    int64_t pwrite_offset = 0;
    int64_t out_pwrite_size = 0;
    int64_t to_upload_size = 0;
    int64_t part_idx = 0;
    bool need_abort = true;
    while (remained_upload_size > 0 && OB_SUCC(ret)) {
      int64_t out_read_size = 0;
      // when remained_read_size is zero, no need read from palf, but also need upload remained data
      // to object storage.
      if (0 < remained_read_size
          && OB_FAIL(palf_handle_guard.raw_read(read_lsn, read_buf + read_buf_raw_read_pos,
                                                in_read_size, out_read_size, io_ctx))) {
        CLOG_LOG(WARN, "raw_read failed", KR(ret), K(tenant_id), K(ls_id), K(block_id));
      } else if (0 < remained_read_size && out_read_size != in_read_size) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error, read partial logs", KR(ret), K(tenant_id), K(ls_id), K(block_id),
                 K(read_lsn), K(read_buf_raw_read_pos), K(read_buf_last_log_end_pos),
                 K(to_upload_size), K(remained_upload_size), K(out_pwrite_size),
                 K(in_read_size), K(out_read_size));
      } else if (read_buf_raw_read_pos < read_buf_last_log_end_pos) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error!!!", KR(ret), K(tenant_id), K(ls_id), K(block_id),
                 K(read_lsn), K(read_buf_raw_read_pos), K(read_buf_last_log_end_pos),
                 K(to_upload_size), K(remained_upload_size), K(out_pwrite_size));
      } else if (0 < remained_read_size
                 && OB_FAIL(check_data_integrity_(read_buf + read_buf_last_log_end_pos,
                                                  out_read_size + (read_buf_raw_read_pos - read_buf_last_log_end_pos),
                                                  ls_id, first_log_scn, read_buf_last_log_end_pos,
                                                  last_log_lsn))) {
        CLOG_LOG(WARN, "check_data_integrity_ failed", KR(ret), K(tenant_id), K(ls_id), K(block_id));
      } else if (FALSE_IT(read_lsn = read_lsn + out_read_size) 
                 || FALSE_IT(remained_read_size -= out_read_size)
                 || remained_read_size < 0) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "remained_read_size < 0, unexpected error!!!",
                 KR(ret), K(tenant_id), K(ls_id), K(block_id), K(remained_read_size), K(out_read_size),
                 K(read_lsn), K(read_buf_raw_read_pos), K(read_buf_last_log_end_pos),
                 K(to_upload_size), K(remained_upload_size), K(out_pwrite_size));
      } else {
        in_read_size = MIN(remained_read_size, SINGLE_PART_SIZE);
        to_upload_size = MIN(remained_upload_size, SINGLE_PART_SIZE);
        if (first_round) {
          if (OB_FAIL(serialize_block_header_(read_buf_.buf_, read_buf_.buf_len_, ls_id.id(), block_id,
                                              first_log_scn))) {
              CLOG_LOG(WARN, "serialize_block_header_ failed", KR(ret), K(ls_id), K(block_id), K(first_log_scn));
            } else {
            to_upload_size += MAX_INFO_BLOCK_SIZE;
            first_round = false;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ext_storage_handler_->upload_one_part(read_buf, to_upload_size, pwrite_offset, part_idx, run_ctx))) {
          CLOG_LOG(WARN, "upload_one_part failed", KR(ret), K(run_ctx), KP(read_buf), K(to_upload_size),
                   K(pwrite_offset), K(to_upload_size), K(part_idx));
        } else {
          part_idx++;
          remained_upload_size -= to_upload_size;
          pwrite_offset += to_upload_size;
          if (remained_read_size == 0 && remained_upload_size > SINGLE_PART_SIZE) {
            ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(ERROR, "remained_upload_size > SINGLE_PART_SIZE when remained_read_size == 0, unexpected error!!!",
                     KR(ret), K(tenant_id), K(ls_id), K(block_id), K(remained_read_size),
                     K(read_lsn), K(read_buf_raw_read_pos), K(read_buf_last_log_end_pos),
                     K(to_upload_size), K(remained_upload_size), K(to_upload_size));
          } else if (FALSE_IT(read_buf_raw_read_pos += (out_read_size - to_upload_size))
                     || FALSE_IT(read_buf_last_log_end_pos -= to_upload_size)) {
          } else if (read_buf_raw_read_pos < 0 || read_buf_last_log_end_pos < 0) {
            ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(ERROR, "read_buf_raw_read_pos < 0 or read_buf_last_log_end_pos < 0, unexpected error!!!",
                     KR(ret), K(tenant_id), K(ls_id), K(block_id), K(remained_read_size),
                     K(read_lsn), K(read_buf_raw_read_pos), K(read_buf_last_log_end_pos),
                     K(to_upload_size), K(remained_upload_size), K(to_upload_size));
          } else {
            MEMMOVE(read_buf, read_buf+to_upload_size, read_buf_raw_read_pos);
            CLOG_LOG(TRACE, "memove success", KR(ret), K(tenant_id), K(ls_id), K(block_id),
                     K(read_lsn), K(read_buf_raw_read_pos), K(read_buf_last_log_end_pos),
                     K(to_upload_size), K(remained_upload_size), K(to_upload_size));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      need_abort = false;
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(run_ctx.wait(out_pwrite_size))) {
      CLOG_LOG(WARN, "wait failed", KR(ret), KR(tmp_ret), K(tenant_id), K(ls_id), K(block_id),
               K(read_lsn), K(read_buf_raw_read_pos), K(read_buf_last_log_end_pos),
               K(to_upload_size), K(remained_upload_size), K(out_pwrite_size));
      need_abort = true;
      ret = tmp_ret;
    }
    if (need_abort) {
      // NB: don't overwrite errno after abort_multi_upload failed.
      tmp_ret = ext_storage_handler_->abort_multi_upload(run_ctx);
      CLOG_LOG(WARN, "abort_multi_upload finish", KR(ret), K(tmp_ret), K(tenant_id),
               K(ls_id), K(block_id));
    } else {
      if (OB_FAIL(ext_storage_handler_->complete_multi_upload(run_ctx))) {
        CLOG_LOG(WARN, "complete_multi_upload failed", KR(ret), KR(tmp_ret), K(tenant_id),
                 K(ls_id), K(block_id));
      }
    }
  }
  return ret;
}

int ObFileUploadMgr::check_data_integrity_(
  const char *buff,
  const int64_t buff_len,
  const share::ObLSID &ls_id,
  share::SCN &first_log_scn,
  int64_t &last_log_end_pos,
  LSN &last_log_lsn)
{
  int ret = OB_SUCCESS;
  palf::MemoryStorage mem_storage;
  palf::MemPalfGroupBufferIterator iter;
  const LSN start_lsn = last_log_lsn;
  LSN end_lsn = last_log_lsn + buff_len;
  if (OB_FAIL(mem_storage.init(last_log_lsn))) {
    CLOG_LOG(WARN, "failed to init MemoryStorage", K(last_log_lsn));
  } else if (OB_FAIL(mem_storage.append(buff, buff_len))) {
    CLOG_LOG(WARN, "failed to append", KR(ret), K(last_log_lsn), KP(buff));
  } else if (OB_FAIL(iter.init(last_log_lsn, [&end_lsn]() { return end_lsn; }, &mem_storage))) {
    CLOG_LOG(WARN, "failed to init iterator", KR(ret), K(end_lsn));
  } else if (OB_FAIL(iter.set_io_context(palf::LogIOContext(MTL_ID(), ls_id.id(), palf::LogIOUser::SHARED_UPLOAD)))) {
    CLOG_LOG(WARN, "failed to set_io_context", KR(ret), K(end_lsn));
  } else {
    palf::LogGroupEntry entry;
    palf::LSN lsn;
    bool is_first_entry = true;
    while (OB_SUCC(ret)) {
      //iterator check data integrity inside
      if (OB_FAIL(iter.next())) {
        if (OB_ITER_END != ret && OB_NEED_RETRY != ret) {
          CLOG_LOG(ERROR, "iter next failed", KR(ret), K(iter));
        }
      } else if (OB_FAIL(iter.get_entry(entry, lsn))) {
        CLOG_LOG(WARN, "get entry failed", K(ret));
      } else {
        if (is_first_entry) {
          if (OB_FAIL(entry.get_log_min_scn(first_log_scn))) {
            CLOG_LOG(ERROR, "failed to get_log_min_scn", KR(ret),  K(entry));
          } else {
            is_first_entry = false;
          }
        }
      }
    }
    if (OB_ITER_END == ret || OB_NEED_RETRY == ret) {
      ret = OB_SUCCESS;    
      last_log_lsn = lsn + entry.get_serialize_size();
      last_log_end_pos += last_log_lsn - start_lsn;
      CLOG_LOG(TRACE, "check_data_integrity_ success", K(iter), K(start_lsn),
               K(last_log_lsn), K(last_log_end_pos), K(first_log_scn));
    }
  }
  return ret;
}

int ObFileUploadMgr::serialize_block_header_(char *buf,
                                             const int64_t buf_len,
                                             const int64_t palf_id,
                                             const palf::block_id_t &block_id,
                                             const share::SCN &min_scn)
{
  int ret = OB_SUCCESS;
  LogBlockHeader block_header;
  int64_t pos = 0;
  if (OB_FAIL(block_header.generate(palf_id, block_id,
                                    LSN(block_id*palf::PALF_BLOCK_SIZE),
                                    min_scn))) {
    CLOG_LOG(WARN, "failed to generate block_header", KR(ret), KP(buf), K(buf_len), K(palf_id),
             K(block_id), K(min_scn));
  } else if (OB_FAIL(block_header.serialize(buf, MAX_INFO_BLOCK_SIZE, pos))) {
    CLOG_LOG(WARN, " failed to serialize block_header", KR(ret));
  } else {
  }
  return ret;
}

int ObFileUploadMgr::try_alloc_read_buf_()
{
  int ret = OB_SUCCESS;
  if (read_buf_.is_valid()) {
  } else if (OB_FAIL(alloc_read_buf("UploadMgr", SINGLE_READ_SIZE + MAX_INFO_BLOCK_SIZE, read_buf_))) {
    CLOG_LOG(WARN, "alloc_read_buf failed", KR(ret));
  } else {}
  return ret;
}

void ObFileUploadMgr::try_destroy_read_buf_()
{
  if (!read_buf_.is_valid()) {
  } else {
    free_read_buf(read_buf_);
  }
}
} // end of namespace logservice
}//end of namespace oceanbase
