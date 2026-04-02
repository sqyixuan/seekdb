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

#include "palf_env_lite.h"
#include "palf_env_lite_mgr.h"
#include "palf_handle_lite.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
namespace palflite
{
PalfEnvLite::PalfEnvLite() : palf_meta_lock_(common::ObLatchIds::PALF_ENV_LOCK),
                             log_alloc_mgr_(NULL),
                             log_block_pool_(NULL),
                             log_rpc_(),
                             cb_thread_pool_(),
                             log_io_worker_(),
                             self_(),
                             palf_handle_impl_map_(64),  // specify min_size=64
                             last_palf_epoch_(0),
                             tenant_id_(0),
                             palf_env_mgr_(NULL),
                             is_running_(true),
                             has_deleted_(false),
                             is_inited_(false)
{
  log_dir_[0] = '\0';
  tmp_log_dir_[0] = '\0';
}

PalfEnvLite::~PalfEnvLite()
{
  destroy();
}

int PalfEnvLite::init(const char *base_dir,
                      const ObAddr &self,
                      const int64_t cluster_id,
                      const int64_t tenant_id,
                      rpc::frame::ObReqTransport *transport,
                      common::ObILogAllocator *log_alloc_mgr,
                      ILogBlockPool *log_block_pool,
                      PalfEnvLiteMgr *palf_env_mgr)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  LogIOWorkerConfig log_io_worker_config;
  log_io_worker_config.io_worker_num_ = 1;
  log_io_worker_config.io_queue_capcity_ = PALF_SLIDING_WINDOW_SIZE;
  log_io_worker_config.batch_width_ = 0;
  log_io_worker_config.batch_depth_ = 0;
  const int64_t io_cb_num = PALF_SLIDING_WINDOW_SIZE;
  // mtl_id is MTL_ID, and in arb_server, this value set to OB_SERVER_TENANT_ID
  // tenant_id_used_to_print_log is the tenant id which used to print log, this value set to as same as normal observer.
  int64_t mtl_id = OB_SERVER_TENANT_ID;
  int64_t tenant_id_used_to_print_log = tenant_id;
  // NB: for arbserver, no need throttling.
  const bool need_ignore_throttle = true;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "PalfEnvLite is inited twiced", K(ret));
  } else if (OB_ISNULL(base_dir) || !self.is_valid() || NULL == transport
             || OB_ISNULL(log_alloc_mgr) || OB_ISNULL(log_block_pool)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(transport), K(base_dir), K(self), KP(transport),
             KP(log_alloc_mgr), KP(log_block_pool));
  } else if (OB_FAIL(ob_make_unique<arbserver::ArbTGHelper>(
    arb_tg_helper_, cluster_id, mtl_id, tenant_id_used_to_print_log))) {
    CLOG_LOG(ERROR, "create ArbTGHelper failed", KR(ret), KPC(this));
  } else if (FALSE_IT(ObTenantEnv::set_tenant(arb_tg_helper_.get_ptr()))) {
  // Note: the arb server do not need batch_rpc, set NULL
  } else if (OB_FAIL(log_rpc_.init(self, cluster_id, tenant_id, transport, NULL))) {
    CLOG_LOG(ERROR, "LogRpc init failed", K(ret));
  } else if (OB_FAIL(cb_thread_pool_.init(io_cb_num, this))) {
    CLOG_LOG(ERROR, "LogIOTaskThreadPool init failed", K(ret));
  } else if (OB_FAIL(log_io_worker_.init(log_io_worker_config,
                                         mtl_id,
                                         cb_thread_pool_.get_tg_id(),
                                         log_alloc_mgr, &throttle_,
                                         need_ignore_throttle,
                                         this))) {
    CLOG_LOG(ERROR, "LogIOWorker init failed", K(ret));
  } else if ((pret = snprintf(log_dir_, MAX_PATH_SIZE, "%s", base_dir)) && false) {
    ret = OB_ERR_UNEXPECTED;
  } else if ((pret = snprintf(tmp_log_dir_, MAX_PATH_SIZE, "%s/tmp_dir", log_dir_)) && false) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "error unexpected", K(ret));
  } else if (pret < 0 || pret >= MAX_PATH_SIZE) {
    ret = OB_BUF_NOT_ENOUGH;
    CLOG_LOG(ERROR, "construct log path failed", K(ret), K(pret));
  } else if (OB_FAIL(palf_handle_impl_map_.init("LOG_HASH_MAP", mtl_id))) {
    CLOG_LOG(ERROR, "palf_handle_impl_map_ init failed", K(ret));
  } else if (OB_FAIL(log_loop_thread_.init(this))) {
    CLOG_LOG(ERROR, "log_loop_thread_ init failed", K(ret));
  } else {
    log_alloc_mgr_ = log_alloc_mgr;
    log_block_pool_ = log_block_pool;
    self_ = self;
    palf_env_mgr_ = palf_env_mgr;
    tenant_id_ = tenant_id;
    is_inited_ = true;
    is_running_ = true;
    CLOG_LOG(INFO, "PalfEnvLite init success", K(ret), K(self_), K(tenant_id), KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int PalfEnvLite::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(scan_all_palf_handle_impl_director_())) {
    CLOG_LOG(WARN, "scan_all_palf_handle_impl_director_ failed", K(ret));
  } else if (OB_FAIL(cb_thread_pool_.start())) {
    CLOG_LOG(ERROR, "LogIOTaskThreadPool start failed", K(ret));
  } else if (OB_FAIL(log_io_worker_.start())) {
    CLOG_LOG(ERROR, "LogIOWorker start failed", K(ret));
  } else if (OB_FAIL(remove_stale_incomplete_palf_())) {
    CLOG_LOG(ERROR, "remove_stale_incomplete_palf_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(log_loop_thread_.start())) {
    CLOG_LOG(ERROR, "log_loop_thread_ start failed", K(ret));
  } else {
    is_running_ = true;
    CLOG_LOG(INFO, "PalfEnv start success", K(ret), K(MTL_ID()));
  }
  return ret;
}

void PalfEnvLite::stop()
{
  if (is_running_) {
    is_running_ = false;
    log_io_worker_.stop();
    cb_thread_pool_.stop();
    log_loop_thread_.stop();
    CLOG_LOG(INFO, "PalfEnvLite stop success", KPC(this));
  }
}

void PalfEnvLite::wait()
{
  log_io_worker_.wait();
  cb_thread_pool_.wait();
  log_loop_thread_.wait();
  CLOG_LOG(INFO, "PalfEnvLite wait success", KPC(this));
}

void PalfEnvLite::destroy()
{
  stop();
  wait();
  CLOG_LOG_RET(WARN, OB_SUCCESS, "PalfEnvLite destroy", KPC(this));
  is_running_ = false;
  is_inited_ = false;
  palf_handle_impl_map_.destroy();
  log_io_worker_.destroy();
  cb_thread_pool_.destroy();
  log_loop_thread_.destroy();
  log_rpc_.destroy();
  log_alloc_mgr_ = NULL;
  if (true == ATOMIC_LOAD(&has_deleted_)) {
    CLOG_LOG_RET(WARN, OB_SUCCESS, "need remove palf_env_lite", KPC(this));
    palf_env_mgr_->remove_dir(log_dir_);
    // Delete directory
  }
  palf_env_mgr_ = NULL;
  self_.reset();
  log_dir_[0] = '\0';
  tmp_log_dir_[0] = '\0';
}

void PalfEnvLite::set_deleted()
{
  ATOMIC_STORE(&has_deleted_, true);
}

bool PalfEnvLite::has_set_deleted() const
{
  return true == ATOMIC_LOAD(&has_deleted_);
}

int PalfEnvLite::create_palf_handle_impl(const int64_t palf_id,
                                         const AccessMode &access_mode,
                                         const PalfBaseInfo &palf_base_info,
                                         IPalfHandleImpl *&ipalf_handle_impl)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char base_dir[MAX_PATH_SIZE] = {'\0'};

  WLockGuard guard(palf_meta_lock_);
  PalfHandleLite *palf_handle_lite = NULL;
  LSKey hash_map_key(palf_id);
  const int64_t palf_epoch = ATOMIC_AAF(&last_palf_epoch_, 1);
  const LogReplicaType replica_type = LogReplicaType::ARBITRATION_REPLICA;
  arbserver::ObArbMonitor *monitor = palf_env_mgr_->get_arb_monitor();
  palf::LogIOAdapter *io_adapter = palf_env_mgr_->get_io_adapter();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "PalfEnvLite is not running", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "PalfEnvLite is not running", K(ret));
  } else if (false == is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(palf_id));
  } else if (OB_ENTRY_EXIST == palf_handle_impl_map_.contains_key(hash_map_key)) {
    ret = OB_ENTRY_EXIST;
    CLOG_LOG(WARN, "palf_handle has exist, ignore this request", K(ret), K(palf_id));
  } else if (0 > (pret = snprintf(base_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", K(pret), K(palf_id));
  // Note:: order is vital, allocate memory may be fail
  } else if (NULL == (palf_handle_lite = MTL_NEW(PalfHandleLite, "PalfEnvLite"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc palf_handle_lite failed", K(ret));
  } else if (OB_FAIL(create_directory(base_dir))) {
    CLOG_LOG(WARN, "prepare_directory_for_creating_ls failed!!!", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_handle_lite->init(palf_id, access_mode, palf_base_info,
      base_dir, log_alloc_mgr_, log_block_pool_, &log_rpc_, &log_io_worker_,
      this, self_, palf_epoch, io_adapter))) {
    CLOG_LOG(ERROR, "PalfHandleImpl init failed", K(ret), K(palf_id));
    // NB: always insert value into hash map finally.
  } else if (OB_FAIL(palf_handle_impl_map_.insert_and_get(hash_map_key, palf_handle_lite))) {
    CLOG_LOG(WARN, "palf_handle_impl_map_ insert_and_get failed", K(ret), K(palf_id));
  } else {
    (void)palf_handle_lite->set_monitor_cb(monitor);
    palf_handle_lite->set_scan_disk_log_finished();
    ipalf_handle_impl = palf_handle_lite;
  }

  if (OB_FAIL(ret) && NULL != palf_handle_lite) {
    // if 'palf_handle_impl' has not been inserted into hash map,
    // need reclaim manually.
    PalfHandleImplFactory::free(palf_handle_lite);
    palf_handle_lite = NULL;
    if (OB_ENTRY_NOT_EXIST == palf_handle_impl_map_.contains_key(hash_map_key)) {
      remove_directory_while_exist_(base_dir);
    }
  }

  CLOG_LOG(INFO, "PalfEnvLite create_palf_handle_impl finished", K(ret), K(palf_id),
      K(access_mode), K(palf_base_info), K(replica_type), KPC(this));

  return ret;
}

int PalfEnvLite::remove_palf_handle_impl(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(palf_meta_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "PalfEnvLite is not inited", K(ret));
  } else if (OB_FAIL(remove_palf_handle_impl_from_map_not_guarded_by_lock_(palf_id))) {
    CLOG_LOG(WARN, "palf instance not exist", K(ret), KPC(this), K(palf_id));
  } else if (OB_FAIL(wait_until_reference_count_to_zero_(palf_id))) {
    CLOG_LOG(WARN, "wait_until_reference_count_to_zero_ failed", K(ret), KPC(this), K(palf_id));
  } else {
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int PalfEnvLite::get_palf_handle_impl(const int64_t palf_id,
                                      IPalfHandleImplGuard &palf_handle_impl_guard)
{
  int ret = OB_SUCCESS;
  IPalfHandleImpl *palf_handle_impl = NULL;
  if (OB_FAIL(get_palf_handle_impl(palf_id, palf_handle_impl))) {
    CLOG_LOG(WARN, "get_palf_handle_impl failed", K(ret), K(palf_id));
  } else {
    palf_handle_impl_guard.palf_handle_impl_ = palf_handle_impl;
    palf_handle_impl_guard.palf_env_impl_ =  this;
    palf_handle_impl_guard.palf_id_ = palf_id;
    // do nothing
  }
  return ret;
}

int PalfEnvLite::get_palf_handle_impl(const int64_t palf_id,
                                      IPalfHandleImpl *&palf_handle_impl)
{
  int ret = OB_SUCCESS;
  LSKey hash_map_key(palf_id);
  if (false == is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_handle_impl_map_.get(hash_map_key, palf_handle_impl))) {
    CLOG_LOG(TRACE, "get from map failed", K(ret), K(palf_id), K(palf_handle_impl));
  } else if (false == palf_handle_impl->check_can_be_used()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    CLOG_LOG(DEBUG, "PalfEnvLite get_palf_handle_impl success", K(palf_id), K(self_), KP(palf_handle_impl));
  }

  if (OB_FAIL(ret)) {
    revert_palf_handle_impl(palf_handle_impl);
    palf_handle_impl = NULL;
  }
  return ret;
}

void PalfEnvLite::revert_palf_handle_impl(IPalfHandleImpl *palf_handle_impl)
{
  if (NULL != palf_handle_impl) {
    palf_handle_impl_map_.revert(palf_handle_impl);
    CLOG_LOG(TRACE, "PalfEnvLite revert_palf_handle_impl success", KP(palf_handle_impl));
  }
}

int PalfEnvLite::scan_all_palf_handle_impl_director_()
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("PalfEnvLiteStart", 0);
	// TODO by runlin: how to avoid modify 'log_disk_usage_limit_size_' after restart?
  ReloadPalfHandleImplFunctor functor(this);
  if (OB_FAIL(scan_dir(log_dir_, functor))) {
    CLOG_LOG(WARN, "scan_dir failed", K(ret));
  } else {
    guard.click("scan_dir");
    CLOG_LOG(INFO, "scan_all_palf_handle_impl_director_ success", K(ret), K(log_dir_), K(guard));
  }
  return ret;
}

int PalfEnvLite::create_directory(const char *base_dir)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  const mode_t mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
  char tmp_base_dir[MAX_PATH_SIZE] = {'\0'};
  char log_dir[MAX_PATH_SIZE] = {'\0'};
  char meta_dir[MAX_PATH_SIZE] = {'\0'};
  if (0 > (pret = snprintf(tmp_base_dir, MAX_PATH_SIZE, "%s%s", base_dir, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", K(pret), K(base_dir));
  } else if (0 > (pret = snprintf(meta_dir, MAX_PATH_SIZE, "%s/meta", tmp_base_dir))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", K(pret), K(base_dir));
  } else if (-1 == (::mkdir(tmp_base_dir, mode))) {
    ret = convert_sys_errno();
    CLOG_LOG(WARN, "mkdir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (-1 == (::mkdir(meta_dir, mode))) {
    ret = convert_sys_errno();
    CLOG_LOG(WARN, "mkdir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (OB_FAIL(rename_with_retry(tmp_base_dir, base_dir))) {
    CLOG_LOG(ERROR, "rename tmp dir to normal dir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else if (OB_FAIL(FileDirectoryUtils::fsync_dir(log_dir_))) {
    CLOG_LOG(ERROR, "fsync_dir failed", K(ret), K(errno), K(tmp_base_dir), K(base_dir));
  } else {
    CLOG_LOG(INFO, "prepare_directory_for_creating_ls success", K(ret), K(base_dir));
  }
  if (OB_FAIL(ret)) {
    remove_directory_while_exist_(tmp_base_dir);
    remove_directory_while_exist_(base_dir);
  }
  return ret;
}

// step:
// 1. rename log directory to tmp directory.
// 2. delete tmp directory.
// NB: '%s.tmp' is invalid block or invalid directory, before the restart phash of PalfEnvLite,
//     need delete thses tmp block or directory.
int PalfEnvLite::remove_directory(const char *log_dir)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char tmp_log_dir[MAX_PATH_SIZE] = {'\0'};
  if (0 > (pret = snprintf(tmp_log_dir, MAX_PATH_SIZE, "%s%s", log_dir, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", K(ret), K(pret), K(log_dir), K(tmp_log_dir));
  } else if (OB_FAIL(rename_with_retry(log_dir, tmp_log_dir))) {
    CLOG_LOG(WARN, "rename log dir to tmp dir failed", K(ret), K(errno), K(tmp_log_dir), K(log_dir));
  } else {
    bool result = true;
    do {
      if (OB_FAIL(FileDirectoryUtils::is_exists(tmp_log_dir, result))) {
        CLOG_LOG(WARN, "check directory exists failed", KPC(this), K(log_dir));
      } else if (!result) {
        CLOG_LOG(WARN, "directory not exists", KPC(this), K(log_dir));
        break;
      } else if (OB_FAIL(remove_directory_rec(tmp_log_dir, log_block_pool_))) {
        CLOG_LOG(WARN, "remove_directory_rec failed", K(tmp_log_dir), KP(log_block_pool_));
      } else {
      }
      if (OB_FAIL(ret) && true == result) {
        CLOG_LOG(WARN, "remove directory failed, may be physical disk full", K(ret), KPC(this));
        usleep(100*1000);
      }
    } while (OB_FAIL(ret));
  }
  (void)FileDirectoryUtils::fsync_dir(log_dir_);
  CLOG_LOG(WARN, "remove_directory finished", KR(ret), K(log_dir), KP(this));
  return ret;
}

bool PalfEnvLite::SwitchStateFunctor::operator() (const LSKey &palf_id, PalfHandleImpl *palf_handle_impl)
{
  int tmp_ret = OB_SUCCESS;
  if (NULL == palf_handle_impl) {
    CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "palf_handle_impl is NULL", KP(palf_handle_impl), K(palf_id));
  } else if (OB_SUCCESS != (tmp_ret = palf_handle_impl->check_and_switch_state())) {
    CLOG_LOG_RET(WARN, tmp_ret, "check_and_switch_state failed", K(tmp_ret), K(palf_id));
  } else {}
  return true;
}

PalfEnvLite::RemoveStaleIncompletePalfFunctor::RemoveStaleIncompletePalfFunctor(PalfEnvLite *palf_env_lite)
  : palf_env_lite_(palf_env_lite)
{}

 PalfEnvLite::RemoveStaleIncompletePalfFunctor::~RemoveStaleIncompletePalfFunctor()
{
  palf_env_lite_ = NULL;
}

int PalfEnvLite::RemoveStaleIncompletePalfFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  const char *d_name = entry->d_name;
  char *saveptr = NULL;
  MEMCPY(file_name, d_name, strlen(d_name));
  char *tmp = strtok_r(file_name, "_", &saveptr);
  char *timestamp_str = NULL;
  if (NULL == tmp || NULL == (timestamp_str = strtok_r(NULL, "_", &saveptr))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected format", K(ret), K(tmp), K(file_name));
  } else {
    int64_t timestamp = atol(timestamp_str);
    int64_t current_timestamp = ObTimeUtility::current_time();
    int64_t delta = current_timestamp - timestamp;
    constexpr int64_t week_us = 7 * 24 * 60 * 60 * 1000 * 1000ll;
    if (delta <= week_us) {
      CLOG_LOG(TRACE, "no need remove this incomplet dir", K(d_name), K(delta),
          K(timestamp), K(timestamp_str), K(current_timestamp));
    } else {
      char path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      int pret = OB_SUCCESS;
      if (0 > (pret = snprintf(path, MAX_PATH_SIZE, "%s/%s", palf_env_lite_->tmp_log_dir_, d_name))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "snprintf failed", K(ret), K(file_name), K(d_name));
      } else if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(path))) {
        CLOG_LOG(WARN, "delete_directory_rec failed", K(ret), K(file_name), K(path), K(entry->d_name), K(timestamp_str));
      } else {
        CLOG_LOG(WARN, "current incomplete palf has bee staled, delete it", K(timestamp), K(current_timestamp), K(path));
      }
    }
  }
  return ret;
}

common::ObILogAllocator* PalfEnvLite::get_log_allocator()
{
  return log_alloc_mgr_;
}

int PalfEnvLite::for_each(const common::ObFunction<int (IPalfHandleImpl *)> &func)
{
  auto func_impl = [&func](const LSKey &ls_key, IPalfHandleImpl *ipalf_handle_impl) -> bool {
    bool bool_ret = true;
    int ret = OB_SUCCESS;
    if (OB_FAIL(func(ipalf_handle_impl))) {
      CLOG_LOG(WARN, "execute func failed", K(ret), K(ls_key));
    } else {
    }
    if (OB_FAIL(ret)) {
     bool_ret = false;
    }
    return bool_ret;
  };
  int ret = OB_SUCCESS;
  if (OB_FAIL(palf_handle_impl_map_.for_each(func_impl))) {
    CLOG_LOG(WARN, "iterate palf_handle_impl_map_ failed", K(ret));
  } else {
  }
  return ret;
}

int PalfEnvLite::get_io_start_time(int64_t &last_working_time)
{
  last_working_time = OB_INVALID_TIMESTAMP;
  return OB_SUCCESS;
}

int64_t PalfEnvLite::get_tenant_id()
{
  return tenant_id_;
}

PalfEnvLite::ReloadPalfHandleImplFunctor::ReloadPalfHandleImplFunctor(PalfEnvLite *palf_env_lite) : palf_env_lite_(palf_env_lite)
{
}

int PalfEnvLite::ReloadPalfHandleImplFunctor::func(const struct dirent *entry)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  ObTimeGuard guard("ReloadFunctor");
  struct stat st;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid args", K(ret), KP(entry));
  } else if (0 > (pret = snprintf(log_dir, MAX_PATH_SIZE, "%s/%s", palf_env_lite_->log_dir_, entry->d_name))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "snprint failed", K(ret), K(pret), K(entry->d_name));
  } else if (0 != stat(log_dir, &st)) {
    CLOG_LOG(INFO, "this entry is not a block", K(ret), K(log_dir), K(errno));
  } else if (false == S_ISDIR(st.st_mode)) {
    CLOG_LOG(WARN, "path is not a directory, ignore it", K(ret), K(log_dir), K(st.st_mode));
  } else {
    const char *path = entry->d_name;
    bool is_number = true;
    const size_t path_len = strlen(path);
    for (size_t i = 0; is_number && i < path_len; ++i) {
      if ('\0' == path[i]) {
        break;
      } else if (!isdigit(path[i])) {
        is_number = false;
      }
    }
    if (!is_number) {
      // do nothing, skip invalid block like tmp
    } else {
      int64_t id = strtol(path, nullptr, 10);
      if (OB_FAIL(palf_env_lite_->reload_palf_handle_impl_(id))) {
        CLOG_LOG(WARN, "reload_palf_handle_impl failed", K(ret));
      }
      guard.click("reload_palf_handle_impl");
      CLOG_LOG(INFO, "reload_palf_handle_impl_", K(ret), K(id), K(guard));
    }
  }
  return ret;
}

int PalfEnvLite::reload_palf_handle_impl_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  PalfHandleLite *tmp_palf_handle_lite;
  char base_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  int64_t start_ts = ObTimeUtility::current_time();
  LSKey hash_map_key(palf_id);
  bool is_integrity = true;
  const int64_t palf_epoch = ATOMIC_AAF(&last_palf_epoch_, 1);
  arbserver::ObArbMonitor *monitor = palf_env_mgr_->get_arb_monitor();
  palf::LogIOAdapter *io_adapter = palf_env_mgr_->get_io_adapter();
  if (0 > (pret = snprintf(base_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "snprint failed", K(ret), K(pret), K(palf_id));
  } else if (NULL == (tmp_palf_handle_lite = MTL_NEW(PalfHandleLite, "PalfEnvLite"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc palf_handle_impl failed", K(ret));
  } else if (OB_FAIL(tmp_palf_handle_lite->load(palf_id, base_dir, log_alloc_mgr_,
          log_block_pool_, &log_rpc_, &log_io_worker_, this, self_,
          palf_epoch, io_adapter, is_integrity))) {
    CLOG_LOG(ERROR, "PalfHandleImpl init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_handle_impl_map_.insert_and_get(hash_map_key, tmp_palf_handle_lite))) {
    CLOG_LOG(WARN, "palf_handle_impl_map_ insert_and_get failed", K(ret), K(palf_id), K(tmp_palf_handle_lite));
  } else if (OB_FAIL(tmp_palf_handle_lite->set_monitor_cb(monitor))) {
    CLOG_LOG(WARN, "set_monitor_cb failed", K(ret), K(palf_id), KP(monitor));
  } else {
    (void) tmp_palf_handle_lite->set_scan_disk_log_finished();
    palf_handle_impl_map_.revert(tmp_palf_handle_lite);
    int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    CLOG_LOG(INFO, "reload_palf_handle_impl success", K(ret), K(palf_id), K(cost_ts), KP(this),
        KP(&palf_handle_impl_map_));
  }

  if (OB_FAIL(ret) && NULL != tmp_palf_handle_lite) {
    // if 'tmp_palf_handle_lite' has not been inserted into hash map,
    // need reclaim manually.
    CLOG_LOG(ERROR, "reload_palf_handle_impl_ failed, need free tmp_palf_handle_lite",
        K(ret), K(tmp_palf_handle_lite));
    if (OB_ENTRY_NOT_EXIST == palf_handle_impl_map_.contains_key(hash_map_key)) {
      PalfHandleImplFactory::free(tmp_palf_handle_lite);
      tmp_palf_handle_lite = NULL;
    }
  } else if (false == is_integrity) {
    CLOG_LOG(WARN, "palf instance is not integrity, remove it", K(palf_id));
    ret = move_incomplete_palf_into_tmp_dir_(palf_id);
  }
  return ret;
}

int PalfEnvLite::wait_until_reference_count_to_zero_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char base_dir[MAX_PATH_SIZE] = {'\0'};
  char tmp_base_dir[MAX_PATH_SIZE] = {'\0'};
  if (false == is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(palf_id));
  } else if (0 > (pret = snprintf(base_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", K(ret), K(pret), K(palf_id));
  } else if (0 > (pret = snprintf(tmp_base_dir, MAX_PATH_SIZE, "%s/%ld%s", log_dir_, palf_id, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", K(ret), K(pret), K(palf_id));
  } else {
    bool normal_dir_exist = true;
    bool tmp_dir_exist = true;
    while (OB_SUCC(FileDirectoryUtils::is_exists(base_dir, normal_dir_exist))
           && OB_SUCC(FileDirectoryUtils::is_exists(tmp_base_dir, tmp_dir_exist))) {
      if (!normal_dir_exist && !tmp_dir_exist) {
        break;
      }
      CLOG_LOG(INFO, "wait_until_reference_count_to_zero_ failed, may be reference count has leaked", K(palf_id),
          K(normal_dir_exist), K(tmp_dir_exist), K(base_dir), K(tmp_base_dir));
      ob_usleep(1000);
    }
  }
  return ret;
}

int PalfEnvLite::remove_palf_handle_impl_from_map_not_guarded_by_lock_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  LSKey hash_map_key(palf_id);
  auto set_delete_func = [](const LSKey &key, IPalfHandleImpl *value) {
    UNUSED(key);
    value->set_deleted();
  };
  if (OB_FAIL(palf_handle_impl_map_.operate(hash_map_key, set_delete_func))) {
    CLOG_LOG(WARN, "operate palf_handle_impl_map_ failed", K(ret), K(palf_id), KPC(this));
  } else if (OB_FAIL(palf_handle_impl_map_.del(hash_map_key))) {
    CLOG_LOG(WARN, "palf_handle_impl_map_ del failed", K(ret), K(palf_id));
  } else {
    CLOG_LOG(INFO, "remove_palf_handle_impl success", K(ret), K(palf_id));
  }
  return ret;
}

int PalfEnvLite::move_incomplete_palf_into_tmp_dir_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  const mode_t mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
  char src_log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char dest_log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  bool tmp_dir_exist = false;
  int64_t timestamp = ObTimeUtility::current_time();
  if (OB_FAIL(remove_palf_handle_impl_from_map_not_guarded_by_lock_(palf_id))) {
    CLOG_LOG(WARN, "remove_palf_handle_impl_from_map_not_guarded_by_lock_ failed, unexpected", K(ret),
        K(palf_id), KPC(this));;
  } else if (OB_FAIL(check_tmp_log_dir_exist_(tmp_dir_exist))) {
  } else if (false == tmp_dir_exist && (-1 == ::mkdir(tmp_log_dir_, mode))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "mkdir tmp log dir failed", K(ret), KPC(this), K(tmp_log_dir_));
  } else if (0 > (pret = snprintf(src_log_dir, MAX_PATH_SIZE, "%s/%ld", log_dir_, palf_id))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed, unexpected error", K(ret));
  } else if (0 > (pret = snprintf(dest_log_dir, MAX_PATH_SIZE, "%s/%ld_%ld", tmp_log_dir_, palf_id, timestamp))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed, unexpected error", K(ret));
  } else if (OB_FAIL(rename_with_retry(src_log_dir, dest_log_dir))) {
    ret = convert_sys_errno();
    CLOG_LOG(ERROR, "::rename failed", K(ret), KPC(this), K(src_log_dir), K(dest_log_dir));
  } else if (OB_FAIL(FileDirectoryUtils::fsync_dir(log_dir_))) {
    CLOG_LOG(ERROR, "fsync_dir failed", K(ret), KPC(this), K(src_log_dir), K(dest_log_dir));
  } else {
  }
  return ret;
}

int PalfEnvLite::check_tmp_log_dir_exist_(bool &exist) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(FileDirectoryUtils::is_exists(tmp_log_dir_, exist))) {
    CLOG_LOG(WARN, "check dir exist failed", K(ret), KPC(this), K(tmp_log_dir_));
  } else {
  }
  return ret;
}

// TODO: need remove_stale_incomplete_palf_
int PalfEnvLite::remove_stale_incomplete_palf_()
{
  int ret = OB_SUCCESS;
  bool exist = false;
  RemoveStaleIncompletePalfFunctor functor(this);
  if (OB_FAIL(check_tmp_log_dir_exist_(exist))) {
    CLOG_LOG(WARN, "check_tmp_log_dir_exist_ failed", K(ret), KPC(this));
  } else if (false == exist) {
  } else if (OB_FAIL(scan_dir(tmp_log_dir_, functor))){
    CLOG_LOG(WARN, "remove_stale_incomplete_palf_ failed", K(ret), KPC(this), K(tmp_log_dir_));
  } else {
  }
  return ret;
}

int PalfEnvLite::update_replayable_point(const SCN &replayable_scn)
{
  return OB_SUCCESS;
}

int PalfEnvLite::get_throttling_options(PalfThrottleOptions &option)
{
 return OB_SUCCESS;
}

int PalfEnvLite::remove_directory_while_exist_(const char *log_dir)
{
  int ret = OB_SUCCESS;
  bool result = true;
  if (OB_FAIL(FileDirectoryUtils::is_exists(log_dir, result))) {
    CLOG_LOG(WARN, "check directory exists failed", KPC(this), K(log_dir));
  } else if (!result) {
    CLOG_LOG(WARN, "directory not exist, remove_directory success!", K(log_dir), K(result));
  } else if (OB_FAIL(remove_directory(log_dir))) {
    CLOG_LOG(WARN, "remove_directory failed", K(log_dir), K(result));
  } else {}
  return ret;
}

int PalfEnvLite::get_options(PalfOptions &options)
{
  return OB_SUCCESS;
}
} // end namespace palf
} // end namespace oceanbase
