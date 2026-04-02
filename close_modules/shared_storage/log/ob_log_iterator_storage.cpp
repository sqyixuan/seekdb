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

#include "ob_log_iterator_storage.h"
#include "logservice/ob_log_service.h"                                      // ObLogService
namespace oceanbase
{
using namespace palf;
namespace logservice
{

ObLogMemoryStorage::ObLogMemoryStorage() 
  : buf_(NULL),
    buf_len_(0),
    start_lsn_(),
    log_tail_(),
    is_inited_(false)
{
}

ObLogMemoryStorage::~ObLogMemoryStorage()
{
  destroy();
}

int ObLogMemoryStorage::init(const LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (false == start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(start_lsn));
  } else {
    buf_ = NULL;
    buf_len_ = 0;
    log_tail_ = start_lsn_ = start_lsn;
    is_inited_ = true;
    CLOG_LOG(TRACE, "ObLogMemoryStorage init success", K(ret), KPC(this));
  }
  return ret;
}

void ObLogMemoryStorage::destroy()
{
  is_inited_ = false;
  buf_ = NULL;
  buf_len_ = 0;
  log_tail_.reset();
  start_lsn_.reset();
}

int ObLogMemoryStorage::append(
  const palf::LSN &lsn,
  const char *buf,
  const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMemoryStorage not inited", K(lsn), KP(buf), K(buf_len));
  } else if (!lsn.is_valid() || NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(lsn), KP(buf), K(buf_len));
    // defense code.
    // NB: because there are several unconsumed data, the new lsn may be smaller
    //     than or equal to log_tail_.
  } else if (lsn > log_tail_ || lsn < start_lsn_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error!!! invalid lsn", KPC(this), K(lsn), KP(buf), K(buf_len));
  } else {
    buf_ = buf;
    buf_len_ = buf_len;
    start_lsn_ = lsn;
    log_tail_ = lsn + buf_len;
    CLOG_LOG(TRACE, "ObLogMemoryStorage append success", K(ret), KPC(this), K(lsn));
  }
  return ret;
}

int ObLogMemoryStorage::pread(
  const LSN &lsn,
  const int64_t in_read_size,
  ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(io_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMemoryStorage not inited", KPC(this), K(lsn), K(in_read_size), K(read_buf));
  } else if (false == lsn.is_valid()
             || 0 >= in_read_size
             || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KPC(this), K(lsn), K(in_read_size), K(read_buf));
  } else if (lsn >= log_tail_) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    CLOG_LOG(TRACE, "out of upper bound",  K(ret), KPC(this), K(lsn), K(read_buf), K(in_read_size),
             K(out_read_size));
  } else if (lsn < start_lsn_) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
    CLOG_LOG(TRACE, "out of lower bound",  K(ret), KPC(this), K(lsn), K(read_buf), K(in_read_size),
             K(out_read_size));
  } else {
    const offset_t pos = lsn - start_lsn_;
    out_read_size = MIN(in_read_size, log_tail_ - lsn);
    MEMCPY(read_buf.buf_, const_cast<char*>(buf_) + pos, out_read_size);
    CLOG_LOG(TRACE, "ObLogMemoryStorage pread success",  K(ret), KPC(this), K(lsn), K(read_buf), K(in_read_size),
             K(out_read_size), K(pos));
  }
  return ret;
}

int ObLogMemoryStorage::get_data_len(
  int64_t &data_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMemoryStorage not inited", KPC(this));
  } else if (FALSE_IT(data_len = log_tail_ - start_lsn_)) {
  } else if (0 > data_len) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error!!! invalid data len", KPC(this), K(data_len));
  } else {}
  return ret;
}

int ObLogMemoryStorage::get_read_pos(
  const palf::LSN &lsn,
  int64_t &read_pos) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMemoryStorage not inited", KPC(this));
  } else if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KPC(this));
  } else if (lsn > log_tail_) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    CLOG_LOG(TRACE, "out of upper bound",  K(ret), KPC(this), K(lsn));
  } else if (lsn < start_lsn_) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
    CLOG_LOG(TRACE, "out of lower bound",  K(ret), KPC(this), K(lsn));
  } else if (FALSE_IT(read_pos = lsn - start_lsn_)) {
  } else {
    CLOG_LOG(TRACE, "get_read_pos success", K(lsn), K(read_pos), KPC(this));
  }
  return ret;
}

const LSN &ObLogMemoryStorage::get_log_tail() const
{
  return log_tail_;
}

const LSN &ObLogMemoryStorage::get_start_lsn() const
{
  return start_lsn_;
}

void ObLogMemoryStorage::reuse(const palf::LSN &start_lsn)
{
  CLOG_LOG(INFO, "reuse ObLogMemoryStorage", KPC(this), K(start_lsn));
  buf_ = NULL;
  buf_len_ = 0;
  log_tail_ = start_lsn_ = start_lsn;
}

void ObLogMemoryStorage::reset()
{
  const LSN start_lsn(PALF_INITIAL_LSN_VAL);
  CLOG_LOG(INFO, "reset ObLogMemoryStorage", KPC(this), K(start_lsn));
  buf_ = NULL;
  buf_len_ = 0;
  log_tail_ = start_lsn_ = start_lsn;
}

int64_t ObLogSharedStorage::MEMORY_LIMIT = palf::PALF_BLOCK_SIZE;
int64_t ObLogSharedStorage::READ_SIZE = palf::PALF_BLOCK_SIZE;
int64_t ObLogSharedStorage::BLOCK_SIZE = palf::PALF_BLOCK_SIZE;
int64_t ObLogSharedStorage::PHY_BLOCK_SIZE = palf::PALF_PHY_BLOCK_SIZE;
int64_t ObLogSharedStorage::MAX_LOG_SIZE = palf::MAX_LOG_BUFFER_SIZE;

ObLogSharedStorage::ObLogSharedStorage() {}

ObLogSharedStorage::~ObLogSharedStorage()
{
  destroy();
}

int ObLogSharedStorage::init(
  const uint64_t tenant_id,
  const int64_t palf_id,
  const palf::LSN &start_lsn,
  const int64_t suggested_max_read_buffer_size,
  ObLogExternalStorageHandler *ext_handler)
{
  UNUSED(tenant_id);
  UNUSED(palf_id);
  UNUSED(start_lsn);
  UNUSED(suggested_max_read_buffer_size);
  UNUSED(ext_handler);
  return OB_NOT_SUPPORTED;
}

void ObLogSharedStorage::destroy() {}

int ObLogSharedStorage::pread(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  UNUSED(lsn);
  UNUSED(in_read_size);
  UNUSED(read_buf);
  UNUSED(out_read_size);
  UNUSED(io_ctx);
  return OB_NOT_SUPPORTED;
}

void ObLogSharedStorage::reset() {}

ObLogLocalStorage::ObLogLocalStorage()
  : is_inited_(false)
{
}

ObLogLocalStorage::~ObLogLocalStorage()
{
  destroy();
}

int ObLogLocalStorage::init(
  const uint64_t tenant_id,
  const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogLocalStorage has inited twice", K(tenant_id), K(palf_id));
  } else if (!is_valid_tenant_id(tenant_id)
             || !is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(palf_id));
  } else if (OB_FAIL(open_palf_handle_(tenant_id, palf_id))) {
    CLOG_LOG(WARN, "open_palf_handle_ failed", K(tenant_id), K(palf_id));
  } else {
    is_inited_ = true;
    CLOG_LOG(TRACE, "init ObLogLocalStorage success", K(tenant_id), K(palf_id), KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int ObLogLocalStorage::pread(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t aligned_offset = lower_align(lsn.val_, palf::LOG_DIO_ALIGN_SIZE);
  const int64_t offset_backoff = lsn.val_ - aligned_offset;
  const int64_t aligned_read_size = upper_align(in_read_size+offset_backoff, palf::LOG_DIO_ALIGN_SIZE);
  const int64_t read_size_backoff = aligned_read_size - in_read_size;
  const LSN aligned_lsn(aligned_offset);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogLocalStorage not inited", K(lsn), K(in_read_size), K(read_buf));
  } else if (!lsn.is_valid()
             || 0 >= in_read_size
             || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(lsn), K(in_read_size), K(read_buf));
  } else if (!palf_handle_guard_.is_valid()) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
  } else if (OB_FAIL(palf_handle_guard_.raw_read(aligned_lsn, read_buf.buf_, aligned_read_size, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "pread from palf_handle failed", K(lsn), K(in_read_size), K(read_buf), K(out_read_size));
  } else if (out_read_size < offset_backoff) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "out_read_size is smaller than offset_backoff, unexpected", K(lsn), K(in_read_size), K(read_buf), K(out_read_size), K(offset_backoff));
  } else {
    MEMMOVE(read_buf.buf_, read_buf.buf_ + offset_backoff, out_read_size - offset_backoff);
    out_read_size = MIN(out_read_size - offset_backoff, in_read_size);
    CLOG_LOG(TRACE, "pread success", K(lsn), K(in_read_size), K(read_buf), K(out_read_size),
             K(aligned_lsn), K(aligned_read_size), K(offset_backoff), K(read_size_backoff));
  }
  return ret;
}

void ObLogLocalStorage::destroy()
{
  is_inited_ = false;
  palf_handle_guard_.reset();
}

int ObLogLocalStorage::get_file_end_lsn(palf::GetFileEndLSN &function)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogLocalStorage not inited");
  } else if (!palf_handle_guard_.is_valid()) {
    function = []() { return LSN(LOG_MAX_LSN_VAL); };
  } else {
    function = [this]() -> LSN {
      LSN end_lsn(LOG_MAX_LSN_VAL);
      (void)palf_handle_guard_.get_end_lsn(end_lsn);
      return end_lsn;
    };
  }
  if (OB_SUCC(ret) && !function.is_valid()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

int ObLogLocalStorage::get_access_mode_version(palf::GetModeVersion &function)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogLocalStorage not inited");
  } else if (!palf_handle_guard_.is_valid()) {
    function = []() { return PALF_INITIAL_PROPOSAL_ID; };
  } else {
    function = [this]() -> int64_t {
      int64_t mode_version = PALF_INITIAL_PROPOSAL_ID;
      (void)palf_handle_guard_.get_access_mode_version(mode_version);
      return mode_version;
    };
  }
  if (OB_SUCC(ret) && !function.is_valid()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

int ObLogLocalStorage::open_palf_handle_(
  const uint64_t tenant_id,
  const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  palf_handle_guard_.reset();
  if (OB_ISNULL(log_service)) {
    CLOG_LOG(WARN, "nullptr, do nothing", KP(log_service), K(tenant_id), K(palf_id));
  } else if (OB_FAIL(log_service->open_palf(ObLSID(palf_id), palf_handle_guard_))
             && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "unexpected error", KP(log_service), K(tenant_id), K(palf_id));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    CLOG_LOG(INFO, "palf not exist", KP(log_service), K(tenant_id), K(palf_id));
    ret = OB_SUCCESS;
  } else {
  }
  return ret;
}

ObLogHybridStorage::ObLogHybridStorage()
  : ILogStorage(ILogStorageType::HYBRID_STORAGE),
    shared_storage_(),
    local_storage_(),
    shared_storage_read_size_("SharedReadSize", STAT_INTERVAL),
    shared_storage_read_cost_("SharedReadCost", STAT_INTERVAL),
    local_storage_read_size_("LocalReadSize", STAT_INTERVAL),
    local_storage_read_cost_("LocalReadCost", STAT_INTERVAL),
    is_inited_(false)
{
}

ObLogHybridStorage::~ObLogHybridStorage()
{
  destroy();
}

int ObLogHybridStorage::init(
  const uint64_t tenant_id,
  const int64_t palf_id,
  const palf::LSN &start_lsn,
  const int64_t suggested_max_read_buffer_size,
  ObLogExternalStorageHandler *ext_handler)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogHybridStorage has inited", KPC(this));
  } else if (OB_FAIL(shared_storage_.init(tenant_id, palf_id, start_lsn, suggested_max_read_buffer_size, ext_handler))) {
    CLOG_LOG(WARN, "init shared_storage failed", KPC(this), K(tenant_id), K(palf_id), K(start_lsn),
             KP(ext_handler));
  } else if (OB_FAIL(local_storage_.init(tenant_id, palf_id))) {
    CLOG_LOG(WARN, "init local_storage failed", KPC(this), K(tenant_id), K(palf_id), K(start_lsn),
             KP(ext_handler));
  } else {
    is_inited_ = true;
    CLOG_LOG(TRACE, "init hybrid storage success", KPC(this), K(tenant_id), K(palf_id), K(start_lsn),
             KP(ext_handler));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObLogHybridStorage::destroy()
{
  CLOG_LOG(TRACE, "destroy ObLogSharedStorage", KPC(this));
  is_inited_ = false;
  local_storage_.destroy();
  shared_storage_.destroy();
}

int ObLogHybridStorage::pread(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogHybridStorage not inited", K(lsn), K(in_read_size), K(read_buf));
  } else if (!lsn.is_valid()
             || 0 >= in_read_size
             || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(lsn), K(in_read_size), K(read_buf));
  } else if (OB_FAIL(pread_impl_(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "pread_impl_ failed", K(lsn), K(in_read_size), K(read_buf));
  } else {
    CLOG_LOG(TRACE, "pread success", K(lsn), K(in_read_size), K(read_buf), K(out_read_size));
  }
  return ret;
}

int ObLogHybridStorage::get_file_end_lsn(palf::GetFileEndLSN &function)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogHybridStorage not inited");
  } else {
    ret = local_storage_.get_file_end_lsn(function);
  }
  return ret;
}

int ObLogHybridStorage::get_access_mode_version(palf::GetModeVersion &function)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogHybridStorage not inited");
  } else {
    ret = local_storage_.get_access_mode_version(function);
  }
  return ret;
}

int ObLogHybridStorage::pread_impl_(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  // read from local storage first
  // read from shared storage only when read from local storage return OB_ERR_OUT_OF_LOWER_BOUND
  if (OB_FAIL(read_from_local_storage_(lsn, in_read_size, read_buf, out_read_size, io_ctx))
      && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    CLOG_LOG(WARN, "read_from_local_storage_ failed", KPC(this), K(lsn), K(in_read_size), K(read_buf)); 
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret 
             && OB_FAIL(read_from_shared_storage_(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "read_from_shared_storage_ failed", KPC(this), K(lsn), K(in_read_size), K(read_buf)); 
    ret = convert_ret_code_(ret);
  } else {
    CLOG_LOG(TRACE, "pread_impl_ success", KPC(this), K(lsn), K(in_read_size), K(read_buf)); 
  }
  return ret;
}

int ObLogHybridStorage::read_from_local_storage_(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t cost_ts = 0;
  if (OB_FAIL(local_storage_.pread(lsn, in_read_size, read_buf, out_read_size, io_ctx))
      && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    CLOG_LOG(WARN, "pread from local_storage failed", KPC(this), K(lsn), K(in_read_size), K(read_buf));
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
  } else if (FALSE_IT(cost_ts = ObTimeUtility::current_time() - start_ts)) {
  } else if (FALSE_IT(local_storage_read_size_.stat(out_read_size))) {
  } else if (FALSE_IT(local_storage_read_cost_.stat(cost_ts))) {
  } else if (FALSE_IT(start_ts = ObTimeUtility::current_time())) {
  } else {
    // release memory
    shared_storage_.reset();
    CLOG_LOG(TRACE, "read_from_local_storage_ success", K(ret), KPC(this), K(lsn), K(in_read_size), K(read_buf),
             K(out_read_size)); 
  }
  CLOG_LOG(TRACE, "read_from_local_storage_ finish", K(ret), KPC(this), K(lsn), K(in_read_size), K(read_buf),
           K(out_read_size)); 
  return ret;
}

int ObLogHybridStorage::read_from_shared_storage_(
  const palf::LSN &lsn,
  const int64_t in_read_size,
  palf::ReadBuf &read_buf,
  int64_t &out_read_size,
  palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t cost_ts = 0;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND; 
    CLOG_LOG(TRACE, "not enable shared storage");
  } else if (OB_FAIL(shared_storage_.pread(lsn, in_read_size, read_buf, out_read_size, io_ctx))) {
    CLOG_LOG(WARN, "pread from shared_storage failed", KPC(this), K(lsn), K(in_read_size), K(read_buf));
    shared_storage_.reset();
  } else if (FALSE_IT(cost_ts = ObTimeUtility::current_time() - start_ts)) {
  } else if (FALSE_IT(shared_storage_read_size_.stat(out_read_size))) {
  } else if (FALSE_IT(shared_storage_read_cost_.stat(cost_ts))) {
  } else {
    CLOG_LOG(TRACE, "read_from_shared_storage_ success", K(ret), KPC(this), K(lsn), K(in_read_size), K(read_buf),
             K(out_read_size));
  }
  return ret;
}

int ObLogHybridStorage::convert_ret_code_(const int ret_code)
{
  int ret = OB_SUCCESS;
  switch (ret_code) {
    // nowdays, we assume that file on shared storage has been recycled when read
    // shared storage return OB_NO_SUCH_FILE_OR_DIRECTORY
    case OB_NO_SUCH_FILE_OR_DIRECTORY:
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  };
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase
