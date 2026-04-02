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
#include "ob_shared_log_upload_handler.h"
namespace oceanbase
{
using namespace palf;
namespace logservice

{
void LogUploadCtx::reset()
{
  has_file_on_ss_ = false;
  max_block_id_on_ss_ = LOG_INVALID_BLOCK_ID;
  start_block_id_ = LOG_INVALID_BLOCK_ID;
  end_block_id_ = LOG_INVALID_BLOCK_ID;
}

bool LogUploadCtx::is_valid() const
{
  return (is_valid_block_id(start_block_id_) &&
          is_valid_block_id(end_block_id_) &&
          start_block_id_ <= end_block_id_
          && (!has_file_on_ss_ || (has_file_on_ss_ && (LOG_INVALID_BLOCK_ID != max_block_id_on_ss_) && (start_block_id_ > max_block_id_on_ss_))));
}

bool LogUploadCtx::operator==(const LogUploadCtx &ctx) const
{
  return has_file_on_ss_ == ctx.has_file_on_ss_
         && max_block_id_on_ss_ == ctx.max_block_id_on_ss_
         && start_block_id_ == ctx.start_block_id_
         && end_block_id_ == ctx.end_block_id_;
}

int LogUploadCtx::after_upload_file(const block_id_t uploaded_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(LOG_INVALID_BLOCK_ID == uploaded_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(uploaded_block_id), KPC(this));
  } else if (OB_UNLIKELY(uploaded_block_id != start_block_id_ || uploaded_block_id >= end_block_id_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected uploaded_block_id", K(uploaded_block_id), KPC(this));
  } else {
    has_file_on_ss_ = true;
    max_block_id_on_ss_ = uploaded_block_id;
    start_block_id_ = uploaded_block_id + 1;
  }
  return ret;
}

bool LogUploadCtx::need_locate_upload_range() const
{
  bool b_ret = true;
  if (is_valid() && (start_block_id_ < end_block_id_)) {
    b_ret = false;
  }
  return b_ret;
}

int LogUploadCtx::update(const LogUploadCtx &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(other), KPC(this));
  } else {
    if ((!is_valid()) || other.start_block_id_ > start_block_id_ || other.end_block_id_ > end_block_id_) {
      *this = other;
    }
  }
  return ret;
}

int LogUploadCtx::get_next_block_to_upload(block_id_t &to_upload_block_id) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (start_block_id_ == end_block_id_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (start_block_id_ < end_block_id_) {
    to_upload_block_id = start_block_id_;
  }
  return ret;
}

ObSharedLogUploadHandler::ObSharedLogUploadHandler()
{
  reset();
}
ObSharedLogUploadHandler::~ObSharedLogUploadHandler()
{
  reset();
}

void ObSharedLogUploadHandler::reset()
{
  is_inited_ = false;
  ls_id_.reset();
  log_upload_ctx_.reset();
}
int ObSharedLogUploadHandler::init(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(id));
  } else {
    log_upload_ctx_.reset();
    ls_id_ = id;
    is_inited_ = true;
  }

  return ret;
}

int ObSharedLogUploadHandler::get_log_upload_ctx(LogUploadCtx &ctx) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ctx = log_upload_ctx_;
  }
  return ret;
}

int ObSharedLogUploadHandler::update_log_upload_ctx(const LogUploadCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K_(ls_id), K(ctx));
  } else if (OB_FAIL(log_upload_ctx_.update(ctx))) {
    PALF_LOG(WARN, "failed to update log_upload_ctx", K_(ls_id), K(ctx), K(log_upload_ctx_));
  } else {
    PALF_LOG(TRACE, "update_log_upload_ctx", K_(ls_id), K(ctx), K(log_upload_ctx_));

  }
  return ret;
}

int ObSharedLogUploadHandler::after_upload_file(const block_id_t uploaded_block_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(LOG_INVALID_BLOCK_ID == uploaded_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K_(ls_id), K(uploaded_block_id));
  } else if (OB_FAIL(log_upload_ctx_.after_upload_file(uploaded_block_id))) {
    PALF_LOG(WARN, "failed to update_start_block_id", K_(ls_id), K(uploaded_block_id));
  } else {/*do nothing*/}
  return ret;
}

}
}
