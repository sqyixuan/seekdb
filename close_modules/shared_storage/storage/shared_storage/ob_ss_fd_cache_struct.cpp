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

#include "ob_ss_fd_cache_struct.h"
#include "share/ob_io_device_helper.h"
#include "share/ob_ss_file_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;

/********************************** ObSSFdCacheKey *********************************/
ObSSFdCacheKey::ObSSFdCacheKey() : macro_id_()
{
}

ObSSFdCacheKey::ObSSFdCacheKey(const MacroBlockId &macro_id) : macro_id_(macro_id)
{
}

ObSSFdCacheKey::ObSSFdCacheKey(const ObSSFdCacheKey &other)
{
  *this = other;
}

void ObSSFdCacheKey::reset()
{
  macro_id_.reset();
}

bool ObSSFdCacheKey::is_valid() const
{
  return macro_id_.is_valid();
}

uint64_t ObSSFdCacheKey::hash() const
{
  uint64_t hash_val = macro_id_.hash();
  return hash_val;
}

int ObSSFdCacheKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

ObSSFdCacheKey &ObSSFdCacheKey::operator=(const ObSSFdCacheKey &other)
{
  if (this != &other) {
    macro_id_ = other.macro_id_;
  }
  return *this;
}

bool ObSSFdCacheKey::operator==(const ObSSFdCacheKey &other) const
{
  return (macro_id_ == other.macro_id_);
}

bool ObSSFdCacheKey::operator!=(const ObSSFdCacheKey &other) const
{
  return !(*this == other);
}


/********************************** ObSSFdCacheNode *********************************/
ObSSFdCacheNode::ObSSFdCacheNode()
  : key_(), fd_(OB_INVALID_FD), ref_cnt_(0), timestamp_us_(OB_INVALID_TIMESTAMP)
{
}

ObSSFdCacheNode::~ObSSFdCacheNode()
{
  reset();
}

bool ObSSFdCacheNode::is_valid() const
{
  return (key_.is_valid() && (OB_INVALID_FD != fd_) && (OB_INVALID_TIMESTAMP != timestamp_us_));
}

void ObSSFdCacheNode::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

int64_t ObSSFdCacheNode::dec_ref()
{
  return ATOMIC_SAF(&ref_cnt_, 1);
}

int64_t ObSSFdCacheNode::get_ref() const
{
  return ATOMIC_LOAD(&ref_cnt_);
}

void ObSSFdCacheNode::reset()
{
  int ret = OB_SUCCESS;
  if (ref_cnt_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ref count > 0 when reset", KR(ret), KPC(this));
  }
  key_.reset();
  fd_ = OB_INVALID_FD;
  ref_cnt_ = 0;
  timestamp_us_ = OB_INVALID_TIMESTAMP;
}


/********************************** ObSSFdCacheHandle *********************************/
ObSSFdCacheHandle::ObSSFdCacheHandle() : fd_cache_node_(nullptr), fd_cache_node_pool_(nullptr)
{
}

ObSSFdCacheHandle::~ObSSFdCacheHandle()
{
  reset();
}

void ObSSFdCacheHandle::reset()
{
  if (OB_NOT_NULL(fd_cache_node_)) {
    if (0 == fd_cache_node_->dec_ref()) {
      close_fd_and_free_node();
    }
  }
  fd_cache_node_ = nullptr;
  fd_cache_node_pool_ = nullptr;
}

void ObSSFdCacheHandle::close_fd_and_free_node()
{
  int ret = OB_SUCCESS;
  // 1. close fd
  int fd = get_fd();
  if (OB_LIKELY(OB_INVALID_FD != fd)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObSSFileUtil::close(fd))) {
      LOG_ERROR("fail to close fd", KR(tmp_ret), K(fd));
    }
  }

  // 2. free memory of fd_cache_node
  fd_cache_node_->reset();
  if (OB_NOT_NULL(fd_cache_node_pool_)) {
    fd_cache_node_pool_->free(fd_cache_node_);
  } else {
    int tmp_ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fd cache node pool is null, cannot free fd cache node", KR(tmp_ret));
  }
}

int ObSSFdCacheHandle::set_fd_cache_node(
    ObSSFdCacheNode *fd_cache_node,
    ObSmallObjPool<ObSSFdCacheNode> *fd_cache_node_pool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fd_cache_node) || OB_ISNULL(fd_cache_node_pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fd cache node or fd cache node pool is null", KR(ret), KP(fd_cache_node),
             KP(fd_cache_node_pool));
  } else if (OB_UNLIKELY(!fd_cache_node->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fd cache node is invalid", KR(ret), KPC(fd_cache_node));
  } else if (OB_UNLIKELY(fd_cache_node->get_ref() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fd cache node ref cnt should be zero", KR(ret), KPC(fd_cache_node));
  } else {
    reset();
    fd_cache_node_ = fd_cache_node;
    fd_cache_node_->inc_ref();
    fd_cache_node_pool_ = fd_cache_node_pool;
  }
  return ret;
}

int ObSSFdCacheHandle::get_fd() const
{
  return ((nullptr == fd_cache_node_) ? OB_INVALID_FD : fd_cache_node_->fd_);
}

int ObSSFdCacheHandle::assign(const ObSSFdCacheHandle &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_UNLIKELY(this->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not assign a valid handle", KR(ret), KPC(this));
    } else if (OB_LIKELY(other.is_valid())) {
      fd_cache_node_ = other.fd_cache_node_;
      fd_cache_node_->inc_ref();
      fd_cache_node_pool_ = other.fd_cache_node_pool_;
    }
  }
  return ret;
}

void ObSSFdCacheHandle::update_timestamp_us()
{
  if (OB_NOT_NULL(fd_cache_node_)) {
    fd_cache_node_->timestamp_us_ = ObTimeUtility::fast_current_time();
  }
}

} // namespace storage
} // namespace oceanbase
