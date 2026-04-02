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

#include "storage/shared_storage/prewarm/ob_replica_prewarm_struct.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;

/**********************ObReplicaPrewarmMicroBlockProducer************************/
ObReplicaPrewarmMicroBlockProducer::ObReplicaPrewarmMicroBlockProducer()
  : is_inited_(false), micro_metas_(), process_idx_(-1), prefetch_idx_(-1), small_buf_allocator_(),
    big_buf_allocator_(), obj_handles_()
{
}

ObReplicaPrewarmMicroBlockProducer::~ObReplicaPrewarmMicroBlockProducer()
{
  reset_io_resources();
}

int ObReplicaPrewarmMicroBlockProducer::init(
    const ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_metas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    ObMemAttr attr(MTL_ID(), "SSRepPrewarmPrd");
    micro_metas_.set_attr(attr);
    small_buf_allocator_.set_attr(attr);
    big_buf_allocator_.set_attr(attr);
    if (OB_FAIL(micro_metas_.assign(micro_metas))) {
      LOG_WARN("fail to assign", KR(ret), K(micro_metas));
    } else if (OB_FAIL(alloc_small_io_bufs())) {
      LOG_WARN("fail to alloc small io bufs", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockProducer::get_next_micro_block(
    ObSSMicroBlockCacheKeyMeta &micro_meta,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  bool is_get = false;
  while (OB_SUCC(ret) && !is_get) {
    if (OB_FAIL(inner_get_next_micro_block(micro_meta, data, is_get))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to inner get next micro block", KR(ret));
      }
    }
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockProducer::inner_get_next_micro_block(
    ObSSMicroBlockCacheKeyMeta &micro_meta,
    ObBufferReader &data,
    bool &is_get)
{
  int ret = OB_SUCCESS;
  is_get = false;
  ++process_idx_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica prewarm micro block producer not init", KR(ret));
  } else if (OB_UNLIKELY((process_idx_ < 0) || (process_idx_ > micro_metas_.count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected micro key idx", KR(ret), K_(process_idx), "micro_meta_count", micro_metas_.count());
  } else if (process_idx_ == micro_metas_.count()) {
    ret = OB_ITER_END;
    LOG_INFO("get next micro block end");
  } else if (OB_FAIL(prefetch_if_need())) {
    LOG_WARN("fail to prefetch if need", KR(ret), K_(process_idx), K_(prefetch_idx));
  } else if (OB_UNLIKELY(process_idx_ > prefetch_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("process idx should not be larger than prefetch idx", KR(ret), K_(process_idx), K_(prefetch_idx));
  } else {
    ObSSMicroBlockCacheKeyMeta &cur_micro_meta = micro_metas_.at(process_idx_);
    const int64_t handle_idx = process_idx_ % PREFETCH_PARALLELISM;
    ObStorageObjectHandle &obj_handle = obj_handles_[handle_idx];
    if (OB_UNLIKELY(!obj_handle.is_valid())) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle should not be invalid", KR(ret), K(handle_idx), K(obj_handle));
    } else if (OB_FAIL(obj_handle.wait())) {
      LOG_WARN("fail to wait", KR(ret), K(obj_handle));
    } else if (0 == obj_handle.get_data_size()) {
      LOG_INFO("micro block not exist in micro cache", "micro_key", cur_micro_meta.micro_key_);
    } else if (OB_FAIL(micro_meta.assign(cur_micro_meta))) {
      LOG_WARN("fail to assign", KR(ret), K(cur_micro_meta));
    } else {
      data.assign(obj_handle.get_buffer(), micro_meta.data_size_);
      is_get = true;
    }
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockProducer::alloc_small_io_bufs()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < PREFETCH_PARALLELISM); ++i) {
    if (OB_ISNULL(small_io_bufs_[i] = static_cast<char *>(small_buf_allocator_.alloc(OB_DEFAULT_SSTABLE_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for small io buf", KR(ret));
    }
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockProducer::get_io_buf(
    const int64_t buf_size,
    ObArenaAllocator &allocator,
    char *&io_buf)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf size", KR(ret), K(buf_size));
  } else if (buf_size <= OB_DEFAULT_SSTABLE_BLOCK_SIZE) {
    io_buf = small_io_bufs_[prefetch_idx_ % PREFETCH_PARALLELISM];
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(buf_size));
  } else {
    io_buf = buf;
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockProducer::prefetch_if_need()
{
  int ret = OB_SUCCESS;
  const int64_t micro_meta_cnt = micro_metas_.count();
  if ((process_idx_ > prefetch_idx_) && (prefetch_idx_ < (micro_meta_cnt - 1))) {
    reset_io_resources();
    ObSSMicroCache *micro_cache = nullptr;
    if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro cache is null", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < PREFETCH_PARALLELISM) && (prefetch_idx_ < (micro_meta_cnt - 1)); ++i) {
      ++prefetch_idx_;
      const ObSSMicroBlockCacheKeyMeta &cur_micro_meta = micro_metas_.at(prefetch_idx_);
      ObIOInfo io_info;
      io_info.tenant_id_ = MTL_ID();
      io_info.timeout_us_ = DEFAULT_IO_WAIT_TIME_MS * 1000L;
      io_info.flag_.set_read();
      io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      io_info.size_ = cur_micro_meta.data_size_;
      const int64_t handle_idx = prefetch_idx_ % PREFETCH_PARALLELISM;
      ObStorageObjectHandle &obj_handle = obj_handles_[handle_idx];
      if (OB_UNLIKELY(obj_handle.is_valid())) { // defence
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("object handle already in use", KR(ret), K(handle_idx));
      } else if (OB_FAIL(get_io_buf(io_info.size_, big_buf_allocator_, io_info.user_data_buf_))) {
        LOG_WARN("fail to get io buf", KR(ret), "buf_size", io_info.size_);
      } else if (OB_FAIL(micro_cache->get_cached_micro_block(cur_micro_meta.micro_key_, io_info,
                         obj_handle, ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE))) {
        LOG_WARN("fail to get cached micro block", KR(ret), "micro_key", cur_micro_meta.micro_key_, K(io_info));
      }
    }
  }
  return ret;
}

void ObReplicaPrewarmMicroBlockProducer::reset_io_resources()
{
  for (int64_t i = 0; i < PREFETCH_PARALLELISM; ++i) {
    obj_handles_[i].reset();
  }
  // Note: only clear big_buf_allocator_ and do not clear small_buf_allocator_, as small_io_bufs_
  // allocated by small_buf_allocator_ will be reused. clear small_buf_allocator_ when
  // ObReplicaPrewarmMicroBlockProducer destruct.
  big_buf_allocator_.clear();
}


/***********************ObReplicaPrewarmMicroBlockReader*************************/
ObReplicaPrewarmMicroBlockReader::ObReplicaPrewarmMicroBlockReader()
  : is_inited_(false), handle_(), bandwidth_throttle_(nullptr), data_buffer_(), rpc_buffer_(),
    rpc_buffer_parse_pos_(0), allocator_(), last_send_time_(0), data_size_(0)
{
  allocator_.set_attr(ObMemAttr(MTL_ID(), "SSRepPrewarmRd"));
}

ObReplicaPrewarmMicroBlockReader::~ObReplicaPrewarmMicroBlockReader()
{
}

int ObReplicaPrewarmMicroBlockReader::init(
    const ObAddr &addr,
    const uint64_t tenant_id,
    const ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_metas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!addr.is_valid() || !is_valid_tenant_id(tenant_id) || micro_metas.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(addr), K(tenant_id), "micro_meta_empty", micro_metas.empty());
  } else if (OB_ISNULL(GCTX.storage_rpc_proxy_) || OB_ISNULL(GCTX.bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc proxy or bandwidth throttle is null", KR(ret), "storage_rpc_proxy",
             GCTX.storage_rpc_proxy_, "bandwidth_throttle", GCTX.bandwidth_throttle_);
  } else {
    const int64_t rpc_timeout = ObStorageRpcProxy::STREAM_RPC_TIMEOUT;
    ObStorageRpcProxy &storage_rpc_proxy = *(GCTX.storage_rpc_proxy_);
    ObSSLSFetchMicroBlockArg arg;
    arg.tenant_id_ = tenant_id;
    arg.micro_metas_.set_attr(ObMemAttr(tenant_id, "SSRepPrewarmRd"));
    if (OB_FAIL(arg.micro_metas_.assign(micro_metas))) {
      LOG_WARN("fail to assign micro keys", KR(ret));
    } else if (OB_FAIL(alloc_buffers())) {
      LOG_WARN("fail to alloc buffers", KR(ret));
    } else if (OB_UNLIKELY(arg.get_serialize_size() > OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("rpc arg must not larger than packet size", KR(ret), "arg_size", arg.get_serialize_size());
    } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_3_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("fetch replica prewarm micro block is not supported", KR(ret));
    } else if (OB_FAIL(storage_rpc_proxy
                       .to(addr)
                       .by(arg.tenant_id_)
                       .timeout(rpc_timeout)
                       .dst_cluster_id(GCONF.cluster_id)
                       .ratelimit(true)
                       .bg_flow(ObRpcProxy::BACKGROUND_FLOW)
                       .group_id(OBCG_STORAGE_STREAM)
                       .fetch_replica_prewarm_micro_block(arg, rpc_buffer_, handle_))) {
      LOG_WARN("fail to send fetch replica prewarm micro block rpc", KR(ret), K(arg));
    } else {
      bandwidth_throttle_ = GCTX.bandwidth_throttle_;
      rpc_buffer_parse_pos_ = 0;
      last_send_time_ = ObTimeUtility::current_time();
      data_size_ = rpc_buffer_.get_position();
      is_inited_ = true;
      LOG_INFO("get first package fetch replica prewarm micro block", K_(rpc_buffer));
    }
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockReader::get_next_micro_block(
    ObSSMicroBlockCacheKeyMeta &micro_meta,
    ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch next buffer if need", KR(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer_.get_data(),
                                           rpc_buffer_.get_position(),
                                           rpc_buffer_parse_pos_,
                                           micro_meta))) {
    LOG_WARN("fail to decode micro meta", KR(ret), K_(rpc_buffer), K_(rpc_buffer_parse_pos));
  } else if (OB_FAIL(data_buffer_.set_pos(0))) {
    LOG_WARN("fail to set data buffer pos", KR(ret), K_(data_buffer));
  } else {
    while (OB_SUCC(ret)) {
      if (data_buffer_.length() > micro_meta.data_size_) {
        ret = OB_ERR_SYS;
        LOG_WARN("data buffer must not larger than size", KR(ret), K_(data_buffer), K(micro_meta));
      } else if (data_buffer_.length() == micro_meta.data_size_) {
        data.assign(data_buffer_.data(), data_buffer_.capacity(), data_buffer_.length());
        LOG_DEBUG("get_next_macro_block", K_(rpc_buffer), K_(rpc_buffer_parse_pos), K(micro_meta));
        break;
      } else if (OB_FAIL(fetch_next_buffer_if_need())) {
        LOG_WARN("fail to fetch next buffer if need", KR(ret));
      } else {
        int64_t need_size = micro_meta.data_size_ - data_buffer_.length();
        int64_t rpc_remain_size = rpc_buffer_.get_position() - rpc_buffer_parse_pos_;
        int64_t copy_size = std::min(need_size, rpc_remain_size);
        if (copy_size > data_buffer_.remain()) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_ERROR("data buffer is not enough, micro block data must not larger than data buffer",
                    KR(ret), K(copy_size), K_(data_buffer), "remain", data_buffer_.remain());
        } else {
          LOG_DEBUG("copy rpc buffer to data buffer", K(need_size), K(rpc_remain_size), K(copy_size),
                    "size", micro_meta.data_size_, K_(rpc_buffer_parse_pos));
          MEMCPY(data_buffer_.current(), rpc_buffer_.get_data() + rpc_buffer_parse_pos_, copy_size);
          if (OB_FAIL(data_buffer_.advance(copy_size))) {
            LOG_ERROR("BUG here! fail to advance data buffer", KR(ret), "remain",
                      data_buffer_.remain(), K(copy_size));
          } else {
            rpc_buffer_parse_pos_ += copy_size;
          }
        }
      }
    }
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockReader::alloc_buffers()
{
  int ret = OB_SUCCESS;
  char *rpc_buf = nullptr;
  char *data_buf = nullptr;
  if (OB_ISNULL(rpc_buf = static_cast<char *>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc rpc buf", KR(ret));
  } else if (!rpc_buffer_.set_data(rpc_buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to set rpc buffer", KR(ret));
  } else if (OB_ISNULL(data_buf = static_cast<char *>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc data buf", KR(ret));
  } else {
    data_buffer_.assign(data_buf, OB_DEFAULT_MACRO_BLOCK_SIZE);
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockReader::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(fetch_next_buffer())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch next buffer", KR(ret));
    }
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockReader::fetch_next_buffer()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(do_fetch_next_buffer_if_need(*bandwidth_throttle_, rpc_buffer_,
                     rpc_buffer_parse_pos_, handle_, last_send_time_, data_size_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to do fetch next buffer if need", KR(ret));
    }
  }
  return ret;
}


/***********************ObReplicaPrewarmMicroBlockWriter*************************/
ObReplicaPrewarmMicroBlockWriter::ObReplicaPrewarmMicroBlockWriter(volatile bool &is_stop)
  : is_inited_(false), is_stop_(is_stop), tenant_id_(OB_INVALID_TENANT_ID), reader_(nullptr)
{
}

ObReplicaPrewarmMicroBlockWriter::~ObReplicaPrewarmMicroBlockWriter()
{
}

int ObReplicaPrewarmMicroBlockWriter::init(
    const uint64_t tenant_id,
    ObReplicaPrewarmMicroBlockReader *reader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(reader) || OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(reader), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    reader_ = reader;
    is_inited_ = true;
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockWriter::process()
{
  int ret = OB_SUCCESS;
  ObBufferReader data;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("writer is not init", KR(ret));
  } else {
    MTL_SWITCH(tenant_id_) {
      int64_t micro_cnt = 0;
      while (OB_SUCC(ret) && !is_stop_) {
        ObSSMicroBlockCacheKeyMeta micro_meta;
        if (OB_FAIL(reader_->get_next_micro_block(micro_meta, data))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next micro block set", KR(ret));
          } else {
            ret = OB_SUCCESS;
            LOG_INFO("get micro block end", K(micro_cnt));
            break;
          }
        } else if (OB_FAIL(write_micro_block_cache(micro_meta, data))) {
          LOG_WARN("fail to write micro block cache", KR(ret), K(index));
        } else {
          micro_cnt++;
        }
      }
    }
  }
  return ret;
}

int ObReplicaPrewarmMicroBlockWriter::write_micro_block_cache(
    const ObSSMicroBlockCacheKeyMeta &micro_meta,
    const blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  ObSEArray<ObSSMicroBlockCacheKey, 1> micro_keys;
  micro_keys.set_attr(ObMemAttr(MTL_ID(), "SSRepPrewarmRd"));
  if (OB_UNLIKELY(!micro_meta.is_valid() ||  (nullptr == data.data()) || (data.length() < 0) ||
      (data.length() > data.capacity() || (data.length() < micro_meta.data_size_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_meta), K(data));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache should not be null", KR(ret), K_(tenant_id));
  } else {
    const uint32_t crc = static_cast<uint32_t>(ob_crc64(data.data(), micro_meta.data_size_));
    if (OB_UNLIKELY(crc != micro_meta.data_crc_)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("micro data checksum error", KR(ret), K(crc), K(micro_meta));
    } else if (OB_FAIL(micro_cache->add_micro_block_cache(micro_meta.micro_key_, data.data(),
                       micro_meta.data_size_, ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE))) {
      LOG_WARN("fail to add micro block cache", KR(ret), K(micro_meta));
    } else if (micro_meta.is_in_l1_) {
      // do nothing
    } else if (OB_FAIL(micro_keys.push_back(micro_meta.micro_key_))) {
      LOG_WARN("fail to push back micro key", KR(ret), "micro_key", micro_meta.micro_key_);
    } else if (OB_FAIL(micro_cache->update_micro_block_heat(micro_keys, true/*need to transfer T1 -> T2*/,
                                    true/*need to update access_time to current_time*/))) {
      LOG_WARN("fail to update micro block heat", KR(ret), K(micro_keys));
    }
    ret = OB_SUCCESS; // Note: ignore ret about add_micro_block_cache and update_micro_block_heat
  }
  return ret;
}


/****************************ObReplicaPrewarmHandler******************************/
ObReplicaPrewarmHandler::ObReplicaPrewarmHandler()
  : is_inited_(false), tg_id_(INVALID_TG_ID)
{
}

ObReplicaPrewarmHandler::~ObReplicaPrewarmHandler()
{
}

int ObReplicaPrewarmHandler::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReplicaPrewarmHdlr, tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT failed", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObReplicaPrewarmHandler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_FAIL(TG_SET_ADAPTIVE_THREAD(tg_id_, MIN_THREAD_COUNT, MAX_THREAD_COUNT))) {
    LOG_WARN("TG_SET_ADAPTIVE_THREAD failed", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed", KR(ret), K_(tg_id));
  } else {
    LOG_INFO("succ to start replica prewarm handler", K_(tg_id));
  }
  return ret;
}

void ObReplicaPrewarmHandler::stop()
{
  LOG_INFO("replica prewarm handler start to stop", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
  }
  LOG_INFO("replica prewarm handler finish to stop", K_(tg_id));
}

void ObReplicaPrewarmHandler::wait()
{
  LOG_INFO("replica prewarm handler start to wait", K_(tg_id));
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
  LOG_INFO("replica prewarm handler finish to wait", K_(tg_id));
}

void ObReplicaPrewarmHandler::destroy()
{
  LOG_INFO("replica prewarm handler start to destroy");
  if (OB_LIKELY(INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  is_inited_ = false;
  LOG_INFO("replica prewarm handler finish to destroy");
}

int ObReplicaPrewarmHandler::push_task(
    volatile bool &is_stop,
    const ObAddr &addr,
    const uint64_t tenant_id,
    const int64_t ls_id,
    const ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_metas,
    int64_t &fetch_micro_key_workers)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "SSRepPreTask");
  ObReplicaPrewarmTask *replica_prewarm_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_UNLIKELY(!addr.is_valid() || !is_valid_tenant_id(tenant_id)
                         || (ObLSID::INVALID_LS_ID == ls_id) || micro_metas.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(addr), K(tenant_id), K(ls_id), "micro_meta_empty",
             micro_metas.empty());
  } else if (OB_ISNULL(replica_prewarm_task = static_cast<ObReplicaPrewarmTask *>(
                                              ob_malloc(sizeof(ObReplicaPrewarmTask), attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for replica prewarm task", KR(ret), "size",
             sizeof(ObReplicaPrewarmTask), K(addr), K(tenant_id), K(ls_id));
  } else if (FALSE_IT(replica_prewarm_task = new (replica_prewarm_task) ObReplicaPrewarmTask(
                                             is_stop, fetch_micro_key_workers))) {
  } else if (OB_FAIL(replica_prewarm_task->init(addr, tenant_id, ls_id, micro_metas))) {
    LOG_WARN("fail to init replica prewarm task", KR(ret), K(addr), K(tenant_id), K(ls_id), K(micro_metas));
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, replica_prewarm_task))) {
    LOG_WARN("TG_PUSH_TASK failed", KR(ret), KPC(replica_prewarm_task));
  }

  // free memory
  if (OB_FAIL(ret) && OB_NOT_NULL(replica_prewarm_task)) {
    replica_prewarm_task->~ObReplicaPrewarmTask();
    ob_free(replica_prewarm_task);
    replica_prewarm_task = nullptr;
  }
  return ret;
}

void ObReplicaPrewarmHandler::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObReplicaPrewarmTask *replica_prewarm_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    replica_prewarm_task = static_cast<ObReplicaPrewarmTask *>(task);
    if (OB_FAIL(replica_prewarm_task->do_task())) {
      LOG_WARN("fail to do task", KR(ret), KPC(replica_prewarm_task));
    }
  }
  // free memory
  if (OB_NOT_NULL(replica_prewarm_task)) {
    replica_prewarm_task->dec_fetch_micro_block_workers();
    replica_prewarm_task->~ObReplicaPrewarmTask();
    ob_free(replica_prewarm_task);
    replica_prewarm_task = nullptr;
  }
}

void ObReplicaPrewarmHandler::handle_drop(void *task)
{
  int ret = OB_SUCCESS;
  ObReplicaPrewarmTask *replica_prewarm_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    // thread has set stop. do not process left tasks, just free memory directly
    replica_prewarm_task = static_cast<ObReplicaPrewarmTask *>(task);
    LOG_INFO("drop replica prewarm task", KPC(replica_prewarm_task));
    replica_prewarm_task->dec_fetch_micro_block_workers();
    replica_prewarm_task->~ObReplicaPrewarmTask();
    ob_free(replica_prewarm_task);
    replica_prewarm_task = nullptr;
  }
}

/****************************ObReplicaPrewarmTask******************************/
ObReplicaPrewarmTask::ObReplicaPrewarmTask(
    volatile bool &is_stop,
    int64_t &fetch_micro_block_workers)
  : is_inited_(false), is_stop_(is_stop), addr_(), tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(ObLSID::INVALID_LS_ID), micro_metas_(),
    fetch_micro_block_workers_(fetch_micro_block_workers), trace_id_()
{
}

ObReplicaPrewarmTask::~ObReplicaPrewarmTask()
{
}

int ObReplicaPrewarmTask::init(
    const ObAddr &addr,
    const uint64_t tenant_id,
    const int64_t ls_id,
    const ObIArray<ObSSMicroBlockCacheKeyMeta> &micro_metas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!addr.is_valid() || !is_valid_tenant_id(tenant_id)
                         || (ObLSID::INVALID_LS_ID == ls_id) || micro_metas.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(addr), K(tenant_id), K(ls_id), "micro_meta_empty",
             micro_metas.empty());
  } else {
    ObMemAttr attr(tenant_id, "SSRepPreTask");
    micro_metas_.set_attr(attr);
    if (OB_FAIL(micro_metas_.assign(micro_metas))) {
      LOG_WARN("fail to assign micro metas", KR(ret), K(addr), K(tenant_id), K(ls_id), K(micro_metas));
    } else {
      addr_ = addr;
      tenant_id_ = tenant_id;
      ls_id_ = ls_id;
      trace_id_ = *ObCurTraceId::get_trace_id();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObReplicaPrewarmTask::do_task()
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard trace_id_guard(trace_id_);
  const int64_t start_us = ObTimeUtility::current_time();
  LOG_INFO("start to do replica prewarm task");
  ObReplicaPrewarmMicroBlockReader *reader = nullptr;
  ObReplicaPrewarmMicroBlockWriter *writer = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica prewarm task is not init", KR(ret));
  } else if (OB_FAIL(get_micro_block_reader(reader))) {
    LOG_WARN("fail to get micro block reader", KR(ret), K_(tenant_id), K_(ls_id), K_(micro_metas));
  } else if (OB_FAIL(get_micro_block_writer(reader, writer))) {
    LOG_WARN("fail to get micro block writer", KR(ret), KP(reader), K_(tenant_id), K_(ls_id));
  } else if (OB_ISNULL(writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("writer is null", KR(ret));
  } else if (OB_FAIL(writer->process())) {
    LOG_WARN("fail to process", KR(ret), K_(tenant_id), K_(ls_id));
  }

  if (OB_NOT_NULL(reader)) {
    free_micro_block_reader(reader);
  }
  if (OB_NOT_NULL(writer)) {
    free_micro_block_writer(writer);
  }
  dec_fetch_micro_block_workers();
  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  LOG_INFO("finish to do replica prewarm task", KR(ret), K(cost_us), K_(is_stop), K_(addr),
           K_(tenant_id), K_(ls_id), "micro_meta_cnt", micro_metas_.count());
  return ret;
}

void ObReplicaPrewarmTask::dec_fetch_micro_block_workers()
{
  ATOMIC_DEC(&fetch_micro_block_workers_);
}

int ObReplicaPrewarmTask::get_micro_block_reader(ObReplicaPrewarmMicroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica prewarm task is not init", KR(ret));
  } else {
    ObReplicaPrewarmMicroBlockReader *tmp_reader = nullptr;
    ObMemAttr attr(tenant_id_, "SSRepPreTask");
    void *buf = nullptr;
    if (OB_ISNULL(buf = ob_malloc(sizeof(ObReplicaPrewarmMicroBlockReader), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), "size", sizeof(ObReplicaPrewarmMicroBlockReader),
               K_(tenant_id), K_(ls_id));
    } else if (FALSE_IT(tmp_reader = new (buf) ObReplicaPrewarmMicroBlockReader())) {
    } else if (OB_FAIL(tmp_reader->init(addr_, tenant_id_, micro_metas_))) {
      LOG_WARN("fail to init replica prewarm reader", KR(ret), K_(addr), K_(tenant_id),
               K_(ls_id), K_(micro_metas));
    } else {
      reader = tmp_reader;
      tmp_reader = nullptr;
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(reader)) {
      free_micro_block_reader(reader);
    }
    if (nullptr != tmp_reader) {
      free_micro_block_reader(tmp_reader);
    }
  }
  return ret;
}

int ObReplicaPrewarmTask::get_micro_block_writer(
    ObReplicaPrewarmMicroBlockReader *reader,
    ObReplicaPrewarmMicroBlockWriter *&writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica prewarm task is not init", KR(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reader is null", KR(ret));
  } else {
    ObMemAttr attr(tenant_id_, "SSRepPreTask");
    void *buf = nullptr;
    if (OB_ISNULL(buf = ob_malloc(sizeof(ObReplicaPrewarmMicroBlockWriter), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (FALSE_IT(writer = new (buf) ObReplicaPrewarmMicroBlockWriter(is_stop_))) {
    } else if (OB_FAIL(writer->init(tenant_id_, reader))) {
      LOG_WARN("fail to init micro block writer", KR(ret), K_(tenant_id), KP(reader));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(writer)) {
      free_micro_block_writer(writer);
    }
  }
  return ret;
}

void ObReplicaPrewarmTask::free_micro_block_reader(ObReplicaPrewarmMicroBlockReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    reader->~ObReplicaPrewarmMicroBlockReader();
    ob_free(reader);
    reader = nullptr;
  }
}

void ObReplicaPrewarmTask::free_micro_block_writer(ObReplicaPrewarmMicroBlockWriter *&writer)
{
  if (OB_NOT_NULL(writer)) {
    writer->~ObReplicaPrewarmMicroBlockWriter();
    ob_free(writer);
    writer = nullptr;
  }
}

/****************************ObLSPrewarmManager******************************/
ObLSPrewarmManager::ObLSPrewarmManager()
  : is_inited_(false), is_stop_(false), batch_get_kvcache_key_task_(is_stop_)
{
}

ObLSPrewarmManager::~ObLSPrewarmManager()
{
  destroy();
}

ObLSPrewarmManager &ObLSPrewarmManager::get_instance()
{
  static ObLSPrewarmManager instance_;
  return instance_;
}

int ObLSPrewarmManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls prewarm manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(batch_get_kvcache_key_task_.init())) {
    LOG_WARN("fail to init batch get kvcache key task", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer,
                                 batch_get_kvcache_key_task_,
                                 SCHEDULE_INTERVAL_US,
                                 true/*schedule repeatly*/))) {
    LOG_WARN("fail to schedule batch get kvcache key task", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("succ to init ls prewarm manager", K_(is_inited));
  }
  return ret;
}

void ObLSPrewarmManager::stop()
{
  if (IS_INIT) {
    is_stop_ = true;
    TG_CANCEL_TASK(lib::TGDefIDs::ServerGTimer, batch_get_kvcache_key_task_);
  }
}

void ObLSPrewarmManager::wait()
{
  if (IS_INIT) {
    TG_WAIT_TASK(lib::TGDefIDs::ServerGTimer, batch_get_kvcache_key_task_);
  }
}

void ObLSPrewarmManager::destroy()
{
  if (IS_INIT) {
    stop();
    wait();
    is_inited_ = false;
    batch_get_kvcache_key_task_.destroy();
  }
}

int ObLSPrewarmManager::get_micro_block_cache_keys(ObIArray<blocksstable::ObMicroBlockCacheKey> &keys, bool &full_scan)
{
  int ret = OB_SUCCESS;
  full_scan = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls prewarm manager is not init", KR(ret));
  } else if (OB_FAIL(batch_get_kvcache_key_task_.get_micro_block_cache_keys(keys, full_scan))) {
    LOG_WARN("fail to get micro block cache keys", KR(ret));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
