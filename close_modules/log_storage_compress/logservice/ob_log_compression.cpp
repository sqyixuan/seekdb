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


#include "ob_log_compression.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "src/observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace logservice
{

using namespace common;
using namespace share;
int decompress(const char *in_buf, const int64_t in_size,
               char *out_buf, const int64_t out_buf_size, int64_t &decompressed_len)
{
  int ret = OB_SUCCESS;
  LogCompressedPayload compressed_payload;
  const LogCompressedPayloadHeader &header = compressed_payload.get_header();
  common::ObCompressorType compressor_type = INVALID_COMPRESSOR;
  common::ObCompressor *compressor = NULL;
  int64_t pos = 0;
  if (OB_UNLIKELY(NULL == in_buf || 0 >= in_size || NULL == out_buf || 0 >= out_buf_size)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KP(in_buf), K(in_size), KP(out_buf), K(out_buf_size));
  } else if (OB_FAIL(compressed_payload.deserialize(in_buf, in_size, pos))) {
    CLOG_LOG(WARN, "failed to deserialize ObCompressedLogEntry");
  } else if (FALSE_IT(compressor_type = header.get_compressor_type())) {
  } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type, compressor))) {
    CLOG_LOG(ERROR, "get_compressor failed", K(compressor_type), K(header));
  } else if (OB_ISNULL(compressor)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "compressor is NULL", K(compressor_type), K(header));
  } else if (OB_UNLIKELY(out_buf_size < header.get_original_len())) {
    ret = OB_BUF_NOT_ENOUGH;
    CLOG_LOG(ERROR, "buf is not enough", K(compressor_type), K(header));
  } else if (OB_FAIL(compressor->decompress(compressed_payload.get_data_buf(),
                                            header.get_compressed_data_len(),
                                            out_buf,
                                            header.get_original_len(),
                                            decompressed_len))) {
    CLOG_LOG(ERROR, "failed to decompress", K(compressor_type), K(header));
  } else if (OB_UNLIKELY(decompressed_len != header.get_original_len())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "uncompress len is not expected", K(decompressed_len), K(header));
 } else {/*do nothing*/}
  return ret;
}

LogCompressedPayloadHeader::LogCompressedPayloadHeader()
{
  reset();
}

LogCompressedPayloadHeader::~LogCompressedPayloadHeader()
{
  reset();
}

void LogCompressedPayloadHeader::reset()
{
  version_ = -1;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  original_len_ = -1;
  compressed_len_ = -1;
}

bool LogCompressedPayloadHeader::is_valid() const
{
  return VERSION == version_
  && is_valid_log_compressor_type(static_cast<ObCompressorType>(compressor_type_))
  && (original_len_ > 0) && (compressed_len_ > 0) && (compressed_len_ < original_len_);
}

int LogCompressedPayloadHeader::generate(
    const common::ObCompressorType compressor_type, const int32_t orig_len,
    const int32_t compressed_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_log_compressor_type(compressor_type))
                 || orig_len <= 0 || compressed_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(compressor_type), K(orig_len), K(compressed_len));
  } else {
    version_ = VERSION;
    compressor_type_ = compressor_type;
    original_len_ = orig_len;
    compressed_len_ = compressed_len;
  }
  return ret;
}

DEFINE_SERIALIZE(LogCompressedPayloadHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, new_pos, version_))) {
    PALF_LOG(WARN, "failed to serialize version_", K(this));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, new_pos, compressor_type_))) {
    PALF_LOG(WARN, "failed to serialize compressor_type_", K(this));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, original_len_))) {
    PALF_LOG(WARN, "failed to serialize original_len_", K(this));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, compressed_len_))) {
    PALF_LOG(WARN, "failed to serialize compressed_len_", K(this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogCompressedPayloadHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KP(buf), K(data_len));
  } else if ((OB_FAIL(serialization::decode_i8(buf, data_len, new_pos, &version_)))) {
    PALF_LOG(WARN, "failed to decode version_", KP(buf), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, new_pos, &compressor_type_))) {
    PALF_LOG(WARN, "failed to decode compressor_type", KP(buf), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &original_len_))) {
    PALF_LOG(WARN, "failed to decode original_len", KP(buf), K(data_len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &compressed_len_))) {
    PALF_LOG(WARN, "failed to decode compressed_len_", KP(buf), K(data_len), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogCompressedPayloadHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i8(version_);
  size += serialization::encoded_length_i8(compressor_type_);
  size += serialization::encoded_length_i32(original_len_);
  size += serialization::encoded_length_i32(compressed_len_);
  return size;
}

bool LogCompressedPayload::is_valid() const
{
  return (header_.is_valid() && (NULL != buf_));
}
void LogCompressedPayload::reset()
{
  header_.reset();
  buf_ = NULL;
}

DEFINE_SERIALIZE(LogCompressedPayload)
{
  int ret = OB_SUCCESS;
  const int64_t data_len = get_data_len();
  int64_t new_pos = pos;

  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(buf), K(buf_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(WARN, "failed to serialize LogCompressedPayload", K(buf_len), K(new_pos));
  } else if (buf_len  - new_pos < data_len) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(WARN, "buffer not enough", K(buf_len), K(data_len), K(new_pos));
  } else {
    MEMCPY((buf + new_pos), buf_, data_len);
    pos = new_pos + data_len;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogCompressedPayload)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(buf), K(data_len));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(WARN, "failed to deserialize header_", K(data_len), K(new_pos));
  } else if (data_len  - new_pos < header_.get_compressed_data_len()) {
    ret = OB_BUF_NOT_ENOUGH;
    PALF_LOG(WARN, "buffer is not enough", K(data_len), K(new_pos), K(header_));
  } else {
    buf_ = buf + new_pos;
    pos = new_pos + header_.get_compressed_data_len();
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogCompressedPayload)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  size += get_data_len();
  return size;
}

ObLogCompressorWrapper::ObLogCompressorWrapper()
: id_(-1),
alloc_mgr_(NULL),
compressor_(NULL),
is_refreshing_config_(false),
last_refresh_config_ts_(OB_INVALID_TIMESTAMP)
{
}

ObLogCompressorWrapper::~ObLogCompressorWrapper()
{
  reset();
}

int ObLogCompressorWrapper::init(const int64_t id, common::ObILogAllocator *alloc_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KP(alloc_mgr));
  } else {
    id_ = id;
    alloc_mgr_ = alloc_mgr;
  }
  return ret;
}

void ObLogCompressorWrapper::reset()
{
  id_ = -1;
  alloc_mgr_ = NULL;
  compressor_ = NULL;
  is_refreshing_config_ = false;
  last_refresh_config_ts_ = OB_INVALID_TIMESTAMP;
}

bool ObLogCompressorWrapper::is_valid() const
{
  return NULL != alloc_mgr_;
}

int ObLogCompressorWrapper::compress_payload(const void *buffer,
                                             const int64_t nbytes,
                                             void *&compression_buf,
                                             bool &log_compressed,
                                             const void *&final_buf,
                                             int64_t &final_nbytes)
{
  int ret = OB_SUCCESS;
  ObCompressor *compressor = NULL;
  log_compressed = false;
  int64_t compressed_data_size = 0;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(MTL_ID()));
  } else if (OB_ISNULL(buffer) || OB_UNLIKELY(nbytes <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(MTL_ID()), K(id_), KP(buffer), K(nbytes));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    CLOG_LOG(WARN, "fail to get tenant data version", K(MTL_ID()));
  } else if ((MOCK_DATA_VERSION_4_2_1_5 > data_version) ||
             (DATA_VERSION_4_3_0_0 <= data_version && DATA_VERSION_4_3_1_0 > data_version)) {
    //log storage compress feature is intruduced in 4.2.2.0 and patched to 4.2.1.5 and 4.3.1.0
    //compatibility doc: 
    //do not do compression, log_compressed is kept False
  } else if (NULL != (compressor = get_compressor_())) {
    //need to compress payload
    ObLogBaseHeader base_header;
    int64_t base_header_pos = 0;
    if (OB_FAIL(base_header.deserialize(static_cast<const char *>(buffer), nbytes, base_header_pos))) {
      CLOG_LOG(ERROR, "basic header deserialize failed", K(base_header_pos), K(id_));
    } else if (base_header.need_pre_replay_barrier()) {
      //pre_barrier no need be compressed
      ret = OB_EAGAIN;
    } else {
      const int64_t pure_data_len = nbytes - base_header_pos;
      int64_t max_overflow_size = 0;
      if (OB_FAIL(compressor->get_max_overflow_size(pure_data_len, max_overflow_size))) {
        CLOG_LOG(WARN, "failed to get_max_overflow_size", K(id_));
      } else {
        LogCompressedPayloadHeader compressed_header;
        const int64_t compressed_header_size = compressed_header.get_serialize_size();
        const int64_t compression_demand_len = pure_data_len + max_overflow_size;
        const int64_t total_buf_size = base_header_pos + compressed_header_size + compression_demand_len;
        int64_t compressed_size = 0;
        int64_t payload_header_pos = 0;

        const ObCompressorType compressor_type = compressor->get_compressor_type();
        if (OB_UNLIKELY(NULL == (compression_buf = alloc_mgr_->alloc_append_compression_buf(total_buf_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          if (REACH_TIME_INTERVAL(5 * 1000 * 1000L)) {
            CLOG_LOG(WARN, "failed to allocate compression buf", K(total_buf_size), K(id_));
          }
        } else if (OB_FAIL(compressor->compress(static_cast<const char *>(buffer) + base_header_pos, pure_data_len,
                                                static_cast<char *>(compression_buf) + base_header_pos + compressed_header_size,
                                                compression_demand_len, compressed_size))) {
          CLOG_LOG(WARN, "failed to compress", K(id_), K(pure_data_len), K(compression_demand_len), K(base_header_pos), K(compressor_type));
        } else if (compressed_size + compressed_header_size >= pure_data_len) {
          //no compression to avoid exceeding the log length limitation, assign ret with OB_EAGAIN to free compression_buf in time
          ret = OB_EAGAIN;
        } else if (OB_FAIL(compressed_header.generate(compressor_type, pure_data_len, compressed_size))) {
          CLOG_LOG(WARN, "failed to generate compressed_header", K(id_));
        } else if (OB_FAIL(compressed_header.serialize(static_cast<char *>(compression_buf) + base_header_pos,
                                                       compressed_header_size, payload_header_pos))) {
          CLOG_LOG(WARN, "failed to serialize compress_header", K(id_), K(compressed_header));
        } else if (OB_UNLIKELY(compressed_header_size != payload_header_pos)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "serialize_size of base header changed", K(compressed_header_size),
                                                                  K(payload_header_pos), K(id_));
        } else {
          base_header.set_compressed();
          int64_t new_base_pos = 0;
          if (OB_FAIL(base_header.serialize(static_cast<char *>(compression_buf), base_header_pos, new_base_pos))) {
            CLOG_LOG(WARN, "failed to serialize base_header", K(id_), K(base_header));
          } else if (OB_UNLIKELY(new_base_pos != base_header_pos)) {
            ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(WARN, "serialize_size of base header changed", K(new_base_pos), K(base_header_pos), K(id_));
          } else {
            log_compressed = true;
            compressed_data_size = new_base_pos + payload_header_pos + compressed_size;
            final_buf = compression_buf;
            final_nbytes = compressed_data_size;
          }
        }
      }
    }
  } else {/*means no need decompress*/}

  if (OB_FAIL(ret)) {
    free_compression_buf(compression_buf);
    // overwrite ret with OB_SUCCESS, append buffer with original content
    ret = OB_SUCCESS;
  }
  return ret;
}

ObCompressor *ObLogCompressorWrapper::get_compressor_()
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = ObClockGenerator::getClock();
  const int64_t last_refresh_ts = ATOMIC_LOAD(&last_refresh_config_ts_);
  if ((OB_INVALID_TIMESTAMP == last_refresh_ts) || (cur_ts - last_refresh_ts > REFRESH_CONFIG_INTERNAL)) {
    if (ATOMIC_BCAS(&is_refreshing_config_, false, true)) {
      ATOMIC_SET(&last_refresh_config_ts_, cur_ts);
      const uint64_t tenant_id = MTL_ID();
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (!tenant_config.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        CLOG_LOG(WARN, "tenant_config is not valid", K(tenant_id), K(id_));
      } else {
        const bool log_storage_compress_all = tenant_config->log_storage_compress_all;
        ObCompressorType compressor_type = INVALID_COMPRESSOR;
        if (log_storage_compress_all) {
          ObCompressor *compressor = NULL;
          if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor_type(
                      tenant_config->log_storage_compress_func, compressor_type))) {
            CLOG_LOG(ERROR, "failed to get_compressor_type", K(id_));
          } else if (!is_valid_log_compressor_type(compressor_type)) {
            ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(ERROR, "compressor type is not valid for log compression", K(compressor_type), K(id_));
          } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type,
                                                                                     compressor))) {
            CLOG_LOG(WARN, "failed to get compressor", K(compressor_type), K(id_));
          } else {
            ATOMIC_SET(&compressor_, compressor);
          }
        } else {
          ATOMIC_SET(&compressor_, NULL);
        }
        CLOG_LOG(INFO, "after refreshinglog storage compression related config",
                 K(log_storage_compress_all), K(compressor_type));
      }
      ATOMIC_SET(&is_refreshing_config_, false);
    }
  }
  return ATOMIC_LOAD(&compressor_);
}

void ObLogCompressorWrapper::free_compression_buf(void *&compression_buf)
{
  if (OB_ISNULL(alloc_mgr_)) {
    CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "alloc_mgr_ is NULL");
  } else if (NULL != compression_buf) {
    alloc_mgr_->free_append_compression_buf(compression_buf);
    compression_buf = NULL;
  }
}

} // end namespace palf
} // end namespace oceanbase
