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


#ifndef OCEANBASE_LOGSERVICE_LOG_COMPRESSION_
#define OCEANBASE_LOGSERVICE_LOG_COMPRESSION_

#include <stdint.h>                           // int64_t...
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"       // TO_STRING_KV
#include "lib/compress/ob_compress_util.h"

namespace oceanbase
{
namespace common{
class ObILogAllocator;
class ObIAllocator;
class ObCompressor;
}
namespace logservice
{

//@param [in] in_buf : compressed log payload
//@param [in] in_size : size of compressed log payload
//@param [in] decompression_buf : buf for decompression
//@param [out] decompression_buf_len : length of  decompression_buf
//@param [out] original_len : size of payload after decompression
int decompress(const char *in_buf, int64_t in_size, char *decompression_buf,
               const int64_t decompression_buf_size, int64_t &original_len);
class LogCompressedPayloadHeader
{
public:
  LogCompressedPayloadHeader();
  ~LogCompressedPayloadHeader();
public:
  void reset();
  bool is_valid() const;
  int generate(const common::ObCompressorType compressor_type,
               const int32_t orig_len,
               const int32_t compressed_len);
  int64_t get_original_len() const {return (int64_t)original_len_;}
  int64_t get_compressed_data_len() const {return (int64_t)compressed_len_;}
  common::ObCompressorType get_compressor_type() const {return static_cast<ObCompressorType>(compressor_type_);}
  TO_STRING_KV(K(version_), K(compressor_type_), K(original_len_), K(compressed_len_));
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  const int8_t VERSION = 1;
  int8_t version_;
  int8_t compressor_type_;//just use one byte
  int32_t original_len_;
  int32_t compressed_len_;
};


class LogCompressedPayload
{
public:
  LogCompressedPayload() {reset();}
  ~LogCompressedPayload() {reset();}
public:
  bool is_valid() const;
  void reset();
  int64_t get_data_len() const { return header_.get_compressed_data_len(); }
  const char *get_data_buf() const { return buf_; }
  const LogCompressedPayloadHeader &get_header() const { return header_; }
  TO_STRING_KV(K(header_), KP(buf_));
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  LogCompressedPayloadHeader header_;
  const char *buf_;
  DISALLOW_COPY_AND_ASSIGN(LogCompressedPayload);
};

class ObLogCompressorWrapper
{
public:
  ObLogCompressorWrapper();
  ~ObLogCompressorWrapper();
  int init(const int64_t id, common::ObILogAllocator *alloc_mgr);
  void reset();
  bool is_valid() const;
public:
/*
 @param[in] buffer: payload buf to be compressed
 @param[in] nbytes: length of payload
 @param[in & out] compression_buf: buf used for compression
 @param[in & out] log_compressed: true if payload is compressed, otherwise assigned with false
 @param[in & out] final_buf: if payload is compressed, final buf will point to compressed payload, otherwise keep unchanged
 @param[in & out] final_bytes: if payload is compressed, final_bytes will be assigned with length of compressed payload, otherwise keep unchanged
 @return:
  // OB_INVALID_ARGUMENT: some arguments is invalid
  // OB_EAGAIN: no need to compress payload
  // OB_ERR_UNEXPECTED: unexpected errors
  // OB_ALLOCATE_MEMORY_FAILED : failed to allocate memory
  // OB_DESERIALIZE_ERROR: failed to deserialize headers
  // OB_SIZE_OVERFLOW: buf is not long enough to serialize headers
*/
  int compress_payload(const void *buffer, const int64_t nbytes,
                       void *&compression_buf, bool &log_compressed,
                       const void *&final_buf, int64_t &final_nbytes);
  void free_compression_buf(void *&compression_buf);
  TO_STRING_KV(K(id_), KP(alloc_mgr_), KP(compressor_),
               K(is_refreshing_config_), K(last_refresh_config_ts_));
private:
  const int64_t REFRESH_CONFIG_INTERNAL = 5 * 1000 * 1000L;
  ObCompressor *get_compressor_();
private:
  int64_t id_;// just for debug log
  common::ObILogAllocator *alloc_mgr_;
  common::ObCompressor *compressor_;
  bool is_refreshing_config_;
  int64_t last_refresh_config_ts_;
};

} // end namespace palf
} // end namespace oceanbase

#endif
