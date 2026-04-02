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
#ifndef OCEANBASE_LOGSERVICE_OB_LOG_ITERATOR_STORAGE_
#define OCEANBASE_LOGSERVICE_OB_LOG_ITERATOR_STORAGE_
#include "lib/utility/ob_macro_utils.h"                                         // DISALLOW_COPY_AND_ASSIGN
#include "logservice/palf/log_storage_interface.h"                              // ILogStorage
#include "logservice/palf/log_iterator_storage.h"                               // MemoryStorage
#include "logservice/palf/log_reader_utils.h"                                   // ReadBuf
#include "logservice/palf_handle_guard.h"                                       // PalfHandleGuard
#include "logservice/palf/palf_iterator.h"                                      // PalfIterator
namespace oceanbase
{
namespace palf
{
class LSN;
}
namespace logservice
{
class ObLogExternalStorageHandler;
class ObLogMemoryStorage {
public:
  ObLogMemoryStorage();
  ~ObLogMemoryStorage();
  int init(const palf::LSN &start_lsn);
  void destroy();
  // @return val
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ERR_UNEXPECTED, lsn is not continous with log_tail_
  int append(const palf::LSN &lsn,
             const char *buf,
             const int64_t buf_len);
  // NB: lsn must be continous with log_tail_
  // @return val
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ERR_OUT_OF_LOWER_BOUND 
  //   OB_ERR_OUT_OF_UPPER_BOUND 
  int pread(const palf::LSN& lsn,
            const int64_t in_read_size,
            palf::ReadBuf &read_buf,
            int64_t &out_read_size,
            palf::LogIOContext &io_ctx);
  int get_data_len(int64_t &data_len) const;
  // @return val
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ERR_OUT_OF_UPPER_BOUND
  //   OB_ERR_OUT_OF_LOWER_BOUND
  int get_read_pos(const palf::LSN &lsn,
                   int64_t &read_pos) const;
  const palf::LSN &get_log_tail() const;
  const palf::LSN &get_start_lsn() const;
  void reuse(const palf::LSN &start_lsn);
  void reset();
  TO_STRING_KV(K_(start_lsn), K_(log_tail), KP(buf_), K_(buf_len), K_(is_inited));
private:
  const char *buf_;
  int64_t buf_len_;
  palf::LSN start_lsn_;
  palf::LSN log_tail_;
  bool is_inited_;
};

class ObLogSharedStorage {
public:
  ObLogSharedStorage();
  virtual ~ObLogSharedStorage();

  // [observer-lite] not support
  int init(const uint64_t tenant_id,
           const int64_t palf_id,
           const palf::LSN &start_lsn,
           const int64_t suggested_max_read_buffer_size,
           ObLogExternalStorageHandler *ext_handler);

  // [observer-lite] not support
  void destroy();

  // [observer-lite] not support
  void reset();

  // [observer-lite] not support
  int pread(const palf::LSN &lsn,
            const int64_t in_read_size,
            palf::ReadBuf &read_buf,
            int64_t &out_read_size,
            palf::LogIOContext &io_ctx);

  TO_STRING_KV(K("ob not support"));
public:
  static int64_t MEMORY_LIMIT;
  // READ SIZE must greater than or equal to single LogGroupEntry size
  static int64_t READ_SIZE;
  static int64_t BLOCK_SIZE;
  static int64_t PHY_BLOCK_SIZE;
  static int64_t MAX_LOG_SIZE;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSharedStorage);
};

class ObLogLocalStorage {
public:
  ObLogLocalStorage();
  ~ObLogLocalStorage();
  int init(const uint64_t tenant_id,
           const int64_t palf_id);
  int pread(const palf::LSN &lsn,
            const int64_t in_read_size,
            palf::ReadBuf &read_buf,
            int64_t &out_read_size,
            palf::LogIOContext &io_ctx);
  void destroy();

  int get_file_end_lsn(palf::GetFileEndLSN &funtion);
  int get_access_mode_version(palf::GetModeVersion &function);
  TO_STRING_KV(K_(is_inited));
protected:
  virtual int open_palf_handle_(const uint64_t tenant_id,
                                const int64_t palf_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogLocalStorage);
  palf::PalfHandleGuard palf_handle_guard_;
  bool is_inited_;
};

class ObLogHybridStorage : public palf::ILogStorage {
public:
  ObLogHybridStorage();
  ~ObLogHybridStorage();

  int init(const uint64_t tenant_id,
           const int64_t palf_id,
           const palf::LSN &start_lsn,
           const int64_t suggested_max_read_buffer_size,
           ObLogExternalStorageHandler *ext_handler);

  void destroy();

  // @retval
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ERR_OUT_OF_UPPER_BOUND
  //   OB_ERR_OUT_OF_LOWER_BOUND
  //   OB_ERR_UNEXPECTED
  int pread(const palf::LSN &lsn,
            const int64_t in_read_size,
            palf::ReadBuf &read_buf,
            int64_t &out_read_size,
            palf::LogIOContext &io_ctx) final;
  int get_file_end_lsn(palf::GetFileEndLSN &funtion);
  int get_access_mode_version(palf::GetModeVersion &function);
  INHERIT_TO_STRING_KV("ILogStorage", ILogStorage, K(shared_storage_), K(local_storage_), K_(is_inited));
private:
  static const int64_t STAT_INTERVAL = 5 * 1000 * 1000;
private:
  int pread_impl_(const palf::LSN &lsn,
                  const int64_t in_read_size,
                  palf::ReadBuf &read_buf,
                  int64_t &out_read_size,
                  palf::LogIOContext &io_ctx);
  int read_from_local_storage_(const palf::LSN &lsn,
                               const int64_t in_read_size,
                               palf::ReadBuf &read_buf,
                               int64_t &out_read_size,
                               palf::LogIOContext &io_ctx);
  int read_from_shared_storage_(const palf::LSN &lsn,
                                const int64_t in_read_size,
                                palf::ReadBuf &read_buf,
                                int64_t &out_read_size,
                                palf::LogIOContext &io_ctx);
  int convert_ret_code_(const int ret_code);
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogHybridStorage);
  ObLogSharedStorage shared_storage_;
  ObLogLocalStorage local_storage_;
  ObMiniStat::ObStatItem shared_storage_read_size_;
  ObMiniStat::ObStatItem shared_storage_read_cost_;
  ObMiniStat::ObStatItem local_storage_read_size_;
  ObMiniStat::ObStatItem local_storage_read_cost_;
  bool is_inited_;
};
} // end namespace logservice
} // end namespace oceanbase
#endif
