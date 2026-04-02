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

#ifndef OCEANBASE_LOGSERVICE_OB_FILE_UPLOAD_MGR_
#define OCEANBASE_LOGSERVICE_OB_FILE_UPLOAD_MGR_

#include "lib/profile/ob_trace_id.h"              // ObCurTraceID::ObTraceId
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_reader_utils.h"
#include "share/ob_thread_pool.h"
#include "log/ob_shared_log_upload_handler.h"

namespace oceanbase
{
namespace common
{
class ObIOFd;
}
namespace share
{
struct ObLSID;
}

namespace palf
{
class LogGroupEntryHeader;
}
namespace logservice
{
class ObLogService;
class ObLogExternalStorageHandler;
class ObSharedLogService;
class ObFileUploadMgr;


class ObFileUploadMgr : public share::ObThreadPool
{
public:
  ObFileUploadMgr();
  ~ObFileUploadMgr();
public:
  int init(const ObAddr &addr,
           ObLogService *log_service,
           ObSharedLogService *shared_log,
           ObLogExternalStorageHandler *ext_storage_handler);
  void destroy();
  int start();
  void run1();
  int upload_ls(ObSharedLogUploadHandler *handle, bool &has_file_to_upload);

private:
  class ObFileUploadFunctor
  {
  public:
    ObFileUploadFunctor(ObFileUploadMgr &upload_mgr)
        : has_file_to_upload_(false), upload_mgr_(upload_mgr) {}
    ~ObFileUploadFunctor(){}
    bool operator()(palf::LSKey &id, ObSharedLogUploadHandler *handler);
    bool has_file_to_upload() const {return has_file_to_upload_;}
    TO_STRING_KV(K(has_file_to_upload_));
  private:
    bool has_file_to_upload_;
    ObFileUploadMgr &upload_mgr_;
  };

  class StatUploadProcessFunctor {
  public:
    StatUploadProcessFunctor();
    ~StatUploadProcessFunctor();
    int64_t get_unuploaded_size() const; 
    int64_t get_uploaded_size() const; 
    bool operator()(palf::LSKey &id, ObSharedLogUploadHandler *handler);
  private:
    int64_t unuploaded_size_;
    int64_t uploaded_size_; 
    DISALLOW_COPY_AND_ASSIGN(StatUploadProcessFunctor);
  };

private:
  int upload_();
  int locate_upload_range_(const palf::LSN &begin_lsn,
                           const palf::LSN &base_lsn,
                           const palf::LSN &end_lsn,
                           ObSharedLogUploadHandler *handle,
                           LogUploadCtx &new_log_upload_ctx);
  int locate_end_block_id_(const share::ObLSID &ls_id,
                           const palf::block_id_t base_block_id,
                           palf::block_id_t &end_block_id);
 /*
     @brief get max_block_id on shared storage
     @return value
     OB_SUCCESS
     OB_ENTRY_NOT_EXIST: no log files on shared storage
     OB_ERR_UNEXPECTED: bugs
*/
  int get_max_block_id_on_ss_(const uint64_t tenant_id,
                              const share::ObLSID &ls_id,
                              const LogUploadCtx &log_upload_ctx,
                              const palf::block_id_t &base_block_id,
                              palf::block_id_t &max_block_id_on_ss,
                              bool &need_update_base_lsn);
  int update_base_lsn_(const share::ObLSID &ls_id, const palf::LSN &base_lsn);
  int do_upload_file_(const uint64_t tenant_id,
                      const share::ObLSID &ls_id,
                      const palf::block_id_t block_id);
  int check_data_integrity_(const char *buff,
                            const int64_t buff_len,
                            const share::ObLSID &ls_id,
                            share::SCN &first_log_scn,
                            int64_t &last_log_end_pos,
                            palf::LSN &last_log_lsn);
  int serialize_block_header_(char *buf,
                              const int64_t buf_len,
                              const int64_t palf_id,
                              const palf::block_id_t &block_id,
                              const share::SCN &min_scn);
  template <typename LIST>
  int sync_base_lsn_(const share::ObLSID &id, const palf::LSN &base_lsn, const LIST &list);

  int try_alloc_read_buf_();
  void try_destroy_read_buf_();

private:
  static constexpr int64_t SINGLE_PART_SIZE = 8 * 1024 * 1024ul;
  static constexpr int64_t SINGLE_READ_SIZE = SINGLE_PART_SIZE + palf::MAX_LOG_BUFFER_SIZE;
  const int64_t UPLOAD_INTERVAL = 1000 * 1000L; // 1s
  const int64_t READ_BUF_SIZE = palf::PALF_PHY_BLOCK_SIZE;
  const int64_t BLOCK_HEADER_SIZE = palf::MAX_INFO_BLOCK_SIZE;
private:
  bool is_inited_;
  ObAddr self_;
  int64_t last_sync_base_lsn_ts_;
  int64_t last_uploaded_size_;
  palf::ReadBuf read_buf_;
  ObLogExternalStorageHandler *ext_storage_handler_;
  ObSharedLogService *log_shared_storage_service_;
  ObLogService *log_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFileUploadMgr);
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_FILE_UPLOAD_MGR_
