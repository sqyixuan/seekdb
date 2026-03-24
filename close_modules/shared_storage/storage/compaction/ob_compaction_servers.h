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
#ifndef OB_SHARE_STORAGE_COMPACTION_COMPACTION_SERVERS_H_
#define OB_SHARE_STORAGE_COMPACTION_COMPACTION_SERVERS_H_
#include "share/ob_ls_id.h"
#include "lib/lock/ob_mutex.h"
#include "share/compaction/ob_compaction_obj_interface.h"
namespace oceanbase
{
namespace compaction
{
struct ObLSBroadcastInfo;
struct ObVirtualTableInfo;
struct ObExecSvr
{
  ObExecSvr()
    : exec_svr_id_(0),
      compaction_scn_(0),
      report_ts_(0)
  {}
  ObExecSvr(const uint64_t svr, const int64_t compaction_scn)
    : exec_svr_id_(svr),
      compaction_scn_(compaction_scn),
      report_ts_(ObTimeUtility::fast_current_time())
  {}
  void reset()
  {
    exec_svr_id_ = 0;
    compaction_scn_ = 0;
    report_ts_ = 0;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(K_(exec_svr_id), K_(compaction_scn), K_(report_ts));
  uint64_t exec_svr_id_;
  int64_t compaction_scn_;
  int64_t report_ts_;
};

class ObCompactionServersObj : public ObCompactionObjInterface
{
public:
  ObCompactionServersObj();
  ~ObCompactionServersObj();
  void reset();
  int init(const share::ObLSID &ls_id);
  bool is_empty() const { return exec_svr_array_.empty(); }
  virtual bool is_valid() const override { return ls_id_.is_valid(); }
  int refresh(const bool is_leader, ObCompactionObjBuffer &buf);
  int update(const uint64_t exec_svr_id, const int64_t compaction_scn);
  int update(const ObCompactionServersObj &other);
  void fill_info(ObLSBroadcastInfo &info);
  void fill_info(ObVirtualTableInfo &info);
  TO_STRING_KV(K_(ls_id), K_(exec_svr_array));
protected:
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  virtual void set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const override;
  int64_t get_serialize_array_start_idx() const { return MAX(0, exec_svr_array_.count() - DEFAULT_ARRAY_CNT); }
  int64_t get_serialize_array_cnt() const { return MIN(DEFAULT_ARRAY_CNT, exec_svr_array_.count()); }
private:
  int inner_update_report_obj();
private:
  static const int32_t SRCS_ONE_BYTE = 8;
  static const int32_t SRCS_RESERVED_BITS = 56;
  static const int64_t COMPACTION_TASKS_VERSION_V1 = 1;
  static const int64_t DEFAULT_ARRAY_CNT = 10;
  union {
    uint64_t info_;
    struct {
      uint64_t version_   : SRCS_ONE_BYTE;
      uint64_t reserved_  : SRCS_RESERVED_BITS;
    };
  };
  // record exec server recently
  ObSEArray<ObExecSvr, DEFAULT_ARRAY_CNT> exec_svr_array_;

  // no need serialize
  lib::ObMutex lock_;
  share::ObLSID ls_id_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_COMPACTION_SERVERS_H_
