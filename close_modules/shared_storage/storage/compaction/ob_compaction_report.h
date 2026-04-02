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
#ifndef OB_SHARE_STORAGE_COMPACTION_COMPACTION_REPORT_H_
#define OB_SHARE_STORAGE_COMPACTION_COMPACTION_REPORT_H_
#include "share/ob_ls_id.h"
#include "lib/hash/ob_hashmap.h"
#include "share/compaction/ob_compaction_obj_interface.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/compaction/ob_basic_compaction_obj_mgr.h"
namespace oceanbase
{
namespace compaction
{

class ObCompactionReportObj : public ObBasicObj, public ObCompactionObjInterface
{
public:
  ObCompactionReportObj();
  ~ObCompactionReportObj();
  void reset();
  bool is_empty() const { return report_ts_ == 0; }
  virtual bool is_valid() const override;
  int refresh(const bool is_leader, ObCompactionObjBuffer &obj_buf);
  int init(const uint64_t svr_id, const ObjExecMode exec_mode);
  int update(const ObCompactionReportObj &refresh_obj);
  void fill_info(ObVirtualTableInfo &info);
  int update_exec_tablet(const int64_t exec_tablet_cnt, const bool is_finish_task = false);
  int64_t get_unfinish_tablet_count() { return ATOMIC_LOAD(&unfinish_tablet_cnt_); }

  INHERIT_TO_STRING_KV("ObCompactionReportObj", ObBasicObj, K_(svr_id), K_(errno), K_(report_ts), K_(finish_tablet_cnt), K_(exec_tablet_cnt), K_(unfinish_tablet_cnt));
protected:
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  virtual void set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const override;
private:
  static const int32_t SRCS_ONE_BYTE = 8;
  static const int32_t SRCS_RESERVED_BITS = 24;
  static const int64_t COMPACTION_REPORT_VERSION_V1 = 1;
  union {
    uint32_t info_;
    struct {
      uint32_t version_   : SRCS_ONE_BYTE;
      uint32_t reserved_  : SRCS_RESERVED_BITS;
    };
  };
  int errno_;
  int64_t report_ts_;
  int64_t finish_tablet_cnt_; // record finish tablet in last update interval
  int64_t exec_tablet_cnt_;
  int64_t unfinish_tablet_cnt_;

  // no need serialize
  lib::ObMutex lock_;
  uint64_t svr_id_;
  ObjExecMode exec_mode_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_COMPACTION_REPORT_H_
