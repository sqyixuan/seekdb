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
#ifndef OB_SHARE_STORAGE_COMPACTION_LS_MERGE_TABLET_H_
#define OB_SHARE_STORAGE_COMPACTION_LS_MERGE_TABLET_H_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_rwlock.h"
#include "storage/compaction/ob_basic_compaction_obj_mgr.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "share/compaction/ob_compaction_obj_interface.h"


namespace oceanbase
{
namespace compaction
{
// used for validating index table checksum
class ObLSCompactionListObj : public ObCompactionObjInterface
{
public:
  ObLSCompactionListObj();
  virtual ~ObLSCompactionListObj();
  virtual void destroy();
  int init(const share::ObLSID &input_ls_id);
  virtual bool is_valid() const;
  int refresh(const bool is_ls_leader, ObCompactionObjBuffer &obj_buf);
  int64_t get_compaction_scn() const { return compaction_scn_; }
  int tablet_need_skip(
      const int64_t merge_version,
      const ObTabletID &tablet_id,
      bool &need_skip) const;
  int add_skip_tablet(
      const int64_t merge_version,
      const ObTabletID &tablet_id);
  void fill_info(ObVirtualTableInfo &info) const;
  TO_STRING_KV(K_(ls_id), K_(compaction_scn), K_(is_inited), "skip_cnt", skip_merge_tablets_.size());
private:
  virtual void set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const override;
  virtual int serialize(char* buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t &pos) override;
  virtual int64_t get_serialize_size(void) const override;
protected:
  typedef hash::ObHashSet<ObTabletID, hash::NoPthreadDefendMode> SkipSet;
  static const int64_t DEFAULT_BUCKET_CNT = 256;
  static const int32_t SRCS_ONE_BYTE = 8;
  static const int32_t SRCS_RESERVED_BITS = 24;
  static const int64_t COMPACTION_LIST_VERSION_V1 = 1;
  union {
    uint32_t info_;
    struct {
      uint32_t version_   : SRCS_ONE_BYTE;
      uint32_t reserved_  : SRCS_RESERVED_BITS;
    };
  };
  mutable obsys::ObRWLock lock_; // prevents concurrency between refresh obj and fill diagnose info
  share::ObLSID ls_id_;
  int64_t compaction_scn_;
  int64_t loop_cnt_; // no need to serialize
  SkipSet skip_merge_tablets_;
private:
  DISABLE_COPY_ASSIGN(ObLSCompactionListObj);
};


} // compaction
} // oceanbase




#endif // OB_SHARE_STORAGE_COMPACTION_LS_MERGE_TABLET_H_
