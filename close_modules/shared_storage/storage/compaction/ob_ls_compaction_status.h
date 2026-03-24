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
#ifndef OB_SHARE_STORAGE_COMPACTION_LS_COMPACTION_STATUS_H_
#define OB_SHARE_STORAGE_COMPACTION_LS_COMPACTION_STATUS_H_
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_delegate.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "share/compaction/ob_compaction_obj_interface.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "share/compaction/ob_new_micro_info.h"

namespace oceanbase
{
namespace storage
{
class ObLSHandle;
}
namespace compaction
{
class ObSvrLSCompactionStatusObj;
struct ObLSBroadcastInfo;
struct ObLSCompactionStatusInfo;
class ObLSCompactionListObj;
struct ObSkipMergeTabletPair;
struct ObTabletCompactionState
{
public:
  ObTabletCompactionState() { reset(); }
  ~ObTabletCompactionState() {}
  void reset()
  {
    compaction_scn_ = 0;
    clog_submitted_ = false;
    output_ = false;
    calc_ckm_ = false;
    verified_ = false;
    refreshed_ = false;
    skip_ = false;
  }
  int update(const ObTabletCompactionState &input_state);
  bool is_valid() const
  {
    return compaction_scn_ > 0;
  }
  int64_t get_submitted_log_scn() const { return (clog_submitted_ || output_ || calc_ckm_ || verified_ || refreshed_ || skip_) ? compaction_scn_ : 0; }
  int64_t get_output_scn() const { return (refreshed_ || output_) ? compaction_scn_ : 0; }
  int64_t get_calc_ckm_scn() const { return (output_ || refreshed_ || calc_ckm_) ? compaction_scn_ : 0; }
  int64_t get_verified_scn() const { return (output_ || refreshed_ || verified_) ? compaction_scn_ : 0; }
  int64_t get_refreshed_scn() const { return refreshed_ ? compaction_scn_ : 0; }
  int64_t get_skip_scn() const { return skip_ ? compaction_scn_ : 0; }
#define DEFINE_SET_SCN_FUNC(flag)                                              \
  void set_##flag##_scn(const int64_t compaction_scn) {                        \
    if (compaction_scn >= compaction_scn_) {                                   \
      reset();                                                                 \
      compaction_scn_ = compaction_scn;                                        \
      clog_submitted_ = true;                                                  \
      flag##_ = true;                                                          \
    }                                                                          \
  }
  DEFINE_SET_SCN_FUNC(output);
  DEFINE_SET_SCN_FUNC(calc_ckm);
  DEFINE_SET_SCN_FUNC(verified);
  DEFINE_SET_SCN_FUNC(refreshed);
  DEFINE_SET_SCN_FUNC(skip);
#undef DEFINE_SET_SCN_FUNC
  void fill_info(ObVirtualTableInfo &info) const;
  ObTabletCompactionState& operator=(const ObTabletCompactionState& other);
  bool operator==(const ObTabletCompactionState& other) const; // for unittest
  TO_STRING_KV(K_(compaction_scn), K_(clog_submitted), K_(output), K_(calc_ckm), K_(verified), K_(refreshed), K_(skip));
  NEED_SERIALIZE_AND_DESERIALIZE;
  /*
   * output_scn < compaction_scn : exec_replica need execute output mode major
   * verified_scn < compaction_scn : verify_replica need execute verify mode major
   * refreshed_scn < compaction_scn: replica could refresh major
   * refreshed_scn >= compaction_scn : FINISH
  */
  int64_t compaction_scn_;
  bool clog_submitted_;
  bool output_;
  bool calc_ckm_;
  bool verified_;
  bool refreshed_;
  bool skip_;
};

struct ObTabletInfoMap
{
  ObTabletInfoMap() {}
  ~ObTabletInfoMap() { destroy(); }
  int init();
  void destroy()
  {
    if (map_.created()) {
      map_.destroy();
    }
  }
  bool is_valid() const { return map_.created(); }
  void reuse() { map_.reuse(); }
  DELEGATE_WITH_RET(map_, set_refactored, int);
  DELEGATE_WITH_RET(map_, erase_refactored, int);
  CONST_DELEGATE_WITH_RET(map_, get_refactored, int);
  CONST_DELEGATE_WITH_RET(map_, size, int64_t);
  bool operator==(const ObTabletInfoMap& other) const; // for unittest
  int get_exist_keys(hash::ObHashSet<ObTabletID> &exist_key_set) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  const static int64_t DEFAULT_MAP_BUCKET_CNT = 1024;
  typedef hash::ObHashMap<common::ObTabletID, ObTabletCompactionState> TabletInfoMap;
  TabletInfoMap map_;
private:
  DISABLE_COPY_ASSIGN(ObTabletInfoMap);
};

struct ObStateCollector
{
  ObStateCollector() { reset(); }
  ~ObStateCollector() {}
  bool is_empty() const
  {
    bool empty = true;
    for (int64_t idx = 0; empty && idx < STATE_ARRAY_SIZE; ++idx) {
      empty = (state_cnt_[idx] == 0);
    }
    return empty;
  }
  void reset()
  {
    MEMSET(state_cnt_, 0, sizeof(int64_t) * STATE_ARRAY_SIZE);
  }
  void add(const ObLSCompactionState ls_state)
  {
    if (is_valid_compaction_state(ls_state)) {
      state_cnt_[ls_state]++;
    }
  }
  static const int64_t STATE_ARRAY_SIZE = COMPACTION_STATE_MAX + 1;
  DECLARE_TO_STRING;
  int64_t state_cnt_[STATE_ARRAY_SIZE];
};

class ObLSCompactionStatusObj : public ObCompactionObjInterface
{
public:
  ObLSCompactionStatusObj();
  virtual ~ObLSCompactionStatusObj();
  virtual void destroy();
  int init(const share::ObLSID &input_ls_id);
  int init(const ObLSCompactionStatusInfo &info);
  virtual bool is_valid() const override;
  int refresh_compaction_scn(const int64_t broadcast_version = 0);
  int update(const ObIArray<const ObSvrLSCompactionStatusObj*> &refresh_svr_obj_array);
  int refresh(
    const bool is_leader,
    const ObSvrLSCompactionStatusObj &cur_svr_finish_tablets,
    ObLSBroadcastInfo &info,
    ObCompactionObjBuffer &obj_buf);
  void fill_info(ObLSBroadcastInfo &info) const;
  virtual void fill_info(ObVirtualTableInfo &info) const;
  void fill_info(ObLSCompactionStatusInfo &info) const;
  int report_ls_index_verified(
      const int64_t compaction_scn,
      const bool is_svr_obj);
  OB_INLINE share::ObLSID get_ls_id() const { return ls_id_; }
  OB_INLINE int64_t get_update_state_ts() const { return ATOMIC_LOAD(&update_state_ts_); }
  OB_INLINE ObLSCompactionState get_state() const { return (ObLSCompactionState)state_; }
  void get_new_micro_info(ObNewMicroInfo &new_micro_info) const;

  TO_STRING_KV(K_(ls_id), "expect_state", compaction_state_to_str(get_state()), K_(compaction_scn),
    K_(verify_replica_cnt), K_(update_state_ts), K_(new_micro_info));
protected:
  void set_state(
    const ObLSCompactionState &input_state,
    const ObServerCompactionEvent::ObCompactionRole role,
    const char *function_name);
  void inner_set_state(
    const ObLSCompactionState &input_state,
    const ObServerCompactionEvent::ObCompactionRole role,
    const char *function_name);
  int read_ls_compaction_status(ObLSCompactionState &state);
#ifdef ERRSIM
  void errsim_fallback_compaction_scn();
#endif
private:
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  virtual void set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const override;
  int update_ls_state(const ObIArray<const ObSvrLSCompactionStatusObj*> &refresh_svr_obj_array);
  int inner_update_ls_state(ObStateCollector &collector);
  int refresh_replica_cnt();
  bool is_compacting() const;
  bool is_extra_check_round() const
  {
#ifdef ERRSIM
    // make special check every round
    return true;
#endif
    return 0 == (loop_cnt_ % 8);
  } // check every 8 rounds
protected:
  static const int32_t SRCS_ONE_BYTE = 8;
  static const int32_t SRCS_FOUR_BIT = 4;
  static const int32_t SRCS_RESERVED_BITS = 52;
  static const int64_t LS_COMPACTION_STATUS_VERSION_V1 = 1;
  static const int64_t EXEC_REPLICA_CNT = 1; // single region
  static const int64_t DEFAULT_ARRAY_SIZE = 64;
  static const int64_t TABLET_ARRAY_SIZE = 256;
  union {
    uint64_t info_;
    struct {
      uint64_t version_ : SRCS_ONE_BYTE;
      uint64_t state_   : SRCS_FOUR_BIT;
      uint64_t reserved_  : SRCS_RESERVED_BITS;
    };
  };
  int64_t compaction_scn_;
  int64_t merged_scn_;
  // no need serialize
  mutable obsys::ObRWLock lock_;
  share::ObLSID ls_id_;
  int64_t verify_replica_cnt_; // only used in ObLSCompactionStatusObj, init when cur svr is leader
  int64_t loop_cnt_;
  int64_t update_state_ts_; // record the update time of cur merge state
  ObNewMicroInfo new_micro_info_;
private:
  bool operator==(const ObLSCompactionStatusObj& other) const; // for unittest
  DISABLE_COPY_ASSIGN(ObLSCompactionStatusObj);
};

class ObSvrLSCompactionStatusObj : public ObLSCompactionStatusObj
{
public:
  ObSvrLSCompactionStatusObj();
  virtual ~ObSvrLSCompactionStatusObj() { destroy(); }
  int init(
    const uint64_t svr_id,
    const share::ObLSID &input_ls_id,
    const ObjExecMode exec_mode);
  virtual bool is_valid() const override;
  int update_tablet_state(
    const ObTabletID &tablet_id,
    const ObTabletCompactionState &tablet_state,
    const ObNewMicroInfo *new_micro_info = NULL);
  int get_tablet_state(const ObTabletID &tablet_id, ObTabletCompactionState &tablet_state) const;
  int refresh(const ObLSBroadcastInfo &info, ObCompactionObjBuffer &buf);
  virtual void destroy() override;
  int get_schedule_tablet(
      ObLSCompactionListObj &list_obj,
      ObLSBroadcastInfo &info,
      ObLS &ls,
      ObIArray<ObTabletID> &schedule_tablet_ids,
      ObIArray<ObTabletID> &no_inc_tablet_ids);
  int get_unsubmitted_clog_tablet(
    const int64_t merge_version,
    ObIArray<ObTabletID> &schedule_tablet_ids);

  // Attention! will return success even if update several tablets failed
  int batch_update_tablet_state(
    const ObTabletCompactionState &tablet_state,
    const ObIArray<ObTabletID> &tablet_ids);
  virtual void fill_info(ObVirtualTableInfo &info) const override;
  static int gene_tablet_state(
    const storage::ObTablet &tablet,
    ObTabletCompactionState &tablet_state);
  TO_STRING_KV(K_(ls_id), K_(svr_id), K_(compaction_scn),
    "finish_state", compaction_state_to_str((ObLSCompactionState)state_),
    "target_state", compaction_state_to_str((ObLSCompactionState)target_state_),
    "exec_mode", exec_mode_to_str(exec_mode_), "map_cnt", tablet_info_map_.size());
private:
  typedef hash::ObHashSet<ObTabletID> TabletIDSet;
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  bool is_erase_tablet_round() const { return 0 == (loop_cnt_ % 32); } // check every 32 rounds
  bool is_executing() const { return COMPACTION_STATE_MAX != target_state_ && target_state_ != get_state(); }
  virtual void set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const override;
  int check_not_exist_tablet(
    storage::ObLSHandle &ls_handle,
    ObLSBroadcastInfo &info,
    const ObIArray<ObTabletID> &not_exist_tablet,
    ObIArray<ObTabletID> &schedule_tablet_ids);
  int loop_map_to_get_tablets(
    const ObIArray<ObTabletID> &tablet_ids,
    ObLSCompactionListObj &list_obj,
    ObLSBroadcastInfo &info,
    ObLS &ls,
    ObIArray<ObTabletID> &schedule_tablet_ids,
    ObIArray<ObTabletID> &no_inc_tablet_ids,
    bool &exist_no_clog_tablet);
  int deal_with_not_exist_tablet(
    const ObTabletID &tablet_id,
    const ObLSBroadcastInfo &info,
    ObLS &ls,
    ObTabletCompactionState &tablet_state);
  int inner_check_merge_reason(
    const ObLSBroadcastInfo &info,
    const ObIArray<ObTabletID> &schedule_tablet_ids,
    ObLSCompactionListObj &list_obj,
    ObLS &ls,
    ObIArray<ObTabletID> &merge_tablet_ids,
    ObIArray<ObTabletID> &no_inc_tablet_ids,
    bool &exist_no_clog_tablet);
  void try_update_scn(const ObLSBroadcastInfo &info);
  int inner_update_tablet_state(
    const ObTabletID &tablet_id,
    const ObTabletCompactionState &input_state);
  int push_tablet_by_state(
    const ObTabletID &tablet_id,
    const ObTabletCompactionState &tablet_state,
    ObLSBroadcastInfo &info,
    ObIArray<ObTabletID> &schedule_tablet_ids);
  void update_target_state(const ObLSBroadcastInfo &info);
  void inner_update_target_state(const ObLSBroadcastInfo &info);
  void inner_update_state_with_check(const ObLSBroadcastInfo &info);
  typedef bool (*JudgeTabletFunc)(const ObTabletCompactionState tmp_state,
                                const int64_t input_compaction_scn);
  static JudgeTabletFunc JUDGE_TABLET_FUNC[ObLSCompactionState::COMPACTION_STATE_MAX];
private:
  // no need serialize
  ObTabletInfoMap tablet_info_map_; // tablet_id -> tablet_state
  uint64_t svr_id_;
  ObjExecMode exec_mode_;
  ObLSCompactionState target_state_;
};

// record information in ObLSCompactionStatusObj
// ObLSCompactionStatusObj can be read/write to shared_storage, is not safe to hold obj outside
struct ObLSCompactionStatusInfo
{
public:
  ObLSCompactionStatusInfo()
   : ls_id_(0),
     compaction_scn_(0),
     refreshed_scn_(0),
     rs_update_state_(false),
     state_(compaction::ObLSCompactionState::COMPACTION_STATE_MAX),
     new_micro_info_()
   {}
  bool is_valid() const
  {
    return ls_id_ > 0
      && refreshed_scn_ >= 0 && refreshed_scn_ <= compaction_scn_;
  }
  void reset()
  {
    ls_id_ = 0;
    compaction_scn_ = 0;
    refreshed_scn_ = 0;
    rs_update_state_ = false;
    state_ = compaction::ObLSCompactionState::COMPACTION_STATE_MAX;
    new_micro_info_.reset();
  }
  TO_STRING_KV(K_(ls_id), K_(compaction_scn), K_(refreshed_scn), K_(rs_update_state), "state", compaction_state_to_str(state_), K_(new_micro_info));
public:
  int64_t ls_id_;
  int64_t compaction_scn_;
  int64_t refreshed_scn_;
  bool rs_update_state_;
  compaction::ObLSCompactionState state_;
  ObNewMicroInfo new_micro_info_;
};

class ObLSCompactionStatusObjLoader
{
public:

  // load svr_ls_tablet_status_obj for update ls_tablet_status_obj
  static int load_to_update(
    const share::ObLSID &ls_id,
    const ObSvrLSCompactionStatusObj &cur_svr_tablet_status,
    const ObLSBroadcastInfo &info,
    ObLSCompactionStatusObj &input_obj,
    ObCompactionObjBuffer &obj_buf);

  static int load_ls_compaction_status_array(
    const uint64_t tenant_id,
    const common::ObIArray<share::ObLSID> &ls_ids,
    common::ObIArray<ObLSCompactionStatusInfo> &ls_status_infos);
  static int batch_update_ls_compaction_status(
    const uint64_t tenant_id,
    const common::ObIArray<ObLSCompactionStatusInfo> &ls_status_infos);
  static int load_ls_compaction_list(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    compaction::ObLSCompactionListObj &input_obj);
  static const int64_t DEFAULT_ARRAY_CNT = 10;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_LS_COMPACTION_STATUS_H_
