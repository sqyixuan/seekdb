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
#ifndef OB_SHARE_STORAGE_COMPACTION_LS_COMPACTION_OBJ_MGR_H_
#define OB_SHARE_STORAGE_COMPACTION_LS_COMPACTION_OBJ_MGR_H_
#include "storage/compaction/ob_basic_compaction_obj_mgr.h"
#include "storage/compaction/ob_ls_compaction_status.h"
#include "storage/compaction/ob_compaction_servers.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_ls_compaction_list.h"

namespace oceanbase
{
namespace compaction
{

struct ObLSBroadcastInfo
{
  ObLSBroadcastInfo()
    : compaction_scn_(0),
      exec_svr_id_(0),
      state_(COMPACTION_STATE_MAX),
      is_leader_(false),
      need_update_skip_merge_meta_flag_(false)
  {}
  void reset()
  {
    compaction_scn_ = 0;
    exec_svr_id_ = 0;
    state_ = COMPACTION_STATE_MAX;
    is_leader_ = false;
    need_update_skip_merge_meta_flag_ = false;
  }
  bool is_valid() const
  {
    return compaction_scn_ >= 0 && is_valid_compaction_state(state_) && (COMPACTION_STATE_IDLE == state_ || 0 != exec_svr_id_);
  }
  bool cur_svr_is_exec_svr() const { return GCTX.get_server_id() == exec_svr_id_; }
  bool schedule_update_skip_tablet() const
  {
    return (cur_svr_is_exec_svr() && state_ > COMPACTION_STATE_COMPACT)
    || (!cur_svr_is_exec_svr() && state_ > COMPACTION_STATE_REPLICA_VERIFY);
  }
  bool need_schedule() const
  {
    return (COMPACTION_STATE_COMPACT == state_)
        || (COMPACTION_STATE_REPLICA_VERIFY == state_ && !cur_svr_is_exec_svr())
        || COMPACTION_STATE_REFRESH == state_;
  }
  ObExecMode get_exec_mode() const
  {
    ObExecMode ret_mode = EXEC_MODE_MAX;
    if (COMPACTION_STATE_COMPACT == state_) {
      ret_mode = cur_svr_is_exec_svr() ? EXEC_MODE_OUTPUT : EXEC_MODE_CALC_CKM;
    }
    return ret_mode;
  }
  ObLSCompactionState get_result_state_for_cur_svr() const
  {
    return COMPACTION_STATE_COMPACT != state_
               ? state_
               : (cur_svr_is_exec_svr() ? COMPACTION_STATE_COMPACT
                                        : COMPACTION_STATE_CALC_CKM);
  }
  TO_STRING_KV(K_(compaction_scn), K_(exec_svr_id), "state", compaction_state_to_str(state_), K_(is_leader), K_(need_update_skip_merge_meta_flag));
  int64_t compaction_scn_;
  uint64_t exec_svr_id_;
  ObLSCompactionState state_;
  bool is_leader_;
  bool need_update_skip_merge_meta_flag_;
  // need wait all skip merge tablet to update major sstable meta before svr_obj::state_ change to IDLE
};

class ObLSObj : public ObBasicObj
{
public:
  ObLSObj();
  virtual ~ObLSObj();
  virtual void destroy() override;
  int init(const share::ObLSID &ls_id);
  const ObLSCompactionListObj &get_ls_compaction_list() const { return ls_compaction_list_; }
  int get_tablet_state(
    const ObTabletID &tablet_id,
    ObTabletCompactionState &tablet_state);
  int refresh(const bool is_leader, ObCompactionObjBuffer &obj_buf);
  int start_merge(const int64_t broadcast_version, const bool is_leader);
  void get_broadcast_info(ObLSBroadcastInfo &info);
  INHERIT_TO_STRING_KV("ObLSObj", ObBasicObj, K_(is_leader), K_(switch_to_leader_ts), K_(ref_cnt),
    K_(ls_compaction_status), K_(cur_svr_ls_compaction_status), K_(compaction_svrs));
private:
  int refresh_exec_svr(const bool is_leader_role, ObLSBroadcastInfo &info);
  int check_locality_info(const common::ObAddr &svr_zone, const share::ObLSInfo &ls_info, bool &has_exec_svr);
  int get_candidate_svrs(const share::ObLSInfo &input_ls_info, ObIArray<uint64_t> &candidates);
  int get_new_exec_svr(const ObIArray<uint64_t> &candidates, ObLSBroadcastInfo &info);
  // should called under lock
  void set_role_with_delay_overwrite(const ObLSID &ls_id, const bool input_is_leader);
public:
  lib::ObMutex lock_; // used for is_leader flag
  bool is_leader_;
  int64_t switch_to_leader_ts_;
  ObLSCompactionStatusObj ls_compaction_status_;
  ObSvrLSCompactionStatusObj cur_svr_ls_compaction_status_; // for record cur svr compaction status
  ObCompactionServersObj compaction_svrs_;
  ObLSCompactionListObj ls_compaction_list_;
};

class ObLSCompactionObjMgr : public ObBasicCompactionObjMgr<share::ObLSID, ObLSObj>
{
public:
  ObLSCompactionObjMgr()
      : ObBasicCompactionObjMgr(),
        exist_ls_leader_flag_(false) {}
  virtual ~ObLSCompactionObjMgr() {}
  static int get_ls_role(const share::ObLSID &ls_id, bool &is_valid, bool &is_leader);
  int refresh(bool &exist_ls_leader_flag, ObCompactionObjBuffer &obj_buf);
  int start_merge(const int64_t broadcast_version);
  bool exist_compacting_obj();
  INHERIT_TO_STRING_KV("ObLSCompactionObjMgr", ObBasicCompactionObjMgr, K_(exist_ls_leader_flag));

protected:
  typedef share::ObLSID KEY;
  virtual int get_valid_key_array(ObIArray<KEY> &keys) override;
  virtual int check_obj_valid(const KEY &key, bool &is_valid, bool &is_leader) override;
  virtual bool is_valid_key(const KEY &key) override { return key.is_valid(); }
  virtual int init_obj(const KEY &key, ObLSObj &value) override { return value.init(key); }
  virtual int64_t get_default_hash_bucket_cnt() const override 
  {
    return lib::mtl_is_mini_mode() ? OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_FOR_SMALL_TENANT : OB_MAX_LS_NUM_PER_TENANT_PER_SERVER;
  }

  bool exist_ls_leader_flag_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_LS_COMPACTION_OBJ_MGR_H_
