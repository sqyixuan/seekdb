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
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_ls_compaction_status.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/compaction/ob_tenant_ls_merge_scheduler.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace obsys;
using namespace storage;
using namespace share;
namespace compaction
{
ERRSIM_POINT_DEF(EN_COMPACTION_GET_SCHEDULE_TABLET);
ERRSIM_POINT_DEF(EN_LS_LEADER_REFRESH);
ERRSIM_POINT_DEF(EN_FALLBACK_COMPACTION_SCN);
ERRSIM_POINT_DEF(EN_RS_UPDATE_LS_STATE);

OB_SERIALIZE_MEMBER_SIMPLE(ObTabletCompactionState, compaction_scn_,
  clog_submitted_, output_, calc_ckm_, verified_, refreshed_, skip_);
void ObTabletCompactionState::fill_info(ObVirtualTableInfo &info) const
{
  ADD_COMPACTION_INFO_PARAM(info.buf_, OB_MAX_VARCHAR_LENGTH, K_(compaction_scn), K_(clog_submitted),
    K_(output), K_(calc_ckm), K_(verified), K_(refreshed), K_(skip));
}
/**
 * -------------------------------------------------------------------ObTabletInfoMap-------------------------------------------------------------------
 */
int ObTabletInfoMap::init()
{
  int ret = OB_SUCCESS;
  if (!map_.created()
      && OB_FAIL(map_.create(DEFAULT_MAP_BUCKET_CNT, "LSCompMap", "LSCompMap", MTL_ID()))) {
    LOG_WARN("failed to create map", KR(ret));
  }
  return ret;
}
// ObTabletInfoMap serialization is not used now
int ObTabletInfoMap::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), K(buf), K(buf_len), K(pos));
  } else {
    const int64_t cnt = map_.size();
    if (OB_FAIL(serialization::encode(buf, buf_len, new_pos, cnt))) {
      LOG_WARN("failed to serialize cnt", KR(ret), K(buf_len), K(cnt));
    } else {
      int64_t idx = 0;
      TabletInfoMap::const_iterator iter = map_.begin();
      for ( ; OB_SUCC(ret) && iter != map_.end(); ++iter) {
        if (OB_FAIL(iter->first.serialize(buf, buf_len, new_pos))) {
          LOG_WARN("failed to serialize tablet_id", KR(ret), K(buf_len), "tablet_id", iter->first);
        } else if (OB_FAIL(iter->second.serialize(buf, buf_len, new_pos))) {
          LOG_WARN("failed to serialize tablet state", KR(ret), K(buf_len), "tablet_state", iter->second);
        } else {
          ++idx;
        }
      } // end of for
      if (OB_SUCC(ret) && OB_UNLIKELY(idx != cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("serialize cnt is invalid", KR(ret), K(idx), K(cnt));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

int ObTabletInfoMap::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(init())) {
    LOG_WARN("failed to init map", KR(ret));
  } else {
    int64_t cnt = 0;
    if (OB_FAIL(serialization::decode(buf, data_len, new_pos, cnt))) {
      LOG_WARN("failed to deserialize cnt", KR(ret), K(data_len));
    } else {
      ObTabletID tmp_tablet_id;
      ObTabletCompactionState tmp_state;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < cnt; ++idx) {
        if (OB_FAIL(tmp_tablet_id.deserialize(buf, data_len, new_pos))) {
          LOG_WARN("failed to deserialize tablet_id", KR(ret), K(data_len));
        } else if (OB_FAIL(tmp_state.deserialize(buf, data_len, new_pos))) {
          LOG_WARN("failed to deserialize tablet state", KR(ret), K(data_len));
        } else if (OB_FAIL(map_.set_refactored(tmp_tablet_id, tmp_state))) {
          LOG_WARN("failed to set refactor", KR(ret), K(tmp_tablet_id), K(tmp_state));
        }
      } // end of for
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  } else {
    pos = new_pos;
  }
  return ret;
}

int64_t ObTabletInfoMap::get_serialize_size() const
{
  int64_t len = 0;
  const int64_t cnt = map_.size();
  len += serialization::encoded_length(cnt);
  for (TabletInfoMap::const_iterator iter = map_.begin(); iter != map_.end(); ++iter) {
    len += iter->first.get_serialize_size();
    len += iter->second.get_serialize_size();
  }
  return len;
}

bool ObTabletInfoMap::operator==(const ObTabletInfoMap& other) const
{
  bool bret = true;
  if (map_.size() != other.size()) {
    bret = false;
  } else {
    TabletInfoMap::const_iterator iter1 = map_.begin();
    ObTabletCompactionState tmp_state;
    int ret = OB_SUCCESS;
    for ( ; bret && iter1 != map_.end(); ++iter1) {
      if (OB_FAIL(other.get_refactored(iter1->first, tmp_state))) {
        bret = false;
      } else if (iter1->second == tmp_state) {
      } else {
        bret = false;
      }
    }
  }
  return bret;
}

int ObTabletInfoMap::get_exist_keys(hash::ObHashSet<ObTabletID> &exist_key_set) const
{
  int ret = OB_SUCCESS;
  for (ObTabletInfoMap::TabletInfoMap::const_iterator iter = map_.begin();
       OB_SUCC(ret) && iter != map_.end(); ++iter) {
    if (OB_FAIL(exist_key_set.set_refactored(iter->first))) {
      LOG_WARN("failed to push back key", KR(ret), "key", iter->first);
    }
  } // end of for
  return ret;
}


int64_t ObStateCollector::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t idx = 0; idx < STATE_ARRAY_SIZE; ++idx) {
    J_KV(compaction_state_to_str(ObLSCompactionState(idx)), state_cnt_[idx]);
    if (idx != STATE_ARRAY_SIZE - 1) {
      J_COMMA();
    }
  }
  J_OBJ_END();
  return pos;
}

#ifdef ERRSIM
void errsim_update_ls_state(const ObLSCompactionState &ls_state, ObStateCollector &collector)
{
  int ret = OB_SUCCESS;
  ret = OB_E(EventTable::EN_SHARED_STORAGE_DONT_UPDATE_LS_STATE) ret;
  if (OB_FAIL(ret)) {
    if (ls_state == -ret) {
      LOG_INFO("ERRSIM EN_SHARED_STORAGE_DONT_UPDATE_LS_STATE", KR(ret), K(ls_state), K(collector));
      collector.reset();
    }
  }
}
#endif

/**
 * -------------------------------------------------------------------ObLSCompactionStatusObj-------------------------------------------------------------------
 */
#define K_STATE "state", compaction_state_to_str(get_state())
#define K_EXE_MODE "exec_mode", exec_mode_to_str(exec_mode_)

#define LS_LEADER_SET_STATE(state) set_state(state, ObServerCompactionEvent::LS_LEADER, __FUNCTION__)
#define LS_LEADER_SET_STATE_WITHOUT_LOCK(state) inner_set_state(state, ObServerCompactionEvent::LS_LEADER, __FUNCTION__)
#define LS_SVR_SET_STATE(state) set_state(state, ObServerCompactionEvent::LS_SVR, __FUNCTION__)
#define LS_SVR_SET_STATE_WITHOUT_LOCK(state) inner_set_state(state, ObServerCompactionEvent::LS_SVR, __FUNCTION__)

int ObTabletCompactionState::update(
    const ObTabletCompactionState &input_state)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input_state.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input state is invalid", KR(ret), K(input_state));
  } else if (OB_UNLIKELY(input_state.compaction_scn_ < compaction_scn_)) {
    ret  = OB_EAGAIN;
    LOG_WARN("input state have smaller compaction scn", KR(ret), K(input_state), KPC(this));
  } else if (input_state.compaction_scn_ > compaction_scn_) {
    *this = input_state;
  } else { // input_state.compaction_scn_ == compaction_scn_
    clog_submitted_ |= input_state.clog_submitted_;
    output_ |= input_state.output_;
    calc_ckm_ |= input_state.calc_ckm_;
    verified_ |= input_state.verified_;
    refreshed_ |= input_state.refreshed_;
    skip_ |= input_state.skip_;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result state is invalid", KR(ret), KPC(this));
    }
  }
  return ret;
}

ObTabletCompactionState& ObTabletCompactionState::operator=(const ObTabletCompactionState& other)
{
  compaction_scn_ = other.compaction_scn_;
  clog_submitted_ = other.clog_submitted_;
  output_ = other.output_;
  calc_ckm_ = other.calc_ckm_;
  verified_ = other.verified_;
  refreshed_ = other.refreshed_;
  skip_ = other.skip_;
  return *this;
}

bool ObTabletCompactionState::operator==(const ObTabletCompactionState& other) const
{
  return compaction_scn_ == other.compaction_scn_
    && clog_submitted_ == other.clog_submitted_
    && output_ == other.output_
    && calc_ckm_ == other.calc_ckm_
    && verified_ == other.verified_
    && refreshed_ == other.refreshed_
    && skip_ == other.skip_;
}

ObLSCompactionStatusObj::ObLSCompactionStatusObj()
  : ObCompactionObjInterface(),
    version_(LS_COMPACTION_STATUS_VERSION_V1),
    state_(COMPACTION_STATE_MAX),
    reserved_(0),
    compaction_scn_(ObTenantLSMergeScheduler::INIT_COMPACTION_SCN),
    merged_scn_(ObTenantLSMergeScheduler::INIT_COMPACTION_SCN),
    lock_(),
    ls_id_(),
    verify_replica_cnt_(-1),
    loop_cnt_(0),
    update_state_ts_(0),
    new_micro_info_()
{}

ObLSCompactionStatusObj::~ObLSCompactionStatusObj()
{
  destroy();
}

int ObLSCompactionStatusObj::init(const ObLSID &input_ls_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSCompactionStatusObj has inited", KR(ret));
  } else if (OB_UNLIKELY(!input_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_ls_id));
  } else if (FALSE_IT(ls_id_ = input_ls_id)) {
  } else if (OB_FAIL(try_reload_obj())) {
    LOG_WARN("failed to reload obj", K(ret), K(input_ls_id));
  } else if (is_reload_obj()) {
    // do nothing
  } else {
    state_ = COMPACTION_STATE_IDLE;

    bool during_restore = false;
    if (OB_ISNULL(MERGE_SCHEDULER_PTR)) {
    } else if (OB_SUCCESS == MERGE_SCHEDULER_PTR->during_restore(during_restore) && !during_restore) {
      merged_scn_ = MERGE_SCHEDULER_PTR->get_inner_table_merged_scn();
      compaction_scn_ = merged_scn_;
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObLSCompactionStatusObj::init(const ObLSCompactionStatusInfo &ls_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSCompactionStatusObj has inited", KR(ret));
  } else if (OB_UNLIKELY(!ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_info));
  } else {
    ls_id_ = ls_info.ls_id_;
    state_ = ls_info.state_;
    compaction_scn_ = ls_info.compaction_scn_;
    merged_scn_ = ls_info.refreshed_scn_;
    new_micro_info_ = ls_info.new_micro_info_;
    is_inited_ = true;
  }
  return ret;
}

void ObLSCompactionStatusObj::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    state_ = COMPACTION_STATE_MAX;
    compaction_scn_ = 0;
    merged_scn_ = 0;
    ls_id_.reset();
    verify_replica_cnt_ = -1;
    loop_cnt_ = 0;
    update_state_ts_ = 0;
    new_micro_info_.reset();
  }
}

bool ObLSCompactionStatusObj::is_valid() const
{
  return is_inited_ && ls_id_.is_valid()
    && is_valid_compaction_state(get_state())
    && merged_scn_ >= 0
    && ((COMPACTION_STATE_IDLE == state_ && compaction_scn_ == merged_scn_)
      || (COMPACTION_STATE_IDLE != state_ && compaction_scn_ > merged_scn_));
}

int ObLSCompactionStatusObj::update_ls_state(
    const ObIArray<const ObSvrLSCompactionStatusObj*> &refresh_svr_obj_array)
{
  int ret = OB_SUCCESS;
  ObWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_valid_compaction_state(get_state()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state", KR(ret), K_STATE);
  } else {
    ObStateCollector collector;
    new_micro_info_.reset();
    for (int64_t obj_idx = 0; OB_SUCC(ret) && obj_idx < refresh_svr_obj_array.count(); ++obj_idx) {
      const ObSvrLSCompactionStatusObj &svr_obj = *refresh_svr_obj_array.at(obj_idx);
      if (compaction_scn_ > svr_obj.compaction_scn_) {
        LOG_TRACE("svr obj may record old tenant major state", KR(ret));
      } else { // equal compaction scn
        collector.add((ObLSCompactionState)svr_obj.state_);
        new_micro_info_.add(svr_obj.new_micro_info_);
      }
    } // end of for
#ifdef ERRSIM
    (void) errsim_update_ls_state(get_state(), collector);
#endif
    if (OB_FAIL(ret)) {
    } else if (collector.is_empty()) {
      // no need to update ls state
    } else if (OB_FAIL(inner_update_ls_state(collector))) {
      LOG_WARN("failed to update ls state", KR(ret), K(collector));
    }
  }
  return ret;
}

int ObLSCompactionStatusObj::inner_update_ls_state(ObStateCollector &collector)
{
  int ret = OB_SUCCESS;
  ObLSCompactionState result_state = ObLSCompactionState::COMPACTION_STATE_MAX;
  const int64_t inner_table_merged_scn = MERGE_SCHEDULER_PTR->get_inner_table_merged_scn();
  #define CNT_OF(STATE) collector.state_cnt_[COMPACTION_STATE_##STATE]
  if (OB_UNLIKELY(verify_replica_cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("verify_replica_cnt is invalid", KR(ret), K_(verify_replica_cnt));
  } else if (compaction_scn_ <= inner_table_merged_scn || merged_scn_ < inner_table_merged_scn) {
    FLOG_INFO("fix ls compaction status obj with inner table merged scn", KR(ret), KPC(this), K(inner_table_merged_scn));
    result_state = COMPACTION_STATE_IDLE;
    merged_scn_ = inner_table_merged_scn;
    compaction_scn_ = inner_table_merged_scn;
  } else if ((COMPACTION_STATE_REPLICA_VERIFY == state_ || COMPACTION_STATE_INDEX_VERIFY == state_)
      && (CNT_OF(COMPACT) < EXEC_REPLICA_CNT || (CNT_OF(CALC_CKM) + CNT_OF(REPLICA_VERIFY)) < verify_replica_cnt_)) {
    result_state = COMPACTION_STATE_COMPACT;
    LOG_INFO("meet unfinish ls", KR(ret), K_(ls_id), K(collector), K(result_state));
  } else {
    const bool single_replica = (0 == verify_replica_cnt_);
    switch (state_) {
    case COMPACTION_STATE_IDLE: {
      // do nothing
      break;
    }
    case COMPACTION_STATE_COMPACT: {
      if (CNT_OF(COMPACT) >= EXEC_REPLICA_CNT
          && CNT_OF(CALC_CKM) >= verify_replica_cnt_) {
        result_state = (single_replica ? COMPACTION_STATE_INDEX_VERIFY : COMPACTION_STATE_REPLICA_VERIFY);
      }
      break;
    }
    case COMPACTION_STATE_REPLICA_VERIFY: {
      if (CNT_OF(REPLICA_VERIFY) >= verify_replica_cnt_) {
        result_state = COMPACTION_STATE_INDEX_VERIFY;
      }
      break;
    }
    case COMPACTION_STATE_INDEX_VERIFY: {
      if (CNT_OF(INDEX_VERIFY) > 0) {
        // ls leader finished index ckm validation, advance the state to RS_VERIFY
        result_state = COMPACTION_STATE_RS_VERIFY;
      } else if (REACH_THREAD_TIME_INTERVAL(30_s)) {
        LOG_INFO("[SS_MERGE] wait for the svr to complete checksum validation", KR(ret), K_(ls_id), K_(compaction_scn));
      }
      break;
    }
    case COMPACTION_STATE_RS_VERIFY: {
      ObLSCompactionState state = COMPACTION_STATE_MAX;
      if (OB_FAIL(read_ls_compaction_status(state))) {
        LOG_WARN("failed to load ls compaction status", K(ret), K(state));
      } else if (ObLSCompactionState::COMPACTION_STATE_REFRESH == state) {
        result_state = COMPACTION_STATE_REFRESH;
      } else if (REACH_THREAD_TIME_INTERVAL(30_s)) { // no need to print for all ls
        LOG_INFO("[SS_MERGE] wait for all ls to complete chechsum validation", KR(ret), K_(ls_id),
          K_(compaction_scn), K(state), K(single_replica));
      }
      break;
    }
    case COMPACTION_STATE_REFRESH: {
      if (CNT_OF(REFRESH) >= verify_replica_cnt_ + EXEC_REPLICA_CNT) {
        result_state = COMPACTION_STATE_IDLE;
        merged_scn_ = compaction_scn_;
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid state", KR(ret), K_STATE);
    }
  }
  if (OB_SUCC(ret) && COMPACTION_STATE_MAX != result_state) {
    LS_LEADER_SET_STATE_WITHOUT_LOCK(result_state);
  }
  #undef CNT_OF
  return ret;
}

int ObLSCompactionStatusObj::read_ls_compaction_status(ObLSCompactionState &state)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLSCompactionStatusInfo, 1> status_info_array;
  ObSEArray<ObLSID, 1> ls_id_array;
  if (OB_FAIL(ls_id_array.push_back(ls_id_))) {
    LOG_WARN("failed to push ls_id", KR(ret), K_(ls_id));
  } else if (OB_FAIL(ObLSCompactionStatusObjLoader::load_ls_compaction_status_array(
                     MTL_ID(), ls_id_array, status_info_array))) {
    LOG_WARN("failed to load ls compaction status", K(ret), K(ls_id_array));
  } else if (OB_UNLIKELY(1 != status_info_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("return info count is unexpected", KR(ret), K(ls_id_array), K(status_info_array));
  } else {
    state = status_info_array.at(0).state_;
  }
  return ret;
}

int ObLSCompactionStatusObj::update(
    const ObIArray<const ObSvrLSCompactionStatusObj*> &refresh_svr_obj_array)
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLSCompactionStatusObj has not been inited", KR(ret));
  } else if (OB_UNLIKELY(refresh_svr_obj_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(ls_id), K(refresh_svr_obj_array));
  } else {
    for (int64_t obj_idx = 0; OB_SUCC(ret) && obj_idx < refresh_svr_obj_array.count(); ++obj_idx) {
      if (OB_UNLIKELY(nullptr == refresh_svr_obj_array.at(obj_idx)
          || !refresh_svr_obj_array.at(obj_idx)->is_valid())) {
        LOG_WARN("obj in array is invalid", KR(ret), K(obj_idx), KPC(refresh_svr_obj_array.at(obj_idx)));
      }
    } // end of for
    if (FAILEDx(update_ls_state(refresh_svr_obj_array))) {
      LOG_WARN("failed to update state in refresh", KR(ret), K_(ls_id), K(refresh_svr_obj_array));
    }
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("[SharedStorage] update ObLSCompactionStatusObj", K(cost_ts), K_(ls_id));
  return ret;
}

// will fill ObLSBroadcastInfo after refresh
int ObLSCompactionStatusObj::refresh(
    const bool is_leader,
    const ObSvrLSCompactionStatusObj &cur_svr_ls_compaction_status,
    ObLSBroadcastInfo &broadcast_info,
    ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLSCompactionStatusObj has not been inited", KR(ret));
  } else if (is_leader) { // leader: write obj to shared_storage
    const bool compacting_flag = is_compacting();
    if (is_extra_check_round()) {
      if (!compacting_flag && OB_FAIL(refresh_compaction_scn())) {
        LOG_WARN("failed to check compaction scn in refresh", KR(ret), K_(ls_id));
      } else if (compacting_flag && OB_FAIL(refresh_replica_cnt())) {
        LOG_WARN("failed to refresh replica cnt", KR(ret), K_(ls_id));
      }
    }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(EN_LS_LEADER_REFRESH)) {
      FLOG_INFO("ERRSIM EN_LS_LEADER_REFRESH", KR(ret));
    }
  }
#endif
    if (OB_SUCC(ret) && verify_replica_cnt_ < 0) { // need init verify replica
      if (OB_FAIL(refresh_replica_cnt())) {
        LOG_WARN("failed to refresh replica cnt", KR(ret), K_(ls_id));
      }
    }
    if (OB_SUCC(ret)) {
      fill_info(broadcast_info); // update after refresh
      if (compacting_flag && OB_FAIL(ObLSCompactionStatusObjLoader::load_to_update(
          ls_id_, cur_svr_ls_compaction_status, broadcast_info, *this, obj_buf))) {
        LOG_WARN("failed to read svr finish obj to update", KR(ret), K_(ls_id));
      } else {
#ifdef ERRSIM
        (void) errsim_fallback_compaction_scn();
#endif
        ObRLockGuard guard(lock_);
        if (OB_FAIL(write_object(obj_buf))) {
          LOG_WARN("failed to write svr ls compaction status obj", KR(ret), K_(ls_id), K(is_leader));
        } else {
          LOG_INFO("[SharedStorage] success to write ls compaction status", KR(ret), K_(ls_id), KPC(this), K(cur_svr_ls_compaction_status));
        }
      }
    }
  } else if (OB_FAIL(read_object(obj_buf))) {
    LOG_WARN("failed to read ls compaction status obj", KR(ret), K_(ls_id), K(is_leader));
  } else {
    fill_info(broadcast_info); // update after read new obj
    LOG_INFO("[SharedStorage] success to read ls compaction status obj", KR(ret), K_(ls_id), K(is_leader), K_STATE);
  }
  ++loop_cnt_;
  return ret;
}

bool ObLSCompactionStatusObj::is_compacting() const
{
  ObRLockGuard guard(lock_);
  return COMPACTION_STATE_IDLE != get_state();
}

int ObLSCompactionStatusObj::refresh_replica_cnt()
{
  int ret = OB_SUCCESS;
  ObLSInfo ls_info;
  if (OB_FAIL(MTL_LS_LOCALITY_CACHE.get_ls_info(ls_id_, ls_info))) {
    LOG_WARN("failed to get ls info", KR(ret), K_(ls_id));
  } else {
    const ObLSInfo::ReplicaArray &replicas = ls_info.get_replicas();
    int64_t data_replica_cnt = 0;
    int64_t skip_replica_cnt = 0;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < replicas.count(); ++idx) {
      const bool in_service = replicas.at(idx).is_in_service();
      const ObReplicaType &replica_type = replicas.at(idx).get_replica_type();
      if (in_service && ObReplicaTypeCheck::is_replica_with_ssstore(replica_type)) {
        ++data_replica_cnt;
      }
      LOG_DEBUG("replica type", KR(ret), K_(ls_id), K(data_replica_cnt), K(idx), K(in_service),
        "replica_type", ObShareUtil::replica_type_to_string(replica_type));
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_SHARED_STORAGE_IGNORE_REPLICA) ret;
      if (OB_FAIL(ret)) {
        skip_replica_cnt = 1;
        ret = OB_SUCCESS;
        LOG_INFO("ERRSIM EN_SHARED_STORAGE_IGNORE_REPLICA", K(ret));
      }
    }
#endif
    if (OB_SUCC(ret)) {
      const int64_t calc_verify_replica_cnt = data_replica_cnt - EXEC_REPLICA_CNT - skip_replica_cnt;
      if (OB_UNLIKELY(calc_verify_replica_cnt < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected data replica cnt", KR(ret), K_(ls_id), K(data_replica_cnt), K(calc_verify_replica_cnt), K(replicas));
      } else {
        ObWLockGuard guard(lock_);
        if (verify_replica_cnt_ != calc_verify_replica_cnt) {
          verify_replica_cnt_ = calc_verify_replica_cnt;
          LOG_INFO("success to init verify replica cnt", KR(ret), K_(ls_id), K_(verify_replica_cnt));
        }
      }
    }
  }
  return ret;
}

int ObLSCompactionStatusObj::refresh_compaction_scn(const int64_t input_merge_version)
{
  int ret = OB_SUCCESS;
  const int64_t broadcast_version = 0 == input_merge_version
                                  ? MTL(ObTenantLSMergeScheduler *)->get_frozen_version()
                                  : input_merge_version;

  if (IS_NOT_INIT) { // restart during major compaction, wait for the ls obj to init
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLSCompactionStatusObj has not been inited", KR(ret));
  } else if (broadcast_version > ObTenantLSMergeScheduler::INIT_COMPACTION_SCN) {
    ObWLockGuard guard(lock_);
    if (OB_UNLIKELY(compaction_scn_ >= broadcast_version)) {
      // do nothing, compaction scn in obj may large than inner table after restart
    } else if (COMPACTION_STATE_IDLE == state_) {
      int64_t tablet_cnt = 0;
      if (OB_FAIL(get_tablet_id_cnt(ls_id_, tablet_cnt))) {
        LOG_WARN("failed to get tablet id cnt", KR(ret), K_(ls_id));
      } else if (tablet_cnt <= 0) {
        compaction_scn_ = broadcast_version;
        merged_scn_ = broadcast_version;
        LOG_INFO("[SS_MERGE] no tablet on ls, just change state", KR(ret),
          K_(ls_id), K_(compaction_scn), K_STATE);
      } else if (OB_FAIL(refresh_replica_cnt())) {
        LOG_WARN("failed to refresh replica cnt", KR(ret));
      } else { // update compaction scn after refresh replica cnt
        compaction_scn_ = broadcast_version;
        LS_LEADER_SET_STATE_WITHOUT_LOCK(COMPACTION_STATE_COMPACT);
        LOG_INFO("start schedule major in ls", KR(ret), K_(ls_id), K_(compaction_scn), K_STATE);
      }
    } else {
      LOG_INFO("[SS_MERGE] cur round is not finish, cannot schedule new round major", KR(ret),
        K_(ls_id), K(broadcast_version), K_(compaction_scn), K_STATE);
    }
  }
  return ret;
}

int ObLSCompactionStatusObj::report_ls_index_verified(
    const int64_t compaction_scn,
    const bool is_svr_obj)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSCompactionStatusObj not inited", KR(ret));
  } else if (OB_UNLIKELY(compaction_scn != compaction_scn_ || compaction_scn < merged_scn_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(compaction_scn), K(compaction_scn_), K(merged_scn_));
  } else if (is_svr_obj) {
    LS_SVR_SET_STATE(ObLSCompactionState::COMPACTION_STATE_INDEX_VERIFY);
  } else {
    LS_LEADER_SET_STATE(ObLSCompactionState::COMPACTION_STATE_RS_VERIFY);
  }
  return ret;
}

bool ObLSCompactionStatusObj::operator==(const ObLSCompactionStatusObj& other) const
{
  return info_ == other.info_
    && compaction_scn_ == other.compaction_scn_
    && merged_scn_ == other.merged_scn_;
}

void ObLSCompactionStatusObj::fill_info(ObLSBroadcastInfo &info) const
{
  ObRLockGuard guard(lock_);
  info.compaction_scn_ = compaction_scn_;
  info.state_ = get_state();
}

void ObLSCompactionStatusObj::fill_info(ObLSCompactionStatusInfo &info) const
{
  ObRLockGuard guard(lock_);
  info.ls_id_ = ls_id_.id();
  info.compaction_scn_ = compaction_scn_;
  info.refreshed_scn_ = merged_scn_;
  info.state_ = get_state();
  info.new_micro_info_ = new_micro_info_;
}

void ObLSCompactionStatusObj::fill_info(ObVirtualTableInfo &info) const
{
  info.type_ = LS_COMPACTION_STATUS;
  ObRLockGuard guard(lock_);
  info.ls_id_ = ls_id_;
  info.last_refresh_ts_ = last_refresh_ts_;
  ADD_COMPACTION_INFO_PARAM(info.buf_, OB_MAX_VARCHAR_LENGTH, K_(compaction_scn),
    K_(merged_scn), "expect_state", compaction_state_to_str(get_state()), K_(loop_cnt), K_(new_micro_info));
}

void ObLSCompactionStatusObj::set_state(
  const ObLSCompactionState &input_state,
  const ObServerCompactionEvent::ObCompactionRole role,
  const char *function_name)
{
  ObWLockGuard guard(lock_);
  inner_set_state(input_state, role, function_name);
}

void ObLSCompactionStatusObj::inner_set_state(
  const ObLSCompactionState &input_state,
  const ObServerCompactionEvent::ObCompactionRole role,
  const char *function_name)
{
  if (state_ != input_state) {
    FLOG_INFO("[SS_MERGE] change state", K_(ls_id), K_(compaction_scn),
              "Role", ObServerCompactionEvent::get_comp_role_str(role),
              "old_state", compaction_state_to_str(get_state()),
              "new_state", compaction_state_to_str(input_state),
              KCSTRING(function_name), K_(verify_replica_cnt));

    ADD_ROLE_COMPACTION_EVENT(
        role,
        compaction_scn_,
        ObServerCompactionEvent::LS_STATE_CHANGED,
        ObTimeUtility::fast_current_time(),
        K_(ls_id),
        "old_state", compaction_state_to_str(get_state()),
        "new_state", compaction_state_to_str(input_state),
        K_(merged_scn),
        KCSTRING(function_name),
        K_(loop_cnt));
    state_ = input_state;
    update_state_ts_ = ObTimeUtility::fast_current_time();
  }
}

void ObLSCompactionStatusObj::get_new_micro_info(ObNewMicroInfo &new_micro_info) const
{
  ObRLockGuard guard(lock_);
  new_micro_info.add_meta_micro_size(new_micro_info_.get_meta_micro_size());
  new_micro_info.add_data_micro_size(new_micro_info_.get_data_micro_size());
}

OB_SERIALIZE_MEMBER_SIMPLE(ObLSCompactionStatusObj,
      info_,
      compaction_scn_,
      merged_scn_,
      new_micro_info_);

void ObLSCompactionStatusObj::set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const
{
  opt.set_ss_compaction_scheduler_object_opt(ObStorageObjectType::LS_COMPACTION_STATUS, ls_id_.id());
}

#ifdef ERRSIM
void ObLSCompactionStatusObj::errsim_fallback_compaction_scn()
{
  int ret = OB_SUCCESS;
  if (EN_FALLBACK_COMPACTION_SCN) {
    ObWLockGuard guard(lock_);
    compaction_scn_ = MTL(ObTenantLSMergeScheduler *)->get_inner_table_merged_scn();
    merged_scn_ = 1;
    state_ = COMPACTION_STATE_COMPACT;
    FLOG_INFO("errsim EN_FALLBACK_COMPACTION_SCN", KR(ret), KPC(this));
  }
}
#endif

/**
 * -------------------------------------------------------------------ObSvrLSCompactionStatusObj-------------------------------------------------------------------
 */
ObSvrLSCompactionStatusObj::ObSvrLSCompactionStatusObj()
  : ObLSCompactionStatusObj(),
    tablet_info_map_(),
    svr_id_(0),
    exec_mode_(OBJ_EXEC_MODE_MAX),
    target_state_(COMPACTION_STATE_MAX)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_STATE_MAX) == ARRAYSIZEOF(JUDGE_TABLET_FUNC), "compaction state func len is mismatch");
}

bool ObSvrLSCompactionStatusObj::is_valid() const
{
  return is_inited_ && ls_id_.is_valid()
    && is_valid_compaction_state(get_state())
    && tablet_info_map_.is_valid()
    && compaction_scn_ >= merged_scn_;
}

int ObSvrLSCompactionStatusObj::init(
    const uint64_t input_svr_id,
    const ObLSID &input_ls_id,
    const ObjExecMode exec_mode)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSvrLSCompactionStatusObj has inited", KR(ret));
  } else if (OB_UNLIKELY(0 == input_svr_id || !input_ls_id.is_valid() || !is_valid_obj_exec_mode(exec_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_svr_id), K(input_ls_id), K(exec_mode));
  } else if (FALSE_IT(svr_id_ = input_svr_id)) {
  } else if (OB_FAIL(ObLSCompactionStatusObj::init(input_ls_id))) {
    LOG_WARN("failed to init", KR(ret), K(input_ls_id));
  } else if (OB_FAIL(tablet_info_map_.init())) {
    LOG_WARN("failed to init tablet info map", KR(ret));
  } else {
    exec_mode_ = exec_mode;
  }
  return ret;
}

int ObSvrLSCompactionStatusObj::get_tablet_state(
    const ObTabletID &tablet_id,
    ObTabletCompactionState &tablet_state) const
{
  tablet_state.reset();
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSvrLSCompactionStatusObj has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(ls_id), K(tablet_id));
  } else {
    ObRLockGuard guard(lock_);
    if (OB_FAIL(tablet_info_map_.get_refactored(tablet_id, tablet_state))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from map", KR(ret), K_(ls_id), K(tablet_id), K(tablet_state));
      }
    }
  }
  return ret;
}

int ObSvrLSCompactionStatusObj::update_tablet_state(
  const ObTabletID &tablet_id,
  const ObTabletCompactionState &input_state,
  const ObNewMicroInfo *input_new_micro_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSvrLSCompactionStatusObj has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !input_state.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(ls_id), K(tablet_id), K(input_state));
  } else if (OB_UNLIKELY(OBJ_EXEC_MODE_WRITE_ONLY != exec_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exec mode for update tablet state", KR(ret), K_EXE_MODE);
  } else {
    ObWLockGuard guard(lock_);
    ret = inner_update_tablet_state(tablet_id, input_state);
    if (OB_SUCC(ret) && OB_NOT_NULL(input_new_micro_info)) {
      (void) new_micro_info_.add(*input_new_micro_info);
      LOG_INFO("success to update tablet state", KR(ret), KPC(input_new_micro_info));
    }
  }
  return ret;
}

int ObSvrLSCompactionStatusObj::inner_update_tablet_state(
    const ObTabletID &tablet_id,
    const ObTabletCompactionState &input_state)
{
  int ret = OB_SUCCESS;
  ObTabletCompactionState tmp_state;
  if (OB_FAIL(tablet_info_map_.get_refactored(tablet_id, tmp_state))) {
    if (OB_HASH_NOT_EXIST == ret) {
      tmp_state = input_state;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get from tablet info map", KR(ret), K(tablet_id));
    }
  } else if (OB_FAIL(tmp_state.update(input_state))) {
    LOG_WARN("failed to update state", KR(ret), K(tablet_id), K(input_state), K(tmp_state));
  }
  if (FAILEDx(tablet_info_map_.set_refactored(tablet_id, tmp_state, 1/*overwrite*/))) {
    LOG_WARN("failed to set map", KR(ret), K_(ls_id), K(tablet_id), K(tmp_state));
  } else {
    LOG_INFO("[SharedStorage] success to update state", KR(ret), K_(ls_id), K(tablet_id),
      K(input_state), K(tmp_state), K(tablet_info_map_.size()));
  }
  return ret;
}

static bool always_return_false(
  const ObTabletCompactionState tmp_state,
  const int64_t input_compaction_scn)
{
  return false;
}
#define JUDGE_TABLET_SCN(name)                                                 \
  static bool judge_tablet_##name(const ObTabletCompactionState tmp_state,     \
                      const int64_t input_compaction_scn) {                    \
    return tmp_state.get_##name() < input_compaction_scn;                      \
  }

JUDGE_TABLET_SCN(output_scn);
JUDGE_TABLET_SCN(calc_ckm_scn);
JUDGE_TABLET_SCN(verified_scn);
JUDGE_TABLET_SCN(refreshed_scn);
#undef JUDGE_TABLET_SCN

bool judge_tablet_refresh_in_idle(
  const ObTabletCompactionState tmp_state,
  const int64_t input_compaction_scn) {
  bool bret = false;
  if (tmp_state.get_skip_scn() < input_compaction_scn) {
    bret = tmp_state.get_refreshed_scn() < input_compaction_scn;
  }
  return bret;
}

ObSvrLSCompactionStatusObj::JudgeTabletFunc
  ObSvrLSCompactionStatusObj::JUDGE_TABLET_FUNC[ObLSCompactionState::COMPACTION_STATE_MAX] = {
    judge_tablet_refresh_in_idle,// IDLE
    judge_tablet_output_scn,     // COMPACT
    judge_tablet_calc_ckm_scn,   // CAL_CKM
    judge_tablet_verified_scn,   // REPLICA_VERIFY
    always_return_false,         // INDEX_VERIFY
    always_return_false,         // RS_VERIFY
    judge_tablet_refreshed_scn,  // REFRESH
  };

int ObSvrLSCompactionStatusObj::get_unsubmitted_clog_tablet(
    const int64_t merge_version,
    ObIArray<ObTabletID> &schedule_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, TABLET_ARRAY_SIZE> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(MTL_ID(), "ObjTabletIDs"));

  if (OB_FAIL(get_tablet_ids(ls_id_, tablet_ids))) {
    LOG_WARN("failed to get tablet id", KR(ret), K_(ls_id));
  } else {
    ObRLockGuard guard(lock_);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_ids.count(); ++idx) {
      const ObTabletID &tablet_id = tablet_ids.at(idx);
      ObTabletCompactionState tablet_state;

      if (OB_FAIL(tablet_info_map_.get_refactored(tablet_id, tablet_state))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get tablet state from map", KR(ret), K(tablet_id));
        } else if (OB_FAIL(schedule_tablet_ids.push_back(tablet_id))) {
          LOG_WARN("failed to push back tablet id", KR(ret), K(tablet_id));
        }
      } else if (tablet_state.get_submitted_log_scn() >= merge_version) {
        // tablet has submitted clog, do nothing
      } else if (OB_FAIL(schedule_tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet id", KR(ret), K(tablet_id));
      }
    }
  }
  return ret;
}


#ifdef ERRSIM
void errsim_refresh(const ObTabletID &tablet_id, const ObLSBroadcastInfo &info, bool &schedule_flag)
{
  int ret = OB_SUCCESS;
  ret = OB_E(EventTable::EN_SHARED_STORAGE_SKIP_USER_TABLET_REFRESH) ret;
  if (OB_FAIL(ret)) {
    if (tablet_id.id() > ObTabletID::MIN_USER_TABLET_ID) {
      schedule_flag = false;
      LOG_INFO("ERRSIM EN_SHARED_STORAGE_SKIP_USER_TABLET_REFRESH", KR(ret),
        K(tablet_id), K(info));
    }
  }
}

void errsim_schedule_in_idle(bool &schedule_flag)
{
  int ret = OB_SUCCESS;
  ret = OB_E(EventTable::EN_SHARED_STORAGE_SCHEULD_TABLET_IN_IDLE) ret;
  if (OB_FAIL(ret)) {
    schedule_flag = true;
    LOG_INFO("ERRSIM EN_SHARED_STORAGE_SCHEULD_TABLET_IN_IDLE", KR(ret));
  }
}

#endif

/* The interface won't return the following tablets:
 * 1. The DURING DDL tablet
 *    - update tablet state directly
 * 2. The tablet has no major and has no clog
 *    - do not add tablet state, wait for the next schedule
 * 3. The tablet has large major SST
 *    - refresh scn > merge version, no need return
 */
int ObSvrLSCompactionStatusObj::get_schedule_tablet(
    ObLSCompactionListObj &list_obj,
    ObLSBroadcastInfo &info,
    ObLS &ls,
    common::ObIArray<ObTabletID> &schedule_tablet_ids,
    common::ObIArray<ObTabletID> &no_inc_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, TABLET_ARRAY_SIZE> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(MTL_ID(), "ObjTabletIDs"));
  bool schedule_flag = true;

  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(info));
  } else if (!info.need_schedule()) {
    LOG_TRACE("should not schedule", K(info), KPC(this));
    if (info.state_ == COMPACTION_STATE_IDLE &&
        info.compaction_scn_ == compaction_scn_ &&
        info.compaction_scn_ > merged_scn_) {
      ObWLockGuard guard(lock_);
      LS_SVR_SET_STATE_WITHOUT_LOCK(info.get_result_state_for_cur_svr());
      merged_scn_ = compaction_scn_;
    }
    if (COMPACTION_STATE_IDLE == info.state_ && REACH_THREAD_TIME_INTERVAL(10_min)) {
      // need refresh new major for migrated tablet with old major after tenant major finish
      schedule_flag = true;
    } else {
      schedule_flag = false;
#ifdef ERRSIM
      errsim_schedule_in_idle(schedule_flag);
#endif
    }
  }

  bool exist_no_clog_tablet = false;
  if (OB_FAIL(ret) || !schedule_flag) {
    // do nothing
  } else if (OB_FAIL(ls.get_tablet_svr()->get_all_tablet_ids(true/*except_ls_inner_tablet*/, tablet_ids))) {
    LOG_WARN("failed to get schedule tablets", K(ret), K_(ls_id), K(info));
  } else if (tablet_ids.empty()) {
    LS_SVR_SET_STATE(info.get_result_state_for_cur_svr());
    LOG_INFO("tablet array is empty, change state now", KR(ret), K_(ls_id), K(info), K_STATE);
  } else if (FALSE_IT(try_update_scn(info))) {
  } else if (OB_FAIL(loop_map_to_get_tablets(tablet_ids, list_obj, info, ls, schedule_tablet_ids, no_inc_tablet_ids, exist_no_clog_tablet))) {
    LOG_WARN("failed to get schedule tablets", K(ret), K(info));
  } else if (schedule_tablet_ids.empty() && no_inc_tablet_ids.empty() && !exist_no_clog_tablet) {
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
    if (OB_FAIL(ls.get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", KR(ret), K(ls));
    } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == migration_status) {
      LS_SVR_SET_STATE(info.get_result_state_for_cur_svr());
    }
  } else if (COMPACTION_STATE_IDLE == info.state_) {
    info.state_ = COMPACTION_STATE_REFRESH;
    FLOG_INFO("ls merge finished, but still have tablet with less compaction scn", KR(ret), K(info),
              "schedule_tablet_cnt", schedule_tablet_ids.count(),
              "no_inc_tablet_cnt", no_inc_tablet_ids.count()/*should be empty*/);
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("ss_merge_errsim", "refresh_in_idle", "ls_id", ls.get_ls_id());
#endif
  } else {
    LOG_INFO("get tablet ids from s2", K(ret), K_(ls_id), K(info), K(tablet_ids.count()),
             K(exist_no_clog_tablet), K(schedule_tablet_ids), K(no_inc_tablet_ids));
  }
  if (OB_SUCC(ret)) {
    (void) update_target_state(info);
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(EN_COMPACTION_GET_SCHEDULE_TABLET)) {
      FLOG_INFO("ERRSIM EN_COMPACTION_GET_SCHEDULE_TABLET", KR(ret));
    }
  }
#endif
  return ret;
}

int ObSvrLSCompactionStatusObj::deal_with_not_exist_tablet(
    const ObTabletID &tablet_id,
    const ObLSBroadcastInfo &info,
    ObLS &ls,
    ObTabletCompactionState &tablet_state)
{
  int ret = OB_SUCCESS;
  // tablet has no old state, check medium info and generate a new state
  ObTabletHandle tablet_hdl;

  if (OB_FAIL(ls.get_tablet_svr()->get_tablet(tablet_id, tablet_hdl, 0 /*timeout*/, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      tablet_state.set_skip_scn(info.compaction_scn_);
    } else {
      LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
    }
  } else if (FALSE_IT(tablet_state.set_refreshed_scn(tablet_hdl.get_obj()->get_last_major_snapshot_version()))) {
  } else if (tablet_state.get_refreshed_scn() >= info.compaction_scn_) {
    // no need to check medium info on tablet, update state directly
  } else if (OB_FAIL(gene_tablet_state(*tablet_hdl.get_obj(), tablet_state))) {
    LOG_WARN("failed to gene tablet state", K(ret), K(tablet_id), K(tablet_state));
  }
  return ret;
}

int ObSvrLSCompactionStatusObj::loop_map_to_get_tablets(
    const ObIArray<ObTabletID> &input_tablet_ids,
    ObLSCompactionListObj &list_obj,
    ObLSBroadcastInfo &info,
    ObLS &ls,
    ObIArray<ObTabletID> &merge_tablet_ids,
    ObIArray<ObTabletID> &no_inc_tablet_ids,
    bool &exist_no_clog_tablet)
{
  int ret = OB_SUCCESS;
  const bool is_erase_round = is_erase_tablet_round();
  TabletIDSet exist_key_set;
  if (!is_erase_round) {
    // do nothing
  } else if (OB_FAIL(exist_key_set.create(256, ObMemAttr(MTL_ID(), "ExistSet")))) {
    LOG_WARN("failed to create exist key set", K(ret));
  } else {
    ObRLockGuard guard(lock_);
    if (OB_FAIL(tablet_info_map_.get_exist_keys(exist_key_set))) {
      LOG_WARN("failed to get exist keys", KR(ret), KPC(this));
    }
  }

  ObSEArray<ObTabletID, 128> schedule_tablet_ids;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < input_tablet_ids.count(); ++idx) {
    const ObTabletID &tablet_id = input_tablet_ids.at(idx);
    ObTabletCompactionState tablet_state;
    bool is_not_exist_tablet = false;

    // get old state from info map
    if (OB_FAIL(get_tablet_state(tablet_id, tablet_state))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        is_not_exist_tablet = true;
      } else {
        LOG_WARN("failed to get from map", KR(ret), K(tablet_id));
      }
    } else if (is_erase_round && OB_FAIL(exist_key_set.erase_refactored(tablet_id))) { // has state in obj, remove from the set
      LOG_WARN("failed to erase from key set", KR(ret), K(tablet_id));
    } else if (tablet_state.get_skip_scn() >= info.compaction_scn_
            || tablet_state.get_refreshed_scn() >= info.compaction_scn_) {
      LOG_INFO("tablet no need to schedule", K(tablet_id), K(tablet_state), K(info));
      continue; // tablet no need to schedule, loop the next tablet
    }

    // check list obj for all tablets
    bool need_skip = false;
    if (FAILEDx(list_obj.tablet_need_skip(info.compaction_scn_, tablet_id, need_skip))) {
      LOG_WARN("failed to check tablet need skip", K(ret), K(tablet_id));
    } else if (need_skip) {
      tablet_state.set_skip_scn(info.compaction_scn_); // update state directly
      if (OB_FAIL(update_tablet_state(tablet_id, tablet_state))) {
        LOG_WARN("failed to update tablet state", KR(ret), K(tablet_id), K(tablet_state));
      }
    } else if (is_not_exist_tablet && OB_FAIL(deal_with_not_exist_tablet(tablet_id, info, ls, tablet_state))) {
      LOG_WARN("failed to deal with not exist tablet", K(ret), K(tablet_id), K(info));
    } else if (OB_FAIL(push_tablet_by_state(tablet_id, tablet_state, info, schedule_tablet_ids))) {
      LOG_WARN("failed to push tablet by state", KR(ret), K(tablet_id), K(tablet_state));
    } else if (!is_not_exist_tablet || !tablet_state.is_valid()) {
      // do nothing
    } else if (OB_FAIL(update_tablet_state(tablet_id, tablet_state))) {
      LOG_WARN("failed to update tablet state", KR(ret), K(tablet_id), K(tablet_state));
    }
  } // end for

  if (FAILEDx(inner_check_merge_reason(info, schedule_tablet_ids, list_obj, ls, merge_tablet_ids, no_inc_tablet_ids, exist_no_clog_tablet))) {
    LOG_WARN("failed to check merge reason", K(ret), K(info));
  }

  if (OB_SUCC(ret) && is_erase_round && exist_key_set.size() > 0) {
    LOG_INFO("success to erase keys from map", KR(ret), K(exist_key_set.size()));
    ObWLockGuard guard(lock_);
    // the remain tablets in set exists in obj, but not exists in input tablet ids, need remove from obj
    for (TabletIDSet::iterator iter = exist_key_set.begin(); iter != exist_key_set.end(); ++iter) {
      (void) tablet_info_map_.erase_refactored(iter->first);
    }
  }
  return ret;
}

int ObSvrLSCompactionStatusObj::inner_check_merge_reason(
    const ObLSBroadcastInfo &info,
    const ObIArray<ObTabletID> &schedule_tablet_ids,
    ObLSCompactionListObj &list_obj,
    ObLS &ls,
    ObIArray<ObTabletID> &merge_tablet_ids,
    ObIArray<ObTabletID> &no_inc_tablet_ids,
    bool &exist_no_clog_tablet)
{
  int ret = OB_SUCCESS;
  exist_no_clog_tablet = false;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < schedule_tablet_ids.count(); ++idx) {
    const ObTabletID &tablet_id = schedule_tablet_ids.at(idx);
    ObTabletHandle tablet_hdl;
    ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE;
    ObTabletCompactionState tablet_state;

    if (OB_FAIL(ls.get_tablet_svr()->get_tablet(tablet_id, tablet_hdl, 0 /*timeout*/, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        tablet_state.set_skip_scn(info.compaction_scn_);
      } else {
        LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
      }
    } else if (tablet_hdl.get_obj()->get_last_major_snapshot_version() >= info.compaction_scn_) {
      // no need to check medium info, only update tablet state
      tablet_state.set_refreshed_scn(tablet_hdl.get_obj()->get_last_major_snapshot_version());
      LOG_INFO("tablet has refreshed new major, update tablet state directly", K(tablet_id), K(info), K(tablet_state));
    } else if (OB_FAIL(MTL(ObTenantLSMergeScheduler *)->get_tablet_merge_reason(info.compaction_scn_,
                                                                                ls.get_ls_id(),
                                                                                *tablet_hdl.get_obj(),
                                                                                merge_reason))) {
      if (OB_ITER_END == ret || OB_ENTRY_NOT_EXIST == ret) {
        /* tablet has not synced clog yet
         * 1. tablet has no major, tablet state is invalid, won't add to info map when generate tablet state
         * 2. tablet has major, update tablet state directly
         */
        ret = OB_SUCCESS;
        exist_no_clog_tablet = true; // cannot change ls svr state
      } else {
        LOG_WARN("failed to get tablet merge reason", K(ret), K(info), K(tablet_id), "ls_id", ls.get_ls_id());
      }
    } else if (ObAdaptiveMergePolicy::DURING_DDL == merge_reason) {
      if (info.is_leader_ && OB_FAIL(list_obj.add_skip_tablet(info.compaction_scn_, tablet_id))) {
        LOG_WARN("failed to add skip tablet info to list obj", K(ret), K(info), K(tablet_id));
      } else {
        tablet_state.set_skip_scn(info.compaction_scn_);
      }
    } else if (ObAdaptiveMergePolicy::NO_INC_DATA == merge_reason) {
      if (OB_FAIL(no_inc_tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to add no inc tablet id", K(ret));
      }
    } else if (OB_FAIL(merge_tablet_ids.push_back(tablet_id))) {
      LOG_WARN("failed to add merge tablet id", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (tablet_state.get_skip_scn() >= info.compaction_scn_
            || tablet_state.get_refreshed_scn() >= info.compaction_scn_) {
      if (OB_FAIL(update_tablet_state(tablet_id, tablet_state))) {
        LOG_WARN("failed to update tablet state", KR(ret), K(tablet_id), K(tablet_state));
      }
    }
  }
  return ret;
}

void ObSvrLSCompactionStatusObj::try_update_scn(const ObLSBroadcastInfo &info)
{
  ObWLockGuard guard(lock_);
  const int64_t inner_table_merged_scn = MERGE_SCHEDULER_PTR->get_inner_table_merged_scn();
  if (info.compaction_scn_ > compaction_scn_ || inner_table_merged_scn >= compaction_scn_) {
    if (COMPACTION_STATE_IDLE != get_state()) {
      FLOG_INFO("unfinish ls svr status", K_STATE, K_(compaction_scn), K(info));
    }
    compaction_scn_ = MAX(info.compaction_scn_, inner_table_merged_scn);
    merged_scn_ = inner_table_merged_scn;
    new_micro_info_.reset();
    LS_SVR_SET_STATE_WITHOUT_LOCK(COMPACTION_STATE_IDLE);
    inner_update_target_state(info);
  }
}

void ObSvrLSCompactionStatusObj::update_target_state(const ObLSBroadcastInfo &info)
{
  ObWLockGuard guard(lock_);
  inner_update_target_state(info);
}

void ObSvrLSCompactionStatusObj::inner_update_target_state(const ObLSBroadcastInfo &info)
{
  if (info.need_schedule()) {
    target_state_ = info.get_result_state_for_cur_svr();
  } else if (COMPACTION_STATE_INDEX_VERIFY == info.state_ && info.is_leader_) {
    target_state_ = COMPACTION_STATE_INDEX_VERIFY;
  } else {
    target_state_ = COMPACTION_STATE_MAX; // means cur svr should waiting, no action
  }
}

int ObSvrLSCompactionStatusObj::gene_tablet_state(
    const storage::ObTablet &tablet,
    ObTabletCompactionState &tablet_state)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  const ObTabletTableStore *table_store = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(tablet_id));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("failed to get table store", K(ret), K(tablet_id));
  } else {
    const int64_t last_major_snapshot = tablet.get_last_major_snapshot_version();
    tablet_state.set_refreshed_scn(last_major_snapshot);
    if (!table_store->get_major_ckm_info().is_empty()) {
      const ObMajorChecksumInfo &ckm_info = table_store->get_major_ckm_info();
      const ObExecMode exec_mode = ckm_info.get_exec_mode();
      if (is_output_exec_mode(exec_mode)) {
        tablet_state.set_output_scn(ckm_info.get_compaction_scn());
      } else if (is_calc_ckm_exec_mode(exec_mode)) {
        tablet_state.set_calc_ckm_scn(ckm_info.get_compaction_scn());
      }
    }
  }
  return ret;
}

int ObSvrLSCompactionStatusObj::batch_update_tablet_state(
  const ObTabletCompactionState &tablet_state,
  const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSvrLSCompactionStatusObj has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!tablet_state.is_valid() || tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_state), K(tablet_ids));
  } else if (OB_UNLIKELY(OBJ_EXEC_MODE_WRITE_ONLY != exec_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exec mode for update tablet state");
  } else {
    int tmp_ret = OB_SUCCESS;
    int64_t succ_tablet_cnt = 0;
    {
      ObWLockGuard guard(lock_);
      for (int64_t idx = 0; idx < tablet_ids.count(); ++idx) {
        if (OB_TMP_FAIL(inner_update_tablet_state(tablet_ids.at(idx), tablet_state))) {
          LOG_WARN_RET(tmp_ret, "failed to update tablet state", K(idx), "tablet_id", tablet_ids.at(idx), K(tablet_state));
        } else {
          ++succ_tablet_cnt;
        }
      }
    } // end of lock
    LOG_INFO("success to update tablet state", KR(ret), "total_tablet_cnt", tablet_ids.count(), K(succ_tablet_cnt));
  }
  return ret;
}

int ObSvrLSCompactionStatusObj::push_tablet_by_state(
    const ObTabletID &tablet_id,
    const ObTabletCompactionState &tablet_state,
    ObLSBroadcastInfo &info,
    ObIArray<ObTabletID> &schedule_tablet_ids)
{
  int ret = OB_SUCCESS;
  bool schedule_flag = false;

  if (0 == tablet_state.get_submitted_log_scn()) { // tablet has no clog, no need to return
    LOG_INFO("tablet has no major and hasn't synced clog, wait for the next schedule", K(tablet_id), K(info), K(tablet_state));
  } else if (tablet_state.get_skip_scn() >= info.compaction_scn_
          || tablet_state.get_refreshed_scn() >= info.compaction_scn_) {
    // no need to schedule
    LOG_INFO("tablet no need to schedule", K(tablet_id), K(tablet_state), K(info)); // debug log
  } else if (info.state_ > COMPACTION_STATE_COMPACT && info.state_ < COMPACTION_STATE_REFRESH
     && (info.cur_svr_is_exec_svr() ? tablet_state.get_output_scn() : tablet_state.get_calc_ckm_scn()) < info.compaction_scn_) {
    // ls state is VERITY / REFRESH, but the cur tablet state has not merged or calc_ckm.
    FLOG_INFO("[SS_MERGE] exist unfinish tablet, clear array", KR(ret), K_STATE, K(info), K(tablet_id), K(tablet_state));
    info.state_ = COMPACTION_STATE_COMPACT;
    schedule_tablet_ids.reuse();
    schedule_flag = true;
    LS_SVR_SET_STATE(COMPACTION_STATE_IDLE);
  } else if (JUDGE_TABLET_FUNC[info.get_result_state_for_cur_svr()](tablet_state, info.compaction_scn_)) {
    schedule_flag = true;
  }
#ifdef ERRSIM
  if (OB_SUCC(ret) && schedule_flag && COMPACTION_STATE_REFRESH == info.state_) {
    errsim_refresh(tablet_id, info, schedule_flag);
  }
#endif
  if (OB_FAIL(ret) || !schedule_flag) {
  } else if (OB_FAIL(schedule_tablet_ids.push_back(tablet_id))) {
    LOG_WARN("failed to push tablet id", K(ret), K(tablet_id));
  } else {
    LOG_TRACE("tablet could schedule", KR(ret), K(info), K(tablet_id), K(tablet_state));
  }
  return ret;
}

int ObSvrLSCompactionStatusObj::refresh(
  const ObLSBroadcastInfo &info,
  ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObSvrLSCompactionStatusObj has not been inited", KR(ret));
  } else if (OBJ_EXEC_MODE_READ_ONLY == exec_mode_) {
    ObWLockGuard guard(lock_);
    if (OB_FAIL(read_object(obj_buf))) {
      LOG_WARN("failed to read svr ls compaction status obj", KR(ret), K_EXE_MODE);
    } else {
      LOG_INFO("[SharedStorage] success to load svr ls compaction status obj", KR(ret), K_(ls_id), K_EXE_MODE);
    }
  } else if (OBJ_EXEC_MODE_WRITE_ONLY == exec_mode_) {
    (void) try_update_scn(info);
#ifdef ERRSIM
    (void) errsim_fallback_compaction_scn();
#endif
    ObRLockGuard guard(lock_);
    if (OB_FAIL(write_object(obj_buf))) {
      LOG_WARN("failed to write svr ls compaction status obj", KR(ret), K_EXE_MODE);
    } else {
      LOG_INFO("[SharedStorage] success to write svr ls compaction status obj", KR(ret), K_(ls_id), K_STATE,
        K_EXE_MODE, K_(compaction_scn), K(tablet_info_map_.size()));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj exec mode", KR(ret), K_EXE_MODE);
  }
  ++loop_cnt_;
  return ret;
}

void ObSvrLSCompactionStatusObj::destroy()
{
  ObLSCompactionStatusObj::destroy();
  svr_id_ = 0;
  exec_mode_ = OBJ_EXEC_MODE_MAX;
  tablet_info_map_.destroy();
}

void ObSvrLSCompactionStatusObj::fill_info(ObVirtualTableInfo &info) const
{
  info.type_ = LS_SVR_COMPACTION_STATUS;
  ObRLockGuard guard(lock_);
  info.ls_id_ = ls_id_;
  info.last_refresh_ts_ = last_refresh_ts_;
#define ADD_PARAM(...) ADD_COMPACTION_INFO_PARAM(info.buf_, OB_MAX_VARCHAR_LENGTH, __VA_ARGS__)
  ADD_PARAM(K_(svr_id), K_(compaction_scn), K_(merged_scn),
    "finish_state", compaction_state_to_str(get_state()));
  if (compaction_scn_ > merged_scn_) {
    if (is_executing()) {
      ADD_PARAM("status", "executing", "target_state", compaction_state_to_str(target_state_));
    } else {
      ADD_PARAM("status", "waiting");
    }
  }
  ADD_PARAM(K_(loop_cnt), K_(new_micro_info));
}

OB_SERIALIZE_MEMBER_SIMPLE(ObSvrLSCompactionStatusObj,
      info_,
      compaction_scn_,
      new_micro_info_);

void ObSvrLSCompactionStatusObj::set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const
{
  opt.set_ss_compactor_ls_svr_object_opt(ObStorageObjectType::LS_SVR_COMPACTION_STATUS, ls_id_.id(), svr_id_);
}

/**
 * -------------------------------------------------------------------ObLSCompactionStatusObjLoader-------------------------------------------------------------------
 */

int ObLSCompactionStatusObjLoader::load_to_update(
    const ObLSID &ls_id,
    const ObSvrLSCompactionStatusObj &cur_svr_ls_compaction_status,
    const ObLSBroadcastInfo &info,
    ObLSCompactionStatusObj &input_obj,
    ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSvrPair, DEFAULT_ARRAY_CNT> svr_infos;
  svr_infos.set_attr(ObMemAttr(MTL_ID(), "TmpSvrINFO"));

  if (OB_FAIL(MTL_SVR_ID_CACHE.get_svr_infos(svr_infos))) {
    LOG_WARN("failed to get svr infos", KR(ret));
  } else if (svr_infos.count() > 0) {
    ObSvrLSCompactionStatusObj svr_array[1];
    ObSEArray<const ObSvrLSCompactionStatusObj*, DEFAULT_ARRAY_CNT> svr_obj_array;
    svr_obj_array.set_attr(ObMemAttr(MTL_ID(), "TmpSvrObj"));
    uint64_t svr_id = 0;
    if (OB_FAIL(ObSvrIDCache::get_svr_id_by_addr(svr_infos, GCTX.self_addr(), svr_id))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_TRACE("svr_id not cached member addr, need wait", KR(ret), K(ls_id));
      } else {
        LOG_WARN("failed to get svr id by addr", KR(ret), K(svr_infos));
      }
    } else if (GCTX.get_server_id() != svr_id) {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_SHARED_STORAGE_IGNORE_REPLICA) ret;
      if (OB_FAIL(ret)) {
        int64_t ignore_svr_id = -ret;
        ret = OB_SUCCESS;
        if (svr_id == ignore_svr_id) {
          LOG_INFO("ERRSIM EN_SHARED_STORAGE_IGNORE_REPLICA", K(ret));
        }
      }
    }
#endif
      if (OB_FAIL(svr_array[0].init(svr_id, ls_id, OBJ_EXEC_MODE_READ_ONLY))) {
        LOG_WARN("failed to init svr finish tablets", KR(ret), K(idx), K(svr_id), K(ls_id));
      } else if (OB_FAIL(svr_array[0].refresh(info, obj_buf))) {
        if (OB_OBJECT_NOT_EXIST == ret) {
          LOG_TRACE("object not exist, need wait", KR(ret), K(ls_id));
        } else {
          LOG_WARN("failed to refresh svr obj", KR(ret), K(ls_id), K(svr_id));
        }
      } else if (OB_FAIL(svr_obj_array.push_back(&svr_array[0]))) {
        LOG_WARN("failed to push back svr addr", KR(ret));
      } else {
        LOG_TRACE("success to read obj", KR(ret), K(ls_id), K(svr_id));
      }
    }
    if (FAILEDx(svr_obj_array.push_back(&cur_svr_ls_compaction_status))) {
      LOG_WARN("failed to push back svr addr", KR(ret));
    } else if (OB_FAIL(input_obj.update(svr_obj_array))) {
      LOG_WARN("failed to update ls finish tablets", KR(ret), K(svr_obj_array));
    } else {
      LOG_INFO("success to get ls svr state", KR(ret), K(ls_id), K(svr_obj_array), K(svr_infos), K(cur_svr_ls_compaction_status));
    }
  } else {
    LOG_INFO("empty member list or empty svr id cache", KR(ret), K(svr_infos));
  }
  ret = OB_OBJECT_NOT_EXIST == ret ? OB_SUCCESS : ret;
  return ret;
}

int ObLSCompactionStatusObjLoader::load_ls_compaction_status_array(
    const uint64_t tenant_id,
    const common::ObIArray<share::ObLSID> &ls_ids,
    common::ObIArray<ObLSCompactionStatusInfo> &ls_status_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || ls_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id array is empty", KR(ret), K(tenant_id), K(ls_ids));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSCompactionStatusObj tmp_obj;
      ObLSCompactionStatusInfo status_info;

      for (int64_t idx = 0; OB_SUCC(ret) && idx < ls_ids.count(); ++idx) {
        status_info.reset();
        tmp_obj.destroy();
        if (OB_FAIL(tmp_obj.init(ls_ids.at(idx)))) {
          LOG_WARN("failed to init obj", KR(ret), K(idx), "ls_id", ls_ids.at(idx));
        } else if (!tmp_obj.is_reload_obj()) {
          // s2 file is not exist
        } else if (FALSE_IT(tmp_obj.fill_info(status_info))) {
        } else if (OB_FAIL(ls_status_infos.push_back(status_info))) {
          LOG_WARN("failed to push status info", KR(ret), "ls_id", ls_ids.at(idx), K(status_info));
        }
      } // end of for
    }
  }
  return ret;
}

int ObLSCompactionStatusObjLoader::batch_update_ls_compaction_status(
    const uint64_t tenant_id,
    const common::ObIArray<ObLSCompactionStatusInfo> &ls_status_infos)
{
  int ret = OB_SUCCESS;
  ObCompactionObjBuffer obj_buf;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || ls_status_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id array is empty", KR(ret), K(tenant_id), K(ls_status_infos));
  } else if (OB_FAIL(obj_buf.init())) {
    LOG_WARN("failed to init obj buf", KR(ret));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSCompactionStatusObj tmp_obj;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < ls_status_infos.count(); ++idx) {
        tmp_obj.destroy();
        const ObLSCompactionStatusInfo &ls_info = ls_status_infos.at(idx);
#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(EN_RS_UPDATE_LS_STATE)) {
            if (idx == 1) { // skip one ls to mock the ls state is overwritten to RS_VERIFY by refresh
              FLOG_INFO("ERRSIM EN_RS_UPDATE_LS_STATE", KR(ret));
              SERVER_EVENT_SYNC_ADD("ss_merge_errsim", "skip_rs_update_ls_state", "tenant_id", MTL_ID(), "ls_id", ls_info.ls_id_);
              continue;
            }
          }
        }
#endif
        if (!ls_info.rs_update_state_) {
          // no need to update
        } else if (OB_FAIL(tmp_obj.init(ls_info))) {
          LOG_WARN("failed to init obj", KR(ret), K(idx), K(ls_info));
        } else if (OB_FAIL(tmp_obj.write_object(obj_buf))) {
          LOG_WARN("failed to write obj", KR(ret), K(ls_info));
        } else {
          LOG_INFO("success to write object", KR(ret), K(ls_info));
        }
      } // end of for
    } // MTL_SWITCH
  }
  return ret;
}

int ObLSCompactionStatusObjLoader::load_ls_compaction_list(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    compaction::ObLSCompactionListObj &input_obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", KR(ret), K(tenant_id), K(ls_id));
  } else {
    MTL_SWITCH(tenant_id) {
      if (OB_FAIL(input_obj.init(ls_id))) {
        LOG_WARN("failed to init list obj", K(ret));
      } else if (OB_UNLIKELY(!input_obj.is_reload_obj())) {
        ret = OB_OBJECT_NOT_EXIST;
        LOG_WARN("list obj not exist", K(ret), K(tenant_id), K(ls_id));
      }
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
