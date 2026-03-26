/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PALF
#include "palf_handle_lite.h"
#include "src/logservice/palf/palf_env_impl.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;
using namespace palf::election;
namespace palflite
{

PalfHandleLite::PalfHandleLite()
  : lock_(),
    config_mgr_(),
    mode_mgr_(),
    state_mgr_(),
    reconfirm_(),
    log_engine_(),
    election_(),
    allocator_(NULL),
    palf_id_(palf::INVALID_PALF_ID),
    self_(),
    fs_cb_wrapper_(),
    role_change_cb_wrpper_(),
    plugins_(),
    last_locate_scn_(),
    last_locate_block_(palf::LOG_INVALID_BLOCK_ID),
    last_check_parent_child_time_us_(OB_INVALID_TIMESTAMP),
    has_set_deleted_(false),
    palf_env_impl_(NULL),
    last_dump_info_ts_us_(OB_INVALID_TIMESTAMP),
    leader_changer_(),
    rc_cb_node_(&leader_changer_),
    is_inited_(false)
{
  log_dir_[0] = '\0';
}

PalfHandleLite::~PalfHandleLite()
{
  destroy();
}

int PalfHandleLite::init(const int64_t palf_id,
                         const palf::AccessMode &access_mode,
                         const palf::PalfBaseInfo &palf_base_info,
                         const char *log_dir,
                         ObILogAllocator *alloc_mgr,
                         palf::ILogBlockPool *log_block_pool,
                         palf::LogRpc *log_rpc,
                         palf::LogIOWorker *log_io_worker,
                         palf::IPalfEnvImpl *palf_env_impl,
                         const common::ObAddr &self,
                         const int64_t palf_epoch,
                         LogIOAdapter *io_adapter)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  palf::LogMeta log_meta;
  palf::LogSnapshotMeta snapshot_meta;
  palf::LogReplicaType replica_type(palf::ARBITRATION_REPLICA);
  const int64_t palf_meta_block_size = 2 * 1024 * 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogServer has inited", K(ret), K(palf_id));
  } else if (false == palf::is_valid_palf_id(palf_id)
             || false == palf::is_valid_access_mode(access_mode)
             || false == palf_base_info.is_valid()
             || NULL == log_dir
             || NULL == alloc_mgr
             || NULL == log_block_pool
             || NULL == log_rpc
             || NULL == log_io_worker
             || NULL == palf_env_impl
             || false == self.is_valid()
             || palf_epoch < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(palf_base_info),
        K(access_mode), K(log_dir), K(alloc_mgr), K(log_block_pool), K(log_rpc),
        K(log_io_worker), K(palf_env_impl), K(self), K(palf_epoch));
  } else if (OB_FAIL(log_meta.generate_by_palf_base_info(palf_base_info, access_mode, replica_type))) {
    PALF_LOG(WARN, "generate_by_palf_base_info failed", K(ret), K(palf_id), K(palf_base_info), K(access_mode), K(replica_type));
  } else if ((pret = snprintf(log_dir_, MAX_PATH_SIZE, "%s", log_dir)) && false) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "error unexpected", K(ret), K(palf_id));
  } else if (OB_FAIL(log_engine_.init(palf_id, log_dir, log_meta, alloc_mgr, log_block_pool, NULL, log_rpc, \
          log_io_worker, NULL, &plugins_, palf_epoch, 0, palf_meta_block_size, io_adapter))) {
    PALF_LOG(WARN, "LogEngine init failed", K(ret), K(palf_id), K(log_dir), K(alloc_mgr),
        K(log_rpc), K(log_io_worker));
  } else if (OB_FAIL(do_init_mem_(palf_id, palf_base_info, log_meta, log_dir, self,
          alloc_mgr, log_rpc, log_io_worker, palf_env_impl))) {
    PALF_LOG(WARN, "PalfHandleLite do_init_mem_ failed", K(ret), K(palf_id));
  } else {
    PALF_EVENT("PalfHandleLite init success", palf_id_, K(ret), K(access_mode), K(palf_base_info),
        K(log_dir), K(log_meta), K(palf_epoch));
  }
  return ret;
}

bool PalfHandleLite::check_can_be_used() const
{
  return false == ATOMIC_LOAD(&has_set_deleted_);
}

int PalfHandleLite::load(const int64_t palf_id,
                         const char *log_dir,
                         ObILogAllocator *alloc_mgr,
                         ILogBlockPool *log_block_pool,
                         LogRpc *log_rpc,
                         LogIOWorker *log_io_worker,
                         IPalfEnvImpl *palf_env_impl,
                         const common::ObAddr &self,
                         const int64_t palf_epoch,
                         LogIOAdapter *io_adapter,
                         bool &is_integrity)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  LogGroupEntryHeader entry_header;
  LSN max_committed_end_lsn;
  const int64_t palf_meta_block_size = 2 * 1024 * 1024;
  palf_base_info.generate_by_default();
  if(IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (false == is_valid_palf_id(palf_id)
             || NULL == log_dir
             || NULL == alloc_mgr
             || NULL == log_rpc
             || NULL == log_io_worker
             || false == self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(log_dir), K(alloc_mgr),
        K(log_rpc), K(log_io_worker));
  } else if (OB_FAIL(log_engine_.load(palf_id, log_dir, alloc_mgr, log_block_pool, NULL, log_rpc,
        log_io_worker, NULL, &plugins_, entry_header, palf_epoch, 0, palf_meta_block_size, io_adapter, is_integrity))) {
    PALF_LOG(WARN, "LogEngine load failed", K(ret), K(palf_id));
    // NB: when 'entry_header' is invalid, means that there is no data on disk, and set max_committed_end_lsn
    //     to 'base_lsn_', we will generate default PalfBaseInfo or get it from LogSnapshotMeta(rebuild).
  } else if (false == is_integrity) {
    PALF_LOG(INFO, "palf instance is not integrity", KPC(this));
  } else if (FALSE_IT(max_committed_end_lsn =
         (true == entry_header.is_valid() ? entry_header.get_committed_end_lsn() :
          log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_)))  {
  } else if (OB_FAIL(do_init_mem_(palf_id, palf_base_info, log_engine_.get_log_meta(), log_dir, self,
          alloc_mgr, log_rpc, log_io_worker, palf_env_impl))) {
    PALF_LOG(WARN, "PalfHandleLite do_init_mem_ failed", K(ret), K(palf_id));
  } else {
    PALF_EVENT("PalfHandleLite load success", palf_id_, K(ret), K(palf_base_info), K(log_dir), K(palf_epoch));
  }
  return ret;
}

void PalfHandleLite::destroy()
{
  WLockGuard guard(lock_);
  PALF_EVENT("PalfHandleLite destroy", palf_id_, KPC(this));
  is_inited_ = false;
  self_.reset();
  palf_id_ = INVALID_PALF_ID;
  allocator_ = NULL;
  election_.stop();
  log_engine_.destroy();
  reconfirm_.destroy();
  state_mgr_.destroy();
  config_mgr_.destroy();
  mode_mgr_.destroy();
  if (false == check_can_be_used()) {
    palf_env_impl_->remove_directory(log_dir_);
  }
  palf_env_impl_ = NULL;
}

int PalfHandleLite::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleLite has not inited!!!", K(ret));
  } else {
    PALF_LOG(INFO, "PalfHandleLite start success", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleLite::set_initial_member_list(
    const common::ObMemberList &member_list,
    const int64_t paxos_replica_num,
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::set_initial_member_list(
    const common::ObMemberList &member_list,
    const common::ObMember &arb_replica,
    const int64_t paxos_replica_num,
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_base_lsn(palf::LSN &lsn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleLite not init", K(ret), KPC(this));
  } else {
    lsn = log_engine_.get_base_lsn_used_for_block_gc();
  }
  return ret;
}

int PalfHandleLite::get_begin_lsn(LSN &lsn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleLite not init", K(ret), KPC(this));
  } else {
    lsn = log_engine_.get_begin_lsn();
  }
  return ret;
}

int PalfHandleLite::get_begin_scn(SCN &scn)
{
  int ret = OB_SUCCESS;
  block_id_t unused_block_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleLite not init", K(ret), KPC(this));
  } else if (OB_FAIL(log_engine_.get_min_block_info_for_gc(unused_block_id, scn))) {
    PALF_LOG(WARN, "LogEngine get_min_block_id_and_min_scn failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleLite::get_base_info(const LSN &base_lsn,
                                  PalfBaseInfo &base_info)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::submit_log(
    const PalfAppendOptions &opts,
    const char *buf,
    const int64_t buf_len,
    const share::SCN &ref_scn,
    LSN &lsn,
    SCN &scn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support submit log", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_palf_id(int64_t &palf_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleLite has not inited", K(ret));
  } else {
    palf_id = palf_id_;
  }
  return ret;
}

int PalfHandleLite::get_role(common::ObRole &role,
                             int64_t &proposal_id,
                             bool &is_pending_state) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleLite has not inited", K(ret));
  } else {
    ObRole curr_role = ObRole::INVALID_ROLE;
    ObReplicaState curr_state = ObReplicaState::INVALID_STATE;
    int64_t curr_leader_epoch = -1;
    do {
      is_pending_state = mode_mgr_.is_in_pending_state();
      // 获取当前的proposal_id
      proposal_id = state_mgr_.get_proposal_id();
      // 获取当前的leader_epoch
      curr_leader_epoch = state_mgr_.get_leader_epoch();
      // 获取当前的role, state
      state_mgr_.get_role_and_state(curr_role, curr_state);
      if (ObRole::LEADER != curr_role || ObReplicaState::ACTIVE != curr_state) {
        // not <LEADER, ACTIVE>
        role = ObRole::FOLLOWER;
        if (ObReplicaState::RECONFIRM == curr_state || ObReplicaState::PENDING == curr_state) {
          is_pending_state = true;
        }
      } else if (false == state_mgr_.check_epoch_is_same_with_election(curr_leader_epoch)) {
        // PALF当前是<LEADER, ACTIVE>, 但epoch在election层发生了变化,
        // 说明election已经切主, PALF最终一定会切为<FOLLOWER, PENDING>, 这种情况也当做pending state,
        // 否则调用者看到PALF是<FOLLOWER, ACTIVE>, 但proposal_id却还是LEADER时的值,
        // 可能会导致非预期行为, 比如role change service可能会提前执行卸任操作.
        role = ObRole::FOLLOWER;
        is_pending_state = true;
      } else {
        // 返回LEADER
        role = ObRole::LEADER;
      }
      // double check proposal_id
      if (proposal_id == state_mgr_.get_proposal_id()) {
        // proposal_id does not change, exit loop
        break;
      } else {
        // proposal_id has changed, need retry
      }
    } while(true);
  }
  return ret;
}

int PalfHandleLite::change_leader_to(const common::ObAddr &dest_addr)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support change leader", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_global_learner_list(common::GlobalLearnerList &learner_list) const
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support mount learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_paxos_member_list(
    common::ObMemberList &member_list,
    int64_t &paxos_replica_num) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleLite has not inited", K(ret));
  } else if (OB_FAIL(config_mgr_.get_curr_member_list(member_list, paxos_replica_num))) {
    PALF_LOG(WARN, "get_curr_member_list failed", K(ret), KPC(this));
  } else {}
  return ret;
}

int PalfHandleLite::get_config_version(LogConfigVersion &config_version) const
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support get config version", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_paxos_member_list_and_learner_list(
    common::ObMemberList &member_list,
    int64_t &paxos_replica_num,
    GlobalLearnerList &learner_list) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleImpl has not inited", K(ret));
  } else if (OB_FAIL(config_mgr_.get_curr_member_list(member_list, paxos_replica_num))) {
    PALF_LOG(WARN, "get_curr_member_list failed", K(ret), KPC(this));
  } else if (OB_FAIL(config_mgr_.get_global_learner_list(learner_list))) {
    PALF_LOG(WARN, "get_global_learner_list failed", K(ret), KPC(this));
  } else {}
  return ret;
}

int PalfHandleLite::get_stable_membership(LogConfigVersion &config_version,
                                          common::ObMemberList &member_list,
                                          int64_t &paxos_replica_num,
                                          common::GlobalLearnerList &learner_list) const
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support get election learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_election_leader(common::ObAddr &addr) const
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support get election learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_parent(common::ObAddr &parent) const
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support get election learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::handle_config_change_pre_check(const ObAddr &server,
                                                   const LogGetMCStReq &req,
                                                   LogGetMCStResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleLite has not inited", K(ret), K_(palf_id));
  } else {
    RLockGuard guard(lock_);
    int64_t curr_proposal_id = state_mgr_.get_proposal_id();
    resp.msg_proposal_id_ = curr_proposal_id;
    LogConfigVersion curr_config_version;
    if (OB_FAIL(config_mgr_.get_config_version(curr_config_version))) {
    } else if (req.config_version_ != curr_config_version) {
      resp.need_update_config_meta_ = true;
    } else {
      LSN max_flushed_end_lsn(PALF_INITIAL_LSN_VAL);
      resp.max_flushed_end_lsn_ = max_flushed_end_lsn;
      resp.need_update_config_meta_ = false;
    }
    resp.is_normal_replica_ = true;
    PALF_LOG(INFO, "config_change_pre_check success", K(ret), KPC(this), K(server),
        K(req), K(resp), K(curr_config_version));
  }
  return ret;
}

int PalfHandleLite::force_set_as_single_replica()
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support force set as single replica", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::force_set_member_list(const common::ObMemberList &member_list,
                                          const int64_t new_replica_num)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support force set member list", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::change_replica_num(
    const common::ObMemberList &member_list,
    const int64_t curr_replica_num,
    const int64_t new_replica_num,
    const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support change replica num", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::add_member(
    const common::ObMember &member,
    const int64_t new_replica_num,
    const palf::LogConfigVersion &config_version,
    const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support add member", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::remove_member(
    const common::ObMember &member,
    const int64_t new_replica_num,
    const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support remove member", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::replace_member(
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const palf::LogConfigVersion &config_version,
    const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support replace member", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::add_learner(const common::ObMember &added_learner, const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support add learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::remove_learner(const common::ObMember &removed_learner, const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support remove learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::switch_learner_to_acceptor(const common::ObMember &learner,
                                               const int64_t new_replica_num,
                                               const palf::LogConfigVersion &config_version,
                                               const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support remove learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::switch_acceptor_to_learner(const common::ObMember &member,
                                               const int64_t new_replica_num,
                                               const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support switch acceptor to learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::replace_learners(const common::ObMemberList &added_learners,
                                     const common::ObMemberList &removed_learners,
                                     const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support switch acceptor to learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::replace_member_with_learner(const common::ObMember &added_member,
                                                const common::ObMember &removed_member,
                                                const palf::LogConfigVersion &config_version,
                                                const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support switch acceptor to learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::add_arb_member(
    const common::ObMember &member,
    const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support add arb member", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::remove_arb_member(
    const common::ObMember &member,
    const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support remove arb member", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::degrade_acceptor_to_learner(const LogMemberAckInfoList &degrade_servers, const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support degrade acceptor to learner", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::upgrade_learner_to_acceptor(const LogMemberAckInfoList &upgrade_servers, const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support upgrade learner to acceptor", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::change_access_mode(const int64_t proposal_id,
                                       const int64_t mode_version,
                                       const AccessMode &access_mode,
                                       const share::SCN &scn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support change access mode", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::handle_register_parent_req(const LogLearner &child, const bool is_to_leader)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support handle register parent req", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::handle_register_parent_resp(const LogLearner &server,
                                                const LogCandidateList &candidate_list,
                                                const RegisterReturn reg_ret)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support handle register parent resp", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::handle_learner_req(const LogLearner &server, const LogLearnerReqType req_type)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support handle learn req", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::set_base_lsn(
    const LSN &lsn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support set base lsn", KR(ret), KPC(this));
  return ret;
}

bool PalfHandleLite::is_sync_enabled() const
{
  PALF_LOG_RET(WARN, OB_NOT_SUPPORTED, "arb replica not support this function", KPC(this));
  return false;
}

int PalfHandleLite::enable_sync()
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support this function", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::disable_sync()
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support this function", KR(ret), KPC(this));
  return ret;
}

bool PalfHandleLite::is_vote_enabled() const
{
  PALF_LOG_RET(WARN, OB_NOT_SUPPORTED, "arb replica not support this function", KPC(this));
  return false;
}

int PalfHandleLite::disable_vote(const bool need_check_log_missing)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support this function", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::enable_vote()
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support this function", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::advance_base_info(const PalfBaseInfo &palf_base_info, const bool is_rebuild)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support this function", KR(ret), KPC(this));
  return ret;
}

int PalfHandleLite::locate_by_scn_coarsely(const SCN &scn, LSN &result_lsn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(ERROR, "arb replica not support this function", KR(ret), KPC(this));
  return ret;
}

void PalfHandleLite::set_deleted()
{
  ATOMIC_STORE(&has_set_deleted_, true);
  PALF_LOG(INFO, "set_deleted success", KPC(this));
}

int PalfHandleLite::locate_by_lsn_coarsely(const LSN &lsn, SCN &result_scn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_min_block_info_for_gc(block_id_t &block_id, SCN &scn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::delete_block(const block_id_t &block_id)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::inner_append_log(const LSN &lsn,
                                     const LogWriteBuf &write_buf,
                                     const share::SCN &scn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::inner_append_log(const LSNArray &lsn_array,
                                     const LogWriteBufArray &write_buf_array,
                                     const SCNArray &scn_array)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::inner_append_meta(const char *buf,
                                      const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleLite not inited");
  } else if (NULL == buf
             || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(ret), KPC(this), K(buf), K(buf_len));
  } else if (OB_FAIL(log_engine_.append_meta(buf, buf_len))) {
    PALF_LOG(ERROR, "LogEngine append_meta failed", K(ret), KPC(this));
  } else {
  }
  return ret;
}

int PalfHandleLite::inner_truncate_log(const LSN &lsn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::inner_truncate_prefix_blocks(const LSN &lsn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::set_scan_disk_log_finished()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(state_mgr_.set_scan_disk_log_finished())) {
    PALF_LOG(WARN, "set_scan_disk_log_finished failed", K(ret), KPC(this));
  } else if (OB_FAIL(check_and_switch_state())) {
    PALF_LOG(WARN, "check_and_switch_state failed", K(ret), KPC(this));
  } else {
    PALF_LOG(INFO, "set_scan_disk_log_finished finished", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleLite::get_access_mode(AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleLite is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(mode_mgr_.get_access_mode(access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleLite::get_access_mode_version(int64_t &mode_version) const
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int PalfHandleLite::get_access_mode(int64_t &mode_version, AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleLite is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(mode_mgr_.get_access_mode(mode_version, access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleLite::get_access_mode_ref_scn(int64_t &mode_version,
                                            palf::AccessMode &access_mode,
                                            share::SCN &ref_scn) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleImpl is not inited", K(ret), KPC(this));
  } else if (OB_FAIL(mode_mgr_.get_access_mode_ref_scn(mode_version, access_mode, ref_scn))) {
    PALF_LOG(WARN, "get_access_mode_ref_scn failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleLite::alloc_palf_buffer_iterator(const LSN &offset,
                                               PalfBufferIterator &iterator)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::alloc_palf_buffer_iterator(const SCN &scn,
                                               PalfBufferIterator &iterator)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::alloc_palf_group_buffer_iterator(const LSN &offset,
                                                     PalfGroupBufferIterator &iterator)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::alloc_palf_group_buffer_iterator(const SCN &scn,
                                                     PalfGroupBufferIterator &iterator)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::register_file_size_cb(palf::PalfFSCbNode *fs_cb)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::unregister_file_size_cb(palf::PalfFSCbNode *fs_cb)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::register_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::unregister_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::register_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::unregister_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::set_location_cache_cb(PalfLocationCacheCb *lc_cb)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::set_election_priority(election::ElectionPriority *priority)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(priority)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "priority is NULL, can't setted", KR(ret), KPC(this));
  } else {
    WLockGuard guard(lock_);
    if (OB_FAIL(election_.set_priority(priority))) {
      PALF_LOG(WARN, "set election priority failed", KR(ret), KPC(this));
    }
  }
  return ret;
}

int PalfHandleLite::reset_election_priority()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "palf handle not init", KR(ret), KPC(this));
  } else {
    WLockGuard guard(lock_);
    if (OB_FAIL(election_.reset_priority())) {
      PALF_LOG(WARN, "fail to reset election priority", KR(ret), KPC(this));
    }
  }
  return ret;
}

int PalfHandleLite::reset_location_cache_cb()
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::set_monitor_cb(palf::PalfLiteMonitorCb *monitor_cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "not initted", KR(ret), KPC(this));
  } else if (OB_ISNULL(monitor_cb)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "lc_cb is NULL, can't register", KR(ret), KPC(this));
  } else if (OB_FAIL(plugins_.add_plugin(monitor_cb))) {
    PALF_LOG(WARN, "add_plugin failed", KR(ret), KPC(this), KP(monitor_cb), K_(plugins));
  } else {
    PALF_LOG(INFO, "set_monitor_cb success", KPC(this), K_(plugins), KP(monitor_cb));
  }
  return ret;
}

int PalfHandleLite::reset_monitor_cb()
{
  int ret = OB_SUCCESS;
  PalfLiteMonitorCb *monitor_cb = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(plugins_.del_plugin(monitor_cb))) {
    PALF_LOG(WARN, "del_plugin failed", KR(ret), KPC(this), K_(plugins));
  }
  return ret;
}

int PalfHandleLite::set_locality_cb(palf::PalfLocalityInfoCb *locality_cb)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::reset_locality_cb()
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::reset_reconfig_checker_cb()
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::check_and_switch_state()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    bool state_changed = false;
    do {
      RLockGuard guard(lock_);
      state_changed = state_mgr_.is_state_changed();
    } while (0);
    if (state_changed) {
      WLockGuard guard(lock_);
      if (OB_FAIL(state_mgr_.switch_state())) {
        PALF_LOG(WARN, "switch_state failed", K(ret));
      }
      PALF_LOG(TRACE, "check_and_switch_state finished", K(ret), KPC(this), K(state_changed));
    }
    do {
      RLockGuard guard(lock_);
      if (OB_FAIL(config_mgr_.sync_meta_for_arb_election_leader())) {
        // overwrite ret
        PALF_LOG(WARN, "sync_meta_for_arb_election_leader failed", K(ret), K_(palf_id));
      }
    } while (0);
    do {
      RLockGuard guard(lock_);
      if (OB_FAIL(leader_changer_.change_leader())) {
        // overwrite ret
        PALF_LOG(WARN, "change_leader failed", K(ret), K_(palf_id));
      }
    } while (0);
    if (palf_reach_time_interval(PALF_DUMP_DEBUG_INFO_INTERVAL_US, last_dump_info_ts_us_)) {
      RLockGuard guard(lock_);
      FLOG_INFO("[PALF_DUMP]", K_(palf_id), K_(self), "[StateMgr]", state_mgr_,
          "[ConfigMgr]", config_mgr_, "[ModeMgr]", mode_mgr_, "[LogEngine]", log_engine_, "[Reconfirm]",
          reconfirm_);
    }
  }
  return OB_SUCCESS;
}

bool PalfHandleLite::is_in_period_freeze_mode() const
{
  return false;
}

int PalfHandleLite::check_and_switch_freeze_mode()
{
  return OB_SUCCESS;
}

int PalfHandleLite::period_freeze_last_log()
{
  return OB_SUCCESS;
}

int PalfHandleLite::handle_prepare_request(const common::ObAddr &server,
                                           const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  bool can_handle_prepare_request = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KPC(this), K(server), K(proposal_id));
  } else {
    RLockGuard guard(lock_);
    if (state_mgr_.can_handle_prepare_request(proposal_id)) {
      can_handle_prepare_request = true;
    }
  }
  if (OB_SUCC(ret) && can_handle_prepare_request) {
    WLockGuard guard(lock_);
    if (!state_mgr_.can_handle_prepare_request(proposal_id)) {
      // can not handle prepare request
      PALF_LOG(INFO, "ignore prepare request", K(ret), KPC(this), K(server), K(proposal_id));
    } else if (OB_FAIL(state_mgr_.handle_prepare_request(server, proposal_id))) {
      PALF_LOG(WARN, "handle_prepare_request failed", K(ret), KPC(this), K(server), K(proposal_id));
    } else {
      PALF_LOG(INFO, "handle_prepare_request success", K(ret), KPC(this), K(server), K_(self), K(proposal_id));
    }
  }
  PALF_LOG(TRACE, "handle_prepare_request", K(ret), KPC(this), K(server), K(can_handle_prepare_request));
  return ret;
}

int PalfHandleLite::handle_prepare_response(const common::ObAddr &server,
                                            const int64_t &proposal_id,
                                            const bool vote_granted,
                                            const int64_t &accept_proposal_id,
                                            const LSN &last_lsn,
                                            const LSN &committed_end_lsn,
                                            const LogModeMeta &log_mode_meta)
{
  int ret = OB_SUCCESS;
  bool can_handle_prepare_resp = true;
  int64_t curr_proposal_id = INVALID_PROPOSAL_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id));
  } else {
    RLockGuard guard(lock_);
    curr_proposal_id = state_mgr_.get_proposal_id();
    if (!state_mgr_.can_handle_prepare_response(proposal_id)) {
      can_handle_prepare_resp = false;
      PALF_LOG(WARN, "cannot handle parepare response", K(ret), KPC(this), K(server),
          K(proposal_id), K(curr_proposal_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (!can_handle_prepare_resp) {
      // check proposal_id
      if (proposal_id > curr_proposal_id) {
        WLockGuard guard(lock_);
        if (!state_mgr_.can_handle_prepare_request(proposal_id)) {
          // can not handle prepare request
        } else if (OB_FAIL(state_mgr_.handle_prepare_request(server, proposal_id))) {
          PALF_LOG(WARN, "handle_prepare_request failed", K(ret), KPC(this));
        } else {}
      }
    } else if (vote_granted) {
      // server grant vote for me, process preapre response
      RLockGuard guard(lock_);
      if (OB_FAIL(mode_mgr_.handle_prepare_response(server, proposal_id, accept_proposal_id,
          last_lsn, log_mode_meta))) {
        PALF_LOG(WARN, "log_mode_mgr.handle_prepare_response failed", K(ret), KPC(this), K(proposal_id),
            K(accept_proposal_id), K(last_lsn), K(log_mode_meta));
      } else if (OB_FAIL(reconfirm_.handle_prepare_response(server, proposal_id, accept_proposal_id,
              last_lsn, committed_end_lsn))) {
        PALF_LOG(WARN, "reconfirm.handle_prepare_response failed", K(ret), KPC(this),
            K(proposal_id), K(accept_proposal_id), K(last_lsn));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int PalfHandleLite::receive_mode_meta(const common::ObAddr &server,
                                      const int64_t proposal_id,
                                      const bool is_applied_mode_meta,
                                      const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  int lock_ret = OB_EAGAIN;
  bool has_accepted = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == server.is_valid() ||
      INVALID_PROPOSAL_ID == proposal_id ||
      false == mode_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), KPC(this), K(server), K(proposal_id), K(mode_meta));
  } else if (OB_FAIL(try_update_proposal_id_(server, proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", KR(ret), KPC(this), K(server), K(proposal_id));
  } else if (OB_SUCCESS != (lock_ret = lock_.wrlock())) {
  } else if (false == mode_mgr_.can_receive_mode_meta(proposal_id, mode_meta, has_accepted)) {
    PALF_LOG(WARN, "can_receive_mode_meta failed", KR(ret), KPC(this), K(proposal_id), K(mode_meta));
  } else if (true == has_accepted) {
    if (OB_FAIL(log_engine_.submit_change_mode_meta_resp(server, proposal_id))) {
      PALF_LOG(WARN, "submit_change_mode_meta_resp failed", KR(ret), KPC(this), K(proposal_id), K(mode_meta));
    }
    if (true == is_applied_mode_meta) {
      // update LogModeMgr::applied_mode_meta requires wlock
      (void) mode_mgr_.after_flush_mode_meta(is_applied_mode_meta, mode_meta);
    }
  } else if (OB_FAIL(mode_mgr_.receive_mode_meta(server, proposal_id, is_applied_mode_meta, mode_meta))) {
    PALF_LOG(WARN, "receive_mode_meta failed", KR(ret), KPC(this), K(server), K(proposal_id),
        K(mode_meta));
  } else { }
  PALF_LOG(INFO, "receive_mode_meta finish", KR(ret), KPC(this), K(server), K(proposal_id),
      K(is_applied_mode_meta), K(mode_meta));
  if (OB_SUCCESS == lock_ret) {
    lock_.wrunlock();
  }
  return ret;
}

int PalfHandleLite::ack_mode_meta(const common::ObAddr &server,
                                  const int64_t msg_proposal_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  const int64_t &curr_proposal_id = state_mgr_.get_proposal_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleLite not init", KR(ret));
  } else if (msg_proposal_id != curr_proposal_id) {
    PALF_LOG(WARN, "proposal_id does not match", KR(ret), KPC(this), K(msg_proposal_id),
        K(server), K(curr_proposal_id));
  } else if (self_ != state_mgr_.get_leader()) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "self is not leader, state not match", KR(ret), KPC(this),
        K(server), K(msg_proposal_id), K(curr_proposal_id));
  } else if (OB_FAIL(mode_mgr_.ack_mode_meta(server, msg_proposal_id))) {
    PALF_LOG(WARN, "ack_mode_meta failed", KR(ret), KPC(this), K(server), K(msg_proposal_id));
  } else {
    PALF_LOG(INFO, "ack_mode_meta success", KR(ret), KPC(this), K(server), K(msg_proposal_id));
  }
  return ret;
}

int PalfHandleLite::handle_election_message(const election::ElectionPrepareRequestMsg &msg)
{
  RLockGuard guard(lock_);
  return election_.handle_message(msg);
}

int PalfHandleLite::handle_election_message(const election::ElectionPrepareResponseMsg &msg)
{
  RLockGuard guard(lock_);
  return election_.handle_message(msg);
}

int PalfHandleLite::handle_election_message(const election::ElectionAcceptRequestMsg &msg)
{
  RLockGuard guard(lock_);
  return election_.handle_message(msg);
}

int PalfHandleLite::handle_election_message(const election::ElectionAcceptResponseMsg &msg)
{
  RLockGuard guard(lock_);
  return election_.handle_message(msg);
}

int PalfHandleLite::handle_election_message(const election::ElectionChangeLeaderMsg &msg)
{
  RLockGuard guard(lock_);
  return election_.handle_message(msg);
}

int PalfHandleLite::do_init_mem_(
    const int64_t palf_id,
    const PalfBaseInfo &palf_base_info,
    const LogMeta &log_meta,
    const char *log_dir,
    const common::ObAddr &self,
    ObILogAllocator *alloc_mgr,
    LogRpc *log_rpc,
    LogIOWorker *log_io_worker,
    IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  int pret = -1;
  const bool is_normal_replica = (log_meta.get_log_replica_property_meta().replica_type_ == NORMAL_REPLICA);
  // inner priority seed: smaller means higher priority
  // reserve some bits for future requirements
  const uint64_t election_inner_priority_seed = is_normal_replica ?
                                                static_cast<uint64_t>(PRIORITY_SEED_BIT::DEFAULT_SEED) :
                                                0ULL | static_cast<uint64_t>(PRIORITY_SEED_BIT::SEED_NOT_NORMOL_REPLICA_BIT);
  palf::PalfRoleChangeCbWrapper &role_change_cb_wrpper = role_change_cb_wrpper_;
  if ((pret = snprintf(log_dir_, MAX_PATH_SIZE, "%s", log_dir)) && false) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "error unexpected", K(ret), K(palf_id));
  } else if (OB_FAIL(sw_.init(palf_id, self, &state_mgr_, &config_mgr_, &mode_mgr_,
          &log_engine_, &fs_cb_wrapper_, alloc_mgr, &plugins_, palf_base_info, is_normal_replica))) {
    PALF_LOG(WARN, "sw_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(election_.init_and_start(palf_id,
                                              self,
                                              election_inner_priority_seed,
                                              1,
                                              [&role_change_cb_wrpper](int64_t id,
                                                                       const ObAddr &dest_addr){
    return role_change_cb_wrpper.on_need_change_leader(id, dest_addr);
  }))) {
    PALF_LOG(WARN, "election_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(election_.set_priority(&election_priority_))) {
    PALF_LOG(WARN, "set election priority failed", K(ret), K(palf_id));
  } else if (OB_FAIL(state_mgr_.init(palf_id, self, log_meta.get_log_prepare_meta(), log_meta.get_log_replica_property_meta(),
          &election_, &sw_, &reconfirm_, &log_engine_, &config_mgr_, &mode_mgr_, &role_change_cb_wrpper_, &plugins_))) {
    PALF_LOG(WARN, "state_mgr_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(config_mgr_.init(palf_id, self, log_meta.get_log_config_meta(), &log_engine_,
      &sw_, &state_mgr_, &election_, &mode_mgr_, &reconfirm_, &plugins_))) {
    PALF_LOG(WARN, "config_mgr_ init failed", K(ret), K(palf_id));
  } else if (is_normal_replica && OB_FAIL(reconfirm_.init(palf_id, self, &sw_, &state_mgr_,
      &config_mgr_, &mode_mgr_, &log_engine_))) {
    PALF_LOG(WARN, "reconfirm_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(mode_mgr_.init(palf_id, self, log_meta.get_log_mode_meta(), &state_mgr_, &log_engine_, &config_mgr_, &sw_))) {
    PALF_LOG(WARN, "mode_mgr_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(leader_changer_.init(&election_))) {
    PALF_LOG(WARN, "leader_changer_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(register_role_change_cb_(&rc_cb_node_))) {
    PALF_LOG(WARN, "register_role_change_cb_ failed", K(ret), K(palf_id));
  } else {
    palf_id_ = palf_id;
    allocator_ = alloc_mgr;
    self_ = self;
    has_set_deleted_ = false;
    palf_env_impl_ = palf_env_impl;
    is_inited_ = true;
    PALF_LOG(INFO, "PalfHandleLite do_init_ success", K(ret), K(palf_id), K(log_dir), K(palf_base_info),
        K(log_meta), K(alloc_mgr), K(log_rpc), K(log_io_worker), K(is_normal_replica));
  }
  if (OB_FAIL(ret)) {
    is_inited_ = true;
    destroy();
  }
  return ret;
}

int PalfHandleLite::get_palf_epoch(int64_t &palf_epoch) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    palf_epoch = log_engine_.get_palf_epoch();
  }
  return ret;
}

int PalfHandleLite::check_req_proposal_id_(const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K(proposal_id));
  } else {
    bool need_update_proposal_id = false;
    do {
      RLockGuard guard(lock_);
      if (proposal_id > state_mgr_.get_proposal_id()) {
        need_update_proposal_id = true;
      }
    } while(0);
    if (need_update_proposal_id) {
      WLockGuard guard(lock_);
      if (proposal_id > state_mgr_.get_proposal_id()) {
        // double check
      }
    }
  }
  return ret;
}

int PalfHandleLite::try_update_proposal_id_(const common::ObAddr &server,
                                            const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id));
  } else {
    bool need_handle_prepare_req = false;
    do {
      // check if need update proposal_id
      RLockGuard guard(lock_);
      if (proposal_id > state_mgr_.get_proposal_id()) {
        need_handle_prepare_req = true;
      }
    } while(0);
    // try update proposal_id
    if (need_handle_prepare_req) {
      WLockGuard guard(lock_);
      if (!state_mgr_.can_handle_prepare_request(proposal_id)) {
        // can not handle prepare request
      } else if (OB_FAIL(state_mgr_.handle_prepare_request(server, proposal_id))) {
        PALF_LOG(WARN, "handle_prepare_request failed", K(ret), K(server), K(proposal_id));
      } else {}
    }
  }
  return ret;
}

int PalfHandleLite::handle_committed_info(const common::ObAddr &server,
                                          const int64_t &msg_proposal_id,
                                          const int64_t prev_log_id,
                                          const int64_t &prev_log_proposal_id,
                                          const LSN &committed_end_lsn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::submit_group_log(const PalfAppendOptions &opts,
                                     const LSN &lsn,
                                     const char *buf,
                                     const int64_t buf_len)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::ack_log(const common::ObAddr &server,
                            const int64_t &proposal_id,
                            const LSN &log_end_lsn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_last_rebuild_lsn(LSN &last_rebuild_lsn) const
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::handle_notify_rebuild_req(const common::ObAddr &server,
                                              const LSN &base_lsn,
                                              const LogInfo &base_prev_log_info)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::fetch_log_from_storage(const common::ObAddr &server,
                                           const FetchLogType fetch_type,
                                           const int64_t &msg_proposal_id,
                                           const LSN &prev_lsn,
                                           const LSN &fetch_start_lsn,
                                           const int64_t fetch_log_size,
                                           const int64_t fetch_log_count,
                                           const int64_t accepted_mode_pid,
                                           const SCN &replayable_point,
                                           FetchLogStat &fetch_stat)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_log(const common::ObAddr &server,
                            const FetchLogType fetch_type,
                            const int64_t msg_proposal_id,
                            const LSN &prev_lsn,
                            const LSN &start_lsn,
                            const int64_t fetch_log_size,
                            const int64_t fetch_log_count,
                            const int64_t accepted_mode_pid)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::receive_config_log(const common::ObAddr &server,
                                       const int64_t &msg_proposal_id,
                                       const int64_t &prev_log_proposal_id,
                                       const LSN &prev_lsn,
                                       const int64_t &prev_mode_pid,
                                       const LogConfigMeta &meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleLite not init", KR(ret));
  } else if (false == server.is_valid() ||
             self_ == server ||
             INVALID_PROPOSAL_ID == msg_proposal_id ||
             false == prev_lsn.is_valid() ||
             INVALID_PROPOSAL_ID == prev_mode_pid ||
             false == meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KR(ret), KPC(this), K(server),
        K(msg_proposal_id), K(prev_lsn), K(meta));
  } else if (OB_FAIL(try_update_proposal_id_(server, msg_proposal_id))) {
    PALF_LOG(WARN, "try_update_proposal_id_ failed", KR(ret), KPC(this), K(server), K(msg_proposal_id));
  } else {
    // need wlock in case of truncating log and writing log_ms_meta in LogConfigMgr
    WLockGuard guard(lock_);
    if (false == state_mgr_.can_receive_config_log(msg_proposal_id) ||
        false == config_mgr_.can_receive_config_log(server, meta)) {
      ret = OB_STATE_NOT_MATCH;
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        PALF_LOG(WARN, "can not receive log", KR(ret), KPC(this), K(msg_proposal_id), "role", state_mgr_.get_role());
      }
    } else if (mode_mgr_.get_accepted_mode_meta().proposal_id_ < prev_mode_pid) {
      // need fetch mode_meta
      // TODO 是否应该换一种mode meta拉取方式，依赖拉日志触发拉mode meta不太可取。
      PALF_LOG(INFO, "pre_check_for_mode_meta don't match, wait resending mode_meta by leader",
          KR(ret), KPC(this), K(server), K(msg_proposal_id), K(prev_mode_pid), K(meta));
    } else if (OB_FAIL(config_mgr_.receive_config_log(server, meta))) {
      PALF_LOG(WARN, "receive_config_log failed", KR(ret), KPC(this), K(server), K(msg_proposal_id),
          K(prev_log_proposal_id), K(prev_lsn));
    } else {
      PALF_LOG(INFO, "receive_config_log success", KR(ret), KPC(this), K(server), K(msg_proposal_id),
          K(prev_lsn), K(prev_log_proposal_id), K(meta));
    }
  }
  return ret;
}

int PalfHandleLite::ack_config_log(const common::ObAddr &server,
                                   const int64_t msg_proposal_id,
                                   const LogConfigVersion &config_version)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_total_used_disk_space(int64_t &total_used_disk_space,
                                              int64_t &unrecyclable_disk_space) const
{
  int ret = OB_SUCCESS;
  total_used_disk_space = 0;
  unrecyclable_disk_space = 0;
  if (OB_FAIL(log_engine_.get_total_used_disk_space(total_used_disk_space, unrecyclable_disk_space))) {
    PALF_LOG(WARN, "get_total_used_disk_space failed", K(ret), KPC(this));
  }
  return ret;
}

int PalfHandleLite::advance_reuse_lsn(const LSN &flush_log_end_lsn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::inner_after_flush_log(const FlushLogCbCtx &flush_log_cb_ctx)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

// NB: execute 'inner_after_flush_meta' is serially.
int PalfHandleLite::inner_after_flush_meta(const FlushMetaCbCtx &flush_meta_cb_ctx)
{
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "inner_after_flush_meta", K(flush_meta_cb_ctx));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (MODE_META == flush_meta_cb_ctx.type_) {
    WLockGuard guard(lock_);
    ret = after_flush_mode_meta_(flush_meta_cb_ctx.proposal_id_,
                                 flush_meta_cb_ctx.is_applied_mode_meta_,
                                 flush_meta_cb_ctx.log_mode_meta_);
  } else {
    RLockGuard guard(lock_);
    switch(flush_meta_cb_ctx.type_) {
      case PREPARE_META:
        ret = after_flush_prepare_meta_(flush_meta_cb_ctx.proposal_id_);
        break;
      case CHANGE_CONFIG_META:
        ret = after_flush_config_change_meta_(flush_meta_cb_ctx.proposal_id_, flush_meta_cb_ctx.config_version_);
        break;
      case SNAPSHOT_META:
        ret = OB_NOT_SUPPORTED;
        break;
      case REPLICA_PROPERTY_META:
        ret = after_flush_replica_property_meta_(flush_meta_cb_ctx.allow_vote_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
  return ret;
}

int PalfHandleLite::inner_after_truncate_prefix_blocks(const TruncatePrefixBlocksCbCtx &truncate_prefix_cb_ctx)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::inner_after_flashback(const FlashbackCbCtx &flashback_ctx)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::after_flush_prepare_meta_(const int64_t &proposal_id)
{
  // caller holds lock
  int ret = OB_SUCCESS;
  const ObAddr &leader = state_mgr_.get_leader();
  if (proposal_id != state_mgr_.get_proposal_id()) {
    PALF_LOG(WARN, "proposal_id has changed during flushing", K(ret), K(proposal_id),
        "curr_proposal_id", state_mgr_.get_proposal_id());
  } else if (false == leader.is_valid() || self_ == leader) {
    // leader is self, no need submit response
    PALF_LOG(INFO, "do not need submit prepare response to self", K_(self), "leader:", leader);
  } else if (OB_FAIL(submit_prepare_response_(leader, proposal_id))) {
    PALF_LOG(WARN, "submit_prepare_response_ failed", K(ret), K(proposal_id), "leader", leader);
  } else {
    PALF_LOG(INFO, "after_flush_prepare_meta_ success", K(proposal_id));
    // do nothing
  }
  return ret;
}

int PalfHandleLite::after_flush_config_change_meta_(const int64_t proposal_id, const LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;
  const ObAddr &leader = state_mgr_.get_leader();
  if (proposal_id != state_mgr_.get_proposal_id()) {
    PALF_LOG(WARN, "proposal_id has changed during flushing", K(ret), K_(palf_id), K_(self), K(proposal_id),
        "curr_proposal_id", state_mgr_.get_proposal_id());
  } else if (OB_FAIL(config_mgr_.after_flush_config_log(config_version))) {
    PALF_LOG(WARN, "LogConfigMgr after_flush_config_log failed", K(ret), KPC(this), K(proposal_id),
        K(config_version));
  } else if (self_ == leader) {
    bool unused_bool = false;
    if (OB_FAIL(config_mgr_.ack_config_log(self_, proposal_id, config_version, unused_bool))) {
      PALF_LOG(WARN, "ack_config_log failed", K(ret), KPC(this), K(config_version));
    }
  } else if (false == leader.is_valid()) {
    PALF_LOG(WARN, "leader is invalid", KPC(this), K(proposal_id), K(config_version));
  } else if (OB_FAIL(log_engine_.submit_change_config_meta_resp(leader, proposal_id, config_version))) {
    PALF_LOG(WARN, "submit_change_config_meta_resp failed", K(ret), KPC(this), K(proposal_id), K(config_version));
  } else {
    PALF_LOG(INFO, "submit_change_config_meta_resp success", K(ret), KPC(this), K(proposal_id), K(config_version));
  }
  return ret;
}

int PalfHandleLite::after_flush_replica_property_meta_(const bool allow_vote)
{
  return (true == allow_vote)? state_mgr_.enable_vote(): state_mgr_.disable_vote();
}

int PalfHandleLite::after_flush_mode_meta_(const int64_t proposal_id,
                                           const bool is_applied_mode_meta,
                                           const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  const ObAddr &leader = state_mgr_.get_leader();
  if (proposal_id != state_mgr_.get_proposal_id()) {
    PALF_LOG(WARN, "proposal_id has changed during flushing", K(ret), K(proposal_id),
        "curr_proposal_id", state_mgr_.get_proposal_id());
  } else if (OB_FAIL(mode_mgr_.after_flush_mode_meta(is_applied_mode_meta, mode_meta))) {
    PALF_LOG(WARN, "after_flush_mode_meta failed", K(ret), K_(palf_id), K(proposal_id), K(mode_meta));
  } else if (self_ == leader) {
    if (OB_FAIL(mode_mgr_.ack_mode_meta(self_, mode_meta.proposal_id_))) {
      PALF_LOG(WARN, "ack_mode_meta failed", K(ret), K_(palf_id), K_(self), K(proposal_id));
    }
  } else if (false == leader.is_valid()) {
    PALF_LOG(WARN, "leader is invalid", KPC(this), K(proposal_id));
  } else if (OB_FAIL(log_engine_.submit_change_mode_meta_resp(leader, proposal_id))) {
    PALF_LOG(WARN, "submit_change_mode_meta_resp failed", K(ret), K_(self), K(leader), K(proposal_id));
  } else {
    PALF_LOG(INFO, "submit_change_mode_meta_resp success", K(ret), K_(self), K(leader), K(proposal_id));
  }
  return ret;
}

int PalfHandleLite::inner_after_truncate_log(const TruncateLogCbCtx &truncate_log_cb_ctx)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::submit_prepare_response_(const common::ObAddr &server,
                                             const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  const bool vote_granted = true;
  LSN unused_prev_lsn;
  LSN max_flushed_end_lsn;
  int64_t max_flushed_log_pid = INVALID_PROPOSAL_ID;
  LSN committed_end_lsn;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(server), K(proposal_id));
  } else if (OB_FAIL(sw_.get_max_flushed_log_info(unused_prev_lsn, max_flushed_end_lsn, max_flushed_log_pid))) {
    PALF_LOG(WARN, "get_max_flushed_log_info failed", K(ret), K_(palf_id));
  } else if (OB_FAIL(sw_.get_committed_end_lsn(committed_end_lsn))) {
    PALF_LOG(WARN, "get_committed_end_lsn failed", K(ret), K_(palf_id));
  } else {
    const LogModeMeta accepted_mode_meta = mode_mgr_.get_accepted_mode_meta();
    // the last log info include proposal_id and lsn, the proposal_id should be the maxest
    // of redo lod and config log.
    // because config log and redo log are stored separately
    int64_t accept_proposal_id = config_mgr_.get_accept_proposal_id();
    if (INVALID_PROPOSAL_ID != max_flushed_log_pid) {
      accept_proposal_id = MAX(config_mgr_.get_accept_proposal_id(), max_flushed_log_pid);
    }
    if (OB_FAIL(log_engine_.submit_prepare_meta_resp(server, proposal_id, vote_granted, accept_proposal_id,
            max_flushed_end_lsn, committed_end_lsn, accepted_mode_meta))) {
      PALF_LOG(WARN, "submit_prepare_response failed", K(ret), K_(palf_id));
    } else {
      PALF_LOG(INFO, "submit_prepare_response success", K(ret), K_(palf_id), K_(self), K(server),
          K(vote_granted), K(accept_proposal_id), K(max_flushed_end_lsn), K(committed_end_lsn),
          K(accepted_mode_meta));
    }
  }
  return ret;
}

int PalfHandleLite::advance_election_epoch_and_downgrade_priority(const int64_t proposal_id,
                                                                  const int64_t downgrade_priority_time_us,
                                                                  const char *reason)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "PalfHandleLite not inited", K(ret), K_(palf_id), K(reason));
  } else if (false == state_mgr_.can_revoke(proposal_id)) {
    ret = OB_NOT_MASTER;
    PALF_LOG(WARN, "revoke_leader failed, not master", K(ret), K_(palf_id), K(proposal_id), K(reason));
// TODO by runlin: not support this function
//  } else if (OB_FAIL(election_.revoke())) {
//    PALF_LOG(WARN, "PalfHandleLite revoke leader failed", K(ret), K_(palf_id));
  } else {
    PALF_LOG(INFO, "PalfHandleLite revoke leader success", K(ret), K_(palf_id), K(reason));
  }
  return ret;
}

int PalfHandleLite::stat(PalfStat &palf_stat)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
    SCN min_block_min_scn;
    ObRole curr_role = INVALID_ROLE;
    ObReplicaState curr_state = INVALID_STATE;
    palf_stat.self_ = self_;
    palf_stat.palf_id_ = palf_id_;
    state_mgr_.get_role_and_state(curr_role, curr_state);
    palf_stat.role_ = (LEADER == curr_role && curr_state == ObReplicaState::ACTIVE)? LEADER: FOLLOWER;
    palf_stat.log_proposal_id_ = state_mgr_.get_proposal_id();
    (void)config_mgr_.get_config_version(palf_stat.config_version_);
    (void)mode_mgr_.get_access_mode(palf_stat.mode_version_, palf_stat.access_mode_);
    (void)config_mgr_.get_curr_member_list(palf_stat.paxos_member_list_, palf_stat.paxos_replica_num_);
    palf_stat.allow_vote_ = state_mgr_.is_allow_vote();
    palf_stat.replica_type_ = state_mgr_.get_replica_type();
    palf_stat.base_lsn_ = log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_;
    (void)log_engine_.get_min_block_info_for_gc(min_block_id, min_block_min_scn);
    palf_stat.begin_lsn_ = LSN(min_block_id * PALF_BLOCK_SIZE);
    palf_stat.begin_scn_ = min_block_min_scn;
    palf_stat.end_lsn_ = get_end_lsn();
    palf_stat.end_scn_ = get_end_scn();
    palf_stat.max_lsn_ = get_max_lsn();
    palf_stat.max_scn_ = get_max_scn();
    PALF_LOG(INFO, "PalfHandleLite stat", K(palf_stat));
  }
  return ret;
}

int PalfHandleLite::flashback(const int64_t mode_version,
                              const SCN &flashback_scn,
                              const int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::try_lock_config_change(int64_t , int64_t )
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support try_lock_config_change", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::unlock_config_change(int64_t lock_owner, int64_t timeout_us)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support unlock_config_change", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support get_config_change_lock_stat", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::inner_flashback(const SCN &flashback_scn)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::get_ack_info_array(LogMemberAckInfoList &ack_info_array,
                                       common::GlobalLearnerList &degraded_list) const
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::diagnose(PalfDiagnoseInfo &info) const
{
  return OB_NOT_SUPPORTED;
}

int PalfHandleLite::update_palf_stat()
{
  return OB_SUCCESS;
}

int PalfHandleLite::register_role_change_cb_(palf::PalfRoleChangeCbNode *role_change_cb)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(role_change_cb_wrpper_.add_cb_impl(role_change_cb))) {
    PALF_LOG(WARN, "add_role_change_cb_impl failed", K(ret), KPC(this), KPC(role_change_cb));
  } else {
    PALF_LOG(INFO, "register_role_change_cb success", KPC(this));
  }
  return ret;
}

int PalfHandleLite::get_arb_member_info(palf::ArbMemberInfo &arb_member_info) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "PalfHandleLite has not inited", K(ret));
  } else {
    arb_member_info.palf_id_ = palf_id_;
    arb_member_info.arb_server_ = self_;
    arb_member_info.log_proposal_id_ = state_mgr_.get_proposal_id();
    (void)config_mgr_.get_config_version(arb_member_info.config_version_);
    (void)mode_mgr_.get_access_mode(arb_member_info.mode_version_, arb_member_info.access_mode_);
    (void)config_mgr_.get_curr_member_list(arb_member_info.paxos_member_list_, arb_member_info.paxos_replica_num_);
    (void)config_mgr_.get_arbitration_member(arb_member_info.arbitration_member_);
    (void)config_mgr_.get_degraded_learner_list(arb_member_info.degraded_list_);
  }
  return ret;
}

int PalfHandleLite::get_arbitration_member(common::ObMember &arb_member) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(config_mgr_.get_arbitration_member(arb_member))) {
    PALF_LOG(WARN, "get_arbitration_member failed", K_(palf_id), K_(self), K(arb_member));
  }
  return ret;
}

int PalfHandleLite::get_remote_arb_member_info(palf::ArbMemberInfo &arb_member_info)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::set_election_silent_flag(const bool election_silent_flag)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

bool PalfHandleLite::is_election_silent() const
{
  PALF_LOG_RET(WARN, OB_NOT_SUPPORTED, "arb replica not support this function", KPC(this));
  return false;
}

PalfHandleLiteLeaderChanger::PalfHandleLiteLeaderChanger()
  : is_inited_(false),
    lock_(),
    dst_palf_id_(OB_INVALID_ID),
    dst_addr_(),
    election_(NULL) { }

int PalfHandleLiteLeaderChanger::init(palf::election::ElectionImpl *election)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(election)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    election_ = election;
    is_inited_ = true;
  }
  return ret;
}

void PalfHandleLiteLeaderChanger::destroy()
{
  common::ObSpinLockGuard guard(lock_);
  is_inited_ = false;
  dst_palf_id_ = OB_INVALID_ID;
  dst_addr_.reset();
  election_ = NULL;
}

int PalfHandleLiteLeaderChanger::on_role_change(const int64_t id)
{
  return OB_SUCCESS;
}

int PalfHandleLiteLeaderChanger::on_need_change_leader(const int64_t ls_id, const common::ObAddr &dst_addr)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    dst_palf_id_ = ls_id;
    dst_addr_ = dst_addr;
    PALF_EVENT("on_need_change_leader success", ls_id, K_(dst_palf_id), K_(dst_addr));
  }
  return ret;
}

int PalfHandleLiteLeaderChanger::change_leader()
{
  int ret = OB_SUCCESS;
  common::ObAddr dst_addr;
  int64_t dst_palf_id = INVALID_PALF_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    common::ObSpinLockGuard guard(lock_);
    dst_addr = dst_addr_;
    dst_palf_id = dst_palf_id_;
    dst_palf_id_ = OB_INVALID_ID;
    dst_addr_.reset();
  }
  if (false == dst_addr.is_valid() || OB_INVALID_ID == dst_palf_id) {
  } else if (OB_FAIL(election_->change_leader_to(dst_addr))) {
    PALF_LOG(WARN, "change_leader_to failed", K(dst_palf_id), K(dst_addr));
  } else {
    PALF_EVENT("change_leader_to success", dst_palf_id, K(dst_addr));
  }
  return ret;
}

int PalfHandleLite::read_data_from_buffer(const LSN &read_begin_lsn,
                                          const int64_t in_read_size,
                                          char *buf,
                                          int64_t &out_read_size) const
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::raw_read(const palf::LSN &lsn,
                             char *read_buf,
                             const int64_t nbytes,
                             int64_t &read_size,
                             LogIOContext &io_ctx)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::try_handle_next_submit_log()
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

int PalfHandleLite::fill_cache_when_slide(const palf::LSN &read_begin_lsn, const int64_t in_read_size)
{
  int ret = OB_NOT_SUPPORTED;
  PALF_LOG(WARN, "arb replica not support this function", K(ret), KPC(this));
  return ret;
}

} // end namespace palf
} // end namespace oceanbase
