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

#ifndef OCEANBASE_LOGSERVICE_PALF_HANDLE_LITE
#define OCEANBASE_LOGSERVICE_PALF_HANDLE_LITE

#include "common/ob_member_list.h"
#include "common/ob_role.h"
#include "lib/hash/ob_link_hashmap_deps.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "logservice/palf/palf_callback_wrapper.h"
#include "logservice/palf/log_engine.h"                      // LogEngine
#include "logservice/palf/lsn.h"
#include "logservice/palf/log_config_mgr.h"
#include "logservice/palf/log_mode_mgr.h"
#include "logservice/palf/log_reconfirm.h"
#include "logservice/palf/log_state_mgr.h"
#include "logservice/palf/log_io_task_cb_utils.h"
#include "logservice/palf/palf_iterator.h"
#include "logservice/palf/election/interface/election_priority.h"
#include "logservice/palf/election/interface/election_msg_handler.h"
#include "logservice/palf/election/message/election_message.h"
#include "logservice/palf/election/algorithm/election_impl.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/leader_coordinator/election_priority_impl/election_priority_impl.h"

namespace oceanbase
{
namespace common
{
class ObMember;
class ObILogAllocator;
}
namespace palf
{
class FlushLogCbCtx;
class ObMemberChangeCtx;
class LSN;
class FlushLogCbCtx;
class TruncateLogCbCtx;
class FlushMetaCbCtx;
class LogIOFlushLogTask;
class LogIOTruncateLogTask;
class LogIOFlushMetaTask;
class ReadBuf;
class LogWriteBuf;
class LogIOWorker;
class LogRpc;
class IPalfEnvImpl;
class LogMeta;
class LogConfigVersion;
class FetchLogStat;
}
namespace palflite
{

class PalfHandleLiteLeaderChanger : public palf::PalfRoleChangeCb
{
public:
  PalfHandleLiteLeaderChanger();
  ~PalfHandleLiteLeaderChanger() { destroy(); }
  int init(palf::election::ElectionImpl *election);
  void destroy();
  int on_role_change(const int64_t id) final override;
  int on_need_change_leader(const int64_t ls_id, const common::ObAddr &dst_addr) final override;
  int change_leader();
private:
  bool is_inited_;
  common::ObSpinLock lock_;
  int64_t dst_palf_id_;
  common::ObAddr dst_addr_;
  palf::election::ElectionImpl *election_;
};

class PalfHandleLite : public palf::IPalfHandleImpl
{
public:
  PalfHandleLite();
  ~PalfHandleLite() override;
  int init(const int64_t palf_id,
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
           palf::LogIOAdapter *io_adapter);

  bool check_can_be_used() const override final;
  // 重启接口
  // 1. 生成迭代器，定位meta_storage和log_storage的终点;
  // 2. 从meta storage中读最新数据，初始化dio_aligned_buf;
  // 3. 初始化log_storage中的dio_aligned_buf;
  // 4. 初始化palf_handle_impl的其他字段.
  int load(const int64_t palf_id,
           const char *log_dir,
           ObILogAllocator *alloc_mgr,
           palf::ILogBlockPool *log_block_pool,
           palf::LogRpc *log_rpc,
           palf::LogIOWorker*log_io_worker,
           palf::IPalfEnvImpl *palf_env_impl,
           const common::ObAddr &self,
           const int64_t palf_epoch,
           palf::LogIOAdapter *io_adapter,
           bool &is_integrity);
  void destroy();
  int start();
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list) override final;
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const common::ObMember &arb_member,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list) override final;
  int submit_log(const palf::PalfAppendOptions &opts,
                 const char *buf,
                 const int64_t buf_len,
                 const share::SCN &ref_scn,
                 palf::LSN &lsn,
                 SCN &scn) override final;

  int submit_group_log(const palf::PalfAppendOptions &opts,
                       const palf::LSN &lsn,
                       const char *buf,
                       const int64_t buf_len) override final;
  int get_role(common::ObRole &role,
               int64_t &proposal_id,
               bool &is_pending_state) const override final;
  int get_palf_id(int64_t &palf_id) const override final;
  int change_leader_to(const common::ObAddr &dest_addr) override final;
  int get_global_learner_list(common::GlobalLearnerList &learner_list) const override final;
  int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const override final;
  virtual int get_config_version(palf::LogConfigVersion &config_version) const override final;
  int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                             int64_t &paxos_replica_num,
                                             common::GlobalLearnerList &learner_list) const override final;
  int get_stable_membership(palf::LogConfigVersion &config_version,
                            common::ObMemberList &member_list,
                            int64_t &paxos_replica_num,
                            common::GlobalLearnerList &learner_list) const override final;
  int get_election_leader(common::ObAddr &addr) const;
  int get_parent(common::ObAddr &parent) const;
  int force_set_as_single_replica() override final;
  int force_set_member_list(const common::ObMemberList &member_list,
                            const int64_t new_replica_num) override final;
  int change_replica_num(const common::ObMemberList &member_list,
                         const int64_t curr_replica_num,
                         const int64_t new_replica_num,
                         const int64_t timeout_us) override final;
  int add_member(const common::ObMember &member,
                const int64_t new_replica_num,
                const palf::LogConfigVersion &config_version,
                const int64_t timeout_us) override final;
  int remove_member(const common::ObMember &member,
                    const int64_t new_replica_num,
                    const int64_t timeout_us) override final;
  int replace_member(const common::ObMember &added_member,
                     const common::ObMember &removed_member,
                     const palf::LogConfigVersion &config_version,
                     const int64_t timeout_us) override final;
  int add_learner(const common::ObMember &added_learner,
                  const int64_t timeout_us) override final;
  int remove_learner(const common::ObMember &removed_learner,
                  const int64_t timeout_us) override final;
  int switch_learner_to_acceptor(const common::ObMember &learner,
                                 const int64_t new_replica_num,
                                 const palf::LogConfigVersion &config_version,
                                 const int64_t timeout_us) override final;
  int switch_acceptor_to_learner(const common::ObMember &member,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us) override final;
  int replace_learners(const common::ObMemberList &added_learners,
                       const common::ObMemberList &removed_learners,
                       const int64_t timeout_us) override final;
  int replace_member_with_learner(const common::ObMember &added_member,
                                  const common::ObMember &removed_member,
                                  const palf::LogConfigVersion &config_version,
                                  const int64_t timeout_us) override final;
  int add_arb_member(const common::ObMember &added_member,
                     const int64_t timeout_us) override final;
  int remove_arb_member(const common::ObMember &arb_member,
                        const int64_t timeout_us) override final;
  int degrade_acceptor_to_learner(const palf::LogMemberAckInfoList &degrade_servers,
                                  const int64_t timeout_us) override final;
  int upgrade_learner_to_acceptor(const palf::LogMemberAckInfoList &upgrade_servers,
                                  const int64_t timeout_us) override final;
  int set_base_lsn(const palf::LSN &lsn) override final;
  int enable_sync() override final;
  int disable_sync() override final;
  void set_deleted() override final;
  bool is_sync_enabled() const override final;
  int advance_base_info(const palf::PalfBaseInfo &palf_base_info, const bool is_rebuild) override final;
  int locate_by_scn_coarsely(const share::SCN &scn, palf::LSN &result_lsn) override final;
  int locate_by_lsn_coarsely(const palf::LSN &lsn, share::SCN &result_scn) override final;
  bool is_vote_enabled() const override final;
  int disable_vote(const bool need_check_log_missing) override final;
  int enable_vote() override final;
  int read_data_from_buffer(const palf::LSN &read_begin_lsn,
                            const int64_t in_read_size,
                            char *buf,
                            int64_t &out_read_size) const;
  int raw_read(const palf::LSN &lsn,
               char *read_buf,
               const int64_t nbytes,
               int64_t &read_size,
               palf::LogIOContext &io_ctx);
  int try_handle_next_submit_log();
  int raw_read(const palf::LSN &lsn,
               char *read_buf,
               const int64_t nbytes,
               int64_t &read_size);
  int fill_cache_when_slide(const palf::LSN &read_begin_lsn, const int64_t in_read_size) override;               
public:
  int delete_block(const palf::block_id_t &block_id) override final;
  int set_scan_disk_log_finished() override;
  int change_access_mode(const int64_t proposal_id,
                         const int64_t mode_version,
                         const palf::AccessMode &access_mode,
                         const share::SCN &scn) override final;
  int get_access_mode(int64_t &mode_version, palf::AccessMode &access_mode) const override final;
  int get_access_mode(palf::AccessMode &access_mode) const override final;
  int get_access_mode_version(int64_t &mode_version) const override final;
  int get_access_mode_ref_scn(int64_t &mode_version,
                              palf::AccessMode &access_mode,
                              share::SCN &ref_scn) const override final;
  // =========================== Iterator start ============================
  int alloc_palf_buffer_iterator(const palf::LSN &offset, palf::PalfBufferIterator &iterator) override final;
  int alloc_palf_buffer_iterator(const share::SCN &scn, palf::PalfBufferIterator &iterator) override final;
  int alloc_palf_group_buffer_iterator(const palf::LSN &offset, palf::PalfGroupBufferIterator &iterator) override final;
  int alloc_palf_group_buffer_iterator(const share::SCN &scn, palf::PalfGroupBufferIterator &iterator) override final;
  // =========================== Iterator end ============================

  // ==================== Callback start ======================
  int register_file_size_cb(palf::PalfFSCbNode *fs_cb) override final;
  int unregister_file_size_cb(palf::PalfFSCbNode *fs_cb) override final;
  int register_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb) override final;
  int unregister_role_change_cb(palf::PalfRoleChangeCbNode *role_change_cb) override final;
  int register_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb) override final;
  int unregister_rebuild_cb(palf::PalfRebuildCbNode *rebuild_cb) override final;
  int set_location_cache_cb(palf::PalfLocationCacheCb *lc_cb) override final;
  int reset_location_cache_cb() override final;
  int set_election_priority(palf::election::ElectionPriority *priority) override final;
  int reset_election_priority() override final;
  int set_monitor_cb(palf::PalfLiteMonitorCb *monitor_cb);
  int reset_monitor_cb();
  int set_locality_cb(palf::PalfLocalityInfoCb *locality_cb) override final;
  int reset_locality_cb() override final;
  int set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker) override final;
  int reset_reconfig_checker_cb() override final;
  // ==================== Callback end ========================
  int diagnose(palf::PalfDiagnoseInfo &info) const override final;
public:
  int get_base_lsn(palf::LSN &lsn) const override final;
  int get_begin_lsn(palf::LSN &lsn) const override final;
  int get_begin_scn(share::SCN &scn) override final;
  int get_base_info(const palf::LSN &base_lsn, palf::PalfBaseInfo &base_info) override final;
  int get_min_block_info_for_gc(palf::block_id_t &block_id, share::SCN &scn) override final;
  // return the block length which the previous data was committed
  const palf::LSN get_end_lsn() const override final
  {
    return palf::LSN(palf::PALF_INITIAL_LSN_VAL);
  }

  palf::LSN get_max_lsn() const override final
  {
    return palf::LSN(palf::PALF_INITIAL_LSN_VAL);
  }

  const share::SCN get_max_scn() const override final
  {
    return share::SCN::invalid_scn();
  }

  const share::SCN get_end_scn() const override final
  {
    return share::SCN::invalid_scn();
  }
  int get_last_rebuild_lsn(palf::LSN &last_rebuild_lsn) const override final;
  int get_total_used_disk_space(int64_t &total_used_disk_space, int64_t &unrecyclable_disk_space) const;
  // return the smallest recycable lsn
  const palf::LSN &get_base_lsn_used_for_block_gc() const override final
  {
    return log_engine_.get_base_lsn_used_for_block_gc();
  }
  int get_ack_info_array(palf::LogMemberAckInfoList &ack_info_array,
                         common::GlobalLearnerList &degraded_list) const override final;
  // =====================  LogIOTask start ==========================
  int inner_after_flush_log(const palf::FlushLogCbCtx &flush_log_cb_ctx) override final;
  int inner_after_truncate_log(const palf::TruncateLogCbCtx &truncate_log_cb_ctx) override final;
  int inner_after_flush_meta(const palf::FlushMetaCbCtx &flush_meta_cb_ctx) override final;
  int inner_after_truncate_prefix_blocks(const palf::TruncatePrefixBlocksCbCtx &truncate_prefix_cb_ctx) override final;
  int advance_reuse_lsn(const palf::LSN &flush_log_end_lsn);
  int inner_after_flashback(const palf::FlashbackCbCtx &flashback_ctx) override final;
  int inner_append_log(const palf::LSN &lsn,
                       const palf::LogWriteBuf &write_buf,
                       const share::SCN &scn) override final;
  int inner_append_log(const palf::LSNArray &lsn_array,
                       const palf::LogWriteBufArray &write_buf_array,
                       const palf::SCNArray &scn_array);
  int inner_append_meta(const char *buf,
                        const int64_t buf_len) override final;
  int inner_truncate_log(const palf::LSN &lsn) override final;
  int inner_truncate_prefix_blocks(const palf::LSN &lsn) override final;
  int inner_flashback(const SCN &flashback_scn) override final;
  // ==================================================================
  int check_and_switch_state() override final;
  int check_and_switch_freeze_mode() override final;
  bool is_in_period_freeze_mode() const override final;
  int period_freeze_last_log() override final;
  int handle_prepare_request(const common::ObAddr &server,
                             const int64_t &proposal_id) override final;
  int handle_prepare_response(const common::ObAddr &server,
                              const int64_t &proposal_id,
                              const bool vote_granted,
                              const int64_t &accept_proposal_id,
                              const palf::LSN &last_lsn,
                              const palf::LSN &committed_end_lsn,
                              const palf::LogModeMeta &log_mode_meta) override final;
  int handle_election_message(const palf::election::ElectionPrepareRequestMsg &msg) override final;
  int handle_election_message(const palf::election::ElectionPrepareResponseMsg &msg) override final;
  int handle_election_message(const palf::election::ElectionAcceptRequestMsg &msg) override final;
  int handle_election_message(const palf::election::ElectionAcceptResponseMsg &msg) override final;
  int handle_election_message(const palf::election::ElectionChangeLeaderMsg &msg) override final;

  int ack_log(const common::ObAddr &server,
              const int64_t &proposal_id,
              const palf::LSN &log_end_lsn) override final;
  int get_log(const common::ObAddr &server,
              const palf::FetchLogType fetch_type,
              const int64_t msg_proposal_id,
              const palf::LSN &prev_lsn,
              const palf::LSN &start_lsn,
              const int64_t fetch_log_size,
              const int64_t fetch_log_count,
              const int64_t accepted_mode_pid) override final;
  int fetch_log_from_storage(const common::ObAddr &server,
                             const palf::FetchLogType fetch_type,
                             const int64_t &msg_proposal_id,
                             const palf::LSN &prev_lsn,
                             const palf::LSN &fetch_start_lsn,
                             const int64_t fetch_log_size,
                             const int64_t fetch_log_count,
                             const int64_t accepted_mode_pid,
                             const SCN &replayable_point,
                             palf::FetchLogStat &fetch_stat) override final;
  int receive_config_log(const common::ObAddr &server,
                         const int64_t &msg_proposal_id,
                         const int64_t &prev_log_proposal_id,
                         const palf::LSN &prev_lsn,
                         const int64_t &prev_mode_pid,
                         const palf::LogConfigMeta &meta) override final;
  int ack_config_log(const common::ObAddr &server,
                     const int64_t proposal_id,
                     const palf::LogConfigVersion &config_version) override final;
  int receive_mode_meta(const common::ObAddr &server,
                        const int64_t proposal_id,
                        const bool is_applied_mode_meta,
                        const palf::LogModeMeta &meta) override final;
  int ack_mode_meta(const common::ObAddr &server,
                     const int64_t proposal_id) override final;
  int handle_notify_rebuild_req(const common::ObAddr &server,
                                const palf::LSN &base_lsn,
                                const palf::LogInfo &base_prev_log_info) override final;
  int handle_committed_info(const common::ObAddr &server,
                            const int64_t &msg_proposal_id,
                            const int64_t prev_log_id,
                            const int64_t &prev_log_proposal_id,
                            const palf::LSN &committed_end_lsn) override final;
  int handle_config_change_pre_check(const ObAddr &server,
                                     const palf::LogGetMCStReq &req,
                                     palf::LogGetMCStResp &resp) override final;
  int advance_election_epoch_and_downgrade_priority(const int64_t proposal_id,
                                                    const int64_t downgrade_priority_time_us,
                                                    const char *reason) override final;
  int stat(palf::PalfStat &palf_stat) override final;
  int get_arb_member_info(palf::ArbMemberInfo &arb_member_info) const override final;
  int get_arbitration_member(common::ObMember &arb_member) const override final;
  int get_remote_arb_member_info(palf::ArbMemberInfo &arb_member_info) override final;
  int handle_register_parent_req(const palf::LogLearner &child,
                                 const bool is_to_leader) override final;
  int handle_register_parent_resp(const palf::LogLearner &server,
                                  const palf::LogCandidateList &candidate_list,
                                  const palf::RegisterReturn reg_ret) override final;
  int handle_learner_req(const palf::LogLearner &server,
                         const palf::LogLearnerReqType req_type) override final;
  int get_palf_epoch(int64_t &palf_epoch) const override final;
  int flashback(const int64_t mode_version,
                const share::SCN &flashback_scn,
                const int64_t timeout_us) override final;

  virtual int try_lock_config_change(int64_t lock_owner, int64_t timeout_us) override final;
  virtual int unlock_config_change(int64_t lock_owner, int64_t timeout_us) override final;
  virtual int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked) override final;
  int update_palf_stat() override final;
  int set_election_silent_flag(const bool election_silent_flag) override final;
  bool is_election_silent() const override final;
  const palf::LSN get_readable_end_lsn() const override final
  {
    return palf::LSN(palf::PALF_INITIAL_LSN_VAL);
  }

  TO_STRING_KV(K_(palf_id), K_(self), K_(has_set_deleted));
private:
  int do_init_mem_(const int64_t palf_id,
                   const palf::PalfBaseInfo &palf_base_info,
                   const palf::LogMeta &log_meta,
                   const char *log_dir,
                   const common::ObAddr &self,
                   ObILogAllocator *alloc_mgr,
                   palf::LogRpc *log_rpc,
                   palf::LogIOWorker *log_io_worker,
                   palf::IPalfEnvImpl *palf_env_impl);
  int check_req_proposal_id_(const int64_t &proposal_id);
  int after_flush_prepare_meta_(const int64_t &proposal_id);
  int after_flush_config_change_meta_(const int64_t proposal_id, const palf::LogConfigVersion &config_version);
  int after_flush_mode_meta_(const int64_t proposal_id,
                             const bool is_applied_mode_meta,
                             const palf::LogModeMeta &mode_meta);
  int after_flush_replica_property_meta_(const bool allow_vote);
  int submit_prepare_response_(const common::ObAddr &server,
                               const int64_t &proposal_id);
  int try_update_proposal_id_(const common::ObAddr &server,
                              const int64_t &proposal_id);
  int register_role_change_cb_(palf::PalfRoleChangeCbNode *role_change_cb);
private:
  class ElectionMsgSender : public palf::election::ElectionMsgSender
  {
  public:
    ElectionMsgSender(palf::LogNetService &net_service) : net_service_(net_service) {};
    virtual int broadcast(const palf::election::ElectionPrepareRequestMsg &msg,
                          const ObIArray<ObAddr> &list) const override final
    {
      int tmp_ret = common::OB_SUCCESS;
      for (int64_t idx = 0; idx < list.count(); ++idx) {
        const_cast<palf::election::ElectionPrepareRequestMsg *>(&msg)->set_receiver(list.at(idx));
        if (OB_SUCCESS != (tmp_ret = net_service_.post_request_to_server_(list.at(idx), msg))) {
          PALF_LOG(INFO, "post prepare request msg failed", K(tmp_ret), "server", list.at(idx),
              K(msg));
        }
      }
      return common::OB_SUCCESS;
    }
    virtual int broadcast(const palf::election::ElectionAcceptRequestMsg &msg,
                          const ObIArray<ObAddr> &list) const override final
    {
      int tmp_ret = common::OB_SUCCESS;
      for (int64_t idx = 0; idx < list.count(); ++idx) {
        const_cast<palf::election::ElectionAcceptRequestMsg *>(&msg)->set_receiver(list.at(idx));
        if (OB_SUCCESS != (tmp_ret = net_service_.post_request_to_server_(list.at(idx), msg))) {
          PALF_LOG(INFO, "post accept request msg failed", K(tmp_ret), "server", list.at(idx),
              K(msg));
        }
      }
      return common::OB_SUCCESS;
    }
    virtual int send(const palf::election::ElectionPrepareResponseMsg &msg) const override final
    {
      return net_service_.post_request_to_server_(msg.get_receiver(), msg);
    }
    virtual int send(const palf::election::ElectionAcceptResponseMsg &msg) const override final
    {
      return net_service_.post_request_to_server_(msg.get_receiver(), msg);
    }
    virtual int send(const palf::election::ElectionChangeLeaderMsg &msg) const override final
    {
      return net_service_.post_request_to_server_(msg.get_receiver(), msg);
    }
  private:
    palf::LogNetService &net_service_;
  };
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef common::ObSpinLock SpinLock;
  typedef common::ObSpinLockGuard SpinLockGuard;
  typedef common::RWLock::WLockGuardWithTimeout WLockGuardWithTimeout;
private:
  mutable RWLock lock_;
  char log_dir_[common::MAX_PATH_SIZE];
  palf::LogSlidingWindow sw_;
  palf::LogConfigMgr config_mgr_;
  palf::LogModeMgr mode_mgr_;
  palf::LogStateMgr state_mgr_;
  palf::LogReconfirm reconfirm_;
  palf::LogEngine log_engine_;
  palf::election::ElectionImpl election_;
  logservice::coordinator::ElectionPriorityImpl election_priority_;
  common::ObILogAllocator *allocator_;
  int64_t palf_id_;
  common::ObAddr self_;
  palf::PalfFSCbWrapper fs_cb_wrapper_;
  palf::PalfRoleChangeCbWrapper role_change_cb_wrpper_;
  palf::LogPlugins plugins_;
  // ======optimization for locate_by_scn_coarsely=========
  mutable SpinLock last_locate_lock_;
  share::SCN last_locate_scn_;
  palf::block_id_t last_locate_block_;
  // ======optimization for locate_by_scn_coarsely=========
  int64_t last_check_parent_child_time_us_;
  // NB: only set has_set_deleted_ to true when this palf_handle has been deleted.
  bool has_set_deleted_;
  palf::IPalfEnvImpl *palf_env_impl_;
  // a spin lock for read/write replica_meta mutex
  // a spin lock for single replica mutex
  int64_t last_dump_info_ts_us_;
  // switch leader to normal replicas
  PalfHandleLiteLeaderChanger leader_changer_;
  palf::PalfRoleChangeCbNode rc_cb_node_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_SERVICE_
