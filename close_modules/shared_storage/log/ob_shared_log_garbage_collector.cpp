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

#include "ob_shared_log_garbage_collector.h"
#include "ob_shared_log_utils.h"                              // ObSharedLogUtils
#include "storage/tx/ob_ts_mgr.h"                             // GTS
#include "logservice/ob_log_service.h"                        // ObLogService
#include "logservice/common_util/ob_log_table_operator.h"     // ObLogTableOperator

namespace oceanbase
{
namespace logservice
{
ObSharedLogGarbageCollector::ObSharedLogGarbageCollector()
  : is_inited_(false),
    self_(),
    proposal_id_(palf::INVALID_PROPOSAL_ID),
    is_leader_(false),
    opts_(),
    block_mgr_(),
    handle_tenant_drop_(false),
    sql_proxy_(NULL),
    rpc_proxy_(NULL),
    location_cb_(NULL)
{ }

ObSharedLogGarbageCollector::~ObSharedLogGarbageCollector()
{
  destroy();
}

int ObSharedLogGarbageCollector::init(const common::ObAddr &self,
                                      const bool handle_tenant_drop,
                                      common::ObMySQLProxy *sql_proxy,
                                      obrpc::ObLogServiceRpcProxy *rpc_proxy,
                                      palf::PalfLocationCacheCb *location_cb)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "ObSharedLogGarbageCollector init twice", K(ret));
  } else if (false == self.is_valid() ||
             OB_ISNULL(sql_proxy) ||
             OB_ISNULL(rpc_proxy) ||
             OB_ISNULL(location_cb)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), K(self), KP(sql_proxy),
        KP(rpc_proxy), KP(location_cb));
  } else if (OB_FAIL(block_mgr_.init())) {
    PALF_LOG(WARN, "block_mgr_ init failed", K(ret));
  } else {
    self_ = self;
    handle_tenant_drop_ = handle_tenant_drop;
    sql_proxy_ = sql_proxy;
    rpc_proxy_ = rpc_proxy;
    location_cb_ = location_cb;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObSharedLogGarbageCollector init", K(ret),
        K(self), KP(sql_proxy));
  }
  return ret;
}

void ObSharedLogGarbageCollector::destroy()
{
  CLOG_LOG(INFO, "ObSharedLogGarbageCollector destroy", K_(self), KP_(sql_proxy));
  stop();
  wait();
  is_inited_ = false;
  self_.reset();
  proposal_id_ = palf::INVALID_PROPOSAL_ID;
  is_leader_ = false;
  opts_.reset();
  block_mgr_.destroy();
  handle_tenant_drop_ = false;
  sql_proxy_ = NULL;
  location_cb_ = NULL;
}

int ObSharedLogGarbageCollector::start()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "ObSharedTimer is NULL", K(ret), KP(timer));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, GC_INTERVAL_US, true))) {
    PALF_LOG(WARN, "ObSharedLogGarbageCollector TG_SCHEDULE failed", K(ret));
  } else {
    PALF_LOG(INFO, "ObSharedLogGarbageCollector start success", KPC(this));
  }
  return ret;
}

void ObSharedLogGarbageCollector::stop()
{
  omt::ObSharedTimer *timer = NULL;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ObSharedTimer is NULL", K(ret), KP(timer));
  } else {
    TG_CANCEL_TASK(timer->get_tg_id(), *this);
    PALF_LOG(INFO, "ObSharedLogGarbageCollector stop finished", KPC(this));
  }
}

void ObSharedLogGarbageCollector::wait()
{
  omt::ObSharedTimer *timer = NULL;
  if (OB_ISNULL(timer = MTL(omt::ObSharedTimer*))) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ObSharedTimer is NULL", K(ret), KP(timer));
  } else {
    TG_WAIT_TASK(timer->get_tg_id(), *this);
    PALF_LOG(INFO, "ObSharedLogGarbageCollector wait finished", KPC(this));
  }
}

void ObSharedLogGarbageCollector::runTimerTask()
{
  int ret = OB_SUCCESS;
  opts_.refresh();
  if (OB_SUCC(switch_to_leader_())) {
    if (handle_tenant_drop_) {
      handle_tenant_gc_();
    }
    handle_tenant_block_gc_(MTL_ID());

  } else if (OB_FAIL(switch_to_follower_())) {
    CLOG_LOG(ERROR, "switch_to_follower_ failed", KR(ret), KPC(this));
  }
}

int ObSharedLogGarbageCollector::handle_tenant_gc_()
{
  int ret = OB_SUCCESS;
  // 1. get deleted tenants
  // select distinct(tenant_id) as tenant_id from __all_tenant_history where IS_DELETED = 1 order by tenant_id;
  // 2. get tenants whose ls replicas are ready for GC
  // select count(*) from __all_ls_status where tenant_id=?;
  // 3. delete tenants
  TenantIDS deleted_tenant_ids, gc_tenant_ids;
  if (OB_FAIL(get_deleted_tenants_from_table_(deleted_tenant_ids))) {
    CLOG_LOG(WARN, "get_deleted_tenants_from_table_ failed", KR(ret), KPC(this));
  } else if (OB_FAIL(get_tenants_ready_for_gc_(deleted_tenant_ids, gc_tenant_ids))) {
    CLOG_LOG(WARN, "get_tenants_ready_for_gc_ failed", KR(ret), KPC(this), K(deleted_tenant_ids));
  } else if (OB_FAIL(delete_tenants_blocks_(gc_tenant_ids))) {
    CLOG_LOG(WARN, "delete_tenants_blocks_ failed", KR(ret), KPC(this), K(gc_tenant_ids));
  } else { }
  return ret;
}

int ObSharedLogGarbageCollector::get_deleted_tenants_from_table_(TenantIDS &tenant_ids) const
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  auto get_dropped_tenants = [&tenant_ids](common::sqlclient::ObMySQLResult *result) -> int
  {
    int ret = OB_SUCCESS;
    uint64_t tenant_id;
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "EXTRACT_INT_FIELD_MYSQL failed", KR(ret));
    } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      CLOG_LOG(WARN, "push_back failed", KR(ret), K(tenant_id));
    } else {
      CLOG_LOG(INFO, "push_back tenant_id success", KR(ret), K(tenant_id));
    }
    return ret;
  };
  common::ObSqlString sql_string;
  ObLogTableOperator table_operator;
  if (OB_FAIL(sql_string.assign_fmt("select distinct(tenant_id) as tenant_id "
      "from %s where IS_DELETED = 1 order by tenant_id", 
      OB_ALL_TENANT_HISTORY_TNAME))) {
    CLOG_LOG(WARN, "construct sql failed", KR(ret), KPC(this));
  } else if (OB_FAIL(table_operator.exec_read(OB_SYS_TENANT_ID, sql_string, *sql_proxy_, get_dropped_tenants))) {
    CLOG_LOG(WARN, "exec_read failed", KR(ret), K(sql_string));
  } else {
    CLOG_LOG(INFO, "get_deleted_tenants_from_table_ success", KR(ret), KPC(this), K(tenant_ids));
  }
  return ret;
}

int ObSharedLogGarbageCollector::get_tenants_ready_for_gc_(
    const TenantIDS &tenant_ids,
    TenantIDS &gc_tenant_ids) const
{
  int ret = OB_SUCCESS;
  gc_tenant_ids.reset();
  return ret;
}

int ObSharedLogGarbageCollector::delete_tenants_blocks_(const TenantIDS &tenant_ids)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(tenant_ids, idx, cnt, true) {
    int64_t start_ts = ObTimeUtility::current_time();
    const uint64_t tenant_id = tenant_ids.at(idx);
    // Note: optimization, just delete tenants whose blocks are stored in shared storage
    // TODO by runlin: need delete it immediately?
    if (OB_FAIL(delete_tenant_blocks_(tenant_id))) {
      CLOG_LOG(WARN, "delete_tenant_blocks_ failed", KR(ret), K(tenant_id));
    } else {
      int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      CLOG_LOG(INFO, "delete_tenant_blocks_ finish", KR(ret), K(tenant_id), K(cost_ts));
    }
  }
  return ret;
}

int ObSharedLogGarbageCollector::delete_tenant_blocks_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSharedLogUtils::delete_tenant(tenant_id))) {
    CLOG_LOG(WARN, "delete_tenant failed", K(tenant_id));
  }
  return ret;
}

int ObSharedLogGarbageCollector::handle_tenant_block_gc_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::SCN gts, retention_scn;
  const bool only_existing_ls = false;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, nullptr, gts))) {
    CLOG_LOG(WARN, "get_gts failed", KR(ret), K(tenant_id));
  } else if (false == opts_.is_valid() || false == gts.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "GCOptions is invalid or scn is invalid", KR(ret), K(tenant_id), K_(opts), K(gts));
  } else if (gts.convert_to_ts() < opts_.retention_time_us_) {
    ret = OB_NEED_RETRY;
    CLOG_LOG(WARN, "gts is smaller than retention_time_us_", KR(OB_NEED_RETRY), K(tenant_id), K_(opts), K(gts));
  } else if (OB_FAIL(retention_scn.convert_from_ts(gts.convert_to_ts() - opts_.retention_time_us_))) {
    CLOG_LOG(WARN, "convert_from_ts failed", KR(ret), K(gts), K_(opts), K(retention_scn));
    // query __all_ls table
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(handle_ls_block_gc_(tenant_id, retention_scn))) {
      CLOG_LOG(WARN, "handle_ls_block_gc_ failed", KR(tmp_ret), K(tenant_id),
          K(retention_scn));
    }
  }
  return ret;
}

/**
@badcases:
  1. the last file of a ls can not be deleted if the ls has been deleted
*/
int ObSharedLogGarbageCollector::handle_ls_block_gc_(
    const uint64_t tenant_id,
    const share::SCN &retention_scn)
{
  #define COMMON_LOG_INFO K(tenant_id), K(ls_id)
  int ret = OB_SUCCESS;

  const share::ObLSID &ls_id = SYS_LS;
  share::SCN min_using_scn = share::SCN::min_scn();
  share::SCN recycle_block_scn, recycle_scn;
  palf::block_id_t recycle_block_id = palf::LOG_INVALID_BLOCK_ID;
  palf::block_id_t min_block_id = palf::LOG_INVALID_BLOCK_ID;
  palf::LSN min_using_upper_lsn;

  if (OB_FAIL(block_mgr_.get_shared_min_block_id(tenant_id, ls_id, min_block_id))) {
    CLOG_LOG(WARN, "get_shared_min_block_id failed", KR(ret), COMMON_LOG_INFO);
    if (OB_HASH_NOT_EXIST == ret) {
      // refresh min_block_id cache on demand
      block_mgr_.refresh_shared_min_block_id(tenant_id, ls_id);
    }
  } else if (OB_FAIL(get_min_using_scn_(tenant_id, min_using_scn, min_using_upper_lsn))) {
    CLOG_LOG(WARN, "get_min_using_scn_ failed", KR(ret), K(tenant_id));
  } else if (FALSE_IT(recycle_scn = MIN(min_using_scn, retention_scn))) {
  } else if (OB_FAIL(locate_block_id_by_scn_(tenant_id, ls_id, min_block_id,
      palf::lsn_2_block(min_using_upper_lsn, palf::PALF_BLOCK_SIZE), recycle_scn,
      recycle_block_id, recycle_block_scn))) {
    CLOG_LOG(WARN, "locate_block_id_by_scn_ failed", KR(ret), COMMON_LOG_INFO, K(recycle_scn));
  } else if (recycle_block_scn > recycle_scn) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "locate_block_id_by_scn_ failed", KR(ret), COMMON_LOG_INFO,
        K(recycle_scn), K(recycle_block_id), K(recycle_block_scn));
  } else if (palf::LOG_INVALID_BLOCK_ID == min_block_id ||
      palf::LOG_INVALID_BLOCK_ID == recycle_block_id ||
      min_block_id > recycle_block_id) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "block_id is invalid", KR(ret), COMMON_LOG_INFO,
        K(min_block_id), K(recycle_block_id), K(recycle_scn));
  } else if (OB_FAIL(block_mgr_.delete_blocks(tenant_id, ls_id,
      min_block_id, recycle_block_id))) {
    CLOG_LOG(WARN, "delete_blocks failed", KR(ret), COMMON_LOG_INFO,
        K(min_block_id), K(recycle_block_id), K(recycle_scn));
  } else {
    CLOG_LOG(INFO, "delete_blocks success", KR(ret), COMMON_LOG_INFO, K(min_block_id),
        K(recycle_block_id), K(recycle_scn), K(min_using_scn), K(retention_scn));
    // record
  }
  #undef COMMON_LOG_INFO
  return ret;
}

int ObSharedLogGarbageCollector::get_min_using_scn_(
    const uint64_t tenant_id,
    share::SCN &min_using_scn,
    palf::LSN &min_using_upper_lsn) const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = SYS_LS;
  min_using_scn.reset();
  CKPTPair ckpt;

  if (OB_FAIL(get_min_ckpt_from_all_replicas_(tenant_id, ls_id, ckpt))) {
    CLOG_LOG(WARN, "get_min_ckpt_from_all_replicas_ failed", K(ret),
        K(tenant_id), "ckpt_scn", ckpt.first, "ckpt_lsn", ckpt.second);
  } else {
    min_using_scn = ckpt.first;
    // NB: nowdays, min_using_upper_lsn is the lsn of block header which contains checkpoint
    //     lsn because of:
    // 1. for upload, we need get the newest block of shared storage, otherwise there is
    //    unexpected error if can not find any block on shared storage and the base lsn
    //    of local is not PALF_INITIAL_LSN_VAL;
    // 2. for migrate, we need get the prev log group entry before checkpoint lsn to construct
    //    PalfBaseInfo.
    min_using_upper_lsn.val_ = (palf::lsn_2_block(ckpt.second, \
        palf::PALF_BLOCK_SIZE)) * palf::PALF_BLOCK_SIZE;
  }
  CLOG_LOG(INFO, "get_min_using_scn_ finish", K(ret), K(tenant_id),
      K(min_using_scn), K(min_using_upper_lsn),
      "ckpt_scn", ckpt.first, "ckpt_lsn", ckpt.second);
  return ret;
}

int ObSharedLogGarbageCollector::get_min_ckpt_from_all_replicas_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    CKPTPair &ckpt) const
{
  #define COMMON_LOG_INFO K(tenant_id), K(ls_id)
  int ret = OB_SUCCESS;
  const int64_t rpc_timeout_us = GCONF.rpc_timeout;
  common::ObAddr leader;
  const bool is_to_leader = true;
  LogGetPalfStatReq palf_stat_req(self_, ls_id.id(), is_to_leader);
  LogGetPalfStatResp palf_stat_resp, dup_palf_stat_resp;
  palf::PalfStat &palf_stat = palf_stat_resp.palf_stat_;
  int64_t migrating_idx = -1;
  CKPTList ckpt_list;

  if (OB_FAIL(location_cb_->nonblock_get_leader(tenant_id, ls_id.id(), leader))) {
    CLOG_LOG(WARN, "location adapter is not inited", K(ret), COMMON_LOG_INFO);
  } else if (OB_FAIL(rpc_proxy_->to(leader).timeout(rpc_timeout_us).trace_time(true).
      max_process_handler_time(rpc_timeout_us).by(MTL_ID()).group_id(share::OBCG_CLOG).
      get_palf_stat(palf_stat_req, palf_stat_resp))) {
    CLOG_LOG(WARN, "get_palf_stat failed", K(ret), COMMON_LOG_INFO, K(palf_stat_req));
    // send RPC fail or not master, need renew leader
  } else if (palf_stat.paxos_member_list_.get_member_number() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid paxos_member_list", COMMON_LOG_INFO, K(leader), K(palf_stat));
  } else if (-1 != (migrating_idx = find_first_migrating_member_(palf_stat.learner_list_))) {
    ret = OB_EAGAIN;
    CLOG_LOG(INFO, "do not delete blocks, REASON: migration", K(ret), COMMON_LOG_INFO,
        "learner_list", palf_stat.learner_list_, K(migrating_idx));
  } else if (OB_FAIL(get_members_ckpt_scn_(tenant_id, ls_id,
      palf_stat.paxos_member_list_, ckpt_list))) {
    CLOG_LOG(WARN, "get_members_ckpt_scn_ failed", K(ret), COMMON_LOG_INFO,
        "paxos_member_list", palf_stat.paxos_member_list_, K(ckpt_list));
  } else if (OB_FAIL(get_members_ckpt_scn_(tenant_id, ls_id,
      palf_stat.learner_list_, ckpt_list))) {
    CLOG_LOG(WARN, "get_members_ckpt_scn_ failed", K(ret), COMMON_LOG_INFO,
        "learner_list", palf_stat.learner_list_, K(ckpt_list));
  } else if (OB_FAIL(rpc_proxy_->to(leader).timeout(rpc_timeout_us).trace_time(true).
      max_process_handler_time(rpc_timeout_us).by(MTL_ID()).group_id(share::OBCG_CLOG).
      get_palf_stat(palf_stat_req, dup_palf_stat_resp))) {
    CLOG_LOG(WARN, "get_palf_stat failed", K(ret), COMMON_LOG_INFO, K(palf_stat_req));
    // send RPC fail or not master, need renew leader
  } else if (palf_stat.config_version_ != dup_palf_stat_resp.palf_stat_.config_version_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "config_version changed", K(ret), COMMON_LOG_INFO,
        K(palf_stat), K(dup_palf_stat_resp));
  } else {
    // sort ckpt_list in ascending order
    std::qsort(&ckpt_list[0], ckpt_list.count(), sizeof(CKPTPair),
        [](const void *x, const void *y) {
            const CKPTPair &arg1 = *(static_cast<const CKPTPair *>(x));
            const CKPTPair &arg2 = *(static_cast<const CKPTPair *>(y));
            if (arg1.first < arg2.first) {
              return -1;
            } else if (arg1.first > arg2.first) {
              return 1;
            } else {
              return 0;
            }
        });
    if (false == ckpt_list[0].first.is_valid()) {
      ret = OB_EAGAIN;
    } else {
      ckpt = ckpt_list[0];
    }
  }
  #undef COMMON_LOG_INFO
  return ret;
}

int64_t ObSharedLogGarbageCollector::find_first_migrating_member_(
    const GlobalLearnerList &learner_list) const
{
  int ret = OB_SUCCESS;
  int64_t result_idx = -1;
  for (int i = 0; i < learner_list.get_member_number() && OB_SUCC(ret); i++) {
    common::ObMember member;
    if (OB_FAIL(learner_list.get_member_by_index(i, member))) {
      CLOG_LOG(WARN, "get_member_by_index failed", KR(ret), K(learner_list), K(i));
    } else if (member.is_migrating()) {
      result_idx = i;
      break;
    }
  }
  return result_idx;
}

int ObSharedLogGarbageCollector::locate_block_id_by_scn_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const palf::block_id_t &min_block_id,
    const palf::block_id_t &max_block_id,
    const share::SCN &in_scn,
    palf::block_id_t &out_block_id,
    share::SCN &out_block_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSharedLogUtils::locate_by_scn_coarsely(tenant_id, ls_id, min_block_id,
      max_block_id, in_scn, out_block_id, out_block_scn))) {
    CLOG_LOG(WARN, "locate_by_scn_coarsely failed", KR(ret), KPC(this), K(tenant_id), K(ls_id),
        K(min_block_id), K(max_block_id), K(in_scn), K(out_block_id), K(out_block_scn));
  }
  return ret;
}

int ObSharedLogGarbageCollector::switch_to_leader_()
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_srv = MTL(logservice::ObLogService*);
  share::ObLSID ls_id(ObLSID::SYS_LS_ID);
  common::ObRole role = INVALID_ROLE;
  int64_t proposal_id = palf::INVALID_PROPOSAL_ID;
  if (OB_ISNULL(log_srv)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "log_srv is nullptr, unexpected error", KR(ret), KPC(this));
  } else if (OB_FAIL(log_srv->get_palf_role(ls_id, role, proposal_id))) {
    CLOG_LOG(TRACE, "get_palf_role failed", KR(ret), KPC(this), K(ls_id));
  } else if (common::LEADER == role) {
    proposal_id_ = proposal_id;
    is_leader_ = true;
  } else {
    ret = OB_NOT_MASTER;
  }
  return ret;
}

int ObSharedLogGarbageCollector::switch_to_follower_()
{
  proposal_id_ = palf::INVALID_PROPOSAL_ID;
  is_leader_ = false;
  return OB_SUCCESS;
}

bool ObSharedLogGarbageCollector::GCOptions::is_valid() const
{
  return tenant_retention_time_us_ >= 0 && retention_time_us_ >= 0;
}

void ObSharedLogGarbageCollector::GCOptions::refresh()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    retention_time_us_ = (tenant_config->shared_log_retention);
  }
  if (REACH_TIME_INTERVAL(100 * 1000 * 1000)) {
    CLOG_LOG(TRACE, "refresh shared_log_retention", KPC(this));
  }
}

int ObSharedLogMinBlockIDCache::init()
{
  int ret = OB_SUCCESS;
  const ObMemAttr bucket_attr(MTL_ID(), "MinBlockCache");
  if (OB_FAIL(block_id_map_.create(BUCKET_NUM, bucket_attr))) {
    CLOG_LOG(WARN, "create map failed");
  } else {
    is_inited_ = true;
    CLOG_LOG(INFO, "ObSharedLogMinBlockIDCache init");
  }
  return ret;
}

void ObSharedLogMinBlockIDCache::destroy()
{
  is_inited_ = false;
  block_id_map_.destroy();
}

int ObSharedLogMinBlockIDCache::get_shared_min_block_id(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    palf::block_id_t &min_block_id) const
{
  int ret = OB_SUCCESS;
  min_block_id = palf::LOG_INVALID_BLOCK_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TENANT_ID == tenant_id || false == ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id));
  } else if (OB_FAIL(block_id_map_.get_refactored(TenantLSID(tenant_id, ls_id), min_block_id))) {
    CLOG_LOG(WARN, "get_refactored failed", K(tenant_id), K(ls_id));
  } else {
    CLOG_LOG(TRACE, "get_shared_min_block_id success", K(tenant_id), K(ls_id), K(min_block_id));
  }
  return ret;
}

int ObSharedLogMinBlockIDCache::refresh_shared_min_block_id(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  palf::block_id_t min_block_id = palf::LOG_INVALID_BLOCK_ID;
  UpdateMapOp update_op;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TENANT_ID == tenant_id || false == ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ObSharedLogUtils::get_oldest_block(tenant_id, ls_id, min_block_id)) ||
      palf::LOG_INVALID_BLOCK_ID == min_block_id) {
    CLOG_LOG(WARN, "get_min_block_id_ failed", K(tenant_id), K(ls_id), K(min_block_id));
  } else if (OB_FAIL(update_op.set_block_id(min_block_id))) {
    CLOG_LOG(WARN, "set_block_id failed", K(min_block_id));
  } else if (OB_FAIL(block_id_map_.set_or_update(TenantLSID(tenant_id, ls_id), min_block_id, update_op))) {
    CLOG_LOG(WARN, "set_refactored failed", K(tenant_id), K(ls_id));
  } else {
    CLOG_LOG(INFO, "refresh_shared_min_block_id success", K(tenant_id),
        K(ls_id), K(min_block_id));
  }
  return ret;
}

int ObSharedLogMinBlockIDCache::delete_blocks(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const palf::block_id_t &min_block_id,
    const palf::block_id_t &max_block_id)
{
  int ret = OB_SUCCESS;
  #define COMMON_LOG_INFO K(tenant_id), K(ls_id), K(min_block_id), K(max_block_id)

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TENANT_ID == tenant_id ||
      false == ls_id.is_valid() ||
      palf::LOG_INVALID_BLOCK_ID == min_block_id ||
      palf::LOG_INVALID_BLOCK_ID == max_block_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", COMMON_LOG_INFO);
  } else {
    // Note: use batch operations to delete multiple blocks may cause
    // holes in shared storages, therefore, we delete blocks sequentially.
    for (palf::block_id_t id = min_block_id; id < max_block_id; id++) {
      UpdateMapOp update_op(id + 1);
      if (OB_FAIL(ObSharedLogUtils::delete_blocks(tenant_id, ls_id, id, id + 1))) {
        CLOG_LOG(WARN, "delete_blocks_ failed", COMMON_LOG_INFO);
      } else if (OB_FAIL(block_id_map_.set_or_update(TenantLSID(tenant_id, ls_id),
          id + 1, update_op))) {
        CLOG_LOG(WARN, "set_refactored failed", COMMON_LOG_INFO);
      }
    }
    CLOG_LOG(INFO, "delete_blocks success", COMMON_LOG_INFO);
  }
  return ret;
  #undef COMMON_LOG_INFO
}

} // end namespace logservice
} // end namespace oceanbase
