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

#ifndef OCEANBASE_LOGSERVICE_OB_SHARED_LOG_GARBAGE_COLLECTOR_
#define OCEANBASE_LOGSERVICE_OB_SHARED_LOG_GARBAGE_COLLECTOR_

#include "lib/container/ob_array.h"                       // ObArray
#include "lib/hash/ob_hashmap.h"                          // ObHashMap
#include "lib/lock/ob_tc_rwlock.h"                        // RWLock
#include "lib/ob_define.h"                                // basic components
#include "lib/task/ob_timer.h"                            // ObTimerTask
#include "lib/utility/ob_macro_utils.h"                   // Macros
#include "share/ob_ls_id.h"                               // ObLSID
#include "share/config/ob_server_config.h"                // GCONF
#include "logservice/common_util/ob_log_ls_define.h"      // TenantLSID
#include "logservice/palf/log_define.h"                   // basic components
#include "logservice/logrpc/ob_log_rpc_proxy.h"           // ObLogServiceRpcProxy
#include "logservice/logrpc/ob_log_rpc_req.h"             // LogGetCkptReq

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace common
{
class ObAddr;
class ObMySQLProxy;
}
namespace obrpc
{
class ObLogServiceRpcProxy;
}

namespace logservice
{

using namespace palf;

class ObSharedLogMinBlockIDCache
{
public:
  ObSharedLogMinBlockIDCache() : is_inited_(false), block_id_map_() { }
  ~ObSharedLogMinBlockIDCache() { destroy(); }
  int init();
  void destroy();
  /**
  @return
  - OB_ENTRY_NOT_EXIST: log files of ls don't exist in shared storage
  */
  int get_shared_min_block_id(const uint64_t tenant_id,
                                const share::ObLSID &ls_id,
                                palf::block_id_t &min_block_id) const;
  int refresh_shared_min_block_id(const uint64_t tenant_id,
                                    const share::ObLSID &ls_id);
  int delete_blocks(const uint64_t tenant_id,
                    const share::ObLSID &ls_id,
                    const palf::block_id_t &min_block_id,
                    const palf::block_id_t &recycle_block_id);
private:
  typedef common::hash::ObHashMap<TenantLSID, block_id_t> LogBlockIDMap;
  static constexpr int64_t BUCKET_NUM = 100;
private:
  class UpdateMapOp
  {
  public:
    UpdateMapOp() : new_block_id_(LOG_INVALID_BLOCK_ID) {}
    UpdateMapOp(const block_id_t block_id) : new_block_id_(block_id) {}
    virtual ~UpdateMapOp() {}
    int set_block_id(const block_id_t block_id)
    {
      new_block_id_ = block_id;
      return OB_SUCCESS;
    }
    void operator() (common::hash::HashMapPair<TenantLSID, block_id_t> &entry)
    {
      entry.second = new_block_id_;
    }
  private:
    block_id_t new_block_id_;
    DISALLOW_COPY_AND_ASSIGN(UpdateMapOp);
  };
private:
  bool is_inited_;
  mutable common::RWLock lock_;
  LogBlockIDMap block_id_map_;
};

class ObSharedLogGarbageCollector : public common::ObTimerTask
{
public:
  ObSharedLogGarbageCollector();
  ~ObSharedLogGarbageCollector();
  int init(const common::ObAddr &self,
           const bool handle_tenant_drop,
           common::ObMySQLProxy *sql_proxy,
           obrpc::ObLogServiceRpcProxy *rpc_proxy,
           palf::PalfLocationCacheCb *location_cb);
  void destroy();
  int start();
  void stop();
  void wait();
  virtual void runTimerTask();
  TO_STRING_KV(K_(self));
private:
  static constexpr int64_t GC_INTERVAL_US = 60 * 1000 * 1000LL; // 60 s
  typedef common::ObSEArray<uint64_t, 32> TenantIDS;
  typedef std::pair<share::SCN, palf::LSN> CKPTPair;
  typedef common::ObArray<CKPTPair> CKPTList;
private:
  class GCOptions
  {
  public:
    GCOptions()
      : tenant_retention_time_us_(DEFAULT_TENANT_RETENTION_US),
        retention_time_us_(DEFAULT_LS_RETENTION_US) { }
    ~GCOptions() { reset(); }
    bool is_valid() const;
    void reset()
    {
      tenant_retention_time_us_ = DEFAULT_TENANT_RETENTION_US;
      retention_time_us_ = DEFAULT_LS_RETENTION_US;
    }
    TO_STRING_KV(K_(tenant_retention_time_us), K_(retention_time_us));
    void refresh();
    static constexpr int64_t DEFAULT_TENANT_RETENTION_US = 0;
    static constexpr int64_t DEFAULT_LS_RETENTION_US = 60 * 60 * 1000 * 1000LL; // 1 hour
    int64_t tenant_retention_time_us_;
    int64_t retention_time_us_;
  };
private:
  // ========== tenant gc ==========
  int handle_tenant_gc_();
  int get_deleted_tenants_from_table_(TenantIDS &tenant_ids) const;
  int get_tenants_ready_for_gc_(const TenantIDS &tenant_ids,
                                TenantIDS &gc_tenant_ids) const;
  int delete_tenants_blocks_(const TenantIDS &tenant_ids);
  int delete_tenant_blocks_(const uint64_t tenant_id);
  // ========== tenant gc ==========
  // ========== block gc ==========
  int handle_tenant_block_gc_(const uint64_t tenant_id);
  int handle_ls_block_gc_(const uint64_t tenant_id,
                          const share::SCN &retention_scn);

  /**
  @descriptions: min_using_scn must be recorded in a log entry whose lsn is less than min_using_upper_lsn 
  */
  int get_min_using_scn_(const uint64_t tenant_id,
                         share::SCN &min_using_scn,
                         palf::LSN &min_using_upper_lsn) const;
  int get_min_ckpt_from_all_replicas_(const uint64_t tenant_id,
                                      const share::ObLSID &ls_id,
                                      CKPTPair &ckpt) const;
  int locate_block_id_by_scn_(const uint64_t tenant_id,
                              const share::ObLSID &ls_id,
                              const palf::block_id_t &min_block_id,
                              const palf::block_id_t &max_block_id,
                              const share::SCN &in_scn,
                              palf::block_id_t &out_block_id,
                              share::SCN &out_block_scn);
  int64_t find_first_migrating_member_(const common::GlobalLearnerList &learner_list) const;
  template <typename LIST>
  int get_members_ckpt_scn_(const uint64_t tenant_id,
                            const share::ObLSID &ls_id,
                            const LIST &list,
                            CKPTList &ckpt_list) const
  {
    int ret = OB_SUCCESS;
    const int64_t rpc_timeout_us = GCONF.rpc_timeout;
    for (int i = 0; i < list.get_member_number() && OB_SUCC(ret); i++) {
      common::ObMember member;
      LogGetCkptReq req(self_, tenant_id, ls_id);
      LogGetCkptResp resp;
      if (OB_FAIL(list.get_member_by_index(i, member))) {
        CLOG_LOG(WARN, "get_member_by_index failed", K(ret), K(list), K(i));
      } else if (OB_FAIL(rpc_proxy_->to(member.get_server()).timeout(rpc_timeout_us).
          trace_time(true).by(tenant_id).group_id(share::OBCG_CLOG).get_ls_ckpt(req, resp))) {
        // TODO: REASON
        CLOG_LOG(WARN, "get_ls_ckpt failed", K(ret), K(tenant_id), K(ls_id), K(req));
      } else if (false == resp.ckpt_scn_.is_valid()) {
        // TODO: REASON
        ret = OB_EAGAIN;
        CLOG_LOG(WARN, "ckpt_scn is invalid", K(ret), K(tenant_id), K(ls_id), K(req));
      } else if (OB_FAIL(ckpt_list.push_back(CKPTPair(resp.ckpt_scn_, resp.ckpt_lsn_)))) {
        CLOG_LOG(WARN, "push_back failed", K(ret), K(tenant_id), K(ls_id), K(ckpt_list));
      }
    }
    return ret;
  }
  // ========== block gc ==========

  int switch_to_leader_();
  int switch_to_follower_();

private:
  bool is_inited_;
  common::ObAddr self_;
  int64_t proposal_id_;
  bool is_leader_;
  GCOptions opts_;
  ObSharedLogMinBlockIDCache block_mgr_;

  bool handle_tenant_drop_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  palf::PalfLocationCacheCb *location_cb_;
};
} // end namespace logservice
} // end namespace oceanbase
#endif // OCEANBASE_LOGSERVICE_OB_SHARED_LOG_GARBAGE_COLLECTOR_

// 1. tenant drop
// 2. min block_id refresh and cache, DONE
// 3. add tenant_id args, DONE
