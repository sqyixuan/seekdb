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

#ifndef OCEANBASE_LOGSERVICE_PALF_ENV_LITE_
#define OCEANBASE_LOGSERVICE_PALF_ENV_LITE_
#include <sys/types.h>
#include "common/ob_member_list.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/guard/ob_unique_guard.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "share/ob_occam_timer.h"
#include "logservice/palf/log_loop_thread.h"
#include "logservice/palf/palf_env_impl.h"
#include "logservice/palf/palf_options.h"
#include "logservice/arbserver/arb_tg_helper.h"
#include "logservice/palf/log_throttle.h"
#include "logservice/palf/log_io_utils.h"
namespace oceanbase
{
namespace common
{
class ObILogAllocator;
}
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}
namespace palflite
{
class PalfEnvLiteMgr;
struct PalfEnvKey {
  PalfEnvKey() : cluster_id_(-1), tenant_id_(OB_INVALID_TENANT_ID) {}
  explicit PalfEnvKey(const int64_t cluster_id, const int64_t tenant_id)
    : cluster_id_(cluster_id), tenant_id_(tenant_id) {}
  ~PalfEnvKey() {reset();}
  PalfEnvKey(const PalfEnvKey &key)
  {
    this->cluster_id_ = key.cluster_id_;
    this->tenant_id_ = key.tenant_id_;
  }
  PalfEnvKey &operator=(const PalfEnvKey &other)
  {
    this->cluster_id_ = other.cluster_id_;
    this->tenant_id_ = other.tenant_id_;
    return *this;
  }

  bool operator==(const PalfEnvKey &palf_env_key) const
  {
    return this->compare(palf_env_key) == 0;
  }
  bool operator!=(const PalfEnvKey &palf_env_key) const
  {
    return this->compare(palf_env_key) != 0;
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&cluster_id_, sizeof(cluster_id_), hash_val);
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int compare(const PalfEnvKey &palf_env_key) const
  {
    if (palf_env_key.cluster_id_ < cluster_id_
        || (palf_env_key.cluster_id_ == cluster_id_
          && palf_env_key.tenant_id_ < tenant_id_)) {
      return 1;
    } else if (palf_env_key.cluster_id_ == cluster_id_
        && palf_env_key.tenant_id_ == tenant_id_) {
      return 0;
    } else {
      return -1;
    }
  }
  bool operator < (const PalfEnvKey &palf_env_key)
  {
    return -1 == compare(palf_env_key);
  }
  void reset() {cluster_id_ = -1; tenant_id_ = OB_INVALID_TENANT_ID;}
  bool is_valid() const {return is_valid_cluster_id(cluster_id_) && is_valid_tenant_id(tenant_id_);}
  int64_t cluster_id_;
  uint64_t tenant_id_;
  TO_STRING_KV(K_(cluster_id), K_(tenant_id));
};

class PalfEnvLite : public LinkHashValue<PalfEnvKey>, public palf::IPalfEnvImpl
{
public:
  friend class PalfEnvLiteMgr;
  PalfEnvLite();
  virtual ~PalfEnvLite();
public:
  int init(const char *base_dir,
           const common::ObAddr &self,
           const int64_t cluster_id,
           const int64_t tenant_id,
           rpc::frame::ObReqTransport *transport,
           common::ObILogAllocator *alloc_mgr,
           palf::ILogBlockPool *log_block_pool,
           PalfEnvLiteMgr *palf_env_mgr);
  // start function contains two meanings:
  //
  // 1. Start all types of worker threads included in PalfEnvLite
  // 2. According to the log_storage and meta_storage files included in base_dir, load all the required metadata and logs for the log streams, and perform fault recovery
  //
  // @return :TODO
  int start();
  void stop();
  void wait();
  void destroy();
  void set_deleted();
  bool has_set_deleted() const;
public:
  // @param [in] palf_id, identifier of the log stream to be created
  // @param [in] palf_base_info, palf's log start information
  // @param [out] palf_handle_impl, the generated palf_handle_impl object after successful creation
  //                           When the palf_handle_impl object is no longer in use, the caller needs to execute revert_palf_handle_impl
  // @return :TODO
  int create_palf_handle_impl(const int64_t palf_id,
                              const palf::AccessMode &access_mode,
                              const palf::PalfBaseInfo &palf_base_info,
                              palf::IPalfHandleImpl *&palf_handle_impl) override final;
  // Delete log stream, called by Garbage Collector
  //
  // @param [in] palf_id, the identifier of the log stream to be deleted
  //
  // @return :TODO
  int get_palf_handle_impl(const int64_t palf_id,
                           palf::IPalfHandleImplGuard &palf_handle_impl_guard) override final;
  int get_palf_handle_impl(const int64_t palf_id,
                           palf::IPalfHandleImpl *&palf_handle_impl) override final;
  int remove_palf_handle_impl(const int64_t palf_id) override final;
  void revert_palf_handle_impl(palf::IPalfHandleImpl *palf_handle_impl) override final;
  common::ObILogAllocator* get_log_allocator() override final;
  int for_each(const common::ObFunction<int (palf::IPalfHandleImpl *ipalf_handle_impl)> &func);
  int create_directory(const char *base_dir) override final;
  int remove_directory(const char *ase_dir) override final;
  bool check_disk_space_enough() override final { return true; };
  bool empty() const { return 0 == palf_handle_impl_map_.count(); }
  int get_io_start_time(int64_t &last_working_time);
  int64_t get_tenant_id() override final;
  int update_replayable_point(const SCN &replayable_scn);
  int get_throttling_options(palf::PalfThrottleOptions &option);
  int64_t get_rebuild_replica_log_lag_threshold() const {return 0;}
  void period_calc_disk_usage() override final {}
  palf::LogSharedQueueTh *get_log_shared_queue_thread() override final { return NULL; }
  int get_options(palf::PalfOptions &options);
  INHERIT_TO_STRING_KV("IPalfEnvImpl", palf::IPalfEnvImpl, K_(self), K_(log_dir));

private:
  class ReloadPalfHandleImplFunctor : public palf::ObBaseDirFunctor
  {
  public:
    ReloadPalfHandleImplFunctor(PalfEnvLite *palf_env_lite);
    int func(const struct dirent *entry) override;
  private:
    PalfEnvLite *palf_env_lite_;
  };
  int reload_palf_handle_impl_(const int64_t palf_id);
  class SwitchStateFunctor
  {
  public:
    SwitchStateFunctor() {}
    ~SwitchStateFunctor() {}
    bool operator() (const palf::LSKey &palf_id, palf::PalfHandleImpl *palf_handle_impl);
  };
  struct RemoveStaleIncompletePalfFunctor : public palf::ObBaseDirFunctor {
    RemoveStaleIncompletePalfFunctor(PalfEnvLite *palf_env_lite);
    ~RemoveStaleIncompletePalfFunctor();
    int func(const dirent *entry) override;
    PalfEnvLite *palf_env_lite_;
  };

private:
  int create_palf_handle_impl_(const int64_t palf_id,
                               const palf::AccessMode &access_mode,
                               const palf::PalfBaseInfo &palf_base_info,
                               const palf::LogReplicaType replica_type,
                               palf::PalfHandleImpl *&palf_handle_impl);
  int scan_all_palf_handle_impl_director_();
  int wait_until_reference_count_to_zero_(const int64_t palf_id);
  // check the diskspace whether is enough to hold a new palf instance.
  bool check_can_create_palf_handle_impl_() const;
  int remove_palf_handle_impl_from_map_not_guarded_by_lock_(const int64_t palf_id);
  int move_incomplete_palf_into_tmp_dir_(const int64_t palf_id);
  int check_tmp_log_dir_exist_(bool &exist) const;
  int remove_stale_incomplete_palf_();
  int remove_directory_while_exist_(const char *log_dir);
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  RWLock palf_meta_lock_;
  ObUniqueGuard<arbserver::ArbTGHelper> arb_tg_helper_;
  common::ObILogAllocator *log_alloc_mgr_;
  palf::ILogBlockPool *log_block_pool_;
  palf::LogRpc log_rpc_;
  palf::LogIOTaskCbThreadPool cb_thread_pool_;
  palf::LogIOWorker log_io_worker_;
  palf::LogWritingThrottle throttle_;

  char log_dir_[common::MAX_PATH_SIZE];
  char tmp_log_dir_[common::MAX_PATH_SIZE];
  common::ObAddr self_;

  palf::PalfHandleImplMap palf_handle_impl_map_;
  palf::LogLoopThread log_loop_thread_;

  // last_palf_epoch_ is used to assign increasing epoch for each palf instance.
  int64_t last_palf_epoch_;
  int64_t tenant_id_;

  PalfEnvLiteMgr *palf_env_mgr_;
  bool is_running_;
  bool has_deleted_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(PalfEnvLite);
};
} // end namespace palflite
} // end namespace oceanbase

#endif
