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

#ifndef OCEANBASE_LOGSERVICE_PALF_ENV_LITE_MGR
#define OCEANBASE_LOGSERVICE_PALF_ENV_LITE_MGR

#include <sys/types.h>
#include "lib/hash/ob_link_hashmap.h"                                       // ObLinkHashMap
#include "lib/lock/ob_spin_lock.h"                                          // ObSpinLock
#include "lib/utility/ob_macro_utils.h"                                     // DISALLOW_COPY_AND_ASSIGN
#include "share/allocator/ob_tenant_mutil_allocator.h"                      // ObTenantMutilAllocator
#include "logservice/arbserver/ob_arb_gc_utils.h"                           // GCMsgEpoch
#include "logservice/arbserver/ob_arb_monitor.h"                            // ObArbMonitor
#include "lib/hash/ob_hashmap.h"                                            // ObHashMap
#include "lib/function/ob_function.h"                                       // ObFunction
#include "palf_env_lite.h"                                                  // PalfEnvLite

namespace oceanbase
{
namespace share
{
class ObTenantRole;
}
namespace common
{
class ObILogAllocator;
} // namespace common
namespace rpc
{
namespace frame
{
class ObReqTransport;
} // namespace frame
} // namespace rpc
namespace palf
{
class ILogBlockPool;
class IPalfEnvImpl;
}
namespace palflite
{

class DummyBlockPool : public palf::ILogBlockPool {
public:
  virtual int create_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path,
                              const int64_t block_size) override final;
  virtual int remove_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path) override final;
};

class PalfEnvImplFactory
{
public:
  static PalfEnvLite *alloc();
  static void free(PalfEnvLite *palf_env_lite);
};

class PalfEnvImplAlloc
{
public:
  typedef common::LinkHashNode<PalfEnvKey> Node;

  static PalfEnvLite* alloc_value();

  static void free_value(PalfEnvLite *palf_env_lite);

  static Node *alloc_node(PalfEnvLite *palf_env_lite);

  static void free_node(Node *node);
};

struct ClusterMetaInfo {
  ClusterMetaInfo();
  ClusterMetaInfo(const ClusterMetaInfo &meta);
  int assign(const ClusterMetaInfo &meta);
  ~ClusterMetaInfo();
  void generate_by_default();
  bool is_valid() const;
  arbserver::GCMsgEpoch epoch_;
  int64_t tenant_count_;
  char cluster_name_[OB_MAX_CLUSTER_NAME_LENGTH + 1];
  TO_STRING_KV(K_(epoch), K_(tenant_count), K_(cluster_name));
};

typedef common::ObLinkHashMap<PalfEnvKey, PalfEnvLite, PalfEnvImplAlloc> PalfEnvLiteMap;
class PalfEnvLiteMgr
{
public:
  PalfEnvLiteMgr();
  virtual ~PalfEnvLiteMgr();
  static PalfEnvLiteMgr &get_instance();
public:
  int init(const char *base_dir,
           const common::ObAddr &self,
           rpc::frame::ObReqTransport *transport,
           share::ObLocalDevice *log_local_device,
           share::ObResourceManager *resource_manager,
           common::ObIOManager *io_manager);

  void destroy();
  int create_palf_env_lite(const PalfEnvKey &palf_env_key);
  int remove_palf_env_lite(const PalfEnvKey &palf_env_key);
  int get_palf_env_lite(const PalfEnvKey &palf_env_key,
                        PalfEnvLite *&palf_env_lite);
  void revert_palf_env_lite(PalfEnvLite *palf_env_lite);
  int check_and_prepare_dir(const char *dir);
  int remove_dir(const char *dir);
  int remove_dir_while_exist(const char *dir);
  bool is_cluster_placeholder_exists(const int64_t cluster_id) const;
  int add_cluster(const common::ObAddr &src_server,
                  const int64_t cluster_id,
                  const common::ObString &cluster_name,
                  const arbserver::GCMsgEpoch &epoch);
  int remove_cluster(const common::ObAddr &src_server,
                     const int64_t cluster_id,
                     const common::ObString &cluster_name,
                     const arbserver::GCMsgEpoch &epoch);
  int create_arbitration_instance(const PalfEnvKey &PalfEnvKey,
                                  const common::ObAddr &src_server,
                                  const int64_t id,
                                  const share::ObTenantRole &tenant_role);
  int delete_arbitration_instance(const PalfEnvKey &palf_env_key,
                                  const common::ObAddr &src_server,
                                  const int64_t &id);
  int set_initial_member_list(const PalfEnvKey &palf_env_key,
                              const common::ObAddr &src_server,
                              const int64_t id,
                          	  const common::ObMemberList &member_list,
                              const common::ObMember &arb_member,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list);
  int handle_gc_message(const arbserver::GCMsgEpoch &epoch,
                        const common::ObAddr &src_server,
                        const int64_t cluster_id,
                        arbserver::TenantLSIDSArray &ls_ids);
  int handle_force_clear_arb_cluster_info_message(const common::ObAddr &src_server,
                                            const int64_t src_cluster_id);
  int get_arb_member_info(const PalfEnvKey &palf_env_key,
                          const common::ObAddr &src_server,
                          const int64_t id,
                          palf::ArbMemberInfo &arb_member_info);
  arbserver::ObArbMonitor *get_arb_monitor() { return &monitor_; }
  palf::LogIOAdapter *get_io_adapter() { return &io_adapter_; }
  template <class Functor>
  int for_each(Functor &func);
  TO_STRING_KV(K_(base_dir), K_(self));
private:
  // Traverse all directories
  int do_load_(const char *base_dir,
               const common::ObAddr &self,
               rpc::frame::ObReqTransport *transport,
               common::ObILogAllocator *alloc_mgr,
               palf::ILogBlockPool *log_block_pool);

  int load_cluster_placeholder_(const char *cluster_placeholder_str,
                                const char *base_dir);
  int generate_cluster_placeholder_(const int64_t cluster_id,
                                    const common::ObString &cluster_name,
                                    const char *base_dir);
  int create_palf_env_lite_(const char *palf_env_lit_id_str,
                            const char *base_dir,
                            const common::ObAddr &self,
                            rpc::frame::ObReqTransport *transport,
                            common::ObILogAllocator *alloc_mgr,
                            palf::ILogBlockPool *log_block_poo);

  int create_palf_env_lite_not_guarded_by_lock_(const PalfEnvKey &palf_env_key,
                                                const char *palf_env_log_dir,
                                                const common::ObAddr &self,
                                                rpc::frame::ObReqTransport *transport,
                                                common::ObILogAllocator *alloc_mgr,
                                                palf::ILogBlockPool *log_block_pool);
  int remove_palf_env_lite_not_guarded_by_lock_(const PalfEnvKey &palf_env_key);
  int wait_until_reference_count_to_zero_(const PalfEnvKey &palf_env_key);
  int handle_gc_message_(const arbserver::GCMsgEpoch &epoch,
                         const ObAddr &src_server,
                         const int64_t src_cluster_id,
                         arbserver::TenantLSIDSArray &ls_ids);
  int handle_gc_message_for_one_cluster_(const int64_t cluster_id,
                                         arbserver::TenantLSIDSArray &ls_ids);
  int handle_gc_message_for_one_tenant_(const arbserver::TenantLSIDS &tenant_ls_id,
                                        PalfEnvLite *palf_evn_lite);
  int remove_cluster_(const common::ObAddr &src_server,
                      const int64_t cluster_id,
                      const common::ObString &cluster_name,
                      const arbserver::GCMsgEpoch &epoch);
  bool contains_any_tenant_(const int64_t cluster_id);
  int remove_dir_(const char *dir, bool need_check_exist);
private:
  typedef oceanbase::common::hash::HashMapPair<long, oceanbase::palflite::ClusterMetaInfo> MapPair;
  int try_create_cluster_meta_info_(const int64_t cluster_id);
  int try_remove_cluster_meta_info_(const int64_t cluster_id);
  template <class Callback>
  int update_cluster_meta_info_(const int64_t cluster_id,
                                Callback &callback);
  int get_cluster_meta_info_(const int64_t cluster_id,
                             ClusterMetaInfo &meta_info) const;
  template <class Callback>
  int iterate_cluster_meta_info_(Callback &callback);
private:
  mutable ObSpinLock lock_;
  char base_dir_[OB_MAX_FILE_NAME_LENGTH];
  common::ObAddr self_;
  rpc::frame::ObReqTransport *transport_;
  DummyBlockPool log_block_pool_;
  PalfEnvLiteMap palf_env_lite_map_;
  ObTenantMutilAllocator allocator_;

  mutable ObSpinLock cluster_meta_lock_;
  static constexpr int64_t DEFAULT_CLUSTER_COUNT = 16;
  common::hash::ObHashMap<int64_t, ClusterMetaInfo> cluster_meta_info_map_;
  arbserver::ObArbMonitor monitor_;
  palf::LogIOAdapter io_adapter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(PalfEnvLiteMgr);
};

template<class Functor>
int PalfEnvLiteMgr::for_each(Functor &func)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "PalfEnvLiteMgr not init", KR(ret), KPC(this));
  } else if (OB_FAIL(palf_env_lite_map_.for_each(func))) {
    CLOG_LOG(WARN, "ObLinkHashMap for_each failed", KR(ret), KPC(this));
  } else {
  }
  return ret;
}

template <class Callback>
int PalfEnvLiteMgr::update_cluster_meta_info_(const int64_t cluster_id,
                                              Callback &callback)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cluster_meta_lock_);
  if (OB_FAIL(cluster_meta_info_map_.read_atomic(cluster_id, callback))) {
    CLOG_LOG(WARN, "read_atomic failed", KR(ret), KPC(this));
  }
  return ret;
}

template <class Callback>
int PalfEnvLiteMgr::iterate_cluster_meta_info_(Callback &callback)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cluster_meta_lock_);
  if (OB_FAIL(cluster_meta_info_map_.foreach_refactored(callback))) {
    CLOG_LOG(WARN, "foreach_refactored failed", KR(ret), KPC(this));
  }
  return ret;
}

} // end namespace palflite
} // end namespace oceanbase

#endif
