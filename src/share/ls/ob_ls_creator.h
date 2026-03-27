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

#ifndef OCEANBASE_SHARE_OB_LS_CREATOR_H_
#define OCEANBASE_SHARE_OB_LS_CREATOR_H_

#include "rootserver/ob_rs_async_rpc_proxy.h" //async rpc
#include "share/ob_ls_id.h"//share::ObLSID
#include "share/unit/ob_unit_info.h"//ResourcePoolName

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObUnit;
struct ObAllTenantInfo;
struct ObLSStatusInfo;
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace palf
{
struct PalfBaseInfo;
}
namespace share
{

class SCN;
struct ObLSReplicaAddr
{
  common::ObAddr addr_;
  common::ObReplicaType replica_type_;

  ObLSReplicaAddr()
      : addr_(),
        replica_type_(common::REPLICA_TYPE_INVALID) {}
  void reset() { *this = ObLSReplicaAddr(); }
  int init(const common::ObAddr &addr,
           const common::ObReplicaType replica_type);
  TO_STRING_KV(K_(addr),
               K_(replica_type));

};
typedef common::ObArray<ObLSReplicaAddr> ObLSAddr;
typedef common::ObIArray<ObLSReplicaAddr> ObILSAddr;

class ObLSCreator
{
public:
  ObLSCreator(obrpc::ObSrvRpcProxy &rpc_proxy,
                     const int64_t tenant_id,
                     const share::ObLSID &id,
                     ObMySQLProxy *proxy = NULL)
    :
      create_ls_proxy_(rpc_proxy, &obrpc::ObSrvRpcProxy::create_ls),
      set_member_list_proxy_(rpc_proxy, &obrpc::ObSrvRpcProxy::set_member_list),
      tenant_id_(tenant_id), id_(id), proxy_(proxy) {}
  virtual ~ObLSCreator() {}
  int create_sys_tenant_ls(const obrpc::ObServerInfoList &rs_list,
      const common::ObIArray<share::ObUnit> &unit_array);
  bool is_valid();

private:

  int create_sys_ls_(
      const ObILSAddr &addr,
      const int64_t paxos_replica_num,
      const share::ObAllTenantInfo &tenant_info,
      const common::ObCompatibilityMode &compat_mode,
      const bool create_with_palf,
      const palf::PalfBaseInfo &palf_base_info);
 int create_ls_(const ObILSAddr &addr, const int64_t paxos_replica_num,
                const share::ObAllTenantInfo &tenant_info,
                const SCN &create_scn,
                const common::ObCompatibilityMode &compat_mode,
                const bool create_with_palf,
                const palf::PalfBaseInfo &palf_base_info,
                common::ObMemberList &member_list,
                common::ObMember &arbitration_service,
                common::GlobalLearnerList &learner_list);
 int check_member_list_and_learner_list_all_in_meta_table_(
                const common::ObMemberList &member_list,
                const common::GlobalLearnerList &learner_list);
 int inner_check_member_list_and_learner_list_(
                const common::ObMemberList &member_list,
                const common::GlobalLearnerList &learner_list);
 int construct_paxos_replica_number_to_persist_(
                const int64_t paxos_replica_num,
                const int64_t arb_replica_num,
                const common::ObMemberList &member_list,
                int64_t &paxos_replica_number_to_persist);
 int set_member_list_(const common::ObMemberList &member_list,
                      const common::ObMember &arb_replica,
                      const int64_t paxos_replica_num,
                      const common::GlobalLearnerList &learner_list);
 int check_create_ls_result_(const int64_t paxos_replica_num,
                             const ObIArray<int> &return_code_array,
                             common::ObMemberList &member_list,
                             common::GlobalLearnerList &learner_list,
                             const bool with_arbitration_service,
                             const int64_t arb_replica_num);
 int check_set_memberlist_result_(const ObIArray<int> &return_code_array,
                                  const int64_t paxos_replica_num);
private:
  rootserver::ObLSCreatorProxy create_ls_proxy_;
  rootserver::ObSetMemberListProxy set_member_list_proxy_;
  const int64_t tenant_id_;
  const share::ObLSID id_;
  ObMySQLProxy *proxy_;
};
}
}

#endif /* !OCEANBASE_SHARE_OB_LS_CREATOR_H_ */
