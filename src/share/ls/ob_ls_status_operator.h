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

#ifndef OCEANBASE_SHARE_OB_LS_STATUS_OPERATOR_H_
#define OCEANBASE_SHARE_OB_LS_STATUS_OPERATOR_H_

#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ls/ob_ls_i_life_manager.h" //ObLSLifeIAgent
#include "common/ob_zone.h"//ObZone
#include "common/ob_role.h"//ObRole
#include "lib/container/ob_array.h"//ObArray
#include "lib/container/ob_iarray.h"//ObIArray
#include "common/ob_member_list.h"
#include "share/ob_tenant_info_proxy.h"//tenant switchover status
#include "share/ls/ob_ls_info.h" //ObLSReplica::MemberList
#include "share/ls/ob_ls_recovery_stat_operator.h"  //ObLSRecoveryStat
#include "share/ls/ob_ls_operator.h"

namespace oceanbase
{

namespace common
{
class ObISQLClient;
class ObString;
class ObSqlString;
class ObIAllocator;
class ObMySQLProxy;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class SCN;
}
namespace rootserver
{
class ObZoneManager;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}

class ObLSStatusOperator;

const int64_t MAX_MEMBERLIST_FLAG_LENGTH = 10;
class ObMemberListFlag
{
  OB_UNIS_VERSION(1);
public:
  enum MemberListFlag
  {
    INVALID_FLAG = -1,
    HAS_ARB_MEMBER = 0,
    MAX_FLAG
  };
public:
  ObMemberListFlag() : flag_(INVALID_FLAG) {}
  explicit ObMemberListFlag(MemberListFlag flag) : flag_(flag) {}
  virtual ~ObMemberListFlag() {}

  void reset() { flag_ = INVALID_FLAG; }
  const MemberListFlag &get_flag() const { return flag_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool is_valid() const { return INVALID_FLAG < flag_ && MAX_FLAG > flag_; }
  bool is_arb_member() const { return HAS_ARB_MEMBER == flag_; }

private:
  // 0: has arb member
  MemberListFlag flag_;
};

struct ObLSStatusInfo
{
  struct Compare {
    bool operator() (const ObLSStatusInfo &left, const ObLSStatusInfo &right)
    {
      return left.get_ls_id() < right.get_ls_id();
    }
  };

  ObLSStatusInfo() : tenant_id_(OB_INVALID_TENANT_ID),
                          ls_id_(), ls_group_id_(OB_INVALID_ID),
                          status_(OB_LS_EMPTY), unit_group_id_(OB_INVALID_ID),
                          primary_zone_(), flag_(ObLSFlag::NORMAL_FLAG) {}
  virtual ~ObLSStatusInfo() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id,
           const ObLSID &id, const uint64_t ls_group_id,
           const ObLSStatus status, const uint64_t unit_group_id,
           const ObZone &primary_zone, const ObLSFlag &flag);
  bool ls_is_creating() const
  {
    return ls_is_creating_status(status_);
  }
  bool ls_is_dropping() const
  {
    return ls_is_dropping_status(status_);
  }
  bool ls_is_tenant_dropping() const
  {
    return ls_is_tenant_dropping_status(status_);
  }
  bool ls_is_wait_offline() const
  {
    return ls_is_wait_offline_status(status_);
  }
  bool ls_is_created() const
  {
    return ls_is_created_status(status_);
  }
  bool ls_is_normal() const
  {
    return ls_is_normal_status(status_);
  }
  bool ls_is_create_abort() const
  {
    return ls_is_create_abort_status(status_);
  }
  bool ls_need_create_abort() const
  {
    return ls_need_create_abort_status(status_);
  }
  bool ls_is_pre_tenant_dropping() const
  {
    return ls_is_pre_tenant_dropping_status(status_);
  }
  bool is_duplicate_ls() const
  {
    return flag_.is_duplicate_ls();
  }
  bool ls_is_block_tablet_in() const
  {
    return flag_.is_block_tablet_in();
  }
  ObLSStatus get_status() const
  {
    return status_;
  }

  ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  uint64_t get_ls_group_id() const { return ls_group_id_; }

  ObLSFlag get_flag() const
  {
    return flag_;
  }
  int assign(const ObLSStatusInfo &other);
  void reset();
  bool is_normal() const
  {
    return OB_LS_NORMAL == status_;
  }
  bool operator==(const ObLSStatusInfo &other) const
  {
    return tenant_id_ == other.tenant_id_
      && ls_id_ == other.ls_id_
      && ls_group_id_ == other.ls_group_id_
      && status_ == other.status_
      && unit_group_id_ == other.unit_group_id_
      && primary_zone_ == other.primary_zone_
      && flag_ == other.flag_;
  }

  bool operator!=(const ObLSStatusInfo &other) const {
    return !(*this == other);
  }

  bool is_user_ls() const { return ls_id_.is_user_ls(); }

  uint64_t tenant_id_;
  ObLSID ls_id_;
  uint64_t ls_group_id_;
  ObLSStatus status_;
  uint64_t unit_group_id_;
  ObZone primary_zone_;
  share::ObLSFlag flag_;

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(ls_group_id),
               "status", ls_status_to_str(status_),
               K_(unit_group_id), K_(primary_zone), K_(flag));
};

typedef ObArray<ObLSStatusInfo> ObLSStatusInfoArray;
typedef ObIArray<ObLSStatusInfo> ObLSStatusInfoIArray;

/*
 * description : read or write __all_ls_status
*/
class ObLSStatusOperator : public ObLSLifeIAgent, public ObLSTemplateOperator
{
 public:
  ObLSStatusOperator() {};
  virtual ~ObLSStatusOperator(){}
public:
  /*
   * description: override of ObLSLifeIAgent
   * @param[in] ls_info: ls info
   * @param[in] create_ls_scn: ls's create scn
   * @param[in] zone_priority: for __all_ls_election_reference_info
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] trans:*/
  virtual int create_new_ls(const ObLSStatusInfo &ls_info,
                            const SCN &current_tenant_scn,
                            const common::ObString &zone_priority,
                            const share::ObTenantSwitchoverStatus &working_sw_status,
                            ObMySQLTransaction &trans) override;
  /*
   * description: override of ObLSLifeIAgent
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] trans:*/
  virtual int drop_ls(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) override;
  /*
   * description: for primary cluster set ls to wait offline from tenant_dropping or dropping status 
   * @param[in] tenant_id: tenant_id
   * @param[in] ls_id: need delete ls
   * @param[in] ls_status: tenant_dropping or dropping status 
   * @param[in] drop_scn: there is no user data after drop_scn except offline
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] trans
   * */
  virtual int set_ls_offline(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObLSStatus &ls_status,
                      const SCN &drop_scn,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) override;
public:

  int get_all_ls_status_by_order(const uint64_t tenant_id,
                                 ObLSStatusInfoIArray &ls_array,
                                 ObISQLClient &client);

  // check whether transfer ls contain duplicate scope ls
  // @params[in]  tenant_id, which tenant to get
  // @params[in]  client, client to execute sql
  // @params[in]  src_ls_id, source ls id for transfer
  // @params[in]  dst_ls_id, destination ls id for transfer
  // @params[out] conrain, whether contain duplicate ls
  /**
   * @description:
   *    get ls list from all_ls_status order by tenant_id, ls_id for switchover tenant
   *    if ls status is OB_LS_TENANT_DROPPING or OB_LS_PRE_TENANT_DROPPING
   *       return OB_TENANT_HAS_BEEN_DROPPED
   *
   *    if ls status is OB_LS_CREATING or OB_LS_CREATED
   *       if (ignore_need_create_abort)
   *          ignore ls
   *       else
   *          return OB_ERR_UNEXPECTED
   *
   *    if ls status is OB_LS_CREATE_ABORT
   *       ignore ls
   * 
   * @param[in] tenant_id
   * @param[in] ignore_need_create_abort
   * @param[out] ls_array returned ls list
   * @param[in] client
   * @return return code
   */
  int get_all_ls_status_by_order_for_switch_tenant(const uint64_t tenant_id,
                                 const bool ignore_need_create_abort,
                                 ObLSStatusInfoIArray &ls_array,
                                 ObISQLClient &client);
  int get_ls_init_member_list(const uint64_t tenant_id, const ObLSID &id,
                              ObMemberList &member_list,
                              ObLSStatusInfo &status_info,
                              ObISQLClient &client,
                              ObMember &arb_member,
                              common::GlobalLearnerList &learner_list);
  int get_ls_status_info(const uint64_t tenant_id, const ObLSID &id,
                         ObLSStatusInfo &status_info, ObISQLClient &client,
                         const int32_t group_id = 0);
  int fill_cell(common::sqlclient::ObMySQLResult *result,
                share::ObLSStatusInfo &status_info);
  /*
   * description: get user tenant max ls id, only for compatible 
   * @param[in] tenant_id
   * @param[out] max_id: max ls id of the tenant
   * @param[in] client*/
  int get_tenant_max_ls_id(const uint64_t tenant_id, ObLSID &max_id,
                           ObISQLClient &client);

 /*
   * description: update ls's status 
   * @param[in] tenant_id
   * @param[in] ls_id
   * @param[in] old_status
   * @param[in] new_status
   * @param[in] working_sw_status only support working on specified switchover status
   * @param[in] trans*/
  int update_ls_status_in_trans(
      const uint64_t tenant_id, 
      const ObLSID &id,
      const ObLSStatus &old_status,
      const ObLSStatus &new_status, 
      const ObTenantSwitchoverStatus &working_sw_status,
      ObMySQLTransaction &trans);


private:

  template<typename T> int set_list_with_hex_str_(
      const common::ObString &str,
      T &learner_list,
      ObMember &arb_member);

  int inner_get_ls_status_(const ObSqlString &sql, const uint64_t exec_tenant_id,
                           const bool need_member_list, ObISQLClient &client,
                           ObMemberList &member_list, share::ObLSStatusInfo &status_info,
                           ObMember &arb_member, common::GlobalLearnerList &learner_list,
                           const int32_t group_id);

  int get_ls_status_(const uint64_t tenant_id, const ObLSID &id, const bool need_member_list,
                     ObMemberList &member_list,
                     ObLSStatusInfo &status_info, ObISQLClient &client,
                     ObMember &arb_member, common::GlobalLearnerList &learner_list,
                     const int32_t group_id);

private:
  const int64_t MAX_ERROR_LOG_PRINT_SIZE = 1024;
};

}
}

#endif /* !OCEANBASE_SHARE_OB_LS_STATUS_OPERATOR_H_ */
