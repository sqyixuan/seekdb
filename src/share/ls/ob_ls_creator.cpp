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

#define USING_LOG_PREFIX SHARE
#include "ob_ls_creator.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ls/ob_ls_life_manager.h"
#include "share/ob_global_stat_proxy.h" // for ObGlobalStatProxy

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;
using namespace oceanbase::palf;

namespace oceanbase
{
namespace share
{
////ObLSReplicaAddr
int ObLSReplicaAddr::init(const common::ObAddr &addr,
           const common::ObReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid()
                  || common::REPLICA_TYPE_INVALID == replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(addr), K(replica_type));
  } else {
    addr_ = addr;
    replica_type_ = replica_type;
  }

  return ret;
}


/////////////////////////
bool ObLSCreator::is_valid()
{
  bool bret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_
      || !id_.is_valid()) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "tenant id or log stream id is invalid", K(bret), K_(tenant_id), K_(id));
  }
  return bret;
}

int ObLSCreator::create_sys_tenant_ls(
    const obrpc::ObServerInfoList &rs_list,
    const common::ObIArray<share::ObUnit> &unit_array)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(0 >= rs_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs list is invalid", KR(ret), K(rs_list));
  } else if (OB_UNLIKELY(rs_list.count() != unit_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(rs_list), K(unit_array));
  } else if (OB_FAIL(tenant_info.init(OB_SYS_TENANT_ID, share::PRIMARY_TENANT_ROLE))) {
    LOG_WARN("tenant info init failed", KR(ret));
  } else {
    ObLSAddr addr;
    const int64_t paxos_replica_num = rs_list.count();
    ObLSReplicaAddr replica_addr;
    const common::ObReplicaType replica_type = common::REPLICA_TYPE_FULL;
    const common::ObCompatibilityMode compat_mode = MYSQL_MODE;
    palf::PalfBaseInfo palf_base_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list.count(); ++i) {
      replica_addr.reset();
      if (rs_list.at(i).zone_ != unit_array.at(i).zone_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone not match", KR(ret), K(rs_list), K(unit_array));
      } else if (OB_FAIL(replica_addr.init(
              rs_list[i].server_,
              replica_type))) {
        LOG_WARN("failed to init replica addr", KR(ret), K(i), K(rs_list), K(replica_type),
                 K(unit_array));
      } else if (OB_FAIL(addr.push_back(replica_addr))) {
        LOG_WARN("failed to push back replica addr", KR(ret), K(i), K(addr),
            K(replica_addr), K(rs_list));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_sys_ls_(addr, paxos_replica_num, tenant_info,
            compat_mode, false/*create_with_palf*/, palf_base_info))) {
      LOG_WARN("failed to create log stream", KR(ret), K_(id), K_(tenant_id),
                                              K(addr), K(paxos_replica_num), K(tenant_info),
                                              K(compat_mode), K(palf_base_info));
    }
  }
  return ret;
}

int ObLSCreator::create_sys_ls_(
    const ObILSAddr &addr,
    const int64_t paxos_replica_num,
    const share::ObAllTenantInfo &tenant_info,
    const common::ObCompatibilityMode &compat_mode,
    const bool create_with_palf,
    const palf::PalfBaseInfo &palf_base_info)
{
  int ret = OB_SUCCESS;
  common::ObMemberList member_list;
  ObMember arbitration_service;
  common::GlobalLearnerList learner_list;
  const SCN create_scn = SCN::base_scn();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(create_ls_(addr, paxos_replica_num, tenant_info,
          create_scn, compat_mode, create_with_palf, palf_base_info,
          member_list, arbitration_service, learner_list))) {
    LOG_WARN("failed to create log stream", KR(ret), K_(id), K_(tenant_id),
        K(addr), K(paxos_replica_num), K(tenant_info),
        K(create_scn), K(compat_mode), K(palf_base_info));
  } else if (OB_FAIL(set_member_list_(member_list, arbitration_service, paxos_replica_num, learner_list))) {
    LOG_WARN("failed to set member list", KR(ret), K(member_list), K(arbitration_service),
        K(paxos_replica_num), K(learner_list));
  }
  return ret;
}

int ObLSCreator::create_ls_(const ObILSAddr &addrs,
                           const int64_t paxos_replica_num,
                           const share::ObAllTenantInfo &tenant_info,
                           const SCN &create_scn,
                           const common::ObCompatibilityMode &compat_mode,
                           const bool create_with_palf,
                           const palf::PalfBaseInfo &palf_base_info,
                           common::ObMemberList &member_list,
                           common::ObMember &arbitration_service,
                           common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  bool need_create_arb_replica = false;
  int64_t arb_replica_count = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(0 >= addrs.count()
                         || 0 >= paxos_replica_num
                         || rootserver::majority(paxos_replica_num) > addrs.count()
                         || !tenant_info.is_valid()
                         || !create_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(addrs), K(paxos_replica_num), K(tenant_info),
        K(create_scn));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else {
      obrpc::ObCreateLSArg arg;
      int tmp_ret = OB_SUCCESS;
      ObArray<int> return_code_array;
      const common::ObReplicaProperty replica_property;
      storage::ObMajorMVMergeInfo major_mv_merge_info;
      major_mv_merge_info.reset();
      lib::Worker::CompatMode new_compat_mode = compat_mode == ORACLE_MODE ?
                                         lib::Worker::CompatMode::ORACLE :
                                         lib::Worker::CompatMode::MYSQL;

      for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); ++i) {
        arg.reset();
        const ObLSReplicaAddr &addr = addrs.at(i);
        if (OB_FAIL(arg.init(tenant_id_, id_, addr.replica_type_,
                replica_property, tenant_info, create_scn, new_compat_mode,
                create_with_palf, palf_base_info, major_mv_merge_info))) {
          LOG_WARN("failed to init create log stream arg", KR(ret), K(addr), K(create_with_palf), K(replica_property),
              K_(id), K_(tenant_id), K(tenant_info), K(create_scn), K(new_compat_mode), K(palf_base_info), K(major_mv_merge_info));
        } else if (OB_TMP_FAIL(create_ls_proxy_.call(addr.addr_, ctx.get_timeout(),
                GCONF.cluster_id, tenant_id_, arg))) {
          LOG_WARN("failed to all async rpc", KR(tmp_ret), K(addr), K(ctx.get_timeout()),
              K(arg), K(tenant_id_));
        }
      }
      //wait all
      if (OB_TMP_FAIL(create_ls_proxy_.wait_all(return_code_array))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to wait all async rpc", KR(ret), KR(tmp_ret));
      }
      if (FAILEDx(check_create_ls_result_(paxos_replica_num, return_code_array, member_list, learner_list,
                                          need_create_arb_replica, arb_replica_count))) {
        LOG_WARN("failed to check ls result", KR(ret), K(paxos_replica_num), K(return_code_array), K(learner_list),
                 K(need_create_arb_replica), K(arb_replica_count));
      }
    }
  }
  return ret;
}

int ObLSCreator::check_create_ls_result_(
    const int64_t paxos_replica_num,
    const ObIArray<int> &return_code_array,
    common::ObMemberList &member_list,
    common::GlobalLearnerList &learner_list,
    const bool with_arbitration_service,
    const int64_t arb_replica_num)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  learner_list.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (return_code_array.count() != create_ls_proxy_.get_results().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc count not equal to result count", KR(ret),
             K(return_code_array.count()), K(create_ls_proxy_.get_results().count()));
  } else {
    const int64_t timestamp = 1;
    // don't use arg/dest here because call() may has failure.
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
      if (OB_SUCCESS != return_code_array.at(i)) {
        LOG_WARN("rpc is failed", KR(ret), K(return_code_array.at(i)), K(i));
      } else {
        const auto *result = create_ls_proxy_.get_results().at(i);
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i));
        } else if (OB_SUCCESS != result->get_result()) {
          LOG_WARN("rpc is failed", KR(ret), K(*result), K(i));
        } else {
          ObAddr addr;
          if (result->get_addr().is_valid()) {
            addr = result->get_addr();
          } else if (create_ls_proxy_.get_dests().count() == create_ls_proxy_.get_results().count()) {
            //one by one match
            addr = create_ls_proxy_.get_dests().at(i);
          }
          //TODO other replica type
          //can not get replica type from arg, arg and result is not match
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(!addr.is_valid())) {
            ret = OB_NEED_RETRY;
            LOG_WARN("addr is invalid, ls create failed", KR(ret), K(addr));
          } else if (result->get_replica_type() == REPLICA_TYPE_FULL) {
            if (OB_FAIL(member_list.add_member(ObMember(addr, timestamp)))) {
              LOG_WARN("failed to add member", KR(ret), K(addr));
            }
          } else if (result->get_replica_type() == REPLICA_TYPE_READONLY) {
            if (OB_FAIL(learner_list.add_learner(ObMember(addr, timestamp)))) {
              LOG_WARN("failed to add member", KR(ret), K(addr));
            }
          } else if (result->get_replica_type() == REPLICA_TYPE_COLUMNSTORE) {
            ObMember member(addr, timestamp);
            member.set_columnstore();
            if (OB_FAIL(learner_list.add_learner(member))) {
              LOG_WARN("failed to add member", KR(ret), K(addr), K(member));
            }
          }
          LOG_TRACE("create ls result", KR(ret), K(i), K(addr), KPC(result));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!id_.is_sys_ls() && with_arbitration_service) {
      if (rootserver::majority(paxos_replica_num/*F-replica*/ + 1/*A-replica*/) > member_list.get_member_number() + arb_replica_num) {
        ret = OB_REPLICA_NUM_NOT_ENOUGH;
        LOG_WARN("success count less than majority with arb-replica", KR(ret), K(paxos_replica_num),
                 K(member_list), K(with_arbitration_service), K(arb_replica_num));
      }
    } else {
      // for sys log stream and non-arb user log stream
      if (rootserver::majority(paxos_replica_num) > member_list.get_member_number()) {
        ret = OB_REPLICA_NUM_NOT_ENOUGH;
        LOG_WARN("success count less than majority", KR(ret), K(paxos_replica_num), K(member_list));
      }
    }
    LS_EVENT_ADD(tenant_id_, id_, "create_ls", ret, paxos_replica_num, member_list);
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_CHECK_MEMBER_LIST_SAME_ERROR);
int ObLSCreator::inner_check_member_list_and_learner_list_(
    const common::ObMemberList &member_list,
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  ObLSInfo ls_info_to_check;
  if (OB_UNLIKELY(ERRSIM_CHECK_MEMBER_LIST_SAME_ERROR)) {
    ret = ERRSIM_CHECK_MEMBER_LIST_SAME_ERROR;
  } else if (OB_ISNULL(GCTX.lst_operator_)
             || OB_UNLIKELY(!is_valid() || !member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list));
  } else if (OB_FAIL(GCTX.lst_operator_->get(
                         GCONF.cluster_id, tenant_id_, id_,
                         share::ObLSTable::DEFAULT_MODE, ls_info_to_check))) {
    LOG_WARN("fail to get ls info", KR(ret), K_(tenant_id), K_(id));
  } else {
    // check member_list all reported in __all_ls_meta_table
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      const share::ObLSReplica *replica = nullptr;
      common::ObAddr server;
      if (OB_FAIL(member_list.get_server_by_index(i, server))) {
        LOG_WARN("fail to get server by index", KR(ret), K(i), K(member_list));
      } else {
        int tmp_ret = ls_info_to_check.find(server, replica);
        if (OB_SUCCESS == tmp_ret) {
          // good, replica exists, bypass
        } else {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("has replica only in member list, need try again", KR(ret), KR(tmp_ret),
                   K(member_list), K(ls_info_to_check), K(i), K(server));
        }
      }
    }
    // check learner_list all reported in __all_ls_meta_table
    for (int64_t i = 0; OB_SUCC(ret) && i < learner_list.get_member_number(); ++i) {
      const share::ObLSReplica *replica = nullptr;
      common::ObAddr server;
      if (OB_FAIL(learner_list.get_server_by_index(i, server))) {
        LOG_WARN("fail to get server by index", KR(ret), K(i), K(learner_list));
      } else {
        int tmp_ret = ls_info_to_check.find(server, replica);
        if (OB_SUCCESS == tmp_ret) {
          // replica exists, bypass
        } else {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("has replica only in learner list, need try again", KR(ret), KR(tmp_ret),
                   K(learner_list), K(ls_info_to_check), K(i), K(server));
        }
      }
    }
  }
  return ret;
}

int ObLSCreator::check_member_list_and_learner_list_all_in_meta_table_(
    const common::ObMemberList &member_list,
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval_us = 1000l * 1000l; // 1s
  ObTimeoutCtx ctx;
  int tmp_ret = OB_SUCCESS;

  if (is_sys_tenant(tenant_id_)) {
    // no need to check sys tenant
  } else if (is_user_tenant(tenant_id_) && id_.is_sys_ls()) {
    // user tenant sys ls is created without meta table
  } else if (OB_UNLIKELY(!is_valid() || !member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait member list and learner list all reported to meta table timeout",
                 KR(ret), K(member_list), K(learner_list), K_(tenant_id), K_(id));
      } else if (OB_SUCCESS != (tmp_ret = inner_check_member_list_and_learner_list_(
                                     member_list, learner_list))) {
        LOG_WARN("fail to check member list and learner list all reported", KR(tmp_ret),
                 K_(tenant_id), K_(id), K(member_list), K(learner_list));
        // has replica only in member_list or learner_list, need try again later
        ob_usleep(retry_interval_us);
      } else {
        // good, all replicas in member_list and learner_list has already reported
        break;
      }
    }
  }
  return ret;
}

int ObLSCreator::construct_paxos_replica_number_to_persist_(
    const int64_t paxos_replica_num,
    const int64_t arb_replica_num,
    const common::ObMemberList &member_list,
    int64_t &paxos_replica_number_to_persist)
{
  int ret = OB_SUCCESS;
  paxos_replica_number_to_persist = 0;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(!member_list.is_valid())
             || OB_UNLIKELY(0 >= paxos_replica_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list), K(paxos_replica_num));
  } else if (!id_.is_user_ls()) {
    // for sys log stream
    if (member_list.get_member_number() >= rootserver::majority(paxos_replica_num)) {
      // good, majority of F-replica created successfully, set paxos_replica_num equal to locality
      paxos_replica_number_to_persist = paxos_replica_num;
    } else {
      // sys log stream needs majority of F-replicas created success
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("majority is not satisfied", KR(ret), K_(tenant_id), K_(id),
               K(member_list), K(paxos_replica_num), K(arb_replica_num));
    }
  } else {
    // for user log stream
    if (member_list.get_member_number() >= rootserver::majority(paxos_replica_num)) {
      // good, majority of F-replica created successfully, set paxos_replica_num equal to locality
      paxos_replica_number_to_persist = paxos_replica_num;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("majority is not satisfied", KR(ret), K_(tenant_id), K_(id),
               K(member_list), K(paxos_replica_num), K(arb_replica_num));
    }
  }
  return ret;
}

int ObLSCreator::set_member_list_(const common::ObMemberList &member_list,
                                  const common::ObMember &arb_replica,
                                  const int64_t paxos_replica_num,
                                  const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObAddr> server_list;
  int tmp_ret = OB_SUCCESS;
  int64_t paxos_replica_number_to_persist = 0;
  int64_t arb_replica_count = arb_replica.is_valid() ? 1 : 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(!member_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(member_list));
  } else if (OB_FAIL(check_member_list_and_learner_list_all_in_meta_table_(member_list, learner_list))) {
    LOG_WARN("fail to check member_list all in meta table", KR(ret), K(member_list), K(learner_list), K_(tenant_id), K_(id));
  } else if (OB_FAIL(construct_paxos_replica_number_to_persist_(
                         paxos_replica_num,
                         arb_replica_count,
                         member_list,
                         paxos_replica_number_to_persist))) {
    LOG_WARN("fail to construct paxos replica number to set", KR(ret), K_(tenant_id), K_(id),
             K(paxos_replica_num), K(arb_replica_count), K(member_list));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else {
      ObArray<int> return_code_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
        ObAddr addr;
        ObSetMemberListArgV2 arg;
        if (OB_FAIL(arg.init(tenant_id_, id_, paxos_replica_number_to_persist, member_list, arb_replica, learner_list))) {
          LOG_WARN("failed to init set member list arg", KR(ret), K_(id), K_(tenant_id),
              K(paxos_replica_number_to_persist), K(member_list), K(arb_replica), K(learner_list));
        } else if (OB_FAIL(member_list.get_server_by_index(i, addr))) {
          LOG_WARN("failed to get member by index", KR(ret), K(i), K(member_list));
        } else if (OB_TMP_FAIL(set_member_list_proxy_.call(addr, ctx.get_timeout(),
                GCONF.cluster_id, tenant_id_, arg))) {
          LOG_WARN("failed to set member list", KR(tmp_ret), K(ctx.get_timeout()), K(arg),
              K(tenant_id_));
        } else if (OB_FAIL(server_list.push_back(addr))) {
          LOG_WARN("failed to push back server list", KR(ret), K(addr));
        }
      }

      if (OB_TMP_FAIL(set_member_list_proxy_.wait_all(return_code_array))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to wait all async rpc", KR(ret), KR(tmp_ret));

      }
      if (FAILEDx(check_set_memberlist_result_(return_code_array, paxos_replica_number_to_persist))) {
        LOG_WARN("failed to check set member liset result", KR(ret),
            K(paxos_replica_num), K(return_code_array));
      }
    }
  }
  return ret;
}

int ObLSCreator::check_set_memberlist_result_(
    const ObIArray<int> &return_code_array,
    const int64_t paxos_replica_num)
{
  int ret = OB_SUCCESS;
  int64_t success_cnt = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (return_code_array.count() != set_member_list_proxy_.get_results().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc count not equal to result count", KR(ret), 
        K(return_code_array.count()), K(set_member_list_proxy_.get_results().count()));
  } else {
    // don't use arg/dest here because call() may has failure.
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
      if (OB_SUCCESS != return_code_array.at(i)) {
        LOG_WARN("rpc is failed", KR(ret), K(return_code_array.at(i)), K(i));
      } else {
        const auto *result = set_member_list_proxy_.get_results().at(i);
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i));
        } else if (OB_SUCCESS != result->get_result()) {
          LOG_WARN("rpc is failed", KR(ret), K(*result), K(i));
        } else {
          success_cnt++;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (rootserver::majority(paxos_replica_num) > success_cnt) {
      ret = OB_REPLICA_NUM_NOT_ENOUGH;
      LOG_WARN("success count less than majority", KR(ret), K(success_cnt),
               K(paxos_replica_num));
    }
  }
  LS_EVENT_ADD(tenant_id_, id_, "set_ls_member_list", ret, paxos_replica_num, success_cnt);
  return ret;
}



}
}
