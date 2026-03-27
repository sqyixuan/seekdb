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

#include "ob_ls_status_operator.h"

#include "rootserver/ob_root_utils.h" // majority
#include "share/ls/ob_ls_status_operator.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"//OBCG_DEFAULT

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::palf;
namespace oceanbase
{
namespace share
{
OB_SERIALIZE_MEMBER(ObMemberListFlag, flag_);

int64_t ObMemberListFlag::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(flag));
  J_OBJ_END();
  return pos;
}

//////////ObLSStatusInfo
bool ObLSStatusInfo::is_valid() const
{
  return ls_id_.is_valid()
         && OB_INVALID_TENANT_ID != tenant_id_
         && (ls_id_.is_sys_ls()
             || (OB_INVALID_ID != ls_group_id_
                 && OB_INVALID_ID != unit_group_id_))
         && !ls_is_invalid_status(status_) 
         && flag_.is_valid();
}

void ObLSStatusInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  ls_group_id_ = OB_INVALID_ID;
  unit_group_id_ = OB_INVALID_ID;
  status_ = OB_LS_EMPTY;
  flag_.reset();
}

int ObLSStatusInfo::init(const uint64_t tenant_id,
                         const ObLSID &id,
                         const uint64_t ls_group_id,
                         const ObLSStatus status,
                         const uint64_t unit_group_id,
                         const ObZone &primary_zone,
                         const ObLSFlag &flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!id.is_valid()
        || !flag.is_valid()
        || OB_INVALID_TENANT_ID == tenant_id
        || ls_is_invalid_status(status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(id), K(ls_group_id),
              K(status), K(unit_group_id), K(flag));
  } else if (OB_FAIL(primary_zone_.assign(primary_zone))) {
    LOG_WARN("failed to assign primary zone", KR(ret), K(primary_zone));
  } else if (OB_FAIL(flag_.assign(flag))) {
    LOG_WARN("failed to assign ls flag", KR(ret), K(flag));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = id;
    ls_group_id_ = ls_group_id;
    unit_group_id_ = unit_group_id;
    status_ = status;
  }
  return ret;
}

int ObLSStatusInfo::assign(const ObLSStatusInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(primary_zone_.assign(other.primary_zone_))) {
      LOG_WARN("failed to assign other primary zone", KR(ret), K(other));
    } else if (OB_FAIL(flag_.assign(other.flag_))) {
      LOG_WARN("failed to assign ls flag", KR(ret), K(other));
    } else {
      tenant_id_ = other.tenant_id_;
      ls_id_ = other.ls_id_;
      ls_group_id_ = other.ls_group_id_;
      unit_group_id_ = other.unit_group_id_;
      status_ = other.status_;
      unit_group_id_ = other.unit_group_id_;
    }
  }
  return ret;
}

////////ObLSStatusOperator
int ObLSStatusOperator::create_new_ls(const ObLSStatusInfo &ls_info,
                                      const SCN &current_tenant_scn,
                                      const common::ObString &zone_priority,
                                      const share::ObTenantSwitchoverStatus &working_sw_status,
                                      ObMySQLTransaction &trans)
{
  UNUSEDx(current_tenant_scn, zone_priority);
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  ObLSFlagStr flag_str;
  common::ObSqlString sql;
  const char *table_name = OB_ALL_LS_STATUS_TNAME;
  if (OB_UNLIKELY(!ls_info.is_valid()
                  || !working_sw_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(ls_info), K(working_sw_status));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                  ls_info.tenant_id_, &trans, true, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(ls_info));
  } else if (working_sw_status != tenant_info.get_switchover_status()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("tenant not in specified switchover status", K(ls_info), K(working_sw_status), K(tenant_info));
  } else if (OB_FAIL(ls_info.get_flag().flag_to_str(flag_str))) {
    LOG_WARN("fail to convert ls flag into string", KR(ret), K(ls_info));
  }

  if (OB_FAIL(ret)) {
  } else {
    ObDMLSqlSplicer dml_splicer;
    if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ls_info.tenant_id_))
      || OB_FAIL(dml_splicer.add_pk_column("ls_id", ls_info.ls_id_.id()))
      || OB_FAIL(dml_splicer.add_column("status", ls_status_to_str(ls_info.status_)))
      || OB_FAIL(dml_splicer.add_column("ls_group_id", ls_info.ls_group_id_))
      || OB_FAIL(dml_splicer.add_column("unit_group_id", ls_info.unit_group_id_))
      || OB_FAIL(dml_splicer.add_column("primary_zone", ls_info.primary_zone_.ptr()))) {
      LOG_WARN("add columns failed", KR(ret), K(ls_info));
    } else if (!ls_info.get_flag().is_normal_flag() && OB_FAIL(dml_splicer.add_column("flag", flag_str.ptr()))) {
      LOG_WARN("add flag column failed", KR(ret), K(ls_info), K(flag_str));
    } else if (OB_FAIL(dml_splicer.splice_insert_sql(table_name, sql))) {
      LOG_WARN("fail to splice insert sql", KR(ret), K(sql), K(ls_info), K(flag_str));
    } else if (OB_FAIL(exec_write(ls_info.tenant_id_, sql, this, trans))) {
      LOG_WARN("failed to exec write", KR(ret), K(ls_info), K(sql));
    } else if (ls_info.ls_id_.is_sys_ls()) {
      LOG_INFO("sys ls no need update max ls id", KR(ret), K(ls_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_max_ls_id(
                   ls_info.tenant_id_, ls_info.ls_id_, trans, false))) {
      LOG_WARN("failed to update tenant max ls id", KR(ret), K(ls_info));
    }
  }

  ALL_LS_EVENT_ADD(ls_info.tenant_id_, ls_info.ls_id_, "create_new_ls", ret, sql);
  return ret;
}

int ObLSStatusOperator::drop_ls(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  common::ObSqlString sql;
  if (OB_UNLIKELY(!ls_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id
                  || !working_sw_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(ls_id), K(tenant_id), K(working_sw_status));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                     tenant_id, &trans, true, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (working_sw_status != tenant_info.get_switchover_status()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("tenant not in specified switchover status", K(tenant_id), K(working_sw_status), K(tenant_info));
  } else {
    if (OB_FAIL(sql.assign_fmt("DELETE from %s where ls_id = %ld and tenant_id = %lu",
                               OB_ALL_LS_STATUS_TNAME, ls_id.id(), tenant_id))) {
      LOG_WARN("failed to assign sql", KR(ret), K(ls_id), K(sql));
    } else if (OB_FAIL(exec_write(tenant_id, sql, this, trans))) {
      LOG_WARN("failed to exec write", KR(ret), K(tenant_id), K(ls_id), K(sql));
    }
  }
  ALL_LS_EVENT_ADD(tenant_id, ls_id, "drop_ls", ret, sql);
  return ret;
}

int ObLSStatusOperator::set_ls_offline(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObLSStatus &ls_status,
                      const SCN &drop_scn,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans)
{
  UNUSEDx(drop_scn);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id
        || !working_sw_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(ls_id), K(tenant_id), K(working_sw_status));
  } else if (OB_FAIL(update_ls_status_in_trans(tenant_id, ls_id,
          ls_status, OB_LS_WAIT_OFFLINE, working_sw_status, trans))) {
    LOG_WARN("failed to update ls status", KR(ret), K(tenant_id), K(ls_id), K(ls_status), K(working_sw_status));
  }
  return ret;
}

int ObLSStatusOperator::update_ls_status_in_trans(
    const uint64_t tenant_id,
    const ObLSID &id, const ObLSStatus &old_status,
    const ObLSStatus &new_status,
    const ObTenantSwitchoverStatus &switch_status,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!id.is_valid()
                  || ls_is_invalid_status(new_status) 
                  || ls_is_invalid_status(old_status)
                  || OB_INVALID_TENANT_ID == tenant_id
                  || !switch_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KR(ret), K(id), K(new_status), K(old_status),
             K(tenant_id), K(switch_status));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                   tenant_id, &trans, true, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (switch_status != tenant_info.get_switchover_status()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("tenant not expect switchover status", KR(ret), K(tenant_info));
  } else {
    //init_member_list is no need after create success
    common::ObSqlString sql;
    const uint64_t exec_tenant_id =
      ObLSLifeIAgent::get_exec_tenant_id(tenant_id);
    common::ObSqlString sub_string;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = sub_string.assign(", init_learner_list = '', b_init_learner_list = ''"))) {
      LOG_WARN("fail to construct substring for learner list", KR(tmp_ret));
      sub_string.reset();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("UPDATE %s set status = '%s',init_member_list = '', b_init_member_list = ''%.*s"
                               " where ls_id = %ld and tenant_id = %lu and status = '%s'",
                               OB_ALL_LS_STATUS_TNAME,
                               ls_status_to_str(new_status),
                               static_cast<int>(sub_string.length()), sub_string.ptr(),
                               id.id(), tenant_id, ls_status_to_str(old_status)))) {
      LOG_WARN("failed to assign sql", KR(ret), K(id), K(new_status),
               K(old_status), K(tenant_id), K(sub_string), K(sql));
    } else if (OB_FAIL(exec_write(tenant_id, sql, this, trans))) {
      LOG_WARN("failed to exec write", KR(ret), K(tenant_id), K(id), K(sql));
    }
    ALL_LS_EVENT_ADD(tenant_id, id, "update_ls_status", ret, sql);
  }
  return ret;
}

int ObLSStatusOperator::get_all_ls_status_by_order(
    const uint64_t tenant_id,
    ObLSStatusInfoIArray &ls_array, ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ls_array.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), K(tenant_id));
  } else {
    ObASHSetInnerSqlWaitGuard ash_inner_sql_guard(ObInnerSqlWaitTypeId::LOG_GET_ALL_LS_STATUS_BY_ORDER);
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                   "SELECT * FROM %s WHERE tenant_id = %lu ORDER BY tenant_id, ls_id",
                   OB_ALL_LS_STATUS_TNAME, tenant_id))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql), K(tenant_id));
    } else if (OB_FAIL(exec_read(tenant_id, sql, client, this, ls_array))) {
      LOG_WARN("failed to exec read", KR(ret), K(tenant_id), K(sql));
    }
  }
  return ret;
}

int ObLSStatusOperator::get_all_ls_status_by_order_for_switch_tenant(
    const uint64_t tenant_id,
    const bool ignore_need_create_abort,
    ObLSStatusInfoIArray &ls_array,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ls_array.reset();
  ObLSStatusInfoArray ori_ls_array;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_id is not valid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_all_ls_status_by_order(tenant_id, ori_ls_array, client))) {
    LOG_WARN("failed to get_all_ls_status_by_order", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ori_ls_array.count(); ++i) {
      const ObLSStatusInfo &info = ori_ls_array.at(i);
      if (ls_is_pre_tenant_dropping_status(info.get_status()) || ls_is_tenant_dropping_status(info.get_status())) {
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_WARN("tenant has been dropped", KR(ret), K(info));
      } else if (ls_need_create_abort_status(info.get_status())) {
        if (ignore_need_create_abort) {
          LOG_INFO("ignore ls", KR(ret), K(info));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ls status", KR(ret), K(info));
        }
      } else if (ls_is_create_abort_status(info.get_status())) {
        LOG_INFO("ignore ls", KR(ret), K(info));
      } else if (OB_FAIL(ls_array.push_back(info))) {
        LOG_WARN("failed to push_back", KR(ret), K(info), K(ls_array));
      }
    }
  }
  return ret;
}

int ObLSStatusOperator::get_ls_init_member_list(
    const uint64_t tenant_id,
    const ObLSID &id, ObMemberList &member_list,
    share::ObLSStatusInfo &status_info, ObISQLClient &client,
    ObMember &arb_member,
    common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  learner_list.reset();
  status_info.reset();
  arb_member.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_ls_status_(tenant_id, id, true /*need_member_list*/,
                                    member_list, status_info, client, arb_member, learner_list, share::OBCG_DEFAULT))) {
    LOG_WARN("failed to get ls status", KR(ret), K(id), K(tenant_id));
  }
  return ret;
}

int ObLSStatusOperator::get_ls_status_info(
  const uint64_t tenant_id,
  const ObLSID &id, ObLSStatusInfo &status_info, ObISQLClient &client,
  const int32_t group_id)
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  common::GlobalLearnerList learner_list;
  ObMember arb_member;
  status_info.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_ls_status_(tenant_id, id, false /*need_member_list*/,
                                    member_list, status_info, client, arb_member, learner_list, group_id))) {
    LOG_WARN("failed to get ls status", KR(ret), K(id), K(tenant_id));
  }
  return ret;
}

template<typename T>
int ObLSStatusOperator::set_list_with_hex_str_(
    const common::ObString &str,
    T &list,
    ObMember &arb_member)
{
  int ret = OB_SUCCESS;
  list.reset();
  arb_member.reset();
  char *deserialize_buf = NULL;
  const int64_t str_size = str.length();
  const int64_t deserialize_size = str.length() / 2 + 1;
  int64_t deserialize_pos = 0;
  ObArenaAllocator allocator("MemberList");
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("str is empty", KR(ret));
  } else if (OB_ISNULL(deserialize_buf = static_cast<char*>(allocator.alloc(deserialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(deserialize_size));
  } else if (OB_FAIL(hex_to_cstr(str.ptr(), str_size, deserialize_buf, deserialize_size))) {
    LOG_WARN("fail to get cstr from hex", KR(ret), K(str_size), K(deserialize_size), K(str));
  } else if (OB_FAIL(list.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
    LOG_WARN("fail to deserialize set member list arg", KR(ret), K(deserialize_pos), K(deserialize_size),
             K(str));
  } else if (OB_UNLIKELY(deserialize_pos > deserialize_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deserialize error", KR(ret), K(deserialize_pos), K(deserialize_size));
  } else if (deserialize_pos < deserialize_size - 1) {
    //When deserialize_buf applies for memory, it applies for one more storage '\0',
    //so after member_list is deserialized, 
    //pos can only go to the position of deserialize_size - 1, and will not point to '\0'
    // have to parse flag
    ObMemberListFlag flag;
    if (OB_FAIL(flag.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
      LOG_WARN("fail to deserialize flag", KR(ret), K(deserialize_pos), K(deserialize_size));
    } else if (OB_UNLIKELY(deserialize_pos > deserialize_size)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("deserialize error", KR(ret), K(deserialize_pos), K(deserialize_size));
    } else if (flag.is_arb_member()) {
      if (OB_FAIL(arb_member.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
        LOG_WARN("fail to deserialize arb member", KR(ret), K(deserialize_pos), K(deserialize_size));
      } else if (OB_UNLIKELY(deserialize_pos > deserialize_size)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("deserialize error", KR(ret), K(deserialize_pos), K(deserialize_size));
      }
    }
  }
  return ret;
}

int ObLSStatusOperator::fill_cell(
    common::sqlclient::ObMySQLResult *result,
    share::ObLSStatusInfo &status_info)
{
  int ret = OB_SUCCESS;
  status_info.reset();
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    ObString status_str;
    ObString primary_zone_str;
    int64_t id_value = OB_INVALID_ID;
    uint64_t ls_group_id = OB_INVALID_ID;
    uint64_t unit_group_id = OB_INVALID_ID;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObString flag_str;
    ObString flag_str_default_value("");
    ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", id_value, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "ls_group_id", ls_group_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "unit_group_id", unit_group_id, uint64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", status_str);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "primary_zone", primary_zone_str);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "flag", flag_str,
                true /* skip_null_error */, true /* skip_column_error */, flag_str_default_value);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get result", KR(ret), K(id_value), K(ls_group_id),
               K(unit_group_id), K(status_str), K(primary_zone_str));
    } else {
      ObLSID ls_id(id_value);
      ObZone zone(primary_zone_str);
      if (OB_FAIL(flag.str_to_flag(flag_str))) {
        // if flag_str is empty then flag is setted to normal
        LOG_WARN("fail to convert string to flag", KR(ret), K(flag_str));
      } else if (OB_FAIL(status_info.init(tenant_id, ls_id, ls_group_id,
                               str_to_ls_status(status_str), unit_group_id,
                               zone, flag))) {
        LOG_WARN("failed to init ls operation", KR(ret), K(tenant_id), K(zone),
                 K(ls_group_id), K(ls_id), K(status_str), K(unit_group_id), K(flag));
      }
    }
  }
  return ret;
}

int ObLSStatusOperator::inner_get_ls_status_(
    const ObSqlString &sql,
    const uint64_t exec_tenant_id,
    const bool need_member_list,
    ObISQLClient &client,
    ObMemberList &member_list,
    share::ObLSStatusInfo &status_info,
    ObMember &arb_member,
    common::GlobalLearnerList &learner_list,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  status_info.reset();
  learner_list.reset();
  arb_member.reset();
  if (OB_UNLIKELY(sql.empty() || OB_INVALID_TENANT_ID == exec_tenant_id || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sql), K(exec_tenant_id), K(group_id));
  } else {
    ObTimeoutCtx ctx;
    const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
      LOG_WARN("failed to set default timeout ctx", KR(ret), K(default_timeout));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(client.read(res, exec_tenant_id, sql.ptr(), group_id))) {
          LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get sql result", KR(ret));
        } else {
          ObString init_member_list_str;
          ObString init_learner_list_str;
          ret = result->next();
          if (OB_ITER_END == ret) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("ls not exist in __all_ls_status table", KR(ret));
          } else if (OB_FAIL(ret)) {
            LOG_WARN("failed to get ls", KR(ret), K(sql));
          } else {
           if (OB_FAIL(fill_cell(result, status_info))) {
              LOG_WARN("failed to construct ls status info", KR(ret));
            } else if (need_member_list) {
             EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(
                  *result, "b_init_member_list", init_member_list_str);
              if (OB_FAIL(ret)) {
                LOG_WARN("failed to get result", KR(ret),
                         K(init_member_list_str));
              } else if (init_member_list_str.empty()) {
                // maybe
              } else if (OB_FAIL(set_list_with_hex_str_(
                             init_member_list_str, member_list, arb_member))) {
                LOG_WARN("failed to set member list", KR(ret),
                         K(init_member_list_str));
              } else {
                // deal with learner list
                EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(
                    *result, "b_init_learner_list", init_learner_list_str);
                if (OB_FAIL(ret)) {
                  LOG_WARN("failed to get result", KR(ret), K(init_learner_list_str));
                } else if (init_learner_list_str.empty()) {
                  // maybe
                } else if (OB_FAIL(set_list_with_hex_str_(init_learner_list_str,
                        learner_list, arb_member))) {
                  LOG_WARN("failed to set learner list", KR(ret), K(init_learner_list_str));
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_ITER_END != result->next()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expect only one row", KR(ret), K(sql));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLSStatusOperator::get_ls_status_(const uint64_t tenant_id,
                                       const ObLSID &id,
                                       const bool need_member_list,
                                       ObMemberList &member_list,
                                       share::ObLSStatusInfo &status_info,
                                       ObISQLClient &client,
                                       ObMember &arb_member,
                                       common::GlobalLearnerList &learner_list,
                                       const int32_t group_id)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  learner_list.reset();
  status_info.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(!id.is_valid()
                  || OB_INVALID_TENANT_ID == tenant_id
                  || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(id), K(tenant_id), K(group_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s where ls_id = %ld and tenant_id = %lu",
                                    OB_ALL_LS_STATUS_TNAME, id.id(), tenant_id))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else if (OB_FAIL(inner_get_ls_status_(sql, get_exec_tenant_id(tenant_id), need_member_list,
                                          client, member_list, status_info, arb_member, learner_list, group_id))) {
    LOG_WARN("fail to inner get ls status info", KR(ret), K(sql), K(tenant_id), "exec_tenant_id",
             get_exec_tenant_id(tenant_id), K(need_member_list));
  }
  return ret;
}

int ObLSStatusOperator::get_tenant_max_ls_id(const uint64_t tenant_id, ObLSID &max_id,
                           ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  max_id.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is not valid", KR(ret), K(tenant_id));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                   "SELECT max(ls_id) as max_ls_id FROM %s WHERE tenant_id = %lu",
                   OB_ALL_LS_STATUS_TNAME, tenant_id))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql), K(tenant_id));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        const uint64_t exec_tenant_id = get_exec_tenant_id(tenant_id);
        if (OB_FAIL(client.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get sql result", KR(ret), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to get max ls id", KR(ret), K(sql), K(exec_tenant_id));
        } else {
          int64_t ls_id = ObLSID::INVALID_LS_ID;
          EXTRACT_INT_FIELD_MYSQL(*result, "max_ls_id", ls_id, int64_t);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to get int", KR(ret), K(sql), K(exec_tenant_id));
          } else {
            max_id = ls_id;
          }
        }
      }
    }
  }
  return ret;
}

}//end of share
}//end of ob
