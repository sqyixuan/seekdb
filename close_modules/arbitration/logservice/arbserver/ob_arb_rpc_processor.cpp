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

#include "ob_arb_rpc_processor.h"
#include "logservice/arbserver/palf_env_lite_mgr.h"

namespace oceanbase
{
namespace arbserver
{

int ObRpcSetMemberListP::process()
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = get_src_cluster_id();
  uint64_t tenant_id = arg_.get_tenant_id();
  share::ObLSID ls_id = arg_.get_ls_id();
  const common::ObMember arb_member = arg_.get_arbitration_service();
  const ObMemberList member_list = arg_.get_member_list();
  const int64_t paxos_replica_num = arg_.get_paxos_replica_num();
  common::ObAddr src_server = get_peer();
  palflite::PalfEnvKey palf_env_key(cluster_id, tenant_id);
  if (OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(palf_env_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "palf_env_mgr_ is nullptr, unexpected error", K(ret), KP(palf_env_mgr_));
  } else if (OB_FAIL(palf_env_mgr_->set_initial_member_list(palf_env_key,
                                                            src_server,
                                                            ls_id.id(),
                                                            member_list,
                                                            arb_member,
                                                            paxos_replica_num,
                                                            arg_.get_learner_list()))) {
    CLOG_LOG(WARN, "set_initial_member_list failed", K(ret), K(cluster_id), K(src_server),
        K(tenant_id), K(ls_id), K(member_list), K(arb_member), K(paxos_replica_num),
        "learner_list", arg_.get_learner_list());
  } else {
    CLOG_LOG(INFO, "set_initial_member_list success", K(ret), K(cluster_id), K(src_server),
        K(tenant_id), K(ls_id), K(member_list), K(arb_member), K(paxos_replica_num),
        "learner_list", arg_.get_learner_list());
  }
  result_.init(ret);
  return ret;
}

int ObRpcCreateArbP::process()
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = get_src_cluster_id();
  uint64_t tenant_id = arg_.get_tenant_id();
  share::ObTenantRole tenant_role = arg_.get_tenant_role();
  share::ObLSID ls_id = arg_.get_ls_id();
  common::ObAddr src_server = get_peer();
  palflite::PalfEnvKey palf_env_key(cluster_id, tenant_id);
  if (OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(palf_env_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "palf_env_mgr_ is nullptr, unexpected error", K(ret), KP(palf_env_mgr_));
  } else if (OB_FAIL(palf_env_mgr_->create_arbitration_instance(palf_env_key, src_server,
          ls_id.id(), tenant_role))) {
    CLOG_LOG(WARN, "create_arbitration_instance failed", K(ret), K(palf_env_key), K(src_server),
        K(ls_id), K(tenant_role));
  } else {
  }
  result_.set_result(ret);
  return ret;
}

int ObRpcDeleteArbP::process()
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = get_src_cluster_id();
  uint64_t tenant_id = arg_.get_tenant_id();
  share::ObLSID ls_id = arg_.get_ls_id();
  common::ObAddr src_server = get_peer();
  palflite::PalfEnvKey palf_env_key(cluster_id, tenant_id);
  if (OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(palf_env_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "palf_env_mgr_ is nullptr, unexpected error", K(ret), KP(palf_env_mgr_));
  } else if (OB_FAIL(palf_env_mgr_->delete_arbitration_instance(palf_env_key, src_server,
          ls_id.id()))) {
    CLOG_LOG(WARN, "delete_arbitration_instance failed", K(ret), K(palf_env_key), K(src_server),
        K(ls_id));
  } else {
  }
  result_.set_result(ret);
  return ret;
}

int ObRpcGCNotifyP::process()
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = get_src_cluster_id();
  common::ObAddr src_server = get_peer();
  const GCMsgEpoch &epoch = arg_.get_epoch();
  arbserver::TenantLSIDSArray &ls_ids = arg_.get_ls_ids();
  if (OB_ISNULL(palf_env_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "palf_env_mgr_ is nullptr, unexpected error", K(ret), KP(palf_env_mgr_));
  } else if (OB_FAIL(palf_env_mgr_->handle_gc_message(epoch,
                                               src_server,
                                               cluster_id,
                                               ls_ids))) {
    CLOG_LOG(WARN, "handle_gc_message failed", K(ret), K(epoch), K(src_server), 
        K(cluster_id), K(ls_ids));
  }
  return ret;
}

int ObRpcForceCleanArbClusterInfoP::process()
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = arg_.get_cluster_id();
  common::ObAddr src_server = get_peer();
  if (OB_ISNULL(palf_env_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "palf_env_mgr_ is nullptr, unexpected error", K(ret), KP(palf_env_mgr_));
  } else if (OB_FAIL(palf_env_mgr_->handle_force_clear_arb_cluster_info_message(src_server, cluster_id))) {
    CLOG_LOG(WARN, "handle_force_clear_arb_cluster_info_message failed", K(ret), K(src_server), K(cluster_id));
  } else {

  }
  return ret;
}

int ObRpcArbSetConfigP::process()
{
  int ret = OB_SUCCESS;
  if (NULL != GCTX.config_mgr_) {
    (void) GCONF.add_extra_config(arg_.ptr());
    (void) GCTX.config_mgr_->reload_config();
    (void) GCONF.print();
    // save new config content to file
    (void) GCTX.config_mgr_->dump2file();
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  CLOG_LOG(INFO, "process set config request finished", K(ret), K_(arg));
  return ret;
}

int ObRpcArbClusterOpP::process()
{
  int ret = OB_SUCCESS;
  common::ObAddr src_server = get_peer();
  const bool is_cluster_add_arb_req = arg_.is_cluster_add_arb_req();
  const int64_t cluster_id = arg_.get_cluster_id();
  const common::ObString &cluster_name = arg_.get_cluster_name();
  const arbserver::GCMsgEpoch &epoch = arg_.get_epoch();

  if (false == arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(arg_));
  } else if (is_cluster_add_arb_req) {
    if (OB_FAIL(palf_env_mgr_->add_cluster(src_server, cluster_id, cluster_name, epoch))) {
      CLOG_LOG(WARN, "add_cluster failed", K(ret), K(src_server), K(cluster_id), K(cluster_name), K(epoch));
    } else {
      CLOG_LOG(INFO, "add_cluster success", K(ret), K(src_server), K(cluster_id), K(cluster_name), K(epoch));
    }
  } else if (!is_cluster_add_arb_req) {
    if (OB_FAIL(palf_env_mgr_->remove_cluster(src_server, cluster_id, cluster_name, epoch))) {
      CLOG_LOG(WARN, "remove_cluster failed", K(ret), K(src_server), K(cluster_id), K(cluster_name), K(epoch));
    } else {
      CLOG_LOG(INFO, "remove_cluster success", K(ret), K(src_server), K(cluster_id), K(cluster_name), K(epoch));
    }
  }
  result_.set_result(ret);
  return ret;
}
} // end namespace arbserver
} // end namespace oceanbase
