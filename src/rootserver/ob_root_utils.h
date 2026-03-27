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

#ifndef _OB_ROOT_UTILS_H
#define _OB_ROOT_UTILS_H 1
#include "share/unit/ob_unit_info.h"
#include "share/ob_check_stop_provider.h"
#include "share/ob_replica_info.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_define.h"
#include "share/ob_common_rpc_proxy.h"
#include "rootserver/ob_replica_addr.h"
#include "rootserver/ob_locality_util.h"
#include "rootserver/ob_zone_manager.h"
#include "share/ob_cluster_role.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace share
{
class ObILSPropertyGetter;
class ObZoneReplicaAttrSet;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObSchemaGetterGuard;
}
}


namespace rootserver
{
class ObDDLService;
class ObUnitManager;
class ObZoneManager;
class ObLocalityDistribution;
template <typename T>
inline T majority(const T n)
{
  return n / 2 + 1;
}

template<typename T>
inline bool is_same_tg(const T &left, const T &right)
{
  bool same = false;
  if (left.tablegroup_id_ == right.tablegroup_id_) {
    if (common::OB_INVALID_ID != left.tablegroup_id_) {
      same = true;
    } else {
      same = left.table_id_ == right.table_id_;
    }
  }
  return same;
}

template<typename T>
inline bool is_same_pg(const T &left, const T &right)
{
  return is_same_tg(left, right) && left.partition_idx_ == right.partition_idx_;
}


enum ObResourceType
{
  RES_CPU = 0,
  RES_MEM = 1,
  RES_LOG_DISK = 2,
  RES_DATA_DISK = 3,
  RES_MAX
};

const char *resource_type_to_str(const ObResourceType &t);

class ObIServerResource
{
public:
  ObIServerResource() = default;
  virtual ~ObIServerResource() = default;
  // return -1 if resource_type is invalid
  virtual const common::ObAddr &get_server() const = 0;
  virtual double get_assigned(ObResourceType resource_type) const = 0;
  virtual double get_capacity(ObResourceType resource_type) const = 0;
  virtual double get_max_assigned(ObResourceType resource_type) const = 0;
};

class ObIServerResourceDemand
{
public:
  ObIServerResourceDemand() = default;
  virtual ~ObIServerResourceDemand() = default;
  // return -1 if resource_type is invalid
  virtual double get_demand(ObResourceType resource_type) const = 0;
};

class ObResourceUtils
{
public:
  // the weight of the i-th resource is equal to the average usage of i-th resource
  template <class T>
      static int calc_server_resource_weight(const common::ObArray<T> &servers,
                                             double *weights, int64_t count)
  {
    int ret = common::OB_SUCCESS;
    if (count != RES_MAX || servers.count() <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      memset(weights, 0, count*sizeof(double));
      ARRAY_FOREACH(servers, i) {
        const T &server_resource = servers.at(i);
        for (int j = RES_CPU; j < RES_MAX; ++j) {
          ObResourceType res_type = static_cast<ObResourceType>(j);
          const double assigned = server_resource.get_assigned(res_type);
          const double capacity = server_resource.get_capacity(res_type);
          if (capacity <= 0 || assigned <= 0) {
            weights[res_type] += 0.0;
          } else if (assigned > capacity) {
            weights[res_type] += 1.0;
          } else {
            const double factor = assigned / capacity;
            weights[res_type] += factor;
            ObCStringHelper helper;
            _RS_LOG(INFO, "server resource weight factor: "
                "[%ld/%ld] server=%s, resource=%s, assigned=%.6g, capacity=%.6g, factor=%.6g, weight=%.6g",
                i, servers.count(),
                helper.convert(server_resource.get_server()),
                resource_type_to_str(res_type),
                assigned,
                capacity,
                factor,
                weights[res_type]);
          }
        }
      }
      const int64_t N = servers.count();
      double sum = 0;
      for (int j = RES_CPU; j < RES_MAX; ++j) {
        weights[j] /= static_cast<double>(N);
        sum += weights[j];
        // sanity check
        if (weights[j] < 0 || weights[j] > 1) {
          ret = common::OB_ERR_UNEXPECTED;
          RS_LOG(ERROR, "weight should be in [0,1]", K(j), "w", weights[j]);
        }
      }

      if (OB_SUCC(ret) && sum > 0) {
        // normalization
        for (int j = RES_CPU; j < RES_MAX; ++j) {
          weights[j] /= sum;
          _RS_LOG(INFO, "resource weight: %s=%.6g",
              resource_type_to_str((ObResourceType)j), weights[j]);
        }
      }
    }
    return ret;
  }

  template <typename T>
      static int calc_load(T &resource, double *weights, int64_t weights_count, double &load)
  {
    int ret = common::OB_SUCCESS;
    if (weights_count != RES_MAX) {
      ret = common::OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid weight vector", K(ret), K(weights_count));
    } else {
      load = 0.0;
      for (int j = RES_CPU; j < RES_MAX; ++j) {
        ObResourceType res_type = static_cast<ObResourceType>(j);
        if (resource.get_capacity(res_type) <= 0
            || resource.get_assigned(res_type) <= 0) {
          // continue
          // load += weights_[j] * 0.0;
        } else if (resource.get_assigned(res_type) > resource.get_capacity(res_type)) {
          load += weights[j] * 1.0;
        } else {
          load += weights[j] * (resource.get_assigned(res_type) / resource.get_capacity(res_type));
        }
      }
    }
    return ret;
  }

  template <typename T1, typename T2>
      static int calc_load(T1 &demand, T2 &usage, double *weights, int64_t weights_count, double &load)
  {
    int ret = common::OB_SUCCESS;
    if (weights_count != RES_MAX) {
      ret = common::OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid weight vector", K(ret), K(weights_count));
    } else {
      load = 0.0;
      for (int j = RES_CPU; j < RES_MAX; ++j) {  // foreach resource type
        ObResourceType res_type = static_cast<ObResourceType>(j);
        if (usage.get_capacity(res_type) <= 0
            || demand.get_demand(res_type) <= 0) {
          // continue
          // load += weights_[j] * 0.0;
        } else if (demand.get_demand(res_type) > usage.get_capacity(res_type)) {
          load += weights[j] * 1.0;
        } else {
          load += weights[j] * (demand.get_demand(res_type) / usage.get_capacity(res_type));
        }
      }
    }
    return ret;
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObResourceUtils);
  ObResourceUtils();
  ~ObResourceUtils();
};


class ObTenantUtils
{
public:
  static int get_tenant_ids(
      share::schema::ObMultiVersionSchemaService *schema_service,
      common::ObIArray<uint64_t> &tenant_ids);
private:

};

class ObRootServiceRoleChecker
{
public:
  static bool is_rootserver();
};

class ObTenantGroupParser
{
public:
  struct TenantNameGroup
  {
    TenantNameGroup() : row_(0), column_(0), tenants_() {}
    int64_t row_;
    int64_t column_;
    common::ObSEArray<common::ObString, 128> tenants_;
    TO_STRING_KV(K_(row), K(column_), K(tenants_));
  };
public:
  ObTenantGroupParser() : all_tenant_names_() {}
  virtual ~ObTenantGroupParser() {}
public:
private:
  int get_next_tenant_name(int64_t &pos,
                           const int64_t end,
                           const common::ObString &ttg_str,
                           common::ObString &tenant_name);
  int jump_to_next_tenant_name(int64_t &pos,
                               const int64_t end,
                               const common::ObString &ttg_str);
  int get_next_tenant_group(int64_t &pos,
                            const int64_t end,
                            const common::ObString &ttg_str,
                            common::ObIArray<TenantNameGroup> &tenant_groups);
  int jump_to_next_ttg(int64_t &pos,
                       const int64_t end,
                       const common::ObString &ttg_str);
  int parse_tenant_vector(int64_t &pos,
                          const int64_t end,
                          const common::ObString &ttg_str,
                          common::ObIArray<common::ObString> &tenant_names);
  int jump_to_next_tenant_vector(int64_t &pos,
                                 const int64_t end,
                                 const common::ObString &ttg_str);
  int parse_vector_tenant_group(int64_t &pos,
                                const int64_t end,
                                const common::ObString &ttg_str,
                                common::ObIArray<TenantNameGroup> &tenant_groups);
  int parse_matrix_tenant_group(int64_t &pos,
                                const int64_t end,
                                const common::ObString &ttg_str,
                                common::ObIArray<TenantNameGroup> &tenant_groups);
  void jump_over_space(
       int64_t &pos,
       const int64_t end,
       const common::ObString &ttg_str);
private:
  common::ObArray<common::ObString> all_tenant_names_;
};

class ObLocalityUtil
{
public:
  ObLocalityUtil() {}
  virtual ~ObLocalityUtil() {}
public:

  static int64_t gen_locality_seed(const int64_t seed) {
    int32_t tmp_seed = 0;
    int64_t my_seed = seed;
    tmp_seed = murmurhash2(&my_seed, sizeof(my_seed), tmp_seed);
    tmp_seed = fnv_hash2(&my_seed, sizeof(my_seed), tmp_seed);
    my_seed = (tmp_seed < 0 ? -tmp_seed : tmp_seed);
    return my_seed;
  }
};

enum PaxosReplicaNumberTaskType
{
  NOP_PAXOS_REPLICA_NUMBER = 0,
  ADD_PAXOS_REPLICA_NUMBER,
  SUB_PAXOS_REPLICA_NUMBER,
  INVALID_PAXOS_REPLICA_NUMBER_TASK,
};

struct AlterPaxosLocalityTask
{
  PaxosReplicaNumberTaskType task_type_;
  common::ObSEArray<common::ObZone,
                    common::OB_MAX_MEMBER_NUMBER,
                    common::ObNullAllocator> zone_set_;
  common::ObSEArray<common::ObReplicaType,
                    common::OB_MAX_MEMBER_NUMBER,
                    common::ObNullAllocator> associated_replica_type_set_;
  AlterPaxosLocalityTask() : task_type_(INVALID_PAXOS_REPLICA_NUMBER_TASK),
                             zone_set_(),
                             associated_replica_type_set_() {}
  void reset() {
    task_type_ = INVALID_PAXOS_REPLICA_NUMBER_TASK;
    zone_set_.reset();
    associated_replica_type_set_.reset();
  }
  TO_STRING_KV(K_(task_type), K_(zone_set), K_(associated_replica_type_set));
};

ObTraceEventRecorder *get_rs_trace_recorder();
inline ObTraceEventRecorder *get_rs_trace_recorder()
{
  auto *ptr = GET_TSI_MULT(ObTraceEventRecorder, 2);
  return ptr;
}

class ObRootUtils
{
public:
  ObRootUtils() {}
  virtual ~ObRootUtils() {}

  static int get_rs_default_timeout_ctx(ObTimeoutCtx &ctx);
  static int get_invalid_server_list(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    common::ObIArray<common::ObAddr> &invalid_server_list);
  static int find_server_info(
      const ObIArray<share::ObServerInfoInTable> &servers_info,
      const common::ObAddr &server,
      share::ObServerInfoInTable &server_info);
  static int get_servers_of_zone(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const common::ObZone &zone,
    ObIArray<common::ObAddr> &servers,
    bool only_active_servers = false);
  static int get_server_count(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObZone &zone,
    int64_t &alive_count,
    int64_t &not_alive_count);
  static int check_server_alive(
      const ObIArray<share::ObServerInfoInTable> &servers_info,
      const common::ObAddr &server,
      bool &is_alive);
  static int get_server_resource_info(
      const ObIArray<obrpc::ObGetServerResourceInfoResult> &server_resources_info,
      const ObAddr &server,
      share::ObServerResourceInfo &resource_info);
  static int get_stopped_zone_list(common::ObIArray<common::ObZone> &stopped_zone_list,
                                   common::ObIArray<common::ObAddr> &stopped_server_list);
  static bool have_other_stop_task(const ObZone &zone);

  // wait the given ls's end_scn be larger than or equal to sys_ls_target_scn
  // @params[in]: sys_ls_target_scn
  // @params[in]: log_ls_svr
  // @params[in]: ls
  // @ret OB_SUCCESS user_ls_sync_scn >= sys_ls_sync_scn
  // @ret OB_NOT_MASTER the current replica is not leader, no need to wait.
  //                    the rpc sender need to find the new leader and send rpc again
  // @ret other error code			failure
  static int wait_user_ls_sync_scn_locally(
      const share::SCN &sys_ls_target_scn,
      logservice::ObLogService *log_ls_svr,
      storage::ObLS &ls);

  template<class T>
      static bool is_subset(const common::ObIArray<T> &superset_array,
                            const common::ObIArray<T> &array);
  static int get_proposal_id_from_sys_ls(int64_t &proposal_id, ObRole &role);

  static int notify_switch_leader(
      obrpc::ObSrvRpcProxy *rpc_proxy,
      const uint64_t tenant_id,
      const obrpc::ObNotifySwitchLeaderArg &arg,
      const ObIArray<common::ObAddr> &addr_list);
  static int try_notify_switch_ls_leader(
      obrpc::ObSrvRpcProxy *rpc_proxy,
      const share::ObLSInfo &ls_info,
      const obrpc::ObNotifySwitchLeaderArg::SwitchLeaderComment &comment);
  static int check_tenant_ls_balance(uint64_t tenant_id, int &check_ret);

  template<typename T>
  static int copy_array(const common::ObIArray<T> &src_array,
                        const int64_t start_pos,
                        const int64_t end_pos,
                        common::ObIArray<T> &dst_array);
};

template<class T>
bool ObRootUtils::is_subset(const common::ObIArray<T> &superset_array,
                            const common::ObIArray<T> &array)
{
  bool bret = true;
  for (int64_t i = 0; i < array.count() && bret; i++) {
    if (has_exist_in_array(superset_array, array.at(i))) {
      //nothing todo
    } else {
      bret = false;
    }
  }
  return bret;
}

template<typename T>
int ObRootUtils::copy_array(
    const common::ObIArray<T> &src_array,
    const int64_t start_pos,
    const int64_t end_pos,
    common::ObIArray<T> &dst_array)
{
  int ret = common::OB_SUCCESS;
  dst_array.reset();
  if (OB_UNLIKELY(start_pos < 0 || start_pos > end_pos || end_pos > src_array.count())) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid start_pos/end_pos", KR(ret),
               K(start_pos), K(end_pos), "src_array_cnt", src_array.count());
  } else if (start_pos == end_pos) {
    // do nothing
  } else if (OB_FAIL(dst_array.reserve(end_pos - start_pos))) {
    COMMON_LOG(WARN, "fail to reserve array", KR(ret), "cnt", end_pos - start_pos);
  } else {
    for (int64_t i = start_pos; OB_SUCC(ret) && i < end_pos; i++) {
      if (OB_FAIL(dst_array.push_back(src_array.at(i)))) {
        COMMON_LOG(WARN, "fail to push back", KR(ret), K(i));
      }
    } // end for
  }
  return ret;
}

class ObClusterInfoGetter
{
public:
  ObClusterInfoGetter() {}
  virtual ~ObClusterInfoGetter() {}
  static common::ObClusterRole get_cluster_role_v2();
  static common::ObClusterRole get_cluster_role();
};
} // end namespace rootserver
} // end namespace oceanbase

#ifndef FOR_BEGIN_END_E
#define FOR_BEGIN_END_E(it, obj, array, extra_condition) \
    for (__typeof__((array).begin()) it = (array).begin() + (obj).begin_; \
        (extra_condition) && (it != (array).end()) && (it != (array).begin() + (obj).end_); ++it)
#endif

#ifndef FOR_BEGIN_END
#define FOR_BEGIN_END(it, obj, array) \
    FOR_BEGIN_END_E(it, (obj), (array), true)
#endif

// record trace events into THE one recorder
#define THE_RS_TRACE ::oceanbase::rootserver::get_rs_trace_recorder()

#define RS_TRACE(...)                           \
  do {                                          \
    if (OB_LIKELY(THE_RS_TRACE != nullptr)) {     \
      REC_TRACE(*THE_RS_TRACE, __VA_ARGS__);        \
    }                                           \
  } while (0)                                   \

#define RS_TRACE_EXT(...)                       \
  do {                                          \
    if (OB_LIKELY(THE_RS_TRACE != nullptr)) {     \
      REC_TRACE_EXT(*THE_RS_TRACE, __VA_ARGS__);    \
    }                                           \
  } while (0)


#endif /* _OB_ROOT_UTILS_H */
