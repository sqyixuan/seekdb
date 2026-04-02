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
#ifndef OCEANBASE_ARB_GC_UTILS_H
#define OCEANBASE_ARB_GC_UTILS_H

#include "lib/utility/ob_unify_serialize.h"         // OB_UNIS_VERSION
#include "lib/container/ob_se_array.h"              // ObSEArray
#include "lib/function/ob_function.h"               // ObFunction
#include "share/ob_ls_id.h"                         // ObLSID

namespace oceanbase
{
namespace arbserver
{
// Epoch of each gc message
struct GCMsgEpoch {
public:
  OB_UNIS_VERSION(1);
public:
  GCMsgEpoch();
  ~GCMsgEpoch();
  GCMsgEpoch(int64_t proposal_id, int64_t seq);
  bool operator<(const GCMsgEpoch &rhs) const;
  bool operator==(const GCMsgEpoch &rhs) const;
  bool operator>(const GCMsgEpoch &rhs) const;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(proposal_id), K_(seq));
  int64_t proposal_id_;
  int64_t seq_;
};

// Unique ls id
struct TenantLSID {
public:
  OB_UNIS_VERSION(1);
public:
  TenantLSID();
  TenantLSID(const uint64_t tenant_id,
             const share::ObLSID &ls_id);
  ~TenantLSID();
  bool operator==(const TenantLSID &tenant_ls_id) const;
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(ls_id));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

// 1. All ls id in one tenant.
// 2. Max ls id in one tenant.
struct TenantLSIDS {
public:
  OB_UNIS_VERSION(1);
public:
  typedef common::ObFunction<int(const uint64_t tenant_id,
                                 const share::ObLSID max_ls_id,
                                 const share::ObLSID)> Functor;
  TenantLSIDS();
  ~TenantLSIDS();
  bool is_valid() const;
  bool exist(const share::ObLSID &ls_id) const;
  bool operator==(const TenantLSIDS &tenant_ls_ids) const;
  void set_max_ls_id(const TenantLSID &tenant_ls_id);
  const TenantLSID &get_max_ls_id() const;
  int push_back(const share::ObLSID &ls_id);
  int for_each(Functor &func);
  TO_STRING_KV(K_(max_ls_id), K_(ls_ids));
private:
  static constexpr int64_t DEFAULT_LS_NUM = 16;
  typedef ObSEArray<share::ObLSID, DEFAULT_LS_NUM> LSIDArray;
  // Max ls id in tenant
  TenantLSID max_ls_id_;
  // All ls id in tenant
  LSIDArray ls_ids_;
  friend class TenantLSIDSArray;
};

struct TenantLSIDSArray {
public:
  OB_UNIS_VERSION(1);
public:
  typedef common::ObFunction<int(const TenantLSIDS &tenant_ls_ids)> Functor;
  TenantLSIDSArray();
  ~TenantLSIDSArray();
  int assign(const TenantLSIDSArray &ths);
  bool operator==(const TenantLSIDSArray &ls_ids) const;
  bool is_valid() const;
  bool exist(const uint64_t tenant_id) const;
  int operate(const uint64_t tenant_id,
              Functor &func);
  void set_max_tenant_id(const uint64_t max_tenant_id);
  uint64_t get_max_tenant_id() const;
  int push_back(const TenantLSIDS &tenant_ls_ids);
  int for_each(Functor &func);
  void reset();
  TO_STRING_KV(K_(array), K_(max_tenant_id));
private:
  static constexpr int64_t DEFAULT_TENANT_NUM = 16;
  typedef ObSEArray<TenantLSIDS, DEFAULT_TENANT_NUM> LSIDSArray;
  LSIDSArray array_;
  uint64_t max_tenant_id_;
};
} // end namespace arbserver
} // end namespace oceanbase
#endif
