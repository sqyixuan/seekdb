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

#ifndef  OCEANBASE_LIBOBCDC_DEFINE_H_
#define  OCEANBASE_LIBOBCDC_DEFINE_H_

#include "logservice/common_util/ob_log_ls_define.h"
#include "storage/tx/ob_tx_log.h"             // ObTransID

namespace oceanbase
{
namespace libobcdc
{
// TransID with Tenant Info(ObTransID is Unique in a Tenant but may not unique amone different tenants)
struct TenantTransID
{
  uint64_t                tenant_id_;
  transaction::ObTransID  trans_id_;    // trans_id(int64_t)

  TenantTransID(const uint64_t tenant_id, const transaction::ObTransID tx_id);
  TenantTransID();
  ~TenantTransID();

  void reset();
  int compare(const TenantTransID &other) const;
  bool operator==(const TenantTransID &other) const;
  uint64_t hash() const;

  bool is_valid() const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  const transaction::ObTransID &get_tx_id() const { return trans_id_; }

  TO_STRING_KV(K_(tenant_id), K_(trans_id));
};

// TenentLSID + ObTransID: Uniquely identifies a LS-Trans
struct PartTransID
{
  // TenentLSID
  logservice::TenantLSID       tls_id_;
  // transaction::ObTransID(int64_t)
  transaction::ObTransID       trans_id_;

  PartTransID() : tls_id_(), trans_id_() {}
  PartTransID(const logservice::TenantLSID &tls_id, const transaction::ObTransID &trans_id);
  ~PartTransID();
  bool operator==(const PartTransID &part_trans_id) const;
  PartTransID &operator=(const PartTransID &other);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  void reset();

  bool is_valid() const { return tls_id_.is_valid() && trans_id_ > 0; }
  bool is_sys_ls() const { return tls_id_.is_sys_log_stream(); }
  const logservice::TenantLSID &get_tls_id() const { return tls_id_; };
  uint64_t get_tenant_id() const { return tls_id_.get_tenant_id(); };
  const share::ObLSID &get_ls_id() const { return tls_id_.get_ls_id(); };
  const transaction::ObTransID &get_tx_id() const { return trans_id_; };

  TO_STRING_KV(K_(tls_id), K_(trans_id));
};

} // end namespace libobcdc
} // end namespace oceanbase

#endif
