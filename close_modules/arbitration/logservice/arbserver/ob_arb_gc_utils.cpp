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

#include "ob_arb_gc_utils.h"
#include "lib/container/ob_se_array_iterator.h"     // ObSEArrayIterator
#include "logservice/palf/log_define.h"             // INVALID_PROPOSAL_ID

namespace oceanbase
{
namespace arbserver
{
OB_SERIALIZE_MEMBER(GCMsgEpoch, proposal_id_, seq_);

GCMsgEpoch::GCMsgEpoch() : 
  proposal_id_(palf::INVALID_PROPOSAL_ID),
  seq_(-1)
{}

GCMsgEpoch::GCMsgEpoch(int64_t proposal_id, int64_t seq) : 
  proposal_id_(proposal_id), seq_(seq)
{}

GCMsgEpoch::~GCMsgEpoch()
{
  reset();
}

void GCMsgEpoch::reset()
{
  proposal_id_ = palf::INVALID_PROPOSAL_ID;
  seq_ = -1;
}

bool GCMsgEpoch::operator<(const GCMsgEpoch &rhs) const
{
  bool bool_ret = false;
  if (false == this->is_valid()) {
    bool_ret = rhs.is_valid();
  } else if (false == rhs.is_valid()) {
    bool_ret = false;
  } else {
    bool_ret =  (this->proposal_id_ < rhs.proposal_id_) ||
        (this->proposal_id_ == rhs.proposal_id_ && this->seq_ < rhs.seq_);
  }
  return bool_ret;
}

bool GCMsgEpoch::operator==(const GCMsgEpoch &rhs) const
{
  return this->proposal_id_ == rhs.proposal_id_ && this->seq_ == rhs.seq_;
}

bool GCMsgEpoch::operator>(const GCMsgEpoch &rhs) const
{
  return rhs < *this;
}

bool GCMsgEpoch::is_valid() const
{
  return palf::INVALID_PROPOSAL_ID != proposal_id_ && -1 != seq_;
}

OB_SERIALIZE_MEMBER(TenantLSID, tenant_id_, ls_id_);

TenantLSID::TenantLSID() : tenant_id_(OB_INVALID_TENANT_ID),
                           ls_id_()
{}

TenantLSID::TenantLSID(const uint64_t tenant_id,
                       const share::ObLSID &ls_id) 
  : tenant_id_(tenant_id),
    ls_id_(ls_id)
{}

TenantLSID::~TenantLSID()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
}

bool TenantLSID::operator==(const TenantLSID &rhs) const
{
  return this->tenant_id_ == rhs.tenant_id_ && this->ls_id_ == rhs.ls_id_;
}

bool TenantLSID::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && ls_id_.is_valid();
}

OB_SERIALIZE_MEMBER(TenantLSIDS, max_ls_id_, ls_ids_);
TenantLSIDS::TenantLSIDS() : max_ls_id_(),
                             ls_ids_()
{}

TenantLSIDS::~TenantLSIDS()
{
}

bool TenantLSIDS::operator==(const TenantLSIDS &rhs) const
{
  bool bool_ret = (this->max_ls_id_ == rhs.max_ls_id_);
  const int64_t count = this->ls_ids_.count();
  const int64_t rhs_count = rhs.ls_ids_.count();
  bool_ret &= (count == rhs_count);
  for (int64_t i = 0; bool_ret && i < count; i++) {
    if (!(this->ls_ids_[i] == rhs.ls_ids_[i])) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool TenantLSIDS::is_valid() const
{
  return max_ls_id_.is_valid() && !ls_ids_.empty();
}

bool TenantLSIDS::exist(const share::ObLSID &ls_id) const
{
  bool bool_ret = false;
  const int64_t count = ls_ids_.count();
  for (int64_t i = 0; !bool_ret && i < count; i++) {
    if (ls_id == ls_ids_[i]) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

void TenantLSIDS::set_max_ls_id(const TenantLSID &tenant_ls_id)
{
  max_ls_id_ = tenant_ls_id;
}

const TenantLSID& TenantLSIDS::get_max_ls_id() const
{
  return max_ls_id_;
}

int TenantLSIDS::push_back(const share::ObLSID &ls_id)
{
  return ls_ids_.push_back(ls_id);
}

int TenantLSIDS::for_each(Functor &func)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = max_ls_id_.tenant_id_;
  const share::ObLSID max_ls_id = max_ls_id_.ls_id_;
  for (auto iter = ls_ids_.begin(); OB_SUCC(ret) && iter != ls_ids_.end(); iter++) {
    if (OB_FAIL(func(tenant_id, max_ls_id, *iter))) {
      CLOG_LOG(WARN, "execute func failed", K(ret), K(tenant_id), K(max_ls_id), K(*iter));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(TenantLSIDSArray, array_, max_tenant_id_);

TenantLSIDSArray::TenantLSIDSArray() :
  array_(), max_tenant_id_(UINT64_MAX)
{
}

TenantLSIDSArray::~TenantLSIDSArray()
{
  max_tenant_id_ = UINT64_MAX;
}

int TenantLSIDSArray::assign(const TenantLSIDSArray &rhs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(array_.assign(rhs.array_))) {
    CLOG_LOG(WARN, "ObSEArray assign failed", K(ret), K(rhs));
  } else {
    max_tenant_id_ = rhs.max_tenant_id_;
  }
  return ret;
}

bool TenantLSIDSArray::operator==(const TenantLSIDSArray &rhs) const
{
  const int64_t count = this->array_.count();
  const int64_t rhs_count = rhs.array_.count();
  bool bool_ret = (count == rhs_count && max_tenant_id_ == rhs.max_tenant_id_);
  for (int64_t i = 0; bool_ret && i < count; i++) {
    if (!(this->array_[i] == rhs.array_[i])) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool TenantLSIDSArray::is_valid() const
{
  return !array_.empty() && is_valid_tenant_id(max_tenant_id_);
}

bool TenantLSIDSArray::exist(const uint64_t tenant_id) const
{
  bool bool_ret = false;
  const int64_t count = array_.count();
  for (int64_t i = 0; !bool_ret && i < count; i++) {
    if (tenant_id == array_[i].max_ls_id_.tenant_id_) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int TenantLSIDSArray::operate(const uint64_t tenant_id,
                              Functor &func)
{
  int ret = OB_SUCCESS;
  const int64_t count = array_.count();
  for (int64_t i = 0; i < count; i++) {
    if (tenant_id == array_[i].max_ls_id_.tenant_id_) {
      ret = func(array_[i]);
    }
  }
  return ret;
}

void TenantLSIDSArray::set_max_tenant_id(const uint64_t max_tenant_id)
{
  max_tenant_id_ = max_tenant_id;
}

uint64_t TenantLSIDSArray::get_max_tenant_id() const
{
  return max_tenant_id_;
}

int TenantLSIDSArray::push_back(const TenantLSIDS &tenant_ls_ids)
{
  return array_.push_back(tenant_ls_ids);
}

int TenantLSIDSArray::for_each(Functor &func)
{
  int ret = OB_SUCCESS;
  for (auto iter = array_.begin(); OB_SUCC(ret) && iter != array_.end(); iter++) {
    if (OB_FAIL(func(*iter))) {
      CLOG_LOG(WARN, "execute func failed", K(*iter));
    }
  }
  return ret;
}

void TenantLSIDSArray::reset()
{
  array_.reset();
  max_tenant_id_ = UINT64_MAX;
}
} // end namespace arbserver
} // end namespace oceanbase
