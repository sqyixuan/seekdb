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

#ifndef OCEANBASE_LIBOBCDC_SERVER_ENDPOINT_ACCESS_INFO_H__
#define OCEANBASE_LIBOBCDC_SERVER_ENDPOINT_ACCESS_INFO_H__

#include "lib/net/ob_addr.h"
#include "lib/container/ob_array_serialization.h"

namespace oceanbase
{
namespace libobcdc
{
static const int64_t MAX_HOSTNAME_LEN = 100;
static const int64_t MAX_HOSTNAME_BUFFER_LEN = MAX_HOSTNAME_LEN + 1;
// Endpoint format: hostname:port
// hostname may be IP, or domain, or SLB addr
class ObCDCEndpoint
{
public:
  ObCDCEndpoint() : host_(), port_(0), is_domain_(false) { reset(); }
  ~ObCDCEndpoint() { reset(); }
public:
  int init(const char *tenant_endpoint);
  ObCDCEndpoint &operator=(const ObCDCEndpoint &other);
  void reset() { MEMSET(host_, '\0', MAX_HOSTNAME_BUFFER_LEN); port_ = 0; is_domain_ = false; }
  OB_INLINE const char *get_host() const { return host_; }
  OB_INLINE int32_t get_port() const { return port_; }
  ObAddr get_addr() const;
  bool is_valid() const;
  TO_STRING_KV(K_(host), K_(port), K_(is_domain), "addr", get_addr());
private:
  int check_domain_or_addr_();

private:
  char    host_[MAX_HOSTNAME_BUFFER_LEN];
  int64_t port_;
  bool    is_domain_; // is domain or IP Addr
};

class ObAccessInfo
{
public:
  ObAccessInfo() : user_(), password_() { reset(); }
  ~ObAccessInfo() { reset(); }
public:
  int init(const char *user, const char *password);
  void reset();
public:
  const char *get_user() const { return user_; }
  const char *get_password() const { return password_; }
  // TODO support get base64 encoded password
  const char *get_base64_encoded_password() const { return nullptr; }
  TO_STRING_KV(K_(user), K_(password));
private:
  char user_[common::OB_MAX_USER_NAME_BUF_LENGTH];
  // TODO use base64 encode to print info.
  char password_[common::OB_MAX_PASSWORD_BUF_LENGTH];
};

typedef common::ObArray<ObCDCEndpoint> ObCDCEndpointList;

} // namespace libobcdc
} // namespace oceanbase
#endif
