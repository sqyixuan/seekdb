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

#ifndef OCEANBASE_SHARE_OB_LICENSE_MGR_H
#define OCEANBASE_SHARE_OB_LICENSE_MGR_H

#include "lib/json/ob_json.h"
#include "ob_license.h"
#include "ob_license_timestamp_service.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/task/ob_timer.h"

#define LICENSE_ULID_LEN 26

namespace oceanbase
{
namespace share
{
class ObLicenseMgr;
class ObLicenseGuard
{
public:
  ObLicenseGuard() : license_(nullptr) {}
  ~ObLicenseGuard() { reset(); }
  int init(ObLicense *license);
  void reset();
  ObLicense *get() { return license_; }
  TO_STRING_KV(KPC_(license));
private:
  ObLicense *license_;
};

class ObLicenseExpirationCheckTask : public ObTimerTask
{
public:
  ObLicenseExpirationCheckTask(ObLicenseMgr *license_mgr) : license_mgr_(license_mgr) {}
  void runTimerTask();
private:
  ObLicenseMgr *license_mgr_ = nullptr;
};

class ObLicenseMgr
{
private:
  static const char *PUBLIC_KEY_PEM;
public:
  ObLicenseMgr();
  ~ObLicenseMgr();
  int load_license(const char *license_str, int64_t license_len);
  int start();
  bool is_start() { return is_start_; }
  static ObLicenseMgr &get_instance();
  TO_STRING_KV(K_(is_start),
               K_(timestamp_service),
               KPC_(license),
               K_(boot_with_expired),
               K_(cluster_ulid),
               K_(last_warn_time));
  bool is_boot_expired() { return boot_with_expired_; }
  int get_license(ObLicenseGuard &license_guard);
  int check_expired(ObLicense *license, int64_t &remain_time);
  int count_server_node_num(int64_t &server_num);
private:
  int extract_license(json::Value *root, ObLicense *&license);
  int parse_date(const common::ObString &time_str, uint64_t &time);
  int replace_license(ObLicense *license);
  int object_get_value(json::Object &json_obj,
                       const char *key,
                       json::Type type,
                       json::Value *&result);
  int unescape_json_string(ObIAllocator &allocate, const ObString &input, ObString &output);
  int copy_string(ObIAllocator &allocator, const ObString &from, ObString &to, bool need_escape = false);
  int array_to_string(ObIAllocator &allocator, const json::Array &json_array, ObString &to);
  // int validate_version(const common::ObString &product_version);
  int validate_license(ObLicense &license);
  int gen_trail_license(ObLicense *&license);
  int load_from_inner_table(ObLicense *&license);
  int store_to_inner_table(ObLicense &license, bool new_row = false);
  int decrypt_license(ObArenaAllocator &allocator,
                      const common::ObString &encrypted_license,
                      common::ObString &license_str);
  int parse_license(const common::ObString &decrypted_license, ObLicense *&license);
  int count_current_tenant(int64_t &tenant_num);
  int count_standby_tenant(int64_t &tenant_num);
  int parse_options(ObLicense &license);
  int refresh();

private:
  bool is_start_;
  bool boot_with_expired_;
  bool is_refreshing_;
  bool is_unittest_;
  ObLicense *license_;
  ObLicenseTimestampService timestamp_service_;
  ObLicenseExpirationCheckTask expiration_check_task_;
  int64_t last_warn_time_;
  int64_t last_refresh_time_;
  SpinRWLock lock_;
  char cluster_ulid_[LICENSE_ULID_LEN + 1] = {0};
  static ObLicenseMgr instance;
};
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_LICENSE_MGR_H
