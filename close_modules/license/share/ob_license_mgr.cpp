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

#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/sha.h>
#include <time.h>

#include "ob_license_mgr.h"
#include "lib/worker.h"
#include "lib/oblog/ob_log.h"
#include "lib/encode/ob_base64_encode.h"
#include "share/ob_cluster_version.h"
#include "lib/container/ob_se_array.h"
#include "lib/time/Time.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/page_arena.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "ob_license_pub_key.h"
#include "lib/random/ob_random.h"
#include "lib/thread/thread_mgr.h"

using namespace oceanbase::common;
using namespace obutil;

namespace oceanbase
{
namespace share
{
const char *ObLicenseMgr::PUBLIC_KEY_PEM = OB_LICENSE_PUBLIC_KEY_PEM;
ObLicenseMgr ObLicenseMgr::instance;
const char *DEFAULT_LICENSE;
const char *FIELD_LICENSE = "license";
const char *FIELD_END_USER = "EndUser";
const char *FIELD_LICENSE_ID = "LicenseID";
const char *FIELD_LICENSE_CODE = "LicenseCode";
const char *FIELD_LICENSE_TYPE = "LicenseType";
const char *FIELD_PRODUCT_TYPE = "ProductType";
const char *FIELD_ISSUANCE_DATE = "IssuanceDate";
const char *FIELD_VALIDITY_PERIOD = "ValidityPeriod";
const char *FIELD_CORE_NUM = "CoreNum";
const char *FIELD_NODE_NUM = "NodeNum";
const char *FIELD_OPTIONS = "Options";
const char *FIELD_VERSION = "Version"; // License Version
const char *FIELD_SIGNATURE = "signature";
const char *TRIAL_END_USER = "Trial User";
const char *TRIAL_LICENSE_TYPE = "Trial";
const char *TRIAL_PRODUCT_TYPE = "SE";
const char *TRIAL_LICENSE_ID = "Trial ID";
const char *TRIAL_LICENSE_CODE = "Trial Code";
const char *DEBUG_TRIAL_LICENSE_CODE = "Debug License";
const char *TRIAL_OPTIONS = "Unlimited";
const char *VALIDATE_PERMANENTLY = "valid permanently";
const int64_t MAX_EXPIRE_DATE = (int64_t) 32472115200 * 1000 * 1000; // 2999-01-01 00:00:00
const int64_t TRIAL_CORE_NUM = 9999;
const int64_t TRIAL_NODE_NUM = 1;
const int64_t LICENSE_EXPIRE_CHECK_PERIOD = (int64_t) 24 * 60 * 60 * 1000 * 1000; // single day
const int64_t LICENSE_EXPIRE_WARNING_TIME = (int64_t) 30 * 24 * 60 * 60 * 1000 * 1000; // 30 day
const int64_t LICENSE_REFRESH_PERIOD = (int64_t) 10 * 60 * 1000 * 1000; // 10 minute
const int64_t TRIAL_VALIDITY_PERIOD = (int64_t) 180 * 24 * 60 * 60 * 1000 * 1000; // 180 day
const char *SELECT_INNER_SQL
    = "SELECT LICENSE_ID, LICENSE_CODE, LICENSE_TYPE, PRODUCT_TYPE, ISSUANCE_DATE, "
      "ACTIVATION_TIME, EXPIRED_TIME, OPTIONS, CORE_NUM, NODE_NUM, END_USER, CLUSTER_ULID from "
      "__all_license";
const char *DELETE_INNER_SQL = "DELETE FROM __all_license";
const char *INSERT_INNER_SQL
    = "INSERT INTO __all_license(END_USER, LICENSE_ID, LICENSE_CODE, LICENSE_TYPE, PRODUCT_TYPE, "
      " ISSUANCE_DATE, ACTIVATION_TIME, EXPIRED_TIME, OPTIONS, CORE_NUM, NODE_NUM, "
      "LTS_TIME, CLUSTER_ULID) "
      "VALUES ('%s', '%s', '%s', '%s', '%s', FROM_UNIXTIME(CAST(%ld as DECIMAL(30, 6)) / "
      "1000000), FROM_UNIXTIME(CAST(%ld as DECIMAL(30, 6)) / 1000000), "
      "FROM_UNIXTIME(CAST(%ld as DECIMAL(30, 6)) / 1000000), '%s', %ld, %ld, "
      "FROM_UNIXTIME(CAST(1 as DECIMAL(30, 6)) / 1000000), '%s')";
const char *UPDATE_INNER_SQL
    = "UPDATE __all_license SET "
      "END_USER = '%s', LICENSE_ID = '%s', LICENSE_CODE = '%s', LICENSE_TYPE = '%s', PRODUCT_TYPE "
      "= '%s', ISSUANCE_DATE = FROM_UNIXTIME(CAST(%ld as DECIMAL(30, 6)) / "
      "1000000), ACTIVATION_TIME = FROM_UNIXTIME(CAST(%ld as DECIMAL(30, 6)) / 1000000), "
      "EXPIRED_TIME = FROM_UNIXTIME(CAST(%ld as DECIMAL(30, 6)) / 1000000), OPTIONS = '%s', "
      "CORE_NUM = %ld, NODE_NUM = %ld, CLUSTER_ULID = '%s'";

const char *COUNT_TENANT_SQL = "SELECT COUNT(*) FROM __ALL_TENANT WHERE TENANT_ID % 2 = 0 AND IN_RECYCLEBIN = 0";
const char *COUNT_STANDBY_SQL = "SELECT COUNT(*) FROM __ALL_VIRTUAL_TENANT_INFO WHERE TENANT_ROLE = 'STANDBY'";
const char *COUNT_SERVER_SQL = "SELECT COUNT(*) FROM __ALL_SERVER";

enum class LicenseOptions
{
  INVALID,
  EE, // Enterprise Edition 
  PS, // Primary-Standby
  MT, // MultiTenant
  AP, // OLAP
  HS, // Horizontal Scaling
  HA, // Multi-Replica High Availability (HA)
  AS, // Arbitrary Service
  MAX
};

const char *options_string[] = {"INVALID",
                                "EE",
                                "PS",
                                "MT",
                                "AP",
                                "HS",
                                "HA",
                                "AS",
                                "MAX"};

const char *option_to_string(LicenseOptions option)
{
  const char *result = nullptr;
  if (option < LicenseOptions::MAX) {
    result = options_string[static_cast<int>(option)];
  }
  return result;
}

LicenseOptions string_to_option(const char *string, int len)
{
  LicenseOptions result = LicenseOptions::INVALID;
  for (int i = 0; i < static_cast<int>(LicenseOptions::MAX); i++) {
    if (0 == strncmp(string, options_string[i], len)) {
      result = static_cast<LicenseOptions>(i);
      break;
    }
  }
  return result;
}

int secure_random(uint8_t *buffer, size_t length) {
  int ret = OB_SUCCESS;
  ObRandom random;
  random.seed(ObSysTime::now().toMicroSeconds());
  for (size_t i = 0; i < length; ++i) {
    buffer[i] = static_cast<uint8_t>(random.get(0, 255));
  }
  return ret;
}

int generate_ulid(char *buf, int64_t buf_len) {
  int ret = OB_SUCCESS;
  uint8_t ulid_bytes[16] = {0};
  uint64_t timestamp = ObSysTime::now().toMilliSeconds() & 0x0000FFFFFFFFFFFF;

  // Crockford Base32
  const static char base32_chars[32] = {
      '0','1','2','3','4','5','6','7','8','9',
      'A','B','C','D','E','F','G','H','J','K',
      'M','N','P','Q','R','S','T','V','W','X','Y','Z'
  };

  if (buf_len < LICENSE_ULID_LEN) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is too short", K(buf_len), K(ret));
  } else if (OB_FAIL(secure_random(ulid_bytes + 6, 10))) {
    LOG_WARN("failed to generate random", KR(ret));
  } else {
    ulid_bytes[0] = static_cast<uint8_t>((timestamp >> 40) & 0xFF);
    ulid_bytes[1] = static_cast<uint8_t>((timestamp >> 32) & 0xFF);
    ulid_bytes[2] = static_cast<uint8_t>((timestamp >> 24) & 0xFF);
    ulid_bytes[3] = static_cast<uint8_t>((timestamp >> 16) & 0xFF);
    ulid_bytes[4] = static_cast<uint8_t>((timestamp >> 8) & 0xFF);
    ulid_bytes[5] = static_cast<uint8_t>(timestamp & 0xFF);

    for (size_t i = 0; i < LICENSE_ULID_LEN; ++i) {
      size_t bit_offset = i * 5;
      uint32_t value = 0;

      for (size_t j = 0; j < 5; ++j) {
          size_t current_bit = bit_offset + j;
          uint8_t byte = 0;

          if (current_bit < 128) {
              size_t byte_pos = current_bit / 8;
              size_t bit_pos = 7 - (current_bit % 8);
              byte = (ulid_bytes[byte_pos] >> bit_pos) & 1;
          }

          value = (value << 1) | byte;
      }

      buf[i] = base32_chars[value & 0x1F];
    }
  }

  return ret;
}

void ObLicenseExpirationCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObLicenseGuard license_guard;
  int64_t remain_time = 0;
  uint64_t expiration_time = 0; ;
  ObLicense *license = nullptr;

  if (OB_ISNULL(license_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("license mgr is NULL", KR(ret), KP(license_mgr_));
  } else if (OB_FAIL(license_mgr_->get_license(license_guard))) {
    LOG_WARN("failed to get license", KR(ret));
  } else if (OB_ISNULL(license = license_guard.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("license is nullptr", KR(ret));
  } else if (FALSE_IT(expiration_time = license->expiration_time_)) {
  } else if (OB_FAIL(license_mgr_->check_expired(license, remain_time))) {
    if (ret == OB_LICENSE_EXPIRED) {
      LOG_ERROR("license is expired, please update your license", KR(ret), KTIME(expiration_time));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("failed to check expired", KR(ret), KPC(license_mgr_));
    }
  } else if (remain_time < LICENSE_EXPIRE_WARNING_TIME) {
    int remain_day = remain_time / ((int64_t) 24 * 60 * 60 * 1000 * 1000) + 1;
    if (remain_time <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid remain time", KR(ret), K(remain_time), KPC(license_mgr_));
    } else {
      LOG_ERROR("license is about to expire", K(remain_day), KTIME(expiration_time));
    }
  }
  FLOG_INFO("finish check expired", KR(ret), K(remain_time), K(expiration_time), KPC(license));
}

int ObLicenseGuard::init(ObLicense *license)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(license)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start license guard with invalid arguments", KR(ret));
  } else if (license->inc_ref() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected ref cnt on oblicense", KR(ret), KP(license), KPC(license));
  } else {
    license_ = license;
  }
  return ret;
}

void ObLicenseGuard::reset()
{
  if (OB_NOT_NULL(license_)) {
    license_->dec_ref();
    license_ = nullptr;
  }
}

ObLicenseMgr::ObLicenseMgr()
    : is_start_(false), boot_with_expired_(false), is_refreshing_(false), is_unittest_(false), license_(nullptr),
    timestamp_service_(), expiration_check_task_(this), last_warn_time_(0), last_refresh_time_(0), lock_()
{
}

ObLicenseMgr::~ObLicenseMgr()
{}

int ObLicenseMgr::start()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLicense *license = nullptr;
  int64_t current_time = 0;

  if (is_start_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("license is already started", K(ret));
  } else if (OB_TMP_FAIL(load_from_inner_table(license))) {
    if (tmp_ret == OB_ITER_END) {
      tmp_ret = OB_SUCCESS;
      LOG_TRACE("in bootstrap, no license in inner table, generate license now", KR(tmp_ret));
    } else if (tmp_ret == OB_TABLE_NOT_EXIST) {
      ret = tmp_ret;
      LOG_TRACE("inner table has not been created", KR(ret));
    } else {
      ret = tmp_ret;
      LOG_WARN("failed to load from inner table", KR(ret));
    }
    if (OB_SUCC(tmp_ret)) {
      int64_t current_time_sys = ObSysTime::now().toMicroSeconds();

      if (OB_FAIL(generate_ulid(cluster_ulid_, LICENSE_ULID_LEN))) {
        LOG_WARN("failed to generate ulid", KR(ret));
      } else if (OB_FAIL(gen_trail_license(license))) {
        LOG_WARN("failed to gen trail license", KR(ret));
      } else if (OB_ISNULL(license)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("license is NULL", K(ret));
      } else if (OB_FAIL(store_to_inner_table(*license, true))) {
        LOG_WARN("failed to store license", KR(ret));
      } else if (OB_FAIL(timestamp_service_.start_with_time(current_time_sys))) {
        LOG_WARN("failed to start license timestamp service", KR(ret));
      }
    }
  } else {
    if (OB_FAIL(timestamp_service_.start_from_inner_table())) {
      LOG_WARN("failed to start license timestamp service", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replace_license(license))) {
    LOG_WARN("failed to replace license", KR(ret));
  } else if (OB_FAIL(timestamp_service_.get_time(current_time))) {
    LOG_WARN("failed to get time", KR(ret));
  } else if (OB_UNLIKELY(current_time > license->expiration_time_)) {
    boot_with_expired_ = true;
  } else {
    boot_with_expired_ = false;
    last_refresh_time_ = current_time;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, expiration_check_task_, LICENSE_EXPIRE_CHECK_PERIOD, true))) {
    LOG_WARN("failed to schedule expiration check task", KR(ret));
  }

  if (OB_SUCC(ret)) {
    is_start_ = true;
    LOG_INFO("successfully start license mgr", KR(ret), K(*this));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(license)) {
    OB_DELETE(ObLicense, ObMemAttr(OB_SYS_TENANT_ID, "license"), license);
  }
  FLOG_INFO("finish start license mgr", KR(ret), KPC(this));

  return ret;
}

int ObLicenseMgr::get_license(ObLicenseGuard &license_guard) {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_TMP_FAIL(this->refresh())) {
    LOG_ERROR("failed to refresh license", KR(ret));
  }

  {
    SpinRLockGuard guard(lock_);

    if (OB_ISNULL(license_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("license is NULL", K(ret), KP(license_), KPC(this));
    } else if (OB_FAIL(license_guard.init(license_))) {
      LOG_WARN("failed to set license guard", KR(ret));
    }
  }

  return ret;
}

ERRSIM_POINT_DEF(LICENSE_MGR_FORCE_REFRESH);
int ObLicenseMgr::refresh()
{
  int ret = OB_SUCCESS;
  int64_t current_time = 0;
  ObLicense *license = nullptr;

  if (OB_LIKELY(ATOMIC_BCAS(&is_refreshing_, false, true))) {
    if (OB_UNLIKELY(is_unittest_)) {
    } else if (OB_FAIL(timestamp_service_.get_time(current_time))) {
      LOG_WARN("failed to get time", KR(ret));
    } else if (last_refresh_time_ + LICENSE_REFRESH_PERIOD > current_time && OB_LIKELY(LICENSE_MGR_FORCE_REFRESH == OB_SUCCESS)) {
      // not need to refresh
    } else if (OB_FAIL(load_from_inner_table(license))) {
      LOG_WARN("failed to load from inner table", KR(ret));
    } else if (OB_FAIL(replace_license(license))) {
      LOG_WARN("failed to replace license", KR(ret));
    } else {
      last_refresh_time_ = current_time;
    }
    is_refreshing_ = false;
  }

  return ret;
}

int ObLicenseMgr::load_license(const char *license_str, int64_t license_len)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator allocator;
  json::Parser parser;
  json::Value *root = nullptr;
  json::Value *signature_obj = nullptr;
  json::Value *signature = nullptr;
  ObString decrypted_license_str;
  ObLicense *license = nullptr;

  LOG_TRACE("begin load license", K(license_str), K(license_len), K(lbt()));
  
  if (OB_UNLIKELY(!is_start_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("license mgr is not started", K(ret), K(is_start_), KPC(license_));
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("failed to init parser", KR(ret));
  } else if (OB_FAIL(parser.parse(license_str, license_len, root))) {
    LOG_WARN("failed to parse license", KR(ret));
    ret = OB_INVALID_LICENSE;
    LOG_USER_ERROR(OB_INVALID_LICENSE, ob_error_name(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), "signature", json::JT_OBJECT, signature_obj))) {
    LOG_WARN("failed to get singature", KR(ret));
  } else if (OB_FAIL(object_get_value(signature_obj->get_object(), "signature", json::JT_STRING, signature))) {
    LOG_WARN("failed to get signature", KR(ret));
  } else if (OB_FAIL(decrypt_license(allocator, signature->get_string(), decrypted_license_str))) {
    LOG_WARN("failed to decrypt license", KR(ret));
  } else if (OB_FAIL(parse_license(decrypted_license_str, license))) {
    LOG_WARN("failed to parse decrypted license", KR(ret));
  } else if (OB_FAIL(validate_license(*license))) {
    LOG_WARN("failed to validate license", KR(ret));
  } else if (OB_FAIL(store_to_inner_table(*license))) {
    LOG_WARN("failed to store license", KR(ret));
  } else if (OB_FAIL(replace_license(license))) {
    LOG_WARN("failed to replace license", KR(ret));
  } else {
    boot_with_expired_ = false;
  }

  LOG_INFO("load license done", KR(ret), KPC(license), K(license_str), K(license_len), K(decrypted_license_str));

  return ret;
}

int ObLicenseMgr::parse_license(const common::ObString &decrypted_license, ObLicense *&license)
{
  int ret = OB_SUCCESS;
  json::Value *license_obj_root = nullptr;
  json::Value *license_root = nullptr;
  ObArenaAllocator allocator;
  json::Parser parser;

  if (decrypted_license.length() == 0) {
    ret = OB_INVALID_LICENSE;
    LOG_WARN("license is empty", KR(ret), K(decrypted_license));
    LOG_USER_ERROR(OB_INVALID_LICENSE, "license is empty");
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("failed to init parser", KR(ret));
  } else if (OB_FAIL(parser.parse(decrypted_license.ptr(), decrypted_license.length(), license_obj_root))) {
    LOG_WARN("failed to parse license", KR(ret));
    ret = OB_INVALID_LICENSE;
    LOG_USER_ERROR(OB_INVALID_LICENSE, ob_error_name(ret));
  } else if (OB_FAIL(object_get_value(license_obj_root->get_object(), "license", json::JT_OBJECT, license_root))) {
      LOG_WARN("failed to get license", KR(ret));
  } else if (OB_FAIL(extract_license(license_root, license))) {
    LOG_WARN("failed to parse license", KR(ret));
  }

  return ret;
}

int ObLicenseMgr::decrypt_license(ObArenaAllocator &allocator,
                                   const common::ObString &encrypted_license,
                                   common::ObString &license_str)
{
  int ret = OB_SUCCESS;
  int vertify_result = -1;
  uint8_t *encrypted_data_buf = nullptr;
  int64_t encrypted_data_len = 0;
  int64_t encrypted_offset = 0;
  int64_t encrypted_buffer_len = encrypted_license.length() + PUBLIC_KEY_SIZE;
  uint8_t *decrypted_data_buf = nullptr;
  int64_t decrypted_offset = 0;
  int64_t decrypted_data_buf_len = encrypted_license.length() + PUBLIC_KEY_SIZE;
  BIO *bio = nullptr;
  RSA *rsa = nullptr;

  if (OB_UNLIKELY(encrypted_license.length() == 0)) {
    ret = OB_INVALID_LICENSE;
    LOG_WARN("encrypted license is empty", KR(ret), K(encrypted_license));
    LOG_USER_ERROR(OB_INVALID_LICENSE, "license signature is empty");
  } else if (OB_ISNULL(encrypted_data_buf = (uint8_t *)allocator.alloc(encrypted_buffer_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(encrypted_buffer_len));
  } else if (OB_ISNULL(decrypted_data_buf = (uint8_t *)allocator.alloc(decrypted_data_buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(decrypted_data_buf_len));
  } else if (OB_FAIL(ObBase64Encoder::decode(encrypted_license.ptr(),
                                             encrypted_license.length(),
                                             encrypted_data_buf,
                                             encrypted_buffer_len,
                                             encrypted_data_len))) {
    if (ret == OB_INVALID_ARGUMENT) {
      ret = OB_INVALID_LICENSE;
      LOG_WARN("license signature is not encoded as base64", KR(ret), K(encrypted_license));
      LOG_USER_ERROR(OB_INVALID_LICENSE, "license signature is not encoded as base64");
    } else {
      LOG_WARN("failed to decode signature", KR(ret));
    }
  } else if (OB_ISNULL(bio = BIO_new_mem_buf(PUBLIC_KEY_PEM, -1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create bio", KR(ret), K(PUBLIC_KEY_PEM));
  } else if (OB_ISNULL(rsa = PEM_read_bio_RSA_PUBKEY(bio, NULL, NULL, NULL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to read pubkey", KR(ret), K(PUBLIC_KEY_PEM));
  } else {
    while (encrypted_offset < encrypted_data_len
           && decrypted_offset + PUBLIC_KEY_SIZE < decrypted_data_buf_len) {
      int chunk_size = (encrypted_data_len - encrypted_offset > PUBLIC_KEY_SIZE)
                           ? PUBLIC_KEY_SIZE
                           : (encrypted_data_len - encrypted_offset);
      int decrypted_len = RSA_public_decrypt(chunk_size,
                                             encrypted_data_buf + encrypted_offset,
                                             decrypted_data_buf + decrypted_offset,
                                             rsa,
                                             RSA_PKCS1_PADDING);

      if (OB_UNLIKELY(decrypted_len == -1)) {
        ret = OB_INVALID_LICENSE;
        LOG_WARN("failed to decrypt data, signature is invalid", KR(ret), K(encrypted_license));
        LOG_USER_ERROR(OB_INVALID_LICENSE, "signature is invalid");
        break;
      }

      decrypted_offset += decrypted_len;
      encrypted_offset += chunk_size;
    }
    if (decrypted_offset + PUBLIC_KEY_SIZE >= decrypted_data_buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("decrypted data buffer is not enough", KR(ret), K(decrypted_offset), K(decrypted_data_buf_len), K(PUBLIC_KEY_SIZE));
    }
  }

  if (OB_SUCC(ret)) {
    license_str.assign_ptr((char *)decrypted_data_buf, decrypted_offset);
  }

  if (OB_NOT_NULL(bio)) {
    BIO_free(bio);
  }
  if (OB_NOT_NULL(rsa)) {
    RSA_free(rsa);
  }

  return ret;
}

int ObLicenseMgr::extract_license(json::Value *root, ObLicense *&license)
{
  int ret = OB_SUCCESS;
  json::Value *end_user = nullptr;
  json::Value *license_id = nullptr;
  json::Value *license_code = nullptr;
  json::Value *license_type = nullptr;
  json::Value *product_type = nullptr;
  // json::Value *product_version = nullptr;
  json::Value *issuance_date = nullptr;
  json::Value *validate_period = nullptr;
  json::Value *core_num = nullptr;
  json::Value *node_num = nullptr;
  json::Value *options = nullptr;
  license = nullptr;
  ObArenaAllocator *license_allocator = nullptr;
  uint64_t issuance_timestamp = 0;
  int64_t current_time = 0;

  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root is NULL", K(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_END_USER, json::JT_STRING, end_user))) {
    LOG_WARN("failed to get EndUser", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_LICENSE_ID, json::JT_STRING, license_id))) {
    LOG_WARN("failed to get LicenseID", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_LICENSE_CODE, json::JT_STRING, license_code))) {
    LOG_WARN("failed to get LicenseCode", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_LICENSE_TYPE, json::JT_STRING, license_type))) {
    LOG_WARN("failed to get LicenseType", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_PRODUCT_TYPE, json::JT_STRING, product_type))) {
    LOG_WARN("failed to get ProductType", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_ISSUANCE_DATE, json::JT_STRING, issuance_date))) {
    LOG_WARN("failed to get IssuanceDate", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_VALIDITY_PERIOD, json::JT_STRING, validate_period))) {
    LOG_WARN("failed to get ValidityPeriod", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_CORE_NUM, json::JT_NUMBER, core_num))) {
    LOG_WARN("failed to get CoreNum", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_NODE_NUM, json::JT_NUMBER, node_num))) {
    LOG_WARN("failed to get NodeNum", KR(ret));
  } else if (OB_FAIL(object_get_value(root->get_object(), FIELD_OPTIONS, json::JT_ARRAY, options))) {
    LOG_WARN("failed to get Options", KR(ret));
  } else if (OB_ISNULL(license = OB_NEW(ObLicense, ObMemAttr(OB_SYS_TENANT_ID, "license")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret));
  } else if (FALSE_IT(license_allocator = &(license->allocator_))) {
  } else if (OB_FAIL(copy_string(*license_allocator, end_user->get_string(), license->end_user_, true))) {
    LOG_WARN("failed to copy end_user", KR(ret));
  } else if (OB_FAIL(copy_string(*license_allocator, license_id->get_string(), license->license_id_, true))) {
    LOG_WARN("failed to copy license_id", KR(ret));
  } else if (OB_FAIL(copy_string(*license_allocator, license_code->get_string(), license->license_code_, true))) {
    LOG_WARN("failed to copy license_code", KR(ret));
  } else if (OB_FAIL(copy_string(*license_allocator, license_type->get_string(), license->license_type_, true))) {
    LOG_WARN("failed to copy license_type", KR(ret));
  } else if (OB_FAIL(copy_string(*license_allocator, product_type->get_string(), license->product_type_, true))) {
    LOG_WARN("failed to copy product_type", KR(ret));
  } else if (FALSE_IT(license->core_num_ = core_num->get_number())) {
  } else if (FALSE_IT(license->node_num_ = node_num->get_number())) {
  } else if (OB_FAIL(array_to_string(*license_allocator, options->get_array(), license->options_))) {
    LOG_WARN("failed to copy options", KR(ret));
  } else if (OB_FAIL(parse_date(validate_period->get_string(), license->expiration_time_))) {
  } else if (OB_FAIL(parse_date(issuance_date->get_string(), issuance_timestamp))) {
    LOG_WARN("failed to parse issuance_date", KR(ret), K(issuance_date->get_string()));
  } else if (OB_FAIL(timestamp_service_.get_time(current_time))) {
    LOG_WARN("failed to get time", KR(ret));
  } else if (OB_FAIL(parse_options(*license))) {
    LOG_WARN("failed to parse options", KR(ret));
  } else {
    license->activation_time_ = current_time;
    license->issuance_date_ = issuance_timestamp;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(license)) {
    OB_DELETE(ObLicense, ObMemAttr(OB_SYS_TENANT_ID, "license"), license);
  }  

  return ret;
}

int ObLicenseMgr::parse_options(ObLicense &license)
{
  int ret = OB_SUCCESS;
  
  if (license.license_type_ == TRIAL_LICENSE_TYPE) {
    license.allow_stand_by_ = true;
    license.allow_multi_tenant_ = true;
    license.allow_olap_ = true;
    license.license_trail_ = true;
  } else {
    license.allow_stand_by_ = false;
    license.allow_multi_tenant_ = false;
    license.allow_olap_ = false;
    license.license_trail_ = false;
    
    const char *options = license.options_.ptr();
    const char *delim = ",";
    const char *option_begin = options;
    const char *option_end = nullptr;
    ObSEArray<LicenseOptions, 5> options_array;
    LicenseOptions option = LicenseOptions::INVALID;
    int option_len = 0;

    while (OB_SUCC(ret) && (option_end = strchr(option_begin, *delim)) != NULL) {
      option_len = option_end - option_begin;
      option = string_to_option(option_begin, option_len);
      if (option == LicenseOptions::INVALID) {
        LOG_WARN("invalid license option", K(option_begin), K(option_len));
      } else if (OB_FAIL(options_array.push_back(option))) {
        LOG_WARN("failed to push back option", KR(ret));
      }
      if (OB_SUCC(ret)) {
        option_begin = option_end + 1;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(option = string_to_option(option_begin, strlen(option_begin)))) {
    } else if (option == LicenseOptions::INVALID) {
      LOG_WARN("invalid license option", K(option_begin), K(option_len));
    } else if (OB_FAIL(options_array.push_back(option))) {
      LOG_WARN("failed to push back option", KR(ret));
    }
    if (OB_SUCC(ret)) {
      for (int i = 0; i < options_array.count(); i++) {
        switch (options_array[i]) {
        case LicenseOptions::EE:
          break;
        case LicenseOptions::PS:
          license.allow_stand_by_ = true;
          break;
        case LicenseOptions::MT:
          license.allow_multi_tenant_ = true;
          break;
        case LicenseOptions::AP:
          license.allow_olap_ = true;
          break;
        default:
          LOG_WARN("unsupported option type",
                    K(i),
                    K(options_array[i]),
                    K(option_to_string(options_array[i])));
        }
      }
    }
  }
  return ret;
}
#ifndef NDEBUG
ERRSIM_POINT_DEF(LOAD_LICENSE_CHECK_OPTIONS);
#endif
int ObLicenseMgr::validate_license(ObLicense &license)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t current_tenant_count = 0;
  int64_t current_server_count = 0;

#ifndef NDEBUG
  if (OB_TMP_FAIL(LOAD_LICENSE_CHECK_OPTIONS)) {
    LOG_INFO("license options check has been disabled by errsim");
  }
#endif

  if (license.activation_time_ > license.expiration_time_) {
    ret = OB_INVALID_LICENSE;
    LOG_WARN("license is expired", KR(ret), K(license.activation_time_), K(license.expiration_time_));
    LOG_USER_ERROR(OB_INVALID_LICENSE, "license is expired");
  } else if (license.issuance_date_ > license.activation_time_) {
    ret = OB_INVALID_LICENSE;
    LOG_WARN("issuance_date should not be after activation_time", KR(ret), K(license.issuance_date_), K(license.activation_time_));
    LOG_USER_ERROR(OB_INVALID_LICENSE, "issuance_date is after activation_time, please check your system clock");
  } else if (OB_FAIL(tmp_ret)) {
    // license options check has been disabled by errsim
    ret = OB_SUCCESS;
  } else if (is_unittest_) {
  } else {
    if (license.allow_multi_tenant_) {
    } else if (OB_FAIL(count_current_tenant(current_tenant_count))) {
      LOG_WARN("failed to count current tenant", KR(ret));
    } else if (current_tenant_count > 1) {
      ret = OB_LICENSE_SCOPE_EXCEEDED;
      LOG_WARN("there are more than one tenant, but the license does not have the Multitenant option", KR(ret), K(current_tenant_count));
      LOG_USER_ERROR(OB_LICENSE_SCOPE_EXCEEDED, "there are more than one tenant, but the license does not have the Multitenant option");
    }
    if (OB_FAIL(ret) || license.allow_stand_by_) {
    } else if (OB_FAIL(count_standby_tenant(current_tenant_count))) {
      LOG_WARN("failed to count standby tenant", KR(ret));
    } else if (current_tenant_count > 0) {
      ret = OB_LICENSE_SCOPE_EXCEEDED;
      LOG_WARN("there are standby tenant, but the license does not have the Primary-Standby option", KR(ret), K(current_tenant_count));
      LOG_USER_ERROR(OB_LICENSE_SCOPE_EXCEEDED, "there are standby tenant, but the license does not have the Primary-Standby option");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(count_server_node_num(current_server_count))) {
      LOG_WARN("failed to count server node num", KR(ret));
    } else if (current_server_count > license.node_num_) {
      ret = OB_LICENSE_SCOPE_EXCEEDED;
      LOG_WARN("there are more sever nodes than the license allows", KR(ret), K(current_server_count), K(license.node_num_));
      LOG_USER_ERROR(OB_LICENSE_SCOPE_EXCEEDED, "there are more sever nodes than the license allows");      
    }
  }

  return ret;
}

int ObLicenseMgr::parse_date(const common::ObString &time_str, uint64_t &time)
{
  int ret = OB_SUCCESS;
  struct tm tm_time;
  time_t result_time = 0;
  ObArenaAllocator allocator;
  memset(&tm_time, 0, sizeof(struct tm));

  if (time_str == VALIDATE_PERMANENTLY) {
    time = MAX_EXPIRE_DATE;
  } else if (OB_ISNULL(strptime(time_str.ptr(), "%Y-%m-%d", &tm_time))) {
    ret = OB_INVALID_LICENSE;
    LOG_WARN("failed to parse issuance_date", KR(ret), K(time_str));
    LOG_USER_ERROR(OB_INVALID_LICENSE, "invalid license date");
  } else if (OB_UNLIKELY((result_time = mktime(&tm_time)) == -1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to mktime", KR(ret));
  } else {
    time = result_time * 1000000;
  }

  return ret;
}

int ObLicenseMgr::replace_license(ObLicense *license)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  if (OB_ISNULL(license)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("license should not be null", KR(ret));
  } else if (OB_UNLIKELY(license->inc_ref() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("license ref count is invalid", KR(ret), KPC(license));
  } else {
    if (OB_NOT_NULL(license_)) {
      license_->dec_ref();
    }
    license_ = license;
  }

  return ret;
}

int ObLicenseMgr::object_get_value(json::Object &json_obj, const char *key, json::Type type, json::Value *&result) {
  int ret = OB_SUCCESS;
  json::Pair *value = json_obj.get_first();

  while (value != json_obj.get_header() && value->name_ != key) {
    value = value->get_next();
  }

  if (OB_UNLIKELY(json_obj.get_header() == value)) {
    ret = OB_INVALID_LICENSE;
    LOG_WARN("field not found in json", KR(ret), K(key));
    LOG_USER_ERROR(OB_INVALID_LICENSE, "invalid license format");
  } else if (OB_UNLIKELY(value->value_->get_type() != type)) {
    ret = OB_INVALID_LICENSE;
    LOG_WARN("field type not match", KR(ret), K(key), K(type));
    LOG_USER_ERROR(OB_INVALID_LICENSE, "invalid license format");
  } else {
    result = value->value_;
  }

  return ret;
}

int unicode_escape_to_utf8(const char *hex_str, char *output) {
  unsigned int code_point = 0;
  for (int i = 0; i < 4; i++) {
      if (!isxdigit(hex_str[i])) return 0;
      code_point = (code_point << 4) | 
          (hex_str[i] >= 'A' ? (hex_str[i] & 0xDF) - 'A' + 10 : hex_str[i] - '0');
  }

  if (code_point <= 0x7F) {
      output[0] = code_point;
      return 1;
  } else if (code_point <= 0x7FF) {
      output[0] = 0xC0 | (code_point >> 6);
      output[1] = 0x80 | (code_point & 0x3F);
      return 2;
  } else if (code_point <= 0xFFFF) {
      output[0] = 0xE0 | (code_point >> 12);
      output[1] = 0x80 | ((code_point >> 6) & 0x3F);
      output[2] = 0x80 | (code_point & 0x3F);
      return 3;
  }
  return 0;
}

int ObLicenseMgr::unescape_json_string(ObIAllocator &allocate, const ObString &input, ObString &output) {
  size_t len = input.length();
  char *buf = nullptr;
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf = (char *) allocate.alloc(len * 4 + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(len));
  } else {
    char *out_ptr = buf;
    for (const char *in_ptr = input.ptr(); in_ptr < input.ptr() + len; in_ptr++) {
      if (*in_ptr == '\\') {
        in_ptr++;
        switch (*in_ptr) {
        case 'u': {
          if (in_ptr + 4 < input.ptr() + len && isxdigit(in_ptr[1]) && isxdigit(in_ptr[2])
              && isxdigit(in_ptr[3]) && isxdigit(in_ptr[4])) {
            char utf8_buf[4];
            int bytes = unicode_escape_to_utf8(in_ptr + 1, utf8_buf);
            if (bytes > 0) {
              MEMCPY(out_ptr, utf8_buf, bytes);
              out_ptr += bytes;
              in_ptr += 4;
            }
          }
          break;
        }
        case '"':
          *out_ptr++ = '"';
          break;
        case '\\':
          *out_ptr++ = '\\';
          break;
        case '/':
          *out_ptr++ = '/';
          break;
        case 'b':
          *out_ptr++ = '\b';
          break;
        case 'f':
          *out_ptr++ = '\f';
          break;
        case 'n':
          *out_ptr++ = '\n';
          break;
        case 'r':
          *out_ptr++ = '\r';
          break;
        case 't':
          *out_ptr++ = '\t';
          break;
        default:
          *out_ptr++ = *in_ptr;
          break;
        }
      } else {
        *out_ptr++ = *in_ptr;
      }
    }
    *out_ptr = '\0';
    output.assign_ptr(buf, out_ptr - buf);
  }

  return ret;
}

int ObLicenseMgr::copy_string(ObIAllocator &allocator, const ObString &from, ObString &to, bool need_escape)
{
  int ret = OB_SUCCESS;
  char *string = nullptr;
  int len = from.length();

  if (len == 0) {
    to = "\0";
  } else if (OB_ISNULL(string = (char *)allocator.alloc(len + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(len));
  } else if (need_escape) {
    if (OB_FAIL(unescape_json_string(allocator, from, to))) {
      LOG_WARN("failed to unescape json string", KR(ret), K(from), K(need_escape));
    }
  } else {
    MEMCPY(string, from.ptr(), len);
    to.assign_ptr(string, len);
    string[len] = '\0';
  }

  return ret;
}

int ObLicenseMgr::array_to_string(ObIAllocator &allocator, const json::Array &json_array, ObString &to) {
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int buf_len = 1024 * 4;
  int pos = 0;
  const json::Value *value = json_array.get_first();

  if (OB_ISNULL(buf = (char *)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(buf_len));
  } else {
    while (value != json_array.get_header()) {
      if (OB_UNLIKELY(value->get_type() != json::JT_STRING)) {
        ret = OB_INVALID_LICENSE;
        LOG_WARN("field should contain string", KR(ret));
        break;
      } else if (OB_UNLIKELY(pos + value->get_string().length() > buf_len - 1)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buffer not enough", KR(ret), K(buf_len), K(pos));
        break;
      }
      MEMCPY(buf + pos, value->get_string().ptr(), value->get_string().length());
      pos += value->get_string().length();
      value = value->get_next();
      if (value != json_array.get_header()) {
        buf[pos] = ',';
        pos = pos + 1;
      }
    }
    buf[pos] = '\0';
  }

  if (OB_SUCC(ret)) {
    to.assign_ptr(buf, pos);
  } else if (buf) {
    allocator.free(buf);
  }

  return ret;
}

int ObLicenseMgr::gen_trail_license(ObLicense *&license)
{
  int ret = OB_SUCCESS;
  license = nullptr;
  char *version_str = nullptr;
  const int version_str_max_len = 64;

  if (OB_ISNULL(license = OB_NEW(ObLicense, ObMemAttr(OB_SYS_TENANT_ID, "license")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret));
  } else {
    license->activation_time_ = ObSysTime::now().toMicroSeconds();
    license->issuance_date_ = ObSysTime::now().toMicroSeconds();
    license->expiration_time_ = license->activation_time_ + TRIAL_VALIDITY_PERIOD;
    license->license_type_ = TRIAL_LICENSE_TYPE;
    license->core_num_ = TRIAL_CORE_NUM;
    license->node_num_ = TRIAL_NODE_NUM;

    license->product_type_ = TRIAL_PRODUCT_TYPE;
    license->end_user_ = TRIAL_END_USER;
    license->license_id_ = TRIAL_LICENSE_ID;
#ifdef NDEBUG
    license->license_code_ = TRIAL_LICENSE_CODE;
#else
    license->license_code_ = DEBUG_TRIAL_LICENSE_CODE;
#endif
    license->options_ = TRIAL_OPTIONS;
    license->allow_olap_ = true;
    license->allow_stand_by_ = true;
    license->allow_multi_tenant_ = true;
  }

  LOG_INFO("gen trail license", KR(ret), KPC(license));

  if (OB_FAIL(ret) && OB_NOT_NULL(license)) {
    OB_DELETE(ObLicense, ObMemAttr(OB_SYS_TENANT_ID, "license"), license);
  }
  
  return ret;
}

int ObLicenseMgr::load_from_inner_table(ObLicense *&license) {
  int ret = OB_SUCCESS;
  license = nullptr;
  ObMySQLProxy *proxy = GCTX.sql_proxy_;
  sqlclient::ObMySQLResult *mysql_res = nullptr;
  ObString string_result;
  int64_t int_result;
  
  SMART_VAR(ObISQLClient::ReadResult, sql_res) {
    if (OB_ISNULL(proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy should not be null", KR(ret));
    } else if (OB_ISNULL(license = OB_NEW(ObLicense, ObMemAttr(OB_SYS_TENANT_ID, "license")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", KR(ret));
    } else if (OB_FAIL(proxy->read(sql_res, OB_SYS_TENANT_ID, SELECT_INNER_SQL))) {
      LOG_WARN("failed to read from inner table __all_license", KR(ret));
    } else if (OB_ISNULL(mysql_res = sql_res.mysql_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql result should not be null", KR(ret));
    } else if (OB_FAIL(mysql_res->next())) {
      LOG_WARN("failed to get next row", KR(ret));
    } else if (OB_FAIL(mysql_res->get_varchar((int64_t)0, string_result))) {
      LOG_WARN("failed to get varchar", KR(ret));
    } else if (OB_FAIL(copy_string(license->allocator_, string_result, license->license_id_))) {
      LOG_WARN("failed to copy string", KR(ret));
    } else if (OB_FAIL(mysql_res->get_varchar((int64_t)1, string_result))) {
      LOG_WARN("failed to get varchar", KR(ret));
    } else if (OB_FAIL(copy_string(license->allocator_, string_result, license->license_code_))) {
      LOG_WARN("failed to copy string", KR(ret));
    } else if (OB_FAIL(mysql_res->get_varchar((int64_t)2, string_result))) {
      LOG_WARN("failed to get varchar", KR(ret));
    } else if (OB_FAIL(copy_string(license->allocator_, string_result, license->license_type_))) {
      LOG_WARN("failed to copy string", KR(ret));
    } else if (OB_FAIL(mysql_res->get_varchar((int64_t)3, string_result))) {
      LOG_WARN("failed to get varchar", KR(ret));
    } else if (OB_FAIL(copy_string(license->allocator_, string_result, license->product_type_))) {
      LOG_WARN("failed to copy string", KR(ret));
    } else if (OB_FAIL(mysql_res->get_timestamp((int64_t)4, nullptr, int_result))) {
      LOG_WARN("failed to get timestamp", KR(ret));
    } else if (FALSE_IT(license->issuance_date_ = int_result)) {
    } else if (OB_FAIL(mysql_res->get_timestamp((int64_t)5, nullptr, int_result))) {
      LOG_WARN("failed to get timestamp", KR(ret));
    } else if (FALSE_IT(license->activation_time_ = int_result)) {
    } else if (OB_FAIL(mysql_res->get_timestamp((int64_t)6, nullptr, int_result))) {
      LOG_WARN("failed to get timestamp", KR(ret));
    } else if (FALSE_IT(license->expiration_time_ = int_result)) {
    } else if (OB_FAIL(mysql_res->get_varchar((int64_t)7, string_result))) {
      LOG_WARN("failed to get varchar", KR(ret));
    } else if (OB_FAIL(copy_string(license->allocator_, string_result, license->options_))) {
      LOG_WARN("failed to copy string", KR(ret));
    } else if (OB_FAIL(mysql_res->get_int((int64_t)8, license->core_num_))) {
      LOG_WARN("failed to get int", KR(ret));
    } else if (OB_FAIL(mysql_res->get_int((int64_t)9, license->node_num_))) {
      LOG_WARN("failed to get int", KR(ret));
    } else if (OB_FAIL(mysql_res->get_varchar((int64_t)10, string_result))) {
      LOG_WARN("failed to get varchar", KR(ret));
    } else if (OB_FAIL(copy_string(license->allocator_, string_result, license->end_user_))) {
      LOG_WARN("failed to copy string", KR(ret));
    } else if (OB_FAIL(mysql_res->get_varchar((int64_t)11, string_result))) {
      LOG_WARN("failed to get varchar", KR(ret));
    } else if (FALSE_IT(MEMCPY(cluster_ulid_, string_result.ptr(), min(string_result.length(), LICENSE_ULID_LEN)))) {
    } else if (OB_FAIL(parse_options(*license))) {
      LOG_WARN("failed to parse options", KR(ret));
    } else if (OB_FAIL(mysql_res->close())) {
      LOG_WARN("failed to close mysql result", KR(ret));
    } else {
      LOG_INFO("successfully load from inner table", KR(ret), KPC(license));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(license)) {
    OB_DELETE(ObLicense, ObMemAttr(OB_SYS_TENANT_ID, "license"), license);
  }

  return ret;
}

int ObLicenseMgr::count_current_tenant(int64_t &tenant_num) {
  int ret = OB_SUCCESS;
  ObMySQLProxy *proxy = GCTX.sql_proxy_;
  sqlclient::ObMySQLResult *mysql_res = nullptr;
  
  SMART_VAR(ObISQLClient::ReadResult, sql_res) {
    if (OB_ISNULL(proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy should not be null", KR(ret));
    } else if (OB_FAIL(proxy->read(sql_res, OB_SYS_TENANT_ID, COUNT_TENANT_SQL))) {
      LOG_WARN("failed to read from inner table __all_license", KR(ret), K(COUNT_TENANT_SQL));
    } else if (OB_ISNULL(mysql_res = sql_res.mysql_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql result should not be null", KR(ret));
    } else if (OB_FAIL(mysql_res->next())) {
      LOG_WARN("failed to get next row", KR(ret));
    } else if (OB_FAIL(mysql_res->get_int((int64_t)0, tenant_num))) {
      LOG_WARN("failed to get int", KR(ret));
    } else if (OB_FAIL(mysql_res->close())) {
      LOG_WARN("failed to close mysql result", KR(ret));
    }
  }

  return ret;
}

int ObLicenseMgr::count_standby_tenant(int64_t &tenant_num) {
  int ret = OB_SUCCESS;
  ObMySQLProxy *proxy = GCTX.sql_proxy_;
  sqlclient::ObMySQLResult *mysql_res = nullptr;
  
  SMART_VAR(ObISQLClient::ReadResult, sql_res) {
    if (OB_ISNULL(proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy should not be null", KR(ret));
    } else if (OB_FAIL(proxy->read(sql_res, OB_SYS_TENANT_ID, COUNT_STANDBY_SQL))) {
      LOG_WARN("failed to read from virtual table __ALL_VIRTUAL_TENANT_INFO", KR(ret), K(COUNT_STANDBY_SQL));
    } else if (OB_ISNULL(mysql_res = sql_res.mysql_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql result should not be null", KR(ret));
    } else if (OB_FAIL(mysql_res->next())) {
      LOG_WARN("failed to get next row", KR(ret));
    } else if (OB_FAIL(mysql_res->get_int((int64_t)0, tenant_num))) {
      LOG_WARN("failed to get int", KR(ret));
    } else if (OB_FAIL(mysql_res->close())) {
      LOG_WARN("failed to close mysql result", KR(ret));
    }
  }

  return ret;
}

int ObLicenseMgr::count_server_node_num(int64_t &server_num) {
  int ret = OB_SUCCESS;
  ObMySQLProxy *proxy = GCTX.sql_proxy_;
  sqlclient::ObMySQLResult *mysql_res = nullptr;
  
  SMART_VAR(ObISQLClient::ReadResult, sql_res) {
    if (OB_ISNULL(proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy should not be null", KR(ret));
    } else if (OB_FAIL(proxy->read(sql_res, OB_SYS_TENANT_ID, COUNT_SERVER_SQL))) {
      LOG_WARN("failed to read from virtual table __all_server", KR(ret), K(COUNT_SERVER_SQL));
    } else if (OB_ISNULL(mysql_res = sql_res.mysql_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql result should not be null", KR(ret));
    } else if (OB_FAIL(mysql_res->next())) {
      LOG_WARN("failed to get next row", KR(ret));
    } else if (OB_FAIL(mysql_res->get_int((int64_t)0, server_num))) {
      LOG_WARN("failed to get int", KR(ret));
    } else if (OB_FAIL(mysql_res->close())) {
      LOG_WARN("failed to close mysql result", KR(ret));
    }
  }

  return ret;
}

int ObLicenseMgr::store_to_inner_table(ObLicense &license, bool new_row) {
  int ret = OB_SUCCESS;
  ObMySQLProxy *proxy = GCTX.sql_proxy_;
  char *sql_buf = nullptr;
  const int sql_buf_len = 8 * 1024;
  int p_ret = 0;
  ObArenaAllocator sql_buf_allocator;
  int64_t affected_row = 0;
  const char *sql_fmt = nullptr;

  if (new_row) {
    sql_fmt = INSERT_INNER_SQL;
  } else {
    sql_fmt = UPDATE_INNER_SQL;
  }

  if (OB_ISNULL(sql_buf = (char *) sql_buf_allocator.alloc(sql_buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(sql_buf_len));
  } else if ((p_ret = snprintf(sql_buf,
                               sql_buf_len,
                               sql_fmt,
                               license.end_user_.ptr(),
                               license.license_id_.ptr(),
                               license.license_code_.ptr(),
                               license.license_type_.ptr(),
                               license.product_type_.ptr(),
                               license.issuance_date_,
                               license.activation_time_,
                               license.expiration_time_,
                               license.options_.ptr(),
                               license.core_num_,
                               license.node_num_,
                               cluster_ulid_)) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to format sql STRING", KR(ret), K(sql_fmt), K(p_ret), K(license), K(new_row));
  } else if (OB_UNLIKELY(p_ret >= sql_buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("sql buf is not enough", KR(ret), K(sql_buf_len), K(p_ret), K(sql_fmt));
  } else if (OB_FAIL(proxy->write(OB_SYS_TENANT_ID, sql_buf, affected_row))) {
    LOG_WARN("failed to write to inner table __all_license", KR(ret));
  }

  LOG_INFO("store license to inner table", KR(ret), K(sql_buf), K(affected_row), K(new_row), KP(proxy), K(p_ret));

  return ret;
}

int ObLicenseMgr::check_expired(ObLicense *license, int64_t &remain_time) {
  int ret = OB_SUCCESS;
  int64_t current_time = 0;

  if (OB_UNLIKELY(!is_start_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("license mgr not start", KR(ret));
  } else if (OB_ISNULL(license)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("license should not be null", KR(ret));
  } else if (OB_FAIL(timestamp_service_.get_time(current_time))) {
    LOG_WARN("failed to get time", KR(ret));
  } else if (OB_UNLIKELY(current_time > license->expiration_time_)) {
    ret = OB_LICENSE_EXPIRED;
    LOG_WARN("license is expired", KR(ret), K(current_time), KPC(license));
  } else {
    remain_time = max((int64_t) 0, (int64_t) license->expiration_time_ - current_time);
  } 

  return ret;
}

ObLicenseMgr &ObLicenseMgr::get_instance()
{  
  return instance;
}



} // namespace share
} // namespace oceanbase
