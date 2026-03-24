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
#include "share/ob_license_utils.h"
#include "share/ob_license_mgr.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_license_utils.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace share
{
using namespace common;

const char *LOGIN_WARNING_FMT = "License will be expired in %d days";
const char *LOGIN_EXPIRED_MSG = "License has been expired";

bool is_license_expired(ObLicenseMgr *license_mgr, ObLicense *license) {
  int ret = OB_SUCCESS;
  int64_t remain_time = 0;
  bool is_expired = false;

  if (OB_ISNULL(license_mgr)) {
    LOG_WARN("license mgr is null", KP(license_mgr));
  } else if (license->license_trail_ && OB_FAIL(license_mgr->check_expired(license, remain_time))) {
    if (ret == OB_LICENSE_EXPIRED) {
      is_expired = true;
    } else {
      LOG_WARN("check expired failed", KR(ret));
    }
  } else if (!license->license_trail_ && license_mgr->is_boot_expired()) {
    is_expired = true;
  }

  return is_expired;
}

int get_license_mgr(ObLicenseMgr *&license_mgr, ObLicenseGuard &guard) {
  int ret = OB_SUCCESS;
  license_mgr = &ObLicenseMgr::get_instance();

  if (OB_UNLIKELY(!license_mgr->is_start())) {
    ret = OB_NOT_INIT;
    LOG_TRACE("license mgr is not start", KR(ret));
  } else if (OB_FAIL(license_mgr->get_license(guard))) {
    LOG_WARN("get license failed", KR(ret)); 
  }

  return ret;
}

int ObLicenseUtils::check_dml_allowed()
{
  ObLicenseMgr *license_mgr = nullptr;
  ObLicenseGuard license_guard;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_TMP_FAIL(get_license_mgr(license_mgr, license_guard))) {
    // do nothing
  } else if (OB_UNLIKELY(is_license_expired(license_mgr, license_guard.get()))) {
    ret = OB_LICENSE_EXPIRED;
    LOG_WARN("license is expired", KR(ret));
  }

  return ret;
}

int ObLicenseUtils::get_login_message(char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int min_buf_len = max(strlen(LOGIN_EXPIRED_MSG), strlen(LOGIN_WARNING_FMT)) + 1;
  int64_t remain_time = 0;
  ObLicenseMgr *license_mgr = nullptr;
  ObLicenseGuard license_guard;

  if (buf_len <= min_buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer is too short", KR(ret), K(buf_len));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is null", KR(ret), K(buf_len));
  } else if (OB_FAIL(get_license_mgr(license_mgr, license_guard))) {
    if (ret == OB_NOT_INIT) {
      LOG_TRACE("license mgr has not been inited", KR(ret));
      ret = OB_SUCCESS;
      buf[0] = '\0';
    } else {
      LOG_WARN("fail to get license mgr", KR(ret));
    }
  } else if (OB_FAIL(license_mgr->check_expired(license_guard.get(), remain_time))) {
    if (ret == OB_LICENSE_EXPIRED) {
      LOG_WARN("license has been expired", KR(ret), K(remain_time));
      MEMCPY(buf, LOGIN_EXPIRED_MSG, strlen(LOGIN_EXPIRED_MSG) + 1);
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check license expired", KR(ret));
    }
  } else if (remain_time < (int64_t)30 * 24 * 60 * 60 * 1000 * 1000) {
    int p_ret = snprintf(buf,
                         buf_len,
                         LOGIN_WARNING_FMT,
                         remain_time / ((int64_t)24 * 60 * 60 * 1000 * 1000) + 1);
    if (p_ret < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to do snprintf", KR(ret), K(p_ret), K(buf_len));
    } else if (p_ret >= buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf is not enough", KR(ret), K(p_ret), K(buf_len));
    }
  } else {
    buf[0] = '\0';
  }

  LOG_TRACE("got login message", KR(ret), K(buf), K(buf_len));
  return ret;
}

int ObLicenseUtils::check_add_server_allowed(int64_t add_num, obrpc::ObAdminServerArg::AdminServerOp arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t current_server_num = 0;
  ObLicenseMgr *license_mgr = nullptr;
  ObLicenseGuard license_guard;

  if (arg != obrpc::ObAdminServerArg::ADD) {
    // do nothing
  } else if (OB_TMP_FAIL(get_license_mgr(license_mgr, license_guard))) {
    // do nothing
  } else if (is_license_expired(license_mgr, license_guard.get())) {
    ret = OB_LICENSE_EXPIRED;
    LOG_WARN("license is expired", KR(ret));
  } else if (OB_TMP_FAIL(license_mgr->count_server_node_num(current_server_num))) {
    LOG_WARN("fail to count server node num", KR(tmp_ret));
  } else if (OB_UNLIKELY(current_server_num + add_num > license_guard.get()->node_num_)) {
    ret = OB_LICENSE_SCOPE_EXCEEDED;
    LOG_WARN("server node num is exceeded", KR(ret), K(current_server_num), K(add_num), K(license_guard.get()->node_num_));
    LOG_USER_ERROR(OB_LICENSE_SCOPE_EXCEEDED, "Node Num is exceeded");
  }

  return ret;
}

int ObLicenseUtils::check_olap_allowed(const int64_t target_tenant_id)
{
  int ret = OB_SUCCESS;

  if (is_user_tenant(target_tenant_id)) {
    int tmp_ret = OB_SUCCESS;
    ObLicenseMgr *license_mgr = nullptr;
    ObLicenseGuard license_guard;
    if (OB_TMP_FAIL(get_license_mgr(license_mgr, license_guard))) {
      // do nothing
    } else if (is_license_expired(license_mgr, license_guard.get())) {
      ret = OB_LICENSE_EXPIRED;
      LOG_WARN("license is expired", KR(ret));
    } else if (OB_UNLIKELY(!license_guard.get()->allow_olap_)) {
      ret = OB_LICENSE_SCOPE_EXCEEDED;
      LOG_WARN("current license does not allow olap", KPC(license_mgr));
    }
  }
  return ret;
}

int ObLicenseUtils::check_add_tenant_allowed(int current_user_tenant_num)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLicenseMgr *license_mgr = nullptr;
  ObLicenseGuard license_guard;

  if (current_user_tenant_num < 1) {
  } else if (OB_TMP_FAIL(get_license_mgr(license_mgr, license_guard))) {
    // do nothing
  } else if (is_license_expired(license_mgr, license_guard.get())) {
    ret = OB_LICENSE_EXPIRED;
    LOG_WARN("license is expired", KR(ret));
  } else if (OB_UNLIKELY(!license_guard.get()->allow_multi_tenant_)) {
    ret = OB_LICENSE_SCOPE_EXCEEDED;
    LOG_WARN("Current license does not allow to create tenant more than 1", KR(ret), K(current_user_tenant_num));
    LOG_USER_ERROR(OB_LICENSE_SCOPE_EXCEEDED, "current license does not have Multitenant option");
  } 

  return ret;
}

int ObLicenseUtils::check_standby_allowed()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLicenseMgr *license_mgr = nullptr;
  ObLicenseGuard license_guard;

  if (OB_TMP_FAIL(get_license_mgr(license_mgr, license_guard))) {
    // do nothing
  } else if (is_license_expired(license_mgr, license_guard.get())) {
    ret = OB_LICENSE_EXPIRED;
    LOG_WARN("license is expired", KR(ret));
  } else if (OB_UNLIKELY(!license_guard.get()->allow_stand_by_)) {
    ret = OB_LICENSE_SCOPE_EXCEEDED;
    LOG_WARN("current license does not allow to create standby tenant", K(license_guard));
    LOG_USER_ERROR(OB_LICENSE_SCOPE_EXCEEDED, "current license does not have Primary-Standby option");
  }
  
  return ret;
}

int ObLicenseUtils::check_create_table_allowed(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t remain_time = 0;
  ObLicenseMgr *license_mgr = nullptr;
  ObLicenseGuard license_guard;

  if (tenant_id == OB_SYS_TENANT_ID) {
    // do nothing
  } else if (OB_TMP_FAIL(get_license_mgr(license_mgr, license_guard))) {
    // do nothing
  } else if (OB_TMP_FAIL(license_mgr->check_expired(license_guard.get(), remain_time))) {
    if (tmp_ret == OB_LICENSE_EXPIRED) {
      ret = tmp_ret;
      LOG_WARN("license is expired", KR(ret));
    } else {
      LOG_WARN("check expired failed", KR(tmp_ret));
    }
  }
 
  return ret;
}

int ObLicenseUtils::check_for_create_tenant(int current_user_tenant_num, bool is_create_standby) {
  int ret = OB_SUCCESS;
  if (is_create_standby && OB_FAIL(ObLicenseUtils::check_standby_allowed())) {
    LOG_WARN("check standby allowed failed", KR(ret));
  } else if (OB_FAIL(check_add_tenant_allowed(current_user_tenant_num))) {
    LOG_WARN("check add tenant allowed failed", KR(ret));
  }
  return ret;
}

void ObLicenseUtils::clear_license_table_if_need() { /* do nothing*/ }

int ObLicenseUtils::start_license_mgr()
{
  int ret = OB_SUCCESS;
  ObLicenseMgr &license_mgr = ObLicenseMgr::get_instance();
  
  if (OB_FAIL(license_mgr.start())) {
    LOG_WARN("failed to start license mgr", KR(ret));
  }

  return ret;
}

int ObLicenseUtils::load_license(const ObString &file_path)
{
  int ret = OB_SUCCESS;
  ObLicenseMgr &license_mgr = ObLicenseMgr::get_instance();
  ObArenaAllocator allocator;
  char *buf = nullptr;
  const int buf_len = 64 * 1024;
  int read_len = 0;
  const char *path = file_path.ptr();
  FILE *fp = nullptr;

  if (OB_UNLIKELY(!license_mgr.is_start())) {
    ret = OB_NOT_INIT;
    LOG_WARN("license mgr is start", K(ret));
  } else if (OB_ISNULL(buf = (char *)allocator.alloc(buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_UNLIKELY(0 != access(path, F_OK))) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("file not exist", K(ret), K(path));
  } else if (OB_ISNULL(fp = fopen(path, "r"))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to open file", K(ret), K(path), K(errno), K(strerror(errno)));
  } else if (FALSE_IT(read_len = fread(buf, 1, buf_len, fp))) {
  } else if (OB_UNLIKELY(0 != ferror(fp))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to read file", K(ret), K(path), K(errno), K(strerror(errno)));
  } else if (OB_UNLIKELY(0 == feof(fp))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("file is too large", K(ret), K(path), K(buf_len));
  } else if (read_len >= buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_ERROR("fread size overflow", K(ret), K(path), K(read_len), K(buf_len));
  } else if (OB_FAIL(license_mgr.load_license(buf, buf_len))) {
    LOG_WARN("fail to load license", K(ret), K(buf), K(read_len));
  }

  if (OB_NOT_NULL(buf)) {
    allocator.free(buf);
  }
  if (OB_NOT_NULL(fp)) {
    fclose(fp);
    fp = NULL;
  }

  return ret;
}

}
}
