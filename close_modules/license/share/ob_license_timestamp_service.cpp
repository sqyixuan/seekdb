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

#include "ob_license_timestamp_service.h"

#include "deps/easy/src/util/easy_time.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "lib/time/Time.h"
#include "observer/ob_server_struct.h"
#include "lib/allocator/page_arena.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "inttypes.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace obutil;
namespace share
{

const char *LTS_UPDATE_INNER_SQL = "UPDATE __all_license SET LTS_TIME = FROM_UNIXTIME(CAST(%ld as DECIMAL(30, 6)) / 1000000)";
const char *LTS_SELECT_INNER_SQL = "SELECT LTS_TIME FROM __all_license";
const int64_t VALID_TIME_THRESHOLD = 60 * 1000 * 1000;
int64_t ObLicenseTimestampService::UPDATE_TIME_DURATION = 60 * 1000 * 1000;
#ifndef NDEBUG
ERRSIM_POINT_DEF(LICENSE_TIMESTAMP_SERVICE_CURRENT_SYS_TIME_SECOND);
#endif
int64_t ObLicenseTimestampService::get_current_sys_time() {
  int64_t result = 0;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(modified_sys_time_)) {
    result = modified_sys_time_;
  }
#ifndef NDEBUG
  else if (OB_TMP_FAIL(LICENSE_TIMESTAMP_SERVICE_CURRENT_SYS_TIME_SECOND)) {
    result = static_cast<int64_t>(abs(tmp_ret)) * 1000 * 1000;
    LOG_INFO("sys time has been modified by errsim point", K(LICENSE_TIMESTAMP_SERVICE_CURRENT_SYS_TIME_SECOND), K(result));
  }
#endif
  else {
    result = ObSysTime::now().toMicroSeconds();
  }
  return result;
}

int ObLicenseTimestampService::is_master_node(bool &is_master) {
  ObLSHandle ls_handle;
  common::ObRole role = INVALID_ROLE;
  int64_t proposal_id = 0;
  int ret = OB_SUCCESS;
  MTL_SWITCH (OB_SYS_TENANT_ID) {
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ObLSID(ObLSID::SYS_LS_ID), ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("failed to get sys ls", KR(ret));
    } else if (OB_FAIL(ls_handle.get_ls()->get_log_handler()->get_role(role, proposal_id))) {
      LOG_WARN("failed to get role", KR(ret));
    } else if (OB_LIKELY(LEADER == role)) {
      is_master = true;
    } else {
      is_master = false;
    }
  }
  return ret;
}

int ObLicenseTimestampService::start_from_inner_table()
{
  int ret = OB_SUCCESS;

  int64_t inner_time = 0;
  int64_t current_sys_time = get_current_sys_time();
  last_tsc_ = rdtsc();
  cpu_mhz_ = get_cpufreq_khz() / 1000;

  if (OB_FAIL(load_from_inner_table(inner_time))) {
    LOG_WARN("failed to load from inner table", KR(ret));
  } else {
    if (current_sys_time < inner_time) {
      LOG_WARN("system clock has rolled back, license may be expire earlier", K(current_sys_time), K(inner_time));
      current_time_ = inner_time;
    } else {
      current_time_ = current_sys_time;
    }
    is_start_ = true;
    LOG_INFO("successfully start license timestamp service", KR(ret), K(*this));
  }

  return ret;
}
ERRSIM_POINT_DEF(LICENSE_TIMESTAMP_SERVICE_FORCE_UPDATE_TIME);
int ObLicenseTimestampService::update_time()
{
  int ret = OB_SUCCESS;
  bool is_master = false;
  int64_t current_time = 0;

  if (OB_UNLIKELY(!is_start_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not is_start", K(ret), K(is_start_), K(current_time_));
  } else if (OB_UNLIKELY(is_unittest_)) {
    is_master = true;
  } else if (OB_FAIL(is_master_node(is_master))) {
    LOG_WARN("failed to check is master node", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (is_master) {
    {
      ObSpinLockGuard lock_guard(lock_);
      int64_t current_tsc = rdtsc();
      int64_t elapsed_time_tsc = (current_tsc - last_tsc_) / cpu_mhz_;
      int64_t current_time_tsc = current_time_ + elapsed_time_tsc;
      int64_t current_time_sys = get_current_sys_time();
      int64_t min_valid_time = current_time_tsc - VALID_TIME_THRESHOLD;
      int64_t max_valid_time = current_time_tsc + VALID_TIME_THRESHOLD;
      
      if (OB_UNLIKELY(current_tsc < last_tsc_)) {
        last_tsc_ = current_tsc;
        LOG_WARN("tsc timestamp has rolled back", K(current_tsc), K(last_tsc_));
      } else if (elapsed_time_tsc < UPDATE_TIME_DURATION && OB_LIKELY(LICENSE_TIMESTAMP_SERVICE_FORCE_UPDATE_TIME == OB_SUCCESS)) {
        // time has been updated by other thread
        LOG_TRACE("time has been updated by other thread", K(elapsed_time_tsc), K(UPDATE_TIME_DURATION));
      } else if (OB_UNLIKELY(current_time_sys < min_valid_time || current_time_sys > max_valid_time)) {
        LOG_WARN("system time is invalidate, license use tsc as time", K(current_time_), K(elapsed_time_tsc), K(current_time_sys), K(VALID_TIME_THRESHOLD));
        current_time = current_time_tsc;
      } else {
        current_time = current_time_sys;
      }
      if (OB_SUCC(ret) && OB_LIKELY(current_time != 0)) {
        last_tsc_ = current_tsc;
        current_time_ = current_time;
      }
    }
    if (OB_FAIL(ret) || OB_UNLIKELY(current_time == 0)) {
    } else if (!is_unittest_ && OB_FAIL(store_to_inner_table(current_time))) {
      LOG_WARN("failed to store license to inner table", KR(ret));
    }
  } else {
    if (OB_FAIL(load_from_inner_table(current_time))){
      LOG_WARN("failed to load from inner table", KR(ret));
    } else {
      last_tsc_ = rdtsc();
      current_time_ = current_time;
    }
  }

  if (current_time != 0) {
    LOG_TRACE("update time done", KR(ret), K(current_time));
  }

  return ret;
}
#ifndef NDEBUG
ERRSIM_POINT_DEF(LICENSE_TIMESTAMP_SERVICE_CURRENT_TIME_SECOND);
#endif
int ObLicenseTimestampService::get_time(int64_t &time) {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t current_tsc = rdtsc();
  int64_t elapsed_time_tsc = (current_tsc - last_tsc_) / get_cpufreq_khz() * 1000;

#ifndef NDEBUG
  if (OB_TMP_FAIL(LICENSE_TIMESTAMP_SERVICE_CURRENT_TIME_SECOND)) {
    current_time_ = static_cast<int64_t>(abs(tmp_ret)) * 1000 * 1000;
    LOG_INFO("time has been modified by errsim point", K(current_time_));
  }
#endif

  if (OB_UNLIKELY(!is_start_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not is_start", K(ret), K(is_start_), K(current_time_));
  } else if (elapsed_time_tsc > UPDATE_TIME_DURATION || OB_TMP_FAIL(LICENSE_TIMESTAMP_SERVICE_FORCE_UPDATE_TIME)) {
    OZ(update_time());
  }
  time = current_time_;

  LOG_TRACE("get time", KR(ret), KTIME(time), K(elapsed_time_tsc));

  return ret;
}

int ObLicenseTimestampService::start_with_time(int64_t time) {
  int ret = OB_SUCCESS;

  if (!is_unittest_ && OB_FAIL(store_to_inner_table(time))) {
    LOG_WARN("failed to store license to inner table", KR(ret));
  } else {
    last_tsc_ = rdtsc();
    current_time_ = time;
    is_start_ = true;
    cpu_mhz_ = get_cpufreq_khz() / 1000;
  }

  return ret;
}

int ObLicenseTimestampService::store_to_inner_table(int64_t time) {
  int ret = OB_SUCCESS;
  ObMySQLProxy *proxy = GCTX.sql_proxy_;
  char *sql_buf = nullptr;
  const int sql_buf_len = 1024;
  int p_ret = 0;
  ObArenaAllocator sql_buf_allocator;
  int64_t affected_row = 0;

  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (OB_ISNULL(sql_buf = (char *) sql_buf_allocator.alloc(sql_buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(sql_buf_len));
  } else if ((p_ret = snprintf(sql_buf,
                               sql_buf_len,
                               LTS_UPDATE_INNER_SQL,
                               time)) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to format sql string", KR(ret), K(LTS_UPDATE_INNER_SQL), K(p_ret), K(time));
  } else if (OB_UNLIKELY(p_ret >= sql_buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("sql buf is not enough", KR(ret), K(sql_buf_len), K(p_ret), K(LTS_UPDATE_INNER_SQL));
  } else if (OB_FAIL(proxy->write(OB_SYS_TENANT_ID, sql_buf, affected_row))) {
    LOG_WARN("failed to update time to inner table __all_license", KR(ret));
  }

  LOG_TRACE("store timestamp to inner table done", KR(ret), K(sql_buf), KP(proxy), K(p_ret));

  return ret;
}

int ObLicenseTimestampService::load_from_inner_table(int64_t &time) {
  int ret = OB_SUCCESS;
  ObMySQLProxy *proxy = GCTX.sql_proxy_;
  sqlclient::ObMySQLResult *mysql_res = nullptr;

  SMART_VAR(ObISQLClient::ReadResult, sql_res) {
    if (OB_ISNULL(proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy should not be null", KR(ret));
    } else if (OB_FAIL(proxy->read(sql_res, OB_SYS_TENANT_ID, LTS_SELECT_INNER_SQL))) {
      LOG_WARN("failed to read from inner table __all_license", KR(ret));
    } else if (OB_ISNULL(mysql_res = sql_res.mysql_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mysql result should not be null", KR(ret));
    } else if (OB_FAIL(mysql_res->next())) {
      LOG_WARN("failed to get next row", KR(ret));
    } else if (OB_FAIL(mysql_res->get_timestamp((int64_t)0, nullptr, time))) {
      LOG_WARN("failed to get varchar", KR(ret));
    } else if (OB_FAIL(mysql_res->close())) {
      LOG_WARN("failed to close mysql result", KR(ret));
    }
  }

  LOG_INFO("load from system table done", KR(ret), KTIME(time), KP(proxy));

  return ret;
}

} // namespace share
} // namespace oceanbase
