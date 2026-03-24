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

#include "ob_arb_server_timer.h"
#include "logservice/arbserver/palf_env_lite_mgr.h"             // PalfEvnLiteMgr

namespace oceanbase
{
namespace arbserver
{
ObArbServerTimer::ObArbServerTimer() :
  tg_id_(-1),
  palf_env_lite_mgr_(NULL),
  timer_interval_(-1),
  is_inited_(false)
{}

ObArbServerTimer::~ObArbServerTimer()
{
  destroy();
}

int ObArbServerTimer::init(const int tg_id, palflite::PalfEnvLiteMgr *palf_env_lite_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObArbServerTimer has inited twice", K(ret), KPC(this));
  } else {
    tg_id_ = tg_id;
    palf_env_lite_mgr_ = palf_env_lite_mgr;
    timer_interval_ = SCAN_TIMER_INTERVAL;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObArbServerTimer init success", KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int ObArbServerTimer::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TG_START(tg_id_))) {
    CLOG_LOG(WARN, "ObArbServerTimer TG_START failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, timer_interval_, true))) {
    CLOG_LOG(WARN, "ObArbServerTimer TG_START failed", K(ret));
  } else {
    CLOG_LOG(INFO, "ObArbServerTimer start success", KPC(this));
  }
  return ret;
}

int ObArbServerTimer::stop()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    TG_STOP(tg_id_);
    CLOG_LOG(INFO, "ObArbServerTimer stop success", KPC(this));
  }
  return ret;
}

void ObArbServerTimer::wait()
{
  if (IS_INIT) {
    TG_WAIT(tg_id_);
    CLOG_LOG(INFO, "ObArbServerTimer wait success", KPC(this));
  }
}

void ObArbServerTimer::destroy()
{
  CLOG_LOG_RET(WARN, OB_SUCCESS, "ObArbServerTimer destroy success", KPC(this));
  stop();
  wait();
  is_inited_ = false;
  palf_env_lite_mgr_ = NULL;
}

void ObArbServerTimer::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(print_tenant_memory_usage_())) {
    CLOG_LOG(WARN, "PalfEvnLiteMgr for_each failed", KR(ret), KPC(this));
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  if (cost_ts >= 100 * 1000) {
    CLOG_LOG(WARN, "ObArbServerTimer runTimerTask cost too much", KPC(this), K(cost_ts));
  }
  CLOG_LOG(TRACE, "ObArbServerTimer runTimerTask success", KPC(this), K(cost_ts));
}

int ObArbServerTimer::print_tenant_memory_usage_()
{
  int ret = OB_SUCCESS;
  lib::ObMallocAllocator *mallocator = lib::ObMallocAllocator::get_instance();
  int64_t kv_cache_mem = 0;
  uint64_t tenant_id = OB_SERVER_TENANT_ID;
  static const int64_t BUF_LEN = 4LL << 10;
  char print_buf[BUF_LEN] = "";
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(databuff_printf(print_buf, BUF_LEN, pos,
                                "=== TENANTS MEMORY INFO ===\n"))) {
      CLOG_LOG(WARN, "print failed", K(ret));
  } else if (OB_FAIL(databuff_printf(print_buf, BUF_LEN, pos,
                            "[TENANT_MEMORY] "
                            "tenant_id=% '9ld "
                            "mem_tenant_limit=% '15ld "
                            "mem_tenant_hold=% '15ld ",
                            tenant_id,
                            lib::get_tenant_memory_limit(tenant_id),
                            lib::get_tenant_memory_hold(tenant_id)))) {
  } else if (OB_ISNULL(mallocator)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ObMallocAllocator is NULL", K(ret), KP(mallocator));
  } else {
    mallocator->print_tenant_memory_usage(tenant_id);
    mallocator->print_tenant_ctx_memory_usage(tenant_id);

    if (OB_SIZE_OVERFLOW == ret) {
      // If the buffer is not enough, truncate directly
      ret = OB_SUCCESS;
      print_buf[BUF_LEN - 2] = '\n';
      print_buf[BUF_LEN - 1] = '\0';
    }
    _CLOG_LOG(INFO, "====== tenants memory info ======\n%s", print_buf);

    // print global chunk freelist
    int64_t pos = CHUNK_MGR.to_string(print_buf, BUF_LEN);
    _CLOG_LOG(INFO, "%.*s", static_cast<int>(pos), print_buf);
  }
  return ret;
}
}
}
