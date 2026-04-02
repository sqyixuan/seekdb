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

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner.h"
#include "ob_log_miner_args.h"
#include "ob_log_miner_logger.h"
#include "lib/oblog/ob_log.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::oblogminer;
using namespace oceanbase::common;
using namespace oceanbase::share;

int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  ObLogMinerArgs args;
  ObLogMiner logminer_instance;
  ObTimerService *timer_service = nullptr;
  // set OB_SERVER_TENANT_ID as oblogminer's MTL_ID.
  // if not set, `ob_malloc` maybe generate error logs
  // due to `OB_INVALID_TENANT_ID`.
  ObTenantBase tenant_ctx(OB_SERVER_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  timer_service = OB_NEW(ObTimerService, "TimerService", OB_SERVER_TENANT_ID);
  if (OB_ISNULL(timer_service)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed");
  } else if (OB_FAIL(timer_service->start())) {
    LOG_WARN("start timer service fail", K(ret));
  } else {
    tenant_ctx.set(timer_service);
    OB_LOGGER.set_file_name(ObLogMinerArgs::LOGMINER_LOG_FILE, true, false);
    OB_LOGGER.set_log_level("WDIAG");
    OB_LOGGER.set_enable_async_log(false);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(args.init(argc, argv))) {
    LOG_ERROR("logminer get invalid arguments", K(argc), K(args));
    LOGMINER_STDOUT("logminer get invalid arguments, please check log[%s] for more detail\n",
        ObLogMinerArgs::LOGMINER_LOG_FILE);
  } else if (args.print_usage_) {
    ObLogMinerCmdArgs::print_usage(argv[0]);
  } else if (OB_FAIL(logminer_instance.init(args))) {
    LOG_ERROR("logminer instance init failed", K(args));
    LOGMINER_STDOUT("logminer init failed, please check log[%s] for more detail\n",
        ObLogMinerArgs::LOGMINER_LOG_FILE);
  } else {
    logminer_instance.run();
    logminer_instance.destroy();

    timer_service->stop();
    timer_service->wait();
    timer_service->destroy();
    ob_delete(timer_service);
  }

  return ret;
}
