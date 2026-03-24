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

#ifndef OCEANBASE_ARB_SERVER_TIMER_H
#define OCEANBASE_ARB_SERVER_TIMER_H

#include "lib/task/ob_timer.h"                        // ObTimerTask
#include "lib/utility/ob_print_utils.h"                // TO_STRING_KV
#include "lib/utility/ob_macro_utils.h"               // DISABLE_COPY_ASSIGN

namespace oceanbase
{
namespace palflite
{
class PalfEnvLiteMgr;
class PalfEnvKey;
}
namespace arbserver
{
class ObArbServerTimer : public common::ObTimerTask {
public:
  ObArbServerTimer();
  ~ObArbServerTimer();
public:
  int init(const int tg_id,
           palflite::PalfEnvLiteMgr *palf_env_lite_mgr);
  int start();
  int stop();
  void wait();
  void destroy();
  void runTimerTask() final;
  TO_STRING_KV(K_(is_inited), K_(tg_id), KP(palf_env_lite_mgr_), K_(timer_interval), KP(this));
private:
  int print_tenant_memory_usage_();
  DISABLE_COPY_ASSIGN(ObArbServerTimer);
private:
  static constexpr int64_t SCAN_TIMER_INTERVAL = 10 * 1000 * 1000;
  static constexpr int64_t ARB_RELOAD_CONFIG_INTERVAL = 60 * 1000 * 1000;
  int tg_id_;
  palflite::PalfEnvLiteMgr *palf_env_lite_mgr_;
  int64_t timer_interval_;
  bool is_inited_;
};
} // end namespace arbserver
} // end namespace oceanbase
#endif
