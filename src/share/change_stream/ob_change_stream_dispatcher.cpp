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
#include "lib/oblog/ob_log_module.h"
#include "lib/thread/ob_thread_name.h"
#include "share/change_stream/ob_change_stream_dispatcher.h"

namespace oceanbase
{
namespace share
{

ObCSDispatcher::ObCSDispatcher()
  : share::ObThreadPool(1),
    is_inited_(false),
    change_stream_refresh_scn_(0),
    next_sn_(0)
{
}

ObCSDispatcher::~ObCSDispatcher()
{
  destroy();
}

int ObCSDispatcher::init()
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
  } else if (OB_FAIL(tx_ring_.init(10000))) {
    LOG_WARN("ObCSDispatcher: init tx_ring failed", K(ret));
  } else if (OB_FAIL(ObThreadPool::init())) {
    LOG_WARN("ObCSDispatcher: thread pool init failed", K(ret));
  } else {
    next_sn_ = 0;
    is_inited_ = true;
  }
  return ret;
}


int ObCSDispatcher::start()
{
  int ret = common::OB_SUCCESS;
  if (!is_inited_) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(ObThreadPool::start())) {
    LOG_WARN("ObCSDispatcher: thread pool start failed", K(ret));
  }
  return ret;
}

void ObCSDispatcher::stop()
{
  ObThreadPool::stop();
}

void ObCSDispatcher::wait()
{
  ObThreadPool::wait();
}

void ObCSDispatcher::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    tx_ring_.destroy();
    next_sn_ = 0;
    is_inited_ = false;
  }
}

int ObCSDispatcher::push(ObCSTxInfo *tx)
{
  int ret = common::OB_SUCCESS;
  if (!is_inited_ || OB_ISNULL(tx)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    const int64_t sn = next_sn_++;
    if (OB_FAIL(tx_ring_.set(sn, tx))) {
      LOG_WARN("ObCSDispatcher: tx_ring set failed", K(ret), K(sn));
    }
  }
  return ret;
}

void ObCSDispatcher::run1()
{
  lib::set_thread_name("CSDispatcher");
  while (!has_set_stop()) {
    usleep(100 * 1000);
  }
}

}  // namespace share
}  // namespace oceanbase
