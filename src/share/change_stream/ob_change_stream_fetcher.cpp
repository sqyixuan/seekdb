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
#include "share/change_stream/ob_change_stream_fetcher.h"
#include "logservice/palf/log_define.h"

namespace oceanbase
{
namespace share
{

ObCSFetcher::ObCSFetcher()
  : share::ObThreadPool(1),
    is_inited_(false),
    log_handler_(nullptr),
    current_lsn_(),
    current_scn_()
{
}

ObCSFetcher::~ObCSFetcher()
{
  destroy();
}

int ObCSFetcher::init()
{
  int ret = common::OB_SUCCESS;
  if (is_inited_) {
    ret = common::OB_INIT_TWICE;
  } else if (OB_FAIL(tx_info_.create(1024, "CSFetcherTx"))) {
    LOG_WARN("ObCSFetcher: create tx_info map failed", K(ret));
  } else if (OB_FAIL(ObThreadPool::init())) {
    LOG_WARN("ObCSFetcher: thread pool init failed", K(ret));
  } else {
    current_scn_.set_min();
    is_inited_ = true;
  }
  return ret;
}

int ObCSFetcher::start()
{
  int ret = common::OB_SUCCESS;
  if (!is_inited_) {
    ret = common::OB_NOT_INIT;
  } else if (OB_FAIL(ObThreadPool::start())) {
    LOG_WARN("ObCSFetcher: thread pool start failed", K(ret));
  }
  return ret;
}

void ObCSFetcher::stop()
{
  ObThreadPool::stop();
}

void ObCSFetcher::wait()
{
  ObThreadPool::wait();
}

void ObCSFetcher::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    tx_info_.destroy();
    log_handler_ = nullptr;
    current_lsn_.reset();
    current_scn_.reset();
    is_inited_ = false;
  }
}

palf::LSN ObCSFetcher::get_min_dep_lsn() const
{
  palf::LSN min_lsn;
  return min_lsn;
}

void ObCSFetcher::run1()
{
  lib::set_thread_name("CSFetcher");
  // Main loop: read CLOG by transaction, assemble into ObCSTxInfo, push on commit.
  // Actual CLOG read is left to integration (log_handler_ and logservice).
  while (!has_set_stop()) {
    // Placeholder: in real implementation, fetch next log entry, update tx_info_,
    // and on commit log call tx_sink_->push(tx).
    usleep(100 * 1000);  // 100ms
  }
}

}  // namespace share
}  // namespace oceanbase
