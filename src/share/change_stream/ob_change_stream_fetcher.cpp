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
#include "lib/allocator/ob_malloc.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/utility/serialization.h"
#include "share/change_stream/ob_change_stream_fetcher.h"
#include "share/change_stream/ob_change_stream_mgr.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/memtable/mvcc/ob_row_data.h"
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
  } else if (FALSE_IT(ObThreadPool::set_run_wrapper(MTL_CTX()))) {
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

void ObCSTxInfo::destroy()
{
  for (int64_t i = 0; i < redo_list_.count(); ++i) {
    if (redo_list_.at(i).redo_buf_ != nullptr) {
      common::ob_free(redo_list_.at(i).redo_buf_);
      redo_list_.at(i).redo_buf_ = nullptr;
    }
  }
  redo_list_.reset();
  reset();
}

palf::LSN ObCSFetcher::get_min_dep_lsn() const
{
  palf::LSN min_lsn;
  return min_lsn;
}

int ObCSFetcher::release_committed_tx(int64_t tx_id)
{
  int ret = common::OB_SUCCESS;
  ObCSTxInfo *tx = nullptr;
  if (OB_FAIL(tx_info_.erase_refactored(tx_id, &tx))) {
    LOG_WARN("ObCSFetcher: release_committed_tx erase failed", K(ret), K(tx_id));
  } else if (OB_NOT_NULL(tx)) {
    tx->destroy();
    OB_DELETE(ObCSTxInfo, "CSTxInfo", tx);
  }
  return ret;
}

void ObCSFetcher::run1()
{
  lib::set_thread_name("CSFetcher");


  while (!has_set_stop()) {
    usleep(100 * 1000);
  }
}

}  // namespace share
}  // namespace oceanbase
