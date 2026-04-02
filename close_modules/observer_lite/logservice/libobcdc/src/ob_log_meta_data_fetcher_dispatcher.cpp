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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_meta_data_fetcher_dispatcher.h"  // ObLogMetaDataFetcherDispatcher

#include "lib/oblog/ob_log_module.h"        // LOG_ERROR
#include "lib/atomic/ob_atomic.h"           // ATOMIC_FAA
#include "lib/utility/ob_macro_utils.h"     // RETRY_FUNC
#include "ob_log_part_trans_task.h"         // PartTransTask

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

ObLogMetaDataFetcherDispatcher::ObLogMetaDataFetcherDispatcher() :
    IObLogFetcherDispatcher(FetcherDispatcherType::DATA_DICT_DIS_TYPE),
    is_inited_(false),
    log_meta_data_replayer_(nullptr),
    checkpoint_seq_(0)
{
}

ObLogMetaDataFetcherDispatcher::~ObLogMetaDataFetcherDispatcher()
{
  destroy();
}

int ObLogMetaDataFetcherDispatcher::init(
    IObLogMetaDataReplayer *log_meta_data_replayer,
    const int64_t start_seq)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_ISNULL(log_meta_data_replayer_ = log_meta_data_replayer)
      || OB_UNLIKELY(start_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(log_meta_data_replayer), K(start_seq));
  } else {
    checkpoint_seq_ = start_seq;
    is_inited_ = true;
  }

  return ret;
}

void ObLogMetaDataFetcherDispatcher::destroy()
{
  is_inited_ = false;
  log_meta_data_replayer_ = nullptr;
  checkpoint_seq_ = 0;
}

int ObLogMetaDataFetcherDispatcher::dispatch(PartTransTask &task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMetaDataSQLQueryer is not initialized", KR(ret));
  } else {
    // All tasks are uniformly assigned checkpoint seq
    task.set_checkpoint_seq(ATOMIC_FAA(&checkpoint_seq_, 1));

    LOG_DEBUG("[STAT] [PART_TRANS] [META_FETCHER_DISPATCHER]", K(task),
        "checkpoint_seq", task.get_checkpoint_seq());

    switch (task.get_type()) {
      case PartTransTask::TASK_TYPE_DDL_TRANS:
        ret = dispatch_to_log_meta_data_replayer_(task, stop_flag);
        break;

      case PartTransTask::TASK_TYPE_LS_HEARTBEAT:
        ret = dispatch_to_log_meta_data_replayer_(task, stop_flag);
        break;

      case PartTransTask::TASK_TYPE_LS_OP_TRANS:
        ret = dispatch_to_log_meta_data_replayer_(task, stop_flag);
        break;

      case PartTransTask::TASK_TYPE_OFFLINE_LS:
        ret = dispatch_to_log_meta_data_replayer_(task, stop_flag);
        break;

      // TODO: Some Trans marked as DML_TRANs could actually be DDL_TRANS
      case PartTransTask::TASK_TYPE_DML_TRANS:
        ret = dispatch_to_log_meta_data_replayer_(task, stop_flag);
        break;

      default:
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("not support task", KR(ret), K(task));
        break;
    }

    if (OB_SUCCESS != ret) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("dispatch task fail", KR(ret), K(task));
      }
    }
  }

  return ret;
}

int ObLogMetaDataFetcherDispatcher::dispatch_to_log_meta_data_replayer_(
    PartTransTask &task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(log_meta_data_replayer_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("log_meta_data_replayer_ is nullptr", KR(ret), K(log_meta_data_replayer_));
  } else {
    const int64_t task_count = 1;
    // Push into committer
    RETRY_FUNC(stop_flag, *log_meta_data_replayer_, push, &task, DATA_OP_TIMEOUT);
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

