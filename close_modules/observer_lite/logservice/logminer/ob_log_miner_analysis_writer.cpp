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

#include "ob_log_miner_analysis_writer.h"
#include "ob_log_miner_record_aggregator.h"
#include "ob_log_miner_batch_record_writer.h"
#include "ob_log_miner_logger.h"


namespace oceanbase
{
namespace oblogminer
{

ObLogMinerAnalysisWriter::ObLogMinerAnalysisWriter():
    is_inited_(false),
    is_stopped_(true),
    aggregator_(nullptr),
    batch_writer_(nullptr) { }

ObLogMinerAnalysisWriter::~ObLogMinerAnalysisWriter()
{
  destroy();
}

int ObLogMinerAnalysisWriter::init(const int64_t start_time_us,
    ILogMinerDataManager *data_manager,
    ILogMinerResourceCollector *resource_collector,
    ILogMinerFileManager *file_manager,
    ILogMinerErrorHandler *err_handle)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogminerAnalysisWriter has been initialized", K(is_inited_));
  } else if (OB_ISNULL(data_manager) || OB_ISNULL(resource_collector) || OB_ISNULL(file_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid argument for ObLogMinerAnalysisWriter", K(data_manager),
        K(resource_collector), K(file_manager));
  } else if (OB_FAIL(init_component<ObLogMinerBatchRecordWriter>(batch_writer_,
      data_manager, resource_collector, file_manager, err_handle))) {
    LOG_ERROR("failed to init ObLogMinerBatchRecordWriter", K(batch_writer_));
  } else if (OB_FAIL(init_component<ObLogMinerRecordAggregator>(aggregator_,
      start_time_us, batch_writer_, data_manager, resource_collector, err_handle))) {
    LOG_ERROR("failed to init ObLogMinerRecordAggregator", K(aggregator_));
  } else {
    is_inited_ = true;
    LOG_INFO("ObLogMinerAnalysisWriter finished to init", KP(data_manager), KP(resource_collector),
        KP(file_manager), KP(err_handle));
    LOGMINER_STDOUT_V("ObLogMinerAnalysisWriter finished to init\n");
  }

  return ret;
}

int ObLogMinerAnalysisWriter::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerAnalysisWriter hasn't been initialized yet", K(is_inited_));
  } else if (OB_FAIL(batch_writer_->start())) {
    LOG_ERROR("batch_writer failed to start");
  } else if (OB_FAIL(aggregator_->start())) {
    LOG_ERROR("aggregator failed to start");
  } else {
    is_stopped_ = false;
    LOG_INFO("ObLogMinerAnalysisWriter starts");
    LOGMINER_STDOUT_V("ObLogMinerAnalysisWriter starts\n");
  }

  return ret;
}

void ObLogMinerAnalysisWriter::stop()
{
  mark_stop_flag();
  aggregator_->stop();
  batch_writer_->stop();
  LOG_INFO("ObLogMinerAnalysisWriter stopped");
}

void ObLogMinerAnalysisWriter::wait()
{
  aggregator_->wait();
  batch_writer_->wait();
  LOG_INFO("ObLogMinerAnalysisWriter finished to wait");
}

void ObLogMinerAnalysisWriter::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    is_stopped_ = true;
    destroy_component<ObLogMinerRecordAggregator>(aggregator_);
    destroy_component<ObLogMinerBatchRecordWriter>(batch_writer_);
    aggregator_ = nullptr;
    batch_writer_ = nullptr;
    LOG_INFO("ObLogMinerAnalysisWriter destroyed");
    LOGMINER_STDOUT_V("ObLogMinerAnalysisWriter destroyed\n");
  }
}

int ObLogMinerAnalysisWriter::push(ObLogMinerRecord *record)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerAnalysisWriter hasn't been initialized yet", K(is_inited_));
  } else if (OB_FAIL(aggregator_->push(record))) {
    LOG_ERROR("failed to push record into aggregator", KPC(record));
  }

  return ret;
}

int ObLogMinerAnalysisWriter::get_total_task_count(int64_t &task_count)
{
  int ret = OB_SUCCESS;
  int64_t aggregator_record_cnt = 0, batch_writer_record_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerAnalysisWriter hasn't been initialized yet", K(is_inited_));
  } else if (OB_FAIL(aggregator_->get_total_record_count(aggregator_record_cnt))) {
    LOG_ERROR("failed to get aggregator total record count");
  } else if (OB_FAIL(batch_writer_->get_total_record_count(batch_writer_record_cnt))) {
    LOG_ERROR("failed to get batch writer record count");
  } else {
    task_count = aggregator_record_cnt + batch_writer_record_cnt;
  }

  return ret;
}

}
}
