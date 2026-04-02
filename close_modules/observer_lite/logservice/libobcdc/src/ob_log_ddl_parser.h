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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_DDL_PARSER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_DDL_PARSER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h"      // ObMQThread

namespace oceanbase
{

namespace libobcdc
{
class PartTransTask;

class IObLogDdlParser
{
public:
  static const int64_t MAX_THREAD_NUM = 256;
public:
  virtual ~IObLogDdlParser() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int push(PartTransTask &task, const int64_t timeout) = 0;
  virtual int get_part_trans_task_count(int64_t &task_num) = 0;
};

typedef common::ObMQThread<IObLogDdlParser::MAX_THREAD_NUM, IObLogDdlParser> DdlParserThread;

class IObLogErrHandler;
class IObLogPartTransParser;
class ObLogDdlParser : public IObLogDdlParser, public DdlParserThread
{
  enum
  {
    DATA_OP_TIMEOUT = 1 * 1000 * 1000,
  };

public:
  ObLogDdlParser();
  virtual ~ObLogDdlParser();

public:
  // DdlParserThread handle function
  virtual int handle(void *task, const int64_t thread_index, volatile bool &stop_flag);

public:
  int start();
  void stop();
  void mark_stop_flag() { DdlParserThread::mark_stop_flag(); }
  int push(PartTransTask &task, const int64_t timeout);
  int get_part_trans_task_count(int64_t &task_num);

public:
  int init(const int64_t thread_num,
      const int64_t queue_size,
      IObLogErrHandler &err_handler,
      IObLogPartTransParser &part_trans_parser);
  void destroy();

private:
  bool                  inited_;
  IObLogErrHandler      *err_handler_;
  IObLogPartTransParser *part_trans_parser_;

  // The serial number of the currently processed task, used to rotate task push to the queue
  int64_t               push_seq_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDdlParser);
};
}
}

#endif
