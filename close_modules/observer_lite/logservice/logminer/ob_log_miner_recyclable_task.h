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

#ifndef OCEANBASE_LOG_MINER_RECYCLABLE_TASK_H_
#define OCEANBASE_LOG_MINER_RECYCLABLE_TASK_H_

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerRecyclableTask
{
public:
  enum class TaskType
  {
    UNKNOWN = 0,
    BINLOG_RECORD,
    LOGMINER_RECORD,
    BATCH_RECORD,
    UNDO_TASK
  };

  explicit ObLogMinerRecyclableTask(TaskType type): type_(type) { }
  ~ObLogMinerRecyclableTask() { type_ = TaskType::UNKNOWN; }

  bool is_binlog_record() const {
    return TaskType::BINLOG_RECORD == type_;
  }
  bool is_logminer_record() const {
    return TaskType::LOGMINER_RECORD == type_;
  }
  bool is_batch_record() const {
    return TaskType::BATCH_RECORD == type_;
  }
  bool is_undo_task() const {
    return TaskType::UNDO_TASK == type_;
  }

  TaskType get_task_type() const
  {
    return type_;
  }

protected:
  TaskType type_;
};

}
}

#endif
