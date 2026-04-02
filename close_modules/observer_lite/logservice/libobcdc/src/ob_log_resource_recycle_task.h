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

#ifndef OCEANBASE_LIBOBCDC_RESOURCE_RECYCLE_TASK_H__
#define OCEANBASE_LIBOBCDC_RESOURCE_RECYCLE_TASK_H__

namespace oceanbase
{
namespace libobcdc
{
class ObLogResourceRecycleTask
{
public:
  enum TaskType
  {
    UNKNOWN_TASK = 0,
    PART_TRANS_TASK = 1,
    BINLOG_RECORD_TASK = 2,
    LOB_DATA_CLEAN_TASK = 3,
  };
  OB_INLINE bool is_unknown_task() const { return UNKNOWN_TASK == task_type_; }
  OB_INLINE bool is_part_trans_task() const { return PART_TRANS_TASK == task_type_; }
  OB_INLINE bool is_binlog_record_task() const { return BINLOG_RECORD_TASK == task_type_; }
  OB_INLINE bool is_lob_data_clean_task() const { return LOB_DATA_CLEAN_TASK == task_type_; }
  OB_INLINE TaskType get_task_type() const { return task_type_; }

  static const char *print_task_type(TaskType task)
  {
    const char *str = "UNKNOWN_TASK";

    switch (task) {
      case PART_TRANS_TASK:
        str = "PartTransTask";
        break;
      case BINLOG_RECORD_TASK:
        str = "BinlogRecordTask";
        break;
      case LOB_DATA_CLEAN_TASK:
        str = "LobDataCleanTask";
      default:
        str = "UNKNOWN_TASK";
        break;
    }

    return str;
  }

public:
  ObLogResourceRecycleTask() : task_type_(UNKNOWN_TASK) {}
  ObLogResourceRecycleTask(TaskType task_type) : task_type_(task_type) {}
  ~ObLogResourceRecycleTask() { task_type_ = UNKNOWN_TASK; }

public:
  TaskType task_type_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
