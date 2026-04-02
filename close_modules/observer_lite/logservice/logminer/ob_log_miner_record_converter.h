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

#ifndef OCEANBASE_LOG_MINER_RECORD_CONVERTER_H_
#define OCEANBASE_LOG_MINER_RECORD_CONVERTER_H_

#include "ob_log_miner_file_manager.h"
#include "ob_log_miner_record.h"

namespace oceanbase
{
namespace oblogminer
{

class ILogMinerRecordConverter
{
public:
  // TODO: there may be concurrency issue in future?
  static ILogMinerRecordConverter *get_converter_instance(const RecordFileFormat format);
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written) = 0;

public:
  // TENANT_ID,TRANS_ID,PRIMARY_KEY,ROW_UNIQUE_ID,SEQ_NO,TENANT_NAME,USER_NAME,TABLE_NAME,OPERATION,
  // OPERATION_CODE,COMMIT_SCN,COMMIT_TIMESTAMP,SQL_REDO,SQL_UNDO,ORG_CLUSTER_ID
  #define MINER_SCHEMA_DEF(field, id, args...) \
    field = id,
  enum class ColType {
    #include "ob_log_miner_analyze_schema.h"
  };
  #undef MINER_SCHEMA_DEF

  const static ColType COL_ORDER[];
  static const char *DELIMITER;
};

class ObLogMinerRecordCsvConverter: public ILogMinerRecordConverter
{
public:
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written);
public:
  ObLogMinerRecordCsvConverter() {};
  ~ObLogMinerRecordCsvConverter() {}
private:
  int write_csv_string_escape_(const ObString &str, common::ObStringBuffer &buffer);
};

class ObLogMinerRecordRedoSqlConverter: public ILogMinerRecordConverter
{
public:
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written);
public:
  ObLogMinerRecordRedoSqlConverter() {}
  ~ObLogMinerRecordRedoSqlConverter() {}

};

class ObLogMinerRecordUndoSqlConverter: public ILogMinerRecordConverter
{
public:
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written);
public:
  ObLogMinerRecordUndoSqlConverter() {}
  ~ObLogMinerRecordUndoSqlConverter() {}
};

class ObLogMinerRecordJsonConverter: public ILogMinerRecordConverter
{
public:
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written);
public:
  ObLogMinerRecordJsonConverter() {};
  ~ObLogMinerRecordJsonConverter() {}
private:
  int write_json_key_(const char *str, common::ObStringBuffer &buffer);
  int write_json_string_escape_(const ObString &str, common::ObStringBuffer &buffer);
};

}
}

#endif
