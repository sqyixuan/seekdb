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

#include "ob_log_miner_record_file_format.h"
namespace oceanbase
{
namespace oblogminer
{
RecordFileFormat get_record_file_format(const common::ObString &file_format_str)
{
  RecordFileFormat format = RecordFileFormat::INVALID;
  if (0 == file_format_str.case_compare("CSV")) {
    format = RecordFileFormat::CSV;
  } else if (0 == file_format_str.case_compare("REDO_ONLY")) {
    format = RecordFileFormat::REDO_ONLY;
  } else if (0 == file_format_str.case_compare("UNDO_ONLY")) {
    format = RecordFileFormat::UNDO_ONLY;
  } else if (0 == file_format_str.case_compare("JSON")) {
    format = RecordFileFormat::JSON;
  } else if (0 == file_format_str.case_compare("AVRO")) {
    format = RecordFileFormat::AVRO;
  } else if (0 == file_format_str.case_compare("PARQUET")) {
    format = RecordFileFormat::PARQUET;
  }
  return format;
}

RecordFileFormat get_record_file_format(const char *file_format_str)
{
  return get_record_file_format(common::ObString(file_format_str));
}
const char *record_file_format_str(const RecordFileFormat format) {
  const char *result = nullptr;
  switch (format) {
    case RecordFileFormat::INVALID:
      result = "INVALID";
      break;
    case RecordFileFormat::CSV:
      result = "CSV";
      break;
    case RecordFileFormat::REDO_ONLY:
      result = "REDO_ONLY";
      break;
    case RecordFileFormat::UNDO_ONLY:
      result = "UNDO_ONLY";
      break;
    case RecordFileFormat::JSON:
      result = "JSON";
      break;
    case RecordFileFormat::PARQUET:
      result = "PARQUET";
      break;
    case RecordFileFormat::AVRO:
      result = "AVRO";
      break;
    default:
      result = "NOT_SUPPORTED";
      break;
  }
  return result;
}
}
}
