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

#ifndef OCEANBASE_LOG_MINER_RECORD_FILE_FORMAT_H_
#define OCEANBASE_LOG_MINER_RECORD_FILE_FORMAT_H_

#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace oblogminer
{
enum class RecordFileFormat
{
  INVALID = -1,
  CSV = 0,
  REDO_ONLY,
  UNDO_ONLY,
  JSON,
  PARQUET,
  AVRO
};
RecordFileFormat get_record_file_format(const common::ObString &file_format_str);

RecordFileFormat get_record_file_format(const char *file_format_str);

const char *record_file_format_str(const RecordFileFormat format);
}
}
#endif
