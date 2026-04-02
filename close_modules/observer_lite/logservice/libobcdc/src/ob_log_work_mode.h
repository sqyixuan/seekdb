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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_WORK_MODE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_WORK_MODE_H_

#include "share/ob_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace libobcdc
{
enum WorkingMode
{
  UNKNOWN_MODE = 0,

  MEMORY_MODE = 1,
  STORAGER_MODE = 2,
  AUTO_MODE = 3,

  MAX_MODE
};
const char *print_working_mode(const WorkingMode mode);
WorkingMode get_working_mode(const char *working_mode_str);

bool is_working_mode_valid(WorkingMode mode);
bool is_memory_working_mode(const WorkingMode mode);
bool is_storage_working_mode(const WorkingMode mode);
bool is_auto_working_mode(const WorkingMode mode);

}
}

#endif
