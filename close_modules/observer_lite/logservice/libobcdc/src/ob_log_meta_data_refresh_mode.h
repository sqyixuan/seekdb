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

#ifndef OCEANBASE_LOG_META_DATA_REFRESH_MODE_H_
#define OCEANBASE_LOG_META_DATA_REFRESH_MODE_H_

#include "share/ob_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace libobcdc
{
enum RefreshMode
{
  UNKNOWN_REFRSH_MODE = 0,

  DATA_DICT = 1,
  ONLINE = 2,

  MAX_REFRESH_MODE
};
const char *print_refresh_mode(const RefreshMode mode);
RefreshMode get_refresh_mode(const char *refresh_mode_str);

bool is_refresh_mode_valid(RefreshMode mode);
bool is_data_dict_refresh_mode(const RefreshMode mode);
bool is_online_refresh_mode(const RefreshMode mode);

}
}

#endif
