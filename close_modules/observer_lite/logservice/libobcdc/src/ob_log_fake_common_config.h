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

#ifndef OCEANBASE_LIBOBCDC_FAKE_COMMON_CONFIG_H__
#define OCEANBASE_LIBOBCDC_FAKE_COMMON_CONFIG_H__

#include "share/ob_define.h"
#include "share/config/ob_common_config.h"    // ObCommonConfig

namespace oceanbase
{
namespace libobcdc
{
class ObLogFakeCommonConfig : public common::ObCommonConfig
{
public:
  ObLogFakeCommonConfig() {}
  virtual ~ObLogFakeCommonConfig() {}

  virtual int check_all() const { return 0; }
  virtual void print() const  { /* do nothing */ }
  virtual common::ObServerRole get_server_type() const { return common::OB_OBLOG; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFakeCommonConfig);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_FAKE_COMMON_CONFIG_H__ */
