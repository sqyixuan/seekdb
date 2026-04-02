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

#include "share/change_stream/ob_change_stream_plugin.h"

namespace oceanbase
{
namespace share
{

ObCSPluginRegistry &ObCSPluginRegistry::get_instance()
{
  static ObCSPluginRegistry instance;
  return instance;
}

ObCSPluginRegistry::ObCSPluginRegistry()
{
  for (int64_t i = 0; i < CS_PLUGIN_MAX_TYPE; i++) {
    factories_[i] = nullptr;
  }
}

int ObCSPluginRegistry::register_factory(CS_PLUGIN_TYPE plugin_type,
                                        ObCSPluginFactoryFunc factory_func)
{
  int ret = common::OB_SUCCESS;
  if (plugin_type < 0 || plugin_type >= CS_PLUGIN_MAX_TYPE || nullptr == factory_func) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    factories_[plugin_type] = factory_func;
  }
  return ret;
}

ObCSPluginFactoryFunc ObCSPluginRegistry::get_factory(CS_PLUGIN_TYPE plugin_type) const
{
  if (plugin_type < 0 || plugin_type >= CS_PLUGIN_MAX_TYPE) {
    return nullptr;
  }
  return factories_[plugin_type];
}

}  // namespace share
}  // namespace oceanbase
