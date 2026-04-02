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

#define USING_LOG_PREFIX SERVER
#include "logservice/arbserver/ob_arb_cluster_white_list.h"
#include "logservice/arbserver/ob_arb_server_config.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace obrpc
{
int check_arb_white_list(int64_t cluster_id, bool& is_arb)
{
  int ret = OB_SUCCESS;
  is_arb = false;
  arbserver::ObArbWhiteList &arb_whitelist = arbserver::ObArbWhiteList::get_instance();
  if (arb_whitelist.is_inited()) {
    is_arb = true;
    if (OB_FAIL(arb_whitelist.exist_cluster_id(cluster_id))) {
      LOG_WARN("cluster_id not in arb_cluster_white_list", K(ret));
    }
  }
  return ret;
}
}
namespace arbserver
{
int ObArbWhiteList::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init arbitration whitelist twice", K(ret));
  } else if (OB_FAIL(white_list_.create(1024, "ArbWhiteList", "ArbWhiteList"))) {
    LOG_WARN("create white_list_ failed", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}
int ObArbWhiteList::add_cluster_id(int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObArbWhiteList has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(white_list_.set_refactored(cluster_id))) {
    LOG_ERROR("add cluster_id failed", K(ret));
  } else if (OB_FAIL(update_config())) {
    LOG_WARN("update config failed", K(ret));
  }
  return ret;
}
int ObArbWhiteList::delete_cluster_id(int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObArbWhiteList has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(white_list_.erase_refactored(cluster_id))) {
    LOG_ERROR("delete cluster_id failed", K(ret));
  } else if (OB_FAIL(update_config())) {
    LOG_WARN("update config failed", K(ret));
  }
  return ret;
}

int ObArbWhiteList::exist_cluster_id(int64_t cluster_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObArbWhiteList has not been initialized");
    ret = OB_NOT_INIT;
  } else if (!white_list_.empty() && OB_HASH_EXIST != white_list_.exist_refactored(cluster_id)) {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

int ObArbWhiteList::add_cluster_id_with_str(const char *str)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id = -1;
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObArbWhiteList has not been initialized");
    ret = OB_NOT_INIT;
  } else if ('\0' == str[0]) {
    // do nothing
  } else if (OB_FAIL(common::ob_atoll(str, cluster_id))) {
    LOG_WARN("fail to parse from string", "string", str, K(ret));
  } else if (OB_FAIL(white_list_.set_refactored(cluster_id))) {
    LOG_WARN("add cluster_id to whitelist failed");
  }
  return ret;
}
int ObArbWhiteList::update_config()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObArbWhiteList has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    const int64_t buf_size = 1024;
    char white_list[buf_size] = ";";
    int64_t pos = 0;
    for (auto it = white_list_.begin(); OB_SUCC(ret) && it != white_list_.end(); ++it) {
      ret = databuff_printf(white_list, buf_size, pos, "%ld;", it->first);
    }
    if (OB_SUCC(ret) && pos < buf_size) {
      if (pos > 0) {
        white_list[pos] = '\0';
      }
      ret = ASCONF.update_config("cluster_id_white_list", white_list);
    }
  }
  return ret;
}
} // end of namespace arbserver
} // end of namespace oceanbase
