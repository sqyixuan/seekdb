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
#include "logservice/arbserver/ob_arb_server_config.h"
#include "lib/file/file_directory_utils.h"
#include "logservice/arbserver/ob_arb_cluster_white_list.h"

using namespace oceanbase::common;
namespace oceanbase
{
typedef lib::ObLockGuard<lib::ObMutex> LockGuard;
namespace arbserver
{
constexpr const char ObArbServerConfig::dump_path_[];
int ObArbServerConfig::init()
{
  int ret = OB_SUCCESS;
  const int64_t interval = 10000000; // 10s
  if (OB_UNLIKELY(inited_)) {
    LOG_WARN("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(ObBaseConfig::init())) {
    LOG_ERROR("init ObBaseConfig failed", K(ret));
  } else if (OB_FAIL(timer_.init())){
    LOG_ERROR("init timer failed", K(ret));
  } else if (OB_FAIL(timer_.start())) {
    LOG_ERROR("start timer failed", K(ret));
  } else if (OB_FAIL(timer_.schedule(task_, interval, true))) {
    LOG_ERROR("schedule task failed", K(ret));
  } else {
    inited_ = true;
  }
  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObArbServerConfig::stop()
{
  if (is_inited()) {
    timer_.stop();
  }
}
void ObArbServerConfig::wait()
{
  if (is_inited()) {
    timer_.wait();
  }
}

void ObArbServerConfig::destroy()
{
  if (is_inited()) {
    timer_.destroy();
    ObBaseConfig::destroy();
    inited_ = false;
  }
}

int ObArbServerConfig::init_config_with_file()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_FAIL(FileDirectoryUtils::is_exists(dump_path_, is_exist))) {
    // do nothing
  } else if (is_exist) {
    ret = load_from_file(dump_path_);
  } else {
    ret = dump2file(dump_path_);
  }
  return ret;
}
ObArbServerConfig &ObArbServerConfig::get_instance()
{
  static ObArbServerConfig arbserver_config;
  return arbserver_config;
}
int ObArbServerConfig::update_config(const char *name, const char *value)
{
  int ret = OB_SUCCESS;
  ObConfigItem *const *item = NULL;
  LockGuard guard(lock_);
  if (OB_ISNULL(item = container_.get(ObConfigStringKey(name))) 
      || !(*item)->set_value(value)) {
    ret = OB_INVALID_CONFIG;
    LOG_ERROR("invalid config value", K(name), K(value), K(ret));
  } else if (OB_FAIL(check_all())) {
    LOG_ERROR("check arbserver config failed", K(ret));
  } else if (OB_FAIL(dump2file(dump_path_))) {
    LOG_ERROR("failed to dump arbserver config to file", K(ret));
  } else {
    LOG_INFO("update config succeed", K(name), K(value));
  }
  return ret;
}

int ObArbServerConfig::reload_config()
{
  int ret = OB_SUCCESS;
  {
    ObArbWhiteList &arb_whitelist = ObArbWhiteList::get_instance();
    if (!arb_whitelist.is_inited()) {
      LOG_ERROR("ObArbWhiteList has not been initialized");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(arb_whitelist.clear_white_list())) {
      LOG_ERROR("clean arb whitelist failed", K(ret));
    } else {
      const int size = 64;
      for (int i = 0; OB_SUCC(ret) && i < cluster_id_white_list.size(); ++i) {
        int64_t cluster_id = -1;
        char str[size] = "";
        if (OB_FAIL(cluster_id_white_list.get(i, str, size))) {
          LOG_ERROR("get string failed", K(ret));
        } else if (OB_FAIL(arb_whitelist.add_cluster_id_with_str(str))) {
          LOG_ERROR("set cluster_id failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      LOG_ERROR("load cluster_id_white_list to local failed", K(ret));
    }
  }
  return ret;
}
void ObArbServerConfig::print() const
{
  OB_LOG(INFO, "===================== *begin arbserver config report * =====================");
  for (auto it = container_.begin(); it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "config item is null", "name", it->first.str());
    } else {
      OB_LOG(INFO, "| %-36s = %s", it->first.str(), it->second->str());
    }
  }
  OB_LOG(INFO, "===================== *stop arbserver config report* =======================");
}


void ObArbServerConfig::ObArbConfigTimerTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ASCONF.load_from_file(dump_path_))) {
    LOG_ERROR("load arbserver config from file failed", K(ret));
  } else if(OB_FAIL(ASCONF.reload_config())) {
    LOG_ERROR("failed to reload config to memory", K(ret));
  } else {
    ASCONF.print();
    LOG_INFO("load config file succeed with timertask");
  }
}

} // end of namespace arbserver
} // end of namespace oceanbase
