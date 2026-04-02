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

#ifndef OCEANBASE_ARB_SERVER_CONFIG_H
#define OCEANBASE_ARB_SERVER_CONFIG_H
#include "lib/task/ob_timer.h"
#include "share/parameter/ob_parameter_macro.h"
#include "share/config/ob_common_config.h"
namespace oceanbase
{
namespace arbserver
{
class ObArbConfigTimerTask;
class ObArbServerConfig : public ObBaseConfig
{

public:
  ObArbServerConfig()
    : inited_(false), timer_(), task_(), 
      lock_(common::ObLatchIds::ARB_SERVER_CONFIG_LOCK)
  {}

  virtual ~ObArbServerConfig() 
  { 
    destroy();
  }
  int init();
  bool is_inited() { return inited_; }
  void stop();
  void wait();
  void destroy();
  int init_config_with_file();
  static ObArbServerConfig& get_instance();
  virtual void print() const;
  int update_config(const char *name, const char * value);
  int reload_config();

public:
#ifdef OB_CLUSTER_PARAMETER
#undef OB_CLUSTER_PARAMETER
#endif
#define OB_CLUSTER_PARAMETER(args...) args
  DEF_STR_LIST(cluster_id_white_list, OB_CLUSTER_PARAMETER, ";", "cluster id white list");

#undef OB_CLUSTER_PARAMETER


class ObArbConfigTimerTask : public common::ObTimerTask
{
public:
  ObArbConfigTimerTask()
  {}
  void runTimerTask(void);
private:
  DISALLOW_COPY_AND_ASSIGN(ObArbConfigTimerTask);
};

private:
  bool inited_;
  common::ObTimer timer_;
  ObArbConfigTimerTask task_;
  lib::ObMutex lock_;
  static constexpr const char dump_path_[] = "etc/arbserver.config";
private:
  DISALLOW_COPY_AND_ASSIGN(ObArbServerConfig);
};
#define ASCONF (::oceanbase::arbserver::ObArbServerConfig::get_instance())

} // end of namespace arbserver
} // end of namespace oceanbase
#endif
