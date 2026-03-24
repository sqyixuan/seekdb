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

#ifndef OCEANBASE_ARB_CLUSTER_WHITE_LIST_H
#define OCEANBASE_ARB_CLUSTER_WHITE_LIST_H
#include "lib/hash/ob_hashset.h"
namespace oceanbase
{
namespace common
{
class ObConfigStrListItem;
}
namespace arbserver
{
class ObArbWhiteList
{
public:
  static ObArbWhiteList& get_instance()
  {
    static ObArbWhiteList one;
    return one;
  }
  int init();
  bool is_inited() { return inited_; }
  int add_cluster_id(int64_t cluster_id);
  int delete_cluster_id(int64_t cluster_id);
  int exist_cluster_id(int64_t cluster_id) const;
  int clear_white_list() { return white_list_.clear(); }
  int add_cluster_id_with_str(const char *str);
  int update_config();
private:
  ObArbWhiteList() : inited_(false), white_list_()
  {}
private:
  bool inited_;
  common::hash::ObHashSet<int64_t> white_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObArbWhiteList);
};
} // end of namespace arbserver
} // end of namespace oceanbase
#endif
