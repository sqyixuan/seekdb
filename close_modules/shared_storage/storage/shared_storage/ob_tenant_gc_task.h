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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_GC_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_GC_H_

#include "lib/task/ob_timer.h"
#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
namespace storage
{

class ObTenantGCTask : public common::ObTimerTask
{
public:
  ObTenantGCTask();
  virtual ~ObTenantGCTask() = default;
  int init(const int tg_id);
  virtual void runTimerTask() override;
  TO_STRING_KV(K(GC_INTERVAL), K_(last_gc_tenant_shared_dir_loop_ts), K_(is_inited)); 
private:
  int get_gc_safe_time_val_(
      int64_t &gc_safe_time_val) const;
  int process_delete_tenant_shared_dir_(
      const uint64_t tenant_id) const;
  void loop_check_tenant_shared_dir_gc_() const;
  int read_is_shared_tenant_deleted_obj_(
      const uint64_t tenant_id,
      int64_t &delete_ts) const;
  int write_is_shared_tenant_deleted_obj_(
      const uint64_t tenant_id) const;

  void loop_check_tenant_private_dir_gc_() const;
  void delete_tenants_(
      ObIArray<storage::ObTenantItem> &items) const;
  int delete_tenant_(
      const storage::ObTenantItem &item) const;
private:
  static const int64_t GC_INTERVAL;
  bool is_inited_;
  int64_t last_gc_tenant_shared_dir_loop_ts_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_GC_H_ */
