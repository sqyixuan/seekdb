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
#ifndef OB_STORAGE_COMPACTION_SS_SCHEDULE_TABLET_FUNC_H_
#define OB_STORAGE_COMPACTION_SS_SCHEDULE_TABLET_FUNC_H_
#include "storage/compaction/ob_basic_schedule_tablet_func.h"
#include "storage/compaction/ob_ls_compaction_obj_mgr.h"
namespace oceanbase
{
namespace compaction
{
struct ObSSTabletStatusCache final : public ObTabletStatusCache
{
  ObSSTabletStatusCache() = default;
  virtual ~ObSSTabletStatusCache() {}
  int init(
    storage::ObLS &ls,
    const int64_t merge_version,
    const storage::ObTablet &tablet,
    const bool is_skip_merge_tenant,
    const ObLSBroadcastInfo &broadcast_info,
    ObTabletCompactionState &tablet_state);
private:
  int check_major_ckm_info(
    const int64_t merge_version,
    const storage::ObTablet &tablet,
    const ObLSBroadcastInfo &broadcast_info,
    ObTabletCompactionState &tablet_state);
};

struct ObSSScheduleTabletFunc final : public ObBasicScheduleTabletFunc
{
  ObSSScheduleTabletFunc(const int64_t merge_version)
  : ObBasicScheduleTabletFunc(merge_version),
    tablet_status_(),
    broadcast_info_(),
    time_guard_()
  {}
  virtual ~ObSSScheduleTabletFunc() {}
  int switch_ls(storage::ObLSHandle &ls_handle);
  int schedule_tablet(
    storage::ObTablet &tablet);
  DELEGATE_WITHOUT_RET(time_guard_, click);
  DELEGATE_WITHOUT_RET(time_guard_, add_time_guard);
  ObLSBroadcastInfo &get_broadcast_info() { return broadcast_info_; }
  virtual const ObCompactionTimeGuard &get_time_guard() const override { return time_guard_; }
  INHERIT_TO_STRING_KV("ObScheduleTabletFunc", ObBasicScheduleTabletFunc, K_(tablet_status),
    K_(broadcast_info), K_(time_guard));
private:
  int schedule_tablet_execute(
    const storage::ObTablet &tablet);
  int create_dag_for_different_state(
    const storage::ObTablet &tablet);
  int refresh_broadcast_info();
  int try_update_tablet_state(const ObTablet &tablet, const ObTabletCompactionState &tablet_state);
private:
  ObSSTabletStatusCache tablet_status_;
  ObLSBroadcastInfo broadcast_info_;
  ObSSCompactionTimeGuard time_guard_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SS_SCHEDULE_TABLET_FUNC_H_
