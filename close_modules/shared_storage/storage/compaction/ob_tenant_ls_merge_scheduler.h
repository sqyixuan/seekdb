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
#ifndef OB_SHARE_STORAGE_COMPACTION_TENANT_LS_MERGE_SCHEDULER_H_
#define OB_SHARE_STORAGE_COMPACTION_TENANT_LS_MERGE_SCHEDULER_H_
#include "lib/literals/ob_literals.h"
#include "lib/container/ob_se_array.h"
#include "share/compaction/ob_compaction_timer_task_mgr.h"
#include "storage/compaction/ob_ls_merge_schedule_iterator.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/compaction/ob_ls_compaction_list.h"
#include "share/compaction/ob_compaction_time_guard.h"
#include "lib/hash/ob_hashset.h"
#include "storage/compaction/ob_schedule_tablet_func.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTabletHandle;
}

namespace compaction
{
struct ObTabletSchedulePair;
struct ObSSScheduleTabletFunc;

struct ObTenantLSMergeLoopTaskMgr : public ObCompactionTimerTask
{
  ObTenantLSMergeLoopTaskMgr();
  ~ObTenantLSMergeLoopTaskMgr() = default;
  virtual void destroy() override;
  virtual int start() override;
  virtual void stop() override;
  virtual void wait() override;
  void refresh_schedule_interval(const int64_t new_schedule_interval);
  int reload_tenant_config(const int64_t new_schedule_interval);
  DEFINE_TIMER_TASK_WITHOUT_TIMEOUT_CHECK(LSMergeLoopTask);
  DEFINE_TIMER_TASK_WITHOUT_TIMEOUT_CHECK(LSMergeVerifyTask);
private:
  int merge_loop_tg_id_;
  int64_t schedule_interval_;
  LSMergeLoopTask merge_loop_task_;
  LSMergeVerifyTask merge_verify_task_;
};

struct ObErrorTabletValidateScheduler final
{
  ObErrorTabletValidateScheduler()
    : tablet_id_set_(),
      is_stopped_(false)
  {}
  ~ObErrorTabletValidateScheduler() { destroy(); }
  int init();
  void reuse() {
    if (tablet_id_set_.created()) {
      tablet_id_set_.reuse();
    }
  }
  void destroy() {
    is_stopped_ = true;
    if (tablet_id_set_.created()) {
      tablet_id_set_.destroy();
    }
  }
  int schedule_error_tablet_verify_ckm(const int64_t compaction_scn);
private:
  typedef hash::ObHashSet<ObTabletID, hash::NoPthreadDefendMode> ValidateTablets;
  static const int64_t DEFAULT_BUCKET_CNT = 32;
  ValidateTablets tablet_id_set_;
  bool is_stopped_;
};

class ObTenantLSMergeScheduler : public ObBasicMergeScheduler
{
public:
  struct TenantScheduleCfg
  {
  public:
      TenantScheduleCfg();
      ~TenantScheduleCfg() = default;
      void refresh();
      TO_STRING_KV(K_(schedule_interval), K_(schedule_batch_size), K_(delay_overwrite_interval));
      int64_t schedule_interval_;
      int64_t schedule_batch_size_;
      int64_t delay_overwrite_interval_;
  };

  ObTenantLSMergeScheduler();
  virtual ~ObTenantLSMergeScheduler();
  static int mtl_init(ObTenantLSMergeScheduler *&scheduler) { return scheduler->init(); }
  int64_t get_dealy_overwrite_interval() const { return schedule_cfg_.delay_overwrite_interval_; }
  int init();
  void destroy();
  int start();
  void stop();
  void wait();
// merge schedule part
  int reload_tenant_config();
  int refresh_tenant_status();
  virtual int schedule_merge(const int64_t broadcast_version) override;
  int schedule_ls_merge();
  int schedule_error_tablet_verify_ckm()
  {
    return validate_scheduler_.schedule_error_tablet_verify_ckm(get_frozen_version());
  }
  int get_tablet_merge_reason(
      const int64_t merge_version,
      const ObLSID &ls_id,
      ObTablet &tablet,
      ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason);
private:
  int schedule_ls_tablets_merge(
      const int64_t merge_version,
      storage::ObLS &ls,
      ObSSScheduleTabletFunc &func);
  int prepare_tablet_medium_info(
      const int64_t merge_version,
      storage::ObLS &ls,
      storage::ObTabletHandle &tablet_hdl,
      ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
      bool &submit_clog_flag);
  int schedule_prepare_medium_info(
      const int64_t merge_version,
      ObLS &ls,
      ObSSScheduleTabletFunc &func);
  void skip_iter_cur_ls();
public:
  static const int64_t DEFAULT_MERGE_SCHEDULE_INTERVAL = 30_s;
  static const int64_t DEFAULT_TABLET_BATCH_CNT = 50000; // 5W
  static const int64_t DEFAULT_ARRAY_CNT = 128;
  static const int64_t DEFAULT_DELAY_OVERWRITE_INTERVAL = 30_s;
private:
  bool is_inited_;
  TenantScheduleCfg schedule_cfg_;
  ObLSMergeIterator ls_tablet_iter_;
  ObSSCompactionTimeGuard time_guard_;
  ObTenantLSMergeLoopTaskMgr task_mgr_;
  ObErrorTabletValidateScheduler validate_scheduler_;
};

} // compaction
} // oceanbase



#endif // OB_SHARE_STORAGE_COMPACTION_TENANT_LS_MERGE_SCHEDULER_H_
