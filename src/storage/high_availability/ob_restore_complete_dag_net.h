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

#ifndef OCEANBASE_STORAGE_RESTORE_COMPLETE_DAG_NET_
#define OCEANBASE_STORAGE_RESTORE_COMPLETE_DAG_NET_

#include "ob_restore_dag_net.h"
#include "ob_restore_helper.h"
#include "ob_storage_ha_struct.h"

namespace oceanbase
{
namespace storage
{
struct ObCompleteRestoreCtx : public ObIHADagNetCtx
{
public:
  ObCompleteRestoreCtx();
  virtual ~ObCompleteRestoreCtx();
  void reset();
  void reuse();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() override;
  virtual bool is_valid() const override;

public:
  uint64_t tenant_id_;
  ObRestoreTask task_;
  int64_t start_ts_;
  int64_t finish_ts_;
  INHERIT_TO_STRING_KV("ObIHADagNetCtx", ObIHADagNetCtx, K_(tenant_id), K_(task),
                          K_(start_ts), K_(finish_ts));
private:
  DISALLOW_COPY_AND_ASSIGN(ObCompleteRestoreCtx);
};

struct ObCompleteRestoreParam : public share::ObIDagInitParam
{
public:
  ObCompleteRestoreParam();
  virtual ~ObCompleteRestoreParam() {}
  virtual bool is_valid() const override;
  void reset();
  VIRTUAL_TO_STRING_KV(K_(task), K_(result));

public:
  ObRestoreTask task_;
  int32_t result_;
};

class ObCompleteRestoreDagNet : public share::ObIDagNet
{
public:
  ObCompleteRestoreDagNet();
  virtual ~ObCompleteRestoreDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  virtual int deal_with_cancel() override;
  ObCompleteRestoreCtx *get_complete_ctx() { return &ctx_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(ctx));
private:
  int start_running_for_complete_();
  int update_restore_status_(ObLS *ls);
  int get_next_complete_status_(
      ObLS *ls, 
      const ObMigrationStatus current_complete_status,
      ObMigrationStatus &next_complete_status);
  int report_ls_meta_table_(ObLS *ls);
  int report_result_();
private:
  bool is_inited_;
  ObCompleteRestoreCtx ctx_;
  ObIRestoreHelper *helper_;
  DISALLOW_COPY_AND_ASSIGN(ObCompleteRestoreDagNet);
};

class ObCompleteRestoreDag : public ObRestoreDag
{
public:
  explicit ObCompleteRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObCompleteRestoreDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int prepare_ctx(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObRestoreDag", ObRestoreDag, KP(this));
protected:
  DISALLOW_COPY_AND_ASSIGN(ObCompleteRestoreDag);
};

class ObInitialCompleteRestoreDag : public ObCompleteRestoreDag
{
public:
  ObInitialCompleteRestoreDag();
  virtual ~ObInitialCompleteRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObCompleteRestoreDag", ObCompleteRestoreDag, KP(this));
private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialCompleteRestoreDag);
};

class ObInitialCompleteRestoreTask : public share::ObITask
{
public:
  ObInitialCompleteRestoreTask();
  virtual ~ObInitialCompleteRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialCompleteRestoreTask"), KP(this), KPC_(ctx));
private:
  int generate_restore_dags_();
  int record_server_event_();
private:
  bool is_inited_;
  ObCompleteRestoreCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialCompleteRestoreTask);
};

class ObWaitDataReadyRestoreDag : public ObCompleteRestoreDag
{
public:
  ObWaitDataReadyRestoreDag();
  virtual ~ObWaitDataReadyRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObCompleteRestoreDag", ObCompleteRestoreDag, KP(this));
private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObWaitDataReadyRestoreDag);
};

class ObWaitDataReadyRestoreTask : public share::ObITask
{
public:
  ObWaitDataReadyRestoreTask();
  virtual ~ObWaitDataReadyRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObWaitDataReadyRestoreTask"), KP(this), KPC(ctx_), KPC(helper_));
private:
  int get_wait_timeout_(int64_t &timeout);
  int wait_log_sync_();
  int wait_log_replay_sync_();
  int check_need_wait_(
      ObLS *ls,
      bool &need_wait);
  int update_ls_migration_status_wait_();
  int update_ls_migration_status_hold_();
  int check_all_tablet_ready_();
  int check_tablet_ready_(
      const common::ObTabletID &tablet_id,
      ObLS *ls,
      const int64_t timeout);
  int wait_log_replay_to_max_minor_end_scn_();
  int check_ls_and_task_status_(
      ObLS *ls);
  int record_server_event_();
  int init_timeout_ctx_(const int64_t timeout, ObTimeoutCtx &timeout_ctx);
private:
  static const int64_t IS_REPLAY_DONE_THRESHOLD_US = 3L * 1000 * 1000L;
  static const int64_t CHECK_CONDITION_INTERVAL = 200_ms;
  bool is_inited_;
  ObCompleteRestoreCtx *ctx_;
  ObIRestoreHelper *helper_;
  ObLSHandle ls_handle_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  palf::LSN log_sync_lsn_;
  share::SCN max_minor_end_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObWaitDataReadyRestoreTask);
};

class ObFinishCompleteRestoreDag : public ObCompleteRestoreDag
{
public:
  ObFinishCompleteRestoreDag();
  virtual ~ObFinishCompleteRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObCompleteRestoreDag", ObCompleteRestoreDag, KP(this));
private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishCompleteRestoreDag);
};

class ObFinishCompleteRestoreTask : public share::ObITask
{
public:
  ObFinishCompleteRestoreTask();
  virtual ~ObFinishCompleteRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObFinishCompleteRestoreTask"), KP(this), KPC_(ctx));
private:
  int generate_initial_complete_restore_dag_();
private:
  bool is_inited_;
  ObCompleteRestoreCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishCompleteRestoreTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_RESTORE_COMPLETE_DAG_NET_

