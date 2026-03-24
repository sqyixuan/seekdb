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

#ifndef OCEABASE_STORAGE_SHARED_STORAGE_PREWARM_OB_LS_PREWARM_HANDLER_
#define OCEABASE_STORAGE_SHARED_STORAGE_PREWARM_OB_LS_PREWARM_HANDLER_

#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "share/ob_common_rpc_proxy.h"
#include "logservice/ob_log_base_type.h"
#include "deps/oblib/src/lib/hash/ob_hashset.h"
#include "storage/shared_storage/task/ob_sync_hot_micro_key_task.h"
#include "storage/shared_storage/task/ob_consume_hot_micro_key_task.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{

class ObSSLSPreWarmHandler : public logservice::ObIReplaySubHandler,
                             public logservice::ObIRoleChangeSubHandler,
                             public logservice::ObICheckpointSubHandler
{
public:
  ObSSLSPreWarmHandler();
  virtual ~ObSSLSPreWarmHandler();
  int init(ObLS *ls);
  void stop();
  void wait();
  void destroy();
  // for role change
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;
  // for replay
  virtual int replay(const void *buffer,
                     const int64_t nbytes,
                     const palf::LSN &lsn,
                     const share::SCN &scn) override final;
  // for checkpoint
  virtual share::SCN get_rec_scn() override final { return share::SCN::max_scn(); }
  virtual int flush(share::SCN &scn) override final;
  // for offline and online
  int offline();
  int online();
  ObLS *get_ls() const { return ls_; }
  int push_micro_cache_keys(const obrpc::ObLSSyncHotMicroKeyArg &arg);

private:
  int switch_role_to(const bool is_leader);

private:
  bool is_inited_;
  bool is_ls_stop_;
  volatile bool is_sync_hot_micro_key_task_stop_;
  volatile bool is_consume_hot_micro_key_task_stop_;
  bool is_leader_;
  ObLS *ls_;
  int64_t ls_id_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLSPreWarmHandler);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEABASE_STORAGE_SHARED_STORAGE_PREWARM_OB_LS_PREWARM_HANDLER_ */
