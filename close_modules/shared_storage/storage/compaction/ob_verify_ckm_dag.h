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
#ifndef OB_STORAGE_COMPACTION_VERIFY_CKM_DAG_H_
#define OB_STORAGE_COMPACTION_VERIFY_CKM_DAG_H_
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/compaction/ob_batch_exec_dag.h"
namespace oceanbase
{
namespace share
{
struct ObTabletReplicaChecksumItem;
}
namespace storage
{
class ObLS;
}
namespace compaction
{
class ObVerifyCkmTask;
class ObLSObj;

struct ObVerifyCkmParam : public ObBatchExecParam<ObTabletID>
{
  ObVerifyCkmParam()
    : ObBatchExecParam(VERIFY_CKM)
  {}
  ObVerifyCkmParam(
    const share::ObLSID &ls_id,
    const int64_t merge_version)
    : ObBatchExecParam(VERIFY_CKM, ls_id, merge_version)
  {}
  virtual ~ObVerifyCkmParam() = default;
};

class ObVerifyCkmDag : public ObBatchExecDag<ObVerifyCkmTask, ObVerifyCkmParam>
{
public:
  ObVerifyCkmDag()
    : ObBatchExecDag(share::ObDagType::DAG_TYPE_VERIFY_CKM)
  {}
  virtual ~ObVerifyCkmDag() = default;
private:
  DISALLOW_COPY_AND_ASSIGN(ObVerifyCkmDag);
};

class ObVerifyCkmTask : public ObBatchExecTask<ObVerifyCkmTask, ObVerifyCkmParam>
{
public:
  ObVerifyCkmTask();
  virtual int inner_process() override;
private:
  bool need_batch_loop(const int64_t start_idx, const int64_t end_idx) const;
  int get_replica_ckms(const ObVerifyCkmParam &param,
                       const int64_t start_idx,
                       const int64_t end_idx,
                       ObIArray<share::ObTabletReplicaChecksumItem> &items);
  int get_tablet_and_verify(storage::ObLS &ls, const ObTabletID &tablet_id,
                            const int64_t compaction_scn,
                            const share::ObTabletReplicaChecksumItem &ckm);
  int loop_validate(
    storage::ObLS &ls,
    compaction::ObLSObj &ls_obj,
    const ObIArray<share::ObTabletReplicaChecksumItem> &items);
  int batch_update_tablet_state(
    const ObVerifyCkmParam &param,
    const int64_t start_idx,
    const int64_t end_idx,
    compaction::ObLSObj &ls_obj);
private:
  DISALLOW_COPY_AND_ASSIGN(ObVerifyCkmTask);
};



} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_VERIFY_CKM_DAG_H_
