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

#ifndef STORAGE_COMPACTION_OB_TABLET_REFRESH_DAG_H_
#define STORAGE_COMPACTION_OB_TABLET_REFRESH_DAG_H_

#include "lib/container/ob_se_array.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
}
namespace compaction
{

struct ObTabletsRefreshSSTableParam : public share::ObIDagInitParam
{
public:
  ObTabletsRefreshSSTableParam();
  ObTabletsRefreshSSTableParam(const share::ObLSID &ls_id, const int64_t compaction_scn);
  virtual ~ObTabletsRefreshSSTableParam() = default;
  virtual bool is_valid() const override { return ls_id_.is_valid() && tablet_id_.is_valid() && compaction_scn_ > 0; }
  int assign(const ObTabletsRefreshSSTableParam &other);
  bool operator == (const ObTabletsRefreshSSTableParam &other) const;
  bool operator != (const ObTabletsRefreshSSTableParam &other) const { return !this->operator==(other); }
  int64_t get_hash() const;

  VIRTUAL_TO_STRING_KV(K_(compaction_scn), K_(ls_id), K_(tablet_id));
public:
  int64_t compaction_scn_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};


// cache major sstable from oss
class ObTabletsRefreshSSTableDag : public share::ObIDag
{
public:
  ObTabletsRefreshSSTableDag();
  virtual ~ObTabletsRefreshSSTableDag();
  int init_by_param(const share::ObIDagInitParam *param);
  virtual int create_first_task() override;
  virtual bool operator == (const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  const ObTabletsRefreshSSTableParam &get_param() const { return param_; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(param));
private:
  bool is_inited_;
  common::ObArenaAllocator arena_;
  ObTabletsRefreshSSTableParam param_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletsRefreshSSTableDag);
};


class ObTabletsRefreshSSTableTask : public share::ObITask
{
public:
  ObTabletsRefreshSSTableTask();
  virtual ~ObTabletsRefreshSSTableTask();
  int init();
protected:
  virtual int process() override;
  int decide_refreshed_scn_by_meta_list(
    const ObTabletID &tablet_id,
    const int64_t last_major_snapshot,
    int64_t &refresh_scn);
protected:
  bool is_inited_;
  int64_t compaction_scn_;
  share::ObLSID ls_id_;
  ObTabletsRefreshSSTableDag *base_dag_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletsRefreshSSTableTask);
};


} // compaction
} // oceanbase

#endif // STORAGE_COMPACTION_OB_TABLET_REFRESH_DAG_H_
