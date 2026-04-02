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
#include "storage/compaction/ob_tablet_id_obj.h"
#include "storage/blocksstable/ob_object_manager.h"

namespace oceanbase
{
namespace compaction
{

ObTabletIDObj::ObTabletIDObj(const common::ObTabletID &input_tablet_id)
  : version_(TABLET_ID_OBJ_VERSION_V1),
    reserved_(0),
    tablet_id_(input_tablet_id),
    compaction_scn_(0),
    last_major_root_macro_seq_(0),
    parallel_cnt_(0),
    cg_dir_cnt_(0)
{}

ObTabletIDObj::ObTabletIDObj(
    const common::ObTabletID &input_tablet_id,
    const int64_t compaction_scn,
    const int64_t last_major_root_macro_seq,
    const int64_t parallel_cnt,
    const int64_t cg_dir_cnt)
  : version_(TABLET_ID_OBJ_VERSION_V1),
    reserved_(0),
    tablet_id_(input_tablet_id),
    compaction_scn_(compaction_scn),
    last_major_root_macro_seq_(last_major_root_macro_seq),
    parallel_cnt_(parallel_cnt),
    cg_dir_cnt_(cg_dir_cnt)
{}

void ObTabletIDObj::reset()
{
  info_ = 0;
  tablet_id_.reset();
  compaction_scn_ = 0;
  last_major_root_macro_seq_ = 0;
  parallel_cnt_ = 0;
  cg_dir_cnt_ = 0;
}

bool ObTabletIDObj::is_valid() const
{
  return tablet_id_.is_valid() && compaction_scn_ > 0 && last_major_root_macro_seq_ >= 0
    && parallel_cnt_ > 0 && cg_dir_cnt_ > 0;
}

void ObTabletIDObj::set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const
{
  opt.set_ss_shared_tablet_id_object_opt(tablet_id_.id());
}

OB_SERIALIZE_MEMBER_SIMPLE(ObTabletIDObj, info_, compaction_scn_, last_major_root_macro_seq_, parallel_cnt_, cg_dir_cnt_);

} // namespace compaction
} // namespace oceanbase
