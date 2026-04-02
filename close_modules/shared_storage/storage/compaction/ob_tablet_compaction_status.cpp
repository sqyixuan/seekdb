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
#include "storage/compaction/ob_tablet_compaction_status.h"
#include "storage/blocksstable/ob_object_manager.h"

namespace oceanbase
{
namespace compaction
{
void ObTabletCompactionStatus::set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const
{
  opt.set_ss_tablet_compaction_status_object_opt(tablet_id_.id(), compaction_scn_);
}

OB_SERIALIZE_MEMBER_SIMPLE(ObTabletCompactionStatus, info_, compaction_scn_, task_idx_, root_macro_seq_);

} // namespace compaction
} // namespace oceanbase
