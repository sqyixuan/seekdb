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

#include "meta_store/ob_shared_storage_obj_meta.h"

namespace oceanbase
{
namespace storage
{

  OB_SERIALIZE_MEMBER(ObGCTabletMetaInfo, scn_, tablet_meta_create_ts_);
  OB_SERIALIZE_MEMBER(ObGCTabletMetaInfoList, tablet_version_arr_);


} // namespace storage
} // namespace oceanbase

