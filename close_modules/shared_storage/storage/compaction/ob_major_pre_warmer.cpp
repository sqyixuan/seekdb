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
#include "storage/compaction/ob_major_pre_warmer.h"
namespace oceanbase
{
using namespace blocksstable;
using namespace storage;
namespace compaction
{

bool ObMajorPreWarmerParam::is_valid() const
{
  return ObPreWarmerParam::is_valid() && is_inited_;
}

int ObMajorPreWarmerParam::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool use_fixed_percentage)
{
  int ret = OB_SUCCESS;
  UNUSED(ls_id);
  UNUSED(tablet_id);
  UNUSED(use_fixed_percentage);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_LIKELY(tenant_config.is_valid())) {
    pre_warm_level_ = static_cast<ObSSMajorPrewarmLevel>(static_cast<uint8_t>(
                      tenant_config->_ss_major_compaction_prewarm_level));
    is_inited_ = true;
  } else {
    pre_warm_level_ = ObSSMajorPrewarmLevel::PREWARM_META_AND_DATA_LEVEL;
    STORAGE_LOG(INFO, "invalid tenant config, use default pre warm level", KR(ret), K_(pre_warm_level));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
