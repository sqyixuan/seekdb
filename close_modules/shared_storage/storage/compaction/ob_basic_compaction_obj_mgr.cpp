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
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_basic_compaction_obj_mgr.h"
#include "close_modules/shared_storage/storage/compaction/ob_tenant_compaction_obj_mgr.h"


namespace oceanbase
{
using namespace lib;
namespace compaction
{

void ObBasicObjHandleHelper::reset_obj_for_ObLSObj(ObBasicObjHandle<ObLSObj> *obj)
{
  MTL(ObTenantCompactionObjMgr*)->get_ls_obj_mgr().release_handle(*obj);
}

void ObBasicObjHandleHelper::reset_obj_for_ObCompactionReportObj(ObBasicObjHandle<ObCompactionReportObj> *obj)
{
  MTL(ObTenantCompactionObjMgr*)->get_svr_obj_mgr().release_handle(*obj);
}

} // namespace compaction
} // namespace oceanbase
