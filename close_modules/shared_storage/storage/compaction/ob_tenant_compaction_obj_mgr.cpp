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
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
namespace compaction
{
/**
 * -------------------------------------------------------------------ObTenantCompactionObjMgr-------------------------------------------------------------------
 */
ObTenantCompactionObjMgr::ObTenantCompactionObjMgr()
  : is_inited_(false),
    svr_id_cache_(),
    ls_obj_mgr_()
{}

ObTenantCompactionObjMgr::~ObTenantCompactionObjMgr()
{}

int ObTenantCompactionObjMgr::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantCompactionObjMgr has inited", KR(ret));
  } else if (!GCTX.is_shared_storage_mode()) {
    FLOG_INFO("cluster is not shared storage mode, should not init ObTenantCompactionObjMgr", KR(ret));
  } else if (OB_FAIL(svr_id_cache_.init())) {
    LOG_WARN("failed to init svr id cache", KR(ret));
  } else if (OB_FAIL(ls_obj_mgr_.init())) {
    LOG_WARN("failed to init ls obj map", KR(ret));
  } else if (OB_FAIL(svr_obj_mgr_.init())) {
    LOG_WARN("failed to init svr obj map", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantCompactionObjMgr::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    ls_obj_mgr_.destroy();
    svr_obj_mgr_.destroy();
  }
}

int ObTenantCompactionObjMgr::refresh()
{
  int ret = OB_SUCCESS;
  bool exist_ls_leader_flag = false;
  ObCompactionObjBuffer obj_buf;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantCompactionObjMgr has not been inited", KR(ret));
  } else if (OB_FAIL(svr_id_cache_.refresh(false /*force_refresh*/))) {
    LOG_WARN("failed to svr id cache", KR(ret));
  } else if (OB_FAIL(obj_buf.init(true/*alloc_big_buf*/))) { // need R/W compaction_list
    LOG_WARN("failed to init obj buf", KR(ret));
  } else if (OB_FAIL(ls_obj_mgr_.refresh(exist_ls_leader_flag /*out_param*/, obj_buf))) {
    LOG_WARN("failed to refresh ls objs", KR(ret));
  } else if (OB_FAIL(svr_obj_mgr_.refresh(exist_ls_leader_flag /*in_param*/, obj_buf))) {
    LOG_WARN("failed to refresh svr objs", KR(ret));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
