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
#ifndef OB_SHARE_STORAGE_COMPACTION_TENANT_COMPACTION_OBJ_MGR_H_
#define OB_SHARE_STORAGE_COMPACTION_TENANT_COMPACTION_OBJ_MGR_H_
#include "share/compaction/ob_compaction_timer_task_mgr.h"
#include "storage/compaction/ob_ls_compaction_obj_mgr.h"
#include "storage/compaction/ob_svr_compaction_obj_mgr.h"
namespace oceanbase
{
namespace compaction
{
class ObTenantCompactionObjMgr
{
public:
  ObTenantCompactionObjMgr();
  ~ObTenantCompactionObjMgr();
  static int mtl_init(ObTenantCompactionObjMgr *&mgr) { return mgr->init(); }
  int init();
  void destroy();
  int refresh();
  OB_INLINE ObLSCompactionObjMgr &get_ls_obj_mgr() { return ls_obj_mgr_; }
  OB_INLINE ObSvrCompactionObjMgr &get_svr_obj_mgr() { return svr_obj_mgr_; }
  OB_INLINE ObSvrIDCache &get_svr_id_cache() { return svr_id_cache_; }
private:
  bool is_inited_;
  ObSvrIDCache svr_id_cache_;
  ObLSCompactionObjMgr ls_obj_mgr_;
  // record current svr compaction status for follower, record all svr for leader
  ObSvrCompactionObjMgr svr_obj_mgr_;
};

#define MTL_LS_OBJ_MGR MTL(ObTenantCompactionObjMgr *)->get_ls_obj_mgr()
#define MTL_SVR_OBJ_MGR MTL(ObTenantCompactionObjMgr *)->get_svr_obj_mgr()
#define MTL_SVR_ID_CACHE MTL(ObTenantCompactionObjMgr *)->get_svr_id_cache()

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_TENANT_COMPACTION_OBJ_MGR_H_
