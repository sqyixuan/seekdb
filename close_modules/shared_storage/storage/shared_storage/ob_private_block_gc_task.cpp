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

#define USING_LOG_PREFIX STORAGE

#include "ob_private_block_gc_task.h"
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_dir_manager.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
namespace storage
{
// The time interval for checking block gc trigger is 5s
const int64_t ObLSPrivateBlockGCHandler::PRIVATE_BLOCK_GC_INTERVAL = 5 * 1000 * 1000L;

// The time interval for block gc check is 24 * 720 * 5s = 1d
const int64_t ObPrivateBlockGCTask::GLOBAL_PRIVATE_BLOCK_GC_INTERVAL = 1 * 24 * 60 * 60 * 1000 * 1000L;

// The time interval for tablet gc is 0 (safe time discard temporarily)
const int64_t ObLSPrivateBlockGCHandler::DEFAULT_PRIVATE_TABLET_GC_SAFE_TIME = 0;

void ObPrivateBlockGCTask::runTimerTask()
{
  LOG_INFO("====== private block gc ======", K(ObLSPrivateBlockGCHandler::PRIVATE_BLOCK_GC_INTERVAL), KPC(this));
  loop_process_tablet_infos();
  loop_process_failed_macro_blocks(); 
  loop_check_tablet_gc();
  loop_check_tablet_version_gc();
  observer_start_macro_block_check();
  loop_check_ls_gc(); 
}

void ObPrivateBlockGCTask::observer_start_macro_block_check()
{
  int ret = OB_SUCCESS;
  const bool observer_start_macro_block_trigger = ATOMIC_LOAD(&observer_start_macro_block_trigger_);
  if (observer_start_macro_block_trigger) {
    LOG_INFO("====== observer start macro block check ======", K(ObLSPrivateBlockGCHandler::PRIVATE_BLOCK_GC_INTERVAL), KPC(this));

    ObLSIterator *iter = NULL;
    common::ObSharedGuard<ObLSIterator> guard;
    ObLSService *ls_svr = MTL(ObLSService*); 
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
      LOG_WARN("get log stream iter failed", KR(ret));
    } else if (OB_ISNULL(iter = guard.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", KR(ret));
    } else {
      ObLS *ls = NULL;
      int ls_cnt = 0;
      for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
        if (OB_ISNULL(ls)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls is NULL", KR(ret));
        } else {
          LOG_INFO("private block loop check", K(ls->get_ls_id()));
          ObLSPrivateBlockGCHandler &handler = ls->get_ls_private_block_gc_handler();
          obsys::ObRLockGuard lock(handler.wait_lock_);
          if (!handler.check_stop()) {
            handler.ls_macro_block_check();
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        if (ls_cnt > 0) {
          LOG_INFO("succeed to gc block", KR(ret), K(ls_cnt));
        } else {
          LOG_INFO("no logstream", KR(ret), K(ls_cnt));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_STORE(&observer_start_macro_block_trigger_, false);
    }
  }
}

int ObPrivateBlockGCTask::ls_gc(
    const ObLSItem &ls_item,
    const bool no_delete_tablet)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = ls_item.ls_id_;
  const int64_t ls_epoch = ls_item.epoch_;
  ObArray<ObPendingFreeTabletItem> gc_items;
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  int gc_macro_block_cnt = 0;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  ObPrivateBlockGCThread *gc_thread = NULL;
  bool all_tablet_deleted = false;
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_id), K(ls_epoch));
  } else if (FALSE_IT(gc_thread = gc_service->get_private_block_gc_thread())) {
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is not valid", K(ret), K(ls_id), K(ls_epoch));
  } else if (no_delete_tablet) {
  } else if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(TENANT_STORAGE_META_PERSISTER.get_items_from_pending_free_tablet_array(ls_id, ls_epoch, gc_items))) {
    LOG_WARN("failed to get_items_from_pending_free_tablet_array", K(ret), K(ls_id), K(ls_epoch));
  } else if (gc_items.count() > 0) {
    ObArenaAllocator allocator("PriBlkGCThrd");
    ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::GCTablet, allocator);
    for (int64_t i = 0; !is_stopped() && i < gc_items.count(); i++) {
      int tmp_ret = OB_SUCCESS;
      const ObPendingFreeTabletItem &gc_item = gc_items.at(i);
      ObPrivateBlockGCHandler handler(ls_id, ls_epoch, gc_item.tablet_id_, gc_item.tablet_meta_version_, 
          gc_item.tablet_transfer_seq_, gc_item.gc_type_, ls_item.min_macro_seq_, ls_item.max_macro_seq_, gc_items.at(i));
      if (OB_TMP_FAIL(gc_thread_guard.add_gc_task(handler))) {
        LOG_WARN("failed to add_gc_task", K(handler), K(gc_thread_guard));
      }
    }
    gc_thread_guard.wait_gc_task_finished();
    gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();
    if (gc_macro_block_cnt > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "private_dir_ls_gc", "tenant_id", MTL_ID(), "ls_id", ls_id.id(), "gc_macro_block_cnt", gc_macro_block_cnt);
    }

    ObArray<ObPendingFreeTabletItem> &thread_succ_gc_items = gc_thread_guard.get_succ_gc_items();
    if (is_stopped()) {
      LOG_INFO("private dir gc service stop, skip delete pending array", K(ls_id), K(ls_epoch), K(is_stopped()));
    } else if (thread_succ_gc_items.count() > gc_items.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("succ items is more than gc_items", K(ret), K(ls_id), K(gc_items.count()), K(thread_succ_gc_items.count()), K(thread_succ_gc_items));
    } else if (thread_succ_gc_items.count() < gc_items.count()) {
      if (thread_succ_gc_items.count() > 0
          && OB_FAIL(TENANT_STORAGE_META_PERSISTER.delete_items_from_pending_free_tablet_array(ls_id, ls_epoch, thread_succ_gc_items))) {
        LOG_WARN("failed to delete_items_from_pending_free_tablet_array", K(ret), K(ls_id), K(thread_succ_gc_items.count()), K(thread_succ_gc_items));
      }
    } else {
      all_tablet_deleted = true;
    }
  } else {
    all_tablet_deleted = true;
  }

  if (OB_FAIL(ret)) {
  } else if (is_stopped()) {
    LOG_INFO("private dir gc service stop, skip delete ls dir", K(ls_id), K(ls_epoch), K(is_stopped()));
  } else if (no_delete_tablet || all_tablet_deleted) {
    if (OB_FAIL(tfm->delete_ls_dir(ls_id.id(), ls_epoch))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("file has been delete", K(ls_id), K(ls_epoch));
      } else {
        LOG_WARN("failed to delete_ls_dir", K(ret), K(ls_id), K(ls_epoch));
      }
    } else if (OB_FAIL(TENANT_STORAGE_META_PERSISTER.ss_delete_tenant_ls_item(ls_id, ls_epoch))) {
      LOG_WARN("failed to delete_tenant_ls_item", K(ret), K(ls_id), K(ls_epoch));
    }
  } else {
    LOG_INFO("skip delete ls dir", K(ls_id), K(ls_epoch), K(no_delete_tablet), K(all_tablet_deleted));
  }

  LOG_INFO("finish ls_gc", K(ret), K(ls_id), K(ls_epoch), K(gc_items.count()), K(gc_macro_block_cnt), K(gc_items));
  return ret;
}

void ObPrivateBlockGCTask::loop_check_ls_gc() 
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== ls gc ======", KPC(this));
  ObTenantStorageMetaService *tsms = MTL(ObTenantStorageMetaService*);
  ObArray<ObLSItem> deleted_ls_items;
  ObArray<ObLSItem> abort_ls_items;
  if (OB_ISNULL(tsms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(tsms->get_ls_items_by_status(ObLSItemStatus::DELETED, deleted_ls_items))) {
    LOG_WARN("failed to get_ls_items_by_status", K(ret), K(deleted_ls_items.count()));
  } else if (OB_FAIL(tsms->get_ls_items_by_status(ObLSItemStatus::CREATE_ABORT, abort_ls_items))) {
    LOG_WARN("failed to get_ls_items_by_status", K(ret), K(abort_ls_items.count()));
  } else {
    for (int64_t i = 0; i < deleted_ls_items.count(); i++) {
      ret = OB_SUCCESS; // reset ret, continue gc ls
      const ObLSItem &item = deleted_ls_items.at(i);
      if (OB_FAIL(ls_gc(item))) {
        LOG_WARN("failed to gc deleted ls", K(ret), K(item));
      }
    }
    for (int64_t i = 0; i < abort_ls_items.count(); i++) {
      ret = OB_SUCCESS; // reset ret, continue gc ls
      const ObLSItem &item = abort_ls_items.at(i);
      if (OB_FAIL(ls_gc(item, true /* no_delete_tablet */))) {
        LOG_WARN("failed to gc abort ls", K(ret), K(item));
      }
    }
  }
}

void ObPrivateBlockGCTask::loop_check_tablet_gc()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== tablet gc ======", KPC(this));
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is NULL", KR(ret));
      } else {
        ObLSPrivateBlockGCHandler &handler = ls->get_ls_private_block_gc_handler();
        obsys::ObRLockGuard lock(handler.wait_lock_);
        if (!handler.check_stop()) {
          handler.gc_tablets();
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        LOG_INFO("succeed to gc block", KR(ret), K(ls_cnt));
      } else {
        LOG_INFO("no logstream", KR(ret), K(ls_cnt));
      }
    }
  }
}

void ObPrivateBlockGCTask::loop_process_tablet_infos()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== process tablet info ======", KPC(this));
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is NULL", KR(ret));
      } else {
        ObLSPrivateBlockGCHandler &handler = ls->get_ls_private_block_gc_handler();
        obsys::ObRLockGuard lock(handler.wait_lock_);
        if (!handler.check_stop()) {
          handler.process_tablet_infos_for_tablet_meta_gc();
          handler.process_tablet_infos_for_macro_block_check();
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        LOG_INFO("succeed to gc block", KR(ret), K(ls_cnt));
      } else {
        LOG_INFO("no logstream", KR(ret), K(ls_cnt));
      }
    }
  }
}

void ObPrivateBlockGCTask::loop_process_failed_macro_blocks()
{
  const int64_t count = get_failed_macro_count_();
  FLOG_INFO("====== process process failed blocks ======", K(count), KPC(this));
  if (count > 0) {
    ObIArray<blocksstable::MacroBlockId> &block_ids = get_failed_macro_ids_();
    delete_macro_blocks(block_ids);
  }
}

void ObPrivateBlockGCTask::loop_check_tablet_version_gc()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== tablet meta version gc ======", KPC(this));
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*); 
  const int64_t next_loop_tablet_meta_version_gc_ts = ATOMIC_LOAD(&next_loop_tablet_meta_version_gc_ts_);
  const int64_t curr_ts = ObTimeUtility::fast_current_time();
  if (next_loop_tablet_meta_version_gc_ts > curr_ts) {
  } else if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is NULL", KR(ret));
      } else {
        LOG_INFO("private block loop check", K(ls->get_ls_id()));
        ObLSPrivateBlockGCHandler &handler = ls->get_ls_private_block_gc_handler();
        obsys::ObRLockGuard lock(handler.wait_lock_);
        if (!handler.check_stop()) {
          handler.gc_tablets_meta_versions();
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      ATOMIC_STORE(&next_loop_tablet_meta_version_gc_ts_, curr_ts + GLOBAL_PRIVATE_BLOCK_GC_INTERVAL);
      if (ls_cnt > 0) {
        LOG_INFO("succeed to gc block", KR(ret), K(ls_cnt), K(next_loop_tablet_meta_version_gc_ts));
      } else {
        LOG_INFO("no logstream", KR(ret), K(ls_cnt), K(next_loop_tablet_meta_version_gc_ts));
      }
    }
  }
}

int ObLSPrivateBlockGCHandler::get_ls_min_macro_block_id_(
    const share::ObLSID &ls_id,
    uint64_t &min_macro_block_id)
{
  int ret = OB_SUCCESS;
  ObTenantStorageMetaService *tsms = MTL(ObTenantStorageMetaService*);
  ObArray<ObLSItem> ls_items;
  min_macro_block_id = UINT64_MAX;
  uint64_t max_macro_block_id = 0;
  if (OB_ISNULL(tsms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(tsms->get_ls_items_by_status(ObLSItemStatus::CREATED, ls_items))) {
    LOG_WARN("failed to get_ls_items_by_status", K(ret), K(ls_items.count()));
  } else if (0 == ls_items.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_items count is 0", K(ret), K(ls_id), KPC(this));
  } else {
    int i = 0;
    for (; OB_SUCC(ret) && i < ls_items.count(); i++) {
      ObLSItem &ls_item = ls_items.at(i);
      if (!ls_item.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls_item is invalid", K(ret), K(ls_id), K(ls_item), KPC(this), K(ls_items.count()), K(ls_items));
      } else if (ls_item.ls_id_ == ls_id) {
        min_macro_block_id = ls_item.min_macro_seq_;
        max_macro_block_id = ls_item.max_macro_seq_;
        break;
      }
    }
    if (i == ls_items.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is not in ls_itmes", K(ret), K(ls_id), K(ls_items));
    } else if (UINT64_MAX != max_macro_block_id
               || UINT64_MAX == min_macro_block_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls min/max_macro_block_id is unexpected", K(ret), K(min_macro_block_id), K(max_macro_block_id), K(ls_id), K(ls_items));
    }
  }
  return ret;
}

int ObLSPrivateBlockGCHandler::gc_tablet_meta_versions_(
    const ObTabletID &tablet_id,
    ObPrivateBlockGCThreadGuard &gc_thread_guard)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = ls_.get_ls_id();
  const int64_t ls_epoch = ls_.get_ls_epoch();
  int64_t current_tablet_version = -1;
  int64_t current_tablet_transfer_seq = -1;
  bool allow_tablet_version_gc = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (!gc_thread_guard.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gc_thread_guard is invalid", K(ret), K(gc_thread_guard));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", K(ret));
  } else if (OB_FAIL(t3m->get_current_version_for_tablet(ls_id, tablet_id, current_tablet_version, 
                                                         current_tablet_transfer_seq, allow_tablet_version_gc))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tablet is not exist", KR(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get_current_version_for_tablet", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (!allow_tablet_version_gc) {
    ret = OB_EAGAIN;
    LOG_INFO("old version is using", KR(ret), K(ls_id), K(tablet_id), K(current_tablet_version));
  } else if (current_tablet_version == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current_tablet_version is invalid", KR(ret), K(ls_id), K(tablet_id));
  } else {
    ObPrivateBlockGCHandler handler(ls_id, ls_epoch, tablet_id, current_tablet_version, current_tablet_transfer_seq);
    if (OB_FAIL(gc_thread_guard.add_gc_task(handler))) {
      LOG_WARN("failed to add_gc_task", K(handler), K(gc_thread_guard));
    }
  }

  return ret;
}

int ObLSPrivateBlockGCHandler::macro_block_check_(
    const ObTabletID &tablet_id,
    const uint64_t min_macro_block_id,
    const uint64_t max_macro_block_id,
    ObPrivateBlockGCThreadGuard &gc_thread_guard)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = ls_.get_ls_id();
  const int64_t ls_epoch = ls_.get_ls_epoch();
  int64_t current_tablet_version = -1;
  int64_t current_tablet_transfer_seq = -1;
  bool unuse_flag = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (tablet_id.is_ls_inner_tablet()) {
  } else if (!gc_thread_guard.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gc_thread_guard is invalid", K(ret), K(gc_thread_guard));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", K(ret));
  } else if (UINT64_MAX == min_macro_block_id
             || UINT64_MAX == max_macro_block_id
             || min_macro_block_id >= max_macro_block_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min/max_macro_block_id is invalid", KR(ret), K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else if (OB_FAIL(t3m->get_current_version_for_tablet(ls_id, tablet_id, current_tablet_version, 
                                                         current_tablet_transfer_seq, unuse_flag))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tablet is not exist", KR(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get_current_version_for_tablet", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (current_tablet_version == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current_tablet_version is invalid", KR(ret), K(ls_id), K(tablet_id));
  } else {
    ObPrivateBlockGCHandler handler(ls_id, ls_epoch, tablet_id, current_tablet_version, current_tablet_transfer_seq, min_macro_block_id, max_macro_block_id);
    if (OB_FAIL(gc_thread_guard.add_gc_task(handler))) {
      LOG_WARN("failed to add_gc_task", K(handler), K(gc_thread_guard));
    }
  }

  return ret;
}

void ObLSPrivateBlockGCHandler::gc_tablets_meta_versions()
{
  int ret = OB_SUCCESS;
  common::ObTabletIDArray tablet_ids;
  int64_t gc_macro_block_cnt = 0;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  ObPrivateBlockGCThread *gc_thread = NULL;
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else if (FALSE_IT(gc_thread = gc_service->get_private_block_gc_thread())) {
  } else if (OB_FAIL(ls_.get_tablet_svr()->get_all_tablet_ids(true, tablet_ids))) { 
    LOG_WARN("failed to get_all_tablet_ids", KR(ret), K(ls_));
  } else {
    ObArenaAllocator allocator("PriBlkGCThrd");
    ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::GCTabletMetaVersion, allocator);
    for (int i = 0; !check_stop() && i < tablet_ids.count(); i++) {
      if (OB_FAIL(gc_tablet_meta_versions_(tablet_ids.at(i), gc_thread_guard))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), K(ls_), K(tablet_ids.at(i)));
        }
        const ObTabletGCInfo tablet_info(tablet_ids.at(i));
        if (OB_FAIL(report_tablet_id_for_gc_service(tablet_info))) {
          LOG_WARN("failed to report tablet id for gc", K(ret), K(tablet_info));
        }
      }
    }
    gc_thread_guard.wait_gc_task_finished();
    gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();

    if (gc_macro_block_cnt > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "private_dir_loop_check_tablet_meta_gc", "tenant_id", MTL_ID(), 
          "ls_id", ls_.get_ls_id().id(), "gc_macro_block_cnt", gc_macro_block_cnt);
    }
  }
  LOG_INFO("finish loop check gc_tablets_meta_versions", K(ret), KPC(this), K(gc_macro_block_cnt));
}

void ObLSPrivateBlockGCHandler::ls_macro_block_check()
{
  int ret = OB_SUCCESS;
  common::ObTabletIDArray tablet_ids;
  int64_t gc_macro_block_cnt = 0;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  uint64_t min_macro_block_id = UINT64_MAX;
  uint64_t max_macro_block_id = 0;
  const share::ObLSID ls_id = ls_.get_ls_id();
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else if (OB_FAIL(get_ls_min_macro_block_id_(ls_id, min_macro_block_id))) {
    LOG_WARN("failed to get_ls_min_macro_block_id_", KR(ret), K(ls_id));
  } else if (FALSE_IT(max_macro_block_id = gc_service->get_mtl_start_max_block_id())) {
  } else if (UINT64_MAX == min_macro_block_id
             || UINT64_MAX == max_macro_block_id
             || min_macro_block_id == max_macro_block_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min/max_macro_block_id is invalid", KR(ret), K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else if (min_macro_block_id > max_macro_block_id) {
    LOG_INFO("max_macro_block_id is less min_macro_block_id, ls maybe create later", K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else if (OB_FAIL(ls_.get_tablet_svr()->get_all_tablet_ids(true, tablet_ids))) { 
    LOG_WARN("failed to get_all_tablet_ids", KR(ret), K(ls_));
  } else {
    ObPrivateBlockGCThread *gc_thread = gc_service->get_private_block_gc_thread();
    ObArenaAllocator allocator("PriBlkGCThrd");
    ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::MacroCheck, allocator);
    for (int i = 0; !check_stop() && i < tablet_ids.count(); i++) {
      if (OB_FAIL(macro_block_check_(tablet_ids.at(i), min_macro_block_id, max_macro_block_id, gc_thread_guard))) {
        LOG_WARN("failed to macro_block_check_", KR(ret), K(ls_), K(tablet_ids.at(i)));
        const ObTabletGCInfo tablet_info(tablet_ids.at(i), ProcessType::MacroCheck);
        if (OB_FAIL(report_tablet_id_for_gc_service(tablet_info))) {
          LOG_WARN("failed to report tablet id for gc", K(ret), K(tablet_info));
        }
      }
    }
    gc_thread_guard.wait_gc_task_finished();
    gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();

    if (gc_macro_block_cnt > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "macro_block_check_delete", "tenant_id", MTL_ID(), 
          "ls_id", ls_.get_ls_id().id(), "gc_macro_block_cnt", gc_macro_block_cnt);
    }
  }
  LOG_INFO("finish loop check macro_block_check_", K(ret), KPC(this), K(tablet_ids.count()), K(gc_macro_block_cnt));
}

void ObLSPrivateBlockGCHandler::process_tablet_infos_for_macro_block_check()
{
  int ret = OB_SUCCESS;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  uint64_t min_macro_block_id = UINT64_MAX;
  uint64_t max_macro_block_id = 0;
  const share::ObLSID ls_id = ls_.get_ls_id();
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else if (OB_FAIL(get_ls_min_macro_block_id_(ls_id, min_macro_block_id))) {
    LOG_WARN("failed to get_ls_min_macro_block_id_", KR(ret), K(ls_id));
  } else if (FALSE_IT(max_macro_block_id = gc_service->get_mtl_start_max_block_id())) {
  } else if (UINT64_MAX == min_macro_block_id
             || UINT64_MAX == max_macro_block_id
             || min_macro_block_id == max_macro_block_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min/max_macro_block_id is invalid", KR(ret), K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else if (min_macro_block_id > max_macro_block_id) {
    LOG_INFO("max_macro_block_id is less min_macro_block_id, ls maybe create later", K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else {
    ObPrivateBlockGCThread *gc_thread = gc_service->get_private_block_gc_thread();
    const int64_t count = get_tablet_infos_count_for_macro_block_check_();
    const ObIArray<ObTabletGCInfo> &tablet_infos = get_tablet_infos_for_macro_block_check_();
    if (count > 0) {
      LOG_INFO("process_tablet_infos_for_macro_block_check start", K(count), KPC(this));
      int64_t gc_macro_block_cnt = 0;
      ObArenaAllocator allocator("PriBlkGCThrd");
      ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::MacroCheck, allocator);
      for (int i = 0; !check_stop() && i < tablet_infos.count(); i++) {
        const ObTabletGCInfo &tablet_info = tablet_infos.at(i);
        if (ProcessType::MacroCheck != tablet_info.gc_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet_info.type and process_type is invalid", K(ret), K(tablet_info), K(ls_));
        } else if (OB_FAIL(macro_block_check_(tablet_info.tablet_id_, min_macro_block_id, max_macro_block_id, gc_thread_guard))) {
          LOG_WARN("failed to macro_block_check_", KR(ret), K(ls_), K(tablet_info));
          if (OB_FAIL(report_tablet_id_for_gc_service(tablet_info))) {
            LOG_WARN("failed to report_tablet_id_for_gc_service", K(ret), K(tablet_info));
          }
        }
      }
      gc_thread_guard.wait_gc_task_finished();
      gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();

      if (gc_macro_block_cnt > 0) {
        SERVER_EVENT_ADD("ss_macro_block_gc", "macro_block_check_delete", 
            "tenant_id", MTL_ID(), "ls_id", ls_.get_ls_id().id(), "gc_macro_block_cnt", gc_macro_block_cnt);
      }
      LOG_INFO("finish process_tablet_infos_for_macro_block_check", K(ret), KPC(this), K(gc_macro_block_cnt));
    }
  }
}

void ObLSPrivateBlockGCHandler::process_tablet_infos_for_tablet_meta_gc()
{
  int ret = OB_SUCCESS;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else {
    ObPrivateBlockGCThread *gc_thread = gc_service->get_private_block_gc_thread();
    const int64_t count = get_tablet_infos_count_for_gc_();
    const ObIArray<ObTabletGCInfo> &tablet_infos = get_tablet_infos_for_gc_();
    if (count > 0) {
      LOG_INFO("process_tablet_infos_for_tablet_meta_gc start", K(count), KPC(this));
      int64_t gc_macro_block_cnt = 0;
      ObArenaAllocator allocator("PriBlkGCThrd");
      ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::GCTabletMetaVersion, allocator);
      for (int i = 0; !check_stop() && i < tablet_infos.count(); i++) {
        const ObTabletGCInfo &tablet_info = tablet_infos.at(i);
        if (ProcessType::GCTabletMetaVersion != tablet_info.gc_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet_info.type and process_type is invalid", K(ret), K(tablet_info), K(ls_));
        } else if (OB_FAIL(gc_tablet_meta_versions_(tablet_info.tablet_id_, gc_thread_guard))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), K(ls_), K(tablet_info));
          }
          if (OB_FAIL(report_tablet_id_for_gc_service(tablet_info))) {
            LOG_WARN("failed to report_tablet_id_for_gc_service", K(ret), K(tablet_info));
          }
        }
      }
      gc_thread_guard.wait_gc_task_finished();
      gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();

      if (gc_macro_block_cnt > 0) {
        SERVER_EVENT_ADD("ss_macro_block_gc", "private_dir_tablet_meta_gc", 
            "tenant_id", MTL_ID(), "ls_id", ls_.get_ls_id().id(), "gc_macro_block_cnt", gc_macro_block_cnt);
      }
      LOG_INFO("finish process_tablet_infos_for_tablet_meta_gc", K(ret), KPC(this), K(gc_macro_block_cnt));
    }
  }
}

void ObLSPrivateBlockGCHandler::gc_tablets()
{
  int ret = OB_SUCCESS;
  ObArray<ObPendingFreeTabletItem> gc_items;
  const int64_t curr_ts = ObTimeUtility::fast_current_time();
  const ObLSID ls_id = ls_.get_ls_id();
  const int64_t ls_epoch = ls_.get_ls_epoch();
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  int gc_macro_block_cnt = 0;
  ObPrivateBlockGCThread *gc_thread = NULL;
  uint64_t min_macro_block_id = 0;
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else if (next_tablet_gc_ts_ > curr_ts) {
  } else if (OB_FAIL(get_ls_min_macro_block_id_(ls_id, min_macro_block_id))) {
    LOG_WARN("failed to get_ls_min_macro_block_id_", KR(ret), K(ls_id));
  } else if (UINT64_MAX == min_macro_block_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min_macro_block_id is invalid", KR(ret), K(ls_id));
  } else if (FALSE_IT(gc_thread = gc_service->get_private_block_gc_thread())) {
  } else if (OB_FAIL(TENANT_STORAGE_META_PERSISTER.get_items_from_pending_free_tablet_array(ls_id, ls_epoch, gc_items))) {
    LOG_WARN("failed to get_items_from_pending_free_tablet_array", K(ret), K(ls_));
  } else if (gc_items.count() > 0) {
    LOG_INFO("tablet gc start", K(ret), K(gc_items.count()), K(ls_), K(gc_items));
    ObArenaAllocator allocator("PriBlkGCThrd");
    ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::GCTablet, allocator);
    for (int64_t i = 0; !check_stop() && i < gc_items.count(); i++) {
      const ObPendingFreeTabletItem &gc_item = gc_items.at(i);
      int64_t safe_ts = gc_item.free_time_ + gc_service->get_private_tablet_gc_safe_time();
      if (GCTabletType::DropLS == gc_item.gc_type_) {
        // ls maybe has not been dropped, drop inner tablet will cause some errors when restart observer
      } 
      // safe time discard temporarily : private_tablet_gc_safe_time_ is 0
      else if (0 == gc_service->get_private_tablet_gc_safe_time()
          || curr_ts >= safe_ts) {
        ObPrivateBlockGCHandler handler(ls_id, ls_epoch, gc_item.tablet_id_, gc_item.tablet_meta_version_, gc_item.tablet_transfer_seq_, gc_item.gc_type_, min_macro_block_id, UINT64_MAX /* not ls replica delete */, gc_item);
        if (true) {
          if (OB_FAIL(gc_thread_guard.add_gc_task(handler))) {
            LOG_WARN("failed to add_gc_task", K(handler), K(gc_thread_guard));
          }
        } 
      } else {
        next_tablet_gc_ts_ = safe_ts;
        LOG_INFO("tablet gc need wait", K(ret), K(gc_item), K(ls_), K(safe_ts), K(i), K(gc_items.count()), K(gc_service->get_private_tablet_gc_safe_time()));
        break;
      }
    }
    gc_thread_guard.wait_gc_task_finished();
    gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();
    ObArray<ObPendingFreeTabletItem> &thread_succ_gc_items = gc_thread_guard.get_succ_gc_items();

    if (thread_succ_gc_items.count() > gc_items.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("succ items is more than gc_items", K(ret), K(ls_), K(gc_items.count()), K(thread_succ_gc_items.count()), K(thread_succ_gc_items));
    } else if (check_stop()) {
      LOG_INFO("ls is offline, skip delete pending_free_tablet_array", K(ls_), K(gc_items.count()), K(gc_macro_block_cnt), K(thread_succ_gc_items.count()));
    } else if (thread_succ_gc_items.count() > 0
        && OB_FAIL(TENANT_STORAGE_META_PERSISTER.delete_items_from_pending_free_tablet_array(ls_id, ls_epoch, thread_succ_gc_items))) {
      LOG_WARN("failed to delete_items_from_pending_free_tablet_array", K(ret), K(ls_), K(thread_succ_gc_items.count()), K(thread_succ_gc_items));
    }


    if (gc_macro_block_cnt > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "private_dir_tablet_gc", "tenant_id", MTL_ID(), "ls_id", ls_id.id(), "gc_macro_block_cnt", gc_macro_block_cnt);
    }
    LOG_INFO("finish gc_tablets", K(ret), K(ls_), K(gc_items.count()), K(gc_macro_block_cnt), K(thread_succ_gc_items.count()), K(gc_items), K(thread_succ_gc_items));
  }
}

int ObLSPrivateBlockGCHandler::report_tablet_id_for_gc_service(
    const ObTabletGCInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  switch (tablet_info.gc_type_) {
  case ProcessType::GCTabletMetaVersion:
    if (OB_FAIL(gc_tablet_info_queue_.add_item(tablet_info))) {
      LOG_WARN("failed to add_item", K(ret), K(tablet_info));
    }
    break;
  case ProcessType::MacroCheck:
    if (OB_FAIL(macro_block_check_tablet_info_queue_.add_item(tablet_info))) {
      LOG_WARN("failed to add_item", K(ret), K(tablet_info));
    }
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("process type is invalid", KR(ret), K(tablet_info));
  }
  return ret;
}

int ObPrivateBlockGCHandler::get_blocks_for_tablet(
  int64_t tablet_meta_version,
  ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  block_ids.reset();
  ObTenantStorageMetaService *tsms = MTL(ObTenantStorageMetaService*);
  if (OB_ISNULL(tsms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(tsms->get_private_blocks_for_tablet(ls_id_, ls_epoch_, tablet_id_, tablet_meta_version, 
        transfer_seq_, block_ids))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("block is deleted", K(ret), KPC(this));
    }
    LOG_WARN("failed to get_private_blocks_for_tablet", K(ret), KPC(this));
  }
  return ret;
}

int ObPrivateBlockGCHandler::list_tablet_meta_version(
  ObIArray<int64_t> &tablet_versions)
{
  int ret = OB_SUCCESS;
  int64_t tablet_meta_version = max_tablet_meta_version_;
  bool is_exist = false;
  tablet_versions.reset();
  do {
    ObStorageObjectOpt opt;
    opt.set_ss_private_tablet_meta_object_opt(ls_id_.id(), tablet_id_.id(), tablet_meta_version, transfer_seq_);
    if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, ls_epoch_, is_exist))) {
      LOG_WARN("fail to check_meta_existence", K(ret), KPC(this));
    } else if (true == is_exist
        && OB_FAIL(tablet_versions.push_back(tablet_meta_version))) {
      LOG_WARN("fail to push_back", K(ret), KPC(this));
    }
    tablet_meta_version--;
  } while (OB_SUCC(ret) && is_exist);
  return ret;
}

int ObPrivateBlockGCHandler::delete_tablet_meta_version(
  int64_t tablet_meta_version)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId file_id;
  ObStorageObjectOpt opt;
  opt.set_ss_private_tablet_meta_object_opt(ls_id_.id(), tablet_id_.id(), tablet_meta_version, transfer_seq_);
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(tfm->delete_file(file_id, ls_epoch_))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(file_id), KPC(this));
    }
  }
  return ret;
}

int ObPrivateBlockGCHandler::try_delete_tablet_meta_dir()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.delete_tablet_meta_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(),
          ls_id_.id(), ls_epoch_, tablet_id_.id()))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete_tablet_meta_tablet_id_dir", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObPrivateBlockGCHandler::try_delete_tablet_data_dir()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.delete_tablet_data_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(),
              tablet_id_.id(), transfer_seq_))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), KPC(this));
    } else if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      LOG_INFO("there are some file in dir, ls migration maybe in process", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete_tablet_data_tablet_id_transfer_seq_dir", K(ret), KPC(this));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.delete_tablet_data_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(),
          tablet_id_.id()))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), KPC(this));
    } else if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      LOG_INFO("there are some file in dir, other transfer_seq maybe exist", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete_tablet_data_tablet_id_dir", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObPrivateBlockGCHandler::get_block_ids_from_dir(
  ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(tfm->list_private_macro_file(tablet_id_.id(), transfer_seq_, block_ids))) {
    LOG_WARN("failed to list private macro file", K(ret), KPC(this));
  }
  return ret;
}

int ObPrivateBlockGCHandler::gc_tablet()
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> tablet_versions;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  bool allow = false;
  if (GCTabletType::InvalidType == gc_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc_type is invalid", K(ret), KPC(this));
  } else if ((GCTabletType::DropLS == gc_type_ && UINT64_MAX == ls_max_block_id_)
             || ls_min_block_id_ >= ls_max_block_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_max_block_id_ is invalid", K(ret), KPC(this));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", K(ret), KPC(this));
  } else if (OB_FAIL(t3m->check_allow_tablet_gc(tablet_id_, transfer_seq_, allow))) {
    LOG_WARN("failed to check_allow_tablet_gc", K(ret), KPC(this));
  } else if (!allow) {
    ret = OB_EAGAIN;
    LOG_INFO("tablet not allow be gc", K(ret), KPC(this));
  } else if (GCTabletType::DropLS == gc_type_
             && OB_FAIL(TENANT_STORAGE_META_PERSISTER.ss_delete_tablet_current_version(
                  tablet_id_, ls_id_, ls_epoch_))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to ss_delete_tablet_current_version", K(ret), KPC(this));
    }
  } else if (OB_FAIL(list_tablet_meta_version(tablet_versions))) {
    LOG_WARN("failed to list_tablet_meta_version", K(ret), KPC(this));
  } else if (tablet_versions.count() == 0) {
    LOG_INFO("there is not tablet_versions, maybe have been gc", KR(ret), KPC(this));
  } else if (OB_FAIL(ObBlockGCHandler::gc_tablet(tablet_versions, ls_min_block_id_, ls_max_block_id_))) {
    LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), KPC(this), K(tablet_versions.count()), K(tablet_versions));
  }
  LOG_INFO("finish private tablet gc", K(ret), KPC(this), K(tablet_versions));
  return ret;
}

int ObPrivateBlockGCHandler::gc_tablet_meta_versions()
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> tablet_versions;
  if (GCTabletType::InvalidType != gc_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta gc, gc_type is valid", K(ret), KPC(this));
  } else if (OB_FAIL(list_tablet_meta_version(tablet_versions))) {
    LOG_WARN("failed to list_tablet_meta_version", K(ret), KPC(this));
  } else if (tablet_versions.count() == 0) {
    LOG_INFO("there is not tablet_versions, maybe have been gc", KR(ret), KPC(this));
  } else if (tablet_versions.count() == 1) {
    // only one version not be gc
  } else if (OB_FAIL(ObBlockGCHandler::gc_tablet_meta_versions(tablet_versions, max_tablet_meta_version_))) {
    LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), KPC(this), K(tablet_versions.count()), K(tablet_versions));
  }
  return ret;
}

int ObPrivateBlockGCHandler::macro_block_check()
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> tablet_versions;
  BlockCollectOP op;
  if (GCTabletType::InvalidType != gc_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta gc, gc_type is valid", K(ret), KPC(this));
  } else if (OB_FAIL(list_tablet_meta_version(tablet_versions))) {
    LOG_WARN("failed to list_tablet_meta_version", K(ret), KPC(this));
  } else if (tablet_versions.count() == 0) {
    LOG_INFO("there is not tablet_versions, maybe have been gc", KR(ret), KPC(this));
  } else if (OB_FAIL(build_macro_block(tablet_versions, -1, op))) {
    LOG_WARN("failed to list_macro_block", K(ret), KPC(this), K(tablet_versions));
  } else if (OB_FAIL(ObBlockGCHandler::macro_block_check(op.get_result_block_id_set(), ls_min_block_id_, ls_max_block_id_))) {
    LOG_WARN("failed to macro_block_check", K(ret), KPC(this));
  }
  return ret;
}

void ObPrivateBlockGCThread::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObPrivateBlockGCHandler *handler = NULL;
  const ObPendingFreeTabletItem *extra_info = NULL;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is NULL", KR(ret), KPC(this));
  } else if (!is_valid_ctx()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is invalid", KR(ret), KPC(this));
  } else if (FALSE_IT(handler = static_cast<ObPrivateBlockGCHandler*>(task))) {
  } else {
    switch (get_gc_task_ctx()->process_type_) {
    case ProcessType::GCTabletMetaVersion:
      if(OB_FAIL(handler->gc_tablet_meta_versions())) {
        LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), KPC(this), KPC(handler));
      }
      break;
    case ProcessType::GCTablet:
      if (OB_FAIL(handler->gc_tablet())) {
        LOG_WARN("failed to gc_tablet", KR(ret), KPC(this), KPC(handler));
      } else if (OB_ISNULL(extra_info = handler->get_extra_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_info is NULL", KR(ret), KPC(this), KPC(handler));
      } else if (OB_FAIL(get_gc_task_ctx()->update_succ_gc_items(*extra_info))) {
        LOG_WARN("failed to update_succ_gc_items", KR(ret), KPC(this), KPC(handler));
      }
      break;
    case ProcessType::MacroCheck:
      if(OB_FAIL(handler->macro_block_check())) {
        LOG_WARN("failed to macro_block_check", KR(ret), KPC(this), KPC(handler));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("process type is invalid", KR(ret), KPC(this), KPC(handler));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (handler->gc_macro_block_cnt_ > 0) {
    get_gc_task_ctx()->update_gc_macro_block_cnt(handler->gc_macro_block_cnt_);
  }
  handle_end(task);
}

} /* namespace storage */
} /* namespace oceanbase */
