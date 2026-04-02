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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PRIVATE_BLOCK_GC_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PRIVATE_BLOCK_GC_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/ob_block_gc_handler.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_block_gc_thread.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
namespace checkpoint
{
class ObTabletGCService;
}

typedef common::hash::ObHashSet<blocksstable::MacroBlockId, common::hash::NoPthreadDefendMode> BlockIDSet;

enum ProcessType
{
  GCTabletMetaVersion = 0,
  GCTablet = 1,
  MacroCheck = 2
};

struct ObTabletGCInfo
{
  ObTabletGCInfo()
    : tablet_id_(),
      gc_type_(ProcessType::GCTabletMetaVersion)
  {}
  ObTabletGCInfo(
      const ObTabletID &tablet_id,
      ProcessType gc_type = ProcessType::GCTabletMetaVersion)
    : tablet_id_(tablet_id),
      gc_type_(gc_type)
  {}
  ~ObTabletGCInfo()
  {
    reset();
  }
  void reset()
  {
    tablet_id_.reset();
    gc_type_ = ProcessType::GCTabletMetaVersion;
  }

  TO_STRING_KV(K_(tablet_id), K_(gc_type));

  ObTabletID tablet_id_;
  ProcessType gc_type_;
};

template <typename T>
class DoubleQueue
{
public:
  DoubleQueue()
    : pos_(0) 
  {
    double_queue_[0].set_tenant_id(MTL_ID());
    double_queue_[1].set_tenant_id(MTL_ID());
  }

  ObIArray<T>& get_queue()
  {
    SpinWLockGuard guard(lock_);
    int tmp_pos = pos_;
    pos_ = (pos_ + 1) % 2;
    double_queue_[pos_].reset();
    return double_queue_[tmp_pos];
  }

  int64_t get_item_count()
  {
    SpinRLockGuard guard(lock_);
    return double_queue_[pos_].count();
  }

  int add_item(const T &item)
  {
    int ret = OB_SUCCESS;
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(double_queue_[pos_].push_back(item))) {
      STORAGE_LOG(WARN, "failed to push_back", K(ret), K(item));
    }
    return ret;
  }

  int add_items(const ObIArray<T> &items)
  {
    int ret = OB_SUCCESS;
    SpinWLockGuard guard(lock_);
    const int64_t queue_size = (items.count() + double_queue_[pos_].count()) * sizeof (T);
    if (queue_size > 1000 * 1000 * 1000) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "add items too much", K(ret), K(queue_size), K(items.count()), 
          K(double_queue_[pos_].count()), K(sizeof (T)), K(pos_));
    } else {
      for (int i = 0; i < items.count(); i++) {
        const T &item = items.at(i);
        if (OB_FAIL(double_queue_[pos_].push_back(item))) {
          STORAGE_LOG(WARN, "failed to push_back", K(ret), K(item));
        }
      }
    }
    return ret;
  }
  void reset()
  {
    SpinWLockGuard guard(lock_);
    double_queue_[0].reset();
    double_queue_[1].reset();
    pos_ = 0;
  }

  TO_STRING_KV(K_(pos));
private:
  common::SpinRWLock lock_;
  int pos_;
  ObArray<T> double_queue_[2];
};

class ObPrivateBlockGCThreadGuard;
class ObPrivateBlockGCThread : public ObBlockGCThread<ObPrivateBlockGCThreadGuard>
{
public:
  int init()
  { return ObBlockGCThread<ObPrivateBlockGCThreadGuard>::init(lib::TGDefIDs::ObPrivateBlockGCThread); }
  virtual ~ObPrivateBlockGCThread() {}
  void handle(void *task);
  static constexpr int64_t THREAD_NUM = 10;
  static constexpr int64_t MINI_MODE_THREAD_NUM = 1;
  static constexpr int64_t MAX_BLOCK_GC_TASK_NUM = 1000 * 1000;
};

class ObPrivateBlockGCThreadGuard : public ObBlockGCThreadGuard<ObPrivateBlockGCThreadGuard>
{
public:
  ObPrivateBlockGCThreadGuard(
      ObPrivateBlockGCThread &gc_thread,
      ProcessType process_type,
      common::ObArenaAllocator &allocator)
    : ObBlockGCThreadGuard(gc_thread, *this),
      process_type_(process_type),
      allocator_(allocator),
      gc_macro_block_cnt_(0)
  {}
  ~ObPrivateBlockGCThreadGuard() {}
  bool is_valid() 
  { return true; }
  common::ObArenaAllocator &get_allocator()
  { return allocator_; }
  void update_gc_macro_block_cnt(const int64_t gc_cnt)
  { ATOMIC_FAA(&gc_macro_block_cnt_, gc_cnt); }
  int64_t get_gc_macro_block_cnt() const
  { return gc_macro_block_cnt_; }
  int update_succ_gc_items(const ObPendingFreeTabletItem &extra_info)
  { 
    SpinWLockGuard guard(lock_);
    return succ_gc_items_.push_back(extra_info); 
  }
  ObArray<ObPendingFreeTabletItem>& get_succ_gc_items()
  { return succ_gc_items_; }
  INHERIT_TO_STRING_KV("ObBlockGCThreadGuard", ObBlockGCThreadGuard<ObPrivateBlockGCThreadGuard>, K_(process_type), K_(gc_macro_block_cnt));

  // todo: add same info in ObPrivateBlockGCHandler
  
  // true: tablet_meta_gc; false: tablet gc
  ProcessType process_type_;
  common::ObArenaAllocator &allocator_;
  int64_t gc_macro_block_cnt_;
  ObArray<ObPendingFreeTabletItem> succ_gc_items_;
  SpinRWLock lock_;
};

class ObLSPrivateBlockGCHandler
{
public:
  static const int64_t PRIVATE_BLOCK_GC_INTERVAL;
  static const int64_t DEFAULT_PRIVATE_TABLET_GC_SAFE_TIME;
  ObLSPrivateBlockGCHandler(ObLS &ls)
    : gc_tablet_info_queue_(),
      macro_block_check_tablet_info_queue_(),
      next_tablet_gc_ts_(0),
      is_set_stop_(false),
      ls_(ls)
  {}

  // ls operation
  void set_stop() { ATOMIC_STORE(&is_set_stop_, true); }
  void set_start() { ATOMIC_STORE(&is_set_stop_, false); }
  // todo add more check
  bool check_stop() { return ATOMIC_LOAD(&is_set_stop_) == true; }
  bool is_finish() { obsys::ObWLockGuard lock(wait_lock_, false); return lock.acquired(); }
  int offline()
  {
    set_stop();
    const int ret = is_finish() ? OB_SUCCESS : OB_EAGAIN;
    STORAGE_LOG(INFO, "block gc handler offline", KR(ret), KPC(this));
    return ret;
  }
  void online()
  {
    set_start();
    STORAGE_LOG(INFO, "block gc handler online", KPC(this));
  }

  // gc info operation
  int report_tablet_id_for_gc_service(const ObTabletGCInfo &tablet_info);

  // gc tablet operation
  void process_tablet_infos_for_macro_block_check();
  void process_tablet_infos_for_tablet_meta_gc();
  void gc_tablets_meta_versions();
  void ls_macro_block_check();
  void gc_tablets();
  void reset()
  {
    gc_tablet_info_queue_.reset();
    macro_block_check_tablet_info_queue_.reset();
    STORAGE_LOG(INFO, "block gc handler reset", KPC(this));
  }

  TO_STRING_KV(K_(gc_tablet_info_queue), K_(macro_block_check_tablet_info_queue), K_(next_tablet_gc_ts), K_(is_set_stop), K_(ls));

  obsys::ObRWLock wait_lock_;

private:
  ObIArray<ObTabletGCInfo>& get_tablet_infos_for_gc_()
  {
    return gc_tablet_info_queue_.get_queue();
  }
  int64_t get_tablet_infos_count_for_gc_()
  {
    return gc_tablet_info_queue_.get_item_count();
  }

  ObIArray<ObTabletGCInfo>& get_tablet_infos_for_macro_block_check_()
  {
    return macro_block_check_tablet_info_queue_.get_queue();
  }
  int64_t get_tablet_infos_count_for_macro_block_check_()
  {
    return macro_block_check_tablet_info_queue_.get_item_count();
  }

  int gc_tablet_meta_versions_(
      const ObTabletID &tablet_id,
      ObPrivateBlockGCThreadGuard &gc_thread_guard);

  int macro_block_check_(
      const ObTabletID &tablet_id,
      const uint64_t min_macro_block_id,
      const uint64_t max_macro_block_id,
      ObPrivateBlockGCThreadGuard &gc_thread_guard);
  int get_ls_min_macro_block_id_(
      const share::ObLSID &ls_id,
      uint64_t &min_macro_block_id);
private:

  DoubleQueue<ObTabletGCInfo> gc_tablet_info_queue_;
  DoubleQueue<ObTabletGCInfo> macro_block_check_tablet_info_queue_;

  int64_t next_tablet_gc_ts_;
  bool is_set_stop_;
  ObLS &ls_;
};

class ObPrivateBlockGCTask : public common::ObTimerTask
{
public:
  static const int64_t GLOBAL_PRIVATE_BLOCK_GC_INTERVAL;
  ObPrivateBlockGCTask(checkpoint::ObTabletGCService &tablet_gc_service)
    : is_stopped_(false),
      next_loop_tablet_meta_version_gc_ts_(0),
      tablet_gc_service_(tablet_gc_service),
      failed_macro_block_id_queue_(),
      mtl_start_max_block_id_(0),
      observer_start_macro_block_trigger_(false)
  {}
  virtual ~ObPrivateBlockGCTask() {}

  virtual void runTimerTask();
  void loop_check_tablet_version_gc();
  void loop_check_tablet_gc();
  void loop_process_tablet_infos();
  int ls_gc(
    const ObLSItem &ls_item,
    const bool no_delete_tablet = false);
  void loop_check_ls_gc();
  void observer_start_macro_block_check();
  void loop_process_failed_macro_blocks();

  int report_failed_macro_ids(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    return failed_macro_block_id_queue_.add_items(block_ids);
  }

  void set_is_stopped() { ATOMIC_STORE(&is_stopped_, true); }
  void clear_is_stopped() { ATOMIC_STORE(&is_stopped_, false); }
  bool is_stopped() { return ATOMIC_LOAD(&is_stopped_); }
  uint64_t get_mtl_start_max_block_id() 
  { return mtl_start_max_block_id_; }
  void set_mtl_start_max_block_id(const uint64_t mtl_start_max_block_id) 
  { 
    mtl_start_max_block_id_ = mtl_start_max_block_id; 
    STORAGE_LOG(INFO, "set_mtl_start_max_block_id", KPC(this));
  }
  void set_observer_start_macro_block_id_trigger()
  { 
    ATOMIC_STORE(&observer_start_macro_block_trigger_, true); 
    STORAGE_LOG(INFO, "set_observer_start_macro_block_id_trigger", KPC(this));
  }
  TO_STRING_KV(K_(is_stopped), K_(next_loop_tablet_meta_version_gc_ts), K_(mtl_start_max_block_id), K_(observer_start_macro_block_trigger));
private:
  int64_t get_failed_macro_count_()
  {
    return failed_macro_block_id_queue_.get_item_count();
  }

  ObIArray<blocksstable::MacroBlockId>& get_failed_macro_ids_()
  {
    return failed_macro_block_id_queue_.get_queue();
  }

private:
  bool is_stopped_;
  int64_t next_loop_tablet_meta_version_gc_ts_;
  checkpoint::ObTabletGCService &tablet_gc_service_;
  DoubleQueue<blocksstable::MacroBlockId> failed_macro_block_id_queue_;
  // mtl init set max_block_id_
  uint64_t mtl_start_max_block_id_;
  // observer start trigger
  bool observer_start_macro_block_trigger_;
};

class ObPrivateBlockGCHandler : public ObBlockGCHandler
{
public:
  ObPrivateBlockGCHandler(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      const int64_t max_tablet_meta_version,
      const int64_t transfer_seq,
      const GCTabletType gc_type,
      const uint64_t ls_min_block_id,
      const uint64_t ls_max_block_id,
      const ObPendingFreeTabletItem &extra_info)
    : ObBlockGCHandler(tablet_id),
      ls_id_(ls_id),
      ls_epoch_(ls_epoch),
      max_tablet_meta_version_(max_tablet_meta_version),
      transfer_seq_(transfer_seq),
      gc_type_(gc_type),
      ls_min_block_id_(ls_min_block_id),
      ls_max_block_id_(ls_max_block_id),
      extra_info_(&extra_info)
  {}

  ObPrivateBlockGCHandler(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      const int64_t max_tablet_meta_version,
      const int64_t transfer_seq)
    : ObBlockGCHandler(tablet_id),
      ls_id_(ls_id),
      ls_epoch_(ls_epoch),
      max_tablet_meta_version_(max_tablet_meta_version),
      transfer_seq_(transfer_seq),
      gc_type_(GCTabletType::InvalidType),
      ls_min_block_id_(0),
      ls_max_block_id_(UINT64_MAX),
      extra_info_(NULL)
  {}

  ObPrivateBlockGCHandler(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      const int64_t max_tablet_meta_version,
      const int64_t transfer_seq,
      const uint64_t ls_min_block_id,
      const uint64_t ls_max_block_id)
    : ObBlockGCHandler(tablet_id),
      ls_id_(ls_id),
      ls_epoch_(ls_epoch),
      max_tablet_meta_version_(max_tablet_meta_version),
      transfer_seq_(transfer_seq),
      gc_type_(GCTabletType::InvalidType),
      ls_min_block_id_(ls_min_block_id),
      ls_max_block_id_(ls_max_block_id),
      extra_info_(NULL)
  {}

  int gc_tablet();
  int gc_tablet_meta_versions();
  int macro_block_check();

  virtual int list_tablet_meta_version(
    ObIArray<int64_t> &tablet_versions);

  virtual int get_blocks_for_tablet(
    int64_t tablet_meta_version,
    ObIArray<blocksstable::MacroBlockId> &block_ids);
  
  virtual int delete_tablet_meta_version(
    int64_t tablet_meta_version);

  virtual int try_delete_tablet_meta_dir();
  virtual int try_delete_tablet_data_dir();

  virtual int get_block_ids_from_dir(
    ObIArray<blocksstable::MacroBlockId> &block_ids);

  virtual int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &collect_check_error_block_op)
  { return OB_SUCCESS; }

  const ObPendingFreeTabletItem* get_extra_info()
  { 
    return extra_info_;
  }
  INHERIT_TO_STRING_KV("ObBlockGCHandler", ObBlockGCHandler, K_(ls_id), K_(ls_epoch), K_(max_tablet_meta_version), K_(gc_type), K_(transfer_seq), K_(ls_max_block_id));

private:
  const share::ObLSID ls_id_;
  const int64_t ls_epoch_;
  const int64_t max_tablet_meta_version_;
  const int64_t transfer_seq_;
  GCTabletType gc_type_;
  const uint64_t ls_min_block_id_;
  const uint64_t ls_max_block_id_;
  const ObPendingFreeTabletItem *extra_info_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PRIVATE_BLOCK_GC_H_ */
