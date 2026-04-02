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

#ifndef OCEABASE_STORAGE_OB_PUBLIC_BLOCK_GC_SERVICE_
#define OCEABASE_STORAGE_OB_PUBLIC_BLOCK_GC_SERVICE_
#include "lib/task/ob_timer.h"
#include "lib/lock/ob_rwlock.h"
#include "logservice/ob_log_base_type.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tx_storage/ob_empty_shell_task.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_block_gc_handler.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_block_gc_thread.h"

namespace oceanbase
{
namespace storage
{
class ObLS;

typedef common::ObLinearHashMap<ObTabletID, bool, common::hash::NoPthreadDefendMode> TabletIDMap;

class ObPublicBlockGCThreadGuard;
class ObPublicBlockGCThread : public ObBlockGCThread<ObPublicBlockGCThreadGuard>
{
public:
  int init()
  { return ObBlockGCThread<ObPublicBlockGCThreadGuard>::init(lib::TGDefIDs::ObPublicBlockGCThread); }
  virtual ~ObPublicBlockGCThread() {}
  void handle(void *task);
  void handle_drop(void *task);
  static constexpr int64_t THREAD_NUM = 10;
  static constexpr int64_t MINI_MODE_THREAD_NUM = 1;
  static constexpr int64_t MAX_BLOCK_GC_TASK_NUM = 1000 * 1000;
};

struct BaseOP {
  BaseOP(
      const bool &is_paused,
      const bool &is_stopped)
    : is_paused_(is_paused),
      is_stopped_(is_stopped)
  {}
  virtual ~BaseOP() {}
  bool is_stopped() { return ATOMIC_LOAD(&is_stopped_) || ATOMIC_LOAD(&is_paused_); }
private:
  const bool &is_paused_;
  const bool &is_stopped_;
};

class ObPublicBlockGCThreadGuard : public ObBlockGCThreadGuard<ObPublicBlockGCThreadGuard>,
                                   public BaseOP
{
public:
  ObPublicBlockGCThreadGuard(
      ObPublicBlockGCThread &gc_thread,
      bool is_tablet_meta_gc,
      const bool &is_paused,
      const bool &is_stopped,
      common::ObArenaAllocator &allocator)
    : ObBlockGCThreadGuard(gc_thread, *this),
      BaseOP(is_paused, is_stopped),
      is_tablet_meta_gc_(is_tablet_meta_gc),
      allocator_(allocator),
      gc_macro_block_cnt_(0),
      next_gc_tablet_meta_ts_(0)
  {}
  ~ObPublicBlockGCThreadGuard() {}

  bool is_valid() 
  { return true; }

  bool is_tablet_meta_gc() 
  { return is_tablet_meta_gc_; }

  int64_t get_next_gc_tablet_meta_ts() const
  { return next_gc_tablet_meta_ts_; }

  int64_t get_gc_macro_block_cnt() const
  { return gc_macro_block_cnt_; }

  common::ObArenaAllocator& get_allocator()
  { return allocator_; }
  void update_gc_macro_block_cnt(const int64_t gc_cnt)
  { ATOMIC_FAA(&gc_macro_block_cnt_, gc_cnt); }
  void update_next_gc_tablet_meta_ts(const int64_t next_gc_tablet_meta_ts)
  {
    if (INT64_MAX != next_gc_tablet_meta_ts) {
      int64_t old_v = 0;
      int64_t new_v = next_gc_tablet_meta_ts;
      do {
        old_v = ATOMIC_LOAD(&next_gc_tablet_meta_ts_);
        if (old_v <= new_v) {
          break;
        }
      } while (ATOMIC_CAS(&next_gc_tablet_meta_ts_, old_v, new_v) != old_v);
    }
  }
  INHERIT_TO_STRING_KV("ObPublicBlockGCThread", ObBlockGCThreadGuard<ObPublicBlockGCThreadGuard>, K_(is_tablet_meta_gc), K_(next_gc_tablet_meta_ts));

private:
  // todo: add same info in ObPublicBlockGCHandler
  
  // true: tablet_meta_gc; false: tablet gc
  bool is_tablet_meta_gc_;
  common::ObArenaAllocator &allocator_;
  int64_t gc_macro_block_cnt_;
  int64_t next_gc_tablet_meta_ts_;
};

struct MarkTabletIDOP : public BaseOP
{
public:
  MarkTabletIDOP(
      const bool &is_paused, 
      const bool &is_stopped, 
      TabletIDMap &tablet_id_map)
    : BaseOP(is_paused, is_stopped),
      tablet_id_map_(tablet_id_map)
  {}
  virtual ~MarkTabletIDOP() {}
  MarkTabletIDOP(MarkTabletIDOP&) = delete;
  MarkTabletIDOP& operator=(const MarkTabletIDOP&) = delete;

  int operator()(
      const ObTabletID &tablet_id);
  TO_STRING_KV(K(tablet_id_map_.count()));
public:
  TabletIDMap &tablet_id_map_;
};

struct TabletMetaGCOP : public BaseOP
{
public:
  TabletMetaGCOP(
      ObPublicBlockGCThreadGuard &gc_guard,
      const bool &is_paused, 
      const bool &is_stopped, 
      const int64_t retain_tablet_meta_ts,
      const share::SCN &major_scn)
    : BaseOP(is_paused, is_stopped),
      gc_guard_(gc_guard),
      retain_tablet_meta_ts_(retain_tablet_meta_ts),
      major_scn_(major_scn)
  {}
  virtual ~TabletMetaGCOP() {}
  TabletMetaGCOP(TabletMetaGCOP&) = delete;
  TabletMetaGCOP& operator=(const TabletMetaGCOP&) = delete;

  int operator()(
      const ObTabletID &tablet_id);

  TO_STRING_KV(K_(retain_tablet_meta_ts), K_(gc_guard), K_(major_scn));
public:
  ObPublicBlockGCThreadGuard &gc_guard_;
  const int64_t retain_tablet_meta_ts_;
  const share::SCN major_scn_;
};

struct TabletGCOP : public BaseOP
{
public:
  TabletGCOP(
      ObPublicBlockGCThreadGuard &gc_guard,
      const bool &is_paused,
      const bool &is_stopped) 
    : BaseOP(is_paused, is_stopped),
      gc_guard_(gc_guard)
  {}
  virtual ~TabletGCOP() {}
  TabletGCOP(TabletGCOP&) = delete;
  TabletGCOP& operator=(const TabletGCOP&) = delete;
  TO_STRING_KV(K_(gc_guard));

  bool operator()(
      const ObTabletID &tablet_id, 
      bool &is_mark);
public:
  ObPublicBlockGCThreadGuard &gc_guard_;
};

class ObPublicBlockGCHandler : public ObBlockGCHandler
{
public:
  ObPublicBlockGCHandler(
      const ObTabletID &tablet_id)
    : ObBlockGCHandler(tablet_id),
      next_gc_tablet_meta_ts_(INT64_MAX),
      retain_tablet_meta_ts_(INT64_MAX),
      major_scn_()
  {}

  ObPublicBlockGCHandler(
      const ObTabletID &tablet_id,
      int64_t retain_tablet_meta_ts,
      const share::SCN &major_scn)
    : ObBlockGCHandler(tablet_id),
      next_gc_tablet_meta_ts_(INT64_MAX),
      retain_tablet_meta_ts_(retain_tablet_meta_ts),
      major_scn_(major_scn)
  {}

  int gc_tablet_meta_versions();
  int gc_tablet();

  // gc block operation
  virtual int list_tablet_meta_version(
    ObIArray<int64_t> &tablet_versions,
    int64_t *min_retain_tablet_meta_version = NULL);

  virtual int get_blocks_for_tablet(
    int64_t tablet_meta_version,
    ObIArray<blocksstable::MacroBlockId> &block_ids);

  virtual int delete_tablet_meta_version(
    int64_t tablet_meta_version);

  // there is not dir in public dir
  virtual int try_delete_tablet_meta_dir()
  { return OB_SUCCESS; }
  virtual int try_delete_tablet_data_dir();

  // list block_id cannot be use in share dir
  virtual int get_block_ids_from_dir(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    return OB_SUCCESS;
  }

  virtual int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &collect_check_error_block_op)
  { return build_macro_block(tablet_versions, min_retain_tablet_meta_version, collect_check_error_block_op); }

  INHERIT_TO_STRING_KV("ObBlockGCHandler", ObBlockGCHandler, K_(next_gc_tablet_meta_ts), K_(retain_tablet_meta_ts));

private:
  int post_gc_tablet_meta_process_(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version);
  int post_gc_tablet_process_(
      const ObIArray<int64_t> &tablet_versions);
  int gc_macro_block_for_detect_();
  int delete_shared_tablet_meta_();
  int delete_shared_tablet_id_();

  int read_is_deleted_obj_(int64_t &delete_ts);
  int write_is_deleted_obj_();
  int delete_is_deleted_obj_();

  int delete_compaction_file_();
  int detect_and_gc_block_(
      const blocksstable::MacroBlockId &block_id);
  int delete_major_prewarm_(const int64_t tablet_meta_version);
  int get_seq_ids_(
    ObIArray<blocksstable::MacroBlockId> &seq_ids);
  virtual int is_exist_macro_block_(
      const blocksstable::MacroBlockId &block_id, 
      bool &is_exist)
  { return OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_id, 0, is_exist); }

public:
  // set it in list_tablet_meta_version for next gc
  int64_t next_gc_tablet_meta_ts_;
private:
  const int64_t retain_tablet_meta_ts_;
  const share::SCN major_scn_;
};

class ObPublicBlockGCTask : public common::ObTimerTask
{

public:
  static const int64_t DEFALT_PUBLIC_TABLET_META_GC_SAFE_TIME;
  static const int64_t DEFALT_PUBLIC_TABLET_GC_SAFE_TIME;
  static const int64_t GC_CHECK_INTERVAL;
  static const int64_t LOOP_CHECK_INTERVAL;
  static const int64_t GLOBAL_GC_CHECK_INTERVAL_TIMES;
  static const int64_t BATCH_DETECT_MACRO_SIZE;

  ObPublicBlockGCTask(ObPublicBlockGCService &public_block_gc_service)
    : public_block_gc_service_(public_block_gc_service),
      next_gc_tablet_meta_ts_(0),
      schema_version_(OB_INVALID_VERSION),
      is_stopped_(false),
      is_paused_(false),
      gc_tablet_safe_time_val_(ObPublicBlockGCTask::DEFALT_PUBLIC_TABLET_GC_SAFE_TIME),
      gc_tablet_meta_safe_time_val_(ObPublicBlockGCTask::DEFALT_PUBLIC_TABLET_META_GC_SAFE_TIME),
      last_gc_tablet_loop_ts_(0),
      last_gc_tablet_meta_loop_ts_(0)
  {}
  virtual ~ObPublicBlockGCTask() {}

  virtual void runTimerTask();

  void set_is_stopped() 
  { 
    ATOMIC_STORE(&is_stopped_, true); 
    STORAGE_LOG(INFO, "public gc service set stop", KPC(this));
  }
  void clear_is_stopped() 
  { 
    ATOMIC_STORE(&is_stopped_, false); 
    STORAGE_LOG(INFO, "public gc service clear stop", KPC(this));
  }
  void set_is_paused() 
  { 
    ATOMIC_STORE(&is_paused_, true); 
    STORAGE_LOG(INFO, "public gc service set pause", KPC(this));
  }
  void clear_is_paused() 
  { 
    ATOMIC_STORE(&is_paused_, false); 
    STORAGE_LOG(INFO, "public gc service clear pause", KPC(this));
  }
  int update_safe_time_config(
    const int64_t gc_tablet_safe_time_val,
    const int64_t gc_tablet_meta_safe_time_val);
  int64_t get_gc_tablet_safe_time_val()
  { return ATOMIC_LOAD(&gc_tablet_safe_time_val_); }

  TO_STRING_KV(K_(next_gc_tablet_meta_ts), K_(gc_tablet_safe_time_val), K_(gc_tablet_meta_safe_time_val), K_(last_gc_tablet_loop_ts), K_(last_gc_tablet_meta_loop_ts));

private:
  void loop_tablet_meta_version_gc_();
  void loop_tablet_gc_();
  template <typename OP>
  int loop_tablet_id_(
      OP &op);

  ObPublicBlockGCService &public_block_gc_service_;
  int64_t next_gc_tablet_meta_ts_;
  int64_t schema_version_;
  bool is_stopped_;

  // for safe time config change
  // set true in update_safe_time_config
  // set false in runTimerTask
  // check in BaseOP
  bool is_paused_;

  int64_t gc_tablet_safe_time_val_;
  int64_t gc_tablet_meta_safe_time_val_;

  int64_t last_gc_tablet_loop_ts_;
  int64_t last_gc_tablet_meta_loop_ts_;
};

class ObPublicBlockGCService : public logservice::ObIRoleChangeSubHandler,
                               public logservice::ObICheckpointSubHandler,
                               public logservice::ObIReplaySubHandler
{
public:
  ObPublicBlockGCService()
    : is_inited_(false),
      timer_for_public_block_gc_(),
      public_block_gc_task_(*this),
      gc_thread_()
  {}
  virtual ~ObPublicBlockGCService() {}

  // mtl function
  static int mtl_init(ObPublicBlockGCService *&m);
  int init();
  int start();
  int stop();
  void wait();
  void destroy();

  // log service interface
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(share::SCN &) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    return OB_SUCCESS;
  }
 
  int inner_switch_to_leader();
  int inner_switch_to_follower();

  virtual void switch_to_follower_forcedly() override
  {
    inner_switch_to_follower();
  }
  virtual int switch_to_leader() override
  {
    return inner_switch_to_leader();
  }
  virtual int switch_to_follower_gracefully() override
  {
    return inner_switch_to_follower();
  }
  virtual int resume_leader() override
  {
    return OB_SUCCESS;
  }
  int update_safe_time_config(
    const int64_t new_gc_tablet_safe_time_val,
    const int64_t new_gc_tablet_meta_safe_time_val)
  { return public_block_gc_task_.update_safe_time_config(new_gc_tablet_safe_time_val, new_gc_tablet_meta_safe_time_val); }

  int64_t get_gc_tablet_safe_time_val()
  { return public_block_gc_task_.get_gc_tablet_safe_time_val(); }

  ObPublicBlockGCThread& get_gc_thread()
  { return gc_thread_; }
private:
  bool is_inited_;
  common::ObTimer timer_for_public_block_gc_;
  ObPublicBlockGCTask public_block_gc_task_;
  ObPublicBlockGCThread gc_thread_;
};

struct ObIsDeletedObj
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t MAX_IS_DELETED_OBJ_SIZE;
  int64_t delete_ts_;
  TO_STRING_KV(K_(delete_ts));
};
OB_SERIALIZE_MEMBER_TEMP(inline, ObIsDeletedObj, delete_ts_);

} // storage
} // oceanbase

#endif
