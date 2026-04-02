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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SEGMENT_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SEGMENT_FILE_MANAGER_H_

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/task/ob_gc_segment_file_task.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "share/ob_ptr_handle.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;

enum class ObSSTmpFileSegDeleteType : uint8_t;
enum class ObSSTmpFileSegMetaOpType : uint8_t;

struct TmpFileSegId final
{
public:
  TmpFileSegId() : tmp_file_id_(INT64_MAX), segment_id_(INT64_MAX) {}
  TmpFileSegId(const int64_t tmp_file_id, const int64_t segment_id)
    : tmp_file_id_(tmp_file_id),
      segment_id_(segment_id) {}
  TmpFileSegId(const TmpFileSegId &other) { *this = other; }
  ~TmpFileSegId() {}
  TmpFileSegId& operator =(const TmpFileSegId &other)
  {
    tmp_file_id_ = other.tmp_file_id_;
    segment_id_ = other.segment_id_;
    return *this;
  }
  bool is_valid() const
  {
    return ((INT64_MAX != tmp_file_id_) && (INT64_MAX != segment_id_));
  }
  bool operator==(const TmpFileSegId &other) const
  {
    return (tmp_file_id_ == other.tmp_file_id_) && (segment_id_ == other.segment_id_);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&tmp_file_id_, sizeof(tmp_file_id_), hash_val);
    hash_val = murmurhash(&segment_id_, sizeof(segment_id_), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return common::OB_SUCCESS; }
  TO_STRING_KV(K_(tmp_file_id), K_(segment_id));
public:
  int64_t tmp_file_id_;
  int64_t segment_id_;
};

struct TmpFileMeta final
{
public:
  TmpFileMeta();
  TmpFileMeta(const bool is_in_local, const int32_t valid_length, const int32_t target_length = 0);
  ~TmpFileMeta() { reset(); }
  void inc_ref_count();
  void dec_ref_count();
  int64_t get_ref() const;
  void set_target_length(const int32_t target_length);
  bool is_tmp_file_appending(const int64_t file_len);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(is_in_local), K_(valid_length), K_(target_length), K_(ref_cnt), K_(append_timestamp_us));
public:
  common::SpinRWLock lock_;     // segment file's meta operator lock
  volatile bool is_in_local_; // segment file store in local or object storage
  int32_t valid_length_;  // segment file's valid length
  int32_t target_length_; // current append segment file's target length
  int64_t ref_cnt_;       // segment file's reference count
  int64_t append_timestamp_us_;  // segment file's last append time stamp
};

class TmpFileMetaHandle final : public ObPtrHandle<TmpFileMeta>
{
public:
  TmpFileMetaHandle();
  virtual ~TmpFileMetaHandle();
  void reset();
  int set_tmpfile_meta(const bool is_in_local, const int32_t valid_length, const int32_t target_length);
  OB_INLINE bool is_valid() const { return (nullptr != ptr_); }
  int assign(const TmpFileMetaHandle &other);
  void update_tmpfile_meta(const bool is_in_local, const int64_t valid_length);
  void set_target_length(const int32_t target_length);
  OB_INLINE bool is_tmp_file_appending(const int64_t file_len) const
  {
    return ((nullptr == ptr_) ? false : ptr_->is_tmp_file_appending(file_len));
  }
  OB_INLINE bool is_in_local() const { return ((nullptr == ptr_) ? false : ptr_->is_in_local_); }
  OB_INLINE int32_t get_valid_length() const { return ((nullptr == ptr_) ? 0 : ptr_->valid_length_); }
  OB_INLINE int32_t get_target_length() const { return ((nullptr == ptr_) ? 0 : ptr_->target_length_); }
  OB_INLINE int64_t get_append_timestamp_us() const { return ((nullptr == ptr_) ? OB_INVALID_TIMESTAMP : ptr_->append_timestamp_us_); }
  TmpFileMeta *get_tmpfile_meta() const { return ptr_; }
  TO_STRING_KV(KP_(ptr));
};

class ObSegMetaUpdateOp
{
protected:
  typedef common::hash::HashMapPair<TmpFileSegId, TmpFileMetaHandle> MapKV;
public:
  ObSegMetaUpdateOp(const TmpFileMetaHandle &meta_handle)
  {
    // op can only be used once, because op used again, meta_handle will assign failed
    meta_handle_.assign(meta_handle);
  }
  virtual ~ObSegMetaUpdateOp() {}
  void operator()(MapKV &entry)
  {
    entry.second.update_tmpfile_meta(meta_handle_.is_in_local(), meta_handle_.get_valid_length());
  }
private:
  TmpFileMetaHandle meta_handle_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSegMetaUpdateOp);
};

struct ObSSTmpFileAppendParam final
{
public:
  ObSSTmpFileAppendParam();
  ObSSTmpFileAppendParam(const bool need_write_io,
                         const bool need_free_file_size,
                         const int64_t free_file_size);
  ~ObSSTmpFileAppendParam() {}
  TO_STRING_KV(K_(need_write_io), K_(need_free_file_size), K_(free_file_size));

public:
  common::ObArenaAllocator arena_allocator_;
  bool need_write_io_; // no need write io when current_io.valid_length <= seg_meta.valid_length
  bool need_free_file_size_; // object storage write: false, local cache write: true
  int64_t free_file_size_; // if need_free_file_size_ is true, free_file_size_ is valid
};

class ObSegmentFileManager
{
public:
  ObSegmentFileManager();
  ~ObSegmentFileManager();
  int init(ObTenantFileManager *file_manager);
  int start();
  void destroy();
  void stop();
  void wait();
  int async_append_file(const blocksstable::ObStorageObjectWriteInfo &write_info,
                        blocksstable::ObStorageObjectHandle &object_handle);
  int async_pread_file(const blocksstable::ObStorageObjectReadInfo &read_info,
                       blocksstable::ObStorageObjectHandle &object_handle);
  int insert_meta(const TmpFileSegId &seg_id, const TmpFileMetaHandle &meta_handle);
  int delete_meta(const TmpFileSegId &seg_id);
  int delete_wild_meta(const int64_t tmp_file_id);
  int update_meta(const TmpFileSegId &seg_id, const TmpFileMetaHandle &meta_handle);
  int try_get_seg_meta(const TmpFileSegId &seg_id, TmpFileMetaHandle &meta_handle, bool &is_meta_exist);
  int push_seg_file_to_remove_queue(const TmpFileSegId &seg_id, const int64_t valid_length);
  int exec_remove_task_once();
  int find_unsealed_tmp_file_to_flush(ObIArray<TmpFileSegId> &seg_files);

public:
  static const int64_t UNSEALED_TMP_FILE_FLUSH_THRESHOLD = 60L * 1000L * 1000L; //60s
  static const int32_t MAX_UNREACHABLE_LENTH = INT32_MAX - 1; // means seg file is appending or is sealed, cannot flush unsealed seg file
  class ObUnsealedSegFile: public common::ObLink
  {
  public:
    ObUnsealedSegFile(const TmpFileSegId &seg_id, const int64_t valid_length)
    : seg_id_(seg_id), valid_length_(valid_length) {}
    ~ObUnsealedSegFile() = default;
    TO_STRING_KV(K_(seg_id), K_(valid_length));
  public:
    TmpFileSegId seg_id_;
    int64_t valid_length_;
  };

private:
  int set_tmp_file_write_through_if_need(const blocksstable::MacroBlockId &file_id,
                                         blocksstable::ObStorageObjectWriteInfo &write_info);
  int handle_write_on_meta_exist(TmpFileMetaHandle &meta_handle,
                                 blocksstable::ObStorageObjectWriteInfo &write_info,
                                 blocksstable::ObStorageObjectHandle &object_handle,
                                 ObSSTmpFileAppendParam &append_param);
  int handle_write_on_meta_not_exist(blocksstable::ObStorageObjectWriteInfo &write_info,
                                     blocksstable::ObStorageObjectHandle &object_handle,
                                     ObSSTmpFileAppendParam &append_param);
  int handle_write_with_local_seg(TmpFileMetaHandle &meta_handle,
                                  blocksstable::ObStorageObjectWriteInfo &write_info,
                                  blocksstable::ObStorageObjectHandle &object_handle,
                                  ObSSTmpFileAppendParam &append_param);
  int handle_write_with_remote_seg(TmpFileMetaHandle &meta_handle,
                                   blocksstable::ObStorageObjectWriteInfo &write_info,
                                   blocksstable::ObStorageObjectHandle &object_handle,
                                   ObSSTmpFileAppendParam &append_param);
  int read_existing_data_and_append_buf(const MacroBlockId &file_id,
                                        const TmpFileMetaHandle &meta_handle,
                                        blocksstable::ObStorageObjectWriteInfo &write_info,
                                        ObSSTmpFileAppendParam &append_param);
  int simulate_io_result(const blocksstable::ObStorageObjectWriteInfo &write_info,
                         blocksstable::ObStorageObjectHandle &object_handle) const;
  int construct_tmp_file_io_callback(const TmpFileSegId &seg_id,
                                     const TmpFileMetaHandle &meta_handle,
                                     const ObSSTmpFileSegMetaOpType seg_meta_op_type,
                                     const ObSSTmpFileSegDeleteType seg_del_type,
                                     common::ObIOCallback *&io_callback,
                                     const int64_t del_seg_valid_len = 0);

private:
  typedef hash::ObHashMap<TmpFileSegId, TmpFileMetaHandle> SegMetaMap;
  struct HeapItem
  {
    int64_t tmp_file_id_;
    int64_t segment_id_;
    int64_t append_timestamp_us_;
    int32_t file_length_;
    HeapItem() : tmp_file_id_(0), segment_id_(0), append_timestamp_us_(OB_INVALID_TIMESTAMP), file_length_(0) {}
    HeapItem(const int64_t tmp_file_id, const int64_t segment_id, const int64_t timestamp, const int32_t file_length)
      : tmp_file_id_(tmp_file_id), segment_id_(segment_id), append_timestamp_us_(timestamp), file_length_(file_length) {}
    TO_STRING_KV(K_(tmp_file_id), K_(segment_id), K_(append_timestamp_us), K_(file_length));
  };
  struct HeapCompare {
    bool operator()(const HeapItem &l, const HeapItem &r) {
      return l.append_timestamp_us_ < r.append_timestamp_us_;
    }
    int get_error_code() { return OB_SUCCESS; }
  };
  class GetWildTmpFileSegIdFunc
  {
  public:
    explicit GetWildTmpFileSegIdFunc(const int64_t tmp_file_id);
    virtual ~GetWildTmpFileSegIdFunc() {}
  public:
    int operator()(const common::hash::HashMapPair<TmpFileSegId, TmpFileMetaHandle> &entry);
    TO_STRING_KV(K_(tmp_file_id), K_(wild_seg_ids));
  public:
    int64_t tmp_file_id_;
    ObSEArray<TmpFileSegId, 16> wild_seg_ids_;
  };
  int push_item_to_heap(const HeapItem &heap_item, ObBinaryHeap<HeapItem, HeapCompare> &seg_file_heap, int64_t &total_flush_size);

private:
  bool is_inited_;
  bool is_stop_;
  ObTenantFileManager *file_manager_;
  common::ObConcurrentFIFOAllocator allocator_;
  SegMetaMap seg_meta_map_; // store unsealed seg meta and local sealed seg meta
  ObGCSegmentFileTask gc_segment_file_task_;
  common::ObLinkQueue unsealed_seg_file_remove_queue_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SEGMENT_FILE_MANAGER_H_ */
