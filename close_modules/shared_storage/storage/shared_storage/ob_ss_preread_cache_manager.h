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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_CACHE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_CACHE_MANAGER_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "deps/oblib/src/lib/list/ob_list.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "storage/shared_storage/task/ob_ss_preread_task.h"

namespace oceanbase
{
namespace storage
{

class ObTenantFileManager;

enum ObLURNodeStatus
{
  FAKE = 0,  // FAKE means will read from object storage;
  NORMAL = 1,  // NORMAL means has been read from object storage;
};

class ObPrereadCacheManager
{
public:
  static const int64_t NOT_PREREAD_IO_SIZE = 512L * 1024L; // 512KB
  friend class ObSSPreReadTask;
  ObPrereadCacheManager();
  ~ObPrereadCacheManager();
  int init(ObTenantFileManager *file_manager);
  int start();
  void destroy();
  void stop();
  void wait();
  int push_file_id_to_lru(const blocksstable::MacroBlockId &file_id, const bool already_exist_in_cache = false, const int64_t file_size = OB_DEFAULT_MACRO_BLOCK_SIZE);
  int evict_tail_lru_node();
  int evict_lru_node(const blocksstable::MacroBlockId &file_id);
  int remove_lru_node(const blocksstable::MacroBlockId &file_id);
  int refresh_lru_node_if_need(const blocksstable::ObStorageObjectReadInfo &read_info);
  int update_to_normal_status(const blocksstable::MacroBlockId &file_id, const int64_t file_length);
  int is_file_id_need_preread(const blocksstable::MacroBlockId &file_id, bool &is_need_preread);
  int is_exist_in_lru(const blocksstable::MacroBlockId &file_id, bool &is_exist);
  int set_need_preread(const blocksstable::MacroBlockId &file_id, const bool is_need_preread);
  OB_INLINE int64_t get_segment_file_map_size() { return segment_file_map_.size(); }

private:
  bool is_read_cache_support_object_type (const blocksstable::ObStorageObjectType object_type);
  bool is_need_cancel_preread(const blocksstable::ObStorageObjectReadInfo &read_info);

private:
  static const int64_t MIN_RETENTION_TIME = 500L * 1000L; // 500ms
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t ONCE_EVICT_LRU_NODE_SIZE = 10 * OB_DEFAULT_MACRO_BLOCK_SIZE; // 20MB
  typedef int64_t TimeStampId;
  struct ObListNode : public ObDLinkBase<ObListNode>
  {
  public:
    ObListNode() {}
    ObListNode(const blocksstable::MacroBlockId segment_file_id,
               const ObLURNodeStatus node_status,
               const int64_t file_length)
      : segment_file_id_(segment_file_id),
        node_status_(node_status),
        time_stamp_id_(ObTimeUtility::fast_current_time()),
        file_length_(file_length),
        is_need_preread_(true) {}
    ObListNode(const ObListNode &node)
    {
      segment_file_id_ = node.segment_file_id_;
      node_status_ = node.node_status_;
      time_stamp_id_ = node.time_stamp_id_;
      file_length_ = node.file_length_;
      is_need_preread_ = node.is_need_preread_;
    }
    ~ObListNode() {}
    ObListNode& operator =(const ObListNode &other)
    {
      segment_file_id_ = other.segment_file_id_;
      node_status_ = other.node_status_;
      time_stamp_id_ = other.time_stamp_id_;
      file_length_ = other.file_length_;
      is_need_preread_ = other.is_need_preread_;
      return *this;
    }
    TO_STRING_KV(K_(segment_file_id), K_(node_status), K_(time_stamp_id), K_(file_length), K_(is_need_preread));
  public:
    blocksstable::MacroBlockId segment_file_id_;
    ObLURNodeStatus node_status_; // FAKE means will read from object storage;
                                  // NORMAL means has been read from object storage;
    TimeStampId time_stamp_id_;
    int64_t file_length_;
    bool is_need_preread_; // When file_id has been read whole, set is_need_preread is false, means the file_id do not need preread
  };
  typedef common::ObDList<ObListNode> LRUList;
  typedef hash::ObHashMap<blocksstable::MacroBlockId, ObListNode> LRUMap;

private:
  bool is_inited_;
  ObTenantFileManager *file_manager_;
  common::ObRecursiveMutex segment_file_lock_;
  LRUList segment_file_list_;
  LRUMap segment_file_map_;
  int tg_id_;
  ObSSPreReadTask preread_task_;
  common::ObLinkQueue preread_queue_; // store TmpFile's segment file and MajorMacro ids that need read from remote object storage
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_CACHE_MANAGER_H_ */
