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

#include "ob_ss_preread_cache_manager.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObPrereadCacheManager::ObPrereadCacheManager()
  : is_inited_(false),
    file_manager_(nullptr),
    segment_file_lock_(ObLatchIds::FILE_MANAGER_LOCK),
    segment_file_list_(),
    segment_file_map_(),
    tg_id_(INVALID_TG_ID),
    preread_task_(),
    preread_queue_()
{
}

ObPrereadCacheManager::~ObPrereadCacheManager() { destroy(); }

int ObPrereadCacheManager::init(ObTenantFileManager *file_manager)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), "PreReadLRU_Map");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("preread cache manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_manager));
  } else if (OB_FAIL(segment_file_map_.create(ObBaseFileManager::OB_DEFAULT_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create map", KR(ret));
  } else if (OB_FAIL(preread_task_.init(this))) {
    LOG_WARN("fail to init preread task", KR(ret));
  } else {
    file_manager_ = file_manager;
    is_inited_ = true;
  }
  return ret;
}

int ObPrereadCacheManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TmpFilePrereadTimer, tg_id_))) {
    LOG_WARN("fail to create timer thread", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer thread", KR(ret), K_(tg_id));
  } else if (OB_FAIL(preread_task_.start(tg_id_))) {
    LOG_WARN("fail to start preread task", KR(ret), K_(tg_id));
  }
  return ret;
}

void ObPrereadCacheManager::destroy()
{
  int ret = OB_SUCCESS;
  if (INVALID_TG_ID != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  const int64_t queue_size = preread_queue_.size();
  for (int64_t i = 0; OB_SUCC(ret) && (i < queue_size); ++i) {
    ObLink *ptr = nullptr;
    ObSegmentFileInfo *segment_file_info = nullptr;
    if (OB_FAIL(preread_queue_.pop(ptr))) {
      LOG_WARN("fail to pop segment_file", KR(ret), K(i), K(queue_size));
    } else {
      segment_file_info = static_cast<ObSegmentFileInfo *>(ptr);
      OB_DELETE(ObSegmentFileInfo, attr, segment_file_info);
    }
  }
  preread_task_.destroy();
  // must first clear list than destory map
  segment_file_list_.clear();
  segment_file_map_.destroy();
  file_manager_ = nullptr;
  is_inited_ = false;
}

void ObPrereadCacheManager::stop()
{
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
}

void ObPrereadCacheManager::wait()
{
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
}

int ObPrereadCacheManager::push_file_id_to_lru(const MacroBlockId &file_id, const bool already_exist_in_cache, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  const ObStorageObjectType object_type = file_id.storage_object_type();
  bool is_node_iter_add_list = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(file_size <= 0 || !file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_size), K(file_id));
  } else if (!is_read_cache_support_object_type(object_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("object type not support push to lru list", KR(ret), K(file_id));
  } else {
    ObRecursiveMutexGuard guard(segment_file_lock_);
    ObListNode list_node;
    ret = segment_file_map_.get_refactored(file_id, list_node);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get segment file", KR(ret), K(file_id));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ObMemAttr attr(MTL_ID(), "SegmentFile");
      ObSegmentFileInfo *segment_file_info = nullptr;
      ObListNode list_node;
      // if file_id already exists in cache, it indicates that the file exists in the local cache, so ObLURNodeStatus is NORMAL
      if (already_exist_in_cache) {
        list_node = ObListNode(file_id, ObLURNodeStatus::NORMAL, file_size/*file_length*/);
      } else {
        list_node = ObListNode(file_id, ObLURNodeStatus::FAKE, 0/*file_length*/);
      }
      ObListNode *node_iter = nullptr;
      if (OB_FAIL(segment_file_map_.set_refactored(file_id, list_node))) {
        LOG_WARN("fail to set map", KR(ret), K(file_id));
      } else if (OB_ISNULL(node_iter = segment_file_map_.get(file_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), KP(node_iter), K(file_id));
      } else if (OB_UNLIKELY(!segment_file_list_.add_first(node_iter))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add first", KR(ret), K(file_id));
      } else {
        is_node_iter_add_list = true;
      }
      if (OB_FAIL(ret)) {
      } else if (already_exist_in_cache) {
        // do nothing, if file_id already exists in cache, it indicates that the file exists in the local cache, so do not push to segment_file_queue_
      } else if (OB_ISNULL(segment_file_info = OB_NEW(ObSegmentFileInfo, attr, file_id, GET_FUNC_TYPE(), true/*is_sealed*/))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret), K(file_id));
      } else if (OB_FAIL(preread_queue_.push(segment_file_info))) {
        LOG_WARN("fail to push", KR(ret), K(file_id), KPC(segment_file_info));
      }
      // free lru list and map on fail
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (is_node_iter_add_list) {
          segment_file_list_.remove(node_iter);
        }
        if (OB_TMP_FAIL(segment_file_map_.erase_refactored(file_id))) {
          if (OB_HASH_NOT_EXIST != tmp_ret) {
            LOG_WARN("fail to erase map node", KR(tmp_ret), K(file_id));
          }
        }
      }
      // free memory on fail
      if (OB_FAIL(ret)) {
        OB_DELETE(ObSegmentFileInfo, attr, segment_file_info);
      }
    }
  }
  if (OB_SUCC(ret) && is_node_iter_add_list) {
    LOG_INFO("succ to push file id to queue", K(file_id), K(already_exist_in_cache));
  }
  return ret;
}

int ObPrereadCacheManager::evict_tail_lru_node()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else {
    // if preread_cahce_size is OB_SERVER_OUTOF_DISK_SPACE, evict the last several NORMAL lru nodes
    ObSEArray<MacroBlockId, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> file_ids;
    int64_t evict_lru_node_size = 0;
    {
      ObRecursiveMutexGuard guard(segment_file_lock_);
      DLIST_FOREACH_BACKWARD_X(node_iter, segment_file_list_, OB_SUCC(ret)/*extra_condition*/) {
        if (ObLURNodeStatus::NORMAL == node_iter->node_status_) {
          // if the last NORMAL lru node's time_stamp_id_ stay in lru is less MIN_RETENTION_TIME, all of lru node cannot evict, so break
          if ((ObTimeUtility::fast_current_time() - node_iter->time_stamp_id_) < MIN_RETENTION_TIME) {
            break;
          }
          if (OB_FAIL(file_ids.push_back(node_iter->segment_file_id_))) {
            LOG_WARN("fail to push back", KR(ret), K(*node_iter));
          } else {
            evict_lru_node_size += node_iter->file_length_;
          }
          if (evict_lru_node_size >= ONCE_EVICT_LRU_NODE_SIZE) {
            break;
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < file_ids.count()); ++i) {
      if (OB_FAIL(evict_lru_node(file_ids.at(i)))) {
        LOG_WARN("fail to evict lru node", KR(ret), K(file_ids.at(i)));
      }
    }
    if (OB_SUCC(ret) && !file_ids.empty()) {
      LOG_INFO("succ to evict tail lru node", K(file_ids), K(segment_file_map_.size()), K(evict_lru_node_size));
    }
  }
  return ret;
}

int ObPrereadCacheManager::evict_lru_node(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else if (ObStorageObjectType::TMP_FILE == file_id.storage_object_type()) { // 1. TMP_FILE
    bool is_meta_exist = false;
    TmpFileMetaHandle meta_handle;
    TmpFileSegId seg_id(file_id.second_id(), file_id.third_id());
    if (OB_ISNULL(file_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret));
    } else if (OB_FAIL(file_manager_->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
      LOG_WARN("fail to try get seg meta", KR(ret), K(seg_id));
    } else if (!is_meta_exist) {
      // try delete local file for the following two cases:
      // 1. segment may be gc concurrently
      // 2. write through 2MB sealed segments, which have no tmp file meta
      if (OB_FAIL(file_manager_->delete_local_file(file_id))) {
        LOG_WARN("fail to delete local tmp file", KR(ret), K(file_id));
      }
    } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle should not be invalid", KR(ret), K(seg_id), K(meta_handle));
    } else { // is_meta_exist
      SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
      // Note: must set is_in_local_ = false, otherwise concurrent read may fail to read local file.
      // e.g., concurrent read get meta with is_in_local = true, then here delete local file,
      // finally concurrent read fail to read local file.
      meta_handle.get_tmpfile_meta()->is_in_local_ = false;
      // Note: must delete meta before delete file
      if (OB_FAIL(file_manager_->get_segment_file_mgr().delete_meta(seg_id))) { // expect will not fail
        LOG_WARN("fail to delete meta", KR(ret), K(seg_id));
      } else if (OB_FAIL(file_manager_->delete_local_file(file_id))) {
        LOG_WARN("fail to delete local tmp file", KR(ret), K(file_id));
      }
    }
  } else { // 2. non-TMP_FILE
    if (OB_FAIL(file_manager_->delete_local_file(file_id))) {
      LOG_WARN("fail to delete local file", KR(ret), K(file_id));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to evict lru node", K(file_id));
  }
  return ret;
}

int ObPrereadCacheManager::remove_lru_node(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  bool is_remove_lru_node = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else {
    ObRecursiveMutexGuard guard(segment_file_lock_);
    ObListNode *node_iter = segment_file_map_.get(file_id);
    if (nullptr == node_iter) {
      // do nothing, file_id does not exist in lru map, maybe has been gc
    } else if (OB_ISNULL(segment_file_list_.remove(node_iter))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove list node", KR(ret), K(file_id));
    } else if (OB_FAIL(segment_file_map_.erase_refactored(file_id))) {
      LOG_WARN("fail to erase map node", KR(ret), K(file_id));
    } else {
      is_remove_lru_node = true;
    }
  }
  if (OB_SUCC(ret) && is_remove_lru_node) {
    LOG_INFO("succ to remove lru node", K(file_id));
  }
  return ret;
}

int ObPrereadCacheManager::refresh_lru_node_if_need(const ObStorageObjectReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  const MacroBlockId file_id = read_info.macro_block_id_;
  ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info));
  // only tmp_file and prereaded major_macro need refresh lru node
  } else if ((ObStorageObjectType::TMP_FILE == object_type) ||
             (true == read_info.is_major_macro_preread_)) {
    // WARNING: do not lock in here, avoid affect aio_read() process
    ObListNode list_node;
    ret = segment_file_map_.get_refactored(file_id, list_node);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get segment file", KR(ret), K(file_id));
    } else if (OB_HASH_NOT_EXIST == ret) {
      // do nothing, the file_id does not exist in lru list
      ret = OB_SUCCESS;
    } else if (ObLURNodeStatus::NORMAL == list_node.node_status_) {
      if ((read_info.offset_ + read_info.size_) > list_node.file_length_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), K(list_node), K(read_info));
      } else if (list_node.file_length_ == (read_info.offset_ + read_info.size_)) {
        // if evict lru node failed, it doesn't matter
        // because when preread cache is full will evict tail lru node or gc_thread will delete tmp file
        if (OB_FAIL(evict_lru_node(file_id))) {
          LOG_WARN("fail to evict lru node", KR(ret), K(file_id), K(read_info));
        }
      }
    } else if (ObLURNodeStatus::FAKE == list_node.node_status_) {
      // when preread is slow than aio_read from object_storage, cannot remove lru node, otherwise free file size is write_cache
      // When file_id has been read whole, set is_need_preread is false, means the file_id do not need preread
      if (is_need_cancel_preread(read_info)) {
        if (OB_FAIL(set_need_preread(file_id, false/*is_need_preread*/))) {
          LOG_WARN("fail to set need preread", KR(ret), K(file_id), K(read_info));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected lru node status", KR(ret), K(list_node));
    }
  }
  return ret;
}

bool ObPrereadCacheManager::is_need_cancel_preread(const ObStorageObjectReadInfo &read_info)
{
  // if LRU Node status is FAKE, tmp_file and major_macro Node has been read half of segment file, do not need preread
  bool is_cancel_preread = ((read_info.offset_ + read_info.size_) >= OB_DEFAULT_MACRO_BLOCK_SIZE / 2);
  return is_cancel_preread;
}

int ObPrereadCacheManager::update_to_normal_status(const MacroBlockId &file_id, const int64_t file_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid() || file_length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id), K(file_length));
  } else {
    ObRecursiveMutexGuard guard(segment_file_lock_);
    ObListNode *node_iter = segment_file_map_.get(file_id);
    if (nullptr == node_iter) {
      // do nothing, file_id does not exist in lru map, maybe has been gc
    } else if (ObLURNodeStatus::FAKE == node_iter->node_status_) {
      node_iter->file_length_ = file_length;
      node_iter->node_status_ = ObLURNodeStatus::NORMAL;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected lru node status", KR(ret), K(*node_iter));
    }
  }
  return ret;
}

int ObPrereadCacheManager::is_file_id_need_preread(const MacroBlockId &file_id, bool &is_need_preread)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else {
    ObListNode list_node;
    ret = segment_file_map_.get_refactored(file_id, list_node);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get segment file", KR(ret), K(file_id));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_need_preread = false;
    } else if (list_node.node_status_ == ObLURNodeStatus::FAKE) {
      is_need_preread = list_node.is_need_preread_;
      // if lru node is fake and not_need_preread, remove lru node, otherwise lru node memory is leak
      // remove lru node, because FAKE status lru node has been evicted when file_id has been read whole, so the file_id do not need preread
      if (!is_need_preread) {
        if (OB_FAIL(remove_lru_node(file_id))) {
          LOG_WARN("fail to remove lru node", KR(ret), K(file_id));
        }
      }
    } else {
      is_need_preread = false;
    }
  }
  return ret;
}

int ObPrereadCacheManager::is_exist_in_lru(const MacroBlockId &file_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else {
    ObListNode list_node;
    ret = segment_file_map_.get_refactored(file_id, list_node);
    if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get segment file", KR(ret), K(file_id));
    } else if (OB_HASH_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      is_exist = true;
    }
  }
  return ret;
}

// When file_id has been read whole, set is_need_preread is false, means the file_id do not need preread
int ObPrereadCacheManager::set_need_preread(const blocksstable::MacroBlockId &file_id, const bool is_need_preread)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread cache manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_id));
  } else {
    ObRecursiveMutexGuard guard(segment_file_lock_);
    ObListNode *node_iter = segment_file_map_.get(file_id);
    if (nullptr == node_iter) {
      // do nothing, file_id does not exist in lru map, maybe has been gc
    } else {
      node_iter->is_need_preread_ = is_need_preread;
    }
  }
  return ret;
}

bool ObPrereadCacheManager::is_read_cache_support_object_type (const ObStorageObjectType object_type)
{
  bool is_support = ((ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == object_type) ||
                     (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type) ||
                     (ObStorageObjectType::TMP_FILE == object_type));
  return is_support;
}

} // namespace storage
} // namespace oceanbase
