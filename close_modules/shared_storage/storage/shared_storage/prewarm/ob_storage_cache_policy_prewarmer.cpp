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

#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "meta_store/ob_shared_storage_obj_meta.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace storage
{

int is_suspend_storage_cache_task(const uint64_t tenant_id, bool &is_suspend)
{
  int ret = OB_SUCCESS;
  is_suspend = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    is_suspend = (tenant_config->suspend_storage_cache_task);
  } else {
    LOG_WARN("fail to get tenant config is_suspend_storage_cache_task", KR(ret), K(tenant_id), K(is_suspend));
  }
  return ret;
}

/*====================================ObMacroBlockAsyncReader====================================*/
ObMacroBlockAsyncReader::ObMacroBlockAsyncReader(const int64_t parallelism)
    : allocator_(ObMemAttr(MTL_ID(), "MacroReader")),
      read_handle_idx_(-1),
      used_parallelism_(MIN(MAX_PARALLELISM, MAX(parallelism, MIN_PARALLELISM))),
      read_handles_()
{}

ObMacroBlockAsyncReader::~ObMacroBlockAsyncReader()
{
  reset_io_resources();
}

void ObMacroBlockAsyncReader::reset_io_resources()
{
  for (int64_t i = 0; i < used_parallelism_; i++) {
    read_handles_[i].reset();
  }
  read_handle_idx_ = -1;
  allocator_.clear();
}

int ObMacroBlockAsyncReader::async_read_from_object_storage(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int64_t block_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  // To reduce memory usage, memory is allocated only during each read operation
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro_id", KR(ret), K(macro_id));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(block_size), K(macro_id));
  } else {
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = block_size;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.buf_ = buf;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_timeout_ms_ =
        OB_IO_MANAGER.get_object_storage_io_timeout_ms(read_info.mtl_tenant_id_);

    ObSSObjectStorageReader reader;
    read_handle_idx_ = (read_handle_idx_ + 1) % used_parallelism_;
    if (OB_UNLIKELY(read_handles_[read_handle_idx_].is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret),
          K(read_handle_idx_), K(read_handles_[read_handle_idx_]), K(read_info));
    } else if (OB_FAIL(reader.aio_read(read_info, read_handles_[read_handle_idx_]))) {
      LOG_WARN("fail to aio read", KR(ret),
          K(read_info), K(read_handle_idx_), K(read_handles_[read_handle_idx_]));
    }
  }
  return ret;
}

int ObMacroBlockAsyncReader::batch_wait()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i <= read_handle_idx_ && i < used_parallelism_; i++) {
    if (read_handles_[i].is_empty()) {
      // do nothing
    } else if (OB_FAIL(read_handles_[i].wait())) {
      LOG_WARN("fail to wait read handle", KR(ret), K(i), K(read_handles_[i]));
    }
  }
  return ret;
}

int ObMacroBlockAsyncReader::batch_read_from_object_storage_with_retry(
    const ObIArray<MacroBlockId> &block_ids,
    const int64_t start_idx,
    const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_RETRY_TIMES = 3;
  const int64_t block_num = block_ids.count();
  const int64_t end_idx = start_idx + batch_size - 1;
  if (OB_UNLIKELY(start_idx < 0 || end_idx >= block_num
      || batch_size <= 0 || batch_size > used_parallelism_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret),
        K(start_idx), K(block_num), K(batch_size), K(used_parallelism_));
  } else {
    int64_t retry_times = 0;
    do {
      reset_io_resources();
      ret = OB_SUCCESS;
      for (int64_t i = start_idx; OB_SUCC(ret) && i <= end_idx; i++) {
        if (OB_FAIL(async_read_from_object_storage(block_ids.at(i)))) {
          LOG_WARN("fail to async read", KR(ret),
              K(i), K(retry_times), K(start_idx), K(end_idx), K(block_num), K(block_ids.at(i)));
        }
      }
      // TODO @fangdan: Refactor into pipeline pattern
      if (FAILEDx(batch_wait())) {
        LOG_WARN("fail to batch wait", KR(ret),
            K(retry_times), K(start_idx), K(end_idx), K(block_num), K(block_ids));
      }
      retry_times++;
    } while (OB_FAIL(ret) && retry_times < MAX_RETRY_TIMES);
  }
  return ret;
}

/*====================================ObStorageCachePolicyPrewarmer====================================*/
ObStorageCachePolicyPrewarmer::ObStorageCachePolicyPrewarmer(const int64_t parallelism)
    : ObMacroBlockAsyncReader(parallelism)
{}

ObStorageCachePolicyPrewarmer::~ObStorageCachePolicyPrewarmer()
{}

bool ObStorageCachePolicyPrewarmer::is_shared_object_meta_macro_(
    const MacroBlockId &macro_id,
    const char *macro_block_buf,
    const int64_t macro_block_size)
{
  bool bret = false;
  uint16_t magic = 0;
  if (OB_LIKELY(macro_id.is_valid() && macro_id.is_meta() && macro_block_size >= sizeof(magic))
      && OB_NOT_NULL(macro_block_buf)) {
    MEMCPY(&magic, macro_block_buf, sizeof(magic));
    bret = (magic == ObSharedObjectHeader::OB_SHARED_BLOCK_HEADER_MAGIC);
  }
  return bret;
}

int ObStorageCachePolicyPrewarmer::prewarm_hot_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObStorageCacheTabletTask *task)
{
  int ret = OB_SUCCESS;
  stat_.reset();
  ObSSMicroCache *micro_cache = nullptr;
  ObSEArray<MacroBlockId, 128> block_ids;
  const uint64_t tenant_id = MTL_ID();
  block_ids.set_attr(ObMemAttr(tenant_id, "BlockIDs"));
  int64_t max_prewarm_size = 0;
  
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid()) || OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ls_id), K(tablet_id), KPC(task));
  } else if (OB_FAIL(get_major_blocks_(tablet_id, block_ids))) {
    LOG_WARN("fail to get_major_blocks", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMicroCache is NULL", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(micro_cache->get_available_space_for_prewarm(max_prewarm_size))) {
    LOG_WARN("fail to get_available_space_for_prewarm", KR(ret), K(ls_id), K(tablet_id));
  } else {
    int tmp_ret = OB_SUCCESS;
    const int64_t block_num = block_ids.count();
    int64_t cur_batch_size = used_parallelism_;
    bool prepare_eviction = false;
    stat_.set_max_micro_cache_size(max_prewarm_size);
    for (int64_t i = 0;
        OB_SUCC(ret) && i < block_num && stat_.get_micro_block_bytes() < max_prewarm_size;
        i += used_parallelism_) {
      tmp_ret = OB_SUCCESS;
      cur_batch_size = MIN(used_parallelism_, block_num - i);
      prepare_eviction = false;
      bool is_suspend = false;
      if (task->get_status() != ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING) {
        ret = OB_CANCELED;
        LOG_INFO("task not in doing status, stop prewarming",
            KR(ret), K(i), K(block_num), K(tablet_id), KPC(task));
      } else if (OB_FAIL(is_suspend_storage_cache_task(tenant_id, is_suspend))) {
        LOG_WARN("fail to get tenant config is_suspend_storage_cache_task", KR(ret), K(tenant_id));
      } else if (is_suspend) {
        ret = OB_CANCELED;
        if (OB_TMP_FAIL(task->set_status(
            ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED))) {
          LOG_WARN("fail to suspend task", KR(ret), KR(tmp_ret), 
              K(i), K(block_num), K(tablet_id), KPC(task));
        }
      } else if (OB_FAIL(micro_cache->is_ready_to_evict_cache(prepare_eviction))) {
        LOG_WARN("fail to check micro cache evict stat",
            KR(ret), K(i), K(block_num), K(tablet_id));
      } else if (prepare_eviction) {
        ret = OB_ITER_END;
        LOG_INFO("micro cache almost full, stop prewarming",
            KR(ret), K(i), K(block_num), K(tablet_id));
      } else if (OB_TMP_FAIL(batch_read_from_object_storage_with_retry(
          block_ids, i, cur_batch_size))) {
        LOG_WARN("fail to batch read macro blocks",
            KR(tmp_ret), K(i), K(block_num), K(cur_batch_size));
      } else {
        bool should_skip = false;
        for (int64_t handle_idx = 0; OB_SUCC(ret) && handle_idx <= read_handle_idx_; handle_idx++) {
          tmp_ret = OB_SUCCESS;
          should_skip = is_shared_object_meta_macro_(
              block_ids[i + handle_idx],
              read_handles_[handle_idx].get_buffer(),
              read_handles_[handle_idx].get_data_size());
          if (!should_skip && OB_TMP_FAIL(prewarm_major_macro_(
              block_ids[i + handle_idx],
              read_handles_[handle_idx].get_buffer(),
              read_handles_[handle_idx].get_data_size()))) {
            LOG_WARN("fail to prewarm major block", KR(tmp_ret), K(i), K(block_num),
                K(handle_idx), K(block_ids[i + handle_idx]), K(read_handles_[handle_idx]));
          }
        } // end iterating batch blocks
      }

      task->set_prewarm_stat(stat_);
    } // end iterating tablet
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::get_major_blocks_(
    const ObTabletID &tablet_id, ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  block_ids.reset();
  ObTenantStorageMetaService *meta_service = nullptr;
  ObGCTabletMetaInfoList tablet_meta_version_list;
  
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(meta_service = MTL(ObTenantStorageMetaService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", KR(ret));
  } else if (OB_FAIL(meta_service->get_gc_tablet_scn_arr(
      tablet_id, ObStorageObjectType::SHARED_MAJOR_META_LIST, tablet_meta_version_list))) {
    LOG_WARN("fail to get tablet_meta_versions", KR(ret), K(tablet_id));
  } else if (tablet_meta_version_list.tablet_version_arr_.count() > 0) {
    const int64_t tablet_version = tablet_meta_version_list
                                     .tablet_version_arr_
                                     .at(tablet_meta_version_list.tablet_version_arr_.count() - 1)
                                     .scn_
                                     .get_val_for_inner_table_field();
    ObSEArray<MacroBlockId, 128> tmp_block_ids;
    tmp_block_ids.set_attr(ObMemAttr(MTL_ID(), "TmpBlockIDs"));
    if (OB_FAIL(meta_service->get_shared_blocks_for_tablet(
        tablet_id, tablet_version, tmp_block_ids))) {
      LOG_WARN("fail to get tablet shared blocks", KR(ret), K(tablet_id), K(tablet_version));
    } else {
      ObStorageObjectType type = ObStorageObjectType::MAX;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_block_ids.count(); i++) {
        type = tmp_block_ids.at(i).storage_object_type();
        if (type == ObStorageObjectType::SHARED_MAJOR_META_MACRO
            || type == ObStorageObjectType::SHARED_MAJOR_DATA_MACRO) {
          if (OB_FAIL(block_ids.push_back(tmp_block_ids.at(i)))) {
            LOG_WARN("fail to push into block_ids", KR(ret),
                K(i), K(tmp_block_ids.at(i)), K(block_ids), K(block_ids.count()));
          }
        }
      } // end iterating tmp_block_ids
    }
  }

  return ret;
}

int ObStorageCachePolicyPrewarmer::prewarm_major_macro_(
    const MacroBlockId &macro_id,
    const char *macro_block_buf,
    const int64_t macro_block_size)
{
  int ret = OB_SUCCESS;
  stat_.update_macro_block_num_and_bytes(1, macro_block_size);

  ObMicroBlockBareIterator micro_iter;
  ObIndexBlockBareIterator idx_micro_iter;
  const bool is_data_block = macro_id.is_data();

  if (OB_FAIL(open_iterator_(
      macro_id, macro_block_buf, macro_block_size, micro_iter, idx_micro_iter))) {
    LOG_WARN("fail to open iterator", KR(ret),
        K(macro_id), KP(macro_block_buf), K(macro_block_size));
  } else {
    int tmp_ret = OB_SUCCESS;
    int64_t micro_offset = 0;
    ObMicroBlockData micro_data;
    ObSSMicroBlockCacheKey micro_key;
    
    while (OB_SUCC(ret)) {
      tmp_ret = OB_SUCCESS;
      micro_offset = 0;
      micro_data.reset();
      micro_key.reset();
      
      if (OB_FAIL(micro_iter.get_next_micro_block_data_and_offset(micro_data, micro_offset))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next micro data failed", KR(ret), K(macro_id), K(micro_iter));
        }
      } else {
        if (is_data_block) {
          if (OB_FAIL(idx_micro_iter.get_next_logic_micro_id(
              micro_key.logic_micro_id_, micro_key.micro_crc_))) {
            LOG_WARN("fail to get_next_logic_micro_id",
                KR(ret), K(macro_id), K(micro_iter), K(idx_micro_iter));
          } else {
            micro_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
          }
        } else {
          micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
          micro_key.micro_id_.macro_id_ = macro_id;
          micro_key.micro_id_.offset_ = micro_offset;
          micro_key.micro_id_.size_ = micro_data.size_;
        }

        if (OB_SUCC(ret)) {
          if (OB_TMP_FAIL(prewarm_major_micro_(micro_key, micro_data))) {
            LOG_WARN("fail to prewarm_major_micro_", KR(tmp_ret),
                K(macro_id), K(micro_iter), K(micro_key), K(micro_data));
          }
        } // end try insert into micro cache
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
    stat_.inc_macro_block_fail_cnt();
  }
  return ret;
}

int ObStorageCachePolicyPrewarmer::open_iterator_(
    const MacroBlockId &macro_id,
    const char *macro_block_buf,
    const int64_t macro_block_size,
    ObMicroBlockBareIterator &micro_iter,
    ObIndexBlockBareIterator &idx_micro_iter)
{
  int ret = OB_SUCCESS;
  micro_iter.reset();
  idx_micro_iter.reset();
  int64_t micro_block_cnt = -1;

  if (OB_ISNULL(macro_block_buf) || OB_UNLIKELY(!macro_id.is_valid() || macro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(macro_id), KP(macro_block_buf), K(macro_block_size));
  } else if (OB_FAIL(micro_iter.open(
      macro_block_buf, macro_block_size,
      true/*need_check_data_integrity*/, false/*need_deserialize*/))) {
    LOG_WARN("fail to open ObMicroBlockBareIterator",
        KR(ret), KP(macro_block_buf), K(macro_block_size));
  } else if (OB_FAIL(micro_iter.get_micro_block_count(micro_block_cnt))) {
    LOG_WARN("fail to get_micro_block_count", KR(ret), K(micro_iter));
  } else if (OB_UNLIKELY(micro_block_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro block cnt invalid", KR(ret), K(micro_block_cnt), K(micro_iter));
  } else if (macro_id.is_data()) {
    if (OB_FAIL(idx_micro_iter.open(
        macro_block_buf, macro_block_size,
        false/*is_macro_meta_block*/, true/*need_check_data_integrity*/))) {
      LOG_WARN("fail to open ObIndexMicroBlockRowIterator", KR(ret), K(macro_id), K(micro_iter));
    } else if (OB_UNLIKELY(micro_block_cnt != idx_micro_iter.get_row_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro block cnt and index micro block rows not match", KR(ret),
          K(micro_block_cnt), K(idx_micro_iter), K(macro_id), K(micro_iter));
    }
  }

  return ret;
}

int ObStorageCachePolicyPrewarmer::prewarm_major_micro_(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObMicroBlockData &micro_data)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  ObSSMicroBaseInfo micro_info;
  ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
  const int64_t MAX_RETRY_TIMES = 3;

  if (OB_UNLIKELY(!micro_key.is_valid() || !micro_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(micro_key), K(micro_data));
  } else {
    stat_.update_micro_block_num_and_bytes(1, micro_data.size_);
    if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObSSMicroCache is NULL", KR(ret));
    } else if (OB_FAIL(micro_cache->check_micro_block_exist(
        micro_key, micro_info, hit_type))) {
      LOG_WARN("fail to check_micro_block_exist", KR(ret), K(micro_key));
    } else if (ObSSCacheHitType::SS_CACHE_MISS == hit_type) {
      if (OB_FAIL(micro_cache->add_micro_block_cache_for_prewarm(
          micro_key, micro_data.buf_, micro_data.size_,
          ObSSMicroCacheAccessType::STORAGE_CACHE_POLICY_PREWARM_TYPE,
          MAX_RETRY_TIMES, true/*transfer_seg*/))) {
        LOG_WARN("fail to add_micro_block_cache_for_prewarm", KR(ret), K(micro_key), K(micro_data));
      } else {
        stat_.inc_micro_block_add_cnt();
      }
    } else {
      stat_.inc_micro_block_hit_cnt();
    }

    if (OB_FAIL(ret)) {
      stat_.inc_micro_block_fail_cnt();
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
