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

#include "storage/shared_storage/micro_cache/task/op/ob_ss_persist_micro_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_mem_data_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
/*-----------------------------------------SSPersistOPEntry-----------------------------------------*/
SSPersistOPEntry::SSPersistOPEntry()
    : phy_blk_idx_(-1), updated_micro_cnt_(0), phy_blk_handle_(), mem_blk_handle_(), io_handle_()
{}

int SSPersistOPEntry::init(
    const int64_t phy_blk_idx, 
    ObSSPhysicalBlock *phy_blk,
    ObSSMemBlock *mem_blk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(phy_blk_handle_.is_valid() || mem_blk_handle_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(*this));
  } else if (OB_UNLIKELY(phy_blk_idx < 0) || OB_ISNULL(phy_blk) || OB_ISNULL(mem_blk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(phy_blk_idx), KP(phy_blk), KP(mem_blk));
  } else {
    phy_blk_idx_ = phy_blk_idx;
    phy_blk_handle_.set_ptr(phy_blk);
    mem_blk_handle_.set_ptr(mem_blk);
  }
  return ret;
}

void SSPersistOPEntry::reset()
{
  phy_blk_idx_ = -1;
  updated_micro_cnt_ = 0;
  phy_blk_handle_.reset();
  mem_blk_handle_.reset();
  io_handle_.reset();
}

/*-----------------------------------------ObSSPersistMicroOp-----------------------------------------*/
ObSSPersistMicroOp::ObSSPersistMicroOp()
    : ObSSMicroCacheOpBase(),
      max_mem_blk_cnt_(0),
      alloc_blk_fail_cnt_(0),
      persist_entry_arr_(nullptr),
      free_list_(),
      in_use_list_()
{}

void ObSSPersistMicroOp::destroy()
{
  if (nullptr != persist_entry_arr_ && nullptr != task_ctx_) {
    for (int64_t i = 0; i < max_mem_blk_cnt_; ++i) {
      persist_entry_arr_[i].reset();
    }
    task_ctx_->allocator_->free(persist_entry_arr_);
    persist_entry_arr_ = nullptr;
  }
  free_list_.destroy();
  in_use_list_.destroy();
  max_mem_blk_cnt_ = 0;
  alloc_blk_fail_cnt_ = 0;
  ObSSMicroCacheOpBase::destroy();
}

int ObSSPersistMicroOp::pre_alloc_persist_entry()
{
  int ret = OB_SUCCESS;
  const int64_t mem_size = sizeof(SSPersistOPEntry) * max_mem_blk_cnt_;
  if (OB_ISNULL(task_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_ctx is null", KR(ret), KP_(task_ctx));
  } else if (OB_ISNULL(persist_entry_arr_ = static_cast<SSPersistOPEntry *>(task_ctx_->allocator_->alloc(mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(max_mem_blk_cnt), K(mem_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < max_mem_blk_cnt_; ++i) {
      SSPersistOPEntry *persist_entry = nullptr;
      persist_entry = new (persist_entry_arr_ + i) SSPersistOPEntry();
      if (OB_ISNULL(persist_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("persist_entry should not be nullptr", KR(ret), K(i), KP_(persist_entry_arr));
      } else if (OB_FALSE_IT(persist_entry->reset())) {
      } else if (OB_FAIL(free_list_.push(persist_entry))) {
        LOG_WARN("fail to push persist_entry into free_list", K(ret), K(i), KPC(persist_entry));
      }
    }
  }

  if (OB_FAIL(ret) && (nullptr != persist_entry_arr_) && (nullptr != task_ctx_)) {
    task_ctx_->allocator_->free(persist_entry_arr_);
    persist_entry_arr_ = nullptr;
  }
  return ret;
}

int ObSSPersistMicroOp::init(
    const uint64_t tenant_id, 
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheOpBase::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(task_ctx.mem_data_mgr_->get_max_mem_blk_count(max_mem_blk_cnt_))) {
    LOG_WARN("fail to get max_mem_blk_cnt", KR(ret));
  } else if (OB_FAIL(free_list_.init(max_mem_blk_cnt_))) {
    LOG_WARN("fail to init free_list", KR(ret), K_(max_mem_blk_cnt));
  } else if (OB_FAIL(in_use_list_.init(max_mem_blk_cnt_))) {
    LOG_WARN("fail to init in_use_list", KR(ret), K_(max_mem_blk_cnt));
  } else if (OB_FAIL(pre_alloc_persist_entry())) {
    LOG_WARN("fail to pre alloc persist entrt", KR(ret));
  } else {
    is_inited_ = true;
  }
  
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObSSPersistMicroOp::execute_persist_micro()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (FALSE_IT(is_closed_ = (!is_op_enabled()))) { // reset 'is_closed' flag
  } else if (is_op_enabled()) {
    persist_sealed_mem_blocks();
  }
  return ret;
}

int ObSSPersistMicroOp::persist_sealed_mem_blocks()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObSSMemBlockHandle> sealed_mem_blk_handles;
  // the loop can only be exited when in_use_list.count()==0. Otherwise, clear the micro cache will hang.
  while (OB_SUCC(ret)) {
    alloc_blk_fail_cnt_ = 0;
    const int64_t max_cnt = free_list_.get_curr_total();
    if (OB_FAIL(mem_data_mgr()->get_sealed_mem_blocks(sealed_mem_blk_handles, max_cnt))) {
      LOG_WARN("fail to get sealed_mem_blocks", KR(ret), K(max_cnt));
    } else if (sealed_mem_blk_handles.count() == 0 && in_use_list_.get_curr_total() == 0) {
      break; // no sealed_mem_blocks to handle, directly exit the loop.
    } else {
      const int64_t sealed_mem_blk_cnt = sealed_mem_blk_handles.count();
      if (OB_FAIL(async_write_sealed_mem_blocks(sealed_mem_blk_handles))) {
        LOG_WARN("fail to async write sealed_mem_blocks", KR(ret), K_(alloc_blk_fail_cnt));
      } else if (OB_FALSE_IT(sealed_mem_blk_handles.reset())) {  // release ref_cnt of sealed_mem_block
      } else if (OB_FAIL(update_micro_meta_of_phy_blocks())) {
        LOG_WARN("fail to update all phy_block's micro meta", KR(ret));
      }

      // if phy_block used_cnt reach limit, exit the loop.
      if ((alloc_blk_fail_cnt_ > 0) && (alloc_blk_fail_cnt_ >= sealed_mem_blk_cnt)) {
        break;
      }
    }

    if (OB_SUCC(ret) && in_use_list_.get_curr_total() > 0) {
      ob_usleep(PER_ROUND_WATI_US);
    }
  }

  LOG_TRACE("finish to persist micro_data task", KR(ret), K_(alloc_blk_fail_cnt));
  return ret;
}

int ObSSPersistMicroOp::do_async_write_sealed_mem_block(
    SSPersistOPEntry &persist_entry, 
    const ObSSMemBlockHandle &sealed_mem_blk_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(persist_entry.is_valid() || !sealed_mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(persist_entry), K(sealed_mem_blk_handle));
  } else {
    const ObSSPhyBlockType block_type = sealed_mem_blk_handle()->is_fg_mem_blk() ? ObSSPhyBlockType::SS_CACHE_DATA_BLK
                                                                                 : ObSSPhyBlockType::SS_REORGAN_BLK;
    if (phy_blk_mgr()->exist_free_block(block_type)) {
      const uint32_t block_size = phy_blk_mgr()->get_block_size();
      const int32_t payload_offset = sealed_mem_blk_handle()->reserved_size_;
      const int32_t payload_size = sealed_mem_blk_handle()->get_payload_size();
      char *cur_buf = sealed_mem_blk_handle()->data_buf_; // promise that it is aligned

      // |--common header--|--blk header--|--cached micro data--|--cached micro index--|
      ObSSNormalPhyBlockHeader blk_header;
      blk_header.payload_offset_ = payload_offset;
      blk_header.payload_size_ = payload_size;
      blk_header.micro_count_ = sealed_mem_blk_handle()->micro_count_;
      blk_header.micro_index_offset_ = sealed_mem_blk_handle()->get_micro_index_offset();
      blk_header.micro_index_size_ = sealed_mem_blk_handle()->index_size_;

      ObSSPhyBlockCommonHeader common_header;
      const int64_t normal_blk_header_size = ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
      const int64_t common_header_size = common_header.header_size_;
      common_header.set_payload_size(payload_size + normal_blk_header_size);
      common_header.set_block_type(ObSSPhyBlockType::SS_CACHE_DATA_BLK);

      int64_t pos = common_header_size;
      if (OB_ISNULL(cur_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_buf should not be null", KR(ret), KP(cur_buf), KPC(sealed_mem_blk_handle.get_ptr()));
      } else if (OB_FAIL(sealed_mem_blk_handle()->handle_when_sealed())) {
        LOG_WARN("fail to handle sealed mem_block", KR(ret), KPC(sealed_mem_blk_handle.get_ptr()));
      } else if (FALSE_IT(MEMSET(cur_buf + pos, '\0', normal_blk_header_size))) {
      } else if (OB_UNLIKELY(!blk_header.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_blk header is invalid", KR(ret), K(blk_header));
      } else if (OB_FAIL(blk_header.serialize(cur_buf, block_size, pos))) {
        LOG_WARN("fail to serialize phy_blk_header", KR(ret), K(block_size), K(blk_header), KP(cur_buf));
      } else if (FALSE_IT(common_header.calc_payload_checksum(cur_buf + common_header_size, 
                 common_header.payload_size_))) {
      } else if (OB_UNLIKELY(!common_header.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy_blk common header is invalid", KR(ret), K(common_header));
      } else if (FALSE_IT(pos = 0)) {
      } else if (OB_FAIL(common_header.serialize(cur_buf, block_size, pos))) {
        LOG_WARN("fail to serialize phy_blk common header", KR(ret), K(block_size), K(common_header), KP(cur_buf));
      } else {
        int64_t phy_blk_idx = -1;
        int64_t phy_blk_offset = -1;
        ObSSPhysicalBlock *phy_blk = nullptr;
        ObSSPhysicalBlockHandle phy_blk_handle;
        if (OB_FAIL(phy_blk_mgr()->alloc_block(phy_blk_idx, phy_blk_handle, block_type))) {
          LOG_WARN("fail to alloc free phy_block", KR(ret), K(block_type));
        } else if (OB_UNLIKELY(!phy_blk_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block handle should be valid", KR(ret), K(phy_blk_idx), K(block_type), K(phy_blk_handle));
        } else if (FALSE_IT(phy_blk_offset = phy_blk_idx * block_size)) {
        } else if (OB_FAIL(ObSSMicroCacheIOHelper::async_write_block(phy_blk_offset, block_size, cur_buf, 
                   phy_blk_handle, persist_entry.io_handle_))) {
          LOG_WARN("fail to write micro_cache block", KR(ret), K(phy_blk_offset), K(block_size), KP(cur_buf));
        } else if (OB_FAIL(persist_entry.init(phy_blk_idx, phy_blk_handle.get_ptr(), sealed_mem_blk_handle.get_ptr()))) {
          LOG_WARN("fail to init persist entry", KR(ret), K(phy_blk_idx), K(phy_blk_handle), K(sealed_mem_blk_handle));
        }

        if (OB_FAIL(ret) && (-1 != phy_blk_idx)) {
          // can't set this phy_block sealed right now, cuz if write timeout, there may exist a flying write_request,
          // if we reuse this phy_block and write new data into it, the flying write may overwrite it.
          if (OB_TMP_FAIL(phy_blk_mgr()->add_reusable_block(phy_blk_idx))) {
            LOG_WARN("fail to add reusable block", KR(ret), KR(tmp_ret), K(phy_blk_idx));
          }
        }
      }
    } else {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObSSPersistMicroOp::async_write_sealed_mem_blocks(
    ObArray<ObSSMemBlockHandle> &sealed_mem_blk_handles)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sealed_mem_blk_handles.count(); ++i) {
    SSPersistOPEntry *persist_entry = nullptr;
    ObSSMemBlockHandle &sealed_mem_blk_handle = sealed_mem_blk_handles[i];
    if (OB_FAIL(free_list_.pop(persist_entry))) {
      LOG_WARN("fail to pop persist_entry from free_list", KR(ret));
    } else if (OB_ISNULL(persist_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("persist_entry should not be nullptr", KR(ret), KP(persist_entry));
    } else if (OB_FAIL(do_async_write_sealed_mem_block(*persist_entry, sealed_mem_blk_handle))) {
      if (OB_EAGAIN == ret) {
        ++alloc_blk_fail_cnt_; // used to adjust persist_task schedule interval.
      } else {
        LOG_WARN("fail to async write sealed_mem_block", KR(ret), KPC(persist_entry));
      }
    } else if (OB_FAIL(in_use_list_.push(persist_entry))) {
      LOG_ERROR("fail to push persist_entry into in_use_list", KR(ret), K(i), K(in_use_list_.get_curr_total()));
    }

    // if fail, recycle persist_entry and push mem_block into sealed_mem_blk_list.
    if (OB_FAIL(ret)) {
      int64_t updated_micro_cnt = 0;
      bool valid_phy_blk = false;
      if (nullptr != persist_entry) {
        updated_micro_cnt = persist_entry->updated_micro_cnt_;
        valid_phy_blk = persist_entry->phy_blk_handle_.is_valid();
        persist_entry->reset();
        if (OB_TMP_FAIL(free_list_.push(persist_entry))) {
          LOG_ERROR("fail to push persist_entry into free_list", KR(tmp_ret), KP(persist_entry),
              K(free_list_.get_curr_total()));
        }
      }
      if (OB_TMP_FAIL(handle_sealed_mem_block(updated_micro_cnt, valid_phy_blk, sealed_mem_blk_handle))) {
        LOG_ERROR("fail to handle sealed mem_block", KR(tmp_ret), K(updated_micro_cnt),
            K(valid_phy_blk), K(sealed_mem_blk_handle));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next sealed_mem_blk
  }
  return ret;
}

int ObSSPersistMicroOp::do_update_micro_meta_of_phy_block(SSPersistOPEntry &persist_entry)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!persist_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("persist_entry should be valid", KR(ret), K(persist_entry));
  } else {
    ObSSMemBlockHandle &sealed_mem_blk_handle = persist_entry.mem_blk_handle_;
    const ObSSPhysicalBlockHandle &phy_blk_handle = persist_entry.phy_blk_handle_;
    // must update valid_len before update_micro_block_meta
    const int64_t blk_valid_len = sealed_mem_blk_handle()->valid_val_;
    phy_blk_handle()->set_sealed(blk_valid_len);

    int64_t updated_micro_size = 0;
    const uint32_t block_size = phy_blk_mgr()->get_block_size();
    const int64_t phy_blk_idx = persist_entry.phy_blk_idx_;
    const int64_t phy_blk_offset = phy_blk_idx * block_size;
    if (OB_FAIL(micro_meta_mgr()->update_micro_block_meta(sealed_mem_blk_handle, phy_blk_offset,
        phy_blk_handle()->reuse_version_, updated_micro_size, persist_entry.updated_micro_cnt_))) {
      LOG_ERROR("fail to update micro block meta, fatal error", KR(ret), K(phy_blk_idx), KPC(phy_blk_handle.get_ptr()),
        KPC(sealed_mem_blk_handle.get_ptr()), K(updated_micro_size), K(blk_valid_len));
    } else {
      LOG_TRACE("ss_cache: succ to persist mem_blk", K(phy_blk_idx), K(sealed_mem_blk_handle), 
          KPC(sealed_mem_blk_handle.get_ptr()), K(updated_micro_size), K(blk_valid_len), 
          "updated_micro_cnt", persist_entry.updated_micro_cnt_);
    }

    const int32_t delta_len = blk_valid_len - updated_micro_size;
    if (OB_UNLIKELY(delta_len > 0)) {
      if (OB_TMP_FAIL(phy_blk_handle()->dec_valid_len(phy_blk_idx, delta_len * -1))) {
        LOG_WARN("fail to update valid_len by delta", KR(ret), KR(tmp_ret), K(delta_len), K(phy_blk_idx),
          K(sealed_mem_blk_handle));
      }
    } else if (OB_UNLIKELY(delta_len < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("updated_micro_size shouldn't be larger than blk_valid_len", 
          KR(ret), K(blk_valid_len), K(updated_micro_size));
    }
  }
  return ret;
}

int ObSSPersistMicroOp::update_micro_meta_of_phy_blocks()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t in_use_list_cnt = in_use_list_.get_curr_total();
  for (int64_t i = 0; OB_SUCC(ret) && i < in_use_list_cnt; ++i) {
    SSPersistOPEntry *persist_entry = nullptr;
    bool io_finished = false;
    if (OB_FAIL(in_use_list_.pop(persist_entry))) {
      LOG_WARN("fail to pop persist_entry from in_use_list", KR(ret));
    } else if (OB_ISNULL(persist_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("persist_entry should not be nullptr", KR(ret), KP(persist_entry));
    } else if (OB_UNLIKELY(!persist_entry->io_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io handle should be valid", KR(ret), KPC(persist_entry));
    } else if (OB_FAIL(persist_entry->io_handle_.check_is_finished(io_finished))) {
      LOG_WARN("fail to check io_handle is_finished", KR(ret), KPC(persist_entry));
    } else {
      if (io_finished) {
        if (OB_FAIL(persist_entry->io_handle_.wait())) {
          LOG_WARN("fail to wait", KR(ret), KPC(persist_entry));
        } else if (OB_FAIL(do_update_micro_meta_of_phy_block(*persist_entry))) {
          LOG_WARN("fail to update micro_meta of phy_block", KR(ret), KPC(persist_entry));
        }
      } else {
        // io_finished=false, push it back to in_use_list and wait until the next round to process it.
        if (OB_FAIL(in_use_list_.push(persist_entry))) {
          LOG_ERROR("fail to push persist_entry into in_use_list", KR(ret), KPC(persist_entry),
              K(in_use_list_.get_curr_total()));
        } else {
          // make sure persist_entry and mem_block won't be recycled if io_finished=false.
          persist_entry = nullptr;
        }
      }
    }

    // Only when io_finished=false, persist_entry is put back into in_use_list, persist_entry and mem_block do not
    // need to be recycled. In other cases, no matter succ or fail, persist_entry and mem_block must be recycled.
    if (nullptr != persist_entry) {
      const int64_t updated_micro_cnt = persist_entry->updated_micro_cnt_;
      const bool valid_phy_blk = persist_entry->phy_blk_handle_.is_valid();
      ObSSMemBlockHandle &mem_blk_handle = persist_entry->mem_blk_handle_;
      if (OB_TMP_FAIL(handle_sealed_mem_block(updated_micro_cnt, valid_phy_blk, mem_blk_handle))) {
        LOG_ERROR("fail to handle sealed mem_block", KR(tmp_ret), KPC(persist_entry));
      }
      persist_entry->reset();
      if (OB_TMP_FAIL(free_list_.push(persist_entry))) {
        LOG_ERROR("fail to push persist_entry into free_list", KR(tmp_ret), KPC(persist_entry), 
            K(free_list_.get_curr_total()));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next sealed_mem_blk
  }
  return ret;
}

int ObSSPersistMicroOp::handle_sealed_mem_block(
    const int64_t updated_micro_cnt,
    const bool valid_phy_blk,
    ObSSMemBlockHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handle));
  } else if (!valid_phy_blk) {
    // 1. If fail to alloc phy_block, add it into sealed_mem_blk_list and wait for next persist.
    if (OB_FAIL(mem_data_mgr()->add_into_sealed_block_list(mem_blk_handle))) {
      LOG_WARN("fail to add into sealed mem_block list", KR(ret), K(mem_blk_handle));
    }
  } else {
    // 2. If a micro_block fail to update meta, it's data_dest_ still points to the memory address of mem_block.
    //    If this mem_block is allocated dynamically, accessing this micro_block again may cause memory core. So it is
    //    necessary to delete these micro_block's meta from meta_map.
    if ((updated_micro_cnt < mem_blk_handle()->valid_count_) &&
        OB_FAIL(micro_meta_mgr()->try_delete_micro_block_meta(mem_blk_handle))) {
      LOG_ERROR("fail to try delete micro_block meta", KR(ret), K(updated_micro_cnt), K(mem_blk_handle));
    }

    // 3. Finally, try to free this mem_block.
    if (OB_FAIL(ret)) {
    } else {
      ObSSMemBlock *mem_blk = mem_blk_handle.get_ptr();
      mem_blk_handle.reset();
      if (OB_NOT_NULL(mem_blk)) {
        bool succ_free = false;
        if (OB_FAIL(mem_blk->try_free(succ_free))) {
          LOG_WARN("fail to try free this mem_block", KR(ret), KP(mem_blk));
        } else if (!succ_free) {
          LOG_INFO("this mem_blk may be refered, can't free right now", KP(mem_blk));
        }
      }
    }
  }
  return ret;
}

} // storage
} // oceanbase
