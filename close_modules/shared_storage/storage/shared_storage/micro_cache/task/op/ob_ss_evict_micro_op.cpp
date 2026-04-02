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

#include "storage/shared_storage/micro_cache/task/op/ob_ss_evict_micro_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/*-----------------------------------------SSHandleSegOpCtx------------------------------------------*/
void SSHandleSegOpCtx::reset()
{
  need_handle_ = false;
  to_delete_ = false;
  handle_cnt_ = 0;
  handle_size_ = 0;
  exe_round_ = 0;
  start_time_us_ = ObTimeUtility::current_time();
  exe_cost_us_ = 0;
}

bool SSHandleSegOpCtx::can_run_next_round() const
{
  return ((exe_round_ < MAX_EXECUTE_ROUND) && (exe_cost_us_ < MAX_HANDLE_ONE_SEG_OP_TIME_US));
}

void SSHandleSegOpCtx::finish_one_round()
{
  ++exe_round_;
  exe_cost_us_ = ObTimeUtility::current_time() - start_time_us_;
}

/*-----------------------------------------SSHandleOpCtx------------------------------------------*/
void SSHandleOpCtx::reset()
{
  MEMSET(need_handle_, 0, sizeof(need_handle_));
  MEMSET(handle_cnt_, 0, sizeof(handle_cnt_));
  total_handle_cnt_ = 0;
  total_handle_size_ = 0;
  total_handle_cost_us_ = 0;
}

bool SSHandleOpCtx::handled_seg_op(const int64_t seg_idx)
{
  bool b_ret = false;
  if (is_valid_arc_seg_idx(seg_idx)) {
    b_ret = (need_handle_[seg_idx] && (handle_cnt_[seg_idx] > 0));
  }
  return b_ret;
}

void SSHandleOpCtx::add_handle_seg_op_ctx(const int64_t seg_idx, const SSHandleSegOpCtx &handle_seg_op_ctx)
{
  if (is_valid_arc_seg_idx(seg_idx)) {
    need_handle_[seg_idx] |= handle_seg_op_ctx.need_handle_;
    handle_cnt_[seg_idx] += handle_seg_op_ctx.handle_cnt_;
    total_handle_cnt_ += handle_seg_op_ctx.handle_cnt_;
    total_handle_size_ += handle_seg_op_ctx.handle_size_;
    total_handle_cost_us_ += handle_seg_op_ctx.exe_cost_us_;
  }
}

/*-----------------------------------------ObSSEvictMicroOp------------------------------------------*/
ObSSEvictMicroOp::ObSSEvictMicroOp()
  : ObSSMicroCacheOpBase(), arc_iter_info_(), handle_op_ctx_()
{}

void ObSSEvictMicroOp::destroy()
{
  handle_op_ctx_.reset();
  arc_iter_info_.destroy();
  ObSSMicroCacheOpBase::destroy();
}

int ObSSEvictMicroOp::init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheOpBase::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arc_iter_info_.init(tenant_id))) {
    LOG_WARN("fail to init arc_iter_info", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSEvictMicroOp::execute_eviction()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (FALSE_IT(is_closed_ = (!is_op_enabled()))) { // reset 'is_closed' flag
  } else if (is_op_enabled()) {
    handle_arc_seg_op();
  }
  return ret;
}

int ObSSEvictMicroOp::handle_arc_seg_op()
{
  int ret = OB_SUCCESS;
  int64_t seg_idx = ARC_T1;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  }

  while (OB_SUCC(ret) && (seg_idx < SS_ARC_SEG_COUNT) && (!handle_op_ctx_.is_timeout())) {
    if (OB_FAIL(handle_one_arc_seg_op(seg_idx))) {
      LOG_WARN("fail to handle one arc_seg op", KR(ret), K(seg_idx));
    }
    arc_iter_info_.reuse(seg_idx);
    if (handle_op_ctx_.handled_seg_op(seg_idx)) {
      micro_meta_mgr()->check_cache_mem_limit();
    }
    ++seg_idx;
  }

  if (handle_op_ctx_.exist_handle_op()) {
    const int64_t cost_us = get_cost_us();
    LOG_TRACE("ss_cache: finish handle arc op", KR(ret), "arc_info", micro_meta_mgr()->get_arc_info(),
      K_(handle_op_ctx), K(cost_us));
  }
  return ret;
}

int ObSSEvictMicroOp::handle_one_arc_seg_op(const int64_t seg_idx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    SSHandleSegOpCtx handle_seg_op_ctx;
    bool finish_handle = false;
    const ObSSARCInfo &arc_info = micro_meta_mgr()->get_arc_info();

    while (OB_SUCC(ret) && (handle_seg_op_ctx.can_run_next_round()) && (!finish_handle)) {
      arc_info.calc_arc_iter_info(arc_iter_info_, seg_idx);
      if (arc_iter_info_.need_handle_arc_seg(seg_idx)) {
        handle_seg_op_ctx.need_handle_ = true;
        arc_iter_info_.adjust_arc_iter_seg_info(seg_idx);
        if (OB_FAIL(micro_meta_mgr()->acquire_cold_micro_blocks(arc_iter_info_, seg_idx))) {
          LOG_WARN("fail to acquire cold micro_blocks", KR(ret), K(seg_idx), K_(arc_iter_info), K(arc_info),
            K(handle_seg_op_ctx));
        } else {
          finish_handle = (arc_iter_info_.get_obtained_micro_cnt(seg_idx) < arc_iter_info_.get_op_micro_cnt(seg_idx));
          if (arc_iter_info_.need_handle_cold_micro(seg_idx)) {
            if (OB_FAIL(handle_cold_micro_blocks(seg_idx, handle_seg_op_ctx))) {
              LOG_WARN("fail to handle cold micro_blocks", KR(ret), K(seg_idx), K_(arc_iter_info), K(handle_seg_op_ctx));
            }
          }

          LOG_TRACE("finish handle some cold micro", K(finish_handle), K(seg_idx), K_(arc_iter_info));
        }
      } else {
        finish_handle = true;
      }
      handle_seg_op_ctx.finish_one_round();
    }

    if (OB_SUCC(ret)) {
      handle_op_ctx_.add_handle_seg_op_ctx(seg_idx, handle_seg_op_ctx);
    }
  }
  return ret;
}

int ObSSEvictMicroOp::handle_cold_micro_blocks(const int64_t seg_idx, SSHandleSegOpCtx &handle_seg_op_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  
  const bool to_delete = arc_iter_info_.is_delete_op_type(seg_idx);
  while (OB_SUCC(ret) && arc_iter_info_.need_handle_cold_micro(seg_idx)) {
    ObSSMicroMetaSnapshot cold_micro;
    if (OB_FAIL(arc_iter_info_.pop_cold_micro(seg_idx, cold_micro))) {
      LOG_WARN("fail to pop cold micro_meta_handle", KR(ret), K_(arc_iter_info), K(seg_idx));
    } else if (OB_FAIL(handle_cold_micro_block(seg_idx, cold_micro, to_delete, handle_seg_op_ctx))) {
      LOG_WARN("fail to handle cold micro_block", KR(ret), K(seg_idx), K(cold_micro), K(to_delete), 
        K_(arc_iter_info), K(handle_seg_op_ctx));
    }
  }
  return ret;
}

int ObSSEvictMicroOp::handle_cold_micro_block(
    const int64_t seg_idx,
    const ObSSMicroMetaSnapshot &cold_micro,
    const bool to_delete,
    SSHandleSegOpCtx &handle_seg_op_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    if (to_delete && OB_FAIL(micro_meta_mgr()->try_delete_micro_block_meta(cold_micro))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // skip it
      } else {
        LOG_WARN("fail to try delete micro_block meta", KR(ret), K(cold_micro));
      }
    } else if (!to_delete && OB_FAIL(micro_meta_mgr()->try_evict_micro_block_meta(cold_micro))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // skip it
      } else {
        LOG_WARN("fail to try evict micro_block meta", KR(ret), K(cold_micro));
      }
    } else {
      const int32_t micro_size = cold_micro.length();
      handle_seg_op_ctx.update_handle_info(1, micro_size);
      if (!to_delete) {
        const int32_t delta_size = micro_size * -1;
        const uint64_t data_dest = cold_micro.data_dest();
        const uint64_t reuse_version = cold_micro.reuse_version();
        int64_t phy_blk_idx = -1;
        if (OB_TMP_FAIL(phy_blk_mgr()->update_block_valid_length(data_dest, reuse_version, delta_size, phy_blk_idx))) {
          if (OB_S2_REUSE_VERSION_MISMATCH == tmp_ret) {
            tmp_ret = OB_SUCCESS;
            LOG_TRACE("fail to update phy_block len cuz reuse_version mismatch", K(data_dest), K(reuse_version),
              K(delta_size));
          } else {
            LOG_ERROR("fail to update block valid length", KR(tmp_ret), K(cold_micro), K(data_dest), 
              K(reuse_version), K(delta_size), K(phy_blk_idx));
          }
        }
      }
      // decrease iter_info's seg_op count
      if (FAILEDx(arc_iter_info_.finish_handle_cold_micro(seg_idx))) {
        LOG_WARN("fail to finish handle cold_micro", KR(ret), K(seg_idx), K(to_delete), K(cold_micro));
      }
    }
  }
  return ret;
}

void ObSSEvictMicroOp::clear_for_next_round()
{
  handle_op_ctx_.reset();
}

} // storage
} // oceanbase
