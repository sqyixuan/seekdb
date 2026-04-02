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

#include "storage/shared_storage/micro_cache/task/op/ob_ss_execute_checkpoint_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_writer.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------SSCkptBaseCtx------------------------------------------*/
void SSCkptBaseCtx::reuse()
{
  need_ckpt_ = false;
  ckpt_item_cnt_ = 0;
  ckpt_cost_us_ = 0;
  total_cost_us_ = 0;
  free_blk_cnt_ = 0;
  prev_super_block_.reset();
  cur_super_block_.reset();
}

void SSCkptBaseCtx::reset() 
{
  need_ckpt_ = false;
  ckpt_item_cnt_ = 0;
  ckpt_cost_us_ = 0;
  total_cost_us_ = 0;
  free_blk_cnt_ = 0;
  exe_round_ = 0;
  prev_super_block_.reset();
  cur_super_block_.reset();
}

/*-----------------------------------------SSMicroCkptCtx------------------------------------------*/
void SSMicroCkptCtx::reuse()
{
  SSCkptBaseCtx::reuse();
  need_scan_blk_ = false;
  lack_phy_blk_ = false;
}

void SSMicroCkptCtx::reset()
{
  SSCkptBaseCtx::reset();
  need_scan_blk_ = false;
  lack_phy_blk_ = false;
}

/*-----------------------------------------ObSSExecuteCheckpointOpBase------------------------------------------*/
ObSSExecuteCheckpointOpBase::ObSSExecuteCheckpointOpBase()
  : ckpt_ctx_(nullptr)
{}

int ObSSExecuteCheckpointOpBase::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMicroCacheOpBase::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  }
  return ret;
}

void ObSSExecuteCheckpointOpBase::destroy()
{
  ckpt_ctx_ = nullptr;
  ObSSMicroCacheOpBase::destroy();
}

int ObSSExecuteCheckpointOpBase::execute_checkpoint()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), KP_(task_ctx));
  } else {
    check_and_gen_checkpoint();
  }
  return ret;
}

int ObSSExecuteCheckpointOpBase::check_and_gen_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_state())) {
    LOG_WARN("fail to check state", KR(ret));
  } else if (OB_FAIL(gen_checkpoint())) {
    LOG_WARN("fail to gen checkpoint", KR(ret));
  }

  if (is_op_enabled()) {
    handle_extra_func();
  }
  return ret;
}

int ObSSExecuteCheckpointOpBase::check_state()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else {
    ckpt_ctx_->reuse();
    ++ckpt_ctx_->exe_round_;
  }
  return ret;
}

int ObSSExecuteCheckpointOpBase::get_prev_super_block()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (OB_FAIL(phy_blk_mgr()->get_ss_super_block(ckpt_ctx_->prev_super_block_))) {
    LOG_WARN("fail to get ss_super_block", KR(ret), KPC_(ckpt_ctx));
  } else if (OB_UNLIKELY(!ckpt_ctx_->prev_super_block_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_super_block must be valid", KR(ret), KPC_(ckpt_ctx));
  }
  return ret;
}

int ObSSExecuteCheckpointOpBase::get_ckpt_block_id_list(
    ObSSLinkedPhyBlockItemWriter &item_writer,
    common::ObIArray<int64_t> &block_id_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (OB_FAIL(item_writer.get_block_id_list(block_id_list))) {
    LOG_ERROR("fail to get block_id list", KR(ret), KPC_(ckpt_ctx));
  }
  return ret;
}

int ObSSExecuteCheckpointOpBase::build_cur_super_block(const bool is_micro_ckpt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else {
    const ObSSMicroCacheSuperBlock &prev_super_blk = ckpt_ctx_->prev_super_block_;
    ObSSMicroCacheSuperBlock &cur_super_blk = ckpt_ctx_->cur_super_block_;
    if (OB_FAIL(cur_super_blk.assign_by_ckpt(is_micro_ckpt, prev_super_blk))) {
      LOG_WARN("fail to assign super_blk by ckpt", KR(ret), K(is_micro_ckpt), KPC_(ckpt_ctx));
    }
  }
  return ret;
}

int ObSSExecuteCheckpointOpBase::update_super_block(const bool is_micro_ckpt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (is_op_enabled()) {
    // Cuz blk_ckpt may execute very frequently, it will cause micro_ckpt fail to update super_blk 
    // continuously. Thus, we need to put 'get-build-update' these threee ops into one func to use lock.
    const int64_t exp_modify_time = ckpt_ctx_->prev_super_block_.modify_time_us_;
    if (OB_FAIL(build_cur_super_block(is_micro_ckpt))) {
      LOG_WARN("fail to build cur ss_super_block", KR(ret), K(is_micro_ckpt), KPC_(ckpt_ctx));
    } else if (OB_FAIL(phy_blk_mgr()->update_ss_super_block(exp_modify_time, ckpt_ctx_->cur_super_block_))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(retry_update_ss_super_block(is_micro_ckpt))) {
          LOG_WARN("fail to retry updating ss_super_blk", KR(ret), K(is_micro_ckpt), KPC_(ckpt_ctx));
        }
      } else {
        LOG_WARN("fail to update ss_super_block", KR(ret), KPC_(ckpt_ctx)); 
      }
    }
    
    if (FAILEDx(tnt_file_mgr()->fsync_cache_file())) {
      LOG_ERROR("fail to fsync micro_cache file", KR(ret), KPC_(ckpt_ctx));
    }
  }
  return ret;
}

int ObSSExecuteCheckpointOpBase::retry_update_ss_super_block(const bool is_micro_ckpt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else {
    ObSSMicroCacheSuperBlock &prev_super_blk = ckpt_ctx_->prev_super_block_;
    ObSSMicroCacheSuperBlock &cur_super_blk = ckpt_ctx_->cur_super_block_;
    if (OB_FAIL(phy_blk_mgr()->update_ss_super_block_for_ckpt(is_micro_ckpt, prev_super_blk, cur_super_blk))) {
      LOG_WARN("fail to update ss_super_blk for ckpt", KR(ret), KPC_(ckpt_ctx));
    }
  }
  return ret;
}

int ObSSExecuteCheckpointOpBase::try_free_phy_blocks(const bool is_micro_ckpt, const bool succ_ckpt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_) || OB_ISNULL(ckpt_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx), KP_(ckpt_ctx));
  } else if (is_op_enabled()) {
    ObSSMicroCacheSuperBlock &tmp_super_blk = (succ_ckpt ? ckpt_ctx_->prev_super_block_ : ckpt_ctx_->cur_super_block_);
    const int64_t DEF_ITEM_CNT = ObSSMicroCacheSuperBlock::DEFAULT_ITEM_CNT;
    const ObSEArray<int64_t, DEF_ITEM_CNT> *tmp_entry_list = nullptr;
    tmp_super_blk.get_ckpt_entry_list(is_micro_ckpt, tmp_entry_list);
    if (OB_ISNULL(tmp_entry_list)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ckpt_entry_list should not be null", KR(ret), KP(tmp_entry_list), KPC_(ckpt_ctx));
    } else if (OB_FAIL(phy_blk_mgr()->try_free_batch_blocks(*tmp_entry_list, ckpt_ctx_->free_blk_cnt_))) {
      LOG_WARN("fail to free reusable phy_blocks", KR(ret), K(succ_ckpt), KPC_(ckpt_ctx));
    }
  }
  return ret;
}

/*-----------------------------------------ObSSExecuteBlkCheckpointOp------------------------------------------*/
ObSSExecuteBlkCheckpointOp::ObSSExecuteBlkCheckpointOp()
  : ObSSExecuteCheckpointOpBase(), blk_ckpt_ctx_()
{}

void ObSSExecuteBlkCheckpointOp::destroy()
{
  blk_ckpt_ctx_.reset();
  ObSSExecuteCheckpointOpBase::destroy();
}

int ObSSExecuteBlkCheckpointOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSExecuteCheckpointOpBase::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else {
    ckpt_ctx_ = &blk_ckpt_ctx_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSExecuteBlkCheckpointOp::check_state()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSExecuteCheckpointOpBase::check_state())) {
    LOG_WARN("fail to check op_base state", KR(ret), K_(blk_ckpt_ctx));
  } else if ((blk_ckpt_ctx_.exe_round_ >= BLK_INFO_CKPT_INTERVAL_ROUND) ||
             (phy_blk_mgr()->get_reusable_set_count() >= MIN_REORGAN_BLK_CNT)) {
    // To reduce the frequency of blk_ckpt, it is only executed when the number of reusable blocks exceeds 
    // the minimum number of sparse blocks required to trigger a reorganization task.
    blk_ckpt_ctx_.need_ckpt_ = true;
  }
  return ret;
}

int ObSSExecuteBlkCheckpointOp::gen_checkpoint()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (FALSE_IT(is_closed_ = (!is_op_enabled()))) { // reset 'is_closed' flag
  } else if (is_op_enabled() && blk_ckpt_ctx_.need_ckpt()) {
    if (OB_FAIL(get_prev_super_block())) {
      LOG_WARN("fail to get prev ss_super_block", KR(ret));
    } else if (OB_FAIL(gen_phy_block_checkpoint())) {
      LOG_WARN("fail to gen phy_block checkpoint", KR(ret));
    } else if (OB_FAIL(update_super_block(false/*is_micro_ckpt*/))) {
      LOG_WARN("fail to update super_block", KR(ret));
    }

    const bool succ_ckpt = ((OB_SUCCESS == ret) ? true : false);
    if (OB_TMP_FAIL(try_free_phy_blocks(false/*is_micro_ckpt*/, succ_ckpt))) {
      LOG_WARN("fail to try free phy_blocks", KR(tmp_ret), K(succ_ckpt));
      ret = tmp_ret;
    }

    blk_ckpt_ctx_.exe_round_ = 0;
    blk_ckpt_ctx_.total_cost_us_ = get_cost_us();
    LOG_TRACE("ss_cache: finish ss_micro_cache phy_blk ckpt", KR(ret), K_(blk_ckpt_ctx));
  }
  return ret;
}

int ObSSExecuteBlkCheckpointOp::gen_phy_block_checkpoint()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (is_op_enabled()) {
    ObSEArray<ObSSPhyBlockReuseInfo, DEFAULT_BLK_CNT> all_blk_info_arr;
    if (OB_FAIL(phy_blk_mgr()->scan_blocks_to_ckpt(all_blk_info_arr))) {
      LOG_WARN("fail to scan all phy_blocks' reuse_info", KR(ret));
    } else if (OB_FAIL(mark_reusable_blocks(all_blk_info_arr))) {
      LOG_WARN("fail to mark reusable blocks", KR(ret));
    } else if (OB_FAIL(build_blk_ckpt_item(all_blk_info_arr))) {
      LOG_WARN("fail to build blk ckpt item", KR(ret));
    } else {
      blk_ckpt_ctx_.ckpt_cost_us_ = ObTimeUtility::current_time() - start_us;
      cache_stat()->task_stat_.update_phy_ckpt_cnt(1, blk_ckpt_ctx_.ckpt_item_cnt_);
    }
  }
  return ret;
}

int ObSSExecuteBlkCheckpointOp::mark_reusable_blocks(ObIArray<ObSSPhyBlockReuseInfo> &all_blk_info_arr)
{
  int ret = OB_SUCCESS;
  const int64_t total_blk_cnt = all_blk_info_arr.count();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (blk_ckpt_ctx_.prev_super_block_.exist_checkpoint()) {
    const int64_t phy_ckpt_cnt = blk_ckpt_ctx_.prev_super_block_.blk_ckpt_entry_list_.count();
    // 1. If previous ckpt is valid, get these phy_blocks reuse info, try to reuse it later.
    for (int64_t i = 0; OB_SUCC(ret) && (i < phy_ckpt_cnt); ++i) {
      const int64_t blk_idx = blk_ckpt_ctx_.prev_super_block_.blk_ckpt_entry_list_.at(i);
      if (OB_LIKELY((blk_idx >= SS_CACHE_SUPER_BLOCK_CNT) && (blk_idx < total_blk_cnt) && 
                    (all_blk_info_arr.at(blk_idx - SS_CACHE_SUPER_BLOCK_CNT).blk_idx_ == blk_idx))) {
        all_blk_info_arr.at(blk_idx - SS_CACHE_SUPER_BLOCK_CNT).need_reuse_ = 1;
      }
    }
  }
  
  if (OB_SUCC(ret)) {
    ObSEArray<int64_t, DEFAULT_BLK_CNT> reusable_blk_arr;
    // 2. Select some phy_blocks from reusable_set, try to reuse it later.
    if (OB_FAIL(phy_blk_mgr()->get_reusable_blocks(reusable_blk_arr, MIN_REUSE_PHY_BLOCK_CNT))) {
      LOG_WARN("fail to get reusable blocks", KR(ret));
    } else if (reusable_blk_arr.count() > 0) {
      const int64_t reusable_blk_cnt = reusable_blk_arr.count();
      for (int64_t i = 0; OB_SUCC(ret) && (i < reusable_blk_cnt); ++i) {
        const int64_t blk_idx = reusable_blk_arr.at(i);
        if (OB_LIKELY((blk_idx >= SS_CACHE_SUPER_BLOCK_CNT) && (blk_idx < total_blk_cnt) && 
                      (all_blk_info_arr.at(blk_idx - SS_CACHE_SUPER_BLOCK_CNT).blk_idx_ == blk_idx))) {
          all_blk_info_arr.at(blk_idx - SS_CACHE_SUPER_BLOCK_CNT).need_reuse_ = 1;
        }
      }
    }
  }
  return ret;
}

int ObSSExecuteBlkCheckpointOp::build_blk_ckpt_item(ObIArray<ObSSPhyBlockReuseInfo> &all_blk_info_arr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  ObSSLinkedPhyBlockItemWriter item_writer;
  ObCompressorType compressor_type = ObCompressorType::NONE_COMPRESSOR;
  ObSEArray<int64_t, DEFAULT_BLK_CNT> unhandled_phy_blks;

  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_cache is null", KR(ret), KP(micro_cache));
  } else if (OB_FALSE_IT(compressor_type = micro_cache->get_blk_ckpt_compressor_type())) {
  } else if (OB_FAIL(item_writer.init(tenant_id_, *(phy_blk_mgr()), ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK,
             compressor_type))) {
    LOG_WARN("fail to init linked phy_block item writer", KR(ret), K_(tenant_id), K(compressor_type));
  } else {
    const int64_t item_buf_size = sizeof(ObSSPhyBlockPersistInfo);
    char item_buf[item_buf_size];
    const int64_t total_blk_cnt = all_blk_info_arr.count();
    blk_ckpt_ctx_.ckpt_item_cnt_ = 0;
    // use prev_ckpt_entry_list to save all blk_idx of phy_blks which need to be reused
    ObIArray<int64_t> &prev_ckpt_entry_list = blk_ckpt_ctx_.prev_super_block_.blk_ckpt_entry_list_;
    prev_ckpt_entry_list.reset();
    prev_ckpt_entry_list.reserve(total_blk_cnt);

    for (int64_t i = 0; OB_SUCC(ret) && (i < total_blk_cnt) && is_op_enabled(); ++i) {
      const int64_t blk_idx = all_blk_info_arr.at(i).blk_idx_;
      ObSSPhyBlockPersistInfo persist_info;
      persist_info.blk_idx_ = blk_idx;
      // For one phy_block, it will be inc reuse_version or not in this round checkpoint_task.
      // If it was the prev ckpt phy_block or it was choosen from reusable_set, it can be considered to reuse.
      // For these 'need_reuse' phy_blocks, we need to check any one of them can be inc reuse_version or not, 
      // if can't be inc reuse_version, need to readd it into reusable_set.
      bool need_reuse = all_blk_info_arr.at(i).need_reuse_;
      bool can_inc_reuse_version = false;
      if (need_reuse && !all_blk_info_arr.at(i).reach_gc_reuse_version()) {
        can_inc_reuse_version = true;
      }

      if (can_inc_reuse_version) {
        persist_info.reuse_version_ = all_blk_info_arr.at(i).next_reuse_version_;
        if (OB_FAIL(prev_ckpt_entry_list.push_back(blk_idx))) {
          LOG_WARN("fail to push back", KR(ret), K(blk_idx), K(total_blk_cnt));
        }
      } else {
        persist_info.reuse_version_ = all_blk_info_arr.at(i).reuse_version_;
        if (need_reuse) {
          if (OB_FAIL(unhandled_phy_blks.push_back(blk_idx))) {
            LOG_WARN("fail to push back", KR(ret), K(blk_idx), "arr_cnt", unhandled_phy_blks.count());
          }
        }
      }
      
      if (OB_SUCC(ret)) {
        int64_t pos = 0;
        if (OB_UNLIKELY(!persist_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block persist info is invalid", KR(ret), K(persist_info), K(i), K(total_blk_cnt));
        } else if (OB_FAIL(persist_info.serialize(item_buf, item_buf_size, pos))) {
          LOG_WARN("fail to serialize phy_blk persist_info", KR(ret), K(i), K(item_buf_size), K(persist_info));
        } else if (OB_FAIL(item_writer.write_item(item_buf, pos))) {
          LOG_WARN("fail to write item", KR(ret), K(i), K(pos), K(item_buf_size));
        } else {
          ++blk_ckpt_ctx_.ckpt_item_cnt_;
        }
      }
    }
  }

  if (FAILEDx(item_writer.close())) {
    LOG_WARN("fail to close item writer", KR(ret));
  }

  // no matter the prev logic succ or not, we need to get block_id list, cuz we need to free these blocks.
  if (OB_TMP_FAIL(get_ckpt_block_id_list(item_writer, blk_ckpt_ctx_.cur_super_block_.blk_ckpt_entry_list_))) {
    LOG_WARN("fail to get ckpt block_id list", KR(ret), KR(tmp_ret), K_(blk_ckpt_ctx));
    tmp_ret = OB_SUCCESS;
  }

  // no matter the prev logic succ or not, we need to add these blocks into reusable list, otherwise they will leak
  if (!unhandled_phy_blks.empty() && OB_TMP_FAIL(phy_blk_mgr()->add_batch_reusable_blocks(unhandled_phy_blks))) {
    LOG_ERROR("fail to add batch reusable phy_blocks, will leak!!!", KR(ret), KR(tmp_ret), K(unhandled_phy_blks), 
      "arr_cnt", unhandled_phy_blks.count(), K(unhandled_phy_blks));
    tmp_ret = OB_SUCCESS;
  }

  return ret;
}

/*-----------------------------------------ObSSExecuteMicroCheckpointOp------------------------------------------*/
ObSSExecuteMicroCheckpointOp::ObSSExecuteMicroCheckpointOp()
  : ObSSExecuteCheckpointOpBase(), enable_update_arc_limit_(false), micro_ckpt_ctx_(), tablet_cache_info_map_(),
    cache_health_stat_(), cache_abnormal_cnt_(0), micro_ckpt_interval_round_(MIN_MICRO_META_CKPT_INTERVAL_ROUND)
{}

void ObSSExecuteMicroCheckpointOp::destroy()
{
  enable_update_arc_limit_ = false;
  micro_ckpt_ctx_.reset();
  tablet_cache_info_map_.destroy();
  cache_health_stat_.reset();
  cache_abnormal_cnt_ = 0;
  micro_ckpt_interval_round_ = MIN_MICRO_META_CKPT_INTERVAL_ROUND;
  ObSSExecuteCheckpointOpBase::destroy();
}

int ObSSExecuteMicroCheckpointOp::init(
    const uint64_t tenant_id,
    ObSSMicroCacheTaskCtx &task_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSExecuteCheckpointOpBase::init(tenant_id, task_ctx))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_cache_info_map_.create(DEFAULT_BUCKET_NUM, ObMemAttr(tenant_id, "SSTabletInfoMap")))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(tenant_id));
  } else {
    enable_update_arc_limit_ = true;
    ckpt_ctx_ = &micro_ckpt_ctx_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSExecuteMicroCheckpointOp::check_state()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSExecuteCheckpointOpBase::check_state())) {
    LOG_WARN("fail to check op_base state", KR(ret), K_(micro_ckpt_ctx));
  } else {
    tablet_cache_info_map_.clear();
    if (micro_ckpt_ctx_.exe_round_ % SCAN_BLOCK_INTERVAL_ROUND == 0) {
      micro_ckpt_ctx_.need_scan_blk_ = true;
    }
    if (micro_ckpt_ctx_.exe_round_ >= micro_ckpt_interval_round_) {
      micro_ckpt_ctx_.need_ckpt_ = true;
    }
  }
  return ret;
}

int ObSSExecuteMicroCheckpointOp::gen_checkpoint()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (FALSE_IT(is_closed_ = (!is_op_enabled()))) { // reset 'is_closed' flag
  } else if (micro_ckpt_ctx_.need_scan_phy_blk()) {
    if (OB_TMP_FAIL(phy_blk_mgr()->scan_blocks_to_reuse())) {
      LOG_WARN("fail to scan blocks to reuse", KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    }
  }
  
  if (OB_SUCC(ret) && is_op_enabled() && micro_ckpt_ctx_.need_ckpt()) {
    ObSEArray<uint64_t, 128> reuse_version_arr;
    if (OB_TMP_FAIL(phy_blk_mgr()->get_block_reuse_version(reuse_version_arr))) {
      LOG_WARN("fail to get block reuse_version", KR(tmp_ret));
    }

    if (OB_FAIL(phy_blk_mgr()->get_ss_super_block(micro_ckpt_ctx_.prev_super_block_))) {
      LOG_WARN("fail to get ss_super_block", KR(ret));
    } else if (OB_UNLIKELY(!micro_ckpt_ctx_.prev_super_block_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss_super_block must be valid", KR(ret), K_(micro_ckpt_ctx));
    } else if (OB_FAIL(gen_micro_meta_checkpoint())) {
      LOG_WARN("fail to try gen micro_meta checkpoint", KR(ret));
    } else if (OB_FAIL(update_super_block(true/*is_micro_ckpt*/))) {
      LOG_WARN("fail to update super_block", KR(ret));
    } else if (OB_TMP_FAIL(phy_blk_mgr()->update_block_gc_reuse_version(reuse_version_arr))) {
      LOG_WARN("fail to update block gc_reuse_version", KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    }

    const bool succ_ckpt = ((OB_SUCCESS == ret) ? true : false);
    if (OB_TMP_FAIL(try_free_phy_blocks(true/*is_micro_ckpt*/, succ_ckpt))) {
      LOG_WARN("fail to try free phy_blocks", KR(tmp_ret), K(succ_ckpt));
      ret = tmp_ret;
    }

    micro_ckpt_ctx_.total_cost_us_ = get_cost_us();
    micro_ckpt_ctx_.exe_round_ = 0;
    LOG_TRACE("ss_cache: finish ss_micro_cache micro ckpt", KR(ret), K_(micro_ckpt_ctx));
  }
  return ret;
}

int ObSSExecuteMicroCheckpointOp::gen_micro_meta_checkpoint()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  const int64_t start_us = ObTimeUtility::current_time();
  ObCompressorType compressor_type = ObCompressorType::NONE_COMPRESSOR;
  ObSSLinkedPhyBlockItemWriter item_writer;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_cache is null", KR(ret), KP(micro_cache));
  } else if (OB_FALSE_IT(compressor_type = micro_cache->get_micro_ckpt_compressor_type())) {
  } else if (!is_op_enabled()) { // skip it
  } else if (OB_FAIL(item_writer.init(tenant_id_, *(phy_blk_mgr()), ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK,
             compressor_type))) {
    LOG_WARN("fail to init linked phy_block item writer", KR(ret), K_(tenant_id), K(compressor_type));
  } else {
    bool is_first_scan = true;
    ObSEArray<ObSSMicroBlockMetaHandle, 64> ckpt_micro_handles;
    ObSEArray<ObSSMicroBlockCacheKey, 8> invalid_micro_keys;
    ObSEArray<ObSSMicroBlockCacheKey, 8> expired_micro_keys;
    const int64_t item_buf_size = sizeof(ObSSMicroBlockMeta) + 128; // make sure the buf is enough
    char item_buf[item_buf_size];
    micro_ckpt_ctx_.ckpt_item_cnt_ = 0;
    bool iter_end = false;
    do {
      // For each round, we only iterate some macros' micro_metas.
      if (OB_FAIL(micro_meta_mgr()->scan_micro_blocks_for_checkpoint(ckpt_micro_handles, 
          invalid_micro_keys, expired_micro_keys, tablet_cache_info_map_, is_first_scan))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to scan micro_blocks", KR(ret));
        } else {
          iter_end = true;
          ret = OB_SUCCESS;
        }
      } 
      
      if (OB_SUCC(ret)) {
        const int64_t ckpt_micro_cnt = ckpt_micro_handles.count();
        for (int64_t i = 0; OB_SUCC(ret) && (i < ckpt_micro_cnt); ++i) {
          const ObSSMicroBlockMetaHandle &micro_meta_handle = ckpt_micro_handles.at(i);
          int64_t pos = 0;
          if (OB_UNLIKELY(!micro_meta_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("micro_meta should not be null", KR(ret), K(i), K(micro_meta_handle));
          } else if (OB_LIKELY(micro_meta_handle()->is_persisted() &&
                               (micro_meta_handle()->is_in_ghost() || micro_meta_handle()->is_valid_field()))) {
            if (OB_FAIL(micro_meta_handle()->serialize(item_buf, item_buf_size, pos))) {
              LOG_WARN("fail to serialize micro_block meta", KR(ret), K(i), KPC(micro_meta_handle.get_ptr()), 
                K(item_buf_size));
            } else if (OB_FAIL(item_writer.write_item(item_buf, pos))) {
              if (OB_EAGAIN == ret) {
                LOG_WARN("micro_ckpt phy_block is not enough", KR(ret), K_(micro_ckpt_ctx));
              } else {
                LOG_WARN("fail to write micro_ckpt item", KR(ret), K(i), K(pos), K(item_buf_size), K_(micro_ckpt_ctx));
              }
            } else {
              ++micro_ckpt_ctx_.ckpt_item_cnt_;
            }
          }
        }

        if (OB_EAGAIN == ret) {
          micro_ckpt_ctx_.lack_phy_blk_ = true;
          ret = OB_SUCCESS;
        }
      }
      ckpt_micro_handles.reset(); // use 'reset()', not 'reuse()', otherwise micro_meta ref_cnt won't dec. 
      is_first_scan = false;
    } while (OB_SUCC(ret) && (!iter_end) && is_op_enabled());

    if (FAILEDx(item_writer.close())) {
      if (OB_EAGAIN == ret) {
        micro_ckpt_ctx_.lack_phy_blk_ = true;
        LOG_WARN("micro_ckpt phy_block is not enough", KR(ret), K_(micro_ckpt_ctx));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to close item writer", KR(ret), K_(micro_ckpt_ctx));
      }
    } else {
      micro_ckpt_ctx_.cur_super_block_.update_micro_ckpt_time();
      cache_stat()->task_stat_.update_micro_ckpt_cnt(1, micro_ckpt_ctx_.ckpt_item_cnt_);
    }

    // no matter the prev logic succ or not, we need to get block_id list, cuz we need to free these blocks.
    if (OB_TMP_FAIL(get_ckpt_block_id_list(item_writer, micro_ckpt_ctx_.cur_super_block_.micro_ckpt_entry_list_))) {
      LOG_WARN("fail to get ckpt block_id list", KR(ret), KR(tmp_ret), K_(micro_ckpt_ctx));
      tmp_ret = OB_SUCCESS;
    }
    if (OB_TMP_FAIL(delete_invalid_micro_metas(invalid_micro_keys))) {
      LOG_WARN("fail to delete invalid micro_block_meta", KR(ret), KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    }
    if (OB_TMP_FAIL(delete_expired_micro_metas(expired_micro_keys))) {
      LOG_WARN("fail to delete expired micro_block_meta", KR(ret), KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    }
    if (OB_TMP_FAIL(update_ls_tablet_cache_info())) {
      LOG_WARN("fail to update ls/tablet cache info", KR(ret), KR(tmp_ret));
      tmp_ret = OB_SUCCESS;
    }

    micro_ckpt_ctx_.ckpt_cost_us_ = ObTimeUtility::current_time() - start_us;
  }
  return ret;
}

int ObSSExecuteMicroCheckpointOp::delete_invalid_micro_metas(const ObIArray<ObSSMicroBlockCacheKey> &invalid_micro_keys)
{
  int ret = OB_SUCCESS;
  const int64_t invalid_micro_cnt = invalid_micro_keys.count();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (invalid_micro_cnt > 0) {
    int64_t delete_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && (i < invalid_micro_cnt); ++i) {
      const ObSSMicroBlockCacheKey &micro_key = invalid_micro_keys.at(i);
      if (OB_FAIL(micro_meta_mgr()->try_delete_invalid_micro_block_meta(micro_key))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to try delete invalid micro_block meta", KR(ret), K(micro_key));
          ret = OB_SUCCESS; // skip it, continue to delete next one
        }
      } else {
        ++delete_cnt;
      }
    }
    if (delete_cnt > 0) {
      LOG_INFO("succ to delete invalid micro_meta", K(delete_cnt));
    }
  }
  return ret;
}

int ObSSExecuteMicroCheckpointOp::delete_expired_micro_metas(const ObIArray<ObSSMicroBlockCacheKey> &expired_micro_keys)
{
  int ret = OB_SUCCESS;
  const int64_t expired_micro_cnt = expired_micro_keys.count();
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (expired_micro_cnt > 0) {
    int64_t delete_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && (i < expired_micro_cnt); ++i) {
      const ObSSMicroBlockCacheKey &micro_key = expired_micro_keys.at(i);
      ObSSMicroBaseInfo expired_micro_info;
      if (OB_FAIL(micro_meta_mgr()->try_delete_expired_micro_block_meta(micro_key, expired_micro_info))) {
        LOG_WARN("fail to try delete expired micro_block meta", KR(ret), K(micro_key));
      } else if (expired_micro_info.is_valid()) {
        ++delete_cnt;
        if (!expired_micro_info.is_in_ghost_ && expired_micro_info.is_persisted_) {
          const uint64_t data_dest = expired_micro_info.data_dest_;
          const uint64_t reuse_version = expired_micro_info.reuse_version_;
          const int32_t delta_size = expired_micro_info.size_ * -1;
          int64_t phy_blk_idx = -1;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(phy_blk_mgr()->update_block_valid_length(data_dest, reuse_version, delta_size, phy_blk_idx))) {
            if (OB_S2_REUSE_VERSION_MISMATCH == tmp_ret) {
              LOG_TRACE("fail to update block valid length", K(micro_key), K(expired_micro_info));
            } else {
              LOG_ERROR("fail to update block valid length", KR(tmp_ret), K(micro_key), K(expired_micro_info));
            }
          }
        }
      }
    }
    if (delete_cnt > 0) {
      LOG_INFO("succ to delete expired micro_meta", K(delete_cnt));
    }
  }
  return ret;
}

int ObSSExecuteMicroCheckpointOp::update_ls_tablet_cache_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_FAIL(ObSSMicroCacheUtil::calc_ls_tablet_cache_info(tenant_id_, tablet_cache_info_map_,
             micro_ckpt_ctx_.cur_super_block_.ls_info_list_,
             micro_ckpt_ctx_.cur_super_block_.tablet_info_list_))) {
    LOG_WARN("fail to calc ls/tablet cache info", KR(ret), K_(tenant_id), K_(micro_ckpt_ctx));
  }
  return ret;
}

void ObSSExecuteMicroCheckpointOp::handle_extra_func()
{
  replay_cache_ckpt_async();
  reserve_blk_and_update_arc_limit();
  handle_micro_cache_stat();
  check_config_and_update();
  check_micro_cache_abnormal();
  adjust_micro_ckpt_interval();
}

void ObSSExecuteMicroCheckpointOp::replay_cache_ckpt_async()
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (IS_NOT_INIT || OB_ISNULL(task_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), K_(is_inited), KP_(task_ctx));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss_micro_cache is null", KR(ret), KP(micro_cache));
  } else if (micro_ckpt_ctx_.need_replay_ckpt()) {
    if (OB_FAIL(micro_cache->read_checkpoint())) {
      LOG_ERROR("fail to read ss_micro_cache checkpoint", KR(ret), KPC(cache_stat()));
    } else {
      // ret = EventTable::EN_SHARED_STORAGE_MICRO_CACHE_REPLAY_CKPT_ERR;
    }

    if (OB_FAIL(ret)) {
      // NOTICE: before clear_micro_cache, execute these two steps
      LOG_INFO("start to clear ss_micro_cache cuz fail to replay ckpt", KPC(cache_stat()));
      micro_cache->enable_cache();
      is_closed_ = true;
      micro_cache->clear_micro_cache();
      LOG_INFO("finish to clear ss_micro_cache cuz fail to replay ckpt", KPC(cache_stat()));
    } else {
      micro_cache->get_task_runner().resume_extra_task_for_ckpt();
      micro_cache->enable_cache();
      LOG_INFO("succ to replay ss_micro_cache ckpt, will enable ss_micro_cache");
    }

    micro_ckpt_ctx_.disable_replay_ckpt(); // only needs to replay once
  }
}

void ObSSExecuteMicroCheckpointOp::handle_micro_cache_stat()
{
  int ret = OB_SUCCESS;
  if (micro_ckpt_ctx_.exe_round_ % MICRO_CACHE_LOAD_UPDATE_ROUND == 0) {
    if (nullptr != task_ctx_ && task_ctx_->is_valid()) {
      ObSSMicroCacheStat *cache_stat_ptr = task_ctx_->cache_stat_;
      const int64_t interval_s = MICRO_CACHE_LOAD_UPDATE_ROUND / ROUND_PER_S; // ROUND_PER_S must be greater than 0
      cache_stat_ptr->cache_load().update_common_io_load_info(cache_stat_ptr->io_stat().common_io_param_, interval_s);
      cache_stat_ptr->cache_load().update_prewarm_io_load_info(cache_stat_ptr->io_stat().prewarm_io_param_, interval_s);
    }
  }

  if (micro_ckpt_ctx_.exe_round_ % MICRO_CACHE_STAT_PRINT_ROUND == 0) {
    if (nullptr != task_ctx_ && task_ctx_->is_valid()) {
      ObSSMicroCacheStat *cache_stat_ptr = task_ctx_->cache_stat_;
      cache_stat_ptr->phy_blk_stat_.set_reusable_block_cnt(task_ctx_->phy_blk_mgr_->get_reusable_set_count());
      cache_stat_ptr->phy_blk_stat_.set_sparse_block_cnt(task_ctx_->phy_blk_mgr_->get_sparse_block_cnt());
      const ObSSARCInfo &arc_info = task_ctx_->micro_meta_mgr_->get_arc_info();
      LOG_INFO("ss_micro_cache stat", KPC(cache_stat_ptr), K(arc_info));
    }
  }
}

void ObSSExecuteMicroCheckpointOp::check_config_and_update()
{
  if (micro_ckpt_ctx_.exe_round_ % CHECK_CONFIG_ROUND == 0) {
    if (nullptr != task_ctx_ && task_ctx_->is_valid()) {
      task_ctx_->micro_meta_mgr_->update_cache_mem_limit_by_config();
      task_ctx_->micro_meta_mgr_->update_cache_expiration_time_by_config();
      task_ctx_->la_micro_mgr_->check_stop_record_la_micro_key();
    }
  }
}

void ObSSExecuteMicroCheckpointOp::dynamic_update_arc_limit()
{
  if (OB_LIKELY(enable_update_arc_limit_)) {
    const int64_t new_cache_limit_size = task_ctx_->phy_blk_mgr_->get_cache_limit_size();
    task_ctx_->micro_meta_mgr_->update_arc_limit(new_cache_limit_size);
  }
}

void ObSSExecuteMicroCheckpointOp::reserve_blk_and_update_arc_limit()
{
  if (micro_ckpt_ctx_.need_ckpt() || micro_ckpt_ctx_.exe_round_ % ESTIMATE_MICRO_CKPT_BLK_CNT_ROUND == 0) {
    if (nullptr != task_ctx_ && task_ctx_->is_valid()) {
      int64_t available_cnt = 0;
      task_ctx_->phy_blk_mgr_->reserve_blk_for_micro_ckpt(available_cnt);
      dynamic_update_arc_limit();
    }
  }
}

void ObSSExecuteMicroCheckpointOp::adjust_micro_ckpt_interval()
{
  if (micro_ckpt_ctx_.exe_round_ % UPDATE_MICRO_META_CKPT_INTERVAL_ROUND == 0) {
    if (nullptr != task_ctx_ && task_ctx_->is_valid()) {
      const int64_t total_micro_cnt = task_ctx_->cache_stat_->micro_stat().get_total_micro_cnt();
      const int64_t new_interval_round = MAX(MIN_MICRO_META_CKPT_INTERVAL_ROUND,
          total_micro_cnt / MICRO_META_COUNT_SCALE_FACTOR * MICRO_META_CKPT_INTERVAL_SCALE_FACTOR);
      micro_ckpt_interval_round_ = new_interval_round;
    }
  }
}

// if this function runs 3 times and within these 3 periods, all "add_op" fail to execute,
// we will think that micro_cache is abnormal.
void ObSSExecuteMicroCheckpointOp::check_micro_cache_abnormal()
{
  int ret = OB_SUCCESS;
  if (micro_ckpt_ctx_.exe_round_ % CHECK_CACHE_ABNORMAL_ROUND == 0) {
    if (nullptr != task_ctx_ && task_ctx_->is_valid()) {
      ObSSMicroCacheStat *cache_stat_ptr = task_ctx_->cache_stat_;
      const int64_t cur_add_cnt = cache_stat_ptr->io_stat().get_add_cnt();
      const int64_t cur_fail_add_cnt = cache_stat_ptr->hit_stat().get_fail_add_cnt();
      cache_health_stat_.update_newest_add_info(cur_add_cnt, cur_fail_add_cnt);
      if (cache_health_stat_.exist_increament_io(10/*at least 10 add_micro_cache requests*/) &&
          cache_health_stat_.is_all_increament_io_failed()) {
        ++cache_abnormal_cnt_;
      } else {
        cache_abnormal_cnt_ = 0;
      }

      if (cache_abnormal_cnt_ >= MICRO_CACHE_ABNORMAL_STAT_CNT) {
        const ObSSARCInfo &arc_info = task_ctx_->micro_meta_mgr_->get_arc_info();
        LOG_ERROR("ss_micro_cache may be in abnormal state", KPC(cache_stat_ptr), K(arc_info));
      }
    }
  }
}

} // storage
} // oceanbase
