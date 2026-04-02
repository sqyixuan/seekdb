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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_EXECUTE_CHECKPOINT_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_EXECUTE_CHECKPOINT_OP_H_

#include "storage/shared_storage/micro_cache/task/op/ob_ss_micro_cache_op_base.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{
class ObSSLinkedPhyBlockItemWriter;

struct SSCkptBaseCtx
{
public:
  bool need_ckpt_;
  int64_t exe_round_;
  int64_t ckpt_item_cnt_;
  int64_t ckpt_cost_us_;
  int64_t total_cost_us_;
  int64_t free_blk_cnt_;
  ObSSMicroCacheSuperBlock prev_super_block_;
  ObSSMicroCacheSuperBlock cur_super_block_;

  SSCkptBaseCtx() : need_ckpt_(false), exe_round_(0), ckpt_item_cnt_(0), ckpt_cost_us_(0),
                    total_cost_us_(0), free_blk_cnt_(0), prev_super_block_(), cur_super_block_() {}
  OB_INLINE bool need_ckpt() const { return need_ckpt_; }
  virtual void reuse();
  virtual void reset();

  TO_STRING_KV(K_(need_ckpt), K_(exe_round), K_(ckpt_item_cnt), K_(ckpt_cost_us), 
    K_(total_cost_us), K_(free_blk_cnt), K_(prev_super_block), K_(cur_super_block));
};

struct SSBlkCkptCtx : public SSCkptBaseCtx
{
public:
  SSBlkCkptCtx() : SSCkptBaseCtx() {}
};

struct SSMicroCkptCtx : public SSCkptBaseCtx
{
public:
  bool need_scan_blk_;
  bool lack_phy_blk_;
  // when restart observer, we will replay ss_micro_cache ckpt async. After replay all ckpt, ss_micro_cache
  // will start to serve.
  bool need_replay_ckpt_;

  SSMicroCkptCtx() : SSCkptBaseCtx(), need_scan_blk_(false), lack_phy_blk_(false), need_replay_ckpt_(false) {}
  virtual void reset() override;
  virtual void reuse() override;
  bool need_scan_phy_blk() const { return need_scan_blk_; }
  bool need_replay_ckpt() const { return need_replay_ckpt_; }
  void enable_replay_ckpt() { need_replay_ckpt_ = true; }
  void disable_replay_ckpt() { need_replay_ckpt_ = false; };

  INHERIT_TO_STRING_KV("SSCkptBaseCtx", SSCkptBaseCtx, K_(need_scan_blk), K_(lack_phy_blk), K_(need_replay_ckpt));
};

/*-----------------------------------------checkpoint op------------------------------------------*/
class ObSSExecuteCheckpointOpBase : public ObSSMicroCacheOpBase
{
public:
  ObSSExecuteCheckpointOpBase();
  virtual ~ObSSExecuteCheckpointOpBase() { destroy(); }

  virtual int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx);
  void destroy();
  virtual int execute_checkpoint();

protected:
  int get_prev_super_block();
  int check_and_gen_checkpoint();
  virtual int check_state();
  virtual int gen_checkpoint() = 0;
  int get_ckpt_block_id_list(ObSSLinkedPhyBlockItemWriter &item_writer, common::ObIArray<int64_t> &block_id_list);
  int update_super_block(const bool is_micro_ckpt);
  int try_free_phy_blocks(const bool is_micro_ckpt, const bool succ_ckpt);
  virtual void handle_extra_func() {}

private:
  int build_cur_super_block(const bool is_micro_ckpt);
  int retry_update_ss_super_block(const bool is_micro_ckpt);

protected:
  SSCkptBaseCtx *ckpt_ctx_;
};

class ObSSExecuteBlkCheckpointOp : public ObSSExecuteCheckpointOpBase
{
public:
  ObSSExecuteBlkCheckpointOp();
  virtual ~ObSSExecuteBlkCheckpointOp() { destroy(); }

  virtual int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx) override;
  void destroy();

private:
  virtual int check_state() override;
  virtual int gen_checkpoint() override;
  int gen_phy_block_checkpoint();
  int mark_reusable_blocks(common::ObIArray<ObSSPhyBlockReuseInfo> &all_blk_info_arr);
  int build_blk_ckpt_item(common::ObIArray<ObSSPhyBlockReuseInfo> &all_blk_info_arr);

private:
  const static int64_t ROUND_PER_S = 100;
  const static int64_t BLK_INFO_CKPT_INTERVAL_ROUND = 300 * ROUND_PER_S; // 5min to ckpt phy_blk info
  const static int64_t MIN_REUSE_PHY_BLOCK_CNT = 100; // the min count of phy_block to reuse per round
  const static int64_t DEFAULT_BLK_CNT = 64;

private:
  SSBlkCkptCtx blk_ckpt_ctx_;
};

class ObSSExecuteMicroCheckpointOp : public ObSSExecuteCheckpointOpBase
{
public:
  ObSSExecuteMicroCheckpointOp();
  virtual ~ObSSExecuteMicroCheckpointOp() { destroy(); }

  virtual int init(const uint64_t tenant_id, ObSSMicroCacheTaskCtx &task_ctx) override;
  void destroy();
  void enable_replay_ckpt() { micro_ckpt_ctx_.enable_replay_ckpt(); }

private:
  virtual int check_state() override;
  virtual int gen_checkpoint() override;
  int gen_micro_meta_checkpoint();
  int update_ls_tablet_cache_info();
  int delete_invalid_micro_metas(const common::ObIArray<ObSSMicroBlockCacheKey> &invalid_micro_keys);
  int delete_expired_micro_metas(const common::ObIArray<ObSSMicroBlockCacheKey> &expired_micro_keys);

  virtual void handle_extra_func() override;
  void replay_cache_ckpt_async();
  void handle_micro_cache_stat();
  void check_config_and_update();
  void reserve_blk_and_update_arc_limit();
  void dynamic_update_arc_limit();
  void check_micro_cache_abnormal();
  void adjust_micro_ckpt_interval();

private:
  const static int64_t ROUND_PER_S = 5;
  const static int64_t SCAN_BLOCK_INTERVAL_ROUND = 300 * ROUND_PER_S; // 5min to scan reusable phy_blk
  const static int64_t MICRO_CACHE_STAT_PRINT_ROUND = 60 * ROUND_PER_S; // 1min to print stat
  const static int64_t MICRO_CACHE_LOAD_UPDATE_ROUND = 5 * ROUND_PER_S; // 5s to update micro_cache load
  const static int64_t CHECK_CONFIG_ROUND = 10 * ROUND_PER_S; // 10s to update micro cache mem limit
  const static int64_t ESTIMATE_MICRO_CKPT_BLK_CNT_ROUND = 60 * ROUND_PER_S; // 1min to re-estimate micro_ckpt_blk_cnt that need to be reserved.
  const static int64_t DEFAULT_BUCKET_NUM = 1024;
  const static int64_t CHECK_CACHE_ABNORMAL_ROUND = 120 * ROUND_PER_S; // 2min to check micro_cache abnormal or not
  const static int64_t MICRO_CACHE_ABNORMAL_STAT_CNT = 3;
  /*
  * Calculate micro_meta_ckpt scheduling interval based on micro_meta count:
  * 
  * - Each 10M micro_meta persistence takes ~60s
  * - Minimum interval is at least 600s
  * - Formula: interval = max(600s, (total_micro_cnt / 10M) * 60s)
  */
  const static int64_t MIN_MICRO_META_CKPT_INTERVAL_ROUND = 600 * ROUND_PER_S;
  const static int64_t MICRO_META_COUNT_SCALE_FACTOR = 10L * 1000 * 1000;
  const static int64_t MICRO_META_CKPT_INTERVAL_SCALE_FACTOR = 60 * ROUND_PER_S;
  const static int64_t UPDATE_MICRO_META_CKPT_INTERVAL_ROUND = 60 * ROUND_PER_S;

private:
  bool enable_update_arc_limit_;
  SSMicroCkptCtx micro_ckpt_ctx_;
  ObSSTabletCacheMap tablet_cache_info_map_;
  ObSSMicroCacheHealthStat cache_health_stat_;
  int64_t cache_abnormal_cnt_;
  int64_t micro_ckpt_interval_round_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_EXECUTE_CHECKPOINT_OP_H_ */
