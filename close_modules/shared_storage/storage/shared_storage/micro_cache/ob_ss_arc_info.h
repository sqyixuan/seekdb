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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_ARC_INFO_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_ARC_INFO_H_

#include "lib/container/ob_heap.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"

namespace oceanbase 
{
namespace storage
{

// arc cache will be splited into 4 areas: T1、B1、T2、B2
constexpr int64_t SS_ARC_SEG_COUNT = 4;
// Order by T1、B1、T2、B2
static const int64_t ARC_T1 = 0;
static const int64_t ARC_B1 = 1;
static const int64_t ARC_T2 = 2;
static const int64_t ARC_B2 = 3;

bool is_valid_arc_seg_idx(const int64_t seg_idx);
int64_t get_arc_seg_idx(const bool is_in_l1, const bool is_in_ghost);

/*
 * This comparator is used to work in 'cold_micro_block heap'
 * We will sort micro_blocks based on their heat_val(access_time).
 */
struct SSTopNColdMicroBlockCmp
{
public:
  bool operator() (const ObSSMicroMetaSnapshot *le, const ObSSMicroMetaSnapshot *re)
  {
    bool b_ret = false;
    if (nullptr != le && nullptr != re) {
      b_ret = (le->heat_val() >= re->heat_val()); // to build a min-root heap
    }
    return b_ret;
  }

  int get_error_code() { return common::OB_SUCCESS; }
};

/*
 * Cuz we will adjust ARC segment's cached_size asynchronously, thus we need to save this info.
 * We mainly focus on micro_block count, not micro_block data size.
 * There exists two types of operation: evict and delete
 * evict: remove micro_block from T1 to B1 or from T2 to B2. It means 'keep meta but drop data'
 * delete: drop this micro_block meta and data
 */
struct ObSSARCSegOpInfo 
{
public:
  bool to_delete_;  // true: delete micro_block; false: evict micro_block
  int64_t op_cnt_;  // the count of micro_block to be evicted or deleted
  int64_t exp_iter_cnt_; // the expected count of micro_block to be choosen from micro_map
  int64_t obtained_cnt_; // the actual count of micro_block which already be choosen from micro_map

  ObSSARCSegOpInfo() : to_delete_(false), op_cnt_(0), exp_iter_cnt_(0), obtained_cnt_(0) {}
  ~ObSSARCSegOpInfo() { reset(); }
  void reset() { to_delete_ = false; op_cnt_ = 0; exp_iter_cnt_ = 0; obtained_cnt_ = 0; }
  OB_INLINE bool is_valid() const { return (op_cnt_ > 0 && exp_iter_cnt_ >= 0 && obtained_cnt_>= 0); }
  OB_INLINE bool exist_op() const { return op_cnt_ > 0; }
  OB_INLINE bool to_delete() const { return to_delete_; }
  OB_INLINE int64_t get_op_cnt() const { return op_cnt_; }
  OB_INLINE void dec_op_cnt() { --op_cnt_; }
  OB_INLINE void inc_obtained_cnt() { ++obtained_cnt_; }
  OB_INLINE void dec_obtained_cnt() { --obtained_cnt_; }
  OB_INLINE int64_t get_exp_iter_cnt() const { return exp_iter_cnt_; }
  OB_INLINE int64_t get_obtained_cnt() const { return obtained_cnt_; }
  OB_INLINE bool need_obtain_more() const { return (obtained_cnt_ < exp_iter_cnt_); }
  void set_op_info(const bool to_delete, const int64_t op_cnt) { to_delete_ = to_delete; op_cnt_ = op_cnt; }
  void update_op_info(const int64_t actual_op_cnt, const int64_t exp_iter_cnt);
  ObSSARCSegOpInfo &operator=(const ObSSARCSegOpInfo &other);

  TO_STRING_KV(K_(to_delete), K_(op_cnt), K_(exp_iter_cnt), K_(obtained_cnt));
};

/*
 * This struct is used for evict_micro task_op. For each round iteration, the acquired micro_blocks will
 * be stored in this struct. When finished current round, reuse this struct and continue next round.
 */
struct ObSSARCIterInfo
{
public:
  struct ObSSARCIterSegInfo {
  public:
    int64_t seg_cnt_;
    ObSSARCSegOpInfo op_info_;

    ObSSARCIterSegInfo() : seg_cnt_(0), op_info_() {}
    ~ObSSARCIterSegInfo() { reset(); }
    void reset() { seg_cnt_ = 0; op_info_.reset(); }
    bool is_valid() const { return (seg_cnt_ > 0 && op_info_.is_valid()); }
    int64_t get_seg_cnt() const { return seg_cnt_; }
    bool is_delete_op() const { return op_info_.to_delete(); }
    bool exist_seg_op() const { return op_info_.exist_op(); }
    int64_t get_op_cnt() const { return op_info_.get_op_cnt(); }
    void dec_op_cnt() { op_info_.dec_op_cnt(); }
    void inc_obtained_cnt() { op_info_.inc_obtained_cnt(); }
    void dec_obtained_cnt() { op_info_.dec_obtained_cnt(); }
    int64_t get_exp_iter_cnt() const { return op_info_.get_exp_iter_cnt(); }
    int64_t get_obtained_cnt() const { return op_info_.get_obtained_cnt(); }
    bool need_obtain_more_cnt() const { return op_info_.need_obtain_more(); }
    void init_iter_seg_info(const int64_t seg_cnt, const bool to_delete, const int64_t op_cnt);
    void update_iter_seg_info(const int64_t actual_op_cnt, const int64_t exp_iter_cnt);
    ObSSARCIterSegInfo &operator=(const ObSSARCIterSegInfo &other);

    TO_STRING_KV(K_(seg_cnt), K_(op_info));
  };
public:
  typedef common::hash::ObHashMap<ObSSMicroBlockCacheKey, ObSSMicroMetaSnapshot *, common::hash::NoPthreadDefendMode> SSColdMicroMap;
  typedef common::ObBinaryHeap<ObSSMicroMetaSnapshot *, SSTopNColdMicroBlockCmp, SS_MAX_ARC_FETCH_CNT> SSColdMicroHeap;

  bool is_inited_;
  SSTopNColdMicroBlockCmp cmp_op_;// comparator for micro_heap
  SSColdMicroMap cold_micro_map_; // to avoid adding the same micro_block twice
  SSColdMicroHeap t1_micro_heap_; // all these micro_heap will sort elements by heat_val
  SSColdMicroHeap b1_micro_heap_;
  SSColdMicroHeap t2_micro_heap_;
  SSColdMicroHeap b2_micro_heap_;
  SSColdMicroHeap *micro_heap_arr_[SS_ARC_SEG_COUNT];
  common::ObSEArray<ObSSARCIterSegInfo, SS_ARC_SEG_COUNT> iter_seg_arr_;
  ObArenaAllocator allocator_;
  ObSSARCIterInfo();
  ~ObSSARCIterInfo() { destroy(); }

  int init(const uint64_t tenant_id);
  void reuse();
  void reuse(const int64_t seg_idx);
  void destroy();
  bool is_delete_op_type(const int64_t seg_idx) const;
  bool need_handle_arc_seg(const int64_t seg_idx) const;
  int64_t get_op_micro_cnt(const int64_t seg_idx) const;
  int64_t get_expected_iter_micro_cnt(const int64_t seg_idx) const;
  int64_t get_obtained_micro_cnt(const int64_t seg_idx) const;
  void adjust_arc_iter_seg_info(const int64_t seg_idx);
  bool need_iterate_cold_micro(const int64_t seg_idx) const;
  bool need_handle_cold_micro(const int64_t seg_idx) const;
  int push_cold_micro(const ObSSMicroBlockCacheKey &micro_key, const ObSSMicroMetaSnapshot &cold_micro,
                      const int64_t seg_idx);
  int pop_cold_micro(const int64_t seg_idx, ObSSMicroMetaSnapshot &cold_micro);
  int get_cold_micro(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroMetaSnapshot &cold_micro) const;
  int exist_cold_micro(const ObSSMicroBlockCacheKey &micro_key, bool &is_exist) const;
  int inner_get_cold_micro(const ObSSMicroBlockCacheKey &micro_key, ObSSMicroMetaSnapshot *&cold_micro) const;
  int finish_handle_cold_micro(const int64_t seg_idx);

  TO_STRING_KV("cold_micro_cnt", cold_micro_map_.size(), K_(iter_seg_arr));

private:
  int init_iter_seg_info_arr();
};

/*
 * For ss_micro_cache, we will use ARC cache algorothm to impl it.
 * Some brief introduction: 
 */
struct ObSSARCInfo
{
public:
  struct ObSSARCSegInfo {
  public:
    common::SpinRWLock lock_;
    int64_t cnt_;            // the actual cached micro block total count
    int64_t size_;           // the actual cached micro_block total size
    ObSSARCSegInfo() : lock_(), cnt_(0), size_(0) {}
    ~ObSSARCSegInfo() { reset(); }
    ObSSARCSegInfo &operator=(const ObSSARCSegInfo &other);
    
    void reset();
    int64_t size() const;
    int64_t count() const;
    int64_t avg_micro_size() const;
    int64_t avg_micro_cnt(const int64_t total_size) const;
    bool is_empty() const;
    void get_seg_info(int64_t &size, int64_t &cnt) const;
    void update_seg_info(const int64_t delta_size, const int64_t delta_cnt);
    TO_STRING_KV(K_(cnt), K_(size));

  public:
    static const int64_t OB_SS_ARC_SEG_INFO_VERSION = 1;
    OB_UNIS_VERSION(OB_SS_ARC_SEG_INFO_VERSION);
  };

public:
  const int64_t DEF_ARC_P_OF_LIMIT_PCT = 50; // when init, p_ = limit_ * 50%
  const int64_t MAX_ARC_P_OF_LIMIT_PCT = 55; // p_ <= limit_ * 55%
  const int64_t MIN_ARC_P_OF_LIMIT_PCT = 20; // p_ >= limit_ * 20%
  const int64_t MAX_MEM_LIMIT_USAGE_PCT = 90;

public:
  int64_t limit_;      // related with cache_file_size
  int64_t work_limit_; // if (T1 + T2) micro data size is larger than it, trigger 'evict'
  int64_t max_p_;      // p_ should be less than max_p_
  int64_t min_p_;      // if p_ is more than min_p_, it should not be less than min_p_ anymore
  int64_t p_;          // the expected limit disk size of T1
  ObSSARCSegInfo seg_info_arr_[SS_ARC_SEG_COUNT]; // order by T1, B1, T2, B2
  int64_t micro_cnt_limit_; // maximum count of micro_meta that can be allocated from SmallAllocator based on current mem_limit
  common::SpinRWLock limit_lock_;

  ObSSARCInfo();
  ~ObSSARCInfo() { reset(); }
  ObSSARCInfo &operator=(const ObSSARCInfo &other);
  void reset();
  void reset_seg_info();
  void reuse();

  bool is_valid() const;
  void get_seg_info(const int64_t seg_idx, int64_t &size, int64_t &cnt) const;
  int adjust_seg_info(const bool is_in_l1, const bool is_in_ghost, const ObSSARCOpType &op_type, 
                      const int64_t delta_size, const int64_t delta_cnt);
  void update_seg_info(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, const int64_t delta_cnt);
  void set_micro_cnt_limit(const int64_t micro_cnt_limit);
  // when resize cache_file or execute evict_task , invoke this to change limit & work_limit
  void update_arc_limit(const int64_t new_limit);
  // when begin/finish free space for prewarm, invoke these two to change work_limit
  void dec_arc_work_limit_for_prewarm();
  void inc_arc_work_limit_for_prewarm();

  bool close_to_evict() const;
  bool trigger_eviction() const;
  void calc_arc_iter_info(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx) const;
  int64_t get_arc_work_limit() const;
  int64_t get_arc_p() const { return ATOMIC_LOAD(&p_); }

  int64_t get_l1_size() const;
  int64_t get_l2_size() const;
  int64_t get_valid_size() const;
  int64_t get_valid_count() const;
  int64_t get_ghost_count() const;

  TO_STRING_KV(K_(limit), K_(work_limit), K_(max_p), K_(min_p), K_(p), "T1_Seg", seg_info_arr_[ARC_T1], "B1_Seg", 
    seg_info_arr_[ARC_B1], "T2_Seg", seg_info_arr_[ARC_T2], "B2_Seg", seg_info_arr_[ARC_B2], K_(micro_cnt_limit));

public:
  static const int64_t OB_SS_ARC_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_ARC_INFO_VERSION);

private:
  void try_update_p(const int64_t delta_p);
  void do_update_arc_limit(const int64_t new_limit_val, const bool need_update_limit);
  int adjust_seg_for_new_add(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, 
                             const int64_t delta_cnt);
  int adjust_seg_for_hit_t1(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, 
                            const int64_t delta_cnt);
  int adjust_seg_for_hit_ghost(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, 
                               const int64_t delta_cnt);
  int adjust_seg_for_evict(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, 
                           const int64_t delta_cnt);
  int adjust_seg_for_delete(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, 
                            const int64_t delta_cnt);
  int adjust_seg_for_abnormal_delete(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, 
                                     const int64_t delta_cnt);
  int adjust_seg_for_expired_delete(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, 
                                    const int64_t delta_cnt);
  int adjust_seg_for_invalidate(const bool is_in_l1, const bool is_in_ghost, const int64_t delta_size, 
                                const int64_t delta_cnt);
  void calc_arc_iter_info_by_disk(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx, bool &reach_limit) const;
  void calc_arc_iter_info_by_mem(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx, bool &reach_limit) const;
  void calc_arc_seg_iter_info(ObSSARCIterInfo &arc_iter_info, const int64_t seg_idx, const int64_t exceed_size,
                              const bool to_delete) const;
  void calc_arc_seg_delta_size(const int64_t t1_size, const int64_t t2_size, const int64_t work_limit,
                               const int64_t total_delta_size, int64_t &t1_delta_size, int64_t &t2_delta_size) const;
  int64_t calc_exceed_micro_cnt_by_mem(const int64_t micro_cnt_limit) const;
};
} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_ARC_INFO_H_ */
