/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_index_skip_scanner.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
ObAdvanceSkipScanner::ObAdvanceSkipScanner(const ObStorageDatumUtils &datum_utils)
  : is_inited_(false),
    left_border_reached_(false),
    micro_start_(-1),
    micro_last_(-1),
    micro_current_(-1),
    datum_utils_(datum_utils),
    complete_range_(),
    range_datums_(nullptr),
    read_info_(nullptr),
    stmt_alloc_(nullptr)
{
}

ObAdvanceSkipScanner::~ObAdvanceSkipScanner()
{
  if (OB_LIKELY(nullptr != range_datums_ && nullptr != stmt_alloc_)) {
    stmt_alloc_->free(range_datums_);
  }
}

void ObAdvanceSkipScanner::reuse()
{
  left_border_reached_ = false;
  micro_start_ = -1;
  micro_last_ = -1;
  micro_current_ = -1;
}

void ObAdvanceSkipScanner::reset()
{
  is_inited_ = false;
  left_border_reached_ = false;
  micro_start_ = -1;
  micro_last_ = -1;
  micro_current_ = -1;
  complete_range_.reset();
  if (OB_LIKELY(nullptr != range_datums_ && nullptr != stmt_alloc_)) {
    stmt_alloc_->free(range_datums_);
  }
  range_datums_ = nullptr;
  read_info_ = nullptr;
  stmt_alloc_ = nullptr;
}

int ObAdvanceSkipScanner::init(
    const bool is_reverse_scan,
    const ObDatumRange &scan_range,
    const ObITableReadInfo &read_info,
    common::ObIAllocator &stmt_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(this), K(lbt()));
  } else if (OB_UNLIKELY(is_reverse_scan)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("reverse scan is not supported", KR(ret), K(is_reverse_scan));
  } else if (OB_FAIL(prepare_ranges(scan_range, read_info.get_schema_rowkey_count(), stmt_allocator))) {
    LOG_WARN("failed to prepare range", KR(ret));
  } else {
    read_info_ = &read_info;
    stmt_alloc_ = &stmt_allocator;
    is_inited_ = true;
    LOG_TRACE("[INDEX SKIP SCAN] success to init", KR(ret), K(*this));
  }
  return ret;
}

int ObAdvanceSkipScanner::switch_info(
    const bool is_reverse_scan,
    const ObDatumRange &scan_range,
    const ObITableReadInfo &read_info,
    common::ObIAllocator &stmt_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this), K(lbt()));
  } else if (OB_UNLIKELY(is_reverse_scan)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("reverse scan is not supported", KR(ret), K(is_reverse_scan));
  } else if (OB_UNLIKELY(read_info_ != &read_info || stmt_alloc_ != &stmt_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument in rescan", KR(ret),
             KP_(read_info), KP(&read_info), KP_(stmt_alloc), KP(&stmt_allocator), K(lbt()));
  } else if (OB_FAIL(prepare_ranges(scan_range, read_info.get_schema_rowkey_count(), stmt_allocator))) {
    LOG_WARN("failed to prepare ranges", KR(ret));
  } else {
    LOG_TRACE("[INDEX SKIP SCAN] success to switch info", KR(ret), K(*this));
  }
  return ret;
}

int ObAdvanceSkipScanner::advance_scan(const ObDatumRange &scan_range)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this), K(lbt()));
  } else if (OB_FAIL(scan_range.end_key_.compare(complete_range_.end_key_, datum_utils_, cmp_ret))) {
    LOG_WARN("failed to compare end_key_", KR(ret), K(scan_range), K_(complete_range));
  } else if (cmp_ret > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("endkey is larger", KR(ret), K(cmp_ret), K(scan_range), K_(complete_range));
  } else if (OB_FAIL(scan_range.start_key_.compare(complete_range_.start_key_, datum_utils_, cmp_ret))) {
    LOG_WARN("failed to compare start_key_", KR(ret), K(scan_range), K_(complete_range));
  } else if (cmp_ret <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("startkey is smaller", KR(ret), K(cmp_ret), K(scan_range), K_(complete_range));
  } else {
    complete_range_.start_key_.reuse();
    for (int64_t i = 0; i < scan_range.start_key_.datum_cnt_; ++i) {
      complete_range_.start_key_.datums_[i] = scan_range.start_key_.datums_[i];
    }
    complete_range_.set_border_flag(scan_range.border_flag_);
    left_border_reached_ = false;
  }
  LOG_DEBUG("[INDEX SKIP SCAN] advance scan", KR(ret), K(scan_range), K_(complete_range), KPC(this));
  return ret;
}

int ObAdvanceSkipScanner::skip(ObMicroIndexInfo &index_info, ObIndexSkipState &prev_state, ObIndexSkipState &state)
{
  int ret = OB_SUCCESS;
  bool state_determined = false;
  const ObCommonDatumRowkey &endkey = index_info.endkey_;
  LOG_DEBUG("[INDEX SKIP SCAN] try skip data by endkey", K(prev_state), K(state), K(endkey), K_(complete_range),
            KPC(this), K(lbt()));
  if (left_border_reached_) {
    LOG_DEBUG("[INDEX SKIP SCAN] left border reached, no need to skip", K(prev_state), K(state),
              K(endkey), K_(complete_range), KPC(this));
  } else {
    int64_t left_ne_pos = -1;
    int left_cmp_ret = 0;
    const bool cmp_datum_cnt = true;
    const ObDatumRowkey &left_border = complete_range_.start_key_;
    const ObBorderFlag &border_flag = complete_range_.border_flag_;
    if (OB_UNLIKELY(!complete_range_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected complete range", KR(ret), K(prev_state), K(state), K_(complete_range), K(endkey), KPC(this));
    } else if (OB_FAIL(endkey.compare(left_border, datum_utils_, left_cmp_ret, cmp_datum_cnt))) {
      LOG_WARN("failed to compare left border", KR(ret), K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
    } else if (left_cmp_ret < 0 || (0 == left_cmp_ret && !border_flag.inclusive_start())) {
      state_determined = true;
      // CASE:1.1 in forward scan
      // the prefix of current node's endkey is less than the complete range's startkey
      // the entire node can be skipped
      state.set_state(0, ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT);
      LOG_TRACE("[INDEX SKIP SCAN] can skip", K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
    }
    LOG_DEBUG("[INDEX SKIP SCAN] check left border", KR(ret), K(left_cmp_ret), K(left_ne_pos),
              K(prev_state), K(state), K(endkey), K_(complete_range), KPC(this));
  }
  if (OB_SUCC(ret) && !state_determined) {
    state.set_state(0, ObIndexSkipNodeState::PREFIX_UNCERTAIN);
  }
  prev_state = state;
  return ret;
}

int ObAdvanceSkipScanner::skip(
    ObIMicroBlockRowScanner &micro_scanner,
    ObMicroIndexInfo &index_info,
    const bool first)
{
  int ret = OB_SUCCESS;
  const bool is_left_border = index_info.is_left_border();
  const bool is_right_border = index_info.is_right_border();
  ObIndexSkipState &state = index_info.skip_state_;
  LOG_DEBUG("[INDEX SKIP SCAN] skip in micro", K(first), K(state), K(micro_scanner), KPC(this));
  if (state.is_skipped()) {
    micro_scanner.skip_to_end();
  } else if (left_border_reached_ && !first) {
    // CASE:1.2 in forward scan
    // left_border_reached_ = true, this means the new left border is already reached
    // first = false means the micro block is already opened
    // this call of skip is after scanning the micro block, so we can set the state to PREFIX_SKIPPED_LEFT
    state.set_state(0, ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT);
  } else if (OB_UNLIKELY(!micro_scanner.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scanner state", KR(ret), K(state), K(micro_scanner), KPC(this));
  } else if (first || -1 == micro_current_) { // -1 == micro_current_ means forward scan in an already opened micro block
    if (OB_FAIL(micro_scanner.end_of_block())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to check end of block", KR(ret), K(micro_scanner));
      } else {
        ret = OB_SUCCESS;
        state.set_state(0, ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT);
        LOG_DEBUG("[INDEX SKIP SCAN] micro scanner reachs end", K(state), K(first));
      }
    } else {
      micro_start_ = micro_scanner.get_current_pos();
      micro_last_ = micro_scanner.get_last_pos();
      micro_current_ = micro_start_;
      LOG_DEBUG("[INDEX SKIP SCAN] micro scanner start", K(micro_start_), K(micro_last_), K(micro_current_), KPC(this));
    }
  }
  if (OB_SUCC(ret) && !left_border_reached_ && !state.is_skipped()) {
    bool has_data = false;
    bool range_covered = false;
    if (OB_FAIL(micro_scanner.skip_to_range(micro_start_, micro_last_, complete_range_, is_left_border, is_right_border,
                                            micro_current_, has_data, range_covered))) {
      LOG_WARN("failed to skip to next prefix range", KR(ret), K(micro_scanner), KPC(this));
    } else if (has_data) {
      micro_start_ = MAX(micro_start_, micro_current_);
      left_border_reached_ = true;
    } else {
      micro_start_ = MAX(micro_start_, micro_current_);
      state.set_state(0, ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT);
    }
    LOG_DEBUG("[INDEX SKIP SCAN] micro scanner skip to range", KR(ret), K(has_data), K(range_covered), K(state), KPC(this));
  }
  return ret;
}

int ObAdvanceSkipScanner::prepare_ranges(
    const ObDatumRange &scan_range,
    const int64_t schema_rowkey_cnt,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (nullptr == range_datums_) {
    void *buf = nullptr;
    const int64_t total_datum_cnt = schema_rowkey_cnt * 2;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStorageDatum) * total_datum_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", KR(ret));
    } else {
      range_datums_ = new (buf) ObStorageDatum[total_datum_cnt];
    }
  }
  if (OB_SUCC(ret)) {
    OZ(complete_range_.start_key_.assign(range_datums_, schema_rowkey_cnt));
    OZ(complete_range_.end_key_.assign(range_datums_ + schema_rowkey_cnt, schema_rowkey_cnt));
    if (OB_FAIL(ret)) {
      if (nullptr != range_datums_) {
        allocator.free(range_datums_);
        range_datums_ = nullptr;
      }
    } else {
      complete_range_.start_key_.reuse();
      complete_range_.end_key_.reuse();
      for (int64_t i = 0; i < schema_rowkey_cnt; ++i) {
        if (i < scan_range.start_key_.datum_cnt_) {
          complete_range_.start_key_.datums_[i] = scan_range.start_key_.datums_[i];
        }
        if (i < scan_range.end_key_.datum_cnt_) {
          complete_range_.end_key_.datums_[i] = scan_range.end_key_.datums_[i];
        }
      }
      complete_range_.set_border_flag(scan_range.border_flag_);
    }
  }
  LOG_DEBUG("[INDEX SKIP SCAN] prepare range", KR(ret), K_(complete_range), K(scan_range));
  return ret;
}

int ObIndexSkipScanFactory::build_index_skip_scanner(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    const ObDatumRange *range,
    ObAdvanceSkipScanner *&skip_scanner)
{
  int ret = OB_SUCCESS;
  const bool is_reverse_scan = access_ctx.query_flag_.is_reverse_scan();
  const ObITableReadInfo *read_info = iter_param.get_read_info();
  ObIAllocator &stmt_allocator = *access_ctx.stmt_allocator_;

  if (OB_UNLIKELY(!iter_param.is_advance_skip_scan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to build index skip scanner", KR(ret), K(iter_param), K(lbt()));
  } else if (OB_UNLIKELY(nullptr == range || !range->is_valid() || nullptr == read_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to build index skip scanner", KPC(range), KP(read_info));
  } else if (nullptr != skip_scanner) {
    if (OB_FAIL(skip_scanner->switch_info(is_reverse_scan, *range, *read_info, stmt_allocator))) {
      LOG_WARN("failed to switch index skip scanner", KR(ret));
    }
  } else if (OB_ISNULL(skip_scanner = OB_NEWx(ObAdvanceSkipScanner, &stmt_allocator, read_info->get_datum_utils()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc index skip scanner", KR(ret));
  } else if (OB_FAIL(skip_scanner->init(is_reverse_scan, *range, *read_info, stmt_allocator))) {
    LOG_WARN("failed to init index skip scanner", KR(ret));
  }
  if (OB_FAIL(ret) && nullptr != skip_scanner) {
    skip_scanner->~ObAdvanceSkipScanner();
    stmt_allocator.free(skip_scanner);
    skip_scanner = nullptr;
  }
  return ret;
}

void ObIndexSkipScanFactory::destroy_index_skip_scanner(ObAdvanceSkipScanner *&skip_scanner)
{
  if (nullptr != skip_scanner) {
    ObIAllocator *stmt_allocator = skip_scanner->get_stmt_alloc();
    skip_scanner->~ObAdvanceSkipScanner();
    if (nullptr != stmt_allocator) {
      stmt_allocator->free(skip_scanner);
    }
    skip_scanner = nullptr;
  }
}

}
}
