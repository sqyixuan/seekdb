/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 * This file contains implementation support for the json table abstraction.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_ai_split_document_op.h"
#include "share/ai_service/ob_ai_spilit_document.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

OB_SERIALIZE_MEMBER((ObAiSplitDocumentSpec, ObOpSpec), context_expr_, option_expr_, has_correlated_expr_);

int ObAiSplitDocumentOp::inner_open()
{
  INIT_SUCC(ret);
  if (OB_FAIL(init())) {
    LOG_WARN("failed to init.", K(ret));
  } else if (OB_ISNULL(MY_SPEC.context_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to open iter, context expr is null.", K(ret));
  }

  return ret;
}

int ObAiSplitDocumentOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to inner rescan", K(ret));
  } else {
  //   node_idx_ = 0;
  //   // todo@dazhi
    if (MY_SPEC.has_correlated_expr_) {
      LOG_WARN("[syldebug] has correlated expr", K(ret));
    }
  }
  return ret;
}

int ObAiSplitDocumentOp::switch_iterator()
{
  // todo@dazhi
  INIT_SUCC(ret);
  return OB_ITER_END;
}

int ObAiSplitDocumentOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();

  if (OB_ISNULL(MY_SPEC.context_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("context expr is not init", K(ret));
  } else if (ObVarcharType != MY_SPEC.context_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("context expr is not varchar", K(ret), K(MY_SPEC.context_expr_->datum_meta_.type_));
  } else if (!already_calc_) {
    ObDatum *context_datum = nullptr;
    if (OB_FAIL(MY_SPEC.context_expr_->eval(eval_ctx_, context_datum))) {
      LOG_WARN("failed to eval context expr", K(ret));
    } else if (context_datum->is_null()) {
      ret = OB_ITER_END;
    } else {
      context_value_ = context_datum->get_string();
      std::string context_value_str(context_value_.ptr(), context_value_.length());
      std::pair<std::vector<std::string>, std::vector<std::string>> chunks =
        to_chunks_with_order_guarantee(context_value_str, 1);

      std::vector<share::Section> sections;
      for (size_t i = 0; i < chunks.first.size(); ++i) {
        sections.emplace_back(chunks.first[i], chunks.second[i]);
      }
      merged_chunks_ = merge_with_title_chunks(sections, 512);
      already_calc_ = true;
      if (merged_chunks_.empty()) {
        ret = OB_ITER_END;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (row_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row index out of range", K(ret), K(row_idx_), K(merged_chunks_.size()));
    } else if (row_idx_ >= merged_chunks_.size()) {
      ret = OB_ITER_END;
    } else if (MY_SPEC.column_exprs_.count() != 4) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column exprs count is not 4", K(ret), K(MY_SPEC.column_exprs_.count()));
    } else { 
      MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_int(row_idx_);
      MY_SPEC.column_exprs_.at(1)->locate_datum_for_write(eval_ctx_).set_int(context_pos_);

      int64_t chunk_length = merged_chunks_[row_idx_].first.length();
      MY_SPEC.column_exprs_.at(2)->locate_datum_for_write(eval_ctx_).set_int(chunk_length);
      ObExprStrResAlloc res_alloc(*MY_SPEC.column_exprs_.at(3), eval_ctx_);
      ObDatum &dst_datum = MY_SPEC.column_exprs_.at(3)->locate_datum_for_write(eval_ctx_);
      ObDatum tmp_datum;
      tmp_datum.set_string(merged_chunks_[row_idx_].first.c_str(), chunk_length);
      // todo@dazhi: maybe need deal with lob here?
      if (OB_FAIL(dst_datum.deep_copy(tmp_datum, res_alloc))) {
        LOG_WARN("failed to deep copy merged chunks", K(ret));
      } else {
        context_pos_ += chunk_length;
        row_idx_++;
      }
    }
  }
  return ret;
}

void ObAiSplitDocumentOp::destroy()
{
  ObOperator::destroy();
}

int ObAiSplitDocumentOp::inner_close()
{
  return ObOperator::inner_close();
}

} // end namespace sql
} // end namespace oceanbase