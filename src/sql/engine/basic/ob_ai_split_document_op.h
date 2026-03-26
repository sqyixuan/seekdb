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
 * This file contains interface support for the ai split document operator.
 */

#ifndef OCEANBASE_BASIC_OB_AI_SPLIT_DOCUMENT_OP_H_
#define OCEANBASE_BASIC_OB_AI_SPLIT_DOCUMENT_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace sql
{
class ObAiSplitDocumentSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
ObAiSplitDocumentSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      context_expr_(nullptr),
      option_expr_(nullptr),
      column_exprs_(alloc),
      has_correlated_expr_(false),
      alloc_(&alloc) {}

  ObExpr* get_context_expr() const { return context_expr_; }
  ObExpr* get_option_expr() const { return option_expr_; }
  void set_context_expr(ObExpr* context_expr) { context_expr_ = context_expr; }
  void set_option_expr(ObExpr* option_expr) { option_expr_ = option_expr; }

  ObExpr* context_expr_;
  ObExpr* option_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_;
  bool has_correlated_expr_; // whether it is variable input, used in operator rescan, same as function table
  ObIAllocator* alloc_;
};

class ObAiSplitDocumentOp : public ObOperator
{
public:
  ObAiSplitDocumentOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      allocator_(&exec_ctx.get_allocator()),
      already_calc_(false),
      context_value_(),
      row_idx_(0),
      context_pos_(0)
  {}
public:
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override;
private:
  common::ObIAllocator *allocator_;
  bool already_calc_;
  ObString context_value_;
  int64_t row_idx_;
  int64_t context_pos_;
  std::vector<std::pair<std::string, std::string>> merged_chunks_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_AI_SPLIT_DOCUMENT_OP_H_ */