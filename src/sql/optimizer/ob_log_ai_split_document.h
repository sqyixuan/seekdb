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
 * This file contains implementation support for the log json table abstraction.
 */

#ifndef _OB_LOG_AI_SPLIT_DOCUMENT_H
#define _OB_LOG_AI_SPLIT_DOCUMENT_H
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
class ObLogAiSplitDocument : public ObLogicalOperator
{
public:
  ObLogAiSplitDocument(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        table_id_(OB_INVALID_ID),
        context_expr_(NULL),
        option_expr_(NULL),
        table_name_(), 
        access_exprs_() {}
  virtual ~ObLogAiSplitDocument() {}
  OB_INLINE const common::ObString &get_table_name() const { return table_name_; }
  OB_INLINE void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  uint64_t get_table_id() const { return table_id_; }
  ObRawExpr* get_context_expr() const { return context_expr_; }
  ObRawExpr* get_option_expr() const { return option_expr_; }
  void set_context_expr(ObRawExpr* context_expr) { context_expr_ = context_expr; }  
  void set_option_expr(ObRawExpr* option_expr) { option_expr_ = option_expr; }

public:
  int generate_access_exprs();
  virtual int get_plan_item_info(PlanText &plan_text, 
                                ObSqlPlanItem &plan_item) override;
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  uint64_t hash(uint64_t seed) const override;

private:
  uint64_t table_id_;
  ObRawExpr* context_expr_;
  ObRawExpr* option_expr_;
  common::ObString table_name_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObLogAiSplitDocument);
};

} // end namespace sql
} // end namespace oceanbase
#endif
 