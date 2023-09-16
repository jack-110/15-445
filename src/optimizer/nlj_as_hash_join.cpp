#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            // Ensure both exprs have tuple_id == 0
            std::vector<AbstractExpressionRef> left_key_expressions;
            auto left_expr_tuple = std::make_shared<ColumnValueExpression>(
                left_expr->GetTupleIdx(), left_expr->GetColIdx(), left_expr->GetReturnType());

            std::vector<AbstractExpressionRef> right_key_expressions;
            auto right_expr_tuple = std::make_shared<ColumnValueExpression>(
                right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());

            if (left_expr->GetTupleIdx() == 0) {
              left_key_expressions.emplace_back(left_expr_tuple);
            } else {
              right_key_expressions.emplace_back(left_expr_tuple);
            }

            if (right_expr->GetTupleIdx() == 0) {
              left_key_expressions.emplace_back(right_expr_tuple);
            } else {
              right_key_expressions.emplace_back(right_expr_tuple);
            }
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                      std::move(right_key_expressions), nlj_plan.GetJoinType());
          }
        }
      }
    } else if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        if (const auto *left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            if (const auto *left_expr_left = dynamic_cast<const ColumnValueExpression *>(left_expr->children_[0].get());
                left_expr_left != nullptr) {
              if (const auto *left_expr_right =
                      dynamic_cast<const ColumnValueExpression *>(left_expr->children_[1].get());
                  left_expr_right != nullptr) {
                if (const auto *right_expr_left =
                        dynamic_cast<const ColumnValueExpression *>(right_expr->children_[0].get());
                    right_expr_left != nullptr) {
                  if (const auto *right_expr_right =
                          dynamic_cast<const ColumnValueExpression *>(right_expr->children_[1].get());
                      right_expr_right != nullptr) {
                    std::vector<AbstractExpressionRef> left_key_expressions;
                    auto left_expr_tuple1 = std::make_shared<ColumnValueExpression>(
                        left_expr_left->GetTupleIdx(), left_expr_left->GetColIdx(), left_expr_left->GetReturnType());
                    auto left_expr_tuple2 = std::make_shared<ColumnValueExpression>(
                        right_expr_left->GetTupleIdx(), right_expr_left->GetColIdx(), right_expr_left->GetReturnType());

                    std::vector<AbstractExpressionRef> right_key_expressions;
                    auto right_expr_tuple1 = std::make_shared<ColumnValueExpression>(
                        left_expr_right->GetTupleIdx(), left_expr_right->GetColIdx(), left_expr_right->GetReturnType());
                    auto right_expr_tuple2 = std::make_shared<ColumnValueExpression>(right_expr_right->GetTupleIdx(),
                                                                                     right_expr_right->GetColIdx(),
                                                                                     right_expr_right->GetReturnType());
                    if (left_expr_left->GetTupleIdx() == 0) {
                      left_key_expressions.emplace_back(left_expr_tuple1);
                    } else {
                      right_key_expressions.emplace_back(left_expr_tuple1);
                    }
                    if (left_expr_right->GetTupleIdx() == 0) {
                      left_key_expressions.emplace_back(right_expr_tuple1);
                    } else {
                      right_key_expressions.emplace_back(right_expr_tuple1);
                    }

                    if (right_expr_left->GetTupleIdx() == 0) {
                      left_key_expressions.emplace_back(left_expr_tuple2);
                    } else {
                      right_key_expressions.emplace_back(left_expr_tuple2);
                    }

                    if (right_expr_right->GetTupleIdx() == 0) {
                      left_key_expressions.emplace_back(right_expr_tuple2);
                    } else {
                      right_key_expressions.emplace_back(right_expr_tuple2);
                    }
                    // Let's check if one of them is from the left table, and the other is from the right table.
                    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                              nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                              std::move(right_key_expressions), nlj_plan.GetJoinType());
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}
}  // namespace bustub
