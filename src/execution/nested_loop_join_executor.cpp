//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_end_ = false;
  left_matched_ = false;
  left_executor_->Init();
  right_executor_->Init();
  RID rid{};
  left_executor_->Next(&left_tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!left_end_) {
    RID right_rid{};
    Tuple right_tuple{};
    if (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        left_matched_ = true;
        std::vector<Value> values{};
        for (int index = 0; index < static_cast<int>(left_executor_->GetOutputSchema().GetColumnCount()); index++) {
          auto value = left_tuple_.GetValue(&left_executor_->GetOutputSchema(), index);
          values.push_back(value);
        }
        for (int index = 0; index < static_cast<int>(right_executor_->GetOutputSchema().GetColumnCount()); index++) {
          auto value = right_tuple.GetValue(&right_executor_->GetOutputSchema(), index);
          values.push_back(value);
        }
        // auto join_schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
    } else {
      RID rid{};
      Tuple left_tuple;
      if (!left_executor_->Next(&left_tuple, &rid)) {
        left_end_ = true;
      }
      if (plan_->GetJoinType() == JoinType::LEFT && !left_matched_) {
        std::vector<Value> values{};
        for (int index = 0; index < static_cast<int>(left_executor_->GetOutputSchema().GetColumnCount()); index++) {
          auto value = left_tuple_.GetValue(&left_executor_->GetOutputSchema(), index);
          values.push_back(value);
        }
        for (int index = 0; index < static_cast<int>(right_executor_->GetOutputSchema().GetColumnCount()); index++) {
          values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        }
        // auto join_schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());
        *tuple = Tuple(values, &plan_->OutputSchema());
        left_matched_ = false;
        right_executor_->Init();
        left_tuple_ = left_tuple;
        return true;
      }
      left_matched_ = false;
      right_executor_->Init();
      left_tuple_ = left_tuple;
    }
  }
  return false;
}

}  // namespace bustub
