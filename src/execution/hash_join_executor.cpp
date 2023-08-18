//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_exec_(std::move(left_child)), right_exec_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  if (plan_->GetJoinType() == JoinType::LEFT) {
    right_exec_->Init();
    RID rid{};
    Tuple tuple{};
    while (right_exec_->Next(&tuple, &rid)) {
      auto join_key = MakeRightJoinKey(&tuple);
      jht_.Insert(join_key, tuple);
    }
    left_exec_->Init();
  } else {
    left_exec_->Init();
    RID rid{};
    Tuple tuple{};
    while (left_exec_->Next(&tuple, &rid)) {
      auto join_key = MakeLeftJoinKey(&tuple);
      jht_.Insert(join_key, tuple);
    }
    right_exec_->Init();
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::LEFT) {
    if (!right_end_) {
      std::vector<Value> values{};
      for (int index = 0; index < static_cast<int>(left_exec_->GetOutputSchema().GetColumnCount()); index++) {
        auto value = left_tuple_.GetValue(&left_exec_->GetOutputSchema(), index);
        values.push_back(value);
      }
      auto right_tuple = right_tuples_[index_];
      for (int index = 0; index < static_cast<int>(right_exec_->GetOutputSchema().GetColumnCount()); index++) {
        auto value = right_tuple.GetValue(&right_exec_->GetOutputSchema(), index);
        values.push_back(value);
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      index_ = index_ + 1;
      if (index_ >= static_cast<int>(right_tuples_.size())) {
        index_ = 1;
        right_end_ = true;
      }
      return true;
    }

    RID left_rid{};
    // Tuple left_tuple{};
    while (left_exec_->Next(&left_tuple_, &left_rid)) {
      auto join_key = MakeLeftJoinKey(&left_tuple_);
      // std::vector<Tuple> right_tuples;
      jht_.GetValue(join_key, &right_tuples_);
      if (plan_->GetJoinType() == JoinType::LEFT && right_tuples_.empty()) {
        std::vector<Value> values{};
        for (int index = 0; index < static_cast<int>(left_exec_->GetOutputSchema().GetColumnCount()); index++) {
          auto value = left_tuple_.GetValue(&left_exec_->GetOutputSchema(), index);
          values.push_back(value);
        }
        for (int index = 0; index < static_cast<int>(right_exec_->GetOutputSchema().GetColumnCount()); index++) {
          values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
      if (static_cast<int>(right_tuples_.size()) >= 2) {
        right_end_ = false;
      }
      for (const auto &right_tuple : right_tuples_) {
        for (int i = 0; i < static_cast<int>(plan_->LeftJoinKeyExpressions().size()); i++) {
          auto left_value = plan_->LeftJoinKeyExpressions()[i]->Evaluate(&left_tuple_, left_exec_->GetOutputSchema());
          auto right_value =
              plan_->RightJoinKeyExpressions()[i]->Evaluate(&right_tuple, right_exec_->GetOutputSchema());
          if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
            std::vector<Value> values{};
            for (int index = 0; index < static_cast<int>(left_exec_->GetOutputSchema().GetColumnCount()); index++) {
              auto value = left_tuple_.GetValue(&left_exec_->GetOutputSchema(), index);
              values.push_back(value);
            }
            for (int index = 0; index < static_cast<int>(right_exec_->GetOutputSchema().GetColumnCount()); index++) {
              auto value = right_tuple.GetValue(&right_exec_->GetOutputSchema(), index);
              values.push_back(value);
            }
            *tuple = Tuple(values, &plan_->OutputSchema());
            return true;
          }
        }
      }
    }
  } else {
    if (!left_end_) {
      std::vector<Value> values{};
      auto left_tuple = left_tuples_[index_];
      for (int index = 0; index < static_cast<int>(left_exec_->GetOutputSchema().GetColumnCount()); index++) {
        auto value = left_tuple.GetValue(&left_exec_->GetOutputSchema(), index);
        values.push_back(value);
      }
      for (int index = 0; index < static_cast<int>(right_exec_->GetOutputSchema().GetColumnCount()); index++) {
        auto value = right_tuple_.GetValue(&right_exec_->GetOutputSchema(), index);
        values.push_back(value);
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      index_ = index_ + 1;
      if (index_ >= static_cast<int>(left_tuples_.size())) {
        index_ = 1;
        left_end_ = true;
      }
      return true;
    }
    RID right_rid{};
    while (right_exec_->Next(&right_tuple_, &right_rid)) {
      auto join_key = MakeRightJoinKey(&right_tuple_);
      // std::vector<Tuple> left_tuples;
      jht_.GetValue(join_key, &left_tuples_);
      if (static_cast<int>(left_tuples_.size()) >= 2) {
        left_end_ = false;
      }
      for (const auto &left_tuple : left_tuples_) {
        for (int i = 0; i < static_cast<int>(plan_->LeftJoinKeyExpressions().size()); i++) {
          auto left_value = plan_->LeftJoinKeyExpressions()[i]->Evaluate(&left_tuple, left_exec_->GetOutputSchema());
          auto right_value =
              plan_->RightJoinKeyExpressions()[i]->Evaluate(&right_tuple_, right_exec_->GetOutputSchema());
          if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
            std::vector<Value> values{};
            for (int index = 0; index < static_cast<int>(left_exec_->GetOutputSchema().GetColumnCount()); index++) {
              auto value = left_tuple.GetValue(&left_exec_->GetOutputSchema(), index);
              values.push_back(value);
            }
            for (int index = 0; index < static_cast<int>(right_exec_->GetOutputSchema().GetColumnCount()); index++) {
              auto value = right_tuple_.GetValue(&right_exec_->GetOutputSchema(), index);
              values.push_back(value);
            }
            *tuple = Tuple(values, &plan_->OutputSchema());
            return true;
          }
        }
      }
    }
  }
  return false;
}

}  // namespace bustub
