//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  RID rid{};
  Tuple tuple{};
  while (child_executor_->Next(&tuple, &rid)) {
    auto agg_key = MakeAggregateKey(&tuple);
    auto agg_value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(agg_key, agg_value);
    empty_ = false;
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (aht_iterator_ != aht_.End()) {
    std::vector<Value> values;
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    *tuple = Tuple(values, &plan_->OutputSchema());
    ++aht_iterator_;
    return true;
  }
  if (empty_) {
    empty_ = false;
    auto agg_schema = AggregationPlanNode::InferAggSchema({}, plan_->GetAggregates(), plan_->GetAggregateTypes());
    if (plan_->OutputSchema().GetColumns().size() != agg_schema.GetColumns().size()) {
      // an empty table and has no-agg function in the select clause.
      return false;
    }
    *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema());
    return true;
  }
  aht_.Clear();
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
