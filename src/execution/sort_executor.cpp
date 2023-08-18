#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  tuples_.clear();
  child_executor_->Init();
  RID rid{};
  Tuple tuple{};
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }

  auto orderby_keys = plan_->GetOrderBy();
  auto sorter = [&](Tuple &lhs, Tuple &rhs) -> bool {
    for (const auto &[order_type, expr] : orderby_keys) {
      auto left_value = expr->Evaluate(&lhs, GetOutputSchema());
      auto right_value = expr->Evaluate(&rhs, GetOutputSchema());
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        continue;
      }
      auto comp = left_value.CompareLessThan(right_value);
      return (comp == CmpBool::CmpTrue && order_type == OrderByType::DESC) ||
             (comp == CmpBool::CmpFalse && order_type != OrderByType::DESC);
    }
    return false;
  };
  std::sort(tuples_.begin(), tuples_.end(), sorter);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  const auto &tuple_next = tuples_.back();
  *tuple = tuple_next;
  tuples_.pop_back();
  return true;
}

}  // namespace bustub
