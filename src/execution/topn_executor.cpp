#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  sorted_tuples_.clear();
  child_executor_->Init();

  auto orderby_keys = plan_->GetOrderBy();
  auto comparator = [&](const Tuple &lhs, const Tuple &rhs) -> bool {
    for (const auto &[order_type, expr] : orderby_keys) {
      auto left_value = expr->Evaluate(&lhs, GetOutputSchema());
      auto right_value = expr->Evaluate(&rhs, GetOutputSchema());
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        // equal on this field, go check next one
        continue;
      }
      auto comp = left_value.CompareLessThan(right_value);
      return (comp == CmpBool::CmpTrue && order_type != OrderByType::DESC) ||
             (comp == CmpBool::CmpFalse && order_type == OrderByType::DESC);
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comparator)> pq(comparator);

  RID rid{};
  Tuple tuple{};
  while (child_executor_->Next(&tuple, &rid)) {
    if (pq.size() < plan_->GetN()) {
      pq.push(tuple);
    } else {
      if (comparator(tuple, pq.top())) {
        pq.pop();
        pq.push(tuple);
      }
    }
  }

  sorted_tuples_.reserve(pq.size());
  while (!pq.empty()) {
    sorted_tuples_.push_back(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_tuples_.empty()) {
    return false;
  }
  *tuple = sorted_tuples_.back();
  sorted_tuples_.pop_back();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return sorted_tuples_.size(); };

}  // namespace bustub
