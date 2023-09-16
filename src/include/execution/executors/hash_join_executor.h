//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class SimpleJoinHashTable {
 public:
  void GetValue(const AggregateKey &join_key, std::vector<Tuple> *tuples) { *tuples = ht_[join_key]; }

  void Insert(const AggregateKey &join_key, const Tuple &tuple) {
    if (ht_.count(join_key) == 0) {
      ht_[join_key] = std::vector<Tuple>();
    }
    ht_[join_key].emplace_back(tuple);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  std::unordered_map<AggregateKey, std::vector<Tuple>> ht_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an JoinKey */
  auto MakeLeftJoinKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_exec_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an JoinKey */
  auto MakeRightJoinKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_exec_->GetOutputSchema()));
    }
    return {keys};
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  int index_ = 1;
  bool left_end_ = true;
  bool right_end_ = true;
  Tuple left_tuple_{};
  Tuple right_tuple_{};
  std::vector<Tuple> left_tuples_;
  std::vector<Tuple> right_tuples_;
  SimpleJoinHashTable jht_;
  std::unique_ptr<AbstractExecutor> left_exec_;
  std::unique_ptr<AbstractExecutor> right_exec_;
};

}  // namespace bustub
