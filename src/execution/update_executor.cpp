//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (first_) {
    int rows = 0;
    RID delete_rid{};
    Tuple delete_tup{};
    while (child_executor_->Next(&delete_tup, &delete_rid)) {
      auto table = table_info_->table_.get();
      table->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, delete_rid);

      std::vector<Value> values{};
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&delete_tup, child_executor_->GetOutputSchema()));
      }
      Tuple new_tuple{values, &table_info_->schema_};
      auto new_rid = table
                         ->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple,
                                       exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->table_oid_)
                         .value();
      rows++;

      auto catalog = exec_ctx_->GetCatalog();
      auto index_infos = catalog->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        auto delete_key =
            delete_tup.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(delete_key, delete_rid, exec_ctx_->GetTransaction());
        auto new_key =
            new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
      }
    }

    Column col{"num", TypeId::INTEGER};
    std::vector<Column> cols{col};
    Schema schema{cols};
    std::vector<Value> values{Value{TypeId::INTEGER, rows}};
    Tuple tup{values, &schema};
    *tuple = tup;
    first_ = false;
    return true;
  }
  return false;
}

}  // namespace bustub
