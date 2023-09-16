//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (first_) {
    int rows = 0;
    RID next_rid{};
    Tuple next_tup{};
    while (child_executor_->Next(&next_tup, &next_rid)) {
      auto table = table_info_->table_.get();
      next_rid = table
                     ->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, next_tup,
                                   exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->table_oid_)
                     .value();
      rows++;

      auto catalog = exec_ctx_->GetCatalog();
      auto index_infos = catalog->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        auto key =
            next_tup.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key, next_rid, exec_ctx_->GetTransaction());
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
