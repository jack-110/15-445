//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (first_) {
    int rows = 0;
    RID delete_rid{};
    Tuple delete_tup{};
    while (child_executor_->Next(&delete_tup, &delete_rid)) {
      auto table = table_info_->table_.get();
      table->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, delete_rid);
      rows++;

      auto catalog = exec_ctx_->GetCatalog();
      auto index_infos = catalog->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        auto delete_key =
            delete_tup.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(delete_key, delete_rid, exec_ctx_->GetTransaction());
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
