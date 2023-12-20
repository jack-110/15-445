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
  try {
    LOG_INFO("Insert executor try to acquire IX lock on table %d", plan_->table_oid_);
    auto success = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
    if (!success) {
      throw new ExecutionException("Insert executor failed to acquire IX lock on table " +
                                   std::to_string(plan_->table_oid_));
    }
  } catch (TransactionAbortException e) {
    throw new ExecutionException("Insert executor failed to acquire IX lock on talbe " +
                                 std::to_string(plan_->table_oid_) + e.GetInfo());
  }
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

      LOG_INFO("Insert executor try to acquire X lock on row %s", next_rid.ToString().c_str());
      // maintain table write set
      auto table_write = TableWriteRecord(table_info_->oid_, next_rid, table_info_->table_.get());
      table_write.wtype_ = WType::INSERT;
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(table_write);

      auto catalog = exec_ctx_->GetCatalog();
      auto index_infos = catalog->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        auto key =
            next_tup.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key, next_rid, exec_ctx_->GetTransaction());
        // maintain index write set
        auto index_write = IndexWriteRecord(next_rid, table_info_->oid_, WType::INSERT, key, index_info->index_oid_,
                                            exec_ctx_->GetCatalog());
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_write);
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
