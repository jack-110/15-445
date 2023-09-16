//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->table_oid_)->table_.get();
  itr_ = std::make_unique<TableIterator>(table->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!itr_->IsEnd()) {
    auto meta_and_tuple = itr_->GetTuple();
    auto meta = meta_and_tuple.first;
    if (!meta.is_deleted_) {
      *rid = itr_->GetRID();
      *tuple = meta_and_tuple.second;
      itr_->operator++();
      return true;
    }
    itr_->operator++();
  }
  return false;
}

}  // namespace bustub
