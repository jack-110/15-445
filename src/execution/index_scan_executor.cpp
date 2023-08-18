//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto index_info = catalog->GetIndex(plan_->index_oid_);
  table_info_ = catalog->GetTable(index_info->table_name_);
  index_tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(index_tree_->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iterator_->IsEnd()) {
    auto pair = iterator_->operator*();
    auto child_rid = pair.second;
    auto table = table_info_->table_.get();
    auto meta_tuple = table->GetTuple(child_rid);
    auto meta = meta_tuple.first;
    if (!meta.is_deleted_) {
      *tuple = meta_tuple.second;
      iterator_->operator++();
      return true;
    }
    iterator_->operator++();
  }
  return false;
}

}  // namespace bustub
