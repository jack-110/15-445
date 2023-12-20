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
#include "common/logger.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  if (exec_ctx_->IsDelete()) {
    try {
      LOG_INFO("SeqScan executor try to acquire IX lock on table %d", plan_->table_oid_);
      auto success = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
      if (!success) {
        throw new ExecutionException("SeqScan executor failed to acquire IX lock on table " +
                                     std::to_string(plan_->table_oid_));
      }
    } catch (TransactionAbortException e) {
      throw new ExecutionException("SeqScan executor failed to acquire IX lock on talbe " +
                                   std::to_string(plan_->table_oid_) + e.GetInfo());
    }
  } else {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        // Aovid upgrade reversely in the same txn, ref https://zhuanlan.zhihu.com/p/630725626
        // For the transaction, it's permitted to read resources that are locked by x/ix locks
        if (!exec_ctx_->GetTransaction()->IsTableExclusiveLocked(plan_->table_oid_) &&
            !exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->table_oid_)) {
          LOG_INFO("SeqScan executor try to acquire IS lock on table %d", plan_->table_oid_);
          // IS ref https://zhuanlan.zhihu.com/p/592700870
          auto success = exec_ctx_->GetLockManager()->LockTable(
              exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->table_oid_);
          if (!success) {
            throw new ExecutionException("SeqScan executor failed to acquire IS lock on table " +
                                         std::to_string(plan_->table_oid_));
          }
        }
      } catch (TransactionAbortException e) {
        throw new ExecutionException("SeqScan executor failed to acquire IS lock on talbe " +
                                     std::to_string(plan_->table_oid_) + e.GetInfo());
      }
    }
  }
  auto catalog = exec_ctx_->GetCatalog();
  auto table = catalog->GetTable(plan_->table_oid_)->table_.get();
  itr_ = std::make_unique<TableIterator>(table->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!itr_->IsEnd()) {
    // row lock
    if (exec_ctx_->IsDelete()) {
      try {
        LOG_INFO("SeqScan executor try to acquire X lock on row %s", itr_->GetRID().ToString().c_str());
        auto success = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, itr_->GetRID());
        if (!success) {
          throw new ExecutionException("SeqScan executor failed to acquire X lock on row " + itr_->GetRID().ToString());
        }
      } catch (TransactionAbortException e) {
        throw new ExecutionException("SeqScan executor failed to acquire X lock on talbe " +
                                     std::to_string(plan_->table_oid_) + e.GetInfo());
      }
    } else {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          if (!exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->table_oid_, itr_->GetRID())) {
            LOG_INFO("SeqScan executor try to acquire S lock on row %s", itr_->GetRID().ToString().c_str());
            auto success = exec_ctx_->GetLockManager()->LockRow(
                exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->table_oid_, itr_->GetRID());
            if (!success) {
              throw new ExecutionException("SeqScan executor failed to acquire S lock on row " +
                                           itr_->GetRID().ToString());
            }
          }
        } catch (TransactionAbortException e) {
          throw new ExecutionException("SeqScan executor failed to acquire S lock on talbe " +
                                       itr_->GetRID().ToString() + e.GetInfo());
        }
      }
    }
    auto meta_and_tuple = itr_->GetTuple();
    auto meta = meta_and_tuple.first;
    if (!meta.is_deleted_) {
      *rid = itr_->GetRID();
      *tuple = meta_and_tuple.second;
      itr_->operator++();
      // unlock table for read operations under read committed level
      if (!exec_ctx_->IsDelete() &&
          exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        try {
          LOG_INFO("SeqScan executor try to unlock S/X lock on row %s", rid->ToString().c_str());
          auto success =
              exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_, *rid, false);
          if (!success) {
            throw new ExecutionException("SeqScan executor failed to unlock S/X lock on row " + rid->ToString());
          }
        } catch (TransactionAbortException e) {
          throw new ExecutionException("SeqScan executor failed to unlock S/X lock on row " + rid->ToString() +
                                       e.GetInfo());
        }
      }
      return true;
    }
    // If the tuple should not be read by this transaction, force unlock the row
    if (exec_ctx_->IsDelete() || exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        LOG_INFO("SeqScan executor try to force unlock S/X lock on row %s", itr_->GetRID().ToString().c_str());
        auto success = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_,
                                                              itr_->GetRID(), true);
        if (!success) {
          throw new ExecutionException("SeqScan executor failed to force unlock S/X lock on row " +
                                       itr_->GetRID().ToString());
        }
      } catch (TransactionAbortException e) {
        throw new ExecutionException("SeqScan executor failed to force unlock S/X lock on row " +
                                     itr_->GetRID().ToString() + e.GetInfo());
      }
    }
    itr_->operator++();
  }
  // unlock table for read operations under read committed level
  if (!exec_ctx_->IsDelete() && exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    try {
      LOG_INFO("SeqScan executor try to unlock IS/IX lock on table %d", plan_->table_oid_);
      auto success = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->table_oid_);
      if (!success) {
        throw new ExecutionException("SeqScan executor failed to unlock IS/IX lock on table " +
                                     std::to_string(plan_->table_oid_));
      }
    } catch (TransactionAbortException e) {
      throw new ExecutionException("SeqScan executor failed to unlock IS/IX lock on talbe " +
                                   std::to_string(plan_->table_oid_) + e.GetInfo());
    }
  }
  return false;
}

}  // namespace bustub
