//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  if (UpgradeLockTable(txn, lock_mode, oid)) {
    return true;
  }

  auto lock_request_queue = GetTableLockRequestQueue(oid);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(request);

  while (!GrantLock(request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      lock_request_queue->request_queue_.remove(request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, request, true);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  auto lock_request_queue = GetTableLockRequestQueue(oid);
  std::lock_guard<std::mutex> lock(lock_request_queue->latch_);
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      // unlock no lock
      if (!request->granted_) {
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(),
                                                AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      }
      // any row locks?
      table_lock_map_latch_.lock();
      if (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() ||
          txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end()) {
        table_lock_map_latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(),
                                                AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
      }
      table_lock_map_latch_.unlock();

      // update transaction state
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
                 txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      // book keeping
      InsertOrDeleteTableLockSet(txn, request, false);
      lock_request_queue->request_queue_.remove(request);

      // notify
      lock_request_queue->cv_.notify_all();
      return true;
    }
  }

  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
