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
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(lock_request);
      InsertOrDeleteTableLockSet(txn, lock_request, false);

      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }

      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;

      InsertOrDeleteTableLockSet(txn, upgrade_lock_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }

      return true;
    }
  }

  LOG_INFO("Acquire new %d lock on table %u for txn %u", lock_mode, oid, txn->GetTransactionId());

  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  LOG_INFO("Try to unlock table %u for txn %u", oid, txn->GetTransactionId());

  auto lock_request_queue = GetTableLockRequestQueue(oid);

  std::lock_guard<std::mutex> lock(lock_request_queue->latch_);

  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      // unlock no lock

      if (!request->granted_) {
        LOG_INFO("unlock %d lock on table %u for txn %u failed for no granted lock", request->lock_mode_, oid,

                 txn->GetTransactionId());

        txn->SetState(TransactionState::ABORTED);

        throw bustub::TransactionAbortException(txn->GetTransactionId(),

                                                AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      }

      // any row locks?

      table_lock_map_latch_.lock();

      if (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() ||

          txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end()) {
        LOG_INFO("unlock %d lock on table %u for txn %u failed for row locks", request->lock_mode_, oid,

                 txn->GetTransactionId());

        table_lock_map_latch_.unlock();

        txn->SetState(TransactionState::ABORTED);

        throw bustub::TransactionAbortException(txn->GetTransactionId(),

                                                AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
      }

      table_lock_map_latch_.unlock();

      LOG_INFO("unlock %d lock on table %u for txn %u", request->lock_mode_, oid, txn->GetTransactionId());

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
