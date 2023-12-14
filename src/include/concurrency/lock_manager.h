//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() = default;

  void StartDeadlockDetection() {
    BUSTUB_ENSURE(txn_manager_ != nullptr, "txn_manager_ is not set.")
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    UnlockAll();

    enable_cycle_detection_ = false;

    if (cycle_detection_thread_ != nullptr) {
      cycle_detection_thread_->join();
      delete cycle_detection_thread_;
    }
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *    - Basically there should be three steps to perform a lock upgrade in general
   *      - 1. Check the precondition of upgrade
   *      - 2. Drop the current lock, reserve the upgrade position
   *      - 3. Wait to get the new lock granted
   *
   *    A lock request being upgraded should be prioritized over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   *
   *    You probably want to consider which type of lock to directly apply on table
   *    when implementing executor later
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @param force unlock the tuple regardless of isolation level, not changing the transaction state
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force = false) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  TransactionManager *txn_manager_;

 private:
  /** Spring 2023 */
  /* You are allowed to modify all functions below. */
  auto UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
    auto lock_request_queue = GetTableLockRequestQueue(oid);

    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    for (auto &lock_request : lock_request_queue->request_queue_) {
      if (txn->GetTransactionId() == lock_request->txn_id_) {
        // check the precondition of upgrade
        if (lock_mode == lock_request->lock_mode_) {
          LOG_INFO("The upgrading %d lock on table %u for txn %u is the same", lock_mode, oid, txn->GetTransactionId());
          return true;
        }

        if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
          LOG_INFO("The upgrading %d lock on table %u for txn %u failed for multiple upgrading txns", lock_mode, oid,
                   txn->GetTransactionId());
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }

        if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
          LOG_INFO("The upgrading %d lock on table %u for txn %u failed for incompitalbe upgrade", lock_mode, oid,
                   txn->GetTransactionId());
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        }

        LOG_INFO("Upgrading %d lock on table %u for txn %u", lock_mode, oid, txn->GetTransactionId());
        
        // drop the current lock, reserve the upgrade position
        InsertOrDeleteTableLockSet(txn, lock_request, false);
        lock_request_queue->request_queue_.remove(lock_request);

        // wait to get the new lock granted
        auto upgrade_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
        lock_request_queue->request_queue_.push_front(upgrade_request);

        lock_request_queue->upgrading_ = txn->GetTransactionId();
        while (!GrantLock(upgrade_request, lock_request_queue)) {
          LOG_INFO("Blocking %d lock on table %u for txn %u", lock_mode, oid, txn->GetTransactionId());
          lock_request_queue->cv_.wait(lock);
          if (txn->GetState() == TransactionState::ABORTED) {
            LOG_INFO("Abort %d lock on table %u for txn %u", lock_mode, oid, txn->GetTransactionId());
            lock_request_queue->upgrading_ = INVALID_TXN_ID;
            lock_request_queue->request_queue_.remove(upgrade_request);
            lock_request_queue->cv_.notify_all();
            return false;
          }
        }

        //book keeping
        upgrade_request->granted_ = true;
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        InsertOrDeleteTableLockSet(txn, upgrade_request, true);

        LOG_INFO("Success to upgrade %d lock on table %u for txn %u", lock_mode, oid, txn->GetTransactionId());

        if (lock_mode != LockMode::EXCLUSIVE) {
          lock_request_queue->cv_.notify_all();
        }

        return true;
      }
    }

    return false;
  }
  auto UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  auto AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
    switch (l1) {
      case LockMode::SHARED:
        return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
      case LockMode::EXCLUSIVE:
        return false;
      case LockMode::INTENTION_SHARED:
        return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE || l2 == LockMode::SHARED ||
               l2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::INTENTION_EXCLUSIVE:
        return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return l2 == LockMode::INTENTION_SHARED;
    }
    return false;
  }

  auto CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
    if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
      return false;
    }

    // check transaction isolation level
    bool is_abort = false;
    AbortReason reason = AbortReason::LOCK_ON_SHRINKING;
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        is_abort = true;
        reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
      } else if (txn->GetState() == TransactionState::SHRINKING) {
        is_abort = true;
        reason = AbortReason::LOCK_ON_SHRINKING;
      }
    } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
          lock_mode != LockMode::INTENTION_SHARED) {
        is_abort = true;
        reason = AbortReason::LOCK_ON_SHRINKING;
      }
    } else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (txn->GetState() == TransactionState::SHRINKING) {
        is_abort = true;
        reason = AbortReason::LOCK_ON_SHRINKING;
      }
    }
    if (is_abort) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), reason);
    }
    return true;
  }

  void GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
    for (const auto &wait_grant : lock_request_queue->request_queue_) {
      if (!wait_grant->granted_) {
        for (const auto &request : lock_request_queue->request_queue_) {
          if (request->granted_ && !AreLocksCompatible(request->lock_mode_, wait_grant->lock_mode_)) {
            return;
          }
        }

        wait_grant->granted_ = true;
      }
    }
  }

  auto CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
    switch (curr_lock_mode) {
      case LockMode::SHARED:
      case LockMode::INTENTION_EXCLUSIVE:
        return requested_lock_mode == LockMode::EXCLUSIVE ||
               requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::INTENTION_SHARED:
        return requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
               requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
               requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return requested_lock_mode == LockMode::EXCLUSIVE;
      default:
        return false;
    }
  }

  auto CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool;

  auto GetTableLockRequestQueue(const table_oid_t &oid) -> std::shared_ptr<LockRequestQueue> {
    std::lock_guard<std::mutex> lock(table_lock_map_latch_);
    auto it = table_lock_map_.find(oid);
    if (it != table_lock_map_.end()) {
      return it->second;
    }

    auto queue = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = queue;
    return queue;
  }

  auto GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                 const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
    LOG_INFO("Try to grant new locks for txn %u on table %u", lock_request->txn_id_, lock_request->oid_);
    for (auto &queue_request : lock_request_queue->request_queue_) {
      if (queue_request->granted_) {
        if (!AreLocksCompatible(queue_request->lock_mode_, lock_request->lock_mode_)) {
          LOG_INFO("Failed to grant new locks for txn %u on table %u for incompatible locks", lock_request->txn_id_,
                   lock_request->oid_);
          return false;
        }
      }
    }

    for (auto &queue_request : lock_request_queue->request_queue_) {
      // if it's the first ungranted lock request
      if (!queue_request->granted_) {
        return queue_request->txn_id_ == lock_request->txn_id_;
      }
    }
    LOG_INFO("Success to grant new locks for txn %u on table %u", lock_request->txn_id_, lock_request->oid_);
    return true;
  }

  void InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request, bool insert) {
    switch (lock_request->lock_mode_) {
      case LockMode::SHARED:
        if (insert) {
          txn->GetSharedTableLockSet()->insert(lock_request->oid_);
        } else {
          txn->GetSharedTableLockSet()->erase(lock_request->oid_);
        }
        break;
      case LockMode::EXCLUSIVE:
        if (insert) {
          txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
        } else {
          txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
        }
        break;
      case LockMode::INTENTION_SHARED:
        if (insert) {
          txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
        } else {
          txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (insert) {
          txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
        } else {
          txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (insert) {
          txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
        } else {
          txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
        }
        break;
    }
  }

  auto FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                 std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool;
  void UnlockAll();

  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;
};

}  // namespace bustub

template <>
struct fmt::formatter<bustub::LockManager::LockMode> : formatter<std::string_view> {
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(bustub::LockManager::LockMode x, FormatContext &ctx) const {
    string_view name = "unknown";
    switch (x) {
      case bustub::LockManager::LockMode::EXCLUSIVE:
        name = "EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::INTENTION_EXCLUSIVE:
        name = "INTENTION_EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::SHARED:
        name = "SHARED";
        break;
      case bustub::LockManager::LockMode::INTENTION_SHARED:
        name = "INTENTION_SHARED";
        break;
      case bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
        name = "SHARED_INTENTION_EXCLUSIVE";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
