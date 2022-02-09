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

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  txn->SetState(TransactionState::GROWING);

  auto & queue = lock_table_[rid];
  queue.cv_.wait(lock, [&]{
    return queue.request_queue_.empty() ||
      queue.request_queue_.back().lock_mode_ == LockMode::SHARED;
  });

  txn->GetSharedLockSet()->emplace(rid);
  queue.request_queue_.push_back(LockRequest{txn->GetTransactionId(), LockMode::SHARED});
  queue.request_queue_.back().granted_ = true;
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  txn->SetState(TransactionState::GROWING);

  auto & queue = lock_table_[rid];
  queue.cv_.wait(lock, [&]{ return queue.request_queue_.empty(); });

  txn->GetExclusiveLockSet()->emplace(rid);
  queue.request_queue_.push_back(LockRequest{txn->GetTransactionId(), LockMode::EXCLUSIVE});
  queue.request_queue_.back().granted_ = true;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  auto & queue = lock_table_[rid];
  bool exist = false;
  for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); it++) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      if (it->lock_mode_ == LockMode::SHARED) {
        exist = true;
        queue.request_queue_.erase(it);
        break;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
    }
  }
  assert(exist);

  txn->GetSharedLockSet()->erase(rid);
  lock.unlock();

  return LockExclusive(txn, rid);
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  auto & queue = lock_table_[rid];
  for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); it++) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      queue.request_queue_.erase(it);
      break;
    }
  }

  txn->SetState(TransactionState::SHRINKING);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  // NOTE: the lock does not need to be held for notification
  lock.unlock();
  queue.cv_.notify_one();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and
      // abort code here
      continue;
    }
  }
}

}  // namespace bustub
