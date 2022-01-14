//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwmutex.h
//
// Identification: src/include/common/rwlatch.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <climits>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

/**
 * Reader-Writer latch backed by std::mutex.
 */
class ReaderWriterLatch {
  using mutex_t = std::mutex;
  using cond_t = std::condition_variable;
  static const uint32_t MAX_READERS = UINT_MAX;

 public:
  ReaderWriterLatch() = default;
  ~ReaderWriterLatch() { std::lock_guard<mutex_t> guard(mutex_); }

  DISALLOW_COPY(ReaderWriterLatch);

  /**
   * Acquire a write latch.
   */
  void WLock(bool page_latch = false) {
    LOG(DEBUG) << "Waiting for writer " << (page_latch ? "page_latch" : "lock");
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_) {
      // Wait the last write finishes.
      reader_.wait(latch);
    }
    // No writes happening now, but may still have reads running.
    writer_entered_ = true;
    while (reader_count_ > 0) {
      // Writer waits all reads finishes
      writer_.wait(latch);
    }
    is_write_lock = true;
  }

  /**
   * Release a write latch.
   */
  void WUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    writer_entered_ = false;
    // Notify all to let the waiting writers and readers to compete
    reader_.notify_all();
    is_write_lock = false;
  }

  bool IsWriteLock() {
    std::lock_guard<mutex_t> guard(mutex_);
    return is_write_lock;
  }

  /**
   * Acquire a read latch.
   */
  void RLock() {
    LOG(DEBUG) << "Waiting for read lock...";
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_ || reader_count_ == MAX_READERS) {
      reader_.wait(latch);
    }
    reader_count_++;
  }

  /**
   * Release a read latch.
   */
  void RUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    reader_count_--;
    if (writer_entered_) {
      if (reader_count_ == 0) {
        writer_.notify_one();
      }
    } else {
      if (reader_count_ == MAX_READERS - 1) {
        reader_.notify_one();
      }
    }
  }

 private:
  mutex_t mutex_;
  cond_t writer_;
  cond_t reader_;
  uint32_t reader_count_{0};
  bool writer_entered_{false};
  bool is_write_lock{false};
};

}  // namespace bustub
