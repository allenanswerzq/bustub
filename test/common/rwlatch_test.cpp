//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwlatch_test.cpp
//
// Identification: test/common/rwlatch_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <atomic>
#include <random>
#include <thread>  // NOLINT
#include <vector>

#include "common/rwlatch.h"
#include "gtest/gtest.h"

namespace bustub {

class Counter {
 public:
  Counter() = default;
  void Add(int num) {
    mutex.WLock();
    count_ += num;
    mutex.WUnlock();
  }
  int Read() {
    int res;
    mutex.RLock();
    res = count_;
    mutex.RUnlock();
    return res;
  }

 private:
  int count_{0};
  ReaderWriterLatch mutex{};
};

// NOLINTNEXTLINE
TEST(RWLatchTest, BasicTest) {
  int num_threads = 100;
  Counter counter{};
  counter.Add(5);
  std::vector<std::thread> threads;
  for (int tid = 0; tid < num_threads; tid++) {
    if (tid % 2 == 0) {
      threads.emplace_back([&counter]() { counter.Read(); });
    } else {
      threads.emplace_back([&counter]() { counter.Add(1); });
    }
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  EXPECT_EQ(counter.Read(), 55);
}

TEST(RWLatchTest, ExtraTest) {
  std::atomic<int> reads{0};
  std::atomic<int> writes{0};
  ReaderWriterLatch rw_lock;

  std::random_device rd;
  std::mt19937 gen(rd());

  auto func = [&]() {
    int rw = std::uniform_int_distribution<>(1, 10000000)(gen) % 2;
    auto sleep = std::uniform_int_distribution<>(1, 100)(gen);

    if (rw) {
      rw_lock.WLock();
      writes++;

      // Verify only one thread touches write
      EXPECT_EQ(writes, 1);
      EXPECT_EQ(reads, 0);

      std::this_thread::sleep_for(std::chrono::duration<int, std::micro>(sleep));

      writes--;
      rw_lock.WUnlock();
    } else {
      rw_lock.RLock();
      reads++;

      EXPECT_EQ(writes, 0);
      ASSERT_GE(reads, 1);

      std::this_thread::sleep_for(std::chrono::duration<int, std::micro>(sleep));

      reads--;
      rw_lock.RUnlock();
    }
  };

  int num_threads = 100;
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(func);
  }

  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
}
}  // namespace bustub
