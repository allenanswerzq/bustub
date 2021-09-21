/**
 * b_plus_tree_test.cpp
 */

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <thread>                   // NOLINT
#include <array>
#include "b_plus_tree_test_util.h"  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

class BPlusTreeConcurrentTest : public ::testing::Test {
 protected:
  using Tree = BPlusTree<int, int, IntegerComparator<false>>;
  std::unique_ptr<Schema> key_schema_;
  IntegerComparator<false> comparator_;
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<Tree> tree_;

  void SetUp() override {
    key_schema_ = std::unique_ptr<Schema>(ParseCreateStatement("a bigint"));
    comparator_ = IntegerComparator<false>();
    disk_manager_ = std::make_unique<DiskManager>("test.db");
    bpm_ = std::make_unique<BufferPoolManager>(1000, disk_manager_.get());
    // int leaf_max_size = RandomInt(3, 10);
    // int internal_max_size = RandomInt(3, 10);
    int leaf_max_size = 3;
    int internal_max_size = 3;
    tree_ = std::make_unique<Tree>("test", bpm_.get(), comparator_, leaf_max_size, internal_max_size);

    // create and fetch header_page
    page_id_t page_id;
    bpm_->NewPage(&page_id);
    (void)page_id;
  }

  void TearDown() override {
    remove("test.db");
    remove("test.log");
  };

  template <typename... Args>
  void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
    std::vector<std::thread> thread_group;

    for (uint64_t i = 0; i < num_threads; ++i) {
      thread_group.push_back(std::thread(args..., i));
    }

    for (uint64_t i = 0; i < num_threads; ++i) {
      thread_group[i].join();
    }
  }

  void InsertHelper(Tree *tree, const std::vector<std::array<int, 2>> & inserts) {
    Transaction *transaction = new Transaction(0);
    int n = inserts.size();
    for (int cnt = RandomInt(0, n + 10); cnt > 0; cnt--) {
      int x = RandomInt(0, n - 1);
      auto key = inserts[x];
      tree->Insert(key[0], key[1], transaction);
    }
    delete transaction;
  }

  void DeleteHelper(Tree *tree, const std::vector<std::array<int, 2>> & inserts) {
    Transaction *transaction = new Transaction(0);
    int n = inserts.size();
    for (int cnt = RandomInt(0, n + 10); cnt > 0; cnt--) {
      int x = RandomInt(0, n - 1);
      auto key = inserts[x];
      tree->Remove(key[0], transaction);
    }
    delete transaction;
  }
};

TEST_F(BPlusTreeConcurrentTest, DISABLED_BasicTest) {
  std::unique_ptr<Transaction> tran = std::make_unique<Transaction>(0);
  for (int i = 0; i < 10; i++) {
    tree_->Insert(i, i, tran.get());
  }
  EXPECT_TRUE(tran->GetPageSet()->empty());
  EXPECT_TRUE(tran->GetDeletedPageSet()->empty());
  tree_->Draw(bpm_.get(), "tree.dot");

  tree_->AcquireReadLatch(7, tran.get());
  tree_->ReleaseAllLatch(tran.get(), false);

  tree_->AcquireWriteLatch(7, tran.get());
}

TEST_F(BPlusTreeConcurrentTest, RandomTest) {
  std::vector<std::array<int, 2>> inserts;
  for (int i = 0; i < 100; i++) {
    int key = RandomInt(0, 10000);
    inserts.push_back({key, i});
  }
  std::vector<std::thread> thread_group;
  for (uint64_t i = 0; i < 3; ++i) {
    thread_group.push_back(std::thread([&]{ this->InsertHelper(tree_.get(), inserts); }));
  }
  for (uint64_t i = 0; i < 0; ++i) {
    thread_group.push_back(std::thread([&]{ this->DeleteHelper(tree_.get(), inserts); }));
  }
  for (uint64_t i = 0; i < thread_group.size(); ++i) {
    thread_group[i].join();
  }
  tree_->Draw(bpm_.get(), "tree.dot");
}


}  // namespace bustub
