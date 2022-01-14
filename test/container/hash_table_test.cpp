//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "common/logger.h"
#include "container/hash/linear_probe_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTableTest, SimpleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  EXPECT_EQ(page_id, 0);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  EXPECT_EQ(ht.GetSize(), 5);

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  EXPECT_EQ(page_id, 0);
  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    // ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  EXPECT_EQ(ht.GetSize(), 0);

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ResizeTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  EXPECT_EQ(page_id, 0);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 1000; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  for (int i = 0; i < 1000; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  EXPECT_EQ(ht.GetSize(), 1000);

  disk_manager->ShutDown();
  bpm->UnpinPage(page_id, true);
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, RemoveTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  EXPECT_EQ(page_id, 0);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 1000; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  for (int i = 0; i < 1000; i++) {
    if (i % 2 == 0) {
      EXPECT_EQ(ht.Remove(nullptr, i, i), true);
    }
  }

  EXPECT_EQ(ht.GetSize(), 500);
  for (int i = 0; i < 1000; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i % 2 == 0) {
      EXPECT_EQ(0, res.size()) << "Failed to remove " << i << std::endl;
    } else {
      EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
      EXPECT_EQ(i, res[0]);
    }
  }

  disk_manager->ShutDown();
  bpm->UnpinPage(page_id, true);
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

class HashTableConcurrentTest : public ::testing::Test {
 protected:
  using HashTable = LinearProbeHashTable<int, int, IntComparator>;
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<HashTable> ht_;
  page_id_t page_id_;

  void SetUp() override {
    disk_manager_ = std::make_unique<DiskManager>("test.db");
    bpm_ = std::make_unique<BufferPoolManager>(1024, disk_manager_.get());
    // create and fetch header_page
    bpm_->NewPage(&page_id_);
    (void)page_id_;

    ht_ = std::make_unique<HashTable>("blah", bpm_.get(), IntComparator(), 1000, HashFunction<int>());
  }

  void TearDown() override {
    disk_manager_->ShutDown();
    bpm_->UnpinPage(page_id_, true);
    remove("test.db");
    remove("test.log");
  };

  template <typename... Args>
  void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
    std::vector<std::thread> thread_group;

    for (uint64_t i = 0; i < num_threads; ++i) {
      thread_group.push_back(std::thread(args..., i));
    }

    for (uint64_t i = 0; i < num_threads; ++i) {
      thread_group[i].join();
    }
  }

  void InsertHelper(HashTable *ht, const std::vector<std::array<int, 2>> &inserts) {
    auto txn = std::make_unique<Transaction>(0);
    for (size_t i = 0; i < inserts.size(); i++) {
      int k = inserts[i][0];
      int v = inserts[i][1];
      ht->Insert(txn.get(), k, v);
    }
  }

  void DeleteHelper(HashTable *ht, const std::vector<std::array<int, 2>> &inserts) {
    auto txn = std::make_unique<Transaction>(0);
    for (size_t i = 0; i < inserts.size(); i++) {
      int k = inserts[i][0];
      int v = inserts[i][1];
      ht->Remove(txn.get(), k, v);
    }
  }

  void ReadHelper(HashTable *ht, const std::vector<std::array<int, 2>> &inserts) {
    auto txn = std::make_unique<Transaction>(0);
    for (size_t i = 0; i < inserts.size(); i++) {
      int k = inserts[i][0];
      // int v = inserts[i][1];
      std::vector<int> res;
      ht->GetValue(nullptr, k, &res);
      // EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
      // EXPECT_EQ(i, res[0]);
      // ht->Remove(txn.get(), k, v);
    }
  }
};

TEST_F(HashTableConcurrentTest, ConcurrentTest) {
  // NOTE: this only tests that there are no any deadlock problems.
  std::vector<std::array<int, 2>> inserts;
  for (int i = 0; i < 1000; i++) {
    inserts.push_back({i, i});
  }
  std::vector<std::thread> thread_group;
  for (uint64_t i = 0; i < 3; ++i) {
    thread_group.push_back(std::thread([&] { this->InsertHelper(ht_.get(), inserts); }));
  }
  for (uint64_t i = 0; i < 3; ++i) {
    thread_group.push_back(std::thread([&] { this->DeleteHelper(ht_.get(), inserts); }));
  }
  for (uint64_t i = 0; i < 3; ++i) {
    thread_group.push_back(std::thread([&] { this->ReadHelper(ht_.get(), inserts); }));
  }
  for (uint64_t i = 0; i < thread_group.size(); ++i) {
    thread_group[i].join();
  }
}

}  // namespace bustub
