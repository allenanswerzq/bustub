/**
 * b_plus_tree_insert_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <iostream>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

// TODO: rewrite this test.
namespace bustub {

TEST(BPlusTreeTests, InsertTest0) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(1000, disk_manager);

  int leaf_max_size = RandomInt(2, 10);
  int internal_max_size = RandomInt(3, 10);
  BPlusTree<int, int, IntegerComparator<false>> tree("foo_pk", bpm, IntegerComparator<false>{}, leaf_max_size,
                                                     internal_max_size);

  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  EXPECT_EQ(page_id, 0);

  std::map<int, int> mp;
  for (int i = 0; i < 100; i++) {
    int key = RandomInt(0, 10000);
    while (mp.count(key)) {
      key = RandomInt(0, 10000);
    }
    tree.Insert(key, i, transaction);
    mp[key] = i;
  }

  std::vector<std::pair<int, int>> inserts;
  for (auto it : mp) {
    inserts.push_back(it);
  }

  std::vector<int> value;
  for (int i = 0; i < 100; i++) {
    value.clear();
    EXPECT_EQ(tree.GetValue(inserts[i].first, &value), true);
    EXPECT_EQ(value.size(), 1);
    EXPECT_EQ(value[0], inserts[i].second);
  }

  tree.Draw(bpm, "tree.dot");

  // Test range scan
  int i = 0;
  for (auto it = tree.Begin(inserts[0].first); it != tree.end(); it++, i++) {
    int val1 = (*it).second;
    int val2 = it->second;
    EXPECT_EQ(val1, val2);
    EXPECT_EQ(it->first, inserts[i].first);
    EXPECT_EQ(val1, inserts[i].second);
  }
  EXPECT_EQ(i, 100);

  i = 0;
  for (auto it = tree.begin(); it != tree.end(); it++, i++) {
    int val1 = (*it).second;
    int val2 = it->second;
    EXPECT_EQ(val1, val2);
    EXPECT_EQ(it->first, inserts[i].first);
    EXPECT_EQ(val1, inserts[i].second);
  }
  EXPECT_EQ(i, 100);

  i = 0;
  for (auto it = tree.begin(); it != tree.end(); ++it, i++) {
    int val1 = (*it).second;
    int val2 = it->second;
    EXPECT_EQ(val1, val2);
    EXPECT_EQ(it->first, inserts[i].first);
    EXPECT_EQ(val1, inserts[i].second);
  }
  EXPECT_EQ(i, 100);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
  remove("tree.dot");
}

TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);

  BPlusTree<int, int, IntegerComparator<true>> tree("foo_pk", bpm, IntegerComparator<true>{}, 2, 3);

  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  EXPECT_EQ(page_id, 0);

  // Test insert
  for (int i = 0; i < 30; i++) {
    tree.Insert(i, i, transaction);
  }

  // Test point search
  std::vector<int> value;
  for (int i = 0; i < 30; i++) {
    value.clear();
    tree.GetValue(i, &value);
    EXPECT_EQ(value.size(), 1);
    EXPECT_EQ(value[0], i);
  }

  tree.Draw(bpm, "tree.dot");

  // Test range scan
  int i = 29;
  for (auto it = tree.Begin(29); it != tree.end(); it++, i--) {
    int val1 = (*it).second;
    int val2 = it->second;
    EXPECT_EQ(it->first, i);
    EXPECT_EQ(val1, val2);
    EXPECT_EQ(val1, i);
  }
  EXPECT_EQ(i, -1);

  i = 29;
  for (auto it = tree.begin(); it != tree.end(); it++, i--) {
    int val1 = (*it).second;
    int val2 = it->second;
    EXPECT_EQ(it->first, i);
    EXPECT_EQ(val1, val2);
    EXPECT_EQ(val1, i);
  }
  EXPECT_EQ(i, -1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
  remove("tree.dot");
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest3) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  tree.Draw(bpm, "tree.dot");

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
