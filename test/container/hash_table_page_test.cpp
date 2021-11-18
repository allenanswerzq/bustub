//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_page_test.cpp
//
// Identification: test/container/hash_table_page_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/hash_table_block_page.h"
#include "storage/page/hash_table_header_page.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTablePageTest, BlockPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(5, disk_manager);

  // get a block page from the BufferPoolManager
  page_id_t block_page_id = INVALID_PAGE_ID;

  auto block_page =
      reinterpret_cast<HashTableBlockPage<int, int, IntComparator> *>(bpm->NewPage(&block_page_id, nullptr)->GetData());

  // insert a few (key, value) pairs
  for (unsigned i = 0; i < 10; i++) {
    block_page->Insert(i, i, i);
  }

  // check for the inserted pairs
  for (unsigned i = 0; i < 10; i++) {
    EXPECT_EQ(i, block_page->KeyAt(i));
    EXPECT_EQ(i, block_page->ValueAt(i));
  }

  // remove a few pairs
  for (unsigned i = 0; i < 10; i++) {
    if (i % 2 == 1) {
      block_page->Remove(i);
    }
  }

  // check for the flags
  for (unsigned i = 0; i < 15; i++) {
    if (i < 10) {
      EXPECT_TRUE(block_page->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(block_page->IsReadable(i));
      } else {
        EXPECT_TRUE(block_page->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(block_page->IsOccupied(i));
    }
  }

  // unpin the header page now that we are done
  bpm->UnpinPage(block_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
