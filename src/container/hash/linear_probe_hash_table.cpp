//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"
#include "storage/page/header_page.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : index_name_(name), buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // Try to get the Hash header_page_id_ from the first page first, since we store all metdata info there
  HeaderPage *first_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (!first_page->GetRootId(index_name_, &header_page_id_)) {
    header_page_ = reinterpret_cast<HashTableHeaderPage*>(
        buffer_pool_manager->NewPage(&header_page_id_)->GetData());
  }
  else {
    header_page_ = reinterpret_cast<HashTableHeaderPage*>(
        buffer_pool_manager->FetchPage(header_page_id_)->GetData());
  }
  // Make room for this hash table
  if (header_page_->GetSize() < kDefaultBlockSize_) {
    for (size_t i = header_page_->GetSize(); i < kDefaultBlockSize_; i++) {
      page_id_t block_page_id;
      Page * page = buffer_pool_manager->NewPage(&block_page_id);
      CHECK(page);
      header_page_->AddBlockPageId(block_page_id);
      buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
    }
  }
  block_size_ = header_page_->GetSize();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  fmt::print("Inserting {}\n", key);
  int bucket_id;
  int block_page_id;
  HashBlockPage * hash_block_page = GetPage(key, &bucket_id, &block_page_id);
  // Inserts and remove can occur concurrently.
  table_latch_.RLock();
  bool res = hash_block_page->Insert(bucket_id, key, value);
  table_latch_.RUnlock();
  buffer_pool_manager_ ->UnpinPage(block_page_id, /*is_dirty*/true);
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableBlockPage<KeyType, ValueType, KeyComparator> *
HASH_TABLE_TYPE::GetPage(const KeyType &key, int* bucket_id, int * block_page_id) {
  uint64_t h = hash_fn_.GetHash(key);
  int block_index = (h / BLOCK_ARRAY_SIZE) % block_size_;
  page_id_t page_id = header_page_->GetBlockPageId(block_index);
  if (bucket_id) {
    *bucket_id = h % BLOCK_ARRAY_SIZE;
  }
  if (block_page_id) {
    *block_page_id = page_id;
  }
  Page * page =  buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<HashBlockPage*>(page->GetData());
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // HashBlockPage * hash_block_page = GetPage(key);
  // if (hash_block_page->IsOccupied(bucket_id)) {
  //   // Inserts and remove can occur concurrently.
  //   table_latch_->RLock();
  //   hash_block_page->Remove(bucket_id);
  //   table_latch_->RUnlock();
  //   buffer_pool_manager_ ->UnpinPage(block_page_id);
  //   return true;
  // }
  // else {
  //   return false;
  // }
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {

}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::UpdateHeaderPageId(int insert_record) {
  HeaderPage *first_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in first_page
    first_page->InsertRecord(index_name_, header_page_id_);
  }
  else {
    // update root_page_id in first_page
    first_page->UpdateRecord(index_name_, header_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}


template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
