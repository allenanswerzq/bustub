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
    header_page_->SetSize(kDefaultBlockSize_);
  }
  block_size_ = header_page_->GetSize();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  int bucket_id;
  int block_page_id;
  int block_index = GetPage(key, bucket_id, block_page_id);
  int curr_block = block_index;
  do {
    Page * page =  buffer_pool_manager_->FetchPage(block_page_id);
    HashBlockPage * hash_block_page =  reinterpret_cast<HashBlockPage*>(page->GetData());
    for (size_t i = bucket_id; i < BLOCK_ARRAY_SIZE; i++) {
      if (hash_block_page->IsReadable(i)) {
        if (result && comparator_(hash_block_page->KeyAt(i), key) == 0) {
          result->push_back(hash_block_page->ValueAt(i));
        }
      }
      else {
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/false);
        return false;
      }
    }
    buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/false);
    curr_block = (curr_block + 1) % block_size_;
    bucket_id = 0;
    block_page_id = header_page_->GetBlockPageId(curr_block);
  } while (curr_block != block_index);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  fmt::print("Hash inserting {}\n", key);
  table_latch_.RLock();
  int bucket_id;
  int block_page_id;
  int block_index = GetPage(key, bucket_id, block_page_id);
  int curr_block = block_index;
  bool succ = false;
  do {
    Page * page =  buffer_pool_manager_->FetchPage(block_page_id);
    HashBlockPage * hash_block_page =  reinterpret_cast<HashBlockPage*>(page->GetData());
    for (size_t i = bucket_id; i < BLOCK_ARRAY_SIZE; i++) {
      if (hash_block_page->IsOccupied(i)) {
        if (comparator_(hash_block_page->KeyAt(i), key) == 0 && hash_block_page->ValueAt(i) == value) {
          buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/false);
          table_latch_.RUnlock();
          return false;
        }
      }
      else {
        CHECK(hash_block_page->Insert(bucket_id, key, value));
        succ = true;
      }
    }
    buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/false);
    curr_block = (curr_block + 1) % block_size_;
    bucket_id = 0;
    block_page_id = header_page_->GetBlockPageId(curr_block);
  } while (!succ && curr_block != block_index);
  table_latch_.RUnlock();
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int HASH_TABLE_TYPE::GetPage(const KeyType &key, int& bucket_index, int& block_page_id) {
  uint64_t h = hash_fn_.GetHash(key);
  int block_index = (h / BLOCK_ARRAY_SIZE) % block_size_;
  block_page_id = header_page_->GetBlockPageId(block_index);
  bucket_index = h % BLOCK_ARRAY_SIZE;
  return block_index;
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
