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
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager_,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : index_name_(name), buffer_pool_manager_(buffer_pool_manager_), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // Try to get the Hash header_page_id_ from the first page first, since we store all metdata info there
  HeaderPage *first_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  HashTableHeaderPage* hash_header_page;
  CHECK(first_page);
  if (first_page && !first_page->GetRootId(index_name_, &header_page_id_)) {
    hash_header_page = reinterpret_cast<HashTableHeaderPage*>(
        buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
    fmt::print("Creating header page...{} {}\n", header_page_id_, index_name_);
    UpdateHeaderPageId(header_page_id_);
    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
  }
  else {
    hash_header_page = reinterpret_cast<HashTableHeaderPage*>(
        buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  }
  CHECK(hash_header_page) << "Can not create or get header page";
  hash_header_page->SetPageId(header_page_id_);
  // Make room for this hash table
  if (hash_header_page->NumBlocks() < kDefaultBlockSize_) {
    for (size_t i = hash_header_page->NumBlocks(); i < kDefaultBlockSize_; i++) {
      page_id_t block_page_id;
      Page * page = buffer_pool_manager_->NewPage(&block_page_id);
      CHECK(page);
      HashBlockPage * hash_block_page =  reinterpret_cast<HashBlockPage*>(page->GetData());
      for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
        CHECK(!hash_block_page->IsOccupied(i) && !hash_block_page->IsReadable(i));
      }
      hash_header_page->AddBlockPageId(block_page_id);
      buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
    }
    hash_header_page->SetSize(kDefaultBlockSize_ * BLOCK_ARRAY_SIZE);
  }
  // block_size_ = hash_header_page->GetSize();
  block_size_ = hash_header_page->NumBlocks();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  size_t bucket_id;
  size_t block_page_id;
  size_t block_index = ComputePosition(key, bucket_id, block_page_id);
  size_t curr_block = block_index;
  size_t curr_bucket = bucket_id;
  bool first = true;
  while (true) {
    Page * page =  buffer_pool_manager_->FetchPage(block_page_id);
    HashBlockPage * hash_block_page =  reinterpret_cast<HashBlockPage*>(page->GetData());
    for (size_t i = curr_bucket; i < BLOCK_ARRAY_SIZE; i++) {
      CHECK(!(!hash_block_page->IsOccupied(i) && hash_block_page->IsReadable(i)));
      if (i == bucket_id && curr_block == block_index && !first) {
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
        return false;
      }
      first = false;
      int occupied = hash_block_page->IsOccupied(i);
      int readable = hash_block_page->IsReadable(i);
      if (occupied && readable) {
        if (result && comparator_(hash_block_page->KeyAt(i), key) == 0) {
          result->push_back(hash_block_page->ValueAt(i));
        }
      }
      else if (occupied && !readable) {
        // tombstore place
      }
      else if (!occupied && !readable) {
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
        return false;
      }
      else {
        CHECK(false) << "Should not happen.";
      }
    }
    buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
    curr_block = (curr_block + 1) % block_size_;
    curr_bucket = 0;
    auto hash_header_page = reinterpret_cast<HashTableHeaderPage*>(
        buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    block_page_id = hash_header_page->GetBlockPageId(curr_block);
    buffer_pool_manager_->UnpinPage(header_page_id_, /*is_dirty*/true);
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  fmt::print("Hash inserting {}\n", key);
  table_latch_.RLock();
  size_t bucket_id;
  size_t block_page_id;
  size_t block_index = ComputePosition(key, bucket_id, block_page_id);
  size_t curr_block = block_index;
  size_t curr_bucket = bucket_id;
  bool first = true;
  while (true) {
    Page * page =  buffer_pool_manager_->FetchPage(block_page_id);
    HashBlockPage * hash_block_page =  reinterpret_cast<HashBlockPage*>(page->GetData());
    for (size_t i = curr_bucket; i < BLOCK_ARRAY_SIZE; i++) {
      if (i == bucket_id && curr_block == block_index && !first) {
        // Alreay tried all buckets, but not find a place to insert.
        // Table is full at here, resize first, then insert again.
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
        table_latch_.RUnlock();
        if (block_size_ > 100) {
          throw Exception("Too many data to inserts.");
        }
        Resize(GetSize());
        return Insert(transaction, key, value);
      }
      first = false;
      bool occupied = hash_block_page->IsOccupied(i);
      bool readable = hash_block_page->IsReadable(i);
      if (occupied && readable) {
        if (comparator_(hash_block_page->KeyAt(i), key) == 0 && hash_block_page->ValueAt(i) == value) {
          buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
          table_latch_.RUnlock();
          fmt::print("Key value pair already exist. {} {}\n", key, value);
          return false;
        }
      }
      else if ((occupied && !readable) || (!occupied && !readable)) {
        fmt::print("Succ inserted key {} {} {} {} {} {}\n", curr_block, block_page_id, i, occupied, readable, BLOCK_ARRAY_SIZE);
        CHECK(hash_block_page->Insert(i, key, value));
        fmt::print("Succ inserted key {} {} {} {} {} {}\n", curr_block, block_page_id, i, hash_block_page->IsOccupied(i), hash_block_page->IsReadable(i), BLOCK_ARRAY_SIZE);
        count_++;
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
        table_latch_.RUnlock();
        return true;
      }
      else {
        CHECK(false) << "Should not happen";
      }
    }
    buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
    curr_block = (curr_block + 1) % block_size_;
    curr_bucket = 0;
    auto hash_header_page = reinterpret_cast<HashTableHeaderPage*>(
        buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    CHECK(hash_header_page->GetPageId() == header_page_id_) << hash_header_page->GetPageId() << " " << header_page_id_;
    block_page_id = hash_header_page->GetBlockPageId(curr_block);
    buffer_pool_manager_->UnpinPage(header_page_id_, /*is_dirty*/true);
  }
  table_latch_.RUnlock();
  CHECK(false) << "Should not reach here.";
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::ComputePosition(const KeyType &key, size_t& bucket_index, size_t& block_page_id) {
  uint64_t h = hash_fn_.GetHash(key);
  size_t block_index = (h / BLOCK_ARRAY_SIZE) % block_size_;
  auto hash_header_page = reinterpret_cast<HashTableHeaderPage*>(
      buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  CHECK(hash_header_page->GetPageId() == header_page_id_) << hash_header_page->GetPageId() << " " << header_page_id_;
  block_page_id = hash_header_page->GetBlockPageId(block_index);
  buffer_pool_manager_->UnpinPage(header_page_id_, /*is_dirty*/true);
  bucket_index = h % BLOCK_ARRAY_SIZE;
  return block_index;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  size_t bucket_id;
  size_t block_page_id;
  size_t block_index = ComputePosition(key, bucket_id, block_page_id);
  size_t curr_block = block_index;
  size_t curr_bucket = bucket_id;
  do {
    Page * page =  buffer_pool_manager_->FetchPage(block_page_id);
    HashBlockPage * hash_block_page =  reinterpret_cast<HashBlockPage*>(page->GetData());
    for (size_t i = curr_bucket; i < BLOCK_ARRAY_SIZE; i++) {
      bool occupied = hash_block_page->IsOccupied(i);
      bool readable = hash_block_page->IsReadable(i);
      if (occupied && readable) {
        if (comparator_(hash_block_page->KeyAt(i), key) == 0 && hash_block_page->ValueAt(i) == value) {
          hash_block_page->Remove(i);
          count_--;
          buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
          table_latch_.RUnlock();
          return true;
        }
      }
      else if (occupied && !readable) {
        // tombstore place
      }
      else if (!occupied && !readable) {
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
        table_latch_.RUnlock();
        return false;
      }
      else {
        CHECK(false) << "Should not happen.";
      }
    }
    buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
    curr_block = (curr_block + 1) % block_size_;
    curr_bucket = 0;
    auto hash_header_page = reinterpret_cast<HashTableHeaderPage*>(
        buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    block_page_id = hash_header_page->GetBlockPageId(curr_block);
    CHECK(hash_header_page->GetPageId() == header_page_id_) << hash_header_page->GetPageId() << " " << header_page_id_;
    buffer_pool_manager_->UnpinPage(hash_header_page->GetPageId(), /*is_dirty*/true);
  } while (true);
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  size_t new_block = (initial_size + BLOCK_ARRAY_SIZE - 1) / BLOCK_ARRAY_SIZE * 2;
  fmt::print("resize: {} {} {} {}\n", new_block, block_size_, initial_size, BLOCK_ARRAY_SIZE);
  auto hash_header_page = reinterpret_cast<HashTableHeaderPage*>(
      buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  if (new_block > block_size_) {
    for (size_t i = hash_header_page->NumBlocks(); i < new_block; i++) {
      page_id_t block_page_id;
      Page * page = buffer_pool_manager_->NewPage(&block_page_id);
      CHECK(page);
      HashBlockPage * hash_block_page =  reinterpret_cast<HashBlockPage*>(page->GetData());
      for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
        CHECK(!hash_block_page->IsOccupied(i) && !hash_block_page->IsReadable(i));
      }
      LOG(DEBUG) << "Adding a new page into hash table: " << block_page_id;
      hash_header_page->AddBlockPageId(block_page_id);
      buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/true);
    }
    hash_header_page->SetSize(new_block * BLOCK_ARRAY_SIZE);
    block_size_ = hash_header_page->NumBlocks();
  }
  CHECK(hash_header_page->GetPageId() == header_page_id_) << hash_header_page->GetPageId() << " " << header_page_id_;
  buffer_pool_manager_->UnpinPage(header_page_id_, /*is_dirty*/true);

  hash_header_page = reinterpret_cast<HashTableHeaderPage*>(
      buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  CHECK(hash_header_page->NumBlocks() == new_block);
  buffer_pool_manager_->UnpinPage(header_page_id_, /*is_dirty*/false);

  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return count_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::UpdateHeaderPageId(int insert_record) {
  HeaderPage *first_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in first_page
    fmt::print("Insert record into header page: {} {}\n", index_name_, header_page_id_);
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
