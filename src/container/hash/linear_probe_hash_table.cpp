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
    : index_name_(name),
      buffer_pool_manager_(buffer_pool_manager_),
      comparator_(comparator),
      hash_fn_(std::move(hash_fn)) {
  // Try to get the Hash header_page_id_ from the first page first, since we store all metdata info there
  HeaderPage *first_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  HashTableHeaderPage *hash_header_page;
  CHECK(first_page);
  if (first_page && !first_page->GetRootId(index_name_, &header_page_id_)) {
    // If we didnt find in the first page, we create a new page and save it into the fisrt page.
    hash_header_page =
        reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_)->GetData());
    UpdateHeaderPageId(header_page_id_);
    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
  } else {
    // Otherwise, we simply fetch that page
    hash_header_page =
        reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  }
  CHECK(hash_header_page) << "Can not create or get header page";
  hash_header_page->SetPageId(header_page_id_);
  if (hash_header_page->NumBlocks() < kDefaultBlockSize_) {
    // Assign default number of block pages to this hash table first
    for (size_t i = hash_header_page->NumBlocks(); i < kDefaultBlockSize_; i++) {
      page_id_t block_page_id;
      Page *page = buffer_pool_manager_->NewPage(&block_page_id);
      CHECK(page);
      // HashBlockPage *hash_block_page = reinterpret_cast<HashBlockPage *>(page->GetData());
      // for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
      //   CHECK(!hash_block_page->IsOccupied(i) && !hash_block_page->IsReadable(i));
      // }
      hash_header_page->AddBlockPageId(block_page_id);
      buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
    }
    hash_header_page->SetSize(kDefaultBlockSize_ * BLOCK_ARRAY_SIZE);
  }
  block_size_ = hash_header_page->NumBlocks();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  size_t bucket_id;
  size_t block_page_id;
  size_t block_index = ComputePosition(key, bucket_id, block_page_id);
  size_t curr_block = block_index;
  size_t curr_bucket = bucket_id;
  bool first = true;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(block_page_id);
    HashBlockPage *hash_block_page = reinterpret_cast<HashBlockPage *>(page->GetData());
    // Acquire read latch to guarantee no inserts or removes happen while we are reading on this block
    // but the inserts are removal can still happen to other blocks in this hash table
    page->RLatch();
    for (size_t i = curr_bucket; i < BLOCK_ARRAY_SIZE; i++) {
      CHECK(!(!hash_block_page->IsOccupied(i) && hash_block_page->IsReadable(i)));
      if (i == bucket_id && curr_block == block_index && !first) {
        // Tried all blocks for this hash table, but not find the key
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
        page->RUnlatch();
        table_latch_.RUnlock();
        return result && result->size();
      }
      first = false;
      int occupied = hash_block_page->IsOccupied(i);
      int readable = hash_block_page->IsReadable(i);
      if (occupied && readable) {
        // Find the key, note there may have may values the same key, so we continue
        // reading unitl we meet a place where (occupied, readable) is (0, 0)
        if (result && comparator_(hash_block_page->KeyAt(i), key) == 0) {
          result->push_back(hash_block_page->ValueAt(i));
        }
      } else if (occupied && !readable) {
        // skip the tombstore place
      } else if (!occupied && !readable) {
        // This key never been written before
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
        page->RUnlatch();
        table_latch_.RUnlock();
        return false;
      } else {
        CHECK(false) << "Should not happen.";
      }
    }
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
    // We have done process on this block, now lets move on to the next block
    curr_block = (curr_block + 1) % block_size_;
    curr_bucket = 0;
    auto hash_header_page =
        reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    block_page_id = hash_header_page->GetBlockPageId(curr_block);
    buffer_pool_manager_->UnpinPage(header_page_id_, /*is_dirty*/ true);
  }
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return InsertImpl(transaction, key, value, /*acquire_lock*/ true);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::InsertImpl(Transaction *transaction, const KeyType &key, const ValueType &value, bool acquire_lock) {
  LOG(DEBUG) << "INserting... " << key;
  if (acquire_lock) {
    table_latch_.RLock();
  }
  size_t bucket_id;
  size_t block_page_id;
  size_t block_index = ComputePosition(key, bucket_id, block_page_id);
  size_t curr_block = block_index;
  size_t curr_bucket = bucket_id;
  bool first = true;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(block_page_id);
    HashBlockPage *hash_block_page = reinterpret_cast<HashBlockPage *>(page->GetData());
    // We are about to write to this block
    page->WLatch();
    for (size_t i = curr_bucket; i < BLOCK_ARRAY_SIZE; i++) {
      if (i == bucket_id && curr_block == block_index && !first) {
        CHECK(acquire_lock) << "Should not reach here.";
        // Alreay tried all buckets, but not find a place to insert.
        // Table is full at here, resize first, then insert again.
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
        if (block_size_ > 100) {
          throw Exception("Too many data to inserts.");
        }
        // NOTE: the unlock order here
        page->WUnlatch();
        table_latch_.RUnlock();
        LOG(DEBUG) << "Hash table is full, try to resize";
        Resize(GetSize());
        return InsertImpl(transaction, key, value, acquire_lock);
      }
      first = false;
      bool occupied = hash_block_page->IsOccupied(i);
      bool readable = hash_block_page->IsReadable(i);
      if (occupied && readable) {
        if (comparator_(hash_block_page->KeyAt(i), key) == 0 && hash_block_page->ValueAt(i) == value) {
          buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
          // Key value pair already exists.
          page->WUnlatch();
          if (acquire_lock) {
            table_latch_.RUnlock();
          }
          return false;
        } else {
          // Keep trying to next place
        }
      } else if ((occupied && !readable) || (!occupied && !readable)) {
        // if in both the tombstore and emtry places
        CHECK(hash_block_page->Insert(i, key, value));
        count_++;
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
        page->WUnlatch();
        if (acquire_lock) {
          table_latch_.RUnlock();
        }
        return true;
      } else {
        CHECK(false) << "Should not happen";
      }
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
    curr_block = (curr_block + 1) % block_size_;
    curr_bucket = 0;
    auto hash_header_page =
        reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    CHECK(hash_header_page->GetPageId() == header_page_id_) << hash_header_page->GetPageId() << " " << header_page_id_;
    block_page_id = hash_header_page->GetBlockPageId(curr_block);
    buffer_pool_manager_->UnpinPage(header_page_id_, /*is_dirty*/ true);
  }
  if (acquire_lock) {
    table_latch_.RUnlock();
  }
  CHECK(false) << "Should not reach here.";
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::ComputePosition(const KeyType &key, size_t &bucket_index, size_t &block_page_id) {
  uint64_t h = hash_fn_.GetHash(key);
  size_t block_index = (h / BLOCK_ARRAY_SIZE) % block_size_;
  auto hash_header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
  CHECK(hash_header_page);
  CHECK(hash_header_page->GetPageId() == header_page_id_) << hash_header_page->GetPageId() << " " << header_page_id_;
  block_page_id = hash_header_page->GetBlockPageId(block_index);
  buffer_pool_manager_->UnpinPage(header_page_id_, /*is_dirty*/ true);
  bucket_index = h % BLOCK_ARRAY_SIZE;
  return block_index;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  LOG(DEBUG) << "Removing ... " << key;
  table_latch_.RLock();
  size_t bucket_id;
  size_t block_page_id;
  size_t block_index = ComputePosition(key, bucket_id, block_page_id);
  size_t curr_block = block_index;
  size_t curr_bucket = bucket_id;
  bool first = true;
  do {
    Page *page = buffer_pool_manager_->FetchPage(block_page_id);
    HashBlockPage *hash_block_page = reinterpret_cast<HashBlockPage *>(page->GetData());
    page->WLatch();
    for (size_t i = curr_bucket; i < BLOCK_ARRAY_SIZE; i++) {
      if (i == bucket_id && curr_block == block_index && !first) {
        page->WUnlatch();
        table_latch_.RUnlock();
        return false;
      }
      first = false;
      bool occupied = hash_block_page->IsOccupied(i);
      bool readable = hash_block_page->IsReadable(i);
      if (occupied && readable) {
        if (comparator_(hash_block_page->KeyAt(i), key) == 0 && hash_block_page->ValueAt(i) == value) {
          hash_block_page->Remove(i);
          count_--;
          buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
          page->WUnlatch();
          table_latch_.RUnlock();
          return true;
        }
      } else if (occupied && !readable) {
        // tombstone place
      } else if (!occupied && !readable) {
        buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
        page->WUnlatch();
        table_latch_.RUnlock();
        return false;
      } else {
        CHECK(false) << "Should not happen.";
      }
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(block_page_id, /*is_dirty*/ true);
    curr_block = (curr_block + 1) % block_size_;
    curr_bucket = 0;
    auto hash_header_page =
        reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(header_page_id_)->GetData());
    block_page_id = hash_header_page->GetBlockPageId(curr_block);
    CHECK(hash_header_page->GetPageId() == header_page_id_) << hash_header_page->GetPageId() << " " << header_page_id_;
    buffer_pool_manager_->UnpinPage(hash_header_page->GetPageId(), /*is_dirty*/ true);
  } while (true);
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  // Acquire write lock, to make sure no other operations are running.
  page_id_t old_header_page_id = header_page_id_;
  table_latch_.WLock();
  if (header_page_id_ != old_header_page_id) {
    LOG(DEBUG) << "Already resized";
    table_latch_.WUnlock();
    return;
  }
  // Create a new header page and allocate new blocks for this hash table.
  size_t new_block_size = (initial_size + BLOCK_ARRAY_SIZE - 1) / BLOCK_ARRAY_SIZE * 2;
  LOG(DEBUG) << "Resizing to ... " << new_block_size;
  page_id_t new_header_page_id;
  auto new_header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&new_header_page_id)->GetData());
  for (size_t i = 0; i < new_block_size; i++) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    CHECK(new_page);
    new_header_page->AddBlockPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(new_page_id, false);
  }
  new_header_page->SetPageId(new_header_page_id);
  buffer_pool_manager_->UnpinPage(new_header_page_id, true);

  // Update the frist page to the new header page.
  header_page_id_ = new_header_page_id;
  UpdateHeaderPageId(false);
  block_size_ = new_block_size;

  // Insert the old values into the new resized hash tables, we have to do this since the position
  // for the same key might be mapped to different places in the new resized hash table
  count_ = 0;
  auto old_header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->FetchPage(old_header_page_id)->GetData());
  for (size_t i = 0; i < old_header_page->NumBlocks(); i++) {
    page_id_t old_page_id = old_header_page->GetBlockPageId(i);
    Page *old_page = buffer_pool_manager_->FetchPage(old_page_id);
    HashBlockPage *old_block_page = reinterpret_cast<HashBlockPage *>(old_page->GetData());
    for (size_t j = 0; j < BLOCK_ARRAY_SIZE; j++) {
      auto key = old_block_page->KeyAt(j);
      auto value = old_block_page->ValueAt(j);
      // TOOD(zhangqiang): check the nullptr passed here.
      InsertImpl(/*transaction*/ nullptr, key, value, /*acquire_lock*/ false);
    }
    buffer_pool_manager_->UnpinPage(old_page_id, false);
  }
  buffer_pool_manager_->UnpinPage(old_header_page_id, false);
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
    // fmt::print("Insert record into header page: {} {}\n", index_name_, header_page_id_);
    first_page->InsertRecord(index_name_, header_page_id_);
  } else {
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
