//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (size_t i = 0; i < array_.size(); i++) {
    if (comparator(KeyAt(i), key) == 0) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (size_t i = 0; i < array_.size(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const {
  CHECK(index < GetSize());
  return array_[index].second;
}
/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  CHECK(index < (int)array_.size()) << index << " " << (int)array_.size() << " " << GetPageId();
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) { return array_[index]; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  std::lock_guard<std::mutex> guard(mutex_);
  LOG(DEBUG) << "INSERT: " << GetPageId() << " " << key;
  if (array_.empty()) {
    array_.push_back({key, value});
  } else {
    bool ok = false;
    for (size_t i = 0; i < array_.size(); i++) {
      if (comparator(KeyAt(i), key) > 0) {
        // k[i-1] < key < k[i]
        array_.insert(array_.begin() + i, {key, value});
        ok = true;
        break;
      }
    }
    if (!ok) {
      array_.push_back({key, value});
    }
  }
  SetSize(array_.size());
  DebugOutput();
  return array_.size();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetArray(const std::vector<MappingType> &array) {
  // std::lock_guard<std::mutex> guard(mutex_);
  CHECK(array_.empty());
  array_ = array;
  SetSize(array_.size());
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString() const {
  std::vector<MappingType> array_copy;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    array_copy = array_;
  }
  std::ostringstream oss;
  oss << "[ ";
  for (size_t i = 0; i < array_copy.size(); i++) {
    if (i > 0) {
      oss << ",";
    }
    oss << array_copy[i].first << " -> " << array_copy[i].second;
  }
  oss << " ]";
  return oss.str();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::DebugOutput() {
  // LOG(DEBUG) << ">>>>>>>>>>>>> internal page " << GetPageId() << " has size: " << array_.size();
  // for (size_t i = 0; i < array_.size(); i++) {
  //   LOG(DEBUG) << i << " "
  //              << "key: " << KeyAt(i) << " value: " << array_[i].second;
  // }
}

/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  std::lock_guard<std::mutex> guard(mutex_);
  CHECK(recipient->GetSize() == 0) << "Expected recipient is empty.";
  // CHECK(GetSize() == GetMaxSize());

  size_t half = GetSize() / 2;
  std::vector<MappingType> give;
  while (array_.size() > half) {
    give.push_back(array_.back());
    array_.pop_back();
  }

  std::reverse(give.begin(), give.end());
  recipient->SetArray(give);

  // Chain these two node together
  int next_page = next_page_id_;
  next_page_id_ = recipient->GetPageId();
  recipient->SetNextPageId(next_page);

  SetSize(array_.size());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  std::lock_guard<std::mutex> guard(mutex_);
  for (size_t i = 0; i < array_.size(); i++) {
    if (comparator(KeyAt(i), key) == 0) {
      if (value) {
        *value = array_[i].second;
      }
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  std::lock_guard<std::mutex> guard(mutex_);
  LOG(DEBUG) << "REMOVE " << GetPageId() << " " << key;
  for (size_t i = 0; i < array_.size(); i++) {
    if (comparator(KeyAt(i), key) == 0) {
      array_.erase(array_.begin() + i);
      break;
    }
  }
  SetSize(array_.size());
  return array_.size();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't
 * forget to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType & /*unused*/,
                                           BufferPoolManager * /*unused*/) {
  std::lock_guard<std::mutex> guard(mutex_);
  for (size_t i = 0; i < array_.size(); i++) {
    recipient->CopyLastFrom(array_[i]);
  }
  page_id_t next_id = GetNextPageId();
  recipient->SetNextPageId(next_id);
  array_.clear();
  SetSize(array_.size());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, const KeyType & /*middle_key*/,
                                                  BufferPoolManager *buffer_pool_manager) {
  std::lock_guard<std::mutex> guard(mutex_);
  CHECK(array_.size() && recipient);
  recipient->CopyLastFrom(array_[0]);
  array_.erase(array_.begin());
  SetSize(array_.size());
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array_.push_back(item);
  SetSize(array_.size());
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, const KeyType & /*unused*/,
                                                   BufferPoolManager * /*unused*/) {
  std::lock_guard<std::mutex> guard(mutex_);
  CHECK(array_.size() && recipient);
  recipient->CopyFirstFrom(array_.back());
  array_.pop_back();
  SetSize(array_.size());
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  array_.insert(array_.begin(), item);
  SetSize(array_.size());
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
template class BPlusTreeLeafPage<int, int, IntegerComparator<true>>;
template class BPlusTreeLeafPage<int, int, IntegerComparator<false>>;

}  // namespace bustub
