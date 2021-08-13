//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  CHECK(index < (int)array_.size());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (size_t i = 0; i < array_.size(); i++) {
    if (ValueAt(i) == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(
    const KeyType &key, const KeyComparator &comparator) const {
  for (size_t i = 1; i < array_.size(); i++) {
    // k[i] <= key < k[i + 1]
    if (comparator(key, KeyAt(i)) < 0) {
      return ValueAt(i - 1);
    }
  }
  return array_.back().second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  CHECK(array_.empty());
  // TODO: check this
  array_.push_back({/*dummy*/ new_key, old_value});
  array_.push_back({new_key, new_value});
  SetSize(array_.size());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DebugOutput() {
  LOG(DEBUG) << ">>>>>>>>>>>>> internal page " << GetPageId()
             << " has size: " << array_.size();
  for (size_t i = 0; i < array_.size(); i++) {
    LOG(DEBUG) << i << " "
               << "key: " << KeyAt(i) << " value: " << ValueAt(i);
  }
}

INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetMininumKey(
    const KeyComparator &comparator) const {
  CHECK(array_.size() > 1);
  // NOTE: the first key is valid after spliting a internal node
  KeyType ans = KeyAt(0);
  for (size_t i = 1; i < array_.size(); i++) {
    if (comparator(KeyAt(i), ans) < 0) {
      ans = KeyAt(i);
    }
  }
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key,
                                           const ValueType &value,
                                           const KeyComparator &comparator) {
  if (array_.empty()) {
    array_.push_back({/*invaild*/ key, value});
    array_.push_back({key, value});
  } else {
    bool ok = false;
    for (size_t i = 1; i < array_.size(); i++) {
      if (comparator(KeyAt(i), key) > 0) {
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

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  int index = ValueIndex(old_value);
  array_.insert(array_.begin() + index + 1, {new_key, new_value});
  return array_.size();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetArray(
    const std::vector<MappingType> &array) {
  CHECK(array_.empty());
  array_ = array;
  SetSize(array_.size());
}

/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient) {
  CHECK(recipient->GetSize() == 0) << "Expected recipient is empty.";

  size_t half = GetSize() / 2;
  std::vector<MappingType> give;
  while (array_.size() > half) {
    give.push_back(array_.back());
    array_.pop_back();
  }

  std::reverse(give.begin(), give.end());
  recipient->SetArray(give);
  SetSize(array_.size());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents
 * page now changes to me. So I need to 'adopt' them by changing their parent
 * page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  CHECK(index < (int)array_.size());
  array_.erase(array_.begin() + index);
  SetSize(array_.size());
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  CHECK(array_.size() == 1);
  ValueType ans = ValueAt(0);
  array_.clear();
  SetSize(array_.size());
  return ans;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the
 * invariant. You also need to use BufferPoolManager to persist changes to the
 * parent page id for those pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, const KeyType &middle_key,
    BufferPoolManager *buffer_pool_manager) {
  CHECK(!IsRootPage() && recipient);

  recipient->SetKeyAt(0, middle_key);
  for (size_t i = array_.size(); i >= 0; i--) {
    recipient->CopyFirstFrom(array_[i], buffer_pool_manager);
  }
  array_.clear();
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the
 * invariant. You also need to use BufferPoolManager to persist changes to the
 * parent page id for those pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient, const KeyType &middle_key,
    BufferPoolManager *buffer_pool_manager) {
  CHECK(array_.size());
  // TODO: write more comments
  recipient->CopyLastFrom(array_[0]);
  recipient->SetKeyAt(recipient->GetSize() - 1, middle_key);
  Remove(0);

  page_id_t parent_id = GetParentPageId();
  BPlusTreeInternalPage * parent = reinterpret_cast<BPlusTreeInternalPage*>(
      buffer_pool_manager->FetchPage(parent_id)->GetData());
  int index = parent->ValueIndex(node->GetPageId());
  // TODO: DOULBE CHECK
  parent->SetKeyAt(index, node->KeyAt(0));
  buffer_pool_manager->UnpinPage(parent_id);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be
 * updated. So I need to 'adopt' it by changing its parent page id, which needs
 * to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_.push_back(pair);
  SetSize(array_.size());

  // Modify the parent pointer for the child page
  page_id_t child_id = ValueAt(GetSize() - 1);
  BPlusTreeInternalPage * child = reinterpret_cast<BPlusTreeInternalPage*>(
      buffer_pool_manager->FetchPage(child_id)->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_id);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s
 * array to position the middle_key at the right place. You also need to use
 * BufferPoolManager to persist changes to the parent page id for those pages
 * that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, const KeyType &middle_key,
    BufferPoolManager *buffer_pool_manager) {
  CHECK(array_.size());
  CHECK(!IsRootPage());

  int last = GetSize() - 1;
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(array_[last]);
  Remove(last);

  page_id_t parent_id = GetParentPageId();
  BPlusTreeInternalPage * parent = reinterpret_cast<BPlusTreeInternalPage*>(
      buffer_pool_manager->FetchPage(parent_id)->GetData());
  int index = parent->ValueIndex(node->GetPageId());
  // TODO: DOULBE CHECK
  parent->SetKeyAt(index, node->KeyAt(0));
  buffer_pool_manager->UnpinPage(parent_id);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be
 * updated. So I need to 'adopt' it by changing its parent page id, which needs
* to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_.insert(array_.begin(), pair);
  SetSize(array_.size());

  // Modify the parent pointer for the child page
  page_id_t child_id = ValueAt(0);
  BPlusTreeInternalPage * child = reinterpret_cast<BPlusTreeInternalPage*>(
      buffer_pool_manager->FetchPage(child_id)->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_id);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
template class BPlusTreeInternalPage<int, int, IntegerComparator<true>>;
template class BPlusTreeInternalPage<int, int, IntegerComparator<false>>;

}  // namespace bustub
