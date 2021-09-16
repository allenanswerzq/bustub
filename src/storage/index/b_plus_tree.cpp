//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  BPlusTreePage *curr = AcquireReadLatch(key, transaction);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(curr);
  ValueType val;
  bool ans = leaf->Lookup(key, &val, comparator_);
  LOG(DEBUG) << "Lookup leaf node: " << curr->GetPageId() << " result: " << ans;
  if (result) {
    result->push_back(val);
  }
  ReleaseAllLatch(transaction, /*is_write*/ false);
  return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    if (!StartNewTree(key, value)) {
      return InsertIntoLeaf(key, value, transaction);
    }
    else {
      return true;
    }
  }
  else {
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (root_page_id_ != INVALID_PAGE_ID) {
    // Another thread already started a new tree.
    return false;
  }
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  CHECK(page_id > 0) << "Expected page id > 0";
  CHECK(page->GetPageId() == page_id);
  LOG(DEBUG) << "Starting a new tree on #page: " << page_id;
  LeafPage *root = reinterpret_cast<LeafPage *>(page->GetData());
  CHECK(root);
  root->SetMaxSize(leaf_max_size_);
  root->SetPageType(IndexPageType::LEAF_PAGE);
  root->SetPageId(page_id);
  root->SetNextPageId(INVALID_PAGE_ID);
  // NOTE: mark this node as the root
  root->SetParentPageId(INVALID_PAGE_ID);
  CHECK(root->Insert(key, value, comparator_) == 1);
  root_page_id_ = page_id;
  UpdateRootPageId(page_id);
  buffer_pool_manager_->UnpinPage(page_id, true);
  LOG(DEBUG) << "root_page_id changed to: " << root_page_id_;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllLatch(Transaction *transaction, bool is_write) {
  // Release all latches in reverse order
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    Page *page = page_set->back();
    page_set->pop_back();
    BPlusTreePage *curr = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (curr->IsLeafPage() || is_write) {
      LOG(DEBUG) << "Releasing write latch " << page->GetPageId();
      page->WUnlatch();
      LOG(DEBUG) << "Released write latch " << page->GetPageId();
    }
    else {
      LOG(DEBUG) << "Releasing read latch " << page->GetPageId();
      page->RUnlatch();
      LOG(DEBUG) << "Released read latch " << page->GetPageId();
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), /*is_dirty*/curr->IsLeafPage());
  }
  auto delete_page_set = transaction->GetDeletedPageSet();
  for (page_id_t page : *delete_page_set) {
    buffer_pool_manager_->DeletePage(page);
  }
  transaction->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::AcquireReadLatch(const KeyType &key, Transaction *transaction) {
  LOG(DEBUG) << "Acquire read latch from root for: " << key;
  CHECK(root_page_id_ != INVALID_PAGE_ID) << "Expected root_page_id exists.";
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *curr = reinterpret_cast<BPlusTreePage *>(curr_page->GetData());
  Page *parent_page = nullptr;
  while (1) {
    if (curr->IsLeafPage()) {
      LOG(DEBUG) << "Acquire write latch for page: " << curr->GetPageId();
      curr_page->WLatch();
    }
    else {
      LOG(DEBUG) << "Acquire read latch for page: " << curr->GetPageId();
      curr_page->RLatch();
    }
    if (parent_page) {
      LOG(DEBUG) << "Releasing read latch " << parent_page->GetPageId();
      parent_page->RUnlatch();
      LOG(DEBUG) << "Released read latch " << parent_page->GetPageId();
      transaction->RemoveLastFromPageSet();
    }
    transaction->AddIntoPageSet(curr_page);
    if (curr->IsLeafPage()) {
      break;
    }
    parent_page = curr_page;
    InternalPage *inner = reinterpret_cast<InternalPage *>(curr);
    page_id_t child = inner->Lookup(key, comparator_);
    curr_page = buffer_pool_manager_->FetchPage(child);
    curr = reinterpret_cast<BPlusTreePage *>(curr_page->GetData());
    CHECK(curr) << "Expected curr not a nullptr";
  }
  return curr;
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::AcquireWriteLatch(const KeyType &key, Transaction *transaction) {
  CHECK(root_page_id_ != INVALID_PAGE_ID) << "Expected root_page_id exists.";
  LOG(DEBUG) << "Acquire write latch from root for key: " << key;
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *curr = reinterpret_cast<BPlusTreePage *>(curr_page->GetData());
  while (1) {
    LOG(DEBUG) << "Acquire write latch for page: " << curr->GetPageId();
    curr_page->WLatch();
    transaction->AddIntoPageSet(curr_page);
    if (curr->IsLeafPage()) {
      break;
    }
    InternalPage *inner = reinterpret_cast<InternalPage *>(curr);
    page_id_t child = inner->Lookup(key, comparator_);
    curr_page = buffer_pool_manager_->FetchPage(child);
    curr = reinterpret_cast<BPlusTreePage *>(curr_page->GetData());
    CHECK(curr) << "Expected curr not a nullptr";
  }
  return curr;
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LOG(DEBUG) << "Insert key: " << key << " into " << root_page_id_;
  BPlusTreePage *curr = AcquireReadLatch(key, transaction);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(curr);
  CHECK(leaf->IsLeafPage()) << "Expected current page to ba a leaf.";
  LOG(DEBUG) << "Standing at leaf node " << leaf->GetPageId();

  if (leaf->Lookup(key, nullptr, comparator_)) {
    // Trying to insert a duplicate key
    LOG(DEBUG) << "Find a existing key, returns.";
    ReleaseAllLatch(transaction, /*is_write*/ false);
    return false;
  }
  else {
    if (leaf->GetSize() + 1 > leaf->GetMaxSize()) {
      // NOTE: Overflow occured, release all read latches, and acquire wirte latch from root
      LOG(DEBUG) << "Overflow: release all read lateches...";
      ReleaseAllLatch(transaction, /*is_write*/ false);

      LOG(DEBUG) << "Overflow: acquire write lateches...";
      curr = AcquireWriteLatch(key, transaction);
      leaf = reinterpret_cast<LeafPage *>(curr);
      if (leaf->GetSize() + 1 > leaf->GetMaxSize()) {
        leaf->Insert(key, value, comparator_);
        LeafPage *new_leaf = Split(leaf);
        new_leaf->SetPageType(IndexPageType::LEAF_PAGE);
        new_leaf->SetMaxSize(leaf_max_size_);
        LOG(DEBUG) << "Overflow: starting to split #page " << leaf->GetPageId() << " to #new page "
                  << new_leaf->GetPageId() << " insert " << new_leaf->KeyAt(0);
        InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
        ReleaseAllLatch(transaction, /*is_write*/ true);
      }
      else {
        // Another thread modified this node while we trying to get write latches.
        leaf->Insert(key, value, comparator_);
        ReleaseAllLatch(transaction, /*is_write*/ true);
      }
    }
    else {
      leaf->Insert(key, value, comparator_);
      ReleaseAllLatch(transaction, /*is_write*/ false);
    }
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  N *new_node = reinterpret_cast<N *>(new_page);
  new_node->SetPageId(new_page_id);
  new_node->SetParentPageId(node->GetParentPageId());
  node->MoveHalfTo(new_node);
  node->DebugOutput();
  new_node->DebugOutput();
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  LOG(DEBUG) << "Start to insert into parent.";
  if (old_node->IsRootPage()) {
    LOG(DEBUG) << "Old node: " << old_node->GetPageId() << " is not root";
    page_id_t page_id;
    Page *page = buffer_pool_manager_->NewPage(&page_id);
    LOG(DEBUG) << "Overflow all the way up to root #page " << page_id;

    std::lock_guard<std::mutex> guard(mutex_);
    root_page_id_ = page_id;
    UpdateRootPageId(root_page_id_);
    InternalPage *root = reinterpret_cast<InternalPage *>(page->GetData());
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root->SetPageType(IndexPageType::INTERNAL_PAGE);
    root->SetMaxSize(internal_max_size_);
    root->SetPageId(root_page_id_);
    root->SetParentPageId(INVALID_PAGE_ID);
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    transaction->AddIntoPageSet(page);
    // root->DebugOutput();
  }
  else {
    LOG(DEBUG) << "Old node is not root: " << old_node->GetPageId();
    page_id_t parent_id = old_node->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(parent_id);
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(page->GetData());
    parent_node->Insert(key, new_node->GetPageId(), comparator_);
    LOG(DEBUG) << "Insert into #parent page " << parent_id;
    if (parent_node->GetSize() > parent_node->GetMaxSize()) {
      InternalPage *split_node = Split(parent_node);
      for (int i = 0; i < split_node->GetSize(); i++) {
        // Update the parent for the splited node.
        Page *child_page = buffer_pool_manager_->FetchPage(split_node->ValueAt(i));
        BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child->SetParentPageId(split_node->GetPageId());
        buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
      }
      split_node->SetPageType(IndexPageType::INTERNAL_PAGE);
      split_node->SetMaxSize(internal_max_size_);
      LOG(DEBUG) << "Starting to split parent " << parent_node->GetPageId() << " to new "
                 << split_node->GetParentPageId() << " with key: " << split_node->KeyAt(0);
      InsertIntoParent(parent_node, split_node->KeyAt(0), split_node, transaction);
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (root_page_id_ == INVALID_PAGE_ID) {
      return;
    }
  }

  LOG(DEBUG) << "Removing key from b+ tree: " << key;
  BPlusTreePage *curr = AcquireReadLatch(key, transaction);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(curr);
  LOG(DEBUG) << "Standing at leaf node: " << leaf->GetPageId();

  if (!leaf->Lookup(key, /*value*/ nullptr, comparator_)) {
    ReleaseAllLatch(transaction, /*is_write*/ false);
    return;
  }

  CHECK(leaf->GetSize() >= 1) << "Expected leaf node size greater than 1";

  if (leaf->IsRootPage()) {
    leaf->RemoveAndDeleteRecord(key, comparator_);
    if (leaf->GetSize() == 0) {
      LOG(DEBUG) << "B+ tree became empty.";
      std::lock_guard<std::mutex> guard(mutex_);
      transaction->AddIntoDeletedPageSet(leaf->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
    }
    ReleaseAllLatch(transaction, /*is_write*/ false);
  }
  else if (leaf->GetSize() - 1 < leaf->GetMinSize()) {
    LOG(DEBUG) << "Overflow: release all read lateches...";
    ReleaseAllLatch(transaction, /*is_write*/ false);

    LOG(DEBUG) << "Overflow: acquire write lateches...";
    curr = AcquireWriteLatch(key, transaction);
    leaf = reinterpret_cast<LeafPage *>(curr);

    if (leaf->GetSize() - 1 < leaf->GetMinSize()) {
      leaf->RemoveAndDeleteRecord(key, comparator_);
      CoalesceOrRedistribute(leaf, transaction);
      ReleaseAllLatch(transaction, /*is_write*/ true);
    }
    else {
      leaf->RemoveAndDeleteRecord(key, comparator_);
      page_id_t parent_id = leaf->GetParentPageId();
      Page * page = buffer_pool_manager_->FetchPage(parent_id);
      InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());
      int index = parent->ValueIndex(leaf->GetPageId());
      parent->SetKeyAt(index, leaf->KeyAt(0));
      ReleaseAllLatch(transaction, /*is_write*/ true);
    }
  }
  else {
    leaf->RemoveAndDeleteRecord(key, comparator_);
    page_id_t parent_id = leaf->GetParentPageId();
    Page * page = buffer_pool_manager_->FetchPage(parent_id);
    InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());
    int index = parent->ValueIndex(leaf->GetPageId());
    parent->SetKeyAt(index, leaf->KeyAt(0));
    ReleaseAllLatch(transaction, /*is_write*/ false);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  LOG(DEBUG) << "Merge or redistribute node: " << node->GetPageId();
  CHECK(node && node->GetSize() < node->GetMinSize());

  page_id_t parent_id = node->GetParentPageId();
  InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
  N *left = nullptr;
  N *right = nullptr;
  int node_index = parent->ValueIndex(node->GetPageId());
  LOG(DEBUG) << "Node index at parent: " << node_index << " id " << parent->GetPageId()
             << " parent size: " << parent->GetSize();
  if (node_index > 0) {
    page_id_t left_id = parent->ValueAt(node_index - 1);
    left = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(left_id)->GetData());
    LOG(DEBUG) << "left_id: " << left_id << " " << left;
  }
  if (node_index + 1 < parent->GetSize()) {
    page_id_t right_id = parent->ValueAt(node_index + 1);
    right = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(right_id)->GetData());
    LOG(DEBUG) << "right_id: " << right_id << " " << right;
  }
  CHECK(left || right) << "Expected either left or right should exist.";

  if (left && left->GetSize() > left->GetMinSize()) {
    // Left node has more than half of the children, borrow one from it
    KeyType middle_key = parent->KeyAt(node_index);
    LOG(DEBUG) << "Moving last to front from: " << left->GetPageId() << " to " << node->GetPageId();
    left->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
    parent->SetKeyAt(node_index, node->KeyAt(0));
    parent->SetKeyAt(node_index - 1, left->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return true;
  }
  else if (right && right->GetSize() > right->GetMinSize()) {
    KeyType middle_key = parent->KeyAt(node_index + 1);
    LOG(DEBUG) << "Moving first to end from: " << right->GetPageId() << " to " << node->GetPageId();
    right->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
    parent->SetKeyAt(node_index, node->KeyAt(0));
    parent->SetKeyAt(node_index + 1, right->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    return true;
  }
  else if (left) {
    // NOTE: in order to keep the list chain on the leaf nodes, we have to
    // notice the merge order here.
    KeyType middle_key = parent->KeyAt(node_index);
    LOG(DEBUG) << "Left merge node: " << node->GetPageId() << " to " << left->GetPageId()
               << " middle_key: " << middle_key << " removing parent index: " << node_index;
    node->MoveAllTo(left, middle_key, buffer_pool_manager_);
    CHECK(node_index >= 1);
    parent->SetKeyAt(node_index - 1, left->KeyAt(0));
    parent->Remove(node_index);
    buffer_pool_manager_->DeletePage(node->GetPageId());
  }
  else if (right) {
    CHECK(node_index + 1 < parent->GetSize());
    KeyType middle_key = parent->KeyAt(node_index + 1);
    LOG(DEBUG) << "Right merge node: " << right->GetPageId() << " and " << node->GetPageId()
               << " middle_key: " << middle_key << " removing parent index: " << node_index;
    right->MoveAllTo(node, middle_key, buffer_pool_manager_);
    parent->SetKeyAt(node_index, node->KeyAt(0));
    parent->Remove(node_index + 1);
    buffer_pool_manager_->DeletePage(right->GetPageId());
  }
  else {
    CHECK(false) << "Should not reach here.";
  }

  if (!parent->IsRootPage()) {
    if (parent->GetSize() < parent->GetMinSize()) {
      CoalesceOrRedistribute(parent, transaction);
    }
    else {
      // Do nothing
    }
  }
  else if (parent->GetSize() == 1) {
    // NOTE: for internal node, size less or equals 1 means empty.
    // Delete old root page, the height of this tree decreased by 1.
    std::lock_guard<std::mutex> guard(mutex_);

    page_id_t new_root_id = parent->ValueAt(0);
    LOG(DEBUG) << "B+ tree height decreases by 1, from page " << root_page_id_ << " to " << new_root_id;
    transaction->AddIntoDeletedPageSet(root_page_id_);

    // Update the new root page
    Page * page = buffer_pool_manager_->FetchPage(new_root_id);
    BPlusTreePage *new_root = reinterpret_cast<BPlusTreePage *>(page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_id, true);

    root_page_id_ = new_root_id;
    UpdateRootPageId(root_page_id_);
  }
  else if (parent->GetSize() == 0) {
    std::lock_guard<std::mutex> guard(mutex_);
    root_page_id_ = INVALID_PAGE_ID;
  }
  else {
    // Do nothing
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, InternalPage *parent, int index, Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  BPlusTreePage *curr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!curr->IsLeafPage()) {
    InternalPage *inner = reinterpret_cast<InternalPage *>(curr);
    // Always go to the leafmost
    page_id_t child = inner->ValueAt(0);
    buffer_pool_manager_->UnpinPage(inner->GetPageId(), false);
    curr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child)->GetData());
  }
  LeafPage *leaf = reinterpret_cast<LeafPage *>(curr);
  return INDEXITERATOR_TYPE(leaf, buffer_pool_manager_, /*pos*/ 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  BPlusTreePage *curr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!curr->IsLeafPage()) {
    InternalPage *inner = reinterpret_cast<InternalPage *>(curr);
    page_id_t child = inner->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(inner->GetPageId(), false);
    curr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child)->GetData());
  }
  LeafPage *leaf = reinterpret_cast<LeafPage *>(curr);
  int pos = leaf->KeyIndex(key, comparator_);
  CHECK(pos != -1);
  return INDEXITERATOR_TYPE(leaf, buffer_pool_manager_, pos);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  }
  else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  // int64_t key;
  // std::ifstream input(file_name);
  // while (input) {
  //   input >> key;

  //   KeyType index_key;
  //   index_key.SetFromInteger(key);
  //   RID rid(key);
  //   Insert(index_key, rid, transaction);
  // }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  // int64_t key;
  // std::ifstream input(file_name);
  // while (input) {
  //   input >> key;
  //   KeyType index_key;
  //   index_key.SetFromInteger(key);
  //   Remove(index_key, transaction);
  // }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    LOG(DEBUG) << "Drawing leaf #page " << leaf->GetPageId();
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    CHECK(leaf->GetSize()) << "Expected have data to draw.";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  }
  else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    LOG(DEBUG) << "Drawing inner #page " << inner->GetPageId();
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    CHECK(inner->GetSize()) << "Expected have inner data to draw.";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      // if (i > 0) {
      out << inner->KeyAt(i);
      // } else {
      //   out << " ";
      // }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
  else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
template class BPlusTree<int, int, IntegerComparator<true>>;
template class BPlusTree<int, int, IntegerComparator<false>>;

}  // namespace bustub
