//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  IndexIterator() = default;

  IndexIterator(LeafPage *leaf, BufferPoolManager *buffer_pool_manager)
      : leaf_(leaf), buffer_pool_manager_(buffer_pool_manager), pos_(0) {}

  ~IndexIterator() = default;

  bool IsEnd();

  const LeafPage *GetLeafPage() const { return leaf_; }
  int GetPos() const { return pos_; }
  const BufferPoolManager *GetBufferPoolManager() const { return buffer_pool_manager_; }

  const MappingType &operator*();
  const MappingType *operator->();

  // Prefix increment
  const IndexIterator &operator++();
  // Postfix increment
  IndexIterator operator++(int);

  bool operator==(const IndexIterator &itr) const {
    return leaf_ == itr.GetLeafPage() && pos_ == itr.GetPos() && buffer_pool_manager_ == GetBufferPoolManager();
  }

  bool operator!=(const IndexIterator &itr) const { return !(*this == itr); }

 private:
  LeafPage *leaf_{nullptr};
  BufferPoolManager *buffer_pool_manager_{nullptr};
  int pos_{0};
};

}  // namespace bustub
