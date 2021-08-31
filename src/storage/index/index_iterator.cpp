/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/logger.h"
#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::IsEnd() {
  if (leaf_->GetNextPageId() == INVALID_PAGE_ID) {
    return pos_ >= leaf_->GetSize();
  } else {
    return false;
  }
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() const {
  CHECK(pos_ < leaf_->GetSize());
  return leaf_->GetItem(pos_);
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType *INDEXITERATOR_TYPE::operator->() const {
  CHECK(pos_ < leaf_->GetSize());
  return &(leaf_->GetItem(pos_));
}

INDEX_TEMPLATE_ARGUMENTS
const INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  CHECK(!IsEnd());
  pos_++;
  if (pos_ >= leaf_->GetSize()) {
    page_id_t next_page = leaf_->GetNextPageId();
    if (next_page == INVALID_PAGE_ID) {
      leaf_ = nullptr;
      buffer_pool_manager_ = nullptr;
      pos_ = 0;
    } else {
      leaf_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_page)->GetData());
      pos_ = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE INDEXITERATOR_TYPE::operator++(int) {
  INDEXITERATOR_TYPE tmp = *this;
  ++(*this);
  return tmp;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;
template class IndexIterator<int, int, IntegerComparator<true>>;
template class IndexIterator<int, int, IntegerComparator<false>>;

}  // namespace bustub
