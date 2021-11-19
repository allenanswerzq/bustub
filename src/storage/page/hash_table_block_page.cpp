//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"
#include "common/logger.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  CHECK(bucket_ind < BLOCK_ARRAY_SIZE);
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  CHECK(bucket_ind < BLOCK_ARRAY_SIZE);
  return array_[bucket_ind].second;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  // char expected = 0;
  // char desired = 1;
  // if (!occupied_[bucket_ind].compare_exchange_strong(expected, desired)) {
  //   // Returns if this bucket already been occupied.
  //   return false;
  // }
  for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
    CHECK(!(!occupied_[i] && readable_[i]));
  }
  occupied_[bucket_ind] = 1;
  array_[bucket_ind] = std::make_pair(key, value);
  readable_[bucket_ind] = 1;
  for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
    CHECK(!(!occupied_[i] && readable_[i]));
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
    CHECK(!(!occupied_[i] && readable_[i]));
  }
  readable_[bucket_ind] = 0;
  CHECK(occupied_[bucket_ind] && !readable_[bucket_ind]);
  for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
    CHECK(!(!occupied_[i] && readable_[i]));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  // for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
  //   CHECK(!(!occupied_[i] && readable_[i]));
  // }
  return occupied_[bucket_ind];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  // for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
  //   CHECK(!(!occupied_[i] && readable_[i]));
  // }
  return readable_[bucket_ind];
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
