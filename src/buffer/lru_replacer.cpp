//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Exist(frame_id_t id) { return mp_.find(id) != mp_.end(); }

void LRUReplacer::Top(frame_id_t id) {
  if (!Exist(id)) {
    li_.insert(li_.begin(), id);
    mp_[id] = li_.begin();
  } else {
    auto it = mp_[id];
    // Remove from the old place
    li_.erase(it);
    // Move to the fisrt of the list
    li_.insert(li_.begin(), id);
    // Remove from the old map place
    mp_.erase(id);
    mp_[id] = li_.begin();
  }
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (Size() == 0) {
    // Returns false if lru_replacer is empty
    return false;
  }
  CHECK(frame_id) << "Expected output frame_id is not nullptr.";
  *frame_id = *li_.rbegin();
  li_.erase(--li_.end());
  mp_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!Exist(frame_id)) {
    // NOTE: do nothing if this frame_id not exists.
    return;
  }
  auto it = mp_[frame_id];
  li_.erase(it);
  mp_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (Exist(frame_id)) {
    // TODO(zhangqiang): really do nothing here?
    return;
  }
  CHECK(Size() < num_pages_) << "Expected replacer is not full.";
  if (Size() >= num_pages_) {
    frame_id_t victim_id;
    CHECK(Victim(&victim_id)) << "Can not get a victim id.";
    Pin(victim_id);
    Top(victim_id);
  } else {
    Top(frame_id);
  }
}

size_t LRUReplacer::Size() { return mp_.size(); }

}  // namespace bustub
