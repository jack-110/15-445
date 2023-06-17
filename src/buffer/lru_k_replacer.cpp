//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // locate the frame
  frame_id_t evict_id = -1;
  auto max = std::numeric_limits<size_t>::min();
  for (auto iterator = node_store_.begin(); iterator != node_store_.end(); ++iterator) {
    LRUKNode &node = iterator->second;
    if (!node.IsEvictable()) {
      continue;
    }

    auto distance = node.GetDistance(current_timestamp_);
    if (distance > max) {
      max = distance;
      evict_id = iterator->first;
    } else if (distance == max) {
      if (node.GetEariestAccessTime() < node_store_.at(evict_id).GetEariestAccessTime()) {
        evict_id = iterator->first;
      }
    }
  }

  // evict the frame
  if (evict_id != -1) {
    curr_size_--;
    *frame_id = evict_id;
    node_store_.erase(evict_id);
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (replacer_size_ < static_cast<size_t>(frame_id)) {
    throw std::runtime_error("Exceed the size of replacer");
  }

  auto iterator = node_store_.find(frame_id);
  if (iterator != node_store_.end()) {
    auto &node = iterator->second;
    node.UpdateHistory(++current_timestamp_);
  } else {
    LRUKNode node(k_, frame_id);
    node.UpdateHistory(++current_timestamp_);
    node_store_.emplace(frame_id, node);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (replacer_size_ < static_cast<size_t>(frame_id)) {
    throw std::runtime_error("Exceed the size of replacer");
  }

  auto iterator = node_store_.find(frame_id);
  if (iterator == node_store_.end()) {
    throw std::runtime_error("The frame " + std::to_string(frame_id) + " does not exist");
  }

  auto &node = iterator->second;
  if (node.IsEvictable() && !set_evictable) {
    // node change from evictable to non-evictable
    curr_size_--;
  } else if (set_evictable && !node.IsEvictable()) {
    // node change from non-evictable to evictable
    curr_size_++;
  }
  node.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto iterator = node_store_.find(frame_id);
  if (iterator == node_store_.end()) {
    return;
  }

  auto node = iterator->second;
  if (!node.IsEvictable()) {
    throw std::runtime_error("The frame is non-evictable");
  }

  curr_size_--;
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
