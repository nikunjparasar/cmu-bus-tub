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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  preliminary_queue_ = std::unordered_map<frame_id_t, std::list<size_t>>();
  lru_cache_queue_ = std::unordered_map<frame_id_t, std::list<size_t>>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  *frame_id = -1;  // default initialization for pointer param out (the evicted frame)
  clock_++;        // increment time for simulation
  // if no frames can be evicted, as the replacer is empty
  if (preliminary_queue_.empty() && lru_cache_queue_.empty()) return false;

  size_t reference_clock = clock_;
  if (!preliminary_queue_.empty()) {
    for (auto e : preliminary_queue_) {
      bool can_evict = evictable_.find(e.first) != evictable_.end();
      size_t oldest_time = e.second.front();
      if (can_evict && oldest_time < reference_clock) {
        *frame_id = e.first;
        reference_clock = oldest_time;  // computation of backward k-distance maximum of all frames
      }
    }
  }
  if (preliminary_queue_.find(*frame_id) != preliminary_queue_.end()) {
    preliminary_queue_.erase(*frame_id);
    evictable_.erase(*frame_id);
    return true;
  }

  for (auto e : lru_cache_queue_) {
    bool can_evict = evictable_.find(e.first) != evictable_.end();
    size_t oldest_time = e.second.front();
    if (can_evict && oldest_time < reference_clock) {
      *frame_id = e.first;
      reference_clock = oldest_time;
    }
  }
  if (*frame_id != -1) {  // check that frame is valid
    lru_cache_queue_.erase(*frame_id);
    evictable_.erase(*frame_id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (frame_id >= static_cast<int>(replacer_size_) || frame_id == -1) return;

  // check cache_queue first
  if (lru_cache_queue_.find(frame_id) != lru_cache_queue_.end()) {
    access_history_t &access_history = lru_cache_queue_[frame_id];
    access_history.pop_front();
    access_history.push_back(clock_++);
    return;
  }
  // if doesnt exist in prelim queue, initialize a new timestamp history
  if (preliminary_queue_.find(frame_id) == preliminary_queue_.end()) {
    preliminary_queue_[frame_id] = access_history_t();
  }
  access_history_t &access_history = preliminary_queue_[frame_id];
  access_history.push_back(clock_++);
  // move it to cache queue if accessed more than k times
  if (access_history.size() >= k_) {
    lru_cache_queue_[frame_id] = access_history;  // move reference to cache queue
    preliminary_queue_.erase(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  clock_++;
  if (preliminary_queue_.find(frame_id) == preliminary_queue_.end() &&
      lru_cache_queue_.find(frame_id) == lru_cache_queue_.end()) {
    return;
  } else if (set_evictable) {
    evictable_[frame_id] = true;
  } else {
    evictable_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  clock_++;
  preliminary_queue_.erase(frame_id);  // remove all the timestamp history
  lru_cache_queue_.erase(frame_id);
  evictable_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return evictable_.size();
}
}  // namespace bustub
