//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  int num_free_pages = 0;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) num_free_pages++;
  }

  if (!num_free_pages) return nullptr;

  *page_id = AllocatePage();
  frame_id_t frame_id = -1;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    Page *evicted_page = &pages_[frame_id];
    page_id_t evicted_page_id = evicted_page->GetPageId();

    if (evicted_page->IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, evicted_page->GetData());
      evicted_page->is_dirty_ = false;
    }

    evicted_page->ResetMemory();
    page_table_.erase(evicted_page_id);
  }

  page_table_.emplace(*page_id, frame_id);
  Page *page = &pages_[frame_id];
  page->page_id_ = *page_id;
  page->pin_count_ = 1;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  int num_free_pages = 0;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) num_free_pages++;
  }

  if (!num_free_pages) return nullptr;

  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    Page *evicted_page = &pages_[frame_id];
    page_id_t evicted_page_id = evicted_page->GetPageId();

    if (evicted_page->IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, evicted_page->GetData());
      evicted_page->is_dirty_ = false;
    }

    evicted_page->ResetMemory();

    page_table_.erase(evicted_page_id);
  }

  page_table_.emplace(page_id, frame_id);
  Page *page = &pages_[frame_id];
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  disk_manager_->ReadPage(page_id, page->GetData());

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) return false;
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() <= 0) return false;
  if (is_dirty) page->is_dirty_ = is_dirty;
  page->pin_count_--;
  if (page->pin_count_ == 0) replacer_->SetEvictable(frame_id, true);
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == -1) return false;
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) return false;
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) return true;
  frame_id_t frame_id = it->second;
  if (pages_[frame_id].GetPinCount() > 0) return false;
  replacer_->Remove(frame_id);

  Page *page = &pages_[frame_id];
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub