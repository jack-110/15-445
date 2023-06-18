//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  int frame_id;
  if (PickFreePage(frame_id)) {
    *page_id = AllocatePage();
    InitPage(*page_id, frame_id);
    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) != 0) {
    auto frame_id = page_table_[page_id];
    UpdatePage(page_id, frame_id);
    return &pages_[frame_id];
  }

  frame_id_t frame_id;
  if (PickFreePage(frame_id)) {
    InitPage(page_id, frame_id);
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0 || pages_[page_table_.at(page_id)].pin_count_ == 0) {
    return false;
  }

  auto frame_id = page_table_.at(page_id);
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  // true -> false is not ok if one page have multiple users.
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0U) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (const auto &item : page_table_) {
    FlushPage(item.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }

  if (pages_[frame_id].is_dirty_) {
    FlushPage(page_id);
  }

  // remove frame from replacer and put it back to free list
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);

  // reset page meta
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
