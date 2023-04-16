#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  LOG_INFO("basic page guard move constructor is called");
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    LOG_INFO("unpin page");
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }

  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  LOG_INFO("basic move assignment is called");
  if (this != &that) {
    // drop itself this.page_ first
    Drop();

    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  LOG_INFO("move constructor is called");
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // drop itself first
    if (guard_.page_ != nullptr) {
      guard_.page_->RUnlatch();
    }

    guard_ = std::move(that.guard_);
  }

  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
  }

  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    if (guard_.page_ != nullptr) {
      guard_.page_->WUnlatch();
    }

    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
  }

  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
