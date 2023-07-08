#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }

  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that) {
    return *this;
  }

  Drop();

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }

  Drop();

  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;

  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
    guard_.page_->RUnlatch();
  }

  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }

  Drop();

  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;

  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
    guard_.page_->WUnlatch();
  }

  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
