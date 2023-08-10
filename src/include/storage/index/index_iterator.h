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
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(IndexIterator &&other) noexcept;
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard guard, int index);
  ~IndexIterator();  // NOLINT

  auto IsEnd() const -> bool;

  auto GetIndex() const -> int { return index_; }

  auto GetPageId() const -> page_id_t { return page_id_; }

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (is_end_) {
      return itr.IsEnd();
    }

    if (itr.IsEnd()) {
      return false;
    }

    if (page_id_ != itr.GetPageId()) {
      return false;
    }

    return index_ == itr.GetIndex();
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    if (is_end_) {
      return !itr.IsEnd();
    }

    if (itr.IsEnd()) {
      return true;
    }

    if (page_id_ != itr.GetPageId()) {
      return true;
    }

    return index_ != itr.GetIndex();
  }

 private:
  // add your own private member variables here
  int index_ = 0;
  page_id_t page_id_;
  bool is_end_{false};
  ReadPageGuard guard_;
  BufferPoolManager *bpm_{nullptr};
};

}  // namespace bustub
