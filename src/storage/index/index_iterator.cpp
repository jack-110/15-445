/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() { is_end_ = true; }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard guard, int index) {
  bpm_ = bpm;
  index_ = index;
  page_id_ = guard.PageId();
  guard_ = std::move(guard);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (is_end_) {
    throw std::runtime_error("Invalid iterator: iterator is at the end.");
  }
  auto leaf_page = guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  return leaf_page->GetMappingAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    throw std::runtime_error("Invalid iterator: iterator is at the end.");
  }
  auto leaf_page = guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (index_ + 1 == leaf_page->GetSize()) {
    auto next_page_id = leaf_page->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      is_end_ = true;
      return *this;
    }
    index_ = 0;
    page_id_ = next_page_id;
    guard_ = bpm_->FetchPageRead(next_page_id);
    return *this;
  }
  index_++;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
