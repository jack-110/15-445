//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  auto key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  auto value = array_[index].second;
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetMappingAt(int index) const -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(const KeyComparator &comparator, B_PLUS_TREE_LEAF_PAGE_TYPE *page) {
  auto right_size = page->GetSize();
  for (int i = 0; i < right_size; i++) {
    Insert(comparator, page->KeyAt(i), page->ValueAt(i));
    page->SetKeyAt(i, KeyType{});
    page->SetValueAt(i, ValueType{});
    page->IncreaseSize(-1);
  }
  BUSTUB_ENSURE(page->GetSize() == 0, "The size of page should 0 after mergeing.");
  SetNextPageId(page->GetNextPageId());
  page->SetNextPageId(INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(const KeyComparator &comparator, B_PLUS_TREE_LEAF_PAGE_TYPE *page) -> KeyType {
  BUSTUB_ENSURE(page->GetSize() == 0, "The new page should be empty.");
  BUSTUB_ENSURE(GetSize() == GetMaxSize(), "The size of leaf node should be equal with max.");
  int num = static_cast<int>(ceil(GetSize() / 2.0));
  for (int i = 1; i <= num; i++) {
    auto index = GetSize() - 1;
    page->Insert(comparator, array_[index].first, array_[index].second);
    DeleteKeytAt(index);
  }
  BUSTUB_ENSURE(GetSize() >= GetMinSize(), "The size of existing page should >= min after spliting.");
  BUSTUB_ENSURE(page->GetSize() >= GetSize(), "The size of new page should >= the size of existing page.");
  return page->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Redistribute(const KeyComparator &comparator, B_PLUS_TREE_LEAF_PAGE_TYPE *page)
    -> KeyType {
  BUSTUB_ENSURE(page->GetSize() > GetMinSize() || GetSize() > GetMinSize(), "leaf redistribute wrong");
  if (GetSize() > GetMinSize()) {
    int index = GetSize() - 1;
    page->Insert(comparator, array_[index].first, array_[index].second);
    DeleteKeytAt(index);
  } else {
    Insert(comparator, page->KeyAt(0), page->ValueAt(0));
    page->DeleteKeytAt(0);
  }
  return page->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyComparator &comparator, const KeyType &key, const ValueType &value)
    -> bool {
  BUSTUB_ASSERT(GetMaxSize() >= GetSize(), "The size of leaf page should not be greater than max.");
  for (int i = 0; i < GetSize(); i++) {
    if (i < GetSize() - 1) {
      BUSTUB_ASSERT(comparator(array_[i].first, array_[i + 1].first) < 0, "The order of leaf page is wrong.");
    }

    if (comparator(array_[i].first, key) == 0) {
      return false;
    }
  }

  int size = GetSize();
  auto cmp = [comparator](const MappingType p1, const MappingType p2) -> bool {
    return comparator(p1.first, p2.first) < 0;
  };
  auto index = std::upper_bound(array_, array_ + size, std::make_pair(key, value), cmp) - array_;

  for (int i = size; i > index; i--) {
    array_[i] = array_[i - 1];
  }

  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyComparator &comparator, const KeyType &key) -> bool {
  auto cmp = [comparator](const MappingType p1, const MappingType p2) -> bool {
    return comparator(p1.first, p2.first) < 0;
  };
  int size = GetSize();
  auto index = std::lower_bound(array_, array_ + size, std::make_pair(key, ValueType{}), cmp) - array_;
  if (comparator(array_[index].first, key) != 0) {
    return false;
  }

  for (int i = index; i < size - 1; i++) {
    array_[i] = array_[i + 1];
  }
  array_[size - 1] = std::make_pair(KeyType{}, ValueType{});
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyComparator &comparator, const KeyType &key,
                                          std::vector<ValueType> *result) const -> bool {
  if (GetSize() == 0) {
    return false;
  }
  auto cmp = [comparator](const MappingType p1, const MappingType p2) -> bool {
    return comparator(p1.first, p2.first) < 0;
  };
  auto index = std::lower_bound(array_, array_ + GetSize(), std::make_pair(key, ValueType{}), cmp) - array_;
  if (comparator(array_[index].first, key) != 0) {
    return false;
  }
  result->push_back(array_[index].second);
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
