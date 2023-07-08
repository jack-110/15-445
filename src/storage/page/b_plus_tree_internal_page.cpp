//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetSize(0);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  auto key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  auto value = array_[index].second;
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int search_index) {
  array_[search_index] = std::make_pair(KeyType{}, ValueType{});
  for (int i = search_index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  array_[GetSize() - 1] = std::make_pair(KeyType{}, ValueType{});
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetSearchIndex(const KeyComparator &comparator, const KeyType &key) const -> int {
  auto size = GetSize();
  auto cmp = [comparator](const MappingType p1, const MappingType p2) -> bool {
    return comparator(p1.first, p2.first) < 0;
  };
  auto index = std::upper_bound(array_ + 1, array_ + size, std::make_pair(key, ValueType{}), cmp) - array_ - 1;
  if (index == 0) {
    return 1;
  }
  return index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetChild(const KeyComparator &comparator, const KeyType &key) const -> page_id_t {
  auto size = GetSize();
  BUSTUB_ENSURE(size > 0, "The size of internal should be > 0.");
  auto cmp = [comparator](const MappingType p1, const MappingType p2) -> bool {
    return comparator(p1.first, p2.first) < 0;
  };
  auto index = std::upper_bound(array_ + 1, array_ + size, std::make_pair(key, ValueType{}), cmp) - array_;
  BUSTUB_ENSURE(comparator(key, array_[index - 1].first) >= 0,
                "The search key should be less than or equal with the key.");
  return array_[index - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyComparator &comparator, const KeyType &key, const ValueType &value)
    -> bool {
  auto size = GetSize();
  BUSTUB_ASSERT(size <= GetMaxSize(), "The size of inner node should be equal with or less than max.");

  auto cmp = [comparator](const MappingType p1, const MappingType p2) -> bool {
    return comparator(p1.first, p2.first) < 0;
  };
  auto index = std::upper_bound(array_, array_ + size, std::make_pair(key, value), cmp) - array_;
  for (int i = size; i > index; --i) {
    array_[i] = array_[i - 1];
  }

  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(const KeyComparator &comparator, B_PLUS_TREE_INTERNAL_PAGE_TYPE *page,
                                           KeyType key) {
  if (GetSize() != 0) {
    page->SetKeyAt(0, key);
  }
  auto right_size = page->GetSize();
  for (int i = 0; i < right_size; i++) {
    Insert(comparator, page->KeyAt(i), page->ValueAt(i));
    page->SetKeyAt(i, KeyType{});
    page->SetValueAt(i, ValueType{});
    page->IncreaseSize(-1);
  }
  BUSTUB_ENSURE(page->GetSize() == 0, "The size of internal page should be 0 after merging.");
  BUSTUB_ENSURE((GetSize() <= GetMaxSize()), "The size of merged page should be less than or equal with max.");
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Redistribute(const KeyComparator &comparator, B_PLUS_TREE_INTERNAL_PAGE_TYPE *page,
                                                  KeyType key) -> KeyType {
  BUSTUB_ENSURE(page->GetSize() < GetMinSize() || GetSize() < GetMinSize(), "internal redistribute wrong");
  if (GetSize() > page->GetSize()) {
    // borrow from the right most from left page
    auto up_key = array_[GetSize() - 1].first;
    page->SetKeyAt(0, key);
    page->InsertAt(0, KeyType{}, array_[GetSize() - 1].second);
    // remove
    DeleteKeytAt(GetSize() - 1);
    return up_key;
  }

  // borrow form the left most from right page
  auto up_key = page->KeyAt(1);
  Insert(comparator, key, page->ValueAt(0));
  page->SetValueAt(0, page->ValueAt(1));
  page->DeleteKeytAt(1);
  return up_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(const KeyComparator &comparator, B_PLUS_TREE_INTERNAL_PAGE_TYPE *page)
    -> KeyType {
  BUSTUB_ENSURE(GetSize() == GetMaxSize(), "The size of internal page should be equal with max.");
  auto size = GetSize();
  auto tmp_key = page->KeyAt(0);
  auto tmp_value = page->ValueAt(0);
  page->IncreaseSize(-1);
  BUSTUB_ENSURE(page->GetSize() == 0, "The size of new should be 0.");

  bool inserted = false;
  auto num = static_cast<int>(ceil((size + 1) / 2.0));
  for (int i = 1; i <= num; i++) {
    auto index = GetSize() - 1;
    if (comparator(tmp_key, array_[index].first) > 0 && !inserted) {
      inserted = true;
      page->Insert(comparator, tmp_key, tmp_value);
    } else {
      page->Insert(comparator, array_[index].first, array_[index].second);
      DeleteKeytAt(index);
    }
  }

  if (!inserted) {
    Insert(comparator, tmp_key, tmp_value);
  }

  auto up_key = page->KeyAt(0);
  page->SetKeyAt(0, KeyType{});
  return up_key;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
