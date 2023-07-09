//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <utility>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Deleted to disallow initialization
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * Writes the necessary header information to a newly created page, must be called after
   * the creation of a new page to make a valid BPlusTreeInternalPage
   * @param max_size Maximal size of the page
   */
  void Init(int max_size = INTERNAL_PAGE_SIZE);

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  auto KeyAt(int index) const -> KeyType;

  /**
   *
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  void SetKeyAt(int index, const KeyType &key);

  /**
   *
   * @param value the value to search for
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   *
   * @param index the index
   * @return the value at the index
   */
  auto ValueAt(int index) const -> ValueType;

  /**
   * @brief Set the Value At object
   *
   * @param index
   * @param value
   */
  void SetValueAt(int index, const ValueType &value);

  /**
   * @brief get position of the search key, so that it's right page >= the key and it's left page < the key.
   *
   * @param comparator
   * @param key
   * @return int
   */
  auto GetSearchIndex(const KeyComparator &comparator, const KeyType &key) const -> int;

  /**
   * @brief Remove the right child of the search index, because the way we merge internal pages.
   *
   * @param search_index
   * @return page_id_t
   */
  void Remove(int search_index);

  /**
   * @brief Get the child page.
   *
   * @param comparator
   * @param key
   * @return page_id_t
   */
  auto GetChild(const KeyComparator &comparator, const KeyType &key) const -> page_id_t;

  /**
   * @brief put the first ⌈n∕2⌉ in the newly created node and the remaining values in the existing node.
   *
   * @param comparator
   * @param page
   * @return KeyTy: peinserte into parent's page. And because the key is from right page, so >= the key, should go
   * right.
   */
  auto Split(const KeyComparator &comparator, B_PLUS_TREE_INTERNAL_PAGE_TYPE *page) -> KeyType;

  auto Insert(const KeyComparator &comparator, const KeyType &key, const ValueType &value) -> bool;

  /**
   * @brief merge the nodes by moving the entries from both the nodes into the left sibling and deleting the now-empty
   * right sibling.
   *
   * @param comparator
   * @param page
   * @param key
   */
  void Merge(const KeyComparator &comparator, B_PLUS_TREE_INTERNAL_PAGE_TYPE *page, KeyType key);

  /**
   * @brief redistributes entries by borrowing a single entry from an adjacent node.
   *
   * @param comparator
   * @param page
   * @param key the key of parent that need to be moved down.
   * @return KeyType, the key that need to be moved up and insert into parent.
   */
  auto Redistribute(const KeyComparator &comparator, B_PLUS_TREE_INTERNAL_PAGE_TYPE *page, KeyType key) -> KeyType;

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // first key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  void InsertAt(int position, const KeyType &key, const ValueType &value) {
    for (int i = GetSize(); i > position; i--) {
      array_[i] = array_[i - 1];
    }
    array_[position].first = key;
    array_[position].second = value;
    IncreaseSize(1);
  }

  void DeleteKeytAt(int position) {
    for (int i = position; i < GetSize() - 1; i++) {
      array_[i] = array_[i + 1];
    }
    array_[GetSize() - 1] = std::make_pair(KeyType{}, ValueType{});
    IncreaseSize(-1);
  }
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
