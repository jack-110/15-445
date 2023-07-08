//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * |  NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  void Init(int max_size = LEAF_PAGE_SIZE);

  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);

  auto Remove(const KeyComparator &comparator, const KeyType &key) -> bool;
  auto Insert(const KeyComparator &comparator, const KeyType &key, const ValueType &value) -> bool;
  auto GetValue(const KeyComparator &comparator, const KeyType &key, std::vector<ValueType> *result) const -> bool;

  /**
   * @brief merge the nodes by moving the entries from both the nodes into the left sibling and deleting the now-empty
   * right sibling.
   *
   * @param comparator
   * @param page
   */
  void Merge(const KeyComparator &comparator, B_PLUS_TREE_LEAF_PAGE_TYPE *page);

  /**
   * @brief put the first ⌈n∕2⌉ in the newly created node and the remaining values in the existing node.
   *
   * @param comparator
   * @param page
   * @return KeyType: inserte into parent's page. And because the key is from right page, so >= the key, should go
   * right.
   */
  auto Split(const KeyComparator &comparator, B_PLUS_TREE_LEAF_PAGE_TYPE *page) -> KeyType;

  /**
   * @brief redistributes entries by borrowing a single entry from an adjacent node.
   *
   * @param comparator
   * @param page
   * @param key the key of parent that need to be moved down.
   * @return KeyType, the key that need to be moved up and insert into parent.
   */
  auto Redistribute(const KeyComparator &comparator, B_PLUS_TREE_LEAF_PAGE_TYPE *page) -> KeyType;

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
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
  void DeleteKeytAt(int position) {
    for (int i = position; i < GetSize() - 1; i++) {
      array_[i] = array_[i + 1];
    }
    array_[GetSize() - 1] = std::make_pair(KeyType{}, ValueType{});
    IncreaseSize(-1);
  }
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
