/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *txn = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *txn);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn = nullptr) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *txn = nullptr);

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  void SplitLeafPage(LeafPage *leaf_page, KeyType &key, page_id_t &value) {
    page_id_t page_id;
    auto guard = bpm_->NewPageGuarded(&page_id);
    auto page = guard.AsMut<LeafPage>();
    page->Init(leaf_max_size_);
    page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(guard.PageId());
    BUSTUB_ENSURE(page->GetSize() == 0, "The size of new page should be 0.");
    value = page_id;
    key = leaf_page->Split(comparator_, page);
  }

  void SplitInternalPage(InternalPage *internal_page, KeyType &key, page_id_t &value) {
    page_id_t page_id;
    auto guard = bpm_->NewPageGuarded(&page_id);
    auto page = guard.AsMut<InternalPage>();
    page->Init(internal_max_size_);
    page->Insert(comparator_, key, value);
    value = page_id;
    key = internal_page->Split(comparator_, page);
  }

  auto CreateTree(const KeyType &key, const ValueType &value) -> bool {
    auto guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
    BUSTUB_ENSURE(header_page->root_page_id_ == INVALID_PAGE_ID, "The tree should be empty when creaate tree.");
    page_id_t page_id;
    auto root_guard = bpm_->NewPageGuarded(&page_id);
    auto root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    if (root_page->Insert(comparator_, key, value)) {
      BUSTUB_ENSURE(root_page->GetSize() == 1, "The size of new root page should be one");
      header_page->root_page_id_ = page_id;
      return true;
    }
    return false;
  }

  void DecreaseTree(Context &ctx, page_id_t page_id) {
    BUSTUB_ENSURE(ctx.header_page_.has_value(), "The header page should be locked when decrease tree.");
    auto guard = std::move(ctx.header_page_.value());
    auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = page_id;
    ctx.root_page_id_ = page_id;
  }

  void IncreaseTree(Context &ctx, const KeyType &key, page_id_t value) {
    BUSTUB_ENSURE(ctx.header_page_.has_value(), "The header page should be locked when increase tree.");
    page_id_t new_root_id;
    auto guard = bpm_->NewPageGuarded(&new_root_id);
    auto page = guard.AsMut<InternalPage>();
    page->Init(internal_max_size_);

    page->Insert(comparator_, KeyType{}, ctx.root_page_id_);
    page->Insert(comparator_, key, value);

    BUSTUB_ENSURE(page->GetSize() >= 2,
                  "The size of root internal page should be greater than or equal with 2 when insert.");

    auto header_guard = std::move(ctx.header_page_.value());
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_id;
  }

  /**
   * @brief traverse the tree with read latch.
   *
   * @param ctx
   * @param key
   * @return const LeafPage*
   */
  void TranverseTreeWithRLatch(Context &ctx, const KeyType &key) const {
    page_id_t page_id = ctx.root_page_id_;
    while (true) {
      auto guard = bpm_->FetchPageRead(page_id);
      auto page = guard.As<BPlusTreePage>();
      if (!ctx.read_set_.empty()) {
        ctx.read_set_.pop_front();
        ctx.header_page_ = std::nullopt;
      }
      BUSTUB_ENSURE(ctx.read_set_.empty(), "The size of read set should be 0 after releasing.");
      if (page->IsLeafPage()) {
        ctx.read_set_.push_back(std::move(guard));
        return;
      }
      auto internal_page = guard.As<InternalPage>();
      BUSTUB_ENSURE(internal_page->GetSize() >= 2, "The size of internal page should be >= 2.");
      ctx.read_set_.push_back(std::move(guard));
      page_id = internal_page->GetChild(comparator_, key);
    }
  }

  /**
   * @brief traverse the tree with write latch.
   *
   * @param ctx
   * @param page_id
   * @param key
   */
  void TranverseTreeWithWLatch(Context &ctx, const KeyType &key, OperationType operation) const {
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    ctx.root_page_id_ = header_page->root_page_id_;
    ctx.header_page_ = std::make_optional(std::move(header_guard));
    page_id_t page_id = ctx.root_page_id_;
    while (true) {
      auto guard = bpm_->FetchPageWrite(page_id);
      auto page = guard.AsMut<BPlusTreePage>();
      if (page->IsSafe(operation)) {
        ctx.write_set_.clear();
        ctx.header_page_ = std::nullopt;
      }

      if (page->IsLeafPage()) {
        ctx.write_set_.push_back(std::move(guard));
        return;
      }
      auto internal_page = guard.As<InternalPage>();
      BUSTUB_ASSERT(internal_page->GetSize() >= 2,
                     "The size of internal page should be >= 2.");
      ctx.write_set_.push_back(std::move(guard));
      page_id = internal_page->GetChild(comparator_, key);
    }
  }

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printalbe B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
