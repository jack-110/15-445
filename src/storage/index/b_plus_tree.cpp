#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  if (IsEmpty()) {
    return false;
  }

  Context ctx;
  TranverseTreeWithRLatch(ctx, key);

  auto leaf_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  auto leaf_page = leaf_guard.As<LeafPage>();
  return leaf_page->GetValue(comparator_, key, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx;
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::make_optional(std::move(header_guard));
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return CreateTree(ctx, key, value);
  }

  TranverseTreeWithWLatch(ctx, key, OperationType::INSERT);

  auto leaf_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
  if (!leaf_page->Insert(comparator_, key, value)) {
    ctx.header_page_ = std::nullopt;
    ctx.write_set_.clear();
    leaf_guard.Drop();
    return false;
  }

  if (!leaf_page->IsFull()) {
    leaf_guard.Drop();
    BUSTUB_ENSURE(ctx.write_set_.empty(), "The write locks should have been released.");
    return true;
  }

  KeyType child_key;
  page_id_t child_value;
  // split an leaf node when number of values reaches max_size after insertion.
  SplitLeafPage(leaf_page, child_key, child_value);
  if (ctx.IsRootPage(leaf_guard.PageId())) {
    IncreaseTree(ctx, child_key, child_value);
    leaf_guard.Drop();
    BUSTUB_ENSURE(ctx.write_set_.empty(), "There should have no locks.");
    return true;
  }
  leaf_guard.Drop();

  // internal page layers
  while (!ctx.write_set_.empty()) {
    auto guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto page = guard.AsMut<InternalPage>();
    if (!page->IsFull()) {
      BUSTUB_ASSERT(ctx.write_set_.empty(), "The number of locks shuold be 0.");
      page->Insert(comparator_, child_key, child_value);
      return true;
    }

    // split an internal node when number of values reaches max_size before insertion.
    // the child_key and child_value updated in this method.
    SplitInternalPage(page, child_key, child_value);
    BUSTUB_ASSERT(page->GetSize() >= page->GetMinSize(),
                  "The size of internal page should be greater than or equal with min when after split.");
    if (ctx.IsRootPage(guard.PageId())) {
      IncreaseTree(ctx, child_key, child_value);
    }
    guard.Drop();
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  if (IsEmpty()) {
    return;
  }

  Context ctx;
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::make_optional(std::move(header_guard));
  TranverseTreeWithWLatch(ctx, key, OperationType::DELETE);

  auto sib1_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto leaf_page = sib1_guard.AsMut<LeafPage>();
  if (!leaf_page->Remove(comparator_, key)) {
    return;
  }

  // root as leaf, and the tree become empty
  if (ctx.IsRootPage(sib1_guard.PageId()) && leaf_page->GetSize() == 0) {
    BUSTUB_ENSURE(ctx.write_set_.empty(), "Theres should be no locks on tree except leaf page lock.");
    DecreaseTree(ctx, INVALID_PAGE_ID);
    return;
  }

  auto size = ctx.write_set_.size();
  for (size_t i = 0; i < size; i++) {
    auto guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto page = guard.AsMut<InternalPage>();
    auto search_index = page->GetSearchIndex(comparator_, key);
    auto left_id = search_index - 1;
    auto right_id = search_index;
    BUSTUB_ENSURE(0 < search_index && search_index < page->GetSize(), "The search index should be in the middle.");

    // first layer: internal page -> leaf page.
    if (i == 0) {
      LeafPage *left_page;
      LeafPage *right_page;
      WritePageGuard sib2_guard;
      if (sib1_guard.PageId() == page->ValueAt(left_id)) {
        left_page = sib1_guard.AsMut<LeafPage>();
        sib2_guard = bpm_->FetchPageWrite(static_cast<page_id_t>(page->ValueAt(right_id)));
        right_page = sib2_guard.AsMut<LeafPage>();
      } else {
        right_page = sib1_guard.AsMut<LeafPage>();
        sib2_guard = bpm_->FetchPageWrite(static_cast<page_id_t>(page->ValueAt(left_id)));
        left_page = sib2_guard.AsMut<LeafPage>();
      }
      BUSTUB_ENSURE(left_page != right_page, "The two pages should not be equal.");
      if (left_page->GetSize() > left_page->GetMinSize() || right_page->GetSize() > right_page->GetMinSize()) {
        // redistribute
        auto up_key = left_page->Redistribute(comparator_, right_page);
        page->SetKeyAt(search_index, up_key);
        BUSTUB_ENSURE(left_page->GetSize() >= left_page->GetMinSize(),
                      "The size of left leaf page should be greater than or equal with min after redistribute.");
        BUSTUB_ENSURE(right_page->GetSize() >= right_page->GetMinSize(),
                      "The size of right leaf page should be greater than or equal with min after redistribute.");
        return;
      }
      // merge
      left_page->Merge(comparator_, right_page);
      BUSTUB_ENSURE(left_page->GetSize() >= left_page->GetMinSize(),
                    "The size of left leaf page should be greater than min size after merged.");
      BUSTUB_ENSURE(right_page->GetSize() == 0, "The size of right leaf page should be equal with 0 after merged.");
      page->Remove(search_index);
      // reach the last page and it's root, so need to check it.
      if (ctx.IsRootPage(guard.PageId()) && page->GetSize() < 2) {
        DecreaseTree(ctx, page->ValueAt(search_index - 1));
        return;
      }
      sib1_guard.Drop();
      sib2_guard.Drop();
      sib1_guard = std::move(guard);
    } else {
      InternalPage *left_page;
      InternalPage *right_page;
      WritePageGuard sib2_guard;
      if (sib1_guard.PageId() == page->ValueAt(left_id)) {
        left_page = sib1_guard.AsMut<InternalPage>();
        sib2_guard = bpm_->FetchPageWrite(static_cast<page_id_t>(page->ValueAt(right_id)));
        right_page = sib2_guard.AsMut<InternalPage>();
      } else {
        right_page = sib1_guard.AsMut<InternalPage>();
        sib2_guard = bpm_->FetchPageWrite(static_cast<page_id_t>(page->ValueAt(left_id)));
        left_page = sib2_guard.AsMut<InternalPage>();
      }
      BUSTUB_ENSURE(left_page != right_page, "The two pages should not be equal.");
      if (left_page->GetSize() > left_page->GetMinSize() || right_page->GetSize() > right_page->GetMinSize()) {
        // redistribute
        BUSTUB_ENSURE(
            left_page->GetSize() < left_page->GetMinSize() || right_page->GetSize() < left_page->GetMinSize(),
            "The size of left internal page or right internal page should be less than min size when redistribute.");
        // also include move the search key down
        auto upper_key = left_page->Redistribute(comparator_, right_page, page->KeyAt(search_index));
        // move the new search key up
        page->SetKeyAt(search_index, upper_key);
        BUSTUB_ENSURE(left_page->GetSize() >= left_page->GetMinSize(),
                      "The size of left internal page should be greater than or equal with min after redistribute.");
        BUSTUB_ENSURE(right_page->GetSize() >= right_page->GetMinSize(),
                      "The size of right internal page should be greater than or equal with min after redistribute.");
        return;
      }
      // merge and also move the search key down
      left_page->Merge(comparator_, right_page, page->KeyAt(search_index));
      BUSTUB_ENSURE(left_page->GetSize() >= left_page->GetMinSize(),
                    "The size of left internal page should be greater than min size after merged.");
      BUSTUB_ENSURE(right_page->GetSize() == 0, "The size of right internal page should be 0 after merged.");
      page->Remove(search_index);
      // reach the last page and it's root, so need to check it.
      if (ctx.IsRootPage(guard.PageId()) && page->GetSize() < 2) {
        DecreaseTree(ctx, page->ValueAt(left_id));
        return;
      }
      sib1_guard.Drop();
      sib2_guard.Drop();
      sib1_guard = std::move(guard);
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  auto guard = bpm_->FetchPageRead(1);
  return INDEXITERATOR_TYPE(bpm_, std::move(guard), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  guard.Drop();
  TranverseTreeWithRLatch(ctx, key);
  auto leaf_guard = std::move(ctx.read_set_.back());
  auto leaf_page = leaf_guard.As<LeafPage>();
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      return INDEXITERATOR_TYPE(bpm_, std::move(leaf_guard), i);
    }
  }
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
