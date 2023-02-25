#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (key.empty()) {
    key = " ";
  }
  auto value = Get<T>(root_, key);
  return value;
}

template <class T>
auto Trie::Get(const std::shared_ptr<const TrieNode> &root, std::string_view key) const -> const T * {
  char key_cahr = key.at(0);
  if (root->HasChildren(key_cahr)) {
    auto node_child = root->GetChild(key_cahr);
    if (key.size() == 1) {
      auto node_end = dynamic_cast<const TrieNodeWithValue<T> *>(node_child.get());
      if (node_end == nullptr) {
        return nullptr;
      }
      auto value = static_cast<T *>((node_end->value_).get());
      return value;
    }
    return Get<T>(node_child, key.substr(1));
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  auto trie = Trie();
  if (key.empty()) {
    key = " ";
  }
  trie.root_ = Put<T>(this->root_, key, std::move(value));

  return trie;
}

template <class T>
auto Trie::Put(const std::shared_ptr<const TrieNode> &root, std::string_view key, T value) const
    -> std::shared_ptr<TrieNode> {
  // optimize one: for root node, we have no need to create a new one, because we have created one.
  auto new_node = std::shared_ptr<TrieNode>(root->Clone());
  auto key_char = key.at(0);
  if (new_node->HasChildren(key_char)) {
    std::shared_ptr<TrieNode> node;
    auto child_node = new_node->GetChild(key_char);
    if (key.size() == 1) {
      node = std::make_shared<TrieNodeWithValue<T>>(child_node->children_, std::make_shared<T>(std::move(value)));
    } else {
      node = Put<T>(child_node, key.substr(1), std::move(value));
    }
    new_node->RemoveChild(key_char);
    (new_node->children_).emplace(key_char, node);
  } else {
    // create a new node, but share the same children.
    Insert<T>(new_node, key, std::move(value));
  }

  return new_node;
}

template <class T>
void Trie::Insert(const std::shared_ptr<TrieNode> &root, std::string_view key, T value) const {
  if (key.size() == 1) {
    auto node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    (root->children_).emplace(key.at(0), node);
    return;
  }

  auto node = std::make_shared<TrieNode>();
  Insert(node, key.substr(1), std::move(value));
  (root->children_).emplace(key.at(0), node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

auto Trie::GetRoot() const -> std::shared_ptr<const TrieNode> { return root_; }

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
