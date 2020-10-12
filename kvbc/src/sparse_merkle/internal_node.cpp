// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "sparse_merkle/internal_node.h"
#include "sparse_merkle/histograms.h"

#include <iostream>
using namespace std;
using namespace concord::diagnostics;

namespace concord {
namespace kvbc {
namespace sparse_merkle {

using namespace detail;

void BatchedInternalNode::updateHashes(size_t index, Version version) {
  TimeRecorder scoped_timer(*histograms.internal_node_update_hashes);
  ConcordAssert(index > 0);
  auto hasher = Hasher();

  while (true) {
    Hash parent_hash;
    if (isLeftChild(index)) {
      parent_hash = hasher.parent(getHash(index), getHash(index + 1));
    } else {
      parent_hash = hasher.parent(getHash(index - 1), getHash(index));
    }
    auto parent_idx = parentIndex(index).value();
    auto& parent = children_.at(parent_idx);
    if (!parent) {
      // We are adding the first child below a non-existent parent. Create the
      // parent first.
      children_[parent_idx] = InternalChild{Hash(), version};
      parent = children_.at(parent_idx);
    }
    // A parent is *always* an InternalChild
    auto& internal_child = std::get<InternalChild>(parent.value());
    internal_child.hash = parent_hash;
    internal_child.version = version;
    if (parent_idx == 0) {
      // We have reached the root of this BatchedInternalNode
      return;
    }
    index = parent_idx;
  }
}

BatchedInternalNode::InsertResult BatchedInternalNode::insert(const LeafChild& child,
                                                              size_t depth,
                                                              Version current_version) {
  TimeRecorder scoped_timer(*histograms.internal_node_insert);
  // The index into the children_ array
  size_t index = 0;
  Nibble child_key = child.key.hash().getNibble(depth);

  for (size_t i = Nibble::SIZE_IN_BITS - 1; i >= 0 && i != SIZE_MAX; i--) {
    if (child_key.getBit(i)) {
      index = rightChildIndex(index);
    } else {
      index = leftChildIndex(index);
    }
    if (isInternal(index)) {
      continue;
    }

    // First try to insert into an empty child, then try to overwrite the child,
    // and lastly split the node.
    if (auto rv = insertIntoEmptyChild(index, child, current_version)) {
      return rv.value();
    }
    if (auto rv = overwrite(index, child)) {
      return rv.value();
    }
    return splitNode(index, child, depth);
  }

  // We have reached the leaf of this BatchedInternalNode and it points to another
  // BatchedInternalNode. Return the Version so the caller can construct an
  // InternalNodeKey and retrieve the next batched node in the tree.
  auto& stored_child = children_[index];
  return InsertIntoExistingNode{std::get<InternalChild>(stored_child.value()).version};
}

std::optional<BatchedInternalNode::InsertResult> BatchedInternalNode::insertIntoEmptyChild(size_t index,
                                                                                           const LeafChild& child,
                                                                                           Version version) {
  if (isEmpty(index)) {
    children_[index] = child;
    updateHashes(index, version);
    return InsertComplete{};
  }
  return std::nullopt;
}

std::optional<BatchedInternalNode::InsertResult> BatchedInternalNode::overwrite(size_t index, const LeafChild& child) {
  auto& stored_child = std::get<LeafChild>(children_.at(index).value());
  if (stored_child.key.hash() == child.key.hash()) {
    auto result = InsertComplete{stored_child.key};
    stored_child = child;
    updateHashes(index, child.key.version());
    return result;
  }
  return std::nullopt;
}

BatchedInternalNode::InsertResult BatchedInternalNode::splitNode(size_t index, const LeafChild& child, size_t depth) {
  auto version = child.key.version();
  auto stored_leaf_child = std::get<LeafChild>(children_.at(index).value());

  // Create a new InternalChild at index.
  children_.at(index).emplace(InternalChild{Hash(), version});

  if (height(index) == 0) {
    // We've reached the leaf of this node. Tell the caller to create new
    // BatchedInternalNodes and insert the previously stored leaf as well as the
    // leaf it was attempting to insert.
    //
    // We purposefully don't call `update_hashes`, since we don't know the true
    // value of the hash of the child until it gets inserted. The caller will fix
    // this BatchedInternalNode appropriately when the hash is known, by calling `linkChild`.
    return CreateNewBatchedInternalNodes{stored_leaf_child};
  }

  // How many prefix bits do the two leaf key hashes have in common?
  auto prefix_bits_in_common = stored_leaf_child.key.hash().prefix_bits_in_common(child.key.hash(), depth);
  ConcordAssert(prefix_bits_in_common > 0);

  // Fill in this node with any necessary internal children.
  Nibble child_key = child.key.hash().getNibble(depth);
  index = insertInternalChildren(index, version, child_key, prefix_bits_in_common);

  // Is there room to insert the 2 children in this BatchedInternalNode?
  if (prefix_bits_in_common < Nibble::SIZE_IN_BITS) {
    return insertTwoLeafChildren(index, version, child_key, prefix_bits_in_common, child, stored_leaf_child);
  }

  // We've reached the leaf of this node. Tell the caller to create new
  // BatchedInternalNodes and insert the previously stored leaf as well as the
  // leaf it was attempting to insert.
  //
  // We purposefully don't call `update_hashes`, since we don't know the true
  // value of the hash of the child until it gets inserted. The caller will fix
  // this BatchedInternalNode appropriately when the hash is known, by calling `linkChild`.
  return CreateNewBatchedInternalNodes{stored_leaf_child};
}

size_t BatchedInternalNode::insertInternalChildren(size_t index,
                                                   Version version,
                                                   Nibble child_key,
                                                   size_t prefix_bits_in_common) {
  size_t add_internal_children_until_height = 0;
  if (prefix_bits_in_common < Nibble::SIZE_IN_BITS) {
    add_internal_children_until_height = Nibble::SIZE_IN_BITS - prefix_bits_in_common;
  }
  for (size_t i = height(index) - 1; i >= add_internal_children_until_height && i != SIZE_MAX; i--) {
    if (child_key.getBit(i)) {
      index = rightChildIndex(index);
    } else {
      index = leftChildIndex(index);
    }
    children_.at(index).emplace(InternalChild{Hash(), version});
  }
  return index;
}

BatchedInternalNode::InsertResult BatchedInternalNode::insertTwoLeafChildren(
    size_t index, Version version, Nibble child_key, size_t prefix_bits_in_common, LeafChild child1, LeafChild child2) {
  size_t child1_index = 0;
  size_t child2_index = 0;
  // prefix_bits_in_common start from MSB. At most there can be 3 bits in common
  // in a Nibble.
  if (child_key.getBit(Nibble::SIZE_IN_BITS - prefix_bits_in_common - 1)) {
    child1_index = rightChildIndex(index);
    child2_index = leftChildIndex(index);
  } else {
    child1_index = leftChildIndex(index);
    child2_index = rightChildIndex(index);
  }
  children_[child1_index] = child1;
  children_[child2_index] = child2;
  updateHashes(child1_index, version);
  return InsertComplete{};
}

BatchedInternalNode::RemoveResult BatchedInternalNode::remove(const Hash& key, size_t depth, Version new_version) {
  TimeRecorder scoped_timer(*histograms.internal_node_remove);
  // The index into the children_ array
  size_t index = 0;

  // The part of the key that corresponds to this BatchedInternalNode
  Nibble child_key = key.getNibble(depth);

  for (size_t i = Nibble::SIZE_IN_BITS - 1; i >= 0 && i != SIZE_MAX; i--) {
    if (child_key.getBit(i)) {
      index = rightChildIndex(index);
    } else {
      index = leftChildIndex(index);
    }
    if (isInternal(index)) {
      continue;
    }

    if (isEmpty(index)) {
      return NotFound{};
    }

    auto& leaf = std::get<LeafChild>(children_[index].value());
    if (leaf.key.hash() == key) {
      // We can remove the key at this index.
      return removeLeafChild(index, new_version, leaf.key.version(), depth);
    }
    return NotFound{};
  }

  // We have reached level 0 of this BatchedInternalNode and it points to
  // another BatchedInternalNode where the child may reside.
  //
  // We must retrieve the version of the next BatchedInternalNode so that the
  // caller can retrieve it and try to delete the Leaf inside that node.
  auto version = std::get<InternalChild>(children_[index].value()).version;
  return Descend{version};
}

BatchedInternalNode::RemoveResult BatchedInternalNode::removeLeafChild(size_t index,
                                                                       Version new_version,
                                                                       Version removed_version,
                                                                       size_t depth) {
  ConcordAssert(index != 0);
  children_[index] = std::nullopt;
  auto peer_index = peerIndex(index);
  if (!peer_index) {
    // Only the root remains.
    ConcordAssert(1 == numChildren());
    updateHashes(index, new_version);
    return RemoveBatchedInternalNode(removed_version);
  }
  // Only remove the peer if it's a LeafChild.
  if (isInternal(peer_index.value())) {
    updateHashes(index, new_version);
    return RemoveComplete{removed_version};
  }

  auto peer = std::get<LeafChild>(children_[peer_index.value()].value());
  children_[peer_index.value()] = std::nullopt;

  // Keep trying to move the peer up toward the root of this
  // BatchedInternalNode.
  while (auto parent_index = parentIndex(peer_index.value())) {
    // The parent has a peer. We can't move up any further.
    if (peerIndex(parent_index.value())) {
      children_[parent_index.value()] = peer;
      updateHashes(parent_index.value(), new_version);
      return RemoveComplete{removed_version};
    }

    // The peer can't be moved up any higher in the root BatchedInternalNode (depth == 0)
    if (parent_index == 0 && depth == 0) {
      children_[peer_index.value()] = peer;
      updateHashes(peer_index.value(), new_version);
      return RemoveComplete{removed_version};
    }
    // Clear the parent, since we are walking up over it.
    children_[parent_index.value()] = std::nullopt;
    peer_index = parent_index;
  }

  // We've reached the root. This peer is the only remaining node left, and we
  // aren't in the root BatchedInternalNode.
  return RemoveBatchedInternalNode(peer, removed_version);
}

bool BatchedInternalNode::safeToRemove() const { return (numLeafChildren() == 0) && (numLevel0Children() == 0); }

std::string BatchedInternalNode::toString() const {
  std::string s("BatchedInternalNode children_:\n");
  for (auto i = 0u; i < BatchedInternalNode::MAX_CHILDREN; i++) {
    if (children_[i]) {
      std::visit(
          [&](auto&& child) {
            s += "  index=" + std::to_string(i) + ", depth=" + std::to_string(4 - height(i)) + ": " + child.toString() +
                 "\n";
          },
          children_[i].value());
    }
  }
  return s;
}

void BatchedInternalNode::linkChild(Nibble child_key, const InternalChild& child) {
  size_t index = nibbleToIndex(child_key);
  children_[index] = child;
  updateHashes(index, child.version);
}

// An invariant maintained is that removing a child is the same as having never
// inserted that child. Therefore, other children may be removed, or shifted up
// the tree when a child is removed.
std::optional<LeafChild> BatchedInternalNode::unlinkChild(Nibble child_key,
                                                          Version version,
                                                          const std::optional<LeafChild>& promoted) {
  size_t index = nibbleToIndex(child_key);
  children_[index] = promoted;
  auto peer_index = peerIndex(index);
  if (peer_index && promoted) {
    updateHashes(index, version);
    return std::nullopt;
  }

  while (true) {
    auto parent_index = parentIndex(index);
    if (!parent_index) {
      // We reached the root, so this BatchedInternalNode is going to be deleted.
      if (!children_[index]) {
        ConcordAssert(numChildren() == 0);
        return std::nullopt;
      }
      auto rv = std::get<LeafChild>(children_[index].value());
      children_[index] = std::nullopt;
      ConcordAssert(numChildren() == 0);
      return rv;
    }
    if (peer_index) {
      if (children_[index] || isInternal(peer_index.value())) {
        updateHashes(index, version);
        return std::nullopt;
      }
      // Move the LeafChild peer upwards
      children_[parent_index.value()] = children_[peer_index.value()];
      children_[peer_index.value()] = std::nullopt;
      index = parent_index.value();
      peer_index = peerIndex(index);
    } else {
      // Move current child up to the parent node
      children_[parent_index.value()] = children_[index];
      children_[index] = std::nullopt;
      index = parent_index.value();
      peer_index = peerIndex(index);
    }
  }
}

Version BatchedInternalNode::version() const {
  auto& root = children_[0];
  if (!root) return 0;
  return std::get<InternalChild>(root.value()).version;
}

size_t BatchedInternalNode::numChildren() const {
  return std::count_if(children_.begin(), children_.end(), [](const std::optional<Child>& c) { return c.has_value(); });
}

size_t BatchedInternalNode::numInternalChildren() const {
  return std::count_if(children_.begin(), children_.end(), [](const std::optional<Child>& c) {
    if (c) {
      return std::holds_alternative<InternalChild>(c.value());
    }
    return false;
  });
}

size_t BatchedInternalNode::numLeafChildren() const {
  return std::count_if(children_.begin(), children_.end(), [](const std::optional<Child>& c) {
    if (c) {
      return std::holds_alternative<LeafChild>(c.value());
    }
    return false;
  });
}

size_t BatchedInternalNode::numLevel0Children() const {
  constexpr size_t start_offset = (MAX_CHILDREN / 2);
  return std::count_if(children_.begin() + start_offset, children_.end(), [](const auto& c) { return c.has_value(); });
}

size_t BatchedInternalNode::nibbleToIndex(Nibble nibble) const {
  constexpr size_t level_0_start = MAX_CHILDREN / 2;
  return level_0_start + nibble.data();
}

size_t BatchedInternalNode::height(size_t index) const {
  if (index == 0) return 4;
  if (index < 3) return 3;
  if (index < 7) return 2;
  if (index < 15) return 1;
  return 0;
}

const Hash& BatchedInternalNode::getHash(size_t index) const {
  auto& child = children_.at(index);
  if (!child) {
    return PLACEHOLDER_HASH;
  }
  return std::visit([](auto&& arg) -> const Hash& { return arg.hash; }, child.value());
}

bool BatchedInternalNode::isInternal(size_t index) const {
  if (auto& child = children_.at(index)) {
    return std::holds_alternative<InternalChild>(child.value());
  }
  return false;
}

std::ostream& operator<<(std::ostream& os, const LeafChild& child) {
  os << "LeafChild {hash=" << child.hash << ", key=" << child.key << "}";
  return os;
}

std::ostream& operator<<(std::ostream& os, const InternalChild& child) {
  os << "InternalChild {hash=" << child.hash << ", version=" << child.version << "}";
  return os;
}

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
