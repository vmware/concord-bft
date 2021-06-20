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

#pragma once

#include <optional>
#include <variant>
#include <algorithm>

#include "sliver.hpp"
#include "sparse_merkle/keys.h"
#include "sparse_merkle/base_types.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {

/*
A partially full BatchedInternalNode may look like the following:

```
Level 4               ---------root-------
                      |                  |
Level 3           LeafChild       InternalChild
                                  /            \
Level 2                     LeafChild    InternalChild
                                                  \
Level 1                                       InternalChild
                                             /             \
Level 0                                 LeafChild      InternalChild

Note that a LeafChild *never* has any children below it, but an InternalChild
always does. An InternalChild at level 0 indicates more children in other
BatchedInternalNodes further down the tree.

Please refer to this diagram to understand the types of children in a
BatchedInternalNode and their corresponding header comments.
*/

// A child that is a leaf of the overall tree. It isn't necessarily at height 0
// in a BatchedInternalNode, although there will not be any children below it.
struct LeafChild {
  LeafChild(Hash _hash, LeafKey _key) : hash(_hash), key(_key) {}
  static constexpr auto SIZE_IN_BYTES = Hash::SIZE_IN_BYTES + LeafKey::SIZE_IN_BYTES;
  std::string toString() const {
    return std::string("LeafChild {hash=") + hash.toString() + ", key = " + key.toString() + "}";
  }
  bool operator==(const LeafChild& other) const { return hash == other.hash && key == other.key; }
  bool operator<(const LeafChild& other) const { return hash < other.hash && key < other.key; }
  // The hash of the value blob stored at `key`
  Hash hash;
  LeafKey key;
};

std::ostream& operator<<(std::ostream& os, const LeafChild&);

// A child that represents an internal node of the sparse binary merkle tree. It
// will refer to another BatchedInternalNode if it resides at height 0.
struct InternalChild {
  static constexpr auto SIZE_IN_BYTES = Hash::SIZE_IN_BYTES + Version::SIZE_IN_BYTES;
  std::string toString() const {
    return std::string("InternalChild {hash=") + hash.toString() + ", version= " + version.toString() + "}";
  }
  bool operator==(const InternalChild& other) const { return hash == other.hash && version == other.version; }
  Hash hash;
  Version version;
};

std::ostream& operator<<(std::ostream& os, const InternalChild&);

typedef std::variant<LeafChild, InternalChild> Child;

// Allow streaming of all variants in this namespace
template <typename T0, typename... Ts>
std::ostream& operator<<(std::ostream& s, std::variant<T0, Ts...> const& v) {
  std::visit([&](auto&& arg) { s << arg; }, v);
  return s;
}

// Allow streaming of all options in this namespace
template <typename T>
std::ostream& operator<<(std::ostream& s, std::optional<T> const& v) {
  if (v) {
    s << v.value();
  } else {
    s << "<<nullopt>>";
  }
  return s;
}

/*
// A batched internal node. The node is a representation of a 4-level tree, such
// that there are a total possibility of 16 leaves.
//
// Children of this tree are stored in an array. A child may represent an
// internal node of the overall sparse merkle tree, a leaf, or a placeholder
// node. If an InternalChild resides at height 0 it will point to another
// BatchedInternalNode. A leaf will always have a pointer to a key containing
// the actual stored data. It may or may not reside at height 0.
//
// The following is a diagram of a complete 4-level tree, re-used with thanks
// from
// https://github.com/libra/libra/blob/master/storage/jellyfish-merkle/src/node_type/mod.rs
//
//   4 ->              +------ root hash ------+
//                     |                       |
//   3 ->        +---- # ----+           +---- # ----+
//               |           |           |           |
//   2 ->        #           #           #           #
//             /   \       /   \       /   \       /   \
//   1 ->     #     #     #     #     #     #     #     #
//           / \   / \   / \   / \   / \   / \   / \   / \
//   0 ->   0   1 2   3 4   5 6   7 8   9 A   B C   D E   F
//   ^
// height
//
//
// We use a similar logical layout to libra for trees that are not full. Further
// quoting the libra source:
//
//       "As illustrated above, at nibble height 0, `0..F` in hex denote 16 chidren hashes. Each `#`
//       means the hash of its two direct children, which will be used to generate the hash of its
//       parent with the hash of its sibling. Finally, we can get the hash of this internal node.
//
//       However, if an internal node doesn't have all 16 chidren exist at height 0 but just a few of
//       them, we have a modified hashing rule on top of what is stated above:
//       1. From top to bottom, a node will be replaced by a leaf child if the subtree rooted at this
//       node has only one child at height 0 and it is a leaf child.
//       2. From top to bottom, a node will be replaced by the placeholder node if the subtree rooted at
//       this node doesn't have any child at height 0. For example, if an internal node has 3 leaf
//       children at index 0, 3, 8, respectively, and 1 internal node at index C, then the computation
//       graph will be like:"
//
//   4 ->              +------ root hash ------+
//                     |                       |
//   3 ->        +---- # ----+           +---- # ----+
//               |           |           |           |
//   2 ->        #           @           8           #
//             /   \                               /   \
//   1 ->     0     3                             #     @
//                                               / \
//   0 ->                                       C   @
//   ^
// height
// Note: @ denotes placeholder hash.
//
// To further elaborate: `C`, a BatchedInternalNode in our tree, is only present
// at height 0 if there are multiple keys with matching prefixes that have more than the
// number of bits from the root of the sparse merkle to height 0 of this tree.
// Concretely, if the displayed BatchedInternalNode is at depth 0 (height 256)
// of the entire sparse merkle tree, then 2 keys with 5 leading bits in common
// would cause `C` to be created and put at height 0 of this
// BatchedInternalNode. This scenario is demonstrated in the test
// `split_until_new_batch_node_needed` in `internal_node_test.cpp`.
//
*/
class BatchedInternalNode {
 public:
  static constexpr size_t MAX_CHILDREN = 31;
  using Children = std::array<std::optional<Child>, MAX_CHILDREN>;

  // The leaf was inserted into this node successfully.
  //
  // It's possible that a leaf with the same key hash as the newly inserted leaf
  // was overwritten.  In this case we need to tell the caller that this
  // occurred so they can add it to their stale list for pruning later.
  struct InsertComplete {
    std::optional<LeafKey> stale_leaf;
  };

  // A node split has occurred, forcing the need to create new
  // BatchedInternalNodes.
  //
  // Depending upon how many bits in common the key hashes of these 2 leaves
  // have, more than one BatchedInternalNode may need to be created.
  //
  // Return the original child that forced a node split.
  struct CreateNewBatchedInternalNodes {
    // This LeafChild was previously stored in this BatchedInternalNode, but was replaced
    // with an InternalChild. It now needs to be inserted in a new node along
    // with the LeafChild originally trying to be inserted.
    LeafChild stored_child;
  };

  // A leaf could not be added at height 0 because there was an existing
  // InternalChild at the leaf (height 0) of this BatchedInternalNode.
  //
  // An attempt should be made to insert the leaf at the next, already existing
  // BatchedInternalNode with `next_node_version`.
  struct InsertIntoExistingNode {
    // The version of the next BatchedInternalNode to try inserting into. The
    // caller knows how to construct an InternalNodeKey using this version.
    Version next_node_version;
  };

  // insert(child, depth) can return one of three results to the sparse merkle
  // tree manipulating the BatchedInternalNode.
  typedef std::variant<InsertComplete, CreateNewBatchedInternalNodes, InsertIntoExistingNode> InsertResult;

  // A remove has completed successfully.
  //
  // The version of the found key is returned so that it can be marked stale.
  //
  // There are still other valid children in this BatchedInternalNode, so it
  // cannot be deleted.
  struct RemoveComplete {
    Version version;
  };

  // The key was not found in this node, and there are no references to it
  // further down the tree.
  struct NotFound {};

  // A remove has completed successfully, and this BatchedInternalNode is now
  // empty. The caller should remove it.
  //
  // In some cases where this value is returned there was one remaining child
  // that was a peer of the removed LeafChild and is able to be promoted to live
  // in the parent BatchedInternalNode. Return it so that the caller can move it
  // to that node.
  struct RemoveBatchedInternalNode {
    RemoveBatchedInternalNode(Version removed_version) : removed_version(removed_version) {}
    RemoveBatchedInternalNode(const std::optional<LeafChild>& promoted, Version removed_version)
        : promoted(promoted), removed_version(removed_version) {}

    std::optional<LeafChild> promoted;
    Version removed_version;
  };

  // A remove has failed, because the the leaf is further down the tree.
  // Instruct the caller to try the next node.
  struct Descend {
    // The version of the next BatchedInternalNode further down the tree.
    Version next_node_version;
  };

  typedef std::variant<RemoveComplete, NotFound, RemoveBatchedInternalNode, Descend> RemoveResult;

  // Construct an empty node.
  BatchedInternalNode() = default;

  // Construct from the passed children container.
  BatchedInternalNode(const Children& children) : children_{children} {}

  // Insert a LeafChild into this internal node. Return a type indicating success or
  // failure that contains the necessary data for the caller (the sparse merkle
  // tree implementation) to do the right thing.
  //
  // `depth` represents how many nibbles down the sparse merkle tree this BatchedInternalNode is.
  //
  // We pass `current_version`, because we may want to insert an existing LeafChild
  // that was promoted as a result of removal of BatchedInternalNodes below this
  // one in the tree. The version of the LeafChild in this case is going to be
  // an older version than the current version. However, we want the internal
  // nodes, and the new root to have the current version.
  InsertResult insert(const LeafChild& child, size_t depth, Version current_version);

  // Remove a LeafChild from this internal node. Return a type indicating to the
  // caller whether the key was successfully removed, and whether the caller
  // needs to take any further action, such as deleting this BatchedInternalNode
  // if it becomes empty.
  //
  // `depth` represents how many nibbles down the sparse merkle tree this BatchedInternalNode is.
  // `new_version` is the version of the updated BatchedInternalNode
  RemoveResult remove(const Hash& key, size_t depth, Version new_version);

  // Write an InternalChild of the BatchedInternalNode at level 0.
  //
  // This occurs when this InternalChild points to another BatchedInternalNode
  // down the tree, and that BatchedInternalNode was just updated.
  void linkChild(Nibble child_key, const InternalChild& child);

  // Remove a pointer to another BatchedInternalNode.
  //
  // This is called when the BatchedInternalNode further down the tree was removed.
  std::optional<LeafChild> unlinkChild(Nibble child_key, Version version, const std::optional<LeafChild>& promoted);

  // Return the root hash of this node.
  const Hash& hash() const { return getHash(0); }

  // Return the version of the root node of this BatchedInternalNode
  Version version() const;

  // Return the number of children that are not std::nullopt.
  size_t numChildren() const;

  // Return the number of internal children in this BatchedInternalNode
  size_t numInternalChildren() const;

  // Return the number of leaf children in this BatchedInternalNode
  size_t numLeafChildren() const;

  // Return the internal children container.
  const Children& children() const { return children_; }

  // Return true if this node is identical to the other one and false otherwise.
  bool operator==(const BatchedInternalNode& other) const { return children_ == other.children_; }

  // Return true if there are no linked children (InternalChild at level 0) and no leaf children.
  bool safeToRemove() const;

  // Return a string representation useful for debugging
  std::string toString() const;

  // **************************************************************************
  // The following const methods are really only useful publicly for testing purposes.
  // **************************************************************************

  // Take a nibble representing the logical location a child at height 0 in a
  // BatchedInternalNode and return the index of that child in `children_`.
  size_t nibbleToIndex(Nibble nibble) const;

  // Return true if the index does not contain a child.
  bool isEmpty(size_t index) const { return !children_.at(index).has_value(); }

  // Return true if the index has a child that contains an internal node.
  bool isInternal(size_t index) const;

  // Return the the height of the node at the given index.
  //
  // The height depends upon MAX_CHILDREN, which dictates the maximum depth of a
  // binary tree.
  size_t height(size_t index) const;

  // Return the index of the parent node, given the index of the child node.
  //
  // Return std::nullopt if the index points to the root of this
  // BatchedInternalNode.
  std::optional<size_t> parentIndex(size_t index) const {
    if (index == 0) return std::nullopt;
    return (index - 1) / 2;
  }

  // Return the peer index.
  //
  // If this node is the root, return std::nullopt;
  // If the node does not have a peer, then return std::nullopt;
  std::optional<size_t> peerIndex(size_t index) const {
    if (index == 0) return std::nullopt;

    auto peer_index = isLeftChild(index) ? index + 1 : index - 1;
    if (isEmpty(peer_index)) {
      return std::nullopt;
    }
    return peer_index;
  }

  // Return the index of the left child of a node at a given index.
  size_t leftChildIndex(size_t index) const { return 2 * index + 1; }

  // Return the index of the right child of a node at a given index.
  size_t rightChildIndex(size_t index) const { return 2 * index + 2; }

  // Is this node a left child?
  bool isLeftChild(size_t index) const { return index % 2 != 0; }

  // The number of children at level 0 of the 4 level tree in this BatchedInternalNode
  size_t numLevel0Children() const;

  // Return the hash of a given index.
  const Hash& getHash(size_t index) const;

 private:
  // A LeafChild collission has occurred. We need to create new InternalChild nodes so
  // that the stored LeafChild that collided with the new LeafChild attempting
  // to be added can live below those new internal nodes.
  //
  // Return the index of the bottom most InternalNode created.
  size_t insertInternalChildren(size_t index, Version version, Nibble child_key, size_t prefix_bits_in_common);

  // After the collission and the creation of new InternalChild nodes, there is
  // still room in this BatchedInternalNode to add the previously stored
  // LeafChild and the new LeafChild. Go ahead and do that insertion in this
  // function.
  //
  // Always return InsertComplete{}.
  InsertResult insertTwoLeafChildren(size_t index,
                                     Version version,
                                     Nibble child_key,
                                     size_t prefix_bits_in_common,
                                     LeafChild child1,
                                     LeafChild child2);

  // Remove the LeafChild at the given index. Updates of children should be set
  // to new_version.
  RemoveResult removeLeafChild(size_t index, Version new_version, Version stored_version, size_t depth);

  // Attempt to insert into an empty child.
  //
  // Return InsertComplete if the node was empty.
  // Otherwise return std::nullopt.
  //
  // We pass the version because we may want to insert an existing LeafChild
  // that was promoted as a result of removal of BatchedInternalNodes below this
  // one in the tree. The version of the LeafChild in this case is going to be
  // an older version than the current version. However, we want the internal
  // nodes, and the new root to have the current version.
  std::optional<InsertResult> insertIntoEmptyChild(size_t index, const LeafChild& child, Version current_version);

  // Attempt to overwrite a matching key at index.
  //
  // Return InsertComplete if the key was overwritten.
  // Otherwise return std::nullopt.
  std::optional<InsertResult> overwrite(size_t index, const LeafChild& child);

  // There has been a collision of leaf keys that do not match.
  //
  // One or more parents must be created to store them.
  //
  // `depth` is the current depth of this BatchedInternalNode in ths sparse merkle
  // tree.
  //
  // Return either CreateNewBatchedInternalNodes or InsertComplete.
  InsertResult splitNode(size_t index, const LeafChild& child, size_t depth);

  // Walk the tree from `index` up to the root of this BatchedInternalNode to
  // compute the root hash.
  //
  // Update the hashes and versions of nodes along the way.
  void updateHashes(size_t index, Version version);

  // All internal and leaf nodes are stored in this array, starting from the
  // root and proceeding level by level from left to right.
  Children children_;
};

}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
