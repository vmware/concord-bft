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

#include "gtest/gtest.h"
#include "sparse_merkle/base_types.h"
#include "sparse_merkle/internal_node.h"

using namespace concord::kvbc::sparse_merkle;

// Return a new hash with the given bit flipped for the given hash.
//
// Flip a given bit starting from the most significant bit. So bit 0 to be
// flipped would be the first bit, bit 1, the second bit, etc...
Hash flipByte0Bit(size_t bit, const Hash& hash) {
  ConcordAssert(bit < 8);
  // Flip the first bit so that key1_hash becomes a sibling of key2_hash
  std::array<uint8_t, Hash::SIZE_IN_BYTES> flipped;
  std::copy(hash.data(), hash.data() + hash.size(), flipped.begin());
  flipped[0] ^= (1 << (7 - bit));
  return Hash(flipped);
}

TEST(internal_node_tests, empty_invariants) {
  BatchedInternalNode node;

  // An empty node has a placeholder hash
  ASSERT_EQ(PLACEHOLDER_HASH, node.hash());

  // An empty node has version 0
  ASSERT_EQ(Version(0), node.version());

  // There are no children of any kind
  ASSERT_EQ(0, node.numChildren());
  ASSERT_EQ(0, node.numInternalChildren());
  ASSERT_EQ(0, node.numLeafChildren());
}

// The logical tree inside the BatchedInternalNode looks like the following at the end of this test
//
//    Root
//     |
//     |
//    Leaf
//
TEST(insert_tests, insert_single_leaf_node) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  size_t depth = 0;
  auto version = Version(1);

  auto key1_hash = hasher.hash(key1, strlen(key1));
  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto leaf_key1 = LeafKey(key1_hash, version);
  auto child = LeafChild{value1_hash, leaf_key1};

  // The key of this child in the 4-level BatchedInternalNode tree.
  Nibble child_key = key1_hash.getNibble(depth);

  // A node was successfully inserted. Since it didn't overwrite another node
  // there is not a stale leaf.
  auto result = node.insert(child, depth, version);
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));
  auto successful_insert = std::get<BatchedInternalNode::InsertComplete>(result);
  ASSERT_FALSE(successful_insert.stale_leaf.has_value());

  // There should be one root and one leaf
  ASSERT_EQ(2, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(1, node.numLeafChildren());

  // The version should be 1.
  ASSERT_EQ(version, node.version());

  Hash parent_hash;
  if (child_key.getBit(3)) {
    // The key was inserted at the right child of the BatchedInternalNode root
    parent_hash = hasher.parent(PLACEHOLDER_HASH, value1_hash);
  } else {
    // The key was inserted at the left child of the BatchedInternalNode root
    parent_hash = hasher.parent(value1_hash, PLACEHOLDER_HASH);
  }

  ASSERT_EQ(node.hash(), parent_hash);
}

// The logical tree inside the BatchedInternalNode looks like the following at the end of this test
//
//    Root
//     |
//     |
//    Leaf
//
TEST(insert_tests, overwrite_single_leaf_node) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));
  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key1_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  // The key of this child in the 4-level BatchedInternalNode tree.
  Nibble child_key = key1_hash.getNibble(depth);

  auto result = node.insert(child1, depth, Version(1));
  result = node.insert(child2, depth, Version(2));

  // The second child overwrote the first, thus returning the key of the first
  // in stale_leaf. No new nodes were generated, since there was no partial
  // match leading to creation of parent nodes overflowing to create new
  // BatchedInternalNodes.
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));
  auto successful_insert = std::get<BatchedInternalNode::InsertComplete>(result);
  ASSERT_EQ(successful_insert.stale_leaf.value(), leaf_key1);

  // There should be one root and one leaf
  ASSERT_EQ(2, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(1, node.numLeafChildren());

  // The version should be 2.
  ASSERT_EQ(Version(2), node.version());

  Hash parent_hash;
  if (child_key.getBit(3)) {
    // The key was inserted at the right child of the BatchedInternalNode root
    parent_hash = hasher.parent(PLACEHOLDER_HASH, value2_hash);
  } else {
    // The key was inserted at the left child of the BatchedInternalNode root
    parent_hash = hasher.parent(value2_hash, PLACEHOLDER_HASH);
  }

  ASSERT_EQ(node.hash(), parent_hash);
}

/*
// The logical tree inside the BatchedInternalNode looks like the following at the end of this test
//
//          Root
//           |
//         /   \
//      Leaf   Leaf
//
*/
TEST(insert_tests, create_two_leaves_with_one_parent) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the first bit so that key1_hash becomes a sibling of key2_hash
  auto key2_hash = flipByte0Bit(0, key1_hash);
  ASSERT_EQ(0, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  // The key of the first child in the 4-level BatchedInternalNode tree.
  Nibble child1_key = key1_hash.getNibble(depth);

  auto result = node.insert(child1, depth, Version(1));
  result = node.insert(child2, depth, Version(2));

  // No nodes were overwritten and no new BatchedInternalNodes created
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));
  auto successful_insert = std::get<BatchedInternalNode::InsertComplete>(result);
  ASSERT_FALSE(successful_insert.stale_leaf.has_value());

  // There should be one root and two leaves
  ASSERT_EQ(3, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());

  // The version should be 2.
  ASSERT_EQ(Version(2), node.version());

  Hash parent_hash;
  if (child1_key.getBit(3)) {
    // The key was inserted at the right child of the BatchedInternalNode root
    parent_hash = hasher.parent(value2_hash, value1_hash);
  } else {
    // The key was inserted at the left child of the BatchedInternalNode root
    parent_hash = hasher.parent(value1_hash, value2_hash);
  }

  ASSERT_EQ(node.hash(), parent_hash);
}
/*
// The logical tree inside the BatchedInternalNode looks like the following at the end of this test
//
//                Root
//                 |
/          -----------------
//         |               |
//      Internal      Placeholder
//         |
//        / \
//    Leaf   Leaf
*/
TEST(insert_tests, split_leaf) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the second bit so that key1_hash becomes a sibling of key2_hash at
  // height 2.
  auto key2_hash = flipByte0Bit(1, key1_hash);
  ASSERT_EQ(1, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  // The key of the first child in the 4-level BatchedInternalNode tree.
  Nibble child1_key = key1_hash.getNibble(depth);

  auto result = node.insert(child1, depth, Version(1));
  result = node.insert(child2, depth, Version(2));

  // No nodes were overwritten and no new BatchedInternalNodes created
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));
  auto successful_insert = std::get<BatchedInternalNode::InsertComplete>(result);
  ASSERT_FALSE(successful_insert.stale_leaf.has_value());

  ASSERT_EQ(4, node.numChildren());
  // The root + one internal node below the root
  ASSERT_EQ(2, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());

  // The version should be 2.
  ASSERT_EQ(Version(2), node.version());

  Hash level_3_parent_hash;
  if (child1_key.getBit(2)) {
    // The key was inserted at the right child of the InternalChild at height 3
    level_3_parent_hash = hasher.parent(value2_hash, value1_hash);
  } else {
    // The key was inserted at the left child of the InternalChild at height 3
    level_3_parent_hash = hasher.parent(value1_hash, value2_hash);
  }

  Hash root_hash;
  if (child1_key.getBit(3)) {
    // The internal node is at the right of the root
    root_hash = hasher.parent(PLACEHOLDER_HASH, level_3_parent_hash);
  } else {
    root_hash = hasher.parent(level_3_parent_hash, PLACEHOLDER_HASH);
  }

  ASSERT_EQ(node.hash(), root_hash);
}

/*
// The logical tree inside the BatchedInternalNode looks like the following at the end of this test
//
//                               Root
//                                 |
//                         -----------------
//                         |               |
//                      Internal      Placeholder
//                         |
//                 ----------------
//                 |              |
//              Internal       Placeholder
//                 |
//          ----------------
//          |              |
//      Internal       Placeholder
//          |
//    ----------------
//    |              |
// Internal       Placeholder
//
*/

TEST(insert_tests, split_until_new_batch_node_needed) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Copy the actual hash and set the first nibble to zeroes to make all
  // internal children left ones.
  std::array<uint8_t, Hash::SIZE_IN_BYTES> key1_hash_data;
  std::copy(key1_hash.data(), key1_hash.data() + key1_hash.size(), key1_hash_data.begin());
  key1_hash_data[0] &= 0x0F;
  key1_hash = Hash{key1_hash_data};

  // Flip the 5th bit so that key1_hash becomes a sibling of key2_hash at
  // height -1 (in a new node).
  auto key2_hash = flipByte0Bit(4, key1_hash);
  ASSERT_EQ(4, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  auto result = node.insert(child1, depth, Version(1));
  result = node.insert(child2, depth, Version(2));

  // A new BatchedInternalNode must be created.
  // The first 4 bits of the key of child1 and child2 matched, thus creating new
  // internal children and causing the previously inserted child1, as well as
  // child2 to need to find a new home in a new BatchedInternalNode further down
  // the tree.
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::CreateNewBatchedInternalNodes>(result));
  auto create_result = std::get<BatchedInternalNode::CreateNewBatchedInternalNodes>(result);
  ASSERT_EQ(create_result.stored_child, child1);

  ASSERT_EQ(5, node.numChildren());
  ASSERT_EQ(5, node.numInternalChildren());
  ASSERT_EQ(0, node.numLeafChildren());

  // NOTE: We cannot check the root hash here, because this tree doesn't yet
  // have the hash of its InternalChild leaf, since the value must be inserted
  // into a new node by the caller, and then the hash updated in this node.

  // Pretend that this BatchedInternalNode was properly updated by the caller
  // already. Inserting a new partially matching prefix key should result in
  // BatchedInternalNode::InsertIntoExistingNode, since there is already an
  // InternalLeaf at height 0 that shares a prefix. This tests the bottom of the
  // insert function, when the entire path is made up of InternalLeafs.
  //
  // We can test this by just re-inserting the same key.
  result = node.insert(child2, depth, Version(2));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::InsertIntoExistingNode>(result));
  auto insert_result = std::get<BatchedInternalNode::InsertIntoExistingNode>(result);
  ASSERT_EQ(insert_result.next_node_version, Version(2));
}

// Add a single LeafChild to a BatchedInternalNode and then remove it.
//
// The logical tree inside the BatchedInternalNode looks like the following
// before remove is called:
//
//    Root
//     |
//     |
//    Leaf
//
// After removal of Leaf, this BatchedInternalNode will be removed by the caller.
//
TEST(remove_tests, remove_a_single_leaf) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* bad_key = "badkey";
  size_t depth = 0;
  auto version = Version(1);

  auto bad_key_hash = hasher.hash(bad_key, strlen(bad_key));
  auto key1_hash = hasher.hash(key1, strlen(key1));
  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto leaf_key1 = LeafKey(key1_hash, version);
  auto child = LeafChild{value1_hash, leaf_key1};

  // A node was successfully inserted. Since it didn't overwrite another node
  // there is not a stale leaf.
  auto result = node.insert(child, depth, version);
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));

  // Attempting to remove a key that doesn't exist should return NotFound
  auto remove_result = node.remove(bad_key_hash, depth, version);
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::NotFound>(remove_result));

  // Try to remove a key that doesn't exist, but where a LeafNode does exist that
  // has a matching prefix. We know the node matching the key doesn't exist,
  // because there would be at least one InternalNode with the matching prefix
  // Leaf and this node below it if that were the case.
  //
  // Flip the 5th bit.
  auto partially_matching_hash = flipByte0Bit(4, key1_hash);
  remove_result = node.remove(partially_matching_hash, depth, version);
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::NotFound>(remove_result));

  // Deleting this key should return RemoveBatchedInternalNode;
  remove_result = node.remove(key1_hash, depth, version);
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::RemoveBatchedInternalNode>(remove_result));
  ASSERT_FALSE(std::get<BatchedInternalNode::RemoveBatchedInternalNode>(remove_result).promoted.has_value());
}

/*
// Add 2 LeafChildren to a BatchedInternalNode and then remove one of them.
//
// Since this is a remove from the second node, the BatchedInternalNode can be
// removed when a peer moves up to the root BatchedInternalNode.
//
// The logical tree inside the BatchedInternalNode looks like the following
// before remove is called:
//
//          Root
//           |
//         /   \
//      Leaf   Leaf
//
// After removal of a Leaf, the other will be promoted and this
// BatchedInternalNode will be removed by the caller.
//
*/
TEST(remove_tests, remove_a_leaf_with_a_peer_second_node) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 1;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the 5th bit so that key1_hash becomes a sibling of key2_hash right
  // below the root of the BatchedInternalNode at depth 1 in the sparse merkle
  // tree.
  auto key2_hash = flipByte0Bit(4, key1_hash);

  ASSERT_EQ(4, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  node.insert(child1, depth, Version(1));
  node.insert(child2, depth, Version(2));

  // There should be one root and two leaves
  ASSERT_EQ(3, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());

  // Deleting key1 should return RemoveBatchedInternalNode with a promoted
  // child2.
  auto result = node.remove(key1_hash, depth, Version(3));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::RemoveBatchedInternalNode>(result));
  auto promoted = std::get<BatchedInternalNode::RemoveBatchedInternalNode>(result).promoted.value();
  ASSERT_EQ(child2, promoted);
}

/*
// Add 2 LeafChildren to a BatchedInternalNode and then remove one of them.
//
// The logical tree inside the BatchedInternalNode looks like the following
// before remove is called:
//
//          Root
//           |
//         /   \
//      Leaf1   Leaf2
//
// Since this is the root BatchedInternalNode the peer will not be promoted.
// The logical tree will look like the following after the remove is called:
//
//            Root
//             |
//     -----------------
//     |               |
// Placeholder       Leaf2
//
*/
TEST(remove_tests, remove_a_leaf_with_a_peer_root_node) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the first bit so that key1_hash becomes a sibling of key2_hash
  auto key2_hash = flipByte0Bit(0, key1_hash);

  ASSERT_EQ(0, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  node.insert(child1, depth, Version(1));
  node.insert(child2, depth, Version(2));

  // There should be one root and two leaves
  ASSERT_EQ(3, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());

  // Deleting key1 should return RemoveComplete, since the peer cannot be promoted above the root node.
  auto result = node.remove(key1_hash, depth, Version(3));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::RemoveComplete>(result));
  ASSERT_EQ(node.hash(), hasher.parent(PLACEHOLDER_HASH, value2_hash));

  // There should be one root and one leaf
  ASSERT_EQ(2, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(1, node.numLeafChildren());
}

/*
// Remove a leaf with a peer at depth 2 (Leaf1). This will cause the peer to
// move up to depth 1, and then stop, since it will have a new peer. In this
// case, call to `remove` will return `RemoveComplete`.
//
// The logical tree inside the BatchedInternalNode looks like the following
// before the remove:
//
//                Root
//                 |
//         -----------------
//         |               |
//      Internal         Leaf2
//         |
//        / \
//   Leaf3   Leaf1
//
//
// The logical tree looks like the following after Leaf1 is removed:
//
//                Root
//                 |
//         -----------------
//         |               |
//       Leaf3            Leaf2
//
*/
TEST(remove_tests, remove_a_leaf_with_a_leaf_peer_from__batched_internal_node_with_3_leaves) {
  BatchedInternalNode node;
  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  const char* value3 = "Rihanna";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the first bit so that key1_hash becomes a sibling of key2_hash at
  // depth 1.
  auto key2_hash = flipByte0Bit(0, key1_hash);

  // Flip the second bit so that key1_hash becomes a sibling of key3_hash at
  // height depth 2.
  auto key3_hash = flipByte0Bit(1, key1_hash);

  ASSERT_EQ(0, key1_hash.prefix_bits_in_common(key2_hash));
  ASSERT_EQ(1, key1_hash.prefix_bits_in_common(key3_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto value3_hash = hasher.hash(value3, strlen(value3));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto leaf_key3 = LeafKey(key3_hash, Version(3));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};
  auto child3 = LeafChild{value3_hash, leaf_key3};

  node.insert(child1, depth, Version(1));
  node.insert(child2, depth, Version(2));
  node.insert(child3, depth, Version(3));

  ASSERT_EQ(5, node.numChildren());
  ASSERT_EQ(2, node.numInternalChildren());
  ASSERT_EQ(3, node.numLeafChildren());

  auto result = node.remove(key1_hash, depth, Version(4));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::RemoveComplete>(result));
  auto removed_version = std::get<BatchedInternalNode::RemoveComplete>(result).version;
  ASSERT_EQ(Version(1), removed_version);

  ASSERT_EQ(3, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());

  // Ensure the root hash is correct.
  ASSERT_EQ(node.hash(), hasher.parent(value3_hash, value2_hash));
  ASSERT_EQ(Version(4), node.version());
}

/*
// Remove a leaf (Leaf2) at depth 1 with an inernal node as a peer.
//
// This should cause a single remove to occur, but the left side of the tree to
// stay as is. RemoveComplete will be returned.
//
// The logical tree inside the BatchedInternalNode looks like the following
// before the remove:
//
//                Root
//                 |
//         -----------------
//         |               |
//      Internal         Leaf2
//         |
//        / \
//   Leaf3   Leaf1
//
//
// The logical tree looks like the following after Leaf2 is removed:
//
//                Root
//                 |
//         -----------------
//         |               |
//      Internal       PLACEHOLDER
//         |
//        / \
//   Leaf3   Leaf1
//
*/
TEST(remove_tests, remove_a_leaf_with_internal_peer_from_batched_internal_node_with_3_leaves) {
  BatchedInternalNode node;
  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  const char* value3 = "Rihanna";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the first bit so that key1_hash becomes a sibling of key2_hash at
  // depth 1.
  auto key2_hash = flipByte0Bit(0, key1_hash);

  // Flip the second bit so that key1_hash becomes a sibling of key3_hash at
  // height depth 2.
  auto key3_hash = flipByte0Bit(1, key1_hash);

  ASSERT_EQ(0, key1_hash.prefix_bits_in_common(key2_hash));
  ASSERT_EQ(1, key1_hash.prefix_bits_in_common(key3_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto value3_hash = hasher.hash(value3, strlen(value3));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto leaf_key3 = LeafKey(key3_hash, Version(3));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};
  auto child3 = LeafChild{value3_hash, leaf_key3};

  node.insert(child1, depth, Version(1));
  node.insert(child2, depth, Version(2));
  node.insert(child3, depth, Version(3));

  ASSERT_EQ(5, node.numChildren());
  ASSERT_EQ(2, node.numInternalChildren());
  ASSERT_EQ(3, node.numLeafChildren());

  auto result = node.remove(key2_hash, depth, Version(4));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::RemoveComplete>(result));
  auto removed_version = std::get<BatchedInternalNode::RemoveComplete>(result).version;
  ASSERT_EQ(Version(2), removed_version);

  ASSERT_EQ(4, node.numChildren());
  ASSERT_EQ(2, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());

  // Ensure the root hash is correct.
  auto internal_hash = hasher.parent(value1_hash, value3_hash);
  ASSERT_EQ(node.hash(), hasher.parent(internal_hash, PLACEHOLDER_HASH));
  ASSERT_EQ(Version(4), node.version());
}

/*
// Remove a leaf (Leaf1) at depth 3 with leaf node as a peer.
//
// This should cause a single remove to occur, with the left side of the tree
// moving up 2 levels. RemoveComplete will be returned.
//
// The logical tree inside the BatchedInternalNode looks like the following
// before the remove:
//
//                  Root
//                   |
//           -----------------
//           |               |
//        Internal         Leaf2
//           |
//          / \
//   Internal  PLACEHOLDER
//      |
//     / \
// Leaf3  Leaf1
//
//
// After Removing Leaf1, the tree will look like the following:
//
//          Root
//           |
//         /   \
//     Leaf3   Leaf2
*/
TEST(remove_tests, remove_a_leaf_at_depth_3_with_a_leaf_peer_and_another_peer_at_depth_1) {
  BatchedInternalNode node;
  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  const char* value3 = "Rihanna";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the first bit so that key1_hash becomes a sibling of key2_hash at
  // depth 1.
  auto key2_hash = flipByte0Bit(0, key1_hash);

  // Flip the third bit bit so that key1_hash becomes a sibling of key3_hash at
  // height depth 3.
  auto key3_hash = flipByte0Bit(2, key1_hash);

  ASSERT_EQ(0, key1_hash.prefix_bits_in_common(key2_hash));
  ASSERT_EQ(2, key1_hash.prefix_bits_in_common(key3_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto value3_hash = hasher.hash(value3, strlen(value3));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto leaf_key3 = LeafKey(key3_hash, Version(3));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};
  auto child3 = LeafChild{value3_hash, leaf_key3};

  node.insert(child1, depth, Version(1));
  node.insert(child2, depth, Version(2));
  node.insert(child3, depth, Version(3));

  ASSERT_EQ(6, node.numChildren());
  ASSERT_EQ(3, node.numInternalChildren());
  ASSERT_EQ(3, node.numLeafChildren());

  auto result = node.remove(key1_hash, depth, Version(4));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::RemoveComplete>(result));
  auto removed_version = std::get<BatchedInternalNode::RemoveComplete>(result).version;
  ASSERT_EQ(Version(1), removed_version);

  ASSERT_EQ(3, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());
  // Ensure the root hash is correct.
  ASSERT_EQ(node.hash(), hasher.parent(value3_hash, value2_hash));
  ASSERT_EQ(Version(4), node.version());
}

/*
// Remove a leaf (Leaf1) at depth 3 with leaf node as a peer, from a
// BatchedInternalNode that is not the root of the sparse merkle tree.
//
// There are no other leaves in the tree, so RemoveBatchedInternalNode with a
// promoted peer should be returned.
//
// The logical tree inside the BatchedInternalNode looks like the following
// before the remove:
//
//                  Root
//                   |
//           -----------------
//           |               |
//        Internal      PLACEHOLDER
//           |
//          / \
//   Internal  PLACEHOLDER
//      |
//     / \
// Leaf3  Leaf1
//
//
// After removal of Leaf1, Leaf3 will be promoted and this BatchedInternalNode
// will be removed by the caller, since this is not the root node of the sparse
// merkle tree.
*/
TEST(remove_tests, remove_a_leaf_at_depth_3_with_a_leaf_peer_and_no_other_leaves) {
  BatchedInternalNode node;
  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value3 = "Rihanna";
  size_t depth = 1;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the seventh bit bit so that key1_hash becomes a sibling of key3_hash at
  // depth 3 in the second BatchedInternalNode down the sparse merkle tree..
  auto key3_hash = flipByte0Bit(6, key1_hash);

  ASSERT_EQ(6, key1_hash.prefix_bits_in_common(key3_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value3_hash = hasher.hash(value3, strlen(value3));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key3 = LeafKey(key3_hash, Version(3));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child3 = LeafChild{value3_hash, leaf_key3};

  node.insert(child1, depth, Version(1));
  node.insert(child3, depth, Version(3));

  ASSERT_EQ(5, node.numChildren());
  ASSERT_EQ(3, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());

  // Deleting key1 should return RemoveBatchedInternalNode with a promoted
  // child3.
  auto result = node.remove(key1_hash, depth, Version(4));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::RemoveBatchedInternalNode>(result));
  auto promoted = std::get<BatchedInternalNode::RemoveBatchedInternalNode>(result).promoted.value();
  ASSERT_EQ(child3, promoted);
}

/*
// Try to Remove a leaf (Leaf1) at depth 4 with leaf node as a peer, but return
// NotFound.
//
// The key given will match in the first Nibble, but 5th bit will be different,
// so NotFound should be returned.
//
// The logical tree inside the BatchedInternalNode looks like the following
// before and after the attempted remove:
//
//                       Root
//                        |
//                -----------------
//                |               |
//              Internal      PLACEHOLDER
//                |
//               / \
//        Internal  PLACEHOLDER
//           |
//          / \
//   Internal  PLACEHOLDER
//      |
//     / \
// Leaf3 Leaf1
//
//
// After removal of Leaf1, Leaf3 will be promoted and this BatchedInternalNode
// will be removed by the caller.
*/
TEST(remove_tests, remove_a_mismatched_leaf_at_depth_4) {
  BatchedInternalNode node;
  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value3 = "Rihanna";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the 4th bit bit so that key1_hash becomes a sibling of key3_hash at
  // height depth 3.
  auto key3_hash = flipByte0Bit(3, key1_hash);

  // Flip the fifth bit of key1_hash so that it will collide in the first
  // nibble.
  auto partially_matching_hash = flipByte0Bit(4, key1_hash);

  ASSERT_EQ(3, key1_hash.prefix_bits_in_common(key3_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value3_hash = hasher.hash(value3, strlen(value3));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key3 = LeafKey(key3_hash, Version(3));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child3 = LeafChild{value3_hash, leaf_key3};

  node.insert(child1, depth, Version(1));
  node.insert(child3, depth, Version(3));

  ASSERT_EQ(6, node.numChildren());
  ASSERT_EQ(4, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());

  auto result = node.remove(partially_matching_hash, depth, Version(4));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::NotFound>(result));

  ASSERT_EQ(6, node.numChildren());
  ASSERT_EQ(4, node.numInternalChildren());
  ASSERT_EQ(2, node.numLeafChildren());
}

/*
// The logical tree inside the BatchedInternalNode looks like the following
// before and after the remove:
//
//                               Root
//                                 |
//                         -----------------
//                         |               |
//                      Internal      Placeholder
//                         |
//                 ----------------
//                 |              |
//              Internal       Placeholder
//                 |
//          ----------------
//          |              |
//      Internal       Placeholder
//          |
//    ----------------
//    |              |
// Internal       Placeholder
//
*/
TEST(remove_tests, descend_because_requested_node_is_internal) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Copy the actual hash and set the first nibble to zeroes to make all
  // internal children left ones.
  std::array<uint8_t, Hash::SIZE_IN_BYTES> key1_hash_data;
  std::copy(key1_hash.data(), key1_hash.data() + key1_hash.size(), key1_hash_data.begin());
  key1_hash_data[0] &= 0x0F;
  key1_hash = Hash{key1_hash_data};

  // Flip the 5th bit so that key1_hash becomes a sibling of key2_hash at
  // height -1 (in a new node).
  auto key2_hash = flipByte0Bit(4, key1_hash);
  ASSERT_EQ(4, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  node.insert(child1, depth, Version(1));
  node.insert(child2, depth, Version(2));

  ASSERT_EQ(5, node.numChildren());
  ASSERT_EQ(5, node.numInternalChildren());
  ASSERT_EQ(0, node.numLeafChildren());

  auto result = node.remove(key1_hash, depth, Version(3));
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::Descend>(result));
  ASSERT_EQ(Version(2), std::get<BatchedInternalNode::Descend>(result).next_node_version);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
