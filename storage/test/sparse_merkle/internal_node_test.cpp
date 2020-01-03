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

using namespace concord::storage::sparse_merkle;

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
TEST(internal_node_tests, insert_single_leaf_node) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));
  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto child = LeafChild{value1_hash, leaf_key1};

  // The key of this child in the 4-level BatchedInternalNode tree.
  Nibble child_key = key1_hash.getNibble(depth);

  // A node was successfully inserted. Since it didn't overwrite another node
  // there is not a stale leaf.
  auto result = node.insert(child, depth);
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));
  auto successful_insert = std::get<BatchedInternalNode::InsertComplete>(result);
  ASSERT_FALSE(successful_insert.stale_leaf.has_value());

  // There should be one root and one leaf
  ASSERT_EQ(2, node.numChildren());
  ASSERT_EQ(1, node.numInternalChildren());
  ASSERT_EQ(1, node.numLeafChildren());

  // The version should be 1.
  ASSERT_EQ(Version(1), node.version());

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
TEST(internal_node_tests, overwrite_single_leaf_node) {
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

  auto result = node.insert(child1, depth);
  result = node.insert(child2, depth);

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
TEST(internal_node_tests, create_two_leafs_with_one_parent) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the first bit so that key1_hash becomes a sibling of key2_hash
  std::array<uint8_t, Hash::SIZE_IN_BYTES> key2_hash_data;
  std::copy(key1_hash.data(), key1_hash.data() + key1_hash.size(), key2_hash_data.begin());
  key2_hash_data[0] ^= 0x80;
  auto key2_hash = Hash(key2_hash_data);

  ASSERT_EQ(0, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  // The key of the first child in the 4-level BatchedInternalNode tree.
  Nibble child1_key = key1_hash.getNibble(depth);

  auto result = node.insert(child1, depth);
  result = node.insert(child2, depth);

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
TEST(internal_node_tests, split_leaf) {
  BatchedInternalNode node;

  Hasher hasher;
  const char* key1 = "artist";
  const char* value1 = "REM";
  const char* value2 = "Nas";
  size_t depth = 0;

  auto key1_hash = hasher.hash(key1, strlen(key1));

  // Flip the second bit so that key1_hash becomes a sibling of key2_hash at
  // height 2.
  std::array<uint8_t, Hash::SIZE_IN_BYTES> key2_hash_data;
  std::copy(key1_hash.data(), key1_hash.data() + key1_hash.size(), key2_hash_data.begin());
  key2_hash_data[0] ^= 0x40;
  auto key2_hash = Hash(key2_hash_data);

  ASSERT_EQ(1, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  // The key of the first child in the 4-level BatchedInternalNode tree.
  Nibble child1_key = key1_hash.getNibble(depth);

  auto result = node.insert(child1, depth);
  result = node.insert(child2, depth);

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

TEST(internal_node_tests, split_until_new_batch_node_needed) {
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
  std::array<uint8_t, Hash::SIZE_IN_BYTES> key2_hash_data;
  std::copy(key1_hash.data(), key1_hash.data() + key1_hash.size(), key2_hash_data.begin());
  key2_hash_data[0] ^= 0x08;
  auto key2_hash = Hash(key2_hash_data);

  ASSERT_EQ(4, key1_hash.prefix_bits_in_common(key2_hash));

  auto value1_hash = hasher.hash(value1, strlen(value1));
  auto value2_hash = hasher.hash(value2, strlen(value2));
  auto leaf_key1 = LeafKey(key1_hash, Version(1));
  auto leaf_key2 = LeafKey(key2_hash, Version(2));
  auto child1 = LeafChild{value1_hash, leaf_key1};
  auto child2 = LeafChild{value2_hash, leaf_key2};

  auto result = node.insert(child1, depth);
  result = node.insert(child2, depth);

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
  result = node.insert(child2, depth);
  ASSERT_TRUE(std::holds_alternative<BatchedInternalNode::InsertIntoExistingNode>(result));
  auto insert_result = std::get<BatchedInternalNode::InsertIntoExistingNode>(result);
  ASSERT_EQ(insert_result.next_node_version, Version(2));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
