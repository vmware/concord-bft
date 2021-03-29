// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include <algorithm>

#include "gtest/gtest.h"
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include "sparse_merkle/internal_node.h"
#include "sliver.hpp"
#include "sha_hash.hpp"

using concordUtils::Sliver;
using concord::util::SHA3_256;
using namespace concord::kvbc::sparse_merkle;

static constexpr size_t ROOT_INDEX = 0;

// Generators for properties
namespace rc {

template <>
struct Arbitrary<Sliver> {
  static auto arbitrary() { return gen::construct<Sliver>(gen::string<std::string>()); }
};

template <>
struct Arbitrary<Nibble> {
  static auto arbitrary() { return gen::construct<Nibble>(gen::inRange<uint8_t>(0, 16)); }
};

template <>
struct Arbitrary<Hash> {
  static auto arbitrary() { return gen::construct<Hash>(gen::arbitrary<SHA3_256::Digest>()); }
};

template <>
struct Arbitrary<Version> {
  static auto arbitrary() { return gen::construct<Version>(gen::arbitrary<uint64_t>()); }
};

template <>
struct Arbitrary<LeafKey> {
  static auto arbitrary() { return gen::construct<LeafKey>(gen::arbitrary<Hash>(), gen::arbitrary<Version>()); }
};

template <>
struct Arbitrary<LeafChild> {
  static auto arbitrary() { return gen::construct<LeafChild>(gen::arbitrary<Hash>(), gen::arbitrary<LeafKey>()); }
};

}  // namespace rc

// Return a vector of up to 16 unique nibbles.
rc::Gen<std::vector<Nibble>> genUniqueNibbles() {
  auto vec_nibble_gen = rc::gen::unique<std::vector<Nibble>>(rc::gen::arbitrary<Nibble>());
  return rc::gen::resize(16, vec_nibble_gen);
}

// Return a generator for the depth of a BatchedInternalNode in a tree
rc::Gen<size_t> genDepth() { return rc::gen::inRange<size_t>(0ul, Hash::MAX_NIBBLES); }

rc::Gen<std::vector<LeafChild>> genLeafChildrenWithSameVersion(const rc::Gen<std::vector<LeafChild>>& childGen) {
  return rc::gen::apply(
      [](Version version, const std::vector<LeafChild>& children) {
        std::vector<LeafChild> output;
        output.reserve(children.size());
        for (const auto& child : children) {
          output.push_back(LeafChild(child.hash, LeafKey(child.key.hash(), version)));
        }
        return output;
      },
      rc::gen::arbitrary<Version>(),
      childGen);
}

// Create a generator to generate a maximum of 16 identical hashes, except that each one has a unique nibble at position
// `depth`.
rc::Gen<std::tuple<std::vector<Hash>, size_t>> genHashesWithUniqueNibblesAtGeneratedDepth() {
  return rc::gen::apply(
      [](const std::vector<Nibble>& nibbles, size_t depth) {
        std::vector<Hash> hashes;
        for (const auto& nibble : nibbles) {
          auto digest = Hash::EMPTY_BUF;
          setNibble(depth, digest, nibble);
          hashes.push_back(Hash(digest));
        }
        return std::make_tuple(hashes, depth);
      },
      genUniqueNibbles(),
      genDepth());
}

rc::Gen<std::tuple<std::vector<LeafChild>, size_t>> genKeysWithMatchingPrefixesUpToDepth(
    const rc::Gen<std::vector<LeafChild>>& childGen) {
  return rc::gen::apply(
      [](auto children, size_t depth) {
        for (auto& child : children) {
          auto new_hash = child.key.hash();
          for (auto i = 0ul; i < depth; i++) {
            new_hash.setNibble(i, PLACEHOLDER_HASH.getNibble(i));
            child.key = LeafKey(new_hash, child.key.version());
          }
        }
        return std::make_tuple(children, depth);
      },
      childGen,
      genDepth());
}

RC_GTEST_PROP(batched_internal_node_properties,
              insert_on_empty_node_contains_leaf_child_at_depth_1,
              (const LeafChild& child)) {
  auto node = BatchedInternalNode();
  auto result = node.insert(child, 0, child.key.version());
  RC_ASSERT(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));

  // There is no stale leaf overwritten, since we insert a new value to an empty node
  // RC_ASSERT_FALSE does compile with GCC :(
  RC_ASSERT(std::get<BatchedInternalNode::InsertComplete>(result).stale_leaf.has_value() == false);

  // Only the inserted child exists
  RC_ASSERT(1u == node.numLeafChildren());

  // The root is the only internal child
  RC_ASSERT(1u == node.numInternalChildren());

  // The LeafChild is directly under the root
  auto left_index = node.leftChildIndex(ROOT_INDEX);
  auto right_index = node.rightChildIndex(ROOT_INDEX);
  RC_ASSERT(!node.isInternal(left_index) && !node.isInternal(right_index));
  RC_ASSERT(!node.isEmpty(left_index) != !node.isEmpty(right_index));
}

RC_GTEST_PROP(batched_internal_node_properties, removing_sole_inserted_child_removes_node, (const LeafChild& child)) {
  auto node = BatchedInternalNode();
  const auto max_depth = Hash::MAX_NIBBLES;
  const auto depth = *rc::gen::inRange<size_t>(0, max_depth);
  auto version = child.key.version();
  node.insert(child, depth, version);
  auto result = node.remove(child.key.hash(), depth, child.key.version() + 1);
  auto rv = std::get<BatchedInternalNode::RemoveBatchedInternalNode>(result);
  RC_ASSERT(rv.promoted.has_value() == false);
  RC_ASSERT(rv.removed_version == version);
  RC_ASSERT(node.safeToRemove());
}

RC_GTEST_PROP(
    batched_internal_node_properties,
    removing_a_child_in_nodes_with_more_than_two_children_leaves_node_as_if_child_was_never_inserted_if_we_ignore_versions,
    ()) {
  auto node = BatchedInternalNode();
  const auto version = Version(1);

  const auto& [key_hashes, depth] = *genHashesWithUniqueNibblesAtGeneratedDepth();

  // Removing one or two nodes will trigger removal of the BatchedInternalNode altogether. We want
  // to ensure that when only a single key is removed, the state of the node is identical to the
  // state before the addition. Therefore, we only work on nodes with more than 2 inserted keys.
  try {
    RC_PRE(key_hashes.size() > 2ul);
  } catch (...) {
    RC_SUCCEED("Test case skipped. Pre-condition not met.");
  }

  for (auto i = 0ul; i < key_hashes.size() - 1; i++) {
    auto key = LeafKey(key_hashes[i], version);
    auto child = LeafChild(*rc::gen::arbitrary<Hash>(), key);
    auto result = node.insert(child, depth, version);
    RC_ASSERT(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));
  }
  auto node_before_last_insert = node;
  auto key = LeafKey(key_hashes[key_hashes.size() - 1], version);
  auto child = LeafChild(*rc::gen::arbitrary<Hash>(), key);
  auto result = node.insert(child, depth, version);
  RC_ASSERT(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));

  // Removing the last key should result in the same node as before the key was inserted.
  auto remove_result = node.remove(key.hash(), depth, version);
  RC_ASSERT(version == std::get<BatchedInternalNode::RemoveComplete>(remove_result).version);
  RC_ASSERT(node_before_last_insert == node);
}

RC_GTEST_PROP(batched_internal_node_properties,
              removing_a_child_in_node_with_one_or_two_children_triggers_removal_of_the_node,
              ()) {
  auto node = BatchedInternalNode();
  const auto version = Version(1);
  const auto& [key_hashes, depth] = *genHashesWithUniqueNibblesAtGeneratedDepth();

  try {
    RC_PRE(key_hashes.size() == 1ul || key_hashes.size() == 2ul);
  } catch (...) {
    RC_SUCCEED("Test case skipped. Pre-condition not met.");
  }

  for (auto key_hash : key_hashes) {
    auto key = LeafKey(key_hash, version);
    auto child = LeafChild(*rc::gen::arbitrary<Hash>(), key);
    auto result = node.insert(child, depth, version);
    RC_ASSERT(std::holds_alternative<BatchedInternalNode::InsertComplete>(result));
  }
  auto remove_result = node.remove(key_hashes[key_hashes.size() - 1], depth, version);
  if (key_hashes.size() == 1) {
    auto remove = std::get<BatchedInternalNode::RemoveBatchedInternalNode>(remove_result);
    RC_ASSERT(version == remove.removed_version);
    RC_ASSERT(remove.promoted.has_value() == false);
  } else {
    if (depth != 0) {
      auto remove = std::get<BatchedInternalNode::RemoveBatchedInternalNode>(remove_result);
      RC_ASSERT(version == remove.removed_version);
      RC_ASSERT(key_hashes[0] == remove.promoted.value().key.hash());
    } else {
      // We can't promote a LeafChild from the root node of the tree.
      auto remove_complete = std::get<BatchedInternalNode::RemoveComplete>(remove_result);
      RC_ASSERT(version == remove_complete.version);
    }
  }
}

RC_GTEST_PROP(batched_internal_node_properties, two_inserts_with_overlapping_prefix_trigger_a_node_split, ()) {
  auto node = BatchedInternalNode();
  auto key_hash1 = *rc::gen::arbitrary<Hash>();
  auto key_hash2 = *rc::gen::arbitrary<Hash>();

  // Only try up to depth 62. The last nibble should be different so we don't have identical keys.
  auto depth = *genDepth();
  if (depth == 63) depth = 62;

  auto version = *rc::gen::arbitrary<Version>();

  // Set identical prefixes
  for (auto i = 0ul; i <= depth; i++) {
    key_hash1.setNibble(i, key_hash2.getNibble(i));
  }
  // Flip the last nibble to ensure distinct keys
  auto flipped = Nibble(key_hash2.getNibble(63).data() ^ 0x0F);
  key_hash1.setNibble(63, flipped);

  auto child1 = LeafChild(*rc::gen::arbitrary<Hash>(), LeafKey(key_hash1, version));
  auto child2 = LeafChild(*rc::gen::arbitrary<Hash>(), LeafKey(key_hash2, version));
  RC_ASSERT(std::holds_alternative<BatchedInternalNode::InsertComplete>(node.insert(child1, depth, version)));

  // Inserting a node with a matching prefix up to and including depth nibbles should result in a
  // CreateNewBatchedInternalNodes and a returning of the previously stored child that caused the
  // collision.
  auto result = std::get<BatchedInternalNode::CreateNewBatchedInternalNodes>(node.insert(child2, depth, version));
  RC_ASSERT(child1 == result.stored_child);

  // There should only be 5 internal children: 1 path from the root to the leaf of the BatchedInternalNode
  RC_ASSERT(5u == node.numInternalChildren());
  RC_ASSERT(0u == node.numLeafChildren());

  // Ensure that there is an InternalChild at the proper leaf index inside the BatchedInternalNode.
  auto leaf_index = node.nibbleToIndex(key_hash1.getNibble(depth));
  RC_ASSERT(node.isInternal(leaf_index));

  // Assert that every parent of this node is also an InternalNode.
  auto parent_index = node.parentIndex(leaf_index);
  for (auto i = 0; i < 4; i++) {
    RC_ASSERT(node.isInternal(parent_index.value()));
    parent_index = node.parentIndex(parent_index.value());
  }
}

RC_GTEST_PROP(batched_internal_node_properties, inserts_are_deterministic, ()) {
  auto node1 = BatchedInternalNode();
  auto node2 = BatchedInternalNode();
  auto [children, depth] = *genKeysWithMatchingPrefixesUpToDepth(rc::gen::arbitrary<std::vector<LeafChild>>());
  for (const auto& child : children) {
    RC_LOG() << child << std::endl;
    auto version = *rc::gen::arbitrary<Version>();
    RC_LOG() << "version1: " << version << std::endl;
    auto result1 = node1.insert(child, depth, version);
    RC_LOG() << "version2: " << version << std::endl;
    auto result2 = node2.insert(child, depth, version);
    RC_LOG() << "result1 index: " << result1.index() << ", result2 index: " << result2.index() << std::endl;
  }
  for (auto i = 0u; i < node1.children().size(); i++) {
    RC_LOG() << i << std::endl;
    RC_LOG() << node1.children()[i] << std::endl;
    RC_LOG() << node2.children()[i] << std::endl;
  }

  RC_ASSERT(node1 == node2);
}

// Insert a bunch of keys that won't trigger splits. Show that insertion order doesn't matter.
//
// The version can't be randomized per insert, otherwise, the last inserted version would bubble to
// the top and make the insert order matter.
//
// The keys also must be unique, or a key with a different hash could be inserted in a different
// order and cause the hash calculation to be different.
RC_GTEST_PROP(batched_internal_node_properties,
              insert_order_of_unique_keys_with_no_splits_does_not_matter,
              (Version version)) {
  auto node1 = BatchedInternalNode();
  auto node2 = BatchedInternalNode();

  const auto& [key_hashes, depth] = *genHashesWithUniqueNibblesAtGeneratedDepth();
  std::vector<LeafChild> children1;
  for (const auto& key_hash : key_hashes) {
    auto key = LeafKey(key_hash, version);
    children1.emplace_back(LeafChild(*rc::gen::arbitrary<Hash>(), key));
  }

  std::vector<LeafChild> children2 = children1;
  std::random_shuffle(children2.begin(), children2.end());

  for (auto i = 0u; i < children1.size(); i++) {
    node1.insert(children1[i], depth, version);
    node2.insert(children2[i], depth, version);
  }
  RC_ASSERT(node1 == node2);
}

RC_GTEST_PROP(batched_internal_node_properties,
              any_insert_order_of_keys_with_same_version_maintains_same_keys,
              (Version version)) {
  auto node1 = BatchedInternalNode();
  auto node2 = BatchedInternalNode();

  // Make the children *mostly* unique.
  auto unique_children_gen = rc::gen::unique<std::vector<LeafChild>>(rc::gen::arbitrary<LeafChild>());
  auto [children1, depth] = *genKeysWithMatchingPrefixesUpToDepth(genLeafChildrenWithSameVersion(unique_children_gen));

  std::vector<LeafChild> children2 = children1;
  std::random_shuffle(children2.begin(), children2.end());

  for (auto i = 0u; i < children1.size(); i++) {
    node1.insert(children1[i], depth, version);
    node2.insert(children2[i], depth, version);
  }

  // Ensure the keys of each node are identical. The hashes will not necessarily be identical
  // because we don't actually update them on node splits. A split results in the tree creating new
  // BatchedInternalNodes and then recursively walking back up the tree once it knows the hashes of
  // the children of this BatchedInternalNode. Since that happens outside of the BatchedInternalNode
  // logic, we can only test that the order of keys is the same.
  for (auto i = 0u; i < node1.children().size(); i++) {
    auto child1 = node1.children()[i];
    auto child2 = node2.children()[i];
    if (child1.has_value()) {
      if (node1.isInternal(i)) {
        RC_ASSERT(std::holds_alternative<InternalChild>(child2.value()));
      } else {
        RC_ASSERT(std::get<LeafChild>(child1.value()).key == std::get<LeafChild>(child2.value()).key);
      }
    } else {
      RC_ASSERT(child2.has_value() == false);
    }
  }
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
