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

#include "gtest/gtest.h"
#include <memory>
#include "sparse_merkle/tree.h"
#include "test_db.h"
#include "hash_defs.h"

#include <iostream>
using namespace std;

using namespace concordUtils;
using namespace concord::storage::sparse_merkle;

void db_put(const shared_ptr<TestDB>& db, const Tree::UpdateBatch& batch) {
  for (const auto& [key, val] : batch.internal_nodes) {
    db->put(key, val);
  }
  for (const auto& [key, val] : batch.leaf_nodes) {
    db->put(key, val);
  }
}

template <class T>
void assert_version(uint64_t version, const T& keys) {
  for (auto& key : keys) {
    ASSERT_EQ(Version(version), key.version());
  }
}
template <class T>
void assert_node_version(uint64_t version, const T& nodes) {
  for (auto& [key, _] : nodes) {
    // Prevent unused variable warning. C++ doesn't allow ignoring in
    // destructuring yet.
    (void)_;
    ASSERT_EQ(Version(version), key.version());
  }
}

// Ensure that getting the latest root from an empty db returns an empty
// BatchedInternalNode;
TEST(tree_tests, empty_db) {
  TestDB db;
  auto root = db.get_latest_root();
  ASSERT_EQ(Version(0), root.version());
  ASSERT_EQ(0, root.numChildren());
  ASSERT_EQ(PLACEHOLDER_HASH, root.hash());
}

// Ensure that we can insert a single leaf to an empty tree
TEST(tree_tests, insert_single_leaf_node) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  const char* key = "key1";
  updates.emplace(Sliver(key), Sliver("val1"));
  auto batch = tree.update(updates);

  // There were no stale nodes since this is the first insert
  ASSERT_EQ(Version(1), batch.stale.stale_since_version);
  ASSERT_EQ(0, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());

  // There's a single InternalNode and a single LeafNode that needs to be
  // written to the database at version 1.
  ASSERT_EQ(1, batch.internal_nodes.size());
  ASSERT_EQ(1, batch.leaf_nodes.size());

  // Only a single node was created. It's key is made up of an empty nibble
  // path, since there are no collisions requring going deeper into the tree.
  auto& root = batch.internal_nodes.at(0);
  ASSERT_EQ(InternalNodeKey(1, NibblePath()), root.first);

  // There should be a root and a single leaf
  ASSERT_EQ(2, root.second.numChildren());
  ASSERT_EQ(1, root.second.numInternalChildren());
  ASSERT_EQ(1, root.second.numLeafChildren());
  ASSERT_EQ(Version(1), root.second.version());

  // The leaf key should contain the right hash and version.
  Hasher hasher;
  auto expected = LeafKey(hasher.hash(key, strlen(key)), Version(1));
  ASSERT_EQ(expected, batch.leaf_nodes[0].first);

  // The hash should have been updated.
  ASSERT_NE(PLACEHOLDER_HASH, root.second.hash());
}

// Insert multiple leaves that create a single BatchedInternalNode since
// the first nibbles of their key's hashes are not equal.
TEST(tree_tests, insert_multiple_create_single_batched) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key1"), Sliver("val1"));
  updates.emplace(Sliver("key2"), Sliver("val2"));
  auto batch = tree.update(updates);

  // There were no stale nodes since this is the first insert
  ASSERT_EQ(Version(1), batch.stale.stale_since_version);
  ASSERT_EQ(0, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());

  // There should be a single internal node (the root node) since the first
  // nibbles differ between key1 and key2.
  ASSERT_EQ(1, batch.internal_nodes.size());
  ASSERT_EQ(2, batch.leaf_nodes.size());

  auto& root = batch.internal_nodes.at(0);
  ASSERT_EQ(Version(1), root.second.version());
  ASSERT_NE(PLACEHOLDER_HASH, root.second.hash());

  // There should be two leafs in the root
  ASSERT_EQ(2, root.second.numLeafChildren());

  // Both leaf nodes should be at version 1
  assert_node_version(1, batch.leaf_nodes);
}

// Insert multiple leaves that create multiple BatchedInternalNode since
// the first nibbles of their key's hashes are equal.
TEST(tree_tests, insert_multiple_create_multiple_batched) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  auto batch = tree.update(updates);

  // There were no stale nodes since this is the first insert
  ASSERT_EQ(Version(1), batch.stale.stale_since_version);
  ASSERT_EQ(0, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());

  // There should be two internal nodes since the first nibbles differ between
  // key3 and key9, but the second nibbles differ.
  ASSERT_EQ(2, batch.internal_nodes.size());
  ASSERT_EQ(2, batch.leaf_nodes.size());

  auto& root = batch.internal_nodes.at(0);
  ASSERT_EQ(Version(1), root.second.version());
  ASSERT_NE(PLACEHOLDER_HASH, root.second.hash());

  // There should be no leafs in the root
  ASSERT_EQ(0, root.second.numLeafChildren());

  // There should be two leafs in the second node
  auto& second_node = batch.internal_nodes.at(1);
  ASSERT_EQ(Version(1), second_node.second.version());
  ASSERT_EQ(2, second_node.second.numLeafChildren());

  // Both leaf nodes should be at version 1
  assert_node_version(1, batch.leaf_nodes);
}

// Perform multiple updates to ensure that we only get back nodes relevant to
// the latest version and that stale indexes are appropriately returned.
TEST(tree_tests, multiple_updates) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  auto batch = tree.update(updates);

  // We must save the output nodes to the database. This is what users of the tree
  // must do. We can ignore saving the stale nodes for now.
  db_put(db, batch);

  // There were no stale nodes since this is the first insert
  ASSERT_EQ(Version(1), batch.stale.stale_since_version);
  ASSERT_EQ(0, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());

  // There should be 2 internal nodes and 2 leaf nodes
  ASSERT_EQ(2, batch.internal_nodes.size());
  ASSERT_EQ(2, batch.leaf_nodes.size());
  assert_node_version(1, batch.internal_nodes);
  assert_node_version(1, batch.leaf_nodes);

  // Insert a new key
  updates.clear();
  updates.emplace(Sliver("key1"), Sliver("val3"));
  batch = tree.update(updates);
  db_put(db, batch);

  // The root should be stale, since we added a new key, and it always updates
  // the root at a minimum.
  ASSERT_EQ(Version(2), batch.stale.stale_since_version);
  ASSERT_EQ(1, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());
  assert_version(1, batch.stale.internal_keys);

  // There should be 1 leaf node at version 2
  ASSERT_EQ(1, batch.leaf_nodes.size());
  assert_node_version(2, batch.leaf_nodes);

  // Insert an update to a key.
  updates.clear();
  updates.emplace(Sliver("key3"), Sliver("val5"));
  batch = tree.update(updates);
  db_put(db, batch);

  // There should be 2 stale internal nodes and a stale leaf node
  ASSERT_EQ(Version(3), batch.stale.stale_since_version);
  ASSERT_EQ(2, batch.stale.internal_keys.size());
  ASSERT_EQ(1, batch.stale.leaf_keys.size());

  // The root node was updated on the second update. But the next
  // BatchedInternalNode down wasn't, since the udpate inserted into the root
  // node.
  ASSERT_EQ(Version(2), batch.stale.internal_keys[0].version());
  ASSERT_EQ(Version(1), batch.stale.internal_keys[1].version());

  assert_version(1, batch.stale.leaf_keys);

  // There should two new internal nodes and one new leaf node
  ASSERT_EQ(2, batch.internal_nodes.size());
  ASSERT_EQ(1, batch.leaf_nodes.size());
  assert_node_version(3, batch.internal_nodes);
  assert_node_version(3, batch.leaf_nodes);

  // Inserting a new leaf that matches the first 2 nibbles of an existing key
  // should trigger 3 new internal nodes, a new leaf node, and 2 stale internal
  // nodes.
  // This key was experimentally found by brute force. The first two nibbles
  // match Hash("key9").
  updates.clear();
  updates.emplace(Sliver("key191"), Sliver("val6"));
  batch = tree.update(updates);
  db_put(db, batch);

  ASSERT_EQ(3, batch.internal_nodes.size());
  ASSERT_EQ(1, batch.leaf_nodes.size());
  assert_node_version(4, batch.internal_nodes);
  assert_node_version(4, batch.leaf_nodes);

  // The leaf node should have the key for 191 only and version 4.
  Hasher hasher;
  auto expected = LeafKey(hasher.hash("key191", strlen("key191")), Version(4));
  ASSERT_EQ(expected, batch.leaf_nodes[0].first);

  // The first internal node should have 1 LeafChild ("key1"), the second should have 1
  // LeafChild ("key9"), and the third should have 2 LeafChild(ren) ("key3" and
  // "key191");
  BatchedInternalNode node1 = batch.internal_nodes[0].second;
  BatchedInternalNode node2 = batch.internal_nodes[1].second;
  BatchedInternalNode node3 = batch.internal_nodes[2].second;
  ASSERT_EQ(1, node1.numLeafChildren());
  ASSERT_EQ(1, node2.numLeafChildren());
  ASSERT_EQ(2, node3.numLeafChildren());

  // The paths of the node keys in order should be "", "b", "ba"
  ASSERT_EQ("", batch.internal_nodes[0].first.path().toString());
  ASSERT_EQ("b", batch.internal_nodes[1].first.path().toString());
  ASSERT_EQ("ba", batch.internal_nodes[2].first.path().toString());

  // The current version is 4, so stale nodes are stale since 4
  // We aren't overwriting any leaf nodes, so there are no stale leafs. We did
  // however update below node "b", so both node "b" and the root ("") must have
  // be updated.
  ASSERT_EQ(Version(4), batch.stale.stale_since_version);
  ASSERT_EQ(2, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());

  // The paths of the stale node keys should be "" and "b"
  ASSERT_EQ("", batch.stale.internal_keys[0].path().toString());
  ASSERT_EQ("b", batch.stale.internal_keys[1].path().toString());

  // Both the root and node with path "b" should be stale at version 3 since the
  // last update ovewrote a key at path "b".
  assert_version(3, batch.stale.internal_keys);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
