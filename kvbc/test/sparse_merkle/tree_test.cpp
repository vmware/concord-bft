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

#include <memory>
#include <set>

#include "gtest/gtest.h"
#include "sparse_merkle/tree.h"
#include "test_db.h"

#include <iostream>
using namespace std;

using namespace concordUtils;
using namespace concord::kvbc;
using namespace concord::kvbc::sparse_merkle;

void db_put(const shared_ptr<TestDB>& db, const UpdateBatch& batch) {
  for (const auto& [key, val] : batch.internal_nodes) {
    db->put(key, val);
  }
  for (const auto& [key, val] : batch.leaf_nodes) {
    db->put(key, val);
  }
}

template <typename T>
::testing::AssertionResult keyVersionMatches(uint64_t version, const T& keys) {
  for (auto& key : keys) {
    if (Version(version) != key.version()) {
      return ::testing::AssertionFailure() << version << " != key.version(): " << key.version().toString();
    }
  }
  return ::testing::AssertionSuccess();
}

template <typename K, typename V>
std::vector<K> keys(const std::vector<std::pair<K, V>>& kvpairs) {
  std::vector<K> rv;
  for (auto& [key, _] : kvpairs) {
    (void)_;  // unused variable hack
    rv.push_back(key);
  }
  return rv;
}

::testing::AssertionResult internalNodeVersionMatches(
    uint64_t version, std::vector<std::pair<InternalNodeKey, BatchedInternalNode>>& nodes) {
  for (auto& [key, node] : nodes) {
    if (Version(version) != key.version()) {
      return ::testing::AssertionFailure() << version << " != key.version(): " << key.version().toString();
    }
    if (Version(version) != node.version()) {
      return ::testing::AssertionFailure() << version << " != node.version(): " << node.version().toString();
    }
  }
  return ::testing::AssertionSuccess();
}

::testing::AssertionResult hashMatches(const char* key, const Hash& expected) {
  Hasher hasher;
  auto hash = hasher.hash(key, strlen(key));
  if (hash != expected) {
    return ::testing::AssertionFailure() << "key: " << key << " hashed to " << hash.toString()
                                         << ", expected: " << expected.toString();
  }
  return ::testing::AssertionSuccess();
}

::testing::AssertionResult internalKeyExists(const char* path,
                                             uint64_t version,
                                             const std::set<InternalNodeKey>& keys) {
  for (const auto& key : keys) {
    if (key.path().toString() == path && Version(version) == key.version()) {
      return ::testing::AssertionSuccess();
    }
  }
  return ::testing::AssertionFailure();
}

::testing::AssertionResult leafKeyMatches(const char* key, uint64_t version, const LeafKey& expected) {
  Hasher hasher;
  auto hash = hasher.hash(key, strlen(key));
  auto leaf_key = LeafKey(hash, Version(version));
  if (leaf_key != expected) {
    return ::testing::AssertionFailure() << "Generated leaf key for key: " << key << ", " << leaf_key.toString()
                                         << " did not match expected: " << expected.toString();
  }
  return ::testing::AssertionSuccess();
}

::testing::AssertionResult leafKeyExists(const char* key,
                                         uint64_t version,
                                         const std::vector<std::pair<LeafKey, LeafNode>>& kvpairs) {
  for (const auto& [k, _] : kvpairs) {
    (void)_;  // unused variable hack
    auto result = leafKeyMatches(key, version, k);
    if (result == ::testing::AssertionSuccess()) {
      return result;
    }
  }
  return ::testing::AssertionFailure();
}

::testing::AssertionResult leafKeyExists(const char* key, uint64_t version, const std::set<LeafKey>& keys) {
  for (const auto& k : keys) {
    auto result = leafKeyMatches(key, version, k);
    if (result == ::testing::AssertionSuccess()) {
      return result;
    }
  }
  return ::testing::AssertionFailure();
}

// Assert that there exists a link from an InternalChild in `linker_node` to `linkee_node`.
// For the link to be valid, the version and hashes must match.
::testing::AssertionResult validLinkExists(const InternalNodeKey& linker_key,
                                           const BatchedInternalNode& linker_node,
                                           const InternalNodeKey& linkee_key,
                                           const BatchedInternalNode& linkee_node) {
  auto depth_of_linker = linker_key.path().length();
  Nibble key_inside_linker = linkee_key.path().get(depth_of_linker);
  InternalChild child_inside_linker =
      std::get<InternalChild>(linker_node.children()[linker_node.nibbleToIndex(key_inside_linker)].value());
  if (linkee_node.hash() != child_inside_linker.hash || linkee_node.version() != child_inside_linker.version) {
    return ::testing::AssertionFailure() << "Linkee hash=" << linkee_node.hash().toString()
                                         << ", version=" << linkee_node.version().toString()
                                         << " child inside linker hash=" << child_inside_linker.hash.toString()
                                         << ", version=" << child_inside_linker.version.toString();
  }
  return ::testing::AssertionSuccess();
}

bool leafChildExists(const char* key, uint64_t version, const BatchedInternalNode& node) {
  auto& children = node.children();
  return children.end() != std::find_if(children.begin(), children.end(), [&](const std::optional<Child>& c) -> bool {
           if (c && std::holds_alternative<LeafChild>(c.value())) {
             return ::testing::AssertionSuccess() == leafKeyMatches(key, version, std::get<LeafChild>(c.value()).key);
           }
           return false;
         });
}

void debug_print(const UpdateBatch& batch) {
  cout << "stale keys:" << endl;
  for (auto& k : batch.stale.internal_keys) {
    cout << "    " << k.toString() << endl;
  }
  cout << "new internal node keys:" << endl;
  for (auto& [k, _] : batch.internal_nodes) {
    (void)_;  // unused variable hack
    cout << "    " << k.toString() << endl;
  }
}

// Ensure that getting the latest root from an empty db returns an empty
// BatchedInternalNode
TEST(tree_tests, empty_db) {
  TestDB db;
  auto root = db.get_latest_root();
  ASSERT_EQ(Version(0), root.version());
  ASSERT_EQ(0, root.numChildren());
  ASSERT_EQ(PLACEHOLDER_HASH, root.hash());
}

// Ensure that the tree is in an empty state when used with an empty db
TEST(tree_tests, empty_db_from_tree) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  ASSERT_EQ(Version(0), tree.get_version());
  ASSERT_EQ(PLACEHOLDER_HASH, tree.get_root_hash());
  ASSERT_TRUE(tree.empty());
}

TEST(tree_tests, tree_version_on_update) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  const SetOfKeyValuePairs updates{std::make_pair(Sliver("key"), Sliver("val"))};
  tree.update(updates);
  ASSERT_EQ(Version(1), tree.get_version());
}

TEST(tree_tests, tree_state_on_deleting_all_keys) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  const auto key = "key";
  const SetOfKeyValuePairs updates{std::make_pair(Sliver(key), Sliver("val"))};
  auto batch = tree.update(updates);
  db_put(db, batch);
  ASSERT_EQ(Version(1), tree.get_version());
  const KeysVector deletes{Sliver(key)};
  batch = tree.remove(deletes);

  // Make sure the tree version is 2 and the root hash is the placeholder hash.
  ASSERT_EQ(Version(2), tree.get_version());
  ASSERT_EQ(PLACEHOLDER_HASH, tree.get_root_hash());
  ASSERT_FALSE(tree.empty());

  // Make sure that the empty root node is persisted.
  ASSERT_EQ(1, batch.internal_nodes.size());
  const auto& [root_key, root_node] = batch.internal_nodes[0];
  ASSERT_EQ(Version(2), root_key.version());
  ASSERT_TRUE(root_key.path().empty());
  ASSERT_EQ(1, root_node.numInternalChildren());
  ASSERT_EQ(0, root_node.numLeafChildren());
}

TEST(tree_tests, stale_index_on_deleting_all_keys_and_adding_after_that) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  const auto key = "key";
  const SetOfKeyValuePairs updates{std::make_pair(Sliver(key), Sliver("val"))};
  auto batch = tree.update(updates);
  db_put(db, batch);
  const KeysVector deletes{Sliver(key)};
  batch = tree.remove(deletes);
  db_put(db, batch);
  batch = tree.update(updates);

  // Make sure the empty root is now stale since version 3.
  ASSERT_EQ(Version(3), batch.stale.stale_since_version);
  ASSERT_EQ(1, batch.stale.internal_keys.size());
  const auto root_key = *batch.stale.internal_keys.begin();
  ASSERT_TRUE(root_key.path().empty());
  ASSERT_EQ(Version(2), root_key.version());

  // Make sure there are no stale leaf keys.
  ASSERT_TRUE(batch.stale.leaf_keys.empty());
}

TEST(tree_tests, tree_version_on_update_and_delete) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  const auto key1 = "key1";
  const auto key2 = "key2";
  const SetOfKeyValuePairs updates1{std::make_pair(Sliver(key1), Sliver("val1"))};
  auto batch = tree.update(updates1);
  db_put(db, batch);
  ASSERT_EQ(Version(1), tree.get_version());
  const SetOfKeyValuePairs updates2{std::make_pair(Sliver(key2), Sliver("val2"))};
  const KeysVector deletes{Sliver(key1)};
  tree.update(updates2, deletes);
  ASSERT_EQ(Version(2), tree.get_version());
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
  ASSERT_TRUE(keyVersionMatches(1, keys(batch.leaf_nodes)));
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

  auto& [root_key, root_node] = batch.internal_nodes.at(0);
  ASSERT_EQ(Version(1), root_node.version());
  ASSERT_NE(PLACEHOLDER_HASH, root_node.hash());

  // There should be no leafs in the root
  ASSERT_EQ(0, root_node.numLeafChildren());

  // There should be two leafs in the second node
  auto& [second_key, second_node] = batch.internal_nodes.at(1);
  ASSERT_EQ(Version(1), second_node.version());
  ASSERT_EQ(2, second_node.numLeafChildren());

  // The root node properly links to the second_node
  ASSERT_TRUE(validLinkExists(root_key, root_node, second_key, second_node));

  // Both leaf nodes should be at version 1
  ASSERT_TRUE(keyVersionMatches(1, keys(batch.leaf_nodes)));
}

// Perform multiple updates to ensure that we only get back nodes relevant to
// the latest version and that stale indexes are appropriately returned.
TEST(tree_tests, multiple_updates) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;

  // These keys overlap in the first nibble
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
  ASSERT_TRUE(internalNodeVersionMatches(1, batch.internal_nodes));
  ASSERT_TRUE(keyVersionMatches(1, keys(batch.leaf_nodes)));

  // Insert a new key that has a different first nibble from the prior 2 keys
  updates.clear();
  updates.emplace(Sliver("key1"), Sliver("val3"));
  batch = tree.update(updates);
  db_put(db, batch);

  // The root should be stale, since we added a new key, and it always updates
  // the root at a minimum.
  ASSERT_EQ(Version(2), batch.stale.stale_since_version);
  ASSERT_EQ(1, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());
  ASSERT_TRUE(keyVersionMatches(1, batch.stale.internal_keys));

  // There should be 1 leaf node at version 2
  ASSERT_EQ(1, batch.leaf_nodes.size());
  ASSERT_TRUE(keyVersionMatches(2, keys(batch.leaf_nodes)));

  // Insert an update to a key.
  updates.clear();
  updates.emplace(Sliver("key3"), Sliver("val5"));
  batch = tree.update(updates);
  db_put(db, batch);

  // There should be 2 stale internal nodes and a stale leaf node
  ASSERT_EQ(Version(3), batch.stale.stale_since_version);
  ASSERT_EQ(2, batch.stale.internal_keys.size());
  ASSERT_EQ(1, batch.stale.leaf_keys.size());

  // The root node was updated on the insert of key1, but the next
  // BatchedInternalNode down wasn't, since key1 was inserted into the root
  // node.
  //
  // Note that since stale nodes are added while ascending the tree, they are
  // returned in a bottom up order.
  ASSERT_TRUE(internalKeyExists("b", 1, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("", 2, batch.stale.internal_keys));

  ASSERT_TRUE(keyVersionMatches(1, batch.stale.leaf_keys));

  // There should two new internal nodes and one new leaf node
  ASSERT_EQ(2, batch.internal_nodes.size());
  ASSERT_EQ(1, batch.leaf_nodes.size());
  ASSERT_TRUE(internalNodeVersionMatches(3, batch.internal_nodes));
  ASSERT_TRUE(keyVersionMatches(3, keys(batch.leaf_nodes)));

  // The root node properly links to the second_node
  auto& [root_key, root_node] = batch.internal_nodes[0];
  auto& [second_key, second_node] = batch.internal_nodes[1];
  ASSERT_TRUE(validLinkExists(root_key, root_node, second_key, second_node));

  // Inserting a new leaf that matches the first 2 nibbles of an existing key
  // should trigger 3 new internal nodes, a new leaf node, and 2 stale internal
  // nodes.
  // This key was experimentally found by brute force. The first two nibbles
  // match Hash("key3").
  updates.clear();
  updates.emplace(Sliver("key191"), Sliver("val6"));
  batch = tree.update(updates);
  db_put(db, batch);

  ASSERT_EQ(3, batch.internal_nodes.size());
  ASSERT_EQ(1, batch.leaf_nodes.size());
  ASSERT_TRUE(internalNodeVersionMatches(4, batch.internal_nodes));
  ASSERT_TRUE(keyVersionMatches(4, keys(batch.leaf_nodes)));

  // The leaf node should have the key for 191 only and version 4.
  Hasher hasher;
  auto expected = LeafKey(hasher.hash("key191", strlen("key191")), Version(4));
  ASSERT_EQ(expected, batch.leaf_nodes[0].first);

  // The first internal node should have 1 LeafChild ("key1"), the second should have 1
  // LeafChild ("key9"), and the third should have 2 LeafChild(ren) ("key3" and
  // "key191");
  auto& [key1, node1] = batch.internal_nodes[0];
  auto& [key2, node2] = batch.internal_nodes[1];
  auto& [key3, node3] = batch.internal_nodes[2];
  ASSERT_EQ(1, node1.numLeafChildren());
  ASSERT_EQ(1, node2.numLeafChildren());
  ASSERT_EQ(2, node3.numLeafChildren());

  // The paths of the node keys in order should be "", "b", "ba"
  ASSERT_EQ("", key1.path().toString());
  ASSERT_EQ("b", key2.path().toString());
  ASSERT_EQ("ba", key3.path().toString());

  // The nodes should linke to each other
  ASSERT_TRUE(validLinkExists(key1, node1, key2, node2));
  ASSERT_TRUE(validLinkExists(key2, node2, key3, node3));

  // The current version is 4, so stale nodes are stale since 4
  // We aren't overwriting any leaf nodes, so there are no stale leafs. We did
  // however update below node "b", so both node "b" and the root ("") must have
  // be updated.
  ASSERT_EQ(Version(4), batch.stale.stale_since_version);
  ASSERT_EQ(2, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());

  // The paths of the stale node keys should be "b" and "" (bottom up order),
  // and they should exist at version 3.
  ASSERT_TRUE(internalKeyExists("b", 3, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("", 3, batch.stale.internal_keys));
}

// Insert 2 keys at the root and remove one of them in a second update. The
// remove should return a stale root node and an updated root node.
TEST(tree_tests, remove_complete_at_root_test) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key1"), Sliver("val1"));
  updates.emplace(Sliver("key2"), Sliver("val2"));
  auto batch = tree.update(updates);
  // There should be two leafs in the root
  ASSERT_EQ(2, batch.internal_nodes[0].second.numLeafChildren());
  db_put(db, batch);

  // Delete one of the children
  KeysVector deletes{Sliver("key1")};
  batch = tree.remove(deletes);
  db_put(db, batch);

  // The root should only have one leaf child, and should be the only internal
  // node that needs writing.
  ASSERT_EQ(1, batch.internal_nodes.size());
  ASSERT_EQ(0, batch.leaf_nodes.size());
  auto& [root_key, root_node] = batch.internal_nodes[0];
  ASSERT_EQ(Version(2), root_key.version());
  ASSERT_EQ(Version(2), root_node.version());
  ASSERT_EQ(1, root_node.numLeafChildren());

  // The old root node is the only stale internal node. The deleted LeafKey at
  // the version 1 should match the hash of "key1".
  ASSERT_EQ(Version(2), batch.stale.stale_since_version);
  ASSERT_EQ(1, batch.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 1, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key1", 1, batch.stale.leaf_keys));
}

// Insert 2 keys that match in the first Nibble, and are inserted at the same
// InternalNode at depth 1. Deleting one of them should cause the other one to
// be promoted, and reside in the root. The root is the only new InternalNodeKey
// to be written.
TEST(tree_tests, remove_and_promote) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  auto batch1 = tree.update(updates);

  // There should be 2 internal nodes, with no leaf children in the root node.
  ASSERT_EQ(2, batch1.internal_nodes.size());
  auto& root_node = batch1.internal_nodes[0].second;
  ASSERT_EQ(0, root_node.numLeafChildren());
  db_put(db, batch1);

  // Delete one of these nodes and cause the other to be promoted.
  KeysVector deletes{Sliver("key9")};
  auto batch2 = tree.remove(deletes);

  // Only the root node exists in the tree now, and it's at version 2.
  ASSERT_EQ(1, batch2.internal_nodes.size());
  auto& [root_key, remove_root_node] = batch2.internal_nodes[0];
  ASSERT_EQ("", root_key.path().toString());
  ASSERT_EQ(Version(2), root_key.version());
  ASSERT_EQ(Version(2), remove_root_node.version());

  // There are no leaf updates, since key3 was not updated. A pointer
  // (LeafChild) was just moved to the root node.
  ASSERT_EQ(0, batch2.leaf_nodes.size());
  ASSERT_EQ(1, remove_root_node.numLeafChildren());
  ASSERT_TRUE(leafChildExists("key3", 1, remove_root_node));

  ASSERT_EQ(1, batch2.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key9", 1, batch2.stale.leaf_keys));

  // Ensure that the bottom most internal node was removed
  ASSERT_EQ(2, batch2.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 1, batch2.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("b", 1, batch2.stale.internal_keys));

  // The hash of the old root should differ from the hash of the new root
  ASSERT_NE(root_node.hash(), remove_root_node.hash());
}

// Add 3 nodes such that they all overlap in the first nibble, and two overlap in
// the second nibble. Thus 1 LeafChild resides at depth 1, and the others reside at depth 2
// after insert. Removing one of the nodes at depth 2 should cause a promotion
// to depth 1 such that two LeafChildren live at depth 1.
TEST(tree_tests, remove_promoted_to_depth_1) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  updates.emplace(Sliver("key191"), Sliver("val3"));
  auto batch = tree.update(updates);

  // There should be 3 internal nodes, with no leaf children in the root node, 1
  // leaf child in the second node, and 2 leaf children in the 3rd node.
  ASSERT_EQ(3, batch.internal_nodes.size());
  auto& [key1, node1] = batch.internal_nodes[0];
  auto& [key2, node2] = batch.internal_nodes[1];
  auto& [key3, node3] = batch.internal_nodes[2];

  ASSERT_EQ(0, node1.numLeafChildren());
  ASSERT_EQ(1, node2.numLeafChildren());
  ASSERT_EQ(2, node3.numLeafChildren());

  // The nodes should link to each other
  ASSERT_TRUE(validLinkExists(key1, node1, key2, node2));
  ASSERT_TRUE(validLinkExists(key2, node2, key3, node3));
  db_put(db, batch);

  // Delete one of the nodes at depth 2, in order to trigger promotion.
  KeysVector deletes{Sliver("key191")};
  batch = tree.remove(deletes);

  // There is a root node and a node at depth 1 that have been updated.
  ASSERT_EQ(2, batch.internal_nodes.size());
  auto& [root_key, root_node] = batch.internal_nodes[0];
  auto& [depth1_key, depth1_node] = batch.internal_nodes[1];
  ASSERT_TRUE(internalNodeVersionMatches(2, batch.internal_nodes));
  ASSERT_EQ("", root_key.path().toString());
  ASSERT_EQ("b", depth1_key.path().toString());

  ASSERT_TRUE(validLinkExists(root_key, root_node, depth1_key, depth1_node));

  // There are no leaf updates.
  ASSERT_EQ(0, batch.leaf_nodes.size());

  // The root node still has zero leaf children and the node at depth 1 has 2
  // children after removal and promotion.
  //
  // Note that the version number of these children is still 1, as they were
  // written in the initial update. The versions of the internal children above
  // them, including the root of the BatchedInternalnode, have all had their
  // versions bumped to indicate the update.
  ASSERT_EQ(0, root_node.numLeafChildren());
  ASSERT_EQ(2, depth1_node.numLeafChildren());
  ASSERT_TRUE(leafChildExists("key3", 1, depth1_node));
  ASSERT_TRUE(leafChildExists("key9", 1, depth1_node));

  // There are 3 old internal keys (root, depth 1, depth 2) and one stale leaf
  // ("key191").
  ASSERT_EQ(Version(2), batch.stale.stale_since_version);
  ASSERT_EQ(3, batch.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 1, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("b", 1, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("ba", 1, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key191", 1, batch.stale.leaf_keys));
}

// Add 3 nodes such that they all overlap in the first nibble, and two overlap
// in the second nibble. Thus 1 LeafChild resides at depth 1, and the others at
// depth 2 after insert. Removing the LeafChild at depth1 should not cause a
// promotion to depth 1 for the other 2 nodes because they overlap in 2 nibbles,
// and therefore share internal nodes at depth 0 and 1 that prohibit promotion.
TEST(tree_tests, remove_no_promotion_to_depth_1) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  updates.emplace(Sliver("key191"), Sliver("val3"));
  auto batch = tree.update(updates);

  // There should be 3 internal nodes, with no leaf children in the root node, 1
  // leaf child in the second node, and 2 leaf children in the 3rd node.
  ASSERT_EQ(3, batch.internal_nodes.size());
  ASSERT_EQ(0, batch.internal_nodes[0].second.numLeafChildren());
  ASSERT_EQ(1, batch.internal_nodes[1].second.numLeafChildren());
  ASSERT_EQ(2, batch.internal_nodes[2].second.numLeafChildren());
  db_put(db, batch);

  // Delete the LeafChild in the node at depth1. This should not trigger any promotion.
  KeysVector deletes{Sliver("key9")};
  batch = tree.remove(deletes);

  // Only the first two nodes have been updated.
  ASSERT_EQ(2, batch.internal_nodes.size());
  ASSERT_TRUE(internalNodeVersionMatches(2, batch.internal_nodes));
  auto& [root_key, root_node] = batch.internal_nodes[0];
  auto& [depth1_key, depth1_node] = batch.internal_nodes[1];
  ASSERT_EQ("", root_key.path().toString());
  ASSERT_EQ("b", depth1_key.path().toString());
  ASSERT_TRUE(validLinkExists(root_key, root_node, depth1_key, depth1_node));

  // There are no leaf updates.
  ASSERT_EQ(0, batch.leaf_nodes.size());

  // The root node still has zero leaf children and the node at depth one has
  // zero leaf children.
  //
  // Note that the version number of these children is still 1, as they were
  // written in the initial update. The versions of the internal children above
  // them, including the root of the BatchedInternalnode, have all had their
  // versions bumped to indicate the update.
  ASSERT_EQ(0, root_node.numLeafChildren());
  ASSERT_EQ(0, depth1_node.numLeafChildren());

  // There are 2 old internal keys (root, depth 1) and one stale leaf
  ASSERT_EQ(Version(2), batch.stale.stale_since_version);
  ASSERT_EQ(2, batch.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 1, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("b", 1, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key9", 1, batch.stale.leaf_keys));
}

// Add 4 nodes that overlap in the first nibble, with 2 nodes also matching in
// one second nibble, and 2 nodes matching in the other second nibble. Thus the
// tree of BatchedInternalNodes will look like the following:
//
//                Root
//                 |
//                "b"
//                 |
//           -------------
//           |           |
//         "ba"        "be"
//
// Each of "ba" and "be" contain 2 leaf children. Deleting a leaf child from "be"
// should cause the other leaf child to go to "b", but leave "ba" alone. Thus
// after the delete, the tree should look like the following:
//
//                Root
//                 |
//                "b"
//                 |
//                "ba"
//
TEST(tree_tests, remove_left_node_and_promote_leaving_right_node_alone) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  updates.emplace(Sliver("key191"), Sliver("val3"));
  updates.emplace(Sliver("key462"), Sliver("val4"));
  auto batch = tree.update(updates);
  db_put(db, batch);

  // There should be 4 internal nodes, with no leaf children in the root node (""), no
  // leaf children in the second node ("b"), and 2 leaf children in the bottom nodes ("ba", "be").
  ASSERT_EQ(4, batch.internal_nodes.size());
  auto& [key1, node1] = batch.internal_nodes[0];
  auto& [key2, node2] = batch.internal_nodes[1];
  auto& [key3, node3] = batch.internal_nodes[2];
  auto& [key4, node4] = batch.internal_nodes[3];
  ASSERT_EQ(0, node1.numLeafChildren());
  ASSERT_EQ(0, node2.numLeafChildren());
  ASSERT_EQ(2, node3.numLeafChildren());
  ASSERT_EQ(2, node4.numLeafChildren());
  ASSERT_EQ("", key1.path().toString());
  ASSERT_EQ("b", key2.path().toString());
  ASSERT_EQ("ba", key3.path().toString());
  ASSERT_EQ("be", key4.path().toString());
  ASSERT_TRUE(validLinkExists(key1, node1, key2, node2));
  ASSERT_TRUE(validLinkExists(key2, node2, key3, node3));
  ASSERT_TRUE(validLinkExists(key2, node2, key4, node4));

  // Delete a LeafChild at "be". This should should trigger a promotion to "b";
  KeysVector deletes{Sliver("key9")};
  batch = tree.remove(deletes);

  // Root and "b" have been updated.
  ASSERT_EQ(2, batch.internal_nodes.size());
  ASSERT_TRUE(internalNodeVersionMatches(2, batch.internal_nodes));
  auto& [root_key, root_node] = batch.internal_nodes[0];
  auto& [depth1_key, depth1_node] = batch.internal_nodes[1];
  ASSERT_EQ("", root_key.path().toString());
  ASSERT_EQ("b", depth1_key.path().toString());
  ASSERT_TRUE(validLinkExists(root_key, root_node, depth1_key, depth1_node));

  // There are no leaf updates.
  ASSERT_EQ(0, batch.leaf_nodes.size());

  // The root node still has zero leaf children and the node at depth one ("b"),
  // has one promoted LeafChild.
  ASSERT_EQ(0, root_node.numLeafChildren());
  ASSERT_EQ(1, depth1_node.numLeafChildren());
  ASSERT_TRUE(leafChildExists("key462", 1, depth1_node));

  // There are 3 old internal keys (root, "b", "be") and one stale leaf
  ASSERT_EQ(Version(2), batch.stale.stale_since_version);
  ASSERT_EQ(3, batch.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 1, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("b", 1, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("be", 1, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key9", 1, batch.stale.leaf_keys));
}

// This is essentially a combination of the prior 3 tests in succession.
// We omit the checks after the initial delete since they are done in the prior test.
TEST(tree_tests, successive_removes) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  updates.emplace(Sliver("key191"), Sliver("val3"));
  updates.emplace(Sliver("key462"), Sliver("val4"));
  auto batch = tree.update(updates);
  db_put(db, batch);

  // Delete a LeafChild at "be". This should should trigger a promotion to "b";
  KeysVector deletes{Sliver("key9")};
  batch = tree.remove(deletes);
  db_put(db, batch);

  // The tree looks like this now:
  //                Root
  //                 |
  //                "b"
  //                 |
  //                "ba"
  //
  // Deleting the last leaf node from "b" (key462), should not cause this tree
  // to change, since there is a collision that forces the "ba" node to be
  // created. In other words there is an InternalLeaf at level 0 of
  // BatchedInternalNode "b".
  KeysVector deletes2{Sliver("key462")};
  batch = tree.remove(deletes2);
  db_put(db, batch);

  // "" and "b" should be updated.
  ASSERT_EQ(2, batch.internal_nodes.size());
  auto& [root_key, root_node] = batch.internal_nodes[0];
  auto& [b_key, b_node] = batch.internal_nodes[1];
  ASSERT_EQ("", root_key.path().toString());
  ASSERT_EQ("b", b_key.path().toString());
  ASSERT_EQ(0, root_node.numLeafChildren());
  ASSERT_EQ(0, b_node.numLeafChildren());

  // The root node properly links to the b_node
  ASSERT_TRUE(validLinkExists(root_key, root_node, b_key, b_node));

  // There are 2 old internal keys (root, "b") and one stale leaf
  ASSERT_EQ(Version(3), batch.stale.stale_since_version);
  ASSERT_EQ(2, batch.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 2, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("b", 2, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key462", 1, batch.stale.leaf_keys));

  // The tree looks still like this:
  //                Root
  //                 |
  //                "b"
  //                 |
  //                "ba"
  //
  // Removing one of the last two remaining keys will cause a promotion of the
  // other all the way to the root.
  KeysVector deletes3{Sliver("key3")};
  batch = tree.remove(deletes3);
  db_put(db, batch);

  // There is a new version of the root only.
  ASSERT_EQ(1, batch.internal_nodes.size());
  auto& [root_key2, root_node2] = batch.internal_nodes[0];
  ASSERT_EQ("", root_key2.path().toString());

  // There is only one leaf and the root
  ASSERT_EQ(1, root_node2.numLeafChildren());
  ASSERT_EQ(1, root_node2.numInternalChildren());

  // There are 3 old internal keys (root, "b", and "ba") and one stale leaf
  ASSERT_EQ(Version(4), batch.stale.stale_since_version);
  ASSERT_EQ(3, batch.stale.internal_keys.size());
  // "ba" was last updated at version 1, "b" and "" at version 3
  ASSERT_TRUE(internalKeyExists("", 3, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("b", 3, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("ba", 1, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key3", 1, batch.stale.leaf_keys));

  // Removing the last key should remove the root and result in no new nodes.
  KeysVector deletes4{Sliver("key191")};
  batch = tree.remove(deletes4);
  db_put(db, batch);

  // Removing the last key should make the tree persist an empty root with the current version.
  ASSERT_EQ(1, batch.internal_nodes.size());
  const auto& [internal_key, internal_node] = batch.internal_nodes[0];
  ASSERT_EQ(Version(5), internal_key.version());
  ASSERT_EQ(NibblePath(), internal_key.path());
  ASSERT_EQ(Version(5), internal_node.version());
  ASSERT_EQ(1, internal_node.numInternalChildren());
  ASSERT_EQ(0, internal_node.numLeafChildren());
  ASSERT_EQ(0, batch.leaf_nodes.size());

  // Removing the last key should update the tree version. Additionally, the root hash should be the empty hash.
  ASSERT_EQ(Version(5), tree.get_version());
  ASSERT_EQ(PLACEHOLDER_HASH, tree.get_root_hash());

  ASSERT_EQ(Version(5), batch.stale.stale_since_version);
  ASSERT_EQ(1, batch.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 4, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key191", 1, batch.stale.leaf_keys));
}

// Test that removing a key that doesn't exist doesn't modify the tree.
TEST(tree_tests, test_remove_not_found) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  updates.emplace(Sliver("key462"), Sliver("val4"));
  auto batch = tree.update(updates);
  db_put(db, batch);

  // Delete the LeafChild in the node at depth1. This should not trigger any promotion.
  KeysVector deletes{Sliver("key191")};
  batch = tree.remove(deletes);

  ASSERT_EQ(0, batch.internal_nodes.size());
  ASSERT_EQ(0, batch.leaf_nodes.size());
  ASSERT_EQ(0, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());
}

TEST(tree_tests, bulk_add_followed_by_bulk_remove) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key1"), Sliver("val1"));
  updates.emplace(Sliver("key2"), Sliver("val2"));
  updates.emplace(Sliver("key3"), Sliver("val3"));
  auto batch1 = tree.update(updates);
  db_put(db, batch1);

  // Delete the LeafChild in the node at depth1. This should not trigger any promotion.
  KeysVector deletes{Sliver("key1"), Sliver("key2"), Sliver("key3")};
  auto batch2 = tree.remove(deletes);

  // The nodes added in batch 1 should be removed in batch 2.
  std::set<InternalNodeKey> inserted_internal_keys;
  std::for_each(batch1.internal_nodes.begin(), batch1.internal_nodes.end(), [&inserted_internal_keys](const auto& p) {
    inserted_internal_keys.insert(p.first);
  });
  std::set<LeafKey> inserted_leaf_keys;
  std::for_each(batch1.leaf_nodes.begin(), batch1.leaf_nodes.end(), [&inserted_leaf_keys](const auto& p) {
    inserted_leaf_keys.insert(p.first);
  });
  std::set<LeafKey> stale_leaf_keys(batch2.stale.leaf_keys.begin(), batch2.stale.leaf_keys.end());

  ASSERT_EQ(inserted_internal_keys, batch2.stale.internal_keys);
  ASSERT_EQ(inserted_leaf_keys, stale_leaf_keys);
}

// Add 4 nodes that overlap in the first nibble, with 2 nodes also matching in
// one second nibble, and 2 nodes matching in the other second nibble. Thus the
// tree of BatchedInternalNodes will look like the following:
//
//                Root
//                 |
//                "b"
//                 |
//           -------------
//           |           |
//         "ba"        "be"
//
// Each of "ba" and "be" contain 2 leaf children. Deleting a leaf child from "be"
// should cause the other leaf child to go to "b", but leave "ba" alone. Thus
// after the delete, the tree should look like the following:
//
//                Root
//                 |
//                "b"
//                 |
//                "ba"
//
// We then add back in the deleted key and ensure that the tree returns to the
// original shape. Note that we leave out the assertions for the first addition
// and removal since we already check the correctness in
// `remove_left_node_and_promote_leaving_right_node_alone`.
TEST(tree_tests, add_remove_add) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  updates.emplace(Sliver("key191"), Sliver("val3"));
  updates.emplace(Sliver("key462"), Sliver("val4"));
  auto batch1 = tree.update(updates);
  db_put(db, batch1);

  // Delete a LeafChild at "be". This should should trigger a promotion to "b";
  KeysVector deletes{Sliver("key9")};
  auto batch2 = tree.remove(deletes);
  db_put(db, batch2);

  // Add back in the deleted key. This should recreate "be".
  SetOfKeyValuePairs updates2;
  updates2.emplace(Sliver("key9"), Sliver("val4"));
  auto batch3 = tree.update(updates2);

  // There should be 3 modified internal nodes, with no leaf children in the root node (""), no
  // leaf children in the second node ("b"), and 2 leaf children in the recreated bottom node ("be").
  ASSERT_EQ(3, batch3.internal_nodes.size());
  auto& [key1, node1] = batch3.internal_nodes[0];
  auto& [key2, node2] = batch3.internal_nodes[1];
  auto& [key3, node3] = batch3.internal_nodes[2];
  ASSERT_EQ(0, node1.numLeafChildren());
  ASSERT_EQ(0, node2.numLeafChildren());
  ASSERT_EQ(2, node3.numLeafChildren());
  ASSERT_EQ("", key1.path().toString());
  ASSERT_EQ("b", key2.path().toString());
  ASSERT_EQ("be", key3.path().toString());
  ASSERT_TRUE(validLinkExists(key1, node1, key2, node2));
  ASSERT_TRUE(validLinkExists(key2, node2, key3, node3));
  ASSERT_TRUE(leafChildExists("key462", 1, node3));
  ASSERT_TRUE(leafChildExists("key9", 3, node3));

  // There is one leaf update
  ASSERT_EQ(1, batch3.leaf_nodes.size());
  ASSERT_EQ(Version(3), batch3.leaf_nodes[0].first.version());
  ASSERT_TRUE(hashMatches("key9", batch3.leaf_nodes[0].first.hash()));
  ASSERT_EQ(Sliver("val4"), batch3.leaf_nodes[0].second.value);

  // There should be 2 stale internal nodes ("" and "b") and no stale leaves
  ASSERT_EQ(Version(3), batch3.stale.stale_since_version);
  ASSERT_EQ(2, batch3.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 2, batch3.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("b", 2, batch3.stale.internal_keys));
  ASSERT_EQ(0, batch3.stale.leaf_keys.size());
}

TEST(tree_tests, mixed_add_and_remove_in_one_update) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);

  // Add a few initial keys so we have something to delete. The tree after
  // addition looks like this, with 2 leaf children in each of the bottom nodes. We
  // don't bother with assertions since this same structure is used in other
  // tests.
  //
  //                Root
  //                 |
  //                "b"
  //                 |
  //           -------------
  //           |           |
  //         "ba"        "be"
  SetOfKeyValuePairs updates;
  updates.emplace(Sliver("key3"), Sliver("val1"));
  updates.emplace(Sliver("key9"), Sliver("val2"));
  updates.emplace(Sliver("key191"), Sliver("val3"));
  updates.emplace(Sliver("key462"), Sliver("val4"));
  auto batch = tree.update(updates);
  db_put(db, batch);

  // Add two new keys that differ from all other keys in the first nibble. This
  // will result in their leaf children living in the root node.
  SetOfKeyValuePairs updates2;
  updates2.emplace("key1", Sliver("val10"));
  updates2.emplace("key2", Sliver("val11"));

  // Delete a LeafChild at "be". This should should trigger a promotion to "b";
  KeysVector deletes{Sliver("key9")};
  batch = tree.update(updates2, deletes);

  // Ensure that the tree looks like the following after this update:
  //                Root
  //                 |
  //                "b"
  //                 |
  //                "ba"
  // The two newly inserted keys ("key1" and "key2"), will end up with leaf
  // children in the root node. Key9 will trigger the removal of
  // BatchedInternalNode "be", and the promotion of "key462" into "b". "ba" will
  // retain the same two leaf children ("key3" and "key191")
  ASSERT_EQ(2, batch.internal_nodes.size());
  ASSERT_TRUE(internalNodeVersionMatches(2, batch.internal_nodes));
  auto& [root_key, root_node] = batch.internal_nodes[0];
  auto& [b_key, b_node] = batch.internal_nodes[1];
  ASSERT_EQ("", root_key.path().toString());
  ASSERT_EQ("b", b_key.path().toString());
  ASSERT_TRUE(validLinkExists(root_key, root_node, b_key, b_node));

  // There are two leaf updates.
  ASSERT_EQ(2, batch.leaf_nodes.size());
  ASSERT_TRUE(leafKeyExists("key1", 2, batch.leaf_nodes));
  ASSERT_TRUE(leafKeyExists("key2", 2, batch.leaf_nodes));

  ASSERT_EQ(2, root_node.numLeafChildren());
  ASSERT_TRUE(leafChildExists("key1", 2, root_node));
  ASSERT_TRUE(leafChildExists("key2", 2, root_node));

  // "b" should have one promoted leaf child
  ASSERT_EQ(1, b_node.numLeafChildren());
  ASSERT_TRUE(leafChildExists("key462", 1, b_node));

  // There are 3 old internal keys (root, "b", "be") and one stale leaf
  ASSERT_EQ(Version(2), batch.stale.stale_since_version);
  ASSERT_EQ(3, batch.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 1, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("b", 1, batch.stale.internal_keys));
  ASSERT_TRUE(internalKeyExists("be", 1, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key9", 1, batch.stale.leaf_keys));
}

// Adding and removing the same key in a single update should cause the key to be added.
TEST(tree_tests, add_and_remove_of_same_key_in_single_update_add_wins) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  SetOfKeyValuePairs updates;
  updates.emplace("key1", Sliver("val1"));
  KeysVector deletes{Sliver("key1")};
  auto batch = tree.update(updates, deletes);

  // There's a single InternalNode and a single LeafNode.
  ASSERT_EQ(1, batch.internal_nodes.size());
  auto root_node = batch.internal_nodes[0].second;
  ASSERT_EQ(1, root_node.numLeafChildren());
  ASSERT_TRUE(leafChildExists("key1", 1, root_node));

  ASSERT_EQ(1, batch.leaf_nodes.size());
  ASSERT_TRUE(leafKeyExists("key1", 1, batch.leaf_nodes));

  // There were no stale nodes since this is the first insert
  ASSERT_EQ(Version(1), batch.stale.stale_since_version);
  ASSERT_EQ(0, batch.stale.internal_keys.size());
  ASSERT_EQ(0, batch.stale.leaf_keys.size());
}

// Adding and removing the same key that already exists results in the new version being added.
TEST(tree_tests, add_and_remove_of_same_existing_key_in_single_update_add_wins) {
  std::shared_ptr<TestDB> db(new TestDB);
  Tree tree(db);
  // Add an initial version of key1
  SetOfKeyValuePairs updates;
  updates.emplace("key1", Sliver("val1"));
  auto batch = tree.update(updates);
  db_put(db, batch);

  SetOfKeyValuePairs updates2;
  updates2.emplace("key1", Sliver("val2"));
  KeysVector deletes{Sliver("key1")};
  batch = tree.update(updates2, deletes);
  db_put(db, batch);

  // There's a single InternalNode and a single LeafNode.
  ASSERT_EQ(1, batch.internal_nodes.size());
  auto root_node = batch.internal_nodes[0].second;
  ASSERT_EQ(1, root_node.numLeafChildren());
  ASSERT_TRUE(leafChildExists("key1", 2, root_node));

  ASSERT_EQ(1, batch.leaf_nodes.size());
  ASSERT_TRUE(leafKeyExists("key1", 2, batch.leaf_nodes));
  ASSERT_EQ(string("val2"), batch.leaf_nodes[0].second.value.toString());

  // There's a single stale node
  ASSERT_EQ(Version(2), batch.stale.stale_since_version);
  ASSERT_EQ(1, batch.stale.internal_keys.size());
  ASSERT_TRUE(internalKeyExists("", 1, batch.stale.internal_keys));
  ASSERT_EQ(1, batch.stale.leaf_keys.size());
  ASSERT_TRUE(leafKeyExists("key1", 1, batch.stale.leaf_keys));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  int res = RUN_ALL_TESTS();
  return res;
}
