// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "categorization/block_merkle_category.h"
#include "kv_types.hpp"
#include "rocksdb/native_client.h"
#include "storage/test/storage_test_common.h"
#include "categorization/column_families.h"

#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace {

using namespace ::testing;
using namespace concord::storage::rocksdb;
using namespace concord::kvbc;
using namespace concord::kvbc::categorization;
using namespace concord::kvbc::categorization::detail;
using namespace std::literals;

const std::string key1 = "key1"s;
const std::string key2 = "key2"s;
const std::string key3 = "key3"s;
const std::string key4 = "key4"s;
const std::string key5 = "key5"s;
const std::string val1 = "val1"s;
const std::string val2 = "val2"s;
const std::string val3 = "val3"s;
const std::string val4 = "val4"s;
const std::string val5 = "val5"s;
const Hash hashed_key1 = hash(key1);
const Hash hashed_key2 = hash(key2);
const Hash hashed_key3 = hash(key3);
const Hash hashed_key4 = hash(key4);
const Hash hashed_key5 = hash(key5);

using KvPairs = std::vector<std::pair<std::string, std::string>>;

// Return all key-value pairs in the column family `cf`
KvPairs getAll(const std::string &cf, std::shared_ptr<concord::storage::rocksdb::NativeClient> &db) {
  auto all = KvPairs{};
  auto iter = db->getIterator(cf);
  iter.first();
  while (iter) {
    all.emplace_back(iter.key(), iter.value());
    iter.next();
  }
  return all;
}

KvPairs getAllLeaves(std::shared_ptr<concord::storage::rocksdb::NativeClient> &db) {
  return getAll(BLOCK_MERKLE_LEAF_NODES_CF, db);
}

KvPairs getAllInternalNodes(std::shared_ptr<concord::storage::rocksdb::NativeClient> &db) {
  return getAll(BLOCK_MERKLE_INTERNAL_NODES_CF, db);
}

KvPairs getAllStale(std::shared_ptr<concord::storage::rocksdb::NativeClient> &db) {
  return getAll(BLOCK_MERKLE_STALE_CF, db);
}

StaleKeys getStale(std::shared_ptr<concord::storage::rocksdb::NativeClient> &db, TreeVersion version) {
  auto ser_stale = db->get(BLOCK_MERKLE_STALE_CF, serialize(version));
  auto stale = StaleKeys{};
  deserialize(*ser_stale, stale);
  return stale;
}

// Keys returned by a sparse_merkle::UpdateBatch are always hashed versions of string keys. However,
// the keys in the block merkle are simply BlockIds. We therefore want to brute force reverse the
// hashes to figure out the block id.
BlockId hashToBlockId(const KeyHash &key_hash, BlockId max_block_id) {
  for (auto i = 1u; i <= max_block_id; i++) {
    if (hash(serialize(BlockKey{i})) == key_hash.value) {
      return i;
    }
  }
  throw std::invalid_argument("Key does not correspond to a block id: " +
                              sparse_merkle::Hash(key_hash.value).toString());
}

// A leaf key is a pair of block id and tree version at its most basic
struct LeafKey {
  BlockId block_id;
  uint64_t tree_version;
  bool operator==(const LeafKey &other) const {
    return block_id == other.block_id && tree_version == other.tree_version;
  }
  bool operator<(const LeafKey &other) const {
    if (block_id < other.block_id) return true;
    if (block_id > other.block_id) return false;
    return tree_version < other.tree_version;
  }
};

std::set<LeafKey> deserializeLeafKeys(const KvPairs &kv_pairs, BlockId max_block_id) {
  auto leaf_keys = std::set<LeafKey>{};
  for (const auto &[k, v] : kv_pairs) {
    (void)v;
    auto key = VersionedKey{};
    deserialize(k, key);
    leaf_keys.insert(LeafKey{hashToBlockId(key.key_hash, max_block_id), key.version});
  }
  return leaf_keys;
}

std::set<LeafKey> deserializeLeafKeys(const std::vector<std::vector<uint8_t>> &serialized, BlockId max_block_id) {
  auto leaf_keys = std::set<LeafKey>{};
  for (const auto &ser : serialized) {
    auto key = VersionedKey{};
    deserialize(ser, key);
    leaf_keys.insert(LeafKey{hashToBlockId(key.key_hash, max_block_id), key.version});
  }
  return leaf_keys;
}

class block_merkle_category : public Test {
  void SetUp() override {
    destroyDb();
    db = TestRocksDb::createNative();
    cat = BlockMerkleCategory{db};
  }

  void TearDown() override { destroyDb(); }

  void destroyDb() {
    cat = BlockMerkleCategory{};
    db.reset();
    ASSERT_EQ(0, db.use_count());
    cleanup();
  }

 protected:
  auto add(BlockId block_id, BlockMerkleInput &&update) {
    auto batch = db->getBatch();
    auto output = cat.add(block_id, std::move(update), batch);
    db->write(std::move(batch));
    return output;
  }

  void deleteGenesisBlock(BlockId block_id, const BlockMerkleOutput &out) {
    auto batch = db->getBatch();
    concord::kvbc::categorization::detail::LocalWriteBatch loc_batch;
    cat.deleteGenesisBlock(block_id, out, loc_batch);
    loc_batch.moveToBatch(batch);
    db->write(std::move(batch));
  }

 protected:
  std::shared_ptr<NativeClient> db;
  BlockMerkleCategory cat;
};

TEST_F(block_merkle_category, empty_updates) {
  auto update = BlockMerkleInput{};
  auto batch = db->getBatch();
  const auto output = cat.add(1, std::move(update), batch);

  // A new root index, an internal node, a leaf node, and stale index node are created.
  ASSERT_EQ(batch.count(), 4);
}

TEST_F(block_merkle_category, put_and_get) {
  auto update = BlockMerkleInput{{{key1, val1}}};
  auto batch = db->getBatch();
  auto block_id = 1u;
  const auto output = cat.add(block_id, std::move(update), batch);

  // A new root index, an internal node, a leaf node, and stale index node are created.
  // Additionally, a key and its value are written.
  ASSERT_EQ(batch.count(), 6);
  ASSERT_EQ(1, output.state_root_version);
  ASSERT_EQ(false, output.keys.find(key1)->second.deleted);

  db->write(std::move(batch));

  auto expected = MerkleValue{{block_id, val1}};

  // Get by key works
  ASSERT_EQ(expected, asMerkle(cat.getLatest(key1).value()));
  // Get by specific block works
  ASSERT_EQ(expected, asMerkle(cat.get(key1, 1).value()));

  // Get by hash works
  ASSERT_EQ(expected, asMerkle(cat.get(hashed_key1, block_id).value()));

  // Getting the latest version by key works
  ASSERT_EQ(block_id, cat.getLatestVersion(key1)->encode());

  // Getting the key at the wrong block fails
  ASSERT_EQ(false, cat.get(key1, block_id + 1).has_value());
  ASSERT_EQ(false, cat.get(hashed_key1, block_id + 1).has_value());

  // Trying to get non-existant keys returns std::nullopt
  ASSERT_EQ(false, cat.getLatest(key2).has_value());
  ASSERT_EQ(false, cat.get(key2, block_id).has_value());
  ASSERT_EQ(false, cat.get(hashed_key2, block_id).has_value());
}

TEST_F(block_merkle_category, multiget) {
  auto update = BlockMerkleInput{{{key1, val1}, {key2, val2}, {key3, val3}}};
  BlockId block_id = 1;
  const auto output = add(block_id, std::move(update));

  // We can get all keys individually
  ASSERT_EQ(val1, asMerkle(cat.getLatest(key1).value()).data);
  ASSERT_EQ(val2, asMerkle(cat.getLatest(key2).value()).data);
  ASSERT_EQ(val3, asMerkle(cat.getLatest(key3).value()).data);

  // We can get all 3 with a multiget
  auto keys = std::vector<std::string>{key1, key2, key3};
  auto versions = std::vector<BlockId>{1u, 1u, 1u};
  auto values = std::vector<std::optional<concord::kvbc::categorization::Value>>{};
  cat.multiGet(keys, versions, values);
  ASSERT_EQ(keys.size(), values.size());
  ASSERT_EQ((MerkleValue{{block_id, val1}}), asMerkle(*values[0]));
  ASSERT_EQ((MerkleValue{{block_id, val2}}), asMerkle(*values[1]));
  ASSERT_EQ((MerkleValue{{block_id, val3}}), asMerkle(*values[2]));

  // Retrieving a key with the non-existant version causes a nullopt for that key only
  versions[1] = 2u;
  cat.multiGet(keys, versions, values);
  ASSERT_EQ(keys.size(), values.size());
  ASSERT_EQ((MerkleValue{{block_id, val1}}), asMerkle(*values[0]));
  ASSERT_EQ(false, values[1].has_value());
  ASSERT_EQ((MerkleValue{{block_id, val3}}), asMerkle(*values[2]));

  // Retrieving a non-existant key causes a nullopt for that key only
  keys.push_back("key4");
  versions.push_back(1u);
  cat.multiGet(keys, versions, values);
  ASSERT_EQ(keys.size(), values.size());
  ASSERT_EQ((MerkleValue{{block_id, val1}}), asMerkle(*values[0]));
  ASSERT_EQ(false, values[1].has_value());
  ASSERT_EQ((MerkleValue{{block_id, val3}}), asMerkle(*values[2]));
  ASSERT_EQ(false, values[3].has_value());

  // Get latest versions
  auto out_versions = std::vector<std::optional<TaggedVersion>>{};
  cat.multiGetLatestVersion(keys, out_versions);
  ASSERT_EQ(keys.size(), out_versions.size());
  ASSERT_EQ(1, out_versions[0]->encode());
  ASSERT_EQ(1, out_versions[1].has_value());
  ASSERT_EQ(1, out_versions[2]->version);
  ASSERT_EQ(false, out_versions[3].has_value());

  // Make sure subsequent calls with the same vector work
  cat.multiGetLatestVersion({key2}, out_versions);
  ASSERT_EQ(1, out_versions.size());
  ASSERT_EQ(1, out_versions[0].has_value());
  ASSERT_EQ(1, out_versions[0]->encode());
  ASSERT_EQ(1, out_versions[0]->version);

  // Get latest values
  cat.multiGetLatest(keys, values);
  ASSERT_EQ(keys.size(), values.size());
  ASSERT_EQ((MerkleValue{{block_id, val1}}), asMerkle(*values[0]));
  ASSERT_EQ((MerkleValue{{block_id, val2}}), asMerkle(*values[1]));
  ASSERT_EQ((MerkleValue{{block_id, val3}}), asMerkle(*values[2]));
  ASSERT_EQ(false, values[3].has_value());

  // Make sure subsequent calls with the same vector work
  cat.multiGetLatest({key2}, values);
  ASSERT_EQ(1, values.size());
  ASSERT_EQ((MerkleValue{{block_id, val2}}), asMerkle(*values[0]));
}

TEST_F(block_merkle_category, overwrite) {
  auto update = BlockMerkleInput{{{key1, val1}, {key2, val2}, {key3, val3}}};
  auto _ = add(1, std::move(update));

  // Overwrite key 2 and add key4
  _ = add(2, BlockMerkleInput{{{key2, "new_val"s}, {key4, val4}}});
  auto expected1 = MerkleValue{{1, val1}};
  auto expected2 = MerkleValue{{2, "new_val"s}};
  auto expected3 = MerkleValue{{1, val3}};
  auto expected4 = MerkleValue{{2, val4}};
  ASSERT_EQ(expected1, asMerkle(*cat.getLatest(key1)));
  ASSERT_EQ(expected2, asMerkle(*cat.getLatest(key2)));
  ASSERT_EQ(expected3, asMerkle(*cat.getLatest(key3)));
  ASSERT_EQ(expected4, asMerkle(*cat.getLatest(key4)));

  auto keys = std::vector<std::string>{key1, key2, key3, key4};
  auto values = std::vector<std::optional<concord::kvbc::categorization::Value>>{};
  cat.multiGetLatest(keys, values);
  ASSERT_EQ(expected1, asMerkle(*values[0]));
  ASSERT_EQ(expected2, asMerkle(*values[1]));
  ASSERT_EQ(expected3, asMerkle(*values[2]));
  ASSERT_EQ(expected4, asMerkle(*values[3]));

  // Multiget will find the old value of key2 at 1, but not key4 since it was written at block 2
  const auto versions = std::vector<BlockId>{1u, 1u, 1u, 1u};
  cat.multiGet(keys, versions, values);
  ASSERT_EQ(expected1, asMerkle(*values[0]));
  ASSERT_EQ((MerkleValue{{1, val2}}), asMerkle(*values[1]));
  ASSERT_EQ(expected3, asMerkle(*values[2]));
  ASSERT_EQ(false, values[3].has_value());
}

TEST_F(block_merkle_category, updates_with_deleted_keys) {
  auto update = BlockMerkleInput{{{key1, val1}, {key2, val2}, {key3, val3}, {key4, val4}, {key5, val5}}};
  auto _ = add(1, std::move(update));

  // Overwrite key 2, delete key 3 and key 5
  _ = add(2, BlockMerkleInput{{{key2, "new_val"s}}, {{key3, key5}}});
  auto expected1 = MerkleValue{{1, val1}};
  auto expected2 = MerkleValue{{2, "new_val"s}};
  auto expected4 = MerkleValue{{1, val4}};
  ASSERT_EQ(expected1, asMerkle(*cat.getLatest(key1)));
  ASSERT_EQ(expected2, asMerkle(*cat.getLatest(key2)));
  ASSERT_FALSE(cat.getLatest(key3).has_value());
  ASSERT_EQ(expected4, asMerkle(*cat.getLatest(key4)));
  ASSERT_FALSE(cat.getLatest(key5).has_value());

  auto keys = std::vector<std::string>{key1, key2, key3, key4, key5};
  auto values = std::vector<std::optional<concord::kvbc::categorization::Value>>{};
  cat.multiGetLatest(keys, values);
  ASSERT_EQ(expected1, asMerkle(*values[0]));
  ASSERT_EQ(expected2, asMerkle(*values[1]));
  ASSERT_FALSE(values[2].has_value());
  ASSERT_EQ(expected4, asMerkle(*values[3]));
  ASSERT_FALSE(values[4].has_value());

  // Multiget will find the old values of key2, key3 and key5 at block 1;
  const auto versions = std::vector<BlockId>{1u, 1u, 1u, 1u, 1u};
  cat.multiGet(keys, versions, values);
  ASSERT_EQ(expected1, asMerkle(*values[0]));
  ASSERT_EQ((MerkleValue{{1, val2}}), asMerkle(*values[1]));
  ASSERT_EQ((MerkleValue{{1, val3}}), asMerkle(*values[2]));
  ASSERT_EQ(expected4, asMerkle(*values[3]));
  ASSERT_EQ((MerkleValue{{1, val5}}), asMerkle(*values[4]));

  // Getting the latest versions shows key3 and key5 as deleted.
  auto expected_v1 = TaggedVersion{false, 1};
  auto expected_v2 = TaggedVersion{false, 2};
  auto expected_v3 = TaggedVersion{true, 2};
  auto expected_v4 = TaggedVersion{false, 1};
  auto expected_v5 = TaggedVersion{true, 2};
  ASSERT_EQ(expected_v1, *cat.getLatestVersion(key1));
  ASSERT_EQ(expected_v2, *cat.getLatestVersion(key2));
  ASSERT_EQ(expected_v3, *cat.getLatestVersion(key3));
  ASSERT_EQ(expected_v4, *cat.getLatestVersion(key4));
  ASSERT_EQ(expected_v5, *cat.getLatestVersion(key5));
  auto tagged_versions = std::vector<std::optional<TaggedVersion>>{};
  cat.multiGetLatestVersion({key1, key2, key3, key4, key5}, tagged_versions);
  ASSERT_EQ(expected_v1, tagged_versions[0]);
  ASSERT_EQ(expected_v2, tagged_versions[1]);
  ASSERT_EQ(expected_v3, tagged_versions[2]);
  ASSERT_EQ(expected_v4, tagged_versions[3]);
  ASSERT_EQ(expected_v5, tagged_versions[4]);
}

TEST_F(block_merkle_category, stale_node_creation_and_deletion) {
  // Create the first block and read its stale keys
  auto update = BlockMerkleInput{{{key1, val1}, {key2, val2}, {key3, val3}, {key4, val4}, {key5, val5}}};
  const auto block_out1 = add(1, std::move(update));

  // There's no stale keys on the first block creation, since nothing existed before the first block.
  auto stale = getStale(db, TreeVersion{1});
  ASSERT_EQ(0, stale.internal_keys.size());
  ASSERT_EQ(0, stale.leaf_keys.size());

  // Create the second block and read its stale keys
  const auto block_out2 = add(2, BlockMerkleInput{{{key2, "new_val"s}}, {{key3, key5}}});

  // Adding a new block adds stale internal keys, but no stale leaf keys, as blocks aren't deleted.
  stale = getStale(db, TreeVersion{2});
  ASSERT_LT(0, stale.internal_keys.size());
  ASSERT_EQ(0, stale.leaf_keys.size());

  // There have been no pruned blocks yet.
  ASSERT_EQ(0, cat.getLastDeletedTreeVersion());
  ASSERT_EQ(2, cat.getLatestTreeVersion());

  // Block @ TreeVersion notation
  // block 1 @ 1
  // block 2 @ 2
  auto max_block_id = 2;
  auto expected = std::set({LeafKey{1, 1}, LeafKey{2, 2}});
  auto leaf_keys = deserializeLeafKeys(getAllLeaves(db), max_block_id);
  ASSERT_EQ(expected, leaf_keys);

  // Pruning the first block causes key2, key3, and key5 from version 1 to be deleted.
  // Keys 1 and 4 are still active at version 1 and so remain in the database.

  // No tree nodes will get deleted though, as none are stale.
  deleteGenesisBlock(1, block_out1);
  ASSERT_FALSE(cat.get(key2, 1));
  ASSERT_FALSE(cat.get(key3, 1));
  ASSERT_FALSE(cat.get(key5, 1));
  ASSERT_TRUE(cat.get(key1, 1));
  ASSERT_TRUE(cat.get(key4, 1));
  ASSERT_EQ(1, cat.getLastDeletedTreeVersion());

  // A new tree version gets created as a result of the block deletion.
  ASSERT_EQ(3, cat.getLatestTreeVersion());

  // block1 @ 1 - marked stale as of 3
  // block 2 @ 2
  // block 1 @ 3 -- written here
  expected = std::set({LeafKey{1, 1}, LeafKey{2, 2}, LeafKey{1, 3}});
  auto expected_stale = std::set({LeafKey{1, 1}});
  leaf_keys = deserializeLeafKeys(getAllLeaves(db), max_block_id);
  stale = getStale(db, TreeVersion{3});
  ASSERT_EQ(expected, leaf_keys);
  ASSERT_EQ(expected_stale, deserializeLeafKeys(stale.leaf_keys, max_block_id));

  // There are stale internal keys as of block deletion
  ASSERT_LT(0, stale.internal_keys.size());

  // Deleting Block2 causes there to still be active keys.
  deleteGenesisBlock(2, block_out2);
  ASSERT_TRUE(cat.getLatest(key1));
  ASSERT_TRUE(cat.getLatest(key2));
  ASSERT_FALSE(cat.getLatest(key3));
  ASSERT_TRUE(cat.getLatest(key4));
  ASSERT_FALSE(cat.getLatest(key5));
  ASSERT_EQ(2, cat.getLastDeletedTreeVersion());

  // block1 @ 1 - marked stale as of 3
  // block 2 @ 2 -- marked stale as of 4
  // block 1 @ 3
  // block 2 @ 4 -- written here
  expected = std::set({LeafKey{1, 1}, LeafKey{2, 2}, LeafKey{1, 3}, LeafKey{2, 4}});
  expected_stale = std::set({LeafKey{2, 2}});
  leaf_keys = deserializeLeafKeys(getAllLeaves(db), max_block_id);
  stale = getStale(db, TreeVersion{4});
  ASSERT_EQ(expected, leaf_keys);
  ASSERT_EQ(expected_stale, deserializeLeafKeys(stale.leaf_keys, max_block_id));

  // A new tree version gets created as a result of the block deletion.
  ASSERT_EQ(4, cat.getLatestTreeVersion());

  // Let's delete the last remaining keys, with a new block addition.
  const auto block_out3 = add(3, BlockMerkleInput{{}, {{key1, key2, key4}}});
  ASSERT_FALSE(cat.getLatest(key1));
  ASSERT_FALSE(cat.getLatest(key2));
  ASSERT_FALSE(cat.getLatest(key3));
  ASSERT_FALSE(cat.getLatest(key4));
  ASSERT_FALSE(cat.getLatest(key5));
  ASSERT_EQ(2, cat.getLastDeletedTreeVersion());
  ASSERT_EQ(5, cat.getLatestTreeVersion());

  // block1 @ 1 - marked stale as of 3
  // block 2 @ 2 -- marked stale as of 4
  // block 1 @ 3
  // block 2 @ 4
  // block 3 @ 5 -- written here
  max_block_id = 3;
  expected = std::set({LeafKey{1, 1}, LeafKey{2, 2}, LeafKey{1, 3}, LeafKey{2, 4}, LeafKey{3, 5}});
  leaf_keys = deserializeLeafKeys(getAllLeaves(db), max_block_id);
  stale = getStale(db, TreeVersion{5});
  ASSERT_EQ(expected, leaf_keys);
  ASSERT_EQ(0, stale.leaf_keys.size());

  // We still see tombstones for keys 1, 2, 4
  ASSERT_TRUE(cat.getLatestVersion(key1)->deleted);
  ASSERT_TRUE(cat.getLatestVersion(key2)->deleted);
  ASSERT_TRUE(cat.getLatestVersion(key4)->deleted);

  // We can still access those keys at their old versions
  auto expected1 = MerkleValue{{1, val1}};
  auto expected2 = MerkleValue{{2, "new_val"s}};
  auto expected4 = MerkleValue{{1, val4}};
  ASSERT_EQ(expected1, asMerkle(*cat.get(key1, 1)));
  ASSERT_EQ(expected2, asMerkle(*cat.get(key2, 2)));
  ASSERT_EQ(expected4, asMerkle(*cat.get(key4, 1)));

  // There exist pruned block indexes for block 1 and 2
  // This is because there are still active keys for those blocks.
  ASSERT_TRUE(db->get(BLOCK_MERKLE_PRUNED_BLOCKS_CF, serialize(BlockVersion{1})));
  ASSERT_TRUE(db->get(BLOCK_MERKLE_PRUNED_BLOCKS_CF, serialize(BlockVersion{2})));

  // There exist active key indexes only for the given active keys from pruned blocks
  ASSERT_TRUE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key1})));
  ASSERT_TRUE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key2})));
  ASSERT_TRUE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key4})));
  ASSERT_FALSE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key3})));
  ASSERT_FALSE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key5})));

  // Deleting block 3 triggers all tree versions up to 5 to be removed
  deleteGenesisBlock(3, block_out3);
  ASSERT_EQ(5, cat.getLastDeletedTreeVersion());
  ASSERT_EQ(6, cat.getLatestTreeVersion());

  // block 1 @ 3 -- marked stale as of 6
  // block 2 @ 4 -- marked stale as of 6
  // block 3 @ 5 -- marked stale as of 6
  expected = std::set({LeafKey{1, 3}, LeafKey{2, 4}, LeafKey{3, 5}});
  expected_stale = expected;
  leaf_keys = deserializeLeafKeys(getAllLeaves(db), max_block_id);
  stale = getStale(db, TreeVersion{6});
  ASSERT_EQ(expected, leaf_keys);
  ASSERT_EQ(expected_stale, deserializeLeafKeys(stale.leaf_keys, max_block_id));

  // There are no more latest versions for any keys.
  ASSERT_FALSE(cat.getLatestVersion(key1));
  ASSERT_FALSE(cat.getLatestVersion(key2));
  ASSERT_FALSE(cat.getLatestVersion(key3));
  ASSERT_FALSE(cat.getLatestVersion(key4));
  ASSERT_FALSE(cat.getLatestVersion(key5));

  // We can no longer retrieve keys 1, 2, 4 at their old versions
  ASSERT_FALSE(cat.get(key1, 1));
  ASSERT_FALSE(cat.get(key2, 2));
  ASSERT_FALSE(cat.get(key4, 1));

  // All pruned block indexes have been cleaned up. There are no active keys for any pruned blocks.
  ASSERT_FALSE(db->get(BLOCK_MERKLE_PRUNED_BLOCKS_CF, serialize(BlockVersion{1})));
  ASSERT_FALSE(db->get(BLOCK_MERKLE_PRUNED_BLOCKS_CF, serialize(BlockVersion{2})));
  ASSERT_FALSE(db->get(BLOCK_MERKLE_PRUNED_BLOCKS_CF, serialize(BlockVersion{3})));

  // All key indexes have been removed.
  ASSERT_FALSE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key1})));
  ASSERT_FALSE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key2})));
  ASSERT_FALSE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key4})));
  ASSERT_FALSE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key3})));
  ASSERT_FALSE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key5})));

  // Adding another block so we can prune it and garbage collect all old stale leaves.
  const auto block_out4 = add(4, BlockMerkleInput{});

  // block 1 @ 3 -- marked stale as of 6
  // block 2 @ 4 -- marked stale as of 6
  // block 3 @ 5 -- marked stale as of 6
  // block 4 @ 7 -- written here
  max_block_id = 4;
  expected = std::set({LeafKey{1, 3}, LeafKey{2, 4}, LeafKey{3, 5}, LeafKey{4, 7}});
  leaf_keys = deserializeLeafKeys(getAllLeaves(db), max_block_id);
  stale = getStale(db, TreeVersion{7});
  ASSERT_EQ(expected, leaf_keys);
  ASSERT_EQ(0, stale.leaf_keys.size());

  deleteGenesisBlock(4, block_out4);

  // block 4 @ 7
  expected = std::set({LeafKey{4, 7}});
  expected_stale = expected;
  leaf_keys = deserializeLeafKeys(getAllLeaves(db), max_block_id);
  stale = getStale(db, TreeVersion{8});
  ASSERT_EQ(expected, leaf_keys);
  ASSERT_EQ(expected_stale, deserializeLeafKeys(stale.leaf_keys, max_block_id));
}

// Prune several nodes in a row. Then add some new blocks. Then prune the rest of the nodes. Make
// sure all the intermediate tree versions get garbage collected.
TEST_F(block_merkle_category, prune_many_nodes) {
  // Put 1001 blocks, overwriting key1 with an indentical value each time. The value doesn't matter
  // for this test.
  // Note that `out` is zero-indexed, while blocks are one-indexed.
  std::vector<BlockMerkleOutput> out;
  for (auto i = 1u; i <= 1001; i++) {
    auto update = BlockMerkleInput{{{key1, val1}}};
    out.push_back(add(i, std::move(update)));
  }
  ASSERT_EQ(1001, cat.getLatestTreeVersion());
  ASSERT_EQ(0, cat.getLastDeletedTreeVersion());

  // We can get key1 at any version.
  for (auto i = 1u; i <= 1001; i++) {
    ASSERT_TRUE(cat.get(key1, i));
  }

  // Prune the first 500 blocks. This will create at least another 500 deleted tree versions after the last initial
  // tree version/block_id.
  for (auto i = 1u; i <= 500; i++) {
    deleteGenesisBlock(i, out[i - 1]);
  }
  auto tree_version_after_first_500_deletes = cat.getLatestTreeVersion();
  ASSERT_LE(1501, tree_version_after_first_500_deletes);
  ASSERT_EQ(500, cat.getLastDeletedTreeVersion());

  // We can only get key1 at blocks 501 on. The key is overwritten in every version and we have
  // pruned blocks with stale versions. Because there are no active keys in the first 500 pruned
  // blocks, they don't generate pruned indexes.
  for (auto i = 1u; i <= 500; i++) {
    ASSERT_FALSE(cat.get(key1, i));
    ASSERT_FALSE(db->get(BLOCK_MERKLE_PRUNED_BLOCKS_CF, serialize(BlockVersion{i})));
  }
  for (auto i = 501u; i <= 1001; i++) {
    ASSERT_TRUE(cat.get(key1, i));
  }

  // Add another block that deletes key1.
  auto last_out = add(1002, BlockMerkleInput{{}, {key1}});
  ASSERT_EQ(tree_version_after_first_500_deletes + 1, cat.getLatestTreeVersion());

  // Prune up to block 1001.
  for (auto i = 501u; i <= 1001; i++) {
    deleteGenesisBlock(i, out[i - 1]);
  }

  ASSERT_EQ(1001, cat.getLastDeletedTreeVersion());

  // Now prune block 1002. This will prune all tree versions created from the first 500 prunes in addition to the
  // version created by block 1002.
  deleteGenesisBlock(1002, last_out);

  // We deleted the intermediate versions from the first 500 prunes, plus the version from the latest block.
  ASSERT_EQ(tree_version_after_first_500_deletes + 1, cat.getLastDeletedTreeVersion());

  // There are still new versions created from pruned blocks 501-1002;
  ASSERT_GT(cat.getLatestTreeVersion(), cat.getLastDeletedTreeVersion());

  // There should be no pruned block indexes, and no keys available at any version
  for (auto i = 1u; i <= 1002; i++) {
    ASSERT_FALSE(db->get(BLOCK_MERKLE_PRUNED_BLOCKS_CF, serialize(BlockVersion{i})));
    // We can't retreive key1 at any version
    ASSERT_FALSE(cat.get(key1, i));
  }
  ASSERT_FALSE(db->get(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(KeyHash{hashed_key1})));
  ASSERT_FALSE(cat.getLatest(key1));
  ASSERT_FALSE(cat.getLatestVersion(key1));
}

TEST_F(block_merkle_category, delete_last_reachable) {
  // Add a bunch of blocks
  std::vector<BlockMerkleOutput> out;
  for (auto i = 1u; i <= 5; i++) {
    auto update = BlockMerkleInput{{{key1, val1}}};
    out.push_back(add(i, std::move(update)));
  }
  ASSERT_EQ(5, cat.getLatestTreeVersion());
  ASSERT_EQ(5, cat.getLatestVersion(key1)->version);
  auto expected5 = MerkleValue{{5, val1}};
  ASSERT_EQ(expected5, asMerkle(*cat.getLatest(key1)));

  // 5 blocks = 5 leaves
  auto all_leaves_5 = getAllLeaves(db);
  ASSERT_EQ(5, all_leaves_5.size());
  // 5 blocks = 5 stale nodes
  auto all_stale_5 = getAllStale(db);
  ASSERT_EQ(5, all_stale_5.size());

  // Ensure the stale node keys are actually just tree versions 1-5
  auto count = 1;
  for (auto &kv : all_stale_5) {
    auto ver = TreeVersion{};
    deserialize(kv.first, ver);
    ASSERT_EQ(ver.version, count);
    count++;
  }

  auto all_internal_5 = getAllInternalNodes(db);

  auto batch = db->getBatch();
  cat.deleteLastReachableBlock(5, out[4], batch);
  db->write(std::move(batch));

  auto all_leaves_4 = getAllLeaves(db);
  ASSERT_EQ(4, all_leaves_4.size());
  auto all_stale_4 = getAllStale(db);
  ASSERT_EQ(4, all_stale_4.size());

  // Ensure the stale node keys are actually just tree versions 1-4
  count = 1;
  for (auto &kv : all_stale_4) {
    auto ver = TreeVersion{};
    deserialize(kv.first, ver);
    ASSERT_EQ(ver.version, count);
    count++;
  }

  // There are fewer internal nodes now
  auto all_internal_4 = getAllInternalNodes(db);
  ASSERT_LT(all_internal_4.size(), all_internal_5.size());

  ASSERT_EQ(4, cat.getLatestTreeVersion());
  ASSERT_EQ(4, cat.getLatestVersion(key1)->version);
  auto expected4 = MerkleValue{{4, val1}};
  ASSERT_EQ(expected4, asMerkle(*cat.getLatest(key1)));

  // Adding back block 5 causes the merkle column familes to be exactly the same as before the removal of block 5
  auto update = BlockMerkleInput{{{key1, val1}}};
  auto new_out = add(5, std::move(update));

  ASSERT_EQ(all_leaves_5, getAllLeaves(db));
  ASSERT_EQ(all_stale_5, getAllStale(db));
  ASSERT_EQ(all_internal_5, getAllInternalNodes(db));
  ASSERT_EQ(5, cat.getLatestTreeVersion());
  ASSERT_EQ(5, cat.getLatestVersion(key1)->version);
  ASSERT_EQ(expected5, asMerkle(*cat.getLatest(key1)));

  // Deleting again generates the same column families as the prior delete
  {
    auto batch = db->getBatch();
    cat.deleteLastReachableBlock(5, new_out, batch);
    db->write(std::move(batch));
    ASSERT_EQ(all_leaves_4, getAllLeaves(db));
    ASSERT_EQ(all_stale_4, getAllStale(db));
    ASSERT_EQ(all_internal_4, getAllInternalNodes(db));
    ASSERT_EQ(4, cat.getLatestTreeVersion());
    ASSERT_EQ(4, cat.getLatestVersion(key1)->version);
    ASSERT_EQ(expected4, asMerkle(*cat.getLatest(key1)));
  }

  // Deleting all blocks does the right thing
  {
    auto batch = db->getBatch();
    for (auto i = 4u; i > 1; i--) {
      cat.deleteLastReachableBlock(i, out[i - 1], batch);
    }
    db->write(std::move(batch));
    ASSERT_EQ(1, getAllLeaves(db).size());
    // There's a key pointing to the latest root and the root itself
    ASSERT_EQ(2, getAllInternalNodes(db).size());
    ASSERT_EQ(1, cat.getLatestTreeVersion());
    ASSERT_EQ(1, getAllStale(db).size());

    auto expected1 = MerkleValue{{1, val1}};
    ASSERT_EQ(1, cat.getLatestVersion(key1)->version);
    ASSERT_EQ(expected1, asMerkle(*cat.getLatest(key1)));
  }
}

}  // namespace

int main(int argc, char *argv[]) {
  ::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
