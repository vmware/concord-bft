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

#include "categorization/block_merkle_category.h"
#include "categorization/column_families.h"
#include "categorization/details.h"

#include "assertUtils.hpp"
#include "kv_types.hpp"
#include "sha_hash.hpp"

using concord::storage::rocksdb::NativeWriteBatch;
using concord::storage::rocksdb::detail::toSlice;
using concordUtils::Sliver;

namespace concord::kvbc::categorization::detail {

std::tuple<MerkleBlockValue, std::vector<KeyHash>, std::vector<KeyHash>> hashNewBlock(const BlockMerkleInput& updates) {
  MerkleBlockValue value;
  auto hashed_added_keys = std::vector<KeyHash>{};
  auto hashed_deleted_keys = std::vector<KeyHash>{};

  // root_hash = h((h(k1) || h(v1)) || ... || (h(kN) || h(vN) || h(dk1) || ... || h(dkN))
  auto root_hasher = Hasher{};
  root_hasher.init();

  // Hash all keys and values as part of the root hash
  for (const auto& [k, v] : updates.kv) {
    auto key_hash = hash(k);
    auto val_hash = hash(v);
    hashed_added_keys.push_back(KeyHash{key_hash});
    root_hasher.update(key_hash.data(), key_hash.size());
    root_hasher.update(val_hash.data(), val_hash.size());
  }

  for (const auto& k : updates.deletes) {
    auto key_hash = hash(k);
    hashed_deleted_keys.push_back(KeyHash{key_hash});
    root_hasher.update(key_hash.data(), key_hash.size());
  }
  value.root_hash = root_hasher.finish();
  return std::make_tuple(value, hashed_added_keys, hashed_deleted_keys);
}

BlockMerkleOutput inputToOutput(const BlockMerkleInput& updates) {
  BlockMerkleOutput output{};
  for (const auto& kv : updates.kv) {
    output.keys.emplace(kv.first, MerkleKeyFlag{false});
  }
  for (const auto& key : updates.deletes) {
    output.keys.emplace(key, MerkleKeyFlag{true});
  }
  return output;
}

VersionedKey leafKeyToVersionedKey(const sparse_merkle::LeafKey& leaf_key) {
  return VersionedKey{KeyHash{leaf_key.hash().dataArray()}, leaf_key.version().value()};
}

// If we change the interface of IDBReader to accept r-values we can get rid of this function in
// favor of the one immediately below it.
BatchedInternalNodeKey toBatchedInternalNodeKey(const sparse_merkle::InternalNodeKey& key) {
  auto path = NibblePath{static_cast<uint8_t>(key.path().length()), key.path().data()};
  return BatchedInternalNodeKey{key.version().value(), std::move(path)};
}

BatchedInternalNodeKey toBatchedInternalNodeKey(sparse_merkle::InternalNodeKey&& key) {
  auto path = NibblePath{static_cast<uint8_t>(key.path().length()), key.path().move_data()};
  return BatchedInternalNodeKey{key.version().value(), std::move(path)};
}

std::vector<uint8_t> rootKey(uint64_t version) {
  auto v = sparse_merkle::Version(version);
  return serialize(toBatchedInternalNodeKey(sparse_merkle::InternalNodeKey::root(v)));
}

// A key used in tree_.update()
Sliver merkleKey(BlockId block_id) {
  auto block_key = serialize(BlockKey{block_id});
  return Sliver::copy((const char*)block_key.data(), block_key.size());
}

Sliver merkleValue(const MerkleBlockValue& value) {
  auto ser_value = serialize(value);
  return Sliver::copy((const char*)ser_value.data(), ser_value.size());
}

std::vector<uint8_t> serializeBatchedInternalNode(sparse_merkle::BatchedInternalNode&& node) {
  BatchedInternalNode cmf_node{};
  cmf_node.bitmask = 0;
  const auto& children = node.children();
  for (auto i = 0u; i < children.size(); ++i) {
    const auto& child = children[i];
    if (child) {
      cmf_node.bitmask |= (1 << i);
      if (auto leaf_child = std::get_if<sparse_merkle::LeafChild>(&child.value())) {
        cmf_node.children.push_back(
            BatchedInternalNodeChild{LeafChild{leaf_child->hash.dataArray(), leafKeyToVersionedKey(leaf_child->key)}});
      } else {
        auto internal_child = std::get<sparse_merkle::InternalChild>(child.value());
        cmf_node.children.push_back(
            BatchedInternalNodeChild{InternalChild{internal_child.hash.dataArray(), internal_child.version.value()}});
      }
    }
  }
  return serialize(cmf_node);
}

sparse_merkle::BatchedInternalNode deserializeBatchedInternalNode(const std::string& buf) {
  BatchedInternalNode cmf_node{};
  deserialize(buf, cmf_node);
  sparse_merkle::BatchedInternalNode::Children children;
  size_t child_index = 0;
  for (auto i = 0u; i < sparse_merkle::BatchedInternalNode::MAX_CHILDREN; ++i) {
    if (cmf_node.bitmask & (1 << i)) {
      const auto& child = cmf_node.children[child_index].child;
      ++child_index;
      if (auto leaf_child = std::get_if<LeafChild>(&child)) {
        auto sm_hash = sparse_merkle::Hash(leaf_child->hash);
        auto sm_key =
            sparse_merkle::LeafKey(sparse_merkle::Hash(leaf_child->key.key_hash.value), leaf_child->key.version);
        children[i] = sparse_merkle::LeafChild(sm_hash, sm_key);
      } else {
        auto internal_child = std::get<InternalChild>(child);
        auto sm_hash = sparse_merkle::Hash(internal_child.hash);
        children[i] = sparse_merkle::InternalChild{sm_hash, internal_child.version};
      }
    }
  }
  return sparse_merkle::BatchedInternalNode(children);
}

std::vector<Hash> hashedKeys(const std::vector<std::string>& keys) {
  std::vector<Hash> hashed_keys;
  hashed_keys.reserve(keys.size());
  std::transform(keys.begin(), keys.end(), std::back_inserter(hashed_keys), [](auto& key) { return hash(key); });
  return hashed_keys;
}

std::vector<Buffer> versionedKeys(const std::vector<std::string>& keys, const std::vector<BlockId>& versions) {
  auto versioned_keys = std::vector<Buffer>{};
  versioned_keys.reserve(keys.size());
  std::transform(
      keys.begin(), keys.end(), versions.begin(), std::back_inserter(versioned_keys), [](auto& key, auto version) {
        auto key_hash = KeyHash{hash(key)};
        return serialize(VersionedKey{key_hash, version});
      });
  return versioned_keys;
}

void putLatestKeyVersion(NativeWriteBatch& batch, const std::string& key, TaggedVersion version) {
  batch.put(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, key, serializeThreadLocal(LatestKeyVersion{version.encode()}));
}

void putKeys(NativeWriteBatch& batch,
             uint64_t block_id,
             std::vector<KeyHash>&& hashed_added_keys,
             std::vector<KeyHash>&& hashed_deleted_keys,
             BlockMerkleInput& updates) {
  auto kv_it = updates.kv.cbegin();
  for (auto key_it = hashed_added_keys.begin(); key_it != hashed_added_keys.end(); key_it++) {
    // Only serialize the Header of a DBValue, to prevent the need to copy a potentially large value.
    auto header = toSlice(serializeThreadLocal(DbValueHeader{false, static_cast<uint32_t>(kv_it->second.size())}));
    std::array<::rocksdb::Slice, 2> val{header, toSlice(kv_it->second)};

    // Write the versioned key/value pair used for direct key lookup
    batch.put(BLOCK_MERKLE_KEYS_CF, serialize(VersionedKey{*key_it, block_id}), val);

    // Put the latest version of the key
    batch.put(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, kv_it->first, serialize(LatestKeyVersion{block_id}));

    ++kv_it;
  }

  const bool deleted = true;
  auto deletes_it = updates.deletes.cbegin();
  for (auto key_it = hashed_deleted_keys.begin(); key_it != hashed_deleted_keys.end(); key_it++) {
    // Write a tombstone to the value. This is necessary for deleteLastReachable().
    auto tombstone = serialize(DbValueHeader{true, 0});
    batch.put(BLOCK_MERKLE_KEYS_CF, serialize(VersionedKey{*key_it, block_id}), tombstone);
    putLatestKeyVersion(batch, *deletes_it, TaggedVersion(deleted, block_id));
    ++deletes_it;
  }
}
template <typename Batch>
void putLatestTreeVersion(uint64_t tree_version, Batch& batch) {
  batch.put(BLOCK_MERKLE_INTERNAL_NODES_CF, rootKey(0), rootKey(tree_version));
}

// Write serialized stale keys at the given *tree* version. These keys are safe to delete when the
// tree version is pruned.
template <typename Batch>
void putStaleKeys(Batch& batch, sparse_merkle::StaleNodeIndexes&& stale_batch) {
  auto stale = StaleKeys{};
  for (auto& key : stale_batch.internal_keys) {
    stale.internal_keys.push_back(serialize(toBatchedInternalNodeKey(key)));
  }
  for (auto& key : stale_batch.leaf_keys) {
    stale.leaf_keys.push_back(serialize(leafKeyToVersionedKey(key)));
  }
  auto tree_version_key = serialize(TreeVersion{stale_batch.stale_since_version.value()});
  batch.put(BLOCK_MERKLE_STALE_CF, tree_version_key, serialize(stale));
}

template <typename Batch>
void putMerkleNodes(Batch& batch, sparse_merkle::UpdateBatch&& update_batch) {
  for (const auto& [leaf_key, leaf_val] : update_batch.leaf_nodes) {
    auto ser_key = serialize(leafKeyToVersionedKey(leaf_key));
    batch.put(BLOCK_MERKLE_LEAF_NODES_CF, ser_key, leaf_val.value.string_view());
  }
  for (auto&& [internal_key, internal_node] : update_batch.internal_nodes) {
    auto ser_key = serialize(toBatchedInternalNodeKey(std::move(internal_key)));
    batch.put(BLOCK_MERKLE_INTERNAL_NODES_CF, ser_key, serializeBatchedInternalNode(std::move(internal_node)));
  }

  // We always add a root key at version 0 with a value that points to the latest root.
  auto tree_version = update_batch.stale.stale_since_version.value();
  putLatestTreeVersion(tree_version, batch);

  putStaleKeys(batch, std::move(update_batch.stale));
}

// As part of `deleteLastReachable`, we need to remove the block at the end of the chain.
// We are essentially reverting to the prior version of the merkle tree.
//
// This assumes the block where the nodes are being removed has not been pruned.
void removeMerkleNodes(NativeWriteBatch& batch, BlockId block_id, uint64_t tree_version) {
  // Remove the leaf
  auto block_key = merkleKey(block_id);
  auto leaf_key = sparse_merkle::LeafKey{hash(block_key), tree_version};
  auto versioned_key = serialize(leafKeyToVersionedKey(leaf_key));
  batch.del(BLOCK_MERKLE_LEAF_NODES_CF, versioned_key);

  // Remove the internal nodes.
  auto start = rootKey(tree_version);
  auto end = rootKey(tree_version + 1);
  batch.delRange(BLOCK_MERKLE_INTERNAL_NODES_CF, start, end);

  // Remove the stale index
  batch.del(BLOCK_MERKLE_STALE_CF, serialize(TreeVersion{tree_version}));

  // Update the latest root to point to the previous tree version
  putLatestTreeVersion(tree_version - 1, batch);
}

// Delete keys that can be safely pruned.
// Return any active key hashes.
std::vector<KeyHash> deleteInactiveKeys(BlockId block_id,
                                        std::vector<Hash>&& hashed_keys,
                                        std::vector<std::string>&& keys,
                                        const std::vector<std::optional<TaggedVersion>>& latest_versions,
                                        detail::LocalWriteBatch& batch,
                                        size_t& deletes_counter) {
  std::vector<KeyHash> active_keys;
  for (auto i = 0u; i < hashed_keys.size(); i++) {
    auto& tagged_version = latest_versions[i];
    auto& hashed_key = hashed_keys[i];
    auto& key = keys[i];
    ConcordAssert(tagged_version.has_value());
    ConcordAssertLE(block_id, tagged_version->version);

    if (block_id == tagged_version->version) {
      if (tagged_version->deleted) {
        // The latest version is a tombstone. We can delete the key and version.
        auto versioned_key = serialize(VersionedKey{KeyHash{hashed_key}, block_id});
        batch.del(BLOCK_MERKLE_KEYS_CF, versioned_key);
        batch.del(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, key);
        deletes_counter++;
      } else {
        active_keys.push_back(KeyHash{hashed_key});
      }
    } else {
      // block_id < tagged_version->version
      // The key has been overwritten. Delete it.
      auto versioned_key = serialize(VersionedKey{KeyHash{hashed_key}, block_id});
      batch.del(BLOCK_MERKLE_KEYS_CF, versioned_key);
      deletes_counter++;
    }
  }
  return active_keys;
}

// Return all keys that are still active in the pruned block
std::vector<KeyHash> remainingActiveKeys(std::vector<KeyHash>& previously_active, std::vector<KeyHash>& deleted) {
  auto cmp = [](auto& a, auto& b) { return a.value < b.value; };
  std::sort(deleted.begin(), deleted.end(), cmp);
  std::sort(previously_active.begin(), previously_active.end(), cmp);
  auto active_keys = std::vector<KeyHash>{};
  std::set_difference(previously_active.begin(),
                      previously_active.end(),
                      deleted.begin(),
                      deleted.end(),
                      std::back_inserter(active_keys),
                      cmp);
  return active_keys;
}

template <typename Batch>
void addStaleKeysToDeleteBatch(const ::rocksdb::PinnableSlice& slice, uint64_t tree_version, Batch& batch) {
  auto stale = StaleKeys{};
  deserialize(slice, stale);
  for (auto& key : stale.internal_keys) {
    batch.del(BLOCK_MERKLE_INTERNAL_NODES_CF, key);
  }
  for (auto& key : stale.leaf_keys) {
    batch.del(BLOCK_MERKLE_LEAF_NODES_CF, key);
  }
  batch.del(BLOCK_MERKLE_STALE_CF, serialize(TreeVersion{tree_version}));
}

BlockMerkleCategory::BlockMerkleCategory(const std::shared_ptr<storage::rocksdb::NativeClient>& db) : db_{db} {
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_INTERNAL_NODES_CF, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_LEAF_NODES_CF, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_KEYS_CF, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_STALE_CF, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_PRUNED_BLOCKS_CF, *db);
  tree_ = sparse_merkle::Tree{std::make_shared<Reader>(*db_)};
}

BlockMerkleOutput BlockMerkleCategory::add(BlockId block_id, BlockMerkleInput&& updates, NativeWriteBatch& batch) {
  auto [merkle_value, hashed_added_keys, hashed_deleted_keys] = hashNewBlock(updates);
  putKeys(batch, block_id, std::move(hashed_added_keys), std::move(hashed_deleted_keys), updates);

  auto tree_update_batch = tree_.update({{merkleKey(block_id), merkleValue(merkle_value)}});
  putMerkleNodes(batch, std::move(tree_update_batch));

  auto output = inputToOutput(updates);
  output.root_hash = tree_.get_root_hash().dataArray();
  output.state_root_version = tree_.get_version().value();
  return output;
}

std::optional<Value> BlockMerkleCategory::get(const std::string& key, BlockId block_id) const {
  return get(hash(key), block_id);
}

std::optional<Value> BlockMerkleCategory::get(const Hash& hashed_key, BlockId block_id) const {
  auto key = VersionedKey{KeyHash{hashed_key}, block_id};
  if (auto ser = db_->get(BLOCK_MERKLE_KEYS_CF, serialize(key))) {
    auto v = DbValue{};
    deserialize(*ser, v);
    if (v.deleted) {
      return std::nullopt;
    }
    auto rv = MerkleValue{};
    rv.data = std::move(v.data);
    rv.block_id = block_id;
    return rv;
  }
  return std::nullopt;
}

std::optional<Value> BlockMerkleCategory::getLatest(const std::string& key) const {
  if (auto latest = getLatestVersion(key)) {
    if (!latest->deleted) {
      return get(hash(key), latest->version);
    }
  }
  return std::nullopt;
}

std::optional<TaggedVersion> BlockMerkleCategory::getLatestVersion(const std::string& key) const {
  const auto serialized = db_->getSlice(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, key);
  if (!serialized) {
    return std::nullopt;
  }
  auto version = LatestKeyVersion{};
  deserialize(*serialized, version);
  return TaggedVersion(version.block_id);
}

void BlockMerkleCategory::multiGet(const std::vector<std::string>& keys,
                                   const std::vector<BlockId>& versions,
                                   std::vector<std::optional<Value>>& values) const {
  ConcordAssertEQ(keys.size(), versions.size());
  auto versioned_keys = versionedKeys(keys, versions);
  multiGet(versioned_keys, versions, values);
}

void BlockMerkleCategory::multiGet(const std::vector<Buffer>& versioned_keys,
                                   const std::vector<BlockId>& versions,
                                   std::vector<std::optional<Value>>& values) const {
  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};

  db_->multiGet(BLOCK_MERKLE_KEYS_CF, versioned_keys, slices, statuses);

  values.clear();
  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto& status = statuses[i];
    const auto& slice = slices[i];
    const auto version = versions[i];
    if (status.ok()) {
      auto v = DbValue{};
      deserialize(slice, v);
      if (v.deleted) {
        values.push_back(std::nullopt);
      } else {
        values.push_back(MerkleValue{{version, v.data}});
      }
    } else if (status.IsNotFound()) {
      values.push_back(std::nullopt);
    } else {
      throw std::runtime_error{"BlockMerkleCategory multiGet() failure: " + status.ToString()};
    }
  }
}

void BlockMerkleCategory::multiGetLatestVersion(const std::vector<std::string>& keys,
                                                std::vector<std::optional<TaggedVersion>>& versions) const {
  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};

  db_->multiGet(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, keys, slices, statuses);
  versions.clear();
  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto& status = statuses[i];
    const auto& slice = slices[i];
    if (status.ok()) {
      auto version = LatestKeyVersion{};
      deserialize(slice, version);
      versions.push_back(TaggedVersion(version.block_id));
    } else if (status.IsNotFound()) {
      versions.push_back(std::nullopt);
    } else {
      throw std::runtime_error{"BlockMerkleCategory multiGet() failure: " + status.ToString()};
    }
  }
}

void BlockMerkleCategory::multiGetLatest(const std::vector<std::string>& keys,
                                         std::vector<std::optional<Value>>& values) const {
  auto hashed_keys = hashedKeys(keys);
  std::vector<std::optional<TaggedVersion>> versions;
  multiGetLatestVersion(keys, versions);

  // Generate the set of versioned keys for all keys that have latest versions and are not deleted
  auto versioned_keys = std::vector<Buffer>{};
  auto found_versions = std::vector<BlockId>{};
  for (auto i = 0u; i < hashed_keys.size(); i++) {
    if (versions[i] && !versions[i]->deleted) {
      auto latest_version = versions[i]->version;
      found_versions.push_back(latest_version);
      versioned_keys.push_back(serialize(VersionedKey{KeyHash{hashed_keys[i]}, latest_version}));
    }
  }

  values.clear();
  // Optimize for all keys existing (having latest versions)
  if (versioned_keys.size() == hashed_keys.size()) {
    return multiGet(versioned_keys, found_versions, values);
  }

  // Retrieve only the keys that have latest versions
  auto retrieved_values = std::vector<std::optional<Value>>{};
  multiGet(versioned_keys, found_versions, retrieved_values);

  // Merge any keys that didn't have latest versions along with the retrieved keys.
  auto value_index = 0u;
  for (auto& version : versions) {
    if (version && !version->deleted) {
      values.push_back(retrieved_values[value_index]);
      ++value_index;
    } else {
      values.push_back(std::nullopt);
    }
  }
  ConcordAssertEQ(values.size(), keys.size());
}

std::unordered_map<BlockId, std::vector<KeyHash>> BlockMerkleCategory::findActiveKeysFromPrunedBlocks(
    const std::vector<Hash>& hashed_keys) const {
  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};
  db_->multiGet(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, hashed_keys, slices, statuses);

  auto found = std::unordered_map<BlockId, std::vector<KeyHash>>{};
  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto& status = statuses[i];
    const auto& slice = slices[i];
    if (status.ok()) {
      auto val = BlockVersion{};
      deserialize(slice, val);
      auto& vec = found[val.version];
      vec.push_back(KeyHash{hashed_keys[i]});
    } else if (status.IsNotFound()) {
      continue;
    } else {
      throw std::runtime_error{"BlockMerkleCategory multiGet() failure: " + status.ToString()};
    }
  }
  return found;
}

// Precondition: the pruned block exists
PrunedBlock BlockMerkleCategory::getPrunedBlock(const Buffer& block_key) {
  auto ser_pruned = db_->get(BLOCK_MERKLE_PRUNED_BLOCKS_CF, block_key);
  ConcordAssert(ser_pruned.has_value());
  auto pruned = PrunedBlock{};
  deserialize(*ser_pruned, pruned);
  return pruned;
}

std::pair<SetOfKeyValuePairs, KeysVector> BlockMerkleCategory::rewriteAlreadyPrunedBlocks(
    std::unordered_map<BlockId, std::vector<KeyHash>>& deleted_keys, detail::LocalWriteBatch& batch) {
  auto merkle_blocks_to_rewrite = SetOfKeyValuePairs{};
  auto merkle_blocks_to_delete = KeysVector{};
  for (auto& [block_id, keys] : deleted_keys) {
    auto block_key = serialize(BlockVersion{block_id});
    auto pruned = getPrunedBlock(block_key);
    for (auto& key : keys) {
      auto versioned_key = serialize(VersionedKey{key, block_id});
      batch.del(BLOCK_MERKLE_KEYS_CF, versioned_key);
      batch.del(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(key));
    }
    auto active_keys = remainingActiveKeys(pruned.active_keys, keys);
    if (active_keys.empty()) {
      batch.del(BLOCK_MERKLE_PRUNED_BLOCKS_CF, block_key);
      merkle_blocks_to_delete.push_back({merkleKey(block_id)});
    } else {
      static constexpr bool write_active_keys = false;
      auto merkle_value = computeRootHash(block_id, active_keys, write_active_keys, batch);
      merkle_blocks_to_rewrite.emplace(merkleKey(block_id), merkleValue(merkle_value));
      auto new_pruned = serialize(PrunedBlock{std::move(active_keys)});
      batch.put(BLOCK_MERKLE_PRUNED_BLOCKS_CF, block_key, new_pruned);
    }
  }
  return std::make_pair(merkle_blocks_to_rewrite, merkle_blocks_to_delete);
}
std::set<std::string> BlockMerkleCategory::getStaleActiveKeys(BlockId block_id, const BlockMerkleOutput& out) const {
  std::set<Hash> hash_stale_keys;
  auto [hashed_keys, _, latest_versions] = getLatestVersions(out);
  (void)latest_versions;
  (void)_;
  auto overwritten_active_keys_from_pruned_blocks = findActiveKeysFromPrunedBlocks(hashed_keys);
  for (auto& kv : overwritten_active_keys_from_pruned_blocks) {
    for (const auto& hashed_key : kv.second) {
      hash_stale_keys.emplace(Hash(hashed_key.value));
    }
  }
  std::set<std::string> stale_keys;
  for (auto& key : out.keys) {
    if (std::find(hash_stale_keys.begin(), hash_stale_keys.end(), hash(key.first)) != hash_stale_keys.end()) {
      stale_keys.emplace(key.first);
    }
  }
  return stale_keys;
}
std::vector<std::string> BlockMerkleCategory::getBlockStaleKeys(BlockId block_id, const BlockMerkleOutput& out) const {
  std::vector<Hash> hash_stale_keys;
  auto [hashed_keys, _, latest_versions] = getLatestVersions(out);
  (void)_;
  for (auto i = 0u; i < hashed_keys.size(); i++) {
    auto& tagged_version = latest_versions[i];
    auto& hashed_key = hashed_keys[i];
    ConcordAssert(tagged_version.has_value());
    ConcordAssertLE(block_id, tagged_version->version);

    if (block_id == tagged_version->version) {
      if (tagged_version->deleted) {
        // The latest version is a tombstone.
        hash_stale_keys.push_back(hashed_key);
      }
    } else {
      // block_id < tagged_version->version
      // The key has been overwritten.
      hash_stale_keys.push_back(hashed_key);
    }
  }
  std::vector<std::string> stale_keys;
  for (auto& key : out.keys) {
    if (std::find(hash_stale_keys.begin(), hash_stale_keys.end(), hash(key.first)) != hash_stale_keys.end()) {
      stale_keys.push_back(key.first);
    }
  }
  return stale_keys;
}

size_t BlockMerkleCategory::deleteGenesisBlock(BlockId block_id,
                                               const BlockMerkleOutput& out,
                                               detail::LocalWriteBatch& batch) {
  auto [hashed_keys, keys, latest_versions] = getLatestVersions(out);
  auto overwritten_active_keys_from_pruned_blocks = findActiveKeysFromPrunedBlocks(hashed_keys);
  size_t num_of_deletes = 0;
  for (auto& kv : overwritten_active_keys_from_pruned_blocks) {
    num_of_deletes += kv.second.size();
  }
  auto [block_adds, block_removes] = rewriteAlreadyPrunedBlocks(overwritten_active_keys_from_pruned_blocks, batch);
  auto active_keys =
      deleteInactiveKeys(block_id, std::move(hashed_keys), std::move(keys), latest_versions, batch, num_of_deletes);
  if (active_keys.empty()) {
    block_removes.push_back(merkleKey(block_id));
  } else {
    auto merkle_value = writePrunedBlock(block_id, std::move(active_keys), batch);
    block_adds.emplace(merkleKey(block_id), merkle_value);
  }
  auto update_batch = tree_.update(block_adds, block_removes);
  putMerkleNodes(batch, std::move(update_batch));
  deleteStaleData(out.state_root_version, batch);
  return num_of_deletes;
}

void BlockMerkleCategory::deleteLastReachableBlock(BlockId block_id,
                                                   const BlockMerkleOutput& out,
                                                   NativeWriteBatch& batch) {
  for (const auto& [key, _] : out.keys) {
    (void)_;
    const auto hashed_key = KeyHash{hash(key)};
    const auto versioned_key = serializeThreadLocal(VersionedKey{hashed_key, block_id});
    // Find the previous version of the key and set it as a last version. If not found, then this is
    // the only version of the key and we can remove the latest version index too.
    auto iter = db_->getIterator(BLOCK_MERKLE_KEYS_CF);
    iter.seekAtMost(versioned_key);
    ConcordAssert(iter);
    iter.prev();
    if (iter) {
      auto prev_key = VersionedKey{};
      deserialize(iter.keyView(), prev_key);
      if (prev_key.key_hash == hashed_key) {
        // Preserve the deleted flag from the value into the version index.
        auto deleted = Deleted{};
        deserialize(iter.valueView(), deleted);
        putLatestKeyVersion(batch, key, TaggedVersion(deleted.value, prev_key.version));
      } else {
        // This is the only version of the key - remove the latest version index too.
        batch.del(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, key);
      }
    } else {
      // No previous keys means this is the only version of the key - remove the latest version index too.
      batch.del(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, key);
    }
    // Remove the value for the key at `block_id`.
    batch.del(BLOCK_MERKLE_KEYS_CF, versioned_key);
  }
  removeMerkleNodes(batch, block_id, out.state_root_version);
}

std::tuple<std::vector<Hash>, std::vector<std::string>, std::vector<std::optional<TaggedVersion>>>
BlockMerkleCategory::getLatestVersions(const BlockMerkleOutput& out) const {
  std::vector<Hash> hashed_keys;
  std::vector<std::string> keys;
  hashed_keys.reserve(out.keys.size());
  keys.reserve(out.keys.size());
  for (auto& [key, _] : out.keys) {
    (void)_;
    hashed_keys.push_back(hash(key));
    keys.push_back(key);
  }

  std::vector<std::optional<TaggedVersion>> latest_versions;
  multiGetLatestVersion(keys, latest_versions);

  return std::make_tuple(hashed_keys, keys, latest_versions);
}

template <typename Batch>
void BlockMerkleCategory::putLastDeletedTreeVersion(uint64_t tree_version, Batch& batch) {
  batch.put(BLOCK_MERKLE_STALE_CF, serialize(TreeVersion{0}), serialize(TreeVersion{tree_version}));
}

uint64_t BlockMerkleCategory::getLastDeletedTreeVersion() const {
  // We store the last deleted tree version at stale index 0. This allows us to delete all stale
  // data for versions last_deleted + 1 up until `tree_version`.
  auto ser_last_deleted_version = db_->get(BLOCK_MERKLE_STALE_CF, serialize(TreeVersion{0}));
  uint64_t last_deleted = 0;
  if (ser_last_deleted_version.has_value()) {
    auto last_deleted_version = TreeVersion{};
    deserialize(*ser_last_deleted_version, last_deleted_version);
    last_deleted = last_deleted_version.version;
  }
  return last_deleted;
}

void BlockMerkleCategory::deleteStaleBatch(uint64_t start, uint64_t end) {
  auto keys = std::vector<Buffer>{};
  keys.reserve(end - start);
  for (auto i = start; i < end; ++i) {
    keys.push_back(serialize(TreeVersion{i}));
  }

  auto batch = db_->getBatch();
  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};
  db_->multiGet(BLOCK_MERKLE_STALE_CF, keys, slices, statuses);

  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto& status = statuses[i];
    const auto& slice = slices[i];
    if (status.ok()) {
      addStaleKeysToDeleteBatch(slice, start + i, batch);
    } else {
      throw std::runtime_error{"BlockMerkleCategory multiGet() failure: " + status.ToString()};
    }
  }
  putLastDeletedTreeVersion(end - 1, batch);
  db_->write(std::move(batch));
}

void BlockMerkleCategory::deleteStaleData(uint64_t tree_version, detail::LocalWriteBatch& batch) {
  auto last_deleted = getLastDeletedTreeVersion();
  ConcordAssertLT(last_deleted, tree_version);

  // Delete any stale keys for tree versions from prior prunings (i.e. trees not associated with blocks).
  static constexpr uint64_t batch_size = 50;
  auto start = last_deleted + 1;
  auto end = start;
  do {
    start = end;
    end = std::min(start + batch_size, tree_version);
    deleteStaleBatch(start, end);
  } while (end < tree_version);

  // Create a batch to delete stale keys for this tree version
  auto ser_stale = db_->getSlice(BLOCK_MERKLE_STALE_CF, serialize(TreeVersion{tree_version}));
  ConcordAssert(ser_stale.has_value());
  addStaleKeysToDeleteBatch(*ser_stale, tree_version, batch);
  putLastDeletedTreeVersion(tree_version, batch);
}

MerkleBlockValue BlockMerkleCategory::computeRootHash(BlockId block_id,
                                                      const std::vector<KeyHash>& active_keys,
                                                      bool write_active_key,
                                                      detail::LocalWriteBatch& batch) {
  auto hasher = Hasher{};
  hasher.init();
  MerkleBlockValue merkle_value;
  auto block_version = serialize(BlockVersion{block_id});
  for (auto& key : active_keys) {
    if (write_active_key) {
      // Save a reference to the active key for use in pruning of the block that "de-activates" this
      // key by overwriting or deleting it.
      batch.put(BLOCK_MERKLE_ACTIVE_KEYS_FROM_PRUNED_BLOCKS_CF, serialize(key), block_version);
    }

    // Calculate a new root hash for all keys in the block
    auto versioned_key = serialize(VersionedKey{key, block_id});
    auto value = db_->getSlice(BLOCK_MERKLE_KEYS_CF, versioned_key);
    ConcordAssert(value.has_value());
    auto val_hash = hash(*value);
    hasher.update(key.value.data(), key.value.size());
    hasher.update(val_hash.data(), val_hash.size());
  }
  merkle_value.root_hash = hasher.finish();
  return merkle_value;
}

Sliver BlockMerkleCategory::writePrunedBlock(BlockId block_id,
                                             std::vector<KeyHash>&& active_keys,
                                             detail::LocalWriteBatch& batch) {
  static constexpr bool write_active_keys = true;
  auto merkle_value = computeRootHash(block_id, active_keys, write_active_keys, batch);
  auto block_key = serialize(BlockVersion{block_id});
  batch.put(BLOCK_MERKLE_PRUNED_BLOCKS_CF, block_key, serialize(PrunedBlock{std::move(active_keys)}));
  return merkleValue(merkle_value);
}

uint64_t BlockMerkleCategory::getLatestTreeVersion() const {
  if (auto latest_root_key = db_->get(BLOCK_MERKLE_INTERNAL_NODES_CF, rootKey(0))) {
    BatchedInternalNodeKey key{};
    deserialize(*latest_root_key, key);
    return key.version;
  }
  return 0;
}

sparse_merkle::BatchedInternalNode BlockMerkleCategory::Reader::get_latest_root() const {
  if (auto latest_root_key = db_.get(BLOCK_MERKLE_INTERNAL_NODES_CF, rootKey(0))) {
    if (auto serialized = db_.get(BLOCK_MERKLE_INTERNAL_NODES_CF, *latest_root_key)) {
      return deserializeBatchedInternalNode(*serialized);
    }
    // TODO: LOG THIS
    // The merkle tree should never ask for a version that doesn't exist.
    std::terminate();
  }
  return sparse_merkle::BatchedInternalNode{};
}

sparse_merkle::BatchedInternalNode BlockMerkleCategory::Reader::get_internal(
    const sparse_merkle::InternalNodeKey& key) const {
  auto ser_key = serialize(toBatchedInternalNodeKey(key));
  if (auto serialized = db_.get(BLOCK_MERKLE_INTERNAL_NODES_CF, ser_key)) {
    return deserializeBatchedInternalNode(*serialized);
  }
  // TODO: LOG THIS
  // The merkle tree should never ask for a version that doesn't exist.
  std::terminate();
}

}  // namespace concord::kvbc::categorization::detail
