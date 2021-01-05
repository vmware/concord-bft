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
using concordUtils::Sliver;

namespace concord::kvbc::categorization::detail {

MerkleBlockValue hashUpdate(const BlockMerkleInput& updates) {
  MerkleBlockValue value;
  value.hashed_added_keys.reserve(updates.kv.size());
  value.hashed_deleted_keys.reserve(updates.deletes.size());

  // root_hash = h((h(k1) || h(v1)) || ... || (h(kN) || h(vN) || h(dk1) || ... || h(dkN))
  auto root_hasher = Hasher{};
  root_hasher.init();
  auto kv_hasher = Hasher{};

  // Hash all keys and values as part of the root hash
  for (const auto& [k, v] : updates.kv) {
    auto key_hash = kv_hasher.digest(k.data(), k.size());
    auto val_hash = kv_hasher.digest(v.data(), v.size());
    value.hashed_added_keys.push_back(KeyHash{key_hash});
    root_hasher.update(key_hash.data(), key_hash.size());
    root_hasher.update(val_hash.data(), val_hash.size());
  }

  for (const auto& k : updates.deletes) {
    auto key_hash = kv_hasher.digest(k.data(), k.size());
    value.hashed_deleted_keys.push_back(KeyHash{key_hash});
    root_hasher.update(key_hash.data(), key_hash.size());
  }
  value.root_hash = root_hasher.finish();
  return value;
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

BlockMerkleCategory::BlockMerkleCategory(const std::shared_ptr<storage::rocksdb::NativeClient>& db) : db_{db} {
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_INTERNAL_NODES_CF, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_LEAF_NODES_CF, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_LATEST_KEY_VERSION, *db);
  createColumnFamilyIfNotExisting(BLOCK_MERKLE_KEYS_CF, *db);
  tree_ = sparse_merkle::Tree{std::make_shared<Reader>(*db_)};
}

BlockMerkleOutput BlockMerkleCategory::add(BlockId block_id, BlockMerkleInput&& updates, NativeWriteBatch& batch) {
  auto merkle_value = hashUpdate(updates);
  putKeys(
      batch, block_id, std::move(merkle_value.hashed_added_keys), std::move(merkle_value.hashed_deleted_keys), updates);

  // We don't want to actually write the hashed keys for new blocks.
  // We only write the remaining active keys when the block is pruned.
  merkle_value.hashed_added_keys.clear();
  merkle_value.hashed_deleted_keys.clear();

  auto block_key = serialize(BlockKey{block_id});
  auto merkle_key = Sliver::copy((const char*)block_key.data(), block_key.size());
  auto ser_value = serialize(merkle_value);
  auto ser_value_sliver = Sliver::copy((const char*)ser_value.data(), ser_value.size());
  auto tree_update_batch = tree_.update(SetOfKeyValuePairs{{merkle_key, ser_value_sliver}});

  auto tree_version = tree_update_batch.stale.stale_since_version.value();
  putMerkleNodes(batch, std::move(tree_update_batch), tree_version);

  auto output = inputToOutput(updates);
  output.root_hash = tree_.get_root_hash().dataArray();
  output.state_root_version = tree_version;
  return output;
}

std::optional<MerkleValue> BlockMerkleCategory::get(const std::string& key, BlockId block_id) const {
  return get(hash(key), block_id);
}

std::optional<MerkleValue> BlockMerkleCategory::get(const Hash& hashed_key, BlockId block_id) const {
  auto key = VersionedKey{KeyHash{hashed_key}, block_id};
  if (auto val = db_->get(BLOCK_MERKLE_KEYS_CF, serialize(key))) {
    auto rv = MerkleValue{};
    rv.data = std::move(*val);
    rv.block_id = block_id;
    return rv;
  }
  return std::nullopt;
}

std::optional<MerkleValue> BlockMerkleCategory::getLatest(const std::string& key) const {
  auto hashed_key = hash(key);
  if (auto latest = getLatestVersion(hashed_key)) {
    if (!latest->deleted) {
      return get(hashed_key, latest->version);
    }
  }
  return std::nullopt;
}

std::optional<TaggedVersion> BlockMerkleCategory::getLatestVersion(const std::string& key) const {
  return getLatestVersion(hash(key));
}

std::optional<TaggedVersion> BlockMerkleCategory::getLatestVersion(const Hash& hashed_key) const {
  const auto serialized = db_->getSlice(BLOCK_MERKLE_LATEST_KEY_VERSION, hashed_key);
  if (!serialized) {
    return std::nullopt;
  }
  auto version = LatestKeyVersion{};
  deserialize(*serialized, version);
  return TaggedVersion(version.block_id);
}

void BlockMerkleCategory::multiGet(const std::vector<std::string>& keys,
                                   const std::vector<BlockId>& versions,
                                   std::vector<std::optional<MerkleValue>>& values) const {
  ConcordAssertEQ(keys.size(), versions.size());
  auto versioned_keys = versionedKeys(keys, versions);
  multiGet(versioned_keys, versions, values);
}

void BlockMerkleCategory::multiGet(const std::vector<Buffer>& versioned_keys,
                                   const std::vector<BlockId>& versions,
                                   std::vector<std::optional<MerkleValue>>& values) const {
  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};

  db_->multiGet(BLOCK_MERKLE_KEYS_CF, versioned_keys, slices, statuses);

  values.clear();
  for (auto i = 0ull; i < slices.size(); ++i) {
    const auto& status = statuses[i];
    const auto& slice = slices[i];
    const auto version = versions[i];
    if (status.ok()) {
      values.push_back(MerkleValue{{version, slice.ToString()}});
    } else if (status.IsNotFound()) {
      values.push_back(std::nullopt);
    } else {
      throw std::runtime_error{"BlockMerkleCategory multiGet() failure: " + status.ToString()};
    }
  }
}

void BlockMerkleCategory::multiGetLatestVersion(const std::vector<std::string>& keys,
                                                std::vector<std::optional<TaggedVersion>>& versions) const {
  auto hashed_keys = hashedKeys(keys);
  multiGetLatestVersion(hashed_keys, versions);
}

void BlockMerkleCategory::multiGetLatestVersion(const std::vector<Hash>& hashed_keys,
                                                std::vector<std::optional<TaggedVersion>>& versions) const {
  auto slices = std::vector<::rocksdb::PinnableSlice>{};
  auto statuses = std::vector<::rocksdb::Status>{};

  db_->multiGet(BLOCK_MERKLE_LATEST_KEY_VERSION, hashed_keys, slices, statuses);
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
                                         std::vector<std::optional<MerkleValue>>& values) const {
  auto hashed_keys = hashedKeys(keys);
  std::vector<std::optional<TaggedVersion>> versions;
  multiGetLatestVersion(hashed_keys, versions);

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
  auto retrieved_values = std::vector<std::optional<MerkleValue>>{};
  multiGet(versioned_keys, found_versions, retrieved_values);

  // Merge any keys that didn't have latest versions along with the retrieved keys.
  auto value_index = 0u;
  for (auto& version : versions) {
    if (version) {
      values.push_back(retrieved_values[value_index]);
      ++value_index;
    } else {
      values.push_back(std::nullopt);
    }
  }
  ConcordAssertEQ(values.size(), keys.size());
}

void BlockMerkleCategory::putKeys(NativeWriteBatch& batch,
                                  uint64_t block_id,
                                  std::vector<KeyHash>&& hashed_added_keys,
                                  std::vector<KeyHash>&& hashed_deleted_keys,
                                  BlockMerkleInput& updates) {
  auto kv_it = updates.kv.begin();
  for (auto key_it = hashed_added_keys.begin(); key_it != hashed_added_keys.end(); key_it++) {
    // Write the versioned key/value pair used for direct key lookup
    batch.put(BLOCK_MERKLE_KEYS_CF, serialize(VersionedKey{*key_it, block_id}), kv_it->second);

    // Put the latest version of the key
    batch.put(BLOCK_MERKLE_LATEST_KEY_VERSION, key_it->value, serialize(LatestKeyVersion{block_id}));

    kv_it++;
  }

  const bool deleted = true;
  for (auto key_it = hashed_deleted_keys.begin(); key_it != hashed_deleted_keys.end(); key_it++) {
    BlockId latest = TaggedVersion(deleted, block_id).encode();
    batch.put(BLOCK_MERKLE_LATEST_KEY_VERSION, key_it->value, serialize(LatestKeyVersion{latest}));
  }
}

void BlockMerkleCategory::putMerkleNodes(NativeWriteBatch& batch,
                                         sparse_merkle::UpdateBatch&& update_batch,
                                         uint64_t tree_version) {
  ConcordAssertEQ(1, update_batch.leaf_nodes.size());
  const auto& [leaf_key, leaf_val] = update_batch.leaf_nodes[0];
  auto ser_key = serialize(leafKeyToVersionedKey(leaf_key));
  batch.put(BLOCK_MERKLE_LEAF_NODES_CF, ser_key, leaf_val.value.string_view());

  for (auto& [internal_key, internal_node] : update_batch.internal_nodes) {
    auto ser_key = serialize(toBatchedInternalNodeKey(std::move(internal_key)));
    batch.put(BLOCK_MERKLE_INTERNAL_NODES_CF, ser_key, serializeBatchedInternalNode(std::move(internal_node)));
  }

  // We always add a root key at version 0 with a value that points to the latest root.
  batch.put(BLOCK_MERKLE_INTERNAL_NODES_CF, rootKey(0), rootKey(tree_version));
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
