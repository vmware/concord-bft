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

#pragma once

#include "updates.h"
#include <utility>
#include <block_digest.h>
#include "details.h"
#include "merkle_tree_serialization.h"
#include "merkle_tree_key_manipulator.h"
#include "merkle_tree_db_adapter.h"
#include "sliver.hpp"
#include "column_families.h"
#include "categorization/types.h"

namespace concord::kvbc::categorization {

// Block is composed out of:
// - BlockID
// - Parent digest
// - Categories' updates_info where update info is essentially a list of keys, metadata on the keys and a hash of the
// category state.
struct Block {
  Block() {
    data.block_id = 0;
    data.parent_digest.fill(0);
  }

  Block(const BlockId block_id) {
    data.block_id = block_id;
    data.parent_digest.fill(0);
  }

  void add(const std::string& category_id, BlockMerkleOutput&& updates_info) {
    data.categories_updates_info.emplace(category_id, std::move(updates_info));
  }

  void add(const std::string& category_id, VersionedOutput&& updates_info) {
    data.categories_updates_info.emplace(category_id, std::move(updates_info));
  }

  void add(const std::string& category_id, ImmutableOutput&& updates_info) {
    data.categories_updates_info.emplace(category_id, std::move(updates_info));
  }

  void setParentHash(const BlockDigest& parent_hash) {
    std::copy(parent_hash.begin(), parent_hash.end(), data.parent_digest.begin());
  }

  BlockId id() const { return data.block_id; }

  static const detail::Buffer& serialize(const Block& block) { return detail::serializeThreadLocal(block.data); }

  template <typename T>
  static Block deserialize(const T& input) {
    Block output;
    detail::deserialize(input, output.data);
    return output;
  }

  // Generate block key from block ID
  // Using CMF for big endian
  static const detail::Buffer& generateKey(const BlockId block_id) {
    BlockKey key{block_id};
    return detail::serializeThreadLocal(key);
  }

  BlockData data;
};

//////////////////////////////////// RAW BLOCKS//////////////////////////////////////

// Raw block is the unit which we can transfer and reconstruct a block in a remote replica.
// It contains all the relevant updates as recieved from the client in the initial call to storage.
// It also contains:
// - parent digest
// - state hash per category (if exists) E.L check why do we pass it.
struct RawBlock {
  RawBlock() = default;
  RawBlock(const Block& block,
           const std::shared_ptr<storage::rocksdb::NativeClient>& native_client,
           const CategoriesMap& categorires);

  BlockMerkleInput getUpdates(const std::string& category_id,
                              const BlockMerkleOutput& update_info,
                              const BlockId& block_id,
                              const std::shared_ptr<storage::rocksdb::NativeClient>& native_client,
                              const CategoriesMap& categorires);

  VersionedInput getUpdates(const std::string& category_id,
                            const VersionedOutput& update_info,
                            const BlockId& block_id,
                            const std::shared_ptr<storage::rocksdb::NativeClient>& native_client,
                            const CategoriesMap& categorires);

  ImmutableInput getUpdates(const std::string& category_id,
                            const ImmutableOutput& update_info,
                            const BlockId& block_id,
                            const std::shared_ptr<storage::rocksdb::NativeClient>& native_client,
                            const CategoriesMap& categorires);

  template <typename T>
  static RawBlock deserialize(const T& input) {
    RawBlock output;
    detail::deserialize(input, output.data);
    return output;
  }

  static const detail::Buffer& serialize(const RawBlock& raw) { return detail::serializeThreadLocal(raw.data); }

  RawBlockData data;
};

inline void addRootHash(const std::string& category_id, RawBlockData& data, const BlockMerkleOutput& update_info) {
  data.block_merkle_root_hash[category_id] = update_info.root_hash;
}

// If optional is null do not include in the raw block
inline void addRootHash(const std::string& category_id, RawBlockData& data, const VersionedOutput& update_info) {
  if (!update_info.root_hash) return;
  data.versioned_root_hash[category_id] = update_info.root_hash.value();
}

inline void addRootHash(const std::string& category_id, RawBlockData& data, const ImmutableOutput& update_info) {
  if (!update_info.tag_root_hashes) return;
  data.immutable_root_hashes[category_id] = update_info.tag_root_hashes.value();
}

inline bool operator==(const RawBlock& l, const RawBlock& r) { return l.data == r.data; }
inline bool operator!=(const RawBlock& l, const RawBlock& r) { return !(l == r); }

}  // namespace concord::kvbc::categorization
