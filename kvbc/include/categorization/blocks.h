// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

  void add(const std::string& category_id, MerkleUpdatesInfo&& updates_info) {
    data.categories_updates_info.emplace(category_id, std::move(updates_info));
  }

  void add(const std::string& category_id, KeyValueUpdatesInfo&& updates_info) {
    data.categories_updates_info.emplace(category_id, std::move(updates_info));
  }

  void add(const std::string& category_id, ImmutableUpdatesInfo&& updates_info) {
    data.categories_updates_info.emplace(category_id, std::move(updates_info));
  }

  void setParentHash(const BlockDigest& parent_hash) {
    std::copy(parent_hash.begin(), parent_hash.end(), data.parent_digest.begin());
  }

  BlockId id() const { return data.block_id; }

  static const detail::Buffer& serialize(const Block& block) { return detail::serialize(block.data); }

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
    return detail::serialize(key);
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
  RawBlock(const Block& block, const storage::rocksdb::NativeClient& native_client);

  MerkleUpdatesData getUpdates(const std::string& category_id,
                               const MerkleUpdatesInfo& update_info,
                               const BlockId& block_id,
                               const storage::rocksdb::NativeClient& native_client);

  KeyValueUpdatesData getUpdates(const std::string& category_id,
                                 const KeyValueUpdatesInfo& update_info,
                                 const BlockId& block_id,
                                 const storage::rocksdb::NativeClient& native_client);

  ImmutableUpdatesData getUpdates(const std::string& category_id,
                                  const ImmutableUpdatesInfo& update_info,
                                  const BlockId& block_id,
                                  const storage::rocksdb::NativeClient& native_client);

  template <typename T>
  static RawBlock deserialize(const T& input) {
    RawBlock output;
    detail::deserialize(input, output.data);
    return output;
  }

  static const detail::Buffer& serialize(const RawBlock& raw) { return detail::serialize(raw.data); }

  RawBlockData data;
};

}  // namespace concord::kvbc::categorization