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

#include "categorization/blocks.h"

namespace concord::kvbc::categorization {

//////////////////////////////////// RAW BLOCKS//////////////////////////////////////

// the constructor converts the input block i.e. the update_infos into a raw block
RawBlock::RawBlock(const Block& block, const storage::rocksdb::NativeClient& native_client) {
  // parent digest (std::copy?)
  data.parent_digest = block.data.parent_digest;
  // recontruct updates of categories
  for (auto& [cat_id, update_info] : block.data.categories_updates_info) {
    std::visit(
        [category_id = cat_id, &block, this, &native_client](const auto& update_info) {
          auto category_updates = getRawUpdates(category_id, update_info, block.id(), native_client);
          data.category_updates.emplace(category_id, std::move(category_updates));
        },
        update_info);
  }
}

// Reconstructs the updates data as recieved from the user
// This set methods are overloaded in order to construct the appropriate updates

// Merkle updates reconstruction
RawBlockMerkleUpdates RawBlock::getRawUpdates(const std::string& category_id,
                                              const MerkleUpdatesInfo& update_info,
                                              const BlockId& block_id,
                                              const storage::rocksdb::NativeClient& native_client) {
  // For old serialization
  using namespace concord::kvbc::v2MerkleTree;
  RawBlockMerkleUpdates data;
  // Copy state hash (std::copy?)
  data.root_hash = update_info.root_hash;
  // Iterate over the keys:
  // if deleted, add to the deleted set.
  // else generate a db key, serialize it and
  // get the value from the corresponding column family
  for (auto& [key, flag] : update_info.keys) {
    if (flag.deleted) {
      data.updates.deletes.push_back(key);
      continue;
    }
    // E.L see how we can optimize the sliver temporary allocation
    auto db_key =
        v2MerkleTree::detail::DBKeyManipulator::genDataDbKey(Key(std::string(key)), update_info.state_root_version);
    auto val = native_client.get(category_id, db_key);
    if (!val.has_value()) {
      throw std::logic_error("Couldn't find value for key");
    }
    // E.L serializtion of the Merkle to CMF will be in later phase
    auto dbLeafVal = v2MerkleTree::detail::deserialize<v2MerkleTree::detail::DatabaseLeafValue>(
        concordUtils::Sliver{std::move(val.value())});
    ConcordAssert(dbLeafVal.addedInBlockId == block_id);

    data.updates.kv[key] = dbLeafVal.leafNode.value.toString();
  }

  return data;
}

// KeyValueUpdatesData updates reconstruction
RawBlockKeyValueUpdates RawBlock::getRawUpdates(const std::string& category_id,
                                                const KeyValueUpdatesInfo& update_info,
                                                const BlockId& block_id,
                                                const storage::rocksdb::NativeClient& native_client) {
  RawBlockKeyValueUpdates data;
  return data;
}

// immutable updates reconstruction
RawBlockImmutableUpdates RawBlock::getRawUpdates(const std::string& category_id,
                                                 const ImmutableUpdatesInfo& update_info,
                                                 const BlockId& block_id,
                                                 const storage::rocksdb::NativeClient& native_client) {
  RawBlockImmutableUpdates data;
  return data;
}

}  // namespace concord::kvbc::categorization