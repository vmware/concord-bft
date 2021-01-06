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

#include "categorization/blocks.h"
#include "categorization/immutable_kv_category.h"

namespace concord::kvbc::categorization {

//////////////////////////////////// RAW BLOCKS//////////////////////////////////////

// the constructor converts the input block i.e. the update_infos into a raw block
RawBlock::RawBlock(const Block& block, const std::shared_ptr<storage::rocksdb::NativeClient>& native_client) {
  // parent digest (std::copy?)
  data.parent_digest = block.data.parent_digest;
  // recontruct updates of categories
  for (auto& [cat_id, update_info] : block.data.categories_updates_info) {
    std::visit(
        [category_id = cat_id, &block, this, &native_client](const auto& update_info) {
          auto category_updates = getUpdates(category_id, update_info, block.id(), native_client);
          data.updates.kv.emplace(category_id, std::move(category_updates));
        },
        update_info);
  }
}

// Reconstructs the updates data as recieved from the user
// This set methods are overloaded in order to construct the appropriate updates

// Merkle updates reconstruction
BlockMerkleInput RawBlock::getUpdates(const std::string& category_id,
                                      const BlockMerkleOutput& update_info,
                                      const BlockId& block_id,
                                      const std::shared_ptr<storage::rocksdb::NativeClient>& native_client) {
  // For old serialization
  using namespace concord::kvbc::v2MerkleTree;
  BlockMerkleInput data;
  // Iterate over the keys:
  // if deleted, add to the deleted set.
  // else generate a db key, serialize it and
  // get the value from the corresponding column family
  for (auto& [key, flag] : update_info.keys) {
    if (flag.deleted) {
      data.deletes.push_back(key);
      continue;
    }
    // E.L see how we can optimize the sliver temporary allocation
    auto db_key =
        v2MerkleTree::detail::DBKeyManipulator::genDataDbKey(Key(std::string(key)), update_info.state_root_version);
    auto val = native_client->get(category_id, db_key);
    if (!val.has_value()) {
      throw std::runtime_error("Couldn't find value for key");
    }
    // E.L serializtion of the Merkle to CMF will be in later phase
    auto dbLeafVal = v2MerkleTree::detail::deserialize<v2MerkleTree::detail::DatabaseLeafValue>(
        concordUtils::Sliver{std::move(val.value())});
    ConcordAssert(dbLeafVal.addedInBlockId == block_id);

    data.kv[key] = dbLeafVal.leafNode.value.toString();
  }

  return data;
}

// VersionedInput reconstruction
VersionedInput RawBlock::getUpdates(const std::string& category_id,
                                    const VersionedOutput& update_info,
                                    const BlockId& block_id,
                                    const std::shared_ptr<storage::rocksdb::NativeClient>& native_client) {
  VersionedInput data;

  return data;
}

// immutable updates reconstruction
// Iterate over the keys from the update info.
// get the value of a key from the DB by performing get on the category.
// assemble a ImmutableValueUpdate from value and tags.
ImmutableInput RawBlock::getUpdates(const std::string& category_id,
                                    const ImmutableOutput& update_info,
                                    const BlockId& block_id,
                                    const std::shared_ptr<storage::rocksdb::NativeClient>& native_client) {
  ImmutableInput data;
  detail::ImmutableKeyValueCategory imm{category_id, native_client};
  for (const auto& [key, tags] : update_info.tagged_keys) {
    auto val = imm.get(key, block_id);
    if (!val.has_value()) {
      throw std::runtime_error("Couldn't find value for key (immutable category)");
    }
    data.kv[key] = ImmutableValueUpdate{detail::asImmutable(*val).data, tags};
  }
  return data;
}

}  // namespace concord::kvbc::categorization
