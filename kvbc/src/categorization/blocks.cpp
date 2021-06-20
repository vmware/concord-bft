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
#include "categorization/block_merkle_category.h"
#include "categorization/versioned_kv_category.h"

namespace concord::kvbc::categorization {

//////////////////////////////////// RAW BLOCKS//////////////////////////////////////

// the constructor converts the input block i.e. the update_infos into a raw block
RawBlock::RawBlock(const Block& block,
                   const std::shared_ptr<storage::rocksdb::NativeClient>& native_client,
                   const CategoriesMap& categorires) {
  // parent digest (std::copy?)
  data.parent_digest = block.data.parent_digest;
  // recontruct updates of categories
  for (auto& [cat_id, update_info] : block.data.categories_updates_info) {
    std::visit(
        [category_id = cat_id, &block, this, &native_client, &categorires](const auto& update_info) {
          auto category_updates = getUpdates(category_id, update_info, block.id(), native_client, categorires);
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
                                      const std::shared_ptr<storage::rocksdb::NativeClient>& native_client,
                                      const CategoriesMap& categorires) {
  ConcordAssert(categorires.count(category_id) == 1);
  addRootHash(category_id, data, update_info);
  BlockMerkleInput data;
  const auto& cat = std::get<detail::BlockMerkleCategory>(categorires.at(category_id));
  for (auto& [key, flag] : update_info.keys) {
    if (flag.deleted) {
      data.deletes.push_back(key);
      continue;
    }
    // get value of the key for a version from storage via the category
    const auto& val = cat.get(key, block_id);
    if (!val.has_value()) {
      LOG_FATAL(CAT_BLOCK_LOG, "Couldn't find value for key [" << key << "] (Merkle category)");
      ConcordAssert(false);
    }
    // Merkle map is string(key) to string(value)
    data.kv[key] = detail::asMerkle(*val).data;
  }

  return data;
}

// VersionedInput reconstruction
VersionedInput RawBlock::getUpdates(const std::string& category_id,
                                    const VersionedOutput& update_info,
                                    const BlockId& block_id,
                                    const std::shared_ptr<storage::rocksdb::NativeClient>& native_client,
                                    const CategoriesMap& categorires) {
  ConcordAssert(categorires.count(category_id) == 1);
  addRootHash(category_id, data, update_info);
  VersionedInput data;
  data.calculate_root_hash = update_info.root_hash.has_value();
  const auto& cat = std::get<detail::VersionedKeyValueCategory>(categorires.at(category_id));
  for (const auto& [key, flags] : update_info.keys) {
    if (flags.deleted) {
      data.deletes.push_back(key);
      continue;
    }
    // get value of the key for a version from storage via the category
    auto val = cat.get(key, block_id);
    if (!val.has_value()) {
      LOG_FATAL(CAT_BLOCK_LOG, "Couldn't find value for key [" << key << "] (versioned kv category)");
      ConcordAssert(false);
    }
    // reconstruct the versioned value which is the key and stale_on_update
    data.kv[key] = ValueWithFlags{detail::asVersioned(*val).data, flags.stale_on_update};
  }
  return data;
}

// immutable updates reconstruction
// Iterate over the keys from the update info.
// get the value of a key from the DB by performing get on the category.
// assemble a ImmutableValueUpdate from value and tags.
ImmutableInput RawBlock::getUpdates(const std::string& category_id,
                                    const ImmutableOutput& update_info,
                                    const BlockId& block_id,
                                    const std::shared_ptr<storage::rocksdb::NativeClient>& native_client,
                                    const CategoriesMap& categorires) {
  ConcordAssert(categorires.count(category_id) == 1);
  addRootHash(category_id, data, update_info);
  ImmutableInput data;
  data.calculate_root_hash = update_info.tag_root_hashes.has_value();
  const auto& cat = std::get<detail::ImmutableKeyValueCategory>(categorires.at(category_id));
  for (const auto& [key, tags] : update_info.tagged_keys) {
    // get value of the key for a version from storage via the category
    auto val = cat.get(key, block_id);
    if (!val.has_value()) {
      LOG_FATAL(CAT_BLOCK_LOG, "Couldn't find value for key [" << key << "] (immutable category)");
      ConcordAssert(false);
    }
    // the tags are being stored in the block itself, and the value we get from storage via the category
    data.kv[key] = ImmutableValueUpdate{detail::asImmutable(*val).data, tags};
  }
  return data;
}

}  // namespace concord::kvbc::categorization
