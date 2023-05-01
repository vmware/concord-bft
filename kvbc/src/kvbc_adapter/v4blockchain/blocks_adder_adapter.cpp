// Concord
//
// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "kvbc_adapter/v4blockchain/blocks_adder_adapter.hpp"
#include "kvbc_app_filter/kvbc_key_types.h"
#include "categorization/db_categories.h"
#include "bftengine/ReplicaConfig.hpp"

using bftEngine::ReplicaConfig;

namespace concord::kvbc::adapter::v4blockchain {

void BlocksAdderAdapter::getParentHeaderHash(const BlockId block_id, uint256be_t &parent_hash) {
  auto key = BlockHeader::blockNumAsKeyToBlockHash((uint8_t *)&block_id, sizeof(block_id));
  LOG_DEBUG(GL, KVLOG(block_id, concordUtils::bufferToHex(key.c_str(), key.size())));
  auto opt_val = kvbc_->getLatest(kvbc::categorization::kExecutionPrivateCategory, key);
  if (opt_val) {
    auto val = std::get<kvbc::categorization::VersionedValue>(*opt_val);
    static_assert(BlockHeader::kHashSizeInBytes == 32);
    std::memcpy(parent_hash.data(), val.data.data(), BlockHeader::kHashSizeInBytes);
    LOG_INFO(GL, concordUtils::bufferToHex(parent_hash.data(), parent_hash.size()));
  }
}

void BlocksAdderAdapter::writeParentHash(BlockId block_id, uint256be_t &parent_hash) {
  if (block_id) {
    getParentHeaderHash(block_id - 1, parent_hash);
    ConcordAssertEQ(parent_hash.empty(), false);
  }
}

void BlocksAdderAdapter::addHeader(concord::kvbc::categorization::Updates &updates) {
  auto cat_itr = updates.categoryUpdates().kv.find(kvbc::categorization::kExecutionPrivateCategory);

  BlockHeader header;
  bool found_existing_header = false;

  BlockId to_be_written_block_num = kvbc_->getLastReachableBlockId();
  auto to_be_written_block_header_key =
      BlockHeader::blockNumAsKeyToBlockHeader((uint8_t *)&to_be_written_block_num, sizeof(to_be_written_block_num));
  LOG_INFO(GL,
           "key size " << to_be_written_block_header_key.size() << " key hash "
                       << std::hash<std::string>{}(to_be_written_block_header_key) << " key "
                       << concordUtils::bufferToHex(to_be_written_block_header_key.c_str(),
                                                    to_be_written_block_header_key.size()));

  if (cat_itr != updates.categoryUpdates().kv.cend()) {
    const auto &kvs = std::get<kvbc::categorization::VersionedInput>(cat_itr->second).kv;
    auto key_itr = kvs.find(to_be_written_block_header_key);
    if (key_itr != kvs.cend()) {
      // Eth block
      header = BlockHeader::deserialize(key_itr->second.data);
      found_existing_header = true;
    }
  }

  if (not found_existing_header) {
    // Reconfig block; !Eth block;
    header.data.number = to_be_written_block_num;
  }

  writeParentHash(to_be_written_block_num, header.data.parent_hash);

  // write block number to block header mapping into updates
  auto serialized_header = header.serialize();
  updates.addCategoryIfNotExisting<kvbc::categorization::VersionedInput>(
      concord::kvbc::categorization::kExecutionPrivateCategory);
  updates.appendKeyValue<kvbc::categorization::VersionedUpdates>(
      concord::kvbc::categorization::kExecutionPrivateCategory,
      std::move(to_be_written_block_header_key),
      kvbc::categorization::VersionedUpdates::Value{serialized_header, true});

  // write block num to block hash mapping in updates
  auto header_hash = header.hash();
  kvbc::categorization::VersionedUpdates::Value val{};
  val.data.append(std::begin(header_hash), std::end(header_hash));
  val.stale_on_update = true;
  auto block_num_as_key_to_block_hash =
      BlockHeader::blockNumAsKeyToBlockHash((uint8_t *)&to_be_written_block_num, sizeof(to_be_written_block_num));
  updates.appendKeyValue<kvbc::categorization::VersionedUpdates>(
      concord::kvbc::categorization::kExecutionPrivateCategory,
      std::move(block_num_as_key_to_block_hash),
      std::move(val));

  // write block header hash to block number mapping in updates
  auto block_hash_as_key_to_block_number =
      BlockHeader::blockHashAsKeyToBlockNum((uint8_t *)&header_hash, sizeof(concord::kvbc::uint256be_t));
  updates.appendKeyValue<kvbc::categorization::VersionedUpdates>(
      concord::kvbc::categorization::kExecutionPrivateCategory,
      std::move(block_hash_as_key_to_block_number),
      kvbc::categorization::VersionedUpdates::Value{concordUtils::toBigEndianStringBuffer(to_be_written_block_num),
                                                    true});

  LOG_INFO(GL,
           "Block number " << to_be_written_block_num << " header size " << serialized_header.size()
                           << " and its header hash "
                           << concordUtils::bufferToHex(header_hash.data(), header_hash.size()));
}

BlockId BlocksAdderAdapter::add(concord::kvbc::categorization::Updates &&updates) {
  if (bftEngine::ReplicaConfig::instance().extraHeader == true) {
    addHeader(updates);
  }
  return kvbc_->add(std::move(updates));
}

}  // namespace concord::kvbc::adapter::v4blockchain