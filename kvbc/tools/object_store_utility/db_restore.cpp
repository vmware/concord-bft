// Concord
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "db_restore.hpp"
#include "string.hpp"
#include "merkle_tree_storage_factory.h"
#include "categorization/base_types.h"
#include "categorization/db_categories.h"

namespace concord::kvbc::tools {
using namespace std::placeholders;

void DBRestore::initRocksDB(const fs::path& rocksdb_path) {
  LOG_DEBUG(logger_, rocksdb_path);
  rocksdb_dataset_ = std::make_unique<v2MerkleTree::RocksDBStorageFactory>(rocksdb_path)->newDatabaseSet();
  const auto linkStChain = true;
  // clang-format off
  auto kvbc_categories = std::map<std::string,             categorization::CATEGORY_TYPE>{
      {categorization::kExecutionProvableCategory,         categorization::CATEGORY_TYPE::block_merkle},
      {categorization::kExecutionPrivateCategory,          categorization::CATEGORY_TYPE::versioned_kv},
      {categorization::kExecutionEventsCategory,           categorization::CATEGORY_TYPE::immutable},
      {categorization::kRequestsRecord,                    categorization::CATEGORY_TYPE::immutable},
      {categorization::kExecutionEventGroupDataCategory,   categorization::CATEGORY_TYPE::immutable},
      {categorization::kExecutionEventGroupTagCategory,    categorization::CATEGORY_TYPE::immutable},
      {categorization::kExecutionEventGroupLatestCategory, categorization::CATEGORY_TYPE::versioned_kv},
      {categorization::kConcordInternalCategoryId,         categorization::CATEGORY_TYPE::versioned_kv},
      {categorization::kConcordReconfigurationCategoryId,  categorization::CATEGORY_TYPE::versioned_kv}};
  // clang-format on
  kv_blockchain_ = std::make_unique<categorization::KeyValueBlockchain>(
      storage::rocksdb::NativeClient::fromIDBClient(rocksdb_dataset_.dataDBClient), linkStChain, kvbc_categories);
}

/**
 * Optimistic approach for a single pass.
 * Retrieve blocks from object store in a natural order and add them to rocksdb while comparing their digests.
 * At the end compare the last block's digest with a one in latest checkpoint descriptor.
 */
void DBRestore::restore() {
  const auto [last_os_block_id, last_os_block_digest] = checker_->getLatestsCheckpointDescriptor();
  const auto last_reachable_rocks_blockid = kv_blockchain_->getLastReachableBlockId();
  Digest last_reachable_rocksdb_block_digest;
  if (last_reachable_rocks_blockid == 0) {
    LOG_WARN(logger_, "rocksdb is empty, will restore the whole blockchain");
  } else {
    auto buffer =
        categorization::RawBlock::serialize(kv_blockchain_->getRawBlock(last_reachable_rocks_blockid).value());
    std::string_view last_reachable_rocks_block(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    last_reachable_rocksdb_block_digest =
        checker_->computeBlockDigest(last_reachable_rocks_blockid, last_reachable_rocks_block);
  }
  LOG_INFO(logger_,
           "Last reachable block in rocksdb: " << last_reachable_rocks_blockid
                                               << ", digest: " << last_reachable_rocksdb_block_digest.toString());

  Digest expected_parent_digest = last_reachable_rocksdb_block_digest;
  for (auto block_id = last_reachable_rocks_blockid + 1; block_id <= last_os_block_id; ++block_id) {
    auto&& [digest, raw_block] = checker_->getBlock(block_id);
    Digest parent_block_digest;
    ConcordAssert(raw_block.data.parent_digest.size() == DIGEST_SIZE);
    static_assert(sizeof(Digest) == DIGEST_SIZE);
    memcpy(const_cast<char*>(parent_block_digest.get()), raw_block.data.parent_digest.data(), DIGEST_SIZE);
    LOG_INFO(logger_,
             "block: " << block_id << " digest: " << digest.toString()
                       << ", parent digest: " << parent_block_digest.toString());
    if (expected_parent_digest != parent_block_digest)
      throw std::runtime_error("block " + std::to_string(block_id) +
                               std::string(" parent digest mismatch. Expected: ") + expected_parent_digest.toString() +
                               std::string(", actual: ") + parent_block_digest.toString());
    expected_parent_digest = digest;
    kv_blockchain_->addBlock(std::move(raw_block.data.updates));
    if (block_id == last_os_block_id) {
      if (digest != last_os_block_digest) {
        throw std::runtime_error("latest checkpoint block " + std::to_string(last_os_block_id) +
                                 std::string(" digest mismatch, expected: ") + last_os_block_digest.toString() +
                                 std::string(" actual: ") + digest.toString());
      } else {
        LOG_INFO(logger_,
                 "last block " << last_os_block_id
                               << ", digest matches latest checkpoint descriptor: " << last_os_block_digest.toString());
      }
    }
  }
  LOG_INFO(logger_,
           "Successfully restored " << (last_os_block_id - last_reachable_rocks_blockid) << " blocks: ("
                                    << last_reachable_rocks_blockid << ", " << last_os_block_id << "]");
}

}  // namespace concord::kvbc::tools
