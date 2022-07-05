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
#include "categorization/base_types.h"
#include "categorization/db_categories.h"
#include "kvbc_adapter/v4blockchain/blocks_utils.hpp"

namespace concord::kvbc::tools {
using namespace std::placeholders;
using namespace concord::storage::rocksdb;

void DBRestore::initRocksDB(const fs::path& rocksdb_path, uint32_t blockchain_version) {
  LOG_DEBUG(logger_, rocksdb_path);
  auto wodb = NativeClient::newClient(rocksdb_path, false, NativeClient::DefaultOptions{});
  const auto linkStChain = true;
  auto kvbc_categories = std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>{
      {concord::kvbc::categorization::kExecutionProvableCategory,
       concord::kvbc::categorization::CATEGORY_TYPE::block_merkle},
      {concord::kvbc::categorization::kExecutionPrivateCategory,
       concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv},
      {concord::kvbc::categorization::kExecutionEventsCategory,
       concord::kvbc::categorization::CATEGORY_TYPE::immutable},
      {concord::kvbc::categorization::kExecutionEventGroupDataCategory,
       concord::kvbc::categorization::CATEGORY_TYPE::immutable},
      {concord::kvbc::categorization::kExecutionEventGroupTagCategory,
       concord::kvbc::categorization::CATEGORY_TYPE::immutable},
      {concord::kvbc::categorization::kExecutionEventGroupLatestCategory,
       concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv},
      {concord::kvbc::categorization::kConcordInternalCategoryId,
       concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv},
      {concord::kvbc::categorization::kConcordReconfigurationCategoryId,
       concord::kvbc::categorization::CATEGORY_TYPE::versioned_kv},
      {concord::kvbc::categorization::kRequestsRecord, concord::kvbc::categorization::CATEGORY_TYPE::immutable}};
  bftEngine::ReplicaConfig::instance().kvBlockchainVersion = blockchain_version;
  kv_blockchain_ = std::make_unique<decltype(kv_blockchain_)::element_type>(wodb, linkStChain, kvbc_categories);
}

/**
 * Optimistic approach for a single pass.
 * Retrieve blocks from object store in a natural order and add them to rocksdb while comparing their digests.
 * At the end compare the last block's digest with a one in latest checkpoint descriptor.
 */
void DBRestore::restore() {
  const auto [last_os_block_id, last_os_block_digest] = checker_->getLatestsCheckpointDescriptor();
  const auto last_reachable_rocks_blockid = kv_blockchain_->getLastBlockId();
  Digest last_reachable_rocksdb_block_digest;
  if (last_reachable_rocks_blockid == 0) {
    LOG_WARN(logger_, "rocksdb is empty, will restore the whole blockchain");
  } else {
    last_reachable_rocksdb_block_digest = *(kv_blockchain_->getParentDigest(last_reachable_rocks_blockid));
  }
  LOG_INFO(logger_,
           "Last reachable block in rocksdb: " << last_reachable_rocks_blockid
                                               << ", digest: " << last_reachable_rocksdb_block_digest.toString());

  auto expected_parent_digest{last_reachable_rocksdb_block_digest};
  for (auto block_id = last_reachable_rocks_blockid + 1; block_id <= last_os_block_id; ++block_id) {
    auto&& [digest, raw_block] = checker_->getBlock(block_id);
    Digest parent_block_digest;
    static_assert(sizeof(Digest) == DIGEST_SIZE);
    std::visit(
        [&parent_block_digest, &expected_parent_digest, this](auto&& l_raw_block) {
          using T = std::decay_t<decltype(l_raw_block)>;
          if constexpr (std::is_same_v<T, concord::kvbc::RawBlock>) {
            parent_block_digest =
                concord::kvbc::adapter::v4blockchain::utils::V4BlockUtils::getparentDigest(l_raw_block);
            auto parsedBlock = concord::kvbc::v4blockchain::detail::Block(l_raw_block.string_view());
            if (expected_parent_digest == parent_block_digest) {
              kv_blockchain_->add(parsedBlock.getUpdates().categoryUpdates());
            }
          } else if constexpr (std::is_same_v<T, concord::kvbc::categorization::RawBlock>) {
            ConcordAssert(l_raw_block.data.parent_digest.size() == DIGEST_SIZE);
            memcpy(const_cast<char*>(parent_block_digest.get()), l_raw_block.data.parent_digest.data(), DIGEST_SIZE);
            if (expected_parent_digest == parent_block_digest) {
              kv_blockchain_->add(std::move(l_raw_block.data.updates));
            }
          }
        },
        std::move(raw_block));
    LOG_INFO(logger_,
             "block: " << block_id << " digest: " << digest.toString()
                       << ", parent digest: " << parent_block_digest.toString());
    if (expected_parent_digest != parent_block_digest) {
      throw std::runtime_error("block " + std::to_string(block_id) +
                               std::string(" parent digest mismatch. Expected: ") + expected_parent_digest.toString() +
                               std::string(", actual: ") + parent_block_digest.toString());
    }
    expected_parent_digest = digest;
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
