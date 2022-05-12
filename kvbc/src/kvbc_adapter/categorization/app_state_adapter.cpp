// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "kvbc_adapter/categorization/app_state_adapter.hpp"

namespace concord::kvbc::adapter::categorization {

AppStateAdapter::AppStateAdapter(std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain> &kvbc)
    : kvbc_{kvbc.get()}, logger_(logging::getLogger("skvbc.replica.appstateadapter")) {
  ConcordAssertNE(kvbc_, nullptr);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IAppState implementation
bool AppStateAdapter::getBlock(uint64_t blockId,
                               char *outBlock,
                               uint32_t outBlockMaxSize,
                               uint32_t *outBlockActualSize) const {
  const auto rawBlock = kvbc_->getRawBlock(blockId);
  if (!rawBlock) {
    throw NotFoundException{"Raw block not found: " + std::to_string(blockId)};
  }
  const auto &ser = concord::kvbc::categorization::RawBlock::serialize(*rawBlock);
  if (ser.size() > outBlockMaxSize) {
    LOG_ERROR(logger_, KVLOG(ser.size(), outBlockMaxSize));
    throw std::runtime_error("not enough space to copy block!");
  }
  *outBlockActualSize = ser.size();
  std::memcpy(outBlock, ser.data(), *outBlockActualSize);
  return true;
}
bool AppStateAdapter::getPrevDigestFromBlock(uint64_t blockId,
                                             bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const {
  ConcordAssert(blockId > 0);
  const auto parent_digest = kvbc_->parentDigest(blockId);

  if (!parent_digest.has_value()) {
    LOG_WARN(logger_, "parent digest not found," << KVLOG(blockId));
    return false;
  }
  static_assert(parent_digest->size() == DIGEST_SIZE);
  static_assert(sizeof(bftEngine::bcst::StateTransferDigest) == DIGEST_SIZE);
  std::memcpy(outPrevBlockDigest, parent_digest->data(), DIGEST_SIZE);
  return true;
}
void AppStateAdapter::getPrevDigestFromBlock(const char *blockData,
                                             const uint32_t blockSize,
                                             bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const {
  ConcordAssertGT(blockSize, 0);
  auto view = std::string_view{blockData, blockSize};
  const auto rawBlock = concord::kvbc::categorization::RawBlock::deserialize(view);

  static_assert(rawBlock.data.parent_digest.size() == DIGEST_SIZE);
  static_assert(sizeof(bftEngine::bcst::StateTransferDigest) == DIGEST_SIZE);
  std::memcpy(outPrevBlockDigest, rawBlock.data.parent_digest.data(), DIGEST_SIZE);
}
bool AppStateAdapter::putBlock(const uint64_t blockId,
                               const char *blockData,
                               const uint32_t blockSize,
                               bool lastBlock) {
  auto view = std::string_view{blockData, blockSize};
  const auto rawBlock = concord::kvbc::categorization::RawBlock::deserialize(view);
  if (kvbc_->hasBlock(blockId)) {
    const auto existingRawBlock = kvbc_->getRawBlock(blockId);
    if (rawBlock != existingRawBlock) {
      LOG_ERROR(logger_,
                "found existing (and different) block ID[" << blockId << "] when receiving from state transfer");

      // TODO consider assert?
      kvbc_->deleteBlock(blockId);
      throw std::runtime_error(
          __PRETTY_FUNCTION__ +
          std::string("found existing (and different) block when receiving state transfer, block ID: ") +
          std::to_string(blockId));
    }
  } else {
    kvbc_->addRawBlock(rawBlock, blockId, lastBlock);
  }
  return true;
}
// This method is used by state-transfer in order to find the latest block id in either the state-transfer chain or
// the main blockchain
uint64_t AppStateAdapter::getLastBlockNum() const {
  const auto last = kvbc_->getLastStatetransferBlockId();
  if (last) {
    return *last;
  }
  return kvbc_->getLastReachableBlockId();
}
size_t AppStateAdapter::postProcessUntilBlockId(uint64_t max_block_id) {
  const BlockId last_reachable_block = kvbc_->getLastReachableBlockId();
  BlockId last_st_block_id = 0;
  if (auto last_st_block_id_opt = kvbc_->getLastStatetransferBlockId()) {
    last_st_block_id = last_st_block_id_opt.value();
  }
  if ((max_block_id == last_reachable_block) && (last_st_block_id == 0)) {
    LOG_INFO(CAT_BLOCK_LOG,
             "Consensus blockchain is fully linked, no proc-processing is required!"
                 << KVLOG(max_block_id, last_reachable_block));
    return 0;
  }
  if ((max_block_id < last_reachable_block) || (max_block_id > last_st_block_id)) {
    auto msg = std::stringstream{};
    msg << "Cannot post-process:" << KVLOG(max_block_id, last_reachable_block, last_st_block_id) << std::endl;
    throw std::invalid_argument{msg.str()};
  }

  try {
    return kvbc_->linkUntilBlockId(last_reachable_block + 1, max_block_id);
  } catch (const std::exception &e) {
    LOG_FATAL(
        CAT_BLOCK_LOG,
        "Aborting due to failure to link," << KVLOG(last_reachable_block, max_block_id) << ", reason: " << e.what());
    std::terminate();
  } catch (...) {
    LOG_FATAL(CAT_BLOCK_LOG, "Aborting due to failure to link," << KVLOG(last_reachable_block, max_block_id));
    std::terminate();
  }
}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}  // End of namespace concord::kvbc::adapter::categorization
