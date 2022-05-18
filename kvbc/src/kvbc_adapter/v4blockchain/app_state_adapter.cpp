// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "db_adapter_interface.h"
#include "kvbc_adapter/v4blockchain/app_state_adapter.hpp"
#include "v4blockchain/detail/blocks.h"
#include "v4blockchain/detail/detail.h"

using namespace concord::kvbc;

namespace concord::kvbc::adapter::v4blockchain {

bool AppStateAdapter::getBlock(uint64_t blockId,
                               char *outBlock,
                               uint32_t outBlockMaxSize,
                               uint32_t *outBlockActualSize) const {
  auto blockData = kvbc_->getBlockData(blockId);
  if (!blockData) {
    throw kvbc::NotFoundException{"block not found: " + std::to_string(blockId)};
  }
  if (blockData->size() > outBlockMaxSize) {
    LOG_ERROR(V4_BLOCK_LOG, KVLOG(blockData->size(), outBlockMaxSize));
    throw std::runtime_error("not enough space to copy block!");
  }
  *outBlockActualSize = blockData->size();
  std::memcpy(outBlock, blockData->c_str(), *outBlockActualSize);
  return true;
}

bool AppStateAdapter::getPrevDigestFromBlock(uint64_t blockId,
                                             bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const {
  ConcordAssert(blockId > 0);
  const auto parent_digest = kvbc_->parentDigest(blockId);
  static_assert(parent_digest.size() == DIGEST_SIZE);
  static_assert(sizeof(bftEngine::bcst::StateTransferDigest) == DIGEST_SIZE);
  std::memcpy(outPrevBlockDigest, parent_digest.data(), DIGEST_SIZE);
  return true;
}

void AppStateAdapter::getPrevDigestFromBlock(const char *blockData,
                                             const uint32_t blockSize,
                                             bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const {
  ConcordAssertGE(blockSize, ::v4blockchain::detail::Block::HEADER_SIZE);
  const auto &digest =
      *reinterpret_cast<const concord::util::digest::BlockDigest *>(blockData + sizeof(concord::kvbc::version_type));

  std::memcpy(outPrevBlockDigest, digest.data(), DIGEST_SIZE);
}

bool AppStateAdapter::putBlock(const uint64_t blockId,
                               const char *blockData,
                               const uint32_t blockSize,
                               bool lastBlock) {
  const auto lastReachable = kvbc_->getLastReachableBlockId();
  if (blockId <= lastReachable) {
    auto bd = kvbc_->getBlockData(blockId);
    auto in_bd = std::string(blockData, blockSize);
    if (bd.has_value() && *bd == in_bd) {
      LOG_INFO(CAT_BLOCK_LOG, "blockchain already contains block " << blockId);
      return true;
    }
    const auto msg = "blockchain already contains a different block, ID " + std::to_string(blockId);
    throw std::invalid_argument{msg};
  }

  kvbc_->addBlockToSTChain(blockId, blockData, blockSize, lastBlock);
  return true;
}

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
    return kvbc_->linkUntilBlockId(max_block_id);
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

}  // namespace concord::kvbc::adapter::v4blockchain
