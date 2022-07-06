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

#pragma once

#include <string>
#include <vector>
#include <memory>

#include "assertUtils.hpp"
#include "Logger.hpp"
#include "blockchain_misc.hpp"
#include "kv_types.hpp"
#include "categorization/base_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"
#include "categorization/kv_blockchain.h"
#include "bcstatetransfer/SimpleBCStateTransfer.hpp"

namespace concord::kvbc::adapter::categorization {

class AppStateAdapter : public bftEngine::bcst::IAppState {
 public:
  virtual ~AppStateAdapter() { kvbc_ = nullptr; }
  explicit AppStateAdapter(std::shared_ptr<concord::kvbc::categorization::KeyValueBlockchain> &kvbc);

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IAppState implementation
  bool hasBlock(BlockId blockId) const override final { return kvbc_->hasBlock(blockId); }
  bool getBlock(uint64_t blockId,
                char *outBlock,
                uint32_t outBlockMaxSize,
                uint32_t *outBlockActualSize) const override final;
  std::future<bool> getBlockAsync(uint64_t blockId,
                                  char *outBlock,
                                  uint32_t outBlockMaxSize,
                                  uint32_t *outBlockActualSize) override final {
    // This function should never be called
    ConcordAssert(false);
    return std::async([]() { return false; });
  }
  bool getPrevDigestFromBlock(uint64_t blockId, bftEngine::bcst::StateTransferDigest *) const override final;
  void getPrevDigestFromBlock(const char *blockData,
                              const uint32_t blockSize,
                              bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const override final;
  bool putBlock(const uint64_t blockId,
                const char *blockData,
                const uint32_t blockSize,
                bool lastBlock = true) override final;
  std::future<bool> putBlockAsync(uint64_t blockId,
                                  const char *block,
                                  const uint32_t blockSize,
                                  bool lastblock) override final {
    // This functions should not be called
    ConcordAssert(false);
    return std::async([]() { return false; });
  }
  uint64_t getLastReachableBlockNum() const override final { return kvbc_->getLastReachableBlockId(); }
  uint64_t getGenesisBlockNum() const override final { return kvbc_->getGenesisBlockId(); }
  // This method is used by state-transfer in order to find the latest block id in either the state-transfer chain or
  // the main blockchain
  uint64_t getLastBlockNum() const override final;
  size_t postProcessUntilBlockId(uint64_t max_block_id) override final;
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

 private:
  concord::kvbc::categorization::KeyValueBlockchain *kvbc_{nullptr};
  logging::Logger logger_;
};
}  // namespace concord::kvbc::adapter::categorization
