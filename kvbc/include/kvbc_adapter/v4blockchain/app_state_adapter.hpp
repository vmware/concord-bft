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

#include "bcstatetransfer/SimpleBCStateTransfer.hpp"
#include "v4blockchain/v4_blockchain.h"

namespace concord::kvbc::adapter::v4blockchain {

class AppStateAdapter : public bftEngine::bcst::IAppState {
 public:
  virtual ~AppStateAdapter() { kvbc_ = nullptr; }
  explicit AppStateAdapter(std::shared_ptr<concord::kvbc::v4blockchain::KeyValueBlockchain> &kvbc)
      : kvbc_{kvbc.get()} {}

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  virtual bool hasBlock(uint64_t id) const override { return kvbc_->hasBlock(id); }
  virtual bool getBlock(uint64_t blockId,
                        char *outBlock,
                        uint32_t outBlockMaxSize,
                        uint32_t *outBlockActualSize) const override;

  virtual std::future<bool> getBlockAsync(uint64_t blockId,
                                          char *outBlock,
                                          uint32_t outBlockMaxSize,
                                          uint32_t *outBlockActualSize) override {
    // This function is implemented in the replica.cpp
    ConcordAssert(false);
    return std::async([]() { return false; });
  }
  virtual bool getPrevDigestFromBlock(uint64_t blockId,
                                      bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const override;

  // Extracts a digest out of in-memory block (raw block).
  virtual void getPrevDigestFromBlock(const char *blockData,
                                      const uint32_t blockSize,
                                      bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) const override;

  virtual bool putBlock(const uint64_t blockId,
                        const char *blockData,
                        const uint32_t blockSize,
                        bool lastBlock) override;

  virtual std::future<bool> putBlockAsync(uint64_t blockId,
                                          const char *block,
                                          const uint32_t blockSize,
                                          bool lastBlock) override {
    // This function is implemented in replica.cpp
    ConcordAssert(false);
    return std::async([]() { return false; });
  }

  virtual uint64_t getLastReachableBlockNum() const override { return kvbc_->getLastReachableBlockId(); }

  virtual uint64_t getGenesisBlockNum() const override { return kvbc_->getGenesisBlockId(); }

  virtual uint64_t getLastBlockNum() const override;

  virtual size_t postProcessUntilBlockId(uint64_t max_block_id) override;

 private:
  concord::kvbc::v4blockchain::KeyValueBlockchain *kvbc_{nullptr};
};

}  // namespace concord::kvbc::adapter::v4blockchain
