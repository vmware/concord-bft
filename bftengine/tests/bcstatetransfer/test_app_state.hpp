// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef BFTENGINE_TESTS_BCSTATETRANSFER_TEST_APP_STATE_HPP_
#define BFTENGINE_TESTS_BCSTATETRANSFER_TEST_APP_STATE_HPP_

#include <cassert>
#include <cstring>
#include <unordered_map>
#include "SimpleBCStateTransfer.hpp"

// This should be the same as test config
const uint32_t kMaxBlockSize = 1024;

namespace bftEngine {

namespace SimpleBlockchainStateTransfer {

struct Block {
  char block[kMaxBlockSize];
  uint32_t actualSize;
  StateTransferDigest digest;
};

class TestAppState : public IAppState {

  public:
    bool hasBlock(uint64_t blockId) override {
      auto it = blocks_.find(blockId);
      return it != blocks_.end();
    }

    bool getBlock(uint64_t blockId, char* outBlock, uint32_t* outBlockSize) override {
      auto it = blocks_.find(blockId);
      if (it == blocks_.end()) return false;
      std::memcpy(outBlock, it->second.block, it->second.actualSize);
      *outBlockSize = it->second.actualSize;
      return true;
    };

    bool getPrevDigestFromBlock(
        uint64_t blockId, StateTransferDigest* outPrevBlockDigest) override {
      assert(blockId > 0);
      auto it = blocks_.find(blockId-1);
      if (it == blocks_.end()) return false;
      std::memcpy(outPrevBlockDigest, &it->second.digest, BLOCK_DIGEST_SIZE);
      return true;
    };

    bool putBlock(uint64_t blockId, char* block, uint32_t blockSize) override {
      assert(blockId < last_block_);
      Block bl;
      computeBlockDigest(blockId, block, blockSize, &bl.digest);
      memcpy(&bl.block, block, blockSize);
      bl.actualSize = blockSize;
      last_block_ = blockId;
      return true;
    }

    // TODO(AJS): How does this differ from getLastBlockNum?
    uint64_t getLastReachableBlockNum() override {
      return last_block_;
    };

    uint64_t getLastBlockNum() override {
      return last_block_;
    };

  private:
    uint64_t last_block_;
    std::unordered_map<uint64_t, Block> blocks_;
};

} // namespace SimpleBlockChainStateTransfer

} //namespace bftEngine


#endif  // BFTENGINE_TESTS_BCSTATETRANSFER_TEST_APP_STATE_HPP_
