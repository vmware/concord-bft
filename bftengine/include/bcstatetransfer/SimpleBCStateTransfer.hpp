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

#ifndef BFTENGINE_SRC_BCSTATETRANSFER_SIMPLEBCSTATETRANSFER_HPP_
#define BFTENGINE_SRC_BCSTATETRANSFER_SIMPLEBCSTATETRANSFER_HPP_

#include <set>

#include "IStateTransfer.hpp"

namespace bftEngine {

// This file contains interfaces of a state transfer module which is designed
// to handle "Blockchain state". The module assumes that the main state is
// composed of a sequence of blocks that are never deleted or updated. The
// state is updated by appending blocks at the end of the sequence. The module
// can also handle a limited amount of arbitrary mutable state (which is
// represented as a small set of fixed size pages).
namespace SimpleBlockchainStateTransfer {

// Each block is required to store the digest of the previous block (this digest
// is used by the state transfer to safely transfer blocks among the replicas).
// The application/storage layer is responsible to store the digests in the
// blocks.
// Blocks are numbered. The first block should be block number 1.
constexpr static uint32_t BLOCK_DIGEST_SIZE = 32;

// represnts a digest
#pragma pack(push, 1)
struct StateTransferDigest {
  char content[BLOCK_DIGEST_SIZE];
};
#pragma pack(pop)

// This method should be used to compute block digests
void computeBlockDigest(const uint64_t blockId,
                        const char* block,
                        const uint32_t blockSize,
                        StateTransferDigest* outDigest);

// This interface should be implemented by the application/storage layer.
// It is used by the state transfer module.
class IAppState {
 public:
  // returns true IFF block blockId exists
  // (i.e. the block is stored in the application/storage layer).
  virtual bool hasBlock(uint64_t blockId) = 0;

  // If block blockId exists, then its content is returned via the arguments
  // outBlock and outBlockSize. Returns true IFF block blockId exists.
  virtual bool getBlock(uint64_t blockId,
                        char* outBlock,
                        uint32_t* outBlockSize) = 0;

  // If block blockId exists, then the digest of block blockId-1 is returned via
  // the argument outPrevBlockDigest. Returns true IFF block blockId exists.
  virtual bool getPrevDigestFromBlock(
      uint64_t blockId, StateTransferDigest* outPrevBlockDigest) = 0;

  // adds block
  // blockId   - the block number
  // block     - pointer to a buffer that contains the new block
  // blockSize - the size of the new block
  virtual bool putBlock(uint64_t blockId, char* block, uint32_t blockSize) = 0;

  // returns the maximal block number n such that all blocks 1 <= i <= n exist.
  // if block 1 does not exist, returns 0.
  virtual uint64_t getLastReachableBlockNum() = 0;

  // returns the maximum block number that is currently stored in
  // the application/storage layer.
  virtual uint64_t getLastBlockNum() = 0;

  // When the state is updated by the application, getLastReachableBlockNum()
  // and getLastBlockNum() should always return the same block number.
  // When that state transfer module is updating the state, then these methods
  // may return different block numbers.
};

struct Config {
  uint16_t myReplicaId;
  uint16_t fVal = 0;
  uint16_t cVal = 0;

  bool pedanticChecks = false;
  uint32_t maxBlockSize = 10 * 1024 * 1024;  // 10MB

  // TODO(GG): change to: 2KB // 2 * 1024 ;  // 2KB
  // NB: use small values for debugging/testing
  uint32_t maxChunkSize = 2 * 1024;  // 128;

  uint16_t maxNumberOfChunksInBatch = 24;
  uint32_t maxPendingDataFromSourceReplica = 32 * 1024 * 1024;

  uint32_t maxNumOfReservedPages = 2048;

  uint32_t refreshTimerMilli = 300;                               // 300ms
  uint32_t checkpointSummariesRetransmissionTimeoutMilli = 2500;  // 2500ms
  uint32_t maxAcceptableMsgDelayMilli = 1 * 60 * 1000;            // 1 minutes
  uint32_t sourceReplicaReplacementTimeoutMilli = 5000;           // 5000ms
  uint32_t fetchRetransmissionTimeoutMilli = 250;                 // 250ms
};

// creates an instance of the state transfer module.
IStateTransfer* create(const Config& config,
                       IAppState* const stateApi,
                       const bool persistentDataStore);

}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine

#endif  // BFTENGINE_SRC_BCSTATETRANSFER_SIMPLEBCSTATETRANSFER_HPP_
