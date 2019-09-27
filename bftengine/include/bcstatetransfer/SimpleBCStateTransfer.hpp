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
#include <memory>

#include "IStateTransfer.hpp"

namespace concord{ namespace storage{ class IDBClient; }}
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
void computeBlockDigest(const uint64_t blockId, const char *block,
                        const uint32_t blockSize, StateTransferDigest *outDigest);

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
                        char *outBlock, uint32_t *outBlockSize) = 0;

  // If block blockId exists, then the digest of block blockId-1 is returned via
  // the argument outPrevBlockDigest. Returns true IFF block blockId exists.
  virtual bool getPrevDigestFromBlock(uint64_t blockId,
                                      StateTransferDigest *outPrevBlockDigest) = 0;

  // adds block
  // blockId   - the block number
  // block     - pointer to a buffer that contains the new block
  // blockSize - the size of the new block
  virtual bool putBlock(uint64_t blockId, char *block, uint32_t blockSize) = 0;

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

#if defined USE_COMM_PLAIN_TCP || defined USE_COMM_TLS_TCP
  uint32_t maxChunkSize = 16384;
  uint16_t maxNumberOfChunksInBatch = 8;
#else
  // maxChunkSize * maxNumberOfChunksInBatch should not exceed 64KB as UDP message size is limited to 64KB.
  uint32_t maxChunkSize = 2048;
  uint16_t maxNumberOfChunksInBatch = 32;
#endif

  uint32_t maxBlockSize = 10 * 1024 * 1024; // 10MB
  uint32_t maxPendingDataFromSourceReplica = 256 * 1024 * 1024; // Maximal internal buffer size for all ST data
  uint32_t maxNumOfReservedPages = 2048;
  uint32_t sizeOfReservedPage = 4096;

  uint32_t refreshTimerMilli = 300; // ms
  uint32_t checkpointSummariesRetransmissionTimeoutMilli = 2500; // ms
  uint32_t maxAcceptableMsgDelayMilli = 60000; // 1 minute
  uint32_t sourceReplicaReplacementTimeoutMilli = 600000; // 10 minutes
  uint32_t fetchRetransmissionTimeoutMilli = 250; // ms
};

// creates an instance of the state transfer module.

IStateTransfer* create(const Config &config, IAppState *const stateApi, std::shared_ptr<::concord::storage::IDBClient> dbc);

}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine

#endif  // BFTENGINE_SRC_BCSTATETRANSFER_SIMPLEBCSTATETRANSFER_HPP_
