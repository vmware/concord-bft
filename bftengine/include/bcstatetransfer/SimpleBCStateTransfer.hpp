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

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <future>

#include "bftengine/IStateTransfer.hpp"
#include "Metrics.hpp"
#include "kvstream.h"
#include "Digest.hpp"

using concord::util::digest::Digest;
using concord::util::digest::BlockDigest;

namespace concord {
namespace storage {
class IDBClient;
class ISTKeyManipulator;
}  // namespace storage
}  // namespace concord
namespace bftEngine {

// This file contains interfaces of a state transfer module which is designed
// to handle "Blockchain state". The module assumes that the main state is
// composed of a sequence of blocks that are never deleted or updated. The
// state is updated by appending blocks at the end of the sequence. The module
// can also handle a limited amount of arbitrary mutable state (which is
// represented as a small set of fixed size pages).
namespace bcst {

// Each block is required to store the digest of the previous block (this digest
// is used by the state transfer to safely transfer blocks among the replicas).
// The application/storage layer is responsible to store the digests in the
// blocks.
// Blocks are numbered. The first block should be block number 1.

// represnts a digest
#pragma pack(push, 1)
struct StateTransferDigest {
  char content[DIGEST_SIZE];
};
#pragma pack(pop)

// This method should be used to compute block digests
void computeBlockDigest(const uint64_t blockId,
                        const char *block,
                        const uint32_t blockSize,
                        StateTransferDigest *outDigest);

BlockDigest computeBlockDigest(const uint64_t blockId, const char *block, const uint32_t blockSize);

// This interface should be implemented by the application/storage layer.
// It is used by the state transfer module.
class IAppState {
 public:
  virtual ~IAppState(){};

  // returns true IFF block blockId exists
  // (i.e. the block is stored in the application/storage layer).
  virtual bool hasBlock(uint64_t blockId) const = 0;

  // If block blockId exists, then its content is returned via the arguments
  // outBlock and outBlockActualSize. Returns true IFF block blockId exists.
  // If outBlockMaxSize is too small, an exception is thrown
  virtual bool getBlock(uint64_t blockId,
                        char *outBlock,
                        uint32_t outBlockMaxSize,
                        uint32_t *outBlockActualSize) const = 0;

  // Get a block (asynchronously)
  // An asynchronous version for the above getBlock.
  // For a given blockId, a job is invoked asynchronously, to get the block from storage and fill outBlock and
  // outBlockActualSize. After job is created, this call returns immediately with a future<bool>, while job is executed
  // by a separate worker thread. Before accesing buffer and size, user must call the returned future.get() to make sure
  // that job has been done. User should 1st check the future value: if true - block exist and outBlock,
  // outBlockActualSize are valid if false - block does not exist, all output should be ignored. If outBlockMaxSize is
  // too small, an exception is thrown.
  virtual std::future<bool> getBlockAsync(uint64_t blockId,
                                          char *outBlock,
                                          uint32_t outBlockMaxSize,
                                          uint32_t *outBlockActualSize) = 0;

  // If block blockId exists, then the digest of block blockId-1 is returned via
  // the argument outPrevBlockDigest. Returns true IFF block blockId exists.
  virtual bool getPrevDigestFromBlock(uint64_t blockId, StateTransferDigest *outPrevBlockDigest) const = 0;

  // Extracts a digest out of in-memory block (raw block).
  virtual void getPrevDigestFromBlock(const char *blockData,
                                      const uint32_t blockSize,
                                      StateTransferDigest *outPrevBlockDigest) const = 0;

  // Add a block
  // blockId   - the block number
  // block     - pointer to a buffer that contains the new block
  // blockSize - the size of the new block
  // lastBlock - when true, for backup replica - try to remove blocks from State Transfer chain and add them to
  // the blockchain
  // Returns true if operation succeeded.
  virtual bool putBlock(const uint64_t blockId, const char *block, const uint32_t blockSize, bool lastBlock = true) = 0;

  // Add a block (asynchronously)
  // An asynchronous version for the above putBlock.
  // For a given blockId, a job is invoked asynchronously, to put the block into storage.
  // After job is created, this call returns immediately with a future<bool>, while job is executed by a
  // separate worker thread. Before accesing buffer and size, user must call the returned future.get() to make sure that
  // job has been done.
  // All exceptions in putBlock are caught within this call implementation.
  // Returns true if operation succeeded.
  virtual std::future<bool> putBlockAsync(uint64_t blockId,
                                          const char *block,
                                          const uint32_t blockSize,
                                          bool lastBlock) = 0;

  // returns the maximal block number n such that all blocks 1 <= i <= n exist.
  // if block 1 does not exist, returns 0.
  virtual uint64_t getLastReachableBlockNum() const = 0;

  // returns the current genesis block in the system
  virtual uint64_t getGenesisBlockNum() const = 0;
  // returns the maximum block number that is currently stored in
  // the application/storage layer.
  virtual uint64_t getLastBlockNum() const = 0;

  // Perform post-processing operations on all blocks until (and include) maxBlockId
  // If those operations have already been done, function should do nothing and return
  // return number of blocks that were actually post-processed
  virtual size_t postProcessUntilBlockId(uint64_t maxBlockId) = 0;

  // When the state is updated by the application, getLastReachableBlockNum()
  // and getLastBlockNum() should always return the same block number.
  // When that state transfer module is updating the state, then these methods
  // may return different block numbers.
};

struct Config {
  uint16_t myReplicaId;
  uint16_t fVal = 0;
  uint16_t cVal = 0;
  uint16_t numReplicas = 0;  // number of consensus replicas
  uint16_t numRoReplicas = 0;
  bool pedanticChecks = false;
  bool isReadOnly = false;

  // sizes
  uint32_t maxChunkSize = 0;
  uint16_t maxNumberOfChunksInBatch = 0;
  uint32_t maxBlockSize = 0;                     // bytes
  uint32_t maxPendingDataFromSourceReplica = 0;  // Maximal internal buffer size for all ST data, bytes
  uint32_t maxNumOfReservedPages = 0;
  uint32_t sizeOfReservedPage = 0;  // bytes
  uint32_t gettingMissingBlocksSummaryWindowSize = 0;
  uint16_t minPrePrepareMsgsForPrimaryAwareness = 0;
  uint32_t fetchRangeSize = 0;
  uint32_t RVT_K = 0;

  // timeouts
  uint32_t refreshTimerMs = 0;
  uint32_t checkpointSummariesRetransmissionTimeoutMs = 0;
  uint32_t maxAcceptableMsgDelayMs = 0;
  uint32_t sourceReplicaReplacementTimeoutMs = 0;
  uint32_t fetchRetransmissionTimeoutMs = 0;
  uint32_t maxFetchRetransmissions = 0;
  uint32_t metricsDumpIntervalSec = 0;
  uint32_t maxTimeSinceLastExecutionInMainWindowMs = 0;
  uint32_t sourceSessionExpiryDurationMs = 0;
  uint32_t sourcePerformanceSnapshotFrequencySec = 0;

  // misc
  bool runInSeparateThread = true;
  bool enableReservedPages = true;
  bool enableSourceBlocksPreFetch = true;
  bool enableSourceSelectorPrimaryAwareness = true;
  bool enableStoreRvbDataDuringCheckpointing = true;
};

inline std::ostream &operator<<(std::ostream &os, const Config &c) {
  os << KVLOG(c.myReplicaId,
              c.fVal,
              c.cVal,
              c.numReplicas,
              c.numRoReplicas,
              c.pedanticChecks,
              c.isReadOnly,
              c.maxChunkSize,
              c.maxNumberOfChunksInBatch,
              c.maxBlockSize,
              c.maxPendingDataFromSourceReplica,
              c.maxNumOfReservedPages,
              c.sizeOfReservedPage,
              c.gettingMissingBlocksSummaryWindowSize,
              c.minPrePrepareMsgsForPrimaryAwareness,
              c.fetchRangeSize);
  os << ",";
  os << KVLOG(c.RVT_K,
              c.refreshTimerMs,
              c.checkpointSummariesRetransmissionTimeoutMs,
              c.maxAcceptableMsgDelayMs,
              c.sourceReplicaReplacementTimeoutMs,
              c.fetchRetransmissionTimeoutMs,
              c.maxFetchRetransmissions,
              c.metricsDumpIntervalSec,
              c.maxTimeSinceLastExecutionInMainWindowMs,
              c.sourceSessionExpiryDurationMs,
              c.sourcePerformanceSnapshotFrequencySec,
              c.runInSeparateThread,
              c.enableReservedPages,
              c.enableSourceBlocksPreFetch,
              c.enableSourceSelectorPrimaryAwareness,
              c.enableStoreRvbDataDuringCheckpointing);
  return os;
}
// creates an instance of the state transfer module.

IStateTransfer *create(const Config &config,
                       IAppState *const stateApi,
                       std::shared_ptr<::concord::storage::IDBClient> dbc,
                       std::shared_ptr<concord::storage::ISTKeyManipulator> stKeyManipulator);

IStateTransfer *create(const Config &config,
                       IAppState *const stateApi,
                       std::shared_ptr<::concord::storage::IDBClient> dbc,
                       std::shared_ptr<concord::storage::ISTKeyManipulator> stKeyManipulator,
                       std::shared_ptr<concordMetrics::Aggregator> aggregator);

}  // namespace bcst
}  // namespace bftEngine
