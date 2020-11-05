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
#include <set>
#include <memory>

#include "bftengine/IStateTransfer.hpp"
#include "Metrics.hpp"
#include "kvstream.h"

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
inline constexpr std::uint32_t BLOCK_DIGEST_SIZE = 32;

// represnts a digest
#pragma pack(push, 1)
struct StateTransferDigest {
  char content[BLOCK_DIGEST_SIZE];
};
#pragma pack(pop)

// This method should be used to compute block digests
void computeBlockDigest(const uint64_t blockId,
                        const char *block,
                        const uint32_t blockSize,
                        StateTransferDigest *outDigest);

std::array<std::uint8_t, BLOCK_DIGEST_SIZE> computeBlockDigest(const uint64_t blockId,
                                                               const char *block,
                                                               const uint32_t blockSize);

// This interface should be implemented by the application/storage layer.
// It is used by the state transfer module.
class IAppState {
 public:
  virtual ~IAppState(){};

  // returns true IFF block blockId exists
  // (i.e. the block is stored in the application/storage layer).
  virtual bool hasBlock(uint64_t blockId) const = 0;

  // If block blockId exists, then its content is returned via the arguments
  // outBlock and outBlockSize. Returns true IFF block blockId exists.
  virtual bool getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) = 0;

  // If block blockId exists, then the digest of block blockId-1 is returned via
  // the argument outPrevBlockDigest. Returns true IFF block blockId exists.
  virtual bool getPrevDigestFromBlock(uint64_t blockId, StateTransferDigest *outPrevBlockDigest) = 0;

  // adds block
  // blockId   - the block number
  // block     - pointer to a buffer that contains the new block
  // blockSize - the size of the new block
  virtual bool putBlock(const uint64_t blockId, const char *block, const uint32_t blockSize) = 0;

  // returns the maximal block number n such that all blocks 1 <= i <= n exist.
  // if block 1 does not exist, returns 0.
  virtual uint64_t getLastReachableBlockNum() const = 0;
  // returns the maximum block number that is currently stored in
  // the application/storage layer.
  virtual uint64_t getLastBlockNum() const = 0;

  // When the state is updated by the application, getLastReachableBlockNum()
  // and getLastBlockNum() should always return the same block number.
  // When that state transfer module is updating the state, then these methods
  // may return different block numbers.
};

struct Config {
  uint16_t myReplicaId;
  uint16_t fVal = 0;
  uint16_t cVal = 0;
  uint16_t numReplicas = 0;
  bool pedanticChecks = false;
  bool isReadOnly = false;

  // sizes
  uint32_t maxChunkSize = 0;
  uint16_t maxNumberOfChunksInBatch = 0;
  uint32_t maxBlockSize = 0;                     // bytes
  uint32_t maxPendingDataFromSourceReplica = 0;  // Maximal internal buffer size for all ST data, bytes
  uint32_t maxNumOfReservedPages = 0;
  uint32_t sizeOfReservedPage = 0;  // bytes

  // timeouts
  uint32_t refreshTimerMs = 0;
  uint32_t checkpointSummariesRetransmissionTimeoutMs = 0;
  uint32_t maxAcceptableMsgDelayMs = 0;
  uint32_t sourceReplicaReplacementTimeoutMs = 0;
  uint32_t fetchRetransmissionTimeoutMs = 0;
  uint32_t metricsDumpIntervalSec = 0;

  // misc
  bool runInSeparateThread = false;
};

inline std::ostream &operator<<(std::ostream &os, const Config &c) {
  os << KVLOG(c.myReplicaId,
              c.fVal,
              c.cVal,
              c.numReplicas,
              c.pedanticChecks,
              c.isReadOnly,
              c.maxChunkSize,
              c.maxNumberOfChunksInBatch,
              c.maxBlockSize,
              c.maxPendingDataFromSourceReplica,
              c.maxNumOfReservedPages,
              c.sizeOfReservedPage,
              c.refreshTimerMs,
              c.checkpointSummariesRetransmissionTimeoutMs,
              c.maxAcceptableMsgDelayMs,
              c.sourceReplicaReplacementTimeoutMs);
  os << KVLOG(c.fetchRetransmissionTimeoutMs, c.metricsDumpIntervalSec, c.runInSeparateThread);
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
