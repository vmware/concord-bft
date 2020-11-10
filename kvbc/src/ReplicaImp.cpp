// Copyright 2018-2020 VMware, all rights reserved
//
// KV Blockchain replica implementation.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <unistd.h>
#include "ReplicaImp.h"
#include <inttypes.h>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <utility>
#include "assertUtils.hpp"
#include "communication/CommDefs.hpp"
#include "kv_types.hpp"
#include "hex_tools.h"
#include "replica_state_sync.h"
#include "sliver.hpp"
#include "bftengine/DbMetadataStorage.hpp"

using bft::communication::ICommunication;
using bftEngine::bcst::StateTransferDigest;
using namespace concord::diagnostics;

using concord::storage::DBMetadataStorage;

namespace concord::kvbc {

/**
 * Opens the database and creates the replica thread. Replica state moves to
 * Starting.
 */
Status ReplicaImp::start() {
  LOG_INFO(logger, "ReplicaImp::Start() id = " << replicaConfig_.replicaId);

  if (m_currentRepStatus != RepStatus::Idle) {
    return Status::IllegalOperation("todo");
  }

  m_currentRepStatus = RepStatus::Starting;

  if (replicaConfig_.isReadOnly) {
    LOG_INFO(logger, "ReadOnly mode");
    m_replicaPtr =
        bftEngine::IReplica::createNewRoReplica(replicaConfig_, m_stateTransfer, m_ptrComm, m_metadataStorage);
  } else {
    createReplicaAndSyncState();
  }
  m_replicaPtr->setControlStateManager(controlStateManager_);
  m_replicaPtr->SetAggregator(aggregator_);
  m_replicaPtr->start();
  m_currentRepStatus = RepStatus::Running;

  /// TODO(IG, GG)
  /// add return value to start/stop

  return Status::OK();
}

void ReplicaImp::createReplicaAndSyncState() {
  bool isNewStorage = m_metadataStorage->isNewStorage();
  bool erasedMetaData;
  m_replicaPtr = bftEngine::IReplica::createNewReplica(
      replicaConfig_, m_cmdHandler, m_stateTransfer, m_ptrComm, m_metadataStorage, erasedMetaData);
  if (erasedMetaData) isNewStorage = true;
  LOG_INFO(logger, "createReplicaAndSyncState: isNewStorage= " << isNewStorage);
  if (!isNewStorage && !m_stateTransfer->isCollectingState()) {
    uint64_t removedBlocksNum = replicaStateSync_->execute(
        logger, *m_bcDbAdapter, getLastReachableBlockNum(), m_replicaPtr->getLastExecutedSequenceNum());
    LOG_INFO(logger,
             "createReplicaAndSyncState: removedBlocksNum = "
                 << removedBlocksNum << ", new m_lastBlock = " << getLastBlockNum()
                 << ", new m_lastReachableBlock = " << getLastReachableBlockNum());
  }
}

/**
 * Closes the database. Call `wait()` after this to wait for thread to stop.
 */
Status ReplicaImp::stop() {
  m_currentRepStatus = RepStatus::Stopping;
  m_replicaPtr->stop();
  m_currentRepStatus = RepStatus::Idle;
  return Status::OK();
}

ReplicaImp::RepStatus ReplicaImp::getReplicaStatus() const { return m_currentRepStatus; }

const ILocalKeyValueStorageReadOnly &ReplicaImp::getReadOnlyStorage() { return *this; }

Status ReplicaImp::addBlockToIdleReplica(const SetOfKeyValuePairs &updates) {
  if (getReplicaStatus() != IReplica::RepStatus::Idle) {
    return Status::IllegalOperation("");
  }

  BlockId d;
  return addBlockInternal(updates, d);
}

Status ReplicaImp::get(const Sliver &key, Sliver &outValue) const {
  // TODO(GG): check legality of operation (the method should be invoked from
  // the replica's internal thread)

  TimeRecorder scoped_timer(*histograms_.get_value);
  BlockId dummy;
  return getInternal(getLastBlockNum(), key, outValue, dummy);
}

Status ReplicaImp::get(BlockId readVersion, const Sliver &key, Sliver &outValue, BlockId &outBlock) const {
  // TODO(GG): check legality of operation (the method should be invoked from
  // the replica's internal thread)

  TimeRecorder scoped_timer(*histograms_.get_block);
  return getInternal(readVersion, key, outValue, outBlock);
}

Status ReplicaImp::getBlockData(BlockId blockId, SetOfKeyValuePairs &outBlockData) const {
  // TODO(GG): check legality of operation (the method should be invoked from
  // the replica's internal thread)

  try {
    TimeRecorder scoped_timer(*histograms_.get_block_data);
    Sliver block = getBlockInternal(blockId);
    outBlockData = m_bcDbAdapter->getBlockData(block);
  } catch (const NotFoundException &e) {
    LOG_ERROR(logger, e.what());
    return Status::NotFound("todo");
  }

  return Status::OK();
}

Status ReplicaImp::mayHaveConflictBetween(const Sliver &key, BlockId fromBlock, BlockId toBlock, bool &outRes) const {
  // TODO(GG): add assert or print warning if fromBlock==0 (all keys have a
  // conflict in block 0)

  TimeRecorder scoped_timer(*histograms_.may_have_conflict_between);
  // we conservatively assume that we have a conflict
  outRes = true;

  Sliver dummy;
  BlockId block = 0;
  Status s = getInternal(toBlock, key, dummy, block);
  if (s.isOK() && block < fromBlock) {
    outRes = false;
  }

  return s;
}

Status ReplicaImp::addBlock(const SetOfKeyValuePairs &updates,
                            BlockId &outBlockId,
                            const concordUtils::SpanWrapper & /*parent_span*/) {
  // TODO(GG): check legality of operation (the method should be invoked from
  // the replica's internal thread)

  // TODO(GG): what do we want to do with several identical keys in the same
  // block?

  TimeRecorder scoped_timer(*histograms_.add_block);
  return addBlockInternal(updates, outBlockId);
}

void ReplicaImp::deleteGenesisBlock() {
  const auto genesisBlock = m_bcDbAdapter->getGenesisBlockId();
  if (genesisBlock == 0) {
    throw std::logic_error{"Cannot delete the genesis block from an empty blockchain"};
  }
  m_bcDbAdapter->deleteBlock(genesisBlock);
}

BlockId ReplicaImp::deleteBlocksUntil(BlockId until) {
  const auto genesisBlock = m_bcDbAdapter->getGenesisBlockId();
  if (genesisBlock == 0) {
    throw std::logic_error{"Cannot delete a block range from an empty blockchain"};
  } else if (until <= genesisBlock) {
    throw std::invalid_argument{"Invalid 'until' value passed to deleteBlocksUntil()"};
  }

  const auto lastBlock = getLastBlock();
  const auto lastDeletedBlock = std::min(lastBlock, until - 1);
  for (auto i = genesisBlock; i <= lastDeletedBlock; ++i) {
    m_bcDbAdapter->deleteBlock(i);
  }
  return lastDeletedBlock;
}

void ReplicaImp::set_command_handler(ICommandsHandler *handler) {
  m_cmdHandler = handler;
  m_cmdHandler->setControlStateManager(controlStateManager_);
}

ReplicaImp::ReplicaImp(ICommunication *comm,
                       const bftEngine::ReplicaConfig &replicaConfig,
                       std::unique_ptr<IStorageFactory> storageFactory,
                       std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : logger(logging::getLogger("skvbc.replicaImp")),
      m_currentRepStatus(RepStatus::Idle),
      m_ptrComm(comm),
      replicaConfig_(replicaConfig),
      aggregator_(aggregator) {
  // Populate ST configuration
  bftEngine::bcst::Config stConfig = {
    replicaConfig_.replicaId,
    replicaConfig_.fVal,
    replicaConfig_.cVal,
    (uint16_t)(replicaConfig_.numReplicas + replicaConfig_.numRoReplicas),
    replicaConfig_.get("st.pedanticChecks", false),
    replicaConfig_.isReadOnly,

#if defined USE_COMM_PLAIN_TCP || defined USE_COMM_TLS_TCP
    replicaConfig_.get<uint32_t>("st.maxChunkSize", 30 * 1024 * 1024),
    replicaConfig_.get<uint16_t>("st.maxNumberOfChunksInBatch", 64),
#else
    replicaConfig_.get<uint32_t>("st.maxChunkSize", 2048),
    replicaConfig_.get<uint16_t>("st.maxNumberOfChunksInBatch", 32),
#endif
    replicaConfig_.get<uint32_t>("st.maxBlockSize", 30 * 1024 * 1024),
    replicaConfig_.get<uint32_t>("st.maxPendingDataFromSourceReplica", 256 * 1024 * 1024),
    replicaConfig_.getmaxNumOfReservedPages(),
    replicaConfig_.getsizeOfReservedPage(),
    replicaConfig_.get<uint32_t>("st.refreshTimerMs", 300),
    replicaConfig_.get<uint32_t>("st.checkpointSummariesRetransmissionTimeoutMs", 2500),
    replicaConfig_.get<uint32_t>("st.maxAcceptableMsgDelayMs", 60000),
    replicaConfig_.get<uint32_t>("st.sourceReplicaReplacementTimeoutMs", 15000),
    replicaConfig_.get<uint32_t>("st.fetchRetransmissionTimeoutMs", 250),
    replicaConfig_.get<uint32_t>("st.metricsDumpIntervalSec", 5),
    replicaConfig_.get("st.runInSeparateThread", replicaConfig_.isReadOnly)
  };

#if !defined USE_COMM_PLAIN_TCP && !defined USE_COMM_TLS_TCP
  // maxChunkSize * maxNumberOfChunksInBatch shouldn't exceed UDP message size which is limited to 64KB
  if (stConfig.maxChunkSize * stConfig.maxNumberOfChunksInBatch > 64 * 1024) {
    LOG_WARN(logger, "overriding incorrect chunking configuration for UDP");
    stConfig.maxChunkSize = 2048;
    stConfig.maxNumberOfChunksInBatch = 32;
  }
#endif

  auto dbSet = storageFactory->newDatabaseSet();
  m_bcDbAdapter = std::move(dbSet.dbAdapter);
  dbSet.dataDBClient->setAggregator(aggregator);
  dbSet.metadataDBClient->setAggregator(aggregator);
  m_metadataDBClient = dbSet.metadataDBClient;
  auto stKeyManipulator = std::shared_ptr<storage::ISTKeyManipulator>{storageFactory->newSTKeyManipulator()};
  m_stateTransfer = bftEngine::bcst::create(stConfig, this, m_metadataDBClient, stKeyManipulator, aggregator_);
  m_metadataStorage = new DBMetadataStorage(m_metadataDBClient.get(), storageFactory->newMetadataKeyManipulator());

  controlStateManager_ =
      std::make_shared<bftEngine::ControlStateManager>(m_stateTransfer, replicaConfig_.getsizeOfReservedPage());
}

ReplicaImp::~ReplicaImp() {
  if (m_replicaPtr) {
    if (m_replicaPtr->isRunning()) {
      m_replicaPtr->stop();
    }
  }
}

Status ReplicaImp::addBlockInternal(const SetOfKeyValuePairs &updates, BlockId &outBlockId) {
  outBlockId = m_bcDbAdapter->addBlock(updates);

  return Status::OK();
}

Status ReplicaImp::getInternal(BlockId readVersion, const Key &key, Sliver &outValue, BlockId &outBlock) const {
  const auto clear = [&outValue, &outBlock]() {
    outValue = Sliver{};
    outBlock = 0;
  };

  try {
    std::tie(outValue, outBlock) = m_bcDbAdapter->getValue(key, readVersion);
  } catch (const NotFoundException &) {
    clear();
  } catch (const std::exception &e) {
    clear();
    return Status::GeneralError(std::string{"getInternal() failed to get value due to a DBAdapter error: "} + e.what());
  } catch (...) {
    clear();
    return Status::GeneralError("getInternal() failed to get value due to an unknown DBAdapter error");
  }

  return Status::OK();
}

/*
 * This method can't return false by current insertBlockInternal impl.
 * It is used only by State Transfer to synchronize state between replicas.
 */
bool ReplicaImp::putBlock(const uint64_t blockId, const char *block_data, const uint32_t blockSize) {
  Sliver block = Sliver::copy(block_data, blockSize);

  if (m_bcDbAdapter->hasBlock(blockId)) {
    // if we already have a block with the same ID
    RawBlock existingBlock = m_bcDbAdapter->getRawBlock(blockId);
    if (existingBlock.length() != block.length() || memcmp(existingBlock.data(), block.data(), block.length()) != 0) {
      // the replica is corrupted !
      LOG_ERROR(logger,
                "found block " << blockId << ", size in db is " << existingBlock.length() << ", inserted is "
                               << block.length() << ", data in db " << existingBlock << ", data inserted " << block);
      LOG_ERROR(logger,
                "Block size test " << (existingBlock.length() != block.length()) << ", block data test "
                                   << (memcmp(existingBlock.data(), block.data(), block.length())));

      m_bcDbAdapter->deleteBlock(blockId);
      throw std::runtime_error(__PRETTY_FUNCTION__ + std::string("data corrupted blockId: ") + std::to_string(blockId));
    }
  } else {
    m_bcDbAdapter->addRawBlock(block, blockId);
  }

  return true;
}

RawBlock ReplicaImp::getBlockInternal(BlockId blockId) const { return m_bcDbAdapter->getRawBlock(blockId); }

/*
 * This method assumes that *outBlock is big enough to hold block content
 * The caller is the owner of the memory
 */
bool ReplicaImp::getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) {
  try {
    RawBlock block = getBlockInternal(blockId);
    *outBlockSize = block.length();
    memcpy(outBlock, block.data(), block.length());
    return true;
  } catch (const NotFoundException &e) {
    LOG_FATAL(logger, e.what());
    throw;
  }
}

bool ReplicaImp::hasBlock(BlockId blockId) const { return m_bcDbAdapter->hasBlock(blockId); }

bool ReplicaImp::getPrevDigestFromBlock(BlockId blockId, StateTransferDigest *outPrevBlockDigest) {
  ConcordAssert(blockId > 0);
  try {
    RawBlock result = getBlockInternal(blockId);
    auto parentDigest = m_bcDbAdapter->getParentDigest(result);
    ConcordAssert(outPrevBlockDigest != nullptr);
    static_assert(parentDigest.size() == BLOCK_DIGEST_SIZE);
    static_assert(sizeof(StateTransferDigest) == BLOCK_DIGEST_SIZE);
    memcpy(outPrevBlockDigest, parentDigest.data(), BLOCK_DIGEST_SIZE);
    return true;
  } catch (const NotFoundException &e) {
    LOG_FATAL(logger, "Block not found for parent digest, ID: " << blockId << " " << e.what());
    throw;
  }
}

}  // namespace concord::kvbc
