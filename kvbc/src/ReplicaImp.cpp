// Copyright 2018-2020 VMware, all rights reserved
//
// KV Blockchain replica implementation.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "ReplicaImp.h"
#include <inttypes.h>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <string_view>
#include <utility>
#include "assertUtils.hpp"
#include "communication/CommDefs.hpp"
#include "kv_types.hpp"
#include "hex_tools.h"
#include "replica_state_sync.h"
#include "sliver.hpp"
#include "bftengine/DbMetadataStorage.hpp"
#include "rocksdb/native_client.h"
#include "pruning_handler.hpp"
#include "IRequestHandler.hpp"
#include "reconfiguration_add_block_handler.hpp"

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
    auto requestHandler = bftEngine::IRequestsHandler::createRequestsHandler(m_cmdHandler);
    requestHandler->setPruningHandler(std::make_shared<pruning::ReadOnlyReplicaPruningHandler>(*this));
    m_replicaPtr = bftEngine::IReplica::createNewRoReplica(replicaConfig_, requestHandler, m_stateTransfer, m_ptrComm);
  } else {
    createReplicaAndSyncState();
  }
  m_replicaPtr->SetAggregator(aggregator_);
  m_replicaPtr->start();
  m_currentRepStatus = RepStatus::Running;

  /// TODO(IG, GG)
  /// add return value to start/stop

  return Status::OK();
}

void ReplicaImp::createReplicaAndSyncState() {
  auto requestHandler = bftEngine::IRequestsHandler::createRequestsHandler(m_cmdHandler);
  requestHandler->setPruningHandler(std::shared_ptr<concord::reconfiguration::IPruningHandler>(
      new concord::kvbc::pruning::PruningHandler(*this, *this, *this, *m_stateTransfer, true)));
  requestHandler->setReconfigurationHandler(
      std::make_shared<kvbc::reconfiguration::ReconfigurationHandler>(*this, *this));
  m_replicaPtr = bftEngine::IReplica::createNewReplica(
      replicaConfig_, requestHandler, m_stateTransfer, m_ptrComm, m_metadataStorage, pm_, secretsManager_);
  const auto lastExecutedSeqNum = m_replicaPtr->getLastExecutedSequenceNum();
  LOG_INFO(logger, KVLOG(lastExecutedSeqNum));
  if (!replicaConfig_.isReadOnly && !m_stateTransfer->isCollectingState()) {
    try {
      uint64_t removedBlocksNum = replicaStateSync_->execute(logger, *m_kvBlockchain, lastExecutedSeqNum);
      LOG_INFO(logger, KVLOG(lastExecutedSeqNum, removedBlocksNum, getLastBlockNum(), getLastReachableBlockNum()));
    } catch (std::exception &e) {
      std::terminate();
    }
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

const IReader &ReplicaImp::getReadOnlyStorage() const { return *this; }

BlockId ReplicaImp::addBlockToIdleReplica(categorization::Updates &&updates) {
  if (getReplicaStatus() != IReplica::RepStatus::Idle) {
    throw std::logic_error{"addBlockToIdleReplica() called on a non-idle replica"};
  }

  return m_kvBlockchain->addBlock(std::move(updates));
}

void ReplicaImp::deleteGenesisBlock() {
  const auto genesisBlock = m_kvBlockchain->getGenesisBlockId();
  if (genesisBlock == 0) {
    throw std::logic_error{"Cannot delete the genesis block from an empty blockchain"};
  }
  m_kvBlockchain->deleteBlock(genesisBlock);
}

BlockId ReplicaImp::deleteBlocksUntil(BlockId until) {
  const auto genesisBlock = m_kvBlockchain->getGenesisBlockId();
  if (genesisBlock == 0) {
    throw std::logic_error{"Cannot delete a block range from an empty blockchain"};
  } else if (until <= genesisBlock) {
    throw std::invalid_argument{"Invalid 'until' value passed to deleteBlocksUntil()"};
  }

  const auto lastReachableBlock = m_kvBlockchain->getLastReachableBlockId();
  const auto lastDeletedBlock = std::min(lastReachableBlock, until - 1);
  for (auto i = genesisBlock; i <= lastDeletedBlock; ++i) {
    ConcordAssert(m_kvBlockchain->deleteBlock(i));
  }
  return lastDeletedBlock;
}

BlockId ReplicaImp::add(categorization::Updates &&updates) { return m_kvBlockchain->addBlock(std::move(updates)); }

std::optional<categorization::Value> ReplicaImp::get(const std::string &category_id,
                                                     const std::string &key,
                                                     BlockId block_id) const {
  return m_kvBlockchain->get(category_id, key, block_id);
}

std::optional<categorization::Value> ReplicaImp::getLatest(const std::string &category_id,
                                                           const std::string &key) const {
  return m_kvBlockchain->getLatest(category_id, key);
}

void ReplicaImp::multiGet(const std::string &category_id,
                          const std::vector<std::string> &keys,
                          const std::vector<BlockId> &versions,
                          std::vector<std::optional<categorization::Value>> &values) const {
  return m_kvBlockchain->multiGet(category_id, keys, versions, values);
}

void ReplicaImp::multiGetLatest(const std::string &category_id,
                                const std::vector<std::string> &keys,
                                std::vector<std::optional<categorization::Value>> &values) const {
  return m_kvBlockchain->multiGetLatest(category_id, keys, values);
}

std::optional<categorization::TaggedVersion> ReplicaImp::getLatestVersion(const std::string &category_id,
                                                                          const std::string &key) const {
  return m_kvBlockchain->getLatestVersion(category_id, key);
}

void ReplicaImp::multiGetLatestVersion(const std::string &category_id,
                                       const std::vector<std::string> &keys,
                                       std::vector<std::optional<categorization::TaggedVersion>> &versions) const {
  return m_kvBlockchain->multiGetLatestVersion(category_id, keys, versions);
}

std::optional<categorization::Updates> ReplicaImp::getBlockUpdates(BlockId block_id) const {
  return m_kvBlockchain->getBlockUpdates(block_id);
}

BlockId ReplicaImp::getGenesisBlockId() const { return m_kvBlockchain->getGenesisBlockId(); }

BlockId ReplicaImp::getLastBlockId() const { return m_kvBlockchain->getLastReachableBlockId(); }

void ReplicaImp::set_command_handler(std::shared_ptr<ICommandsHandler> handler) { m_cmdHandler = handler; }

ReplicaImp::ReplicaImp(ICommunication *comm,
                       const bftEngine::ReplicaConfig &replicaConfig,
                       std::unique_ptr<IStorageFactory> storageFactory,
                       std::shared_ptr<concordMetrics::Aggregator> aggregator,
                       const std::shared_ptr<concord::performance::PerformanceManager> &pm,
                       std::map<std::string, categorization::CATEGORY_TYPE> kvbc_categories,
                       const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &secretsManager)
    : logger(logging::getLogger("skvbc.replicaImp")),
      m_currentRepStatus(RepStatus::Idle),
      m_dbSet{storageFactory->newDatabaseSet()},
      m_bcDbAdapter{std::move(m_dbSet.dbAdapter)},
      m_metadataDBClient{m_dbSet.metadataDBClient},
      m_ptrComm(comm),
      replicaConfig_(replicaConfig),
      aggregator_(aggregator),
      pm_{pm},
      secretsManager_{secretsManager} {
  // Populate ST configuration
  bftEngine::bcst::Config stConfig = {
    replicaConfig_.replicaId,
    replicaConfig_.fVal,
    replicaConfig_.cVal,
    replicaConfig_.numReplicas,
    replicaConfig_.numRoReplicas,
    replicaConfig_.get("concord.bft.st.pedanticChecks", false),
    replicaConfig_.isReadOnly,

#if defined USE_COMM_PLAIN_TCP || defined USE_COMM_TLS_TCP
    replicaConfig_.get<uint32_t>("concord.bft.st.maxChunkSize", 30 * 1024 * 1024),
    replicaConfig_.get<uint16_t>("concord.bft.st.maxNumberOfChunksInBatch", 64),
#else
    replicaConfig_.get<uint32_t>("concord.bft.st.maxChunkSize", 2048),
    replicaConfig_.get<uint16_t>("concord.bft.st.maxNumberOfChunksInBatch", 32),
#endif
    replicaConfig_.get<uint32_t>("concord.bft.st.maxBlockSize", 30 * 1024 * 1024),
    replicaConfig_.get<uint32_t>("concord.bft.st.maxPendingDataFromSourceReplica", 256 * 1024 * 1024),
    replicaConfig_.getmaxNumOfReservedPages(),
    replicaConfig_.getsizeOfReservedPage(),
    replicaConfig_.get<uint32_t>("concord.bft.st.refreshTimerMs", 300),
    replicaConfig_.get<uint32_t>("concord.bft.st.checkpointSummariesRetransmissionTimeoutMs", 2500),
    replicaConfig_.get<uint32_t>("concord.bft.st.maxAcceptableMsgDelayMs", 60000),
    replicaConfig_.get<uint32_t>("concord.bft.st.sourceReplicaReplacementTimeoutMs", 15000),
    replicaConfig_.get<uint32_t>("concord.bft.st.fetchRetransmissionTimeoutMs", 1000),
    replicaConfig_.get<uint32_t>("concord.bft.st.metricsDumpIntervalSec", 5),
    replicaConfig_.get("concord.bft.st.runInSeparateThread", replicaConfig_.isReadOnly),
    replicaConfig_.get("concord.bft.st.enableReservedPages", !replicaConfig_.isReadOnly)
  };

#if !defined USE_COMM_PLAIN_TCP && !defined USE_COMM_TLS_TCP
  // maxChunkSize * maxNumberOfChunksInBatch shouldn't exceed UDP message size which is limited to 64KB
  if (stConfig.maxChunkSize * stConfig.maxNumberOfChunksInBatch > 64 * 1024) {
    LOG_WARN(logger, "overriding incorrect chunking configuration for UDP");
    stConfig.maxChunkSize = 2048;
    stConfig.maxNumberOfChunksInBatch = 32;
  }
#endif

  if (!replicaConfig.isReadOnly) {
    const auto linkStChain = true;
    auto [it, inserted] =
        kvbc_categories.insert(std::make_pair(kConcordInternalCategoryId, categorization::CATEGORY_TYPE::versioned_kv));
    if (!inserted && it->second != categorization::CATEGORY_TYPE::versioned_kv) {
      const auto msg = "Invalid Concord internal category type: " + categorization::categoryStringType(it->second);
      LOG_ERROR(logger, msg);
      throw std::invalid_argument{msg};
    }
    m_kvBlockchain.emplace(
        storage::rocksdb::NativeClient::fromIDBClient(m_dbSet.dataDBClient), linkStChain, kvbc_categories);
    m_kvBlockchain->setAggregator(aggregator);

    auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    concord::diagnostics::StatusHandler handler(
        "pruning", "Pruning Status", [this]() { return m_kvBlockchain->getPruningStatus(); });
    registrar.status.registerHandler(handler);
  }
  m_dbSet.dataDBClient->setAggregator(aggregator);
  m_dbSet.metadataDBClient->setAggregator(aggregator);
  auto stKeyManipulator = std::shared_ptr<storage::ISTKeyManipulator>{storageFactory->newSTKeyManipulator()};
  m_stateTransfer = bftEngine::bcst::create(stConfig, this, m_metadataDBClient, stKeyManipulator, aggregator_);
  m_metadataStorage = new DBMetadataStorage(m_metadataDBClient.get(), storageFactory->newMetadataKeyManipulator());
  bftEngine::ControlStateManager::instance(m_stateTransfer);
}

ReplicaImp::~ReplicaImp() {
  if (m_replicaPtr) {
    if (m_replicaPtr->isRunning()) {
      m_replicaPtr->stop();
    }
  }
}

/*
 * This method can't return false by current insertBlockInternal impl.
 * It is used only by State Transfer to synchronize state between replicas.
 */
bool ReplicaImp::putBlock(const uint64_t blockId, const char *blockData, const uint32_t blockSize) {
  if (replicaConfig_.isReadOnly) {
    return putBlockToObjectStore(blockId, blockData, blockSize);
  }

  auto view = std::string_view{blockData, blockSize};
  const auto rawBlock = categorization::RawBlock::deserialize(view);
  if (m_kvBlockchain->hasBlock(blockId)) {
    const auto existingRawBlock = m_kvBlockchain->getRawBlock(blockId);
    if (rawBlock != existingRawBlock) {
      LOG_ERROR(logger,
                "found existing (and different) block ID[" << blockId << "] when receiving from state transfer");

      // TODO consider assert?
      m_kvBlockchain->deleteBlock(blockId);
      throw std::runtime_error(
          __PRETTY_FUNCTION__ +
          std::string("found existing (and different) block when receiving state transfer, block ID: ") +
          std::to_string(blockId));
    }
  } else {
    m_kvBlockchain->addRawBlock(rawBlock, blockId);
  }
  return true;
}

bool ReplicaImp::putBlockToObjectStore(const uint64_t blockId, const char *blockData, const uint32_t blockSize) {
  Sliver block = Sliver::copy(blockData, blockSize);

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

uint64_t ReplicaImp::getLastReachableBlockNum() const {
  if (replicaConfig_.isReadOnly) {
    return m_bcDbAdapter->getLastReachableBlockId();
  }
  return m_kvBlockchain->getLastReachableBlockId();
}

uint64_t ReplicaImp::getLastBlockNum() const {
  if (replicaConfig_.isReadOnly) {
    return m_bcDbAdapter->getLatestBlockId();
  }
  const auto last = m_kvBlockchain->getLastStatetransferBlockId();
  if (last) {
    return *last;
  }
  return m_kvBlockchain->getLastReachableBlockId();
}

RawBlock ReplicaImp::getBlockInternal(BlockId blockId) const { return m_bcDbAdapter->getRawBlock(blockId); }

/*
 * This method assumes that *outBlock is big enough to hold block content
 * The caller is the owner of the memory
 */
bool ReplicaImp::getBlock(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) {
  if (replicaConfig_.isReadOnly) {
    return getBlockFromObjectStore(blockId, outBlock, outBlockSize);
  }
  const auto rawBlock = m_kvBlockchain->getRawBlock(blockId);
  if (!rawBlock) {
    throw NotFoundException{"Raw block not found: " + std::to_string(blockId)};
  }
  const auto &ser = categorization::RawBlock::serialize(*rawBlock);
  *outBlockSize = ser.size();
  LOG_DEBUG(logger, KVLOG(blockId, *outBlockSize));
  std::memcpy(outBlock, ser.data(), *outBlockSize);
  return true;
}

bool ReplicaImp::getBlockFromObjectStore(uint64_t blockId, char *outBlock, uint32_t *outBlockSize) {
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

bool ReplicaImp::hasBlock(BlockId blockId) const {
  if (replicaConfig_.isReadOnly) {
    return m_bcDbAdapter->hasBlock(blockId);
  }
  return m_kvBlockchain->hasBlock(blockId);
}

bool ReplicaImp::getPrevDigestFromBlock(BlockId blockId, StateTransferDigest *outPrevBlockDigest) {
  if (replicaConfig_.isReadOnly) {
    return getPrevDigestFromObjectStoreBlock(blockId, outPrevBlockDigest);
  }
  ConcordAssert(blockId > 0);
  const auto parent_digest = m_kvBlockchain->parentDigest(blockId);
  ConcordAssert(parent_digest.has_value());
  static_assert(parent_digest->size() == BLOCK_DIGEST_SIZE);
  static_assert(sizeof(StateTransferDigest) == BLOCK_DIGEST_SIZE);
  std::memcpy(outPrevBlockDigest, parent_digest->data(), BLOCK_DIGEST_SIZE);
  return true;
}

bool ReplicaImp::getPrevDigestFromObjectStoreBlock(uint64_t blockId,
                                                   bftEngine::bcst::StateTransferDigest *outPrevBlockDigest) {
  ConcordAssert(blockId > 0);
  try {
    const auto rawBlockSer = m_bcDbAdapter->getRawBlock(blockId);
    const auto rawBlock = categorization::RawBlock::deserialize(rawBlockSer);
    ConcordAssert(outPrevBlockDigest != nullptr);
    static_assert(rawBlock.data.parent_digest.size() == BLOCK_DIGEST_SIZE);
    static_assert(sizeof(StateTransferDigest) == BLOCK_DIGEST_SIZE);
    memcpy(outPrevBlockDigest, rawBlock.data.parent_digest.data(), BLOCK_DIGEST_SIZE);
    return true;
  } catch (const NotFoundException &e) {
    LOG_FATAL(logger, "Block not found for parent digest, ID: " << blockId << " " << e.what());
    throw;
  }
}

}  // namespace concord::kvbc
