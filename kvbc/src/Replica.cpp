// Copyright 2018-2020 VMware, all rights reserved
//
// KV Blockchain replica implementation.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "Replica.h"
#include <inttypes.h>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <string_view>
#include <utility>
#include "assertUtils.hpp"
#include "endianness.hpp"
#include "communication/CommDefs.hpp"
#include "kv_types.hpp"
#include "hex_tools.h"
#include "replica_state_sync.h"
#include "sliver.hpp"
#include "metadata_block_id.h"
#include "bftengine/DbMetadataStorage.hpp"
#include "rocksdb/native_client.h"
#include "pruning_handler.hpp"
#include "IRequestHandler.hpp"
#include "RequestHandler.h"
#include "reconfiguration_kvbc_handler.hpp"
#include "st_reconfiguraion_sm.hpp"
#include "bftengine/ControlHandler.hpp"
#include "throughput.hpp"
#include "bftengine/EpochManager.hpp"
#include "bftengine/ReconfigurationCmd.hpp"
#include "client/reconfiguration/st_based_reconfiguration_client.hpp"
#include "client/reconfiguration/client_reconfiguration_engine.hpp"
#include "bftengine/ReplicaConfig.hpp"

using bft::communication::ICommunication;
using bftEngine::bcst::StateTransferDigest;
using concord::kvbc::categorization::kConcordInternalCategoryId;
using namespace concord::diagnostics;

using concord::storage::DBMetadataStorage;

namespace concord::kvbc {

Status Replica::initInternals() {
  LOG_INFO(logger, "Replica::initInternals() id = " << replicaConfig_.replicaId);

  if (m_currentRepStatus != RepStatus::Idle) {
    const auto msg = "Replica::initInternals(): replica not in idle state, cannot initialize";
    LOG_ERROR(logger, msg);
    return Status::IllegalOperation(msg);
  }

  m_currentRepStatus = RepStatus::Starting;

  if (replicaConfig_.isReadOnly) {
    LOG_INFO(logger, "ReadOnly mode");
    auto requestHandler = bftEngine::IRequestsHandler::createRequestsHandler(m_cmdHandler, cronTableRegistry_);
    requestHandler->setReconfigurationHandler(std::make_shared<pruning::ReadOnlyReplicaPruningHandler>(*this));
    m_replicaPtr = bftEngine::IReplica::createNewRoReplica(replicaConfig_, requestHandler, m_stateTransfer, m_ptrComm);
    m_stateTransfer->addOnTransferringCompleteCallback([this](std::uint64_t) {
      std::vector<concord::client::reconfiguration::State> stateFromReservedPages;
      uint64_t wedgePt{0};
      uint64_t cmdEpochNum{0};
      if (bftEngine::ReconfigurationCmd::instance().getStateFromResPages(
              stateFromReservedPages, wedgePt, cmdEpochNum)) {
        // get replicas latest global epoch from res pages
        auto replicasGlobalEpochNum = bftEngine::EpochManager::instance().getGlobalEpochNumber();
        LOG_INFO(GL,
                 "reconfiguration command in res pages" << KVLOG(
                     wedgePt, cmdEpochNum, replicasGlobalEpochNum, m_replicaPtr->getLastExecutedSequenceNum()));
        // OnTransferringComplete callback is called for every 150 seqNum/checkpt, but we want to push reconfiguration
        // command only when wedge pt is reached
        if (static_cast<uint64_t>(m_replicaPtr->getLastExecutedSequenceNum()) >= wedgePt ||
            replicasGlobalEpochNum > cmdEpochNum) {
          creClient_->pushUpdate(stateFromReservedPages);
        }
      }
    });
  } else {
    createReplicaAndSyncState();
  }
  m_replicaPtr->SetAggregator(aggregator_);
  return Status::OK();
}

/**
 * Opens the database and creates the replica thread. Replica state moves to
 * Starting.
 */
Status Replica::start() {
  if (m_currentRepStatus == RepStatus::Idle) {
    auto initStatus = initInternals();
    if (initStatus != Status::OK()) {
      return initStatus;
    }
  }
  if (m_currentRepStatus != RepStatus::Starting) {
    const auto msg = "Replica::start(): replica not initialized or already started";
    LOG_ERROR(logger, msg);
    return Status::IllegalOperation(msg);
  }
  m_replicaPtr->start();
  m_currentRepStatus = RepStatus::Running;
  if (replicaConfig_.isReadOnly) startRoReplicaCreEngine();
  /// TODO(IG, GG)
  /// add return value to start/stop

  return Status::OK();
}

class KvbcRequestHandler : public bftEngine::RequestHandler {
 public:
  static std::shared_ptr<KvbcRequestHandler> create(
      const std::shared_ptr<IRequestsHandler> &user_req_handler,
      const std::shared_ptr<concord::cron::CronTableRegistry> &cron_table_registry,
      categorization::KeyValueBlockchain &blockchain) {
    return std::shared_ptr<KvbcRequestHandler>{
        new KvbcRequestHandler{user_req_handler, cron_table_registry, blockchain}};
  }

 public:
  // Make sure we persist the last kvbc block ID in metadata after every execute() call.
  void onFinishExecutingReadWriteRequests() override {
    bftEngine::RequestHandler::onFinishExecutingReadWriteRequests();
    constexpr auto in_transaction = true;
    persistLastBlockIdInMetadata<in_transaction>(blockchain_, persistent_storage_);
  }

  void setPersistentStorage(const std::shared_ptr<bftEngine::impl::PersistentStorage> &persistent_storage) {
    persistent_storage_ = persistent_storage;
  }

 private:
  KvbcRequestHandler(const std::shared_ptr<IRequestsHandler> &user_req_handler,
                     const std::shared_ptr<concord::cron::CronTableRegistry> &cron_table_registry,
                     categorization::KeyValueBlockchain &blockchain)
      : blockchain_{blockchain} {
    setUserRequestHandler(user_req_handler);
    setCronTableRegistry(cron_table_registry);
  }

  KvbcRequestHandler(const KvbcRequestHandler &) = delete;
  KvbcRequestHandler(KvbcRequestHandler &&) = delete;
  KvbcRequestHandler &operator=(const RequestHandler &) = delete;
  KvbcRequestHandler &operator=(KvbcRequestHandler &&) = delete;

 private:
  std::shared_ptr<bftEngine::impl::PersistentStorage> persistent_storage_;
  categorization::KeyValueBlockchain &blockchain_;
};
void Replica::registerReconfigurationHandlers(std::shared_ptr<bftEngine::IRequestsHandler> requestHandler) {
  requestHandler->setReconfigurationHandler(
      std::make_shared<kvbc::reconfiguration::ReconfigurationHandler>(*this, *this),
      concord::reconfiguration::ReconfigurationHandlerType::PRE);
  requestHandler->setReconfigurationHandler(
      std::make_shared<kvbc::reconfiguration::InternalKvReconfigurationHandler>(*this, *this),
      concord::reconfiguration::ReconfigurationHandlerType::PRE);
  requestHandler->setReconfigurationHandler(
      std::make_shared<kvbc::reconfiguration::KvbcClientReconfigurationHandler>(*this, *this),
      concord::reconfiguration::ReconfigurationHandlerType::PRE);
  requestHandler->setReconfigurationHandler(
      std::make_shared<kvbc::reconfiguration::InternalPostKvReconfigurationHandler>(*this, *this),
      concord::reconfiguration::ReconfigurationHandlerType::POST);
  auto pruning_handler = std::shared_ptr<kvbc::pruning::PruningHandler>(
      new concord::kvbc::pruning::PruningHandler(*this, *this, *this, true));
  requestHandler->setReconfigurationHandler(pruning_handler);
  stReconfigurationSM_->registerHandler(m_cmdHandler->getReconfigurationHandler());
  stReconfigurationSM_->registerHandler(pruning_handler);
  if (bftEngine::ReplicaConfig::instance().pruningEnabled_) {
    stReconfigurationSM_->pruneOnStartup();
  }
}
uint64_t Replica::getStoredReconfigData(const std::string &kCategory,
                                        const std::string &key,
                                        const kvbc::BlockId &bid) {
  if (bid == 0) {
    const auto val = getLatest(kCategory, key);
    if (!val.has_value()) return 0;
    const auto &data = std::get<categorization::VersionedValue>(*val).data;
    ConcordAssertEQ(data.size(), sizeof(uint64_t));
    return concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
  }
  const auto val = get(kCategory, key, bid);
  if (!val.has_value()) return 0;
  const auto &data = std::get<categorization::VersionedValue>(*val).data;
  ConcordAssertEQ(data.size(), sizeof(uint64_t));
  return concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
}
void Replica::handleWedgeEvent() {
  auto lastExecutedSeqNum = m_replicaPtr->getLastExecutedSequenceNum();
  uint64_t wedgeBlock{0};
  auto version = getLatestVersion(kConcordInternalCategoryId, std::string{keyTypes::reconfiguration_wedge_key});
  if (!version.has_value()) return;
  wedgeBlock = version.value().version;

  uint64_t wedgeBftSeqNum =
      getStoredReconfigData(kConcordInternalCategoryId, std::string{keyTypes::bft_seq_num_key}, wedgeBlock);
  if (wedgeBftSeqNum == 0) {
    // We may have a pruning that run in the background
    return;
  }

  uint64_t wedgePoint = (wedgeBftSeqNum + 2 * checkpointWindowSize);
  wedgePoint = wedgePoint - (wedgePoint % checkpointWindowSize);
  uint64_t latestKnownEpoch =
      getStoredReconfigData(kConcordInternalCategoryId, std::string{keyTypes::reconfiguration_epoch_key}, 0);
  uint64_t wedgeEpoch =
      getStoredReconfigData(kConcordInternalCategoryId, std::string{keyTypes::reconfiguration_epoch_key}, wedgeBlock);

  LOG_INFO(logger,
           "stored wedge info " << KVLOG(wedgePoint, wedgeBlock, wedgeEpoch, lastExecutedSeqNum, latestKnownEpoch));
  if (wedgeEpoch == latestKnownEpoch) {
    bftEngine::ControlStateManager::instance().setStopAtNextCheckpoint(wedgeBftSeqNum);
    if (wedgePoint == (uint64_t)lastExecutedSeqNum) {
      bftEngine::IControlHandler::instance()->onStableCheckpoint();
    }
    LOG_INFO(logger, "wedge the system on sequence number: " << lastExecutedSeqNum);
  }
}
void Replica::handleNewEpochEvent() {
  uint64_t epoch = 0;
  epoch = getStoredReconfigData(kConcordInternalCategoryId, std::string{keyTypes::reconfiguration_epoch_key}, 0);

  auto bft_seq_num = m_replicaPtr->getLastExecutedSequenceNum();
  // Create a new epoch block if needed
  bftEngine::EpochManager::instance().setAggregator(aggregator_);
  if (bftEngine::EpochManager::instance().isNewEpoch()) {
    epoch += 1;
    auto data = concordUtils::toBigEndianStringBuffer(epoch);
    concord::kvbc::categorization::VersionedUpdates ver_updates;
    ver_updates.addUpdate(std::string{kvbc::keyTypes::reconfiguration_epoch_key},
                          std::string(data.begin(), data.end()));
    // All blocks are expected to have the BFT sequence number as a key.
    ver_updates.addUpdate(std::string{kvbc::keyTypes::bft_seq_num_key},
                          concordUtils::toBigEndianStringBuffer(bft_seq_num));
    concord::kvbc::categorization::Updates updates;
    updates.add(kConcordInternalCategoryId, std::move(ver_updates));
    try {
      auto bid = add(std::move(updates));
      persistLastBlockIdInMetadata<false>(*m_kvBlockchain, m_replicaPtr->persistentStorage());
      bftEngine::EpochManager::instance().setGlobalEpochNumber(epoch);
      bftEngine::EpochManager::instance().setNewEpochFlag(false);
      LOG_INFO(logger, "new epoch block" << KVLOG(bid, epoch, bft_seq_num));
    } catch (const std::exception &e) {
      LOG_ERROR(logger, "failed to persist the reconfiguration block: " << e.what());
      throw;
    }
    bftEngine::EpochManager::instance().markEpochAsStarted();
    // update reserved pages with reconfiguration command from previous epoch
    saveReconfigurationCmdToResPages<concord::messages::AddRemoveWithWedgeCommand>(
        std::string{kvbc::keyTypes::reconfiguration_add_remove, 0x1});
  }
  LOG_INFO(logger, "replica epoch is: " << epoch);
  bftEngine::EpochManager::instance().setSelfEpochNumber(epoch);
}
template <typename T>
void Replica::saveReconfigurationCmdToResPages(const std::string &key) {
  auto res = getLatest(kConcordInternalCategoryId, key);
  if (res.has_value()) {
    auto blockid = getLatestVersion(kConcordInternalCategoryId, key).value().version;
    auto strval = std::visit([](auto &&arg) { return arg.data; }, *res);
    T cmd;
    std::vector<uint8_t> bytesval(strval.begin(), strval.end());
    concord::messages::deserialize(bytesval, cmd);
    concord::messages::ReconfigurationRequest rreq;
    rreq.command = cmd;
    auto sequenceNum =
        getStoredReconfigData(kConcordInternalCategoryId, std::string{keyTypes::bft_seq_num_key}, blockid);
    auto epochNum =
        getStoredReconfigData(kConcordInternalCategoryId, std::string{keyTypes::reconfiguration_epoch_key}, blockid);
    auto wedgePoint = (sequenceNum + 2 * checkpointWindowSize);
    wedgePoint = wedgePoint - (wedgePoint % checkpointWindowSize);
    bftEngine::ReconfigurationCmd::instance().saveReconfigurationCmdToResPages(
        rreq, key, blockid, wedgePoint, epochNum);
  }
}
void Replica::createReplicaAndSyncState() {
  ConcordAssert(m_kvBlockchain.has_value());
  auto requestHandler = KvbcRequestHandler::create(m_cmdHandler, cronTableRegistry_, *m_kvBlockchain);
  registerReconfigurationHandlers(requestHandler);
  m_replicaPtr = bftEngine::IReplica::createNewReplica(
      replicaConfig_, requestHandler, m_stateTransfer, m_ptrComm, m_metadataStorage, pm_, secretsManager_);
  requestHandler->setPersistentStorage(m_replicaPtr->persistentStorage());

  // Make sure that when state transfer completes, we persist the last kvbc block ID in metadata.
  m_stateTransfer->addOnTransferringCompleteCallback([this](std::uint64_t) {
    constexpr auto in_transaction = false;
    persistLastBlockIdInMetadata<in_transaction>(*m_kvBlockchain, m_replicaPtr->persistentStorage());
  });

  const auto lastExecutedSeqNum = m_replicaPtr->getLastExecutedSequenceNum();
  LOG_INFO(logger, KVLOG(lastExecutedSeqNum));
  if (!replicaConfig_.isReadOnly && !m_stateTransfer->isCollectingState()) {
    try {
      const auto maxNumOfBlocksToDelete = replicaConfig_.maxNumOfRequestsInBatch;
      const auto removedBlocksNum = replicaStateSync_->execute(
          logger, *m_kvBlockchain, m_replicaPtr->persistentStorage(), lastExecutedSeqNum, maxNumOfBlocksToDelete);
      LOG_INFO(logger,
               KVLOG(lastExecutedSeqNum,
                     removedBlocksNum,
                     getLastBlockNum(),
                     getLastReachableBlockNum(),
                     maxNumOfBlocksToDelete));
    } catch (std::exception &e) {
      LOG_FATAL(logger, "exception: " << e.what());
      std::terminate();
    }
  }
  handleNewEpochEvent();
  handleWedgeEvent();
}

/**
 * Closes the database. Call `wait()` after this to wait for thread to stop.
 */
Status Replica::stop() {
  m_currentRepStatus = RepStatus::Stopping;
  m_replicaPtr->stop();
  m_currentRepStatus = RepStatus::Idle;
  return Status::OK();
}

Replica::RepStatus Replica::getReplicaStatus() const { return m_currentRepStatus; }

const IReader &Replica::getReadOnlyStorage() const { return *this; }

BlockId Replica::addBlockToIdleReplica(categorization::Updates &&updates) {
  if (getReplicaStatus() != IReplica::RepStatus::Idle) {
    throw std::logic_error{"addBlockToIdleReplica() called on a non-idle replica"};
  }

  return m_kvBlockchain->addBlock(std::move(updates));
}

void Replica::deleteGenesisBlock() {
  const auto genesisBlock = m_kvBlockchain->getGenesisBlockId();
  if (genesisBlock == 0) {
    throw std::logic_error{"Cannot delete the genesis block from an empty blockchain"};
  }
  m_kvBlockchain->deleteBlock(genesisBlock);
}

BlockId Replica::deleteBlocksUntil(BlockId until) {
  const auto genesisBlock = m_kvBlockchain->getGenesisBlockId();
  if (genesisBlock == 0) {
    throw std::logic_error{"Cannot delete a block range from an empty blockchain"};
  } else if (until <= genesisBlock) {
    throw std::invalid_argument{"Invalid 'until' value passed to deleteBlocksUntil()"};
  }

  const auto lastReachableBlock = m_kvBlockchain->getLastReachableBlockId();
  const auto lastDeletedBlock = std::min(lastReachableBlock, until - 1);
  const auto start = std::chrono::steady_clock::now();
  for (auto i = genesisBlock; i <= lastDeletedBlock; ++i) {
    ConcordAssert(m_kvBlockchain->deleteBlock(i));
  }
  auto jobDuration =
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
  histograms_.delete_batch_blocks_duration->recordAtomic(jobDuration);
  return lastDeletedBlock;
}

BlockId Replica::add(categorization::Updates &&updates) { return m_kvBlockchain->addBlock(std::move(updates)); }

std::optional<categorization::Value> Replica::get(const std::string &category_id,
                                                  const std::string &key,
                                                  BlockId block_id) const {
  return m_kvBlockchain->get(category_id, key, block_id);
}

std::optional<categorization::Value> Replica::getLatest(const std::string &category_id, const std::string &key) const {
  return m_kvBlockchain->getLatest(category_id, key);
}

void Replica::multiGet(const std::string &category_id,
                       const std::vector<std::string> &keys,
                       const std::vector<BlockId> &versions,
                       std::vector<std::optional<categorization::Value>> &values) const {
  return m_kvBlockchain->multiGet(category_id, keys, versions, values);
}

void Replica::multiGetLatest(const std::string &category_id,
                             const std::vector<std::string> &keys,
                             std::vector<std::optional<categorization::Value>> &values) const {
  return m_kvBlockchain->multiGetLatest(category_id, keys, values);
}

std::optional<categorization::TaggedVersion> Replica::getLatestVersion(const std::string &category_id,
                                                                       const std::string &key) const {
  return m_kvBlockchain->getLatestVersion(category_id, key);
}

void Replica::multiGetLatestVersion(const std::string &category_id,
                                    const std::vector<std::string> &keys,
                                    std::vector<std::optional<categorization::TaggedVersion>> &versions) const {
  return m_kvBlockchain->multiGetLatestVersion(category_id, keys, versions);
}

std::optional<categorization::Updates> Replica::getBlockUpdates(BlockId block_id) const {
  return m_kvBlockchain->getBlockUpdates(block_id);
}

BlockId Replica::getGenesisBlockId() const {
  if (replicaConfig_.isReadOnly) return m_bcDbAdapter->getGenesisBlockId();
  return m_kvBlockchain->getGenesisBlockId();
}

BlockId Replica::getLastBlockId() const {
  if (replicaConfig_.isReadOnly) {
    return m_bcDbAdapter->getLastReachableBlockId();
  }
  return m_kvBlockchain->getLastReachableBlockId();
}

void Replica::set_command_handler(std::shared_ptr<ICommandsHandler> handler) { m_cmdHandler = handler; }

Replica::Replica(ICommunication *comm,
                 const bftEngine::ReplicaConfig &replicaConfig,
                 std::unique_ptr<IStorageFactory> storageFactory,
                 std::shared_ptr<concordMetrics::Aggregator> aggregator,
                 const std::shared_ptr<concord::performance::PerformanceManager> &pm,
                 std::map<std::string, categorization::CATEGORY_TYPE> kvbc_categories,
                 const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &secretsManager)
    : logger(logging::getLogger("skvbc.replica")),
      m_currentRepStatus(RepStatus::Idle),
      m_dbSet{storageFactory->newDatabaseSet()},
      m_bcDbAdapter{std::move(m_dbSet.dbAdapter)},
      m_metadataDBClient{m_dbSet.metadataDBClient},
      m_ptrComm(comm),
      replicaConfig_(replicaConfig),
      aggregator_(aggregator),
      pm_{pm},
      secretsManager_{secretsManager},
      blocksIOWorkersPool_((replicaConfig.numWorkerThreadsForBlockIO > 0) ? replicaConfig.numWorkerThreadsForBlockIO
                                                                          : std::thread::hardware_concurrency()) {
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
    replicaConfig_.get<uint16_t>("concord.bft.st.maxNumberOfChunksInBatch", 256),
#else
    replicaConfig_.get<uint32_t>("concord.bft.st.maxChunkSize", 2048),
    replicaConfig_.get<uint16_t>("concord.bft.st.maxNumberOfChunksInBatch", 32),
#endif
    replicaConfig_.get<uint32_t>("concord.bft.st.maxBlockSize", 30 * 1024 * 1024),
    replicaConfig_.get<uint32_t>("concord.bft.st.maxPendingDataFromSourceReplica", 256 * 1024 * 1024),
    replicaConfig_.getmaxNumOfReservedPages(),
    replicaConfig_.getsizeOfReservedPage(),
    replicaConfig_.get<uint32_t>("concord.bft.st.gettingMissingBlocksSummaryWindowSize", 600),
    replicaConfig_.get<uint32_t>("concord.bft.st.refreshTimerMs", 300),
    replicaConfig_.get<uint32_t>("concord.bft.st.checkpointSummariesRetransmissionTimeoutMs", 2500),
    replicaConfig_.get<uint32_t>("concord.bft.st.maxAcceptableMsgDelayMs", 60000),
    replicaConfig_.get<uint32_t>("concord.bft.st.sourceReplicaReplacementTimeoutMs", 0),
    replicaConfig_.get<uint32_t>("concord.bft.st.fetchRetransmissionTimeoutMs", 2000),
    replicaConfig_.get<uint32_t>("concord.bft.st.maxFetchRetransmissions", 2),
    replicaConfig_.get<uint32_t>("concord.bft.st.metricsDumpIntervalSec", 5),
    replicaConfig_.get("concord.bft.st.runInSeparateThread", replicaConfig_.isReadOnly),
    replicaConfig_.get("concord.bft.st.enableReservedPages", true),
    replicaConfig_.get("concord.bft.st.enableSourceBlocksPreFetch", true)
  };

#if !defined USE_COMM_PLAIN_TCP && !defined USE_COMM_TLS_TCP
  // maxChunkSize * maxNumberOfChunksInBatch shouldn't exceed UDP message size which is limited to 64KB
  if (stConfig.maxChunkSize * stConfig.maxNumberOfChunksInBatch > 64 * 1024) {
    LOG_WARN(logger, "overriding incorrect chunking configuration for UDP");
    stConfig.maxChunkSize = 2048;
    stConfig.maxNumberOfChunksInBatch = 32;
  }
#endif

  if (stConfig.gettingMissingBlocksSummaryWindowSize > 0 and stConfig.gettingMissingBlocksSummaryWindowSize < 100) {
    LOG_WARN(logger, "Overriding incorrect ST throughput measurement window size configuration to 100");
    stConfig.gettingMissingBlocksSummaryWindowSize = 100;
  }

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
  if (!replicaConfig.isReadOnly) {
    stReconfigurationSM_ = std::make_unique<concord::kvbc::StReconfigurationHandler>(*m_stateTransfer, *this);
  }
  // Instantiate IControlHandler.
  // If an application instantiation has already taken a place this will have no effect.
  bftEngine::IControlHandler::instance(new bftEngine::ControlHandler());
  bftEngine::ReconfigurationCmd::instance().setAggregator(aggregator);
}

Replica::~Replica() {
  if (m_replicaPtr) {
    if (m_replicaPtr->isRunning()) {
      m_replicaPtr->stop();
    }
  }
  if (creEngine_) creEngine_->stop();
}

/*
 * This method can't return false by current insertBlockInternal impl.
 * It is used only by State Transfer to synchronize state between replicas.
 */
bool Replica::putBlock(const uint64_t blockId, const char *blockData, const uint32_t blockSize, bool lastBlock) {
  if (replicaConfig_.isReadOnly) {
    return putBlockToObjectStore(blockId, blockData, blockSize, lastBlock);
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
    m_kvBlockchain->addRawBlock(rawBlock, blockId, lastBlock);
  }
  return true;
}

std::future<bool> Replica::putBlockAsync(uint64_t blockId,
                                         const char *block,
                                         const uint32_t blockSize,
                                         bool lastBlock) {
  static uint64_t callCounter = 0;
  static constexpr size_t snapshotThresh = 1000;

  auto future = blocksIOWorkersPool_.async(
      [this](uint64_t blockId, const char *block, const uint32_t blockSize, bool lastBlock) {
        auto start = std::chrono::steady_clock::now();
        bool result = false;

        LOG_TRACE(logger, "Job Started: " << KVLOG(blockId, blockSize));
        result = putBlock(blockId, block, blockSize, lastBlock);

        auto jobDuration =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
        LOG_TRACE(logger, "Job done: " << KVLOG(result, blockId, jobDuration));
        histograms_.put_block_duration->recordAtomic(jobDuration);
        return result;
      },
      std::forward<decltype(blockId)>(blockId),
      std::forward<decltype(block)>(block),
      std::forward<decltype(blockSize)>(blockSize),
      std::forward<decltype(lastBlock)>(lastBlock));

  if ((++callCounter % snapshotThresh) == 0) {
    auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    registrar.perf.snapshot("iappstate");
    LOG_INFO(logger, registrar.perf.toString(registrar.perf.get("iappstate")));
  }
  return future;
}

bool Replica::putBlockToObjectStore(const uint64_t blockId,
                                    const char *blockData,
                                    const uint32_t blockSize,
                                    bool lastBlock) {
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
    m_bcDbAdapter->addRawBlock(block, blockId, lastBlock);
  }

  return true;
}

uint64_t Replica::getLastReachableBlockNum() const {
  if (replicaConfig_.isReadOnly) {
    return m_bcDbAdapter->getLastReachableBlockId();
  }
  return m_kvBlockchain->getLastReachableBlockId();
}

uint64_t Replica::getGenesisBlockNum() const { return getGenesisBlockId(); }

uint64_t Replica::getLastBlockNum() const {
  if (replicaConfig_.isReadOnly) {
    return m_bcDbAdapter->getLatestBlockId();
  }
  const auto last = m_kvBlockchain->getLastStatetransferBlockId();
  if (last) {
    return *last;
  }
  return m_kvBlockchain->getLastReachableBlockId();
}

RawBlock Replica::getBlockInternal(BlockId blockId) const { return m_bcDbAdapter->getRawBlock(blockId); }

/*
 * This method assumes that *outBlock is big enough to hold block content
 * The caller is the owner of the memory
 */
bool Replica::getBlock(uint64_t blockId, char *outBlock, uint32_t outBlockMaxSize, uint32_t *outBlockActualSize) {
  if (replicaConfig_.isReadOnly) {
    return getBlockFromObjectStore(blockId, outBlock, outBlockMaxSize, outBlockActualSize);
  }
  const auto rawBlock = m_kvBlockchain->getRawBlock(blockId);
  if (!rawBlock) {
    throw NotFoundException{"Raw block not found: " + std::to_string(blockId)};
  }
  const auto &ser = categorization::RawBlock::serialize(*rawBlock);
  if (ser.size() > outBlockMaxSize) {
    LOG_ERROR(logger, KVLOG(ser.size(), outBlockMaxSize));
    throw std::runtime_error("not enough space to copy block!");
  }
  *outBlockActualSize = ser.size();
  LOG_DEBUG(logger, KVLOG(blockId, *outBlockActualSize));
  std::memcpy(outBlock, ser.data(), *outBlockActualSize);
  return true;
}

std::future<bool> Replica::getBlockAsync(uint64_t blockId,
                                         char *outBlock,
                                         uint32_t outBlockMaxSize,
                                         uint32_t *outBlockActualSize) {
  static uint64_t callCounter = 0;
  static constexpr size_t snapshotThresh = 1000;

  auto future = blocksIOWorkersPool_.async(
      [this](uint64_t blockId, char *outBlock, uint32_t outBlockMaxSize, uint32_t *outBlockActualSize) {
        bool result = false;
        auto start = std::chrono::steady_clock::now();
        *outBlockActualSize = 0;
        LOG_TRACE(logger, "Job Started: " << KVLOG(blockId));

        // getBlock will throw exception if block doesn't exist
        try {
          result = getBlock(blockId, outBlock, outBlockMaxSize, outBlockActualSize);
        } catch (NotFoundException &ex) {
          *outBlockActualSize = 0;
          LOG_ERROR(logger, "Block not found:" << KVLOG(blockId));
          return false;
        }
        if (result) ConcordAssertGT(*outBlockActualSize, 0);
        auto jobDuration =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
        LOG_TRACE(logger, "Job done: " << KVLOG(result, blockId, *outBlockActualSize, jobDuration));
        histograms_.get_block_duration->recordAtomic(jobDuration);
        return result;
      },
      std::forward<decltype(blockId)>(blockId),
      std::forward<decltype(outBlock)>(outBlock),
      std::forward<decltype(outBlockMaxSize)>(outBlockMaxSize),
      std::forward<decltype(outBlockActualSize)>(outBlockActualSize));

  if ((++callCounter % snapshotThresh) == 0) {
    auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
    registrar.perf.snapshot("iappstate");
    LOG_INFO(logger, registrar.perf.toString(registrar.perf.get("iappstate")));
  }
  return future;
}

bool Replica::getBlockFromObjectStore(uint64_t blockId,
                                      char *outBlock,
                                      uint32_t outblockMaxSize,
                                      uint32_t *outBlockSize) {
  try {
    RawBlock block = getBlockInternal(blockId);
    if (block.length() > outblockMaxSize) {
      LOG_ERROR(logger, KVLOG(block.length(), outblockMaxSize));
      throw std::runtime_error("not enough space to copy block!");
    }
    *outBlockSize = block.length();
    memcpy(outBlock, block.data(), block.length());
    return true;
  } catch (const NotFoundException &e) {
    LOG_FATAL(logger, e.what());
    throw;
  }
}

bool Replica::hasBlock(BlockId blockId) const {
  if (replicaConfig_.isReadOnly) {
    return m_bcDbAdapter->hasBlock(blockId);
  }
  return m_kvBlockchain->hasBlock(blockId);
}

bool Replica::getPrevDigestFromBlock(BlockId blockId, StateTransferDigest *outPrevBlockDigest) {
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

void Replica::getPrevDigestFromBlock(const char *blockData,
                                     const uint32_t blockSize,
                                     StateTransferDigest *outPrevBlockDigest) {
  ConcordAssertGT(blockSize, 0);
  auto view = std::string_view{blockData, blockSize};
  const auto rawBlock = categorization::RawBlock::deserialize(view);

  static_assert(rawBlock.data.parent_digest.size() == BLOCK_DIGEST_SIZE);
  static_assert(sizeof(StateTransferDigest) == BLOCK_DIGEST_SIZE);
  std::memcpy(outPrevBlockDigest, rawBlock.data.parent_digest.data(), BLOCK_DIGEST_SIZE);
}

bool Replica::getPrevDigestFromObjectStoreBlock(uint64_t blockId,
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
void Replica::registerStBasedReconfigurationHandler(
    std::shared_ptr<concord::client::reconfiguration::IStateHandler> handler) {
  // api for higher level application to register the handler
  if (handler && creEngine_) creEngine_->registerHandler(handler);
}
BlockId Replica::getLastKnownReconfigCmdBlockNum() const {
  std::string blockRawData;
  if (replicaConfig_.isReadOnly) {
    m_bcDbAdapter->getLastKnownReconfigurationCmdBlock(blockRawData);
    if (!blockRawData.empty()) {
      bftEngine::ReconfigurationCmd::ReconfigurationCmdData::cmdBlock cmdData;
      std::istringstream inStream;
      inStream.str(blockRawData);
      concord::serialize::Serializable::deserialize(inStream, cmdData);
      return cmdData.blockId_;
    } else {
      // write a genesis reconfiguration block
      bftEngine::ReconfigurationCmd::ReconfigurationCmdData::cmdBlock cmdblockGenesis = {0, 0, 0};
      std::ostringstream outStream;
      concord::serialize::Serializable::serialize(outStream, cmdblockGenesis);
      auto data = outStream.str();
      m_bcDbAdapter->setLastKnownReconfigurationCmdBlock(data);
    }
  }
  return 0;
}
void Replica::setLastKnownReconfigCmdBlock(const std::vector<uint8_t> &blockData) {
  if (replicaConfig_.isReadOnly) {
    std::string page(blockData.begin(), blockData.end());
    m_bcDbAdapter->setLastKnownReconfigurationCmdBlock(page);
  }
}
void Replica::startRoReplicaCreEngine() {
  concord::client::reconfiguration::Config cre_config;
  BlockId id = getLastKnownReconfigCmdBlockNum();
  creClient_.reset(new concord::client::reconfiguration::STBasedReconfigurationClient(
      [this](const std::vector<uint8_t> &blockData) { setLastKnownReconfigCmdBlock(blockData); }, id));
  cre_config.id_ = replicaConfig_.replicaId;
  cre_config.interval_timeout_ms_ = 1000;
  creEngine_.reset(
      new concord::client::reconfiguration::ClientReconfigurationEngine(cre_config, creClient_.get(), aggregator_));
  creEngine_->start();
}

}  // namespace concord::kvbc
