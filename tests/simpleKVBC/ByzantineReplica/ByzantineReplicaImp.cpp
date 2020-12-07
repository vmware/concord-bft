// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "bftengine/DebugPersistentStorage.hpp"
#include "bftengine/MsgHandlersRegistrator.hpp"
#include "bftengine/IncomingMsgsStorageImp.hpp"
#include "bftengine/IStateTransfer.hpp"
#include "bftengine/MsgReceiver.hpp"
#include "bftengine/Replica.hpp"
#include "bftengine/PersistentStorageImp.hpp"
#include "bftengine/ReplicaLoader.hpp"
#include "preprocessor/PreProcessor.hpp"
#include "ReplicaImp.hpp"
#include "ReplicaImp.h"

#include "ByzantineReplicaImp.hpp"

using namespace bftEngine;
using namespace bftEngine::impl;
using namespace preprocessor;
using bft::communication::ICommunication;
using concord::kvbc::IStorageFactory;

namespace bftEngine::impl {

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

/**
 * Private state duplicated from BFTEngine.cpp.
 */

bool cryptoInitialized = false;
std::mutex mutexForCryptoInitialization;
}  // namespace

namespace byzantine {

/**
 * Local copy of BFTEngine.cpp's ReplicaInternal private class. If that was accessible or reusable somehow, this
 * definition here shouldn't be needed.
 */

class ReplicaInternal : public IReplica {
  friend class IReplica;

 public:
  bool isRunning() const override;

  int64_t getLastExecutedSequenceNum() const override { return replica_->getLastExecutedSequenceNum(); }

  void start() override;

  void stop() override;

  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) override;

  void restartForDebug(uint32_t delayMillis) override;

  void setControlStateManager(std::shared_ptr<bftEngine::ControlStateManager> controlStateManager) override {
    replica_->setControlStateManager(controlStateManager);
  }

 public:
  std::unique_ptr<ReplicaBase> replica_;
  std::condition_variable debugWait_;
  std::mutex debugWaitLock_;
};

bool ReplicaInternal::isRunning() const { return replica_->isRunning(); }

void ReplicaInternal::start() {
  replica_->start();
  preprocessor::PreProcessor::setAggregator(replica_->getAggregator());
}

void ReplicaInternal::stop() {
  unique_lock<std::mutex> lk(debugWaitLock_);
  if (replica_->isRunning()) {
    replica_->stop();
  }

  debugWait_.notify_all();
}

void ReplicaInternal::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) { replica_->SetAggregator(a); }

void ReplicaInternal::restartForDebug(uint32_t delayMillis) {
  {
    unique_lock<std::mutex> lk(debugWaitLock_);
    replica_->stop();
    if (delayMillis > 0) {
      std::cv_status res = debugWait_.wait_for(lk, std::chrono::milliseconds(delayMillis));
      if (std::cv_status::no_timeout == res)  // stop() was called
        return;
    }
  }

  if (!replica_->isReadOnly()) {
    ReplicaImp *replicaImp = dynamic_cast<ReplicaImp *>(replica_.get());

    shared_ptr<PersistentStorage> persistentStorage(replicaImp->getPersistentStorage());
    ReplicaLoader::ErrorCode loadErrCode;
    LoadedReplicaData ld = ReplicaLoader::loadReplica(persistentStorage, loadErrCode);
    ConcordAssert(loadErrCode == ReplicaLoader::ErrorCode::Success);

    replica_.reset(new concord::kvbc::test::ByzantineBftEngineReplicaImp(ld,
                                                                         replicaImp->getRequestsHandler(),
                                                                         replicaImp->getStateTransfer(),
                                                                         replicaImp->getMsgsCommunicator(),
                                                                         persistentStorage,
                                                                         replicaImp->getMsgHandlersRegistrator(),
                                                                         replicaImp->timers()));

  } else {
    //  TODO [TK] rep.reset(new ReadOnlyReplicaImp());
  }
  replica_->start();
}
}  // namespace byzantine
}  // namespace bftEngine::impl

namespace concord::kvbc::test {

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Byzantine replica extending bftEngine::impl::ReplicaImp
 *
 * These default constructors could do more than just calling the parent class constructor. If the Byzantine behavior is
 * not injected via the constructor arguments (e.g. custom MsgsCommunicator and/or MsgHandlersRegistrator), the
 * constructors can inject the Byzantine components here by themselves.
 */

ByzantineBftEngineReplicaImp::ByzantineBftEngineReplicaImp(const ReplicaConfig &config,
                                                           IRequestsHandler *requestsHandler,
                                                           IStateTransfer *stateTrans,
                                                           shared_ptr<MsgsCommunicator> msgsCommunicator,
                                                           shared_ptr<PersistentStorage> persistentStorage,
                                                           shared_ptr<MsgHandlersRegistrator> msgHandlers,
                                                           concordUtil::Timers &timers)
    : bftEngine::impl::ReplicaImp(
          config, requestsHandler, stateTrans, msgsCommunicator, persistentStorage, msgHandlers, timers) {
  LOG_DEBUG(GL, "ByzantineBftEngineReplicaImp constructor!");
}

ByzantineBftEngineReplicaImp::ByzantineBftEngineReplicaImp(const LoadedReplicaData &ld,
                                                           IRequestsHandler *requestsHandler,
                                                           IStateTransfer *stateTrans,
                                                           shared_ptr<MsgsCommunicator> msgsCommunicator,
                                                           shared_ptr<PersistentStorage> persistentStorage,
                                                           shared_ptr<MsgHandlersRegistrator> msgHandlers,
                                                           concordUtil::Timers &timers)
    : bftEngine::impl::ReplicaImp(
          ld, requestsHandler, stateTrans, msgsCommunicator, persistentStorage, msgHandlers, timers) {
  LOG_DEBUG(GL, "ByzantineBftEngineReplicaImp constructor!");
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Byzantine replica extending concord::kvbc::ReplicaImp
 *
 * With an extra refactoring of the original concord::kvbc::ReplicaImp implementation, many of the code duplicated here
 * shouldn't be required.
 */

ByzantineKvbcReplicaImp::ByzantineKvbcReplicaImp(ICommunication *comm,
                                                 const bftEngine::ReplicaConfig &replicaConfig,
                                                 std::unique_ptr<IStorageFactory> storageFactory,
                                                 std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : concord::kvbc::ReplicaImp(comm, replicaConfig, std::move(storageFactory), aggregator) {
  LOG_DEBUG(GL, "ByzantineKvbcReplicaImp constructor!");
}

/**
 * Custom implementation to invoke the createReplicaAndSyncState function.
 */
Status ByzantineKvbcReplicaImp::start() {
  LOG_INFO(logger, "ByzantineKvbcReplicaImp::Start() id = " << replicaConfig_.replicaId);

  if (m_currentRepStatus != RepStatus::Idle) {
    return Status::IllegalOperation("todo");
  }

  m_currentRepStatus = RepStatus::Starting;

  if (replicaConfig_.isReadOnly) {
    LOG_INFO(logger, "ReadOnly mode");
    m_replicaPtr = bftEngine::IReplica::createNewRoReplica(replicaConfig_, m_stateTransfer, m_ptrComm);
  } else {
    createReplicaAndSyncState();
  }
  concord::kvbc::ReplicaImp::m_replicaPtr->setControlStateManager(controlStateManager_);
  m_replicaPtr->SetAggregator(aggregator_);
  m_replicaPtr->start();
  m_currentRepStatus = RepStatus::Running;

  /// TODO(IG, GG)
  /// add return value to start/stop

  return Status::OK();
}

/**
 * Custom implementation to invoke the createNewReplica function.
 */
void ByzantineKvbcReplicaImp::createReplicaAndSyncState() {
  bool isNewStorage = m_metadataStorage->isNewStorage();
  bool erasedMetaData;
  m_replicaPtr = concord::kvbc::test::createNewReplica(
      replicaConfig_, m_cmdHandler, m_stateTransfer, m_ptrComm, m_metadataStorage, erasedMetaData);
  if (erasedMetaData) isNewStorage = true;
  LOG_INFO(logger, "ByzantineKvbcReplicaImp::createReplicaAndSyncState: isNewStorage= " << isNewStorage);
  if (!isNewStorage && !m_stateTransfer->isCollectingState()) {
    uint64_t removedBlocksNum = replicaStateSync_->execute(
        logger, *m_bcDbAdapter, getLastReachableBlockNum(), m_replicaPtr->getLastExecutedSequenceNum());
    LOG_INFO(logger,
             "ByzantineKvbcReplicaImp::createReplicaAndSyncState: removedBlocksNum = "
                 << removedBlocksNum << ", new m_lastBlock = " << getLastBlockNum()
                 << ", new m_lastReachableBlock = " << getLastReachableBlockNum());
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Custom implementation to inject the ByzantineBftEngineReplicaImp instance.
 */
bftEngine::IReplica::IReplicaPtr createNewReplica(const ReplicaConfig &replicaConfig,
                                                  IRequestsHandler *requestsHandler,
                                                  IStateTransfer *stateTransfer,
                                                  bft::communication::ICommunication *communication,
                                                  MetadataStorage *metadataStorage,
                                                  bool &erasedMetadata) {
  erasedMetadata = false;
  {
    std::lock_guard<std::mutex> lock(mutexForCryptoInitialization);
    if (!cryptoInitialized) {
      cryptoInitialized = true;
      CryptographyWrapper::init();
    }
  }

  shared_ptr<PersistentStorage> persistentStoragePtr;
  uint16_t numOfObjects = 0;
  bool isNewStorage = true;

  if (replicaConfig.debugPersistentStorageEnabled)
    if (metadataStorage == nullptr)
      persistentStoragePtr.reset(new DebugPersistentStorage(replicaConfig.fVal, replicaConfig.cVal));

  // Testing/real metadataStorage passed.
  if (metadataStorage != nullptr) {
    persistentStoragePtr.reset(new PersistentStorageImp(replicaConfig.fVal, replicaConfig.cVal));
    unique_ptr<MetadataStorage> metadataStoragePtr(metadataStorage);
    auto objectDescriptors =
        ((PersistentStorageImp *)persistentStoragePtr.get())->getDefaultMetadataObjectDescriptors(numOfObjects);
    isNewStorage = metadataStoragePtr->initMaxSizeOfObjects(objectDescriptors.get(), numOfObjects);
    bool erasedMetaData;
    ((PersistentStorageImp *)persistentStoragePtr.get())->init(move(metadataStoragePtr), erasedMetaData);
    if (erasedMetaData) {
      isNewStorage = true;
      erasedMetadata = true;
    }
  }
  auto replicaInternal = std::make_unique<bftEngine::impl::byzantine::ReplicaInternal>();
  shared_ptr<MsgHandlersRegistrator> msgHandlersPtr(new MsgHandlersRegistrator());
  auto incomingMsgsStorageImpPtr =
      std::make_unique<IncomingMsgsStorageImp>(msgHandlersPtr, timersResolution, replicaConfig.replicaId);
  auto &timers = incomingMsgsStorageImpPtr->timers();
  shared_ptr<IncomingMsgsStorage> incomingMsgsStoragePtr{std::move(incomingMsgsStorageImpPtr)};

  /**
   * A custom implementation for MsgsCommunicator and/or MsgReceiver could be used here to inject different Byzantine
   * behavior regardging incoming and outgoing messages.
   */

  shared_ptr<bft::communication::IReceiver> msgReceiverPtr(new MsgReceiver(incomingMsgsStoragePtr));
  shared_ptr<MsgsCommunicator> msgsCommunicatorPtr(
      new MsgsCommunicator(communication, incomingMsgsStoragePtr, msgReceiverPtr));

  if (isNewStorage) {
    replicaInternal->replica_.reset(new ByzantineBftEngineReplicaImp(replicaConfig,
                                                                     requestsHandler,
                                                                     stateTransfer,
                                                                     msgsCommunicatorPtr,
                                                                     persistentStoragePtr,
                                                                     msgHandlersPtr,
                                                                     timers));
  } else {
    ReplicaLoader::ErrorCode loadErrCode;
    auto loadedReplicaData = ReplicaLoader::loadReplica(persistentStoragePtr, loadErrCode);
    if (loadErrCode != ReplicaLoader::ErrorCode::Success) {
      LOG_ERROR(GL, "Unable to load replica state from storage. Error " << (uint32_t)loadErrCode);
      return nullptr;
    }
    // TODO(GG): compare ld.repConfig and replicaConfig
    replicaInternal->replica_.reset(new ByzantineBftEngineReplicaImp(loadedReplicaData,
                                                                     requestsHandler,
                                                                     stateTransfer,
                                                                     msgsCommunicatorPtr,
                                                                     persistentStoragePtr,
                                                                     msgHandlersPtr,
                                                                     timers));
  }
  PreProcessor::addNewPreProcessor(msgsCommunicatorPtr,
                                   incomingMsgsStoragePtr,
                                   msgHandlersPtr,
                                   *requestsHandler,
                                   *dynamic_cast<InternalReplicaApi *>(replicaInternal->replica_.get()),
                                   timers);
  return replicaInternal;
}
};  // namespace concord::kvbc::test