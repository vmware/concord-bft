// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these sub-components is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "Replica.hpp"
#include "ReplicaImp.hpp"
#include "ReadOnlyReplica.hpp"
#include "ReplicaLoader.hpp"
#include "DebugPersistentStorage.hpp"
#include "PersistentStorageImp.hpp"
#include "IncomingMsgsStorageImp.hpp"
#include "MsgsCommunicator.hpp"
#include "PreProcessor.hpp"
#include "MsgReceiver.hpp"
#include "RequestHandler.h"
#include "ReservedPagesClient.hpp"
#include <condition_variable>
#include <memory>
#include <mutex>

bftEngine::IReservedPages *bftEngine::ReservedPagesClientBase::res_pages_ = nullptr;

namespace bftEngine::impl {

namespace {
bool cryptoInitialized = false;
std::mutex mutexForCryptoInitialization;
}  // namespace

class ReplicaInternal : public IReplica {
  friend class IReplica;

 public:
  bool isRunning() const override;

  int64_t getLastExecutedSequenceNum() const override { return replica_->getLastExecutedSequenceNum(); }

  void start() override;

  void stop() override;

  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) override;

  void restartForDebug(uint32_t delayMillis) override;

 private:
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
    auto pm = make_shared<concord::performance::PerformanceManager>();
    replica_.reset(new ReplicaImp(ld,
                                  replicaImp->getRequestsHandler(),
                                  replicaImp->getStateTransfer(),
                                  replicaImp->getMsgsCommunicator(),
                                  persistentStorage,
                                  replicaImp->getMsgHandlersRegistrator(),
                                  replicaImp->timers(),
                                  pm,
                                  replicaImp->getSecretsManager()));

  } else {
    //  TODO [TK] rep.reset(new ReadOnlyReplicaImp());
  }
  replica_->start();
}

}  // namespace bftEngine::impl

namespace bftEngine {

IReplica::IReplicaPtr IReplica::createNewReplica(const ReplicaConfig &replicaConfig,
                                                 shared_ptr<IRequestsHandler> requestsHandler,
                                                 IStateTransfer *stateTransfer,
                                                 bft::communication::ICommunication *communication,
                                                 MetadataStorage *metadataStorage,
                                                 std::shared_ptr<concord::performance::PerformanceManager> pm,
                                                 const shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm) {
  {
    std::lock_guard<std::mutex> lock(mutexForCryptoInitialization);
    if (!cryptoInitialized) {
      cryptoInitialized = true;
      CryptographyWrapper::init();
    }
  }

  shared_ptr<PersistentStorage> persistentStoragePtr;
  if (replicaConfig.debugPersistentStorageEnabled)
    if (metadataStorage == nullptr)
      persistentStoragePtr.reset(new impl::DebugPersistentStorage(replicaConfig.fVal, replicaConfig.cVal));

  // Testing/real metadataStorage passed.
  uint16_t numOfObjects = 0;
  bool isNewStorage = true;
  if (metadataStorage != nullptr) {
    persistentStoragePtr.reset(new impl::PersistentStorageImp(replicaConfig.fVal, replicaConfig.cVal));
    unique_ptr<MetadataStorage> metadataStoragePtr(metadataStorage);
    auto objectDescriptors =
        ((PersistentStorageImp *)persistentStoragePtr.get())->getDefaultMetadataObjectDescriptors(numOfObjects);
    isNewStorage = metadataStoragePtr->initMaxSizeOfObjects(objectDescriptors.get(), numOfObjects);
    bool erasedMetaData;
    ((PersistentStorageImp *)persistentStoragePtr.get())->init(move(metadataStoragePtr), erasedMetaData);
    if (erasedMetaData) isNewStorage = true;
  }
  auto replicaInternal = std::make_unique<ReplicaInternal>();
  shared_ptr<MsgHandlersRegistrator> msgHandlersPtr(new MsgHandlersRegistrator());
  auto incomingMsgsStorageImpPtr =
      std::make_unique<IncomingMsgsStorageImp>(msgHandlersPtr, timersResolution, replicaConfig.replicaId);
  auto &timers = incomingMsgsStorageImpPtr->timers();
  shared_ptr<IncomingMsgsStorage> incomingMsgsStoragePtr{std::move(incomingMsgsStorageImpPtr)};
  shared_ptr<bft::communication::IReceiver> msgReceiverPtr(new MsgReceiver(incomingMsgsStoragePtr));
  shared_ptr<MsgsCommunicator> msgsCommunicatorPtr(
      new MsgsCommunicator(communication, incomingMsgsStoragePtr, msgReceiverPtr));
  if (isNewStorage) {
    replicaInternal->replica_.reset(new ReplicaImp(replicaConfig,
                                                   requestsHandler,
                                                   stateTransfer,
                                                   msgsCommunicatorPtr,
                                                   persistentStoragePtr,
                                                   msgHandlersPtr,
                                                   timers,
                                                   pm,
                                                   sm));
  } else {
    ReplicaLoader::ErrorCode loadErrCode;
    auto loadedReplicaData = ReplicaLoader::loadReplica(persistentStoragePtr, loadErrCode);
    if (loadErrCode != ReplicaLoader::ErrorCode::Success) {
      LOG_ERROR(GL, "Unable to load replica state from storage. Error " << (uint32_t)loadErrCode);
      return nullptr;
    }
    // TODO(GG): compare ld.repConfig and replicaConfig
    replicaInternal->replica_.reset(new ReplicaImp(loadedReplicaData,
                                                   requestsHandler,
                                                   stateTransfer,
                                                   msgsCommunicatorPtr,
                                                   persistentStoragePtr,
                                                   msgHandlersPtr,
                                                   timers,
                                                   pm,
                                                   sm));
  }
  preprocessor::PreProcessor::addNewPreProcessor(msgsCommunicatorPtr,
                                                 incomingMsgsStoragePtr,
                                                 msgHandlersPtr,
                                                 *requestsHandler,
                                                 *dynamic_cast<InternalReplicaApi *>(replicaInternal->replica_.get()),
                                                 timers,
                                                 pm);
  return replicaInternal;
}

IReplica::IReplicaPtr IReplica::createNewRoReplica(const ReplicaConfig &replicaConfig,
                                                   std::shared_ptr<IRequestsHandler> requestsHandler,
                                                   IStateTransfer *stateTransfer,
                                                   bft::communication::ICommunication *communication) {
  {
    std::lock_guard<std::mutex> lock(mutexForCryptoInitialization);
    if (!cryptoInitialized) {
      cryptoInitialized = true;
      CryptographyWrapper::init();
    }
  }

  auto replicaInternal = std::make_unique<ReplicaInternal>();
  auto msgHandlers = std::make_shared<MsgHandlersRegistrator>();
  auto incomingMsgsStorageImpPtr =
      std::make_unique<IncomingMsgsStorageImp>(msgHandlers, timersResolution, replicaConfig.replicaId);
  auto &timers = incomingMsgsStorageImpPtr->timers();
  std::shared_ptr<IncomingMsgsStorage> incomingMsgsStorage{std::move(incomingMsgsStorageImpPtr)};
  auto msgReceiver = std::make_shared<MsgReceiver>(incomingMsgsStorage);
  auto msgsCommunicator = std::make_shared<MsgsCommunicator>(communication, incomingMsgsStorage, msgReceiver);

  replicaInternal->replica_ = std::make_unique<ReadOnlyReplica>(
      replicaConfig, requestsHandler, stateTransfer, msgsCommunicator, msgHandlers, timers);
  return replicaInternal;
}

std::shared_ptr<IRequestsHandler> IRequestsHandler::createRequestsHandler(
    std::shared_ptr<IRequestsHandler> userReqHandler) {
  auto reqHandler = new bftEngine::RequestHandler();
  reqHandler->setUserRequestHandler(userReqHandler);
  return std::shared_ptr<IRequestsHandler>(reqHandler);
}

}  // namespace bftEngine
