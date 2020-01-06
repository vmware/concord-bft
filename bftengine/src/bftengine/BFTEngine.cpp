// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
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
#include "DebugPersistentStorage.hpp"
#include "PersistentStorageImp.hpp"
#include "IncomingMsgsStorageImp.hpp"
#include "MsgsCommunicator.hpp"
#include "MsgReceiver.hpp"
#include "bftengine/ReplicaConfig.hpp"

#include <condition_variable>
#include <mutex>

namespace bftEngine::impl {

namespace {
bool cryptoInitialized = false;
std::mutex mutexForCryptoInitialization;
}  // namespace

class ReplicaInternal : public Replica {
 public:
  ~ReplicaInternal() override;

  bool isRunning() const override;

  uint64_t getLastExecutedSequenceNum() const override;

  bool requestsExecutionWasInterrupted() const override;

  void start() override;

  void stop() override;

  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) override;

  void restartForDebug(uint32_t delayMillis) override;

  ReplicaImp *replica_ = nullptr;

 private:
  std::condition_variable debugWait_;
  std::mutex debugWaitLock_;
};

ReplicaInternal::~ReplicaInternal() { delete replica_; }

bool ReplicaInternal::isRunning() const { return replica_->isRunning(); }

uint64_t ReplicaInternal::getLastExecutedSequenceNum() const {
  return static_cast<uint64_t>(replica_->getLastExecutedSequenceNum());
}

bool ReplicaInternal::requestsExecutionWasInterrupted() const {
  const bool run = replica_->isRunning();
  const bool isRecovering = replica_->isRecoveringFromExecutionOfRequests();
  return (!run && isRecovering);
}

void ReplicaInternal::start() { return replica_->start(); }

void ReplicaInternal::stop() {
  unique_lock<std::mutex> lk(debugWaitLock_);
  if (replica_->isRunning()) {
    replica_->stop();
  }

  debugWait_.notify_all();
}

void ReplicaInternal::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) {
  return replica_->SetAggregator(a);
}

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

  shared_ptr<PersistentStorage> persistentStorage(replica_->getPersistentStorage());
  RequestsHandler *requestsHandler = replica_->getRequestsHandler();
  IStateTransfer *stateTransfer = replica_->getStateTransfer();
  shared_ptr<MsgsCommunicator> msgsComm = replica_->getMsgsCommunicator();
  shared_ptr<MsgHandlersRegistrator> msgHandlersRegistrator = replica_->getMsgHandlersRegistrator();

  // delete rep; TODO(GG): enable after debugging and update ~ReplicaImp
  replica_ = nullptr;

  ReplicaLoader::ErrorCode loadErrCode;

  LoadedReplicaData ld = ReplicaLoader::loadReplica(persistentStorage, loadErrCode);

  Assert(loadErrCode == ReplicaLoader::ErrorCode::Success);

  replica_ = new ReplicaImp(ld, requestsHandler, stateTransfer, msgsComm, persistentStorage, msgHandlersRegistrator);
  replica_->start();
}

}  // namespace bftEngine::impl

namespace bftEngine {
Replica *Replica::createNewReplica(ReplicaConfig *replicaConfig,
                                   RequestsHandler *requestsHandler,
                                   IStateTransfer *stateTransfer,
                                   ICommunication *communication,
                                   MetadataStorage *metadataStorage) {
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

  // Initialize the configuration singleton here to use correct values during persistent storage initialization.
  replicaConfig->singletonFromThis();

  if (replicaConfig->debugPersistentStorageEnabled)
    if (metadataStorage == nullptr)
      persistentStoragePtr.reset(new impl::DebugPersistentStorage(replicaConfig->fVal, replicaConfig->cVal));

  // Testing/real metadataStorage passed.
  if (metadataStorage != nullptr) {
    persistentStoragePtr.reset(new impl::PersistentStorageImp(replicaConfig->fVal, replicaConfig->cVal));
    unique_ptr<MetadataStorage> metadataStoragePtr(metadataStorage);
    auto objectDescriptors =
        ((PersistentStorageImp *)persistentStoragePtr.get())->getDefaultMetadataObjectDescriptors(numOfObjects);
    isNewStorage = metadataStoragePtr->initMaxSizeOfObjects(objectDescriptors.get(), numOfObjects);
    ((PersistentStorageImp *)persistentStoragePtr.get())->init(move(metadataStoragePtr));
  }

  auto *replicaInternal = new ReplicaInternal();
  shared_ptr<MsgHandlersRegistrator> msgHandlersPtr(new MsgHandlersRegistrator());
  shared_ptr<IncomingMsgsStorage> incomingMsgsStoragePtr(new IncomingMsgsStorageImp(msgHandlersPtr, timersResolution));
  shared_ptr<IReceiver> msgReceiverPtr(new MsgReceiver(incomingMsgsStoragePtr));
  shared_ptr<MsgsCommunicator> msgsCommunicatorPtr(
      new MsgsCommunicator(communication, incomingMsgsStoragePtr, msgReceiverPtr));
  if (isNewStorage) {
    replicaInternal->replica_ = new ReplicaImp(
        *replicaConfig, requestsHandler, stateTransfer, msgsCommunicatorPtr, persistentStoragePtr, msgHandlersPtr);
  } else {
    ReplicaLoader::ErrorCode loadErrCode;
    auto loadedReplicaData = ReplicaLoader::loadReplica(persistentStoragePtr, loadErrCode);
    if (loadErrCode != ReplicaLoader::ErrorCode::Success) {
      LOG_ERROR_F(GL, "Unable to load replica state from storage. Error %X", (uint32_t)loadErrCode);
      return nullptr;
    }
    // TODO(GG): compare ld.repConfig and replicaConfig
    replicaInternal->replica_ = new ReplicaImp(
        loadedReplicaData, requestsHandler, stateTransfer, msgsCommunicatorPtr, persistentStoragePtr, msgHandlersPtr);
  }

  return replicaInternal;
}

Replica::~Replica() = default;

}  // namespace bftEngine
