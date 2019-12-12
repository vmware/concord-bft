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

#include "Replica.hpp"
#include "ReplicaImp.hpp"
#include "DebugPersistentStorage.hpp"
#include "PersistentStorageImp.hpp"
#include "MsgsCommunicator.hpp"
#include "bftengine/ReplicaConfigSingleton.hpp"

#include <condition_variable>
#include <mutex>

namespace bftEngine {
namespace impl {

namespace {
bool cryptoInitialized = false;
std::mutex mutexForCryptoInitialization;
}  // namespace

struct ReplicaInternal : public Replica {
  ~ReplicaInternal() override;

  bool isRunning() const override;

  uint64_t getLastExecutedSequenceNum() const override;

  bool requestsExecutionWasInterrupted() const override;

  void start() override;

  void stop() override;

  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) override;

  void restartForDebug(uint32_t delayMillis) override;

  void stopWhenStateIsNotCollected() override;

  ReplicaImp *replica = nullptr;

 private:
  std::condition_variable debugWait;
  std::mutex debugWaitLock;
};

ReplicaInternal::~ReplicaInternal() { delete replica; }

bool ReplicaInternal::isRunning() const { return replica->isRunning(); }

uint64_t ReplicaInternal::getLastExecutedSequenceNum() const {
  return static_cast<uint64_t>(replica->getLastExecutedSequenceNum());
}

bool ReplicaInternal::requestsExecutionWasInterrupted() const {
  const bool run = replica->isRunning();
  const bool isRecovering = replica->isRecoveringFromExecutionOfRequests();
  return (!run && isRecovering);
}

void ReplicaInternal::start() { return replica->start(); }

void ReplicaInternal::stopWhenStateIsNotCollected() {
  if (replica->isRunning()) {
    replica->stopWhenStateIsNotCollected();
  }
}

void ReplicaInternal::stop() {
  unique_lock<std::mutex> lk(debugWaitLock);
  if (replica->isRunning()) {
    replica->stop();
  }

  debugWait.notify_all();
}

void ReplicaInternal::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) { return replica->SetAggregator(a); }

void ReplicaInternal::restartForDebug(uint32_t delayMillis) {
  {
    unique_lock<std::mutex> lk(debugWaitLock);
    replica->stopWhenStateIsNotCollected();
    if (delayMillis > 0) {
      std::cv_status res = debugWait.wait_for(lk, std::chrono::milliseconds(delayMillis));
      if (std::cv_status::no_timeout == res)  // stop() was called
        return;
    }
  }

  shared_ptr<PersistentStorage> persistentStorage(replica->getPersistentStorage());
  RequestsHandler *requestsHandler = replica->getRequestsHandler();
  IStateTransfer *stateTransfer = replica->getStateTransfer();
  shared_ptr<MsgsCommunicator> msgsComm = replica->getMsgsCommunicator();

  // delete rep; TODO(GG): enable after debugging and update ~ReplicaImp
  replica = nullptr;

  ReplicaLoader::ErrorCode loadErrCode;

  LoadedReplicaData ld = ReplicaLoader::loadReplica(persistentStorage, loadErrCode);

  Assert(loadErrCode == ReplicaLoader::ErrorCode::Success);

  replica = new ReplicaImp(ld, requestsHandler, stateTransfer, msgsComm, persistentStorage);
  replica->start();
}

}  // namespace impl

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
  ReplicaConfigSingleton::GetInstance(replicaConfig);

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
  shared_ptr<MsgsCommunicator> msgsCommunicatorPtr(new MsgsCommunicator(communication));
  if (isNewStorage) {
    replicaInternal->replica =
        new ReplicaImp(*replicaConfig, requestsHandler, stateTransfer, msgsCommunicatorPtr, persistentStoragePtr);
  } else {
    ReplicaLoader::ErrorCode loadErrCode;
    auto loadedReplicaData = ReplicaLoader::loadReplica(persistentStoragePtr, loadErrCode);
    if (loadErrCode != ReplicaLoader::ErrorCode::Success) {
      LOG_ERROR_F(GL, "Unable to load replica state from storage. Error %X", (uint32_t)loadErrCode);
      return nullptr;
    }
    // TODO(GG): compare ld.repConfig and replicaConfig
    replicaInternal->replica =
        new ReplicaImp(loadedReplicaData, requestsHandler, stateTransfer, msgsCommunicatorPtr, persistentStoragePtr);
  }

  return replicaInternal;
}

Replica::~Replica() {}

}  // namespace bftEngine
