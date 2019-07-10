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

#include <mutex>
#include <zconf.h>

namespace bftEngine {
namespace impl {

bool cryptoInitialized = false;
std::mutex mutexForCryptoInitialization;

struct ReplicaInternal : public Replica {
  virtual ~ReplicaInternal() override;

  virtual bool isRunning() const override;

  uint64_t getLastExecutedSequenceNum() const override;

  virtual bool requestsExecutionWasInterrupted() const override;

  virtual void start() override;

  virtual void stop() override;

  virtual void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) override;

  virtual void restartForDebug(uint32_t delayMillis) override;

  ReplicaImp *rep;
};

ReplicaInternal::~ReplicaInternal() {
  delete rep;
}

bool ReplicaInternal::isRunning() const {
  return rep->isRunning();
}

uint64_t ReplicaInternal::getLastExecutedSequenceNum() const {
  return static_cast<uint64_t>(rep->getLastExecutedSequenceNum());
}

bool ReplicaInternal::requestsExecutionWasInterrupted() const {
  const bool run = rep->isRunning();
  const bool isRecovering = rep->isRecoveringFromExecutionOfRequests();
  return (!run && isRecovering);
}

void ReplicaInternal::start() {
  return rep->start();
}

void ReplicaInternal::stop() {
  return rep->stop();
}

void ReplicaInternal::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) {
  return rep->SetAggregator(a);
}

void ReplicaInternal::restartForDebug(uint32_t delayMillis) {
  Assert(debugPersistentStorageEnabled);
  rep->stopWhenStateIsNotCollected();

  shared_ptr<PersistentStorage> persistentStorage(rep->getPersistentStorage());
  RequestsHandler *requestsHandler = rep->getRequestsHandler();
  IStateTransfer *stateTransfer = rep->getStateTransfer();
  ICommunication *comm = rep->getCommunication();

  // delete rep; TODO(GG): enable after debugging and update ~ReplicaImp
  rep = nullptr;

  ReplicaLoader::ErrorCode loadErrCode;

  LoadedReplicaData ld = ReplicaLoader::loadReplica(persistentStorage, loadErrCode);

  Assert(loadErrCode == ReplicaLoader::ErrorCode::Success);

  rep = new ReplicaImp(ld, requestsHandler, stateTransfer, comm, persistentStorage);

  if(delayMillis > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(delayMillis));
  }
  rep->start();
}
}
}

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

  if (!debugPersistentStorageEnabled) Assert(metadataStorage != nullptr);

  if (debugPersistentStorageEnabled)
    if (metadataStorage == nullptr)
      persistentStoragePtr.reset(new impl::DebugPersistentStorage(replicaConfig->fVal, replicaConfig->cVal));

  if (metadataStorage != nullptr) {
    persistentStoragePtr.reset(new impl::PersistentStorageImp(replicaConfig->fVal, replicaConfig->cVal));
    shared_ptr<MetadataStorage> metadataStoragePtr(metadataStorage);
    auto objectDescriptors =
        ((PersistentStorageImp *) persistentStoragePtr.get())->getDefaultMetadataObjectDescriptors(numOfObjects);
    isNewStorage = metadataStoragePtr->initMaxSizeOfObjects(objectDescriptors.get(), numOfObjects);
    ((PersistentStorageImp *) persistentStoragePtr.get())->init(metadataStoragePtr);
  }

  auto *replicaInternal = new ReplicaInternal();
  if (isNewStorage) {
    replicaInternal->rep =
        new ReplicaImp(*replicaConfig, requestsHandler, stateTransfer, communication, persistentStoragePtr);
  } else {
    ReplicaLoader::ErrorCode loadErrCode;
    auto loadedReplicaData = ReplicaLoader::loadReplica(persistentStoragePtr, loadErrCode);
    if (loadErrCode != ReplicaLoader::ErrorCode::Success) {
      LOG_ERROR_F(GL, "Unable to load replica state from storage. Error %X", (uint32_t) loadErrCode);
      return nullptr;
    }
    // TODO(GG): compare ld.repConfig and replicaConfig
    replicaInternal->rep = new ReplicaImp(loadedReplicaData, requestsHandler, stateTransfer,
                                          communication, persistentStoragePtr);
  }

  return replicaInternal;
}

Replica::~Replica() {}

}
