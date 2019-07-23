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

#include <mutex>

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

  virtual void restartForDebug() override;

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

void ReplicaInternal::restartForDebug() {
  Assert(debugPersistentStorageEnabled);
  rep->stopWhenStateIsNotCollected();

  PersistentStorage *persistentStorage = rep->getPersistentStorage();
  RequestsHandler *requestsHandler = rep->getRequestsHandler();
  IStateTransfer *stateTransfer = rep->getStateTransfer();
  ICommunication *comm = rep->getCommunication();

  // delete rep; TODO(GG): enable after debugging and update ~ReplicaImp
  rep = nullptr;

  ReplicaLoader::ErrorCode loadErrCode;

  LoadedReplicaData ld = ReplicaLoader::loadReplica(persistentStorage, loadErrCode);

  Assert(loadErrCode == ReplicaLoader::ErrorCode::Success);

  rep = new ReplicaImp(ld, requestsHandler, stateTransfer, comm, persistentStorage);

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

  bool isNewReplica = true;

  PersistentStorage *ps = nullptr;

  if (debugPersistentStorageEnabled) {
    Assert(metadataStorage == nullptr);
    ps = new impl::DebugPersistentStorage(replicaConfig->fVal, replicaConfig->cVal);
  } else if (metadataStorage != nullptr) {
    // TODO(GG):
    // - use metadataStorage to create an instance of PersistentStorage
    // - if needed, write false to isNewReplica
    Assert(false);
  }

  ReplicaInternal *retVal = nullptr;
  if (isNewReplica) {
    retVal = new ReplicaInternal();
    retVal->rep = new ReplicaImp(*replicaConfig, requestsHandler, stateTransfer, communication, ps);
  } else {
    LoadedReplicaData ld;
    ReplicaLoader::ErrorCode loadErrCode;
    ld = ReplicaLoader::loadReplica(ps, loadErrCode);
    if (loadErrCode != ReplicaLoader::ErrorCode::Success) {
      LOG_ERROR_F(GL, "Unable to load replica state from storage. Error %X", (uint32_t) loadErrCode);
      return nullptr;
    }
    retVal = new ReplicaInternal();

    // TODO(GG): compare ld.repConfig and replicaConfig

    retVal->rep = new ReplicaImp(ld, requestsHandler, stateTransfer, communication, ps);
  }

  return retVal;
}

Replica::~Replica() {}

}
