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

namespace bftEngine {
namespace impl {

namespace {
bool cryptoInitialized = false;
std::mutex mutexForCryptoInitialization;
}  // namespace

struct ReplicaInternal : public Replica {
  virtual ~ReplicaInternal() override;

  virtual bool isRunning() const override;

  uint64_t getLastExecutedSequenceNum() const override;

  virtual bool requestsExecutionWasInterrupted() const override;

  virtual void start() override;

  virtual void stop() override;

  virtual void restartForDebug(uint32_t delayMillis) override;

  ReplicaImp *rep;

 private:
  std::condition_variable debugWait;
  std::mutex debugWaitLock;
};

ReplicaInternal::~ReplicaInternal() { delete rep; }

bool ReplicaInternal::isRunning() const { return rep->isRunning(); }

uint64_t ReplicaInternal::getLastExecutedSequenceNum() const {
  return static_cast<uint64_t>(rep->getLastExecutedSequenceNum());
}

bool ReplicaInternal::requestsExecutionWasInterrupted() const {
  const bool run = rep->isRunning();
  const bool isRecovering = rep->isRecoveringFromExecutionOfRequests();
  return (!run && isRecovering);
}

void ReplicaInternal::start() { return rep->start(); }

void ReplicaInternal::stop() {
  unique_lock<std::mutex> lk(debugWaitLock);
  if (rep->isRunning()) {
    rep->stop();
  }

  debugWait.notify_all();
}

void ReplicaInternal::restartForDebug(uint32_t delayMillis) {
  {
    unique_lock<std::mutex> lk(debugWaitLock);
    rep->stop();
    if (delayMillis > 0) {
      std::cv_status res = debugWait.wait_for(lk, std::chrono::milliseconds(delayMillis));
      if (std::cv_status::no_timeout == res)  // stop() was called
        return;
    }
  }

  shared_ptr<PersistentStorage> persistentStorage(rep->getPersistentStorage());
  RequestsHandler *requestsHandler = rep->getRequestsHandler();
  IStateTransfer *stateTransfer = rep->getStateTransfer();
  shared_ptr<MsgsCommunicator> msgsComm = rep->getMsgsCommunicator();
  shared_ptr<MsgHandlersRegistrator> msgHandlersRegistrator = rep->getMsgHandlersRegistrator();

  // delete rep; TODO(GG): enable after debugging and update ~ReplicaImp
  rep = nullptr;

  ReplicaLoader::ErrorCode loadErrCode;

  LoadedReplicaData ld = ReplicaLoader::loadReplica(persistentStorage, loadErrCode);

  Assert(loadErrCode == ReplicaLoader::ErrorCode::Success);

  rep = new ReplicaImp(ld, requestsHandler, stateTransfer, msgsComm, persistentStorage, msgHandlersRegistrator);
  rep->start();
}
}  // namespace impl
}  // namespace bftEngine

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
    replicaInternal->rep = new ReplicaImp(
        *replicaConfig, requestsHandler, stateTransfer, msgsCommunicatorPtr, persistentStoragePtr, msgHandlersPtr);
  } else {
    ReplicaLoader::ErrorCode loadErrCode;
    auto loadedReplicaData = ReplicaLoader::loadReplica(persistentStoragePtr, loadErrCode);
    if (loadErrCode != ReplicaLoader::ErrorCode::Success) {
      LOG_ERROR_F(GL, "Unable to load replica state from storage. Error %X", (uint32_t)loadErrCode);
      return nullptr;
    }
    // TODO(GG): compare ld.repConfig and replicaConfig
    replicaInternal->rep = new ReplicaImp(
        loadedReplicaData, requestsHandler, stateTransfer, msgsCommunicatorPtr, persistentStoragePtr, msgHandlersPtr);
  }

  return replicaInternal;
}

Replica::~Replica() = default;

}  // namespace bftEngine
