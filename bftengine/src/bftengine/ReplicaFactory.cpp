// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ReplicaFactory.hpp"
#include "DebugPersistentStorage.hpp"
#include "DbCheckpointManager.hpp"
#include "IncomingMsgsStorageImp.hpp"
#include "MsgReceiver.hpp"
#include "PreProcessor.hpp"
#include <util/filesystem.hpp>
#include <ccron/ticks_generator.hpp>

bftEngine::IReservedPages *bftEngine::ReservedPagesClientBase::res_pages_ = nullptr;
namespace bftEngine {

logging::Logger ReplicaFactory::logger_ = logging::getLogger("concord.replica_factory");
namespace impl {

class ReplicaInternal : public IReplica {
 public:
  ReplicaInternal(std::unique_ptr<IExternalObject> preprocessor = nullptr,
                  const std::shared_ptr<concord::cron::TicksGenerator> &ticks_gen = nullptr,
                  const std::shared_ptr<PersistentStorage> &persistent_storage = nullptr,
                  const std::shared_ptr<IInternalBFTClient> &internal_client = nullptr)
      : preprocessor_{std::move(preprocessor)},
        ticks_gen_{ticks_gen},
        persistent_storage_{persistent_storage},
        internal_client_{internal_client} {}

  bool isRunning() const override;
  int64_t getLastExecutedSequenceNum() const override { return replica_->getLastExecutedSequenceNum(); }
  void start() override;
  void stop() override;
  void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) override;
  void restartForDebug(uint32_t delayMillisec) override;
  std::shared_ptr<concord::cron::TicksGenerator> ticksGenerator() const override { return ticks_gen_; }
  std::shared_ptr<PersistentStorage> persistentStorage() const override { return persistent_storage_; }
  std::shared_ptr<IInternalBFTClient> internalClient() const override { return internal_client_; }
  std::shared_ptr<MsgsCommunicator> getMsgsCommunicator() const override { return replica_->getMsgsCommunicator(); }
  std::shared_ptr<MsgHandlersRegistrator> getMsgHandlersRegistrator() const override {
    return replica_->getMsgHandlersRegistrator();
  }
  concordUtil::Timers *getTimers() override { return replica_->getTimers(); }
  void setReplica(std::unique_ptr<ReplicaBase> &&replica) { replica_ = std::move(replica); }

 private:
  std::unique_ptr<IExternalObject> preprocessor_;
  std::unique_ptr<ReplicaBase> replica_;
  std::condition_variable debug_wait_;
  std::mutex debug_wait_lock_;
  std::shared_ptr<concord::cron::TicksGenerator> ticks_gen_;
  std::shared_ptr<PersistentStorage> persistent_storage_;
  std::shared_ptr<IInternalBFTClient> internal_client_;
};

bool ReplicaInternal::isRunning() const { return replica_->isRunning(); }

void ReplicaInternal::start() {
  if (preprocessor_ && replica_->getAggregator()) {
    preprocessor_->setAggregator(replica_->getAggregator());
  }
  replica_->start();
}

void ReplicaInternal::stop() {
  LOG_TRACE(GL, "ReplicaInternal::stop started");
  unique_lock<std::mutex> lk(debug_wait_lock_);
  if (replica_->isRunning()) {
    replica_->stop();
  }

  debug_wait_.notify_all();
  LOG_TRACE(GL, "ReplicaInternal::stop done");
}

void ReplicaInternal::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) { replica_->SetAggregator(a); }

void ReplicaInternal::restartForDebug(uint32_t delayMillisec) {
  {
    unique_lock<std::mutex> lk(debug_wait_lock_);
    replica_->stop();
    if (delayMillisec > 0) {
      std::cv_status res = debug_wait_.wait_for(lk, std::chrono::milliseconds(delayMillisec));
      if (std::cv_status::no_timeout == res)  // stop() was called
        return;
    }
  }

  if (!replica_->isReadOnly()) {
    auto replicaImp = dynamic_cast<ReplicaImp *>(replica_.get());

    shared_ptr<PersistentStorage> persistentStorage(replicaImp->getPersistentStorage());
    ReplicaLoader::ErrorCode loadErrCode;
    auto ld = ReplicaLoader::loadReplica(persistentStorage, loadErrCode);
    ConcordAssert(loadErrCode == ReplicaLoader::ErrorCode::Success);
    auto pm = make_shared<concord::performance::PerformanceManager>();
    replica_ = make_unique<ReplicaImp>(ld,
                                       replicaImp->getRequestsHandler(),
                                       replicaImp->getStateTransfer(),
                                       replicaImp->getMsgsCommunicator(),
                                       persistentStorage,
                                       replicaImp->getMsgHandlersRegistrator(),
                                       replicaImp->timers(),
                                       pm,
                                       replicaImp->getSecretsManager());

  } else {
    //  TODO [TK] rep.reset(new ReadOnlyReplicaImp());
  }
  replica_->start();
}

}  // namespace impl

ReplicaFactory::IReplicaPtr ReplicaFactory::createReplica(
    const ReplicaConfig &replicaConfig,
    shared_ptr<IRequestsHandler> requestsHandler,
    IStateTransfer *stateTransfer,
    bft::communication::ICommunication *communication,
    MetadataStorage *metadataStorage,
    std::shared_ptr<concord::performance::PerformanceManager> pm,
    const shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm) {
  shared_ptr<PersistentStorage> persistentStoragePtr;
  if (replicaConfig.debugPersistentStorageEnabled)
    if (metadataStorage == nullptr)
      persistentStoragePtr = std::make_unique<impl::DebugPersistentStorage>(replicaConfig.fVal, replicaConfig.cVal);

  // Testing/real metadataStorage passed.
  uint16_t numOfObjects = 0;
  bool isNewStorage = true;
  if (metadataStorage != nullptr) {
    persistentStoragePtr.reset(new impl::PersistentStorageImp(
        replicaConfig.numReplicas,
        replicaConfig.fVal,
        replicaConfig.cVal,
        replicaConfig.numReplicas + replicaConfig.numRoReplicas + replicaConfig.numOfClientProxies +
            replicaConfig.numOfExternalClients + replicaConfig.numOfClientServices + replicaConfig.numReplicas,
        replicaConfig.clientBatchingMaxMsgsNbr));
    unique_ptr<MetadataStorage> metadataStoragePtr(metadataStorage);
    auto objectDescriptors = static_cast<PersistentStorageImp *>(persistentStoragePtr.get())
                                 ->getDefaultMetadataObjectDescriptors(numOfObjects);
    isNewStorage = metadataStoragePtr->initMaxSizeOfObjects(objectDescriptors, numOfObjects);
    // Check if we need to remove the metadata or start a new epoch
    bool erasedMetaData = false;
    uint32_t actualObjectSize = 0;
    metadataStoragePtr->read(ConstMetadataParameterIds::ERASE_METADATA_ON_STARTUP,
                             sizeof(erasedMetaData),
                             (char *)&erasedMetaData,
                             actualObjectSize);
    bool startNewEpoch = false;
    metadataStoragePtr->read(
        ConstMetadataParameterIds::START_NEW_EPOCH, sizeof(startNewEpoch), (char *)&startNewEpoch, actualObjectSize);
    if (startNewEpoch) {
      bftEngine::EpochManager::instance().startNewEpoch();
      LOG_INFO(GL, "We should start a new epoch");
    }
    LOG_INFO(GL, KVLOG(erasedMetaData));
    if (erasedMetaData) {
      // Here when metadata is erased, we need to update DBCheckpointManager.
      DbCheckpointManager::instance().setIsMetadataErased(true);
      metadataStoragePtr->eraseData();
      isNewStorage = metadataStoragePtr->initMaxSizeOfObjects(objectDescriptors, numOfObjects);
      auto secFileDir = ReplicaConfig::instance().getkeyViewFilePath();
      LOG_INFO(GL, "removing " << secFileDir << " files if exist");
      try {
        for (auto &it : fs::directory_iterator{secFileDir}) {
          if (it.path().string().find(secFilePrefix) != std::string::npos) {
            fs::remove(it.path());
          }
        }
      } catch (std::exception &e) {
        LOG_FATAL(GL, "unable to remove the secret file, as we erased the metadata we won't be able to restart");
        ConcordAssert(false);
      }
    }

    // Init the persistent storage
    static_cast<PersistentStorageImp *>(persistentStoragePtr.get())->init(std::move(metadataStoragePtr));
  }
  auto replicaInternal = std::make_unique<ReplicaInternal>();
  shared_ptr<MsgHandlersRegistrator> msgHandlersPtr(new MsgHandlersRegistrator());
  auto incomingMsgsStorageImpPtr =
      std::make_unique<IncomingMsgsStorageImp>(msgHandlersPtr, timersResolution, replicaConfig.replicaId);
  auto &timers = incomingMsgsStorageImpPtr->timers();
  shared_ptr<IncomingMsgsStorage> incomingMsgsStoragePtr{std::move(incomingMsgsStorageImpPtr)};
  shared_ptr<bft::communication::IReceiver> msgReceiverPtr = std::make_shared<MsgReceiver>(incomingMsgsStoragePtr);
  shared_ptr<MsgsCommunicator> msgsCommunicatorPtr(
      new MsgsCommunicator(communication, incomingMsgsStoragePtr, msgReceiverPtr));
  if (isNewStorage) {
    auto replicaImp = std::make_unique<ReplicaImp>(replicaConfig,
                                                   requestsHandler,
                                                   stateTransfer,
                                                   msgsCommunicatorPtr,
                                                   persistentStoragePtr,
                                                   msgHandlersPtr,
                                                   timers,
                                                   pm,
                                                   sm);
    auto preprocessorPtr = ReplicaFactory::createPreProcessor(replicaConfig,
                                                              msgsCommunicatorPtr,
                                                              incomingMsgsStoragePtr,
                                                              msgHandlersPtr,
                                                              *requestsHandler,
                                                              *static_cast<InternalReplicaApi *>(replicaImp.get()),
                                                              timers,
                                                              pm);
    replicaInternal = std::make_unique<ReplicaInternal>(
        std::move(preprocessorPtr), replicaImp->ticksGenerator(), persistentStoragePtr, replicaImp->internalClient());
    replicaInternal->setReplica(std::move(replicaImp));
  } else {
    ReplicaLoader::ErrorCode loadErrCode;
    auto loadedReplicaData = ReplicaLoader::loadReplica(persistentStoragePtr, loadErrCode);
    if (loadErrCode != ReplicaLoader::ErrorCode::Success) {
      LOG_ERROR(GL, "Unable to load replica state from storage. Error " << static_cast<uint32_t>(loadErrCode));
      return nullptr;
    }
    // TODO(GG): compare ld.repConfig and replicaConfig
    auto replicaImp = std::make_unique<ReplicaImp>(loadedReplicaData,
                                                   requestsHandler,
                                                   stateTransfer,
                                                   msgsCommunicatorPtr,
                                                   persistentStoragePtr,
                                                   msgHandlersPtr,
                                                   timers,
                                                   pm,
                                                   sm);

    auto preprocessorPtr = ReplicaFactory::createPreProcessor(replicaConfig,
                                                              msgsCommunicatorPtr,
                                                              incomingMsgsStoragePtr,
                                                              msgHandlersPtr,
                                                              *requestsHandler,
                                                              *static_cast<InternalReplicaApi *>(replicaImp.get()),
                                                              timers,
                                                              pm);

    replicaInternal = std::make_unique<ReplicaInternal>(
        std::move(preprocessorPtr), replicaImp->ticksGenerator(), persistentStoragePtr, replicaImp->internalClient());
    replicaInternal->setReplica(std::move(replicaImp));
  }
  return replicaInternal;
}

ReplicaFactory::IReplicaPtr ReplicaFactory::createRoReplica(const ReplicaConfig &replicaConfig,
                                                            std::shared_ptr<IRequestsHandler> requestsHandler,
                                                            IStateTransfer *stateTransfer,
                                                            bft::communication::ICommunication *communication,
                                                            MetadataStorage *metadataStorage) {
  auto replicaInternal = std::make_unique<ReplicaInternal>();
  auto msgHandlers = std::make_shared<MsgHandlersRegistrator>();
  auto incomingMsgsStorageImpPtr =
      std::make_unique<IncomingMsgsStorageImp>(msgHandlers, timersResolution, replicaConfig.replicaId);
  auto &timers = incomingMsgsStorageImpPtr->timers();
  std::shared_ptr<IncomingMsgsStorage> incomingMsgsStorage{std::move(incomingMsgsStorageImpPtr)};
  auto msgReceiver = std::make_shared<MsgReceiver>(incomingMsgsStorage);
  auto msgsCommunicator = std::make_shared<MsgsCommunicator>(communication, incomingMsgsStorage, msgReceiver);
  replicaInternal->setReplica(std::make_unique<ReadOnlyReplica>(
      replicaConfig, requestsHandler, stateTransfer, msgsCommunicator, msgHandlers, timers, metadataStorage));
  return replicaInternal;
}

std::unique_ptr<preprocessor::PreProcessor> ReplicaFactory::createPreProcessor(
    const ReplicaConfig &replicaConfig,
    shared_ptr<MsgsCommunicator> &msgsCommunicator,
    shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
    shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
    bftEngine::IRequestsHandler &requestsHandler,
    InternalReplicaApi &replica,
    concordUtil::Timers &timers,
    shared_ptr<concord::performance::PerformanceManager> &pm) {
  if (replicaConfig.getnumOfExternalClients() + ReplicaConfig::instance().getnumOfClientProxies() <= 0) {
    LOG_ERROR(logger_, "Wrong configuration: a number of clients could not be zero!");
    return nullptr;
  }

  if (replicaConfig.getpreExecutionFeatureEnabled()) {
    return make_unique<preprocessor::PreProcessor>(
        msgsCommunicator, incomingMsgsStorage, msgHandlersRegistrator, requestsHandler, replica, timers, pm);
  }
  return nullptr;
}

}  // namespace bftEngine
