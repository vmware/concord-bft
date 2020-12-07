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

#include "bftengine/IStateTransfer.hpp"
#include "bftengine/Replica.hpp"
#include "ReplicaImp.hpp"
#include "ReplicaImp.h"

using namespace bftEngine;
using namespace bftEngine::impl;
using bft::communication::ICommunication;
using concord::kvbc::IStorageFactory;

namespace concord::kvbc::test {

bftEngine::IReplica::IReplicaPtr createNewReplica(const ReplicaConfig &replicaConfig,
                                                  IRequestsHandler *requestsHandler,
                                                  IStateTransfer *stateTransfer,
                                                  bft::communication::ICommunication *communication,
                                                  MetadataStorage *metadataStorage,
                                                  bool &erasedMetadata);

class ByzantineBftEngineReplicaImp : public bftEngine::impl::ReplicaImp {
 public:
  ByzantineBftEngineReplicaImp(const ReplicaConfig &config,
                               IRequestsHandler *requestsHandler,
                               IStateTransfer *stateTrans,
                               shared_ptr<MsgsCommunicator> msgsCommunicator,
                               shared_ptr<PersistentStorage> persistentStorage,
                               shared_ptr<MsgHandlersRegistrator> msgHandlers,
                               concordUtil::Timers &timers);

  ByzantineBftEngineReplicaImp(const LoadedReplicaData &ld,
                               IRequestsHandler *requestsHandler,
                               IStateTransfer *stateTrans,
                               shared_ptr<MsgsCommunicator> msgsCommunicator,
                               shared_ptr<PersistentStorage> persistentStorage,
                               shared_ptr<MsgHandlersRegistrator> msgHandlers,
                               concordUtil::Timers &timers);
};

class ByzantineKvbcReplicaImp : public concord::kvbc::ReplicaImp {
 public:
  ByzantineKvbcReplicaImp(ICommunication *comm,
                          const bftEngine::ReplicaConfig &replicaConfig,
                          std::unique_ptr<IStorageFactory> storageFactory,
                          std::shared_ptr<concordMetrics::Aggregator> aggregator);

  Status start() override;

 private:
  void createReplicaAndSyncState();
};
};  // namespace concord::kvbc::test