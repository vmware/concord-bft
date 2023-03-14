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

#pragma once

#include "Replica.hpp"
#include "ReplicaImp.hpp"
#include "ReadOnlyReplica.hpp"
#include "ReplicaLoader.hpp"

namespace preprocessor {
class PreProcessor;
}

namespace bftEngine {

class ReplicaFactory {
 public:
  using IReplicaPtr = std::unique_ptr<IReplica>;
  static IReplicaPtr createReplica(const ReplicaConfig &,
                                   std::shared_ptr<IRequestsHandler>,
                                   IStateTransfer *,
                                   bft::communication::ICommunication *,
                                   MetadataStorage *,
                                   std::shared_ptr<concord::performance::PerformanceManager> pm,
                                   const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm);
  static IReplicaPtr createReplica(const ReplicaConfig &,
                                   std::shared_ptr<IRequestsHandler>,
                                   IStateTransfer *,
                                   bft::communication::ICommunication *,
                                   MetadataStorage *,
                                   bool &erasedMetadata,
                                   std::shared_ptr<concord::performance::PerformanceManager> pm,
                                   const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm);

  static IReplicaPtr createRoReplica(const ReplicaConfig &,
                                     std::shared_ptr<IRequestsHandler>,
                                     IStateTransfer *,
                                     bft::communication::ICommunication *,
                                     MetadataStorage *);

  static void setAggregator(const std::shared_ptr<concordMetrics::Aggregator> &aggregator);
  static logging::Logger logger_;

 private:
  static std::unique_ptr<preprocessor::PreProcessor> createPreProcessor(
      const ReplicaConfig &replicaConfig,
      shared_ptr<MsgsCommunicator> &msgsCommunicator,
      shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
      shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
      bftEngine::IRequestsHandler &requestsHandler,
      InternalReplicaApi &replica,
      concordUtil::Timers &timers,
      shared_ptr<concord::performance::PerformanceManager> &pm);
};
}  // namespace bftEngine
