// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ReplicaStatusHandlers.hpp"
#include "diagnostics.h"
#include "json_output.hpp"

#include <string>

#define getName(var) #var

using namespace std;
using concordUtils::toPair;

namespace bftEngine::impl {

ReplicaStatusHandlers::ReplicaStatusHandlers(InternalReplicaApi &replica) : replica_(replica) {}

void ReplicaStatusHandlers::registerStatusHandlers() const {
  auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
  auto &msgQueue = replica_.getIncomingMsgsStorage();

  auto make_handler_callback = [&msgQueue](const string &name, const string &description) {
    return concord::diagnostics::StatusHandler(name, description, [&msgQueue, name]() {
      GetStatus get_status{name, std::promise<std::string>()};
      auto result = get_status.output.get_future();
      // Send a GetStatus InternalMessage to ReplicaImp, then wait for the result to be published.
      msgQueue.pushInternalMsg(std::move(get_status));
      return result.get();
    });
  };

  // TODO: handler name left for backward compatibility with callers, change the name 'replica'.
  auto replica_handler = make_handler_callback("replica", "Last stable sequence number of the concord-bft replica");
  auto state_transfer_handler = make_handler_callback("state-transfer", "Status of blockchain state transfer");
  auto key_exchange_handler = make_handler_callback("key-exchange", "Status of key-exchange");
  auto preexecution_handler = make_handler_callback("pre-execution", "Status of pre-execution");
  auto replica_state_handler = make_handler_callback("replica-state", "Internal state of the concord-bft replica");

  registrar.status.registerHandler(replica_handler);
  registrar.status.registerHandler(state_transfer_handler);
  registrar.status.registerHandler(key_exchange_handler);
  registrar.status.registerHandler(preexecution_handler);
  registrar.status.registerHandler(replica_state_handler);
}

std::string ReplicaStatusHandlers::preExecutionStatus(std::shared_ptr<concordMetrics::Aggregator> aggregator) const {
  const auto &curView = replica_.getCurrentView();
  const auto &primaryId = replica_.getReplicasInfo().primaryOfView(curView);
  std::ostringstream oss;
  std::unordered_map<std::string, std::string> result, nested_data;
  result.insert(toPair("Replica ID", std::to_string(replica_.getReplicasInfo().myId())));
  result.insert(toPair("Primary", std::to_string(primaryId)));
  const auto numOfClients =
      replica_.getReplicaConfig().numOfExternalClients + replica_.getReplicaConfig().numOfClientProxies;
  result.insert(toPair("numOfClients", std::to_string(numOfClients)));

  result.insert(
      toPair(getName(preProcReqReceived), aggregator->GetCounter("preProcessor", "preProcReqReceived").Get()));
  result.insert(toPair(getName(preProcReqInvalid), aggregator->GetCounter("preProcessor", "preProcReqInvalid").Get()));
  result.insert(toPair(getName(preProcReqIgnored), aggregator->GetCounter("preProcessor", "preProcReqIgnored").Get()));
  result.insert(toPair(getName(preProcConsensusNotReached),
                       aggregator->GetCounter("preProcessor", "preProcConsensusNotReached").Get()));
  result.insert(toPair(getName(preProcessRequestTimedout),
                       aggregator->GetCounter("preProcessor", "preProcessRequestTimedOut").Get()));
  result.insert(
      toPair(getName(preProcReqCompleted), aggregator->GetCounter("preProcessor", "preProcReqCompleted").Get()));
  result.insert(toPair(getName(preProcPossiblePrimaryFaultDetected),
                       aggregator->GetCounter("preProcessor", "preProcPossiblePrimaryFaultDetected").Get()));
  result.insert(toPair(getName(preProcBatchReqReceived),
                       aggregator->GetCounter("preProcessor", "preProcBatchReqReceived").Get()));
  result.insert(
      toPair(getName(preProcInFlyRequestsNum), aggregator->GetGauge("preProcessor", "PreProcInFlyRequestsNum").Get()));

  oss << concordUtils::kContainerToJson(result);
  return oss.str();
}

}  // namespace bftEngine::impl