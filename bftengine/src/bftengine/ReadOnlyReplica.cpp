// Concord
//
// Copyright (c) 2018, 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "ReadOnlyReplica.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "messages/CheckpointMsg.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "CheckpointInfo.hpp"
#include "Logger.hpp"
#include "PersistentStorage.hpp"
#include "ClientsManager.hpp"
#include "MsgsCommunicator.hpp"
#include "KeyStore.h"

using concordUtil::Timers;

namespace bftEngine::impl {

ReadOnlyReplica::ReadOnlyReplica(const ReplicaConfig &config,
                                 IStateTransfer *stateTransfer,
                                 std::shared_ptr<MsgsCommunicator> msgComm,
                                 std::shared_ptr<PersistentStorage> persistentStorage,
                                 std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg,
                                 concordUtil::Timers &timers)
    : ReplicaForStateTransfer(config, stateTransfer, msgComm, msgHandlerReg, true, timers),
      ps_(persistentStorage),
      ro_metrics_{metrics_.RegisterCounter("receivedCheckpointMsgs"),
                  metrics_.RegisterCounter("sentAskForCheckpointMsgs"),
                  metrics_.RegisterCounter("receivedInvalidMsgs"),
                  metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum)} {
  repsInfo = new ReplicasInfo(config, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);
  msgHandlers_->registerMsgHandler(MsgCode::Checkpoint,
                                   bind(&ReadOnlyReplica::messageHandler<CheckpointMsg>, this, std::placeholders::_1));
  metrics_.Register();
  // must be initialized although is not used by ReadOnlyReplica for proper behavior of StateTransfer
  ClientsManager::setNumResPages(
      (config.numOfClientProxies + config.numOfExternalClients + config.numReplicas) *
      ClientsManager::reservedPagesPerClient(config.sizeOfReservedPage, config.maxReplyMessageSize));
  ClusterKeyStore::setNumResPages(config.numReplicas);
}

void ReadOnlyReplica::start() {
  ReplicaForStateTransfer::start();
  lastExecutedSeqNum = ps_->getLastExecutedSeqNum();
  askForCheckpointMsgTimer_ = timers_.add(std::chrono::seconds(5),  // TODO [TK] config
                                          Timers::Timer::RECURRING,
                                          [this](Timers::Handle) {
                                            if (!this->isCollectingState()) sendAskForCheckpointMsg();
                                          });
  msgsCommunicator_->startMsgsProcessing(config_.replicaId);
}

void ReadOnlyReplica::stop() {
  timers_.cancel(askForCheckpointMsgTimer_);
  ReplicaForStateTransfer::stop();
}

void ReadOnlyReplica::onTransferringCompleteImp(int64_t newStateCheckpoint) {
  lastExecutedSeqNum = newStateCheckpoint;
  ps_->beginWriteTran();
  ps_->setLastExecutedSeqNum(lastExecutedSeqNum);
  ps_->endWriteTran();
  ro_metrics_.last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
}

void ReadOnlyReplica::onReportAboutInvalidMessage(MessageBase *msg, const char *reason) {
  ro_metrics_.received_invalid_msg_.Get().Inc();
  LOG_WARN(GL,
           "Node " << config_.replicaId << " received invalid message from Node " << msg->senderId()
                   << " type=" << msg->type() << " reason: " << reason);
}
void ReadOnlyReplica::sendAskForCheckpointMsg() {
  ro_metrics_.sent_ask_for_checkpoint_msg_.Get().Inc();
  LOG_INFO(GL, "sending AskForCheckpointMsg");
  auto msg = std::make_unique<AskForCheckpointMsg>(config_.replicaId);
  for (auto id : repsInfo->idsOfPeerReplicas()) send(msg.get(), id);
}

template <>
void ReadOnlyReplica::onMessage<CheckpointMsg>(CheckpointMsg *msg) {
  ro_metrics_.received_checkpoint_msg_.Get().Inc();
  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgSeqNum = msg->seqNumber();
  const Digest msgDigest = msg->digestOfState();
  const bool msgIsStable = msg->isStableState();

  LOG_INFO(GL,
           "Node " << config_.replicaId << " received Checkpoint message from node " << msgSenderId << " for seqNumber "
                   << msgSeqNum << " (size=" << msg->size() << ", stable=" << (msgIsStable ? "true" : "false")
                   << ", digestPrefix=" << *((int *)(&msgDigest)) << ")");

  // not relevant
  if (!msgIsStable || msgSeqNum <= lastExecutedSeqNum) return;

  // previous CheckpointMsg from the same sender
  auto pos = tableOfStableCheckpoints.find(msgSenderId);
  if (pos != tableOfStableCheckpoints.end() && pos->second->seqNumber() >= msgSeqNum) return;
  if (pos != tableOfStableCheckpoints.end()) delete pos->second;
  CheckpointMsg *x = new CheckpointMsg(msgSenderId, msgSeqNum, msgDigest, msgIsStable);
  tableOfStableCheckpoints[msgSenderId] = x;
  LOG_INFO(GL,
           "Node " << config_.replicaId
                   << " added stable Checkpoint message to tableOfStableCheckpoints (message from node " << msgSenderId
                   << " for seqNumber " << msgSeqNum << ")");

  // not enough CheckpointMsg's
  if ((uint16_t)tableOfStableCheckpoints.size() < config_.fVal + 1) return;

  // check if got enough relevant CheckpointMsgs
  uint16_t numRelevant = 0;
  for (auto tableItrator = tableOfStableCheckpoints.begin(); tableItrator != tableOfStableCheckpoints.end();) {
    if (tableItrator->second->seqNumber() <= lastExecutedSeqNum) {
      delete tableItrator->second;
      tableItrator = tableOfStableCheckpoints.erase(tableItrator);
    } else {
      numRelevant++;
      tableItrator++;
    }
  }
  ConcordAssert(numRelevant == tableOfStableCheckpoints.size());
  LOG_INFO(GL, "numRelevant=" << numRelevant);

  // if enough - invoke state transfer
  if (numRelevant >= config_.fVal + 1) {
    LOG_INFO(GL, "call to startCollectingState()");
    stateTransfer->startCollectingState();
  }
}

}  // namespace bftEngine::impl
