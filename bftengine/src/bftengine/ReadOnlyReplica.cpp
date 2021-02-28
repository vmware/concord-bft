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
#include "messages/ClientRequestMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "ClientsManager.hpp"
#include "CheckpointInfo.hpp"
#include "Logger.hpp"
#include "kvstream.h"
#include "PersistentStorage.hpp"
#include "MsgsCommunicator.hpp"
#include "KeyStore.h"

using concordUtil::Timers;

namespace bftEngine::impl {

ReadOnlyReplica::ReadOnlyReplica(const ReplicaConfig &config,
                                 IStateTransfer *stateTransfer,
                                 std::shared_ptr<MsgsCommunicator> msgComm,
                                 std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg,
                                 IRequestsHandler *request_handler,
                                 concordUtil::Timers &timers)
    : ReplicaForStateTransfer(config, stateTransfer, msgComm, msgHandlerReg, true, timers),
      bftRequestsHandler_(request_handler),
      ro_metrics_{metrics_.RegisterCounter("receivedCheckpointMsgs"),
                  metrics_.RegisterCounter("sentAskForCheckpointMsgs"),
                  metrics_.RegisterCounter("receivedInvalidMsgs"),
                  metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum)} {
  repsInfo = new ReplicasInfo(config, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);
  msgHandlers_->registerMsgHandler(MsgCode::Checkpoint,
                                   bind(&ReadOnlyReplica::messageHandler<CheckpointMsg>, this, std::placeholders::_1));
  msgHandlers_->registerMsgHandler(
      MsgCode::ClientRequest, bind(&ReadOnlyReplica::messageHandler<ClientRequestMsg>, this, std::placeholders::_1));
  metrics_.Register();
  // must be initialized although is not used by ReadOnlyReplica for proper behavior of StateTransfer
  ClientsManager::setNumResPages(
      (config.numOfClientProxies + config.numOfExternalClients + config.numReplicas) *
      ClientsManager::reservedPagesPerClient(config.sizeOfReservedPage, config.maxReplyMessageSize));
  ClusterKeyStore::setNumResPages(config.numReplicas);
}

void ReadOnlyReplica::start() {
  ReplicaForStateTransfer::start();
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

void ReadOnlyReplica::onTransferringCompleteImp(uint64_t newStateCheckpoint) {
  lastExecutedSeqNum = newStateCheckpoint * checkpointWindowSize;

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

/*
 * The read-only replica may get only read request from client and only to get reconfiguration data (such as pruning)
 * Thus, there is no need to validate the client identity, it will be validate using its public key in the command
 * handler. Notice that also doesn't need to have a client manager in this case.
 */
template <>
void ReadOnlyReplica::onMessage<ClientRequestMsg>(ClientRequestMsg *m) {
  const NodeIdType senderId = m->senderId();
  const NodeIdType clientId = m->clientProxyId();
  const bool readOnly = m->isReadOnly();
  const ReqId reqSeqNum = m->requestSeqNum();
  const uint8_t flags = m->flags();

  SCOPED_MDC_CID(m->getCid());
  LOG_DEBUG(MSGS, KVLOG(clientId, reqSeqNum, senderId) << " flags: " << std::bitset<8>(flags));

  const auto &span_context = m->spanContext<std::remove_pointer<decltype(m)>::type>();
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
  span.setTag("rid", config_.getreplicaId());
  span.setTag("cid", m->getCid());
  span.setTag("seq_num", reqSeqNum);

  if (readOnly) {
    executeReadOnlyRequest(span, m);
  } else {
    LOG_WARN(GL, "Read-only replica should not get write request from a client");
  }
  delete m;
}

template <>
void ReadOnlyReplica::onMessage<CheckpointMsg>(CheckpointMsg *msg) {
  ro_metrics_.received_checkpoint_msg_.Get().Inc();
  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgSeqNum = msg->seqNumber();
  const Digest msgDigest = msg->digestOfState();
  const bool msgIsStable = msg->isStableState();

  LOG_INFO(GL, KVLOG(msgSenderId, msgSeqNum, msg->size(), msgIsStable) << ", digest: " << msgDigest.toString());

  // not relevant
  if (!msgIsStable || msgSeqNum <= lastExecutedSeqNum) return;

  // previous CheckpointMsg from the same sender
  auto pos = tableOfStableCheckpoints.find(msgSenderId);
  if (pos != tableOfStableCheckpoints.end() && pos->second->seqNumber() >= msgSeqNum) return;
  if (pos != tableOfStableCheckpoints.end()) delete pos->second;
  CheckpointMsg *x = new CheckpointMsg(msgSenderId, msgSeqNum, msgDigest, msgIsStable);
  tableOfStableCheckpoints[msgSenderId] = x;
  LOG_INFO(GL,
           "Added stable Checkpoint message to tableOfStableCheckpoints (message from node "
               << msgSenderId << " for seqNumber " << msgSeqNum << ")");

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
void ReadOnlyReplica::executeReadOnlyRequest(concordUtils::SpanWrapper &parent_span, ClientRequestMsg *request) {
  ConcordAssert(request->isReadOnly());

  auto span = concordUtils::startChildSpan("bft_execute_read_only_request", parent_span);
  // The client should know not to realy on ROR replicas replies to get the current primary
  ClientReplyMsg reply(0, request->requestSeqNum(), config_.getreplicaId());

  uint16_t clientId = request->clientProxyId();

  int status = 0;
  bftEngine::IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  accumulatedRequests.push_back(bftEngine::IRequestsHandler::ExecutionRequest{clientId,
                                                                              static_cast<uint64_t>(lastExecutedSeqNum),
                                                                              request->flags(),
                                                                              request->requestLength(),
                                                                              request->requestBuf(),
                                                                              reply.maxReplyLength(),
                                                                              reply.replyBuf()});
  { bftRequestsHandler_.execute(accumulatedRequests, request->getCid(), span); }
  const IRequestsHandler::ExecutionRequest &single_request = accumulatedRequests.back();
  status = single_request.outExecutionStatus;
  const uint32_t actualReplyLength = single_request.outActualReplySize;
  const uint32_t actualReplicaSpecificInfoLength = single_request.outReplicaSpecificInfoSize;
  LOG_DEBUG(GL,
            "Executed read only request. " << KVLOG(clientId,
                                                    lastExecutedSeqNum,
                                                    request->requestLength(),
                                                    reply.maxReplyLength(),
                                                    actualReplyLength,
                                                    actualReplicaSpecificInfoLength,
                                                    status));
  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)
  if (!status) {
    if (actualReplyLength > 0) {
      reply.setReplyLength(actualReplyLength);
      reply.setReplicaSpecificInfoLength(actualReplicaSpecificInfoLength);
      ReplicaBase::send(&reply, clientId);
    } else {
      LOG_ERROR(GL, "Received zero size response. " << KVLOG(clientId));
    }

  } else {
    LOG_ERROR(GL, "Received error while executing RO request. " << KVLOG(clientId, status));
  }
}

}  // namespace bftEngine::impl
