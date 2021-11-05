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

#include <bftengine/Replica.hpp>
#include <optional>
#include <functional>
#include "ReadOnlyReplica.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "messages/CheckpointMsg.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "CheckpointInfo.hpp"
#include "Logger.hpp"
#include "kvstream.h"
#include "PersistentStorage.hpp"
#include "MsgsCommunicator.hpp"
#include "SigManager.hpp"
#include "ReconfigurationCmd.hpp"

using concordUtil::Timers;

namespace bftEngine::impl {

ReadOnlyReplica::ReadOnlyReplica(const ReplicaConfig &config,
                                 std::shared_ptr<IRequestsHandler> requestsHandler,
                                 IStateTransfer *stateTransfer,
                                 std::shared_ptr<MsgsCommunicator> msgComm,
                                 std::shared_ptr<MsgHandlersRegistrator> msgHandlerReg,
                                 concordUtil::Timers &timers)
    : ReplicaForStateTransfer(config, requestsHandler, stateTransfer, msgComm, msgHandlerReg, true, timers),
      ro_metrics_{metrics_.RegisterCounter("receivedCheckpointMsgs"),
                  metrics_.RegisterCounter("sentAskForCheckpointMsgs"),
                  metrics_.RegisterCounter("receivedInvalidMsgs"),
                  metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum)} {
  LOG_INFO(GL, "Initialising ReadOnly Replica");
  repsInfo = new ReplicasInfo(config, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);
  msgHandlers_->registerMsgHandler(
      MsgCode::Checkpoint, std::bind(&ReadOnlyReplica::messageHandler<CheckpointMsg>, this, std::placeholders::_1));
  msgHandlers_->registerMsgHandler(
      MsgCode::ClientRequest,
      std::bind(&ReadOnlyReplica::messageHandler<ClientRequestMsg>, this, std::placeholders::_1));
  metrics_.Register();

  sigManager_.reset(SigManager::init(config_.replicaId,
                                     config_.replicaPrivateKey,
                                     config_.publicKeysOfReplicas,
                                     concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat,
                                     config_.clientTransactionSigningEnabled ? &config_.publicKeysOfClients : nullptr,
                                     concord::util::crypto::KeyFormat::PemFormat,
                                     *repsInfo));
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
  ro_metrics_.received_invalid_msg_++;
  LOG_WARN(GL,
           "Node " << config_.replicaId << " received invalid message from Node " << msg->senderId()
                   << " type=" << msg->type() << " reason: " << reason);
}
void ReadOnlyReplica::sendAskForCheckpointMsg() {
  ro_metrics_.sent_ask_for_checkpoint_msg_++;
  LOG_INFO(GL, "sending AskForCheckpointMsg");
  auto msg = std::make_unique<AskForCheckpointMsg>(config_.replicaId);
  for (auto id : repsInfo->idsOfPeerReplicas()) send(msg.get(), id);
}

template <>
void ReadOnlyReplica::onMessage<CheckpointMsg>(CheckpointMsg *msg) {
  if (isCollectingState()) {
    delete msg;
    return;
  }
  ro_metrics_.received_checkpoint_msg_++;
  const ReplicaId msgGenReplicaId = msg->idOfGeneratedReplica();
  const SeqNum msgSeqNum = msg->seqNumber();
  const Digest msgDigest = msg->digestOfState();
  const bool msgIsStable = msg->isStableState();
  const EpochNum msgEpochNum = msg->epochNumber();
  EpochNum replicasLastKnownEpochVal = 0;
  LOG_INFO(GL,
           KVLOG(msg->senderId(), msgGenReplicaId, msgSeqNum, msg->size(), msgIsStable)
               << ", digest: " << msgDigest.toString());
  // Reconfiguration cmd block is synced to RO replica via reserved pages
  auto epochNumberFromResPages = ReconfigurationCmd::instance().getReconfigurationCommandEpochNumber();
  if (epochNumberFromResPages.has_value()) {
    replicasLastKnownEpochVal = epochNumberFromResPages.value();
  }
  // not relevant
  if (!msgIsStable || msgSeqNum <= lastExecutedSeqNum) return;

  // previous CheckpointMsg from the same sender
  auto pos = tableOfStableCheckpoints.find(msgGenReplicaId);
  if (pos != tableOfStableCheckpoints.end() &&
      (pos->second->seqNumber() >= msgSeqNum || msgEpochNum < replicasLastKnownEpochVal))
    return;
  if (pos != tableOfStableCheckpoints.end()) delete pos->second;
  CheckpointMsg *x = new CheckpointMsg(msgGenReplicaId, msgSeqNum, msgDigest, msgIsStable);
  x->setEpochNumber(msgEpochNum);
  tableOfStableCheckpoints[msgGenReplicaId] = x;
  LOG_INFO(GL,
           "Added stable Checkpoint message to tableOfStableCheckpoints (message generated from node "
               << msgGenReplicaId << " for seqNumber " << msgSeqNum << " sender node " << msg->senderId() << ")");

  // not enough CheckpointMsg's
  if ((uint16_t)tableOfStableCheckpoints.size() < config_.fVal + 1) return;

  // check if got enough relevant CheckpointMsgs
  uint16_t numRelevant = 0;
  for (auto tableItrator = tableOfStableCheckpoints.begin(); tableItrator != tableOfStableCheckpoints.end();) {
    if (tableItrator->second->seqNumber() <= lastExecutedSeqNum) {
      if (msgEpochNum <= replicasLastKnownEpochVal) {
        delete tableItrator->second;
        tableItrator = tableOfStableCheckpoints.erase(tableItrator);
      } else {  // committer replicas have moved to higer epoch
        numRelevant++;
        tableItrator++;
      }
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

template <>
void ReadOnlyReplica::onMessage<ClientRequestMsg>(ClientRequestMsg *m) {
  const NodeIdType senderId = m->senderId();
  const NodeIdType clientId = m->clientProxyId();
  const bool reconfig_flag = (m->flags() & MsgFlag::RECONFIG_FLAG) != 0;
  const ReqId reqSeqNum = m->requestSeqNum();
  const uint64_t flags = m->flags();

  SCOPED_MDC_CID(m->getCid());
  LOG_DEBUG(CNSUS, KVLOG(clientId, reqSeqNum, senderId) << " flags: " << std::bitset<sizeof(uint64_t) * 8>(flags));

  const auto &span_context = m->spanContext<std::remove_pointer<ClientRequestMsg>::type>();
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
  span.setTag("rid", config_.getreplicaId());
  span.setTag("cid", m->getCid());
  span.setTag("seq_num", reqSeqNum);

  // A read only replica can handle only reconfiguration requests. Those requests are signed by the operator and
  // the validation is done in the reconfiguration engine. Thus, we don't need to check the client validity as in
  // the committers

  if (reconfig_flag) {
    LOG_INFO(GL, "ro replica has received a reconfiguration request");
    executeReadOnlyRequest(span, m);
    delete m;
    return;
  }

  delete m;
}

void ReadOnlyReplica::executeReadOnlyRequest(concordUtils::SpanWrapper &parent_span, const ClientRequestMsg &request) {
  auto span = concordUtils::startChildSpan("bft_execute_read_only_request", parent_span);
  // Read only replica does not know who is the primary, so it always return 0. It is the client responsibility to treat
  // the replies accordingly.
  ClientReplyMsg reply(0, request.requestSeqNum(), config_.getreplicaId());

  const uint16_t clientId = request.clientProxyId();

  int status = 0;
  bftEngine::IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  accumulatedRequests.push_back(bftEngine::IRequestsHandler::ExecutionRequest{clientId,
                                                                              static_cast<uint64_t>(lastExecutedSeqNum),
                                                                              request.getCid(),
                                                                              request.flags(),
                                                                              request.requestLength(),
                                                                              request.requestBuf(),
                                                                              "",
                                                                              reply.maxReplyLength(),
                                                                              reply.replyBuf()});

  // DD: Do we need to take care of Time Service here?
  bftRequestsHandler_->execute(accumulatedRequests, std::nullopt, request.getCid(), span);
  const IRequestsHandler::ExecutionRequest &single_request = accumulatedRequests.back();
  status = single_request.outExecutionStatus;
  const uint32_t actualReplyLength = single_request.outActualReplySize;
  const uint32_t actualReplicaSpecificInfoLength = single_request.outReplicaSpecificInfoSize;
  LOG_DEBUG(GL,
            "Executed read only request. " << KVLOG(clientId,
                                                    lastExecutedSeqNum,
                                                    request.requestLength(),
                                                    reply.maxReplyLength(),
                                                    actualReplyLength,
                                                    actualReplicaSpecificInfoLength,
                                                    status));
  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)
  if (!status) {
    if (actualReplyLength > 0) {
      reply.setReplyLength(actualReplyLength);
      reply.setReplicaSpecificInfoLength(actualReplicaSpecificInfoLength);
      send(&reply, clientId);
    } else {
      LOG_ERROR(GL, "Received zero size response. " << KVLOG(clientId));
    }

  } else {
    LOG_ERROR(GL, "Received error while executing RO request. " << KVLOG(clientId, status));
  }
}

}  // namespace bftEngine::impl
