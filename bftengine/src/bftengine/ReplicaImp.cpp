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

#include "ReplicaImp.hpp"
#include "Timers.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "ControllerWithSimpleHistory.hpp"
#include "DebugStatistics.hpp"
#include "SysConsts.hpp"
#include "ReplicaConfig.hpp"
#include "MsgsCommunicator.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "ReplicaLoader.hpp"
#include "PersistentStorage.hpp"
#include "OpenTracing.hpp"
#include "diagnostics.h"
#include "TimeUtils.hpp"

#include "messages/ClientRequestMsg.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/CheckpointMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "messages/StartSlowCommitMsg.hpp"
#include "messages/ReqMissingDataMsg.hpp"
#include "messages/SimpleAckMsg.hpp"
#include "messages/ViewChangeMsg.hpp"
#include "messages/NewViewMsg.hpp"
#include "messages/PartialCommitProofMsg.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "messages/ReplicaStatusMsg.hpp"
#include "messages/AskForCheckpointMsg.hpp"
#include "KeyManager.h"

#include <string>
#include <type_traits>

using concordUtil::Timers;
using namespace std;
using namespace std::chrono;
using namespace std::placeholders;

namespace bftEngine::impl {

void ReplicaImp::registerStatusHandlers() {
  auto &registrar = concord::diagnostics::RegistrarSingleton::getInstance();
  auto &msgQueue = getIncomingMsgsStorage();

  auto make_handler_callback = [&msgQueue](const string &name, const string &description) {
    return concord::diagnostics::StatusHandler(name, description, [&msgQueue, name]() {
      GetStatus get_status{name, std::promise<std::string>()};
      auto result = get_status.output.get_future();
      // Send a GetStatus InternalMessage to ReplicaImp, then wait for the result to be published.
      msgQueue.pushInternalMsg(std::move(get_status));
      return result.get();
    });
  };

  auto replica_handler = make_handler_callback("replica", "Internal state of the concord-bft replica");
  auto state_transfer_handler = make_handler_callback("state-transfer", "Status of blockchain state transfer");

  registrar.registerStatusHandler(replica_handler);
  registrar.registerStatusHandler(state_transfer_handler);
}

void ReplicaImp::registerMsgHandlers() {
  msgHandlers_->registerMsgHandler(MsgCode::Checkpoint, bind(&ReplicaImp::messageHandler<CheckpointMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::CommitPartial,
                                   bind(&ReplicaImp::messageHandler<CommitPartialMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::CommitFull, bind(&ReplicaImp::messageHandler<CommitFullMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::FullCommitProof,
                                   bind(&ReplicaImp::messageHandler<FullCommitProofMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::NewView, bind(&ReplicaImp::messageHandler<NewViewMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::PrePrepare, bind(&ReplicaImp::messageHandler<PrePrepareMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::PartialCommitProof,
                                   bind(&ReplicaImp::messageHandler<PartialCommitProofMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::PreparePartial,
                                   bind(&ReplicaImp::messageHandler<PreparePartialMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::PrepareFull, bind(&ReplicaImp::messageHandler<PrepareFullMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::ReqMissingData,
                                   bind(&ReplicaImp::messageHandler<ReqMissingDataMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::SimpleAck, bind(&ReplicaImp::messageHandler<SimpleAckMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::StartSlowCommit,
                                   bind(&ReplicaImp::messageHandler<StartSlowCommitMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::ViewChange, bind(&ReplicaImp::messageHandler<ViewChangeMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::ClientRequest,
                                   bind(&ReplicaImp::messageHandler<ClientRequestMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::ReplicaStatus,
                                   bind(&ReplicaImp::messageHandler<ReplicaStatusMsg>, this, _1));

  msgHandlers_->registerMsgHandler(MsgCode::AskForCheckpoint,
                                   bind(&ReplicaImp::messageHandler<AskForCheckpointMsg>, this, _1));

  msgHandlers_->registerInternalMsgHandler([this](InternalMessage &&msg) { onInternalMsg(std::move(msg)); });
}

template <typename T>
void ReplicaImp::messageHandler(MessageBase *msg) {
  if (validateMessage(msg) && !isCollectingState())
    onMessage<T>(static_cast<T *>(msg));
  else
    delete msg;
}

template <class T>
void onMessage(T *);

void ReplicaImp::send(MessageBase *m, NodeIdType dest) {
  // debug code begin
  if (m->type() == MsgCode::Checkpoint && static_cast<CheckpointMsg *>(m)->digestOfState().isZero())
    LOG_WARN(GL, "Debug: checkpoint digest is zero");
  // debug code end
  ReplicaBase::send(m, dest);
}

void ReplicaImp::sendAndIncrementMetric(MessageBase *m, NodeIdType id, CounterHandle &counterMetric) {
  send(m, id);
  counterMetric.Get().Inc();
}

void ReplicaImp::onReportAboutInvalidMessage(MessageBase *msg, const char *reason) {
  LOG_WARN(GL, "Received invalid message. " << KVLOG(msg->senderId(), msg->type(), reason));

  // TODO(GG): logic that deals with invalid messages (e.g., a node that sends invalid messages may have a problem (old
  // version,bug,malicious,...)).
}

template <>
void ReplicaImp::onMessage<ClientRequestMsg>(ClientRequestMsg *m) {
  if (isSeqNumToStopAt(lastExecutedSeqNum)) {
    LOG_INFO(GL,
             "Ignoring ClientRequest because system is stopped at checkpoint pending control state operation (upgrade, "
             "etc...)");
    return;
  }

  metric_received_client_requests_.Get().Inc();
  const NodeIdType senderId = m->senderId();
  const NodeIdType clientId = m->clientProxyId();
  const bool readOnly = m->isReadOnly();
  const ReqId reqSeqNum = m->requestSeqNum();
  const uint8_t flags = m->flags();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_CID(m->getCid());
  LOG_DEBUG(GL, "Received ClientRequestMsg. " << KVLOG(clientId, reqSeqNum, flags, senderId));

  const auto &span_context = m->spanContext<std::remove_pointer<decltype(m)>::type>();
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
  span.setTag("rid", config_.replicaId);
  span.setTag("cid", m->getCid());
  span.setTag("seq_num", reqSeqNum);

  if (ReplicaConfigSingleton::GetInstance().GetKeyExchangeOnStart()) {
    // If Multi sig keys havn't been replaced for all replicas and it's not a key ex msg
    // then don't accept the msg.
    if (!KeyManager::get().keysExchanged && !(flags & KEY_EXCHANGE_FLAG)) {
      LOG_INFO(GL, "KEY EXCHANGE didn't complete yet, dropping msg");
      delete m;
      return;
    }
  }

  if (isCollectingState()) {
    LOG_INFO(GL,
             "ClientRequestMsg is ignored because this replica is collecting missing state from the other replicas. "
                 << KVLOG(reqSeqNum, clientId));
    delete m;
    return;
  }

  // check message validity
  const bool invalidClient = !isValidClient(clientId);
  const bool sentFromReplicaToNonPrimary = repsInfo->isIdOfReplica(senderId) && !isCurrentPrimary();

  // TODO(GG): more conditions (make sure that a request of client A cannot be generated by client B!=A)
  if (invalidClient || sentFromReplicaToNonPrimary) {
    std::ostringstream oss("ClientRequestMsg is invalid. ");
    oss << KVLOG(invalidClient, sentFromReplicaToNonPrimary);
    onReportAboutInvalidMessage(m, oss.str().c_str());
    delete m;
    return;
  }

  if (readOnly) {
    executeReadOnlyRequest(span, m);
    delete m;
    return;
  }

  if (!currentViewIsActive()) {
    LOG_INFO(GL, "ClientRequestMsg is ignored because current view is inactive. " << KVLOG(reqSeqNum, clientId));
    delete m;
    return;
  }

  const ReqId seqNumberOfLastReply = seqNumberOfLastReplyToClient(clientId);

  if (seqNumberOfLastReply < reqSeqNum) {
    if (isCurrentPrimary()) {
      // TODO(GG): use config/parameter
      if (requestsQueueOfPrimary.size() >= 700) {
        LOG_WARN(GL,
                 "ClientRequestMsg dropped. Primary reqeust queue is full. "
                     << KVLOG(clientId, reqSeqNum, requestsQueueOfPrimary.size()));
        delete m;
        return;
      }
      if (clientsManager->noPendingAndRequestCanBecomePending(clientId, reqSeqNum)) {
        LOG_DEBUG(CNSUS,
                  "Pushing to primary queue, request [" << reqSeqNum << "], client [" << clientId
                                                        << "], senderId=" << senderId);
        requestsQueueOfPrimary.push(m);
        primaryCombinedReqSize += m->size();
        tryToSendPrePrepareMsg(true);
        return;
      } else {
        LOG_INFO(GL,
                 "ClientRequestMsg is ignored because: request is old, OR primary is current working on a request from "
                 "the same client. "
                     << KVLOG(clientId, reqSeqNum));
      }
    } else {  // not the current primary
      if (clientsManager->noPendingAndRequestCanBecomePending(clientId, reqSeqNum)) {
        clientsManager->addPendingRequest(clientId, reqSeqNum);

        // TODO(GG): add a mechanism that retransmits (otherwise we may start unnecessary view-change)
        send(m, currentPrimary());

        LOG_INFO(GL, "Forwarding ClientRequestMsg to the current primary. " << KVLOG(reqSeqNum, clientId));
      } else {
        LOG_INFO(GL,
                 "ClientRequestMsg is ignored because: request is old, OR primary is current working on a request "
                 "from the same client. "
                     << KVLOG(clientId, reqSeqNum));
      }
    }
  } else if (seqNumberOfLastReply == reqSeqNum) {
    LOG_DEBUG(
        GL,
        "ClientRequestMsg has already been executed: retransmitting reply to client. " << KVLOG(reqSeqNum, clientId));

    ClientReplyMsg *repMsg = clientsManager->allocateMsgWithLatestReply(clientId, currentPrimary());
    send(repMsg, clientId);
    delete repMsg;
  } else {
    LOG_INFO(GL,
             "ClientRequestMsg is ignored because request sequence number is not monotonically increasing. "
                 << KVLOG(clientId, reqSeqNum, seqNumberOfLastReply));
  }

  delete m;
}

void ReplicaImp::tryToSendPrePrepareMsg(bool batchingLogic) {
  if (isSeqNumToStopAt(lastExecutedSeqNum)) {
    LOG_INFO(GL,
             "Not sending PrePrepareMsg because system is stopped at checkpoint pending control state operation "
             "(upgrade, etc...)");
    return;
  }

  ConcordAssert(isCurrentPrimary());
  ConcordAssert(currentViewIsActive());

  if (primaryLastUsedSeqNum + 1 > lastStableSeqNum + kWorkWindowSize) {
    LOG_INFO(GL,
             "Will not send PrePrepare since next sequence number ["
                 << primaryLastUsedSeqNum + 1 << "] exceeds window threshold [" << lastStableSeqNum + kWorkWindowSize
                 << "]");
    return;
  }

  if (primaryLastUsedSeqNum + 1 > lastExecutedSeqNum + config_.concurrencyLevel) {
    LOG_INFO(GL,
             "Will not send PrePrepare since next sequence number ["
                 << primaryLastUsedSeqNum + 1 << "] exceeds concurrency threshold ["
                 << lastExecutedSeqNum + config_.concurrencyLevel << "]");
    return;  // TODO(GG): should also be checked by the non-primary replicas
  }

  if (requestsQueueOfPrimary.empty()) return;

  // remove irrelevant requests from the head of the requestsQueueOfPrimary (and update requestsInQueue)
  ClientRequestMsg *first = requestsQueueOfPrimary.front();
  while (first != nullptr &&
         !clientsManager->noPendingAndRequestCanBecomePending(first->clientProxyId(), first->requestSeqNum())) {
    SCOPED_MDC_CID(first->getCid());
    primaryCombinedReqSize -= first->size();
    delete first;
    requestsQueueOfPrimary.pop();
    first = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
  }

  const size_t requestsInQueue = requestsQueueOfPrimary.size();

  if (requestsInQueue == 0) return;

  ConcordAssertGE(primaryLastUsedSeqNum, lastExecutedSeqNum);

  uint64_t concurrentDiff = ((primaryLastUsedSeqNum + 1) - lastExecutedSeqNum);
  uint64_t minBatchSize = 1;

  // update maxNumberOfPendingRequestsInRecentHistory (if needed)
  if (requestsInQueue > maxNumberOfPendingRequestsInRecentHistory)
    maxNumberOfPendingRequestsInRecentHistory = requestsInQueue;

  // TODO(GG): the batching logic should be part of the configuration - TBD.
  if (batchingLogic && (concurrentDiff >= 2)) {
    minBatchSize = concurrentDiff * batchingFactor;

    const size_t maxReasonableMinBatchSize = 350;  // TODO(GG): use param from configuration

    if (minBatchSize > maxReasonableMinBatchSize) minBatchSize = maxReasonableMinBatchSize;
  }

  if (requestsInQueue < minBatchSize) {
    LOG_INFO(GL,
             "Not enough client requests to fill the batch threshold size. " << KVLOG(minBatchSize, requestsInQueue));
    metric_not_enough_client_requests_event_.Get().Inc();
    return;
  }

  // update batchingFactor
  if (((primaryLastUsedSeqNum + 1) % kWorkWindowSize) ==
      0)  // TODO(GG): do we want to update batchingFactor when the view is changed
  {
    const size_t aa = 4;  // TODO(GG): read from configuration
    batchingFactor = (maxNumberOfPendingRequestsInRecentHistory / aa);
    if (batchingFactor < 1) batchingFactor = 1;
    maxNumberOfPendingRequestsInRecentHistory = 0;
    LOG_DEBUG(GL, "PrePrepare batching factor updated. " << KVLOG(batchingFactor));
  }

  // because maxConcurrentAgreementsByPrimary <  MaxConcurrentFastPaths
  ConcordAssertLE((primaryLastUsedSeqNum + 1), lastExecutedSeqNum + MaxConcurrentFastPaths);

  CommitPath firstPath = controller->getCurrentFirstPath();

  ConcordAssertOR((config_.cVal != 0), (firstPath != CommitPath::FAST_WITH_THRESHOLD));

  controller->onSendingPrePrepare((primaryLastUsedSeqNum + 1), firstPath);

  ClientRequestMsg *nextRequest = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
  const auto &span_context = nextRequest ? nextRequest->spanContext<ClientRequestMsg>() : concordUtils::SpanContext{};

  PrePrepareMsg *pp = new PrePrepareMsg(
      config_.replicaId, curView, (primaryLastUsedSeqNum + 1), firstPath, span_context, primaryCombinedReqSize);
  uint16_t initialStorageForRequests = pp->remainingSizeForRequests();
  while (nextRequest != nullptr) {
    if (nextRequest->size() <= pp->remainingSizeForRequests()) {
      SCOPED_MDC_CID(nextRequest->getCid());
      if (clientsManager->noPendingAndRequestCanBecomePending(nextRequest->clientProxyId(),
                                                              nextRequest->requestSeqNum())) {
        pp->addRequest(nextRequest->body(), nextRequest->size());
        clientsManager->addPendingRequest(nextRequest->clientProxyId(), nextRequest->requestSeqNum());
      }
      primaryCombinedReqSize -= nextRequest->size();
    } else if (nextRequest->size() > initialStorageForRequests) {
      // The message is too big
      LOG_ERROR(GL,
                "Request was dropped because it exceeds maximum allowed size. "
                    << KVLOG(nextRequest->senderId(), nextRequest->size(), initialStorageForRequests));
    }
    delete nextRequest;
    requestsQueueOfPrimary.pop();
    nextRequest = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
  }

  if (pp->numberOfRequests() == 0) {
    LOG_WARN(GL, "No client requests added to PrePrepare batch. Nothing to send.");
    return;
  }

  pp->finishAddingRequests();
  startConsensusProcess(pp);
}

void ReplicaImp::startConsensusProcess(PrePrepareMsg *pp) {
  if (!isCurrentPrimary()) return;
  auto firstPath = pp->firstPath();
  if (config_.debugStatisticsEnabled) {
    DebugStatistics::onSendPrePrepareMessage(pp->numberOfRequests(), requestsQueueOfPrimary.size());
  }
  primaryLastUsedSeqNum++;
  metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
  SCOPED_MDC_SEQ_NUM(std::to_string(primaryLastUsedSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(firstPath));
  {
    LOG_INFO(CNSUS,
             "Sending PrePrepare with the following payload of the following correlation ids ["
                 << pp->getBatchCorrelationIdAsString() << "]");
  }
  SeqNumInfo &seqNumInfo = mainLog->get(primaryLastUsedSeqNum);
  seqNumInfo.addSelfMsg(pp);

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
    ps_->setPrePrepareMsgInSeqNumWindow(primaryLastUsedSeqNum, pp);
    if (firstPath == CommitPath::SLOW) ps_->setSlowStartedInSeqNumWindow(primaryLastUsedSeqNum, true);
    ps_->endWriteTran();
  }

  for (ReplicaId x : repsInfo->idsOfPeerReplicas()) {
    sendRetransmittableMsgToReplica(pp, x, primaryLastUsedSeqNum);
  }

  if (firstPath == CommitPath::SLOW) {
    seqNumInfo.startSlowPath();
    metric_slow_path_count_.Get().Inc();
    sendPreparePartial(seqNumInfo);
  } else {
    sendPartialProof(seqNumInfo);
  }
}

void ReplicaImp::sendInternalNoopPrePrepareMsg(CommitPath firstPath) {
  PrePrepareMsg *pp = new PrePrepareMsg(config_.replicaId, curView, (primaryLastUsedSeqNum + 1), firstPath, 0);
  startConsensusProcess(pp);
}

void ReplicaImp::bringTheSystemToCheckpointBySendingNoopCommands(SeqNum seqNumToStopAt, CommitPath firstPath) {
  if (!isCurrentPrimary()) return;
  while (primaryLastUsedSeqNum != seqNumToStopAt) {
    sendInternalNoopPrePrepareMsg(firstPath);
  }
}

bool ReplicaImp::isSeqNumToStopAt(SeqNum seq_num) {
  // There might be a race condition between the time the replica starts to the time the controlStateManager is
  // initiated.
  if (!controlStateManager_) return false;
  if (seqNumToStopAt_ > 0 && seq_num == seqNumToStopAt_) return true;
  if (seqNumToStopAt_ > 0 && seq_num > seqNumToStopAt_) return false;
  auto seq_num_to_stop_at = controlStateManager_->getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value()) {
    seqNumToStopAt_ = seq_num_to_stop_at.value();
    if (seqNumToStopAt_ == seq_num) return true;
  }

  return false;
}
template <typename T>
bool ReplicaImp::relevantMsgForActiveView(const T *msg) {
  const SeqNum msgSeqNum = msg->seqNumber();
  const ViewNum msgViewNum = msg->viewNumber();

  const bool isCurrentViewActive = currentViewIsActive();
  if (isCurrentViewActive && (msgViewNum == curView) && (msgSeqNum > strictLowerBoundOfSeqNums) &&
      (mainLog->insideActiveWindow(msgSeqNum))) {
    ConcordAssertGT(msgSeqNum, lastStableSeqNum);
    ConcordAssertLE(msgSeqNum, lastStableSeqNum + kWorkWindowSize);

    return true;
  } else if (!isCurrentViewActive) {
    LOG_INFO(GL,
             "My current view is not active, ignoring msg."
                 << KVLOG(curView, isCurrentViewActive, msg->senderId(), msgSeqNum, msgViewNum));
    return false;
  } else {
    const SeqNum activeWindowStart = mainLog->currentActiveWindow().first;
    const SeqNum activeWindowEnd = mainLog->currentActiveWindow().second;
    const bool myReplicaMayBeBehind = (curView < msgViewNum) || (msgSeqNum > activeWindowEnd);
    if (myReplicaMayBeBehind) {
      onReportAboutAdvancedReplica(msg->senderId(), msgSeqNum, msgViewNum);
      LOG_INFO(GL,
               "Msg is not relevant for my current view. The sending replica may be in advance."
                   << KVLOG(curView,
                            isCurrentViewActive,
                            msg->senderId(),
                            msgSeqNum,
                            msgViewNum,
                            activeWindowStart,
                            activeWindowEnd));
    } else {
      const bool msgReplicaMayBeBehind = (curView > msgViewNum) || (msgSeqNum + kWorkWindowSize < activeWindowStart);

      if (msgReplicaMayBeBehind) {
        onReportAboutLateReplica(msg->senderId(), msgSeqNum, msgViewNum);
        LOG_INFO(
            GL,
            "Msg is not relevant for my current view. The sending replica may be behind." << KVLOG(curView,
                                                                                                   isCurrentViewActive,
                                                                                                   msg->senderId(),
                                                                                                   msgSeqNum,
                                                                                                   msgViewNum,
                                                                                                   activeWindowStart,
                                                                                                   activeWindowEnd));
      }
    }
    return false;
  }
}

template <>
void ReplicaImp::onMessage<PrePrepareMsg>(PrePrepareMsg *msg) {
  if (isSeqNumToStopAt(lastExecutedSeqNum)) {
    LOG_INFO(GL,
             "Ignoring PrePrepareMsg because system is stopped at checkpoint pending control state operation (upgrade, "
             "etc...)");
    return;
  }
  metric_received_pre_prepares_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_DEBUG(GL, KVLOG(msg->senderId(), msg->size()));
  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "handle_bft_preprepare");
  span.setTag("rid", config_.replicaId);
  span.setTag("seq_num", msgSeqNum);

  if (!currentViewIsActive() && viewsManager->waitingForMsgs() && msgSeqNum > lastStableSeqNum) {
    ConcordAssert(!msg->isNull());  // we should never send (and never accept) null PrePrepare message

    if (viewsManager->addPotentiallyMissingPP(msg, lastStableSeqNum)) {
      LOG_INFO(GL, "PrePrepare added to views manager. " << KVLOG(lastStableSeqNum));
      tryToEnterView();
    } else {
      LOG_INFO(GL, "PrePrepare discarded.");
    }

    return;  // TODO(GG): memory deallocation is confusing .....
  }

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg) && (msg->senderId() == currentPrimary())) {
    sendAckIfNeeded(msg, msg->senderId(), msgSeqNum);
    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);
    const bool slowStarted = (msg->firstPath() == CommitPath::SLOW || seqNumInfo.slowPathStarted());

    // For MDC it doesn't matter which type of fast path
    SCOPED_MDC_PATH(CommitPathToMDCString(slowStarted ? CommitPath::SLOW : CommitPath::OPTIMISTIC_FAST));
    if (seqNumInfo.addMsg(msg)) {
      {
        LOG_INFO(CNSUS,
                 "PrePrepare with the following correlation IDs [" << msg->getBatchCorrelationIdAsString() << "]");
      }
      msgAdded = true;

      if (ps_) {
        ps_->beginWriteTran();
        ps_->setPrePrepareMsgInSeqNumWindow(msgSeqNum, msg);
        if (slowStarted) ps_->setSlowStartedInSeqNumWindow(msgSeqNum, true);
        ps_->endWriteTran();
      }

      if (!slowStarted)  // TODO(GG): make sure we correctly handle a situation where StartSlowCommitMsg is handled
                         // before PrePrepareMsg
      {
        sendPartialProof(seqNumInfo);
      } else {
        seqNumInfo.startSlowPath();
        metric_slow_path_count_.Get().Inc();
        sendPreparePartial(seqNumInfo);
        ;
      }
    }
  }

  if (!msgAdded) delete msg;
}

void ReplicaImp::tryToStartSlowPaths() {
  if (!isCurrentPrimary() || isCollectingState() || !currentViewIsActive())
    return;  // TODO(GG): consider to stop the related timer when this method is not needed (to avoid useless
             // invocations)

  const SeqNum minSeqNum = lastExecutedSeqNum + 1;
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));

  if (minSeqNum > lastStableSeqNum + kWorkWindowSize) {
    LOG_INFO(GL,
             "Try to start slow path: minSeqNum > lastStableSeqNum + kWorkWindowSize."
                 << KVLOG(minSeqNum, lastStableSeqNum, kWorkWindowSize));
    return;
  }

  const SeqNum maxSeqNum = primaryLastUsedSeqNum;

  ConcordAssertLE(maxSeqNum, lastStableSeqNum + kWorkWindowSize);
  ConcordAssertLE(minSeqNum, maxSeqNum + 1);

  if (minSeqNum > maxSeqNum) return;

  sendCheckpointIfNeeded();  // TODO(GG): TBD - do we want it here ?

  const Time currTime = getMonotonicTime();

  for (SeqNum i = minSeqNum; i <= maxSeqNum; i++) {
    SeqNumInfo &seqNumInfo = mainLog->get(i);

    if (seqNumInfo.partialProofs().hasFullProof() ||                          // already has a full proof
        seqNumInfo.slowPathStarted() ||                                       // slow path has already  started
        seqNumInfo.partialProofs().getSelfPartialCommitProof() == nullptr ||  // did not start a fast path
        (!seqNumInfo.hasPrePrepareMsg()))
      continue;  // slow path is not needed

    const Time timeOfPartProof = seqNumInfo.partialProofs().getTimeOfSelfPartialProof();

    if (currTime - timeOfPartProof < milliseconds(controller->timeToStartSlowPathMilli())) break;
    SCOPED_MDC_SEQ_NUM(std::to_string(i));
    SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));
    LOG_INFO(CNSUS,
             "Primary initiates slow path for seqNum="
                 << i << " (currTime=" << duration_cast<microseconds>(currTime.time_since_epoch()).count()
                 << " timeOfPartProof=" << duration_cast<microseconds>(timeOfPartProof.time_since_epoch()).count()
                 << " threshold for degradation [" << controller->timeToStartSlowPathMilli() << "ms]");

    controller->onStartingSlowCommit(i);

    seqNumInfo.startSlowPath();
    metric_slow_path_count_.Get().Inc();

    if (ps_) {
      ps_->beginWriteTran();
      ps_->setSlowStartedInSeqNumWindow(i, true);
      ps_->endWriteTran();
    }

    // send StartSlowCommitMsg to all replicas

    StartSlowCommitMsg *startSlow = new StartSlowCommitMsg(config_.replicaId, curView, i);

    for (ReplicaId x : repsInfo->idsOfPeerReplicas()) {
      sendRetransmittableMsgToReplica(startSlow, x, i);
    }

    delete startSlow;

    sendPreparePartial(seqNumInfo);
  }
}

void ReplicaImp::tryToAskForMissingInfo() {
  if (!currentViewIsActive() || isCollectingState()) return;

  ConcordAssertLE(maxSeqNumTransferredFromPrevViews, lastStableSeqNum + kWorkWindowSize);

  const bool recentViewChange = (maxSeqNumTransferredFromPrevViews > lastStableSeqNum);

  SeqNum minSeqNum = 0;
  SeqNum maxSeqNum = 0;

  if (!recentViewChange) {
    const int16_t searchWindow = 4;  // TODO(GG): TBD - read from configuration
    minSeqNum = lastExecutedSeqNum + 1;
    maxSeqNum = std::min(minSeqNum + searchWindow - 1, lastStableSeqNum + kWorkWindowSize);
  } else {
    const int16_t searchWindow = 32;  // TODO(GG): TBD - read from configuration
    minSeqNum = lastStableSeqNum + 1;
    while (minSeqNum <= lastStableSeqNum + kWorkWindowSize) {
      SeqNumInfo &seqNumInfo = mainLog->get(minSeqNum);
      if (!seqNumInfo.isCommitted__gg()) break;
      minSeqNum++;
    }
    maxSeqNum = std::min(minSeqNum + searchWindow - 1, lastStableSeqNum + kWorkWindowSize);
  }

  if (minSeqNum > lastStableSeqNum + kWorkWindowSize) return;

  const Time curTime = getMonotonicTime();

  SeqNum lastRelatedSeqNum = 0;

  // TODO(GG): improve/optimize the following loops

  for (SeqNum i = minSeqNum; i <= maxSeqNum; i++) {
    ConcordAssert(mainLog->insideActiveWindow(i));

    const SeqNumInfo &seqNumInfo = mainLog->get(i);

    Time t = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();

    const Time lastInfoRequest = seqNumInfo.getTimeOfLastInfoRequest();

    if ((t < lastInfoRequest)) t = lastInfoRequest;

    if (t != MinTime && (t < curTime)) {
      auto diffMilli = duration_cast<milliseconds>(curTime - t);
      if (diffMilli.count() >= dynamicUpperLimitOfRounds->upperLimit()) lastRelatedSeqNum = i;
    }
  }

  for (SeqNum i = minSeqNum; i <= lastRelatedSeqNum; i++) {
    if (!recentViewChange)
      tryToSendReqMissingDataMsg(i);
    else
      tryToSendReqMissingDataMsg(i, true, currentPrimary());
  }
}

template <>
void ReplicaImp::onMessage<StartSlowCommitMsg>(StartSlowCommitMsg *msg) {
  metric_received_start_slow_commits_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_INFO(GL, " ");

  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_start_slow_commit_msg");
  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, currentPrimary(), msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    if (!seqNumInfo.slowPathStarted() && !seqNumInfo.isPrepared()) {
      LOG_INFO(GL, "Start slow path.");

      seqNumInfo.startSlowPath();
      metric_slow_path_count_.Get().Inc();

      if (ps_) {
        ps_->beginWriteTran();
        ps_->setSlowStartedInSeqNumWindow(msgSeqNum, true);
        ps_->endWriteTran();
      }

      if (seqNumInfo.hasPrePrepareMsg() == false)
        tryToSendReqMissingDataMsg(msgSeqNum);
      else
        sendPreparePartial(seqNumInfo);
    }
  }

  delete msg;
}

void ReplicaImp::sendPartialProof(SeqNumInfo &seqNumInfo) {
  PartialProofsSet &partialProofs = seqNumInfo.partialProofs();

  if (!seqNumInfo.hasPrePrepareMsg()) return;

  PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
  Digest &ppDigest = pp->digestOfRequests();
  const SeqNum seqNum = pp->seqNumber();

  if (!partialProofs.hasFullProof()) {
    // send PartialCommitProofMsg to all collectors

    PartialCommitProofMsg *part = partialProofs.getSelfPartialCommitProof();

    if (part == nullptr) {
      IThresholdSigner *commitSigner = nullptr;
      CommitPath commitPath = pp->firstPath();

      ConcordAssertOR((config_.cVal != 0), (commitPath != CommitPath::FAST_WITH_THRESHOLD));

      if ((commitPath == CommitPath::FAST_WITH_THRESHOLD) && (config_.cVal > 0))
        commitSigner = config_.thresholdSignerForCommit;
      else
        commitSigner = config_.thresholdSignerForOptimisticCommit;

      Digest tmpDigest;
      Digest::calcCombination(ppDigest, curView, seqNum, tmpDigest);

      const auto &span_context = pp->spanContext<std::remove_pointer<decltype(pp)>::type>();
      part = new PartialCommitProofMsg(
          config_.replicaId, curView, seqNum, commitPath, tmpDigest, commitSigner, span_context);
      partialProofs.addSelfMsgAndPPDigest(part, tmpDigest);
    }

    partialProofs.setTimeOfSelfPartialProof(getMonotonicTime());

    // send PartialCommitProofMsg (only if, from my point of view, at most MaxConcurrentFastPaths are in progress)
    if (seqNum <= lastExecutedSeqNum + MaxConcurrentFastPaths) {
      // TODO(GG): improve the following code (use iterators instead of a simple array)
      int8_t numOfRouters = 0;
      ReplicaId routersArray[2];

      repsInfo->getCollectorsForPartialProofs(curView, seqNum, &numOfRouters, routersArray);

      for (int i = 0; i < numOfRouters; i++) {
        ReplicaId router = routersArray[i];
        if (router != config_.replicaId) {
          sendRetransmittableMsgToReplica(part, router, seqNum);
        }
      }
    }
  }
}

void ReplicaImp::sendPreparePartial(SeqNumInfo &seqNumInfo) {
  ConcordAssert(currentViewIsActive());

  if (seqNumInfo.getSelfPreparePartialMsg() == nullptr && seqNumInfo.hasPrePrepareMsg() && !seqNumInfo.isPrepared()) {
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();

    ConcordAssertNE(pp, nullptr);

    LOG_DEBUG(GL, "Sending PreparePartialMsg. " << KVLOG(pp->seqNumber()));

    const auto &span_context = pp->spanContext<std::remove_pointer<decltype(pp)>::type>();
    PreparePartialMsg *p = PreparePartialMsg::create(curView,
                                                     pp->seqNumber(),
                                                     config_.replicaId,
                                                     pp->digestOfRequests(),
                                                     config_.thresholdSignerForSlowPathCommit,
                                                     span_context);
    seqNumInfo.addSelfMsg(p);

    if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(p, currentPrimary(), pp->seqNumber());
  }
}

void ReplicaImp::sendCommitPartial(const SeqNum s) {
  ConcordAssert(currentViewIsActive());
  ConcordAssert(mainLog->insideActiveWindow(s));
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(s));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  SeqNumInfo &seqNumInfo = mainLog->get(s);
  PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();

  ConcordAssert(seqNumInfo.isPrepared());
  ConcordAssertNE(pp, nullptr);
  ConcordAssertEQ(pp->seqNumber(), s);

  if (seqNumInfo.committedOrHasCommitPartialFromReplica(config_.replicaId)) return;  // not needed

  LOG_DEBUG(CNSUS, "Sending CommitPartialMsg");

  Digest d;
  Digest::digestOfDigest(pp->digestOfRequests(), d);

  auto prepareFullMsg = seqNumInfo.getValidPrepareFullMsg();

  CommitPartialMsg *c =
      CommitPartialMsg::create(curView,
                               s,
                               config_.replicaId,
                               d,
                               config_.thresholdSignerForSlowPathCommit,
                               prepareFullMsg->spanContext<std::remove_pointer<decltype(prepareFullMsg)>::type>());
  seqNumInfo.addSelfCommitPartialMsgAndDigest(c, d);

  if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(c, currentPrimary(), s);
}

template <>
void ReplicaImp::onMessage<PartialCommitProofMsg>(PartialCommitProofMsg *msg) {
  metric_received_partial_commit_proofs_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const SeqNum msgView = msg->viewNumber();
  const NodeIdType msgSender = msg->senderId();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(msg->commitPath()));
  ConcordAssert(repsInfo->isIdOfPeerReplica(msgSender));
  ConcordAssert(repsInfo->isCollectorForPartialProofs(msgView, msgSeqNum));

  LOG_DEBUG(GL, KVLOG(msgSender, msg->size()));

  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_partial_commit_proof_msg");
  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    if (msgSeqNum > lastExecutedSeqNum) {
      SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);
      PartialProofsSet &pps = seqNumInfo.partialProofs();

      if (pps.addMsg(msg)) {
        return;
      }
    }
  }

  delete msg;
  return;
}

template <>
void ReplicaImp::onMessage<FullCommitProofMsg>(FullCommitProofMsg *msg) {
  metric_received_full_commit_proofs_.Get().Inc();

  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_full_commit_proof_msg");
  const SeqNum msgSeqNum = msg->seqNumber();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msg->seqNumber()));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::OPTIMISTIC_FAST));

  LOG_DEBUG(CNSUS, "Reached consensus, Received FullCommitProofMsg message");

  if (relevantMsgForActiveView(msg)) {
    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);
    PartialProofsSet &pps = seqNumInfo.partialProofs();

    if (!pps.hasFullProof() && pps.addMsg(msg))  // TODO(GG): consider to verify the signature in another thread
    {
      ConcordAssert(seqNumInfo.hasPrePrepareMsg());

      seqNumInfo.forceComplete();  // TODO(GG): remove forceComplete() (we know that  seqNumInfo is committed because
                                   // of the  FullCommitProofMsg message)

      if (ps_) {
        ps_->beginWriteTran();
        ps_->setFullCommitProofMsgInSeqNumWindow(msgSeqNum, msg);
        ps_->setForceCompletedInSeqNumWindow(msgSeqNum, true);
        ps_->endWriteTran();
      }

      if (msg->senderId() == config_.replicaId) sendToAllOtherReplicas(msg);

      const bool askForMissingInfoAboutCommittedItems =
          (msgSeqNum > lastExecutedSeqNum + config_.concurrencyLevel);  // TODO(GG): check/improve this logic

      auto execution_span = concordUtils::startChildSpan("bft_execute_committed_reqs", span);
      executeNextCommittedRequests(execution_span, askForMissingInfoAboutCommittedItems);
      return;
    } else if (pps.hasFullProof()) {
      const auto fullProofCollectorId = pps.getFullProof()->senderId();
      LOG_INFO(GL,
               "FullCommitProof for seq num " << msgSeqNum << " was already received from replica "
                                              << fullProofCollectorId << " and has been processed."
                                              << " Ignoring the FullCommitProof from replica " << msg->senderId());
    }
  }

  delete msg;
  return;
}

void ReplicaImp::onInternalMsg(InternalMessage &&msg) {
  metric_received_internal_msgs_.Get().Inc();

  // Handle a full commit proof sent by self
  if (auto *fcp = std::get_if<FullCommitProofMsg *>(&msg)) {
    return onInternalMsg(*fcp);
  }

  // Handle prepare related internal messages
  if (auto *csf = std::get_if<CombinedSigFailedInternalMsg>(&msg)) {
    return onPrepareCombinedSigFailed(csf->seqNumber, csf->view, csf->replicasWithBadSigs);
  }
  if (auto *css = std::get_if<CombinedSigSucceededInternalMsg>(&msg)) {
    return onPrepareCombinedSigSucceeded(
        css->seqNumber, css->view, css->combinedSig.data(), css->combinedSig.size(), css->span_context_);
  }
  if (auto *vcs = std::get_if<VerifyCombinedSigResultInternalMsg>(&msg)) {
    return onPrepareVerifyCombinedSigResult(vcs->seqNumber, vcs->view, vcs->isValid);
  }

  // Handle Commit related internal messages
  if (auto *ccss = std::get_if<CombinedCommitSigSucceededInternalMsg>(&msg)) {
    return onCommitCombinedSigSucceeded(
        ccss->seqNumber, ccss->view, ccss->combinedSig.data(), ccss->combinedSig.size(), ccss->span_context_);
  }
  if (auto *ccsf = std::get_if<CombinedCommitSigFailedInternalMsg>(&msg)) {
    return onCommitCombinedSigFailed(ccsf->seqNumber, ccsf->view, ccsf->replicasWithBadSigs);
  }
  if (auto *vccs = std::get_if<VerifyCombinedCommitSigResultInternalMsg>(&msg)) {
    return onCommitVerifyCombinedSigResult(vccs->seqNumber, vccs->view, vccs->isValid);
  }

  // Handle a response from a RetransmissionManagerJob
  if (auto *rpr = std::get_if<RetranProcResultInternalMsg>(&msg)) {
    onRetransmissionsProcessingResults(rpr->lastStableSeqNum, rpr->view, rpr->suggestedRetransmissions);
    return retransmissionsManager->OnProcessingComplete();
  }

  // Handle a status request for the diagnostics subsystem
  if (auto *get_status = std::get_if<GetStatus>(&msg)) {
    return onInternalMsg(*get_status);
  }

  ConcordAssert(false);
}

std::string ReplicaImp::getReplicaState() const {
  auto primary = getReplicasInfo().primaryOfView(curView);
  std::ostringstream oss;
  oss << "Replica ID: " << getReplicasInfo().myId() << std::endl;
  oss << "Primary: " << primary << std::endl;
  oss << "View Change: "
      << KVLOG(viewChangeProtocolEnabled,
               autoPrimaryRotationEnabled,
               curView,
               utcstr(timeOfLastViewEntrance),
               lastAgreedView,
               utcstr(timeOfLastAgreedView),
               viewChangeTimerMilli,
               autoPrimaryRotationTimerMilli)
      << std::endl
      << std::endl;
  oss << "Sequence Numbers: "
      << KVLOG(primaryLastUsedSeqNum,
               lastStableSeqNum,
               strictLowerBoundOfSeqNums,
               maxSeqNumTransferredFromPrevViews,
               mainLog->currentActiveWindow().first,
               mainLog->currentActiveWindow().second,
               lastViewThatTransferredSeqNumbersFullyExecuted)
      << std::endl
      << std::endl;
  oss << "Other: "
      << KVLOG(restarted_,
               requestsQueueOfPrimary.size(),
               maxNumberOfPendingRequestsInRecentHistory,
               batchingFactor,
               utcstr(lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas),
               utcstr(timeOfLastStateSynch),
               recoveringFromExecutionOfRequests,
               checkpointsLog->currentActiveWindow().first,
               checkpointsLog->currentActiveWindow().second,
               clientsManager->numberOfRequiredReservedPages())
      << std::endl;
  return oss.str();
}

void ReplicaImp::onInternalMsg(GetStatus &status) const {
  if (status.key == "replica") {
    return status.output.set_value(getReplicaState());
  }

  if (status.key == "state-transfer") {
    return status.output.set_value(stateTransfer->getStatus());
  }
  // We must always return something to unblock the future.
  return status.output.set_value("** - Invalid Key - **");
}

void ReplicaImp::onInternalMsg(FullCommitProofMsg *msg) {
  if (isCollectingState() || (!currentViewIsActive()) || (curView != msg->viewNumber()) ||
      (!mainLog->insideActiveWindow(msg->seqNumber()))) {
    delete msg;
    return;
  }
  onMessage(msg);
}

template <>
void ReplicaImp::onMessage<PreparePartialMsg>(PreparePartialMsg *msg) {
  metric_received_prepare_partials_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  bool msgAdded = false;

  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_prepare_partial_msg");

  if (relevantMsgForActiveView(msg)) {
    ConcordAssert(isCurrentPrimary());

    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG(GL, "Received relevant PreparePartialMsg." << KVLOG(msgSender));

    controller->onMessage(msg);

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();

    if (fcp != nullptr) {
      send(fcp, msgSender);
    } else if (commitFull != nullptr) {
      send(commitFull, msgSender);
    } else if (preFull != nullptr) {
      send(preFull, msgSender);
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_DEBUG(GL,
              "Node " << config_.replicaId << " ignored the PreparePartialMsg from node " << msgSender << " (seqNumber "
                      << msgSeqNum << ")");
    delete msg;
  }
}

template <>
void ReplicaImp::onMessage<CommitPartialMsg>(CommitPartialMsg *msg) {
  metric_received_commit_partials_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  bool msgAdded = false;

  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_commit_partial_msg");
  if (relevantMsgForActiveView(msg)) {
    ConcordAssert(isCurrentPrimary());

    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG(GL, "Received CommitPartialMsg from node " << msgSender);

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    if (fcp != nullptr) {
      send(fcp, msgSender);
    } else if (commitFull != nullptr) {
      send(commitFull, msgSender);
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_INFO(GL, "Ignored CommitPartialMsg. " << KVLOG(msgSender));
    delete msg;
  }
}

template <>
void ReplicaImp::onMessage<PrepareFullMsg>(PrepareFullMsg *msg) {
  metric_received_prepare_fulls_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));
  bool msgAdded = false;

  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_preprare_full_msg");
  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG(GL, "received PrepareFullMsg");

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();

    if (fcp != nullptr) {
      send(fcp, msgSender);
    } else if (commitFull != nullptr) {
      send(commitFull, msgSender);
    } else if (preFull != nullptr) {
      // nop
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_DEBUG(GL, "Ignored PrepareFullMsg." << KVLOG(msgSender));
    delete msg;
  }
}
template <>
void ReplicaImp::onMessage<CommitFullMsg>(CommitFullMsg *msg) {
  metric_received_commit_fulls_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));
  bool msgAdded = false;

  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_commit_full_msg");
  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG(GL, "Received CommitFullMsg" << KVLOG(msgSender));

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    if (fcp != nullptr) {
      send(fcp,
           msgSender);  // TODO(GG): do we really want to send this message ? (msgSender already has a CommitFullMsg
                        // for the same seq number)
    } else if (commitFull != nullptr) {
      // nop
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_DEBUG(GL,
              "Node " << config_.replicaId << " ignored the CommitFullMsg from node " << msgSender << " (seqNumber "
                      << msgSeqNum << ")");
    delete msg;
  }
}

void ReplicaImp::onPrepareCombinedSigFailed(SeqNum seqNumber,
                                            ViewNum view,
                                            const std::set<uint16_t> &replicasWithBadSigs) {
  LOG_DEBUG(GL, KVLOG(seqNumber, view));

  if ((isCollectingState()) || (!currentViewIsActive()) || (curView != view) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG(GL, "Dropping irrelevant signature." << KVLOG(seqNumber, view));

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, view, replicasWithBadSigs);

  // TODO(GG): add logic that handles bad replicas ...
}

void ReplicaImp::onPrepareCombinedSigSucceeded(SeqNum seqNumber,
                                               ViewNum view,
                                               const char *combinedSig,
                                               uint16_t combinedSigLen,
                                               const concordUtils::SpanContext &span_context) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));
  LOG_DEBUG(GL, KVLOG(view));

  if ((isCollectingState()) || (!currentViewIsActive()) || (curView != view) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(GL, "Not sending prepare full: Invalid state, view, or sequence number." << KVLOG(view, curView));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, view, combinedSig, combinedSigLen, span_context);

  FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

  PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();

  ConcordAssertNE(preFull, nullptr);

  if (fcp != nullptr) return;  // don't send if we already have FullCommitProofMsg
  LOG_DEBUG(CNSUS, "Sending prepare full" << KVLOG(view));
  if (ps_) {
    ps_->beginWriteTran();
    ps_->setPrepareFullMsgInSeqNumWindow(seqNumber, preFull);
    ps_->endWriteTran();
  }

  for (ReplicaId x : repsInfo->idsOfPeerReplicas()) sendRetransmittableMsgToReplica(preFull, x, seqNumber);

  ConcordAssert(seqNumInfo.isPrepared());

  sendCommitPartial(seqNumber);
}

void ReplicaImp::onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) {
  LOG_DEBUG(GL, KVLOG(view));

  if ((isCollectingState()) || (!currentViewIsActive()) || (curView != view) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(GL,
             "Not sending commit partial: Invalid state, view, or sequence number."
                 << KVLOG(seqNumber, view, curView, mainLog->insideActiveWindow(seqNumber)));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCombinedPrepareSigVerification(seqNumber, view, isValid);

  if (!isValid) return;  // TODO(GG): we should do something about the replica that sent this invalid message

  FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

  if (fcp != nullptr) return;  // don't send if we already have FullCommitProofMsg

  ConcordAssert(seqNumInfo.isPrepared());

  if (ps_) {
    PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();
    ConcordAssertNE(preFull, nullptr);
    ps_->beginWriteTran();
    ps_->setPrepareFullMsgInSeqNumWindow(seqNumber, preFull);
    ps_->endWriteTran();
  }

  sendCommitPartial(seqNumber);
}

void ReplicaImp::onCommitCombinedSigFailed(SeqNum seqNumber,
                                           ViewNum view,
                                           const std::set<uint16_t> &replicasWithBadSigs) {
  LOG_DEBUG(GL, KVLOG(seqNumber, view));

  if ((isCollectingState()) || (!currentViewIsActive()) || (curView != view) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG(GL, "Invalid state, view, or sequence number." << KVLOG(seqNumber, view, curView));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCommitSignaturesProcessing(seqNumber, view, replicasWithBadSigs);

  // TODO(GG): add logic that handles bad replicas ...
}

void ReplicaImp::onCommitCombinedSigSucceeded(SeqNum seqNumber,
                                              ViewNum view,
                                              const char *combinedSig,
                                              uint16_t combinedSigLen,
                                              const concordUtils::SpanContext &span_context) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));
  LOG_DEBUG(GL, KVLOG(view));

  if (isCollectingState() || (!currentViewIsActive()) || (curView != view) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(GL,
             "Not sending full commit: Invalid state, view, or sequence number."
                 << KVLOG(view, curView, mainLog->insideActiveWindow(seqNumber)));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCommitSignaturesProcessing(seqNumber, view, combinedSig, combinedSigLen, span_context);

  FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();
  CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

  ConcordAssertNE(commitFull, nullptr);
  if (fcp != nullptr) return;  // ignore if we already have FullCommitProofMsg
  LOG_DEBUG(CNSUS, "Sending full commit" << KVLOG(view));
  if (ps_) {
    ps_->beginWriteTran();
    ps_->setCommitFullMsgInSeqNumWindow(seqNumber, commitFull);
    ps_->endWriteTran();
  }

  for (ReplicaId x : repsInfo->idsOfPeerReplicas()) sendRetransmittableMsgToReplica(commitFull, x, seqNumber);

  ConcordAssert(seqNumInfo.isCommitted__gg());

  bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum + config_.concurrencyLevel);

  auto span = concordUtils::startChildSpanFromContext(
      commitFull->spanContext<std::remove_pointer<decltype(commitFull)>::type>(), "bft_execute_committed_reqs");
  executeNextCommittedRequests(span, askForMissingInfoAboutCommittedItems);
}

void ReplicaImp::onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  LOG_DEBUG(GL, KVLOG(view));

  if (isCollectingState() || (!currentViewIsActive()) || (curView != view) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(
        GL, "Invalid state, view, or sequence number." << KVLOG(view, curView, mainLog->insideActiveWindow(seqNumber)));
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCombinedCommitSigVerification(seqNumber, view, isValid);

  if (!isValid) return;  // TODO(GG): we should do something about the replica that sent this invalid message

  ConcordAssert(seqNumInfo.isCommitted__gg());

  CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();
  ConcordAssertNE(commitFull, nullptr);
  if (ps_) {
    ps_->beginWriteTran();
    ps_->setCommitFullMsgInSeqNumWindow(seqNumber, commitFull);
    ps_->endWriteTran();
  }
  LOG_DEBUG(CNSUS, "Request commited, proceeding to try to execute" << KVLOG(view));
  auto span = concordUtils::startChildSpanFromContext(
      commitFull->spanContext<std::remove_pointer<decltype(commitFull)>::type>(), "bft_execute_committed_reqs");
  bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum + config_.concurrencyLevel);
  executeNextCommittedRequests(span, askForMissingInfoAboutCommittedItems);
}

template <>
void ReplicaImp::onMessage<CheckpointMsg>(CheckpointMsg *msg) {
  metric_received_checkpoints_.Get().Inc();
  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgSeqNum = msg->seqNumber();
  const Digest msgDigest = msg->digestOfState();
  const bool msgIsStable = msg->isStableState();
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_INFO(GL, "Received checkpoint message from node. " << KVLOG(msgSenderId, msg->size(), msgIsStable, msgDigest));
  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_checkpoint_msg");

  if ((msgSeqNum > lastStableSeqNum) && (msgSeqNum <= lastStableSeqNum + kWorkWindowSize)) {
    ConcordAssert(mainLog->insideActiveWindow(msgSeqNum));
    CheckpointInfo &checkInfo = checkpointsLog->get(msgSeqNum);
    bool msgAdded = checkInfo.addCheckpointMsg(msg, msg->senderId());

    if (msgAdded) {
      LOG_DEBUG(GL, "Added checkpoint message: " << KVLOG(msgSenderId));
    }

    if (checkInfo.isCheckpointCertificateComplete()) {
      ConcordAssertNE(checkInfo.selfCheckpointMsg(), nullptr);
      onSeqNumIsStable(msgSeqNum);

      return;
    }
  } else if (checkpointsLog->insideActiveWindow(msgSeqNum)) {
    CheckpointInfo &checkInfo = checkpointsLog->get(msgSeqNum);
    bool msgAdded = checkInfo.addCheckpointMsg(msg, msg->senderId());
    if (msgAdded && checkInfo.isCheckpointSuperStable()) {
      onSeqNumIsSuperStable(msgSeqNum);
    }
  } else {
    delete msg;
  }

  bool askForStateTransfer = false;

  if (msgIsStable && msgSeqNum > lastExecutedSeqNum) {
    auto pos = tableOfStableCheckpoints.find(msgSenderId);
    if (pos == tableOfStableCheckpoints.end() || pos->second->seqNumber() < msgSeqNum) {
      if (pos != tableOfStableCheckpoints.end()) delete pos->second;
      CheckpointMsg *x = new CheckpointMsg(msgSenderId, msgSeqNum, msgDigest, msgIsStable);
      tableOfStableCheckpoints[msgSenderId] = x;

      LOG_INFO(GL, "Added stable Checkpoint message to tableOfStableCheckpoints: " << KVLOG(msgSenderId));

      if ((uint16_t)tableOfStableCheckpoints.size() >= config_.fVal + 1) {
        uint16_t numRelevant = 0;
        uint16_t numRelevantAboveWindow = 0;
        auto tableItrator = tableOfStableCheckpoints.begin();
        while (tableItrator != tableOfStableCheckpoints.end()) {
          if (tableItrator->second->seqNumber() <= lastExecutedSeqNum) {
            delete tableItrator->second;
            tableItrator = tableOfStableCheckpoints.erase(tableItrator);
          } else {
            numRelevant++;
            if (tableItrator->second->seqNumber() > lastStableSeqNum + kWorkWindowSize) numRelevantAboveWindow++;
            tableItrator++;
          }
        }
        ConcordAssertEQ(numRelevant, tableOfStableCheckpoints.size());

        LOG_DEBUG(GL, KVLOG(numRelevant, numRelevantAboveWindow));

        if (numRelevantAboveWindow >= config_.fVal + 1) {
          askForStateTransfer = true;
        } else if (numRelevant >= config_.fVal + 1) {
          Time timeOfLastCommit = MinTime;
          if (mainLog->insideActiveWindow(lastExecutedSeqNum))
            timeOfLastCommit = mainLog->get(lastExecutedSeqNum).lastUpdateTimeOfCommitMsgs();

          if ((getMonotonicTime() - timeOfLastCommit) >
              (milliseconds(timeToWaitBeforeStartingStateTransferInMainWindowMilli))) {
            askForStateTransfer = true;
          }
        }
      }
    }
  }

  if (askForStateTransfer) {
    LOG_INFO(GL, "Call to startCollectingState()");

    stateTransfer->startCollectingState();
  } else if (msgSeqNum > lastStableSeqNum + kWorkWindowSize) {
    onReportAboutAdvancedReplica(msgSenderId, msgSeqNum);
  } else if (msgSeqNum + kWorkWindowSize < lastStableSeqNum) {
    onReportAboutLateReplica(msgSenderId, msgSeqNum);
  }
}
/**
 * Is sent from a read-only replica
 */
template <>
void ReplicaImp::onMessage<AskForCheckpointMsg>(AskForCheckpointMsg *msg) {
  // metric_received_checkpoints_.Get().Inc(); // TODO [TK]

  LOG_INFO(GL, "Received AskForCheckpoint message: " << KVLOG(msg->senderId(), lastStableSeqNum));

  const CheckpointInfo &checkpointInfo = checkpointsLog->get(lastStableSeqNum);
  CheckpointMsg *checkpointMsg = checkpointInfo.selfCheckpointMsg();

  if (checkpointMsg == nullptr) {
    LOG_INFO(GL, "This replica does not have the current checkpoint. " << KVLOG(msg->senderId(), lastStableSeqNum));
  } else {
    // TODO [TK] check if already sent within a configurable time period
    auto destination = msg->senderId();
    LOG_INFO(GL, "Sending CheckpointMsg: " << KVLOG(destination));
    send(checkpointMsg, msg->senderId());
  }
}

bool ReplicaImp::handledByRetransmissionsManager(const ReplicaId sourceReplica,
                                                 const ReplicaId destReplica,
                                                 const ReplicaId primaryReplica,
                                                 const SeqNum seqNum,
                                                 const uint16_t msgType) {
  ConcordAssert(retransmissionsLogicEnabled);

  if (sourceReplica == destReplica) return false;

  const bool sourcePrimary = (sourceReplica == primaryReplica);

  if (sourcePrimary && ((msgType == MsgCode::PrePrepare) || (msgType == MsgCode::StartSlowCommit))) return true;

  const bool dstPrimary = (destReplica == primaryReplica);

  if (dstPrimary && ((msgType == MsgCode::PreparePartial) || (msgType == MsgCode::CommitPartial))) return true;

  //  TODO(GG): do we want to use acks for FullCommitProofMsg ?

  if (msgType == MsgCode::PartialCommitProof) {
    const bool destIsCollector =
        repsInfo->getCollectorsForPartialProofs(destReplica, curView, seqNum, nullptr, nullptr);
    if (destIsCollector) return true;
  }

  return false;
}

void ReplicaImp::sendAckIfNeeded(MessageBase *msg, const NodeIdType sourceNode, const SeqNum seqNum) {
  if (!retransmissionsLogicEnabled) return;

  if (!repsInfo->isIdOfPeerReplica(sourceNode)) return;

  if (handledByRetransmissionsManager(sourceNode, config_.replicaId, currentPrimary(), seqNum, msg->type())) {
    SimpleAckMsg *ackMsg = new SimpleAckMsg(seqNum, curView, config_.replicaId, msg->type());

    send(ackMsg, sourceNode);

    delete ackMsg;
  }
}

void ReplicaImp::sendRetransmittableMsgToReplica(MessageBase *msg,
                                                 ReplicaId destReplica,
                                                 SeqNum s,
                                                 bool ignorePreviousAcks) {
  send(msg, destReplica);

  if (!retransmissionsLogicEnabled) return;

  if (handledByRetransmissionsManager(config_.replicaId, destReplica, currentPrimary(), s, msg->type()))
    retransmissionsManager->onSend(destReplica, s, msg->type(), ignorePreviousAcks);
}

void ReplicaImp::onRetransmissionsTimer(Timers::Handle timer) {
  ConcordAssert(retransmissionsLogicEnabled);

  retransmissionsManager->tryToStartProcessing();
}

void ReplicaImp::onRetransmissionsProcessingResults(SeqNum relatedLastStableSeqNum,
                                                    const ViewNum relatedViewNumber,
                                                    const std::forward_list<RetSuggestion> &suggestedRetransmissions) {
  ConcordAssert(retransmissionsLogicEnabled);

  if (isCollectingState() || (relatedViewNumber != curView) || (!currentViewIsActive())) return;
  if (relatedLastStableSeqNum + kWorkWindowSize <= lastStableSeqNum) return;

  const uint16_t myId = config_.replicaId;
  const uint16_t primaryId = currentPrimary();

  for (const RetSuggestion &s : suggestedRetransmissions) {
    if ((s.msgSeqNum <= lastStableSeqNum) || (s.msgSeqNum > lastStableSeqNum + kWorkWindowSize)) continue;

    ConcordAssertNE(s.replicaId, myId);

    ConcordAssert(handledByRetransmissionsManager(myId, s.replicaId, primaryId, s.msgSeqNum, s.msgType));

    switch (s.msgType) {
      case MsgCode::PrePrepare: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PrePrepareMsg *msgToSend = seqNumInfo.getSelfPrePrepareMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(GL,
                  "Replica " << myId << " retransmits to replica " << s.replicaId << " PrePrepareMsg with seqNumber "
                             << s.msgSeqNum);
      } break;
      case MsgCode::PartialCommitProof: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PartialCommitProofMsg *msgToSend = seqNumInfo.partialProofs().getSelfPartialCommitProof();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(GL,
                  "Replica " << myId << " retransmits to replica " << s.replicaId
                             << " PartialCommitProofMsg with seqNumber " << s.msgSeqNum);
      } break;
        /*  TODO(GG): do we want to use acks for FullCommitProofMsg ?
         */
      case MsgCode::StartSlowCommit: {
        StartSlowCommitMsg *msgToSend = new StartSlowCommitMsg(myId, curView, s.msgSeqNum);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        delete msgToSend;
        LOG_DEBUG(GL,
                  "Replica " << myId << " retransmits to replica " << s.replicaId
                             << " StartSlowCommitMsg with seqNumber " << s.msgSeqNum);
      } break;
      case MsgCode::PreparePartial: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PreparePartialMsg *msgToSend = seqNumInfo.getSelfPreparePartialMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(GL,
                  "Replica " << myId << " retransmits to replica " << s.replicaId
                             << " PreparePartialMsg with seqNumber " << s.msgSeqNum);
      } break;
      case MsgCode::PrepareFull: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PrepareFullMsg *msgToSend = seqNumInfo.getValidPrepareFullMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(GL,
                  "Replica " << myId << " retransmits to replica " << s.replicaId << " PrepareFullMsg with seqNumber "
                             << s.msgSeqNum);
      } break;

      case MsgCode::CommitPartial: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        CommitPartialMsg *msgToSend = seqNumInfo.getSelfCommitPartialMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(GL,
                  "Replica " << myId << " retransmits to replica " << s.replicaId << " CommitPartialMsg with seqNumber "
                             << s.msgSeqNum);
      } break;

      case MsgCode::CommitFull: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        CommitFullMsg *msgToSend = seqNumInfo.getValidCommitFullMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_INFO(GL, "Retransmit CommitFullMsg: " << KVLOG(s.msgSeqNum, s.replicaId));
      } break;

      default:
        ConcordAssert(false);
    }
  }
}

template <>
void ReplicaImp::onMessage<ReplicaStatusMsg>(ReplicaStatusMsg *msg) {
  metric_received_replica_statuses_.Get().Inc();
  // TODO(GG): we need filter for msgs (to avoid denial of service attack) + avoid sending messages at a high rate.
  // TODO(GG): for some communication modules/protocols, we can also utilize information about
  // connection/disconnection.

  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handling_status_report");
  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgLastStable = msg->getLastStableSeqNum();
  const ViewNum msgViewNum = msg->getViewNumber();
  ConcordAssertEQ(msgLastStable % checkpointWindowSize, 0);

  LOG_DEBUG(GL, KVLOG(msgSenderId));

  /////////////////////////////////////////////////////////////////////////
  // Checkpoints
  /////////////////////////////////////////////////////////////////////////

  if (lastStableSeqNum > msgLastStable + kWorkWindowSize) {
    CheckpointMsg *checkMsg = checkpointsLog->get(lastStableSeqNum).selfCheckpointMsg();

    if (checkMsg == nullptr || !checkMsg->isStableState()) {
      // TODO(GG): warning
    } else {
      sendAndIncrementMetric(checkMsg, msgSenderId, metric_sent_checkpoint_msg_due_to_status_);
    }

    delete msg;
    return;
  } else if (msgLastStable > lastStableSeqNum + kWorkWindowSize) {
    tryToSendStatusReport();  // ask for help
  } else {
    // Send checkpoints that may be useful for msgSenderId
    const SeqNum beginRange =
        std::max(checkpointsLog->currentActiveWindow().first, msgLastStable + checkpointWindowSize);
    const SeqNum endRange = std::min(checkpointsLog->currentActiveWindow().second, msgLastStable + kWorkWindowSize);

    ConcordAssertEQ(beginRange % checkpointWindowSize, 0);

    if (beginRange <= endRange) {
      ConcordAssertLE(endRange - beginRange, kWorkWindowSize);

      for (SeqNum i = beginRange; i <= endRange; i = i + checkpointWindowSize) {
        CheckpointMsg *checkMsg = checkpointsLog->get(i).selfCheckpointMsg();
        if (checkMsg != nullptr) {
          sendAndIncrementMetric(checkMsg, msgSenderId, metric_sent_checkpoint_msg_due_to_status_);
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId in older view
  /////////////////////////////////////////////////////////////////////////

  if (msgViewNum < curView) {
    ViewChangeMsg *myVC = viewsManager->getMyLatestViewChangeMsg();
    ConcordAssertNE(myVC, nullptr);  // because curView>0
    sendAndIncrementMetric(myVC, msgSenderId, metric_sent_viewchange_msg_due_to_status_);
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId needes information to enter view curView
  /////////////////////////////////////////////////////////////////////////

  else if ((msgViewNum == curView) && (!msg->currentViewIsActive())) {
    auto sendViewChangeMsg = [&msg, &msgSenderId, this]() {
      if (msg->hasListOfMissingViewChangeMsgForViewChange() &&
          msg->isMissingViewChangeMsgForViewChange(config_.replicaId)) {
        ViewChangeMsg *myVC = viewsManager->getMyLatestViewChangeMsg();
        ConcordAssertNE(myVC, nullptr);
        sendAndIncrementMetric(myVC, msgSenderId, metric_sent_viewchange_msg_due_to_status_);
      }
    };

    if (isCurrentPrimary() || (repsInfo->primaryOfView(curView) == msgSenderId))  // if the primary is involved
    {
      if (!isCurrentPrimary())  // I am not the primary of curView
      {
        sendViewChangeMsg();
      } else  // I am the primary of curView
      {
        // send NewViewMsg
        if (!msg->currentViewHasNewViewMessage() && viewsManager->viewIsActive(curView)) {
          NewViewMsg *nv = viewsManager->getMyNewViewMsgForCurrentView();
          ConcordAssertNE(nv, nullptr);
          sendAndIncrementMetric(nv, msgSenderId, metric_sent_newview_msg_due_to_status_);
        }

        // send all VC msgs that can help making progress (needed because the original senders may not send
        // the ViewChangeMsg msgs used by the primary)
        // if viewsManager->viewIsActive(curView), we can send only the VC msgs which are really needed for
        // curView (see in ViewsManager)
        if (msg->hasListOfMissingViewChangeMsgForViewChange()) {
          for (auto *vcMsg : viewsManager->getViewChangeMsgsForView(curView)) {
            if (msg->isMissingViewChangeMsgForViewChange(vcMsg->idOfGeneratedReplica())) {
              send(vcMsg, msgSenderId);
            }
          }
        }
      }

      if (viewsManager->viewIsActive(curView)) {
        if (msg->hasListOfMissingPrePrepareMsgForViewChange()) {
          for (SeqNum i = msgLastStable + 1; i <= msgLastStable + kWorkWindowSize; i++) {
            if (mainLog->insideActiveWindow(i) && msg->isMissingPrePrepareMsgForViewChange(i)) {
              PrePrepareMsg *prePrepareMsg = mainLog->get(i).getPrePrepareMsg();
              if (prePrepareMsg != nullptr) {
                sendAndIncrementMetric(prePrepareMsg, msgSenderId, metric_sent_preprepare_msg_due_to_status_);
              }
            }
          }
        }
      } else  // if I am also not in curView --- In this case we take messages from viewsManager
      {
        if (msg->hasListOfMissingPrePrepareMsgForViewChange()) {
          for (SeqNum i = msgLastStable + 1; i <= msgLastStable + kWorkWindowSize; i++) {
            if (msg->isMissingPrePrepareMsgForViewChange(i)) {
              PrePrepareMsg *prePrepareMsg =
                  viewsManager->getPrePrepare(i);  // TODO(GG): we can avoid sending misleading message by using the
                                                   // digest of the expected pre prepare message
              if (prePrepareMsg != nullptr) {
                sendAndIncrementMetric(prePrepareMsg, msgSenderId, metric_sent_preprepare_msg_due_to_status_);
              }
            }
          }
        }
      }
    } else {
      sendViewChangeMsg();
    }
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId is also in view curView
  /////////////////////////////////////////////////////////////////////////

  else if ((msgViewNum == curView) && msg->currentViewIsActive()) {
    if (isCurrentPrimary()) {
      if (viewsManager->viewIsActive(curView)) {
        SeqNum beginRange =
            std::max(lastStableSeqNum + 1,
                     msg->getLastExecutedSeqNum() + 1);  // Notice that after a view change, we don't have to pass the
                                                         // PrePrepare messages from the previous view. TODO(GG): verify
        SeqNum endRange = std::min(lastStableSeqNum + kWorkWindowSize, msgLastStable + kWorkWindowSize);

        for (SeqNum i = beginRange; i <= endRange; i++) {
          if (msg->isPrePrepareInActiveWindow(i)) continue;

          PrePrepareMsg *prePrepareMsg = mainLog->get(i).getSelfPrePrepareMsg();
          if (prePrepareMsg != nullptr) {
            sendAndIncrementMetric(prePrepareMsg, msgSenderId, metric_sent_preprepare_msg_due_to_status_);
          }
        }
      } else {
        tryToSendStatusReport();
      }
    } else {
      // TODO(GG): TBD
    }
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId is in a newer view curView
  /////////////////////////////////////////////////////////////////////////
  else {
    ConcordAssertGT(msgViewNum, curView);
    tryToSendStatusReport();
  }

  delete msg;
}

void ReplicaImp::tryToSendStatusReport(bool onTimer) {
  // TODO(GG): in some cases, we can limit the amount of such messages by using information about
  // connection/disconnection (from the communication module)
  // TODO(GG): explain that the current "Status Report" approch is relatively simple (it can be more efficient and
  // sophisticated).

  const Time currentTime = getMonotonicTime();

  const milliseconds milliSinceLastTime =
      duration_cast<milliseconds>(currentTime - lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas);

  if (milliSinceLastTime < milliseconds(minTimeBetweenStatusRequestsMilli)) return;

  const int64_t dynamicMinTimeBetweenStatusRequestsMilli =
      (int64_t)((double)dynamicUpperLimitOfRounds->upperLimit() * factorForMinTimeBetweenStatusRequestsMilli);

  if (milliSinceLastTime < milliseconds(dynamicMinTimeBetweenStatusRequestsMilli)) return;

  // TODO(GG): handle restart/pause !! (restart/pause may affect time measurements...)
  lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas = currentTime;

  const bool viewIsActive = currentViewIsActive();
  const bool hasNewChangeMsg = viewsManager->hasNewViewMessage(curView);
  const bool listOfPPInActiveWindow = viewIsActive;
  const bool listOfMissingVCMsg = !viewIsActive && !viewsManager->viewIsPending(curView);
  const bool listOfMissingPPMsg = !viewIsActive && viewsManager->viewIsPending(curView);

  ReplicaStatusMsg msg(config_.replicaId,
                       curView,
                       lastStableSeqNum,
                       lastExecutedSeqNum,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPPInActiveWindow,
                       listOfMissingVCMsg,
                       listOfMissingPPMsg);

  if (listOfPPInActiveWindow) {
    const SeqNum start = lastStableSeqNum + 1;
    const SeqNum end = lastStableSeqNum + kWorkWindowSize;

    for (SeqNum i = start; i <= end; i++) {
      if (mainLog->get(i).hasPrePrepareMsg()) msg.setPrePrepareInActiveWindow(i);
    }
  }
  if (listOfMissingVCMsg) {
    for (ReplicaId i : repsInfo->idsOfPeerReplicas()) {
      if (!viewsManager->hasViewChangeMessageForFutureView(i)) msg.setMissingViewChangeMsgForViewChange(i);
    }
  } else if (listOfMissingPPMsg) {
    std::vector<SeqNum> missPP;
    if (viewsManager->getNumbersOfMissingPP(lastStableSeqNum, &missPP)) {
      for (SeqNum i : missPP) {
        ConcordAssertGT(i, lastStableSeqNum);
        ConcordAssertLE(i, lastStableSeqNum + kWorkWindowSize);
        msg.setMissingPrePrepareMsgForViewChange(i);
      }
    }
  }

  sendToAllOtherReplicas(&msg);
  if (!onTimer) metric_sent_status_msgs_not_due_timer_.Get().Inc();
}

template <>
void ReplicaImp::onMessage<ViewChangeMsg>(ViewChangeMsg *msg) {
  if (!viewChangeProtocolEnabled) {
    delete msg;
    return;
  }
  metric_received_view_changes_.Get().Inc();

  const ReplicaId generatedReplicaId =
      msg->idOfGeneratedReplica();  // Notice that generatedReplicaId may be != msg->senderId()
  ConcordAssertNE(generatedReplicaId, config_.replicaId);

  bool msgAdded = viewsManager->add(msg);

  if (!msgAdded) return;

  LOG_INFO(VC_LOG, KVLOG(generatedReplicaId, msg->newView(), msg->lastStable(), msg->numberOfElements(), msgAdded));

  // if the current primary wants to leave view
  if (generatedReplicaId == currentPrimary() && msg->newView() > curView) {
    LOG_INFO(VC_LOG, "Primary asks to leave view: " << KVLOG(generatedReplicaId, curView));
    MoveToHigherView(curView + 1);
  }

  ViewNum maxKnownCorrectView = 0;
  ViewNum maxKnownAgreedView = 0;
  viewsManager->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
  LOG_INFO(VC_LOG, KVLOG(maxKnownCorrectView, maxKnownAgreedView));

  if (maxKnownCorrectView > curView) {
    // we have at least f+1 view-changes with view number >= maxKnownCorrectView
    MoveToHigherView(maxKnownCorrectView);

    // update maxKnownCorrectView and maxKnownAgreedView
    // TODO(GG): consider to optimize (this part is not always needed)
    viewsManager->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
    LOG_INFO(VC_LOG,
             "Computed new view numbers. " << KVLOG(
                 maxKnownCorrectView, maxKnownAgreedView, viewsManager->viewIsActive(curView), lastAgreedView));
  }

  if (viewsManager->viewIsActive(curView)) return;  // return, if we are still in the previous view

  if (maxKnownAgreedView != curView) return;  // return, if we can't move to the new view yet

  // Replica now has at least 2f+2c+1 ViewChangeMsg messages with view  >= curView

  if (lastAgreedView < curView) {
    lastAgreedView = curView;
    metric_last_agreed_view_.Get().Set(lastAgreedView);
    timeOfLastAgreedView = getMonotonicTime();
  }

  tryToEnterView();
}

template <>
void ReplicaImp::onMessage<NewViewMsg>(NewViewMsg *msg) {
  if (!viewChangeProtocolEnabled) {
    delete msg;
    return;
  }
  metric_received_new_views_.Get().Inc();

  const ReplicaId senderId = msg->senderId();

  ConcordAssertNE(senderId, config_.replicaId);  // should be verified in ViewChangeMsg

  bool msgAdded = viewsManager->add(msg);

  if (!msgAdded) return;

  LOG_INFO(VC_LOG, KVLOG(senderId, msg->newView(), msgAdded, curView, viewsManager->viewIsActive(curView)));

  if (viewsManager->viewIsActive(curView)) return;  // return, if we are still in the previous view

  tryToEnterView();
}

void ReplicaImp::MoveToHigherView(ViewNum nextView) {
  ConcordAssert(viewChangeProtocolEnabled);
  ConcordAssertLT(curView, nextView);

  const bool wasInPrevViewNumber = viewsManager->viewIsActive(curView);

  LOG_INFO(VC_LOG, KVLOG(curView, nextView, wasInPrevViewNumber));

  ViewChangeMsg *pVC = nullptr;

  if (!wasInPrevViewNumber) {
    pVC = viewsManager->getMyLatestViewChangeMsg();
    ConcordAssertNE(pVC, nullptr);
    pVC->setNewViewNumber(nextView);
  } else {
    std::vector<ViewsManager::PrevViewInfo> prevViewInfo;
    for (SeqNum i = lastStableSeqNum + 1; i <= lastStableSeqNum + kWorkWindowSize; i++) {
      SeqNumInfo &seqNumInfo = mainLog->get(i);

      if (seqNumInfo.getPrePrepareMsg() != nullptr) {
        ViewsManager::PrevViewInfo x;

        seqNumInfo.getAndReset(x.prePrepare, x.prepareFull);
        x.hasAllRequests = true;

        ConcordAssertNE(x.prePrepare, nullptr);
        ConcordAssertEQ(x.prePrepare->viewNumber(), curView);
        // (x.prepareFull!=nullptr) ==> (x.hasAllRequests==true)
        ConcordAssertOR(x.prepareFull == nullptr, x.hasAllRequests);
        // (x.prepareFull!=nullptr) ==> (x.prepareFull->viewNumber() == curView)
        ConcordAssertOR(x.prepareFull == nullptr, x.prepareFull->viewNumber() == curView);

        prevViewInfo.push_back(x);
      } else {
        seqNumInfo.resetAndFree();
      }
    }

    if (ps_) {
      ViewChangeMsg *myVC = (curView == 0 ? nullptr : viewsManager->getMyLatestViewChangeMsg());
      SeqNum stableLowerBoundWhenEnteredToView = viewsManager->stableLowerBoundWhenEnteredToView();
      const DescriptorOfLastExitFromView desc{
          curView, lastStableSeqNum, lastExecutedSeqNum, prevViewInfo, myVC, stableLowerBoundWhenEnteredToView};
      ps_->beginWriteTran();
      ps_->setDescriptorOfLastExitFromView(desc);
      ps_->clearSeqNumWindow();
      ps_->endWriteTran();
    }

    pVC = viewsManager->exitFromCurrentView(lastStableSeqNum, lastExecutedSeqNum, prevViewInfo);

    ConcordAssertNE(pVC, nullptr);
    pVC->setNewViewNumber(nextView);
  }

  curView = nextView;
  metric_view_.Get().Set(nextView);
  metric_current_primary_.Get().Set(curView % config_.numReplicas);

  auto newView = curView;
  auto newPrimary = currentPrimary();
  LOG_INFO(VC_LOG,
           "Sending view change message. "
               << KVLOG(newView, wasInPrevViewNumber, newPrimary, lastExecutedSeqNum, lastStableSeqNum));

  pVC->finalizeMessage();
  sendToAllOtherReplicas(pVC);
}

void ReplicaImp::GotoNextView() {
  // at this point we don't have f+1 ViewChangeMsg messages with view >= curView

  MoveToHigherView(curView + 1);

  // at this point we don't have enough ViewChangeMsg messages (2f+2c+1) to enter the new view (because 2f+2c+1 > f+1)
}

bool ReplicaImp::tryToEnterView() {
  ConcordAssert(!currentViewIsActive());

  std::vector<PrePrepareMsg *> prePreparesForNewView;

  bool enteredView =
      viewsManager->tryToEnterView(curView, lastStableSeqNum, lastExecutedSeqNum, &prePreparesForNewView);

  LOG_INFO(VC_LOG,
           "Called viewsManager->tryToEnterView " << KVLOG(curView, lastStableSeqNum, lastExecutedSeqNum, enteredView));
  if (enteredView)
    onNewView(prePreparesForNewView);
  else
    tryToSendStatusReport();

  return enteredView;
}

void ReplicaImp::onNewView(const std::vector<PrePrepareMsg *> &prePreparesForNewView) {
  SeqNum firstPPSeq = 0;
  SeqNum lastPPSeq = 0;

  if (!prePreparesForNewView.empty()) {
    firstPPSeq = prePreparesForNewView.front()->seqNumber();
    lastPPSeq = prePreparesForNewView.back()->seqNumber();
  }

  LOG_INFO(VC_LOG,
           KVLOG(curView,
                 prePreparesForNewView.size(),
                 firstPPSeq,
                 lastPPSeq,
                 lastStableSeqNum,
                 lastExecutedSeqNum,
                 viewsManager->stableLowerBoundWhenEnteredToView()));

  ConcordAssert(viewsManager->viewIsActive(curView));
  ConcordAssertGE(lastStableSeqNum, viewsManager->stableLowerBoundWhenEnteredToView());
  ConcordAssertGE(lastExecutedSeqNum,
                  lastStableSeqNum);  // we moved to the new state, only after synchronizing the state

  timeOfLastViewEntrance = getMonotonicTime();  // TODO(GG): handle restart/pause

  NewViewMsg *newNewViewMsgToSend = nullptr;

  if (repsInfo->primaryOfView(curView) == config_.replicaId) {
    NewViewMsg *nv = viewsManager->getMyNewViewMsgForCurrentView();

    nv->finalizeMessage(*repsInfo);

    ConcordAssertEQ(nv->newView(), curView);

    newNewViewMsgToSend = nv;
  }

  if (prePreparesForNewView.empty()) {
    primaryLastUsedSeqNum = lastStableSeqNum;
    metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
    strictLowerBoundOfSeqNums = lastStableSeqNum;
    maxSeqNumTransferredFromPrevViews = lastStableSeqNum;
    LOG_INFO(VC_LOG,
             "No PrePrepare-s for the new view: " << KVLOG(
                 primaryLastUsedSeqNum, strictLowerBoundOfSeqNums, maxSeqNumTransferredFromPrevViews));
  } else {
    primaryLastUsedSeqNum = lastPPSeq;
    metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
    strictLowerBoundOfSeqNums = firstPPSeq - 1;
    maxSeqNumTransferredFromPrevViews = lastPPSeq;
    LOG_INFO(VC_LOG,
             "There are PrePrepare-s for the new view: " << KVLOG(firstPPSeq,
                                                                  lastPPSeq,
                                                                  primaryLastUsedSeqNum,
                                                                  strictLowerBoundOfSeqNums,
                                                                  maxSeqNumTransferredFromPrevViews));
  }

  if (ps_) {
    vector<ViewChangeMsg *> viewChangeMsgsForCurrentView = viewsManager->getViewChangeMsgsForCurrentView();
    NewViewMsg *newViewMsgForCurrentView = viewsManager->getNewViewMsgForCurrentView();

    bool myVCWasUsed = false;
    for (size_t i = 0; i < viewChangeMsgsForCurrentView.size() && !myVCWasUsed; i++) {
      ConcordAssertNE(viewChangeMsgsForCurrentView[i], nullptr);
      if (viewChangeMsgsForCurrentView[i]->idOfGeneratedReplica() == config_.replicaId) myVCWasUsed = true;
    }

    ViewChangeMsg *myVC = nullptr;
    if (!myVCWasUsed) {
      myVC = viewsManager->getMyLatestViewChangeMsg();
    } else {
      // debug/test: check that my VC should be included
      ViewChangeMsg *tempMyVC = viewsManager->getMyLatestViewChangeMsg();
      ConcordAssertNE(tempMyVC, nullptr);
      Digest d;
      tempMyVC->getMsgDigest(d);
      ConcordAssert(newViewMsgForCurrentView->includesViewChangeFromReplica(config_.replicaId, d));
    }

    DescriptorOfLastNewView viewDesc{curView,
                                     newViewMsgForCurrentView,
                                     viewChangeMsgsForCurrentView,
                                     myVC,
                                     viewsManager->stableLowerBoundWhenEnteredToView(),
                                     maxSeqNumTransferredFromPrevViews};

    ps_->beginWriteTran();
    ps_->setDescriptorOfLastNewView(viewDesc);
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
    ps_->setStrictLowerBoundOfSeqNums(strictLowerBoundOfSeqNums);
  }

  const bool primaryIsMe = (config_.replicaId == repsInfo->primaryOfView(curView));

  for (size_t i = 0; i < prePreparesForNewView.size(); i++) {
    PrePrepareMsg *pp = prePreparesForNewView[i];
    ConcordAssertGE(pp->seqNumber(), firstPPSeq);
    ConcordAssertLE(pp->seqNumber(), lastPPSeq);
    ConcordAssertEQ(pp->firstPath(), CommitPath::SLOW);  // TODO(GG): don't we want to use the fast path?
    SeqNumInfo &seqNumInfo = mainLog->get(pp->seqNumber());

    if (ps_) {
      ps_->setPrePrepareMsgInSeqNumWindow(pp->seqNumber(), pp);
      ps_->setSlowStartedInSeqNumWindow(pp->seqNumber(), true);
    }

    if (primaryIsMe)
      seqNumInfo.addSelfMsg(pp);
    else
      seqNumInfo.addMsg(pp);

    seqNumInfo.startSlowPath();
    metric_slow_path_count_.Get().Inc();
  }

  if (ps_) ps_->endWriteTran();

  clientsManager->clearAllPendingRequests();

  // clear requestsQueueOfPrimary
  while (!requestsQueueOfPrimary.empty()) {
    auto msg = requestsQueueOfPrimary.front();
    primaryCombinedReqSize -= msg->size();
    requestsQueueOfPrimary.pop();
    delete msg;
  }

  // send messages

  if (newNewViewMsgToSend != nullptr) {
    LOG_INFO(VC_LOG, "Sending NewView message to all replicas. " << KVLOG(curView));
    sendToAllOtherReplicas(newNewViewMsgToSend);
  }

  for (size_t i = 0; i < prePreparesForNewView.size(); i++) {
    PrePrepareMsg *pp = prePreparesForNewView[i];
    SeqNumInfo &seqNumInfo = mainLog->get(pp->seqNumber());
    sendPreparePartial(seqNumInfo);
  }

  LOG_INFO(VC_LOG, "Start working in new view: " << KVLOG(curView));

  controller->onNewView(curView, primaryLastUsedSeqNum);
  metric_current_active_view_.Get().Set(curView);
}

void ReplicaImp::sendCheckpointIfNeeded() {
  if (isCollectingState() || !currentViewIsActive()) return;

  const SeqNum lastCheckpointNumber = (lastExecutedSeqNum / checkpointWindowSize) * checkpointWindowSize;

  if (lastCheckpointNumber == 0) return;

  ConcordAssert(checkpointsLog->insideActiveWindow(lastCheckpointNumber));

  CheckpointInfo &checkInfo = checkpointsLog->get(lastCheckpointNumber);
  CheckpointMsg *checkpointMessage = checkInfo.selfCheckpointMsg();

  if (!checkpointMessage) {
    LOG_INFO(GL, "My Checkpoint message is missing. " << KVLOG(lastCheckpointNumber, lastExecutedSeqNum));
    return;
  }

  if (checkInfo.checkpointSentAllOrApproved()) return;

  // TODO(GG): 3 seconds, should be in configuration
  if ((getMonotonicTime() - checkInfo.selfExecutionTime()) >= 3s) {
    checkInfo.setCheckpointSentAllOrApproved();
    sendToAllOtherReplicas(checkpointMessage, true);
    return;
  }

  const SeqNum refSeqNumberForCheckpoint = lastCheckpointNumber + MaxConcurrentFastPaths;

  if (lastExecutedSeqNum < refSeqNumberForCheckpoint) return;

  if (mainLog->insideActiveWindow(lastExecutedSeqNum))  // TODO(GG): condition is needed ?
  {
    SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum);

    if (seqNumInfo.partialProofs().hasFullProof()) {
      checkInfo.tryToMarkCheckpointCertificateCompleted();

      ConcordAssert(checkInfo.isCheckpointCertificateComplete());

      onSeqNumIsStable(lastCheckpointNumber);

      checkInfo.setCheckpointSentAllOrApproved();

      return;
    }
  }

  checkInfo.setCheckpointSentAllOrApproved();
  sendToAllOtherReplicas(checkpointMessage, true);
}

void ReplicaImp::onTransferringCompleteImp(SeqNum newStateCheckpoint) {
  ConcordAssertEQ(newStateCheckpoint % checkpointWindowSize, 0);

  LOG_INFO(GL, KVLOG(newStateCheckpoint));

  if (ps_) {
    ps_->beginWriteTran();
  }

  if (newStateCheckpoint <= lastExecutedSeqNum) {
    LOG_DEBUG(GL,
              "Executing onTransferringCompleteImp(newStateCheckpoint) where newStateCheckpoint <= lastExecutedSeqNum");
    if (ps_) ps_->endWriteTran();
    return;
  }
  lastExecutedSeqNum = newStateCheckpoint;
  if (ps_) ps_->setLastExecutedSeqNum(lastExecutedSeqNum);
  if (config_.debugStatisticsEnabled) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
  }
  bool askAnotherStateTransfer = false;

  timeOfLastStateSynch = getMonotonicTime();  // TODO(GG): handle restart/pause

  clientsManager->loadInfoFromReservedPages();

  if (newStateCheckpoint > lastStableSeqNum + kWorkWindowSize) {
    const SeqNum refPoint = newStateCheckpoint - kWorkWindowSize;
    const bool withRefCheckpoint = (checkpointsLog->insideActiveWindow(refPoint) &&
                                    (checkpointsLog->get(refPoint).selfCheckpointMsg() != nullptr));

    onSeqNumIsStable(refPoint, withRefCheckpoint, true);
  }

  // newStateCheckpoint should be in the active window
  ConcordAssert(checkpointsLog->insideActiveWindow(newStateCheckpoint));

  // create and send my checkpoint
  Digest digestOfNewState;
  const uint64_t checkpointNum = newStateCheckpoint / checkpointWindowSize;
  stateTransfer->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char *)&digestOfNewState);
  CheckpointMsg *checkpointMsg = new CheckpointMsg(config_.replicaId, newStateCheckpoint, digestOfNewState, false);
  CheckpointInfo &checkpointInfo = checkpointsLog->get(newStateCheckpoint);
  checkpointInfo.addCheckpointMsg(checkpointMsg, config_.replicaId);
  checkpointInfo.setCheckpointSentAllOrApproved();

  if (newStateCheckpoint > primaryLastUsedSeqNum) primaryLastUsedSeqNum = newStateCheckpoint;

  if (checkpointInfo.isCheckpointSuperStable()) {
    onSeqNumIsSuperStable(newStateCheckpoint);
  }
  if (ps_) {
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
    ps_->setCheckpointMsgInCheckWindow(newStateCheckpoint, checkpointMsg);
    ps_->endWriteTran();
  }
  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);

  sendToAllOtherReplicas(checkpointMsg);

  if ((uint16_t)tableOfStableCheckpoints.size() >= config_.fVal + 1) {
    uint16_t numOfStableCheckpoints = 0;
    auto tableItrator = tableOfStableCheckpoints.begin();
    while (tableItrator != tableOfStableCheckpoints.end()) {
      if (tableItrator->second->seqNumber() >= newStateCheckpoint) numOfStableCheckpoints++;

      if (tableItrator->second->seqNumber() <= lastExecutedSeqNum) {
        delete tableItrator->second;
        tableItrator = tableOfStableCheckpoints.erase(tableItrator);
      } else {
        tableItrator++;
      }
    }
    if (numOfStableCheckpoints >= config_.fVal + 1) onSeqNumIsStable(newStateCheckpoint);

    if ((uint16_t)tableOfStableCheckpoints.size() >= config_.fVal + 1) askAnotherStateTransfer = true;
  }

  if (askAnotherStateTransfer) {
    LOG_INFO(GL, "Call to startCollectingState()");

    stateTransfer->startCollectingState();
  }
}

void ReplicaImp::onSeqNumIsSuperStable(SeqNum superStableSeqNum) {
  auto seq_num_to_stop_at = controlStateManager_->getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value() && seq_num_to_stop_at.value() == superStableSeqNum) {
    LOG_INFO(GL, "Informing control state manager that consensus should be stopped: " << KVLOG(superStableSeqNum));
    if (getRequestsHandler()->getControlHandlers()) {
      metric_on_call_back_of_super_stable_cp_.Get().Set(1);
      // TODO: With state transfered replica, this maight be called twice. Consider to clean the reserved page at the
      // end of this method
      getRequestsHandler()->getControlHandlers()->onSuperStableCheckpoint();
    }
  }
}
void ReplicaImp::onSeqNumIsStable(SeqNum newStableSeqNum, bool hasStateInformation, bool oldSeqNum) {
  ConcordAssertOR(hasStateInformation, oldSeqNum);  // !hasStateInformation ==> oldSeqNum
  ConcordAssertEQ(newStableSeqNum % checkpointWindowSize, 0);

  if (newStableSeqNum <= lastStableSeqNum) return;

  LOG_INFO(GL,
           "New stable sequence number. " << KVLOG(lastStableSeqNum, newStableSeqNum, hasStateInformation, oldSeqNum));

  if (ps_) ps_->beginWriteTran();

  lastStableSeqNum = newStableSeqNum;
  metric_last_stable_seq_num_.Get().Set(lastStableSeqNum);

  if (ps_) ps_->setLastStableSeqNum(lastStableSeqNum);

  if (lastStableSeqNum > strictLowerBoundOfSeqNums) {
    strictLowerBoundOfSeqNums = lastStableSeqNum;
    if (ps_) ps_->setStrictLowerBoundOfSeqNums(strictLowerBoundOfSeqNums);
  }

  if (lastStableSeqNum > primaryLastUsedSeqNum) {
    primaryLastUsedSeqNum = lastStableSeqNum;
    metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
    if (ps_) ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
  }

  mainLog->advanceActiveWindow(lastStableSeqNum + 1);

  // Basically, once a checkpoint become stable, we advance the checkpoints log window to it.
  // Alas, by doing so, we does not leave time for a checkpoint to try and become super stable.
  // For that we added another cell to the checkpoints log such that the "oldest" cell contains the checkpoint is
  // candidate for becoming super stable. So, once a checkpoint becomes stable we check if the previous checkpoint is in
  // the log, and if so we advance the log to that previous checkpoint.
  if (checkpointsLog->insideActiveWindow(newStableSeqNum - checkpointWindowSize)) {
    checkpointsLog->advanceActiveWindow(newStableSeqNum - checkpointWindowSize);
  } else {
    // If for some reason the previous checkpoint is not in the log, we advance the log to the current stable
    // checkpoint.
    checkpointsLog->advanceActiveWindow(lastStableSeqNum);
  }

  if (hasStateInformation) {
    if (lastStableSeqNum > lastExecutedSeqNum) {
      lastExecutedSeqNum = lastStableSeqNum;
      if (ps_) ps_->setLastExecutedSeqNum(lastExecutedSeqNum);
      metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
      if (config_.debugStatisticsEnabled) {
        DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
      }
      clientsManager->loadInfoFromReservedPages();
    }

    CheckpointInfo &checkpointInfo = checkpointsLog->get(lastStableSeqNum);
    CheckpointMsg *checkpointMsg = checkpointInfo.selfCheckpointMsg();

    if (checkpointMsg == nullptr) {
      Digest digestOfState;
      const uint64_t checkpointNum = lastStableSeqNum / checkpointWindowSize;
      stateTransfer->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char *)&digestOfState);
      checkpointMsg = new CheckpointMsg(config_.replicaId, lastStableSeqNum, digestOfState, true);
      checkpointInfo.addCheckpointMsg(checkpointMsg, config_.replicaId);
    } else {
      checkpointMsg->setStateAsStable();
    }

    if (!checkpointInfo.isCheckpointCertificateComplete()) checkpointInfo.tryToMarkCheckpointCertificateCompleted();
    ConcordAssert(checkpointInfo.isCheckpointCertificateComplete());

    // Call onSeqNumIsSuperStable in case that the self message was the last one to add
    if (checkpointInfo.isCheckpointSuperStable()) {
      onSeqNumIsSuperStable(lastStableSeqNum);
    }
    if (ps_) {
      ps_->setCheckpointMsgInCheckWindow(lastStableSeqNum, checkpointMsg);
      ps_->setCompletedMarkInCheckWindow(lastStableSeqNum, true);
    }
  }

  if (ps_) ps_->endWriteTran();

  if (!oldSeqNum && currentViewIsActive() && (currentPrimary() == config_.replicaId) && !isCollectingState()) {
    tryToSendPrePrepareMsg();
  }
}

void ReplicaImp::tryToSendReqMissingDataMsg(SeqNum seqNumber, bool slowPathOnly, uint16_t destReplicaId) {
  if ((!currentViewIsActive()) || (seqNumber <= strictLowerBoundOfSeqNums) ||
      (!mainLog->insideActiveWindow(seqNumber)) || (!mainLog->insideActiveWindow(seqNumber))) {
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);
  PartialProofsSet &partialProofsSet = seqNumInfo.partialProofs();

  const Time curTime = getMonotonicTime();

  {
    Time t = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();
    const Time lastInfoRequest = seqNumInfo.getTimeOfLastInfoRequest();

    if ((t < lastInfoRequest)) t = lastInfoRequest;

    if (t == MinTime && (t < curTime)) {
      auto diffMilli = duration_cast<milliseconds>(curTime - t);
      if (diffMilli.count() < dynamicUpperLimitOfRounds->upperLimit() / 4)  // TODO(GG): config
        return;
    }
  }

  seqNumInfo.setTimeOfLastInfoRequest(curTime);

  LOG_INFO(GL, "Try to request missing data. " << KVLOG(seqNumber, curView));

  ReqMissingDataMsg reqData(config_.replicaId, curView, seqNumber);

  const bool routerForPartialProofs = repsInfo->isCollectorForPartialProofs(curView, seqNumber);

  const bool routerForPartialPrepare = (currentPrimary() == config_.replicaId);

  const bool routerForPartialCommit = (currentPrimary() == config_.replicaId);

  const bool missingPrePrepare = (seqNumInfo.getPrePrepareMsg() == nullptr);
  const bool missingBigRequests = (!missingPrePrepare) && (!seqNumInfo.hasPrePrepareMsg());

  ReplicaId firstRepId = 0;
  ReplicaId lastRepId = config_.numReplicas - 1;
  if (destReplicaId != ALL_OTHER_REPLICAS) {
    firstRepId = destReplicaId;
    lastRepId = destReplicaId;
  }

  for (ReplicaId destRep = firstRepId; destRep <= lastRepId; destRep++) {
    if (destRep == config_.replicaId) continue;  // don't send to myself

    const bool destIsPrimary = (currentPrimary() == destRep);

    const bool missingPartialPrepare =
        (routerForPartialPrepare && (!seqNumInfo.preparedOrHasPreparePartialFromReplica(destRep)));

    const bool missingFullPrepare = !seqNumInfo.preparedOrHasPreparePartialFromReplica(destRep);

    const bool missingPartialCommit =
        (routerForPartialCommit && (!seqNumInfo.committedOrHasCommitPartialFromReplica(destRep)));

    const bool missingFullCommit = !seqNumInfo.committedOrHasCommitPartialFromReplica(destRep);

    const bool missingPartialProof = !slowPathOnly && routerForPartialProofs && !partialProofsSet.hasFullProof() &&
                                     !partialProofsSet.hasPartialProofFromReplica(destRep);

    const bool missingFullProof = !slowPathOnly && !partialProofsSet.hasFullProof();

    bool sendNeeded = missingPartialProof || missingPartialPrepare || missingFullPrepare || missingPartialCommit ||
                      missingFullCommit || missingFullProof;

    if (destIsPrimary && !sendNeeded) sendNeeded = missingBigRequests || missingPrePrepare;

    if (!sendNeeded) continue;

    reqData.resetFlags();

    if (destIsPrimary && missingPrePrepare) reqData.setPrePrepareIsMissing();

    if (missingPartialProof) reqData.setPartialProofIsMissing();
    if (missingPartialPrepare) reqData.setPartialPrepareIsMissing();
    if (missingFullPrepare) reqData.setFullPrepareIsMissing();
    if (missingPartialCommit) reqData.setPartialCommitIsMissing();
    if (missingFullCommit) reqData.setFullCommitIsMissing();
    if (missingFullProof) reqData.setFullCommitProofIsMissing();

    const bool slowPathStarted = seqNumInfo.slowPathStarted();

    if (slowPathStarted) reqData.setSlowPathHasStarted();

    auto destinationReplica = destRep;
    LOG_INFO(GL, "Send ReqMissingDataMsg. " << KVLOG(destinationReplica, seqNumber, reqData.getFlags()));

    send(&reqData, destRep);
    metric_sent_req_for_missing_data_.Get().Inc();
  }
}

template <>
void ReplicaImp::onMessage<ReqMissingDataMsg>(ReqMissingDataMsg *msg) {
  metric_received_req_missing_datas_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_INFO(GL, "Received ReqMissingDataMsg. " << KVLOG(msgSender, msg->getFlags()));
  if ((currentViewIsActive()) && (msgSeqNum > strictLowerBoundOfSeqNums) && (mainLog->insideActiveWindow(msgSeqNum)) &&
      (mainLog->insideActiveWindow(msgSeqNum))) {
    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    if (config_.replicaId == currentPrimary()) {
      PrePrepareMsg *pp = seqNumInfo.getSelfPrePrepareMsg();
      if (msg->getPrePrepareIsMissing()) {
        if (pp != nullptr) {
          sendAndIncrementMetric(pp, msgSender, metric_sent_preprepare_msg_due_to_reqMissingData_);
        }
      }

      if (seqNumInfo.slowPathStarted() && !msg->getSlowPathHasStarted()) {
        StartSlowCommitMsg startSlowMsg(config_.replicaId, curView, msgSeqNum);
        sendAndIncrementMetric(&startSlowMsg, msgSender, metric_sent_startSlowPath_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getPartialProofIsMissing()) {
      // TODO(GG): consider not to send if msgSender is not a collector

      PartialCommitProofMsg *pcf = seqNumInfo.partialProofs().getSelfPartialCommitProof();

      if (pcf != nullptr) {
        sendAndIncrementMetric(pcf, msgSender, metric_sent_partialCommitProof_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getPartialPrepareIsMissing() && (currentPrimary() == msgSender)) {
      PreparePartialMsg *pr = seqNumInfo.getSelfPreparePartialMsg();

      if (pr != nullptr) {
        sendAndIncrementMetric(pr, msgSender, metric_sent_preparePartial_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getFullPrepareIsMissing()) {
      PrepareFullMsg *pf = seqNumInfo.getValidPrepareFullMsg();

      if (pf != nullptr) {
        sendAndIncrementMetric(pf, msgSender, metric_sent_prepareFull_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getPartialCommitIsMissing() && (currentPrimary() == msgSender)) {
      CommitPartialMsg *c = mainLog->get(msgSeqNum).getSelfCommitPartialMsg();
      if (c != nullptr) {
        sendAndIncrementMetric(c, msgSender, metric_sent_commitPartial_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getFullCommitIsMissing()) {
      CommitFullMsg *c = mainLog->get(msgSeqNum).getValidCommitFullMsg();
      if (c != nullptr) {
        sendAndIncrementMetric(c, msgSender, metric_sent_commitFull_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getFullCommitProofIsMissing() && seqNumInfo.partialProofs().hasFullProof()) {
      FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();
      sendAndIncrementMetric(fcp, msgSender, metric_sent_fullCommitProof_msg_due_to_reqMissingData_);
    }
  } else {
    LOG_INFO(GL, "Ignore the ReqMissingDataMsg message. " << KVLOG(msgSender));
  }

  delete msg;
}

void ReplicaImp::onViewsChangeTimer(Timers::Handle timer)  // TODO(GG): review/update logic
{
  ConcordAssert(viewChangeProtocolEnabled);

  if (isCollectingState()) return;

  Time currTime = getMonotonicTime();

  //////////////////////////////////////////////////////////////////////////////
  //
  //////////////////////////////////////////////////////////////////////////////

  if (autoPrimaryRotationEnabled && currentViewIsActive()) {
    const uint64_t timeout = (isCurrentPrimary() ? (autoPrimaryRotationTimerMilli)
                                                 : (autoPrimaryRotationTimerMilli + viewChangeTimeoutMilli));

    const uint64_t diffMilli = duration_cast<milliseconds>(currTime - timeOfLastViewEntrance).count();

    if (diffMilli > timeout) {
      LOG_INFO(VC_LOG,
               "Initiate automatic view change in view=" << curView << " (" << diffMilli
                                                         << " milli seconds after start working in the previous view)");

      GotoNextView();
      return;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //
  //////////////////////////////////////////////////////////////////////////////

  uint64_t viewChangeTimeout = viewChangeTimerMilli;
  if (autoIncViewChangeTimer && ((lastViewThatTransferredSeqNumbersFullyExecuted + 1) < curView)) {
    uint64_t factor = (curView - lastViewThatTransferredSeqNumbersFullyExecuted);
    viewChangeTimeout = viewChangeTimeout * factor;  // TODO(GG): review logic here
  }

  if (currentViewIsActive()) {
    if (isCurrentPrimary()) return;

    const Time timeOfEarliestPendingRequest = clientsManager->timeOfEarliestPendingRequest();

    const bool hasPendingRequest = (timeOfEarliestPendingRequest != MaxTime);

    if (!hasPendingRequest) return;

    const uint64_t diffMilli1 = duration_cast<milliseconds>(currTime - timeOfLastStateSynch).count();
    const uint64_t diffMilli2 = duration_cast<milliseconds>(currTime - timeOfLastViewEntrance).count();
    const uint64_t diffMilli3 = duration_cast<milliseconds>(currTime - timeOfEarliestPendingRequest).count();

    if ((diffMilli1 > viewChangeTimeout) && (diffMilli2 > viewChangeTimeout) && (diffMilli3 > viewChangeTimeout)) {
      LOG_INFO(
          VC_LOG,
          "Ask to leave view=" << curView << " (" << diffMilli3 << " ms after the earliest pending client request).");

      GotoNextView();
      return;
    }
  } else  // not currentViewIsActive()
  {
    if (lastAgreedView != curView) return;
    if (repsInfo->primaryOfView(lastAgreedView) == config_.replicaId) return;

    currTime = getMonotonicTime();
    const uint64_t timeSinceLastStateTransferMilli =
        duration_cast<milliseconds>(currTime - timeOfLastStateSynch).count();
    const uint64_t timeSinceLastAgreedViewMilli = duration_cast<milliseconds>(currTime - timeOfLastAgreedView).count();

    if ((timeSinceLastStateTransferMilli > viewChangeTimeout) && (timeSinceLastAgreedViewMilli > viewChangeTimeout)) {
      LOG_INFO(VC_LOG,
               "Unable to activate the last agreed view (despite receiving 2f+2c+1 view change msgs). "
               "State transfer hasn't kicked-in for a while either. Asking to leave the current view: "
                   << KVLOG(curView, timeSinceLastAgreedViewMilli, timeSinceLastStateTransferMilli));
      GotoNextView();
      return;
    }
  }
}

void ReplicaImp::onStatusReportTimer(Timers::Handle timer) {
  tryToSendStatusReport(true);

#ifdef DEBUG_MEMORY_MSG
  MessageBase::printLiveMessages();
#endif
}

void ReplicaImp::onSlowPathTimer(Timers::Handle timer) {
  tryToStartSlowPaths();
  auto newPeriod = milliseconds(controller->slowPathsTimerMilli());
  timers_.reset(timer, newPeriod);
  metric_slow_path_timer_.Get().Set(controller->slowPathsTimerMilli());
}

void ReplicaImp::onInfoRequestTimer(Timers::Handle timer) {
  tryToAskForMissingInfo();
  auto newPeriod = milliseconds(dynamicUpperLimitOfRounds->upperLimit() / 2);
  timers_.reset(timer, newPeriod);
  metric_info_request_timer_.Get().Set(dynamicUpperLimitOfRounds->upperLimit() / 2);
}

void ReplicaImp::onSuperStableCheckpointTimer(concordUtil::Timers::Handle) {
  if (!isSeqNumToStopAt(lastExecutedSeqNum)) return;
  if (!checkpointsLog->insideActiveWindow(lastExecutedSeqNum)) return;
  CheckpointMsg *cpMsg = checkpointsLog->get(lastExecutedSeqNum).selfCheckpointMsg();
  // At this point the cpMsg cannot be evacuated
  if (!cpMsg) return;
  LOG_INFO(GL,
           "sending checkpoint message to help other replicas to reach super stable checkpoint"
               << KVLOG(lastExecutedSeqNum));
  sendToAllOtherReplicas(cpMsg, true);
}
template <>
void ReplicaImp::onMessage<SimpleAckMsg>(SimpleAckMsg *msg) {
  metric_received_simple_acks_.Get().Inc();
  SCOPED_MDC_SEQ_NUM(std::to_string(msg->seqNumber()));
  uint16_t relatedMsgType = (uint16_t)msg->ackData();  // TODO(GG): does this make sense ?
  if (retransmissionsLogicEnabled) {
    LOG_DEBUG(GL, KVLOG(msg->senderId(), relatedMsgType));
    retransmissionsManager->onAck(msg->senderId(), msg->seqNumber(), relatedMsgType);
  } else {
    LOG_WARN(GL, "Received Ack, but retransmissions not enabled. " << KVLOG(msg->senderId(), relatedMsgType));
  }

  delete msg;
}

void ReplicaImp::onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum) {
  // TODO(GG): simple implementation - should be improved
  tryToSendStatusReport();
}

void ReplicaImp::onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum) {
  // TODO(GG): simple implementation - should be improved
  tryToSendStatusReport();
}

void ReplicaImp::onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum) {
  // TODO(GG): simple implementation - should be improved
  tryToSendStatusReport();
}

void ReplicaImp::onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum) {
  // TODO(GG): simple implementation - should be improved
  tryToSendStatusReport();
}

ReplicaImp::ReplicaImp(const LoadedReplicaData &ld,
                       IRequestsHandler *requestsHandler,
                       IStateTransfer *stateTrans,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<PersistentStorage> persistentStorage,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers)
    : ReplicaImp(false,
                 ld.repConfig,
                 requestsHandler,
                 stateTrans,
                 ld.sigManager,
                 ld.repsInfo,
                 ld.viewsManager,
                 msgsCommunicator,
                 msgHandlers,
                 timers) {
  ConcordAssertNE(persistentStorage, nullptr);

  ps_ = persistentStorage;

  lastAgreedView = ld.viewsManager->latestActiveView();

  if (ld.viewsManager->viewIsActive(lastAgreedView)) {
    curView = lastAgreedView;
  } else {
    curView = lastAgreedView + 1;
    ViewChangeMsg *t = ld.viewsManager->getMyLatestViewChangeMsg();
    ConcordAssert(t != nullptr);
    ConcordAssert(t->newView() == curView);
    t->finalizeMessage();  // needed to initialize the VC message
  }

  metric_view_.Get().Set(curView);
  metric_last_agreed_view_.Get().Set(lastAgreedView);
  metric_current_primary_.Get().Set(curView % config_.numReplicas);

  const bool inView = ld.viewsManager->viewIsActive(curView);

  primaryLastUsedSeqNum = ld.primaryLastUsedSeqNum;
  metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
  lastStableSeqNum = ld.lastStableSeqNum;
  metric_last_stable_seq_num_.Get().Set(lastStableSeqNum);
  lastExecutedSeqNum = ld.lastExecutedSeqNum;
  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
  strictLowerBoundOfSeqNums = ld.strictLowerBoundOfSeqNums;
  maxSeqNumTransferredFromPrevViews = ld.maxSeqNumTransferredFromPrevViews;
  lastViewThatTransferredSeqNumbersFullyExecuted = ld.lastViewThatTransferredSeqNumbersFullyExecuted;

  mainLog->resetAll(lastStableSeqNum + 1);
  checkpointsLog->resetAll(lastStableSeqNum);

  bool viewIsActive = inView;
  LOG_INFO(GL,
           "Restarted ReplicaImp from persistent storage. " << KVLOG(curView,
                                                                     lastAgreedView,
                                                                     viewIsActive,
                                                                     primaryLastUsedSeqNum,
                                                                     lastStableSeqNum,
                                                                     lastExecutedSeqNum,
                                                                     strictLowerBoundOfSeqNums,
                                                                     maxSeqNumTransferredFromPrevViews,
                                                                     lastViewThatTransferredSeqNumbersFullyExecuted));

  if (inView) {
    const bool isPrimaryOfView = (repsInfo->primaryOfView(curView) == config_.replicaId);

    SeqNum s = ld.lastStableSeqNum;

    for (size_t i = 0; i < kWorkWindowSize; i++) {
      s++;
      ConcordAssert(mainLog->insideActiveWindow(s));

      const SeqNumData &e = ld.seqNumWinArr[i];

      if (!e.isPrePrepareMsgSet()) continue;

      // such properties should be verified by the code the loads the persistent data
      ConcordAssertEQ(e.getPrePrepareMsg()->seqNumber(), s);

      SeqNumInfo &seqNumInfo = mainLog->get(s);

      // add prePrepareMsg

      if (isPrimaryOfView)
        seqNumInfo.addSelfMsg(e.getPrePrepareMsg(), true);
      else
        seqNumInfo.addMsg(e.getPrePrepareMsg(), true);

      ConcordAssert(e.getPrePrepareMsg()->equals(*seqNumInfo.getPrePrepareMsg()));

      const CommitPath pathInPrePrepare = e.getPrePrepareMsg()->firstPath();

      // TODO(GG): check this when we load the data from disk
      ConcordAssertOR(pathInPrePrepare != CommitPath::SLOW, e.getSlowStarted());

      if (pathInPrePrepare != CommitPath::SLOW) {
        // add PartialCommitProofMsg

        PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
        ConcordAssert(e.getPrePrepareMsg()->equals(*pp));
        Digest &ppDigest = pp->digestOfRequests();
        const SeqNum seqNum = pp->seqNumber();

        IThresholdSigner *commitSigner = nullptr;

        ConcordAssertOR((config_.cVal != 0), (pathInPrePrepare != CommitPath::FAST_WITH_THRESHOLD));

        if ((pathInPrePrepare == CommitPath::FAST_WITH_THRESHOLD) && (config_.cVal > 0))
          commitSigner = config_.thresholdSignerForCommit;
        else
          commitSigner = config_.thresholdSignerForOptimisticCommit;

        Digest tmpDigest;
        Digest::calcCombination(ppDigest, curView, seqNum, tmpDigest);

        PartialCommitProofMsg *p =
            new PartialCommitProofMsg(config_.replicaId, curView, seqNum, pathInPrePrepare, tmpDigest, commitSigner);
        seqNumInfo.partialProofs().addSelfMsgAndPPDigest(
            p,
            tmpDigest);  // TODO(GG): consider using a method that directly adds the message/digest (as in the
                         // examples below)
      }

      if (e.getSlowStarted()) {
        seqNumInfo.startSlowPath();

        // add PreparePartialMsg
        PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
        PreparePartialMsg *p = PreparePartialMsg::create(curView,
                                                         pp->seqNumber(),
                                                         config_.replicaId,
                                                         pp->digestOfRequests(),
                                                         config_.thresholdSignerForSlowPathCommit);
        bool added = seqNumInfo.addSelfMsg(p, true);
        ConcordAssert(added);
      }

      if (e.isPrepareFullMsgSet()) {
        seqNumInfo.addMsg(e.getPrepareFullMsg(), true);

        Digest d;
        Digest::digestOfDigest(e.getPrePrepareMsg()->digestOfRequests(), d);
        CommitPartialMsg *c =
            CommitPartialMsg::create(curView, s, config_.replicaId, d, config_.thresholdSignerForSlowPathCommit);

        seqNumInfo.addSelfCommitPartialMsgAndDigest(c, d, true);
      }

      if (e.isCommitFullMsgSet()) {
        seqNumInfo.addMsg(e.getCommitFullMsg(), true);
        ConcordAssert(e.getCommitFullMsg()->equals(*seqNumInfo.getValidCommitFullMsg()));
      }

      if (e.isFullCommitProofMsgSet()) {
        PartialProofsSet &pps = seqNumInfo.partialProofs();
        bool added = pps.addMsg(e.getFullCommitProofMsg());  // TODO(GG): consider using a method that directly adds
                                                             // the message (as in the examples below)
        ConcordAssert(added);  // we should verify the relevant signature when it is loaded
        ConcordAssert(e.getFullCommitProofMsg()->equals(*pps.getFullProof()));
      }

      if (e.getForceCompleted()) seqNumInfo.forceComplete();
    }
  }

  ConcordAssert(ld.lastStableSeqNum % checkpointWindowSize == 0);

  for (SeqNum s = ld.lastStableSeqNum; s <= ld.lastStableSeqNum + kWorkWindowSize; s = s + checkpointWindowSize) {
    size_t i = (s - ld.lastStableSeqNum) / checkpointWindowSize;
    ConcordAssertLT(i, (sizeof(ld.checkWinArr) / sizeof(ld.checkWinArr[0])));
    const CheckData &e = ld.checkWinArr[i];

    ConcordAssert(checkpointsLog->insideActiveWindow(s));
    ConcordAssert(s == 0 ||                                                         // no checkpoints yet
                  s > ld.lastStableSeqNum ||                                        // not stable
                  e.isCheckpointMsgSet() ||                                         // if stable need to be set
                  ld.lastStableSeqNum == ld.lastExecutedSeqNum - kWorkWindowSize);  // after ST last executed may be on
                                                                                    // the upper working window boundary

    if (!e.isCheckpointMsgSet()) continue;

    CheckpointInfo &checkInfo = checkpointsLog->get(s);

    ConcordAssertEQ(e.getCheckpointMsg()->seqNumber(), s);
    ConcordAssertEQ(e.getCheckpointMsg()->senderId(), config_.replicaId);
    ConcordAssertOR((s != ld.lastStableSeqNum), e.getCheckpointMsg()->isStableState());

    checkInfo.addCheckpointMsg(e.getCheckpointMsg(), config_.replicaId);
    ConcordAssert(checkInfo.selfCheckpointMsg()->equals(*e.getCheckpointMsg()));

    if (e.getCompletedMark()) checkInfo.tryToMarkCheckpointCertificateCompleted();
  }

  if (ld.isExecuting) {
    ConcordAssert(viewsManager->viewIsActive(curView));
    ConcordAssert(mainLog->insideActiveWindow(lastExecutedSeqNum + 1));
    const SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    ConcordAssertNE(pp, nullptr);
    ConcordAssertEQ(pp->seqNumber(), lastExecutedSeqNum + 1);
    ConcordAssertEQ(pp->viewNumber(), curView);
    ConcordAssertGT(pp->numberOfRequests(), 0);

    Bitmap b = ld.validRequestsThatAreBeingExecuted;
    size_t expectedValidRequests = 0;
    for (uint32_t i = 0; i < b.numOfBits(); i++) {
      if (b.get(i)) expectedValidRequests++;
    }
    ConcordAssertLE(expectedValidRequests, pp->numberOfRequests());

    recoveringFromExecutionOfRequests = true;
    mapOfRequestsThatAreBeingRecovered = b;
  }

  internalThreadPool.start(8);  // TODO(GG): use configuration
}

ReplicaImp::ReplicaImp(const ReplicaConfig &config,
                       IRequestsHandler *requestsHandler,
                       IStateTransfer *stateTrans,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<PersistentStorage> persistentStorage,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers)
    : ReplicaImp(
          true, config, requestsHandler, stateTrans, nullptr, nullptr, nullptr, msgsCommunicator, msgHandlers, timers) {
  if (persistentStorage != nullptr) {
    ps_ = persistentStorage;

    ConcordAssert(!ps_->hasReplicaConfig());

    ps_->beginWriteTran();
    ps_->setReplicaConfig(config);
    ps_->endWriteTran();
  }

  auto numThreads = 8;
  LOG_INFO(GL, "Starting internal replica thread pool. " << KVLOG(numThreads));
  internalThreadPool.start(numThreads);  // TODO(GG): use configuration
}

ReplicaImp::ReplicaImp(bool firstTime,
                       const ReplicaConfig &config,
                       IRequestsHandler *requestsHandler,
                       IStateTransfer *stateTrans,
                       SigManager *sigMgr,
                       ReplicasInfo *replicasInfo,
                       ViewsManager *viewsMgr,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers)
    : ReplicaForStateTransfer(config, stateTrans, msgsCommunicator, msgHandlers, firstTime, timers),
      viewChangeProtocolEnabled{config.viewChangeProtocolEnabled},
      autoPrimaryRotationEnabled{config.autoPrimaryRotationEnabled},
      restarted_{!firstTime},
      replyBuffer{(char *)std::malloc(config_.maxReplyMessageSize - sizeof(ClientReplyMsgHeader))},
      bftRequestsHandler_{requestsHandler},
      timeOfLastStateSynch{getMonotonicTime()},    // TODO(GG): TBD
      timeOfLastViewEntrance{getMonotonicTime()},  // TODO(GG): TBD
      timeOfLastAgreedView{getMonotonicTime()},    // TODO(GG): TBD
      metric_view_{metrics_.RegisterGauge("view", curView)},
      metric_last_stable_seq_num_{metrics_.RegisterGauge("lastStableSeqNum", lastStableSeqNum)},
      metric_last_executed_seq_num_{metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum)},
      metric_last_agreed_view_{metrics_.RegisterGauge("lastAgreedView", lastAgreedView)},
      metric_current_active_view_{metrics_.RegisterGauge("currentActiveView", 0)},
      metric_viewchange_timer_{metrics_.RegisterGauge("viewChangeTimer", 0)},
      metric_retransmissions_timer_{metrics_.RegisterGauge("retransmissionTimer", 0)},
      metric_status_report_timer_{metrics_.RegisterGauge("statusReportTimer", 0)},
      metric_slow_path_timer_{metrics_.RegisterGauge("slowPathTimer", 0)},
      metric_info_request_timer_{metrics_.RegisterGauge("infoRequestTimer", 0)},
      metric_current_primary_{metrics_.RegisterGauge("currentPrimary", curView % config_.numReplicas)},
      metric_concurrency_level_{metrics_.RegisterGauge("concurrencyLevel", config_.concurrencyLevel)},
      metric_primary_last_used_seq_num_{metrics_.RegisterGauge("primaryLastUsedSeqNum", primaryLastUsedSeqNum)},
      metric_on_call_back_of_super_stable_cp_{metrics_.RegisterGauge("OnCallBackOfSuperStableCP", 0)},
      metric_first_commit_path_{metrics_.RegisterStatus(
          "firstCommitPath", CommitPathToStr(ControllerWithSimpleHistory_debugInitialFirstPath))},
      metric_slow_path_count_{metrics_.RegisterCounter("slowPathCount", 0)},
      metric_received_internal_msgs_{metrics_.RegisterCounter("receivedInternalMsgs")},
      metric_received_client_requests_{metrics_.RegisterCounter("receivedClientRequestMsgs")},
      metric_received_pre_prepares_{metrics_.RegisterCounter("receivedPrePrepareMsgs")},
      metric_received_start_slow_commits_{metrics_.RegisterCounter("receivedStartSlowCommitMsgs")},
      metric_received_partial_commit_proofs_{metrics_.RegisterCounter("receivedPartialCommitProofMsgs")},
      metric_received_full_commit_proofs_{metrics_.RegisterCounter("receivedFullCommitProofMsgs")},
      metric_received_prepare_partials_{metrics_.RegisterCounter("receivedPreparePartialMsgs")},
      metric_received_commit_partials_{metrics_.RegisterCounter("receivedCommitPartialMsgs")},
      metric_received_prepare_fulls_{metrics_.RegisterCounter("receivedPrepareFullMsgs")},
      metric_received_commit_fulls_{metrics_.RegisterCounter("receivedCommitFullMsgs")},
      metric_received_checkpoints_{metrics_.RegisterCounter("receivedCheckpointMsgs")},
      metric_received_replica_statuses_{metrics_.RegisterCounter("receivedReplicaStatusMsgs")},
      metric_received_view_changes_{metrics_.RegisterCounter("receivedViewChangeMsgs")},
      metric_received_new_views_{metrics_.RegisterCounter("receivedNewViewMsgs")},
      metric_received_req_missing_datas_{metrics_.RegisterCounter("receivedReqMissingDataMsgs")},
      metric_received_simple_acks_{metrics_.RegisterCounter("receivedSimpleAckMsgs")},
      metric_sent_status_msgs_not_due_timer_{metrics_.RegisterCounter("sentStatusMsgsNotDueTime")},
      metric_sent_req_for_missing_data_{metrics_.RegisterCounter("sentReqForMissingData")},
      metric_sent_checkpoint_msg_due_to_status_{metrics_.RegisterCounter("sentCheckpointMsgDueToStatus")},
      metric_sent_viewchange_msg_due_to_status_{metrics_.RegisterCounter("sentViewChangeMsgDueToTimer")},
      metric_sent_newview_msg_due_to_status_{metrics_.RegisterCounter("sentNewviewMsgDueToCounter")},
      metric_sent_preprepare_msg_due_to_status_{metrics_.RegisterCounter("sentPreprepareMsgDueToStatus")},
      metric_sent_preprepare_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentPreprepareMsgDueToReqMissingData")},
      metric_sent_startSlowPath_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentStartSlowPathMsgDueToReqMissingData")},
      metric_sent_partialCommitProof_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentPartialCommitProofMsgDueToReqMissingData")},
      metric_sent_preparePartial_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentPrepreparePartialMsgDueToReqMissingData")},
      metric_sent_prepareFull_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentPrepareFullMsgDueToReqMissingData")},
      metric_sent_commitPartial_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentCommitPartialMsgDueToRewMissingData")},
      metric_sent_commitFull_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentCommitFullMsgDueToReqMissingData")},
      metric_sent_fullCommitProof_msg_due_to_reqMissingData_{
          metrics_.RegisterCounter("sentFullCommitProofMsgDueToReqMissingData")},
      metric_not_enough_client_requests_event_{metrics_.RegisterCounter("notEnoughClientRequestsEvent")},
      metric_total_finished_consensuses_{metrics_.RegisterCounter("totalOrderedRequests")},
      metric_total_slowPath_{metrics_.RegisterCounter("totalSlowPaths")},
      metric_total_fastPath_{metrics_.RegisterCounter("totalFastPaths")} {
  ConcordAssertLT(config_.replicaId, config_.numReplicas);
  // TODO(GG): more asserts on params !!!!!!!!!!!

  // !firstTime ==> ((sigMgr != nullptr) && (replicasInfo != nullptr) && (viewsMgr != nullptr))
  ConcordAssert(firstTime || ((sigMgr != nullptr) && (replicasInfo != nullptr) && (viewsMgr != nullptr)));

  registerMsgHandlers();
  registerStatusHandlers();

  // Register metrics component with the default aggregator.
  metrics_.Register();

  if (firstTime) {
    sigManager = new SigManager(config_.replicaId,
                                config_.numReplicas + config_.numOfClientProxies + config_.numOfExternalClients,
                                config_.replicaPrivateKey,
                                config_.publicKeysOfReplicas);
    repsInfo = new ReplicasInfo(config_, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);
    viewsManager = new ViewsManager(repsInfo, sigManager, config_.thresholdVerifierForSlowPathCommit);
  } else {
    sigManager = sigMgr;
    repsInfo = replicasInfo;
    viewsManager = viewsMgr;

    // TODO(GG): consider to add relevant asserts
  }

  std::set<NodeIdType> clientsSet;
  const auto numOfEntities = config_.numReplicas + config_.numOfClientProxies + config_.numOfExternalClients;
  for (uint16_t i = config_.numReplicas; i < numOfEntities; i++) clientsSet.insert(i);
  clientsManager =
      new ClientsManager(config_.replicaId, clientsSet, ReplicaConfigSingleton::GetInstance().GetSizeOfReservedPage());
  clientsManager->setNumResPages((config_.numOfClientProxies + config_.numOfExternalClients) *
                                 config_.maxReplyMessageSize / config_.sizeOfReservedPage);
  clientsManager->init(stateTransfer.get());

  // autoPrimaryRotationEnabled implies viewChangeProtocolEnabled
  // Note: "p=>q" is equivalent to "not p or q"
  ConcordAssertOR(!autoPrimaryRotationEnabled, viewChangeProtocolEnabled);

  viewChangeTimerMilli = (viewChangeTimeoutMilli > 0) ? viewChangeTimeoutMilli : config.viewChangeTimerMillisec;
  ConcordAssertGT(viewChangeTimerMilli, 0);

  if (autoPrimaryRotationEnabled) {
    autoPrimaryRotationTimerMilli =
        (autoPrimaryRotationTimerMilli > 0) ? autoPrimaryRotationTimerMilli : config.autoPrimaryRotationTimerMillisec;
    ConcordAssertGT(autoPrimaryRotationTimerMilli, 0);
  }

  // TODO(GG): use config ...
  dynamicUpperLimitOfRounds = new DynamicUpperLimitWithSimpleFilter<int64_t>(400, 2, 2500, 70, 32, 1000, 2, 2);

  mainLog =
      new SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumInfo, SeqNumInfo>(1, (InternalReplicaApi *)this);

  checkpointsLog = new SequenceWithActiveWindow<kWorkWindowSize + 2 * checkpointWindowSize,
                                                checkpointWindowSize,
                                                SeqNum,
                                                CheckpointInfo,
                                                CheckpointInfo>(0, (InternalReplicaApi *)this);

  // create controller . TODO(GG): do we want to pass the controller as a parameter ?
  controller =
      new ControllerWithSimpleHistory(config_.cVal, config_.fVal, config_.replicaId, curView, primaryLastUsedSeqNum);

  if (retransmissionsLogicEnabled)
    retransmissionsManager =
        new RetransmissionsManager(&internalThreadPool, &getIncomingMsgsStorage(), kWorkWindowSize, 0);
  else
    retransmissionsManager = nullptr;

  LOG_INFO(GL,
           "ReplicaConfig parameters isReadOnly="
               << config_.isReadOnly << ", numReplicas=" << config_.numReplicas
               << ", numRoReplicas=" << config_.numRoReplicas << ", fVal=" << config_.fVal << ", cVal=" << config_.cVal
               << ", replicaId=" << config_.replicaId << ", numOfClientProxies=" << config_.numOfClientProxies
               << ", numOfExternalClients=" << config_.numOfExternalClients << ", statusReportTimerMillisec="
               << config_.statusReportTimerMillisec << ", concurrencyLevel=" << config_.concurrencyLevel
               << ", viewChangeProtocolEnabled=" << config_.viewChangeProtocolEnabled
               << ", viewChangeTimerMillisec=" << config_.viewChangeTimerMillisec
               << ", autoPrimaryRotationEnabled=" << config_.autoPrimaryRotationEnabled
               << ", autoPrimaryRotationTimerMillisec=" << config_.autoPrimaryRotationTimerMillisec
               << ", preExecReqStatusCheckTimerMillisec=" << config_.preExecReqStatusCheckTimerMillisec
               << ", maxExternalMessageSize=" << config_.maxExternalMessageSize << ", maxReplyMessageSize="
               << config_.maxReplyMessageSize << ", maxNumOfReservedPages=" << config_.maxNumOfReservedPages
               << ", sizeOfReservedPage=" << config_.sizeOfReservedPage
               << ", debugStatisticsEnabled=" << config_.debugStatisticsEnabled
               << ", metricsDumpIntervalSeconds=" << config_.metricsDumpIntervalSeconds);
}

ReplicaImp::~ReplicaImp() {
  // TODO(GG): rewrite this method !!!!!!!! (notice that the order may be important here ).
  // TODO(GG): don't delete objects that are passed as params (TBD)

  internalThreadPool.stop();

  delete viewsManager;
  delete controller;
  delete dynamicUpperLimitOfRounds;
  delete mainLog;
  delete checkpointsLog;
  delete clientsManager;
  delete sigManager;
  delete repsInfo;
  free(replyBuffer);

  if (config_.debugStatisticsEnabled) {
    DebugStatistics::freeDebugStatisticsData();
  }
}

void ReplicaImp::stop() {
  if (retransmissionsLogicEnabled) timers_.cancel(retranTimer_);
  timers_.cancel(slowPathTimer_);
  timers_.cancel(infoReqTimer_);
  timers_.cancel(statusReportTimer_);
  if (viewChangeProtocolEnabled) timers_.cancel(viewChangeTimer_);
  if (enableRetransmitSuperStableCheckpoint_) timers_.cancel(superStableCheckpointRetransmitTimer_);
  ReplicaForStateTransfer::stop();
}

void ReplicaImp::addTimers() {
  int statusReportTimerMilli = (sendStatusPeriodMilli > 0) ? sendStatusPeriodMilli : config_.statusReportTimerMillisec;
  ConcordAssertGT(statusReportTimerMilli, 0);
  metric_status_report_timer_.Get().Set(statusReportTimerMilli);
  statusReportTimer_ = timers_.add(milliseconds(statusReportTimerMilli),
                                   Timers::Timer::RECURRING,
                                   [this](Timers::Handle h) { onStatusReportTimer(h); });
  if (viewChangeProtocolEnabled) {
    int t = viewChangeTimerMilli;
    if (autoPrimaryRotationEnabled && t > autoPrimaryRotationTimerMilli) t = autoPrimaryRotationTimerMilli;
    metric_viewchange_timer_.Get().Set(t / 2);
    // TODO(GG): What should be the time period here?
    // TODO(GG): Consider to split to 2 different timers
    viewChangeTimer_ =
        timers_.add(milliseconds(t / 2), Timers::Timer::RECURRING, [this](Timers::Handle h) { onViewsChangeTimer(h); });
  }
  if (retransmissionsLogicEnabled) {
    metric_retransmissions_timer_.Get().Set(retransmissionsTimerMilli);
    retranTimer_ = timers_.add(milliseconds(retransmissionsTimerMilli),
                               Timers::Timer::RECURRING,
                               [this](Timers::Handle h) { onRetransmissionsTimer(h); });
  }
  const int slowPathsTimerPeriod = controller->timeToStartSlowPathMilli();
  metric_slow_path_timer_.Get().Set(slowPathsTimerPeriod);
  slowPathTimer_ = timers_.add(
      milliseconds(slowPathsTimerPeriod), Timers::Timer::RECURRING, [this](Timers::Handle h) { onSlowPathTimer(h); });

  metric_info_request_timer_.Get().Set(dynamicUpperLimitOfRounds->upperLimit() / 2);
  infoReqTimer_ = timers_.add(milliseconds(dynamicUpperLimitOfRounds->upperLimit() / 2),
                              Timers::Timer::RECURRING,
                              [this](Timers::Handle h) { onInfoRequestTimer(h); });
}

void ReplicaImp::start() {
  LOG_INFO(GL, "Running ReplicaImp");
  ReplicaForStateTransfer::start();
  if (!firstTime_ || config_.debugPersistentStorageEnabled) clientsManager->loadInfoFromReservedPages();
  addTimers();
  recoverRequests();

  // The following line will start the processing thread.
  // It must happen after the replica recovers requests in the main thread.
  msgsCommunicator_->startMsgsProcessing(config_.replicaId);
}

void ReplicaImp::recoverRequests() {
  if (recoveringFromExecutionOfRequests) {
    LOG_INFO(GL, "Recovering execution of requests");
    SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    ConcordAssertNE(pp, nullptr);
    auto span = concordUtils::startSpan("bft_recover_requests_on_start");
    executeRequestsInPrePrepareMsg(span, pp, true);
    metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
    metric_total_finished_consensuses_.Get().Inc();
    if (seqNumInfo.slowPathStarted()) {
      metric_total_slowPath_.Get().Inc();
    } else {
      metric_total_fastPath_.Get().Inc();
    }
    recoveringFromExecutionOfRequests = false;
    mapOfRequestsThatAreBeingRecovered = Bitmap();
  }
}

void ReplicaImp::executeReadOnlyRequest(concordUtils::SpanWrapper &parent_span, ClientRequestMsg *request) {
  ConcordAssert(request->isReadOnly());
  ConcordAssert(!isCollectingState());

  auto span = concordUtils::startChildSpan("bft_execute_read_only_request", parent_span);
  ClientReplyMsg reply(currentPrimary(), request->requestSeqNum(), config_.replicaId);

  uint16_t clientId = request->clientProxyId();

  int error = 0;
  uint32_t actualReplyLength = 0;
  uint32_t actualReplicaSpecificInfoLength = 0;

  error = bftRequestsHandler_.execute(clientId,
                                      lastExecutedSeqNum,
                                      READ_ONLY_FLAG,
                                      request->requestLength(),
                                      request->requestBuf(),
                                      reply.maxReplyLength(),
                                      reply.replyBuf(),
                                      actualReplyLength,
                                      actualReplicaSpecificInfoLength,
                                      span);

  LOG_DEBUG(GL,
            "Executed read only request. " << KVLOG(clientId,
                                                    lastExecutedSeqNum,
                                                    request->requestLength(),
                                                    reply.maxReplyLength(),
                                                    actualReplyLength,
                                                    actualReplicaSpecificInfoLength,
                                                    error));

  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)
  if (!error) {
    if (actualReplyLength > 0) {
      reply.setReplyLength(actualReplyLength);
      reply.setReplicaSpecificInfoLength(actualReplicaSpecificInfoLength);
      send(&reply, clientId);
    } else {
      LOG_ERROR(GL, "Received zero size response. " << KVLOG(clientId));
    }

  } else {
    LOG_ERROR(GL, "Received error while executing RO request. " << KVLOG(clientId, error));
  }

  if (config_.debugStatisticsEnabled) {
    DebugStatistics::onRequestCompleted(true);
  }
}

void ReplicaImp::executeRequestsInPrePrepareMsg(concordUtils::SpanWrapper &parent_span,
                                                PrePrepareMsg *ppMsg,
                                                bool recoverFromErrorInRequestsExecution) {
  auto span = concordUtils::startChildSpan("bft_execute_requests_in_preprepare", parent_span);
  ConcordAssertAND(!isCollectingState(), currentViewIsActive());
  ConcordAssertNE(ppMsg, nullptr);
  ConcordAssertEQ(ppMsg->viewNumber(), curView);
  ConcordAssertEQ(ppMsg->seqNumber(), lastExecutedSeqNum + 1);

  const uint16_t numOfRequests = ppMsg->numberOfRequests();

  // recoverFromErrorInRequestsExecution ==> (numOfRequests > 0)
  ConcordAssertOR(!recoverFromErrorInRequestsExecution, (numOfRequests > 0));

  if (numOfRequests > 0) {
    Bitmap requestSet(numOfRequests);
    size_t reqIdx = 0;
    RequestsIterator reqIter(ppMsg);
    char *requestBody = nullptr;

    //////////////////////////////////////////////////////////////////////
    // Phase 1:
    // a. Find the requests that should be executed
    // b. Send reply for each request that has already been executed
    //////////////////////////////////////////////////////////////////////
    if (!recoverFromErrorInRequestsExecution) {
      while (reqIter.getAndGoToNext(requestBody)) {
        ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);
        SCOPED_MDC_CID(req.getCid());
        NodeIdType clientId = req.clientProxyId();

        const bool validClient = isValidClient(clientId);
        if (!validClient) {
          LOG_WARN(GL, "The client is not valid. " << KVLOG(clientId));
          continue;
        }

        if (seqNumberOfLastReplyToClient(clientId) >= req.requestSeqNum()) {
          ClientReplyMsg *replyMsg = clientsManager->allocateMsgWithLatestReply(clientId, currentPrimary());
          send(replyMsg, clientId);
          delete replyMsg;
          continue;
        }

        requestSet.set(reqIdx);
        reqIdx++;
      }
      reqIter.restart();

      if (ps_) {
        DescriptorOfLastExecution execDesc{lastExecutedSeqNum + 1, requestSet};
        ps_->beginWriteTran();
        ps_->setDescriptorOfLastExecution(execDesc);
        ps_->endWriteTran();
      }
    } else {
      requestSet = mapOfRequestsThatAreBeingRecovered;
    }

    //////////////////////////////////////////////////////////////////////
    // Phase 2: execute requests + send replies
    // In this phase the application state may be changed. We also change data in the state transfer module.
    // TODO(GG): Explain what happens in recovery mode (what are the requirements from  the application, and from the
    // state transfer module.
    //////////////////////////////////////////////////////////////////////

    reqIdx = 0;
    requestBody = nullptr;

    auto dur = controller->durationSincePrePrepare(lastExecutedSeqNum + 1);
    if (dur > 0) {
      // Primary
      LOG_DEBUG(CNSUS, "Consensus reached, duration [" << dur << "ms]");

    } else {
      LOG_DEBUG(CNSUS, "Consensus reached");
    }

    while (reqIter.getAndGoToNext(requestBody)) {
      size_t tmp = reqIdx;
      reqIdx++;
      if (!requestSet.get(tmp)) {
        continue;
      }

      ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);
      SCOPED_MDC_CID(req.getCid());
      NodeIdType clientId = req.clientProxyId();

      uint32_t actualReplyLength = 0;
      uint32_t actualReplicaSpecificInfoLength = 0;
      bftRequestsHandler_.execute(
          clientId,
          lastExecutedSeqNum + 1,
          req.flags(),
          req.requestLength(),
          req.requestBuf(),
          ReplicaConfigSingleton::GetInstance().GetMaxReplyMessageSize() - sizeof(ClientReplyMsgHeader),
          replyBuffer,
          actualReplyLength,
          actualReplicaSpecificInfoLength,
          span);

      ConcordAssertGT(actualReplyLength,
                      0);  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)

      ClientReplyMsg *replyMsg = clientsManager->allocateNewReplyMsgAndWriteToStorage(
          clientId, req.requestSeqNum(), currentPrimary(), replyBuffer, actualReplyLength);
      replyMsg->setReplicaSpecificInfoLength(actualReplicaSpecificInfoLength);
      send(replyMsg, clientId);
      delete replyMsg;
      clientsManager->removePendingRequestOfClient(clientId);
    }
  }

  uint64_t checkpointNum{};
  if ((lastExecutedSeqNum + 1) % checkpointWindowSize == 0) {
    checkpointNum = (lastExecutedSeqNum + 1) / checkpointWindowSize;
    stateTransfer->createCheckpointOfCurrentState(checkpointNum);
  }

  //////////////////////////////////////////////////////////////////////
  // Phase 3: finalize the execution of lastExecutedSeqNum+1
  // TODO(GG): Explain what happens in recovery mode
  //////////////////////////////////////////////////////////////////////

  LOG_DEBUG(GL, "Finalized execution. " << KVLOG(lastExecutedSeqNum + 1, curView, lastStableSeqNum));

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setLastExecutedSeqNum(lastExecutedSeqNum + 1);
  }

  lastExecutedSeqNum = lastExecutedSeqNum + 1;

  if (config_.debugStatisticsEnabled) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
  }
  if (lastViewThatTransferredSeqNumbersFullyExecuted < curView &&
      (lastExecutedSeqNum >= maxSeqNumTransferredFromPrevViews)) {
    lastViewThatTransferredSeqNumbersFullyExecuted = curView;
    if (ps_) {
      ps_->setLastViewThatTransferredSeqNumbersFullyExecuted(lastViewThatTransferredSeqNumbersFullyExecuted);
    }
  }

  if (lastExecutedSeqNum % checkpointWindowSize == 0) {
    Digest checkDigest;
    const uint64_t checkpointNum = lastExecutedSeqNum / checkpointWindowSize;
    stateTransfer->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char *)&checkDigest);
    CheckpointMsg *checkMsg = new CheckpointMsg(config_.replicaId, lastExecutedSeqNum, checkDigest, false);
    CheckpointInfo &checkInfo = checkpointsLog->get(lastExecutedSeqNum);
    checkInfo.addCheckpointMsg(checkMsg, config_.replicaId);

    if (ps_) ps_->setCheckpointMsgInCheckWindow(lastExecutedSeqNum, checkMsg);

    if (checkInfo.isCheckpointCertificateComplete()) {
      onSeqNumIsStable(lastExecutedSeqNum);
    }
    checkInfo.setSelfExecutionTime(getMonotonicTime());
    if (checkInfo.isCheckpointSuperStable()) {
      onSeqNumIsSuperStable(lastStableSeqNum);
    }

    KeyManager::get().onCheckpoint(checkpointNum);
  }

  if (ps_) ps_->endWriteTran();

  if (numOfRequests > 0) bftRequestsHandler_.onFinishExecutingReadWriteRequests();

  sendCheckpointIfNeeded();

  bool firstCommitPathChanged = controller->onNewSeqNumberExecution(lastExecutedSeqNum);

  if (firstCommitPathChanged) {
    metric_first_commit_path_.Get().Set(CommitPathToStr(controller->getCurrentFirstPath()));
  }
  // TODO(GG): clean the following logic
  if (mainLog->insideActiveWindow(lastExecutedSeqNum)) {  // update dynamicUpperLimitOfRounds
    const SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum);
    const Time firstInfo = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();
    const Time currTime = getMonotonicTime();
    if ((firstInfo < currTime)) {
      const int64_t durationMilli = duration_cast<milliseconds>(currTime - firstInfo).count();
      dynamicUpperLimitOfRounds->add(durationMilli);
    }
  }

  if (config_.debugStatisticsEnabled) {
    DebugStatistics::onRequestCompleted(false);
  }
}

void ReplicaImp::executeNextCommittedRequests(concordUtils::SpanWrapper &parent_span, const bool requestMissingInfo) {
  ConcordAssertAND(!isCollectingState(), currentViewIsActive());
  ConcordAssertGE(lastExecutedSeqNum, lastStableSeqNum);
  auto span = concordUtils::startChildSpan("bft_execute_next_committed_requests", parent_span);

  while (lastExecutedSeqNum < lastStableSeqNum + kWorkWindowSize) {
    SeqNum nextExecutedSeqNum = lastExecutedSeqNum + 1;
    SeqNumInfo &seqNumInfo = mainLog->get(nextExecutedSeqNum);

    PrePrepareMsg *prePrepareMsg = seqNumInfo.getPrePrepareMsg();

    const bool ready = (prePrepareMsg != nullptr) && (seqNumInfo.isCommitted__gg());

    if (requestMissingInfo && !ready) {
      LOG_INFO(GL, "Asking for missing information: " << KVLOG(nextExecutedSeqNum, curView, lastStableSeqNum));

      tryToSendReqMissingDataMsg(nextExecutedSeqNum);
    }

    if (!ready) break;

    ConcordAssertEQ(prePrepareMsg->seqNumber(), nextExecutedSeqNum);
    ConcordAssertEQ(prePrepareMsg->viewNumber(), curView);  // TODO(GG): TBD

    executeRequestsInPrePrepareMsg(span, prePrepareMsg);
    metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
    metric_total_finished_consensuses_.Get().Inc();
    if (seqNumInfo.slowPathStarted()) {
      metric_total_slowPath_.Get().Inc();
    } else {
      metric_total_fastPath_.Get().Inc();
    }
  }
  if (!stopAtNextCheckpoint_ && controlStateManager_->getCheckpointToStopAt().has_value()) {
    // If, following the last execution, we discover that we need to jump to the
    // next checkpoint, the primary sends noop commands until filling the working window.
    bringTheSystemToCheckpointBySendingNoopCommands(controlStateManager_->getCheckpointToStopAt().value());
    stopAtNextCheckpoint_ = true;
    // Enable retransmitting of the super stable checkpoint to help weak connected replicas to reach the super stable
    // checkpoint.
    // Note that once we get to this point, we are at a dead end - once the replica stopped from processing requests it
    // unable to resume itself without external interfere as resuming required to process a command.
    // Thus, we don't care to activate this timer.
    enableRetransmitSuperStableCheckpoint_ = true;
    superStableCheckpointRetransmitTimer_ = timers_.add(milliseconds(timeoutOfSuperStableCheckpointTimerMs_),
                                                        Timers::Timer::RECURRING,
                                                        [this](Timers::Handle h) { onSuperStableCheckpointTimer(h); });
  }
  if (isCurrentPrimary() && requestsQueueOfPrimary.size() > 0) tryToSendPrePrepareMsg(true);
}

IncomingMsgsStorage &ReplicaImp::getIncomingMsgsStorage() { return *msgsCommunicator_->getIncomingMsgsStorage(); }

// TODO(GG): the timer for state transfer !!!!

// TODO(GG): !!!! view changes and retransmissionsLogic --- check ....

}  // namespace bftEngine::impl
