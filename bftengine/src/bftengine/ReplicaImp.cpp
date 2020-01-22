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
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "ControllerWithSimpleHistory.hpp"
#include "DebugStatistics.hpp"
#include "SysConsts.hpp"
#include "ReplicaConfig.hpp"
#include "TimersSingleton.hpp"
#include "MsgsCommunicator.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "ReplicaLoader.hpp"
#include "PersistentStorage.hpp"

#include "messages/ClientRequestMsg.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/CheckpointMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "messages/PartialExecProofMsg.hpp"
#include "messages/FullExecProofMsg.hpp"
#include "messages/StartSlowCommitMsg.hpp"
#include "messages/ReqMissingDataMsg.hpp"
#include "messages/SimpleAckMsg.hpp"
#include "messages/ViewChangeMsg.hpp"
#include "messages/NewViewMsg.hpp"
#include "messages/PartialCommitProofMsg.hpp"
#include "messages/FullCommitProofMsg.hpp"
#include "messages/ReplicaStatusMsg.hpp"

using concordUtil::Timers;
using namespace std;
using namespace std::chrono;
using namespace std::placeholders;

namespace bftEngine::impl {

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

  msgHandlers_->registerMsgHandler(MsgCode::PartialExecProof,
                                   bind(&ReplicaImp::messageHandler<PartialExecProofMsg>, this, _1));

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

void ReplicaImp::onReportAboutInvalidMessage(MessageBase *msg, const char *reason) {
  LOG_WARN(GL,
           "Node " << config_.replicaId << " received invalid message from Node " << msg->senderId()
                   << " type=" << msg->type() << " reason: " << reason);

  // TODO(GG): logic that deals with invalid messages (e.g., a node that sends invalid messages may have a problem (old
  // version,bug,malicious,...)).
}

template <>
void ReplicaImp::onMessage<ClientRequestMsg>(ClientRequestMsg *m) {
  metric_received_client_requests_.Get().Inc();
  const NodeIdType senderId = m->senderId();
  const NodeIdType clientId = m->clientProxyId();
  const bool readOnly = m->isReadOnly();
  const ReqId reqSeqNum = m->requestSeqNum();

  LOG_INFO_F(GL,
             "Node %d received ClientRequestMsg (clientId=%d reqSeqNum=%" PRIu64 ", readOnly=%d) from Node %d",
             config_.replicaId,
             clientId,
             reqSeqNum,
             readOnly ? 1 : 0,
             senderId);

  if (isCollectingState()) {
    LOG_INFO(GL,
             "ClientRequestMsg reqSeqNum="
                 << reqSeqNum
                 << " is ignored because this replica is collecting missing state from the other replicas");
    delete m;
    return;
  }

  // check message validity
  const bool invalidClient = !isValidClient(clientId);
  const bool sentFromReplicaToNonPrimary = repsInfo->isIdOfReplica(senderId) && !isCurrentPrimary();

  // TODO(GG): more conditions (make sure that a request of client A cannot be generated by client B!=A)
  if (invalidClient || sentFromReplicaToNonPrimary) {
    std::ostringstream oss("ClientRequestMsg is invalid: invalidClient=");
    oss << invalidClient << ", sentFromReplicaToNonPrimary=" << sentFromReplicaToNonPrimary;
    onReportAboutInvalidMessage(m, oss.str().c_str());
    delete m;
    return;
  }

  if (readOnly) {
    executeReadOnlyRequest(m);
    delete m;
    return;
  }

  if (!currentViewIsActive()) {
    LOG_INFO_F(GL, "ClientRequestMsg is ignored because current view is inactive");
    delete m;
    return;
  }

  const ReqId seqNumberOfLastReply = seqNumberOfLastReplyToClient(clientId);

  if (seqNumberOfLastReply < reqSeqNum) {
    if (isCurrentPrimary()) {
      if (clientsManager->noPendingAndRequestCanBecomePending(clientId, reqSeqNum) &&
          (requestsQueueOfPrimary.size() < 700))  // TODO(GG): use config/parameter
      {
        requestsQueueOfPrimary.push(m);
        tryToSendPrePrepareMsg(true);
        return;
      } else {
        LOG_INFO(GL,
                 "ClientRequestMsg reqSeqNum="
                     << reqSeqNum
                     << " is ignored because: request is old, OR primary is current working on a request "
                        "from the same client, OR queue contains too many requests");
      }
    } else {  // not the current primary
      if (clientsManager->noPendingAndRequestCanBecomePending(clientId, reqSeqNum)) {
        clientsManager->addPendingRequest(clientId, reqSeqNum);

        // TODO(GG): add a mechanism that retransmits (otherwise we may start unnecessary view-change)
        send(m, currentPrimary());

        LOG_INFO(GL, "Sending ClientRequestMsg reqSeqNum=" << reqSeqNum << " to current primary");
      } else {
        LOG_INFO(GL,
                 "ClientRequestMsg reqSeqNum="
                     << reqSeqNum
                     << " is ignored because request is old or replica has another pending request from the "
                        "same client");
      }
    }
  } else if (seqNumberOfLastReply == reqSeqNum) {
    LOG_DEBUG(GL,
              "ClientRequestMsg reqSeqNum=" << reqSeqNum << " has already been executed - retransmit reply to client");

    ClientReplyMsg *repMsg = clientsManager->allocateMsgWithLatestReply(clientId, currentPrimary());

    send(repMsg, clientId);

    delete repMsg;
  } else {
    LOG_INFO(GL, "ClientRequestMsg reqSeqNum=" << reqSeqNum << " is ignored because request is old");
  }

  delete m;
}

void ReplicaImp::tryToSendPrePrepareMsg(bool batchingLogic) {
  Assert(isCurrentPrimary() && currentViewIsActive());

  if (primaryLastUsedSeqNum + 1 > lastStableSeqNum + kWorkWindowSize) return;

  if (primaryLastUsedSeqNum + 1 > lastExecutedSeqNum + config_.concurrencyLevel)
    return;  // TODO(GG): should also be checked by the non-primary replicas

  if (requestsQueueOfPrimary.empty()) return;

  // remove irrelevant requests from the head of the requestsQueueOfPrimary (and update requestsInQueue)
  ClientRequestMsg *first = requestsQueueOfPrimary.front();
  while (first != nullptr &&
         !clientsManager->noPendingAndRequestCanBecomePending(first->clientProxyId(), first->requestSeqNum())) {
    delete first;
    requestsQueueOfPrimary.pop();
    first = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
  }

  const size_t requestsInQueue = requestsQueueOfPrimary.size();

  if (requestsInQueue == 0) return;

  Assert(primaryLastUsedSeqNum >= lastExecutedSeqNum);

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

  if (requestsInQueue < minBatchSize) return;

  // update batchingFactor
  if (((primaryLastUsedSeqNum + 1) % kWorkWindowSize) ==
      0)  // TODO(GG): do we want to update batchingFactor when the view is changed
  {
    const size_t aa = 4;  // TODO(GG): read from configuration
    batchingFactor = (maxNumberOfPendingRequestsInRecentHistory / aa);
    if (batchingFactor < 1) batchingFactor = 1;
    maxNumberOfPendingRequestsInRecentHistory = 0;
  }

  // because maxConcurrentAgreementsByPrimary <  MaxConcurrentFastPaths
  Assert((primaryLastUsedSeqNum + 1) <= lastExecutedSeqNum + MaxConcurrentFastPaths);

  CommitPath firstPath = controller->getCurrentFirstPath();

  // assert: (cVal==0) --> (firstPath != CommitPath::FAST_WITH_THRESHOLD)
  Assert((config_.cVal != 0) || (firstPath != CommitPath::FAST_WITH_THRESHOLD));

  controller->onSendingPrePrepare((primaryLastUsedSeqNum + 1), firstPath);

  PrePrepareMsg *pp = new PrePrepareMsg(config_.replicaId, curView, (primaryLastUsedSeqNum + 1), firstPath, false);

  ClientRequestMsg *nextRequest = requestsQueueOfPrimary.front();
  while (nextRequest != nullptr && nextRequest->size() <= pp->remainingSizeForRequests()) {
    if (clientsManager->noPendingAndRequestCanBecomePending(nextRequest->clientProxyId(),
                                                            nextRequest->requestSeqNum())) {
      pp->addRequest(nextRequest->body(), nextRequest->size());
      clientsManager->addPendingRequest(nextRequest->clientProxyId(), nextRequest->requestSeqNum());
    }
    delete nextRequest;
    requestsQueueOfPrimary.pop();
    nextRequest = (requestsQueueOfPrimary.size() > 0 ? requestsQueueOfPrimary.front() : nullptr);
  }

  pp->finishAddingRequests();

  Assert(pp->numberOfRequests() > 0);

  if (config_.debugStatisticsEnabled) {
    DebugStatistics::onSendPrePrepareMessage(pp->numberOfRequests(), requestsQueueOfPrimary.size());
  }
  primaryLastUsedSeqNum++;

  SeqNumInfo &seqNumInfo = mainLog->get(primaryLastUsedSeqNum);
  seqNumInfo.addSelfMsg(pp);

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
    ps_->setPrePrepareMsgInSeqNumWindow(primaryLastUsedSeqNum, pp);
    if (firstPath == CommitPath::SLOW) ps_->setSlowStartedInSeqNumWindow(primaryLastUsedSeqNum, true);
    ps_->endWriteTran();
  }

  LOG_DEBUG(GL,
            "Node " << config_.replicaId << " Sending PrePrepareMsg (seqNumber=" << pp->seqNumber() << ", requests="
                    << (int)pp->numberOfRequests() << ", queue size=" << (int)requestsQueueOfPrimary.size() << ")");

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

template <typename T>
bool ReplicaImp::relevantMsgForActiveView(const T *msg) {
  const SeqNum msgSeqNum = msg->seqNumber();
  const ViewNum msgViewNum = msg->viewNumber();

  if (currentViewIsActive() && (msgViewNum == curView) && (msgSeqNum > strictLowerBoundOfSeqNums) &&
      (mainLog->insideActiveWindow(msgSeqNum))) {
    Assert(msgSeqNum > lastStableSeqNum);
    Assert(msgSeqNum <= lastStableSeqNum + kWorkWindowSize);

    return true;
  } else {
    const bool myReplicaMayBeBehind = (curView < msgViewNum) || (msgSeqNum > mainLog->currentActiveWindow().second);
    if (myReplicaMayBeBehind) {
      onReportAboutAdvancedReplica(msg->senderId(), msgSeqNum, msgViewNum);
    } else {
      const bool msgReplicaMayBeBehind =
          (curView > msgViewNum) || (msgSeqNum + kWorkWindowSize < mainLog->currentActiveWindow().first);

      if (msgReplicaMayBeBehind) onReportAboutLateReplica(msg->senderId(), msgSeqNum, msgViewNum);
    }

    return false;
  }
}

template <>
void ReplicaImp::onMessage<PrePrepareMsg>(PrePrepareMsg *msg) {
  metric_received_pre_prepares_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();

  LOG_DEBUG_F(GL,
              "Node %d received PrePrepareMsg from node %d for seqNumber %" PRId64 " (size=%d)",
              (int)config_.replicaId,
              (int)msg->senderId(),
              msgSeqNum,
              (int)msg->size());

  if (!currentViewIsActive() && viewsManager->waitingForMsgs() && msgSeqNum > lastStableSeqNum) {
    Assert(!msg->isNull());  // we should never send (and never accept) null PrePrepare message

    if (viewsManager->addPotentiallyMissingPP(msg, lastStableSeqNum)) {
      LOG_DEBUG_F(GL,
                  "Node %d adds PrePrepareMsg for seqNumber %" PRId64 " to viewsManager",
                  (int)config_.replicaId,
                  msgSeqNum);
      tryToEnterView();
    } else {
      LOG_DEBUG_F(GL,
                  "Node %d does not add PrePrepareMsg for seqNumber %" PRId64 " to viewsManager",
                  (int)config_.replicaId,
                  msgSeqNum);
    }

    return;  // TODO(GG): memory deallocation is confusing .....
  }

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg) && (msg->senderId() == currentPrimary())) {
    sendAckIfNeeded(msg, msg->senderId(), msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    if (seqNumInfo.addMsg(msg)) {
      msgAdded = true;

      const bool slowStarted = (msg->firstPath() == CommitPath::SLOW || seqNumInfo.slowPathStarted());

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

  if (minSeqNum > lastStableSeqNum + kWorkWindowSize) {
    LOG_INFO_F(GL, "Replica::tryToStartSlowPaths() : minSeqNum > lastStableSeqNum + kWorkWindowSize");
    return;
  }

  const SeqNum maxSeqNum = primaryLastUsedSeqNum;

  Assert(maxSeqNum <= lastStableSeqNum + kWorkWindowSize);
  Assert(minSeqNum <= maxSeqNum + 1);

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

    LOG_INFO_F(GL,
               "Primary initiates slow path for seqNum=%" PRId64 " (currTime=%" PRIu64 " timeOfPartProof=%" PRIu64,
               i,
               duration_cast<microseconds>(currTime.time_since_epoch()).count(),
               duration_cast<microseconds>(timeOfPartProof.time_since_epoch()).count());

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

  Assert(maxSeqNumTransferredFromPrevViews <= lastStableSeqNum + kWorkWindowSize);

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
    Assert(mainLog->insideActiveWindow(i));

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

  LOG_INFO_F(GL, "Node %d received StartSlowCommitMsg for seqNumber %" PRId64 "", config_.replicaId, msgSeqNum);

  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, currentPrimary(), msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    if (!seqNumInfo.slowPathStarted() && !seqNumInfo.isPrepared()) {
      LOG_INFO_F(GL, "Node %d starts slow path for seqNumber %" PRId64 "", config_.replicaId, msgSeqNum);

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

      Assert((config_.cVal != 0) || (commitPath != CommitPath::FAST_WITH_THRESHOLD));

      if ((commitPath == CommitPath::FAST_WITH_THRESHOLD) && (config_.cVal > 0))
        commitSigner = config_.thresholdSignerForCommit;
      else
        commitSigner = config_.thresholdSignerForOptimisticCommit;

      Digest tmpDigest;
      Digest::calcCombination(ppDigest, curView, seqNum, tmpDigest);

      part = new PartialCommitProofMsg(config_.replicaId, curView, seqNum, commitPath, tmpDigest, commitSigner);
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
  Assert(currentViewIsActive());

  if (seqNumInfo.getSelfPreparePartialMsg() == nullptr && seqNumInfo.hasPrePrepareMsg() && !seqNumInfo.isPrepared()) {
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();

    Assert(pp != nullptr);

    LOG_DEBUG_F(GL, "Sending PreparePartialMsg for seqNumber %" PRId64 "", pp->seqNumber());

    PreparePartialMsg *p = PreparePartialMsg::create(
        curView, pp->seqNumber(), config_.replicaId, pp->digestOfRequests(), config_.thresholdSignerForSlowPathCommit);
    seqNumInfo.addSelfMsg(p);

    if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(p, currentPrimary(), pp->seqNumber());
  }
}

void ReplicaImp::sendCommitPartial(const SeqNum s) {
  Assert(currentViewIsActive());
  Assert(mainLog->insideActiveWindow(s));

  SeqNumInfo &seqNumInfo = mainLog->get(s);
  PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();

  Assert(seqNumInfo.isPrepared());
  Assert(pp != nullptr);
  Assert(pp->seqNumber() == s);

  if (seqNumInfo.committedOrHasCommitPartialFromReplica(config_.replicaId)) return;  // not needed

  LOG_DEBUG_F(GL, "Sending CommitPartialMsg for seqNumber %" PRId64 "", s);

  Digest d;
  Digest::digestOfDigest(pp->digestOfRequests(), d);

  CommitPartialMsg *c =
      CommitPartialMsg::create(curView, s, config_.replicaId, d, config_.thresholdSignerForSlowPathCommit);
  seqNumInfo.addSelfCommitPartialMsgAndDigest(c, d);

  if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(c, currentPrimary(), s);
}

template <>
void ReplicaImp::onMessage<PartialCommitProofMsg>(PartialCommitProofMsg *msg) {
  metric_received_partial_commit_proofs_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const SeqNum msgView = msg->viewNumber();
  const NodeIdType msgSender = msg->senderId();

  Assert(repsInfo->isIdOfPeerReplica(msgSender));
  Assert(repsInfo->isCollectorForPartialProofs(msgView, msgSeqNum));

  LOG_DEBUG_F(GL,
              "Node %d received PartialCommitProofMsg (size=%d) from node %d for seqNumber %" PRId64 "",
              (int)config_.replicaId,
              (int)msg->size(),
              (int)msgSender,
              msgSeqNum);

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
  LOG_DEBUG_F(GL,
              "Node %d received FullCommitProofMsg message for seqNumber %d",
              (int)config_.replicaId,
              (int)msg->seqNumber());

  const SeqNum msgSeqNum = msg->seqNumber();

  if (relevantMsgForActiveView(msg)) {
    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);
    PartialProofsSet &pps = seqNumInfo.partialProofs();

    if (!pps.hasFullProof() && pps.addMsg(msg))  // TODO(GG): consider to verify the signature in another thread
    {
      Assert(seqNumInfo.hasPrePrepareMsg());

      seqNumInfo.forceComplete();  // TODO(GG): remove forceComplete() (we know that  seqNumInfo is committed because of
                                   // the  FullCommitProofMsg message)

      if (ps_) {
        ps_->beginWriteTran();
        ps_->setFullCommitProofMsgInSeqNumWindow(msgSeqNum, msg);
        ps_->setForceCompletedInSeqNumWindow(msgSeqNum, true);
        ps_->endWriteTran();
      }

      if (msg->senderId() == config_.replicaId) sendToAllOtherReplicas(msg);

      const bool askForMissingInfoAboutCommittedItems =
          (msgSeqNum > lastExecutedSeqNum + config_.concurrencyLevel);  // TODO(GG): check/improve this logic
      executeNextCommittedRequests(askForMissingInfoAboutCommittedItems);

      return;
    }
  }

  delete msg;
  return;
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

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg)) {
    Assert(isCurrentPrimary());

    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG_F(GL,
                "Node %d received PreparePartialMsg from node %d for seqNumber %" PRId64 "",
                (int)config_.replicaId,
                (int)msgSender,
                msgSeqNum);

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
    LOG_DEBUG_F(GL,
                "Node %d ignored the PreparePartialMsg from node %d (seqNumber %" PRId64 ")",
                (int)config_.replicaId,
                (int)msgSender,
                msgSeqNum);
    delete msg;
  }
}

template <>
void ReplicaImp::onMessage<CommitPartialMsg>(CommitPartialMsg *msg) {
  metric_received_commit_partials_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg)) {
    Assert(isCurrentPrimary());

    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG_F(GL,
                "Node %d received CommitPartialMsg from node %d for seqNumber %" PRId64 "",
                (int)config_.replicaId,
                (int)msgSender,
                msgSeqNum);

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
    LOG_DEBUG_F(GL,
                "Node %d ignored the CommitPartialMsg from node %d (seqNumber %" PRId64 ")",
                (int)config_.replicaId,
                (int)msgSender,
                msgSeqNum);
    delete msg;
  }
}

template <>
void ReplicaImp::onMessage<PrepareFullMsg>(PrepareFullMsg *msg) {
  metric_received_prepare_fulls_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG_F(GL, "Node %d received PrepareFullMsg for seqNumber %" PRId64 "", (int)config_.replicaId, msgSeqNum);

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
    LOG_DEBUG_F(GL,
                "Node %d ignored the PrepareFullMsg from node %d (seqNumber %" PRId64 ")",
                (int)config_.replicaId,
                (int)msgSender,
                msgSeqNum);
    delete msg;
  }
}
template <>
void ReplicaImp::onMessage<CommitFullMsg>(CommitFullMsg *msg) {
  metric_received_commit_fulls_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG_F(GL, "Node %d received CommitFullMsg for seqNumber %" PRId64 "", (int)config_.replicaId, msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

    if (fcp != nullptr) {
      send(fcp,
           msgSender);  // TODO(GG): do we really want to send this message ? (msgSender already has a CommitFullMsg for
                        // the same seq number)
    } else if (commitFull != nullptr) {
      // nop
    } else {
      msgAdded = seqNumInfo.addMsg(msg);
    }
  }

  if (!msgAdded) {
    LOG_DEBUG_F(GL,
                "Node %d ignored the CommitFullMsg from node %d (seqNumber %" PRId64 ")",
                (int)config_.replicaId,
                (int)msgSender,
                msgSeqNum);
    delete msg;
  }
}

void ReplicaImp::onPrepareCombinedSigFailed(SeqNum seqNumber,
                                            ViewNum v,
                                            const std::set<uint16_t> &replicasWithBadSigs) {
  LOG_DEBUG_F(GL,
              "Node %d - onPrepareCombinedSigFailed seqNumber=%" PRId64 " view=%" PRId64 "",
              (int)config_.replicaId,
              seqNumber,
              v);

  if ((isCollectingState()) || (!currentViewIsActive()) || (curView != v) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onPrepareCombinedSigFailed - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)config_.replicaId,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, v, replicasWithBadSigs);

  // TODO(GG): add logic that handles bad replicas ...
}

void ReplicaImp::onPrepareCombinedSigSucceeded(SeqNum seqNumber,
                                               ViewNum v,
                                               const char *combinedSig,
                                               uint16_t combinedSigLen) {
  LOG_DEBUG_F(GL,
              "Node %d - onPrepareCombinedSigSucceeded seqNumber=%" PRId64 " view=%" PRId64 "",
              (int)config_.replicaId,
              seqNumber,
              v);

  if ((isCollectingState()) || (!currentViewIsActive()) || (curView != v) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onPrepareCombinedSigSucceeded - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)config_.replicaId,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, v, combinedSig, combinedSigLen);

  FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

  PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();

  Assert(preFull != nullptr);

  if (fcp != nullptr) return;  // don't send if we already have FullCommitProofMsg

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setPrepareFullMsgInSeqNumWindow(seqNumber, preFull);
    ps_->endWriteTran();
  }

  for (ReplicaId x : repsInfo->idsOfPeerReplicas()) sendRetransmittableMsgToReplica(preFull, x, seqNumber);

  Assert(seqNumInfo.isPrepared());

  sendCommitPartial(seqNumber);
}

void ReplicaImp::onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum v, bool isValid) {
  LOG_DEBUG_F(GL,
              "Node %d - onPrepareVerifyCombinedSigResult seqNumber=%" PRId64 " view=%" PRId64 "",
              (int)config_.replicaId,
              seqNumber,
              v);

  if ((isCollectingState()) || (!currentViewIsActive()) || (curView != v) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onPrepareVerifyCombinedSigResult - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)config_.replicaId,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCombinedPrepareSigVerification(seqNumber, v, isValid);

  if (!isValid) return;  // TODO(GG): we should do something about the replica that sent this invalid message

  FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();

  if (fcp != nullptr) return;  // don't send if we already have FullCommitProofMsg

  Assert(seqNumInfo.isPrepared());

  if (ps_) {
    PrepareFullMsg *preFull = seqNumInfo.getValidPrepareFullMsg();
    Assert(preFull != nullptr);
    ps_->beginWriteTran();
    ps_->setPrepareFullMsgInSeqNumWindow(seqNumber, preFull);
    ps_->endWriteTran();
  }

  sendCommitPartial(seqNumber);
}

void ReplicaImp::onCommitCombinedSigFailed(SeqNum seqNumber, ViewNum v, const std::set<uint16_t> &replicasWithBadSigs) {
  LOG_DEBUG_F(GL,
              "Node %d - onCommitCombinedSigFailed seqNumber=%" PRId64 " view=%" PRId64 "",
              (int)config_.replicaId,
              seqNumber,
              v);

  if ((isCollectingState()) || (!currentViewIsActive()) || (curView != v) ||
      (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onCommitCombinedSigFailed - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)config_.replicaId,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCommitSignaturesProcessing(seqNumber, v, replicasWithBadSigs);

  // TODO(GG): add logic that handles bad replicas ...
}

void ReplicaImp::onCommitCombinedSigSucceeded(SeqNum seqNumber,
                                              ViewNum v,
                                              const char *combinedSig,
                                              uint16_t combinedSigLen) {
  LOG_DEBUG_F(GL,
              "Node %d - onCommitCombinedSigSucceeded seqNumber=%" PRId64 " view=%" PRId64 "",
              (int)config_.replicaId,
              seqNumber,
              v);

  if (isCollectingState() || (!currentViewIsActive()) || (curView != v) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onCommitCombinedSigSucceeded - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)config_.replicaId,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCommitSignaturesProcessing(seqNumber, v, combinedSig, combinedSigLen);

  FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();
  CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();

  Assert(commitFull != nullptr);

  if (fcp != nullptr) return;  // ignore if we already have FullCommitProofMsg

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setCommitFullMsgInSeqNumWindow(seqNumber, commitFull);
    ps_->endWriteTran();
  }

  for (ReplicaId x : repsInfo->idsOfPeerReplicas()) sendRetransmittableMsgToReplica(commitFull, x, seqNumber);

  Assert(seqNumInfo.isCommitted__gg());

  bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum + config_.concurrencyLevel);

  executeNextCommittedRequests(askForMissingInfoAboutCommittedItems);
}

void ReplicaImp::onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum v, bool isValid) {
  LOG_DEBUG_F(GL,
              "Node %d - onCommitVerifyCombinedSigResult seqNumber=%" PRId64 " view=%" PRId64 "",
              config_.replicaId,
              seqNumber,
              v);

  if (isCollectingState() || (!currentViewIsActive()) || (curView != v) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onCommitVerifyCombinedSigResult - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)config_.replicaId,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog->get(seqNumber);

  seqNumInfo.onCompletionOfCombinedCommitSigVerification(seqNumber, v, isValid);

  if (!isValid) return;  // TODO(GG): we should do something about the replica that sent this invalid message

  Assert(seqNumInfo.isCommitted__gg());

  if (ps_) {
    CommitFullMsg *commitFull = seqNumInfo.getValidCommitFullMsg();
    Assert(commitFull != nullptr);
    ps_->beginWriteTran();
    ps_->setCommitFullMsgInSeqNumWindow(seqNumber, commitFull);
    ps_->endWriteTran();
  }

  bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum + config_.concurrencyLevel);
  executeNextCommittedRequests(askForMissingInfoAboutCommittedItems);
}
template <>
void ReplicaImp::onMessage<CheckpointMsg>(CheckpointMsg *msg) {
  metric_received_checkpoints_.Get().Inc();
  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgSeqNum = msg->seqNumber();
  const Digest msgDigest = msg->digestOfState();
  const bool msgIsStable = msg->isStableState();

  LOG_DEBUG_F(GL,
              "Node %d received Checkpoint message from node %d for seqNumber %" PRId64
              " (size=%d, stable=%s, digestPrefix=%d)",
              (int)config_.replicaId,
              (int)msgSenderId,
              msgSeqNum,
              (int)msg->size(),
              (int)msgIsStable ? "true" : "false",
              *((int *)(&msgDigest)));

  if ((msgSeqNum > lastStableSeqNum) && (msgSeqNum <= lastStableSeqNum + kWorkWindowSize)) {
    Assert(mainLog->insideActiveWindow(msgSeqNum));
    CheckpointInfo &checkInfo = checkpointsLog->get(msgSeqNum);
    bool msgAdded = checkInfo.addCheckpointMsg(msg, msg->senderId());

    if (msgAdded)
      LOG_DEBUG_F(GL,
                  "Node %d added Checkpoint message from node %d for seqNumber %" PRId64 "",
                  (int)config_.replicaId,
                  (int)msgSenderId,
                  msgSeqNum);

    if (checkInfo.isCheckpointCertificateComplete()) {
      Assert(checkInfo.selfCheckpointMsg() != nullptr);
      onSeqNumIsStable(msgSeqNum);

      return;
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

      LOG_INFO_F(GL,
                 "Node %d added stable Checkpoint message to tableOfStableCheckpoints (message from node %d for "
                 "seqNumber %" PRId64 ")",
                 (int)config_.replicaId,
                 (int)msgSenderId,
                 msgSeqNum);

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
        Assert(numRelevant == tableOfStableCheckpoints.size());

        LOG_DEBUG_F(GL, "numRelevant=%d    numRelevantAboveWindow=%d", (int)numRelevant, (int)numRelevantAboveWindow);

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
    LOG_INFO_F(GL, "call to startCollectingState()");

    stateTransfer->startCollectingState();
  } else if (msgSeqNum > lastStableSeqNum + kWorkWindowSize) {
    onReportAboutAdvancedReplica(msgSenderId, msgSeqNum);
  } else if (msgSeqNum + kWorkWindowSize < lastStableSeqNum) {
    onReportAboutLateReplica(msgSenderId, msgSeqNum);
  }
}

bool ReplicaImp::handledByRetransmissionsManager(const ReplicaId sourceReplica,
                                                 const ReplicaId destReplica,
                                                 const ReplicaId primaryReplica,
                                                 const SeqNum seqNum,
                                                 const uint16_t msgType) {
  Assert(retransmissionsLogicEnabled);

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
  Assert(retransmissionsLogicEnabled);

  retransmissionsManager->tryToStartProcessing();
}

void ReplicaImp::onRetransmissionsProcessingResults(
    SeqNum relatedLastStableSeqNum,
    const ViewNum relatedViewNumber,
    const std::forward_list<RetSuggestion> *const suggestedRetransmissions) {
  Assert(retransmissionsLogicEnabled);

  if (isCollectingState() || (relatedViewNumber != curView) || (!currentViewIsActive())) return;
  if (relatedLastStableSeqNum + kWorkWindowSize <= lastStableSeqNum) return;

  const uint16_t myId = config_.replicaId;
  const uint16_t primaryId = currentPrimary();

  for (const RetSuggestion &s : *suggestedRetransmissions) {
    if ((s.msgSeqNum <= lastStableSeqNum) || (s.msgSeqNum > lastStableSeqNum + kWorkWindowSize)) continue;

    Assert(s.replicaId != myId);

    Assert(handledByRetransmissionsManager(myId, s.replicaId, primaryId, s.msgSeqNum, s.msgType));

    switch (s.msgType) {
      case MsgCode::PrePrepare: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PrePrepareMsg *msgToSend = seqNumInfo.getSelfPrePrepareMsg();
        Assert(msgToSend != nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG_F(GL,
                    "Replica %d retransmits to replica %d PrePrepareMsg with seqNumber %" PRId64 "",
                    (int)myId,
                    (int)s.replicaId,
                    s.msgSeqNum);
      } break;
      case MsgCode::PartialCommitProof: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PartialCommitProofMsg *msgToSend = seqNumInfo.partialProofs().getSelfPartialCommitProof();
        Assert(msgToSend != nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG_F(GL,
                    "Replica %d retransmits to replica %d PartialCommitProofMsg with seqNumber %" PRId64 "",
                    (int)myId,
                    (int)s.replicaId,
                    s.msgSeqNum);
      } break;
        /*  TODO(GG): do we want to use acks for FullCommitProofMsg ?
         */
      case MsgCode::StartSlowCommit: {
        StartSlowCommitMsg *msgToSend = new StartSlowCommitMsg(myId, curView, s.msgSeqNum);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        delete msgToSend;
        LOG_DEBUG_F(GL,
                    "Replica %d retransmits to replica %d StartSlowCommitMsg with seqNumber %" PRId64 "",
                    (int)myId,
                    (int)s.replicaId,
                    s.msgSeqNum);
      } break;
      case MsgCode::PreparePartial: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PreparePartialMsg *msgToSend = seqNumInfo.getSelfPreparePartialMsg();
        Assert(msgToSend != nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG_F(GL,
                    "Replica %d retransmits to replica %d PreparePartialMsg with seqNumber %" PRId64 "",
                    (int)myId,
                    (int)s.replicaId,
                    s.msgSeqNum);
      } break;
      case MsgCode::PrepareFull: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PrepareFullMsg *msgToSend = seqNumInfo.getValidPrepareFullMsg();
        Assert(msgToSend != nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG_F(GL,
                    "Replica %d retransmits to replica %d PrepareFullMsg with seqNumber %" PRId64 "",
                    (int)myId,
                    (int)s.replicaId,
                    s.msgSeqNum);
      } break;

      case MsgCode::CommitPartial: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        CommitPartialMsg *msgToSend = seqNumInfo.getSelfCommitPartialMsg();
        Assert(msgToSend != nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG_F(GL,
                    "Replica %d retransmits to replica %d CommitPartialMsg with seqNumber %" PRId64 "",
                    (int)myId,
                    (int)s.replicaId,
                    s.msgSeqNum);
      } break;

      case MsgCode::CommitFull: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        CommitFullMsg *msgToSend = seqNumInfo.getValidCommitFullMsg();
        Assert(msgToSend != nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_INFO_F(GL,
                   "Replica %d retransmits to replica %d CommitFullMsg with seqNumber %" PRId64 "",
                   (int)myId,
                   (int)s.replicaId,
                   s.msgSeqNum);
      } break;

      default:
        Assert(false);
    }
  }
}

template <>
void ReplicaImp::onMessage<ReplicaStatusMsg>(ReplicaStatusMsg *msg) {
  metric_received_replica_statuses_.Get().Inc();
  // TODO(GG): we need filter for msgs (to avoid denial of service attack) + avoid sending messages at a high rate.
  // TODO(GG): for some communication modules/protocols, we can also utilize information about connection/disconnection.

  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgLastStable = msg->getLastStableSeqNum();
  const ViewNum msgViewNum = msg->getViewNumber();
  Assert(msgLastStable % checkpointWindowSize == 0);

  LOG_DEBUG_F(GL, "Node %d received ReplicaStatusMsg from node %d", (int)config_.replicaId, (int)msgSenderId);

  /////////////////////////////////////////////////////////////////////////
  // Checkpoints
  /////////////////////////////////////////////////////////////////////////

  if (lastStableSeqNum > msgLastStable + kWorkWindowSize) {
    CheckpointMsg *checkMsg = checkpointsLog->get(lastStableSeqNum).selfCheckpointMsg();

    if (checkMsg == nullptr || !checkMsg->isStableState()) {
      // TODO(GG): warning
    } else {
      send(checkMsg, msgSenderId);
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

    Assert(beginRange % checkpointWindowSize == 0);

    if (beginRange <= endRange) {
      Assert(endRange - beginRange <= kWorkWindowSize);

      for (SeqNum i = beginRange; i <= endRange; i = i + checkpointWindowSize) {
        CheckpointMsg *checkMsg = checkpointsLog->get(i).selfCheckpointMsg();
        if (checkMsg != nullptr) send(checkMsg, msgSenderId);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId in older view
  /////////////////////////////////////////////////////////////////////////

  if (msgViewNum < curView) {
    ViewChangeMsg *myVC = viewsManager->getMyLatestViewChangeMsg();
    Assert(myVC != nullptr);  // because curView>0
    send(myVC, msgSenderId);
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId needes information to enter view curView
  /////////////////////////////////////////////////////////////////////////

  else if ((msgViewNum == curView) && (!msg->currentViewIsActive())) {
    if (isCurrentPrimary() || (repsInfo->primaryOfView(curView) == msgSenderId))  // if the primary is involved
    {
      if (!isCurrentPrimary())  // I am not the primary of curView
      {
        // send ViewChangeMsg
        if (msg->hasListOfMissingViewChangeMsgForViewChange() &&
            msg->isMissingViewChangeMsgForViewChange(config_.replicaId)) {
          ViewChangeMsg *myVC = viewsManager->getMyLatestViewChangeMsg();
          Assert(myVC != nullptr);
          send(myVC, msgSenderId);
        }
      } else  // I am the primary of curView
      {
        // send NewViewMsg
        if (!msg->currentViewHasNewViewMessage() && viewsManager->viewIsActive(curView)) {
          NewViewMsg *nv = viewsManager->getMyNewViewMsgForCurrentView();
          Assert(nv != nullptr);
          send(nv, msgSenderId);
        }

        // send ViewChangeMsg
        if (msg->hasListOfMissingViewChangeMsgForViewChange() &&
            msg->isMissingViewChangeMsgForViewChange(config_.replicaId)) {
          ViewChangeMsg *myVC = viewsManager->getMyLatestViewChangeMsg();
          Assert(myVC != nullptr);
          send(myVC, msgSenderId);
        }
        // TODO(GG): send all VC msgs that can help making progress (needed because the original senders may not send
        // the ViewChangeMsg msgs used by the primary)

        // TODO(GG): if viewsManager->viewIsActive(curView), we can send only the VC msgs which are really needed for
        // curView (see in ViewsManager)
      }

      if (viewsManager->viewIsActive(curView)) {
        if (msg->hasListOfMissingPrePrepareMsgForViewChange()) {
          for (SeqNum i = msgLastStable + 1; i <= msgLastStable + kWorkWindowSize; i++) {
            if (mainLog->insideActiveWindow(i) && msg->isMissingPrePrepareMsgForViewChange(i)) {
              PrePrepareMsg *prePrepareMsg = mainLog->get(i).getPrePrepareMsg();
              if (prePrepareMsg != nullptr) send(prePrepareMsg, msgSenderId);
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
              if (prePrepareMsg != nullptr) send(prePrepareMsg, msgSenderId);
            }
          }
        }
      }
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
          if (prePrepareMsg != 0) send(prePrepareMsg, msgSenderId);
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
    Assert(msgViewNum > curView);
    tryToSendStatusReport();
  }

  delete msg;
}

void ReplicaImp::tryToSendStatusReport() {
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
        Assert((i > lastStableSeqNum) && (i <= lastStableSeqNum + kWorkWindowSize));
        msg.setMissingPrePrepareMsgForViewChange(i);
      }
    }
  }

  sendToAllOtherReplicas(&msg);
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
  Assert(generatedReplicaId != config_.replicaId);

  LOG_INFO_F(GL,
             "Node %d received ViewChangeMsg (generatedReplicaId=%d, newView=%" PRId64 ", lastStable=%" PRId64
             ", numberOfElements=%d).",
             (int)config_.replicaId,
             (int)generatedReplicaId,
             msg->newView(),
             msg->lastStable(),
             (int)msg->numberOfElements());

  bool msgAdded = viewsManager->add(msg);

  LOG_INFO_F(GL, "ViewChangeMsg add=%d", (int)msgAdded);

  if (!msgAdded) return;

  // if the current primary wants to leave view
  if (generatedReplicaId == currentPrimary() && msg->newView() > curView) {
    LOG_INFO_F(GL, "Primary asks to leave view (primary Id=%d , view=%" PRId64 ")", (int)generatedReplicaId, curView);
    MoveToHigherView(curView + 1);
  }

  ViewNum maxKnownCorrectView = 0;
  ViewNum maxKnownAgreedView = 0;
  viewsManager->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
  LOG_INFO_F(
      GL, "maxKnownCorrectView=%" PRId64 ", maxKnownAgreedView=%" PRId64 "", maxKnownCorrectView, maxKnownAgreedView);

  if (maxKnownCorrectView > curView) {
    // we have at least f+1 view-changes with view number >= maxKnownCorrectView
    MoveToHigherView(maxKnownCorrectView);

    // update maxKnownCorrectView and maxKnownAgreedView
    // TODO(GG): consider to optimize (this part is not always needed)
    viewsManager->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
    LOG_INFO_F(
        GL, "maxKnownCorrectView=%" PRId64 ", maxKnownAgreedView=%" PRId64 "", maxKnownCorrectView, maxKnownAgreedView);
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

  Assert(senderId != config_.replicaId);  // should be verified in ViewChangeMsg

  LOG_INFO_F(GL,
             "Node %d received NewViewMsg message (senderId=%d, newView=%" PRId64 ")",
             (int)config_.replicaId,
             (int)senderId,
             msg->newView());

  bool added = viewsManager->add(msg);

  LOG_INFO_F(GL, "NewViewMsg add=%d", (int)added);

  if (!added) return;

  if (viewsManager->viewIsActive(curView)) return;  // return, if we are still in the previous view

  tryToEnterView();
}

void ReplicaImp::MoveToHigherView(ViewNum nextView) {
  Assert(viewChangeProtocolEnabled);
  Assert(curView < nextView);

  const bool wasInPrevViewNumber = viewsManager->viewIsActive(curView);

  LOG_INFO_F(GL,
             "**************** In MoveToHigherView (curView=%" PRId64 ", nextView=%" PRId64 ", wasInPrevViewNumber=%d)",
             curView,
             nextView,
             (int)wasInPrevViewNumber);

  ViewChangeMsg *pVC = nullptr;

  if (!wasInPrevViewNumber) {
    pVC = viewsManager->getMyLatestViewChangeMsg();
    Assert(pVC != nullptr);
    pVC->setNewViewNumber(nextView);
  } else {
    std::vector<ViewsManager::PrevViewInfo> prevViewInfo;
    for (SeqNum i = lastStableSeqNum + 1; i <= lastStableSeqNum + kWorkWindowSize; i++) {
      SeqNumInfo &seqNumInfo = mainLog->get(i);

      if (seqNumInfo.getPrePrepareMsg() != nullptr) {
        ViewsManager::PrevViewInfo x;

        seqNumInfo.getAndReset(x.prePrepare, x.prepareFull);
        x.hasAllRequests = true;

        Assert(x.prePrepare != nullptr);
        Assert(x.prePrepare->viewNumber() == curView);
        // (x.prepareFull!=nullptr) ==> (x.hasAllRequests==true)
        Assert(x.prepareFull == nullptr || x.hasAllRequests);
        // (x.prepareFull!=nullptr) ==> (x.prepareFull->viewNumber() == curView)
        Assert(x.prepareFull == nullptr || x.prepareFull->viewNumber() == curView);

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

    Assert(pVC != nullptr);
    pVC->setNewViewNumber(nextView);
  }

  curView = nextView;
  metric_view_.Get().Set(nextView);

  LOG_INFO_F(GL,
             "Sending view change message: new view=%" PRId64
             ", wasInPrevViewNumber=%d, new primary=%d, lastExecutedSeqNum=%" PRId64 ", lastStableSeqNum=%" PRId64 "",
             curView,
             (int)wasInPrevViewNumber,
             (int)currentPrimary(),
             lastExecutedSeqNum,
             lastStableSeqNum);

  pVC->finalizeMessage();
  sendToAllOtherReplicas(pVC);
}

void ReplicaImp::GotoNextView() {
  // at this point we don't have f+1 ViewChangeMsg messages with view >= curView

  MoveToHigherView(curView + 1);

  // at this point we don't have enough ViewChangeMsg messages (2f+2c+1) to enter the new view (because 2f+2c+1 > f+1)
}

bool ReplicaImp::tryToEnterView() {
  Assert(!currentViewIsActive());

  std::vector<PrePrepareMsg *> prePreparesForNewView;

  LOG_INFO_F(GL,
             "**************** Calling to viewsManager->tryToEnterView(curView=%" PRId64 ", lastStableSeqNum=%" PRId64
             ", lastExecutedSeqNum=%" PRId64 ")",
             curView,
             lastStableSeqNum,
             lastExecutedSeqNum);

  bool succ = viewsManager->tryToEnterView(curView, lastStableSeqNum, lastExecutedSeqNum, &prePreparesForNewView);

  if (succ)
    onNewView(prePreparesForNewView);
  else
    tryToSendStatusReport();

  return succ;
}

void ReplicaImp::onNewView(const std::vector<PrePrepareMsg *> &prePreparesForNewView) {
  SeqNum firstPPSeq = 0;
  SeqNum lastPPSeq = 0;

  if (!prePreparesForNewView.empty()) {
    firstPPSeq = prePreparesForNewView.front()->seqNumber();
    lastPPSeq = prePreparesForNewView.back()->seqNumber();
  }

  LOG_INFO_F(GL,
             "**************** In onNewView curView=%" PRId64 " (num of PPs=%ld, first safe seq=%" PRId64
             ","
             " last safe seq=%" PRId64 ", lastStableSeqNum=%" PRId64 ", lastExecutedSeqNum=%" PRId64
             ","
             "stableLowerBoundWhenEnteredToView= %" PRId64 ")",
             curView,
             prePreparesForNewView.size(),
             firstPPSeq,
             lastPPSeq,
             lastStableSeqNum,
             lastExecutedSeqNum,
             viewsManager->stableLowerBoundWhenEnteredToView());

  Assert(viewsManager->viewIsActive(curView));
  Assert(lastStableSeqNum >= viewsManager->stableLowerBoundWhenEnteredToView());
  Assert(lastExecutedSeqNum >= lastStableSeqNum);  // we moved to the new state, only after synchronizing the state

  timeOfLastViewEntrance = getMonotonicTime();  // TODO(GG): handle restart/pause

  NewViewMsg *newNewViewMsgToSend = nullptr;

  if (repsInfo->primaryOfView(curView) == config_.replicaId) {
    NewViewMsg *nv = viewsManager->getMyNewViewMsgForCurrentView();

    nv->finalizeMessage(*repsInfo);

    Assert(nv->newView() == curView);

    newNewViewMsgToSend = nv;
  }

  if (prePreparesForNewView.empty()) {
    primaryLastUsedSeqNum = lastStableSeqNum;
    strictLowerBoundOfSeqNums = lastStableSeqNum;
    maxSeqNumTransferredFromPrevViews = lastStableSeqNum;
  } else {
    primaryLastUsedSeqNum = lastPPSeq;
    strictLowerBoundOfSeqNums = firstPPSeq - 1;
    maxSeqNumTransferredFromPrevViews = lastPPSeq;
  }

  if (ps_) {
    vector<ViewChangeMsg *> viewChangeMsgsForCurrentView = viewsManager->getViewChangeMsgsForCurrentView();
    NewViewMsg *newViewMsgForCurrentView = viewsManager->getNewViewMsgForCurrentView();

    bool myVCWasUsed = false;
    for (size_t i = 0; i < viewChangeMsgsForCurrentView.size() && !myVCWasUsed; i++) {
      Assert(viewChangeMsgsForCurrentView[i] != nullptr);
      if (viewChangeMsgsForCurrentView[i]->idOfGeneratedReplica() == config_.replicaId) myVCWasUsed = true;
    }

    ViewChangeMsg *myVC = nullptr;
    if (!myVCWasUsed) {
      myVC = viewsManager->getMyLatestViewChangeMsg();
    } else {
      // debug/test: check that my VC should be included
      ViewChangeMsg *tempMyVC = viewsManager->getMyLatestViewChangeMsg();
      Assert(tempMyVC != nullptr);
      Digest d;
      tempMyVC->getMsgDigest(d);
      Assert(newViewMsgForCurrentView->includesViewChangeFromReplica(config_.replicaId, d));
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
    Assert(pp->seqNumber() >= firstPPSeq);
    Assert(pp->seqNumber() <= lastPPSeq);
    Assert(pp->firstPath() == CommitPath::SLOW);  // TODO(GG): don't we want to use the fast path?
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
    delete requestsQueueOfPrimary.front();
    requestsQueueOfPrimary.pop();
  }

  // send messages

  if (newNewViewMsgToSend != nullptr) sendToAllOtherReplicas(newNewViewMsgToSend);

  for (size_t i = 0; i < prePreparesForNewView.size(); i++) {
    PrePrepareMsg *pp = prePreparesForNewView[i];
    SeqNumInfo &seqNumInfo = mainLog->get(pp->seqNumber());
    sendPreparePartial(seqNumInfo);
  }

  LOG_INFO_F(GL, "**************** Start working in view %" PRId64 "", curView);

  controller->onNewView(curView, primaryLastUsedSeqNum);
}

void ReplicaImp::sendCheckpointIfNeeded() {
  if (isCollectingState() || !currentViewIsActive()) return;

  const SeqNum lastCheckpointNumber = (lastExecutedSeqNum / checkpointWindowSize) * checkpointWindowSize;

  if (lastCheckpointNumber == 0) return;

  Assert(checkpointsLog->insideActiveWindow(lastCheckpointNumber));

  CheckpointInfo &checkInfo = checkpointsLog->get(lastCheckpointNumber);
  CheckpointMsg *checkpointMessage = checkInfo.selfCheckpointMsg();

  if (!checkpointMessage) {
    LOG_INFO_F(GL, "My Checkpoint message is missing");  // TODO(GG): TBD
    return;
  }

  if (checkInfo.checkpointSentAllOrApproved()) return;

  // TODO(GG): 3 seconds, should be in configuration
  if ((getMonotonicTime() - checkInfo.selfExecutionTime()) >= 3s) {
    checkInfo.setCheckpointSentAllOrApproved();
    sendToAllOtherReplicas(checkpointMessage);
    return;
  }

  const SeqNum refSeqNumberForCheckpoint = lastCheckpointNumber + MaxConcurrentFastPaths;

  if (lastExecutedSeqNum < refSeqNumberForCheckpoint) return;

  if (mainLog->insideActiveWindow(lastExecutedSeqNum))  // TODO(GG): condition is needed ?
  {
    SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum);

    if (seqNumInfo.partialProofs().hasFullProof()) {
      checkInfo.tryToMarkCheckpointCertificateCompleted();

      Assert(checkInfo.isCheckpointCertificateComplete());

      onSeqNumIsStable(lastCheckpointNumber);

      checkInfo.setCheckpointSentAllOrApproved();

      return;
    }
  }

  checkInfo.setCheckpointSentAllOrApproved();
  sendToAllOtherReplicas(checkpointMessage);
}

void ReplicaImp::onTransferringCompleteImp(SeqNum newStateCheckpoint) {
  Assert(newStateCheckpoint % checkpointWindowSize == 0);

  LOG_INFO_F(GL, "onTransferringCompleteImp with newStateCheckpoint==%" PRId64 "", newStateCheckpoint);

  if (ps_) {
    ps_->beginWriteTran();
  }

  if (newStateCheckpoint <= lastExecutedSeqNum) {
    LOG_DEBUG_F(
        GL, "Executing onTransferringCompleteImp(newStateCheckpoint) where newStateCheckpoint <= lastExecutedSeqNum");
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
  Assert(checkpointsLog->insideActiveWindow(newStateCheckpoint));

  // create and send my checkpoint
  Digest digestOfNewState;
  const uint64_t checkpointNum = newStateCheckpoint / checkpointWindowSize;
  stateTransfer->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char *)&digestOfNewState);
  CheckpointMsg *checkpointMsg = new CheckpointMsg(config_.replicaId, newStateCheckpoint, digestOfNewState, false);
  CheckpointInfo &checkpointInfo = checkpointsLog->get(newStateCheckpoint);
  checkpointInfo.addCheckpointMsg(checkpointMsg, config_.replicaId);
  checkpointInfo.setCheckpointSentAllOrApproved();

  if (newStateCheckpoint > primaryLastUsedSeqNum) primaryLastUsedSeqNum = newStateCheckpoint;

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
    LOG_INFO_F(GL, "call to startCollectingState()");

    stateTransfer->startCollectingState();
  }
}

void ReplicaImp::onSeqNumIsStable(SeqNum newStableSeqNum, bool hasStateInformation, bool oldSeqNum) {
  Assert(hasStateInformation || oldSeqNum);  // !hasStateInformation ==> oldSeqNum
  Assert(newStableSeqNum % checkpointWindowSize == 0);

  if (newStableSeqNum <= lastStableSeqNum) return;

  LOG_DEBUG_F(GL, "onSeqNumIsStable: lastStableSeqNum is now == %" PRId64 "", newStableSeqNum);

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
    if (ps_) ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
  }

  mainLog->advanceActiveWindow(lastStableSeqNum + 1);

  checkpointsLog->advanceActiveWindow(lastStableSeqNum);

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
    Assert(checkpointInfo.isCheckpointCertificateComplete());

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

  LOG_INFO_F(GL, "Node %d tries to request missing data for seqNumber %" PRId64 "", (int)config_.replicaId, seqNumber);

  ReqMissingDataMsg reqData(config_.replicaId, curView, seqNumber);

  const bool routerForPartialProofs = repsInfo->isCollectorForPartialProofs(curView, seqNumber);

  const bool routerForPartialPrepare = (currentPrimary() == config_.replicaId);

  const bool routerForPartialCommit = (currentPrimary() == config_.replicaId);

  const bool missingPrePrepare = (seqNumInfo.getPrePrepareMsg() == nullptr);
  const bool missingBigRequests = (!missingPrePrepare) && (!seqNumInfo.hasPrePrepareMsg());

  ReplicaId firstRepId = 0;
  ReplicaId lastRepId = numOfReplicas - 1;
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

    LOG_INFO_F(GL,
               "Node %d sends ReqMissingDataMsg to %d - seqNumber %" PRId64 " , flags=%X",
               config_.replicaId,
               destRep,
               seqNumber,
               (unsigned int)reqData.getFlags());

    send(&reqData, destRep);
  }
}

template <>
void ReplicaImp::onMessage<ReqMissingDataMsg>(ReqMissingDataMsg *msg) {
  metric_received_req_missing_datas_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  LOG_INFO_F(GL,
             "Node %d received ReqMissingDataMsg message from Node %d - seqNumber %" PRId64 " , flags=%X",
             (int)config_.replicaId,
             (int)msgSender,
             msgSeqNum,
             (unsigned int)msg->getFlags());

  if ((currentViewIsActive()) && (msgSeqNum > strictLowerBoundOfSeqNums) && (mainLog->insideActiveWindow(msgSeqNum)) &&
      (mainLog->insideActiveWindow(msgSeqNum))) {
    SeqNumInfo &seqNumInfo = mainLog->get(msgSeqNum);

    if (config_.replicaId == currentPrimary()) {
      PrePrepareMsg *pp = seqNumInfo.getSelfPrePrepareMsg();
      if (msg->getPrePrepareIsMissing()) {
        if (pp != 0) send(pp, msgSender);
      }

      if (seqNumInfo.slowPathStarted() && !msg->getSlowPathHasStarted()) {
        StartSlowCommitMsg startSlowMsg(config_.replicaId, curView, msgSeqNum);
        send(&startSlowMsg, msgSender);
      }
    }

    if (msg->getPartialProofIsMissing()) {
      // TODO(GG): consider not to send if msgSender is not a collector

      PartialCommitProofMsg *pcf = seqNumInfo.partialProofs().getSelfPartialCommitProof();

      if (pcf != nullptr) send(pcf, msgSender);
    }

    if (msg->getPartialPrepareIsMissing() && (currentPrimary() == msgSender)) {
      PreparePartialMsg *pr = seqNumInfo.getSelfPreparePartialMsg();

      if (pr != nullptr) send(pr, msgSender);
    }

    if (msg->getFullPrepareIsMissing()) {
      PrepareFullMsg *pf = seqNumInfo.getValidPrepareFullMsg();

      if (pf != nullptr) send(pf, msgSender);
    }

    if (msg->getPartialCommitIsMissing() && (currentPrimary() == msgSender)) {
      CommitPartialMsg *c = mainLog->get(msgSeqNum).getSelfCommitPartialMsg();
      if (c != nullptr) send(c, msgSender);
    }

    if (msg->getFullCommitIsMissing()) {
      CommitFullMsg *c = mainLog->get(msgSeqNum).getValidCommitFullMsg();
      if (c != nullptr) send(c, msgSender);
    }

    if (msg->getFullCommitProofIsMissing() && seqNumInfo.partialProofs().hasFullProof()) {
      FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();
      send(fcp, msgSender);
    }
  } else {
    LOG_INFO_F(
        GL, "Node %d ignores the ReqMissingDataMsg message from Node %d", (int)config_.replicaId, (int)msgSender);
  }

  delete msg;
}

void ReplicaImp::onViewsChangeTimer(Timers::Handle timer)  // TODO(GG): review/update logic
{
  Assert(viewChangeProtocolEnabled);

  if (isCollectingState()) return;

  const Time currTime = getMonotonicTime();

  //////////////////////////////////////////////////////////////////////////////
  //
  //////////////////////////////////////////////////////////////////////////////

  if (autoPrimaryRotationEnabled && currentViewIsActive()) {
    const uint64_t timeout = (isCurrentPrimary() ? (autoPrimaryRotationTimerMilli)
                                                 : (autoPrimaryRotationTimerMilli + viewChangeTimeoutMilli));

    const uint64_t diffMilli = duration_cast<milliseconds>(currTime - timeOfLastViewEntrance).count();

    if (diffMilli > timeout) {
      LOG_INFO_F(GL,
                 "**************** Node %d initiates automatic view change in view %" PRId64 " (%" PRIu64
                 " milli seconds after start working in the previous view)",
                 config_.replicaId,
                 curView,
                 diffMilli);

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
      LOG_INFO_F(GL,
                 "**************** Node %d asks to leave view %" PRId64 " (%" PRIu64
                 ""
                 " milli seconds after receiving a client request)",
                 config_.replicaId,
                 curView,
                 diffMilli3);

      GotoNextView();
      return;
    }
  } else  // not currentViewIsActive()
  {
    if (lastAgreedView != curView) return;
    if (repsInfo->primaryOfView(lastAgreedView) == config_.replicaId) return;

    const Time currTime = getMonotonicTime();
    const uint64_t diffMilli1 = duration_cast<milliseconds>(currTime - timeOfLastStateSynch).count();
    const uint64_t diffMilli2 = duration_cast<milliseconds>(currTime - timeOfLastAgreedView).count();

    if ((diffMilli1 > viewChangeTimeout) && (diffMilli2 > viewChangeTimeout)) {
      LOG_INFO_F(GL,
                 "**************** Node %d asks to jump to view %" PRId64 " (%" PRIu64
                 ""
                 " milliseconds after receiving 2f+2c+1 view change msgs)",
                 config_.replicaId,
                 curView,
                 diffMilli2);
      GotoNextView();
      return;
    }
  }
}

void ReplicaImp::onStatusReportTimer(Timers::Handle timer) {
  tryToSendStatusReport();

#ifdef DEBUG_MEMORY_MSG
  MessageBase::printLiveMessages();
#endif
}

void ReplicaImp::onSlowPathTimer(Timers::Handle timer) {
  tryToStartSlowPaths();
  auto newPeriod = milliseconds(controller->slowPathsTimerMilli());
  TimersSingleton::getInstance().reset(timer, newPeriod);
}

void ReplicaImp::onInfoRequestTimer(Timers::Handle timer) {
  tryToAskForMissingInfo();
  auto newPeriod = milliseconds(dynamicUpperLimitOfRounds->upperLimit() / 2);
  TimersSingleton::getInstance().reset(timer, newPeriod);
}

template <>
void ReplicaImp::onMessage<SimpleAckMsg>(SimpleAckMsg *msg) {
  metric_received_simple_acks_.Get().Inc();
  if (retransmissionsLogicEnabled) {
    uint16_t relatedMsgType = (uint16_t)msg->ackData();  // TODO(GG): does this make sense ?

    LOG_DEBUG_F(GL,
                "Node %d received SimpleAckMsg message from node %d for seqNumber %" PRId64 " and type %d",
                config_.replicaId,
                msg->senderId(),
                msg->seqNumber(),
                (int)relatedMsgType);

    retransmissionsManager->onAck(msg->senderId(), msg->seqNumber(), relatedMsgType);
  } else {
    // TODO(GG): print warning ?
  }

  delete msg;
}

void ReplicaImp::onMerkleExecSignature(ViewNum v, SeqNum s, uint16_t signatureLength, const char *signature) {
  Assert(false);
  // TODO(GG): use code from previous drafts
}

template <>
void ReplicaImp::onMessage<PartialExecProofMsg>(PartialExecProofMsg *m) {
  Assert(false);
  // TODO(GG): use code from previous drafts
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
                       shared_ptr<MsgHandlersRegistrator> msgHandlers)
    : ReplicaImp(false,
                 ld.repConfig,
                 requestsHandler,
                 stateTrans,
                 ld.sigManager,
                 ld.repsInfo,
                 ld.viewsManager,
                 msgsCommunicator,
                 msgHandlers) {
  Assert(persistentStorage != nullptr);

  ps_ = persistentStorage;

  curView = ld.viewsManager->latestActiveView();
  lastAgreedView = curView;
  metric_view_.Get().Set(curView);
  metric_last_agreed_view_.Get().Set(lastAgreedView);

  const bool inView = ld.viewsManager->viewIsActive(curView);

  primaryLastUsedSeqNum = ld.primaryLastUsedSeqNum;
  lastStableSeqNum = ld.lastStableSeqNum;
  metric_last_stable_seq_num_.Get().Set(lastStableSeqNum);
  lastExecutedSeqNum = ld.lastExecutedSeqNum;
  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
  strictLowerBoundOfSeqNums = ld.strictLowerBoundOfSeqNums;
  maxSeqNumTransferredFromPrevViews = ld.maxSeqNumTransferredFromPrevViews;
  lastViewThatTransferredSeqNumbersFullyExecuted = ld.lastViewThatTransferredSeqNumbersFullyExecuted;

  mainLog->resetAll(lastStableSeqNum + 1);
  checkpointsLog->resetAll(lastStableSeqNum);

  if (inView) {
    const bool isPrimaryOfView = (repsInfo->primaryOfView(curView) == config_.replicaId);

    SeqNum s = ld.lastStableSeqNum;

    for (size_t i = 0; i < kWorkWindowSize; i++) {
      s++;
      Assert(mainLog->insideActiveWindow(s));

      const SeqNumData &e = ld.seqNumWinArr[i];

      if (!e.isPrePrepareMsgSet()) continue;

      // such properties should be verified by the code the loads the persistent data
      Assert(e.getPrePrepareMsg()->seqNumber() == s);

      SeqNumInfo &seqNumInfo = mainLog->get(s);

      // add prePrepareMsg

      if (isPrimaryOfView)
        seqNumInfo.addSelfMsg(e.getPrePrepareMsg(), true);
      else
        seqNumInfo.addMsg(e.getPrePrepareMsg(), true);

      Assert(e.getPrePrepareMsg()->equals(*seqNumInfo.getPrePrepareMsg()));

      const CommitPath pathInPrePrepare = e.getPrePrepareMsg()->firstPath();

      Assert(pathInPrePrepare != CommitPath::SLOW ||
             e.getSlowStarted());  // TODO(GG): check this when we load the data from disk

      if (pathInPrePrepare != CommitPath::SLOW) {
        // add PartialCommitProofMsg

        PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
        Assert(e.getPrePrepareMsg()->equals(*pp));
        Digest &ppDigest = pp->digestOfRequests();
        const SeqNum seqNum = pp->seqNumber();

        IThresholdSigner *commitSigner = nullptr;

        Assert((config_.cVal != 0) || (pathInPrePrepare != CommitPath::FAST_WITH_THRESHOLD));

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
            tmpDigest);  // TODO(GG): consider using a method that directly adds the message/digest (as in the examples
                         // below)
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
        Assert(added);
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
        Assert(e.getCommitFullMsg()->equals(*seqNumInfo.getValidCommitFullMsg()));
      }

      if (e.isFullCommitProofMsgSet()) {
        PartialProofsSet &pps = seqNumInfo.partialProofs();
        bool added = pps.addMsg(e.getFullCommitProofMsg());  // TODO(GG): consider using a method that directly adds the
                                                             // message (as in the examples below)
        Assert(added);  // we should verify the relevant signature when it is loaded
        Assert(e.getFullCommitProofMsg()->equals(*pps.getFullProof()));
      }

      if (e.getForceCompleted()) seqNumInfo.forceComplete();
    }
  }

  Assert(ld.lastStableSeqNum % checkpointWindowSize == 0);

  for (SeqNum s = ld.lastStableSeqNum; s <= ld.lastStableSeqNum + kWorkWindowSize; s = s + checkpointWindowSize) {
    size_t i = (s - ld.lastStableSeqNum) / checkpointWindowSize;
    Assert(i < (sizeof(ld.checkWinArr) / sizeof(ld.checkWinArr[0])));
    const CheckData &e = ld.checkWinArr[i];

    Assert(checkpointsLog->insideActiveWindow(s));
    Assert(e.isCheckpointMsgSet() || (s > ld.lastStableSeqNum || s == 0));

    if (!e.isCheckpointMsgSet()) continue;

    CheckpointInfo &checkInfo = checkpointsLog->get(s);

    Assert(e.getCheckpointMsg()->seqNumber() == s);
    Assert(e.getCheckpointMsg()->senderId() == config_.replicaId);
    Assert((s != ld.lastStableSeqNum) || e.getCheckpointMsg()->isStableState());

    checkInfo.addCheckpointMsg(e.getCheckpointMsg(), config_.replicaId);
    Assert(checkInfo.selfCheckpointMsg()->equals(*e.getCheckpointMsg()));

    if (e.getCompletedMark()) checkInfo.tryToMarkCheckpointCertificateCompleted();
  }

  if (ld.isExecuting) {
    Assert(viewsManager->viewIsActive(curView));
    Assert(mainLog->insideActiveWindow(lastExecutedSeqNum + 1));
    const SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    Assert(pp != nullptr);
    Assert(pp->seqNumber() == lastExecutedSeqNum + 1);
    Assert(pp->viewNumber() == curView);
    Assert(pp->numberOfRequests() > 0);

    Bitmap b = ld.validRequestsThatAreBeingExecuted;
    size_t expectedValidRequests = 0;
    for (uint32_t i = 0; i < b.numOfBits(); i++) {
      if (b.get(i)) expectedValidRequests++;
    }
    Assert(expectedValidRequests <= pp->numberOfRequests());

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
                       shared_ptr<MsgHandlersRegistrator> msgHandlers)
    : ReplicaImp(true, config, requestsHandler, stateTrans, nullptr, nullptr, nullptr, msgsCommunicator, msgHandlers) {
  if (persistentStorage != nullptr) {
    ps_ = persistentStorage;

    Assert(!ps_->hasReplicaConfig());

    ps_->beginWriteTran();
    ps_->setReplicaConfig(config);
    ps_->endWriteTran();
  }

  internalThreadPool.start(8);  // TODO(GG): use configuration
}

ReplicaImp::ReplicaImp(bool firstTime,
                       const ReplicaConfig &config,
                       IRequestsHandler *requestsHandler,
                       IStateTransfer *stateTrans,
                       SigManager *sigMgr,
                       ReplicasInfo *replicasInfo,
                       ViewsManager *viewsMgr,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers)
    : ReplicaForStateTransfer(config, stateTrans, msgsCommunicator, msgHandlers, firstTime),
      viewChangeProtocolEnabled{config.viewChangeProtocolEnabled},
      autoPrimaryRotationEnabled{config.autoPrimaryRotationEnabled},
      restarted_{!firstTime},
      replyBuffer{(char *)std::malloc(config_.maxReplyMessageSize - sizeof(ClientReplyMsgHeader))},
      userRequestsHandler{requestsHandler},
      timeOfLastStateSynch{getMonotonicTime()},    // TODO(GG): TBD
      timeOfLastViewEntrance{getMonotonicTime()},  // TODO(GG): TBD
      timeOfLastAgreedView{getMonotonicTime()},    // TODO(GG): TBD
      metric_view_{metrics_.RegisterGauge("view", curView)},
      metric_last_stable_seq_num_{metrics_.RegisterGauge("lastStableSeqNum", lastStableSeqNum)},
      metric_last_executed_seq_num_{metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum)},
      metric_last_agreed_view_{metrics_.RegisterGauge("lastAgreedView", lastAgreedView)},
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
      metric_received_simple_acks_{metrics_.RegisterCounter("receivedSimpleAckMsgs")} {
  Assert(config_.replicaId < numOfReplicas);
  // TODO(GG): more asserts on params !!!!!!!!!!!

  // !firstTime ==> ((sigMgr != nullptr) && (replicasInfo != nullptr) && (viewsMgr != nullptr))
  Assert(firstTime || ((sigMgr != nullptr) && (replicasInfo != nullptr) && (viewsMgr != nullptr)));

  registerMsgHandlers();
  // Register metrics component with the default aggregator.
  metrics_.Register();

  if (firstTime) {
    sigManager = new SigManager(config_.replicaId,
                                numOfReplicas + config_.numOfClientProxies,
                                config_.replicaPrivateKey,
                                config_.publicKeysOfReplicas);
    repsInfo = new ReplicasInfo(config_.replicaId,
                                numOfReplicas,
                                config_.fVal,
                                config_.cVal,
                                dynamicCollectorForPartialProofs,
                                dynamicCollectorForExecutionProofs);
    viewsManager = new ViewsManager(repsInfo, sigManager, config_.thresholdVerifierForSlowPathCommit);
  } else {
    sigManager = sigMgr;
    repsInfo = replicasInfo;
    viewsManager = viewsMgr;

    // TODO(GG): consider to add relevant asserts
  }

  std::set<NodeIdType> clientsSet;
  for (uint16_t i = numOfReplicas; i < numOfReplicas + config_.numOfClientProxies; i++) clientsSet.insert(i);

  clientsManager =
      new ClientsManager(config_.replicaId, clientsSet, ReplicaConfigSingleton::GetInstance().GetSizeOfReservedPage());

  clientsManager->init(stateTransfer);

  if (!firstTime || config_.debugPersistentStorageEnabled)
    clientsManager->loadInfoFromReservedPages();
  else
    clientsManager->clearReservedPages();

  // autoPrimaryRotationEnabled implies viewChangeProtocolEnabled
  // Note: "p=>q" is equivalent to "not p or q"
  AssertOR(!autoPrimaryRotationEnabled, viewChangeProtocolEnabled);

  viewChangeTimerMilli = (viewChangeTimeoutMilli > 0) ? viewChangeTimeoutMilli : config.viewChangeTimerMillisec;
  Assert(viewChangeTimerMilli > 0);

  if (autoPrimaryRotationEnabled) {
    autoPrimaryRotationTimerMilli =
        (autoPrimaryRotationTimerMilli > 0) ? autoPrimaryRotationTimerMilli : config.autoPrimaryRotationTimerMillisec;
    Assert(autoPrimaryRotationTimerMilli > 0);
  }

  // TODO(GG): use config ...
  dynamicUpperLimitOfRounds = new DynamicUpperLimitWithSimpleFilter<int64_t>(400, 2, 2500, 70, 32, 1000, 2, 2);

  mainLog =
      new SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumInfo, SeqNumInfo>(1, (InternalReplicaApi *)this);

  checkpointsLog = new SequenceWithActiveWindow<kWorkWindowSize + checkpointWindowSize,
                                                checkpointWindowSize,
                                                SeqNum,
                                                CheckpointInfo,
                                                CheckpointInfo>(0, (InternalReplicaApi *)this);

  // create controller . TODO(GG): do we want to pass the controller as a parameter ?
  controller =
      new ControllerWithSimpleHistory(config_.cVal, config_.fVal, config_.replicaId, curView, primaryLastUsedSeqNum);

  if (retransmissionsLogicEnabled)
    retransmissionsManager =
        new RetransmissionsManager(this, &internalThreadPool, &getIncomingMsgsStorage(), kWorkWindowSize, 0);
  else
    retransmissionsManager = nullptr;

  LOG_INFO(GL,
           "numOfClientProxies=" << config_.numOfClientProxies
                                 << " maxExternalMessageSize=" << config_.maxExternalMessageSize
                                 << " maxReplyMessageSize=" << config_.maxReplyMessageSize
                                 << " maxNumOfReservedPages=" << config_.maxNumOfReservedPages << " sizeOfReservedPage="
                                 << config_.sizeOfReservedPage << " viewChangeTimerMilli=" << viewChangeTimerMilli);
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

  if (config_.debugStatisticsEnabled) {
    DebugStatistics::freeDebugStatisticsData();
  }
}

void ReplicaImp::stop() {
  if (retransmissionsLogicEnabled) TimersSingleton::getInstance().cancel(retranTimer_);
  TimersSingleton::getInstance().cancel(slowPathTimer_);
  TimersSingleton::getInstance().cancel(infoReqTimer_);
  TimersSingleton::getInstance().cancel(statusReportTimer_);
  if (viewChangeProtocolEnabled) TimersSingleton::getInstance().cancel(viewChangeTimer_);

  ReplicaForStateTransfer::stop();
}

void ReplicaImp::addTimers() {
  int statusReportTimerMilli = (sendStatusPeriodMilli > 0) ? sendStatusPeriodMilli : config_.statusReportTimerMillisec;
  Assert(statusReportTimerMilli > 0);
  statusReportTimer_ = TimersSingleton::getInstance().add(milliseconds(statusReportTimerMilli),
                                                          Timers::Timer::RECURRING,
                                                          [this](Timers::Handle h) { onStatusReportTimer(h); });
  if (viewChangeProtocolEnabled) {
    int t = viewChangeTimerMilli;
    if (autoPrimaryRotationEnabled && t > autoPrimaryRotationTimerMilli) t = autoPrimaryRotationTimerMilli;

    // TODO(GG): What should be the time period here?
    // TODO(GG): Consider to split to 2 different timers
    viewChangeTimer_ = TimersSingleton::getInstance().add(
        milliseconds(t / 2), Timers::Timer::RECURRING, [this](Timers::Handle h) { onViewsChangeTimer(h); });
  }
  if (retransmissionsLogicEnabled) {
    retranTimer_ = TimersSingleton::getInstance().add(milliseconds(retransmissionsTimerMilli),
                                                      Timers::Timer::RECURRING,
                                                      [this](Timers::Handle h) { onRetransmissionsTimer(h); });
  }
  const int slowPathsTimerPeriod = controller->timeToStartSlowPathMilli();

  slowPathTimer_ = TimersSingleton::getInstance().add(
      milliseconds(slowPathsTimerPeriod), Timers::Timer::RECURRING, [this](Timers::Handle h) { onSlowPathTimer(h); });

  infoReqTimer_ = TimersSingleton::getInstance().add(milliseconds(dynamicUpperLimitOfRounds->upperLimit() / 2),
                                                     Timers::Timer::RECURRING,
                                                     [this](Timers::Handle h) { onInfoRequestTimer(h); });
}

void ReplicaImp::start() {
  ReplicaForStateTransfer::start();
  addTimers();
  processMessages();
}

void ReplicaImp::processMessages() {
  LOG_INFO_F(GL, "Running");
  if (recoveringFromExecutionOfRequests) {
    const SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    Assert(pp != nullptr);
    executeRequestsInPrePrepareMsg(pp, true);
    recoveringFromExecutionOfRequests = false;
    mapOfRequestsThatAreBeingRecovered = Bitmap();
  }
}

void ReplicaImp::executeReadOnlyRequest(ClientRequestMsg *request) {
  Assert(request->isReadOnly());
  Assert(!isCollectingState());

  ClientReplyMsg reply(currentPrimary(), request->requestSeqNum(), config_.replicaId);

  uint16_t clientId = request->clientProxyId();

  int error = 0;
  uint32_t actualReplyLength = 0;

  if (!supportDirectProofs) {
    error = userRequestsHandler->execute(clientId,
                                         lastExecutedSeqNum,
                                         READ_ONLY_FLAG,
                                         request->requestLength(),
                                         request->requestBuf(),
                                         reply.maxReplyLength(),
                                         reply.replyBuf(),
                                         actualReplyLength);
  } else {
    // TODO(GG): use code from previous drafts
    Assert(false);
  }

  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)

  if (!error && actualReplyLength > 0) {
    reply.setReplyLength(actualReplyLength);
    send(&reply, clientId);
  }

  if (config_.debugStatisticsEnabled) {
    DebugStatistics::onRequestCompleted(true);
  }
}

void ReplicaImp::executeRequestsInPrePrepareMsg(PrePrepareMsg *ppMsg, bool recoverFromErrorInRequestsExecution) {
  Assert(!isCollectingState() && currentViewIsActive());
  Assert(ppMsg != nullptr);
  Assert(ppMsg->viewNumber() == curView);
  Assert(ppMsg->seqNumber() == lastExecutedSeqNum + 1);

  const uint16_t numOfRequests = ppMsg->numberOfRequests();

  Assert(!recoverFromErrorInRequestsExecution ||
         (numOfRequests > 0));  // recoverFromErrorInRequestsExecution ==> (numOfRequests > 0)

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
        NodeIdType clientId = req.clientProxyId();

        const bool validClient = isValidClient(clientId);
        if (!validClient) {
          LOG_WARN(GL, "The client clientId=%d is not valid" << clientId);
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
    while (reqIter.getAndGoToNext(requestBody)) {
      size_t tmp = reqIdx;
      reqIdx++;
      if (!requestSet.get(tmp)) continue;

      ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);
      NodeIdType clientId = req.clientProxyId();

      uint32_t actualReplyLength = 0;
      userRequestsHandler->execute(
          clientId,
          lastExecutedSeqNum + 1,
          req.isReadOnly() ? READ_ONLY_FLAG : EMPTY_FLAGS,
          req.requestLength(),
          req.requestBuf(),
          ReplicaConfigSingleton::GetInstance().GetMaxReplyMessageSize() - sizeof(ClientReplyMsgHeader),
          replyBuffer,
          actualReplyLength);

      Assert(actualReplyLength > 0);  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)

      ClientReplyMsg *replyMsg = clientsManager->allocateNewReplyMsgAndWriteToStorage(
          clientId, req.requestSeqNum(), currentPrimary(), replyBuffer, actualReplyLength);
      send(replyMsg, clientId);
      delete replyMsg;
      clientsManager->removePendingRequestOfClient(clientId);
    }
  }

  if ((lastExecutedSeqNum + 1) % checkpointWindowSize == 0) {
    const uint64_t checkpointNum = (lastExecutedSeqNum + 1) / checkpointWindowSize;
    stateTransfer->createCheckpointOfCurrentState(checkpointNum);
  }

  //////////////////////////////////////////////////////////////////////
  // Phase 3: finalize the execution of lastExecutedSeqNum+1
  // TODO(GG): Explain what happens in recovery mode
  //////////////////////////////////////////////////////////////////////

  LOG_DEBUG_F(
      GL, "\nReplica - executeRequestsInPrePrepareMsg() - lastExecutedSeqNum==%" PRId64 "", (lastExecutedSeqNum + 1));

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setLastExecutedSeqNum(lastExecutedSeqNum + 1);
  }

  lastExecutedSeqNum = lastExecutedSeqNum + 1;

  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
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
  }

  if (ps_) ps_->endWriteTran();

  if (numOfRequests > 0) userRequestsHandler->onFinishExecutingReadWriteRequests();

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

void ReplicaImp::executeNextCommittedRequests(const bool requestMissingInfo) {
  Assert(!isCollectingState() && currentViewIsActive());
  Assert(lastExecutedSeqNum >= lastStableSeqNum);

  LOG_DEBUG_F(GL, "Calling to executeNextCommittedRequests(requestMissingInfo=%d)", (int)requestMissingInfo);

  while (lastExecutedSeqNum < lastStableSeqNum + kWorkWindowSize) {
    SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);

    PrePrepareMsg *prePrepareMsg = seqNumInfo.getPrePrepareMsg();

    const bool ready = (prePrepareMsg != nullptr) && (seqNumInfo.isCommitted__gg());

    if (requestMissingInfo && !ready) {
      LOG_DEBUG_F(GL,
                  "executeNextCommittedRequests - Asking for missing information about %" PRId64 "",
                  lastExecutedSeqNum + 1);

      tryToSendReqMissingDataMsg(lastExecutedSeqNum + 1);
    }

    if (!ready) break;

    Assert(prePrepareMsg->seqNumber() == lastExecutedSeqNum + 1);
    Assert(prePrepareMsg->viewNumber() == curView);  // TODO(GG): TBD

    executeRequestsInPrePrepareMsg(prePrepareMsg);
  }

  if (isCurrentPrimary() && requestsQueueOfPrimary.size() > 0) tryToSendPrePrepareMsg(true);
}

IncomingMsgsStorage &ReplicaImp::getIncomingMsgsStorage() { return *msgsCommunicator_->getIncomingMsgsStorage(); }

// TODO(GG): the timer for state transfer !!!!

// TODO(GG): !!!! view changes and retransmissionsLogic --- check ....

}  // namespace bftEngine::impl
