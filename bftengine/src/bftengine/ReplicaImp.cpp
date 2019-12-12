// Concord
//
// Copyright (c) 2018, 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ReplicaImp.hpp"
#include "assertUtils.hpp"
#include "Logger.hpp"
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
#include "messages/StateTransferMsg.hpp"
#include "messages/StopInternalMsg.hpp"
#include "messages/StopWhenStateIsNotCollectedInternalMsg.hpp"
#include "messages/ReplicaStatusMsg.hpp"
#include "ControllerWithSimpleHistory.hpp"
#include "DebugStatistics.hpp"
#include "NullStateTransfer.hpp"
#include "SysConsts.hpp"
#include "ReplicaConfigSingleton.hpp"

using concordUtil::Timers;
using namespace std::chrono;

namespace bftEngine {
namespace impl {

std::unordered_map<uint16_t, PtrToMetaMsgHandler> ReplicaImp::createMapOfMetaMsgHandlers() {
  std::unordered_map<uint16_t, PtrToMetaMsgHandler> r;

  r[MsgCode::Checkpoint] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<CheckpointMsg>;
  r[MsgCode::CommitPartial] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<CommitPartialMsg>;
  r[MsgCode::CommitFull] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<CommitFullMsg>;
  r[MsgCode::FullCommitProof] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<FullCommitProofMsg>;
  r[MsgCode::NewView] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<NewViewMsg>;
  r[MsgCode::PrePrepare] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PrePrepareMsg>;
  r[MsgCode::PartialCommitProof] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PartialCommitProofMsg>;
  r[MsgCode::PartialExecProof] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PartialExecProofMsg>;
  r[MsgCode::PreparePartial] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PreparePartialMsg>;
  r[MsgCode::PrepareFull] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PrepareFullMsg>;
  r[MsgCode::ReqMissingData] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<ReqMissingDataMsg>;
  r[MsgCode::SimpleAckMsg] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<SimpleAckMsg>;
  r[MsgCode::StartSlowCommit] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<StartSlowCommitMsg>;
  r[MsgCode::ViewChange] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<ViewChangeMsg>;
  r[MsgCode::Request] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<ClientRequestMsg>;
  r[MsgCode::ReplicaStatus] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<ReplicaStatusMsg>;

  r[MsgCode::StateTransfer] = &ReplicaImp::metaMessageHandler<StateTransferMsg>;

  return r;
}

template <typename T>
void ReplicaImp::metaMessageHandler(MessageBase *msg) {
  T *validMsg = nullptr;
  bool isValid = T::ToActualMsgType(*replicasInfo_, msg, validMsg);

  if (isValid) {
    onMessage(validMsg);
  } else {
    onReportAboutInvalidMessage(msg);
    delete msg;
    return;
  }
}

template <typename T>
void ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState(MessageBase *msg) {
  if (stateTransfer_->isCollectingState()) {
    delete msg;
  } else {
    metaMessageHandler<T>(msg);
  }
}

void ReplicaImp::onReportAboutInvalidMessage(MessageBase *msg) {
  LOG_WARN_F(GL,
             "Node %d received invalid message from Node %d (type==%d)",
             (int)myReplicaId_,
             (int)msg->senderId(),
             (int)msg->type());

  // TODO(GG): logic that deals with invalid messages (e.g., a node that sends invalid messages may have a problem (old
  // version,bug,malicious,...)).
}

void ReplicaImp::send(MessageBase *m, NodeIdType dest) {
  // debug code begin

  if (m->type() == MsgCode::Checkpoint) {
    CheckpointMsg *c = (CheckpointMsg *)m;
    if (c->digestOfState().isZero()) LOG_WARN_F(GL, "Debug: checkpoint digest is zero");
  }

  // debug code end

  sendRaw(m->body(), dest, m->type(), m->size());
}

void ReplicaImp::sendToAllOtherReplicas(MessageBase *m) {
  for (ReplicaId dest : replicasInfo_->idsOfPeerReplicas()) sendRaw(m->body(), dest, m->type(), m->size());
}

void ReplicaImp::sendRaw(char *m, NodeIdType dest, uint16_t type, MsgSize size) {
  int errorCode = 0;

  if (dest == ALL_OTHER_REPLICAS) {
    for (ReplicaId d : replicasInfo_->idsOfPeerReplicas()) sendRaw(m, d, type, size);
    return;
  }

  if (debugStatisticsEnabled_) {
    DebugStatistics::onSendExMessage(type);
  }

  errorCode = msgsCommunicator_->sendAsyncMessage(dest, m, size);

  if (errorCode != 0) {
    LOG_ERROR_F(GL,
                "In ReplicaImp::sendRaw - communication->sendAsyncMessage returned error %d for message type %d",
                errorCode,
                (int)type);
  }
}

IncomingMsg ReplicaImp::recvMsg() {
  while (true) {
    auto msg = msgsCommunicator_->popInternalOrExternalMsg(timersResolution);

    // TODO(GG): make sure that we don't check timers too often
    // (i.e. much faster than timersResolution)
    timers_.evaluate();

    if (msg.tag != IncomingMsg::INVALID) {
      return msg;
    }
  }
}

void ReplicaImp::onMessage(ClientRequestMsg *m) {
  metric_received_client_requests_.Get().Inc();
  const NodeIdType senderId = m->senderId();
  const NodeIdType clientId = m->clientProxyId();
  const bool readOnly = m->isReadOnly();
  const ReqId reqSeqNum = m->requestSeqNum();

  LOG_INFO_F(GL,
             "Node %d received ClientRequestMsg (clientId=%d reqSeqNum=%" PRIu64 ", readOnly=%d) from Node %d",
             myReplicaId_,
             clientId,
             reqSeqNum,
             readOnly ? 1 : 0,
             senderId);

  if (stateTransfer_->isCollectingState()) {
    LOG_INFO_F(GL,
               "ClientRequestMsg is ignored because this replica is collecting missing state from the other replicas");
    delete m;
    return;
  }

  // check message validity
  const bool invalidClient = !clientsManager_->isValidClient(clientId);
  const bool sentFromReplicaToNonPrimary = replicasInfo_->isIdOfReplica(senderId) && !isCurrentPrimary();

  if (invalidClient || sentFromReplicaToNonPrimary)  // TODO(GG): more conditions (make sure that a request of client A
                                                     // cannot be generated by client B!=A)
  {
    LOG_INFO_F(GL, "ClientRequestMsg is invalid");
    onReportAboutInvalidMessage(m);
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

  const ReqId seqNumberOfLastReply = clientsManager_->seqNumberOfLastReplyToClient(clientId);

  if (seqNumberOfLastReply < reqSeqNum) {
    if (isCurrentPrimary()) {
      if (clientsManager_->noPendingAndRequestCanBecomePending(clientId, reqSeqNum) &&
          (requestsQueueOfPrimary_.size() < 700))  // TODO(GG): use config/parameter
      {
        requestsQueueOfPrimary_.push(m);
        tryToSendPrePrepareMsg(true);
        return;
      } else {
        LOG_INFO_F(GL,
                   "ClientRequestMsg is ignored because: request is old, OR primary is current working on a request "
                   "from the same client, OR queue contains too many requests");
      }
    } else  // not the current primary
    {
      if (clientsManager_->noPendingAndRequestCanBecomePending(clientId, reqSeqNum)) {
        clientsManager_->addPendingRequest(clientId, reqSeqNum);

        send(m,
             currentPrimary());  // TODO(GG): add a mechanism that retransmits (otherwise we may start unnecessary
                                 // view-change )

        LOG_INFO_F(GL, "Sending ClientRequestMsg to current primary");
      } else {
        LOG_INFO_F(GL,
                   "ClientRequestMsg is ignored because request is old or replica has another pending request from the "
                   "same client");
      }
    }
  } else if (seqNumberOfLastReply == reqSeqNum) {
    LOG_DEBUG_F(GL, "ClientRequestMsg has already been executed - retransmit reply to client");

    ClientReplyMsg *repMsg = clientsManager_->allocateMsgWithLatestReply(clientId, currentPrimary());

    send(repMsg, clientId);

    delete repMsg;
  } else {
    LOG_INFO_F(GL, "ClientRequestMsg is ignored because request is old");
  }

  delete m;
}

void ReplicaImp::tryToSendPrePrepareMsg(bool batchingLogic) {
  Assert(isCurrentPrimary() && currentViewIsActive());

  if (primaryLastUsedSeqNum_ + 1 > lastStableSeqNum_ + kWorkWindowSize) return;

  if (primaryLastUsedSeqNum_ + 1 > lastExecutedSeqNum_ + maxConcurrentAgreementsByPrimary_)
    return;  // TODO(GG): should also be checked by the non-primary replicas

  if (requestsQueueOfPrimary_.empty()) return;

  // remove irrelevant requests from the head of the requestsQueueOfPrimary (and update requestsInQueue)
  ClientRequestMsg *first = requestsQueueOfPrimary_.front();
  while (first != nullptr &&
         !clientsManager_->noPendingAndRequestCanBecomePending(first->clientProxyId(), first->requestSeqNum())) {
    delete first;
    requestsQueueOfPrimary_.pop();
    first = (!requestsQueueOfPrimary_.empty() ? requestsQueueOfPrimary_.front() : nullptr);
  }

  const size_t requestsInQueue = requestsQueueOfPrimary_.size();

  if (requestsInQueue == 0) return;

  Assert(primaryLastUsedSeqNum_ >= lastExecutedSeqNum_);

  uint64_t concurrentDiff = ((primaryLastUsedSeqNum_ + 1) - lastExecutedSeqNum_);
  uint64_t minBatchSize = 1;

  // update maxNumberOfPendingRequestsInRecentHistory (if needed)
  if (requestsInQueue > maxNumberOfPendingRequestsInRecentHistory_)
    maxNumberOfPendingRequestsInRecentHistory_ = requestsInQueue;

  // TODO(GG): the batching logic should be part of the configuration - TBD.
  if (batchingLogic && (concurrentDiff >= 2)) {
    minBatchSize = concurrentDiff * batchingFactor_;

    const size_t maxReasonableMinBatchSize = 350;  // TODO(GG): use param from configuration

    if (minBatchSize > maxReasonableMinBatchSize) minBatchSize = maxReasonableMinBatchSize;
  }

  if (requestsInQueue < minBatchSize) return;

  // update batchingFactor
  if (((primaryLastUsedSeqNum_ + 1) % kWorkWindowSize) ==
      0)  // TODO(GG): do we want to update batchingFactor when the view is changed
  {
    const size_t aa = 4;  // TODO(GG): read from configuration
    batchingFactor_ = (maxNumberOfPendingRequestsInRecentHistory_ / aa);
    if (batchingFactor_ < 1) batchingFactor_ = 1;
    maxNumberOfPendingRequestsInRecentHistory_ = 0;
  }

  // because maxConcurrentAgreementsByPrimary <  MaxConcurrentFastPaths
  Assert((primaryLastUsedSeqNum_ + 1) <= lastExecutedSeqNum_ + MaxConcurrentFastPaths);

  CommitPath firstPath = controller_->getCurrentFirstPath();

  // assert: (cVal==0) --> (firstPath != CommitPath::FAST_WITH_THRESHOLD)
  Assert((cVal_ != 0) || (firstPath != CommitPath::FAST_WITH_THRESHOLD));

  controller_->onSendingPrePrepare((primaryLastUsedSeqNum_ + 1), firstPath);

  PrePrepareMsg *pp = new PrePrepareMsg(myReplicaId_, currentView_, (primaryLastUsedSeqNum_ + 1), firstPath, false);

  ClientRequestMsg *nextRequest = requestsQueueOfPrimary_.front();
  while (nextRequest != nullptr && nextRequest->size() <= pp->remainingSizeForRequests()) {
    if (clientsManager_->noPendingAndRequestCanBecomePending(nextRequest->clientProxyId(),
                                                             nextRequest->requestSeqNum())) {
      pp->addRequest(nextRequest->body(), nextRequest->size());
      clientsManager_->addPendingRequest(nextRequest->clientProxyId(), nextRequest->requestSeqNum());
    }
    delete nextRequest;
    requestsQueueOfPrimary_.pop();
    nextRequest = (requestsQueueOfPrimary_.size() > 0 ? requestsQueueOfPrimary_.front() : nullptr);
  }

  pp->finishAddingRequests();

  Assert(pp->numberOfRequests() > 0);

  if (debugStatisticsEnabled_) {
    DebugStatistics::onSendPrePrepareMessage(pp->numberOfRequests(), requestsQueueOfPrimary_.size());
  }
  primaryLastUsedSeqNum_++;

  SeqNumInfo &seqNumInfo = mainLog_->get(primaryLastUsedSeqNum_);
  seqNumInfo.addSelfMsg(pp);

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum_);
    ps_->setPrePrepareMsgInSeqNumWindow(primaryLastUsedSeqNum_, pp);
    if (firstPath == CommitPath::SLOW) ps_->setSlowStartedInSeqNumWindow(primaryLastUsedSeqNum_, true);
    ps_->endWriteTran();
  }

  LOG_DEBUG(GL,
            "Node " << myReplicaId_ << " Sending PrePrepareMsg (seqNumber=" << pp->seqNumber() << ", requests="
                    << (int)pp->numberOfRequests() << ", queue size=" << (int)requestsQueueOfPrimary_.size() << ")");

  for (ReplicaId x : replicasInfo_->idsOfPeerReplicas()) {
    sendRetransmittableMsgToReplica(pp, x, primaryLastUsedSeqNum_);
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

  if (currentViewIsActive() && (msgViewNum == currentView_) && (msgSeqNum > strictLowerBoundOfSeqNums_) &&
      (mainLog_->insideActiveWindow(msgSeqNum))) {
    Assert(msgSeqNum > lastStableSeqNum_);
    Assert(msgSeqNum <= lastStableSeqNum_ + kWorkWindowSize);

    return true;
  } else {
    const bool myReplicaMayBeBehind =
        (currentView_ < msgViewNum) || (msgSeqNum > mainLog_->currentActiveWindow().second);
    if (myReplicaMayBeBehind) {
      onReportAboutAdvancedReplica(msg->senderId(), msgSeqNum, msgViewNum);
    } else {
      const bool msgReplicaMayBeBehind =
          (currentView_ > msgViewNum) || (msgSeqNum + kWorkWindowSize < mainLog_->currentActiveWindow().first);

      if (msgReplicaMayBeBehind) onReportAboutLateReplica(msg->senderId(), msgSeqNum, msgViewNum);
    }

    return false;
  }
}

void ReplicaImp::onMessage(PrePrepareMsg *msg) {
  metric_received_pre_prepares_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();

  LOG_DEBUG_F(GL,
              "Node %d received PrePrepareMsg from node %d for seqNumber %" PRId64 " (size=%d)",
              (int)myReplicaId_,
              (int)msg->senderId(),
              msgSeqNum,
              (int)msg->size());

  if (!currentViewIsActive() && viewsManager_->waitingForMsgs() && msgSeqNum > lastStableSeqNum_) {
    Assert(!msg->isNull());  // we should never send (and never accept) null PrePrepare message

    if (viewsManager_->addPotentiallyMissingPP(msg, lastStableSeqNum_)) {
      LOG_DEBUG_F(
          GL, "Node %d adds PrePrepareMsg for seqNumber %" PRId64 " to viewsManager", (int)myReplicaId_, msgSeqNum);
      tryToEnterView();
    } else {
      LOG_DEBUG_F(GL,
                  "Node %d does not add PrePrepareMsg for seqNumber %" PRId64 " to viewsManager",
                  (int)myReplicaId_,
                  msgSeqNum);
    }

    return;  // TODO(GG): memory deallocation is confusing .....
  }

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg) && (msg->senderId() == currentPrimary())) {
    sendAckIfNeeded(msg, msg->senderId(), msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);

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
  if (!isCurrentPrimary() || stateTransfer_->isCollectingState() || !currentViewIsActive())
    return;  // TODO(GG): consider to stop the related timer when this method is not needed (to avoid useless
             // invocations)

  const SeqNum minSeqNum = lastExecutedSeqNum_ + 1;

  if (minSeqNum > lastStableSeqNum_ + kWorkWindowSize) {
    LOG_INFO_F(GL, "Replica::tryToStartSlowPaths() : minSeqNum > lastStableSeqNum + kWorkWindowSize");
    return;
  }

  const SeqNum maxSeqNum = primaryLastUsedSeqNum_;

  Assert(maxSeqNum <= lastStableSeqNum_ + kWorkWindowSize);
  Assert(minSeqNum <= maxSeqNum + 1);

  if (minSeqNum > maxSeqNum) return;

  sendCheckpointIfNeeded();  // TODO(GG): TBD - do we want it here ?

  const Time currTime = getMonotonicTime();

  for (SeqNum i = minSeqNum; i <= maxSeqNum; i++) {
    SeqNumInfo &seqNumInfo = mainLog_->get(i);

    if (seqNumInfo.partialProofs().hasFullProof() ||                          // already has a full proof
        seqNumInfo.slowPathStarted() ||                                       // slow path has already  started
        seqNumInfo.partialProofs().getSelfPartialCommitProof() == nullptr ||  // did not start a fast path
        (!seqNumInfo.hasPrePrepareMsg()))
      continue;  // slow path is not needed

    const Time timeOfPartProof = seqNumInfo.partialProofs().getTimeOfSelfPartialProof();

    if (currTime - timeOfPartProof < milliseconds(controller_->timeToStartSlowPathMilli())) break;

    LOG_INFO_F(GL,
               "Primary initiates slow path for seqNum=%" PRId64 " (currTime=%" PRIu64 " timeOfPartProof=%" PRIu64,
               i,
               duration_cast<microseconds>(currTime.time_since_epoch()).count(),
               duration_cast<microseconds>(timeOfPartProof.time_since_epoch()).count());

    controller_->onStartingSlowCommit(i);

    seqNumInfo.startSlowPath();
    metric_slow_path_count_.Get().Inc();

    if (ps_) {
      ps_->beginWriteTran();
      ps_->setSlowStartedInSeqNumWindow(i, true);
      ps_->endWriteTran();
    }

    // send StartSlowCommitMsg to all replicas

    StartSlowCommitMsg *startSlow = new StartSlowCommitMsg(myReplicaId_, currentView_, i);

    for (ReplicaId x : replicasInfo_->idsOfPeerReplicas()) {
      sendRetransmittableMsgToReplica(startSlow, x, i);
    }

    delete startSlow;

    sendPreparePartial(seqNumInfo);
  }
}

void ReplicaImp::tryToAskForMissingInfo() {
  if (!currentViewIsActive() || stateTransfer_->isCollectingState()) return;

  Assert(maxSeqNumTransferredFromPrevViews_ <= lastStableSeqNum_ + kWorkWindowSize);

  const bool recentViewChange = (maxSeqNumTransferredFromPrevViews_ > lastStableSeqNum_);

  SeqNum minSeqNum = 0;
  SeqNum maxSeqNum = 0;

  if (!recentViewChange) {
    const int16_t searchWindow = 4;  // TODO(GG): TBD - read from configuration
    minSeqNum = lastExecutedSeqNum_ + 1;
    maxSeqNum = std::min(minSeqNum + searchWindow - 1, lastStableSeqNum_ + kWorkWindowSize);
  } else {
    const int16_t searchWindow = 32;  // TODO(GG): TBD - read from configuration
    minSeqNum = lastStableSeqNum_ + 1;
    while (minSeqNum <= lastStableSeqNum_ + kWorkWindowSize) {
      SeqNumInfo &seqNumInfo = mainLog_->get(minSeqNum);
      if (!seqNumInfo.isCommitted__gg()) break;
      minSeqNum++;
    }
    maxSeqNum = std::min(minSeqNum + searchWindow - 1, lastStableSeqNum_ + kWorkWindowSize);
  }

  if (minSeqNum > lastStableSeqNum_ + kWorkWindowSize) return;

  const Time curTime = getMonotonicTime();

  SeqNum lastRelatedSeqNum = 0;

  // TODO(GG): improve/optimize the following loops

  for (SeqNum i = minSeqNum; i <= maxSeqNum; i++) {
    Assert(mainLog_->insideActiveWindow(i));

    const SeqNumInfo &seqNumInfo = mainLog_->get(i);

    Time t = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();

    const Time lastInfoRequest = seqNumInfo.getTimeOfLastInfoRequest();

    if ((t < lastInfoRequest)) t = lastInfoRequest;

    if (t != MinTime && (t < curTime)) {
      auto diffMilli = duration_cast<milliseconds>(curTime - t);
      if (diffMilli.count() >= dynamicUpperLimitOfRounds_->upperLimit()) lastRelatedSeqNum = i;
    }
  }

  for (SeqNum i = minSeqNum; i <= lastRelatedSeqNum; i++) {
    if (!recentViewChange)
      tryToSendReqMissingDataMsg(i);
    else
      tryToSendReqMissingDataMsg(i, true, currentPrimary());
  }
}

void ReplicaImp::onMessage(StartSlowCommitMsg *msg) {
  metric_received_start_slow_commits_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();

  LOG_INFO_F(GL, "Node %d received StartSlowCommitMsg for seqNumber %" PRId64 "", myReplicaId_, msgSeqNum);

  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, currentPrimary(), msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);

    if (!seqNumInfo.slowPathStarted() && !seqNumInfo.isPrepared()) {
      LOG_INFO_F(GL, "Node %d starts slow path for seqNumber %" PRId64 "", myReplicaId_, msgSeqNum);

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

      Assert((cVal_ != 0) || (commitPath != CommitPath::FAST_WITH_THRESHOLD));

      if ((commitPath == CommitPath::FAST_WITH_THRESHOLD) && (cVal_ > 0))
        commitSigner = thresholdSignerForCommit_;
      else
        commitSigner = thresholdSignerForOptimisticCommit_;

      Digest tmpDigest;
      Digest::calcCombination(ppDigest, currentView_, seqNum, tmpDigest);

      part = new PartialCommitProofMsg(myReplicaId_, currentView_, seqNum, commitPath, tmpDigest, commitSigner);
      partialProofs.addSelfMsgAndPPDigest(part, tmpDigest);
    }

    partialProofs.setTimeOfSelfPartialProof(getMonotonicTime());

    // send PartialCommitProofMsg (only if, from my point of view, at most MaxConcurrentFastPaths are in progress)
    if (seqNum <= lastExecutedSeqNum_ + MaxConcurrentFastPaths) {
      // TODO(GG): improve the following code (use iterators instead of a simple array)
      int8_t numOfRouters = 0;
      ReplicaId routersArray[2];

      replicasInfo_->getCollectorsForPartialProofs(currentView_, seqNum, &numOfRouters, routersArray);

      for (int i = 0; i < numOfRouters; i++) {
        ReplicaId router = routersArray[i];
        if (router != myReplicaId_) {
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
        currentView_, pp->seqNumber(), myReplicaId_, pp->digestOfRequests(), thresholdSignerForSlowPathCommit_);
    seqNumInfo.addSelfMsg(p);

    if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(p, currentPrimary(), pp->seqNumber());
  }
}

void ReplicaImp::sendCommitPartial(const SeqNum s) {
  Assert(currentViewIsActive());
  Assert(mainLog_->insideActiveWindow(s));

  SeqNumInfo &seqNumInfo = mainLog_->get(s);
  PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();

  Assert(seqNumInfo.isPrepared());
  Assert(pp != nullptr);
  Assert(pp->seqNumber() == s);

  if (seqNumInfo.committedOrHasCommitPartialFromReplica(myReplicaId_)) return;  // not needed

  LOG_DEBUG_F(GL, "Sending CommitPartialMsg for seqNumber %" PRId64 "", s);

  Digest d;
  Digest::digestOfDigest(pp->digestOfRequests(), d);

  CommitPartialMsg *c = CommitPartialMsg::create(currentView_, s, myReplicaId_, d, thresholdSignerForSlowPathCommit_);
  seqNumInfo.addSelfCommitPartialMsgAndDigest(c, d);

  if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(c, currentPrimary(), s);
}

void ReplicaImp::onMessage(PartialCommitProofMsg *msg) {
  metric_received_partial_commit_proofs_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const SeqNum msgView = msg->viewNumber();
  const NodeIdType msgSender = msg->senderId();

  Assert(replicasInfo_->isIdOfPeerReplica(msgSender));
  Assert(replicasInfo_->isCollectorForPartialProofs(msgView, msgSeqNum));

  LOG_DEBUG_F(GL,
              "Node %d received PartialCommitProofMsg (size=%d) from node %d for seqNumber %" PRId64 "",
              (int)myReplicaId_,
              (int)msg->size(),
              (int)msgSender,
              msgSeqNum);

  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    if (msgSeqNum > lastExecutedSeqNum_) {
      SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);
      PartialProofsSet &pps = seqNumInfo.partialProofs();

      if (pps.addMsg(msg)) {
        return;
      }
    }
  }

  delete msg;
  return;
}

void ReplicaImp::onInternalMsg(FullCommitProofMsg *msg) {
  if ((stateTransfer_->isCollectingState()) || (!currentViewIsActive()) || (currentView_ != msg->viewNumber()) ||
      (!mainLog_->insideActiveWindow(msg->seqNumber()))) {
    delete msg;
    return;
  }

  onMessage(msg);
}

void ReplicaImp::onMessage(FullCommitProofMsg *msg) {
  metric_received_full_commit_proofs_.Get().Inc();
  LOG_DEBUG_F(
      GL, "Node %d received FullCommitProofMsg message for seqNumber %d", (int)myReplicaId_, (int)msg->seqNumber());

  const SeqNum msgSeqNum = msg->seqNumber();

  if (relevantMsgForActiveView(msg)) {
    SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);
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

      if (msg->senderId() == myReplicaId_) sendToAllOtherReplicas(msg);

      const bool askForMissingInfoAboutCommittedItems =
          (msgSeqNum > lastExecutedSeqNum_ + maxConcurrentAgreementsByPrimary_);  // TODO(GG): check/improve this logic
      executeNextCommittedRequests(askForMissingInfoAboutCommittedItems);

      return;
    }
  }

  delete msg;
  return;
}

void ReplicaImp::onMessage(PreparePartialMsg *msg) {
  metric_received_prepare_partials_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg)) {
    Assert(isCurrentPrimary());

    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG_F(GL,
                "Node %d received PreparePartialMsg from node %d for seqNumber %" PRId64 "",
                (int)myReplicaId_,
                (int)msgSender,
                msgSeqNum);

    controller_->onMessage(msg);

    SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);

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
                (int)myReplicaId_,
                (int)msgSender,
                msgSeqNum);
    delete msg;
  }
}

void ReplicaImp::onMessage(CommitPartialMsg *msg) {
  metric_received_commit_partials_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg)) {
    Assert(isCurrentPrimary());

    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG_F(GL,
                "Node %d received CommitPartialMsg from node %d for seqNumber %" PRId64 "",
                (int)myReplicaId_,
                (int)msgSender,
                msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);

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
                (int)myReplicaId_,
                (int)msgSender,
                msgSeqNum);
    delete msg;
  }
}

void ReplicaImp::onMessage(PrepareFullMsg *msg) {
  metric_received_prepare_fulls_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG_F(GL, "Node %d received PrepareFullMsg for seqNumber %" PRId64 "", (int)myReplicaId_, msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);

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
                (int)myReplicaId_,
                (int)msgSender,
                msgSeqNum);
    delete msg;
  }
}

void ReplicaImp::onMessage(CommitFullMsg *msg) {
  metric_received_commit_fulls_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  bool msgAdded = false;

  if (relevantMsgForActiveView(msg)) {
    sendAckIfNeeded(msg, msgSender, msgSeqNum);

    LOG_DEBUG_F(GL, "Node %d received CommitFullMsg for seqNumber %" PRId64 "", (int)myReplicaId_, msgSeqNum);

    SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);

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
                (int)myReplicaId_,
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
              (int)myReplicaId_,
              seqNumber,
              v);

  if ((stateTransfer_->isCollectingState()) || (!currentViewIsActive()) || (currentView_ != v) ||
      (!mainLog_->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onPrepareCombinedSigFailed - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)myReplicaId_,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog_->get(seqNumber);

  seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, v, replicasWithBadSigs);

  // TODO(GG): add logic that handles bad replicas ...
}

void ReplicaImp::onPrepareCombinedSigSucceeded(SeqNum seqNumber,
                                               ViewNum v,
                                               const char *combinedSig,
                                               uint16_t combinedSigLen) {
  LOG_DEBUG_F(GL,
              "Node %d - onPrepareCombinedSigSucceeded seqNumber=%" PRId64 " view=%" PRId64 "",
              (int)myReplicaId_,
              seqNumber,
              v);

  if ((stateTransfer_->isCollectingState()) || (!currentViewIsActive()) || (currentView_ != v) ||
      (!mainLog_->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onPrepareCombinedSigSucceeded - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)myReplicaId_,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog_->get(seqNumber);

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

  for (ReplicaId x : replicasInfo_->idsOfPeerReplicas()) sendRetransmittableMsgToReplica(preFull, x, seqNumber);

  Assert(seqNumInfo.isPrepared());

  sendCommitPartial(seqNumber);
}

void ReplicaImp::onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum v, bool isValid) {
  LOG_DEBUG_F(GL,
              "Node %d - onPrepareVerifyCombinedSigResult seqNumber=%" PRId64 " view=%" PRId64 "",
              (int)myReplicaId_,
              seqNumber,
              v);

  if ((stateTransfer_->isCollectingState()) || (!currentViewIsActive()) || (currentView_ != v) ||
      (!mainLog_->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onPrepareVerifyCombinedSigResult - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)myReplicaId_,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog_->get(seqNumber);

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
              (int)myReplicaId_,
              seqNumber,
              v);

  if ((stateTransfer_->isCollectingState()) || (!currentViewIsActive()) || (currentView_ != v) ||
      (!mainLog_->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onCommitCombinedSigFailed - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)myReplicaId_,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog_->get(seqNumber);

  seqNumInfo.onCompletionOfCommitSignaturesProcessing(seqNumber, v, replicasWithBadSigs);

  // TODO(GG): add logic that handles bad replicas ...
}

void ReplicaImp::onCommitCombinedSigSucceeded(SeqNum seqNumber,
                                              ViewNum v,
                                              const char *combinedSig,
                                              uint16_t combinedSigLen) {
  LOG_DEBUG_F(GL,
              "Node %d - onCommitCombinedSigSucceeded seqNumber=%" PRId64 " view=%" PRId64 "",
              (int)myReplicaId_,
              seqNumber,
              v);

  if ((stateTransfer_->isCollectingState()) || (!currentViewIsActive()) || (currentView_ != v) ||
      (!mainLog_->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onCommitCombinedSigSucceeded - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)myReplicaId_,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog_->get(seqNumber);

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

  for (ReplicaId x : replicasInfo_->idsOfPeerReplicas()) sendRetransmittableMsgToReplica(commitFull, x, seqNumber);

  Assert(seqNumInfo.isCommitted__gg());

  bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum_ + maxConcurrentAgreementsByPrimary_);

  executeNextCommittedRequests(askForMissingInfoAboutCommittedItems);
}

void ReplicaImp::onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum v, bool isValid) {
  LOG_DEBUG_F(GL,
              "Node %d - onCommitVerifyCombinedSigResult seqNumber=%" PRId64 " view=%" PRId64 "",
              myReplicaId_,
              seqNumber,
              v);

  if ((stateTransfer_->isCollectingState()) || (!currentViewIsActive()) || (currentView_ != v) ||
      (!mainLog_->insideActiveWindow(seqNumber))) {
    LOG_DEBUG_F(GL,
                "Node %d - onCommitVerifyCombinedSigResult - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
                (int)myReplicaId_,
                seqNumber,
                v);

    return;
  }

  SeqNumInfo &seqNumInfo = mainLog_->get(seqNumber);

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

  bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum_ + maxConcurrentAgreementsByPrimary_);
  executeNextCommittedRequests(askForMissingInfoAboutCommittedItems);
}

void ReplicaImp::onMessage(CheckpointMsg *msg) {
  metric_received_checkpoints_.Get().Inc();
  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgSeqNum = msg->seqNumber();
  const Digest msgDigest = msg->digestOfState();
  const bool msgIsStable = msg->isStableState();

  LOG_DEBUG_F(GL,
              "Node %d received Checkpoint message from node %d for seqNumber %" PRId64
              " (size=%d, stable=%s, digestPrefix=%d)",
              (int)myReplicaId_,
              (int)msgSenderId,
              msgSeqNum,
              (int)msg->size(),
              (int)msgIsStable ? "true" : "false",
              *((int *)(&msgDigest)));

  if ((msgSeqNum > lastStableSeqNum_) && (msgSeqNum <= lastStableSeqNum_ + kWorkWindowSize)) {
    Assert(mainLog_->insideActiveWindow(msgSeqNum));
    CheckpointInfo &checkInfo = checkpointsLog_->get(msgSeqNum);
    bool msgAdded = checkInfo.addCheckpointMsg(msg, msg->senderId());

    if (msgAdded)
      LOG_DEBUG_F(GL,
                  "Node %d added Checkpoint message from node %d for seqNumber %" PRId64 "",
                  (int)myReplicaId_,
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

  if (msgIsStable && msgSeqNum > lastExecutedSeqNum_) {
    auto pos = tableOfStableCheckpoints_.find(msgSenderId);
    if (pos == tableOfStableCheckpoints_.end() || pos->second->seqNumber() < msgSeqNum) {
      if (pos != tableOfStableCheckpoints_.end()) delete pos->second;
      CheckpointMsg *x = new CheckpointMsg(msgSenderId, msgSeqNum, msgDigest, msgIsStable);
      tableOfStableCheckpoints_[msgSenderId] = x;

      LOG_INFO_F(GL,
                 "Node %d added stable Checkpoint message to tableOfStableCheckpoints (message from node %d for "
                 "seqNumber %" PRId64 ")",
                 (int)myReplicaId_,
                 (int)msgSenderId,
                 msgSeqNum);

      if ((uint16_t)tableOfStableCheckpoints_.size() >= fVal_ + 1) {
        uint16_t numRelevant = 0;
        uint16_t numRelevantAboveWindow = 0;
        auto tableItrator = tableOfStableCheckpoints_.begin();
        while (tableItrator != tableOfStableCheckpoints_.end()) {
          if (tableItrator->second->seqNumber() <= lastExecutedSeqNum_) {
            delete tableItrator->second;
            tableItrator = tableOfStableCheckpoints_.erase(tableItrator);
          } else {
            numRelevant++;
            if (tableItrator->second->seqNumber() > lastStableSeqNum_ + kWorkWindowSize) numRelevantAboveWindow++;
            tableItrator++;
          }
        }
        Assert(numRelevant == tableOfStableCheckpoints_.size());

        LOG_DEBUG_F(GL, "numRelevant=%d    numRelevantAboveWindow=%d", (int)numRelevant, (int)numRelevantAboveWindow);

        if (numRelevantAboveWindow >= fVal_ + 1) {
          askForStateTransfer = true;
        } else if (numRelevant >= fVal_ + 1) {
          Time timeOfLastCommit = MinTime;
          if (mainLog_->insideActiveWindow(lastExecutedSeqNum_))
            timeOfLastCommit = mainLog_->get(lastExecutedSeqNum_).lastUpdateTimeOfCommitMsgs();

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

    stateTransfer_->startCollectingState();
  } else if (msgSeqNum > lastStableSeqNum_ + kWorkWindowSize) {
    onReportAboutAdvancedReplica(msgSenderId, msgSeqNum);
  } else if (msgSeqNum + kWorkWindowSize < lastStableSeqNum_) {
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
        replicasInfo_->getCollectorsForPartialProofs(destReplica, currentView_, seqNum, nullptr, nullptr);
    if (destIsCollector) return true;
  }

  return false;
}

void ReplicaImp::sendAckIfNeeded(MessageBase *msg, const NodeIdType sourceNode, const SeqNum seqNum) {
  if (!retransmissionsLogicEnabled) return;

  if (!replicasInfo_->isIdOfPeerReplica(sourceNode)) return;

  if (handledByRetransmissionsManager(sourceNode, myReplicaId_, currentPrimary(), seqNum, msg->type())) {
    SimpleAckMsg *ackMsg = new SimpleAckMsg(seqNum, currentView_, myReplicaId_, msg->type());

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

  if (handledByRetransmissionsManager(myReplicaId_, destReplica, currentPrimary(), s, msg->type()))
    retransmissionsManager_->onSend(destReplica, s, msg->type(), ignorePreviousAcks);
}

void ReplicaImp::onRetransmissionsTimer(Timers::Handle timer) {
  Assert(retransmissionsLogicEnabled);

  retransmissionsManager_->tryToStartProcessing();
}

void ReplicaImp::onRetransmissionsProcessingResults(
    SeqNum relatedLastStableSeqNum,
    const ViewNum relatedViewNumber,
    const std::forward_list<RetSuggestion> *const suggestedRetransmissions) {
  Assert(retransmissionsLogicEnabled);

  if (stateTransfer_->isCollectingState() || (relatedViewNumber != currentView_) || (!currentViewIsActive())) return;
  if (relatedLastStableSeqNum + kWorkWindowSize <= lastStableSeqNum_) return;

  const uint16_t myId = myReplicaId_;
  const uint16_t primaryId = currentPrimary();

  for (const RetSuggestion &s : *suggestedRetransmissions) {
    if ((s.msgSeqNum <= lastStableSeqNum_) || (s.msgSeqNum > lastStableSeqNum_ + kWorkWindowSize)) continue;

    Assert(s.replicaId != myId);

    Assert(handledByRetransmissionsManager(myId, s.replicaId, primaryId, s.msgSeqNum, s.msgType));

    switch (s.msgType) {
      case MsgCode::PrePrepare: {
        SeqNumInfo &seqNumInfo = mainLog_->get(s.msgSeqNum);
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
        SeqNumInfo &seqNumInfo = mainLog_->get(s.msgSeqNum);
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
        StartSlowCommitMsg *msgToSend = new StartSlowCommitMsg(myId, currentView_, s.msgSeqNum);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        delete msgToSend;
        LOG_DEBUG_F(GL,
                    "Replica %d retransmits to replica %d StartSlowCommitMsg with seqNumber %" PRId64 "",
                    (int)myId,
                    (int)s.replicaId,
                    s.msgSeqNum);
      } break;
      case MsgCode::PreparePartial: {
        SeqNumInfo &seqNumInfo = mainLog_->get(s.msgSeqNum);
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
        SeqNumInfo &seqNumInfo = mainLog_->get(s.msgSeqNum);
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
        SeqNumInfo &seqNumInfo = mainLog_->get(s.msgSeqNum);
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
        SeqNumInfo &seqNumInfo = mainLog_->get(s.msgSeqNum);
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

void ReplicaImp::onMessage(ReplicaStatusMsg *msg) {
  metric_received_replica_statuses_.Get().Inc();
  // TODO(GG): we need filter for msgs (to avoid denial of service attack) + avoid sending messages at a high rate.
  // TODO(GG): for some communication modules/protocols, we can also utilize information about connection/disconnection.

  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgLastStable = msg->getLastStableSeqNum();
  const ViewNum msgViewNum = msg->getViewNumber();
  Assert(msgLastStable % checkpointWindowSize == 0);

  LOG_INFO_F(GL, "Node %d received ReplicaStatusMsg from node %d", (int)myReplicaId_, (int)msgSenderId);

  /////////////////////////////////////////////////////////////////////////
  // Checkpoints
  /////////////////////////////////////////////////////////////////////////

  if (lastStableSeqNum_ > msgLastStable + kWorkWindowSize) {
    CheckpointMsg *checkMsg = checkpointsLog_->get(lastStableSeqNum_).selfCheckpointMsg();

    if (checkMsg == nullptr || !checkMsg->isStableState()) {
      // TODO(GG): warning
    } else {
      send(checkMsg, msgSenderId);
    }

    delete msg;
    return;
  } else if (msgLastStable > lastStableSeqNum_ + kWorkWindowSize) {
    tryToSendStatusReport();  // ask for help
  } else {
    // Send checkpoints that may be useful for msgSenderId
    const SeqNum beginRange =
        std::max(checkpointsLog_->currentActiveWindow().first, msgLastStable + checkpointWindowSize);
    const SeqNum endRange = std::min(checkpointsLog_->currentActiveWindow().second, msgLastStable + kWorkWindowSize);

    Assert(beginRange % checkpointWindowSize == 0);

    if (beginRange <= endRange) {
      Assert(endRange - beginRange <= kWorkWindowSize);

      for (SeqNum i = beginRange; i <= endRange; i = i + checkpointWindowSize) {
        CheckpointMsg *checkMsg = checkpointsLog_->get(i).selfCheckpointMsg();
        if (checkMsg != nullptr) send(checkMsg, msgSenderId);
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId in older view
  /////////////////////////////////////////////////////////////////////////

  if (msgViewNum < currentView_) {
    ViewChangeMsg *myVC = viewsManager_->getMyLatestViewChangeMsg();
    Assert(myVC != nullptr);  // because curView>0
    send(myVC, msgSenderId);
  }

  /////////////////////////////////////////////////////////////////////////
  // msgSenderId needes information to enter view curView
  /////////////////////////////////////////////////////////////////////////

  else if ((msgViewNum == currentView_) && (!msg->currentViewIsActive())) {
    if (isCurrentPrimary() ||
        (replicasInfo_->primaryOfView(currentView_) == msgSenderId))  // if the primary is involved
    {
      if (!isCurrentPrimary())  // I am not the primary of curView
      {
        // send ViewChangeMsg
        if (msg->hasListOfMissingViewChangeMsgForViewChange() &&
            msg->isMissingViewChangeMsgForViewChange(myReplicaId_)) {
          ViewChangeMsg *myVC = viewsManager_->getMyLatestViewChangeMsg();
          Assert(myVC != nullptr);
          send(myVC, msgSenderId);
        }
      } else  // I am the primary of curView
      {
        // send NewViewMsg
        if (!msg->currentViewHasNewViewMessage() && viewsManager_->viewIsActive(currentView_)) {
          NewViewMsg *nv = viewsManager_->getMyNewViewMsgForCurrentView();
          Assert(nv != nullptr);
          send(nv, msgSenderId);
        }

        // send ViewChangeMsg
        if (msg->hasListOfMissingViewChangeMsgForViewChange() &&
            msg->isMissingViewChangeMsgForViewChange(myReplicaId_)) {
          ViewChangeMsg *myVC = viewsManager_->getMyLatestViewChangeMsg();
          Assert(myVC != nullptr);
          send(myVC, msgSenderId);
        }
        // TODO(GG): send all VC msgs that can help making progress (needed because the original senders may not send
        // the ViewChangeMsg msgs used by the primary)

        // TODO(GG): if viewsManager->viewIsActive(curView), we can send only the VC msgs which are really needed for
        // curView (see in ViewsManager)
      }

      if (viewsManager_->viewIsActive(currentView_)) {
        if (msg->hasListOfMissingPrePrepareMsgForViewChange()) {
          for (SeqNum i = msgLastStable + 1; i <= msgLastStable + kWorkWindowSize; i++) {
            if (mainLog_->insideActiveWindow(i) && msg->isMissingPrePrepareMsgForViewChange(i)) {
              PrePrepareMsg *prePrepareMsg = mainLog_->get(i).getPrePrepareMsg();
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
                  viewsManager_->getPrePrepare(i);  // TODO(GG): we can avoid sending misleading message by using the
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

  else if ((msgViewNum == currentView_) && msg->currentViewIsActive()) {
    if (isCurrentPrimary()) {
      if (viewsManager_->viewIsActive(currentView_)) {
        SeqNum beginRange =
            std::max(lastStableSeqNum_ + 1,
                     msg->getLastExecutedSeqNum() + 1);  // Notice that after a view change, we don't have to pass the
                                                         // PrePrepare messages from the previous view. TODO(GG): verify
        SeqNum endRange = std::min(lastStableSeqNum_ + kWorkWindowSize, msgLastStable + kWorkWindowSize);

        for (SeqNum i = beginRange; i <= endRange; i++) {
          if (msg->isPrePrepareInActiveWindow(i)) continue;

          PrePrepareMsg *prePrepareMsg = mainLog_->get(i).getSelfPrePrepareMsg();
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
    Assert(msgViewNum > currentView_);
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
      duration_cast<milliseconds>(currentTime - lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas_);

  if (milliSinceLastTime < milliseconds(minTimeBetweenStatusRequestsMilli)) return;

  const int64_t dynamicMinTimeBetweenStatusRequestsMilli =
      (int64_t)((double)dynamicUpperLimitOfRounds_->upperLimit() * factorForMinTimeBetweenStatusRequestsMilli);

  if (milliSinceLastTime < milliseconds(dynamicMinTimeBetweenStatusRequestsMilli)) return;

  // TODO(GG): handle restart/pause !! (restart/pause may affect time measurements...)
  lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas_ = currentTime;

  const bool viewIsActive = currentViewIsActive();
  const bool hasNewChangeMsg = viewsManager_->hasNewViewMessage(currentView_);
  const bool listOfPPInActiveWindow = viewIsActive;
  const bool listOfMissingVCMsg = !viewIsActive && !viewsManager_->viewIsPending(currentView_);
  const bool listOfMissingPPMsg = !viewIsActive && viewsManager_->viewIsPending(currentView_);

  ReplicaStatusMsg msg(myReplicaId_,
                       currentView_,
                       lastStableSeqNum_,
                       lastExecutedSeqNum_,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPPInActiveWindow,
                       listOfMissingVCMsg,
                       listOfMissingPPMsg);

  if (listOfPPInActiveWindow) {
    const SeqNum start = lastStableSeqNum_ + 1;
    const SeqNum end = lastStableSeqNum_ + kWorkWindowSize;

    for (SeqNum i = start; i <= end; i++) {
      if (mainLog_->get(i).hasPrePrepareMsg()) msg.setPrePrepareInActiveWindow(i);
    }
  }
  if (listOfMissingVCMsg) {
    for (ReplicaId i : replicasInfo_->idsOfPeerReplicas()) {
      if (!viewsManager_->hasViewChangeMessageForFutureView(i)) msg.setMissingViewChangeMsgForViewChange(i);
    }
  } else if (listOfMissingPPMsg) {
    std::vector<SeqNum> missPP;
    if (viewsManager_->getNumbersOfMissingPP(lastStableSeqNum_, &missPP)) {
      for (SeqNum i : missPP) {
        Assert((i > lastStableSeqNum_) && (i <= lastStableSeqNum_ + kWorkWindowSize));
        msg.setMissingPrePrepareMsgForViewChange(i);
      }
    }
  }

  sendToAllOtherReplicas(&msg);
}

void ReplicaImp::onMessage(ViewChangeMsg *msg) {
  if (!viewChangeProtocolEnabled_) {
    delete msg;
    return;
  }
  metric_received_view_changes_.Get().Inc();

  const ReplicaId generatedReplicaId =
      msg->idOfGeneratedReplica();  // Notice that generatedReplicaId may be != msg->senderId()
  Assert(generatedReplicaId != myReplicaId_);

  LOG_INFO_F(GL,
             "Node %d received ViewChangeMsg (generatedReplicaId=%d, newView=%" PRId64 ", lastStable=%" PRId64
             ", numberOfElements=%d).",
             (int)myReplicaId_,
             (int)generatedReplicaId,
             msg->newView(),
             msg->lastStable(),
             (int)msg->numberOfElements());

  bool msgAdded = viewsManager_->add(msg);

  LOG_INFO_F(GL, "ViewChangeMsg add=%d", (int)msgAdded);

  if (!msgAdded) return;

  // if the current primary wants to leave view
  if (generatedReplicaId == currentPrimary() && msg->newView() > currentView_) {
    LOG_INFO_F(
        GL, "Primary asks to leave view (primary Id=%d , view=%" PRId64 ")", (int)generatedReplicaId, currentView_);
    MoveToHigherView(currentView_ + 1);
  }

  ViewNum maxKnownCorrectView = 0;
  ViewNum maxKnownAgreedView = 0;
  viewsManager_->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
  LOG_INFO_F(
      GL, "maxKnownCorrectView=%" PRId64 ", maxKnownAgreedView=%" PRId64 "", maxKnownCorrectView, maxKnownAgreedView);

  if (maxKnownCorrectView > currentView_) {
    // we have at least f+1 view-changes with view number >= maxKnownCorrectView
    MoveToHigherView(maxKnownCorrectView);

    // update maxKnownCorrectView and maxKnownAgreedView			// TODO(GG): consider to optimize (this
    // part is not always needed)
    viewsManager_->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
    LOG_INFO_F(
        GL, "maxKnownCorrectView=%" PRId64 ", maxKnownAgreedView=%" PRId64 "", maxKnownCorrectView, maxKnownAgreedView);
  }

  if (viewsManager_->viewIsActive(currentView_)) return;  // return, if we are still in the previous view

  if (maxKnownAgreedView != currentView_) return;  // return, if we can't move to the new view yet

  // Replica now has at least 2f+2c+1 ViewChangeMsg messages with view  >= curView

  if (lastAgreedView_ < currentView_) {
    lastAgreedView_ = currentView_;
    metric_last_agreed_view_.Get().Set(lastAgreedView_);
    timeOfLastAgreedView_ = getMonotonicTime();
  }

  tryToEnterView();
}

void ReplicaImp::onMessage(NewViewMsg *msg) {
  if (!viewChangeProtocolEnabled_) {
    delete msg;
    return;
  }
  metric_received_new_views_.Get().Inc();

  const ReplicaId senderId = msg->senderId();

  Assert(senderId != myReplicaId_);  // should be verified in ViewChangeMsg

  LOG_INFO_F(GL,
             "Node %d received NewViewMsg message (senderId=%d, newView=%" PRId64 ")",
             (int)myReplicaId_,
             (int)senderId,
             msg->newView());

  bool added = viewsManager_->add(msg);

  LOG_INFO_F(GL, "NewViewMsg add=%d", (int)added);

  if (!added) return;

  if (viewsManager_->viewIsActive(currentView_)) return;  // return, if we are still in the previous view

  tryToEnterView();
}

void ReplicaImp::MoveToHigherView(ViewNum nextView) {
  Assert(viewChangeProtocolEnabled_);

  Assert(currentView_ < nextView);

  const bool wasInPrevViewNumber = viewsManager_->viewIsActive(currentView_);

  LOG_INFO_F(GL,
             "**************** In MoveToHigherView (curView=%" PRId64 ", nextView=%" PRId64 ", wasInPrevViewNumber=%d)",
             currentView_,
             nextView,
             (int)wasInPrevViewNumber);

  ViewChangeMsg *pVC = nullptr;

  if (!wasInPrevViewNumber) {
    pVC = viewsManager_->getMyLatestViewChangeMsg();
    Assert(pVC != nullptr);
    pVC->setNewViewNumber(nextView);
  } else {
    std::vector<ViewsManager::PrevViewInfo> prevViewInfo;
    for (SeqNum i = lastStableSeqNum_ + 1; i <= lastStableSeqNum_ + kWorkWindowSize; i++) {
      SeqNumInfo &seqNumInfo = mainLog_->get(i);

      if (seqNumInfo.getPrePrepareMsg() != nullptr) {
        ViewsManager::PrevViewInfo x;

        seqNumInfo.getAndReset(x.prePrepare, x.prepareFull);
        x.hasAllRequests = true;

        Assert(x.prePrepare != nullptr);
        Assert(x.prePrepare->viewNumber() == currentView_);
        Assert(x.prepareFull == nullptr || x.hasAllRequests);  // (x.prepareFull!=nullptr) ==> (x.hasAllRequests==true)
        Assert(x.prepareFull == nullptr ||
               x.prepareFull->viewNumber() ==
                   currentView_);  // (x.prepareFull!=nullptr) ==> (x.prepareFull->viewNumber() == curView)

        prevViewInfo.push_back(x);
      } else {
        seqNumInfo.resetAndFree();
      }
    }

    if (ps_) {
      ViewChangeMsg *myVC = (currentView_ == 0 ? nullptr : viewsManager_->getMyLatestViewChangeMsg());
      SeqNum stableLowerBoundWhenEnteredToView = viewsManager_->stableLowerBoundWhenEnteredToView();
      const DescriptorOfLastExitFromView desc{
          currentView_, lastStableSeqNum_, lastExecutedSeqNum_, prevViewInfo, myVC, stableLowerBoundWhenEnteredToView};
      ps_->beginWriteTran();
      ps_->setDescriptorOfLastExitFromView(desc);
      ps_->clearSeqNumWindow();
      ps_->endWriteTran();
    }

    pVC = viewsManager_->exitFromCurrentView(lastStableSeqNum_, lastExecutedSeqNum_, prevViewInfo);

    Assert(pVC != nullptr);
    pVC->setNewViewNumber(nextView);
  }

  currentView_ = nextView;
  metric_view_.Get().Set(nextView);

  LOG_INFO_F(GL,
             "Sending view change message: new view=%" PRId64
             ", wasInPrevViewNumber=%d, new primary=%d, lastExecutedSeqNum=%" PRId64 ", lastStableSeqNum=%" PRId64 "",
             currentView_,
             (int)wasInPrevViewNumber,
             (int)currentPrimary(),
             lastExecutedSeqNum_,
             lastStableSeqNum_);

  pVC->finalizeMessage(*replicasInfo_);

  sendToAllOtherReplicas(pVC);
}

void ReplicaImp::GotoNextView() {
  // at this point we don't have f+1 ViewChangeMsg messages with view >= curView

  MoveToHigherView(currentView_ + 1);

  // at this point we don't have enough ViewChangeMsg messages (2f+2c+1) to enter to the new view (because 2f+2c+1 >
  // f+1)
}

bool ReplicaImp::tryToEnterView() {
  Assert(!currentViewIsActive());

  std::vector<PrePrepareMsg *> prePreparesForNewView;

  LOG_INFO_F(GL,
             "**************** Calling to viewsManager->tryToEnterView(curView=%" PRId64 ", lastStableSeqNum=%" PRId64
             ", lastExecutedSeqNum=%" PRId64 ")",
             currentView_,
             lastStableSeqNum_,
             lastExecutedSeqNum_);

  bool succ =
      viewsManager_->tryToEnterView(currentView_, lastStableSeqNum_, lastExecutedSeqNum_, &prePreparesForNewView);

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
             currentView_,
             prePreparesForNewView.size(),
             firstPPSeq,
             lastPPSeq,
             lastStableSeqNum_,
             lastExecutedSeqNum_,
             viewsManager_->stableLowerBoundWhenEnteredToView());

  Assert(viewsManager_->viewIsActive(currentView_));
  Assert(lastStableSeqNum_ >= viewsManager_->stableLowerBoundWhenEnteredToView());
  Assert(lastExecutedSeqNum_ >= lastStableSeqNum_);  // we moved to the new state, only after synchronizing the state

  timeOfLastViewEntrance_ = getMonotonicTime();  // TODO(GG): handle restart/pause

  NewViewMsg *newNewViewMsgToSend = nullptr;

  if (replicasInfo_->primaryOfView(currentView_) == myReplicaId_) {
    NewViewMsg *nv = viewsManager_->getMyNewViewMsgForCurrentView();

    nv->finalizeMessage(*replicasInfo_);

    Assert(nv->newView() == currentView_);

    newNewViewMsgToSend = nv;
  }

  if (prePreparesForNewView.empty()) {
    primaryLastUsedSeqNum_ = lastStableSeqNum_;
    strictLowerBoundOfSeqNums_ = lastStableSeqNum_;
    maxSeqNumTransferredFromPrevViews_ = lastStableSeqNum_;
  } else {
    primaryLastUsedSeqNum_ = lastPPSeq;
    strictLowerBoundOfSeqNums_ = firstPPSeq - 1;
    maxSeqNumTransferredFromPrevViews_ = lastPPSeq;
  }

  if (ps_) {
    vector<ViewChangeMsg *> viewChangeMsgsForCurrentView = viewsManager_->getViewChangeMsgsForCurrentView();
    NewViewMsg *newViewMsgForCurrentView = viewsManager_->getNewViewMsgForCurrentView();

    bool myVCWasUsed = false;
    for (size_t i = 0; i < viewChangeMsgsForCurrentView.size() && !myVCWasUsed; i++) {
      Assert(viewChangeMsgsForCurrentView[i] != nullptr);
      if (viewChangeMsgsForCurrentView[i]->idOfGeneratedReplica() == myReplicaId_) myVCWasUsed = true;
    }

    ViewChangeMsg *myVC = nullptr;
    if (!myVCWasUsed) {
      myVC = viewsManager_->getMyLatestViewChangeMsg();
    } else {
      // debug/test: check that my VC should be included
      ViewChangeMsg *tempMyVC = viewsManager_->getMyLatestViewChangeMsg();
      Assert(tempMyVC != nullptr);
      Digest d;
      tempMyVC->getMsgDigest(d);
      Assert(newViewMsgForCurrentView->includesViewChangeFromReplica(myReplicaId_, d));
    }

    DescriptorOfLastNewView viewDesc{currentView_,
                                     newViewMsgForCurrentView,
                                     viewChangeMsgsForCurrentView,
                                     myVC,
                                     viewsManager_->stableLowerBoundWhenEnteredToView(),
                                     maxSeqNumTransferredFromPrevViews_};

    ps_->beginWriteTran();
    ps_->setDescriptorOfLastNewView(viewDesc);
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum_);
    ps_->setStrictLowerBoundOfSeqNums(strictLowerBoundOfSeqNums_);
  }

  const bool primaryIsMe = (myReplicaId_ == replicasInfo_->primaryOfView(currentView_));

  for (size_t i = 0; i < prePreparesForNewView.size(); i++) {
    PrePrepareMsg *pp = prePreparesForNewView[i];
    Assert(pp->seqNumber() >= firstPPSeq);
    Assert(pp->seqNumber() <= lastPPSeq);
    Assert(pp->firstPath() == CommitPath::SLOW);  // TODO(GG): don't we want to use the fast path?
    SeqNumInfo &seqNumInfo = mainLog_->get(pp->seqNumber());

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

  clientsManager_->clearAllPendingRequests();

  // clear requestsQueueOfPrimary
  while (!requestsQueueOfPrimary_.empty()) {
    delete requestsQueueOfPrimary_.front();
    requestsQueueOfPrimary_.pop();
  }

  // send messages

  if (newNewViewMsgToSend != nullptr) sendToAllOtherReplicas(newNewViewMsgToSend);

  for (size_t i = 0; i < prePreparesForNewView.size(); i++) {
    PrePrepareMsg *pp = prePreparesForNewView[i];
    SeqNumInfo &seqNumInfo = mainLog_->get(pp->seqNumber());
    sendPreparePartial(seqNumInfo);
  }

  LOG_INFO_F(GL, "**************** Start working in view %" PRId64 "", currentView_);

  controller_->onNewView(currentView_, primaryLastUsedSeqNum_);
}

void ReplicaImp::sendCheckpointIfNeeded() {
  if (stateTransfer_->isCollectingState() || !currentViewIsActive()) return;

  const SeqNum lastCheckpointNumber = (lastExecutedSeqNum_ / checkpointWindowSize) * checkpointWindowSize;

  if (lastCheckpointNumber == 0) return;

  Assert(checkpointsLog_->insideActiveWindow(lastCheckpointNumber));

  CheckpointInfo &checkInfo = checkpointsLog_->get(lastCheckpointNumber);
  CheckpointMsg *checkpointMessage = checkInfo.selfCheckpointMsg();

  if (!checkpointMessage) {
    LOG_INFO_F(GL, "My Checkpoint message is missing");  // TODO(GG): TBD
    return;
  }

  // LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 1");

  if (checkInfo.checkpointSentAllOrApproved()) return;

  // LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 2");

  // TODO(GG): 3 seconds, should be in configuration
  if ((getMonotonicTime() - checkInfo.selfExecutionTime()) >= 3s) {
    checkInfo.setCheckpointSentAllOrApproved();
    sendToAllOtherReplicas(checkpointMessage);
    return;
  }

  // LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 3");

  const SeqNum refSeqNumberForCheckpoint = lastCheckpointNumber + MaxConcurrentFastPaths;

  if (lastExecutedSeqNum_ < refSeqNumberForCheckpoint) return;

  // LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 4");

  if (mainLog_->insideActiveWindow(lastExecutedSeqNum_))  // TODO(GG): condition is needed ?
  {
    SeqNumInfo &seqNumInfo = mainLog_->get(lastExecutedSeqNum_);

    // LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 5");

    if (seqNumInfo.partialProofs().hasFullProof()) {
      // LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 6");

      checkInfo.tryToMarkCheckpointCertificateCompleted();

      Assert(checkInfo.isCheckpointCertificateComplete());

      onSeqNumIsStable(lastCheckpointNumber);

      checkInfo.setCheckpointSentAllOrApproved();

      return;
    }
  }

  // LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 7");

  checkInfo.setCheckpointSentAllOrApproved();
  sendToAllOtherReplicas(checkpointMessage);
}

void ReplicaImp::onTransferringCompleteImp(SeqNum newStateCheckpoint) {
  Assert(newStateCheckpoint % checkpointWindowSize == 0);

  LOG_INFO_F(GL, "onTransferringCompleteImp with newStateCheckpoint==%" PRId64 "", newStateCheckpoint);

  if (mainThreadShouldStopWhenStateIsNotCollected_) {
    mainThreadShouldStopWhenStateIsNotCollected_ = false;
    mainThreadShouldStop_ = true;  // main thread will be stopped
  }

  if (ps_) {
    ps_->beginWriteTran();
  }

  if (newStateCheckpoint <= lastExecutedSeqNum_) {
    LOG_DEBUG_F(
        GL, "Executing onTransferringCompleteImp(newStateCheckpoint) where newStateCheckpoint <= lastExecutedSeqNum");
    if (ps_) ps_->endWriteTran();
    return;
  }
  lastExecutedSeqNum_ = newStateCheckpoint;
  if (ps_) ps_->setLastExecutedSeqNum(lastExecutedSeqNum_);
  if (debugStatisticsEnabled_) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum_);
  }
  bool askAnotherStateTransfer = false;

  timeOfLastStateSynch_ = getMonotonicTime();  // TODO(GG): handle restart/pause

  clientsManager_->loadInfoFromReservedPages();

  if (newStateCheckpoint > lastStableSeqNum_ + kWorkWindowSize) {
    const SeqNum refPoint = newStateCheckpoint - kWorkWindowSize;
    const bool withRefCheckpoint = (checkpointsLog_->insideActiveWindow(refPoint) &&
                                    (checkpointsLog_->get(refPoint).selfCheckpointMsg() != nullptr));

    onSeqNumIsStable(refPoint, withRefCheckpoint, true);
  }

  // newStateCheckpoint should be in the active window
  Assert(checkpointsLog_->insideActiveWindow(newStateCheckpoint));

  // create and send my checkpoint
  Digest digestOfNewState;
  const uint64_t checkpointNum = newStateCheckpoint / checkpointWindowSize;
  stateTransfer_->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char *)&digestOfNewState);
  CheckpointMsg *checkpointMsg = new CheckpointMsg(myReplicaId_, newStateCheckpoint, digestOfNewState, false);
  CheckpointInfo &checkpointInfo = checkpointsLog_->get(newStateCheckpoint);
  checkpointInfo.addCheckpointMsg(checkpointMsg, myReplicaId_);
  checkpointInfo.setCheckpointSentAllOrApproved();

  if (newStateCheckpoint > primaryLastUsedSeqNum_) primaryLastUsedSeqNum_ = newStateCheckpoint;

  if (ps_) {
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum_);
    ps_->setCheckpointMsgInCheckWindow(newStateCheckpoint, checkpointMsg);
    ps_->endWriteTran();
  }
  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum_);

  sendToAllOtherReplicas(checkpointMsg);

  if ((uint16_t)tableOfStableCheckpoints_.size() >= fVal_ + 1) {
    uint16_t numOfStableCheckpoints = 0;
    auto tableItrator = tableOfStableCheckpoints_.begin();
    while (tableItrator != tableOfStableCheckpoints_.end()) {
      if (tableItrator->second->seqNumber() >= newStateCheckpoint) numOfStableCheckpoints++;

      if (tableItrator->second->seqNumber() <= lastExecutedSeqNum_) {
        delete tableItrator->second;
        tableItrator = tableOfStableCheckpoints_.erase(tableItrator);
      } else {
        tableItrator++;
      }
    }
    if (numOfStableCheckpoints >= fVal_ + 1) onSeqNumIsStable(newStateCheckpoint);

    if ((uint16_t)tableOfStableCheckpoints_.size() >= fVal_ + 1) askAnotherStateTransfer = true;
  }

  if (askAnotherStateTransfer) {
    LOG_INFO_F(GL, "call to startCollectingState()");

    stateTransfer_->startCollectingState();
  }
}

void ReplicaImp::onSeqNumIsStable(SeqNum newStableSeqNum, bool hasStateInformation, bool oldSeqNum) {
  Assert(hasStateInformation || oldSeqNum);  // !hasStateInformation ==> oldSeqNum
  Assert(newStableSeqNum % checkpointWindowSize == 0);

  if (newStableSeqNum <= lastStableSeqNum_) return;

  LOG_DEBUG_F(GL, "onSeqNumIsStable: lastStableSeqNum is now == %" PRId64 "", newStableSeqNum);

  if (ps_) ps_->beginWriteTran();

  lastStableSeqNum_ = newStableSeqNum;
  metric_last_stable_seq_num_.Get().Set(lastStableSeqNum_);

  if (ps_) ps_->setLastStableSeqNum(lastStableSeqNum_);

  if (lastStableSeqNum_ > strictLowerBoundOfSeqNums_) {
    strictLowerBoundOfSeqNums_ = lastStableSeqNum_;
    if (ps_) ps_->setStrictLowerBoundOfSeqNums(strictLowerBoundOfSeqNums_);
  }

  if (lastStableSeqNum_ > primaryLastUsedSeqNum_) {
    primaryLastUsedSeqNum_ = lastStableSeqNum_;
    if (ps_) ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum_);
  }

  mainLog_->advanceActiveWindow(lastStableSeqNum_ + 1);

  checkpointsLog_->advanceActiveWindow(lastStableSeqNum_);

  if (hasStateInformation) {
    if (lastStableSeqNum_ > lastExecutedSeqNum_) {
      lastExecutedSeqNum_ = lastStableSeqNum_;
      if (ps_) ps_->setLastExecutedSeqNum(lastExecutedSeqNum_);
      metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum_);
      if (debugStatisticsEnabled_) {
        DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum_);
      }
      clientsManager_->loadInfoFromReservedPages();
    }

    CheckpointInfo &checkpointInfo = checkpointsLog_->get(lastStableSeqNum_);
    CheckpointMsg *checkpointMsg = checkpointInfo.selfCheckpointMsg();

    if (checkpointMsg == nullptr) {
      Digest digestOfState;
      const uint64_t checkpointNum = lastStableSeqNum_ / checkpointWindowSize;
      stateTransfer_->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char *)&digestOfState);
      checkpointMsg = new CheckpointMsg(myReplicaId_, lastStableSeqNum_, digestOfState, true);
      checkpointInfo.addCheckpointMsg(checkpointMsg, myReplicaId_);
    } else {
      checkpointMsg->setStateAsStable();
    }

    if (!checkpointInfo.isCheckpointCertificateComplete()) checkpointInfo.tryToMarkCheckpointCertificateCompleted();
    Assert(checkpointInfo.isCheckpointCertificateComplete());

    if (ps_) {
      ps_->setCheckpointMsgInCheckWindow(lastStableSeqNum_, checkpointMsg);
      ps_->setCompletedMarkInCheckWindow(lastStableSeqNum_, true);
    }
  }

  if (ps_) ps_->endWriteTran();

  if (!oldSeqNum && currentViewIsActive() && (currentPrimary() == myReplicaId_) &&
      !stateTransfer_->isCollectingState()) {
    tryToSendPrePrepareMsg();
  }
}

void ReplicaImp::tryToSendReqMissingDataMsg(SeqNum seqNumber, bool slowPathOnly, uint16_t destReplicaId) {
  if ((!currentViewIsActive()) || (seqNumber <= strictLowerBoundOfSeqNums_) ||
      (!mainLog_->insideActiveWindow(seqNumber)) || (!mainLog_->insideActiveWindow(seqNumber))) {
    return;
  }

  SeqNumInfo &seqNumInfo = mainLog_->get(seqNumber);
  PartialProofsSet &partialProofsSet = seqNumInfo.partialProofs();

  const Time curTime = getMonotonicTime();

  {
    Time t = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();
    const Time lastInfoRequest = seqNumInfo.getTimeOfLastInfoRequest();

    if ((t < lastInfoRequest)) t = lastInfoRequest;

    if (t == MinTime && (t < curTime)) {
      auto diffMilli = duration_cast<milliseconds>(curTime - t);
      if (diffMilli.count() < dynamicUpperLimitOfRounds_->upperLimit() / 4)  // TODO(GG): config
        return;
    }
  }

  seqNumInfo.setTimeOfLastInfoRequest(curTime);

  LOG_INFO_F(GL, "Node %d tries to request missing data for seqNumber %" PRId64 "", (int)myReplicaId_, seqNumber);

  ReqMissingDataMsg reqData(myReplicaId_, currentView_, seqNumber);

  const bool routerForPartialProofs = replicasInfo_->isCollectorForPartialProofs(currentView_, seqNumber);

  const bool routerForPartialPrepare = (currentPrimary() == myReplicaId_);

  const bool routerForPartialCommit = (currentPrimary() == myReplicaId_);

  const bool missingPrePrepare = (seqNumInfo.getPrePrepareMsg() == nullptr);
  const bool missingBigRequests = (!missingPrePrepare) && (!seqNumInfo.hasPrePrepareMsg());

  ReplicaId firstRepId = 0;
  ReplicaId lastRepId = numOfReplicas_ - 1;
  if (destReplicaId != ALL_OTHER_REPLICAS) {
    firstRepId = destReplicaId;
    lastRepId = destReplicaId;
  }

  for (ReplicaId destRep = firstRepId; destRep <= lastRepId; destRep++) {
    if (destRep == myReplicaId_) continue;  // don't send to myself

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
               myReplicaId_,
               destRep,
               seqNumber,
               (unsigned int)reqData.getFlags());

    send(&reqData, destRep);
  }
}

void ReplicaImp::onMessage(ReqMissingDataMsg *msg) {
  metric_received_req_missing_datas_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();
  const ReplicaId msgSender = msg->senderId();

  LOG_INFO_F(GL,
             "Node %d received ReqMissingDataMsg message from Node %d - seqNumber %" PRId64 " , flags=%X",
             (int)myReplicaId_,
             (int)msgSender,
             msgSeqNum,
             (unsigned int)msg->getFlags());

  if ((currentViewIsActive()) && (msgSeqNum > strictLowerBoundOfSeqNums_) &&
      (mainLog_->insideActiveWindow(msgSeqNum)) && (mainLog_->insideActiveWindow(msgSeqNum))) {
    SeqNumInfo &seqNumInfo = mainLog_->get(msgSeqNum);

    if (myReplicaId_ == currentPrimary()) {
      PrePrepareMsg *pp = seqNumInfo.getSelfPrePrepareMsg();
      if (msg->getPrePrepareIsMissing()) {
        if (pp != 0) send(pp, msgSender);
      }

      if (seqNumInfo.slowPathStarted() && !msg->getSlowPathHasStarted()) {
        StartSlowCommitMsg startSlowMsg(myReplicaId_, currentView_, msgSeqNum);
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
      CommitPartialMsg *c = mainLog_->get(msgSeqNum).getSelfCommitPartialMsg();
      if (c != nullptr) send(c, msgSender);
    }

    if (msg->getFullCommitIsMissing()) {
      CommitFullMsg *c = mainLog_->get(msgSeqNum).getValidCommitFullMsg();
      if (c != nullptr) send(c, msgSender);
    }

    if (msg->getFullCommitProofIsMissing() && seqNumInfo.partialProofs().hasFullProof()) {
      FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();
      send(fcp, msgSender);
    }
  } else {
    LOG_INFO_F(GL, "Node %d ignores the ReqMissingDataMsg message from Node %d", (int)myReplicaId_, (int)msgSender);
  }

  delete msg;
}

void ReplicaImp::onViewsChangeTimer(Timers::Handle timer)  // TODO(GG): review/update logic
{
  Assert(viewChangeProtocolEnabled_);

  if (stateTransfer_->isCollectingState()) return;

  const Time currTime = getMonotonicTime();

  //////////////////////////////////////////////////////////////////////////////
  //
  //////////////////////////////////////////////////////////////////////////////

  if (autoPrimaryUpdateEnabled && currentViewIsActive()) {
    const uint64_t timeout =
        (isCurrentPrimary() ? (autoPrimaryUpdateMilli) : (autoPrimaryUpdateMilli + viewChangeTimeoutMilli));

    const uint64_t diffMilli = duration_cast<milliseconds>(currTime - timeOfLastViewEntrance_).count();

    if (diffMilli > timeout) {
      LOG_INFO_F(GL,
                 "**************** Node %d initiates automatic view change in view %" PRId64 " (%" PRIu64
                 " milli seconds after start working in the previous view)",
                 myReplicaId_,
                 currentView_,
                 diffMilli);

      GotoNextView();
      return;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //
  //////////////////////////////////////////////////////////////////////////////

  uint64_t viewChangeTimeout = viewChangeTimerMilli_;
  if (autoIncViewChangeTimer && ((lastViewThatTransferredSeqNumbersFullyExecuted_ + 1) < currentView_)) {
    uint64_t factor = (currentView_ - lastViewThatTransferredSeqNumbersFullyExecuted_);
    viewChangeTimeout = viewChangeTimeout * factor;  // TODO(GG): review logic here
  }

  if (currentViewIsActive()) {
    if (isCurrentPrimary()) return;

    const Time timeOfEarliestPendingRequest = clientsManager_->timeOfEarliestPendingRequest();

    const bool hasPendingRequest = (timeOfEarliestPendingRequest != MaxTime);

    if (!hasPendingRequest) return;

    const uint64_t diffMilli1 = duration_cast<milliseconds>(currTime - timeOfLastStateSynch_).count();
    const uint64_t diffMilli2 = duration_cast<milliseconds>(currTime - timeOfLastViewEntrance_).count();
    const uint64_t diffMilli3 = duration_cast<milliseconds>(currTime - timeOfEarliestPendingRequest).count();

    if ((diffMilli1 > viewChangeTimeout) && (diffMilli2 > viewChangeTimeout) && (diffMilli3 > viewChangeTimeout)) {
      LOG_INFO_F(GL,
                 "**************** Node %d asks to leave view %" PRId64 " (%" PRIu64
                 ""
                 " milli seconds after receiving a client request)",
                 myReplicaId_,
                 currentView_,
                 diffMilli3);

      GotoNextView();
      return;
    }
  } else  // not currentViewIsActive()
  {
    if (lastAgreedView_ != currentView_) return;
    if (replicasInfo_->primaryOfView(lastAgreedView_) == myReplicaId_) return;

    const Time currTime = getMonotonicTime();
    const uint64_t diffMilli1 = duration_cast<milliseconds>(currTime - timeOfLastStateSynch_).count();
    const uint64_t diffMilli2 = duration_cast<milliseconds>(currTime - timeOfLastAgreedView_).count();

    if ((diffMilli1 > viewChangeTimeout) && (diffMilli2 > viewChangeTimeout)) {
      LOG_INFO_F(GL,
                 "**************** Node %d asks to jump to view %" PRId64 " (%" PRIu64
                 ""
                 " milliseconds after receiving 2f+2c+1 view change msgs)",
                 myReplicaId_,
                 currentView_,
                 diffMilli2);
      GotoNextView();
      return;
    }
  }
}

void ReplicaImp::onStateTranTimer(Timers::Handle timer) { stateTransfer_->onTimer(); }

void ReplicaImp::onStatusReportTimer(Timers::Handle timer) {
  tryToSendStatusReport();

#ifdef DEBUG_MEMORY_MSG
  MessageBase::printLiveMessages();
#endif
}

void ReplicaImp::onSlowPathTimer(Timers::Handle timer) {
  tryToStartSlowPaths();
  auto newPeriod = milliseconds(controller_->slowPathsTimerMilli());
  timers_.reset(timer, newPeriod);
}

void ReplicaImp::onInfoRequestTimer(Timers::Handle timer) {
  tryToAskForMissingInfo();
  auto newPeriod = milliseconds(dynamicUpperLimitOfRounds_->upperLimit() / 2);
  timers_.reset(timer, newPeriod);
}

void ReplicaImp::onDebugStatTimer(Timers::Handle timer) {
  if (debugStatisticsEnabled_) {
    DebugStatistics::onCycleCheck();
  }
}

void ReplicaImp::onMetricsTimer(Timers::Handle timer) { metrics_.UpdateAggregator(); }

void ReplicaImp::onMessage(SimpleAckMsg *msg) {
  metric_received_simple_acks_.Get().Inc();
  if (retransmissionsLogicEnabled) {
    uint16_t relatedMsgType = (uint16_t)msg->ackData();  // TODO(GG): does this make sense ?

    LOG_DEBUG_F(GL,
                "Node %d received SimpleAckMsg message from node %d for seqNumber %" PRId64 " and type %d",
                myReplicaId_,
                msg->senderId(),
                msg->seqNumber(),
                (int)relatedMsgType);

    retransmissionsManager_->onAck(msg->senderId(), msg->seqNumber(), relatedMsgType);
  } else {
    // TODO(GG): print warning ?
  }

  delete msg;
}

void ReplicaImp::onMessage(StateTransferMsg *m) {
  metric_received_state_transfers_.Get().Inc();
  size_t h = sizeof(MessageBase::Header);
  stateTransfer_->handleStateTransferMessage(m->body() + h, m->size() - h, m->senderId());
}

void ReplicaImp::freeStateTransferMsg(char *m) {
  // This method may be called by external threads
  char *p = (m - sizeof(MessageBase::Header));
  std::free(p);
}

void ReplicaImp::sendStateTransferMessage(char *m, uint32_t size, uint16_t replicaId) {
  // This method may be called by external threads
  // TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the replica's main
  // thread

  if (mainThread_.get_id() == std::this_thread::get_id()) {
    MessageBase *p = new MessageBase(myReplicaId_, MsgCode::StateTransfer, size + sizeof(MessageBase::Header));
    char *x = p->body() + sizeof(MessageBase::Header);
    memcpy(x, m, size);
    send(p, replicaId);
    delete p;
  } else {
    // TODO(GG): implement
    Assert(false);
  }
}

void ReplicaImp::onTransferringComplete(int64_t checkpointNumberOfNewState) {
  // This method may be called by external threads
  // TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the replica's main
  // thread
  if (mainThread_.get_id() == std::this_thread::get_id()) {
    onTransferringCompleteImp(checkpointNumberOfNewState * checkpointWindowSize);
  } else {
    // TODO(GG): implement
    Assert(false);
  }
}

void ReplicaImp::changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) {
  // This method may be called by external threads
  // TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the replica's main
  // thread

  if (mainThread_.get_id() == std::this_thread::get_id()) {
    timers_.reset(stateTranTimer_, milliseconds(timerPeriodMilli));
  } else {
    // TODO(GG): implement
    Assert(false);
  }
}

void ReplicaImp::onMerkleExecSignature(ViewNum v, SeqNum s, uint16_t signatureLength, const char *signature) {
  Assert(false);
  // TODO(GG): use code from previous drafts
}

void ReplicaImp::onMessage(PartialExecProofMsg *m) {
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
                       RequestsHandler *requestsHandler,
                       IStateTransfer *stateTrans,
                       shared_ptr<MsgsCommunicator> &msgsCommunicator,
                       shared_ptr<PersistentStorage> &persistentStorage)
    : ReplicaImp(false, ld.repConfig, requestsHandler, stateTrans, ld.sigManager, ld.repsInfo, ld.viewsManager) {
  Assert(persistentStorage != nullptr);

  ps_ = persistentStorage;

  currentView_ = ld.viewsManager->latestActiveView();
  lastAgreedView_ = currentView_;
  metric_view_.Get().Set(currentView_);
  metric_last_agreed_view_.Get().Set(lastAgreedView_);

  const bool inView = ld.viewsManager->viewIsActive(currentView_);

  primaryLastUsedSeqNum_ = ld.primaryLastUsedSeqNum;
  lastStableSeqNum_ = ld.lastStableSeqNum;
  metric_last_stable_seq_num_.Get().Set(lastStableSeqNum_);
  lastExecutedSeqNum_ = ld.lastExecutedSeqNum;
  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum_);
  strictLowerBoundOfSeqNums_ = ld.strictLowerBoundOfSeqNums;
  maxSeqNumTransferredFromPrevViews_ = ld.maxSeqNumTransferredFromPrevViews;
  lastViewThatTransferredSeqNumbersFullyExecuted_ = ld.lastViewThatTransferredSeqNumbersFullyExecuted;

  mainLog_->resetAll(lastStableSeqNum_ + 1);
  checkpointsLog_->resetAll(lastStableSeqNum_);

  if (inView) {
    const bool isPrimaryOfView = (replicasInfo_->primaryOfView(currentView_) == myReplicaId_);

    SeqNum s = ld.lastStableSeqNum;

    for (size_t i = 0; i < kWorkWindowSize; i++) {
      s++;
      Assert(mainLog_->insideActiveWindow(s));

      const SeqNumData &e = ld.seqNumWinArr[i];

      if (!e.isPrePrepareMsgSet()) continue;

      // such properties should be verified by the code the loads the persistent data
      Assert(e.getPrePrepareMsg()->seqNumber() == s);

      SeqNumInfo &seqNumInfo = mainLog_->get(s);

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

        Assert((cVal_ != 0) || (pathInPrePrepare != CommitPath::FAST_WITH_THRESHOLD));

        if ((pathInPrePrepare == CommitPath::FAST_WITH_THRESHOLD) && (cVal_ > 0))
          commitSigner = thresholdSignerForCommit_;
        else
          commitSigner = thresholdSignerForOptimisticCommit_;

        Digest tmpDigest;
        Digest::calcCombination(ppDigest, currentView_, seqNum, tmpDigest);

        PartialCommitProofMsg *p =
            new PartialCommitProofMsg(myReplicaId_, currentView_, seqNum, pathInPrePrepare, tmpDigest, commitSigner);
        seqNumInfo.partialProofs().addSelfMsgAndPPDigest(
            p,
            tmpDigest);  // TODO(GG): consider using a method that directly adds the message/digest (as in the examples
                         // below)
      }

      if (e.getSlowStarted()) {
        seqNumInfo.startSlowPath();

        // add PreparePartialMsg
        PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
        PreparePartialMsg *p = PreparePartialMsg::create(
            currentView_, pp->seqNumber(), myReplicaId_, pp->digestOfRequests(), thresholdSignerForSlowPathCommit_);
        bool added = seqNumInfo.addSelfMsg(p, true);
        Assert(added);
      }

      if (e.isPrepareFullMsgSet()) {
        seqNumInfo.addMsg(e.getPrepareFullMsg(), true);

        Digest d;
        Digest::digestOfDigest(e.getPrePrepareMsg()->digestOfRequests(), d);
        CommitPartialMsg *c =
            CommitPartialMsg::create(currentView_, s, myReplicaId_, d, thresholdSignerForSlowPathCommit_);

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

    Assert(checkpointsLog_->insideActiveWindow(s));
    // e.checkpointMsg==nullptr ==> (s>ld.lastStableSeqNum || s == 0)
    Assert(e.isCheckpointMsgSet() || (s > ld.lastStableSeqNum || s == 0));

    if (!e.isCheckpointMsgSet()) continue;

    CheckpointInfo &checkInfo = checkpointsLog_->get(s);

    Assert(e.getCheckpointMsg()->seqNumber() == s);
    Assert(e.getCheckpointMsg()->senderId() == myReplicaId_);
    Assert((s != ld.lastStableSeqNum) || e.getCheckpointMsg()->isStableState());

    checkInfo.addCheckpointMsg(e.getCheckpointMsg(), myReplicaId_);
    Assert(checkInfo.selfCheckpointMsg()->equals(*e.getCheckpointMsg()));

    if (e.getCompletedMark()) checkInfo.tryToMarkCheckpointCertificateCompleted();
  }

  if (ld.isExecuting) {
    Assert(viewsManager_->viewIsActive(currentView_));
    Assert(mainLog_->insideActiveWindow(lastExecutedSeqNum_ + 1));
    const SeqNumInfo &seqNumInfo = mainLog_->get(lastExecutedSeqNum_ + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    Assert(pp != nullptr);
    Assert(pp->seqNumber() == lastExecutedSeqNum_ + 1);
    Assert(pp->viewNumber() == currentView_);
    Assert(pp->numberOfRequests() > 0);

    Bitmap b = ld.validRequestsThatAreBeingExecuted;
    size_t expectedValidRequests = 0;
    for (uint32_t i = 0; i < b.numOfBits(); i++) {
      if (b.get(i)) expectedValidRequests++;
    }
    Assert(expectedValidRequests <= pp->numberOfRequests());

    recoveringFromExecutionOfRequests_ = true;
    mapOfRequestsThatAreBeingRecovered_ = b;
  }

  int comStatus = msgsCommunicator->start(myReplicaId_);
  Assert(comStatus == 0);

  internalThreadPool_.start(numOfThreads_);
}

ReplicaImp::ReplicaImp(const ReplicaConfig &config,
                       RequestsHandler *requestsHandler,
                       IStateTransfer *stateTrans,
                       shared_ptr<MsgsCommunicator> &msgsCommunicator,
                       shared_ptr<PersistentStorage> &persistentStorage)
    : ReplicaImp(true, config, requestsHandler, stateTrans, nullptr, nullptr, nullptr) {
  if (persistentStorage != nullptr) {
    ps_ = persistentStorage;

    Assert(!ps_->hasReplicaConfig());

    ps_->beginWriteTran();
    ps_->setReplicaConfig(config);
    ps_->endWriteTran();
  }

  int comStatus = msgsCommunicator->start(myReplicaId_);
  Assert(comStatus == 0);

  internalThreadPool_.start(numOfThreads_);
}

ReplicaImp::ReplicaImp(bool firstTime,
                       const ReplicaConfig &config,
                       RequestsHandler *requestsHandler,
                       IStateTransfer *stateTrans,
                       SigManager *sigMgr,
                       ReplicasInfo *replicasInfo,
                       ViewsManager *viewsMgr)
    : myReplicaId_{config.replicaId},
      fVal_{config.fVal},
      cVal_{config.cVal},
      numOfReplicas_{(uint16_t)(3 * config.fVal + 2 * config.cVal + 1)},
      numOfClientProxies_{config.numOfClientProxies},
      viewChangeProtocolEnabled_{((!forceViewChangeProtocolEnabled && !forceViewChangeProtocolDisabled)
                                      ? config.autoViewChangeEnabled
                                      : forceViewChangeProtocolEnabled)},
      supportDirectProofs_{false},
      debugStatisticsEnabled_{config.debugStatisticsEnabled},
      metaMsgHandlers_{createMapOfMetaMsgHandlers()},
      mainThread_(),
      mainThreadStarted_(false),
      mainThreadShouldStop_(false),
      mainThreadShouldStopWhenStateIsNotCollected_(false),
      restarted_{!firstTime},
      retransmissionsManager_{nullptr},
      controller_{nullptr},
      replicasInfo_{nullptr},
      sigManager_{nullptr},
      viewsManager_{nullptr},
      maxConcurrentAgreementsByPrimary_{0},
      currentView_{0},
      primaryLastUsedSeqNum_{0},
      lastStableSeqNum_{0},
      lastExecutedSeqNum_{0},
      strictLowerBoundOfSeqNums_{0},
      maxSeqNumTransferredFromPrevViews_{0},
      mainLog_{nullptr},
      checkpointsLog_{nullptr},
      clientsManager_{nullptr},
      replyBuffer_{(char *)std::malloc(config.maxReplyMessageSize - sizeof(ClientReplyMsgHeader))},
      stateTransfer_{(stateTrans != nullptr ? stateTrans : new NullStateTransfer())},
      maxNumberOfPendingRequestsInRecentHistory_{0},
      batchingFactor_{1},
      userRequestsHandler_{requestsHandler},
      thresholdSignerForExecution_{nullptr},
      thresholdVerifierForExecution_{nullptr},
      thresholdSignerForSlowPathCommit_{config.thresholdSignerForSlowPathCommit},
      thresholdVerifierForSlowPathCommit_{config.thresholdVerifierForSlowPathCommit},
      thresholdSignerForCommit_{config.thresholdSignerForCommit},
      thresholdVerifierForCommit_{config.thresholdVerifierForCommit},
      thresholdSignerForOptimisticCommit_{config.thresholdSignerForOptimisticCommit},
      thresholdVerifierForOptimisticCommit_{config.thresholdVerifierForOptimisticCommit},
      dynamicUpperLimitOfRounds_{nullptr},
      lastViewThatTransferredSeqNumbersFullyExecuted_{0},
      lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas_{MinTime},
      timeOfLastStateSynch_{getMonotonicTime()},    // TODO(GG): TBD
      timeOfLastViewEntrance_{getMonotonicTime()},  // TODO(GG): TBD
      lastAgreedView_{0},
      timeOfLastAgreedView_{getMonotonicTime()},  // TODO(GG): TBD
      viewChangeTimerMilli_{0},
      startSyncEvent_{false},
      metrics_{concordMetrics::Component("replica", std::make_shared<concordMetrics::Aggregator>())},
      metric_view_{metrics_.RegisterGauge("view", currentView_)},
      metric_last_stable_seq_num_{metrics_.RegisterGauge("lastStableSeqNum", lastStableSeqNum_)},
      metric_last_executed_seq_num_{metrics_.RegisterGauge("lastExecutedSeqNum", lastExecutedSeqNum_)},
      metric_last_agreed_view_{metrics_.RegisterGauge("lastAgreedView", lastAgreedView_)},
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
      metric_received_state_transfers_{metrics_.RegisterCounter("receivedStateTransferMsgs")} {
  Assert(myReplicaId_ < numOfReplicas_);
  // TODO(GG): more asserts on params !!!!!!!!!!!

  // !firstTime ==> ((sigMgr != nullptr) && (replicasInfo != nullptr) && (viewsMgr != nullptr))
  Assert(firstTime || ((sigMgr != nullptr) && (replicasInfo != nullptr) && (viewsMgr != nullptr)));

  if (debugStatisticsEnabled_) {
    DebugStatistics::initDebugStatisticsData();
  }

  // Register metrics component with the default aggregator.
  metrics_.Register();

  if (firstTime) {
    std::set<SigManager::PublicKeyDesc> replicasSigPublicKeys;

    for (auto e : config.publicKeysOfReplicas) {
      SigManager::PublicKeyDesc keyDesc = {e.first, e.second};
      replicasSigPublicKeys.insert(keyDesc);
    }

    sigManager_ = new SigManager(
        myReplicaId_, numOfReplicas_ + numOfClientProxies_, config.replicaPrivateKey, replicasSigPublicKeys);
    replicasInfo_ = new ReplicasInfo(myReplicaId_,
                                     *sigManager_,
                                     numOfReplicas_,
                                     fVal_,
                                     cVal_,
                                     dynamicCollectorForPartialProofs,
                                     dynamicCollectorForExecutionProofs);
    viewsManager_ = new ViewsManager(replicasInfo_, thresholdVerifierForSlowPathCommit_);
  } else {
    sigManager_ = sigMgr;
    replicasInfo_ = replicasInfo;
    viewsManager_ = viewsMgr;

    // TODO(GG): consider to add relevant asserts
  }

  std::set<NodeIdType> clientsSet;
  for (uint16_t i = numOfReplicas_; i < numOfReplicas_ + numOfClientProxies_; i++) clientsSet.insert(i);

  clientsManager_ =
      new ClientsManager(myReplicaId_, clientsSet, ReplicaConfigSingleton::GetInstance().GetSizeOfReservedPage());

  if (firstTime || !config.debugPersistentStorageEnabled) {
    stateTransfer_->init(kWorkWindowSize / checkpointWindowSize + 1,
                         clientsManager_->numberOfRequiredReservedPages(),
                         ReplicaConfigSingleton::GetInstance().GetSizeOfReservedPage());
  } else  // !firstTime && debugPersistentStorageEnabled
  {
    // TODO: add asserts
  }

  clientsManager_->init(stateTransfer_);

  if (!firstTime || config.debugPersistentStorageEnabled)
    clientsManager_->loadInfoFromReservedPages();
  else
    clientsManager_->clearReservedPages();

  int statusReportTimerMilli = (sendStatusPeriodMilli > 0) ? sendStatusPeriodMilli : config.statusReportTimerMillisec;
  Assert(statusReportTimerMilli > 0);

  viewChangeTimerMilli_ = (viewChangeTimeoutMilli > 0) ? viewChangeTimeoutMilli : config.viewChangeTimerMillisec;
  Assert(viewChangeTimerMilli_ > 0);

  int concurrencyLevel = config.concurrencyLevel;
  Assert(concurrencyLevel > 0);
  Assert(concurrencyLevel < MaxConcurrentFastPaths);

  LOG_INFO_F(GL, "\nConcurrency Level: %d\n", concurrencyLevel);  // TODO(GG): all configuration should be displayed

  Assert(concurrencyLevel <= maxLegalConcurrentAgreementsByPrimary);
  maxConcurrentAgreementsByPrimary_ = (uint16_t)concurrencyLevel;

  // TODO(GG): use config ...
  dynamicUpperLimitOfRounds_ = new DynamicUpperLimitWithSimpleFilter<int64_t>(400, 2, 2500, 70, 32, 1000, 2, 2);

  mainLog_ =
      new SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumInfo, SeqNumInfo>(1, (InternalReplicaApi *)this);

  checkpointsLog_ = new SequenceWithActiveWindow<kWorkWindowSize + checkpointWindowSize,
                                                 checkpointWindowSize,
                                                 SeqNum,
                                                 CheckpointInfo,
                                                 CheckpointInfo>(0, (InternalReplicaApi *)this);

  // create controller . TODO(GG): do we want to pass the controller as a parameter ?
  controller_ = new ControllerWithSimpleHistory(cVal_, fVal_, myReplicaId_, currentView_, primaryLastUsedSeqNum_);

  statusReportTimer_ = timers_.add(milliseconds(statusReportTimerMilli),
                                   Timers::Timer::RECURRING,
                                   [this](Timers::Handle h) { onStatusReportTimer(h); });

  if (viewChangeProtocolEnabled_) {
    int t = viewChangeTimerMilli_;
    if (autoPrimaryUpdateEnabled && t > autoPrimaryUpdateMilli) t = autoPrimaryUpdateMilli;

    // TODO(GG): what should be the time period here? .
    // TODO(GG):Consider to split to 2 different timers
    viewChangeTimer_ =
        timers_.add(milliseconds(t / 2), Timers::Timer::RECURRING, [this](Timers::Handle h) { onViewsChangeTimer(h); });
  }

  stateTranTimer_ = timers_.add(5s, Timers::Timer::RECURRING, [this](Timers::Handle h) { onStateTranTimer(h); });

  if (retransmissionsLogicEnabled) {
    retranTimer_ = timers_.add(milliseconds(retransmissionsTimerMilli),
                               Timers::Timer::RECURRING,
                               [this](Timers::Handle h) { onRetransmissionsTimer(h); });
  }

  const int slowPathsTimerPeriod = controller_->timeToStartSlowPathMilli();

  slowPathTimer_ = timers_.add(
      milliseconds(slowPathsTimerPeriod), Timers::Timer::RECURRING, [this](Timers::Handle h) { onSlowPathTimer(h); });

  infoReqTimer_ = timers_.add(milliseconds(dynamicUpperLimitOfRounds_->upperLimit() / 2),
                              Timers::Timer::RECURRING,
                              [this](Timers::Handle h) { onInfoRequestTimer(h); });

  if (debugStatisticsEnabled_) {
    debugStatTimer_ = timers_.add(seconds(DEBUG_STAT_PERIOD_SECONDS),
                                  Timers::Timer::RECURRING,
                                  [this](Timers::Handle h) { onDebugStatTimer(h); });
  }

  metricsTimer_ = timers_.add(100ms, Timers::Timer::RECURRING, [this](Timers::Handle h) { onMetricsTimer(h); });

  if (retransmissionsLogicEnabled)
    retransmissionsManager_ =
        new RetransmissionsManager(this, &internalThreadPool_, msgsCommunicator_, kWorkWindowSize, 0);
  else
    retransmissionsManager_ = nullptr;

  LOG_INFO(GL,
           "numOfClientProxies=" << numOfClientProxies_ << " maxExternalMessageSize=" << config.maxExternalMessageSize
                                 << " maxReplyMessageSize=" << config.maxReplyMessageSize << " maxNumOfReservedPages="
                                 << config.maxNumOfReservedPages << " sizeOfReservedPage=" << config.sizeOfReservedPage
                                 << " viewChangeTimerMilli=" << viewChangeTimerMilli_);
}

ReplicaImp::~ReplicaImp() {
  // TODO(GG): rewrite this method !!!!!!!! (notice that the order may be important here ).
  // TODO(GG): don't delete objects that are passed as params (TBD)

  internalThreadPool_.stop();
  delete thresholdSignerForCommit_;
  delete thresholdVerifierForCommit_;
  delete thresholdSignerForExecution_;
  delete thresholdVerifierForExecution_;
  delete viewsManager_;
  delete controller_;
  delete dynamicUpperLimitOfRounds_;
  delete mainLog_;
  delete checkpointsLog_;

  if (debugStatisticsEnabled_) {
    DebugStatistics::freeDebugStatisticsData();
  }
}

void ReplicaImp::start() {
  Assert(!mainThreadStarted_);
  Assert(!mainThreadShouldStop_);
  Assert(!mainThreadShouldStopWhenStateIsNotCollected_);
  mainThreadStarted_ = true;

  std::thread mThread([this] { processMessages(); });
  mainThread_.swap(mThread);
  startSyncEvent_.set();
}

void ReplicaImp::stop() {
  std::unique_ptr<InternalMessage> stopMsg(new StopInternalMsg(this));
  msgsCommunicator_->pushInternalMsg(std::move(stopMsg));

  mainThread_.join();

  msgsCommunicator_->stop();

  Assert(mainThreadShouldStop_);

  mainThreadShouldStop_ = false;
  mainThreadShouldStopWhenStateIsNotCollected_ = false;
  mainThreadStarted_ = false;
}

void ReplicaImp::stopWhenStateIsNotCollected() {
  std::unique_ptr<InternalMessage> stopMsg(new StopWhenStateIsNotCollectedInternalMsg(this));
  msgsCommunicator_->pushInternalMsg(std::move(stopMsg));

  mainThread_.join();

  msgsCommunicator_->stop();

  Assert(mainThreadShouldStop_);

  mainThreadShouldStop_ = false;
  mainThreadShouldStopWhenStateIsNotCollected_ = false;
  mainThreadStarted_ = false;
}

bool ReplicaImp::isRunning() const { return mainThreadStarted_; }

SeqNum ReplicaImp::getLastExecutedSequenceNum() const { return lastExecutedSeqNum_; }

void ReplicaImp::processMessages() {
  // TODO(GG): change this method to support "restart" ("start" after "stop")

  startSyncEvent_.wait_one();

  stateTransfer_->startRunning(this);

  LOG_INFO_F(GL, "Running");

  if (recoveringFromExecutionOfRequests_) {
    const SeqNumInfo &seqNumInfo = mainLog_->get(lastExecutedSeqNum_ + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    Assert(pp != nullptr);
    executeRequestsInPrePrepareMsg(pp, true);

    recoveringFromExecutionOfRequests_ = false;
    mapOfRequestsThatAreBeingRecovered_ = Bitmap();
  }

  while (!mainThreadShouldStop_) {
    Assert(ps_ == nullptr || !ps_->isInWriteTran());

    auto msg = recvMsg();  // wait for a message
    if (msg.tag == IncomingMsg::INTERNAL) {
      metric_received_internal_msgs_.Get().Inc();
      msg.internal->handle();
      continue;
    }

    // TODO: (AJS) Don't turn this back into a raw
    // pointer. Pass the smart pointer through the
    // messsage handlers so they take ownership.
    auto m = msg.external.release();

    if (debugStatisticsEnabled_) {
      DebugStatistics::onReceivedExMessage(m->type());
    }

    auto g = metaMsgHandlers_.find(m->type());
    if (g != metaMsgHandlers_.end()) {
      PtrToMetaMsgHandler ptrMetaHandler = g->second;
      (this->*ptrMetaHandler)(m);
    } else {
      LOG_WARN_F(GL, "Unknown message");
      delete m;
    }
  }
}

void ReplicaImp::executeReadOnlyRequest(ClientRequestMsg *request) {
  Assert(request->isReadOnly());
  Assert(!stateTransfer_->isCollectingState());

  ClientReplyMsg reply(currentPrimary(), request->requestSeqNum(), myReplicaId_);

  uint16_t clientId = request->clientProxyId();

  int error = 0;

  uint32_t actualReplyLength = 0;

  if (!supportDirectProofs_) {
    error = userRequestsHandler_->execute(clientId,
                                          lastExecutedSeqNum_,
                                          true,
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

  if (debugStatisticsEnabled_) {
    DebugStatistics::onRequestCompleted(true);
  }
}

void ReplicaImp::executeRequestsInPrePrepareMsg(PrePrepareMsg *ppMsg, bool recoverFromErrorInRequestsExecution) {
  Assert(!stateTransfer_->isCollectingState() && currentViewIsActive());
  Assert(ppMsg != nullptr);
  Assert(ppMsg->viewNumber() == currentView_);
  Assert(ppMsg->seqNumber() == lastExecutedSeqNum_ + 1);

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

        const bool validClient = clientsManager_->isValidClient(clientId);
        if (!validClient) {
          LOG_WARN(GL, "The client clientId=%d is not valid" << clientId);
          continue;
        }

        if (clientsManager_->seqNumberOfLastReplyToClient(clientId) >= req.requestSeqNum()) {
          ClientReplyMsg *replyMsg = clientsManager_->allocateMsgWithLatestReply(clientId, currentPrimary());
          send(replyMsg, clientId);
          delete replyMsg;
          continue;
        }

        requestSet.set(reqIdx);
        reqIdx++;
      }
      reqIter.restart();

      if (ps_) {
        DescriptorOfLastExecution execDesc{lastExecutedSeqNum_ + 1, requestSet};
        ps_->beginWriteTran();
        ps_->setDescriptorOfLastExecution(execDesc);
        ps_->endWriteTran();
      }
    } else {
      requestSet = mapOfRequestsThatAreBeingRecovered_;
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
      userRequestsHandler_->execute(
          clientId,
          lastExecutedSeqNum_ + 1,
          req.isReadOnly(),
          req.requestLength(),
          req.requestBuf(),
          ReplicaConfigSingleton::GetInstance().GetMaxReplyMessageSize() - sizeof(ClientReplyMsgHeader),
          replyBuffer_,
          actualReplyLength);

      Assert(actualReplyLength > 0);  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)

      ClientReplyMsg *replyMsg = clientsManager_->allocateNewReplyMsgAndWriteToStorage(
          clientId, req.requestSeqNum(), currentPrimary(), replyBuffer_, actualReplyLength);
      send(replyMsg, clientId);
      delete replyMsg;
      clientsManager_->removePendingRequestOfClient(clientId);
    }
  }

  if ((lastExecutedSeqNum_ + 1) % checkpointWindowSize == 0) {
    const uint64_t checkpointNum = (lastExecutedSeqNum_ + 1) / checkpointWindowSize;
    stateTransfer_->createCheckpointOfCurrentState(checkpointNum);
  }

  //////////////////////////////////////////////////////////////////////
  // Phase 3: finalize the execution of lastExecutedSeqNum+1
  // TODO(GG): Explain what happens in recovery mode
  //////////////////////////////////////////////////////////////////////

  LOG_DEBUG_F(
      GL, "\nReplica - executeRequestsInPrePrepareMsg() - lastExecutedSeqNum==%" PRId64 "", (lastExecutedSeqNum_ + 1));

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setLastExecutedSeqNum(lastExecutedSeqNum_ + 1);
  }

  lastExecutedSeqNum_ = lastExecutedSeqNum_ + 1;

  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum_);
  if (debugStatisticsEnabled_) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum_);
  }
  if (lastViewThatTransferredSeqNumbersFullyExecuted_ < currentView_ &&
      (lastExecutedSeqNum_ >= maxSeqNumTransferredFromPrevViews_)) {
    lastViewThatTransferredSeqNumbersFullyExecuted_ = currentView_;
    if (ps_) {
      ps_->setLastViewThatTransferredSeqNumbersFullyExecuted(lastViewThatTransferredSeqNumbersFullyExecuted_);
    }
  }

  if (lastExecutedSeqNum_ % checkpointWindowSize == 0) {
    Digest checkDigest;
    const uint64_t checkpointNum = lastExecutedSeqNum_ / checkpointWindowSize;
    stateTransfer_->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char *)&checkDigest);
    CheckpointMsg *checkMsg = new CheckpointMsg(myReplicaId_, lastExecutedSeqNum_, checkDigest, false);
    CheckpointInfo &checkInfo = checkpointsLog_->get(lastExecutedSeqNum_);
    checkInfo.addCheckpointMsg(checkMsg, myReplicaId_);

    if (ps_) ps_->setCheckpointMsgInCheckWindow(lastExecutedSeqNum_, checkMsg);

    if (checkInfo.isCheckpointCertificateComplete()) {
      onSeqNumIsStable(lastExecutedSeqNum_);
    }
    checkInfo.setSelfExecutionTime(getMonotonicTime());
  }

  if (ps_) ps_->endWriteTran();

  if (numOfRequests > 0) userRequestsHandler_->onFinishExecutingReadWriteRequests();

  sendCheckpointIfNeeded();

  bool firstCommitPathChanged = controller_->onNewSeqNumberExecution(lastExecutedSeqNum_);

  if (firstCommitPathChanged) {
    metric_first_commit_path_.Get().Set(CommitPathToStr(controller_->getCurrentFirstPath()));
  }
  // TODO(GG): clean the following logic
  if (mainLog_->insideActiveWindow(lastExecutedSeqNum_)) {  // update dynamicUpperLimitOfRounds
    const SeqNumInfo &seqNumInfo = mainLog_->get(lastExecutedSeqNum_);
    const Time firstInfo = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();
    const Time currTime = getMonotonicTime();
    if ((firstInfo < currTime)) {
      const int64_t durationMilli = duration_cast<milliseconds>(currTime - firstInfo).count();
      dynamicUpperLimitOfRounds_->add(durationMilli);
    }
  }

  if (debugStatisticsEnabled_) {
    DebugStatistics::onRequestCompleted(false);
  }
}

void ReplicaImp::executeNextCommittedRequests(const bool requestMissingInfo) {
  Assert(!stateTransfer_->isCollectingState() && currentViewIsActive());
  Assert(lastExecutedSeqNum_ >= lastStableSeqNum_);

  LOG_DEBUG_F(GL, "Calling to executeNextCommittedRequests(requestMissingInfo=%d)", (int)requestMissingInfo);

  while (lastExecutedSeqNum_ < lastStableSeqNum_ + kWorkWindowSize) {
    SeqNumInfo &seqNumInfo = mainLog_->get(lastExecutedSeqNum_ + 1);

    PrePrepareMsg *prePrepareMsg = seqNumInfo.getPrePrepareMsg();

    const bool ready = (prePrepareMsg != nullptr) && (seqNumInfo.isCommitted__gg());

    if (requestMissingInfo && !ready) {
      LOG_DEBUG_F(GL,
                  "executeNextCommittedRequests - Asking for missing information about %" PRId64 "",
                  lastExecutedSeqNum_ + 1);

      tryToSendReqMissingDataMsg(lastExecutedSeqNum_ + 1);
    }

    if (!ready) break;

    Assert(prePrepareMsg->seqNumber() == lastExecutedSeqNum_ + 1);
    Assert(prePrepareMsg->viewNumber() == currentView_);  // TODO(GG): TBD

    executeRequestsInPrePrepareMsg(prePrepareMsg);
  }

  if (isCurrentPrimary() && requestsQueueOfPrimary_.size() > 0) tryToSendPrePrepareMsg(true);
}

void ReplicaImp::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  metrics_.SetAggregator(aggregator);
}

// TODO(GG): the timer for state transfer !!!!

// TODO(GG): !!!! view changes and retransmissionsLogic --- check ....

}  // namespace impl
}  // namespace bftEngine
