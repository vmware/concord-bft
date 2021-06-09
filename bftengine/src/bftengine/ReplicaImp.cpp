// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
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
#include "json_output.hpp"

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
#include "messages/ReplicaAsksToLeaveViewMsg.hpp"
#include "CryptoManager.hpp"
#include "ControlHandler.hpp"

#include <memory>
#include <string>
#include <type_traits>
#include <bitset>
#include "KeyExchangeManager.h"

#define getName(var) #var

using concordUtil::Timers;
using concordUtils::toPair;
using namespace std;
using namespace std::chrono;
using namespace std::placeholders;
using namespace concord::diagnostics;

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

  msgHandlers_->registerMsgHandler(MsgCode::ReplicaAsksToLeaveView,
                                   bind(&ReplicaImp::messageHandler<ReplicaAsksToLeaveViewMsg>, this, _1));

  msgHandlers_->registerInternalMsgHandler([this](InternalMessage &&msg) { onInternalMsg(std::move(msg)); });
}

template <typename T>
void ReplicaImp::messageHandler(MessageBase *msg) {
  T *trueTypeObj = new T(msg);
  delete msg;
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) {
    if constexpr (!std::is_same_v<T, ClientRequestMsg>) {
      LOG_INFO(GL, "Received protocol message while pruning, ignoring the message");
      delete trueTypeObj;
      return;
    }
  }
  if (validateMessage(trueTypeObj) && !isCollectingState())
    onMessage<T>(trueTypeObj);
  else
    delete trueTypeObj;
}

template <class T>
void onMessage(T *);

void ReplicaImp::send(MessageBase *m, NodeIdType dest) {
  if (clientsManager->isInternal(dest)) {
    LOG_DEBUG(GL, "Not sending reply to internal client id - " << dest);
    return;
  }
  // debug code begin
  if (m->type() == MsgCode::Checkpoint && static_cast<CheckpointMsg *>(m)->digestOfState().isZero())
    LOG_WARN(GL, "Debug: checkpoint digest is zero");
  // debug code end
  TimeRecorder scoped_timer(*histograms_.send);
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
  metric_received_client_requests_.Get().Inc();
  const NodeIdType senderId = m->senderId();
  const NodeIdType clientId = m->clientProxyId();
  const bool readOnly = m->isReadOnly();
  const ReqId reqSeqNum = m->requestSeqNum();
  const uint8_t flags = m->flags();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_CID(m->getCid());
  LOG_DEBUG(MSGS, KVLOG(clientId, reqSeqNum, senderId) << " flags: " << std::bitset<8>(flags));

  const auto &span_context = m->spanContext<std::remove_pointer<decltype(m)>::type>();
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_client_request");
  span.setTag("rid", config_.getreplicaId());
  span.setTag("cid", m->getCid());
  span.setTag("seq_num", reqSeqNum);

  if (ReplicaConfig::instance().getkeyExchangeOnStart()) {
    // If Multi sig keys haven't been replaced for all replicas and it's not a key ex msg then don't accept the msg.
    if (!KeyExchangeManager::instance().keysExchanged && !(flags & KEY_EXCHANGE_FLAG)) {
      LOG_INFO(KEY_EX_LOG, "Didn't complete yet, dropping msg");
      delete m;
      return;
    }
  }

  // check message validity
  const bool invalidClient = !isValidClient(clientId);
  const bool sentFromReplicaToNonPrimary = repsInfo->isIdOfReplica(senderId) && !isCurrentPrimary();

  if (invalidClient) {
    ++numInvalidClients;
  }

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

  if ((isCurrentPrimary() && isSeqNumToStopAt(primaryLastUsedSeqNum + 1)) || isSeqNumToStopAt(lastExecutedSeqNum + 1)) {
    LOG_INFO(GL,
             "Ignoring ClientRequest because system is stopped at checkpoint pending control state operation (upgrade, "
             "etc...)");
    delete m;
    return;
  }

  if (!currentViewIsActive()) {
    LOG_INFO(GL, "ClientRequestMsg is ignored because current view is inactive. " << KVLOG(reqSeqNum, clientId));
    delete m;
    return;
  }

  if (!isReplyAlreadySentToClient(clientId, reqSeqNum)) {
    if (isCurrentPrimary()) {
      histograms_.requestsQueueOfPrimarySize->record(requestsQueueOfPrimary.size());
      // TODO(GG): use config/parameter
      if (requestsQueueOfPrimary.size() >= 700) {
        LOG_WARN(GL,
                 "ClientRequestMsg dropped. Primary request queue is full. "
                     << KVLOG(clientId, reqSeqNum, requestsQueueOfPrimary.size()));
        delete m;
        return;
      }
      if (clientsManager->canBecomePending(clientId, reqSeqNum)) {
        LOG_DEBUG(CNSUS,
                  "Pushing to primary queue, request [" << reqSeqNum << "], client [" << clientId
                                                        << "], senderId=" << senderId);
        if (time_to_collect_batch_ == MinTime) time_to_collect_batch_ = getMonotonicTime();
        requestsQueueOfPrimary.push(m);
        primary_queue_size_.Get().Set(requestsQueueOfPrimary.size());
        primaryCombinedReqSize += m->size();
        tryToSendPrePrepareMsg(true);
        return;
      } else {
        LOG_INFO(GL,
                 "ClientRequestMsg is ignored because: request is old, or primary is currently working on it"
                     << KVLOG(clientId, reqSeqNum));
      }
    } else {  // not the current primary
      if (clientsManager->canBecomePending(clientId, reqSeqNum)) {
        clientsManager->addPendingRequest(clientId, reqSeqNum, m->getCid());

        // TODO(GG): add a mechanism that retransmits (otherwise we may start unnecessary view-change)
        send(m, currentPrimary());
        LOG_INFO(GL, "Forwarding ClientRequestMsg to the current primary." << KVLOG(reqSeqNum, clientId));
      } else {
        LOG_INFO(GL,
                 "ClientRequestMsg is ignored because: request is old, or primary is currently working on it"
                     << KVLOG(clientId, reqSeqNum));
      }
    }
  } else {  // Reply has already been sent to the client for this request
    auto repMsg = clientsManager->allocateReplyFromSavedOne(clientId, reqSeqNum, currentPrimary());
    LOG_DEBUG(
        GL,
        "ClientRequestMsg has already been executed: retransmitting reply to client." << KVLOG(reqSeqNum, clientId));
    if (repMsg) {
      send(repMsg.get(), clientId);
    }
  }
  delete m;
}

template <>
void ReplicaImp::onMessage<ReplicaAsksToLeaveViewMsg>(ReplicaAsksToLeaveViewMsg *m) {
  MDC_PUT(MDC_SEQ_NUM_KEY, std::to_string(getCurrentView()));
  if (m->viewNumber() == getCurrentView()) {
    LOG_INFO(VC_LOG,
             "Received ReplicaAsksToLeaveViewMsg " << KVLOG(m->viewNumber(), m->senderId(), m->idOfGeneratedReplica()));
    complainedReplicas.store(std::unique_ptr<ReplicaAsksToLeaveViewMsg>(m));
    tryToGotoNextView();
  } else {
    LOG_WARN(VC_LOG,
             "Ignoring ReplicaAsksToLeaveViewMsg " << KVLOG(
                 getCurrentView(), currentViewIsActive(), m->viewNumber(), m->senderId(), m->idOfGeneratedReplica()));
    delete m;
  }
}

bool ReplicaImp::checkSendPrePrepareMsgPrerequisites() {
  if (!isCurrentPrimary()) {
    LOG_WARN(GL, "Called in a non-primary replica; won't send PrePrepareMsgs!");
    return false;
  }

  if (!currentViewIsActive()) {
    LOG_INFO(GL, "View " << getCurrentView() << " is not active yet. Won't send PrePrepareMsg-s.");
    return false;
  }

  if (isSeqNumToStopAt(primaryLastUsedSeqNum + 1)) {
    LOG_INFO(GL,
             "Not sending PrePrepareMsg because system is stopped at checkpoint pending control state operation "
             "(upgrade, etc...)");
    return false;
  }

  if (primaryLastUsedSeqNum + 1 > lastStableSeqNum + kWorkWindowSize) {
    LOG_INFO(GL,
             "Will not send PrePrepare since next sequence number ["
                 << primaryLastUsedSeqNum + 1 << "] exceeds window threshold [" << lastStableSeqNum + kWorkWindowSize
                 << "]");
    return false;
  }

  if (primaryLastUsedSeqNum + 1 > lastExecutedSeqNum + config_.getconcurrencyLevel()) {
    LOG_INFO(GL,
             "Will not send PrePrepare since next sequence number ["
                 << primaryLastUsedSeqNum + 1 << "] exceeds concurrency threshold ["
                 << lastExecutedSeqNum + config_.getconcurrencyLevel() << "]");
    return false;
  }

  ConcordAssertGE(primaryLastUsedSeqNum, lastExecutedSeqNum);
  // Because maxConcurrentAgreementsByPrimary <  MaxConcurrentFastPaths
  ConcordAssertLE((primaryLastUsedSeqNum + 1), lastExecutedSeqNum + MaxConcurrentFastPaths);

  if (requestsQueueOfPrimary.empty()) LOG_DEBUG(GL, "requestsQueueOfPrimary is empty");

  return (!requestsQueueOfPrimary.empty());
}

void ReplicaImp::removeDuplicatedRequestsFromRequestsQueue() {
  // Remove duplicated requests that are result of client retrials from the head of the requestsQueueOfPrimary
  ClientRequestMsg *first = requestsQueueOfPrimary.front();
  while (first != nullptr && !clientsManager->canBecomePending(first->clientProxyId(), first->requestSeqNum())) {
    primaryCombinedReqSize -= first->size();
    delete first;
    requestsQueueOfPrimary.pop();
    first = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
  }
  primary_queue_size_.Get().Set(requestsQueueOfPrimary.size());
}

PrePrepareMsg *ReplicaImp::buildPrePrepareMsgBatchByOverallSize(uint32_t requiredBatchSizeInBytes) {
  if (primaryCombinedReqSize < requiredBatchSizeInBytes) {
    LOG_DEBUG(GL,
              "Not sufficient messages size in the primary replica queue to fill a batch"
                  << KVLOG(primaryCombinedReqSize, requiredBatchSizeInBytes));
    return nullptr;
  }
  if (!checkSendPrePrepareMsgPrerequisites()) return nullptr;

  removeDuplicatedRequestsFromRequestsQueue();
  return buildPrePrepareMessageByBatchSize(requiredBatchSizeInBytes);
}

PrePrepareMsg *ReplicaImp::buildPrePrepareMsgBatchByRequestsNum(uint32_t requiredRequestsNum) {
  if (requestsQueueOfPrimary.size() < requiredRequestsNum) {
    LOG_DEBUG(GL,
              "Not enough messages in the primary replica queue to fill a batch"
                  << KVLOG(requestsQueueOfPrimary.size(), requiredRequestsNum));
    return nullptr;
  }
  if (!checkSendPrePrepareMsgPrerequisites()) return nullptr;

  removeDuplicatedRequestsFromRequestsQueue();
  return buildPrePrepareMessageByRequestsNum(requiredRequestsNum);
}

bool ReplicaImp::tryToSendPrePrepareMsg(bool batchingLogic) {
  if (!checkSendPrePrepareMsgPrerequisites()) return false;

  removeDuplicatedRequestsFromRequestsQueue();
  PrePrepareMsg *pp = nullptr;
  if (batchingLogic)
    pp = reqBatchingLogic_.batchRequests();
  else
    pp = buildPrePrepareMessage();
  if (!pp) return false;
  if (batchingLogic) {
    batch_closed_on_logic_on_.Get().Inc();
    accumulating_batch_time_.add(
        std::chrono::duration_cast<std::chrono::microseconds>(getMonotonicTime() - time_to_collect_batch_).count());
    accumulating_batch_avg_time_.Get().Set((uint64_t)accumulating_batch_time_.avg());
    if (accumulating_batch_time_.numOfElements() == 1000)
      accumulating_batch_time_.reset();  // We reset the average on every 1000 samples
  } else {
    batch_closed_on_logic_off_.Get().Inc();
  }
  time_to_collect_batch_ = MinTime;
  startConsensusProcess(pp);
  return true;
}

PrePrepareMsg *ReplicaImp::createPrePrepareMessage() {
  CommitPath firstPath = controller->getCurrentFirstPath();

  ConcordAssertOR((config_.getcVal() != 0), (firstPath != CommitPath::FAST_WITH_THRESHOLD));
  if (requestsQueueOfPrimary.empty()) {
    LOG_INFO(GL, "PrePrepareMessage has not created - requestsQueueOfPrimary is empty");
    return nullptr;
  }

  controller->onSendingPrePrepare((primaryLastUsedSeqNum + 1), firstPath);
  return new PrePrepareMsg(config_.getreplicaId(),
                           getCurrentView(),
                           (primaryLastUsedSeqNum + 1),
                           firstPath,
                           requestsQueueOfPrimary.front()->spanContext<ClientRequestMsg>(),
                           primaryCombinedReqSize);
}

ClientRequestMsg *ReplicaImp::addRequestToPrePrepareMessage(ClientRequestMsg *&nextRequest,
                                                            PrePrepareMsg &prePrepareMsg,
                                                            uint16_t maxStorageForRequests) {
  if (nextRequest->size() <= prePrepareMsg.remainingSizeForRequests()) {
    SCOPED_MDC_CID(nextRequest->getCid());
    if (clientsManager->canBecomePending(nextRequest->clientProxyId(), nextRequest->requestSeqNum())) {
      prePrepareMsg.addRequest(nextRequest->body(), nextRequest->size());
      clientsManager->addPendingRequest(
          nextRequest->clientProxyId(), nextRequest->requestSeqNum(), nextRequest->getCid());
    }
    primaryCombinedReqSize -= nextRequest->size();
  } else if (nextRequest->size() > maxStorageForRequests) {  // The message is too big
    LOG_ERROR(GL,
              "Request was dropped because it exceeds maximum allowed size" << KVLOG(
                  prePrepareMsg.seqNumber(), nextRequest->senderId(), nextRequest->size(), maxStorageForRequests));
  }
  delete nextRequest;
  requestsQueueOfPrimary.pop();
  primary_queue_size_.Get().Set(requestsQueueOfPrimary.size());
  return (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
}

PrePrepareMsg *ReplicaImp::finishAddingRequestsToPrePrepareMsg(PrePrepareMsg *&prePrepareMsg,
                                                               uint16_t maxSpaceForReqs,
                                                               uint32_t requiredRequestsSize,
                                                               uint32_t requiredRequestsNum) {
  if (prePrepareMsg->numberOfRequests() == 0) {
    LOG_INFO(GL, "No client requests added to the PrePrepare batch, delete the message");
    delete prePrepareMsg;
    return nullptr;
  }
  prePrepareMsg->finishAddingRequests();
  LOG_DEBUG(GL,
            KVLOG(prePrepareMsg->seqNumber(),
                  prePrepareMsg->getCid(),
                  maxSpaceForReqs,
                  requiredRequestsSize,
                  prePrepareMsg->requestsSize(),
                  requiredRequestsNum,
                  prePrepareMsg->numberOfRequests()));
  return prePrepareMsg;
}

PrePrepareMsg *ReplicaImp::buildPrePrepareMessage() {
  PrePrepareMsg *prePrepareMsg = createPrePrepareMessage();
  if (!prePrepareMsg) return nullptr;
  SCOPED_MDC("pp_msg_cid", prePrepareMsg->getCid());

  uint16_t maxSpaceForReqs = prePrepareMsg->remainingSizeForRequests();
  ClientRequestMsg *nextRequest = requestsQueueOfPrimary.front();
  while (nextRequest != nullptr)
    nextRequest = addRequestToPrePrepareMessage(nextRequest, *prePrepareMsg, maxSpaceForReqs);

  return finishAddingRequestsToPrePrepareMsg(prePrepareMsg, maxSpaceForReqs, 0, 0);
}

PrePrepareMsg *ReplicaImp::buildPrePrepareMessageByRequestsNum(uint32_t requiredRequestsNum) {
  PrePrepareMsg *prePrepareMsg = createPrePrepareMessage();
  if (!prePrepareMsg) return nullptr;
  SCOPED_MDC("pp_msg_cid", prePrepareMsg->getCid());

  uint16_t maxSpaceForReqs = prePrepareMsg->remainingSizeForRequests();
  ClientRequestMsg *nextRequest = requestsQueueOfPrimary.front();
  while (nextRequest != nullptr && prePrepareMsg->numberOfRequests() < requiredRequestsNum)
    nextRequest = addRequestToPrePrepareMessage(nextRequest, *prePrepareMsg, maxSpaceForReqs);

  return finishAddingRequestsToPrePrepareMsg(prePrepareMsg, maxSpaceForReqs, 0, requiredRequestsNum);
}

PrePrepareMsg *ReplicaImp::buildPrePrepareMessageByBatchSize(uint32_t requiredBatchSizeInBytes) {
  PrePrepareMsg *prePrepareMsg = createPrePrepareMessage();
  if (!prePrepareMsg) return nullptr;
  SCOPED_MDC("pp_msg_cid", prePrepareMsg->getCid());

  uint16_t maxSpaceForReqs = prePrepareMsg->remainingSizeForRequests();
  ClientRequestMsg *nextRequest = requestsQueueOfPrimary.front();
  while (nextRequest != nullptr &&
         (maxSpaceForReqs - prePrepareMsg->remainingSizeForRequests() < requiredBatchSizeInBytes))
    nextRequest = addRequestToPrePrepareMessage(nextRequest, *prePrepareMsg, maxSpaceForReqs);

  return finishAddingRequestsToPrePrepareMsg(prePrepareMsg, maxSpaceForReqs, requiredBatchSizeInBytes, 0);
}

void ReplicaImp::startConsensusProcess(PrePrepareMsg *pp) {
  static constexpr bool isInternalNoop = false;
  startConsensusProcess(pp, isInternalNoop);
}

void ReplicaImp::startConsensusProcess(PrePrepareMsg *pp, bool isInternalNoop) {
  if (!isCurrentPrimary()) return;
  auto firstPath = pp->firstPath();
  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onSendPrePrepareMessage(pp->numberOfRequests(), requestsQueueOfPrimary.size());
  }
  metric_bft_batch_size_.Get().Set(pp->numberOfRequests());
  primaryLastUsedSeqNum++;
  metric_primary_last_used_seq_num_.Get().Set(primaryLastUsedSeqNum);
  SCOPED_MDC_SEQ_NUM(std::to_string(primaryLastUsedSeqNum));
  SCOPED_MDC_PATH(CommitPathToMDCString(firstPath));

  if (isInternalNoop) {
    LOG_INFO(CNSUS, "Sending PrePrepare message containing internal NOOP");
  } else {
    LOG_INFO(CNSUS,
             "Sending PrePrepare message" << KVLOG(pp->numberOfRequests())
                                          << " correlation ids: " << pp->getBatchCorrelationIdAsString());
    consensus_times_.start(primaryLastUsedSeqNum);
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
  if (primaryLastUsedSeqNum + 1 > lastStableSeqNum + kWorkWindowSize) {
    LOG_DEBUG(CNSUS,
              "Will not send noop PrePrepare since next sequence number ["
                  << primaryLastUsedSeqNum + 1 << "] exceeds window threshold [" << lastStableSeqNum + kWorkWindowSize
                  << "]");
    return;
  }
  PrePrepareMsg *pp = new PrePrepareMsg(
      config_.getreplicaId(), curView, (primaryLastUsedSeqNum + 1), firstPath, sizeof(ClientRequestMsgHeader));
  ClientRequestMsg emptyClientRequest(config_.getreplicaId());
  pp->addRequest(emptyClientRequest.body(), emptyClientRequest.size());
  pp->finishAddingRequests();
  static constexpr bool isInternalNoop = true;
  startConsensusProcess(pp, isInternalNoop);
}

void ReplicaImp::bringTheSystemToCheckpointBySendingNoopCommands(SeqNum seqNumToStopAt, CommitPath firstPath) {
  if (!isCurrentPrimary() || seqNumToStopAt <= primaryLastUsedSeqNum) return;
  LOG_INFO(GL, "Sending noops for [" << primaryLastUsedSeqNum + 1 << " --> " << seqNumToStopAt << " ]");
  while (primaryLastUsedSeqNum < seqNumToStopAt) {
    sendInternalNoopPrePrepareMsg(firstPath);
  }
}

bool ReplicaImp::isSeqNumToStopAt(SeqNum seq_num) {
  if (ControlStateManager::instance().getPruningProcessStatus()) return true;
  auto seq_num_to_stop_at = ControlStateManager::instance().getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value()) {
    if (seq_num_to_stop_at < seq_num) return true;
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
  if (isSeqNumToStopAt(msg->seqNumber())) {
    LOG_INFO(GL,
             "Ignoring PrePrepareMsg because system is stopped at checkpoint pending control state operation (upgrade, "
             "etc...)");
    return;
  }
  metric_received_pre_prepares_.Get().Inc();
  const SeqNum msgSeqNum = msg->seqNumber();

  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_DEBUG(MSGS, KVLOG(msg->senderId(), msg->size()));
  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "handle_bft_preprepare");
  span.setTag("rid", config_.getreplicaId());
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

    // Check to see if this is a noop.
    bool isNoop = false;
    if (msg->numberOfRequests() == 1) {
      auto it = RequestsIterator(msg);
      char *requestBody = nullptr;
      it.getCurrent(requestBody);
      isNoop = (reinterpret_cast<ClientRequestMsgHeader *>(requestBody)->requestLength == 0);
    }

    // For MDC it doesn't matter which type of fast path
    SCOPED_MDC_PATH(CommitPathToMDCString(slowStarted ? CommitPath::SLOW : CommitPath::OPTIMISTIC_FAST));
    if (seqNumInfo.addMsg(msg)) {
      if (isNoop) {
        LOG_INFO(CNSUS, "Internal NOOP PrePrepare received");
      } else {
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

    StartSlowCommitMsg *startSlow = new StartSlowCommitMsg(config_.getreplicaId(), curView, i);

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
    const int16_t searchWindow = 32;  // TODO(GG): TBD - read from configuration
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
    if (!recentViewChange) {
      // during exchange we need missing data to be performed in slow path
      auto exchanging =
          (!KeyExchangeManager::instance().keysExchanged && ReplicaConfig::instance().getkeyExchangeOnStart());
      tryToSendReqMissingDataMsg(i, exchanging);
    } else {
      if (isCurrentPrimary()) {
        tryToSendReqMissingDataMsg(i, true);  // This Replica is Primary, need to ask everyone else
      } else {
        tryToSendReqMissingDataMsg(i, true, currentPrimary());  // Ask the Primary
      }
    }
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
      std::shared_ptr<IThresholdSigner> commitSigner;
      CommitPath commitPath = pp->firstPath();

      ConcordAssertOR((config_.getcVal() != 0), (commitPath != CommitPath::FAST_WITH_THRESHOLD));

      if ((commitPath == CommitPath::FAST_WITH_THRESHOLD) && (config_.getcVal() > 0))
        commitSigner = CryptoManager::instance().thresholdSignerForCommit(seqNum);
      else
        commitSigner = CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum);

      Digest tmpDigest;
      Digest::calcCombination(ppDigest, curView, seqNum, tmpDigest);

      const auto &span_context = pp->spanContext<std::remove_pointer<decltype(pp)>::type>();
      part = new PartialCommitProofMsg(
          config_.getreplicaId(), curView, seqNum, commitPath, tmpDigest, commitSigner, span_context);
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
        if (router != config_.getreplicaId()) {
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

    LOG_DEBUG(MSGS, "Sending PreparePartialMsg. " << KVLOG(pp->seqNumber()));

    const auto &span_context = pp->spanContext<std::remove_pointer<decltype(pp)>::type>();
    PreparePartialMsg *p =
        PreparePartialMsg::create(curView,
                                  pp->seqNumber(),
                                  config_.getreplicaId(),
                                  pp->digestOfRequests(),
                                  CryptoManager::instance().thresholdSignerForSlowPathCommit(pp->seqNumber()),
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

  if (seqNumInfo.committedOrHasCommitPartialFromReplica(config_.getreplicaId())) return;  // not needed

  LOG_DEBUG(CNSUS, "Sending CommitPartialMsg");

  Digest d;
  Digest::digestOfDigest(pp->digestOfRequests(), d);

  auto prepareFullMsg = seqNumInfo.getValidPrepareFullMsg();

  CommitPartialMsg *c =
      CommitPartialMsg::create(curView,
                               s,
                               config_.getreplicaId(),
                               d,
                               CryptoManager::instance().thresholdSignerForSlowPathCommit(s),
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

  LOG_DEBUG(MSGS, KVLOG(msgSender, msg->size()));

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
  pm_->Delay<concord::performance::SlowdownPhase::ConsensusFullCommitMsgProcess>(
      (char *)msg,
      msg->sizeNeededForObjAndMsgInLocalBuffer(),
      std::bind(&IncomingMsgsStorage::pushExternalMsgRaw, &getIncomingMsgsStorage(), _1, _2));

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

      if (msg->senderId() == config_.getreplicaId()) sendToAllOtherReplicas(msg);

      const bool askForMissingInfoAboutCommittedItems =
          (msgSeqNum > lastExecutedSeqNum + config_.getconcurrencyLevel());  // TODO(GG): check/improve this logic

      auto execution_span = concordUtils::startChildSpan("bft_execute_committed_reqs", span);
      metric_total_committed_sn_.Get().Inc();
      pm_->Delay<concord::performance::SlowdownPhase::ConsensusFullCommitMsgProcess>();
      executeNextCommittedRequests(execution_span, msgSeqNum, askForMissingInfoAboutCommittedItems);
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

std::string ReplicaImp::getReplicaLastStableSeqNum() const {
  std::ostringstream oss;
  std::unordered_map<std::string, std::string> result, nested_data;

  nested_data.insert(toPair(getName(lastStableSeqNum), lastStableSeqNum));
  result.insert(
      toPair("Sequence Numbers ", concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));

  oss << concordUtils::kContainerToJson(result);
  return oss.str();
}

std::string ReplicaImp::getReplicaState() const {
  auto primary = getReplicasInfo().primaryOfView(curView);
  std::ostringstream oss;
  std::unordered_map<std::string, std::string> result, nested_data;

  result.insert(toPair("Replica ID", std::to_string(getReplicasInfo().myId())));

  result.insert(toPair("Primary ", std::to_string(primary)));

  nested_data.insert(toPair(getName(viewChangeProtocolEnabled), viewChangeProtocolEnabled));
  nested_data.insert(toPair(getName(autoPrimaryRotationEnabled), autoPrimaryRotationEnabled));
  nested_data.insert(toPair(getName(curView), curView));
  nested_data.insert(toPair(getName(timeOfLastViewEntrance), utcstr(timeOfLastViewEntrance)));
  nested_data.insert(toPair(getName(lastAgreedView), lastAgreedView));
  nested_data.insert(toPair(getName(timeOfLastAgreedView), utcstr(timeOfLastAgreedView)));
  nested_data.insert(toPair(getName(viewChangeTimerMilli), viewChangeTimerMilli));
  nested_data.insert(toPair(getName(autoPrimaryRotationTimerMilli), autoPrimaryRotationTimerMilli));
  result.insert(
      toPair("View Change ", concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));
  nested_data.clear();

  nested_data.insert(toPair(getName(primaryLastUsedSeqNum), primaryLastUsedSeqNum));
  nested_data.insert(toPair(getName(lastStableSeqNum), lastStableSeqNum));
  nested_data.insert(toPair("lastStableCheckpoint", lastStableSeqNum / checkpointWindowSize));
  nested_data.insert(toPair(getName(strictLowerBoundOfSeqNums), strictLowerBoundOfSeqNums));
  nested_data.insert(toPair(getName(maxSeqNumTransferredFromPrevViews), maxSeqNumTransferredFromPrevViews));
  nested_data.insert(toPair(getName(mainLog->currentActiveWindow().first), mainLog->currentActiveWindow().first));
  nested_data.insert(toPair(getName(mainLog->currentActiveWindow().second), mainLog->currentActiveWindow().second));
  nested_data.insert(
      toPair(getName(lastViewThatTransferredSeqNumbersFullyExecuted), lastViewThatTransferredSeqNumbersFullyExecuted));
  result.insert(
      toPair("Sequence Numbers ", concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));
  nested_data.clear();

  nested_data.insert(toPair(getName(restarted_), restarted_));
  nested_data.insert(toPair(getName(requestsQueueOfPrimary.size()), requestsQueueOfPrimary.size()));
  nested_data.insert(toPair(getName(requestsBatcg312her_->getMaxNumberOfPendingRequestsInRecentHistory()),
                            reqBatchingLogic_.getMaxNumberOfPendingRequestsInRecentHistory()));
  nested_data.insert(toPair(getName(reqBatchingLogic_->getBatchingFactor()), reqBatchingLogic_.getBatchingFactor()));
  nested_data.insert(toPair(getName(lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas),
                            utcstr(lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas)));
  nested_data.insert(toPair(getName(timeOfLastStateSynch), utcstr(timeOfLastStateSynch)));
  nested_data.insert(toPair(getName(recoveringFromExecutionOfRequests), recoveringFromExecutionOfRequests));
  nested_data.insert(
      toPair(getName(checkpointsLog->currentActiveWindow().first), checkpointsLog->currentActiveWindow().first));
  nested_data.insert(
      toPair(getName(checkpointsLog->currentActiveWindow().second), checkpointsLog->currentActiveWindow().second));
  nested_data.insert(toPair(getName(clientsManager->numberOfRequiredReservedPages()),
                            clientsManager->numberOfRequiredReservedPages()));
  nested_data.insert(toPair(getName(numInvalidClients), numInvalidClients));
  nested_data.insert(toPair(getName(numValidNoOps), numValidNoOps));
  result.insert(toPair("Other ", concordUtils::kvContainerToJson(nested_data, [](const auto &arg) { return arg; })));

  oss << concordUtils::kContainerToJson(result);
  return oss.str();
}

void ReplicaImp::onInternalMsg(GetStatus &status) const {
  if (status.key == "replica") {  // TODO: change this key name (coordinate with deployment)
    return status.output.set_value(getReplicaLastStableSeqNum());
  }

  if (status.key == "replica-state") {
    return status.output.set_value(getReplicaState());
  }

  if (status.key == "state-transfer") {
    return status.output.set_value(stateTransfer->getStatus());
  }

  if (status.key == "key-exchange") {
    return status.output.set_value(KeyExchangeManager::instance().getStatus());
  }

  if (status.key == "pre-execution") {
    return status.output.set_value(replStatusHandlers_.preExecutionStatus(getAggregator()));
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

    LOG_DEBUG(MSGS, "Received relevant PreparePartialMsg." << KVLOG(msgSender));

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
    LOG_DEBUG(MSGS,
              "Node " << config_.getreplicaId() << " ignored the PreparePartialMsg from node " << msgSender
                      << " (seqNumber " << msgSeqNum << ")");
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

    LOG_DEBUG(MSGS, "Received CommitPartialMsg from node " << msgSender);

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

    LOG_DEBUG(MSGS, "received PrepareFullMsg");

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
    LOG_DEBUG(MSGS, "Ignored PrepareFullMsg." << KVLOG(msgSender));
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

    LOG_DEBUG(MSGS, "Received CommitFullMsg" << KVLOG(msgSender));

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
    LOG_DEBUG(MSGS,
              "Node " << config_.getreplicaId() << " ignored the CommitFullMsg from node " << msgSender
                      << " (seqNumber " << msgSeqNum << ")");
    delete msg;
  }
}

void ReplicaImp::onPrepareCombinedSigFailed(SeqNum seqNumber,
                                            ViewNum view,
                                            const std::set<uint16_t> &replicasWithBadSigs) {
  LOG_WARN(THRESHSIGN_LOG, KVLOG(seqNumber, view));
  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetPrepareSignatures();
    LOG_INFO(GL, "Collecting state, reset prepare signatures");
    return;
  }
  if ((!currentViewIsActive()) || (curView != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(GL, "Dropping irrelevant signature." << KVLOG(seqNumber, view));

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
  LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, combinedSigLen));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetPrepareSignatures();
    LOG_INFO(GL, "Collecting state, reset prepare signatures");
    return;
  }
  if ((!currentViewIsActive()) || (curView != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(GL, "Not sending prepare full: Invalid view, or sequence number." << KVLOG(view, curView));
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
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));

  LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, isValid));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetPrepareSignatures();
    LOG_INFO(GL, "Collecting state, reset prepare signatures");
    return;
  }

  if ((!currentViewIsActive()) || (curView != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(GL,
             "Not sending commit partial: Invalid view, or sequence number."
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
  LOG_WARN(THRESHSIGN_LOG, KVLOG(seqNumber, view, replicasWithBadSigs.size()));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetCommitSignatres();
    LOG_INFO(GL, "Collecting state, reset commit signatures");
    return;
  }

  if ((!currentViewIsActive()) || (curView != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_DEBUG(GL, "Invalid view, or sequence number." << KVLOG(seqNumber, view, curView));
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
  LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, combinedSigLen));

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetCommitSignatres();
    LOG_INFO(GL, "Collecting state, reset commit signatures");
    return;
  }

  if ((!currentViewIsActive()) || (curView != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(GL,
             "Not sending full commit: Invalid view, or sequence number."
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

  bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum + config_.getconcurrencyLevel());

  auto span = concordUtils::startChildSpanFromContext(
      commitFull->spanContext<std::remove_pointer<decltype(commitFull)>::type>(), "bft_execute_committed_reqs");
  metric_total_committed_sn_.Get().Inc();
  executeNextCommittedRequests(span, seqNumber, askForMissingInfoAboutCommittedItems);
}

void ReplicaImp::onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum view, bool isValid) {
  SCOPED_MDC_PRIMARY(std::to_string(currentPrimary()));
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNumber));
  SCOPED_MDC_PATH(CommitPathToMDCString(CommitPath::SLOW));

  if (!isValid) {
    LOG_WARN(THRESHSIGN_LOG, KVLOG(seqNumber, view, isValid));
  } else {
    LOG_TRACE(THRESHSIGN_LOG, KVLOG(seqNumber, view, isValid));
  }

  if (isCollectingState() && mainLog->insideActiveWindow(seqNumber)) {
    mainLog->get(seqNumber).resetCommitSignatres();
    LOG_INFO(GL, "Collecting state, reset commit signatures");
    return;
  }

  if ((!currentViewIsActive()) || (curView != view) || (!mainLog->insideActiveWindow(seqNumber))) {
    LOG_INFO(GL, "Invalid view, or sequence number." << KVLOG(view, curView, mainLog->insideActiveWindow(seqNumber)));
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
  bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum + config_.getconcurrencyLevel());
  metric_total_committed_sn_.Get().Inc();
  executeNextCommittedRequests(span, seqNumber, askForMissingInfoAboutCommittedItems);
}

template <>
void ReplicaImp::onMessage<CheckpointMsg>(CheckpointMsg *msg) {
  metric_received_checkpoints_.Get().Inc();
  const ReplicaId msgSenderId = msg->senderId();
  const SeqNum msgSeqNum = msg->seqNumber();
  const Digest msgDigest = msg->digestOfState();
  const bool msgIsStable = msg->isStableState();
  SCOPED_MDC_SEQ_NUM(std::to_string(msgSeqNum));
  LOG_INFO(
      GL,
      "Received checkpoint message from node. " << KVLOG(msgSenderId, msgSeqNum, msg->size(), msgIsStable, msgDigest));
  LOG_INFO(GL, "My " << KVLOG(lastStableSeqNum, lastExecutedSeqNum));
  auto span = concordUtils::startChildSpanFromContext(msg->spanContext<std::remove_pointer<decltype(msg)>::type>(),
                                                      "bft_handle_checkpoint_msg");

  if ((msgSeqNum > lastStableSeqNum) && (msgSeqNum <= lastStableSeqNum + kWorkWindowSize)) {
    ConcordAssert(mainLog->insideActiveWindow(msgSeqNum));
    CheckpointInfo &checkInfo = checkpointsLog->get(msgSeqNum);
    bool msgAdded = checkInfo.addCheckpointMsg(msg, msg->senderId());

    if (msgAdded) {
      LOG_DEBUG(GL, "Added checkpoint message: " << KVLOG(msgSenderId));
    }

    if (checkInfo.isCheckpointCertificateComplete()) {  // 2f + 1
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
    if (pos == tableOfStableCheckpoints.end() || pos->second->seqNumber() <= msgSeqNum) {
      // <= to allow repeating checkpoint message since state transfer may not kick in when we are inside active window
      if (pos != tableOfStableCheckpoints.end()) delete pos->second;
      CheckpointMsg *x = new CheckpointMsg(msgSenderId, msgSeqNum, msgDigest, msgIsStable);
      tableOfStableCheckpoints[msgSenderId] = x;
      LOG_INFO(GL, "Added stable Checkpoint message to tableOfStableCheckpoints: " << KVLOG(msgSenderId));
      for (auto &[r, cp] : tableOfStableCheckpoints) {
        if (cp->seqNumber() == msgSeqNum && cp->digestOfState() != x->digestOfState()) {
          metric_indicator_of_non_determinism_.Get().Inc();
          LOG_ERROR(GL,
                    "Detect non determinism, for checkpoint: "
                        << msgSeqNum << " [replica: " << r << ", digest: " << cp->digestOfState()
                        << "] Vs [replica: " << msgSenderId << ", digest: " << x->digestOfState() << "]");
        }
      }
      if ((uint16_t)tableOfStableCheckpoints.size() >= config_.getfVal() + 1) {
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

        if (numRelevantAboveWindow >= config_.getfVal() + 1) {
          LOG_INFO(GL, "Number of stable checkpoints above window: " << numRelevantAboveWindow);
          askForStateTransfer = true;
        } else if (numRelevant >= config_.getfVal() + 1) {
          static uint32_t maxTimeSinceLastExecutionInMainWindowMs =
              config_.get<uint32_t>("concord.bft.st.maxTimeSinceLastExecutionInMainWindowMs", 5000);

          Time timeOfLastEcecution = MinTime;
          if (mainLog->insideActiveWindow(lastExecutedSeqNum))
            timeOfLastEcecution = mainLog->get(lastExecutedSeqNum).lastUpdateTimeOfCommitMsgs();
          if ((getMonotonicTime() - timeOfLastEcecution) > (milliseconds(maxTimeSinceLastExecutionInMainWindowMs))) {
            LOG_INFO(GL,
                     "Number of stable checkpoints in current window: "
                         << numRelevant << " time since last execution: "
                         << (getMonotonicTime() - timeOfLastEcecution).count() << " ms");
            askForStateTransfer = true;
          }
        }
      }
    }
  }

  if (askForStateTransfer && !stateTransfer->isCollectingState()) {
    LOG_INFO(GL, "Call to startCollectingState()");
    time_in_state_transfer_.start();
    clientsManager->clearAllPendingRequests();  // to avoid entering a new view on old request timeout
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

  // DD: handlers are supposed to either save or delete messages
  std::unique_ptr<AskForCheckpointMsg> m{msg};
  LOG_INFO(GL, "Received AskForCheckpoint message: " << KVLOG(m->senderId(), lastStableSeqNum));

  const CheckpointInfo &checkpointInfo = checkpointsLog->get(lastStableSeqNum);
  CheckpointMsg *checkpointMsg = checkpointInfo.selfCheckpointMsg();

  if (checkpointMsg == nullptr) {
    LOG_INFO(GL, "This replica does not have the current checkpoint. " << KVLOG(m->senderId(), lastStableSeqNum));
  } else {
    // TODO [TK] check if already sent within a configurable time period
    auto destination = m->senderId();
    LOG_INFO(GL, "Sending CheckpointMsg: " << KVLOG(destination));
    send(checkpointMsg, m->senderId());
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

  if (handledByRetransmissionsManager(sourceNode, config_.getreplicaId(), currentPrimary(), seqNum, msg->type())) {
    SimpleAckMsg *ackMsg = new SimpleAckMsg(seqNum, curView, config_.getreplicaId(), msg->type());

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

  if (handledByRetransmissionsManager(config_.getreplicaId(), destReplica, currentPrimary(), s, msg->type()))
    retransmissionsManager->onSend(destReplica, s, msg->type(), ignorePreviousAcks);
}

void ReplicaImp::onRetransmissionsTimer(Timers::Handle timer) {
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  ConcordAssert(retransmissionsLogicEnabled);

  retransmissionsManager->tryToStartProcessing();
}

void ReplicaImp::onRetransmissionsProcessingResults(SeqNum relatedLastStableSeqNum,
                                                    const ViewNum relatedViewNumber,
                                                    const std::forward_list<RetSuggestion> &suggestedRetransmissions) {
  ConcordAssert(retransmissionsLogicEnabled);

  if (isCollectingState() || (relatedViewNumber != curView) || (!currentViewIsActive())) return;
  if (relatedLastStableSeqNum + kWorkWindowSize <= lastStableSeqNum) return;

  const uint16_t myId = config_.getreplicaId();
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
        LOG_DEBUG(MSGS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId << " PrePrepareMsg with seqNumber "
                             << s.msgSeqNum);
      } break;
      case MsgCode::PartialCommitProof: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PartialCommitProofMsg *msgToSend = seqNumInfo.partialProofs().getSelfPartialCommitProof();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(MSGS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId
                             << " PartialCommitProofMsg with seqNumber " << s.msgSeqNum);
      } break;
        /*  TODO(GG): do we want to use acks for FullCommitProofMsg ?
         */
      case MsgCode::StartSlowCommit: {
        StartSlowCommitMsg *msgToSend = new StartSlowCommitMsg(myId, curView, s.msgSeqNum);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        delete msgToSend;
        LOG_DEBUG(MSGS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId
                             << " StartSlowCommitMsg with seqNumber " << s.msgSeqNum);
      } break;
      case MsgCode::PreparePartial: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PreparePartialMsg *msgToSend = seqNumInfo.getSelfPreparePartialMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(MSGS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId
                             << " PreparePartialMsg with seqNumber " << s.msgSeqNum);
      } break;
      case MsgCode::PrepareFull: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        PrepareFullMsg *msgToSend = seqNumInfo.getValidPrepareFullMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(MSGS,
                  "Replica " << myId << " retransmits to replica " << s.replicaId << " PrepareFullMsg with seqNumber "
                             << s.msgSeqNum);
      } break;

      case MsgCode::CommitPartial: {
        SeqNumInfo &seqNumInfo = mainLog->get(s.msgSeqNum);
        CommitPartialMsg *msgToSend = seqNumInfo.getSelfCommitPartialMsg();
        ConcordAssertNE(msgToSend, nullptr);
        sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
        LOG_DEBUG(MSGS,
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
  if (msgLastStable % checkpointWindowSize != 0) {
    LOG_ERROR(MSGS,
              "ERROR detected in peer msgSenderId = "
                  << msgSenderId << ". Reported Last Stable Sequence not consistent with checkpointWindowSize "
                  << KVLOG(msgLastStable, checkpointWindowSize));
    return;
  }

  LOG_DEBUG(MSGS, KVLOG(msgSenderId, msgLastStable, msgViewNum, lastStableSeqNum));

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

  } else if (msgLastStable > lastStableSeqNum + kWorkWindowSize) {
    tryToSendStatusReport();  // ask for help
  } else {
    if (lastStableSeqNum != 0 && msgLastStable == lastStableSeqNum) {
      // Maybe the sender needs to get to an n/n checkpoint
      CheckpointMsg *checkMsg = checkpointsLog->get(msgLastStable).selfCheckpointMsg();
      if (checkMsg != nullptr) {
        sendAndIncrementMetric(checkMsg, msgSenderId, metric_sent_checkpoint_msg_due_to_status_);
      }
    }
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
    auto sendViewChangeMsgs = [&msg, &msgSenderId, this]() {
      // Send all View Change messages we have. We only have ViewChangeMsg for View > 0
      if (curView > 0 && msg->hasListOfMissingViewChangeMsgForViewChange()) {
        for (auto *vcMsg : viewsManager->getViewChangeMsgsForView(curView)) {
          if (msg->isMissingViewChangeMsgForViewChange(vcMsg->idOfGeneratedReplica())) {
            sendAndIncrementMetric(vcMsg, msgSenderId, metric_sent_viewchange_msg_due_to_status_);
          }
        }
      }
    };

    if (isCurrentPrimary() || (repsInfo->primaryOfView(curView) == msgSenderId))  // if the primary is involved
    {
      if (isCurrentPrimary())  // I am the primary of curView
      {
        // send NewViewMsg for View > 0
        if (curView > 0 && !msg->currentViewHasNewViewMessage() && viewsManager->viewIsActive(curView)) {
          NewViewMsg *nv = viewsManager->getMyNewViewMsgForCurrentView();
          ConcordAssertNE(nv, nullptr);
          sendAndIncrementMetric(nv, msgSenderId, metric_sent_newview_msg_due_to_status_);
        }
      }
      // send all VC msgs that can help making progress (needed because the original senders may not send
      // the ViewChangeMsg msgs used by the primary)
      // if viewsManager->viewIsActive(curView), we can send only the VC msgs which are really needed for
      // curView (see in ViewsManager)
      sendViewChangeMsgs();

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
      sendViewChangeMsgs();
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

  /////////////////////////////////////////////////////////////////////////
  // We are in the same View as msgSenderId, send complaints he is missing
  /////////////////////////////////////////////////////////////////////////

  if (msg->getViewNumber() == getCurrentView()) {
    for (const auto &i : complainedReplicas.getAllMsgs()) {
      if (!msg->hasComplaintFromReplica(i.first)) {
        sendAndIncrementMetric(i.second.get(), msgSenderId, metric_sent_replica_asks_to_leave_view_msg_due_to_status_);
      }
    }
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

  ReplicaStatusMsg msg(config_.getreplicaId(),
                       curView,
                       lastStableSeqNum,
                       lastExecutedSeqNum,
                       viewIsActive,
                       hasNewChangeMsg,
                       listOfPPInActiveWindow,
                       listOfMissingVCMsg,
                       listOfMissingPPMsg);

  for (const auto &i : complainedReplicas.getAllMsgs()) {
    msg.setComplaintFromReplica(i.first);
  }
  if (listOfPPInActiveWindow) {
    const SeqNum start = lastStableSeqNum + 1;
    const SeqNum end = lastStableSeqNum + kWorkWindowSize;

    for (SeqNum i = start; i <= end; i++) {
      if (mainLog->get(i).hasPrePrepareMsg()) msg.setPrePrepareInActiveWindow(i);
    }
  }
  if (listOfMissingVCMsg) {
    for (ReplicaId i : repsInfo->idsOfPeerReplicas()) {
      if (!viewsManager->hasViewChangeMessageForFutureView(i, curView)) msg.setMissingViewChangeMsgForViewChange(i);
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
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  if (!viewChangeProtocolEnabled) {
    delete msg;
    return;
  }
  metric_received_view_changes_.Get().Inc();

  const ReplicaId generatedReplicaId =
      msg->idOfGeneratedReplica();  // Notice that generatedReplicaId may be != msg->senderId()
  ConcordAssertNE(generatedReplicaId, config_.getreplicaId());

  bool msgAdded = viewsManager->add(msg);

  if (!msgAdded) return;

  LOG_INFO(VC_LOG, KVLOG(generatedReplicaId, msg->newView(), msg->lastStable(), msg->numberOfElements(), msgAdded));

  ViewChangeMsg::ComplaintsIterator iter(msg);
  char *complaint = nullptr;
  MsgSize size = 0;
  ReplicasAskedToLeaveViewInfo complainedReplicasForHigherView(config_);

  while (msg->newView() > curView && !complainedReplicasForHigherView.hasQuorumToLeaveView() &&
         iter.getAndGoToNext(complaint, size)) {
    auto baseMsg = MessageBase(msg->senderId(), (MessageBase::Header *)complaint, size, true);
    auto complaintMsg = std::make_unique<ReplicaAsksToLeaveViewMsg>(&baseMsg);
    LOG_INFO(VC_LOG,
             "Got complaint in ViewChangeMsg" << KVLOG(curView,
                                                       msg->senderId(),
                                                       msg->newView(),
                                                       msg->idOfGeneratedReplica(),
                                                       complaintMsg->senderId(),
                                                       complaintMsg->viewNumber(),
                                                       complaintMsg->idOfGeneratedReplica()));
    if (msg->newView() == curView + 1) {
      if (complainedReplicas.getComplaintFromReplica(complaintMsg->idOfGeneratedReplica()) != nullptr) {
        LOG_INFO(VC_LOG,
                 "Already have a valid complaint from Replica " << complaintMsg->idOfGeneratedReplica() << " for View "
                                                                << complaintMsg->viewNumber());
      } else if (complaintMsg->viewNumber() == curView && validateMessage(complaintMsg.get())) {
        onMessage<ReplicaAsksToLeaveViewMsg>(complaintMsg.release());
      } else {
        LOG_WARN(VC_LOG, "Invalid complaint in ViewChangeMsg for current View.");
      }
    } else {
      if (complaintMsg->viewNumber() + 1 == msg->newView() && validateMessage(complaintMsg.get())) {
        complainedReplicasForHigherView.store(std::move(complaintMsg));
      } else {
        LOG_WARN(VC_LOG, "Invalid complaint in ViewChangeMsg for a higher View.");
      }
    }
  }
  if (complainedReplicasForHigherView.hasQuorumToLeaveView()) {
    ConcordAssert(msg->newView() > curView + 1);
    complainedReplicas = std::move(complainedReplicasForHigherView);
    LOG_INFO(VC_LOG,
             "Got quorum of Replicas complaining for a higher View in VCMsg: " << KVLOG(msg->newView(), curView));
    MoveToHigherView(msg->newView());
  }

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
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  if (!viewChangeProtocolEnabled) {
    delete msg;
    return;
  }
  metric_received_new_views_.Get().Inc();

  const ReplicaId senderId = msg->senderId();

  ConcordAssertNE(senderId, config_.getreplicaId());  // should be verified in ViewChangeMsg

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

  LOG_INFO(VC_LOG, "Moving to higher view: " << KVLOG(curView, nextView, wasInPrevViewNumber));

  ViewChangeMsg *pVC = nullptr;

  if (!wasInPrevViewNumber) {
    pVC = viewsManager->getMyLatestViewChangeMsg();
    ConcordAssertNE(pVC, nullptr);
    pVC->setNewViewNumber(nextView);
    time_in_active_view_.end();
    pVC->clearAllComplaints();
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

  for (const auto &i : complainedReplicas.getAllMsgs()) {
    pVC->addComplaint(i.second.get());
    const auto &complaint = i.second;
    LOG_DEBUG(VC_LOG,
              "Putting complaint in VC msg: " << KVLOG(
                  curView, nextView, complaint->idOfGeneratedReplica(), complaint->viewNumber()));
  }
  complainedReplicas.clear();

  curView = nextView;
  metric_view_.Get().Set(nextView);
  metric_current_primary_.Get().Set(curView % config_.getnumReplicas());

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
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  SeqNum firstPPSeq = 0;
  SeqNum lastPPSeq = 0;

  time_in_active_view_.start();
  consensus_times_.clear();

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

  if (repsInfo->primaryOfView(curView) == config_.getreplicaId()) {
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
      if (viewChangeMsgsForCurrentView[i]->idOfGeneratedReplica() == config_.getreplicaId()) myVCWasUsed = true;
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
      ConcordAssert(newViewMsgForCurrentView->includesViewChangeFromReplica(config_.getreplicaId(), d));
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

  const bool primaryIsMe = (config_.getreplicaId() == repsInfo->primaryOfView(curView));

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
  primary_queue_size_.Get().Set(requestsQueueOfPrimary.size());

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
  metric_sent_replica_asks_to_leave_view_msg_.Get().Set(0);
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

void ReplicaImp::onTransferringCompleteImp(uint64_t newStateCheckpoint) {
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
  TimeRecorder scoped_timer(*histograms_.onTransferringCompleteImp);
  time_in_state_transfer_.end();
  LOG_INFO(GL, KVLOG(newStateCheckpoint));

  if (ps_) {
    ps_->beginWriteTran();
  }
  SeqNum newCheckpointSeqNum = newStateCheckpoint * checkpointWindowSize;
  if (newCheckpointSeqNum <= lastExecutedSeqNum) {
    LOG_DEBUG(GL,
              "Executing onTransferringCompleteImp(newStateCheckpoint) where newStateCheckpoint <= lastExecutedSeqNum");
    if (ps_) ps_->endWriteTran();
    return;
  }
  lastExecutedSeqNum = newCheckpointSeqNum;
  if (ps_) ps_->setLastExecutedSeqNum(lastExecutedSeqNum);
  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
  }
  bool askAnotherStateTransfer = false;

  timeOfLastStateSynch = getMonotonicTime();  // TODO(GG): handle restart/pause

  clientsManager->loadInfoFromReservedPages();

  if (ReplicaConfig::instance().getkeyExchangeOnStart()) {
    KeyExchangeManager::instance().loadKeysFromReservedPages();
  }

  if (newCheckpointSeqNum > lastStableSeqNum + kWorkWindowSize) {
    const SeqNum refPoint = newCheckpointSeqNum - kWorkWindowSize;
    const bool withRefCheckpoint = (checkpointsLog->insideActiveWindow(refPoint) &&
                                    (checkpointsLog->get(refPoint).selfCheckpointMsg() != nullptr));

    onSeqNumIsStable(refPoint, withRefCheckpoint, true);
  }

  ConcordAssert(checkpointsLog->insideActiveWindow(newCheckpointSeqNum));

  // create and send my checkpoint
  Digest digestOfNewState;
  stateTransfer->getDigestOfCheckpoint(newStateCheckpoint, sizeof(Digest), (char *)&digestOfNewState);
  CheckpointMsg *checkpointMsg =
      new CheckpointMsg(config_.getreplicaId(), newCheckpointSeqNum, digestOfNewState, false);
  CheckpointInfo &checkpointInfo = checkpointsLog->get(newCheckpointSeqNum);
  checkpointInfo.addCheckpointMsg(checkpointMsg, config_.getreplicaId());
  checkpointInfo.setCheckpointSentAllOrApproved();

  if (newCheckpointSeqNum > primaryLastUsedSeqNum) primaryLastUsedSeqNum = newCheckpointSeqNum;

  if (checkpointInfo.isCheckpointSuperStable()) {
    onSeqNumIsSuperStable(newCheckpointSeqNum);
  }
  if (ps_) {
    ps_->setPrimaryLastUsedSeqNum(primaryLastUsedSeqNum);
    ps_->setCheckpointMsgInCheckWindow(newCheckpointSeqNum, checkpointMsg);
    ps_->endWriteTran();
  }
  metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);

  sendToAllOtherReplicas(checkpointMsg);

  if ((uint16_t)tableOfStableCheckpoints.size() >= config_.getfVal() + 1) {
    uint16_t numOfStableCheckpoints = 0;
    auto tableItrator = tableOfStableCheckpoints.begin();
    while (tableItrator != tableOfStableCheckpoints.end()) {
      if (tableItrator->second->seqNumber() >= newCheckpointSeqNum) numOfStableCheckpoints++;

      if (tableItrator->second->seqNumber() <= lastExecutedSeqNum) {
        delete tableItrator->second;
        tableItrator = tableOfStableCheckpoints.erase(tableItrator);
      } else {
        tableItrator++;
      }
    }
    if (numOfStableCheckpoints >= config_.getfVal() + 1) onSeqNumIsStable(newCheckpointSeqNum);

    if ((uint16_t)tableOfStableCheckpoints.size() >= config_.getfVal() + 1) askAnotherStateTransfer = true;
  }

  if (askAnotherStateTransfer) {
    LOG_INFO(GL, "Call to another startCollectingState()");
    clientsManager->clearAllPendingRequests();  // to avoid entering a new view on old request timeout
    stateTransfer->startCollectingState();
  } else {
    if (!currentViewIsActive()) {
      LOG_INFO(GL, "tryToEnterView after State Transfer finished ...");
      tryToEnterView();
    }
  }
}

void ReplicaImp::onSeqNumIsSuperStable(SeqNum superStableSeqNum) {
  auto seq_num_to_stop_at = ControlStateManager::instance().getCheckpointToStopAt();
  if (seq_num_to_stop_at.has_value() && seq_num_to_stop_at.value() == superStableSeqNum) {
    LOG_INFO(GL, "Informing control state manager that consensus should be stopped: " << KVLOG(superStableSeqNum));

    metric_on_call_back_of_super_stable_cp_.Get().Set(1);
    // TODO: With state transfered replica, this might be called twice. Consider to clean the reserved page at the
    // end of this method
    IControlHandler::instance()->onSuperStableCheckpoint();
    // Now, we know that the inmemory var in controlStateManager_ is set to the correct point.
    // We want that when the replicas resume, they won't have the wedge point in their reserved pages (otherwise they
    // will simply try to wedge again). Yet, as the reserved pages are transferred via ST we can cleat this data only in
    // an n/n checkpoint that is about to be wedged because we know that there is no ST going on now.
    ControlStateManager::instance().clearCheckpointToStopAt();
  }
}

void ReplicaImp::onSeqNumIsStable(SeqNum newStableSeqNum, bool hasStateInformation, bool oldSeqNum) {
  ConcordAssertOR(hasStateInformation, oldSeqNum);  // !hasStateInformation ==> oldSeqNum
  ConcordAssertEQ(newStableSeqNum % checkpointWindowSize, 0);

  if (newStableSeqNum <= lastStableSeqNum) return;
  checkpoint_times_.end(newStableSeqNum);
  TimeRecorder scoped_timer(*histograms_.onSeqNumIsStable);

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
      if (config_.getdebugStatisticsEnabled()) {
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
      checkpointMsg = new CheckpointMsg(config_.getreplicaId(), lastStableSeqNum, digestOfState, true);
      checkpointInfo.addCheckpointMsg(checkpointMsg, config_.getreplicaId());
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

  if (!oldSeqNum && currentViewIsActive() && (currentPrimary() == config_.getreplicaId()) && !isCollectingState()) {
    tryToSendPrePrepareMsg();
  }

  auto seq_num_to_stop_at = ControlStateManager::instance().getCheckpointToStopAt();

  // Below we handle the case of removing a node. Note that when removing nodes we cannot assume we will have n/n
  // checkpoint because some of the replicas may not be responsive. For that we also mark that we got to a stable (n-f)
  // checkpoint.
  // Note: Currently this is not sage to rely on this n-f wedged checkpoint on any other reconfiguration action except
  // of removing nodes.
  // TODO (YB): we need to improve this mechanism so that it will be supported by many reconfiguration actions as
  // possible.
  if (seq_num_to_stop_at.has_value() && seq_num_to_stop_at.value() == newStableSeqNum) {
    LOG_INFO(GL,
             "Informing control state manager that consensus should be stopped (without n/n replicas): " << KVLOG(
                 newStableSeqNum, metric_last_stable_seq_num_.Get().Get()));
    IControlHandler::instance()->onStableCheckpoint();
    // Mark the metadata storage for deletion if we need to
    auto seq_num_to_remove_metadata_storage = ControlStateManager::instance().getEraseMetadataFlag();
    // We would want to set this flag only when we sure that the replica needs to remove the metadata.
    if (seq_num_to_remove_metadata_storage.has_value() &&
        seq_num_to_remove_metadata_storage.value() == newStableSeqNum) {
      LOG_INFO(GL, "informing metadata storage to clean the data before shutting down (without n/n replicas)");
      if (ps_) ps_->setEraseMetadataStorageFlag();
      stateTransfer->setEraseMetadataFlag();
    }
  }

  // If we want to wedge the system and we got to a stable checkpoint we need to check if we need to send more noops in
  // order to bring the system to the wedge point
  if (isCurrentPrimary() && seq_num_to_stop_at.has_value() && seq_num_to_stop_at.value() > newStableSeqNum &&
      primaryLastUsedSeqNum < seq_num_to_stop_at.value()) {
    bringTheSystemToCheckpointBySendingNoopCommands(seq_num_to_stop_at.value());
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

    if (t != MinTime && (t < curTime)) {
      auto diffMilli = duration_cast<milliseconds>(curTime - t);
      if (diffMilli.count() < dynamicUpperLimitOfRounds->upperLimit() / 4)  // TODO(GG): config
        return;
    }
  }

  seqNumInfo.setTimeOfLastInfoRequest(curTime);

  LOG_INFO(GL, "Try to request missing data. " << KVLOG(seqNumber, curView));

  ReqMissingDataMsg reqData(config_.getreplicaId(), curView, seqNumber);

  const bool routerForPartialProofs = repsInfo->isCollectorForPartialProofs(curView, seqNumber);

  const bool routerForPartialPrepare = (currentPrimary() == config_.getreplicaId());

  const bool routerForPartialCommit = (currentPrimary() == config_.getreplicaId());

  const bool missingPrePrepare = (seqNumInfo.getPrePrepareMsg() == nullptr);
  const bool missingBigRequests = (!missingPrePrepare) && (!seqNumInfo.hasPrePrepareMsg());

  ReplicaId firstRepId = 0;
  ReplicaId lastRepId = config_.getnumReplicas() - 1;
  if (destReplicaId != ALL_OTHER_REPLICAS) {
    firstRepId = destReplicaId;
    lastRepId = destReplicaId;
  }

  for (ReplicaId destRep = firstRepId; destRep <= lastRepId; destRep++) {
    if (destRep == config_.getreplicaId()) continue;  // don't send to myself

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
    LOG_INFO(GL,
             "Send ReqMissingDataMsg. " << KVLOG(
                 destinationReplica, seqNumber, reqData.getFlags(), reqData.getFlagsAsBits()));

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
  LOG_INFO(GL, "Received ReqMissingDataMsg. " << KVLOG(msgSender, msg->getFlags(), msg->getFlagsAsBits()));
  if ((currentViewIsActive()) && (mainLog->insideActiveWindow(msgSeqNum) || mainLog->isPressentInHistory(msgSeqNum))) {
    SeqNumInfo &seqNumInfo = mainLog->getFromActiveWindowOrHistory(msgSeqNum);

    if (config_.getreplicaId() == currentPrimary()) {
      PrePrepareMsg *pp = seqNumInfo.getSelfPrePrepareMsg();
      if (msg->getPrePrepareIsMissing()) {
        if (pp != nullptr) {
          sendAndIncrementMetric(pp, msgSender, metric_sent_preprepare_msg_due_to_reqMissingData_);
        }
      }

      if (seqNumInfo.slowPathStarted() && !msg->getSlowPathHasStarted()) {
        StartSlowCommitMsg startSlowMsg(config_.getreplicaId(), curView, msgSeqNum);
        sendAndIncrementMetric(&startSlowMsg, msgSender, metric_sent_startSlowPath_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getFullCommitIsMissing()) {
      CommitFullMsg *c = seqNumInfo.getValidCommitFullMsg();
      if (c != nullptr) {
        LOG_DEBUG(GL, "sending FullCommit message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
        sendAndIncrementMetric(c, msgSender, metric_sent_commitFull_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getFullCommitProofIsMissing() && seqNumInfo.partialProofs().hasFullProof()) {
      FullCommitProofMsg *fcp = seqNumInfo.partialProofs().getFullProof();
      LOG_DEBUG(GL, "sending FullCommitProof message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
      sendAndIncrementMetric(fcp, msgSender, metric_sent_fullCommitProof_msg_due_to_reqMissingData_);
    }

    if (msg->getPartialProofIsMissing()) {
      // TODO(GG): consider not to send if msgSender is not a collector
      PartialCommitProofMsg *pcf = seqNumInfo.partialProofs().getSelfPartialCommitProof();

      if (pcf != nullptr) {
        LOG_DEBUG(GL, "sending PartialProof message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
        sendAndIncrementMetric(pcf, msgSender, metric_sent_partialCommitProof_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getPartialPrepareIsMissing() && (currentPrimary() == msgSender)) {
      PreparePartialMsg *pr = seqNumInfo.getSelfPreparePartialMsg();

      if (pr != nullptr) {
        LOG_DEBUG(GL, "sending PartialPrepare message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
        sendAndIncrementMetric(pr, msgSender, metric_sent_preparePartial_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getFullPrepareIsMissing()) {
      PrepareFullMsg *pf = seqNumInfo.getValidPrepareFullMsg();

      if (pf != nullptr) {
        LOG_DEBUG(GL, "sending FullPrepare message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
        sendAndIncrementMetric(pf, msgSender, metric_sent_prepareFull_msg_due_to_reqMissingData_);
      }
    }

    if (msg->getPartialCommitIsMissing() && (currentPrimary() == msgSender)) {
      CommitPartialMsg *c = seqNumInfo.getSelfCommitPartialMsg();
      if (c != nullptr) {
        LOG_DEBUG(GL, "sending PartialCommit message as a response of RFMD" << KVLOG(msgSender, msgSeqNum));
        sendAndIncrementMetric(c, msgSender, metric_sent_commitPartial_msg_due_to_reqMissingData_);
      }
    }
  } else {
    LOG_INFO(GL,
             "Ignore the ReqMissingDataMsg message. " << KVLOG(msgSender,
                                                               currentViewIsActive(),
                                                               mainLog->insideActiveWindow(msgSeqNum),
                                                               mainLog->isPressentInHistory(msgSeqNum)));
  }

  delete msg;
}

void ReplicaImp::onViewsChangeTimer(Timers::Handle timer)  // TODO(GG): review/update logic
{
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  SCOPED_MDC_SEQ_NUM(std::to_string(getCurrentView()));
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
    if (isCurrentPrimary() || complainedReplicas.getComplaintFromReplica(config_.replicaId) != nullptr) return;

    std::string cidOfEarliestPendingRequest = std::string();
    const Time timeOfEarliestPendingRequest = clientsManager->infoOfEarliestPendingRequest(cidOfEarliestPendingRequest);

    const bool hasPendingRequest = (timeOfEarliestPendingRequest != MaxTime);

    if (!hasPendingRequest) return;

    const uint64_t diffMilli1 = duration_cast<milliseconds>(currTime - timeOfLastStateSynch).count();
    const uint64_t diffMilli2 = duration_cast<milliseconds>(currTime - timeOfLastViewEntrance).count();
    const uint64_t diffMilli3 = duration_cast<milliseconds>(currTime - timeOfEarliestPendingRequest).count();

    if ((diffMilli1 > viewChangeTimeout) && (diffMilli2 > viewChangeTimeout) && (diffMilli3 > viewChangeTimeout)) {
      LOG_INFO(
          VC_LOG,
          "Ask to leave view=" << curView << " (" << diffMilli3 << " ms after the earliest pending client request)."
                               << KVLOG(cidOfEarliestPendingRequest, lastViewThatTransferredSeqNumbersFullyExecuted));

      std::unique_ptr<ReplicaAsksToLeaveViewMsg> askToLeaveView(ReplicaAsksToLeaveViewMsg::create(
          config_.getreplicaId(), curView, ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout));
      sendToAllOtherReplicas(askToLeaveView.get());
      complainedReplicas.store(std::move(askToLeaveView));
      metric_sent_replica_asks_to_leave_view_msg_.Get().Inc();

      tryToGotoNextView();
      return;
    }
  } else  // not currentViewIsActive()
  {
    if (lastAgreedView != curView) return;
    if (repsInfo->primaryOfView(lastAgreedView) == config_.getreplicaId()) return;

    currTime = getMonotonicTime();
    const uint64_t timeSinceLastStateTransferMilli =
        duration_cast<milliseconds>(currTime - timeOfLastStateSynch).count();
    const uint64_t timeSinceLastAgreedViewMilli = duration_cast<milliseconds>(currTime - timeOfLastAgreedView).count();

    if ((timeSinceLastStateTransferMilli > viewChangeTimeout) && (timeSinceLastAgreedViewMilli > viewChangeTimeout)) {
      LOG_INFO(VC_LOG,
               "Unable to activate the last agreed view (despite receiving 2f+2c+1 view change msgs). "
               "State transfer hasn't kicked-in for a while either. Asking to leave the current view: "
                   << KVLOG(curView,
                            timeSinceLastAgreedViewMilli,
                            timeSinceLastStateTransferMilli,
                            lastViewThatTransferredSeqNumbersFullyExecuted));

      std::unique_ptr<ReplicaAsksToLeaveViewMsg> askToLeaveView(ReplicaAsksToLeaveViewMsg::create(
          config_.getreplicaId(), curView, ReplicaAsksToLeaveViewMsg::Reason::NewPrimaryGetInChargeTimeout));
      sendToAllOtherReplicas(askToLeaveView.get());
      complainedReplicas.store(std::move(askToLeaveView));
      metric_sent_replica_asks_to_leave_view_msg_.Get().Inc();

      tryToGotoNextView();
      return;
    }
  }
}

void ReplicaImp::onStatusReportTimer(Timers::Handle timer) {
  if (isCollectingState() || bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;

  tryToSendStatusReport(true);

#ifdef DEBUG_MEMORY_MSG
  MessageBase::printLiveMessages();
#endif
}

void ReplicaImp::onSlowPathTimer(Timers::Handle timer) {
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  tryToStartSlowPaths();
  auto newPeriod = milliseconds(controller->slowPathsTimerMilli());
  timers_.reset(timer, newPeriod);
  metric_slow_path_timer_.Get().Set(controller->slowPathsTimerMilli());
}

void ReplicaImp::onInfoRequestTimer(Timers::Handle timer) {
  if (bftEngine::ControlStateManager::instance().getPruningProcessStatus()) return;
  tryToAskForMissingInfo();
  auto newPeriod = milliseconds(dynamicUpperLimitOfRounds->upperLimit() / 2);
  timers_.reset(timer, newPeriod);
  metric_info_request_timer_.Get().Set(dynamicUpperLimitOfRounds->upperLimit() / 2);
}

template <>
void ReplicaImp::onMessage<SimpleAckMsg>(SimpleAckMsg *msg) {
  metric_received_simple_acks_.Get().Inc();
  SCOPED_MDC_SEQ_NUM(std::to_string(msg->seqNumber()));
  uint16_t relatedMsgType = (uint16_t)msg->ackData();  // TODO(GG): does this make sense ?
  if (retransmissionsLogicEnabled) {
    LOG_DEBUG(MSGS, KVLOG(msg->senderId(), relatedMsgType));
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
                       shared_ptr<IRequestsHandler> requestsHandler,
                       IStateTransfer *stateTrans,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<PersistentStorage> persistentStorage,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers,
                       shared_ptr<concord::performance::PerformanceManager> &pm,
                       const shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm)
    : ReplicaImp(false,
                 ld.repConfig,
                 requestsHandler,
                 stateTrans,
                 ld.repsInfo,
                 ld.viewsManager,
                 msgsCommunicator,
                 msgHandlers,
                 timers,
                 pm,
                 sm) {
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
  metric_current_primary_.Get().Set(curView % config_.getnumReplicas());

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
  metric_total_committed_sn_.Get().Inc(lastExecutedSeqNum);
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
    const bool isPrimaryOfView = (repsInfo->primaryOfView(curView) == config_.getreplicaId());

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

        std::shared_ptr<IThresholdSigner> commitSigner;

        ConcordAssertOR((config_.getcVal() != 0), (pathInPrePrepare != CommitPath::FAST_WITH_THRESHOLD));

        if ((pathInPrePrepare == CommitPath::FAST_WITH_THRESHOLD) && (config_.getcVal() > 0))
          commitSigner = CryptoManager::instance().thresholdSignerForCommit(seqNum);
        else
          commitSigner = CryptoManager::instance().thresholdSignerForOptimisticCommit(seqNum);

        Digest tmpDigest;
        Digest::calcCombination(ppDigest, curView, seqNum, tmpDigest);

        PartialCommitProofMsg *p = new PartialCommitProofMsg(
            config_.getreplicaId(), curView, seqNum, pathInPrePrepare, tmpDigest, commitSigner);
        seqNumInfo.partialProofs().addSelfMsgAndPPDigest(
            p,
            tmpDigest);  // TODO(GG): consider using a method that directly adds the message/digest (as in the
        // examples below)
      }
      std::shared_ptr<ISecureStore> secStore;
      // If sm_ has a SecretsManager instance - encrypted config is enabled and EncryptedFileSecureStore is used
      if (sm_) {
        secStore.reset(new KeyExchangeManager::EncryptedFileSecureStore(
            sm_, ReplicaConfig::instance().getkeyViewFilePath(), config_.getreplicaId()));
      } else {
        secStore.reset(new KeyExchangeManager::FileSecureStore(ReplicaConfig::instance().getkeyViewFilePath(),
                                                               config_.getreplicaId()));
      }

      if (e.getSlowStarted()) {
        seqNumInfo.startSlowPath();

        // add PreparePartialMsg
        PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
        PreparePartialMsg *p =
            PreparePartialMsg::create(curView,
                                      pp->seqNumber(),
                                      config_.getreplicaId(),
                                      pp->digestOfRequests(),
                                      CryptoManager::instance().thresholdSignerForSlowPathCommit(pp->seqNumber()));
        bool added = seqNumInfo.addSelfMsg(p, true);
        if (!added) {
          LOG_INFO(GL, "Failed to add sn [" << s << "] to main log, trying different crypto system");
          KeyExchangeManager::loadCryptoFromKeyView(secStore, config_.getreplicaId(), config_.getnumReplicas());
          p = PreparePartialMsg::create(curView,
                                        pp->seqNumber(),
                                        config_.getreplicaId(),
                                        pp->digestOfRequests(),
                                        CryptoManager::instance().thresholdSignerForSlowPathCommit(pp->seqNumber()));
          added = seqNumInfo.addSelfMsg(p, true);
        }
        ConcordAssert(added);
      }

      if (e.isPrepareFullMsgSet()) {
        bool failedToAdd = false;
        try {
          seqNumInfo.addMsg(e.getPrepareFullMsg(), true);
        } catch (const std::exception &e) {
          failedToAdd = true;
          LOG_INFO(GL, "Failed to add sn [" << s << "] to main log, trying different crypto system");
          std::cout << e.what() << '\n';
          KeyExchangeManager::loadCryptoFromKeyView(secStore, config_.getreplicaId(), config_.getnumReplicas());
        }
        if (failedToAdd) seqNumInfo.addMsg(e.getPrepareFullMsg(), true);

        Digest d;
        Digest::digestOfDigest(e.getPrePrepareMsg()->digestOfRequests(), d);
        CommitPartialMsg *c = CommitPartialMsg::create(
            curView, s, config_.getreplicaId(), d, CryptoManager::instance().thresholdSignerForSlowPathCommit(s));

        bool added = seqNumInfo.addSelfCommitPartialMsgAndDigest(c, d, true);
        if (!added) {
          LOG_INFO(GL, "Failed to add sn [" << s << "] to main log, trying different crypto system");
          KeyExchangeManager::loadCryptoFromKeyView(secStore, config_.getreplicaId(), config_.getnumReplicas());
          c = CommitPartialMsg::create(
              curView, s, config_.getreplicaId(), d, CryptoManager::instance().thresholdSignerForSlowPathCommit(s));
          seqNumInfo.addSelfCommitPartialMsgAndDigest(c, d, true);
        }
      }

      if (e.isCommitFullMsgSet()) {
        bool failedToAdd = false;
        try {
          seqNumInfo.addMsg(e.getCommitFullMsg(), true);
        } catch (const std::exception &e) {
          failedToAdd = true;
          LOG_INFO(GL, "Failed to add sn [" << s << "] to main log, trying different crypto system");
          std::cout << e.what() << '\n';
          KeyExchangeManager::loadCryptoFromKeyView(secStore, config_.getreplicaId(), config_.getnumReplicas());
        }
        if (failedToAdd) seqNumInfo.addMsg(e.getCommitFullMsg(), true);

        ConcordAssert(e.getCommitFullMsg()->equals(*seqNumInfo.getValidCommitFullMsg()));
      }

      if (e.isFullCommitProofMsgSet()) {
        PartialProofsSet &pps = seqNumInfo.partialProofs();
        bool added = pps.addMsg(e.getFullCommitProofMsg());  // TODO(GG): consider using a method that directly adds
        // the message (as in the examples below)
        if (!added) {
          LOG_INFO(GL, "Failed to add sn [" << s << "] to main log, trying different crypto system");
          KeyExchangeManager::loadCryptoFromKeyView(secStore, config_.getreplicaId(), config_.getnumReplicas());
          added = pps.addMsg(e.getFullCommitProofMsg());
        }
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
    ConcordAssertEQ(e.getCheckpointMsg()->senderId(), config_.getreplicaId());
    ConcordAssertOR((s != ld.lastStableSeqNum), e.getCheckpointMsg()->isStableState());

    checkInfo.addCheckpointMsg(e.getCheckpointMsg(), config_.getreplicaId());
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
                       shared_ptr<IRequestsHandler> requestsHandler,
                       IStateTransfer *stateTrans,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<PersistentStorage> persistentStorage,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers,
                       shared_ptr<concord::performance::PerformanceManager> &pm,
                       const shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm)
    : ReplicaImp(
          true, config, requestsHandler, stateTrans, nullptr, nullptr, msgsCommunicator, msgHandlers, timers, pm, sm) {
  if (persistentStorage != nullptr) {
    ps_ = persistentStorage;
  }

  auto numThreads = 8;
  LOG_INFO(GL, "Starting internal replica thread pool. " << KVLOG(numThreads));
  internalThreadPool.start(numThreads);  // TODO(GG): use configuration
}

ReplicaImp::ReplicaImp(bool firstTime,
                       const ReplicaConfig &config,
                       shared_ptr<IRequestsHandler> requestsHandler,
                       IStateTransfer *stateTrans,
                       ReplicasInfo *replicasInfo,
                       ViewsManager *viewsMgr,
                       shared_ptr<MsgsCommunicator> msgsCommunicator,
                       shared_ptr<MsgHandlersRegistrator> msgHandlers,
                       concordUtil::Timers &timers,
                       shared_ptr<concord::performance::PerformanceManager> &pm,
                       const shared_ptr<concord::secretsmanager::ISecretsManagerImpl> &sm)
    : ReplicaForStateTransfer(config, requestsHandler, stateTrans, msgsCommunicator, msgHandlers, firstTime, timers),
      viewChangeProtocolEnabled{config.viewChangeProtocolEnabled},
      autoPrimaryRotationEnabled{config.autoPrimaryRotationEnabled},
      restarted_{!firstTime},
      replyBuffer{(char *)std::malloc(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader))},
      timeOfLastStateSynch{getMonotonicTime()},    // TODO(GG): TBD
      timeOfLastViewEntrance{getMonotonicTime()},  // TODO(GG): TBD
      timeOfLastAgreedView{getMonotonicTime()},    // TODO(GG): TBD
      complainedReplicas(config),
      pm_{pm},
      sm_{sm},
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
      metric_current_primary_{metrics_.RegisterGauge("currentPrimary", curView % config_.getnumReplicas())},
      metric_concurrency_level_{metrics_.RegisterGauge("concurrencyLevel", config_.getconcurrencyLevel())},
      metric_primary_last_used_seq_num_{metrics_.RegisterGauge("primaryLastUsedSeqNum", primaryLastUsedSeqNum)},
      metric_on_call_back_of_super_stable_cp_{metrics_.RegisterGauge("OnCallBackOfSuperStableCP", 0)},
      metric_sent_replica_asks_to_leave_view_msg_{metrics_.RegisterGauge("sentReplicaAsksToLeaveViewMsg", 0)},
      metric_bft_batch_size_{metrics_.RegisterGauge("bft_batch_size", 0)},
      my_id{metrics_.RegisterGauge("my_id", config.replicaId)},
      primary_queue_size_{metrics_.RegisterGauge("primary_queue_size", 0)},
      consensus_avg_time_{metrics_.RegisterGauge("consensus_rolling_avg_time", 0)},
      accumulating_batch_avg_time_{metrics_.RegisterGauge("accumualating_batch_avg_time", 0)},
      metric_first_commit_path_{metrics_.RegisterStatus(
          "firstCommitPath", CommitPathToStr(ControllerWithSimpleHistory_debugInitialFirstPath))},
      batch_closed_on_logic_off_{metrics_.RegisterCounter("total_number_batch_closed_on_logic_off")},
      batch_closed_on_logic_on_{metrics_.RegisterCounter("total_number_batch_closed_on_logic_on")},
      metric_indicator_of_non_determinism_{metrics_.RegisterCounter("indicator_of_non_determinism")},
      metric_total_committed_sn_{metrics_.RegisterCounter("total_committed_seqNum")},
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
      metric_sent_replica_asks_to_leave_view_msg_due_to_status_{
          metrics_.RegisterCounter("sentReplicaAsksToLeaveViewMsgDueToStatus")},
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
      metric_total_finished_consensuses_{metrics_.RegisterCounter("totalOrderedRequests")},
      metric_total_slowPath_{metrics_.RegisterCounter("totalSlowPaths")},
      metric_total_fastPath_{metrics_.RegisterCounter("totalFastPaths")},
      metric_total_slowPath_requests_{metrics_.RegisterCounter("totalSlowPathRequests")},
      metric_total_fastPath_requests_{metrics_.RegisterCounter("totalFastPathRequests")},
      metric_total_preexec_requests_executed_{metrics_.RegisterCounter("totalPreExecRequestsExecuted")},
      consensus_times_(histograms_.consensus),
      checkpoint_times_(histograms_.checkpointFromCreationToStable),
      time_in_active_view_(histograms_.timeInActiveView),
      time_in_state_transfer_(histograms_.timeInStateTransfer),
      reqBatchingLogic_(*this, config_, metrics_, timers),
      replStatusHandlers_(*this) {
  ConcordAssertLT(config_.getreplicaId(), config_.getnumReplicas());
  // TODO(GG): more asserts on params !!!!!!!!!!!

  ConcordAssert(firstTime || ((replicasInfo != nullptr) && (viewsMgr != nullptr)));

  registerMsgHandlers();
  replStatusHandlers_.registerStatusHandlers();

  // Register metrics component with the default aggregator.
  metrics_.Register();

  sigManager_.reset(SigManager::init(config_.replicaId,
                                     config_.replicaPrivateKey,
                                     config_.publicKeysOfReplicas,
                                     KeyFormat::HexaDecimalStrippedFormat,
                                     config_.clientTransactionSigningEnabled ? &config.publicKeysOfClients : nullptr,
                                     KeyFormat::PemFormat,
                                     config_.numReplicas,
                                     config_.numRoReplicas,
                                     config_.numOfClientProxies,
                                     config_.numOfExternalClients));
  if (firstTime) {
    repsInfo = new ReplicasInfo(config_, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);
    viewsManager = new ViewsManager(repsInfo);
  } else {
    repsInfo = replicasInfo;
    viewsManager = viewsMgr;
  }

  std::set<NodeIdType> clientsSet;
  const auto numOfEntities = config_.getnumReplicas() + config_.getnumRoReplicas() + config_.getnumOfClientProxies() +
                             config_.getnumOfExternalClients();
  for (uint16_t i = config_.getnumReplicas() + config_.getnumRoReplicas(); i < numOfEntities; i++) clientsSet.insert(i);
  clientsManager = new ClientsManager(metrics_, clientsSet);
  clientsManager->initInternalClientInfo(config_.getnumReplicas());
  internalBFTClient_.reset(new InternalBFTClient(
      config_.getreplicaId(), clientsManager->getHighestIdOfNonInternalClient(), msgsCommunicator_));

  ClientsManager::setNumResPages(
      (config.numOfClientProxies + config.numOfExternalClients + config.numReplicas) *
      ClientsManager::reservedPagesPerClient(config.getsizeOfReservedPage(), config.maxReplyMessageSize));
  ClusterKeyStore::setNumResPages(config.numReplicas);

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

  mainLog.reset(new WindowOfSeqNumInfo(1, (InternalReplicaApi *)this));

  checkpointsLog = new SequenceWithActiveWindow<kWorkWindowSize + 2 * checkpointWindowSize,
                                                checkpointWindowSize,
                                                SeqNum,
                                                CheckpointInfo,
                                                CheckpointInfo>(0, (InternalReplicaApi *)this);

  // create controller . TODO(GG): do we want to pass the controller as a parameter ?
  controller = new ControllerWithSimpleHistory(
      config_.getcVal(), config_.getfVal(), config_.getreplicaId(), curView, primaryLastUsedSeqNum);

  if (retransmissionsLogicEnabled)
    retransmissionsManager =
        new RetransmissionsManager(&internalThreadPool, &getIncomingMsgsStorage(), kWorkWindowSize, 0);
  else
    retransmissionsManager = nullptr;

  if (currentViewIsActive()) {
    time_in_active_view_.start();
  }

  // Instantiate IControlHandler.
  // If an application instantiation has already taken a place this will have no effect.
  IControlHandler::instance(new ControlHandler);

  LOG_INFO(GL, "ReplicaConfig parameters: " << config);
}

ReplicaImp::~ReplicaImp() {
  // TODO(GG): rewrite this method !!!!!!!! (notice that the order may be important here ).
  // TODO(GG): don't delete objects that are passed as params (TBD)

  internalThreadPool.stop();

  delete viewsManager;
  delete controller;
  delete dynamicUpperLimitOfRounds;
  delete checkpointsLog;
  delete clientsManager;
  delete repsInfo;
  free(replyBuffer);

  for (auto it = tableOfStableCheckpoints.begin(); it != tableOfStableCheckpoints.end(); it++) {
    delete it->second;
  }
  tableOfStableCheckpoints.clear();

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::freeDebugStatisticsData();
  }
}

void ReplicaImp::stop() {
  if (retransmissionsLogicEnabled) timers_.cancel(retranTimer_);
  timers_.cancel(slowPathTimer_);
  timers_.cancel(infoReqTimer_);
  timers_.cancel(statusReportTimer_);
  if (viewChangeProtocolEnabled) timers_.cancel(viewChangeTimer_);
  ReplicaForStateTransfer::stop();
}

void ReplicaImp::addTimers() {
  int statusReportTimerMilli =
      (sendStatusPeriodMilli > 0) ? sendStatusPeriodMilli : config_.getstatusReportTimerMillisec();
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

  // requires the init of state transfer
  std::shared_ptr<ISecureStore> sec;
  std::shared_ptr<ISecureStore> backupsec;
  // If sm_ has a SecretsManager instance - encrypted config is enabled and EncryptedFileSecureStore is used
  if (sm_) {
    sec.reset(new KeyExchangeManager::EncryptedFileSecureStore(
        sm_, ReplicaConfig::instance().getkeyViewFilePath(), config_.replicaId));
    backupsec.reset(new KeyExchangeManager::EncryptedFileSecureStore(
        sm_, ReplicaConfig::instance().getkeyViewFilePath(), config_.replicaId, "_backed"));
  } else {
    sec.reset(
        new KeyExchangeManager::FileSecureStore(ReplicaConfig::instance().getkeyViewFilePath(), config_.replicaId));
    backupsec.reset(new KeyExchangeManager::FileSecureStore(
        ReplicaConfig::instance().getkeyViewFilePath(), config_.replicaId, "_backed"));
  }

  KeyExchangeManager::InitData id{};
  id.cl = internalBFTClient_;
  id.id = config_.getreplicaId();
  id.clusterSize = config_.getnumReplicas();
  id.reservedPages = stateTransfer.get();
  id.sizeOfReservedPage = ReplicaConfig::instance().getsizeOfReservedPage();
  id.kg = &CryptoManager::instance();
  id.ke = &CryptoManager::instance();
  id.sec = sec;
  id.backupSec = backupsec;
  id.timers = &timers_;
  id.a = aggregator_;
  id.interval = std::chrono::seconds(config_.getmetricsDumpIntervalSeconds());
  id.keyExchangeOnStart = ReplicaConfig::instance().getkeyExchangeOnStart();

  KeyExchangeManager::start(&id);
  if (!firstTime_ || config_.getdebugPersistentStorageEnabled()) clientsManager->loadInfoFromReservedPages();
  addTimers();
  recoverRequests();

  // The following line will start the processing thread.
  // It must happen after the replica recovers requests in the main thread.
  msgsCommunicator_->startMsgsProcessing(config_.getreplicaId());
  if (ReplicaConfig::instance().getkeyExchangeOnStart()) {
    KeyExchangeManager::instance().sendInitialKey();
  }
}

void ReplicaImp::recoverRequests() {
  if (recoveringFromExecutionOfRequests) {
    LOG_INFO(GL, "Recovering execution of requests");
    SeqNumInfo &seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);
    PrePrepareMsg *pp = seqNumInfo.getPrePrepareMsg();
    ConcordAssertNE(pp, nullptr);
    auto span = concordUtils::startSpan("bft_recover_requests_on_start");
    SCOPED_MDC_SEQ_NUM(std::to_string(pp->seqNumber()));
    const uint16_t numOfRequests = pp->numberOfRequests();
    executeRequestsInPrePrepareMsg(span, pp, true);
    metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
    metric_total_finished_consensuses_.Get().Inc();
    if (seqNumInfo.slowPathStarted()) {
      metric_total_slowPath_.Get().Inc();
      if (numOfRequests > 0) {
        metric_total_slowPath_requests_.Get().Inc(numOfRequests);
      }
    } else {
      metric_total_fastPath_.Get().Inc();
      if (numOfRequests > 0) {
        metric_total_fastPath_requests_.Get().Inc(numOfRequests);
      }
    }
    recoveringFromExecutionOfRequests = false;
    mapOfRequestsThatAreBeingRecovered = Bitmap();
  }
}

void ReplicaImp::executeReadOnlyRequest(concordUtils::SpanWrapper &parent_span, ClientRequestMsg *request) {
  ConcordAssert(request->isReadOnly());
  ConcordAssert(!isCollectingState());

  auto span = concordUtils::startChildSpan("bft_execute_read_only_request", parent_span);
  ClientReplyMsg reply(currentPrimary(), request->requestSeqNum(), config_.getreplicaId());

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
  {
    TimeRecorder scoped_timer(*histograms_.executeReadOnlyRequest);
    bftRequestsHandler_->execute(accumulatedRequests, request->getCid(), span);
  }
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
      send(&reply, clientId);
    } else {
      LOG_ERROR(GL, "Received zero size response. " << KVLOG(clientId));
    }

  } else {
    LOG_ERROR(GL, "Received error while executing RO request. " << KVLOG(clientId, status));
  }

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onRequestCompleted(true);
  }
}

void ReplicaImp::executeRequestsInPrePrepareMsg(concordUtils::SpanWrapper &parent_span,
                                                PrePrepareMsg *ppMsg,
                                                bool recoverFromErrorInRequestsExecution) {
  TimeRecorder scoped_timer(*histograms_.executeRequestsInPrePrepareMsg);
  auto span = concordUtils::startChildSpan("bft_execute_requests_in_preprepare", parent_span);
  ConcordAssertAND(!isCollectingState(), currentViewIsActive());
  ConcordAssertNE(ppMsg, nullptr);
  ConcordAssertEQ(ppMsg->viewNumber(), curView);
  ConcordAssertEQ(ppMsg->seqNumber(), lastExecutedSeqNum + 1);

  const uint16_t numOfRequests = ppMsg->numberOfRequests();

  // recoverFromErrorInRequestsExecution ==> (numOfRequests > 0)
  ConcordAssertOR(!recoverFromErrorInRequestsExecution, (numOfRequests > 0));

  if (numOfRequests > 0) {
    histograms_.numRequestsInPrePrepareMsg->record(numOfRequests);
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
          ++numInvalidClients;
        }
        const bool validNoop = ((clientId == currentPrimary()) && (req.requestLength() == 0));
        if (validNoop) {
          ++numValidNoOps;
          continue;
        }
        if (!validClient) {
          LOG_WARN(GL, "The client is not valid. " << KVLOG(clientId));
          continue;
        }
        if (isReplyAlreadySentToClient(clientId, req.requestSeqNum())) {
          auto replyMsg = clientsManager->allocateReplyFromSavedOne(clientId, req.requestSeqNum(), currentPrimary());
          if (replyMsg) {
            send(replyMsg.get(), clientId);
          }
          reqIdx++;
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

    auto dur = controller->durationSincePrePrepare(lastExecutedSeqNum + 1);
    if (dur > 0) {
      // Primary
      LOG_DEBUG(CNSUS, "Consensus reached, sleep_duration_ms [" << dur << "ms]");

    } else {
      LOG_DEBUG(CNSUS, "Consensus reached");
    }
    executeRequestsAndSendResponses(ppMsg, requestSet, span);
  }
  uint64_t checkpointNum{};
  if ((lastExecutedSeqNum + 1) % checkpointWindowSize == 0) {
    checkpointNum = (lastExecutedSeqNum + 1) / checkpointWindowSize;
    stateTransfer->createCheckpointOfCurrentState(checkpointNum);
    checkpoint_times_.start(lastExecutedSeqNum);
  }

  //////////////////////////////////////////////////////////////////////
  // Phase 3: finalize the execution of lastExecutedSeqNum+1
  // TODO(GG): Explain what happens in recovery mode
  //////////////////////////////////////////////////////////////////////

  LOG_DEBUG(CNSUS, "Finalized execution. " << KVLOG(lastExecutedSeqNum + 1, curView, lastStableSeqNum));

  if (ps_) {
    ps_->beginWriteTran();
    ps_->setLastExecutedSeqNum(lastExecutedSeqNum + 1);
  }

  lastExecutedSeqNum = lastExecutedSeqNum + 1;

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onLastExecutedSequenceNumberChanged(lastExecutedSeqNum);
  }
  if (lastViewThatTransferredSeqNumbersFullyExecuted < curView &&
      (lastExecutedSeqNum >= maxSeqNumTransferredFromPrevViews)) {
    // we store the old value of the seqNum column so we can return to it after logging the view number
    auto mdcSeqNum = MDC_GET(MDC_SEQ_NUM_KEY);
    MDC_PUT(MDC_SEQ_NUM_KEY, std::to_string(curView));

    LOG_INFO(VC_LOG,
             "Rebuilding of previous View's Working Window complete. "
                 << KVLOG(curView,
                          lastViewThatTransferredSeqNumbersFullyExecuted,
                          lastExecutedSeqNum,
                          maxSeqNumTransferredFromPrevViews));
    lastViewThatTransferredSeqNumbersFullyExecuted = curView;
    MDC_PUT(MDC_SEQ_NUM_KEY, mdcSeqNum);
    if (ps_) {
      ps_->setLastViewThatTransferredSeqNumbersFullyExecuted(lastViewThatTransferredSeqNumbersFullyExecuted);
    }
  }

  if (lastExecutedSeqNum % checkpointWindowSize == 0) {
    Digest checkDigest;
    const uint64_t checkpointNum = lastExecutedSeqNum / checkpointWindowSize;
    stateTransfer->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char *)&checkDigest);
    CheckpointMsg *checkMsg = new CheckpointMsg(config_.getreplicaId(), lastExecutedSeqNum, checkDigest, false);
    CheckpointInfo &checkInfo = checkpointsLog->get(lastExecutedSeqNum);
    checkInfo.addCheckpointMsg(checkMsg, config_.getreplicaId());

    if (ps_) ps_->setCheckpointMsgInCheckWindow(lastExecutedSeqNum, checkMsg);

    if (checkInfo.isCheckpointCertificateComplete()) {
      onSeqNumIsStable(lastExecutedSeqNum);
    }
    checkInfo.setSelfExecutionTime(getMonotonicTime());
    if (checkInfo.isCheckpointSuperStable()) {
      onSeqNumIsSuperStable(lastStableSeqNum);
    }

    KeyExchangeManager::instance().onCheckpoint(checkpointNum);
  }

  if (ps_) ps_->endWriteTran();

  if (numOfRequests > 0) bftRequestsHandler_->onFinishExecutingReadWriteRequests();

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

  if (config_.getdebugStatisticsEnabled()) {
    DebugStatistics::onRequestCompleted(false);
  }
}

void ReplicaImp::executeRequestsAndSendResponses(PrePrepareMsg *ppMsg,
                                                 Bitmap &requestSet,
                                                 concordUtils::SpanWrapper &span) {
  SCOPED_MDC("pp_msg_cid", ppMsg->getCid());
  IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  size_t reqIdx = 0;
  RequestsIterator reqIter(ppMsg);
  char *requestBody = nullptr;
  while (reqIter.getAndGoToNext(requestBody)) {
    size_t tmp = reqIdx;
    reqIdx++;
    if (!requestSet.get(tmp)) {
      continue;
    }
    ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);
    if (req.flags() & EMPTY_CLIENT_FLAG) {
      continue;
    }
    SCOPED_MDC_CID(req.getCid());
    NodeIdType clientId = req.clientProxyId();

    accumulatedRequests.push_back(IRequestsHandler::ExecutionRequest{
        clientId,
        static_cast<uint64_t>(lastExecutedSeqNum + 1),
        req.flags(),
        req.requestLength(),
        req.requestBuf(),
        static_cast<uint32_t>(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
        (char *)std::malloc(config_.getmaxReplyMessageSize() - sizeof(ClientReplyMsgHeader)),
        req.requestSeqNum()});
  }
  if (ReplicaConfig::instance().blockAccumulation) {
    LOG_DEBUG(GL,
              "Executing all the requests of preprepare message with cid: " << ppMsg->getCid() << " with accumulation");
    {
      TimeRecorder scoped_timer(*histograms_.executeWriteRequest);
      bftRequestsHandler_->execute(accumulatedRequests, ppMsg->getCid(), span);
    }
  } else {
    LOG_DEBUG(
        GL,
        "Executing all the requests of preprepare message with cid: " << ppMsg->getCid() << " without accumulation");
    IRequestsHandler::ExecutionRequestsQueue singleRequest;
    for (auto &req : accumulatedRequests) {
      singleRequest.push_back(req);
      {
        TimeRecorder scoped_timer(*histograms_.executeWriteRequest);
        bftRequestsHandler_->execute(singleRequest, ppMsg->getCid(), span);
      }
      req = singleRequest.at(0);
      singleRequest.clear();
    }
  }
  for (auto &req : accumulatedRequests) {
    ConcordAssertGT(req.outActualReplySize,
                    0);  // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)
    auto status = req.outExecutionStatus;
    if (status != 0) {
      const auto requestSeqNum = req.requestSequenceNum;
      LOG_WARN(CNSUS, "Request execution failed: " << KVLOG(req.clientId, requestSeqNum, ppMsg->getCid()));
    } else {
      if (req.flags & HAS_PRE_PROCESSED_FLAG) metric_total_preexec_requests_executed_.Get().Inc();
      auto replyMsg = clientsManager->allocateNewReplyMsgAndWriteToStorage(
          req.clientId, req.requestSequenceNum, currentPrimary(), req.outReply, req.outActualReplySize);
      replyMsg->setReplicaSpecificInfoLength(req.outReplicaSpecificInfoSize);
      free(req.outReply);
      send(replyMsg.get(), req.clientId);
    }
    if (clientsManager->isValidClient(req.clientId))
      clientsManager->removePendingForExecutionRequest(req.clientId, req.requestSequenceNum);
  }
}

void ReplicaImp::tryToRemovePendingRequestsForSeqNum(SeqNum seqNum) {
  if (lastExecutedSeqNum >= seqNum) return;
  SCOPED_MDC_SEQ_NUM(std::to_string(seqNum));
  SeqNumInfo &seqNumInfo = mainLog->get(seqNum);
  PrePrepareMsg *prePrepareMsg = seqNumInfo.getPrePrepareMsg();
  if (prePrepareMsg == nullptr) return;
  LOG_INFO(GL, "clear pending requests" << KVLOG(seqNum));

  RequestsIterator reqIter(prePrepareMsg);
  char *requestBody = nullptr;
  while (reqIter.getAndGoToNext(requestBody)) {
    ClientRequestMsg req((ClientRequestMsgHeader *)requestBody);
    if (!clientsManager->isValidClient(req.clientProxyId())) continue;
    auto clientId = req.clientProxyId();
    LOG_DEBUG(GL, "removing pending requests for client" << KVLOG(clientId));
    clientsManager->markRequestAsCommitted(clientId, req.requestSeqNum());
  }
}

void ReplicaImp::executeNextCommittedRequests(concordUtils::SpanWrapper &parent_span,
                                              SeqNum seqNumber,
                                              const bool requestMissingInfo) {
  ConcordAssertAND(!isCollectingState(), currentViewIsActive());
  ConcordAssertGE(lastExecutedSeqNum, lastStableSeqNum);
  auto span = concordUtils::startChildSpan("bft_execute_next_committed_requests", parent_span);
  consensus_times_.end(seqNumber);
  // First of all, we remove the pending request before the execution, to prevent long execution from affecting VC
  tryToRemovePendingRequestsForSeqNum(seqNumber);

  while (lastExecutedSeqNum < lastStableSeqNum + kWorkWindowSize) {
    SeqNum nextExecutedSeqNum = lastExecutedSeqNum + 1;
    SCOPED_MDC_SEQ_NUM(std::to_string(nextExecutedSeqNum));
    SeqNumInfo &seqNumInfo = mainLog->get(nextExecutedSeqNum);

    PrePrepareMsg *prePrepareMsg = seqNumInfo.getPrePrepareMsg();

    const bool ready = (prePrepareMsg != nullptr) && (seqNumInfo.isCommitted__gg());

    if (requestMissingInfo && !ready) {
      LOG_INFO(GL, "Asking for missing information: " << KVLOG(nextExecutedSeqNum, curView, lastStableSeqNum));
      // during exchange we need missing data to be performed in slow path
      auto exchanging =
          (!KeyExchangeManager::instance().keysExchanged && ReplicaConfig::instance().getkeyExchangeOnStart());
      tryToSendReqMissingDataMsg(nextExecutedSeqNum, exchanging);
    }

    if (!ready) break;

    ConcordAssertEQ(prePrepareMsg->seqNumber(), nextExecutedSeqNum);
    ConcordAssertEQ(prePrepareMsg->viewNumber(), curView);  // TODO(GG): TBD
    const uint16_t numOfRequests = prePrepareMsg->numberOfRequests();

    executeRequestsInPrePrepareMsg(span, prePrepareMsg);
    consensus_time_.add(seqNumInfo.getCommitDurationMs());
    consensus_avg_time_.Get().Set((uint64_t)consensus_time_.avg());
    if (consensus_time_.numOfElements() == 1000) consensus_time_.reset();  // We reset the average every 1000 samples
    metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);
    metric_total_finished_consensuses_.Get().Inc();
    if (seqNumInfo.slowPathStarted()) {
      metric_total_slowPath_.Get().Inc();
      if (numOfRequests > 0) {
        metric_total_slowPath_requests_.Get().Inc(numOfRequests);
      }
    } else {
      metric_total_fastPath_.Get().Inc();
      if (numOfRequests > 0) {
        metric_total_fastPath_requests_.Get().Inc(numOfRequests);
      }
    }
  }
  if (ControlStateManager::instance().getCheckpointToStopAt().has_value()) {
    // If, following the last execution, we discover that we need to jump to the
    // next checkpoint, the primary sends noop commands until filling the working window.
    auto checkpointToStopAt = ControlStateManager::instance().getCheckpointToStopAt().value();
    auto next_checkpoint = checkpointToStopAt - checkpointWindowSize;
    bringTheSystemToCheckpointBySendingNoopCommands(next_checkpoint);
  }
  if (ControlStateManager::instance().getCheckpointToStopAt().has_value() &&
      lastExecutedSeqNum == ControlStateManager::instance().getCheckpointToStopAt()) {
    // We are about to stop execution. To avoid VC we now clear all pending requests
    clientsManager->clearAllPendingRequests();
  }
  if (isCurrentPrimary() && requestsQueueOfPrimary.size() > 0) tryToSendPrePrepareMsg(true);
}

void ReplicaImp::tryToGotoNextView() {
  if (complainedReplicas.hasQuorumToLeaveView()) {
    GotoNextView();
  } else {
    LOG_INFO(VC_LOG, "Insufficient quorum for moving to next view " << KVLOG(getCurrentView()));
  }
}

IncomingMsgsStorage &ReplicaImp::getIncomingMsgsStorage() { return *msgsCommunicator_->getIncomingMsgsStorage(); }

// TODO(GG): the timer for state transfer !!!!

// TODO(GG): !!!! view changes and retransmissionsLogic --- check ....

}  // namespace bftEngine::impl
