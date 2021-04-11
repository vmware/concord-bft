// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "PreProcessor.hpp"
#include "InternalReplicaApi.hpp"
#include "Logger.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "OpenTracing.hpp"

namespace preprocessor {

using namespace bftEngine;
using namespace concord::util;
using namespace concordUtils;
using namespace std;
using namespace std::placeholders;
using namespace concordUtil;

uint8_t RequestState::reqProcessingHistoryHeight = 10;

//**************** Class PreProcessor ****************//

vector<shared_ptr<PreProcessor>> PreProcessor::preProcessors_;

//**************** Static functions ****************//

void PreProcessor::addNewPreProcessor(shared_ptr<MsgsCommunicator> &msgsCommunicator,
                                      shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                                      shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                                      bftEngine::IRequestsHandler &requestsHandler,
                                      InternalReplicaApi &replica,
                                      concordUtil::Timers &timers,
                                      shared_ptr<concord::performance::PerformanceManager> &pm) {
  if (ReplicaConfig::instance().getnumOfExternalClients() + ReplicaConfig::instance().getnumOfClientProxies() <= 0) {
    LOG_ERROR(logger(), "Wrong configuration: a number of clients could not be zero!");
    return;
  }

  if (ReplicaConfig::instance().getpreExecutionFeatureEnabled())
    preProcessors_.push_back(make_unique<PreProcessor>(
        msgsCommunicator, incomingMsgsStorage, msgHandlersRegistrator, requestsHandler, replica, timers, pm));
}

void PreProcessor::setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  if (ReplicaConfig::instance().getpreExecutionFeatureEnabled() && aggregator) {
    for (const auto &elem : preProcessors_) elem->metricsComponent_.SetAggregator(aggregator);
  }
}

//**************************************************//

bool PreProcessor::validateMessage(MessageBase *msg) const {
  try {
    msg->validate(myReplica_.getReplicasInfo());
    return true;
  } catch (std::exception &e) {
    LOG_WARN(logger(),
             "Received invalid message from Node " << msg->senderId() << " type: " << msg->type()
                                                   << " reason: " << e.what());
    return false;
  }
}

PreProcessor::PreProcessor(shared_ptr<MsgsCommunicator> &msgsCommunicator,
                           shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                           shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                           IRequestsHandler &requestsHandler,
                           const InternalReplicaApi &myReplica,
                           concordUtil::Timers &timers,
                           shared_ptr<concord::performance::PerformanceManager> &pm)
    : msgsCommunicator_(msgsCommunicator),
      incomingMsgsStorage_(incomingMsgsStorage),
      msgHandlersRegistrator_(msgHandlersRegistrator),
      requestsHandler_(requestsHandler),
      myReplica_(myReplica),
      myReplicaId_(myReplica.getReplicaConfig().replicaId),
      maxPreExecResultSize_(myReplica.getReplicaConfig().maxExternalMessageSize),
      idsOfPeerReplicas_(myReplica.getIdsOfPeerReplicas()),
      numOfReplicas_(myReplica.getReplicaConfig().numReplicas + myReplica.getReplicaConfig().numRoReplicas),
      numOfInternalClients_(myReplica.getReplicaConfig().numOfClientProxies),
      clientBatchingEnabled_(myReplica.getReplicaConfig().clientBatchingEnabled),
      clientMaxBatchSize_(clientBatchingEnabled_ ? myReplica.getReplicaConfig().clientBatchingMaxMsgsNbr : 1),
      metricsComponent_{concordMetrics::Component("preProcessor", std::make_shared<concordMetrics::Aggregator>())},
      metricsLastDumpTime_(0),
      metricsDumpIntervalInSec_{myReplica_.getReplicaConfig().metricsDumpIntervalSeconds},
      preProcessorMetrics_{metricsComponent_.RegisterCounter("preProcReqReceived"),
                           metricsComponent_.RegisterCounter("preProcReqInvalid"),
                           metricsComponent_.RegisterCounter("preProcReqIgnored"),
                           metricsComponent_.RegisterCounter("preProcConsensusNotReached"),
                           metricsComponent_.RegisterCounter("preProcessRequestTimedout"),
                           metricsComponent_.RegisterCounter("preProcReqSentForFurtherProcessing"),
                           metricsComponent_.RegisterCounter("preProcPossiblePrimaryFaultDetected"),
                           metricsComponent_.RegisterCounter("preProcReqForwardedByNonPrimaryNotIgnored"),
                           metricsComponent_.RegisterGauge("PreProcInFlyRequestsNum", 0)},
      preExecReqStatusCheckPeriodMilli_(myReplica_.getReplicaConfig().preExecReqStatusCheckTimerMillisec),
      timers_{timers},
      recorder_{histograms_.totalPreExecutionDuration},
      lastViewNum_(myReplica.getCurrentView()),
      pm_{pm} {
  registerMsgHandlers();
  metricsComponent_.Register();
  const uint16_t numOfExternalClients = myReplica.getReplicaConfig().numOfExternalClients;
  const uint16_t numOfReqEntries = numOfExternalClients * clientMaxBatchSize_;
  const uint16_t firstClientRequestId = (numOfReplicas_ + numOfInternalClients_) * clientMaxBatchSize_;
  for (uint16_t i = 0; i < numOfReqEntries; i++) {
    // Placeholders for all clients including batches
    ongoingRequests_[firstClientRequestId + i] = make_shared<RequestState>();
    // Allocate a buffer for the pre-execution result per client * batch
    preProcessResultBuffers_.push_back(Sliver(new char[maxPreExecResultSize_], maxPreExecResultSize_));
  }
  RequestState::reqProcessingHistoryHeight *= clientMaxBatchSize_;
  uint64_t numOfThreads = myReplica.getReplicaConfig().preExecConcurrencyLevel;
  if (!numOfThreads) {
    if (myReplica.getReplicaConfig().numOfExternalClients)
      numOfThreads = myReplica.getReplicaConfig().numOfExternalClients * clientMaxBatchSize_;
    else  // For testing purpose
      numOfThreads = myReplica.getReplicaConfig().numOfClientProxies / numOfReplicas_;
  }
  threadPool_.start(numOfThreads);
  LOG_INFO(logger(),
           KVLOG(numOfReplicas_,
                 numOfExternalClients,
                 numOfInternalClients_,
                 numOfReqEntries,
                 firstClientRequestId,
                 clientBatchingEnabled_,
                 clientMaxBatchSize_,
                 maxPreExecResultSize_,
                 preExecReqStatusCheckPeriodMilli_,
                 numOfThreads));
  RequestProcessingState::init(numOfRequiredReplies(), &histograms_);
  addTimers();
}

PreProcessor::~PreProcessor() {
  cancelTimers();
  threadPool_.stop();
}

void PreProcessor::addTimers() {
  // This timer is used for a periodic detection of timed out client requests.
  // Each such request contains requestTimeoutMilli parameter that defines its lifetime.
  if (preExecReqStatusCheckPeriodMilli_ != 0)
    requestsStatusCheckTimer_ = timers_.add(chrono::milliseconds(preExecReqStatusCheckPeriodMilli_),
                                            Timers::Timer::RECURRING,
                                            [this](Timers::Handle h) { onRequestsStatusCheckTimer(); });

  metricsTimer_ =
      timers_.add(100ms, Timers::Timer::RECURRING, [this](Timers::Handle h) { updateAggregatorAndDumpMetrics(); });
}

void PreProcessor::cancelTimers() {
  try {
    timers_.cancel(metricsTimer_);
    if (preExecReqStatusCheckPeriodMilli_ != 0) timers_.cancel(requestsStatusCheckTimer_);
  } catch (std::invalid_argument &e) {
  }
}

// This function should be always called under a reqEntry->mutex lock
// Resend PreProcessRequestMsg to replicas that have previously rejected it
void PreProcessor::resendPreProcessRequest(const RequestProcessingStateUniquePtr &reqStatePtr) {
  const auto &rejectedReplicasList = reqStatePtr->getRejectedReplicasList();
  const auto &preProcessReqMsg = reqStatePtr->getPreProcessRequest();
  if (!rejectedReplicasList.empty() && preProcessReqMsg) {
    SCOPED_MDC_CID(preProcessReqMsg->getCid());
    const auto &clientId = preProcessReqMsg->clientId();
    const auto &reqSeqNum = preProcessReqMsg->reqSeqNum();
    for (const auto &destId : rejectedReplicasList) {
      LOG_DEBUG(logger(), "Resending PreProcessRequestMsg" << KVLOG(clientId, reqSeqNum, destId));
      sendMsg(preProcessReqMsg->body(), destId, preProcessReqMsg->type(), preProcessReqMsg->size());
    }
    reqStatePtr->resetRejectedReplicasList();
  }
}

void PreProcessor::onRequestsStatusCheckTimer() {
  // Pass through all ongoing requests and abort the pre-execution for those that are timed out.
  for (const auto &reqEntry : ongoingRequests_) {
    lock_guard<mutex> lock(reqEntry.second->mutex);
    const auto &reqStatePtr = reqEntry.second->reqProcessingStatePtr;
    if (reqStatePtr) {
      if (reqStatePtr->isReqTimedOut()) {
        preProcessorMetrics_.preProcessRequestTimedout.Get().Inc();
        preProcessorMetrics_.preProcPossiblePrimaryFaultDetected.Get().Inc();
        // The request could expire do to failed primary replica, let ReplicaImp address that
        // TBD YS: This causes a request to retry in case the primary is OK. Consider passing a kind of NOOP message.
        const auto &reqEntryIndex = reqEntry.first;
        const auto &reqSeqNum = reqStatePtr->getReqSeqNum();
        const auto &clientId = reqStatePtr->getClientId();
        const auto &reqOffsetInBatch = reqStatePtr->getReqOffsetInBatch();
        SCOPED_MDC_CID(reqStatePtr->getReqCid());
        LOG_INFO(logger(), "Let replica handle request" << KVLOG(reqSeqNum, reqEntryIndex, clientId, reqOffsetInBatch));
        preProcessorMetrics_.preProcReqSentForFurtherProcessing.Get().Inc();
        incomingMsgsStorage_->pushExternalMsg(reqStatePtr->buildClientRequestMsg(true));
        releaseClientPreProcessRequest(reqEntry.second, CANCEL);
      } else if (myReplica_.isCurrentPrimary() && reqStatePtr->definePreProcessingConsensusResult() == CONTINUE)
        resendPreProcessRequest(reqStatePtr);
    }
  }
}

bool PreProcessor::checkClientMsgCorrectness(
    uint64_t reqSeqNum, const string &cid, bool isReadOnly, uint16_t clientId, NodeIdType senderId) const {
  SCOPED_MDC_CID(cid);
  if (myReplica_.isCollectingState()) {
    LOG_INFO(logger(),
             "Ignore ClientPreProcessRequestMsg as the replica is collecting missing state from other replicas"
                 << KVLOG(reqSeqNum, clientId));
    return false;
  }
  if (isReadOnly) {
    LOG_INFO(logger(), "Ignore ClientPreProcessRequestMsg as it is signed as read-only" << KVLOG(reqSeqNum, clientId));
    return false;
  }
  const bool &invalidClient = !myReplica_.isValidClient(clientId);
  const bool &sentFromReplicaToNonPrimary = myReplica_.isIdOfReplica(senderId) && !myReplica_.isCurrentPrimary();
  if (invalidClient || sentFromReplicaToNonPrimary) {
    LOG_WARN(logger(),
             "Ignore ClientPreProcessRequestMsg as invalid"
                 << KVLOG(reqSeqNum, clientId, invalidClient, sentFromReplicaToNonPrimary));
    return false;
  }
  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(), "Ignore ClientPreProcessRequestMsg as current view is inactive" << KVLOG(reqSeqNum, clientId));
    return false;
  }
  return true;
}

bool PreProcessor::checkClientBatchMsgCorrectness(const ClientBatchRequestMsgUniquePtr &clientBatchReqMsg) {
  if (!clientBatchingEnabled_) {
    LOG_ERROR(logger(),
              "Batching functionality is disabled => reject message"
                  << KVLOG(clientBatchReqMsg->clientId(), clientBatchReqMsg->senderId(), clientBatchReqMsg->getCid()));
    return false;
  }
  const auto &clientRequestMsgs = clientBatchReqMsg->getClientPreProcessRequestMsgs();
  for (const auto &msg : clientRequestMsgs) {
    if (!checkClientMsgCorrectness(msg->requestSeqNum(), msg->getCid(), false, msg->clientProxyId(), msg->senderId()))
      return false;
  }
  return true;
}

void PreProcessor::updateAggregatorAndDumpMetrics() {
  metricsComponent_.UpdateAggregator();
  auto currTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
  if (currTime - metricsLastDumpTime_ >= metricsDumpIntervalInSec_) {
    metricsLastDumpTime_ = currTime;
    LOG_INFO(logger(), "--preProcessor metrics dump--" + metricsComponent_.ToJson());
  }
}

void PreProcessor::sendRejectPreProcessReplyMsg(NodeIdType clientId,
                                                uint16_t reqOffsetInBatch,
                                                NodeIdType senderId,
                                                SeqNum reqSeqNum,
                                                SeqNum ongoingReqSeqNum,
                                                uint64_t reqRetryId,
                                                const string &cid,
                                                const string &ongoingCid) {
  auto replyMsg =
      make_shared<PreProcessReplyMsg>(&histograms_, myReplicaId_, clientId, reqOffsetInBatch, reqSeqNum, reqRetryId);
  replyMsg->setupMsgBody(getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch), 0, cid, STATUS_REJECT);
  LOG_DEBUG(
      logger(),
      KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch, ongoingReqSeqNum, ongoingCid)
          << " Sending PreProcessReplyMsg with STATUS_REJECT as another PreProcessRequest from the same client is "
             "in progress");
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
}

template <>
void PreProcessor::onMessage<ClientPreProcessRequestMsg>(ClientPreProcessRequestMsg *msg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onMessage);
  preProcessorMetrics_.preProcReqReceived.Get().Inc();
  ClientPreProcessReqMsgUniquePtr clientMsg(msg);
  const string &cid = clientMsg->getCid();
  const NodeIdType &senderId = clientMsg->senderId();
  const NodeIdType &clientId = clientMsg->clientProxyId();
  const ReqId &reqSeqNum = clientMsg->requestSeqNum();
  const auto &reqTimeoutMilli = clientMsg->requestTimeoutMilli();
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), "Received ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, clientId, senderId, reqTimeoutMilli));
  if (!checkClientMsgCorrectness(reqSeqNum, cid, clientMsg->isReadOnly(), clientId, senderId)) {
    preProcessorMetrics_.preProcReqIgnored.Get().Inc();
    return;
  }
  handleSingleClientRequestMessage(move(clientMsg), false, 0);
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestPreProcessingRightNow(const RequestStateSharedPtr &reqEntry,
                                                  ReqId reqSeqNum,
                                                  NodeIdType clientId,
                                                  NodeIdType senderId) {
  if (reqEntry->reqProcessingStatePtr) {
    const auto &ongoingReqSeqNum = reqEntry->reqProcessingStatePtr->getReqSeqNum();
    const auto &ongoingCid = reqEntry->reqProcessingStatePtr->getReqCid();
    LOG_DEBUG(logger(),
              KVLOG(reqSeqNum, clientId, senderId)
                  << " is ignored:" << KVLOG(ongoingReqSeqNum, ongoingCid) << " is in progress");
    preProcessorMetrics_.preProcReqIgnored.Get().Inc();
    return true;
  }
  return false;
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestPassingConsensusOrPostExec(SeqNum reqSeqNum,
                                                       NodeIdType clientId,
                                                       NodeIdType senderId,
                                                       const string &cid) {
  // Count requests arrived through non-primary replicas and accepted in spite of the
  // same request has been processing by the primary replica
  if (clientId != senderId && myReplica_.isClientRequestInProcess(clientId, reqSeqNum)) {
    preProcessorMetrics_.preProcReqForwardedByNonPrimaryNotIgnored.Get().Inc();
    LOG_DEBUG(logger(),
              "Not ignoring client request arrived through a non-primary replica"
                  << KVLOG(cid, reqSeqNum, clientId, senderId));
  }
  // Verify that a request is not passing consensus/PostExec right now. The primary replica should not ignore client
  // requests forwarded by non-primary replicas, otherwise this will cause timeouts caused by missing PreProcess
  // request messages from the primary. TBD YS: save senderId in the header and set it before re-sending
  if ((clientId == senderId) && myReplica_.isClientRequestInProcess(clientId, reqSeqNum)) {
    LOG_DEBUG(
        logger(),
        "The specified request has been processing right now - ignore" << KVLOG(cid, reqSeqNum, clientId, senderId));
    preProcessorMetrics_.preProcReqIgnored.Get().Inc();
    return true;
  }
  return false;
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestAlreadyExecuted(ReqId reqSeqNum, NodeIdType clientId, const string &cid) {
  const bool replySentToClient = myReplica_.isReplyAlreadySentToClient(clientId, reqSeqNum);
  if (replySentToClient) {
    LOG_INFO(logger(),
             "Request has already been executed - let replica decide how to proceed further"
                 << KVLOG(cid, reqSeqNum, clientId));
    preProcessorMetrics_.preProcReqSentForFurtherProcessing.Get().Inc();
    return true;
  }
  return false;
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestPreProcessedBefore(const RequestStateSharedPtr &reqEntry,
                                               SeqNum reqSeqNum,
                                               NodeIdType clientId,
                                               const string &cid) {
  if (!reqEntry->reqProcessingStatePtr) {
    // Verify that an arrived request is newer than any other in the requests history for this client
    for (const auto &oldReqState : reqEntry->reqProcessingHistory) {
      if (oldReqState->getReqSeqNum() > reqSeqNum) {
        LOG_DEBUG(logger(),
                  "The request will be ignored as newer request from this client has been already pre-processed"
                      << KVLOG(cid, reqSeqNum, clientId, oldReqState->getReqCid(), oldReqState->getReqSeqNum()));
        preProcessorMetrics_.preProcReqIgnored.Get().Inc();
        return true;
      }
    }
  }
  return false;
}

void PreProcessor::handleSingleClientRequestMessage(ClientPreProcessReqMsgUniquePtr clientMsg,
                                                    bool arrivedInBatch,
                                                    uint16_t msgOffsetInBatch) {
  SCOPED_MDC_CID(clientMsg->getCid());
  const NodeIdType &senderId = clientMsg->senderId();
  const NodeIdType &clientId = clientMsg->clientProxyId();
  const ReqId &reqSeqNum = clientMsg->requestSeqNum();
  PreProcessRequestMsgSharedPtr preProcessRequestMsg;
  LOG_DEBUG(logger(), KVLOG(reqSeqNum, clientId, senderId, arrivedInBatch, msgOffsetInBatch));
  bool registerSucceeded = false;
  {
    const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, msgOffsetInBatch)];
    lock_guard<mutex> lock(reqEntry->mutex);
    if (isRequestPreProcessingRightNow(reqEntry, reqSeqNum, clientId, senderId)) return;

    if (isRequestPassingConsensusOrPostExec(reqSeqNum, clientId, senderId, clientMsg->getCid())) return;

    if (isRequestAlreadyExecuted(reqSeqNum, clientId, clientMsg->getCid())) {
      // The request has already been committed and executed - let replica decide how to proceed further.
      incomingMsgsStorage_->pushExternalMsg(move(clientMsg));
      return;
    }

    if (isRequestPreProcessedBefore(reqEntry, reqSeqNum, clientId, clientMsg->getCid())) return;

    if (myReplica_.isCurrentPrimary())
      registerSucceeded = registerRequestOnPrimaryReplica(
          move(clientMsg), preProcessRequestMsg, msgOffsetInBatch, (reqEntry->reqRetryId)++);
    else
      registerAndHandleClientPreProcessReqOnNonPrimary(move(clientMsg), arrivedInBatch, msgOffsetInBatch);
  }
  if (myReplica_.isCurrentPrimary() && registerSucceeded)
    return handleClientPreProcessRequestByPrimary(preProcessRequestMsg);

  LOG_DEBUG(logger(),
            "ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, clientId, senderId)
                                         << " is ignored because request is old/duplicated");
  preProcessorMetrics_.preProcReqIgnored.Get().Inc();
}

template <>
void PreProcessor::onMessage<ClientBatchRequestMsg>(ClientBatchRequestMsg *msg) {
  ClientBatchRequestMsgUniquePtr clientBatch(msg);
  LOG_DEBUG(logger(),
            "Received ClientBatchPreProcessRequestMsg"
                << KVLOG(clientBatch->clientId(), clientBatch->getCid(), clientBatch->numOfMessagesInBatch()));
  if (!checkClientBatchMsgCorrectness(clientBatch)) {
    preProcessorMetrics_.preProcReqIgnored.Get().Inc();
    return;
  }
  ClientMsgsList &clientMsgs = clientBatch->getClientPreProcessRequestMsgs();
  uint16_t offset = 0;
  for (auto &clientMsg : clientMsgs) {
    LOG_DEBUG(logger(),
              "Start handling single message from the batch:" << KVLOG(clientMsg->requestSeqNum(),
                                                                       clientMsg->clientProxyId(),
                                                                       clientMsg->senderId(),
                                                                       clientMsg->requestTimeoutMilli()));
    handleSingleClientRequestMessage(move(clientMsg), true, offset++);
  }
  if (!myReplica_.isCurrentPrimary()) {
    sendMsg(clientBatch->body(), myReplica_.currentPrimary(), clientBatch->type(), clientBatch->size());
    LOG_DEBUG(logger(),
              "Sent ClientBatchRequestMsg" << KVLOG(clientBatch->clientId(), clientBatch->getCid())
                                           << " to the current primary");
  }
}

// Non-primary replica request handling
template <>
void PreProcessor::onMessage<PreProcessRequestMsg>(PreProcessRequestMsg *msg) {
  SCOPED_MDC_CID(msg->getCid());
  PreProcessRequestMsgSharedPtr preProcessReqMsg(msg);
  const NodeIdType &senderId = preProcessReqMsg->senderId();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  const NodeIdType &clientId = preProcessReqMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  LOG_DEBUG(logger(), "Received PreProcessRequestMsg" << KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch));

  if (myReplica_.isCollectingState()) {
    LOG_INFO(logger(),
             "Ignore PreProcessRequestMsg as the replica is collecting missing state from other replicas"
                 << KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch));
    return;
  }

  if (myReplica_.isCurrentPrimary()) {
    LOG_WARN(logger(),
             "Ignore PreProcessRequestMsg as current replica is the primary"
                 << KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch));
    return;
  }

  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(),
             "Ignore PreProcessRequestMsg as current view is inactive"
                 << KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch));
    return;
  }
  bool registerSucceeded = false;
  {
    const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
    lock_guard<mutex> lock(reqEntry->mutex);
    if (reqEntry->reqProcessingStatePtr && reqEntry->reqProcessingStatePtr->getPreProcessRequest()) {
      auto const &ongoingReqSeqNum = reqEntry->reqProcessingStatePtr->getPreProcessRequest()->reqSeqNum();
      auto const &ongoingCid = reqEntry->reqProcessingStatePtr->getPreProcessRequest()->getCid();
      LOG_DEBUG(logger(),
                KVLOG(reqSeqNum, ongoingReqSeqNum, ongoingCid, senderId, clientId, reqOffsetInBatch)
                    << " Another PreProcessRequest from the same client is in progress. Sending PreProcessReplyMsg "
                       "with STATUS_REJECT");
      return sendRejectPreProcessReplyMsg(clientId,
                                          reqOffsetInBatch,
                                          senderId,
                                          reqSeqNum,
                                          ongoingReqSeqNum,
                                          msg->reqRetryId(),
                                          msg->getCid(),
                                          ongoingCid);
    }
    registerSucceeded = registerRequest(ClientPreProcessReqMsgUniquePtr(), preProcessReqMsg, reqOffsetInBatch);
  }
  if (registerSucceeded) {
    preProcessorMetrics_.preProcInFlyRequestsNum.Get().Inc();  // Increase the metric on non-primary replica
    // Pre-process the request, calculate a hash of the result and send a reply back
    launchAsyncReqPreProcessingJob(preProcessReqMsg, false, false);
  }
}

// Primary replica handling
template <>
void PreProcessor::onMessage<PreProcessReplyMsg>(PreProcessReplyMsg *msg) {
  PreProcessReplyMsgSharedPtr preProcessReplyMsg(msg);
  const NodeIdType &senderId = preProcessReplyMsg->senderId();
  const NodeIdType &clientId = preProcessReplyMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReplyMsg->reqOffsetInBatch();
  const SeqNum &reqSeqNum = preProcessReplyMsg->reqSeqNum();
  string cid = preProcessReplyMsg->getCid();
  const auto &status = preProcessReplyMsg->status();
  string replyStatus = "STATUS_GOOD";
  if (status == STATUS_REJECT) replyStatus = "STATUS_REJECT";
  PreProcessingResult result = CANCEL;
  {
    SCOPED_MDC_CID(cid);
    LOG_DEBUG(logger(),
              "Received PreProcessReplyMsg" << KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch, replyStatus));
    const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
    lock_guard<mutex> lock(reqEntry->mutex);
    if (!reqEntry->reqProcessingStatePtr || reqEntry->reqProcessingStatePtr->getReqSeqNum() != reqSeqNum) {
      // Look for the request in the requests history and check for the non-determinism
      for (const auto &oldReqState : reqEntry->reqProcessingHistory)
        if (oldReqState->getReqSeqNum() == reqSeqNum)
          oldReqState->detectNonDeterministicPreProcessing(
              preProcessReplyMsg->resultsHash(), preProcessReplyMsg->senderId(), preProcessReplyMsg->reqRetryId());
      LOG_DEBUG(logger(),
                KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch)
                    << " will be ignored as no such ongoing request exists or different one found for this client");
      return;
    }
    reqEntry->reqProcessingStatePtr->handlePreProcessReplyMsg(preProcessReplyMsg);
    if (status == STATUS_REJECT) {
      LOG_DEBUG(
          logger(),
          "Received PreProcessReplyMsg with STATUS_REJECT" << KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch));
      return;
    }
    result = reqEntry->reqProcessingStatePtr->definePreProcessingConsensusResult();
    if (result == CONTINUE) resendPreProcessRequest(reqEntry->reqProcessingStatePtr);
  }
  handlePreProcessReplyMsg(cid, result, clientId, reqOffsetInBatch, reqSeqNum);
}

template <typename T>
void PreProcessor::messageHandler(MessageBase *msg) {
  T *trueTypeObj = new T(msg);
  delete msg;
  if (validateMessage(trueTypeObj)) {
    onMessage<T>(trueTypeObj);
  } else {
    preProcessorMetrics_.preProcReqInvalid.Get().Inc();
    delete trueTypeObj;
  }
}

template <>
void PreProcessor::messageHandler<PreProcessReplyMsg>(MessageBase *msg) {
  PreProcessReplyMsg *trueTypeObj = new PreProcessReplyMsg(msg);
  trueTypeObj->setPreProcessorHistograms(&histograms_);
  delete msg;
  if (validateMessage(trueTypeObj)) {
    onMessage(trueTypeObj);
  } else {
    preProcessorMetrics_.preProcReqInvalid.Get().Inc();
    delete trueTypeObj;
  }
}

void PreProcessor::registerMsgHandlers() {
  msgHandlersRegistrator_->registerMsgHandler(
      MsgCode::ClientPreProcessRequest, bind(&PreProcessor::messageHandler<ClientPreProcessRequestMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::ClientBatchRequest,
                                              bind(&PreProcessor::messageHandler<ClientBatchRequestMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessRequest,
                                              bind(&PreProcessor::messageHandler<PreProcessRequestMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessReply,
                                              bind(&PreProcessor::messageHandler<PreProcessReplyMsg>, this, _1));
}

void PreProcessor::handlePreProcessReplyMsg(
    const string &cid, PreProcessingResult result, NodeIdType clientId, uint16_t reqOffsetInBatch, SeqNum reqSeqNum) {
  SCOPED_MDC_CID(cid);
  switch (result) {
    case NONE:      // No action required - pre-processing has been already completed
    case CONTINUE:  // Not enough equal hashes collected
      break;
    case COMPLETE:  // Pre-processing consensus reached
      finalizePreProcessing(clientId, reqOffsetInBatch);
      break;
    case CANCEL:  // Pre-processing consensus not reached
      cancelPreProcessing(clientId, reqOffsetInBatch);
      break;
    case RETRY_PRIMARY:  // Primary replica generated pre-processing result hash different that one passed consensus
      LOG_INFO(logger(), "Retry primary replica pre-processing for" << KVLOG(reqSeqNum, clientId));
      PreProcessRequestMsgSharedPtr preProcessRequestMsg;
      {
        const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
        lock_guard<mutex> lock(reqEntry->mutex);
        if (reqEntry->reqProcessingStatePtr)
          preProcessRequestMsg = reqEntry->reqProcessingStatePtr->getPreProcessRequest();
      }
      if (preProcessRequestMsg) launchAsyncReqPreProcessingJob(preProcessRequestMsg, true, true);
  }
}

void PreProcessor::cancelPreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch) {
  preProcessorMetrics_.preProcConsensusNotReached.Get().Inc();
  SeqNum reqSeqNum = 0;
  const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
  {
    lock_guard<mutex> lock(reqEntry->mutex);
    if (reqEntry->reqProcessingStatePtr) {
      reqSeqNum = reqEntry->reqProcessingStatePtr->getReqSeqNum();
      const auto &cid = reqEntry->reqProcessingStatePtr->getReqCid();
      SCOPED_MDC_CID(cid);
      LOG_WARN(
          logger(),
          "Pre-processing consensus not reached; cancel request" << KVLOG(cid, reqSeqNum, clientId, reqOffsetInBatch));
      releaseClientPreProcessRequest(reqEntry, CANCEL);
    }
  }
}

void PreProcessor::finalizePreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch) {
  std::unique_ptr<ClientRequestMsg> clientRequestMsg;
  const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
  {
    concord::diagnostics::TimeRecorder scoped_timer(*histograms_.finalizePreProcessing);
    lock_guard<mutex> lock(reqEntry->mutex);
    auto &reqProcessingStatePtr = reqEntry->reqProcessingStatePtr;
    if (reqProcessingStatePtr) {
      const auto cid = reqProcessingStatePtr->getReqCid();
      const auto reqSeqNum = reqProcessingStatePtr->getReqSeqNum();
      // Copy of the message body is unavoidable here, as we need to create a new message type which lifetime is
      // controlled by the replica while all PreProcessReply messages get released here.
      clientRequestMsg = make_unique<ClientRequestMsg>(clientId,
                                                       HAS_PRE_PROCESSED_FLAG,
                                                       reqSeqNum,
                                                       reqProcessingStatePtr->getPrimaryPreProcessedResultLen(),
                                                       reqProcessingStatePtr->getPrimaryPreProcessedResult(),
                                                       reqProcessingStatePtr->getReqTimeoutMilli(),
                                                       cid);
      LOG_DEBUG(logger(),
                "Pass pre-processed request to the replica" << KVLOG(cid, reqSeqNum, clientId, reqOffsetInBatch));
      incomingMsgsStorage_->pushExternalMsg(move(clientRequestMsg));
      preProcessorMetrics_.preProcReqSentForFurtherProcessing.Get().Inc();
      releaseClientPreProcessRequest(reqEntry, COMPLETE);
      LOG_INFO(logger(), "Pre-processing completed for" << KVLOG(cid, reqSeqNum, clientId, reqOffsetInBatch));
    }
  }
}

uint16_t PreProcessor::numOfRequiredReplies() { return myReplica_.getReplicaConfig().fVal; }

// This function should be always called under a reqEntry->mutex lock
bool PreProcessor::registerRequest(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                   PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                                   uint16_t reqOffsetInBatch) {
  NodeIdType clientId = 0;
  SeqNum reqSeqNum = 0;
  string cid;
  bool clientReqMsgSpecified = false;
  if (clientReqMsg) {
    clientId = clientReqMsg->clientProxyId();
    reqSeqNum = clientReqMsg->requestSeqNum();
    cid = clientReqMsg->getCid();
    clientReqMsgSpecified = true;
  } else {
    clientId = preProcessRequestMsg->clientId();
    reqSeqNum = preProcessRequestMsg->reqSeqNum();
    cid = preProcessRequestMsg->getCid();
  }
  SCOPED_MDC_CID(cid);
  const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
  if (!reqEntry->reqProcessingStatePtr)
    reqEntry->reqProcessingStatePtr = make_unique<RequestProcessingState>(
        numOfReplicas_, clientId, reqOffsetInBatch, cid, reqSeqNum, move(clientReqMsg), preProcessRequestMsg);
  else if (!reqEntry->reqProcessingStatePtr->getPreProcessRequest())
    // The request was registered before as arrived directly from the client
    reqEntry->reqProcessingStatePtr->setPreProcessRequest(preProcessRequestMsg);
  else {
    LOG_WARN(logger(),
             KVLOG(reqSeqNum) << " could not be registered: the entry for" << KVLOG(clientId, reqOffsetInBatch)
                              << " is occupied by reqSeqNum: " << reqEntry->reqProcessingStatePtr->getReqSeqNum());
    return false;
  }
  if (clientReqMsgSpecified) {
    LOG_DEBUG(logger(), KVLOG(reqSeqNum, clientId, reqOffsetInBatch) << " registered ClientPreProcessReqMsg");
  } else {
    LOG_DEBUG(logger(), KVLOG(reqSeqNum, clientId, reqOffsetInBatch) << " registered PreProcessRequestMsg");
  }
  return true;
}

void PreProcessor::releaseClientPreProcessRequestSafe(uint16_t clientId,
                                                      uint16_t reqOffsetInBatch,
                                                      PreProcessingResult result) {
  const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
  lock_guard<mutex> lock(reqEntry->mutex);
  releaseClientPreProcessRequest(reqEntry, result);
}

// This function should be always called under a reqEntry->mutex lock
void PreProcessor::releaseClientPreProcessRequest(const RequestStateSharedPtr &reqEntry, PreProcessingResult result) {
  auto &givenReq = reqEntry->reqProcessingStatePtr;
  if (givenReq) {
    const auto &clientId = givenReq->getClientId();
    const auto &reqOffsetInBatch = givenReq->getReqOffsetInBatch();
    SeqNum reqSeqNum = givenReq->getReqSeqNum();
    if (result == COMPLETE) {
      if (reqEntry->reqProcessingHistory.size() >= reqEntry->reqProcessingHistoryHeight) {
        auto &removeFromHistoryReq = reqEntry->reqProcessingHistory.front();
        SCOPED_MDC_CID(removeFromHistoryReq->getReqCid());
        reqSeqNum = removeFromHistoryReq->getReqSeqNum();
        LOG_DEBUG(logger(), KVLOG(reqSeqNum, clientId, reqOffsetInBatch) << " removed from the history");
        removeFromHistoryReq.reset();
        reqEntry->reqProcessingHistory.pop_front();
      }
      SCOPED_MDC_CID(givenReq->getReqCid());
      LOG_DEBUG(logger(), KVLOG(reqSeqNum, clientId, reqOffsetInBatch) << " released and moved to the history");
      // No need to keep whole messages in the memory => release them before archiving
      givenReq->releaseResources();
      reqEntry->reqProcessingHistory.push_back(move(givenReq));
    } else {  // No consensus reached => release request
      SCOPED_MDC_CID(givenReq->getReqCid());
      LOG_INFO(logger(), KVLOG(reqSeqNum, clientId, reqOffsetInBatch) << " no consensus reached, request released");
      givenReq.reset();
    }
    preProcessorMetrics_.preProcInFlyRequestsNum.Get().Dec();
  }
}

void PreProcessor::sendMsg(char *msg, NodeIdType dest, uint16_t msgType, MsgSize msgSize) {
  int errorCode = msgsCommunicator_->sendAsyncMessage(dest, msg, msgSize);
  if (errorCode != 0) {
    LOG_ERROR(logger(), "sendMsg: sendAsyncMessage returned error" << KVLOG(errorCode, dest, msgType));
  }
}

// This function should be called under a reqEntry->mutex lock
bool PreProcessor::registerRequestOnPrimaryReplica(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                                   PreProcessRequestMsgSharedPtr &preProcessRequestMsg,
                                                   uint16_t reqOffsetInBatch,
                                                   uint64_t reqRetryId) {
  preProcessRequestMsg =
      make_shared<PreProcessRequestMsg>(myReplicaId_,
                                        clientReqMsg->clientProxyId(),
                                        reqOffsetInBatch,
                                        clientReqMsg->requestSeqNum(),
                                        reqRetryId,
                                        clientReqMsg->requestLength(),
                                        clientReqMsg->requestBuf(),
                                        clientReqMsg->getCid(),
                                        clientReqMsg->spanContext<ClientPreProcessReqMsgUniquePtr::element_type>());
  return registerRequest(move(clientReqMsg), preProcessRequestMsg, reqOffsetInBatch);
}

// Primary replica: start client request handling
void PreProcessor::handleClientPreProcessRequestByPrimary(PreProcessRequestMsgSharedPtr preProcessRequestMsg) {
  const auto &reqSeqNum = preProcessRequestMsg->reqSeqNum();
  const auto &clientId = preProcessRequestMsg->clientId();
  const auto &senderId = preProcessRequestMsg->senderId();
  auto time_recorder = TimeRecorder(*recorder_.get());
  LOG_INFO(logger(),
           "Start request processing by a primary replica"
               << KVLOG(reqSeqNum, preProcessRequestMsg->getCid(), clientId, senderId));
  sendPreProcessRequestToAllReplicas(preProcessRequestMsg);
  // Pre-process the request and calculate a hash of the result
  launchAsyncReqPreProcessingJob(preProcessRequestMsg, true, false, std::move(time_recorder));
}

// Non-primary replica: start client request handling
// This function should be called under a reqEntry->mutex lock
void PreProcessor::registerAndHandleClientPreProcessReqOnNonPrimary(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                                                    bool arrivedInBatch,
                                                                    uint16_t reqOffsetInBatch) {
  const auto &reqSeqNum = clientReqMsg->requestSeqNum();
  const auto &clientId = clientReqMsg->clientProxyId();
  const auto &senderId = clientReqMsg->senderId();
  const auto &reqTimeoutMilli = clientReqMsg->requestTimeoutMilli();
  // Save parameters required for a message sending before being moved to registerRequest
  const auto msgBody = clientReqMsg->body();
  const auto msgType = clientReqMsg->type();
  const auto msgSize = clientReqMsg->size();
  const auto cid = clientReqMsg->getCid();
  // Register a client request message with an empty PreProcessRequestMsg to allow follow up.
  if (registerRequest(move(clientReqMsg), PreProcessRequestMsgSharedPtr(), reqOffsetInBatch)) {
    LOG_INFO(logger(),
             "Start request processing by a non-primary replica"
                 << KVLOG(reqSeqNum, cid, clientId, reqOffsetInBatch, senderId, reqTimeoutMilli));
    if (arrivedInBatch) return;  // Need to re-send the whole batch to the primary
    sendMsg(msgBody, myReplica_.currentPrimary(), msgType, msgSize);
    LOG_DEBUG(logger(),
              "Sent ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, cid, clientId, reqOffsetInBatch)
                                                << " to the current primary");
  }
}

const char *PreProcessor::getPreProcessResultBuffer(uint16_t clientId,
                                                    ReqId reqSeqNum,
                                                    uint16_t reqOffsetInBatch) const {
  // Pre-allocated buffers scheme:
  // |first client's first buffer|...|first client's last buffer|......
  // |last client's first buffer|...|last client's last buffer|
  // First client id starts after the last replica id.
  // First buffer offset = numOfReplicas_ * batchSize_
  // The number of buffers per client comes from the configuration parameter clientBatchingMaxMsgsNbr.
  const auto bufferOffset =
      (clientId - numOfReplicas_ - numOfInternalClients_) * clientMaxBatchSize_ + reqOffsetInBatch;
  LOG_DEBUG(logger(), KVLOG(clientId, reqSeqNum, reqOffsetInBatch, bufferOffset));
  return preProcessResultBuffers_[bufferOffset].data();
}

const uint16_t PreProcessor::getOngoingReqIndex(uint16_t clientId, uint16_t reqOffsetInBatch) const {
  // Index for ongoing requests starts from the first_client_id * batchSize_, e.g 28 * 10 = 280 (not from 0)
  const auto ongoingReqIndex = clientId * clientMaxBatchSize_ + reqOffsetInBatch;
  LOG_DEBUG(logger(), KVLOG(clientId, reqOffsetInBatch, ongoingReqIndex));
  return ongoingReqIndex;
}

// Primary replica: ask all replicas to pre-process the request
void PreProcessor::sendPreProcessRequestToAllReplicas(const PreProcessRequestMsgSharedPtr &preProcessReqMsg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.sendPreProcessRequestToAllReplicas);
  const set<ReplicaId> &idsOfPeerReplicas = myReplica_.getIdsOfPeerReplicas();
  SCOPED_MDC_CID(preProcessReqMsg->getCid());
  for (auto destId : idsOfPeerReplicas) {
    if (destId != myReplicaId_) {
      // sendMsg works asynchronously, so we can launch it sequentially here
      LOG_DEBUG(logger(),
                "Sending PreProcessRequestMsg clientId: " << preProcessReqMsg->clientId()
                                                          << ", reqSeqNum: " << preProcessReqMsg->reqSeqNum()
                                                          << ", to the replica: " << destId);
      sendMsg(preProcessReqMsg->body(), destId, preProcessReqMsg->type(), preProcessReqMsg->size());
    }
  }
}

void PreProcessor::setPreprocessingRightNow(uint16_t clientId, uint16_t reqOffsetInBatch, bool set) {
  const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
  lock_guard<mutex> lock(reqEntry->mutex);
  if (reqEntry->reqProcessingStatePtr) reqEntry->reqProcessingStatePtr->setPreprocessingRightNow(set);
}

void PreProcessor::launchAsyncReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                  bool isPrimary,
                                                  bool isRetry,
                                                  TimeRecorder &&time_recorder) {
  setPreprocessingRightNow(preProcessReqMsg->clientId(), preProcessReqMsg->reqOffsetInBatch(), true);
  auto *preProcessJob = new AsyncPreProcessJob(*this, preProcessReqMsg, isPrimary, isRetry, std::move(time_recorder));
  threadPool_.add(preProcessJob);
}

uint32_t PreProcessor::launchReqPreProcessing(uint16_t clientId,
                                              uint16_t reqOffsetInBatch,
                                              const string &cid,
                                              ReqId reqSeqNum,
                                              uint32_t reqLength,
                                              char *reqBuf,
                                              const concordUtils::SpanContext &span_context) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.launchReqPreProcessing);
  // Unused for now. Replica Specific Info not currently supported in pre-execution.
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_process_preprocess_msg");
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), "Pass request for a pre-execution" << KVLOG(reqSeqNum, clientId, reqOffsetInBatch, reqSeqNum));
  bftEngine::IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  accumulatedRequests.push_back(bftEngine::IRequestsHandler::ExecutionRequest{
      clientId,
      reqSeqNum,
      PRE_PROCESS_FLAG,
      reqLength,
      reqBuf,
      maxPreExecResultSize_,
      (char *)getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch)});
  requestsHandler_.execute(accumulatedRequests, cid, span);
  const IRequestsHandler::ExecutionRequest &request = accumulatedRequests.back();
  const auto status = request.outExecutionStatus;
  const auto resultLen = request.outActualReplySize;
  LOG_DEBUG(logger(), "Pre-execution operation done" << KVLOG(reqSeqNum, clientId, reqOffsetInBatch, reqSeqNum));
  if (status != 0 || !resultLen) {
    LOG_FATAL(logger(), "Pre-execution failed!" << KVLOG(clientId, reqOffsetInBatch, reqSeqNum, status, resultLen));
    ConcordAssert(false);
  }
  return resultLen;
}

// For test purposes
ReqId PreProcessor::getOngoingReqIdForClient(uint16_t clientId, uint16_t reqOffsetInBatch) {
  const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
  lock_guard<mutex> lock(reqEntry->mutex);
  if (reqEntry->reqProcessingStatePtr) return reqEntry->reqProcessingStatePtr->getReqSeqNum();
  return 0;
}

PreProcessingResult PreProcessor::handlePreProcessedReqByPrimaryAndGetConsensusResult(uint16_t clientId,
                                                                                      uint16_t reqOffsetInBatch,
                                                                                      uint32_t resultBufLen) {
  const auto &reqEntry = ongoingRequests_[getOngoingReqIndex(clientId, reqOffsetInBatch)];
  lock_guard<mutex> lock(reqEntry->mutex);
  if (reqEntry->reqProcessingStatePtr) {
    reqEntry->reqProcessingStatePtr->handlePrimaryPreProcessed(
        getPreProcessResultBuffer(clientId, reqEntry->reqProcessingStatePtr->getReqSeqNum(), reqOffsetInBatch),
        resultBufLen);
    return reqEntry->reqProcessingStatePtr->definePreProcessingConsensusResult();
  }
  return NONE;
}

void PreProcessor::handlePreProcessedReqPrimaryRetry(NodeIdType clientId,
                                                     uint16_t reqOffsetInBatch,
                                                     uint32_t resultBufLen) {
  if (handlePreProcessedReqByPrimaryAndGetConsensusResult(clientId, reqOffsetInBatch, resultBufLen) == COMPLETE)
    finalizePreProcessing(clientId, reqOffsetInBatch);
  else
    cancelPreProcessing(clientId, reqOffsetInBatch);
}

void PreProcessor::handlePreProcessedReqByPrimary(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                  uint16_t clientId,
                                                  uint32_t resultBufLen) {
  const uint16_t &reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.handlePreProcessedReqByPrimary);
  const PreProcessingResult result =
      handlePreProcessedReqByPrimaryAndGetConsensusResult(clientId, reqOffsetInBatch, resultBufLen);
  if (result != NONE)
    handlePreProcessReplyMsg(
        preProcessReqMsg->getCid(), result, clientId, reqOffsetInBatch, preProcessReqMsg->reqSeqNum());
}

void PreProcessor::handlePreProcessedReqByNonPrimary(uint16_t clientId,
                                                     uint16_t reqOffsetInBatch,
                                                     ReqId reqSeqNum,
                                                     uint64_t reqRetryId,
                                                     uint32_t resBufLen,
                                                     const std::string &cid) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.handlePreProcessedReqByNonPrimary);
  setPreprocessingRightNow(clientId, reqOffsetInBatch, false);
  auto replyMsg =
      make_shared<PreProcessReplyMsg>(&histograms_, myReplicaId_, clientId, reqOffsetInBatch, reqSeqNum, reqRetryId);
  replyMsg->setupMsgBody(getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch), resBufLen, cid, STATUS_GOOD);
  // Release the request before sending a reply to the primary to be able accepting new messages
  releaseClientPreProcessRequestSafe(clientId, reqOffsetInBatch, COMPLETE);
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
  LOG_INFO(logger(),
           "Pre-processing completed by a non-primary replica"
               << KVLOG(reqSeqNum, clientId, reqOffsetInBatch, cid, reqRetryId, myReplica_.currentPrimary()));
}

void PreProcessor::handleReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                             bool isPrimary,
                                             bool isRetry) {
  const string cid = preProcessReqMsg->getCid();
  const uint16_t &clientId = preProcessReqMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  const auto &span_context = preProcessReqMsg->spanContext<PreProcessRequestMsgSharedPtr::element_type>();
  uint32_t actualResultBufLen = launchReqPreProcessing(clientId,
                                                       preProcessReqMsg->reqOffsetInBatch(),
                                                       cid,
                                                       reqSeqNum,
                                                       preProcessReqMsg->requestLength(),
                                                       preProcessReqMsg->requestBuf(),
                                                       span_context);
  if (isPrimary && isRetry) {
    handlePreProcessedReqPrimaryRetry(clientId, reqOffsetInBatch, actualResultBufLen);
    return;
  }
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), "Request pre-processed" << KVLOG(isPrimary, reqSeqNum, clientId, reqOffsetInBatch));
  if (isPrimary) {
    pm_->Delay<concord::performance::SlowdownPhase::PreProcessorAfterPreexecPrimary>();
    handlePreProcessedReqByPrimary(preProcessReqMsg, clientId, actualResultBufLen);
  } else {
    pm_->Delay<concord::performance::SlowdownPhase::PreProcessorAfterPreexecNonPrimary>();
    handlePreProcessedReqByNonPrimary(clientId,
                                      reqOffsetInBatch,
                                      reqSeqNum,
                                      preProcessReqMsg->reqRetryId(),
                                      actualResultBufLen,
                                      preProcessReqMsg->getCid());
  }
}

//**************** Class AsyncPreProcessJob ****************//

AsyncPreProcessJob::AsyncPreProcessJob(PreProcessor &preProcessor,
                                       const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                       bool isPrimary,
                                       bool isRetry,
                                       TimeRecorder &&time_recorder)
    : preProcessor_(preProcessor),
      preProcessReqMsg_(preProcessReqMsg),
      isPrimary_(isPrimary),
      isRetry_(isRetry),
      time_recorder_(std::move(time_recorder)) {}

void AsyncPreProcessJob::execute() {
  MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(preProcessor_.myReplicaId_));
  MDC_PUT(MDC_THREAD_KEY, "async-preprocess");
  preProcessor_.handleReqPreProcessingJob(preProcessReqMsg_, isPrimary_, isRetry_);
}

void AsyncPreProcessJob::release() { delete this; }

}  // namespace preprocessor
