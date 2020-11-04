// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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

//**************** Class PreProcessor ****************//

vector<shared_ptr<PreProcessor>> PreProcessor::preProcessors_;

//**************** Static functions ****************//

void PreProcessor::addNewPreProcessor(shared_ptr<MsgsCommunicator> &msgsCommunicator,
                                      shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                                      shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                                      bftEngine::IRequestsHandler &requestsHandler,
                                      InternalReplicaApi &replica,
                                      concordUtil::Timers &timers) {
  if (ReplicaConfig::instance().getnumOfExternalClients() + ReplicaConfig::instance().getnumOfClientProxies() <= 0) {
    LOG_ERROR(logger(), "Wrong configuration: a number of clients could not be zero!");
    return;
  }

  if (ReplicaConfig::instance().getpreExecutionFeatureEnabled())
    preProcessors_.push_back(make_unique<PreProcessor>(
        msgsCommunicator, incomingMsgsStorage, msgHandlersRegistrator, requestsHandler, replica, timers));
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
                           concordUtil::Timers &timers)
    : msgsCommunicator_(msgsCommunicator),
      incomingMsgsStorage_(incomingMsgsStorage),
      msgHandlersRegistrator_(msgHandlersRegistrator),
      requestsHandler_(requestsHandler),
      myReplica_(myReplica),
      myReplicaId_(myReplica.getReplicaConfig().replicaId),
      maxPreExecResultSize_(myReplica.getReplicaConfig().maxExternalMessageSize),
      idsOfPeerReplicas_(myReplica.getIdsOfPeerReplicas()),
      numOfReplicas_(myReplica.getReplicaConfig().numReplicas),
      numOfClients_(myReplica.getReplicaConfig().numOfExternalClients +
                    myReplica_.getReplicaConfig().numOfClientProxies),
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
                           metricsComponent_.RegisterGauge("PreProcInFlyRequestsNum", 0)},
      preExecReqStatusCheckPeriodMilli_(myReplica_.getReplicaConfig().preExecReqStatusCheckTimerMillisec),
      timers_{timers} {
  registerMsgHandlers();
  metricsComponent_.Register();
  sigManager_ = make_shared<SigManager>(myReplicaId_,
                                        numOfReplicas_ + numOfClients_,
                                        myReplica.getReplicaConfig().replicaPrivateKey,
                                        myReplica.getReplicaConfig().publicKeysOfReplicas);
  const uint16_t firstClientId = numOfReplicas_;
  for (auto i = 0; i < numOfClients_; i++) {
    // Placeholders for all clients
    ongoingRequests_[firstClientId + i] = make_shared<ClientRequestState>();
  }
  // Allocate a buffer for the pre-execution result per client
  for (auto id = 0; id < numOfClients_; id++) {
    preProcessResultBuffers_.push_back(Sliver(new char[maxPreExecResultSize_], maxPreExecResultSize_));
  }
  uint64_t numOfThreads = myReplica.getReplicaConfig().preExecConcurrencyLevel;
  if (!numOfThreads) numOfThreads = numOfClients_;
  threadPool_.start(numOfThreads);
  LOG_INFO(logger(),
           KVLOG(firstClientId, numOfClients_, maxPreExecResultSize_, preExecReqStatusCheckPeriodMilli_, numOfThreads));
  RequestProcessingState::init(numOfRequiredReplies());
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
}

void PreProcessor::cancelTimers() {
  try {
    if (preExecReqStatusCheckPeriodMilli_ != 0) timers_.cancel(requestsStatusCheckTimer_);
  } catch (std::invalid_argument &e) {
  }
}

// This function should be always called under a clientEntry->mutex lock
// Resend PreProcessRequestMsg to replicas that have previously rejected it
void PreProcessor::resendPreProcessRequest(const RequestProcessingStateUniquePtr &clientReqStatePtr) {
  const auto &rejectedReplicasList = clientReqStatePtr->getRejectedReplicasList();
  const auto &preProcessReqMsg = clientReqStatePtr->getPreProcessRequest();
  if (!rejectedReplicasList.empty() && preProcessReqMsg) {
    SCOPED_MDC_CID(preProcessReqMsg->getCid());
    const auto &clientId = preProcessReqMsg->clientId();
    const auto &reqSeqNum = preProcessReqMsg->reqSeqNum();
    for (const auto &destId : rejectedReplicasList) {
      LOG_DEBUG(logger(), "Resending PreProcessRequestMsg" << KVLOG(clientId, reqSeqNum, destId));
      sendMsg(preProcessReqMsg->body(), destId, preProcessReqMsg->type(), preProcessReqMsg->size());
    }
    clientReqStatePtr->resetRejectedReplicasList();
  }
}

void PreProcessor::onRequestsStatusCheckTimer() {
  // Pass through all ongoing requests and abort the pre-execution for those that are timed out.
  for (const auto &clientEntry : ongoingRequests_) {
    lock_guard<mutex> lock(clientEntry.second->mutex);
    const auto &clientReqStatePtr = clientEntry.second->reqProcessingStatePtr;
    if (clientReqStatePtr) {
      if (clientReqStatePtr->isReqTimedOut()) {
        preProcessorMetrics_.preProcessRequestTimedout.Get().Inc();
        preProcessorMetrics_.preProcPossiblePrimaryFaultDetected.Get().Inc();
        // The request could expire do to failed primary replica, let ReplicaImp to address that
        // TBD YS: This causes a request to retry in case the primary is OK. Consider passing a kind of NOOP message.
        const auto &clientId = clientEntry.first;
        const auto &reqSeqNum = clientReqStatePtr->getReqSeqNum();
        SCOPED_MDC_CID(clientReqStatePtr->getReqCid());
        LOG_INFO(logger(), "Let replica to handle request" << KVLOG(reqSeqNum, clientId));
        incomingMsgsStorage_->pushExternalMsg(clientReqStatePtr->buildClientRequestMsg(true));
        releaseClientPreProcessRequest(clientEntry.second, clientId, CANCEL);
      } else if (myReplica_.isCurrentPrimary() && clientReqStatePtr->definePreProcessingConsensusResult() == CONTINUE)
        resendPreProcessRequest(clientReqStatePtr);
    }
  }
}

bool PreProcessor::checkClientMsgCorrectness(const ClientPreProcessReqMsgUniquePtr &clientReqMsg,
                                             ReqId reqSeqNum) const {
  SCOPED_MDC_CID(clientReqMsg->getCid());
  if (myReplica_.isCollectingState()) {
    LOG_INFO(logger(),
             "Ignore ClientPreProcessRequestMsg as the replica is collecting missing state from other replicas"
                 << KVLOG(reqSeqNum));
    return false;
  }
  if (clientReqMsg->isReadOnly()) {
    LOG_INFO(logger(), "Ignore ClientPreProcessRequestMsg as it is signed as read-only" << KVLOG(reqSeqNum));
    return false;
  }
  const bool &invalidClient = !myReplica_.isValidClient(clientReqMsg->clientProxyId());
  const bool &sentFromReplicaToNonPrimary =
      myReplica_.isIdOfReplica(clientReqMsg->senderId()) && !myReplica_.isCurrentPrimary();
  if (invalidClient || sentFromReplicaToNonPrimary) {
    LOG_WARN(
        logger(),
        "Ignore ClientPreProcessRequestMsg as invalid" << KVLOG(reqSeqNum, invalidClient, sentFromReplicaToNonPrimary));
    return false;
  }
  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(), "Ignore ClientPreProcessRequestMsg as current view is inactive" << KVLOG(reqSeqNum));
    return false;
  }
  return true;
}

void PreProcessor::updateAggregatorAndDumpMetrics() {
  if (preProcessorMetrics_.preProcReqReceived.Get().Get() % 10 == 0) {
    metricsComponent_.UpdateAggregator();
    auto currTime =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
    if (currTime - metricsLastDumpTime_ >= metricsDumpIntervalInSec_) {
      metricsLastDumpTime_ = currTime;
      LOG_INFO(logger(), "--preProcessor metrics dump--" + metricsComponent_.ToJson());
    }
  }
}

void PreProcessor::sendRejectPreProcessReplyMsg(NodeIdType clientId,
                                                NodeIdType senderId,
                                                SeqNum reqSeqNum,
                                                SeqNum ongoingReqSeqNum,
                                                uint64_t reqRetryId,
                                                const string &cid,
                                                const string &ongoingCid) {
  auto replyMsg = make_shared<PreProcessReplyMsg>(sigManager_, myReplicaId_, clientId, reqSeqNum, reqRetryId);
  replyMsg->setupMsgBody(getPreProcessResultBuffer(clientId), 0, cid, STATUS_REJECT);
  LOG_DEBUG(
      logger(),
      KVLOG(reqSeqNum, senderId, clientId, ongoingReqSeqNum, ongoingCid)
          << " Sending PreProcessReplyMsg with STATUS_REJECT as another PreProcessRequest from the same client is "
             "in progress");
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
}

template <>
void PreProcessor::onMessage<ClientPreProcessRequestMsg>(ClientPreProcessRequestMsg *msg) {
  updateAggregatorAndDumpMetrics();
  preProcessorMetrics_.preProcReqReceived.Get().Inc();
  ClientPreProcessReqMsgUniquePtr clientPreProcessReqMsg(msg);

  SCOPED_MDC_CID(clientPreProcessReqMsg->getCid());
  const NodeIdType &senderId = clientPreProcessReqMsg->senderId();
  const NodeIdType &clientId = clientPreProcessReqMsg->clientProxyId();
  const ReqId &reqSeqNum = clientPreProcessReqMsg->requestSeqNum();
  const auto &reqTimeoutMilli = clientPreProcessReqMsg->requestTimeoutMilli();
  LOG_DEBUG(logger(), "Received ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, clientId, senderId, reqTimeoutMilli));
  if (!checkClientMsgCorrectness(clientPreProcessReqMsg, reqSeqNum)) {
    preProcessorMetrics_.preProcReqIgnored.Get().Inc();
    return;
  }
  PreProcessRequestMsgSharedPtr preProcessRequestMsg;
  bool registerSucceeded = false;
  ReqId seqNumberOfLastReply = 0;
  {
    const auto &clientEntry = ongoingRequests_[clientId];
    lock_guard<mutex> lock(clientEntry->mutex);
    if (clientEntry->reqProcessingStatePtr) {
      const auto &ongoingReqSeqNum = clientEntry->reqProcessingStatePtr->getReqSeqNum();
      const auto &ongoingCid = clientEntry->reqProcessingStatePtr->getReqCid();
      LOG_DEBUG(logger(),
                KVLOG(reqSeqNum, clientId, senderId)
                    << " is ignored:" << KVLOG(ongoingReqSeqNum, ongoingCid) << " is in progress");
      preProcessorMetrics_.preProcReqIgnored.Get().Inc();
      return;
    }
    // Verify that a request is not passing consensus/PostExec right now. The primary replica should not ignore client
    // requests forwarded by non-primary replicas, otherwise this will cause timeouts caused by missing PreProcess
    // request messages from the primary.
    if ((clientId == senderId) && myReplica_.isClientRequestInProcess(clientId, reqSeqNum)) {
      LOG_DEBUG(logger(),
                "Specified request has been processing right now - ignore" << KVLOG(reqSeqNum, clientId, senderId));
      preProcessorMetrics_.preProcReqIgnored.Get().Inc();
      return;
    }
    seqNumberOfLastReply = myReplica_.seqNumberOfLastReplyToClient(clientId);
    if (seqNumberOfLastReply == reqSeqNum) {
      LOG_INFO(logger(),
               "Request has already been executed - let replica to decide how to proceed further"
                   << KVLOG(reqSeqNum, clientId));
      return incomingMsgsStorage_->pushExternalMsg(clientPreProcessReqMsg->convertToClientRequestMsg(false));
    }
    if (seqNumberOfLastReply < reqSeqNum)
      registerSucceeded = registerReplicaDependentRequest(
          move(clientPreProcessReqMsg), preProcessRequestMsg, ++(clientEntry->reqRetryId));
  }

  if (myReplica_.isCurrentPrimary() && registerSucceeded)
    return handleClientPreProcessRequestByPrimary(preProcessRequestMsg);

  LOG_DEBUG(logger(),
            "ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, clientId, senderId)
                                         << " is ignored because request is old/duplicated");
  preProcessorMetrics_.preProcReqIgnored.Get().Inc();
}  // namespace preprocessor

// Non-primary replica request handling
template <>
void PreProcessor::onMessage<PreProcessRequestMsg>(PreProcessRequestMsg *msg) {
  SCOPED_MDC_CID(msg->getCid());
  PreProcessRequestMsgSharedPtr preProcessReqMsg(msg);
  const NodeIdType &senderId = preProcessReqMsg->senderId();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  const NodeIdType &clientId = preProcessReqMsg->clientId();
  LOG_DEBUG(logger(), "Received PreProcessRequestMsg" << KVLOG(reqSeqNum, senderId, clientId));

  if (myReplica_.isCollectingState()) {
    LOG_INFO(logger(),
             "Ignore PreProcessRequestMsg as the replica is collecting missing state from other replicas"
                 << KVLOG(reqSeqNum));
    return;
  }

  if (myReplica_.isCurrentPrimary()) {
    LOG_WARN(logger(),
             "Ignore PreProcessRequestMsg as current replica is the primary" << KVLOG(reqSeqNum, senderId, clientId));
    return;
  }

  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(), "Ignore PreProcessRequestMsg as current view is inactive" << KVLOG(reqSeqNum));
    return;
  }
  bool registerSucceeded = false;
  {
    const auto &clientEntry = ongoingRequests_[clientId];
    lock_guard<mutex> lock(clientEntry->mutex);
    if (clientEntry->reqProcessingStatePtr && clientEntry->reqProcessingStatePtr->getPreProcessRequest()) {
      auto const &ongoingReqSeqNum = clientEntry->reqProcessingStatePtr->getPreProcessRequest()->reqSeqNum();
      auto const &ongoingCid = clientEntry->reqProcessingStatePtr->getPreProcessRequest()->getCid();
      LOG_DEBUG(logger(),
                KVLOG(reqSeqNum, ongoingReqSeqNum, ongoingCid, senderId, clientId)
                    << " Another PreProcessRequest from the same client is in progress. Sending PreProcessReplyMsg "
                       "with STATUS_REJECT");
      return sendRejectPreProcessReplyMsg(
          clientId, senderId, reqSeqNum, ongoingReqSeqNum, msg->reqRetryId(), msg->getCid(), ongoingCid);
    }
    registerSucceeded = registerRequest(ClientPreProcessReqMsgUniquePtr(), preProcessReqMsg);
  }
  if (registerSucceeded)
    // Pre-process the request, calculate a hash of the result and send a reply back
    launchAsyncReqPreProcessingJob(preProcessReqMsg, false, false);
}

// Primary replica handling
template <>
void PreProcessor::onMessage<PreProcessReplyMsg>(PreProcessReplyMsg *msg) {
  PreProcessReplyMsgSharedPtr preProcessReplyMsg(msg);
  const NodeIdType &senderId = preProcessReplyMsg->senderId();
  const NodeIdType &clientId = preProcessReplyMsg->clientId();
  const SeqNum &reqSeqNum = preProcessReplyMsg->reqSeqNum();
  string cid = preProcessReplyMsg->getCid();
  const auto &status = preProcessReplyMsg->status();
  string replyStatus = "STATUS_GOOD";
  if (status == STATUS_REJECT) replyStatus = "STATUS_REJECT";
  PreProcessingResult result = CANCEL;
  {
    SCOPED_MDC_CID(cid);
    LOG_DEBUG(logger(), "Received PreProcessReplyMsg" << KVLOG(reqSeqNum, senderId, clientId, replyStatus));
    const auto &clientEntry = ongoingRequests_[clientId];
    lock_guard<mutex> lock(clientEntry->mutex);
    if (!clientEntry->reqProcessingStatePtr || clientEntry->reqProcessingStatePtr->getReqSeqNum() != reqSeqNum) {
      // Look for the request in the requests history and check for the non-determinism
      for (const auto &oldReqState : clientEntry->reqProcessingHistory)
        if (oldReqState->getReqSeqNum() == reqSeqNum)
          oldReqState->detectNonDeterministicPreProcessing(
              preProcessReplyMsg->resultsHash(), preProcessReplyMsg->senderId(), preProcessReplyMsg->reqRetryId());
      LOG_DEBUG(logger(),
                KVLOG(reqSeqNum, senderId, clientId)
                    << " will be ignored as no such ongoing request exists or different one found for this client");
      return;
    }
    clientEntry->reqProcessingStatePtr->handlePreProcessReplyMsg(preProcessReplyMsg);
    if (status == STATUS_REJECT) {
      LOG_DEBUG(logger(), "Received PreProcessReplyMsg with STATUS_REJECT" << KVLOG(reqSeqNum, senderId, clientId));
      return;
    }
    result = clientEntry->reqProcessingStatePtr->definePreProcessingConsensusResult();
    if (result == CONTINUE) resendPreProcessRequest(clientEntry->reqProcessingStatePtr);
  }
  handlePreProcessReplyMsg(cid, result, clientId, reqSeqNum);
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
  trueTypeObj->setSigManager(sigManager_);
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
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessRequest,
                                              bind(&PreProcessor::messageHandler<PreProcessRequestMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessReply,
                                              bind(&PreProcessor::messageHandler<PreProcessReplyMsg>, this, _1));
}

void PreProcessor::handlePreProcessReplyMsg(const string &cid,
                                            PreProcessingResult result,
                                            NodeIdType clientId,
                                            SeqNum reqSeqNum) {
  SCOPED_MDC_CID(cid);
  switch (result) {
    case NONE:      // No action required - pre-processing has been already completed
    case CONTINUE:  // Not enough equal hashes collected
      break;
    case COMPLETE:  // Pre-processing consensus reached
      finalizePreProcessing(clientId);
      break;
    case CANCEL:  // Pre-processing consensus not reached
      cancelPreProcessing(clientId);
      break;
    case RETRY_PRIMARY:  // Primary replica generated pre-processing result hash different that one passed consensus
      LOG_INFO(logger(), "Retry primary replica pre-processing for" << KVLOG(reqSeqNum, clientId));
      PreProcessRequestMsgSharedPtr preProcessRequestMsg;
      {
        const auto &clientEntry = ongoingRequests_[clientId];
        lock_guard<mutex> lock(clientEntry->mutex);
        if (clientEntry->reqProcessingStatePtr)
          preProcessRequestMsg = clientEntry->reqProcessingStatePtr->getPreProcessRequest();
      }
      if (preProcessRequestMsg) launchAsyncReqPreProcessingJob(preProcessRequestMsg, true, true);
  }
}

void PreProcessor::cancelPreProcessing(NodeIdType clientId) {
  preProcessorMetrics_.preProcConsensusNotReached.Get().Inc();
  SeqNum reqSeqNum = 0;
  const auto &clientEntry = ongoingRequests_[clientId];
  {
    lock_guard<mutex> lock(clientEntry->mutex);
    if (clientEntry->reqProcessingStatePtr) {
      reqSeqNum = clientEntry->reqProcessingStatePtr->getReqSeqNum();
      SCOPED_MDC_CID(clientEntry->reqProcessingStatePtr->getReqCid());
      releaseClientPreProcessRequest(clientEntry, clientId, CANCEL);
      LOG_WARN(logger(), "Pre-processing consensus not reached - cancel request" << KVLOG(reqSeqNum, clientId));
    }
  }
}

void PreProcessor::finalizePreProcessing(NodeIdType clientId) {
  unique_ptr<ClientRequestMsg> clientRequestMsg;
  const auto &clientEntry = ongoingRequests_[clientId];
  {
    lock_guard<mutex> lock(clientEntry->mutex);
    auto &reqProcessingStatePtr = clientEntry->reqProcessingStatePtr;
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
      LOG_DEBUG(logger(), "Pass pre-processed request to the replica" << KVLOG(cid, reqSeqNum, clientId));
      incomingMsgsStorage_->pushExternalMsg(move(clientRequestMsg));
      preProcessorMetrics_.preProcReqSentForFurtherProcessing.Get().Inc();
      releaseClientPreProcessRequest(clientEntry, clientId, COMPLETE);
      LOG_INFO(logger(), "Pre-processing completed for" << KVLOG(cid, reqSeqNum, clientId));
    }
  }
}

uint16_t PreProcessor::numOfRequiredReplies() { return myReplica_.getReplicaConfig().fVal; }

// This function should be always called under a clientEntry->mutex lock
bool PreProcessor::registerRequest(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                   PreProcessRequestMsgSharedPtr preProcessRequestMsg) {
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
  // Only one request is supported per client for now
  const auto &clientEntry = ongoingRequests_[clientId];
  if (!clientEntry->reqProcessingStatePtr)
    clientEntry->reqProcessingStatePtr = make_unique<RequestProcessingState>(
        numOfReplicas_, clientId, cid, reqSeqNum, move(clientReqMsg), preProcessRequestMsg);
  else if (!clientEntry->reqProcessingStatePtr->getPreProcessRequest())
    // The request was registered before as arrived directly from the client
    clientEntry->reqProcessingStatePtr->setPreProcessRequest(preProcessRequestMsg);
  else {
    LOG_WARN(logger(),
             KVLOG(reqSeqNum) << " could not be registered: the entry for" << KVLOG(clientId)
                              << " is occupied by reqSeqNum: " << clientEntry->reqProcessingStatePtr->getReqSeqNum());
    return false;
  }
  if (clientReqMsgSpecified) {
    LOG_DEBUG(logger(), KVLOG(reqSeqNum, clientId) << " registered ClientPreProcessReqMsg");
  } else {
    LOG_DEBUG(logger(), KVLOG(reqSeqNum, clientId) << " registered PreProcessRequestMsg");
  }
  preProcessorMetrics_.preProcInFlyRequestsNum.Get().Inc();
  return true;
}

void PreProcessor::releaseClientPreProcessRequestSafe(uint16_t clientId, PreProcessingResult result) {
  const auto &clientEntry = ongoingRequests_[clientId];
  lock_guard<mutex> lock(clientEntry->mutex);
  releaseClientPreProcessRequest(clientEntry, clientId, result);
}

// This function should be always called under a clientEntry->mutex lock
void PreProcessor::releaseClientPreProcessRequest(const ClientRequestStateSharedPtr &clientEntry,
                                                  uint16_t clientId,
                                                  PreProcessingResult result) {
  auto &givenReq = clientEntry->reqProcessingStatePtr;
  if (givenReq) {
    SeqNum requestSeqNum = 0;
    if (result == COMPLETE) {
      if (clientEntry->reqProcessingHistory.size() >= clientEntry->reqProcessingHistoryHeight) {
        auto &removeFromHistoryReq = clientEntry->reqProcessingHistory.front();
        SCOPED_MDC_CID(removeFromHistoryReq->getReqCid());
        requestSeqNum = removeFromHistoryReq->getReqSeqNum();
        LOG_DEBUG(logger(), KVLOG(requestSeqNum, clientId) << " removed from the history");
        removeFromHistoryReq.reset();
        clientEntry->reqProcessingHistory.pop_front();
      }
      SCOPED_MDC_CID(givenReq->getReqCid());
      requestSeqNum = givenReq->getReqSeqNum();
      LOG_DEBUG(logger(), KVLOG(requestSeqNum, clientId) << " released and moved to the history");
      // No need to keep whole messages in the memory => release them before archiving
      givenReq->releaseResources();
      clientEntry->reqProcessingHistory.push_back(move(givenReq));
    } else {  // No consensus reached => release request
      SCOPED_MDC_CID(givenReq->getReqCid());
      LOG_INFO(logger(), KVLOG(requestSeqNum, clientId) << " no consensus reached, request released");
      givenReq.reset();
    }
    preProcessorMetrics_.preProcInFlyRequestsNum.Get().Dec();
  }
}

void PreProcessor::sendMsg(char *msg, NodeIdType dest, uint16_t msgType, MsgSize msgSize) {
  int errorCode = msgsCommunicator_->sendAsyncMessage(dest, msg, msgSize);
  if (errorCode != 0) {
    LOG_ERROR(logger(), "sendMsg: sendAsyncMessage returned error: " << errorCode << " for" << KVLOG(msgType));
  }
}

// This function should be called under a clientEntry->mutex lock
bool PreProcessor::registerReplicaDependentRequest(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                                   PreProcessRequestMsgSharedPtr &preProcessRequestMsg,
                                                   uint64_t reqRetryId) {
  if (myReplica_.isCurrentPrimary()) {
    preProcessRequestMsg =
        make_shared<PreProcessRequestMsg>(myReplicaId_,
                                          clientReqMsg->clientProxyId(),
                                          clientReqMsg->requestSeqNum(),
                                          reqRetryId,
                                          clientReqMsg->requestLength(),
                                          clientReqMsg->requestBuf(),
                                          clientReqMsg->getCid(),
                                          clientReqMsg->spanContext<ClientPreProcessReqMsgUniquePtr::element_type>());
    return registerRequest(move(clientReqMsg), preProcessRequestMsg);
  }
  handleClientPreProcessRequestByNonPrimary(move(clientReqMsg));
  return true;
}

// Primary replica: start client request handling
void PreProcessor::handleClientPreProcessRequestByPrimary(PreProcessRequestMsgSharedPtr preProcessRequestMsg) {
  const auto &reqSeqNum = preProcessRequestMsg->reqSeqNum();
  const auto &clientId = preProcessRequestMsg->clientId();
  const auto &senderId = preProcessRequestMsg->senderId();
  LOG_INFO(logger(),
           "Start request processing by a primary replica"
               << KVLOG(reqSeqNum, preProcessRequestMsg->getCid(), clientId, senderId));
  sendPreProcessRequestToAllReplicas(preProcessRequestMsg);
  // Pre-process the request and calculate a hash of the result
  launchAsyncReqPreProcessingJob(preProcessRequestMsg, true, false);
}

// Non-primary replica: start client request handling
// This function should be called under a clientEntry->mutex lock
void PreProcessor::handleClientPreProcessRequestByNonPrimary(ClientPreProcessReqMsgUniquePtr clientReqMsg) {
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
  if (registerRequest(move(clientReqMsg), PreProcessRequestMsgSharedPtr())) {
    LOG_INFO(logger(),
             "Start request processing by a non-primary replica"
                 << KVLOG(reqSeqNum, cid, clientId, senderId, reqTimeoutMilli));
    sendMsg(msgBody, myReplica_.currentPrimary(), msgType, msgSize);
    LOG_DEBUG(logger(),
              "Sent ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, cid, clientId) << " to the current primary");
  }
}

const char *PreProcessor::getPreProcessResultBuffer(uint16_t clientId) const {
  return preProcessResultBuffers_[getClientReplyBufferId(clientId)].data();
}

// Primary replica: ask all replicas to pre-process the request
void PreProcessor::sendPreProcessRequestToAllReplicas(const PreProcessRequestMsgSharedPtr &preProcessReqMsg) {
  const set<ReplicaId> &idsOfPeerReplicas = myReplica_.getIdsOfPeerReplicas();
  SCOPED_MDC_CID(preProcessReqMsg->getCid());
  for (auto destId : idsOfPeerReplicas) {
    if (destId != myReplicaId_) {
      // sendMsg works asynchronously, so we can launch it sequentially here
      LOG_DEBUG(logger(),
                "Sending PreProcessRequestMsg clientId: " << preProcessReqMsg->clientId()
                                                          << ", requestSeqNum: " << preProcessReqMsg->reqSeqNum()
                                                          << ", to the replica: " << destId);
      sendMsg(preProcessReqMsg->body(), destId, preProcessReqMsg->type(), preProcessReqMsg->size());
    }
  }
}

void PreProcessor::setPreprocessingRightNow(uint16_t clientId, bool set) {
  const auto &clientEntry = ongoingRequests_[clientId];
  lock_guard<mutex> lock(clientEntry->mutex);
  if (clientEntry->reqProcessingStatePtr) clientEntry->reqProcessingStatePtr->setPreprocessingRightNow(set);
}

void PreProcessor::launchAsyncReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                  bool isPrimary,
                                                  bool isRetry) {
  setPreprocessingRightNow(preProcessReqMsg->clientId(), true);
  auto *preProcessJob = new AsyncPreProcessJob(*this, preProcessReqMsg, isPrimary, isRetry);
  threadPool_.add(preProcessJob);
}

uint32_t PreProcessor::launchReqPreProcessing(uint16_t clientId,
                                              const string &cid,
                                              ReqId reqSeqNum,
                                              uint32_t reqLength,
                                              char *reqBuf,
                                              const concordUtils::SpanContext &span_context) {
  uint32_t resultLen = 0;
  // Unused for now. Replica Specific Info not currently supported in pre-execution.
  uint32_t replicaSpecificInfoLen = 0;
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_process_preprocess_msg");
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), "Pass request for a pre-execution" << KVLOG(reqSeqNum, clientId, reqSeqNum));
  auto status = requestsHandler_.execute(clientId,
                                         reqSeqNum,
                                         PRE_PROCESS_FLAG,
                                         reqLength,
                                         reqBuf,
                                         maxPreExecResultSize_,
                                         (char *)getPreProcessResultBuffer(clientId),
                                         resultLen,
                                         replicaSpecificInfoLen,
                                         span);
  LOG_DEBUG(logger(), "Pre-execution operation done" << KVLOG(reqSeqNum, clientId, reqSeqNum));
  if (status != 0 || !resultLen) {
    LOG_FATAL(logger(), "Pre-execution failed!" << KVLOG(clientId, reqSeqNum, status, resultLen));
    ConcordAssert(false);
  }
  return resultLen;
}

ReqId PreProcessor::getOngoingReqIdForClient(uint16_t clientId) {
  const auto &clientEntry = ongoingRequests_[clientId];
  lock_guard<mutex> lock(clientEntry->mutex);
  if (clientEntry->reqProcessingStatePtr) return clientEntry->reqProcessingStatePtr->getReqSeqNum();
  return 0;
}

PreProcessingResult PreProcessor::handlePreProcessedReqByPrimaryAndGetConsensusResult(uint16_t clientId,
                                                                                      uint32_t resultBufLen) {
  const auto &clientEntry = ongoingRequests_[clientId];
  lock_guard<mutex> lock(clientEntry->mutex);
  if (clientEntry->reqProcessingStatePtr) {
    clientEntry->reqProcessingStatePtr->handlePrimaryPreProcessed(getPreProcessResultBuffer(clientId), resultBufLen);
    return clientEntry->reqProcessingStatePtr->definePreProcessingConsensusResult();
  }
  return NONE;
}

void PreProcessor::handlePreProcessedReqPrimaryRetry(NodeIdType clientId, uint32_t resultBufLen) {
  if (handlePreProcessedReqByPrimaryAndGetConsensusResult(clientId, resultBufLen) == COMPLETE)
    finalizePreProcessing(clientId);
  else
    cancelPreProcessing(clientId);
}

void PreProcessor::handlePreProcessedReqByPrimary(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                  uint16_t clientId,
                                                  uint32_t resultBufLen) {
  const PreProcessingResult result = handlePreProcessedReqByPrimaryAndGetConsensusResult(clientId, resultBufLen);
  if (result != NONE)
    handlePreProcessReplyMsg(preProcessReqMsg->getCid(), result, clientId, preProcessReqMsg->reqSeqNum());
}

void PreProcessor::handlePreProcessedReqByNonPrimary(
    uint16_t clientId, ReqId reqSeqNum, uint64_t reqRetryId, uint32_t resBufLen, const std::string &cid) {
  setPreprocessingRightNow(clientId, false);
  auto replyMsg = make_shared<PreProcessReplyMsg>(sigManager_, myReplicaId_, clientId, reqSeqNum, reqRetryId);
  replyMsg->setupMsgBody(getPreProcessResultBuffer(clientId), resBufLen, cid, STATUS_GOOD);
  // Release the request before sending a reply to the primary to be able accepting new messages
  releaseClientPreProcessRequestSafe(clientId, COMPLETE);
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
  LOG_INFO(logger(),
           "Pre-processing completed by a non-primary replica"
               << KVLOG(reqSeqNum, cid, reqRetryId, myReplica_.currentPrimary()));
}

void PreProcessor::handleReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                             bool isPrimary,
                                             bool isRetry) {
  const string cid = preProcessReqMsg->getCid();
  const uint16_t &clientId = preProcessReqMsg->clientId();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  const auto &span_context = preProcessReqMsg->spanContext<PreProcessRequestMsgSharedPtr::element_type>();
  uint32_t actualResultBufLen = launchReqPreProcessing(
      clientId, cid, reqSeqNum, preProcessReqMsg->requestLength(), preProcessReqMsg->requestBuf(), span_context);
  if (isPrimary && isRetry) {
    handlePreProcessedReqPrimaryRetry(clientId, actualResultBufLen);
    return;
  }
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), "Request pre-processed" << KVLOG(isPrimary, reqSeqNum, clientId));
  if (isPrimary)
    handlePreProcessedReqByPrimary(preProcessReqMsg, clientId, actualResultBufLen);
  else
    handlePreProcessedReqByNonPrimary(
        clientId, reqSeqNum, preProcessReqMsg->reqRetryId(), actualResultBufLen, preProcessReqMsg->getCid());
}

//**************** Class AsyncPreProcessJob ****************//

AsyncPreProcessJob::AsyncPreProcessJob(PreProcessor &preProcessor,
                                       const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                       bool isPrimary,
                                       bool isRetry)
    : preProcessor_(preProcessor), preProcessReqMsg_(preProcessReqMsg), isPrimary_(isPrimary), isRetry_(isRetry) {}

void AsyncPreProcessJob::execute() {
  MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(preProcessor_.myReplicaId_));
  MDC_PUT(MDC_THREAD_KEY, "async-preprocess");
  preProcessor_.handleReqPreProcessingJob(preProcessReqMsg_, isPrimary_, isRetry_);
}

void AsyncPreProcessJob::release() { delete this; }

}  // namespace preprocessor
