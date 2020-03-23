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
#include "TimersSingleton.hpp"

namespace preprocessor {

using namespace bftEngine;
using namespace concord::util;
using namespace concordUtils;
using namespace std;
using namespace std::placeholders;
using namespace concordUtil;

//**************** Class PreProcessor ****************//

vector<shared_ptr<PreProcessor>> PreProcessor::preProcessors_;

void PreProcessor::setAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator) {
  if (aggregator) {
    for (const auto &elem : preProcessors_) elem->metricsComponent_.SetAggregator(aggregator);
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

template <typename T>
void PreProcessor::messageHandler(MessageBase *msg) {
  if (validateMessage(msg))
    onMessage<T>(static_cast<T *>(msg));
  else {
    preProcessorMetrics_.preProcReqInvalid.Get().Inc();
    delete msg;
  }
}

bool PreProcessor::validateMessage(MessageBase *msg) const {
  try {
    msg->validate(myReplica_.getReplicasInfo());
    return true;
  } catch (std::exception &e) {
    LOG_WARN(
        GL,
        "Received invalid message from Node " << msg->senderId() << " type=" << msg->type() << " reason: " << e.what());
    return false;
  }
}

PreProcessor::PreProcessor(shared_ptr<MsgsCommunicator> &msgsCommunicator,
                           shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                           shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                           IRequestsHandler &requestsHandler,
                           const InternalReplicaApi &myReplica)
    : msgsCommunicator_(msgsCommunicator),
      incomingMsgsStorage_(incomingMsgsStorage),
      msgHandlersRegistrator_(msgHandlersRegistrator),
      requestsHandler_(requestsHandler),
      myReplica_(myReplica),
      myReplicaId_(myReplica.getReplicaConfig().replicaId),
      maxReplyMsgSize_(myReplica.getReplicaConfig().maxReplyMessageSize - sizeof(ClientReplyMsgHeader)),
      idsOfPeerReplicas_(myReplica.getIdsOfPeerReplicas()),
      numOfReplicas_(myReplica.getReplicaConfig().numReplicas),
      numOfClients_(myReplica.getReplicaConfig().numOfClientProxies),
      metricsComponent_{concordMetrics::Component("preProcessor", std::make_shared<concordMetrics::Aggregator>())},
      metricsLastDumpTime_(0),
      metricsDumpIntervalInSec_{myReplica_.getReplicaConfig().metricsDumpIntervalSeconds},
      preProcessorMetrics_{metricsComponent_.RegisterCounter("preProcReqReceived"),
                           metricsComponent_.RegisterCounter("preProcReqInvalid"),
                           metricsComponent_.RegisterCounter("preProcReqIgnored"),
                           metricsComponent_.RegisterCounter("preProcConsensusNotReached"),
                           metricsComponent_.RegisterCounter("preProcessRequestTimedout"),
                           metricsComponent_.RegisterCounter("preProcReqSentForFurtherProcessing")} {
  registerMsgHandlers();
  metricsComponent_.Register();
  sigManager_ = make_shared<SigManager>(myReplicaId_,
                                        numOfReplicas_ + numOfClients_,
                                        myReplica.getReplicaConfig().replicaPrivateKey,
                                        myReplica.getReplicaConfig().publicKeysOfReplicas);
  const uint16_t firstClientId = numOfReplicas_;
  for (auto i = 0; i < numOfClients_; i++) {
    // Placeholders for all clients
    ongoingRequests_[firstClientId + i] = make_unique<ClientRequestInfo>();
  }
  // Allocate a buffer for the pre-execution result per client
  for (auto id = 0; id < numOfClients_; id++) {
    preProcessResultBuffers_.push_back(Sliver(new char[maxReplyMsgSize_], maxReplyMsgSize_));
  }
  threadPool_.start(numOfClients_);
  requestsStatusCheckTimer_ = TimersSingleton::getInstance().add(
      chrono::milliseconds(myReplica_.getReplicaConfig().preExecReqStatusCheckTimerMillisec),
      Timers::Timer::RECURRING,
      [this](Timers::Handle h) { onRequestsStatusCheckTimer(h); });
}

PreProcessor::~PreProcessor() {
  try {
    TimersSingleton::getInstance().cancel(requestsStatusCheckTimer_);
  } catch (std::invalid_argument &e) {
  }
  threadPool_.stop();
}

void PreProcessor::onRequestsStatusCheckTimer(Timers::Handle timer) {
  // Pass through all ongoing requests and abort those that are timed out.
  for (auto &clientEntry : ongoingRequests_) {
    lock_guard<mutex> lock(clientEntry.second->mutex);
    if (clientEntry.second->clientReqInfoPtr && clientEntry.second->clientReqInfoPtr->isReqTimedOut()) {
      preProcessorMetrics_.preProcessRequestTimedout.Get().Inc();
      releaseClientPreProcessRequest(clientEntry.second, clientEntry.first);
    }
  }
}

bool PreProcessor::checkClientMsgCorrectness(const ClientPreProcessReqMsgUniquePtr &clientReqMsg,
                                             ReqId reqSeqNum) const {
  if (myReplica_.isCollectingState()) {
    LOG_INFO(GL,
             "ClientPreProcessRequestMsg reqSeqNum="
                 << reqSeqNum << " is ignored because the replica is collecting missing state from other replicas");
    return false;
  }
  if (clientReqMsg->isReadOnly()) {
    LOG_INFO(GL,
             "ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum << " is ignored because it is signed as read-only");
    return false;
  }
  const bool &invalidClient = !myReplica_.isValidClient(clientReqMsg->clientProxyId());
  const bool &sentFromReplicaToNonPrimary =
      myReplica_.isIdOfReplica(clientReqMsg->senderId()) && !myReplica_.isCurrentPrimary();
  if (invalidClient || sentFromReplicaToNonPrimary) {
    LOG_INFO(GL,
             "ClientPreProcessRequestMsg reqSeqNum="
                 << reqSeqNum << " is ignored as invalid: invalidClient=" << invalidClient
                 << ", sentFromReplicaToNonPrimary=" << sentFromReplicaToNonPrimary);
    return false;
  }
  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(GL, "ClientPreProcessRequestMsg is ignored because current view is inactive, reqSeqNum=" << reqSeqNum);
    return false;
  }
  return true;
}

template <>
void PreProcessor::onMessage<ClientPreProcessRequestMsg>(ClientPreProcessRequestMsg *msg) {
  if (preProcessorMetrics_.preProcReqReceived.Get().Get() % 10 == 0) {
    metricsComponent_.UpdateAggregator();
    auto currTime =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
    if (currTime - metricsLastDumpTime_ >= metricsDumpIntervalInSec_) {
      metricsLastDumpTime_ = currTime;
      LOG_INFO(GL, "--preProcessor metrics dump--" + metricsComponent_.ToJson());
    }
  }
  preProcessorMetrics_.preProcReqReceived.Get().Inc();
  ClientPreProcessReqMsgUniquePtr clientPreProcessReqMsg(msg);

  MDC_CID_PUT(GL, clientPreProcessReqMsg->getCid());
  const NodeIdType &senderId = clientPreProcessReqMsg->senderId();
  const NodeIdType &clientId = clientPreProcessReqMsg->clientProxyId();
  const ReqId &reqSeqNum = clientPreProcessReqMsg->requestSeqNum();
  LOG_INFO(GL,
           "Received ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum << " from clientId=" << clientId
                                                            << ", senderId=" << senderId);
  if (!checkClientMsgCorrectness(clientPreProcessReqMsg, reqSeqNum)) {
    preProcessorMetrics_.preProcReqIgnored.Get().Inc();
    return;
  }
  {
    auto &clientEntry = ongoingRequests_[clientId];
    lock_guard<mutex> lock(clientEntry->mutex);
    if (clientEntry->clientReqInfoPtr != nullptr) {
      const ReqId &ongoingReqSeqNum = clientEntry->clientReqInfoPtr->getReqSeqNum();
      if (ongoingReqSeqNum != reqSeqNum) {
        LOG_WARN(GL,
                 " reqSeqNum=" << reqSeqNum << " from clientId=" << clientId
                               << " is ignored: previous client request=" << ongoingReqSeqNum << " is in process");
        preProcessorMetrics_.preProcReqIgnored.Get().Inc();
        return;
      }
    }
  }
  const ReqId &seqNumberOfLastReply = myReplica_.seqNumberOfLastReplyToClient(clientId);
  if (seqNumberOfLastReply < reqSeqNum) {
    if (myReplica_.isCurrentPrimary()) {
      return handleClientPreProcessRequest(move(clientPreProcessReqMsg));
    } else {  // Not the current primary => pass it to the primary
      sendMsg(msg->body(), myReplica_.currentPrimary(), msg->type(), msg->size());
      LOG_INFO(GL, "Sending ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum << " to current primary");
      return;
    }
  }
  if (seqNumberOfLastReply == reqSeqNum) {
    LOG_INFO(GL,
             "ClientPreProcessRequestMsg reqSeqNum="
                 << reqSeqNum << " has already been executed - let replica to decide how to proceed further");
    return incomingMsgsStorage_->pushExternalMsg(clientPreProcessReqMsg->convertToClientRequestMsg(false));
  }
  LOG_INFO(GL, "ClientPreProcessRequestMsg reqSeqNum=" << reqSeqNum << " is ignored because request is old");
  preProcessorMetrics_.preProcReqIgnored.Get().Inc();
}

// Non-primary replica request handling
template <>
void PreProcessor::onMessage<PreProcessRequestMsg>(PreProcessRequestMsg *msg) {
  MDC_CID_PUT(GL, msg->getCid());
  PreProcessRequestMsgSharedPtr preProcessReqMsg(msg);
  LOG_DEBUG(
      GL, "Received reqSeqNum=" << preProcessReqMsg->reqSeqNum() << " from senderId=" << preProcessReqMsg->senderId());

  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(GL, "PreProcessRequestMsg is ignored because current view is inactive, reqSeqNum=" << msg->reqSeqNum());
    return;
  }

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
  MDC_CID_PUT(GL, cid);
  auto &clientEntry = ongoingRequests_[clientId];
  PreProcessingResult result = CANCEL;
  LOG_DEBUG(GL, "reqSeqNum=" << preProcessReplyMsg->reqSeqNum() << " received from replica=" << senderId);
  {
    lock_guard<mutex> lock(clientEntry->mutex);
    if (!clientEntry->clientReqInfoPtr || clientEntry->clientReqInfoPtr->getReqSeqNum() != reqSeqNum) {
      LOG_DEBUG(GL,
                "reqSeqNum=" << reqSeqNum << " received from replica=" << preProcessReplyMsg->senderId() << " clientId="
                             << clientId << " will be ignored as no such ongoing request or different one has found");
      return;
    }
    clientEntry->clientReqInfoPtr->handlePreProcessReplyMsg(preProcessReplyMsg);
    result = clientEntry->clientReqInfoPtr->getPreProcessingConsensusResult();
  }
  handleReqPreProcessedByOneReplica(cid, result, clientId, reqSeqNum);
}

void PreProcessor::handleReqPreProcessedByOneReplica(const string &cid,
                                                     PreProcessingResult result,
                                                     NodeIdType clientId,
                                                     SeqNum reqSeqNum) {
  MDC_CID_PUT(GL, cid);
  switch (result) {
    case CONTINUE:  // Not enough equal hashes collected
      break;
    case COMPLETE:  // Pre-processing consensus reached
      finalizePreProcessing(clientId);
      break;
    case CANCEL:  // Pre-processing consensus not reached
      cancelPreProcessing(clientId);
      break;
    case RETRY_PRIMARY:  // Primary replica generated pre-processing result hash different that one passed consensus
      LOG_INFO(GL, "Retry primary replica pre-processing for clientId=" << clientId << " reqSeqNum=" << reqSeqNum);
      PreProcessRequestMsgSharedPtr preProcessRequestMsg;
      {
        auto &clientEntry = ongoingRequests_[clientId];
        lock_guard<mutex> lock(clientEntry->mutex);
        preProcessRequestMsg = clientEntry->clientReqInfoPtr->getPreProcessRequest();
      }
      launchAsyncReqPreProcessingJob(preProcessRequestMsg, true, true);
  }
}

void PreProcessor::cancelPreProcessing(NodeIdType clientId) {
  preProcessorMetrics_.preProcConsensusNotReached.Get().Inc();
  SeqNum reqSeqNum = 0;
  auto &clientEntry = ongoingRequests_[clientId];
  {
    lock_guard<mutex> lock(clientEntry->mutex);
    reqSeqNum = clientEntry->clientReqInfoPtr->getReqSeqNum();
    if (!clientEntry->clientReqInfoPtr)
      incomingMsgsStorage_->pushExternalMsg(clientEntry->clientReqInfoPtr->convertClientPreProcessToClientMsg(true));
    releaseClientPreProcessRequest(clientEntry, clientId);
  }
  LOG_INFO(GL,
           "Pre-processing consensus not reached for clientId="
               << clientId << " reqSeqNum=" << reqSeqNum
               << "; abort pre-processing step and pass the request to the replica for a regular processing");
}

void PreProcessor::finalizePreProcessing(NodeIdType clientId) {
  SeqNum reqSeqNum = 0;
  unique_ptr<ClientRequestMsg> clientRequestMsg;
  auto &clientEntry = ongoingRequests_[clientId];
  {
    lock_guard<mutex> lock(clientEntry->mutex);
    reqSeqNum = clientEntry->clientReqInfoPtr->getReqSeqNum();
    // Copy of the message body is unavoidable here, as we need to create a new message type which lifetime is
    // controlled by the replica while all PreProcessReply messages get released here.
    clientRequestMsg = make_unique<ClientRequestMsg>(clientId,
                                                     HAS_PRE_PROCESSED_FLAG,
                                                     reqSeqNum,
                                                     clientEntry->clientReqInfoPtr->getPrimaryPreProcessedResultLen(),
                                                     clientEntry->clientReqInfoPtr->getPrimaryPreProcessedResult(),
                                                     clientEntry->clientReqInfoPtr->getReqTimeoutMilli(),
                                                     clientEntry->clientReqInfoPtr->getReqCid());
  }
  incomingMsgsStorage_->pushExternalMsg(move(clientRequestMsg));
  preProcessorMetrics_.preProcReqSentForFurtherProcessing.Get().Inc();
  releaseClientPreProcessRequestSafe(clientId);
  LOG_DEBUG(GL, "Pre-processing completed for clientId=" << clientId << " reqSeqNum=" << reqSeqNum);
}

uint16_t PreProcessor::numOfRequiredReplies() { return myReplica_.getReplicaConfig().fVal + 1; }

void PreProcessor::registerRequest(ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                   PreProcessRequestMsgSharedPtr preProcessRequestMsg) {
  const uint16_t &clientId = clientReqMsg->clientProxyId();
  const ReqId &requestSeqNum = clientReqMsg->requestSeqNum();
  {
    // Only one request is supported per client for now
    auto &clientEntry = ongoingRequests_[clientId];
    lock_guard<mutex> lock(clientEntry->mutex);
    clientEntry->clientReqInfoPtr = make_unique<RequestProcessingInfo>(
        numOfReplicas_, numOfRequiredReplies(), requestSeqNum, move(clientReqMsg), preProcessRequestMsg);
  }
  LOG_DEBUG(GL, "clientId=" << clientId << " requestSeqNum=" << requestSeqNum << " registered");
}

void PreProcessor::releaseClientPreProcessRequestSafe(uint16_t clientId) {
  auto &clientEntry = ongoingRequests_[clientId];
  lock_guard<mutex> lock(clientEntry->mutex);
  releaseClientPreProcessRequest(clientEntry, clientId);
}

void PreProcessor::releaseClientPreProcessRequest(ClientRequestInfoSharedPtr clientEntry, uint16_t clientId) {
  LOG_DEBUG(
      GL, "clientId=" << clientId << " requestSeqNum=" << clientEntry->clientReqInfoPtr->getReqSeqNum() << " released");
  clientEntry->clientReqInfoPtr.reset();
}

void PreProcessor::sendMsg(char *msg, NodeIdType dest, uint16_t msgType, MsgSize msgSize) {
  int errorCode = msgsCommunicator_->sendAsyncMessage(dest, msg, msgSize);
  if (errorCode != 0) {
    LOG_ERROR(GL, "sendMsg: sendAsyncMessage returned error=" << errorCode << " for message type=" << msgType);
  }
}

// Primary replica: start client request handling
void PreProcessor::handleClientPreProcessRequest(ClientPreProcessReqMsgUniquePtr clientReqMsg) {
  PreProcessRequestMsgSharedPtr preProcessRequestMsg = make_shared<PreProcessRequestMsg>(myReplicaId_,
                                                                                         clientReqMsg->clientProxyId(),
                                                                                         clientReqMsg->requestSeqNum(),
                                                                                         clientReqMsg->requestLength(),
                                                                                         clientReqMsg->requestBuf(),
                                                                                         clientReqMsg->getCid());
  registerRequest(move(clientReqMsg), preProcessRequestMsg);
  sendPreProcessRequestToAllReplicas(preProcessRequestMsg);
  // Pre-process the request and calculate a hash of the result
  launchAsyncReqPreProcessingJob(preProcessRequestMsg, true, false);
}

const char *PreProcessor::getPreProcessResultBuffer(uint16_t clientId) const {
  return preProcessResultBuffers_[getClientReplyBufferId(clientId)].data();
}

// Primary replica: ask all replicas to pre-process the request
void PreProcessor::sendPreProcessRequestToAllReplicas(PreProcessRequestMsgSharedPtr preProcessReqMsg) {
  const set<ReplicaId> &idsOfPeerReplicas = myReplica_.getIdsOfPeerReplicas();
  for (auto destId : idsOfPeerReplicas) {
    if (destId != myReplicaId_)
      // sendMsg works asynchronously, so we can launch it sequentially here
      sendMsg(preProcessReqMsg->body(), destId, preProcessReqMsg->type(), preProcessReqMsg->size());
  }
}

void PreProcessor::launchAsyncReqPreProcessingJob(PreProcessRequestMsgSharedPtr preProcessReqMsg,
                                                  bool isPrimary,
                                                  bool isRetry) {
  auto *preProcessJob = new AsyncPreProcessJob(*this, preProcessReqMsg, isPrimary, isRetry);
  threadPool_.add(preProcessJob);
}

uint32_t PreProcessor::launchReqPreProcessing(uint16_t clientId, ReqId reqSeqNum, uint32_t reqLength, char *reqBuf) {
  uint32_t resultLen = 0;
  requestsHandler_.execute(clientId,
                           reqSeqNum,
                           PRE_PROCESS_FLAG,
                           reqLength,
                           reqBuf,
                           maxReplyMsgSize_,
                           (char *)getPreProcessResultBuffer(clientId),
                           resultLen);
  if (!resultLen)
    throw std::runtime_error("actualResultLength is 0 for clientId=" + to_string(clientId) +
                             ", requestSeqNum=" + to_string(reqSeqNum));
  return resultLen;
}

PreProcessingResult PreProcessor::getPreProcessingConsensusResult(uint16_t clientId) {
  auto &clientEntry = ongoingRequests_[clientId];
  lock_guard<mutex> lock(clientEntry->mutex);
  return clientEntry->clientReqInfoPtr->getPreProcessingConsensusResult();
}

ReqId PreProcessor::getOngoingReqIdForClient(uint16_t clientId) {
  auto &clientEntry = ongoingRequests_[clientId];
  lock_guard<mutex> lock(clientEntry->mutex);
  if (clientEntry->clientReqInfoPtr) return clientEntry->clientReqInfoPtr->getReqSeqNum();
  return 0;
}

void PreProcessor::handlePreProcessedReqPrimaryRetry(NodeIdType clientId, SeqNum reqSeqNum) {
  if (getPreProcessingConsensusResult(clientId) == COMPLETE)
    finalizePreProcessing(clientId);
  else
    cancelPreProcessing(clientId);
}

void PreProcessor::handlePreProcessedReqByPrimary(PreProcessRequestMsgSharedPtr preProcessReqMsg,
                                                  uint16_t clientId,
                                                  uint32_t resultBufLen) {
  auto &clientEntry = ongoingRequests_[clientId];
  string cid;
  PreProcessingResult result = CANCEL;
  {
    lock_guard<mutex> lock(clientEntry->mutex);
    if (clientEntry->clientReqInfoPtr) {
      clientEntry->clientReqInfoPtr->handlePrimaryPreProcessed(getPreProcessResultBuffer(clientId), resultBufLen);
      result = clientEntry->clientReqInfoPtr->getPreProcessingConsensusResult();
      cid = clientEntry->clientReqInfoPtr->getPreProcessRequest()->getCid();
    }
  }
  handleReqPreProcessedByOneReplica(cid, result, clientId, preProcessReqMsg->reqSeqNum());
}

void PreProcessor::handlePreProcessedReqByNonPrimary(uint16_t clientId,
                                                     ReqId reqSeqNum,
                                                     uint32_t resBufLen,
                                                     const std::string &cid) {
  auto replyMsg = make_shared<PreProcessReplyMsg>(sigManager_, myReplicaId_, clientId, reqSeqNum);
  replyMsg->setupMsgBody(getPreProcessResultBuffer(clientId), resBufLen, cid);
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
  LOG_DEBUG(GL, "Sent reqSeqNum=" << reqSeqNum << " to the primary replica=" << myReplica_.currentPrimary());
}

void PreProcessor::handleReqPreProcessingJob(PreProcessRequestMsgSharedPtr preProcessReqMsg,
                                             bool isPrimary,
                                             bool isRetry) {
  MDC_CID_PUT(GL, preProcessReqMsg->getCid());
  const uint16_t &clientId = preProcessReqMsg->clientId();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  uint32_t actualResultBufLen =
      launchReqPreProcessing(clientId, reqSeqNum, preProcessReqMsg->requestLength(), preProcessReqMsg->requestBuf());
  if (isPrimary && isRetry) {
    handlePreProcessedReqPrimaryRetry(clientId, reqSeqNum);
    return;
  }
  if (isPrimary)
    handlePreProcessedReqByPrimary(preProcessReqMsg, clientId, actualResultBufLen);
  else
    handlePreProcessedReqByNonPrimary(clientId, reqSeqNum, actualResultBufLen, preProcessReqMsg->getCid());
}

void PreProcessor::addNewPreProcessor(shared_ptr<MsgsCommunicator> &msgsCommunicator,
                                      shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                                      shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                                      bftEngine::IRequestsHandler &requestsHandler,
                                      InternalReplicaApi &replica) {
  preProcessors_.push_back(make_unique<PreProcessor>(
      msgsCommunicator, incomingMsgsStorage, msgHandlersRegistrator, requestsHandler, replica));
}

//**************** Class AsyncPreProcessJob ****************//

AsyncPreProcessJob::AsyncPreProcessJob(PreProcessor &preProcessor,
                                       PreProcessRequestMsgSharedPtr preProcessReqMsg,
                                       bool isPrimary,
                                       bool isRetry)
    : preProcessor_(preProcessor), preProcessReqMsg_(preProcessReqMsg), isPrimary_(isPrimary), isRetry_(isRetry) {}

void AsyncPreProcessJob::execute() {
  MDC_PUT(GL, "rid", preProcessor_.myReplicaId_);
  preProcessor_.handleReqPreProcessingJob(preProcessReqMsg_, isPrimary_, isRetry_);
}

void AsyncPreProcessJob::release() { delete this; }

}  // namespace preprocessor
