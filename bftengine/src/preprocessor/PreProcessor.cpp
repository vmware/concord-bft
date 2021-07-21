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
#include <optional>
#include "InternalReplicaApi.hpp"
#include "Logger.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "OpenTracing.hpp"
#include "SigManager.hpp"
#include "messages/PreProcessResultMsg.hpp"

namespace preprocessor {

using namespace bftEngine;
using namespace concord::util;
using namespace concordUtils;
using namespace std;
using namespace std::placeholders;
using namespace concordUtil;

uint8_t RequestState::reqProcessingHistoryHeight = 10;

//**************** Class RequestsBatch ****************//

void RequestsBatch::init() {
  for (uint16_t i = 0; i < PreProcessor::clientMaxBatchSize_; i++) {
    // Placeholders for all requests in a batch
    requestsMap_[i] = make_shared<RequestState>();
  }
}

void RequestsBatch::addReply(PreProcessReplyMsgSharedPtr replyMsg) {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  repliesList_.push_back(replyMsg);
}

// Should be called under batchMutex_
void RequestsBatch::setBatchParameters(const std::string &cid, uint32_t batchSize) {
  cid_ = cid;
  batchSize_ = batchSize;
}

// Should be called under batchMutex_
void RequestsBatch::resetBatchParams() {
  batchSize_ = 0;
  batchInProcess_ = false;
  batchRegistered_ = false;
  numOfCompletedReqs_ = 0;
  cid_ = "";
}

// Called when ClientBatchRequestMsg received
void RequestsBatch::registerBatch(const std::string &cid, uint32_t batchSize) {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  if (!batchRegistered_) {
    batchRegistered_ = true;
    setBatchParameters(cid, batchSize);
    LOG_INFO(preProcessor_.logger(), "The batch has been registered" << KVLOG(clientId_, cid_, batchSize_));
  }
}

// On non-primary: called when PreProcessBatchRequestMsg received
void RequestsBatch::startBatch(const std::string &cid, uint32_t batchSize) {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  if (!batchInProcess_) {
    batchInProcess_ = true;
    batchRegistered_ = true;
    setBatchParameters(cid, batchSize);
    LOG_INFO(preProcessor_.logger(), "The batch has been started" << KVLOG(clientId_, cid_, batchSize_));
  }
}

// Called by the primary replica in case not all the messages successfully passed the checks
void RequestsBatch::updateBatchSize(uint32_t batchSize) {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  batchSize_ = batchSize;
}

const string RequestsBatch::getCid() const {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  return cid_;
}

RequestStateSharedPtr &RequestsBatch::getRequestState(uint16_t reqOffsetInBatch) {
  ConcordAssertLE(reqOffsetInBatch, PreProcessor::clientMaxBatchSize_ - 1);
  return requestsMap_[reqOffsetInBatch];
}

void RequestsBatch::handlePossiblyExpiredRequests() {
  for (const auto &req : requestsMap_) {
    const auto &reqStateEntry = req.second;
    lock_guard<mutex> lock(reqStateEntry->mutex);
    if (reqStateEntry->reqProcessingStatePtr) preProcessor_.handlePossiblyExpiredRequest(reqStateEntry);
  }
  bool cancelled = false;
  string cid;
  atomic_uint32_t batchSize = 0;
  {
    const std::lock_guard<std::mutex> lock(batchMutex_);
    if (batchSize_ && (numOfCompletedReqs_ >= batchSize_)) {
      cid = cid_;
      batchSize = batchSize_.load();
      resetBatchParams();
      cancelled = true;
    }
  }
  if (cancelled)
    LOG_INFO(preProcessor_.logger(), "The batch has been cancelled as expired" << KVLOG(clientId_, cid, batchSize));
}

void RequestsBatch::cancelBatchAndReleaseReqs() {
  bool cancelBatch = false;
  string cid;
  atomic_uint32_t batchSize = 0;
  {
    const lock_guard<mutex> lock(batchMutex_);
    for (const auto &reqEntry : requestsMap_) {
      if (reqEntry.second) preProcessor_.releaseClientPreProcessRequestSafe(clientId_, reqEntry.second, COMPLETE);
      cancelBatch = true;
    }
    if (cancelBatch) {
      cid = cid_;
      batchSize = batchSize_.load();
      resetBatchParams();
    }
  }
  if (cancelBatch) LOG_INFO(preProcessor_.logger(), "The batch has been cancelled" << KVLOG(clientId_, cid, batchSize));
}

// On non-primary replicas
void RequestsBatch::releaseReqsAndSendBatchedReplyIfCompleted() {
  uint32_t replyMsgsSize = 0;
  const auto senderId = preProcessor_.myReplicaId_;
  const auto primaryId = preProcessor_.myReplica_.currentPrimary();
  string cid;
  atomic_uint32_t batchSize = 0;
  PreProcessBatchReplyMsgSharedPtr batchReplyMsg;
  {
    const lock_guard<mutex> lock(batchMutex_);
    if (repliesList_.size() != batchSize_) return;

    cid = cid_;
    batchSize = batchSize_.load();
    // The last batch request has pre-processed => send batched reply message
    for (auto const &replyMsg : repliesList_) {
      replyMsgsSize += replyMsg->size();
      preProcessor_.releaseClientPreProcessRequestSafe(clientId_, replyMsg->reqOffsetInBatch(), COMPLETE);
    }
    batchReplyMsg = make_shared<PreProcessBatchReplyMsg>(clientId_, senderId, repliesList_, cid_, replyMsgsSize);
    repliesList_.clear();
    resetBatchParams();
  }
  preProcessor_.sendMsg(batchReplyMsg->body(), primaryId, batchReplyMsg->type(), batchReplyMsg->size());
  LOG_INFO(preProcessor_.logger(),
           "Pre-processing completed and the batched reply message sent to the primary replica"
               << KVLOG(senderId, clientId_, cid, batchSize, primaryId));
}

// On primary replica
void RequestsBatch::finalizeBatchIfCompleted() {
  string cid;
  atomic_uint32_t batchSize = 0;
  {
    const lock_guard<mutex> lock(batchMutex_);
    cid = cid_;
    batchSize = batchSize_.load();
    if (numOfCompletedReqs_ != batchSize_) return;
    resetBatchParams();
  }
  LOG_INFO(preProcessor_.logger(), "The batch has been released" << KVLOG(clientId_, cid, batchSize));
}

void RequestsBatch::sendCancelBatchedPreProcessingMsgToNonPrimaries(const ClientMsgsList &clientMsgs,
                                                                    NodeIdType destId) {
  uint32_t offset = 0;
  uint32_t overallPreProcessReqMsgsSize = 0;
  uint32_t reqRetryId = 0;
  PreProcessReqMsgsList reqsBatch;
  for (auto &clientMsg : clientMsgs) {
    const auto &reqEntry = requestsMap_[offset];
    reqRetryId = (reqEntry->reqRetryId)++;
    auto preProcessReqMsg =
        make_shared<PreProcessRequestMsg>(REQ_TYPE_CANCEL,
                                          preProcessor_.myReplicaId_,
                                          clientId_,
                                          offset++,
                                          clientMsg->requestSeqNum(),
                                          reqRetryId,
                                          0,
                                          nullptr,
                                          clientMsg->getCid(),
                                          nullptr,
                                          0,
                                          clientMsg->spanContext<ClientPreProcessReqMsgUniquePtr::element_type>());
    reqsBatch.push_back(preProcessReqMsg);
    overallPreProcessReqMsgsSize += preProcessReqMsg->size();
  }
  auto preProcessBatchReqMsg = make_shared<PreProcessBatchRequestMsg>(
      REQ_TYPE_CANCEL, clientId_, preProcessor_.myReplicaId_, reqsBatch, cid_, overallPreProcessReqMsgsSize);
  LOG_DEBUG(preProcessor_.logger(),
            "Sending PreProcessBatchRequestMsg with REQ_TYPE_CANCEL" << KVLOG(clientId_, cid_, destId));
  preProcessor_.sendMsg(
      preProcessBatchReqMsg->body(), destId, preProcessBatchReqMsg->type(), preProcessBatchReqMsg->size());
}

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
    LOG_ERROR(logger(), "Message validation failed: " << e.what());
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
      numOfReplicas_(myReplica.getReplicaConfig().numReplicas + myReplica.getReplicaConfig().numRoReplicas),
      numOfInternalClients_(myReplica.getReplicaConfig().numOfClientProxies),
      clientBatchingEnabled_(myReplica.getReplicaConfig().clientBatchingEnabled),
      metricsComponent_{concordMetrics::Component("preProcessor", std::make_shared<concordMetrics::Aggregator>())},
      metricsLastDumpTime_(0),
      metricsDumpIntervalInSec_{myReplica_.getReplicaConfig().metricsDumpIntervalSeconds},
      preProcessorMetrics_{metricsComponent_.RegisterAtomicCounter("preProcReqReceived"),
                           metricsComponent_.RegisterCounter("preProcBatchReqReceived"),
                           metricsComponent_.RegisterCounter("preProcReqInvalid"),
                           metricsComponent_.RegisterAtomicCounter("preProcReqIgnored"),
                           metricsComponent_.RegisterAtomicCounter("preProcReqRejected"),
                           metricsComponent_.RegisterCounter("preProcConsensusNotReached"),
                           metricsComponent_.RegisterCounter("preProcessRequestTimedOut"),
                           metricsComponent_.RegisterCounter("preProcPossiblePrimaryFaultDetected"),
                           metricsComponent_.RegisterCounter("preProcReqCompleted"),
                           metricsComponent_.RegisterCounter("preProcReqRetried"),
                           metricsComponent_.RegisterAtomicGauge("preProcessingTimeAvg", 0),
                           metricsComponent_.RegisterAtomicGauge("launchAsyncPreProcessJobTimeAvg", 0),
                           metricsComponent_.RegisterAtomicGauge("PreProcInFlyRequestsNum", 0)},
      totalPreProcessingTime_(true),
      launchAsyncJobTimeAvg_(true),
      preExecReqStatusCheckPeriodMilli_(myReplica_.getReplicaConfig().preExecReqStatusCheckTimerMillisec),
      timers_{timers},
      totalPreExecDurationRecorder_{histograms_.totalPreExecutionDuration},
      launchAsyncPreProcessJobRecorder_{histograms_.launchAsyncPreProcessJob},
      pm_{pm},
      batchedPreProcessEnabled_(myReplica_.getReplicaConfig().batchedPreProcessEnabled) {
  clientMaxBatchSize_ = clientBatchingEnabled_ ? myReplica.getReplicaConfig().clientBatchingMaxMsgsNbr : 1,
  registerMsgHandlers();
  metricsComponent_.Register();
  const uint16_t numOfExternalClients = myReplica.getReplicaConfig().numOfExternalClients;
  const uint16_t numOfReqEntries = numOfExternalClients * clientMaxBatchSize_;
  for (uint16_t i = 0; i < numOfReqEntries; i++) {
    // Placeholders for all clients including batches
    preProcessResultBuffers_.emplace_back(std::make_pair(false, Sliver()));
  }
  const uint16_t firstClientId = numOfReplicas_ + numOfInternalClients_;
  for (uint16_t i = 0; i < numOfExternalClients; i++) {
    // Placeholders for all client batches
    const uint16_t clientId = firstClientId + i;
    ongoingReqBatches_[clientId] = make_shared<RequestsBatch>(*this, clientId);
    ongoingReqBatches_[clientId]->init();
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
  msgLoopThread_ = std::thread{&PreProcessor::msgProcessingLoop, this};
  LOG_INFO(logger(),
           KVLOG(numOfReplicas_,
                 numOfExternalClients,
                 numOfInternalClients_,
                 numOfReqEntries,
                 firstClientId,
                 clientBatchingEnabled_,
                 clientMaxBatchSize_,
                 maxPreExecResultSize_,
                 preExecReqStatusCheckPeriodMilli_,
                 numOfThreads,
                 ReplicaConfig::instance().preExecutionResultAuthEnabled));
  RequestProcessingState::init(numOfRequiredReplies(), &histograms_);
  PreProcessReplyMsg::setPreProcessorHistograms(&histograms_);
  addTimers();
}

PreProcessor::~PreProcessor() {
  msgLoopDone_ = true;
  msgLoopSignal_.notify_all();
  cancelTimers();
  threadPool_.stop();
  if (msgLoopThread_.joinable()) msgLoopThread_.join();
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
  const auto &batchCid = reqStatePtr->getBatchCid();
  if (!rejectedReplicasList.empty() && preProcessReqMsg) {
    SCOPED_MDC_CID(preProcessReqMsg->getCid());
    const auto &clientId = preProcessReqMsg->clientId();
    const auto &reqSeqNum = preProcessReqMsg->reqSeqNum();
    for (const auto &destId : rejectedReplicasList) {
      LOG_DEBUG(logger(), "Resending PreProcessRequestMsg" << KVLOG(clientId, reqSeqNum, batchCid, destId));
      sendMsg(preProcessReqMsg->body(), destId, preProcessReqMsg->type(), preProcessReqMsg->size());
    }
    reqStatePtr->resetRejectedReplicasList();
  }
}

void PreProcessor::handlePossiblyExpiredRequest(const RequestStateSharedPtr &reqStateEntry) {
  const auto &reqStatePtr = reqStateEntry->reqProcessingStatePtr;
  if (reqStatePtr->isReqTimedOut()) {
    preProcessorMetrics_.preProcessRequestTimedOut++;
    preProcessorMetrics_.preProcPossiblePrimaryFaultDetected++;
    // The request could expire do to failed primary replica, let ReplicaImp address that
    const auto &reqSeqNum = reqStatePtr->getReqSeqNum();
    const auto &clientId = reqStatePtr->getClientId();
    const auto &batchCid = reqStatePtr->getBatchCid();
    SCOPED_MDC_CID(reqStatePtr->getReqCid());
    LOG_INFO(logger(), "Let replica handle request" << KVLOG(reqSeqNum, clientId, batchCid));
    incomingMsgsStorage_->pushExternalMsg(reqStatePtr->buildClientRequestMsg(true));
    releaseClientPreProcessRequest(reqStateEntry, EXPIRED);
  } else if (myReplica_.isCurrentPrimary() && reqStatePtr->definePreProcessingConsensusResult() == CONTINUE)
    resendPreProcessRequest(reqStatePtr);
}

void PreProcessor::onRequestsStatusCheckTimer() {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onRequestsStatusCheckTimer);
  // Pass through all ongoing requests and abort the pre-execution for those that are timed out.
  for (auto &batchEntry : ongoingReqBatches_) batchEntry.second->handlePossiblyExpiredRequests();
}

bool PreProcessor::checkClientMsgCorrectness(uint64_t reqSeqNum,
                                             const string &cid,
                                             bool isReadOnly,
                                             uint16_t clientId,
                                             NodeIdType senderId,
                                             const std::string &batchCid) const {
  SCOPED_MDC_CID(cid);
  if (myReplica_.isCollectingState()) {
    LOG_INFO(logger(),
             "Ignore ClientPreProcessRequestMsg as the replica is collecting missing state from other replicas"
                 << KVLOG(reqSeqNum, senderId, clientId, batchCid));
    return false;
  }
  if (isReadOnly) {
    LOG_INFO(logger(),
             "Ignore ClientPreProcessRequestMsg as it is signed as read-only" << KVLOG(reqSeqNum, clientId, batchCid));
    return false;
  }
  const bool &invalidClient = !myReplica_.isValidClient(clientId);
  const bool &sentFromReplicaToNonPrimary = myReplica_.isIdOfReplica(senderId) && !myReplica_.isCurrentPrimary();
  if (invalidClient || sentFromReplicaToNonPrimary) {
    LOG_WARN(logger(),
             "Ignore ClientPreProcessRequestMsg as invalid"
                 << KVLOG(reqSeqNum, senderId, clientId, invalidClient, sentFromReplicaToNonPrimary, batchCid));
    return false;
  }
  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(),
             "Ignore ClientPreProcessRequestMsg as current view is inactive"
                 << KVLOG(reqSeqNum, senderId, clientId, batchCid));
    return false;
  }
  return true;
}

bool PreProcessor::checkClientBatchMsgCorrectness(const ClientBatchRequestMsgUniquePtr &clientBatchReqMsg) {
  if (!clientBatchingEnabled_) {
    LOG_ERROR(logger(),
              "Batching functionality is disabled => reject message"
                  << KVLOG(clientBatchReqMsg->clientId(), clientBatchReqMsg->senderId(), clientBatchReqMsg->getCid()));
    preProcessorMetrics_.preProcReqIgnored++;
    return false;
  }
  const auto &clientRequestMsgs = clientBatchReqMsg->getClientPreProcessRequestMsgs();
  bool valid = true;
  for (const auto &msg : clientRequestMsgs) {
    if (!checkClientMsgCorrectness(msg->requestSeqNum(),
                                   msg->getCid(),
                                   false,
                                   msg->clientProxyId(),
                                   clientBatchReqMsg->senderId(),
                                   clientBatchReqMsg->getCid())) {
      preProcessorMetrics_.preProcReqIgnored++;
      valid = false;
    } else if (!validateMessage(msg.get())) {
      preProcessorMetrics_.preProcReqInvalid++;
      valid = false;
    }
  }
  return valid;
}

void PreProcessor::updateAggregatorAndDumpMetrics() {
  metricsComponent_.UpdateAggregator();
  auto currTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
  if (currTime - metricsLastDumpTime_ >= metricsDumpIntervalInSec_) {
    metricsLastDumpTime_ = currTime;
    LOG_INFO(logger(), "--preProcessor metrics dump--" + metricsComponent_.ToJson());
  }
}

void PreProcessor::cancelPreProcessingOnNonPrimary(const ClientPreProcessReqMsgUniquePtr &clientReqMsg,
                                                   NodeIdType destId,
                                                   uint16_t reqOffsetInBatch,
                                                   uint64_t reqRetryId,
                                                   const string &batchCid) {
  const auto clientId = clientReqMsg->clientProxyId();
  const auto reqSeqNum = clientReqMsg->requestSeqNum();
  auto preProcessReqMsg =
      make_shared<PreProcessRequestMsg>(REQ_TYPE_CANCEL,
                                        myReplicaId_,
                                        clientId,
                                        reqOffsetInBatch,
                                        reqSeqNum,
                                        reqRetryId,
                                        0,
                                        nullptr,
                                        clientReqMsg->getCid(),
                                        nullptr,
                                        0,
                                        clientReqMsg->spanContext<ClientPreProcessReqMsgUniquePtr::element_type>());
  SCOPED_MDC_CID(preProcessReqMsg->getCid());
  LOG_DEBUG(logger(),
            "Sending PreProcessRequestMsg with REQ_TYPE_CANCEL" << KVLOG(clientId, batchCid, reqSeqNum, destId));
  sendMsg(preProcessReqMsg->body(), destId, preProcessReqMsg->type(), preProcessReqMsg->size());
  preProcessorMetrics_.preProcReqRejected++;
}

void PreProcessor::sendRejectPreProcessReplyMsg(NodeIdType clientId,
                                                uint16_t reqOffsetInBatch,
                                                NodeIdType senderId,
                                                SeqNum reqSeqNum,
                                                SeqNum ongoingReqSeqNum,
                                                uint64_t reqRetryId,
                                                const string &cid,
                                                const string &ongoingCid) {
  auto replyMsg = make_shared<PreProcessReplyMsg>(myReplicaId_,
                                                  clientId,
                                                  reqOffsetInBatch,
                                                  reqSeqNum,
                                                  reqRetryId,
                                                  getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch),
                                                  0,
                                                  cid,
                                                  STATUS_REJECT);
  LOG_DEBUG(
      logger(),
      KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch, ongoingReqSeqNum, ongoingCid)
          << " Sending PreProcessReplyMsg with STATUS_REJECT as another PreProcessRequest from the same client is "
             "in progress");
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
}

template <>
void PreProcessor::onMessage<ClientPreProcessRequestMsg>(ClientPreProcessRequestMsg *msg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onClientPreProcessRequestMsg);
  preProcessorMetrics_.preProcReqReceived++;
  ClientPreProcessReqMsgUniquePtr clientMsg(msg);
  const string &cid = clientMsg->getCid();
  const NodeIdType &senderId = clientMsg->senderId();
  const NodeIdType &clientId = clientMsg->clientProxyId();
  const ReqId &reqSeqNum = clientMsg->requestSeqNum();
  const auto &reqTimeoutMilli = clientMsg->requestTimeoutMilli();
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), "Received ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, clientId, senderId, reqTimeoutMilli));
  if (!checkClientMsgCorrectness(reqSeqNum, cid, clientMsg->isReadOnly(), clientId, senderId)) return;
  PreProcessRequestMsgSharedPtr preProcessRequestMsg;
  handleSingleClientRequestMessage(move(clientMsg), senderId, false, 0, preProcessRequestMsg);
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestPreProcessingRightNow(const RequestStateSharedPtr &reqEntry,
                                                  ReqId reqSeqNum,
                                                  NodeIdType clientId,
                                                  const string &batchCid,
                                                  NodeIdType senderId) {
  if (reqEntry->reqProcessingStatePtr) {
    const auto &ongoingReqSeqNum = reqEntry->reqProcessingStatePtr->getReqSeqNum();
    const auto &ongoingCid = reqEntry->reqProcessingStatePtr->getReqCid();
    const auto &ongoingBatchCid = reqEntry->reqProcessingStatePtr->getBatchCid();
    LOG_DEBUG(logger(),
              KVLOG(reqSeqNum, clientId, batchCid, senderId)
                  << " is ignored:" << KVLOG(ongoingReqSeqNum, ongoingCid, ongoingBatchCid) << " is in progress");
    preProcessorMetrics_.preProcReqIgnored++;
    return true;
  }
  return false;
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestPassingConsensusOrPostExec(
    SeqNum reqSeqNum, NodeIdType senderId, NodeIdType clientId, const string &batchCid, const string &cid) {
  if (myReplica_.isClientRequestInProcess(clientId, reqSeqNum)) {
    LOG_DEBUG(logger(),
              "The specified request is in consensus or being post-executed right now - ignore"
                  << KVLOG(cid, batchCid, reqSeqNum, senderId, clientId));
    preProcessorMetrics_.preProcReqIgnored++;
    return true;
  }
  return false;
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestPreProcessedBefore(const RequestStateSharedPtr &reqEntry,
                                               SeqNum reqSeqNum,
                                               NodeIdType clientId,
                                               const string &batchCid,
                                               const string &cid) {
  if (!reqEntry->reqProcessingStatePtr) {
    // Verify that an arrived request is newer than any other in the requests history for this client
    for (const auto &oldReqState : reqEntry->reqProcessingHistory) {
      if (oldReqState->getReqSeqNum() > reqSeqNum) {
        LOG_DEBUG(
            logger(),
            "The request is ignored as the newer request from this client has already been pre-processed"
                << KVLOG(cid, batchCid, reqSeqNum, clientId, oldReqState->getReqCid(), oldReqState->getReqSeqNum()));
        preProcessorMetrics_.preProcReqIgnored++;
        return true;
      }
    }
  }
  return false;
}

bool PreProcessor::handleSingleClientRequestMessage(ClientPreProcessReqMsgUniquePtr clientMsg,
                                                    NodeIdType senderId,
                                                    bool arrivedInBatch,
                                                    uint16_t reqOffsetInBatch,
                                                    PreProcessRequestMsgSharedPtr &preProcessRequestMsg,
                                                    const string &batchCid,
                                                    uint32_t batchSize) {
  SCOPED_MDC_CID(clientMsg->getCid());
  const NodeIdType &clientId = clientMsg->clientProxyId();
  const ReqId &reqSeqNum = clientMsg->requestSeqNum();
  LOG_DEBUG(logger(), KVLOG(batchCid, reqSeqNum, clientId, senderId, arrivedInBatch, reqOffsetInBatch));
  bool registerSucceeded = false;
  {
    const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
    lock_guard<mutex> lock(reqEntry->mutex);
    const bool reqToBeDeclined =
        (isRequestPreProcessingRightNow(reqEntry, reqSeqNum, clientId, batchCid, senderId) ||
         isRequestPassingConsensusOrPostExec(reqSeqNum, senderId, clientId, batchCid, clientMsg->getCid()) ||
         isRequestPreProcessedBefore(reqEntry, reqSeqNum, clientId, batchCid, clientMsg->getCid()));
    if (reqToBeDeclined) {
      if (!batchedPreProcessEnabled_ && (senderId != clientId))
        // Send 'cancel' request to non-primary replicas to release them from waiting to a 'real' PreProcessRequestMsg,
        // which will not arrive in this case. Doing this to avoid request from being timed out on non-primary replicas.
        cancelPreProcessingOnNonPrimary(clientMsg, senderId, reqOffsetInBatch, (reqEntry->reqRetryId)++);
      return false;
    }
    if (myReplica_.isReplyAlreadySentToClient(clientId, reqSeqNum)) {
      if (!batchedPreProcessEnabled_ && (senderId != clientId))
        cancelPreProcessingOnNonPrimary(clientMsg, senderId, reqOffsetInBatch, (reqEntry->reqRetryId)++);
      LOG_INFO(logger(),
               "Request has already been executed - let replica decide how to proceed further"
                   << KVLOG(batchCid, batchSize, reqSeqNum, clientId, senderId));
      incomingMsgsStorage_->pushExternalMsg(clientMsg->convertToClientRequestMsg(false));
      return false;
    }
    if (myReplica_.isCurrentPrimary())
      registerSucceeded = registerRequestOnPrimaryReplica(
          batchCid, batchSize, move(clientMsg), preProcessRequestMsg, reqOffsetInBatch, reqEntry);
    else {
      registerAndHandleClientPreProcessReqOnNonPrimary(
          batchCid, batchSize, move(clientMsg), arrivedInBatch, reqOffsetInBatch);
      return true;
    }
  }
  if (myReplica_.isCurrentPrimary() && registerSucceeded) {
    handleClientPreProcessRequestByPrimary(preProcessRequestMsg, arrivedInBatch);
    return true;
  }
  LOG_DEBUG(logger(),
            "ClientPreProcessRequestMsg" << KVLOG(batchCid, reqSeqNum, clientId, senderId)
                                         << " is ignored because request is old/duplicated");
  preProcessorMetrics_.preProcReqIgnored++;
  return false;
}

void PreProcessor::sendPreProcessBatchReqToAllReplicas(ClientBatchRequestMsgUniquePtr clientBatchMsg,
                                                       const PreProcessReqMsgsList &preProcessReqMsgList,
                                                       uint32_t requestsSize) {
  const auto clientId = clientBatchMsg->clientId();
  const auto &batchCid = clientBatchMsg->getCid();
  const auto batchSize = preProcessReqMsgList.size();
  ongoingReqBatches_[clientId]->updateBatchSize(batchSize);
  LOG_DEBUG(logger(), "Send PreProcessBatchRequestMsg to non-primary replicas" << KVLOG(clientId, batchCid, batchSize));

  auto preProcessBatchReqMsg = make_shared<PreProcessBatchRequestMsg>(
      REQ_TYPE_PRE_PROCESS, clientId, myReplicaId_, preProcessReqMsgList, batchCid, requestsSize);

  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.sendPreProcessBatchRequestToAllReplicas);
  const set<ReplicaId> &idsOfPeerReplicas = myReplica_.getIdsOfPeerReplicas();
  for (auto destId : idsOfPeerReplicas) {
    if (destId != myReplicaId_)
      // sendMsg works asynchronously, so we can launch it sequentially here
      sendMsg(preProcessBatchReqMsg->body(), destId, REQ_TYPE_PRE_PROCESS, preProcessBatchReqMsg->size());
  }
}

template <>
void PreProcessor::onMessage<ClientBatchRequestMsg>(ClientBatchRequestMsg *msg) {
  preProcessorMetrics_.preProcBatchReqReceived++;
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onClientBatchPreProcessRequestMsg);
  ClientBatchRequestMsgUniquePtr clientBatchMsg(msg);
  const auto clientId = clientBatchMsg->clientId();
  // senderId should be taken from ClientBatchRequestMsg as it does not get re-set in batched client messages
  const auto senderId = clientBatchMsg->senderId();
  const auto &batchCid = clientBatchMsg->getCid();
  const auto batchSize = clientBatchMsg->numOfMessagesInBatch();
  LOG_DEBUG(logger(), "Received ClientBatchRequestMsg" << KVLOG(batchCid, senderId, clientId, batchSize));
  if (!checkClientBatchMsgCorrectness(clientBatchMsg)) {
    preProcessorMetrics_.preProcReqIgnored++;
    return;
  }
  ClientMsgsList &clientMsgs = clientBatchMsg->getClientPreProcessRequestMsgs();
  const auto &batchEntry = ongoingReqBatches_[clientId];
  if (batchedPreProcessEnabled_ && batchEntry->isBatchRegistered()) {
    const auto &ongoingBatchCid = batchEntry->getCid();
    if (batchCid != ongoingBatchCid) {
      LOG_INFO(logger(),
               "Different ClientBatchRequestMsg for this client is in process; ignoring"
                   << KVLOG(batchCid, ongoingBatchCid, senderId, clientId));
    } else
      LOG_DEBUG(logger(), "This batch is already in process; ignoring" << KVLOG(batchCid, senderId, clientId));
    return;
  }
  uint16_t offset = 0;
  PreProcessReqMsgsList preProcessReqMsgList;
  uint32_t overallPreProcessReqMsgsSize = 0;
  bool thereAreMsgsPassedChecks = false;
  for (auto &clientMsg : clientMsgs) {
    preProcessorMetrics_.preProcReqReceived++;
    PreProcessRequestMsgSharedPtr preProcessRequestMsg;
    const bool msgPassedChecks = handleSingleClientRequestMessage(
        move(clientMsg), senderId, true, offset++, preProcessRequestMsg, batchCid, batchSize);
    if (msgPassedChecks) {
      thereAreMsgsPassedChecks = true;
      if (batchedPreProcessEnabled_ && myReplica_.isCurrentPrimary() && preProcessRequestMsg) {
        overallPreProcessReqMsgsSize += preProcessRequestMsg->size();
        preProcessReqMsgList.push_back(preProcessRequestMsg);
      }
    }
  }
  if (myReplica_.isCurrentPrimary()) {
    if (batchedPreProcessEnabled_ && !preProcessReqMsgList.empty())
      // For the non-batched inter-replicas communication PreProcessReq messages get sent to all non-primary replicas
      // in handleClientPreProcessRequestByPrimary function one-by-one
      sendPreProcessBatchReqToAllReplicas(move(clientBatchMsg), preProcessReqMsgList, overallPreProcessReqMsgsSize);
  } else if (thereAreMsgsPassedChecks) {
    LOG_DEBUG(logger(), "Pass ClientBatchRequestMsg to the current primary" << KVLOG(batchCid, senderId, clientId));
    sendMsg(clientBatchMsg->body(), myReplica_.currentPrimary(), clientBatchMsg->type(), clientBatchMsg->size());
  }
}  // namespace preprocessor

bool PreProcessor::checkPreProcessReqPrerequisites(SeqNum reqSeqNum,
                                                   const string &cid,
                                                   NodeIdType senderId,
                                                   NodeIdType clientId,
                                                   const string &batchCid,
                                                   uint16_t reqOffsetInBatch) {
  if (myReplica_.isCollectingState()) {
    LOG_INFO(logger(),
             "Ignore PreProcessRequestMsg as the replica is collecting missing state from other replicas"
                 << KVLOG(reqSeqNum, cid, batchCid, senderId, clientId, reqOffsetInBatch));
    return false;
  }

  if (myReplica_.isCurrentPrimary()) {
    LOG_WARN(logger(),
             "Ignore PreProcessRequestMsg as current replica is the primary"
                 << KVLOG(reqSeqNum, cid, batchCid, senderId, clientId, reqOffsetInBatch));
    return false;
  }

  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(),
             "Ignore PreProcessRequestMsg as current view is inactive"
                 << KVLOG(reqSeqNum, cid, batchCid, senderId, clientId, reqOffsetInBatch));
    return false;
  }
  return true;
}

bool PreProcessor::checkPreProcessBatchReqMsgCorrectness(const PreProcessBatchReqMsgSharedPtr &batchReq) {
  const auto &preProcessRequestMsgs = batchReq->getPreProcessRequestMsgs();
  bool valid = true;
  for (const auto &msg : preProcessRequestMsgs) {
    if (!checkPreProcessReqPrerequisites(msg->reqSeqNum(),
                                         msg->getCid(),
                                         batchReq->senderId(),
                                         msg->clientId(),
                                         batchReq->getCid(),
                                         msg->reqOffsetInBatch())) {
      preProcessorMetrics_.preProcReqIgnored++;
      valid = false;
    } else if (!validateMessage(msg.get())) {
      preProcessorMetrics_.preProcReqInvalid++;
      valid = false;
    }
  }
  return valid;
}

// Non-primary replica request handling
template <>
void PreProcessor::onMessage<PreProcessBatchRequestMsg>(PreProcessBatchRequestMsg *msg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onPreProcessBatchRequestMsg);
  PreProcessBatchReqMsgSharedPtr batchMsg(msg);
  const auto &batchCid = msg->getCid();
  const NodeIdType &senderId = batchMsg->senderId();
  const NodeIdType &clientId = batchMsg->clientId();
  const string reqType = batchMsg->reqType() == REQ_TYPE_PRE_PROCESS ? "REQ_TYPE_PRE_PROCESS" : "REQ_TYPE_CANCEL";
  LOG_DEBUG(logger(),
            "Received PreProcessBatchRequestMsg"
                << KVLOG(reqType, senderId, clientId, batchCid, batchMsg->numOfMessagesInBatch()));
  if (!checkPreProcessBatchReqMsgCorrectness(batchMsg)) return;

  PreProcessReqMsgsList &preProcessReqMsgs = batchMsg->getPreProcessRequestMsgs();
  const auto batchSize = preProcessReqMsgs.size();
  for (auto &singleMsg : preProcessReqMsgs) {
    LOG_DEBUG(logger(),
              "Start handling single message from the batch:" << KVLOG(
                  batchCid, singleMsg->reqSeqNum(), singleMsg->getCid(), senderId, clientId, batchSize));
    handleSinglePreProcessRequestMsg(singleMsg, batchCid, batchSize);
  }
  if (batchMsg->reqType() == REQ_TYPE_CANCEL && !ongoingReqBatches_[clientId]->isBatchInProcess())
    // Don't cancel the batch if it has received PreProcessBatchRequestMsg before
    ongoingReqBatches_[clientId]->cancelBatchAndReleaseReqs();
}

void PreProcessor::handleSinglePreProcessRequestMsg(PreProcessRequestMsgSharedPtr preProcessReqMsg,
                                                    const string &batchCid,
                                                    uint32_t batchSize) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onPreProcessRequestMsg);
  const NodeIdType &senderId = preProcessReqMsg->senderId();
  const NodeIdType &clientId = preProcessReqMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  const RequestType reqType = preProcessReqMsg->reqType();
  string cid = preProcessReqMsg->getCid();
  LOG_DEBUG(logger(), KVLOG(batchCid, reqSeqNum, clientId, senderId, reqOffsetInBatch));
  SCOPED_MDC_CID(cid);
  if (!checkPreProcessReqPrerequisites(reqSeqNum, cid, senderId, clientId, batchCid, reqOffsetInBatch)) return;
  bool registerSucceeded = false;
  {
    const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
    lock_guard<mutex> lock(reqEntry->mutex);
    if (reqEntry->reqProcessingStatePtr) {
      // The primary replica requested to cancel the request => release it, but don't cancel pre-processing when
      // reqEntry->reqProcessingStatePtr->getPreProcessRequest() != null_ptr
      if (reqType == REQ_TYPE_CANCEL && !reqEntry->reqProcessingStatePtr->getPreProcessRequest())
        return releaseClientPreProcessRequest(reqEntry, CANCELLED_BY_PRIMARY);
      if (reqEntry->reqProcessingStatePtr->getPreProcessRequest()) {
        auto const &ongoingReqSeqNum = reqEntry->reqProcessingStatePtr->getPreProcessRequest()->reqSeqNum();
        auto const &ongoingCid = reqEntry->reqProcessingStatePtr->getPreProcessRequest()->getCid();
        // The replica is processing some request and cannot handle PreProcessRequest now - send reject to the primary
        return sendRejectPreProcessReplyMsg(clientId,
                                            reqOffsetInBatch,
                                            senderId,
                                            reqSeqNum,
                                            ongoingReqSeqNum,
                                            preProcessReqMsg->reqRetryId(),
                                            preProcessReqMsg->getCid(),
                                            ongoingCid);
      }
    } else {
      if (reqType == REQ_TYPE_CANCEL) return;  // No registered client request found; do nothing
    }
    registerSucceeded =
        registerRequest(batchCid, batchSize, ClientPreProcessReqMsgUniquePtr(), preProcessReqMsg, reqOffsetInBatch);
  }
  if (registerSucceeded) {
    LOG_INFO(logger(),
             "Start PreProcessRequestMsg processing by a non-primary replica"
                 << KVLOG(reqSeqNum, batchCid, batchSize, cid, clientId, reqOffsetInBatch, senderId));
    preProcessorMetrics_.preProcInFlyRequestsNum++;  // Increase the metric on non-primary replica
    auto totalPreExecDurationRecorder = TimeRecorder(*totalPreExecDurationRecorder_.get());
    // Pre-process the request, calculate a hash of the result and send a reply message back
    launchAsyncReqPreProcessingJob(preProcessReqMsg, false, false, std::move(totalPreExecDurationRecorder));
  }
}

// Non-primary replica request handling
template <>
void PreProcessor::onMessage<PreProcessRequestMsg>(PreProcessRequestMsg *message) {
  auto msg = PreProcessRequestMsgSharedPtr(message);
  const string reqType = msg->reqType() == REQ_TYPE_PRE_PROCESS ? "REQ_TYPE_PRE_PROCESS" : "REQ_TYPE_CANCEL";
  LOG_DEBUG(logger(),
            "Received PreProcessRequestMsg" << KVLOG(
                reqType, msg->reqSeqNum(), msg->getCid(), msg->senderId(), msg->clientId(), msg->reqOffsetInBatch()));
  handleSinglePreProcessRequestMsg(msg);
}

void PreProcessor::handleSinglePreProcessReplyMsg(PreProcessReplyMsgSharedPtr preProcessReplyMsg,
                                                  const string &batchCid) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onPreProcessReplyMsg);
  const NodeIdType &senderId = preProcessReplyMsg->senderId();
  const NodeIdType &clientId = preProcessReplyMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReplyMsg->reqOffsetInBatch();
  const SeqNum &reqSeqNum = preProcessReplyMsg->reqSeqNum();
  string cid = preProcessReplyMsg->getCid();
  const auto &status = preProcessReplyMsg->status();
  PreProcessingResult result = CANCEL;
  SCOPED_MDC_CID(cid);
  {
    const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
    lock_guard<mutex> lock(reqEntry->mutex);
    if (!reqEntry->reqProcessingStatePtr || reqEntry->reqProcessingStatePtr->getReqSeqNum() != reqSeqNum) {
      // Look for the request in the requests history and check for the non-determinism
      for (const auto &oldReqState : reqEntry->reqProcessingHistory)
        if (oldReqState->getReqSeqNum() == reqSeqNum)
          oldReqState->detectNonDeterministicPreProcessing(
              preProcessReplyMsg->resultsHash(), preProcessReplyMsg->senderId(), preProcessReplyMsg->reqRetryId());
      LOG_DEBUG(logger(),
                KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch, batchCid)
                    << " will be ignored as no such ongoing request exists or different one found for this client");
      return;
    }
    reqEntry->reqProcessingStatePtr->handlePreProcessReplyMsg(preProcessReplyMsg);
    if (status == STATUS_REJECT) {
      LOG_DEBUG(logger(),
                "Received PreProcessReplyMsg with STATUS_REJECT"
                    << KVLOG(reqSeqNum, senderId, clientId, reqOffsetInBatch, batchCid));
      return;
    }
    result = reqEntry->reqProcessingStatePtr->definePreProcessingConsensusResult();
    if (result == CONTINUE) resendPreProcessRequest(reqEntry->reqProcessingStatePtr);
  }
  handlePreProcessReplyMsg(cid, result, clientId, reqOffsetInBatch, reqSeqNum, batchCid);
}

// Primary replica handling
template <>
void PreProcessor::onMessage<PreProcessReplyMsg>(PreProcessReplyMsg *message) {
  auto msg = PreProcessReplyMsgSharedPtr(message);
  string replyStatus = "STATUS_GOOD";
  if (msg->status() == STATUS_REJECT) replyStatus = "STATUS_REJECT";
  LOG_DEBUG(logger(),
            "Received PreProcessReplyMsg"
                << KVLOG(msg->reqSeqNum(), msg->senderId(), msg->clientId(), msg->reqOffsetInBatch(), replyStatus));
  handleSinglePreProcessReplyMsg(msg);
}

// Primary replica handling
template <>
void PreProcessor::onMessage<PreProcessBatchReplyMsg>(PreProcessBatchReplyMsg *msg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onPreProcessBatchReplyMsg);
  PreProcessBatchReplyMsgSharedPtr batchMsg(msg);
  const auto &batchCid = msg->getCid();
  const NodeIdType &senderId = batchMsg->senderId();
  const NodeIdType &clientId = batchMsg->clientId();
  PreProcessReplyMsgsList &preProcessReplyMsgs = batchMsg->getPreProcessReplyMsgs();
  const auto batchSize = preProcessReplyMsgs.size();
  LOG_DEBUG(logger(), "Received PreProcessBatchReplyMsg" << KVLOG(batchCid, senderId, clientId, batchSize));
  const auto &batchEntry = ongoingReqBatches_[clientId];
  const auto &ongoingBatchCid = batchEntry->getCid();
  if (!batchEntry->isBatchInProcess() || batchEntry->getCid() != batchCid) {
    LOG_DEBUG(logger(),
              "The batch will be ignored as no such ongoing batch exists or different one found for this client"
                  << KVLOG(batchCid, ongoingBatchCid, senderId, clientId));
    return;
  }
  for (auto &singleReplyMsg : preProcessReplyMsgs) {
    const auto &reqSeqNum = singleReplyMsg->reqSeqNum();
    const auto &cid = singleReplyMsg->getCid();
    LOG_DEBUG(
        logger(),
        "Start handling single message from the replies batch:" << KVLOG(batchCid, reqSeqNum, cid, senderId, clientId));
    handleSinglePreProcessReplyMsg(singleReplyMsg, batchCid);
  }
}

template <typename T>
void PreProcessor::messageHandler(MessageBase *msg) {
  if (!msgs_.write_available()) {
    LOG_ERROR(logger(), "PreProcessor queue is full, returning message");
    incomingMsgsStorage_->pushExternalMsg(std::unique_ptr<MessageBase>(msg));
    return;
  }
  T *trueTypeObj = new T(msg);
  delete msg;
  msgs_.push(trueTypeObj);
  msgLoopSignal_.notify_one();
}

void PreProcessor::msgProcessingLoop() {
  while (!msgLoopDone_) {
    {
      std::unique_lock<std::mutex> l(msgLock_);
      while (!msgLoopDone_ && !msgs_.read_available()) {
        msgLoopSignal_.wait_until(l, chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_TIMEOUT_MILLI));
      }
    }

    while (!msgLoopDone_ && msgs_.read_available()) {
      auto msg = msgs_.front();
      msgs_.pop();
      if (validateMessage(msg)) {
        switch (msg->type()) {
          case (MsgCode::ClientBatchRequest): {
            onMessage<ClientBatchRequestMsg>(static_cast<ClientBatchRequestMsg *>(msg));
            break;
          }
          case (MsgCode::ClientPreProcessRequest): {
            onMessage<ClientPreProcessRequestMsg>(static_cast<ClientPreProcessRequestMsg *>(msg));
            break;
          }
          case (MsgCode::PreProcessRequest): {
            onMessage<PreProcessRequestMsg>(static_cast<PreProcessRequestMsg *>(msg));
            break;
          }
          case (MsgCode::PreProcessBatchRequest): {
            onMessage<PreProcessBatchRequestMsg>(static_cast<PreProcessBatchRequestMsg *>(msg));
            break;
          }
          case (MsgCode::PreProcessReply): {
            onMessage<PreProcessReplyMsg>(static_cast<PreProcessReplyMsg *>(msg));
            break;
          }
          case (MsgCode::PreProcessBatchReply): {
            onMessage<PreProcessBatchReplyMsg>(static_cast<PreProcessBatchReplyMsg *>(msg));
            break;
          }
          default:
            LOG_ERROR(logger(), "Unknown message" << KVLOG(msg->type()));
        }
      } else {
        preProcessorMetrics_.preProcReqInvalid++;
        delete msg;
      }
    }
  }
}

void PreProcessor::registerMsgHandlers() {
  msgHandlersRegistrator_->registerMsgHandler(
      MsgCode::ClientPreProcessRequest, bind(&PreProcessor::messageHandler<ClientPreProcessRequestMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::ClientBatchRequest,
                                              bind(&PreProcessor::messageHandler<ClientBatchRequestMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessRequest,
                                              bind(&PreProcessor::messageHandler<PreProcessRequestMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessBatchRequest,
                                              bind(&PreProcessor::messageHandler<PreProcessBatchRequestMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessReply,
                                              bind(&PreProcessor::messageHandler<PreProcessReplyMsg>, this, _1));
  msgHandlersRegistrator_->registerMsgHandler(MsgCode::PreProcessBatchReply,
                                              bind(&PreProcessor::messageHandler<PreProcessBatchReplyMsg>, this, _1));
}

void PreProcessor::handlePreProcessReplyMsg(const string &cid,
                                            PreProcessingResult result,
                                            NodeIdType clientId,
                                            uint16_t reqOffsetInBatch,
                                            SeqNum reqSeqNum,
                                            const string &batchCid) {
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), KVLOG(result, batchCid, cid, reqSeqNum, clientId, reqOffsetInBatch));
  switch (result) {
    case NONE:      // No action required - pre-processing has been already completed
    case CONTINUE:  // Not enough equal hashes collected
    case EXPIRED:
      break;
    case COMPLETE:  // Pre-processing consensus reached
      finalizePreProcessing(clientId, reqOffsetInBatch, batchCid);
      break;
    case CANCEL:  // Pre-processing consensus not reached
      cancelPreProcessing(clientId, reqOffsetInBatch);
      break;
    case RETRY_PRIMARY: {
      // Primary replica generated pre-processing result hash different that one passed consensus
      LOG_INFO(logger(), "Retry primary replica pre-processing for" << KVLOG(reqSeqNum, clientId));
      PreProcessRequestMsgSharedPtr preProcessRequestMsg;
      {
        const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
        lock_guard<mutex> lock(reqEntry->mutex);
        if (reqEntry->reqProcessingStatePtr)
          preProcessRequestMsg = reqEntry->reqProcessingStatePtr->getPreProcessRequest();
      }
      if (preProcessRequestMsg) launchAsyncReqPreProcessingJob(preProcessRequestMsg, true, true);
      break;
    }
    case CANCELLED_BY_PRIMARY:
      LOG_ERROR(logger(),
                "Received reply message with status CANCELLED_BY_PRIMARY" << KVLOG(reqSeqNum, clientId, batchCid));
      break;
  }
}

void PreProcessor::cancelPreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch) {
  preProcessorMetrics_.preProcConsensusNotReached++;
  SeqNum reqSeqNum = 0;
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
  {
    lock_guard<mutex> lock(reqEntry->mutex);
    if (reqEntry->reqProcessingStatePtr) {
      reqSeqNum = reqEntry->reqProcessingStatePtr->getReqSeqNum();
      const auto &cid = reqEntry->reqProcessingStatePtr->getReqCid();
      const auto &batchCid = reqEntry->reqProcessingStatePtr->getBatchCid();
      SCOPED_MDC_CID(cid);
      LOG_WARN(logger(),
               "Pre-processing consensus not reached; cancel request"
                   << KVLOG(cid, reqSeqNum, clientId, reqOffsetInBatch, batchCid));
      releaseClientPreProcessRequest(reqEntry, CANCEL);
    }
  }
}

void PreProcessor::finalizePreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch, const string &batchCid) {
  std::unique_ptr<impl::MessageBase> preProcessMsg;
  const auto &batchEntry = ongoingReqBatches_[clientId];
  const auto &reqEntry = batchEntry->getRequestState(reqOffsetInBatch);
  {
    concord::diagnostics::TimeRecorder scoped_timer(*histograms_.finalizePreProcessing);
    lock_guard<mutex> lock(reqEntry->mutex);
    auto &reqProcessingStatePtr = reqEntry->reqProcessingStatePtr;
    if (reqProcessingStatePtr) {
      const auto cid = reqProcessingStatePtr->getReqCid();
      const auto reqSeqNum = reqProcessingStatePtr->getReqSeqNum();
      auto &preProcessReqMsg = reqProcessingStatePtr->getPreProcessRequest();
      const auto &span_context = preProcessReqMsg->spanContext<PreProcessRequestMsgSharedPtr::element_type>();
      // Copy of the message body is unavoidable here, as we need to create a new message type which lifetime is
      // controlled by the replica while all PreProcessReply messages get released here.
      if (ReplicaConfig::instance().preExecutionResultAuthEnabled) {
        auto sigsList = reqProcessingStatePtr->getPreProcessResultSignatures();
        auto sigsBuf = PreProcessResultSignature::serializeResultSignatureList(sigsList);
        preProcessMsg = make_unique<PreProcessResultMsg>(clientId,
                                                         reqSeqNum,
                                                         reqProcessingStatePtr->getPrimaryPreProcessedResultLen(),
                                                         reqProcessingStatePtr->getPrimaryPreProcessedResult(),
                                                         reqProcessingStatePtr->getReqTimeoutMilli(),
                                                         cid,
                                                         span_context,
                                                         reqProcessingStatePtr->getReqSignature(),
                                                         reqProcessingStatePtr->getReqSignatureLength(),
                                                         sigsBuf);
        LOG_DEBUG(logger(),
                  "Pass PreProcessResultMsg to ReplicaImp for consensus"
                      << KVLOG(cid, batchCid, reqSeqNum, clientId, reqOffsetInBatch));
      } else {
        preProcessMsg = make_unique<ClientRequestMsg>(clientId,
                                                      HAS_PRE_PROCESSED_FLAG,
                                                      reqSeqNum,
                                                      reqProcessingStatePtr->getPrimaryPreProcessedResultLen(),
                                                      reqProcessingStatePtr->getPrimaryPreProcessedResult(),
                                                      reqProcessingStatePtr->getReqTimeoutMilli(),
                                                      cid,
                                                      span_context,
                                                      reqProcessingStatePtr->getReqSignature(),
                                                      reqProcessingStatePtr->getReqSignatureLength());
        LOG_DEBUG(logger(),
                  "Pass pre-processed ClientRequestMsg to ReplicaImp for consensus"
                      << KVLOG(cid, batchCid, reqSeqNum, clientId, reqOffsetInBatch));
      }

      preProcessorMetrics_.preProcReqCompleted++;
      incomingMsgsStorage_->pushExternalMsg(move(preProcessMsg));

      releaseClientPreProcessRequest(reqEntry, COMPLETE);
      LOG_INFO(logger(), "Pre-processing completed for" << KVLOG(cid, batchCid, reqSeqNum, clientId, reqOffsetInBatch));
    }
  }
  if (batchedPreProcessEnabled_) batchEntry->finalizeBatchIfCompleted();
}

uint16_t PreProcessor::numOfRequiredReplies() { return myReplica_.getReplicaConfig().fVal; }

// This function should be always called under a reqEntry->mutex lock
bool PreProcessor::registerRequest(const string &batchCid,
                                   uint32_t batchSize,
                                   ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                   PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                                   uint16_t reqOffsetInBatch) {
  NodeIdType clientId = 0;
  NodeIdType senderId = 0;
  SeqNum reqSeqNum = 0;
  string cid;
  uint32_t requestSignatureLength = 0;
  char *requestSignature = nullptr;

  bool clientReqMsgSpecified = false;
  if (clientReqMsg) {
    clientId = clientReqMsg->clientProxyId();
    senderId = clientReqMsg->senderId();
    reqSeqNum = clientReqMsg->requestSeqNum();
    cid = clientReqMsg->getCid();
    clientReqMsgSpecified = true;
    requestSignatureLength = clientReqMsg->requestSignatureLength();
    requestSignature = clientReqMsg->requestSignature();
  } else {
    clientId = preProcessRequestMsg->clientId();
    senderId = preProcessRequestMsg->senderId();
    reqSeqNum = preProcessRequestMsg->reqSeqNum();
    cid = preProcessRequestMsg->getCid();
    requestSignatureLength = preProcessRequestMsg->requestSignatureLength();
    requestSignature = preProcessRequestMsg->requestSignature();
  }
  SCOPED_MDC_CID(cid);
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
  if (batchedPreProcessEnabled_ && batchSize > 1) {
    if (preProcessRequestMsg)
      ongoingReqBatches_[clientId]->startBatch(batchCid, batchSize);
    else
      ongoingReqBatches_[clientId]->registerBatch(batchCid, batchSize);
  }
  if (!reqEntry->reqProcessingStatePtr) {
    reqEntry->reqProcessingStatePtr = make_unique<RequestProcessingState>(numOfReplicas_,
                                                                          batchCid,
                                                                          clientId,
                                                                          reqOffsetInBatch,
                                                                          cid,
                                                                          reqSeqNum,
                                                                          move(clientReqMsg),
                                                                          preProcessRequestMsg,
                                                                          requestSignature,
                                                                          requestSignatureLength);
  } else if (!reqEntry->reqProcessingStatePtr->getPreProcessRequest())
    // The request was registered before as arrived directly from the client
    reqEntry->reqProcessingStatePtr->setPreProcessRequest(preProcessRequestMsg);
  else {
    const auto &reqState = reqEntry->reqProcessingStatePtr;
    LOG_WARN(logger(),
             "Request" << KVLOG(reqSeqNum, clientId, batchCid) << " could not be registered: the entry for"
                       << KVLOG(clientId, senderId, reqOffsetInBatch) << " is occupied by "
                       << KVLOG(reqState->getReqSeqNum(), reqState->getBatchCid()));
    return false;
  }
  if (clientReqMsgSpecified) {
    LOG_DEBUG(logger(),
              KVLOG(batchCid, reqSeqNum, senderId, clientId, reqOffsetInBatch) << " registered ClientPreProcessReqMsg");
  } else {
    LOG_DEBUG(logger(),
              KVLOG(batchCid, reqSeqNum, senderId, clientId, reqOffsetInBatch) << " registered PreProcessRequestMsg");
  }
  return true;
}

void PreProcessor::releaseClientPreProcessRequestSafe(uint16_t clientId,
                                                      const RequestStateSharedPtr &reqEntry,
                                                      PreProcessingResult result) {
  lock_guard<mutex> lock(reqEntry->mutex);
  releaseClientPreProcessRequest(reqEntry, result);
}

void PreProcessor::releaseClientPreProcessRequestSafe(uint16_t clientId,
                                                      uint16_t reqOffsetInBatch,
                                                      PreProcessingResult result) {
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
  lock_guard<mutex> lock(reqEntry->mutex);
  releaseClientPreProcessRequest(reqEntry, result);
}

// This function should be always called under a reqEntry->mutex lock
void PreProcessor::releaseClientPreProcessRequest(const RequestStateSharedPtr &reqEntry, PreProcessingResult result) {
  auto &givenReq = reqEntry->reqProcessingStatePtr;
  if (givenReq) {
    const auto &clientId = givenReq->getClientId();
    const auto &reqOffsetInBatch = givenReq->getReqOffsetInBatch();
    const auto &batchCid = givenReq->getBatchCid();
    SeqNum reqSeqNum = givenReq->getReqSeqNum();
    if (result == COMPLETE) {
      if (reqEntry->reqProcessingHistory.size() >= reqEntry->reqProcessingHistoryHeight) {
        auto &removeFromHistoryReq = reqEntry->reqProcessingHistory.front();
        SCOPED_MDC_CID(removeFromHistoryReq->getReqCid());
        reqSeqNum = removeFromHistoryReq->getReqSeqNum();
        LOG_DEBUG(logger(), KVLOG(reqSeqNum, batchCid, clientId, reqOffsetInBatch) << " removed from the history");
        removeFromHistoryReq.reset();
        reqEntry->reqProcessingHistory.pop_front();
      }
      SCOPED_MDC_CID(givenReq->getReqCid());
      LOG_INFO(logger(),
               KVLOG(reqSeqNum, batchCid, clientId, reqOffsetInBatch) << " released and moved to the history");
      // No need to keep whole messages in the memory => release them before archiving
      givenReq->releaseResources();
      reqEntry->reqProcessingHistory.push_back(move(givenReq));
    } else {
      SCOPED_MDC_CID(givenReq->getReqCid());
      switch (result) {
        case CANCEL:
          LOG_INFO(logger(),
                   "Release request - no consensus reached" << KVLOG(reqSeqNum, batchCid, clientId, reqOffsetInBatch));
          break;
        case EXPIRED:
          LOG_INFO(logger(), "Release request - expired" << KVLOG(reqSeqNum, batchCid, clientId, reqOffsetInBatch));
          break;
        case CANCELLED_BY_PRIMARY:
          LOG_DEBUG(logger(),
                    "Release request - processing has been cancelled by the primary replica"
                        << KVLOG(reqSeqNum, batchCid, clientId, reqOffsetInBatch));
          break;
        default:;
      }
      givenReq.reset();
    }
    if (batchedPreProcessEnabled_) ongoingReqBatches_[clientId]->increaseNumOfCompletedReqs();
    preProcessorMetrics_.preProcInFlyRequestsNum--;
  }
}

void PreProcessor::sendMsg(char *msg, NodeIdType dest, uint16_t msgType, MsgSize msgSize) {
  int errorCode = msgsCommunicator_->sendAsyncMessage(dest, msg, msgSize);
  if (errorCode != 0) {
    LOG_ERROR(logger(), "sendMsg: sendAsyncMessage returned error" << KVLOG(errorCode, dest, msgType));
  }
}

// This function should be called under a reqEntry->mutex lock
void PreProcessor::countRetriedRequests(const ClientPreProcessReqMsgUniquePtr &clientReqMsg,
                                        const RequestStateSharedPtr &reqEntry) {
  if (!reqEntry->reqProcessingHistory.empty() &&
      reqEntry->reqProcessingHistory.back()->getReqSeqNum() == (SeqNum)clientReqMsg->requestSeqNum()) {
    LOG_DEBUG(logger(),
              "The request is going to be retried" << KVLOG(clientReqMsg->getCid(),
                                                            clientReqMsg->requestSeqNum(),
                                                            clientReqMsg->clientProxyId(),
                                                            clientReqMsg->senderId()));
    preProcessorMetrics_.preProcReqRetried++;
  }
}

// This function should be called under a reqEntry->mutex lock
bool PreProcessor::registerRequestOnPrimaryReplica(const string &batchCid,
                                                   uint32_t batchSize,
                                                   ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                                   PreProcessRequestMsgSharedPtr &preProcessRequestMsg,
                                                   uint16_t reqOffsetInBatch,
                                                   RequestStateSharedPtr reqEntry) {
  (reqEntry->reqRetryId)++;
  countRetriedRequests(clientReqMsg, reqEntry);
  const auto reqSeqNum = clientReqMsg->requestSeqNum();
  const auto clientId = clientReqMsg->clientProxyId();
  const auto senderId = clientReqMsg->senderId();
  const auto requestTimeoutMilli = clientReqMsg->requestTimeoutMilli();
  preProcessRequestMsg =
      make_shared<PreProcessRequestMsg>(REQ_TYPE_PRE_PROCESS,
                                        myReplicaId_,
                                        clientId,
                                        reqOffsetInBatch,
                                        reqSeqNum,
                                        reqEntry->reqRetryId,
                                        clientReqMsg->requestLength(),
                                        clientReqMsg->requestBuf(),
                                        clientReqMsg->getCid(),
                                        clientReqMsg->requestSignature(),
                                        clientReqMsg->requestSignatureLength(),
                                        clientReqMsg->spanContext<ClientPreProcessReqMsgUniquePtr::element_type>());
  const auto registerSucceeded =
      registerRequest(batchCid, batchSize, move(clientReqMsg), preProcessRequestMsg, reqOffsetInBatch);
  if (registerSucceeded)
    LOG_INFO(
        logger(),
        "Start request processing by a primary replica" << KVLOG(
            reqSeqNum, batchCid, batchSize, preProcessRequestMsg->getCid(), clientId, senderId, requestTimeoutMilli));
  return registerSucceeded;
}

// Primary replica: start client request handling
void PreProcessor::handleClientPreProcessRequestByPrimary(PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                                                          bool arrivedInBatch) {
  auto time_recorder = TimeRecorder(*totalPreExecDurationRecorder_.get());
  // For requests arrived in a batch PreProcessBatchRequestMsg will be sent to non-primaries
  if (!arrivedInBatch || !batchedPreProcessEnabled_) sendPreProcessRequestToAllReplicas(preProcessRequestMsg);
  // Pre-process the request and calculate a hash of the result
  launchAsyncReqPreProcessingJob(preProcessRequestMsg, true, false, std::move(time_recorder));
}

// Non-primary replica: start client request handling
// This function should be called under a reqEntry->mutex lock
void PreProcessor::registerAndHandleClientPreProcessReqOnNonPrimary(const string &batchCid,
                                                                    uint32_t batchSize,
                                                                    ClientPreProcessReqMsgUniquePtr clientReqMsg,
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
  const auto sigLen = clientReqMsg->requestSignatureLength();

  // Register a client request message with an empty PreProcessRequestMsg to allow follow up.
  if (registerRequest(batchCid, batchSize, move(clientReqMsg), PreProcessRequestMsgSharedPtr(), reqOffsetInBatch)) {
    LOG_INFO(logger(),
             "Start ClientPreProcessReq processing by a non-primary replica" << KVLOG(
                 reqSeqNum, batchCid, cid, clientId, reqOffsetInBatch, senderId, reqTimeoutMilli, msgSize, sigLen));

    if (arrivedInBatch) return;  // Need to pass the whole batch to the primary
    sendMsg(msgBody, myReplica_.currentPrimary(), msgType, msgSize);
    LOG_DEBUG(logger(),
              "Sent ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, batchCid, cid, clientId, reqOffsetInBatch)
                                                << " to the current primary");
  }
}

const char *PreProcessor::getPreProcessResultBuffer(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch) {
  // Allocate on first use buffers scheme:
  // |first client's first buffer|...|first client's last buffer|......
  // |last client's first buffer|...|last client's last buffer|
  // First client id starts after the last replica id.
  // First buffer offset = numOfReplicas_ * batchSize_
  // The number of buffers per client comes from the configuration parameter clientBatchingMaxMsgsNbr.
  const auto bufferOffset =
      (clientId - numOfReplicas_ - numOfInternalClients_) * clientMaxBatchSize_ + reqOffsetInBatch;
  LOG_TRACE(logger(), KVLOG(clientId, reqSeqNum, reqOffsetInBatch, bufferOffset, reqSeqNum));
  char *buf = nullptr;
  if (!preProcessResultBuffers_[bufferOffset].first) {
    buf = new char[maxPreExecResultSize_];
    {
      std::unique_lock lock(resultBufferLock_);
      if (!preProcessResultBuffers_[bufferOffset].first) {
        preProcessResultBuffers_[bufferOffset].second = Sliver(buf, maxPreExecResultSize_);
        preProcessResultBuffers_[bufferOffset].first = true;
      } else
        delete[] buf;
    }
  }
  return preProcessResultBuffers_[bufferOffset].second.data();
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
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
  lock_guard<mutex> lock(reqEntry->mutex);
  if (reqEntry->reqProcessingStatePtr) reqEntry->reqProcessingStatePtr->setPreprocessingRightNow(set);
}

void PreProcessor::launchAsyncReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                  bool isPrimary,
                                                  bool isRetry,
                                                  TimeRecorder &&totalPreExecDurationRecorder) {
  auto launchAsyncPreProcessJobRecorder = TimeRecorder(*launchAsyncPreProcessJobRecorder_.get());
  setPreprocessingRightNow(preProcessReqMsg->clientId(), preProcessReqMsg->reqOffsetInBatch(), true);
  auto *preProcessJob = new AsyncPreProcessJob(*this,
                                               preProcessReqMsg,
                                               isPrimary,
                                               isRetry,
                                               std::move(totalPreExecDurationRecorder),
                                               std::move(launchAsyncPreProcessJobRecorder));
  threadPool_.add(preProcessJob);
}

uint32_t PreProcessor::launchReqPreProcessing(uint16_t clientId,
                                              uint16_t reqOffsetInBatch,
                                              const string &cid,
                                              ReqId reqSeqNum,
                                              uint32_t reqLength,
                                              char *reqBuf,
                                              std::string signature,
                                              const concordUtils::SpanContext &span_context) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.launchReqPreProcessing);
  // Unused for now. Replica Specific Info not currently supported in pre-execution.
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_process_preprocess_msg");
  LOG_DEBUG(logger(), "Pass request for a pre-execution" << KVLOG(cid, reqSeqNum, clientId, reqOffsetInBatch));
  bftEngine::IRequestsHandler::ExecutionRequestsQueue accumulatedRequests;
  accumulatedRequests.push_back(bftEngine::IRequestsHandler::ExecutionRequest{
      clientId,
      reqSeqNum,
      cid,
      PRE_PROCESS_FLAG,
      reqLength,
      reqBuf,
      std::move(signature),
      maxPreExecResultSize_,
      (char *)getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch)});
  requestsHandler_.execute(accumulatedRequests, std::nullopt, cid, span);
  const IRequestsHandler::ExecutionRequest &request = accumulatedRequests.back();
  const auto status = request.outExecutionStatus;
  const auto resultLen = request.outActualReplySize;
  LOG_DEBUG(logger(), "Pre-execution operation done" << KVLOG(cid, reqSeqNum, clientId, reqOffsetInBatch));
  if (status != 0 || !resultLen) {
    LOG_FATAL(
        logger(),
        "Pre-execution failed!" << KVLOG(cid, clientId, reqOffsetInBatch, reqSeqNum, (uint32_t)status, resultLen));
    ConcordAssert(false);
  }
  return resultLen;
}

// For test purposes
ReqId PreProcessor::getOngoingReqIdForClient(uint16_t clientId, uint16_t reqOffsetInBatch) {
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
  lock_guard<mutex> lock(reqEntry->mutex);
  if (reqEntry->reqProcessingStatePtr) return reqEntry->reqProcessingStatePtr->getReqSeqNum();
  return 0;
}

PreProcessingResult PreProcessor::handlePreProcessedReqByPrimaryAndGetConsensusResult(uint16_t clientId,
                                                                                      uint16_t reqOffsetInBatch,
                                                                                      uint32_t resultBufLen) {
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
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
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.handlePreProcessedReqPrimaryRetry);
  if (handlePreProcessedReqByPrimaryAndGetConsensusResult(clientId, reqOffsetInBatch, resultBufLen) == COMPLETE)
    finalizePreProcessing(clientId, reqOffsetInBatch);
  else
    cancelPreProcessing(clientId, reqOffsetInBatch);
}

void PreProcessor::handleReqPreProcessedByPrimary(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
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

void PreProcessor::releaseReqAndSendReplyMsg(PreProcessReplyMsgSharedPtr replyMsg) {
  // Release the request before sending a reply message to the primary replica to be able accepting new messages
  releaseClientPreProcessRequestSafe(replyMsg->clientId(), replyMsg->reqOffsetInBatch(), COMPLETE);
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
  LOG_INFO(logger(),
           "Pre-processing completed by a non-primary replica and the reply message sent to the primary"
               << KVLOG(replyMsg->reqSeqNum(),
                        replyMsg->clientId(),
                        replyMsg->reqOffsetInBatch(),
                        replyMsg->getCid(),
                        replyMsg->reqRetryId(),
                        myReplica_.currentPrimary()));
}

void PreProcessor::handleReqPreProcessedByNonPrimary(uint16_t clientId,
                                                     uint16_t reqOffsetInBatch,
                                                     ReqId reqSeqNum,
                                                     uint64_t reqRetryId,
                                                     uint32_t resBufLen,
                                                     const std::string &cid) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.handlePreProcessedReqByNonPrimary);
  setPreprocessingRightNow(clientId, reqOffsetInBatch, false);
  auto replyMsg = make_shared<PreProcessReplyMsg>(myReplicaId_,
                                                  clientId,
                                                  reqOffsetInBatch,
                                                  reqSeqNum,
                                                  reqRetryId,
                                                  getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch),
                                                  resBufLen,
                                                  cid,
                                                  STATUS_GOOD);
  const auto &batchEntry = ongoingReqBatches_[clientId];
  if (batchedPreProcessEnabled_ && batchEntry->isBatchInProcess()) {
    batchEntry->addReply(replyMsg);
    batchEntry->releaseReqsAndSendBatchedReplyIfCompleted();
  } else {
    releaseReqAndSendReplyMsg(replyMsg);
  }
}

void PreProcessor::handleReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                             bool isPrimary,
                                             bool isRetry) {
  const string cid = preProcessReqMsg->getCid();
  const uint16_t &clientId = preProcessReqMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  const auto &span_context = preProcessReqMsg->spanContext<PreProcessRequestMsgSharedPtr::element_type>();
  uint32_t actualResultBufLen = launchReqPreProcessing(
      clientId,
      preProcessReqMsg->reqOffsetInBatch(),
      cid,
      reqSeqNum,
      preProcessReqMsg->requestLength(),
      preProcessReqMsg->requestBuf(),
      std::string(preProcessReqMsg->requestSignature(), preProcessReqMsg->requestSignatureLength()),
      span_context);
  if (isPrimary && isRetry) {
    handlePreProcessedReqPrimaryRetry(clientId, reqOffsetInBatch, actualResultBufLen);
    return;
  }
  SCOPED_MDC_CID(cid);
  LOG_DEBUG(logger(), "Request pre-processed" << KVLOG(isPrimary, reqSeqNum, clientId, reqOffsetInBatch));
  if (isPrimary) {
    pm_->Delay<concord::performance::SlowdownPhase::PreProcessorAfterPreexecPrimary>();
    handleReqPreProcessedByPrimary(preProcessReqMsg, clientId, actualResultBufLen);
  } else {
    pm_->Delay<concord::performance::SlowdownPhase::PreProcessorAfterPreexecNonPrimary>();
    handleReqPreProcessedByNonPrimary(clientId,
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
                                       TimeRecorder &&totalPreExecDurationRecorder,
                                       TimeRecorder &&launchAsyncPreProcessJobRecorder)
    : preProcessor_(preProcessor),
      preProcessReqMsg_(preProcessReqMsg),
      isPrimary_(isPrimary),
      isRetry_(isRetry),
      totalJobDurationRecorder_(std::move(totalPreExecDurationRecorder)),
      launchAsyncPreProcessJobRecorder_(std::move(launchAsyncPreProcessJobRecorder)) {}

void AsyncPreProcessJob::execute() {
  const auto launchAsyncJobDurationInMs = launchAsyncPreProcessJobRecorder_.wrapUpRecording() / 1000000;
  preProcessor_.launchAsyncJobTimeAvg_.add((double)launchAsyncJobDurationInMs);
  preProcessor_.preProcessorMetrics_.launchAsyncPreProcessJobTimeAvg.Get().Set(
      (uint64_t)preProcessor_.launchAsyncJobTimeAvg_.avg());
  if (preProcessor_.launchAsyncJobTimeAvg_.numOfElements() == resetFrequency_) {
    LOG_DEBUG(preProcessor_.logger(),
              "Launch async job average duration in ms" << KVLOG(preProcessor_.launchAsyncJobTimeAvg_.avg()));
    preProcessor_.launchAsyncJobTimeAvg_.reset();
  }
  MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(preProcessor_.myReplicaId_));
  MDC_PUT(MDC_THREAD_KEY, "async-preprocess");
  preProcessor_.handleReqPreProcessingJob(preProcessReqMsg_, isPrimary_, isRetry_);
}

void AsyncPreProcessJob::release() {
  const auto preExecDurationInMs = totalJobDurationRecorder_.wrapUpRecording() / 1000000;
  preProcessor_.totalPreProcessingTime_.add((double)preExecDurationInMs);
  preProcessor_.preProcessorMetrics_.preProcessingTimeAvg.Get().Set(
      (uint64_t)preProcessor_.totalPreProcessingTime_.avg());
  if (preProcessor_.totalPreProcessingTime_.numOfElements() == resetFrequency_) {
    LOG_DEBUG(preProcessor_.logger(),
              "Total pre-processing average duration in ms" << KVLOG(preProcessor_.totalPreProcessingTime_.avg()));
    preProcessor_.totalPreProcessingTime_.reset();
  }
  delete this;
}

}  // namespace preprocessor
