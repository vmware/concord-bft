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

#include "log/logger.hpp"
#include "InternalReplicaApi.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "OpenTracing.hpp"
#include "SigManager.hpp"
#include "messages/PreProcessResultMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "ControlStateManager.hpp"

namespace preprocessor {

using namespace bftEngine;
using namespace concord::util;
using namespace concordUtils;
using namespace std;
using namespace std::placeholders;
using namespace concordUtil;

uint8_t RequestState::reqProcessingHistoryHeight = 1;

//**************** Class RequestsBatch ****************//

void RequestsBatch::init() {
  for (uint16_t i = 0; i < PreProcessor::clientMaxBatchSize_; i++) {
    // Placeholders for all requests in a batch
    requestsMap_[i] = make_shared<RequestState>();
  }
}

bool RequestsBatch::isBatchRegistered(string &batchCid) const {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  batchCid = batchCid_;
  return batchRegistered_;
}

bool RequestsBatch::isBatchInProcess() const {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  return batchInProcess_;
}

bool RequestsBatch::isBatchInProcess(string &batchCid) const {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  batchCid = batchCid_;
  return batchInProcess_;
}

uint64_t RequestsBatch::getBlockId() const {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  return cidToBlockId_.second;
}

// Should be called under batchMutex_
void RequestsBatch::setBatchParameters(const std::string &batchCid, uint32_t batchSize) {
  batchCid_ = batchCid;
  batchSize_ = batchSize;
  // We want to preserve block-id between retries, therefore cidToBlockId_ is not being part of the reset,
  // and is being set only if batchCid is new.
  if (cidToBlockId_.first != batchCid_) {
    LOG_DEBUG(preProcessor_.logger(),
              "Resetting batch block id. Old batchCid: " << cidToBlockId_.first << " to: " << batchCid_
                                                         << ". Old block id: " << cidToBlockId_.second
                                                         << " to: " << GlobalData::current_block_id);
    cidToBlockId_.first = batchCid_;
    cidToBlockId_.second = GlobalData::current_block_id;
  }
}

// Should be called under batchMutex_
void RequestsBatch::resetBatchParams() {
  const auto batchCid = batchCid_;
  const auto batchSize = batchSize_;
  const auto numOfCompletedReqs = numOfCompletedReqs_.load();
  batchSize_ = 0;
  batchInProcess_ = false;
  batchRegistered_ = false;
  numOfCompletedReqs_ = 0;
  batchCid_ = "";
  repliesList_.clear();
  LOG_INFO(preProcessor_.logger(),
           "The batch parameters have been reset" << KVLOG(clientId_, batchCid, numOfCompletedReqs, batchSize));
}

// Called when ClientBatchRequestMsg received
void RequestsBatch::registerBatch(NodeIdType senderId, const std::string &batchCid, uint32_t batchSize) {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  if (!batchRegistered_) {
    batchRegistered_ = true;
    setBatchParameters(batchCid, batchSize);
    LOG_INFO(preProcessor_.logger(),
             "The batch has been registered" << KVLOG(senderId, clientId_, batchCid_, batchSize_));
  }
}

// On non-primary: called when PreProcessBatchRequestMsg received
void RequestsBatch::startBatch(NodeIdType senderId, const std::string &batchCid, uint32_t batchSize) {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  if (!batchInProcess_) {
    batchInProcess_ = true;
    batchRegistered_ = true;
    setBatchParameters(batchCid, batchSize);
    LOG_INFO(preProcessor_.logger(), "The batch has been started" << KVLOG(senderId, clientId_, batchCid_, batchSize_));
  }
}

const string RequestsBatch::getBatchCid() const {
  const std::lock_guard<std::mutex> lock(batchMutex_);
  return batchCid_;
}

RequestStateSharedPtr &RequestsBatch::getRequestState(uint16_t reqOffsetInBatch) {
  ConcordAssertLE(reqOffsetInBatch, PreProcessor::clientMaxBatchSize_ - 1);
  return requestsMap_[reqOffsetInBatch];
}

void RequestsBatch::increaseNumOfCompletedReqs(uint32_t count) {
  numOfCompletedReqs_ += count;
  LOG_DEBUG(preProcessor_.logger(),
            "Increased a number of completed requests" << KVLOG(clientId_, batchCid_, batchSize_, numOfCompletedReqs_));
}

void RequestsBatch::handlePossiblyExpiredRequests() {
  uint32_t reqsExpired = 0;
  for (const auto &req : requestsMap_)
    if (preProcessor_.handlePossiblyExpiredRequest(req.second)) reqsExpired++;

  bool batchCancelled = false;
  string batchCid;
  atomic_uint32_t batchSize = 0;
  auto numOfCompletedReqs = numOfCompletedReqs_.load();
  {
    const std::lock_guard<std::mutex> lock(batchMutex_);
    if (batchSize_ && reqsExpired && (numOfCompletedReqs_ >= batchSize_)) {
      batchCid = batchCid_;
      batchSize = batchSize_;
      resetBatchParams();
      batchCancelled = true;
    }
  }
  if (batchCancelled)
    LOG_INFO(preProcessor_.logger(),
             "The batch has been cancelled as expired"
                 << KVLOG(clientId_, batchCid, reqsExpired, numOfCompletedReqs, batchSize));
}

void RequestsBatch::cancelBatchAndReleaseRequests(const string &batchCidToCancel, PreProcessingResult status) {
  atomic_uint32_t batchSize = 0;
  {
    const lock_guard<mutex> lock(batchMutex_);
    if (batchCid_ != batchCidToCancel) {
      LOG_INFO(preProcessor_.logger(),
               "The batch has been cancelled/completed earlier; do nothing"
                   << KVLOG(clientId_, batchCidToCancel, batchCid_));
      return;
    }
    for (const auto &reqEntry : requestsMap_) {
      if (status == CANCEL) preProcessor_.preProcessorMetrics_.preProcConsensusNotReached++;
      if (reqEntry.second) preProcessor_.releaseClientPreProcessRequestSafe(clientId_, reqEntry.second, status);
    }
    batchSize = batchSize_;
    resetBatchParams();
  }
  LOG_INFO(preProcessor_.logger(), "The batch has been cancelled" << KVLOG(clientId_, batchCidToCancel, batchSize));
}

// On non-primary replicas
void RequestsBatch::releaseReqsAndSendBatchedReplyIfCompleted(PreProcessReplyMsgSharedPtr replyMsg) {
  uint32_t replyMsgsSize = 0;
  const auto senderId = preProcessor_.myReplicaId_;
  const auto primaryId = preProcessor_.myReplica_.currentPrimary();
  string batchCid;
  atomic_uint32_t batchSize = 0;
  PreProcessBatchReplyMsgSharedPtr batchReplyMsg;
  list<uint16_t> reqOffsetsInBatch;
  {
    unique_lock<mutex> lock(batchMutex_);
    if (!batchInProcess_) {
      LOG_DEBUG(preProcessor_.logger(), "The batch is not in process; ignore" << KVLOG(clientId_, batchCid_));
      return;
    }
    repliesList_.push_back(replyMsg);
    if (repliesList_.size() < batchSize_) {
      LOG_DEBUG(preProcessor_.logger(),
                "Not all replies collected" << KVLOG(senderId, clientId_, batchCid_, repliesList_.size(), batchSize_));
      return;
    }
    batchCid = batchCid_;
    batchSize = batchSize_;
    // The last batch request has pre-processed => send batched reply message
    for (auto const &reply : repliesList_) {
      replyMsgsSize += reply->size();
      reqOffsetsInBatch.emplace_front(reply->reqOffsetInBatch());
    }
    batchReplyMsg = make_shared<PreProcessBatchReplyMsg>(
        clientId_, senderId, repliesList_, batchCid_, replyMsgsSize, preProcessor_.myReplica_.getCurrentView());
  }
  for (const auto reqOffsetInBatch : reqOffsetsInBatch)
    preProcessor_.releaseClientPreProcessRequestSafe(clientId_, reqOffsetInBatch, COMPLETE);
  {
    unique_lock<mutex> lock(batchMutex_);
    resetBatchParams();
  }
  preProcessor_.sendMsg(batchReplyMsg->body(), primaryId, batchReplyMsg->type(), batchReplyMsg->size());
  LOG_INFO(preProcessor_.logger(),
           "Pre-processing completed and the batched reply message sent to the primary replica"
               << KVLOG(senderId, clientId_, batchCid, batchSize, primaryId));
}

// On a primary replica
// Should be called under an entry lock
void RequestsBatch::cancelRequestAndBatchIfCompleted(const string &reqBatchCid,
                                                     uint16_t reqOffsetInBatch,
                                                     PreProcessingResult status) {
  unique_lock<mutex> lock(batchMutex_);
  if (batchCid_ != reqBatchCid) {
    LOG_INFO(preProcessor_.logger(),
             "The batch has been cancelled/completed earlier; do nothing" << KVLOG(clientId_, reqBatchCid, batchCid_));
    return;
  }
  const auto &reqEntry = requestsMap_[reqOffsetInBatch];
  if (reqEntry) {
    if (status == CANCEL) preProcessor_.preProcessorMetrics_.preProcConsensusNotReached++;
    lock.unlock();
    preProcessor_.releaseClientPreProcessRequest(reqEntry, status);
    lock.lock();
    finalizeBatchIfCompleted();
  }
}

// On a primary replica
void RequestsBatch::finalizeBatchIfCompletedSafe(const std::string &batchCid) {
  const lock_guard<mutex> lock(batchMutex_);
  if (!batchCid_.empty() && (batchCid == batchCid_)) finalizeBatchIfCompleted();
}

// On a primary replica; unsafe
void RequestsBatch::finalizeBatchIfCompleted() {
  if (!batchSize_) {
    LOG_WARN(preProcessor_.logger(), "Release called for an empty batch" << KVLOG(clientId_));
    return;
  }
  if (numOfCompletedReqs_ < batchSize_) {
    LOG_DEBUG(preProcessor_.logger(),
              "The batch is not completed yet" << KVLOG(clientId_, batchCid_, numOfCompletedReqs_, batchSize_));
    return;
  }
  const auto batchCid = batchCid_;
  const auto batchSize = batchSize_;
  const auto numOfCompletedReqs = numOfCompletedReqs_.load();
  resetBatchParams();
  LOG_INFO(preProcessor_.logger(),
           "The batch has been released" << KVLOG(clientId_, batchCid, numOfCompletedReqs, batchSize));
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
                                          0,
                                          preProcessor_.myReplica_.getCurrentView(),
                                          clientMsg->spanContext<ClientPreProcessReqMsgSharedPtr::element_type>());
    reqsBatch.push_back(preProcessReqMsg);
    overallPreProcessReqMsgsSize += preProcessReqMsg->size();
  }
  auto preProcessBatchReqMsg = make_shared<PreProcessBatchRequestMsg>(REQ_TYPE_CANCEL,
                                                                      clientId_,
                                                                      preProcessor_.myReplicaId_,
                                                                      reqsBatch,
                                                                      batchCid_,
                                                                      overallPreProcessReqMsgsSize,
                                                                      preProcessor_.myReplica_.getCurrentView());
  LOG_DEBUG(preProcessor_.logger(),
            "Sending PreProcessBatchRequestMsg with REQ_TYPE_CANCEL" << KVLOG(clientId_, batchCid_, destId));
  preProcessor_.sendMsg(
      preProcessBatchReqMsg->body(), destId, preProcessBatchReqMsg->type(), preProcessBatchReqMsg->size());
}

bool PreProcessor::validateMessage(MessageBase *msg) const {
  try {
    msg->validate(myReplica_.getReplicasInfo());
    return true;
  } catch (std::exception &e) {
    LOG_WARN(logger(), "Message validation failed" << KVLOG(msg->type(), e.what()));
    return false;
  }
}

void PreProcessor::stop() {
  if (!msgLoopDone_) {
    msgLoopDone_ = true;
    msgLoopSignal_.notify_all();
    cancelTimers();
  }

  if (!threadPool_.isStopped()) {
    threadPool_.stop();
    if (msgLoopThread_.joinable()) {
      msgLoopThread_.join();
    }
  }
}

PreProcessor::PreProcessor(shared_ptr<MsgsCommunicator> &msgsCommunicator,
                           shared_ptr<IncomingMsgsStorage> &incomingMsgsStorage,
                           shared_ptr<MsgHandlersRegistrator> &msgHandlersRegistrator,
                           IRequestsHandler &requestsHandler,
                           InternalReplicaApi &myReplica,
                           concordUtil::Timers &timers,
                           shared_ptr<concord::performance::PerformanceManager> &pm)
    : msgsCommunicator_(msgsCommunicator),
      incomingMsgsStorage_(incomingMsgsStorage),
      msgHandlersRegistrator_(msgHandlersRegistrator),
      requestsHandler_(requestsHandler),
      myReplica_(myReplica),
      myReplicaId_(myReplica.getReplicaConfig().replicaId),
      maxExternalMsgSize_(myReplica.getReplicaConfig().maxExternalMessageSize),
      maxPreExecResultSize_(maxExternalMsgSize_ - sizeof(uint64_t)),
      numOfReplicas_(myReplica.getReplicaConfig().numReplicas + myReplica.getReplicaConfig().numRoReplicas),
      numOfClientProxies_(myReplica.getReplicaConfig().numOfClientProxies),
      clientBatchingEnabled_(myReplica.getReplicaConfig().clientBatchingEnabled),
      threadPool_("PreProcessor::threadPool"),
      memoryPool_(maxExternalMsgSize_, timers),
      metricsComponent_{concordMetrics::Component("preProcessor", std::make_shared<concordMetrics::Aggregator>())},
      metricsLastDumpTime_(0),
      metricsDumpIntervalInSec_{myReplica_.getReplicaConfig().metricsDumpIntervalSeconds},
      preProcessorMetrics_{metricsComponent_.RegisterAtomicCounter("preProcReqReceived"),
                           metricsComponent_.RegisterCounter("preProcBatchReqReceived"),
                           metricsComponent_.RegisterCounter("preProcReqInvalid"),
                           metricsComponent_.RegisterAtomicCounter("preProcReqIgnored"),
                           metricsComponent_.RegisterCounter("preProcConsensusNotReached"),
                           metricsComponent_.RegisterCounter("preProcessRequestTimedOut"),
                           metricsComponent_.RegisterCounter("preProcPossiblePrimaryFaultDetected"),
                           metricsComponent_.RegisterCounter("preProcReqCompleted"),
                           metricsComponent_.RegisterCounter("preProcReqRetried"),
                           metricsComponent_.RegisterAtomicGauge("preProcessingTimeAvg", 0),
                           metricsComponent_.RegisterAtomicGauge("launchAsyncPreProcessJobTimeAvg", 0),
                           metricsComponent_.RegisterAtomicGauge("PreProcInFlyRequestsNum", 0)},
      metric_pre_exe_duration_{metricsComponent_, "metric_pre_exe_duration_", 10000, 1000, true},
      totalPreProcessingTime_(true),
      launchAsyncJobTimeAvg_(true),
      preExecReqStatusCheckPeriodMilli_(myReplica_.getReplicaConfig().preExecReqStatusCheckTimerMillisec),
      timers_{timers},
      totalPreExecDurationRecorder_{histograms_.totalPreExecutionDuration},
      launchAsyncPreProcessJobRecorder_{histograms_.launchAsyncPreProcessJob},
      pm_{pm},
      memoryPoolEnabled_(myReplica_.getReplicaConfig().enablePreProcessorMemoryPool) {
  // register a stop call back for the new preprocessor in order to stop it before replica is destroyed
  myReplica_.registerStopCallback([this]() { this->stop(); });

  clientMaxBatchSize_ = clientBatchingEnabled_ ? myReplica.getReplicaConfig().clientBatchingMaxMsgsNbr : 1,
  registerMsgHandlers();
  metricsComponent_.Register();
  const uint16_t numOfExternalClients = myReplica.getReplicaConfig().numOfExternalClients;
  const uint16_t numOfReqEntries = numOfExternalClients * clientMaxBatchSize_;
  for (uint16_t i = 0; i < numOfReqEntries; i++) {
    // Placeholders for all clients including batches
    preProcessResultBuffers_.emplace_back(make_shared<SafeResultBuffer>());
  }
  if (memoryPoolEnabled_) {
    // Initially, allocate a memory for all batches of one client (clientMaxBatchSize_)
    memoryPool_.allocatePool(clientMaxBatchSize_, numOfReqEntries);
  }
  const uint16_t firstClientId = numOfReplicas_ + numOfClientProxies_;
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
           "PreProcessor initialization:" << KVLOG(numOfReplicas_,
                                                   numOfExternalClients,
                                                   numOfClientProxies_,
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
  LOG_TRACE(logger(), "~PreProcessor start");

  stop();

  if (!memoryPoolEnabled_) {
    for (const auto &result : preProcessResultBuffers_) {
      delete[] result->buffer;
    }
  }
  while (msgs_.read_available()) delete msgs_.front();
  LOG_TRACE(logger(), "~PreProcessor done");
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
  timers_.cancel(metricsTimer_);
  if (preExecReqStatusCheckPeriodMilli_ != 0) {
    timers_.cancel(requestsStatusCheckTimer_);
  }
}

// This function should be always called under a reqEntry->mutex lock
// Resend PreProcessRequestMsg to replicas that have previously rejected it
void PreProcessor::resendPreProcessRequest(const RequestProcessingStateUniquePtr &reqStatePtr) {
  const auto &rejectedReplicasList = reqStatePtr->getRejectedReplicasList();
  const auto &preProcessReqMsg = reqStatePtr->getPreProcessRequest();
  const auto &batchCid = reqStatePtr->getBatchCid();
  if (!rejectedReplicasList.empty() && preProcessReqMsg) {
    const auto &clientId = preProcessReqMsg->clientId();
    const auto &reqSeqNum = preProcessReqMsg->reqSeqNum();
    const auto &reqCid = preProcessReqMsg->getCid();
    for (const auto &destId : rejectedReplicasList) {
      LOG_DEBUG(logger(), "Resending PreProcessRequestMsg" << KVLOG(clientId, batchCid, reqSeqNum, reqCid, destId));
      sendMsg(preProcessReqMsg->body(), destId, preProcessReqMsg->type(), preProcessReqMsg->size());
    }
    reqStatePtr->resetRejectedReplicasList();
  }
}

bool PreProcessor::handlePossiblyExpiredRequest(const RequestStateSharedPtr &reqStateEntry) {
  lock_guard<mutex> lock(reqStateEntry->mutex);
  if (!reqStateEntry->reqProcessingStatePtr) return false;
  const auto &reqStatePtr = reqStateEntry->reqProcessingStatePtr;
  if (reqStatePtr->isReqTimedOut()) {
    preProcessorMetrics_.preProcessRequestTimedOut++;
    preProcessorMetrics_.preProcPossiblePrimaryFaultDetected++;
    // The request could expire do to failed primary replica, let ReplicaImp address that
    const auto &reqSeqNum = reqStatePtr->getReqSeqNum();
    const auto &clientId = reqStatePtr->getClientId();
    const auto &batchCid = reqStatePtr->getBatchCid();
    const auto &reqCid = reqStatePtr->getReqCid();
    if (!reqStatePtr->getPreProcessRequest()) {
      /* preProcInFlyRequestsNum metric does not get increased when the PreProcessRequestMsg does not
       * arrive for the request, which mostly happens during a view change. In this case, we only
       * decrease this metric, so it gets overflowed. We increase the metric here as compensation.
       */
      preProcessorMetrics_.preProcInFlyRequestsNum++;
    }
    LOG_INFO(logger(), "Let replica handle request" << KVLOG(batchCid, reqSeqNum, reqCid, clientId));
    incomingMsgsStorage_->pushExternalMsg(reqStatePtr->buildClientRequestMsg(true));
    releaseClientPreProcessRequest(reqStateEntry, EXPIRED);
    return true;
  }
  if (myReplica_.isCurrentPrimary() && reqStatePtr->definePreProcessingConsensusResult() == CONTINUE)
    resendPreProcessRequest(reqStatePtr);
  return false;
}

void PreProcessor::onRequestsStatusCheckTimer() {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onRequestsStatusCheckTimer);
  // Pass through all ongoing requests and abort the pre-execution for those that are timed out.
  for (auto &batchEntry : ongoingReqBatches_) batchEntry.second->handlePossiblyExpiredRequests();
}

bool PreProcessor::checkClientMsgCorrectness(uint64_t reqSeqNum,
                                             const string &reqCid,
                                             bool isReadOnly,
                                             uint16_t clientId,
                                             NodeIdType senderId,
                                             const std::string &batchCid) const {
  if (myReplica_.isCollectingState()) {
    // Do not put log as INFO level: logs slow down State Transfer and are printed thosands of times.
    // There are other logs which shows replica is in ST, and everybody should be aware that most messages are
    // blocks during ST.
    LOG_DEBUG(logger(),
              "Ignore ClientPreProcessRequestMsg as the replica is collecting missing state from other replicas"
                  << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId));
    return false;
  }
  if (isReadOnly) {
    LOG_INFO(logger(),
             "Ignore ClientPreProcessRequestMsg as it is signed as read-only"
                 << KVLOG(batchCid, reqSeqNum, reqCid, clientId));
    return false;
  }
  const bool &invalidClient = !myReplica_.isValidClient(clientId);
  const bool &sentFromReplicaToNonPrimary = myReplica_.isIdOfReplica(senderId) && !myReplica_.isCurrentPrimary();
  if (invalidClient || sentFromReplicaToNonPrimary) {
    LOG_WARN(logger(),
             "Ignore ClientPreProcessRequestMsg as invalid"
                 << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, invalidClient, sentFromReplicaToNonPrimary));
    return false;
  }
  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(),
             "Ignore ClientPreProcessRequestMsg as current view is inactive"
                 << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId));
    return false;
  }
  return true;
}

bool PreProcessor::checkClientBatchMsgCorrectness(const ClientBatchRequestMsgUniquePtr &clientBatchReqMsg) {
  if (!clientBatchingEnabled_) {
    LOG_WARN(logger(),
             "Batching functionality is disabled => reject message"
                 << KVLOG(clientBatchReqMsg->clientId(), clientBatchReqMsg->senderId(), clientBatchReqMsg->getCid()));
    preProcessorMetrics_.preProcReqIgnored++;
    return false;
  }

  const auto numMsgsInBatch = clientBatchReqMsg->numOfMessagesInBatch();
  if (numMsgsInBatch > clientMaxBatchSize_) {
    LOG_WARN(logger(),
             "Ignoring client batch that exceeds max number of requests ("
                 << numMsgsInBatch << " > " << clientMaxBatchSize_ << ")"
                 << KVLOG(clientBatchReqMsg->clientId(), clientBatchReqMsg->senderId(), clientBatchReqMsg->getCid()));
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
    LOG_DEBUG(logger(), "--preProcessor metrics dump--" + metricsComponent_.ToJson());
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
  auto replyMsg = make_shared<PreProcessReplyMsg>(myReplicaId_,
                                                  clientId,
                                                  reqOffsetInBatch,
                                                  reqSeqNum,
                                                  reqRetryId,
                                                  getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch),
                                                  0,
                                                  cid,
                                                  STATUS_REJECT,
                                                  OperationResult::NOT_READY,
                                                  myReplica_.getCurrentView());
  const auto &batchCid = ongoingReqBatches_[clientId]->getBatchCid();
  LOG_DEBUG(
      logger(),
      KVLOG(batchCid, reqSeqNum, senderId, clientId, reqOffsetInBatch, ongoingReqSeqNum, ongoingCid)
          << " Sending PreProcessReplyMsg with STATUS_REJECT as another PreProcessRequest from the same client is "
             "in progress");
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
}

template <>
void PreProcessor::onMessage<ClientPreProcessRequestMsg>(ClientPreProcessReqMsgUniquePtr msg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onClientPreProcessRequestMsg);
  preProcessorMetrics_.preProcReqReceived++;
  const string &reqCid = msg->getCid();
  const NodeIdType &senderId = msg->senderId();
  const NodeIdType &clientId = msg->clientProxyId();
  const ReqId &reqSeqNum = msg->requestSeqNum();
  const auto &reqTimeoutMilli = msg->requestTimeoutMilli();
  LOG_DEBUG(logger(),
            "Received ClientPreProcessRequestMsg" << KVLOG(reqSeqNum, reqCid, clientId, senderId, reqTimeoutMilli));
  if (!checkClientMsgCorrectness(reqSeqNum, reqCid, msg->isReadOnly(), clientId, senderId, "")) return;
  PreProcessRequestMsgSharedPtr preProcessRequestMsg;
  handleSingleClientRequestMessage(std::move(msg), senderId, false, 0, preProcessRequestMsg, "", 1);
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestPreProcessingRightNow(const RequestStateSharedPtr &reqEntry,
                                                  ReqId reqSeqNum,
                                                  NodeIdType clientId,
                                                  const string &batchCid,
                                                  NodeIdType senderId) {
  if (reqEntry->reqProcessingStatePtr) {
    const auto &ongoingReqSeqNum = reqEntry->reqProcessingStatePtr->getReqSeqNum();
    const auto &ongoingReqCid = reqEntry->reqProcessingStatePtr->getReqCid();
    const auto &ongoingBatchCid = reqEntry->reqProcessingStatePtr->getBatchCid();
    LOG_DEBUG(logger(),
              KVLOG(batchCid, reqSeqNum, clientId, senderId)
                  << " is ignored:" << KVLOG(ongoingBatchCid, ongoingReqSeqNum, ongoingReqCid) << " is in progress");
    preProcessorMetrics_.preProcReqIgnored++;
    return true;
  }
  return false;
}

// Should be called under reqEntry->mutex lock
bool PreProcessor::isRequestPassingConsensusOrPostExec(
    SeqNum reqSeqNum, NodeIdType senderId, NodeIdType clientId, const string &batchCid, const string &reqCid) {
  if (myReplica_.isClientRequestInProcess(clientId, reqSeqNum)) {
    LOG_DEBUG(logger(),
              "The specified request is in consensus or being post-executed right now - ignore"
                  << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId));
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
                                               const string &reqCid) {
  if (!reqEntry->reqProcessingStatePtr) {
    // Verify that an arrived request is newer than any other in the requests' history for this client
    for (const auto &oldReqState : reqEntry->reqProcessingHistory) {
      if (oldReqState->getReqSeqNum() > reqSeqNum) {
        LOG_DEBUG(
            logger(),
            "The request is ignored as the newer request from this client has already been pre-processed"
                << KVLOG(batchCid, reqSeqNum, reqCid, clientId, oldReqState->getReqCid(), oldReqState->getReqSeqNum()));
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
  const NodeIdType &clientId = clientMsg->clientProxyId();
  const ReqId &reqSeqNum = clientMsg->requestSeqNum();
  const auto &reqCid = clientMsg->getCid();

  bool registerSucceeded = false;
  {
    const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
    lock_guard<mutex> lock(reqEntry->mutex);
    const bool reqToBeDeclined =
        (isRequestPreProcessingRightNow(reqEntry, reqSeqNum, clientId, batchCid, senderId) ||
         isRequestPassingConsensusOrPostExec(reqSeqNum, senderId, clientId, batchCid, clientMsg->getCid()) ||
         isRequestPreProcessedBefore(reqEntry, reqSeqNum, clientId, batchCid, clientMsg->getCid()));
    if (reqToBeDeclined) return false;
    if (myReplica_.isReplyAlreadySentToClient(clientId, reqSeqNum)) {
      LOG_INFO(logger(),
               "Request has already been executed - let replica decide how to proceed further"
                   << KVLOG(batchCid, batchSize, reqSeqNum, reqCid, clientId, senderId));
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
    handleClientPreProcessRequestByPrimary(preProcessRequestMsg, batchCid, arrivedInBatch);
    return true;
  }
  LOG_DEBUG(logger(),
            "ClientPreProcessRequestMsg"
                << KVLOG(batchCid, reqSeqNum, reqCid, clientId, senderId, arrivedInBatch, reqOffsetInBatch)
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
  LOG_DEBUG(logger(), "Send PreProcessBatchRequestMsg to non-primary replicas" << KVLOG(clientId, batchCid, batchSize));
  auto preProcessBatchReqMsg = make_shared<PreProcessBatchRequestMsg>(REQ_TYPE_PRE_PROCESS,
                                                                      clientId,
                                                                      myReplicaId_,
                                                                      preProcessReqMsgList,
                                                                      batchCid,
                                                                      requestsSize,
                                                                      myReplica_.getCurrentView());
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.sendPreProcessBatchRequestToAllReplicas);
  const set<ReplicaId> &idsOfPeerReplicas = myReplica_.getIdsOfPeerReplicas();
  for (auto destId : idsOfPeerReplicas) {
    if (destId != myReplicaId_)
      // sendMsg works asynchronously, so we can launch it sequentially here
      sendMsg(preProcessBatchReqMsg->body(), destId, REQ_TYPE_PRE_PROCESS, preProcessBatchReqMsg->size());
  }
}

template <>
void PreProcessor::onMessage<ClientBatchRequestMsg>(ClientBatchRequestMsgUniquePtr msg) {
  preProcessorMetrics_.preProcBatchReqReceived++;
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onClientBatchPreProcessRequestMsg);
  const auto clientId = msg->clientId();
  // senderId should be taken from ClientBatchRequestMsg as it does not get re-set in batched client messages
  const auto senderId = msg->senderId();
  const auto &batchCid = msg->getCid();
  const auto batchSize = msg->numOfMessagesInBatch();
  LOG_DEBUG(logger(), "Received ClientBatchRequestMsg" << KVLOG(batchCid, senderId, clientId, batchSize));
  if (!checkClientBatchMsgCorrectness(msg)) {
    preProcessorMetrics_.preProcReqIgnored++;
    return;
  }
  ClientMsgsList &clientMsgs = msg->getClientPreProcessRequestMsgs();
  const auto &batchEntry = ongoingReqBatches_[clientId];
  string ongoingBatchCid;
  if (batchEntry->isBatchRegistered(ongoingBatchCid)) {
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
  uint32_t numOfReqsToSkip = 0;
  for (auto &clientMsg : clientMsgs) {
    preProcessorMetrics_.preProcReqReceived++;
    PreProcessRequestMsgSharedPtr preProcessRequestMsg;
    const auto reqSeqNum = clientMsg->requestSeqNum();
    const auto reqCid = clientMsg->getCid();
    const bool reqToBeProcessed = handleSingleClientRequestMessage(
        move(clientMsg), senderId, true, offset++, preProcessRequestMsg, batchCid, batchSize);
    if (myReplica_.isCurrentPrimary()) {
      if (preProcessRequestMsg) {
        overallPreProcessReqMsgsSize += preProcessRequestMsg->size();
        preProcessReqMsgList.push_back(preProcessRequestMsg);
      }
      if (!reqToBeProcessed) {
        LOG_INFO(logger(), "Request processing will be skipped" << KVLOG(clientId, batchCid, reqSeqNum, reqCid));
        numOfReqsToSkip++;
      }
    }
  }
  if (numOfReqsToSkip == batchSize) {
    LOG_WARN(logger(),
             "None of the batch requests could be processed: skip the whole batch"
                 << KVLOG(myReplica_.currentPrimary(), senderId, clientId, batchCid));
    return;
  }
  if (myReplica_.isCurrentPrimary()) {
    if (numOfReqsToSkip) {
      LOG_INFO(logger(),
               "Some of the batch requests will be skipped"
                   << KVLOG(senderId, clientId, batchCid, batchSize, numOfReqsToSkip));
      batchEntry->increaseNumOfCompletedReqs(numOfReqsToSkip);
    }
    if (!preProcessReqMsgList.empty())
      // For the non-batched inter-replicas communication PreProcessReq messages get sent to all non-primary replicas
      // in handleClientPreProcessRequestByPrimary function one-by-one
      sendPreProcessBatchReqToAllReplicas(std::move(msg), preProcessReqMsgList, overallPreProcessReqMsgsSize);
  } else {
    LOG_DEBUG(logger(), "Pass ClientBatchRequestMsg to the current primary" << KVLOG(senderId, clientId, batchCid));
    sendMsg(msg->body(), myReplica_.currentPrimary(), msg->type(), msg->size());
  }
}  // namespace preprocessor

bool PreProcessor::checkPreProcessReqPrerequisites(SeqNum reqSeqNum,
                                                   const string &reqCid,
                                                   NodeIdType senderId,
                                                   NodeIdType clientId,
                                                   const string &batchCid,
                                                   uint16_t reqOffsetInBatch) {
  if (myReplica_.isCollectingState()) {
    // Do not put log as INFO level: logs slow down State Transfer and are printed thosands of times.
    // There are other logs which shows replica is in ST, and everybody should be aware that most messages are
    // blocks during ST.
    LOG_DEBUG(logger(),
              "Ignore PreProcessRequestMsg as the replica is collecting missing state from other replicas"
                  << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, reqOffsetInBatch));
    return false;
  }

  if (myReplica_.isCurrentPrimary()) {
    LOG_WARN(logger(),
             "Ignore PreProcessRequestMsg as current replica is the primary"
                 << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, reqOffsetInBatch));
    return false;
  }

  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(),
             "Ignore PreProcessRequestMsg as current view is inactive"
                 << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, reqOffsetInBatch));
    return false;
  }
  return true;
}

bool PreProcessor::checkPreProcessBatchReqMsgCorrectness(const PreProcessBatchReqMsgUniquePtr &batchReq) {
  const auto &preProcessRequestMsgs = batchReq->getPreProcessRequestMsgs();
  bool valid = true;

  if (batchReq->viewNum() != myReplica_.getCurrentView()) {
    LOG_INFO(logger(),
             "Ignore PreProcessBatchRequestMsg as the view is invalid"
                 << KVLOG(batchReq->senderId(), batchReq->clientId(), batchReq->getCid(), batchReq->viewNum()));
    return false;
  }

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
void PreProcessor::onMessage<PreProcessBatchRequestMsg>(PreProcessBatchReqMsgUniquePtr msg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onPreProcessBatchRequestMsg);
  const auto &batchCid = msg->getCid();
  const NodeIdType &senderId = msg->senderId();
  const NodeIdType &clientId = msg->clientId();
  const string reqType = msg->reqType() == REQ_TYPE_PRE_PROCESS ? "REQ_TYPE_PRE_PROCESS" : "REQ_TYPE_CANCEL";
  LOG_DEBUG(logger(),
            "Received PreProcessBatchRequestMsg"
                << KVLOG(reqType, senderId, clientId, batchCid, msg->numOfMessagesInBatch()));
  if (!checkPreProcessBatchReqMsgCorrectness(msg)) return;

  PreProcessReqMsgsList &preProcessReqMsgs = msg->getPreProcessRequestMsgs();
  const auto batchSize = preProcessReqMsgs.size();

  const auto &batchEntry = ongoingReqBatches_[clientId];
  if (msg->reqType() == REQ_TYPE_CANCEL && !batchEntry->isBatchInProcess()) {
    // Don't cancel the batch if it has received PreProcessBatchRequestMsg before
    batchEntry->cancelBatchAndReleaseRequests(batchCid, CANCELLED_BY_PRIMARY);
    return;
  }
  if (!batchEntry->isBatchInProcess()) {
    uint32_t numOfReqsToSkip = 0;
    for (auto &singleMsg : preProcessReqMsgs) {
      const auto reqSeqNum = singleMsg->reqSeqNum();
      const auto reqCid = singleMsg->getCid();
      LOG_INFO(logger(),
               "Start PreProcessRequestMsg processing by a non-primary replica"
                   << KVLOG(senderId, clientId, batchCid, reqSeqNum, reqCid, batchSize));
      const auto reqToBeProcessed = handleSinglePreProcessRequestMsg(singleMsg, batchCid, batchSize);
      if (!reqToBeProcessed) {
        LOG_INFO(logger(), "Request processing will be skipped" << KVLOG(clientId, batchCid, reqSeqNum, reqCid));
        numOfReqsToSkip++;
      }
    }
    if (numOfReqsToSkip == batchSize) {
      LOG_WARN(
          logger(),
          "None of the batch requests could be processed: skip the whole batch" << KVLOG(senderId, clientId, batchCid));
      return;
    }
    if (numOfReqsToSkip) {
      LOG_INFO(logger(),
               "Some of the batch requests will be skipped"
                   << KVLOG(senderId, clientId, batchCid, batchSize, numOfReqsToSkip));
      ongoingReqBatches_[clientId]->increaseNumOfCompletedReqs(numOfReqsToSkip);
    }
  } else
    LOG_INFO(logger(), "The batch is in process; ignore the message" << KVLOG(batchCid, senderId, clientId, batchSize));
}

bool PreProcessor::handleSinglePreProcessRequestMsg(PreProcessRequestMsgSharedPtr preProcessReqMsg,
                                                    const string &batchCid,
                                                    uint32_t batchSize) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onPreProcessRequestMsg);
  const NodeIdType &senderId = preProcessReqMsg->senderId();
  const NodeIdType &clientId = preProcessReqMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  const RequestType reqType = preProcessReqMsg->reqType();
  const string &reqCid = preProcessReqMsg->getCid();
  LOG_DEBUG(logger(), KVLOG(batchCid, reqSeqNum, reqCid, clientId, senderId, reqOffsetInBatch));
  if (!checkPreProcessReqPrerequisites(reqSeqNum, reqCid, senderId, clientId, batchCid, reqOffsetInBatch)) return false;
  bool registerSucceeded = false;
  {
    const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
    lock_guard<mutex> lock(reqEntry->mutex);
    if (reqEntry->reqProcessingStatePtr) {
      // The primary replica requested to cancel the request => release it, but don't cancel pre-processing when
      // reqEntry->reqProcessingStatePtr->getPreProcessRequest() != null_ptr
      if (reqType == REQ_TYPE_CANCEL && !reqEntry->reqProcessingStatePtr->getPreProcessRequest()) {
        releaseClientPreProcessRequest(reqEntry, CANCELLED_BY_PRIMARY);
        return false;
      }
      if (reqEntry->reqProcessingStatePtr->getPreProcessRequest()) {
        auto const &ongoingReqSeqNum = reqEntry->reqProcessingStatePtr->getPreProcessRequest()->reqSeqNum();
        auto const &ongoingCid = reqEntry->reqProcessingStatePtr->getPreProcessRequest()->getCid();
        // The replica is processing some request and cannot handle PreProcessRequest now - send reject to the primary
        sendRejectPreProcessReplyMsg(clientId,
                                     reqOffsetInBatch,
                                     senderId,
                                     reqSeqNum,
                                     ongoingReqSeqNum,
                                     preProcessReqMsg->reqRetryId(),
                                     preProcessReqMsg->getCid(),
                                     ongoingCid);
        return false;
      }
    } else {
      if (reqType == REQ_TYPE_CANCEL) return false;  // No registered client request found; do nothing
    }
    registerSucceeded =
        registerRequest(batchCid, batchSize, ClientPreProcessReqMsgUniquePtr(), preProcessReqMsg, reqOffsetInBatch);
  }
  if (registerSucceeded) {
    LOG_INFO(logger(),
             "Start PreProcessRequestMsg processing by a non-primary replica"
                 << KVLOG(batchCid, reqSeqNum, reqCid, batchSize, reqCid, clientId, reqOffsetInBatch, senderId));
    preProcessorMetrics_.preProcInFlyRequestsNum++;  // Increase the metric on non-primary replica
    auto totalPreExecDurationRecorder = TimeRecorder(*totalPreExecDurationRecorder_.get());
    // Pre-process the request, calculate a hash of the result and send a reply message back
    launchAsyncReqPreProcessingJob(preProcessReqMsg, batchCid, false, std::move(totalPreExecDurationRecorder));
    return true;
  }
  return false;
}

bool PreProcessor::checkPreProcessRequestMsgCorrectness(const PreProcessRequestMsgSharedPtr &requestMsg) {
  if (requestMsg->viewNum() != myReplica_.getCurrentView()) {
    LOG_INFO(logger(),
             "Ignore PreProcessRequestMsg as the view is invalid" << KVLOG(requestMsg->getCid(),
                                                                           requestMsg->reqSeqNum(),
                                                                           requestMsg->senderId(),
                                                                           requestMsg->clientId(),
                                                                           requestMsg->reqOffsetInBatch(),
                                                                           requestMsg->viewNum()));
    return false;
  }
  return true;
}

// Non-primary replica request handling
template <>
void PreProcessor::onMessage<PreProcessRequestMsg>(PreProcessRequestMsgUniquePtr msg) {
  const string reqType = msg->reqType() == REQ_TYPE_PRE_PROCESS ? "REQ_TYPE_PRE_PROCESS" : "REQ_TYPE_CANCEL";
  LOG_DEBUG(logger(),
            "Received PreProcessRequestMsg" << KVLOG(
                reqType, msg->reqSeqNum(), msg->getCid(), msg->senderId(), msg->clientId(), msg->reqOffsetInBatch()));
  PreProcessRequestMsgSharedPtr message = move(msg);
  if (checkPreProcessRequestMsgCorrectness(message)) {
    handleSinglePreProcessRequestMsg(message, "", 1);
  }
}

void PreProcessor::handleSinglePreProcessReplyMsg(PreProcessReplyMsgSharedPtr preProcessReplyMsg,
                                                  const string &batchCid) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onPreProcessReplyMsg);
  const NodeIdType &senderId = preProcessReplyMsg->senderId();
  const NodeIdType &clientId = preProcessReplyMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReplyMsg->reqOffsetInBatch();
  const SeqNum &reqSeqNum = preProcessReplyMsg->reqSeqNum();
  string reqCid = preProcessReplyMsg->getCid();
  const auto &status = preProcessReplyMsg->status();
  PreProcessingResult consensusResult = CANCEL;
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
                KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, reqOffsetInBatch)
                    << " will be ignored as no such ongoing request exists or different one found for this client");
      return;
    }
    reqEntry->reqProcessingStatePtr->handlePreProcessReplyMsg(preProcessReplyMsg);
    if (status == STATUS_REJECT) {
      LOG_DEBUG(logger(),
                "Received PreProcessReplyMsg with STATUS_REJECT"
                    << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, reqOffsetInBatch));
      return;
    }
    consensusResult = reqEntry->reqProcessingStatePtr->definePreProcessingConsensusResult();
    LOG_DEBUG(logger(),
              "Handle single pre-process reply message"
                  << KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, consensusResult));
    if (consensusResult == CONTINUE) resendPreProcessRequest(reqEntry->reqProcessingStatePtr);
    holdCompleteOrCancelRequest(reqCid, consensusResult, clientId, reqOffsetInBatch, reqSeqNum, batchCid);
  }
}

bool PreProcessor::checkPreProcessReplyMsgCorrectness(const PreProcessReplyMsgSharedPtr &replyMsg) {
  if (replyMsg->viewNum() != myReplica_.getCurrentView()) {
    LOG_INFO(logger(),
             "Ignore PreProcessReplyMsg as the view is invalid" << KVLOG(replyMsg->getCid(),
                                                                         replyMsg->reqSeqNum(),
                                                                         replyMsg->senderId(),
                                                                         replyMsg->clientId(),
                                                                         replyMsg->reqOffsetInBatch(),
                                                                         replyMsg->viewNum()));
    return false;
  }
  return true;
}

// Primary replica handling
template <>
void PreProcessor::onMessage<PreProcessReplyMsg>(PreProcessReplyMsgUniquePtr msg) {
  string replyStatus = "STATUS_GOOD";
  if (msg->status() == STATUS_REJECT) replyStatus = "STATUS_REJECT";
  LOG_DEBUG(
      logger(),
      "Received PreProcessReplyMsg" << KVLOG(
          msg->reqSeqNum(), msg->getCid(), msg->senderId(), msg->clientId(), msg->reqOffsetInBatch(), replyStatus));
  PreProcessReplyMsgSharedPtr message = move(msg);
  if (checkPreProcessReplyMsgCorrectness(message)) {
    handleSinglePreProcessReplyMsg(message, "");
  }
}

bool PreProcessor::checkPreProcessReplyPrerequisites(
    SeqNum reqSeqNum, const string &reqCid, NodeIdType senderId, const string &batchCid, uint16_t offsetInBatch) {
  if (myReplica_.isCollectingState()) {
    // Do not put log as INFO level: logs slow down State Transfer and are printed thosands of times.
    // There are other logs which shows replica is in ST, and everybody should be aware that most messages are
    // blocks during ST.
    LOG_DEBUG(logger(),
              "Ignore PreProcessReplyMsg as the replica is collecting missing state from other replicas"
                  << KVLOG(batchCid, reqSeqNum, reqCid, senderId, offsetInBatch));
    return false;
  }

  if (!myReplica_.isCurrentPrimary()) {
    LOG_WARN(logger(),
             "Ignore PreProcessReplyMsg as current replica is not the primary"
                 << KVLOG(batchCid, reqSeqNum, reqCid, senderId, offsetInBatch));
    return false;
  }

  if (!myReplica_.currentViewIsActive()) {
    LOG_INFO(logger(),
             "Ignore PreProcessReplyMsg as current view is inactive"
                 << KVLOG(batchCid, reqSeqNum, reqCid, senderId, offsetInBatch));
    return false;
  }
  return true;
}

bool PreProcessor::checkPreProcessBatchReplyMsgCorrectness(const PreProcessBatchReplyMsgUniquePtr &batchReply) {
  const auto &preProcessReplyMsgs = batchReply->getPreProcessReplyMsgs();
  NodeIdType senderId = batchReply->senderId();
  const string batchCid = batchReply->getCid();
  bool valid = true;

  if (batchReply->viewNum() != myReplica_.getCurrentView()) {
    LOG_INFO(
        logger(),
        "Ignore PreProcessBatchReplyMsg as the view is invalid" << KVLOG(batchCid, senderId, batchReply->viewNum()));
    return false;
  }

  for (const auto &replyMsg : preProcessReplyMsgs) {
    if (!checkPreProcessReplyPrerequisites(
            senderId, batchCid, replyMsg->reqSeqNum(), replyMsg->getCid(), replyMsg->reqOffsetInBatch()) ||
        !validateMessage(replyMsg.get())) {
      valid = false;
      break;
    }
  }
  return valid;
}

// Primary replica handling
template <>
void PreProcessor::onMessage<PreProcessBatchReplyMsg>(PreProcessBatchReplyMsgUniquePtr msg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.onPreProcessBatchReplyMsg);
  const auto &batchCid = msg->getCid();
  const NodeIdType &senderId = msg->senderId();
  const NodeIdType &clientId = msg->clientId();
  PreProcessReplyMsgsList &preProcessReplyMsgs = msg->getPreProcessReplyMsgs();
  const auto batchSize = preProcessReplyMsgs.size();
  LOG_DEBUG(logger(), "Received PreProcessBatchReplyMsg" << KVLOG(batchCid, senderId, clientId, batchSize));
  const auto &batchEntry = ongoingReqBatches_[clientId];
  string ongoingBatchCid;
  const bool batchIsOngoing = batchEntry->isBatchInProcess(ongoingBatchCid);
  if (!batchIsOngoing || ongoingBatchCid != batchCid) {
    LOG_DEBUG(logger(),
              "PreProcessBatchReplyMsg is ignored: no such batch exists or different one is ongoing for this client"
                  << KVLOG(senderId, clientId, batchCid, batchIsOngoing, ongoingBatchCid));
    return;
  }

  if (!checkPreProcessBatchReplyMsgCorrectness(msg)) return;

  for (auto &singleReplyMsg : preProcessReplyMsgs) {
    const auto &reqSeqNum = singleReplyMsg->reqSeqNum();
    const auto &reqCid = singleReplyMsg->getCid();
    LOG_DEBUG(logger(),
              "Start handling single message from the replies batch:" << KVLOG(
                  batchCid, reqSeqNum, reqCid, senderId, clientId));
    handleSinglePreProcessReplyMsg(singleReplyMsg, batchCid);
  }
}

template <typename T>
void PreProcessor::messageHandler(unique_ptr<MessageBase> msg) {
  if (!msgs_.write_available()) {
    LOG_WARN(logger(), "PreProcessor queue is full, returning message");
    incomingMsgsStorage_->pushExternalMsg(std::move(msg));
    return;
  }
  msgs_.push(msg.release());
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
      MessageBase *msg = msgs_.front();
      msgs_.pop();
      if (bftEngine::ControlStateManager::instance().isWedged()) {
        LOG_INFO(logger(), "The replica is wedged, the request is ignored");
        delete msg;
        continue;
      }
      switch (msg->type()) {
        case (MsgCode::ClientBatchRequest): {
          auto msgPtr = make_unique<ClientBatchRequestMsg>(msg);
          if (validateMessage(msgPtr.get())) {
            onMessage<ClientBatchRequestMsg>(std::move(msgPtr));
          } else {
            preProcessorMetrics_.preProcReqInvalid++;
          }
          break;
        }
        case (MsgCode::ClientPreProcessRequest): {
          auto msgPtr = make_unique<ClientPreProcessRequestMsg>(msg);
          if (validateMessage(msgPtr.get())) {
            onMessage<ClientPreProcessRequestMsg>(std::move(msgPtr));
          } else {
            preProcessorMetrics_.preProcReqInvalid++;
          }
          break;
        }
        case (MsgCode::PreProcessRequest): {
          auto msgPtr = make_unique<PreProcessRequestMsg>(msg);
          if (validateMessage(msgPtr.get())) {
            onMessage<PreProcessRequestMsg>(std::move(msgPtr));
          } else {
            preProcessorMetrics_.preProcReqInvalid++;
          }
          break;
        }
        case (MsgCode::PreProcessBatchRequest): {
          auto msgPtr = make_unique<PreProcessBatchRequestMsg>(msg);
          if (validateMessage(msgPtr.get())) {
            onMessage<PreProcessBatchRequestMsg>(std::move(msgPtr));
          } else {
            preProcessorMetrics_.preProcReqInvalid++;
          }
          break;
        }
        case (MsgCode::PreProcessReply): {
          auto msgPtr = make_unique<PreProcessReplyMsg>(msg);
          if (validateMessage(msgPtr.get())) {
            onMessage<PreProcessReplyMsg>(std::move(msgPtr));
          } else {
            preProcessorMetrics_.preProcReqInvalid++;
          }
          break;
        }
        case (MsgCode::PreProcessBatchReply): {
          auto msgPtr = make_unique<PreProcessBatchReplyMsg>(msg);
          if (validateMessage(msgPtr.get())) {
            onMessage<PreProcessBatchReplyMsg>(std::move(msgPtr));
          } else {
            preProcessorMetrics_.preProcReqInvalid++;
          }
          break;
        }
        default:
          LOG_ERROR(logger(), "Unknown message" << KVLOG(msg->type()));
      }
      delete msg;
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

// Should be called under an entry lock
void PreProcessor::holdCompleteOrCancelRequest(const string &reqCid,
                                               PreProcessingResult result,
                                               NodeIdType clientId,
                                               uint16_t reqOffsetInBatch,
                                               SeqNum reqSeqNum,
                                               const string &batchCid) {
  LOG_DEBUG(logger(),
            "Decide how to continue request processing"
                << KVLOG(result, batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch));
  switch (result) {
    case NONE:      // No action required - pre-processing has been already completed
    case CONTINUE:  // Not enough equal hashes collected
    case EXPIRED:
    case FAILED:
      break;
    case COMPLETE:  // Pre-processing consensus reached
      finalizePreProcessing(clientId, reqOffsetInBatch, batchCid);
      break;
    case CANCEL:  // Pre-processing consensus not reached
      cancelPreProcessing(clientId, batchCid, reqOffsetInBatch);
      break;
    case CANCELLED_BY_PRIMARY:
      LOG_WARN(
          logger(),
          "Received reply message with status CANCELLED_BY_PRIMARY" << KVLOG(batchCid, reqSeqNum, reqCid, clientId));
      break;
  }
}

// Should be called under an entry lock
void PreProcessor::cancelPreProcessing(NodeIdType clientId, const string &batchCid, uint16_t reqOffsetInBatch) {
  const auto &batchEntry = ongoingReqBatches_[clientId];
  const auto &reqEntry = batchEntry->getRequestState(reqOffsetInBatch);
  if (!batchCid.empty()) {
    batchEntry->cancelRequestAndBatchIfCompleted(batchCid, reqOffsetInBatch, CANCEL);
    return;
  }
  preProcessorMetrics_.preProcConsensusNotReached++;
  SeqNum reqSeqNum = 0;
  if (reqEntry->reqProcessingStatePtr) {
    reqSeqNum = reqEntry->reqProcessingStatePtr->getReqSeqNum();
    const auto &reqCid = reqEntry->reqProcessingStatePtr->getReqCid();
    LOG_WARN(logger(),
             "Pre-processing consensus not reached; cancel request"
                 << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch));
    releaseClientPreProcessRequest(reqEntry, CANCEL);
  }
}

// Should be called under an entry lock
void PreProcessor::finalizePreProcessing(NodeIdType clientId, uint16_t reqOffsetInBatch, const string &batchCid) {
  std::unique_ptr<impl::MessageBase> preProcessMsg;
  const auto &batchEntry = ongoingReqBatches_[clientId];
  const auto &reqEntry = batchEntry->getRequestState(reqOffsetInBatch);
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.finalizePreProcessing);
  auto &reqProcessingStatePtr = reqEntry->reqProcessingStatePtr;
  if (reqProcessingStatePtr) {
    const auto reqCid = reqProcessingStatePtr->getReqCid();
    const auto reqSeqNum = reqProcessingStatePtr->getReqSeqNum();
    auto &preProcessReqMsg = reqProcessingStatePtr->getPreProcessRequest();
    const auto &span_context = preProcessReqMsg->spanContext<PreProcessRequestMsgSharedPtr::element_type>();
    // Copy of the message body is unavoidable here, as we need to create a new message type which lifetime is
    // controlled by the replica while all PreProcessReply messages get released here.
    auto preProcessResult = static_cast<uint32_t>(reqProcessingStatePtr->getAgreedPreProcessResult());

    if (preProcessResult == static_cast<uint32_t>(OperationResult::SUCCESS)) {
      preProcessResult = static_cast<uint32_t>(OperationResult::UNKNOWN);
    }
    if (ReplicaConfig::instance().preExecutionResultAuthEnabled) {
      const auto &sigsSet = reqProcessingStatePtr->getPreProcessResultSignatures();
      auto sigsBuf = PreProcessResultSignature::serializeResultSignatures(sigsSet, numOfRequiredReplies());
      preProcessMsg = make_unique<PreProcessResultMsg>(clientId,
                                                       preProcessResult,
                                                       reqSeqNum,
                                                       reqProcessingStatePtr->getPrimaryPreProcessedResultLen(),
                                                       reqProcessingStatePtr->getPrimaryPreProcessedResultData(),
                                                       reqProcessingStatePtr->getReqTimeoutMilli(),
                                                       reqCid,
                                                       span_context,
                                                       reqProcessingStatePtr->getReqSignature(),
                                                       reqProcessingStatePtr->getReqSignatureLength(),
                                                       sigsBuf,
                                                       reqOffsetInBatch);
      LOG_DEBUG(logger(),
                "Pass PreProcessResultMsg to ReplicaImp for consensus"
                    << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch, preProcessResult));
    } else {
      preProcessMsg = make_unique<ClientRequestMsg>(clientId,
                                                    HAS_PRE_PROCESSED_FLAG,
                                                    reqSeqNum,
                                                    reqProcessingStatePtr->getPrimaryPreProcessedResultLen(),
                                                    reqProcessingStatePtr->getPrimaryPreProcessedResultData(),
                                                    reqProcessingStatePtr->getReqTimeoutMilli(),
                                                    reqCid,
                                                    preProcessResult,
                                                    span_context,
                                                    reqProcessingStatePtr->getReqSignature(),
                                                    reqProcessingStatePtr->getReqSignatureLength(),
                                                    0,
                                                    reqOffsetInBatch);
      LOG_DEBUG(logger(),
                "Pass pre-processed ClientRequestMsg to ReplicaImp for consensus"
                    << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch, preProcessResult));
    }

    preProcessorMetrics_.preProcReqCompleted++;
    incomingMsgsStorage_->pushExternalMsg(std::move(preProcessMsg));

    releaseClientPreProcessRequest(reqEntry, COMPLETE);
    if (myReplica_.isCurrentPrimary()) {
      metric_pre_exe_duration_.finishMeasurement(reqCid);
    }
    LOG_INFO(logger(),
             "Pre-processing completed for" << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch));
    batchEntry->finalizeBatchIfCompletedSafe(batchCid);
  }
}

uint16_t PreProcessor::numOfRequiredReplies() { return myReplica_.getReplicaConfig().fVal + 1; }

// This function should be always called under a reqEntry->mutex lock
bool PreProcessor::registerRequest(const string &batchCid,
                                   uint32_t batchSize,
                                   ClientPreProcessReqMsgUniquePtr clientReqMsg,
                                   PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                                   uint16_t reqOffsetInBatch) {
  NodeIdType clientId = 0;
  NodeIdType senderId = 0;
  SeqNum reqSeqNum = 0;
  string reqCid;
  uint32_t requestSignatureLength = 0;
  char *requestSignature = nullptr;

  bool clientReqMsgSpecified = false;
  if (clientReqMsg) {
    clientId = clientReqMsg->clientProxyId();
    senderId = clientReqMsg->senderId();
    reqSeqNum = clientReqMsg->requestSeqNum();
    reqCid = clientReqMsg->getCid();
    clientReqMsgSpecified = true;
    requestSignatureLength = clientReqMsg->requestSignatureLength();
    requestSignature = clientReqMsg->requestSignature();
  } else {
    clientId = preProcessRequestMsg->clientId();
    senderId = preProcessRequestMsg->senderId();
    reqSeqNum = preProcessRequestMsg->reqSeqNum();
    reqCid = preProcessRequestMsg->getCid();
    requestSignatureLength = preProcessRequestMsg->requestSignatureLength();
    requestSignature = preProcessRequestMsg->requestSignature();
  }
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
  if (!batchCid.empty()) {
    if (preProcessRequestMsg)
      ongoingReqBatches_[clientId]->startBatch(senderId, batchCid, batchSize);
    else
      ongoingReqBatches_[clientId]->registerBatch(senderId, batchCid, batchSize);
  }
  if (!reqEntry->reqProcessingStatePtr) {
    reqEntry->reqProcessingStatePtr = make_unique<RequestProcessingState>(myReplicaId_,
                                                                          numOfReplicas_,
                                                                          batchCid,
                                                                          clientId,
                                                                          reqOffsetInBatch,
                                                                          reqCid,
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
             "Request" << KVLOG(batchCid, reqSeqNum, reqCid, clientId) << " could not be registered: the entry for"
                       << KVLOG(clientId, senderId, reqOffsetInBatch) << " is occupied by "
                       << KVLOG(reqState->getReqSeqNum(), reqState->getBatchCid()));
    return false;
  }
  if (clientReqMsgSpecified) {
    LOG_DEBUG(logger(),
              KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, reqOffsetInBatch)
                  << " registered ClientPreProcessReqMsg");
  } else {
    LOG_DEBUG(logger(),
              KVLOG(batchCid, reqSeqNum, reqCid, senderId, clientId, reqOffsetInBatch)
                  << " registered PreProcessRequestMsg");
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
    const auto clientId = givenReq->getClientId();
    const auto reqOffsetInBatch = givenReq->getReqOffsetInBatch();
    const auto batchCid = givenReq->getBatchCid();
    auto reqSeqNum = givenReq->getReqSeqNum();
    auto reqCid = givenReq->getReqCid();
    if (result == COMPLETE) {
      if (reqEntry->reqProcessingHistory.size() >= reqEntry->reqProcessingHistoryHeight) {
        auto &removeFromHistoryReq = reqEntry->reqProcessingHistory.front();
        auto remReqSeqNum = removeFromHistoryReq->getReqSeqNum();
        auto &remReqCid = removeFromHistoryReq->getReqCid();
        LOG_DEBUG(logger(), "Request will be removed from the history" << KVLOG(clientId, remReqSeqNum, remReqCid));
        removeFromHistoryReq.reset();
        reqEntry->reqProcessingHistory.pop_front();
      }
      LOG_INFO(logger(),
               "Request has been released and moved to the history"
                   << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch));
      // No need to keep whole messages in the memory => release them before archiving
      givenReq->releaseResources();
      reqEntry->reqProcessingHistory.push_back(std::move(givenReq));
    } else {
      switch (result) {
        case CANCEL:
          LOG_INFO(logger(),
                   "Release request - no consensus reached"
                       << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch));
          break;
        case EXPIRED:
          LOG_INFO(logger(),
                   "Release request - expired" << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch));
          break;
        case CANCELLED_BY_PRIMARY:
          LOG_DEBUG(logger(),
                    "Release request - processing has been cancelled by the primary replica"
                        << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch));
          break;
        default:;
      }
    }
    reqEntry->reqProcessingStatePtr.reset();
    if (!batchCid.empty()) ongoingReqBatches_[clientId]->increaseNumOfCompletedReqs(1);
    // Prevent preProcInFlyRequestsNum metric to be overflowed during a view change
    if (!myReplica_.isCurrentPrimary() && (preProcessorMetrics_.preProcInFlyRequestsNum.Get().Get() > 0)) {
      preProcessorMetrics_.preProcInFlyRequestsNum--;
    }
    if (memoryPoolEnabled_) releasePreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch);
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
  // save in local var because we might use it (for metrics) after clientReqMsg obj is moved
  const std::string reqCid = clientReqMsg->getCid();
  if (myReplica_.isCurrentPrimary()) {
    metric_pre_exe_duration_.addStartTimeStamp(reqCid);
  }

  (reqEntry->reqRetryId)++;
  countRetriedRequests(clientReqMsg, reqEntry);
  const auto reqSeqNum = clientReqMsg->requestSeqNum();
  const auto clientId = clientReqMsg->clientProxyId();
  const auto senderId = clientReqMsg->senderId();
  const auto requestTimeoutMilli = clientReqMsg->requestTimeoutMilli();
  uint64_t blockId = 0;
  if (!batchCid.empty()) {
    ongoingReqBatches_[clientId]->registerBatch(senderId, batchCid, batchSize);
    blockId = ongoingReqBatches_[clientId]->getBlockId();
  }
  preProcessRequestMsg =
      make_shared<PreProcessRequestMsg>(REQ_TYPE_PRE_PROCESS,
                                        myReplicaId_,
                                        clientId,
                                        reqOffsetInBatch,
                                        reqSeqNum,
                                        reqEntry->reqRetryId,
                                        clientReqMsg->requestLength(),
                                        clientReqMsg->requestBuf(),
                                        reqCid,
                                        clientReqMsg->requestSignature(),
                                        clientReqMsg->requestSignatureLength(),
                                        blockId,
                                        myReplica_.getCurrentView(),
                                        clientReqMsg->spanContext<ClientPreProcessReqMsgUniquePtr::element_type>(),
                                        clientReqMsg->result());
  const auto registerSucceeded =
      registerRequest(batchCid, batchSize, move(clientReqMsg), preProcessRequestMsg, reqOffsetInBatch);
  if (registerSucceeded) {
    LOG_INFO(logger(),
             "Start request processing by a primary replica"
                 << KVLOG(batchCid, reqSeqNum, reqCid, batchSize, clientId, senderId, requestTimeoutMilli, blockId));
  } else {
    // delete this cid from time stamp map cause registration failed
    metric_pre_exe_duration_.deleteSingleEntry(reqCid);
  }
  return registerSucceeded;
}

// Primary replica: start client request handling
void PreProcessor::handleClientPreProcessRequestByPrimary(PreProcessRequestMsgSharedPtr preProcessRequestMsg,
                                                          const string &batchCid,
                                                          bool arrivedInBatch) {
  auto time_recorder = TimeRecorder(*totalPreExecDurationRecorder_.get());
  // For requests arrived in a batch PreProcessBatchRequestMsg will be sent to non-primaries
  if (!arrivedInBatch) sendPreProcessRequestToAllReplicas(preProcessRequestMsg);
  // Pre-process the request and calculate a hash of the result
  launchAsyncReqPreProcessingJob(preProcessRequestMsg, batchCid, true, std::move(time_recorder));
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
  const auto reqCid = clientReqMsg->getCid();
  const auto sigLen = clientReqMsg->requestSignatureLength();

  // Register a client request message with an empty PreProcessRequestMsg to allow follow up.
  if (registerRequest(batchCid, batchSize, move(clientReqMsg), PreProcessRequestMsgSharedPtr(), reqOffsetInBatch)) {
    LOG_INFO(logger(),
             "ClientPreProcessReq has been registered on a non-primary replica" << KVLOG(
                 batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch, senderId, reqTimeoutMilli, msgSize, sigLen));

    if (arrivedInBatch) return;  // Need to pass the whole batch to the primary
    sendMsg(msgBody, myReplica_.currentPrimary(), msgType, msgSize);
    LOG_DEBUG(logger(),
              "Sent ClientPreProcessRequestMsg" << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch)
                                                << " to the current primary");
  }
}

uint32_t PreProcessor::getBufferOffset(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch) const {
  const auto clientIndex = clientId - numOfReplicas_ - numOfClientProxies_;
  ConcordAssertGE(clientIndex, 0);
  const auto reqOffset = clientIndex * clientMaxBatchSize_ + reqOffsetInBatch;
  LOG_DEBUG(logger(), KVLOG(clientId, reqSeqNum, clientIndex, reqOffsetInBatch));
  return reqOffset;
}

const char *PreProcessor::getPreProcessResultBuffer(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch) {
  // Buffers structure scheme:
  // |first client's first buffer|...|first client's last buffer|......
  // |last client's first buffer|...|last client's last buffer|
  // First client id starts after the last replica id.
  // First buffer offset = numOfReplicas_ * batchSize_
  // The number of buffers per client comes from the configuration parameter clientBatchingMaxMsgsNbr.
  const auto bufferOffset = getBufferOffset(clientId, reqSeqNum, reqOffsetInBatch);
  std::unique_lock lock(preProcessResultBuffers_[bufferOffset]->mutex);
  if (!preProcessResultBuffers_[bufferOffset]->buffer) {
    if (memoryPoolEnabled_) {
      preProcessResultBuffers_[bufferOffset]->buffer = memoryPool_.getChunk();
      LOG_TRACE(logger(),
                "Allocate memory from the pool" << KVLOG(clientId, reqSeqNum, reqOffsetInBatch, bufferOffset));
    } else {
      preProcessResultBuffers_[bufferOffset]->buffer = new char[maxExternalMsgSize_];
      LOG_INFO(logger(), "Allocate raw memory" << KVLOG(clientId, reqSeqNum, reqOffsetInBatch, bufferOffset));
    }
  }
  return preProcessResultBuffers_[bufferOffset]->buffer;
}

void PreProcessor::releasePreProcessResultBuffer(uint16_t clientId, ReqId reqSeqNum, uint16_t reqOffsetInBatch) {
  const auto bufferOffset = getBufferOffset(clientId, reqSeqNum, reqOffsetInBatch);
  std::unique_lock lock(preProcessResultBuffers_[bufferOffset]->mutex);
  if (preProcessResultBuffers_[bufferOffset]->buffer) {
    memoryPool_.returnChunk(preProcessResultBuffers_[bufferOffset]->buffer);
    preProcessResultBuffers_[bufferOffset]->buffer = nullptr;
    LOG_TRACE(logger(), "Returned memory to the pool" << KVLOG(clientId, reqSeqNum, reqOffsetInBatch, bufferOffset));
  }
}

// Primary replica: ask all replicas to pre-process the request
void PreProcessor::sendPreProcessRequestToAllReplicas(const PreProcessRequestMsgSharedPtr &preProcessReqMsg) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.sendPreProcessRequestToAllReplicas);
  const set<ReplicaId> &idsOfPeerReplicas = myReplica_.getIdsOfPeerReplicas();
  for (auto destId : idsOfPeerReplicas) {
    if (destId != myReplicaId_) {
      // sendMsg works asynchronously, so we can launch it sequentially here
      LOG_DEBUG(logger(),
                "Sending PreProcessRequestMsg to a non-primary replica" << KVLOG(
                    preProcessReqMsg->clientId(), preProcessReqMsg->reqSeqNum(), preProcessReqMsg->getCid(), destId));
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
                                                  const string &batchCid,
                                                  bool isPrimary,
                                                  TimeRecorder &&totalPreExecDurationRecorder) {
  auto launchAsyncPreProcessJobRecorder = TimeRecorder(*launchAsyncPreProcessJobRecorder_.get());
  setPreprocessingRightNow(preProcessReqMsg->clientId(), preProcessReqMsg->reqOffsetInBatch(), true);
  auto *preProcessJob = new AsyncPreProcessJob(*this,
                                               preProcessReqMsg,
                                               batchCid,
                                               isPrimary,
                                               std::move(totalPreExecDurationRecorder),
                                               std::move(launchAsyncPreProcessJobRecorder));
  threadPool_.add(preProcessJob);
}

OperationResult PreProcessor::launchReqPreProcessing(const string &batchCid,
                                                     const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                     uint32_t &resultLen) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.launchReqPreProcessing);
  const string &reqCid = preProcessReqMsg->getCid();
  uint16_t clientId = preProcessReqMsg->clientId();
  uint16_t reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  ReqId reqSeqNum = preProcessReqMsg->reqSeqNum();
  const auto &span_context = preProcessReqMsg->spanContext<PreProcessRequestMsgSharedPtr::element_type>();
  // Unused for now. Replica Specific Info not currently supported in pre-execution.
  auto span = concordUtils::startChildSpanFromContext(span_context, "bft_process_preprocess_msg");
  LOG_DEBUG(logger(),
            "Pass request for a pre-execution" << KVLOG(batchCid, reqSeqNum, reqCid, clientId, reqOffsetInBatch));
  uint64_t blockId = preProcessReqMsg->primaryBlockId();
  if (preProcessReqMsg->primaryBlockId() - GlobalData::block_delta > GlobalData::current_block_id) {
    blockId = 0;
    LOG_INFO(logger(),
             "Primary block [" << preProcessReqMsg->primaryBlockId()
                               << "] is too advanced for conflict detection optimization, replica block id ["
                               << GlobalData::current_block_id << "] delta [" << GlobalData::block_delta << "]");
    GlobalData::increment_step = true;
  }
  auto preProcessResultBuffer = (char *)getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch);
  IRequestsHandler::ExecutionRequest request = bftEngine::IRequestsHandler::ExecutionRequest{
      clientId,
      reqSeqNum,
      reqCid,
      PRE_PROCESS_FLAG,
      preProcessReqMsg->requestLength(),
      preProcessReqMsg->requestBuf(),
      std::string(preProcessReqMsg->requestSignature(), preProcessReqMsg->requestSignatureLength()),
      maxPreExecResultSize_,
      preProcessResultBuffer,
      reqSeqNum,
      reqOffsetInBatch,
      preProcessReqMsg->result()};
  requestsHandler_.preExecute(request, std::nullopt, reqCid, span);
  auto preProcessResult = static_cast<OperationResult>(request.outExecutionStatus);
  resultLen = request.outActualReplySize;
  if (preProcessResult != OperationResult::SUCCESS) {
    LOG_ERROR(logger(),
              "Pre-execution failed" << KVLOG(
                  clientId, reqSeqNum, batchCid, reqCid, reqOffsetInBatch, (uint32_t)preProcessResult, resultLen));
  }
  if (request.outActualReplySize == 0) {
    const string err{"Executed data is empty"};
    strcpy(preProcessResultBuffer, err.c_str());
    resultLen = err.size();
    preProcessResult = OperationResult::EXEC_DATA_EMPTY;
    LOG_ERROR(logger(),
              "Pre-execution failed" << KVLOG(
                  clientId, batchCid, reqSeqNum, reqCid, reqOffsetInBatch, reqSeqNum, (uint32_t)preProcessResult));
  }
  // Append the conflict detection block id and add its size to the resulting length.
  memcpy(preProcessResultBuffer + resultLen, reinterpret_cast<char *>(&blockId), sizeof(uint64_t));
  resultLen += sizeof(uint64_t);
  LOG_INFO(logger(),
           "Pre-execution operation has been successfully completed by Execution engine"
               << KVLOG(clientId, batchCid, reqSeqNum, reqCid, reqOffsetInBatch, blockId));
  return preProcessResult;
}

// For test purposes
ReqId PreProcessor::getOngoingReqIdForClient(uint16_t clientId, uint16_t reqOffsetInBatch) {
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
  lock_guard<mutex> lock(reqEntry->mutex);
  if (reqEntry->reqProcessingStatePtr) return reqEntry->reqProcessingStatePtr->getReqSeqNum();
  return 0;
}

// For test purposes
RequestsBatchSharedPtr PreProcessor::getOngoingBatchForClient(uint16_t clientId) {
  return ongoingReqBatches_[clientId];
}

void PreProcessor::handleReqPreProcessedByPrimary(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                                  const string &batchCid,
                                                  uint16_t clientId,
                                                  uint32_t resultBufLen,
                                                  OperationResult preProcessResult) {
  const uint16_t &reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  const auto &reqCid = preProcessReqMsg->getCid();
  const auto &reqSeqNum = preProcessReqMsg->reqSeqNum();
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.handlePreProcessedReqByPrimary);
  PreProcessingResult result = NONE;
  const auto &reqEntry = ongoingReqBatches_[clientId]->getRequestState(reqOffsetInBatch);
  {
    lock_guard<mutex> lock(reqEntry->mutex);
    if (reqEntry->reqProcessingStatePtr) {
      reqEntry->reqProcessingStatePtr->handlePrimaryPreProcessed(
          getPreProcessResultBuffer(clientId, reqEntry->reqProcessingStatePtr->getReqSeqNum(), reqOffsetInBatch),
          resultBufLen,
          preProcessResult);
      result = reqEntry->reqProcessingStatePtr->definePreProcessingConsensusResult();
    }
    LOG_DEBUG(logger(),
              "Request has been pre-processed by the primary replica"
                  << KVLOG(clientId, batchCid, reqSeqNum, reqCid, (uint32_t)preProcessResult, result));
    if (result != NONE) holdCompleteOrCancelRequest(reqCid, result, clientId, reqOffsetInBatch, reqSeqNum, batchCid);
  }
}

void PreProcessor::releaseReqAndSendReplyMsg(PreProcessReplyMsgSharedPtr replyMsg) {
  // Release the request before sending a reply message to the primary replica to be able accepting new messages
  releaseClientPreProcessRequestSafe(replyMsg->clientId(), replyMsg->reqOffsetInBatch(), COMPLETE);
  sendMsg(replyMsg->body(), myReplica_.currentPrimary(), replyMsg->type(), replyMsg->size());
  LOG_INFO(logger(),
           "Pre-processing completed by a non-primary replica and the reply message sent to the primary"
               << KVLOG(replyMsg->clientId(),
                        replyMsg->reqSeqNum(),
                        replyMsg->getCid(),
                        replyMsg->reqOffsetInBatch(),
                        replyMsg->reqRetryId(),
                        myReplica_.currentPrimary(),
                        (uint32_t)replyMsg->preProcessResult()));
}

void PreProcessor::handleReqPreProcessedByNonPrimary(uint16_t clientId,
                                                     uint16_t reqOffsetInBatch,
                                                     ReqId reqSeqNum,
                                                     uint64_t reqRetryId,
                                                     uint32_t resBufLen,
                                                     const std::string &reqCid,
                                                     OperationResult preProcessResult) {
  concord::diagnostics::TimeRecorder scoped_timer(*histograms_.handlePreProcessedReqByNonPrimary);
  const auto status = (preProcessResult == OperationResult::SUCCESS) ? STATUS_GOOD : STATUS_FAILED;
  auto replyMsg = make_shared<PreProcessReplyMsg>(myReplicaId_,
                                                  clientId,
                                                  reqOffsetInBatch,
                                                  reqSeqNum,
                                                  reqRetryId,
                                                  getPreProcessResultBuffer(clientId, reqSeqNum, reqOffsetInBatch),
                                                  resBufLen,
                                                  reqCid,
                                                  status,
                                                  preProcessResult,
                                                  myReplica_.getCurrentView());
  const auto &batchEntry = ongoingReqBatches_[clientId];
  if (batchEntry->isBatchInProcess()) {
    batchEntry->releaseReqsAndSendBatchedReplyIfCompleted(replyMsg);
  } else {
    releaseReqAndSendReplyMsg(replyMsg);
  }
  setPreprocessingRightNow(clientId, reqOffsetInBatch, false);
}

void PreProcessor::handleReqPreProcessingJob(const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                             const string &batchCid,
                                             bool isPrimary) {
  const string &reqCid = preProcessReqMsg->getCid();
  const uint16_t &clientId = preProcessReqMsg->clientId();
  const uint16_t &reqOffsetInBatch = preProcessReqMsg->reqOffsetInBatch();
  const SeqNum &reqSeqNum = preProcessReqMsg->reqSeqNum();
  uint32_t actualResultBufLen = 0;
  const auto preProcessResult = launchReqPreProcessing(batchCid, preProcessReqMsg, actualResultBufLen);
  SCOPED_MDC_CID(reqCid);
  if (isPrimary) {
    pm_->Delay<concord::performance::SlowdownPhase::PreProcessorAfterPreexecPrimary>();
    handleReqPreProcessedByPrimary(preProcessReqMsg, batchCid, clientId, actualResultBufLen, preProcessResult);
  } else {
    pm_->Delay<concord::performance::SlowdownPhase::PreProcessorAfterPreexecNonPrimary>();
    handleReqPreProcessedByNonPrimary(clientId,
                                      reqOffsetInBatch,
                                      reqSeqNum,
                                      preProcessReqMsg->reqRetryId(),
                                      actualResultBufLen,
                                      preProcessReqMsg->getCid(),
                                      preProcessResult);
  }
}

//**************** Class AsyncPreProcessJob ****************//

AsyncPreProcessJob::AsyncPreProcessJob(PreProcessor &preProcessor,
                                       const PreProcessRequestMsgSharedPtr &preProcessReqMsg,
                                       const string &batchCid,
                                       bool isPrimary,
                                       TimeRecorder &&totalPreExecDurationRecorder,
                                       TimeRecorder &&launchAsyncPreProcessJobRecorder)
    : preProcessor_(preProcessor),
      preProcessReqMsg_(preProcessReqMsg),
      batchCid_(batchCid),
      isPrimary_(isPrimary),
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
  preProcessor_.handleReqPreProcessingJob(preProcessReqMsg_, batchCid_, isPrimary_);
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
