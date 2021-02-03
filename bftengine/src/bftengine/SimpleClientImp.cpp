// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <queue>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <cmath>
#include <condition_variable>
#include <memory>
#include <SimpleClient.hpp>

#include "ClientMsgs.hpp"
#include "OpenTracing.hpp"
#include "SimpleClient.hpp"
#include "assertUtils.hpp"
#include "TimeUtils.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "messages/ClientBatchRequestMsg.hpp"
#include "messages/MsgsCertificate.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"
#include "Logger.hpp"

using namespace std;
using namespace std::chrono;
using namespace bft::communication;
using namespace preprocessor;

namespace bftEngine {
namespace impl {
class SimpleClientImp : public SimpleClient, public IReceiver {
 public:
  SimpleClientImp(ICommunication* communication,
                  uint16_t clientId,
                  uint16_t fVal,
                  uint16_t cVal,
                  SimpleClientParams& p,
                  const std::shared_ptr<concord::performance::PerformanceManager>& pm);
  ~SimpleClientImp() override;

  OperationResult sendRequest(uint8_t flags,
                              const char* request,
                              uint32_t lengthOfRequest,
                              uint64_t reqSeqNum,
                              uint64_t timeoutMilli,
                              uint32_t lengthOfReplyBuffer,
                              char* replyBuffer,
                              uint32_t& actualReplyLength,
                              const std::string& cid,
                              const std::string& spanContext) override;

  OperationResult sendBatch(const deque<ClientRequest>& clientRequests,
                            deque<ClientReply>& clientReplies,
                            const std::string& cid) override;

  // IReceiver methods
  void onNewMessage(NodeNum sourceNode, const char* const message, size_t messageLength) override;
  void onConnectionStatusChanged(NodeNum node, ConnectionStatus newStatus) override;

  // used by  MsgsCertificate
  static bool equivalent(ClientReplyMsg* r1, ClientReplyMsg* r2) {
    if (r1->reqSeqNum() != r2->reqSeqNum()) return false;
    if (r1->currentPrimaryId() != r2->currentPrimaryId()) return false;
    if (r1->replyLength() != r2->replyLength()) return false;

    char* p1 = r1->replyBuf();
    char* p2 = r2->replyBuf();

    return (memcmp(p1, p2, r1->replyLength()) == 0);
  }

 protected:
  bool isSystemReady() const;
  void sendPendingRequest(bool isBatch, const std::string& cid);
  void onMessageFromReplica(MessageBase* msg);
  void onRetransmission(bool isBatch, const std::string& cid);
  void reset();
  bool allRequiredRepliesReceived();
  void sendRequestToAllOrToPrimary(bool sendToAll, char* data, uint64_t size);
  OperationResult isBatchValid(uint64_t requestsNbr, uint64_t repliesNbr);
  OperationResult isBatchRequestValid(const ClientRequest& req);
  OperationResult preparePendingRequestsFromBatch(const deque<ClientRequest>& clientRequests, uint64_t& maxTimeToWait);
  void verifySendRequestPrerequisites();
  tuple<bool, bool> pendingRequestProcessing(bool isBatch,
                                             const Time& beginTime,
                                             uint64_t reqTimeoutMilli,
                                             const std::string& cid);

 protected:
  static const uint32_t maxLegalMsgSize_ = 64 * 1024;  // TODO(GG): ???
  static const uint16_t timersResolutionMilli_ = 50;

  const uint16_t clientId_;
  const uint16_t numberOfReplicas_;
  const uint16_t numberOfRequiredReplicas_;
  const uint16_t fVal_;
  const uint16_t cVal_;
  const std::set<uint16_t> replicas_;
  ICommunication* const communication_;

  // SeqNumber -> MsgsCertificate
  typedef MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> Certificate;
  std::unordered_map<uint64_t, std::unique_ptr<Certificate>> replysCertificate_;

  std::mutex lock_;  // protects _msgQueue and pendingRequest
  std::condition_variable condVar_;

  std::queue<MessageBase*> msgQueue_;
  std::deque<ClientRequestMsg*> pendingRequest_;

  Time timeOfLastTransmission_ = MinTime;
  uint16_t numberOfTransmissions_ = 0;

  bool primaryReplicaIsKnown_ = false;
  uint16_t knownPrimaryReplica_;

  DynamicUpperLimitWithSimpleFilter<uint64_t> limitOfExpectedOperationTime_;

  // configuration params
  uint16_t clientSendsRequestToAllReplicasFirstThresh_;
  uint16_t clientSendsRequestToAllReplicasPeriodThresh_;
  uint16_t clientPeriodicResetThresh_;
  std::shared_ptr<concord::performance::PerformanceManager> pm_ = nullptr;

  logging::Logger logger_ = logging::getLogger("concord.bft.client");
};

bool SimpleClientImp::isSystemReady() const {
  uint16_t connectedReplicasNum = 0;
  for (uint16_t rid : replicas_)
    if (communication_->getCurrentConnectionStatus(rid) == ConnectionStatus::Connected) connectedReplicasNum++;

  bool systemReady = (connectedReplicasNum >= numberOfRequiredReplicas_);
  if (!systemReady)
    LOG_WARN(logger_,
             "The system is not ready: connectedReplicasNum=" << connectedReplicasNum << " numberOfRequiredReplicas="
                                                              << numberOfRequiredReplicas_);
  return systemReady;
}

void SimpleClientImp::onMessageFromReplica(MessageBase* msg) {
  ClientReplyMsg* replyMsg = static_cast<ClientReplyMsg*>(msg);
  replyMsg->validate(ReplicasInfo());
  ConcordAssert(replyMsg != nullptr);
  ConcordAssert(replyMsg->type() == REPLY_MSG_TYPE);

  LOG_DEBUG(
      logger_,
      "Received ClientReplyMsg" << KVLOG(
          clientId_, replyMsg->reqSeqNum(), replyMsg->senderId(), replyMsg->size(), (int)replyMsg->currentPrimaryId()));

  bool pendingReqFound = false;
  for (auto const& m : pendingRequest_) {
    if (m->requestSeqNum() == replyMsg->reqSeqNum()) {
      pendingReqFound = true;
      break;
    }
  }
  if (!pendingReqFound) {
    delete msg;
    return;
  }

  auto iter = replysCertificate_.find(replyMsg->reqSeqNum());
  if (iter == std::cend(replysCertificate_)) {
    auto cert = std::make_unique<Certificate>(numberOfReplicas_, fVal_, numberOfRequiredReplicas_, clientId_);
    iter = replysCertificate_.insert(std::make_pair(replyMsg->reqSeqNum(), std::move(cert))).first;
  }
  iter->second->addMsg(replyMsg, replyMsg->senderId());
  if (iter->second->isInconsistent()) iter->second->resetAndFree();
}

void SimpleClientImp::onRetransmission(bool isBatch, const std::string& cid) {
  client_metrics_.retransmissions.Get().Inc();
  sendPendingRequest(isBatch, cid);
}

// in this version we assume that the set of replicas is 0,1,2,...,numberOfReplicas (TODO(GG): should be changed to
// support full dynamic reconfiguration)
static std::set<ReplicaId> generateSetOfReplicas_helpFunc(const int16_t numberOfReplicas) {
  std::set<ReplicaId> retVal;
  for (int16_t i = 0; i < numberOfReplicas; i++) retVal.insert(i);
  return retVal;
}

SimpleClientImp::SimpleClientImp(ICommunication* communication,
                                 uint16_t clientId,
                                 uint16_t fVal,
                                 uint16_t cVal,
                                 SimpleClientParams& p,
                                 const std::shared_ptr<concord::performance::PerformanceManager>& pm)
    : SimpleClient(clientId, pm),
      clientId_{clientId},
      numberOfReplicas_(3 * fVal + 2 * cVal + 1),
      numberOfRequiredReplicas_(2 * fVal + cVal + 1),
      fVal_{fVal},
      cVal_{cVal},
      replicas_{generateSetOfReplicas_helpFunc(numberOfReplicas_)},
      communication_{communication},
      limitOfExpectedOperationTime_(p.clientInitialRetryTimeoutMilli,
                                    p.numberOfStandardDeviationsToTolerate,
                                    p.clientMaxRetryTimeoutMilli,
                                    p.clientMinRetryTimeoutMilli,
                                    p.samplesPerEvaluation,
                                    p.samplesUntilReset,
                                    p.clientSendsRequestToAllReplicasFirstThresh,
                                    p.clientSendsRequestToAllReplicasPeriodThresh),
      clientSendsRequestToAllReplicasFirstThresh_{p.clientSendsRequestToAllReplicasFirstThresh},
      clientSendsRequestToAllReplicasPeriodThresh_{p.clientSendsRequestToAllReplicasPeriodThresh},
      clientPeriodicResetThresh_{p.clientPeriodicResetThresh},
      pm_{pm} {
  ConcordAssert(fVal_ >= 1);

  timeOfLastTransmission_ = MinTime;
  numberOfTransmissions_ = 0;
  primaryReplicaIsKnown_ = false;
  knownPrimaryReplica_ = 0;

  communication_->setReceiver(clientId_, this);
}

SimpleClientImp::~SimpleClientImp() {}

bool SimpleClientImp::allRequiredRepliesReceived() {
  if (replysCertificate_.empty()) return false;
  for (auto& elem : replysCertificate_) {
    if (!elem.second->isComplete()) {
      LOG_DEBUG(logger_, "Not all replies received" << KVLOG(clientId_));
      return false;
    }
  }
  LOG_DEBUG(logger_, "All replies received" << KVLOG(clientId_));
  return true;
}

void SimpleClientImp::verifySendRequestPrerequisites() {
  ConcordAssert(replysCertificate_.empty());
  ConcordAssert(msgQueue_.empty());
  ConcordAssert(pendingRequest_.empty());
  ConcordAssert(timeOfLastTransmission_ == MinTime);
  ConcordAssert(numberOfTransmissions_ == 0);
}

tuple<bool, bool> SimpleClientImp::pendingRequestProcessing(bool isBatch,
                                                            const Time& beginTime,
                                                            uint64_t reqTimeoutMilli,
                                                            const std::string& cid) {
  bool requestCommitted = false;
  bool requestTimedOut = false;
  auto predicate = [this] { return !msgQueue_.empty(); };
  static const chrono::milliseconds timersRes(timersResolutionMilli_);
  while (true) {
    queue<MessageBase*> newMsgs;
    bool hasData = false;
    {
      unique_lock<std::mutex> mlock(lock_);
      hasData = condVar_.wait_for(mlock, timersRes, predicate);
      if (hasData) msgQueue_.swap(newMsgs);
    }
    if (hasData) {
      while (!newMsgs.empty()) {
        MessageBase* msg = newMsgs.front();
        onMessageFromReplica(msg);
        newMsgs.pop();
      }
    }
    if (allRequiredRepliesReceived()) {
      requestCommitted = true;
      break;
    }
    const Time currTime = getMonotonicTime();
    if (reqTimeoutMilli != INFINITE_TIMEOUT &&
        (uint64_t)duration_cast<milliseconds>(currTime - beginTime).count() > reqTimeoutMilli) {
      requestTimedOut = true;
      break;
    }
    if ((uint64_t)duration_cast<milliseconds>(currTime - timeOfLastTransmission_).count() >
        limitOfExpectedOperationTime_.upperLimit()) {
      onRetransmission(isBatch, cid);
    }
  }
  return {requestCommitted, requestTimedOut};
}

OperationResult SimpleClientImp::sendRequest(uint8_t flags,
                                             const char* request,
                                             uint32_t lenOfRequest,
                                             uint64_t reqSeqNum,
                                             uint64_t reqTimeoutMilli,
                                             uint32_t lenOfReplyBuffer,
                                             char* replyBuffer,
                                             uint32_t& actualReplyLength,
                                             const std::string& cid,
                                             const std::string& spanContext) {
  bool isReadOnly = flags & READ_ONLY_REQ;
  bool isPreProcessRequired = flags & PRE_PROCESS_REQ;
  const std::string msgCid = cid.empty() ? std::to_string(reqSeqNum) + "-" + std::to_string(clientId_) : cid;
  const auto& maxRetransmissionTimeout = limitOfExpectedOperationTime_.upperLimit();
  LOG_DEBUG(logger_,
            KVLOG(clientId_,
                  reqSeqNum,
                  msgCid,
                  isReadOnly,
                  isPreProcessRequired,
                  lenOfRequest,
                  reqTimeoutMilli,
                  maxRetransmissionTimeout,
                  spanContext.empty()));
  ConcordAssert(!(isReadOnly && isPreProcessRequired));

  if (!communication_->isRunning()) communication_->Start();
  if (!isReadOnly && !isSystemReady()) {
    LOG_WARN(logger_,
             "The system is not ready yet to handle requests => reject"
                 << KVLOG(reqSeqNum, clientId_, cid, reqTimeoutMilli));
    reset();
    return NOT_READY;
  }
  verifySendRequestPrerequisites();
  const Time beginTime = getMonotonicTime();
  ClientRequestMsg* reqMsg;
  concordUtils::SpanContext ctx{spanContext};
  if (isPreProcessRequired)
    reqMsg = new ClientPreProcessRequestMsg(clientId_, reqSeqNum, lenOfRequest, request, reqTimeoutMilli, msgCid, ctx);
  else
    reqMsg = new ClientRequestMsg(clientId_, flags, reqSeqNum, lenOfRequest, request, reqTimeoutMilli, msgCid, ctx);
  pendingRequest_.push_back(reqMsg);
  sendPendingRequest(false, cid);

  client_metrics_.retransmissionTimer.Get().Set(maxRetransmissionTimeout);
  metrics_.UpdateAggregator();

  auto [requestCommitted, requestTimedOut] = pendingRequestProcessing(false, beginTime, reqTimeoutMilli, cid);
  if (requestCommitted) {
    uint64_t durationMilli = duration_cast<milliseconds>(getMonotonicTime() - beginTime).count();
    limitOfExpectedOperationTime_.add(durationMilli);
    LOG_DEBUG(logger_,
              "Request has committed" << KVLOG(
                  clientId_, reqSeqNum, isReadOnly, isPreProcessRequired, (int)maxRetransmissionTimeout));

    const auto& elem = replysCertificate_.find(reqSeqNum);
    ClientReplyMsg* correctReply = elem->second->bestCorrectMsg();
    primaryReplicaIsKnown_ = true;
    knownPrimaryReplica_ = correctReply->currentPrimaryId();
    OperationResult res = SUCCESS;
    if (correctReply->replyLength() <= lenOfReplyBuffer) {
      memcpy(replyBuffer, correctReply->replyBuf(), correctReply->replyLength());
      actualReplyLength = correctReply->replyLength();
    } else
      res = BUFFER_TOO_SMALL;
    reset();
    return res;
  } else if (requestTimedOut) {
    LOG_DEBUG(logger_, "Client " << clientId_ << " request :" << reqSeqNum << " timeout");
    if (reqTimeoutMilli >= maxRetransmissionTimeout) {
      LOG_DEBUG(logger_, "Primary is set to UNKNOWN" << KVLOG(clientId_, reqSeqNum));
      primaryReplicaIsKnown_ = false;
      limitOfExpectedOperationTime_.add(reqTimeoutMilli);
    }
    reset();
    return TIMEOUT;
  }
  ConcordAssert(false);
}

OperationResult SimpleClientImp::isBatchRequestValid(const ClientRequest& req) {
  OperationResult res = SUCCESS;
  if (req.flags & READ_ONLY_REQ) {
    LOG_ERROR(logger_, "Read-only requests could not be sent in a batch" << KVLOG(req.reqSeqNum, clientId_, req.cid));
    res = INVALID_REQUEST;
  } else if (!(req.flags & PRE_PROCESS_REQ)) {
    LOG_ERROR(logger_,
              "Requests batching is supported only for requests intended for pre-processing"
                  << KVLOG(req.reqSeqNum, clientId_, req.cid));
    res = INVALID_REQUEST;
  }
  if (res != SUCCESS) reset();
  return res;
}

OperationResult SimpleClientImp::isBatchValid(uint64_t requestsNbr, uint64_t repliesNbr) {
  if (!communication_->isRunning()) communication_->Start();
  OperationResult res = SUCCESS;
  if (!requestsNbr) {
    LOG_ERROR(logger_, "An empty request list specified");
    res = INVALID_REQUEST;
  } else if (requestsNbr != repliesNbr) {
    LOG_ERROR(logger_,
              "The number of requests is not equal to the number of replies" << KVLOG(requestsNbr, repliesNbr));
    res = INVALID_REQUEST;
  } else if (!isSystemReady()) {
    LOG_WARN(logger_, "The system is not ready yet to handle requests => reject" << KVLOG(clientId_));
    res = NOT_READY;
  }
  if (res != SUCCESS) reset();
  return res;
}

OperationResult SimpleClientImp::preparePendingRequestsFromBatch(const deque<ClientRequest>& clientRequests,
                                                                 uint64_t& maxTimeToWait) {
  OperationResult res = SUCCESS;
  ClientRequestMsg* reqMsg = nullptr;
  const auto& maxRetransmissionTimeout = limitOfExpectedOperationTime_.upperLimit();
  for (auto& req : clientRequests) {
    res = isBatchRequestValid(req);
    if (res != SUCCESS) return res;
    const auto& cid = req.cid.empty() ? to_string(req.reqSeqNum) + "-" + to_string(clientId_) : req.cid;
    if (maxTimeToWait != INFINITE_TIMEOUT) {
      if (req.timeoutMilli != INFINITE_TIMEOUT)
        maxTimeToWait += req.timeoutMilli;
      else
        maxTimeToWait = INFINITE_TIMEOUT;
    }
    LOG_DEBUG(logger_,
              KVLOG(clientId_,
                    req.reqSeqNum,
                    cid,
                    req.lengthOfRequest,
                    req.timeoutMilli,
                    maxRetransmissionTimeout,
                    req.span_context.empty()));
    concordUtils::SpanContext ctx{req.span_context};
    reqMsg = new ClientRequestMsg(clientId_,
                                  req.flags,
                                  req.reqSeqNum,
                                  req.lengthOfRequest,
                                  (char*)req.request.data(),
                                  req.timeoutMilli,
                                  cid,
                                  ctx);
    pendingRequest_.push_back(reqMsg);
  }
  return res;
}

OperationResult SimpleClientImp::sendBatch(const deque<ClientRequest>& clientRequests,
                                           deque<ClientReply>& clientReplies,
                                           const std::string& cid) {
  LOG_DEBUG(logger_, KVLOG(clientId_, clientRequests.size(), cid));
  OperationResult res = isBatchValid(clientRequests.size(), clientReplies.size());
  if (res != SUCCESS) return res;
  verifySendRequestPrerequisites();

  uint64_t maxTimeToWait = 0;
  res = preparePendingRequestsFromBatch(clientRequests, maxTimeToWait);
  if (res != SUCCESS) return res;

  const Time beginTime = getMonotonicTime();
  sendPendingRequest(true, cid);

  const auto& maxRetransmissionTimeout = limitOfExpectedOperationTime_.upperLimit();
  client_metrics_.retransmissionTimer.Get().Set(maxRetransmissionTimeout);
  metrics_.UpdateAggregator();

  auto [requestCommitted, requestTimedOut] = pendingRequestProcessing(true, beginTime, maxTimeToWait, cid);
  if (requestCommitted) {
    uint64_t durationMilli = duration_cast<milliseconds>(getMonotonicTime() - beginTime).count();
    limitOfExpectedOperationTime_.add(durationMilli);
    string reqCid;
    for (auto& reply : replysCertificate_) {
      ClientReplyMsg* correctReply = reply.second->bestCorrectMsg();
      const auto reqSeqNum = correctReply->reqSeqNum();
      for (const auto& req : clientRequests)
        if (req.reqSeqNum == reqSeqNum) reqCid = req.cid;
      ClientReply* givenReply = nullptr;
      for (auto& rep : clientReplies)
        if (rep.cid == reqCid) givenReply = &rep;
      ConcordAssertNE(givenReply, nullptr);
      LOG_DEBUG(logger_, KVLOG(clientId_, reqSeqNum, reqCid) << " has committed");
      primaryReplicaIsKnown_ = true;
      knownPrimaryReplica_ = correctReply->currentPrimaryId();
      if (correctReply->replyLength() <= givenReply->lengthOfReplyBuffer) {
        memcpy(givenReply->replyBuffer, correctReply->replyBuf(), correctReply->replyLength());
        givenReply->actualReplyLength = correctReply->replyLength();
      } else {
        reset();
        return BUFFER_TOO_SMALL;
      }
    }
    reset();
    return SUCCESS;
  } else if (requestTimedOut) {
    LOG_DEBUG(logger_, "Batch timed out" << KVLOG(clientId_, pendingRequest_[0]->requestSeqNum(), maxTimeToWait));
    if (maxTimeToWait >= maxRetransmissionTimeout) {
      LOG_DEBUG(logger_, KVLOG(clientId_, pendingRequest_[0]->requestSeqNum()) << " primary is set to UNKNOWN");
      primaryReplicaIsKnown_ = false;
      limitOfExpectedOperationTime_.add(maxTimeToWait);
    }
    reset();
    return TIMEOUT;
  }
  ConcordAssert(false);
}

void SimpleClientImp::reset() {
  LOG_DEBUG(logger_, KVLOG(clientId_, replysCertificate_.size()));
  for (auto& elem : replysCertificate_) elem.second->resetAndFree();
  replysCertificate_.clear();

  std::queue<MessageBase*> newMsgs;
  {
    std::unique_lock<std::mutex> mlock(lock_);
    msgQueue_.swap(newMsgs);

    for (auto const& msg : pendingRequest_) delete msg;
    pendingRequest_.clear();
  }

  while (!newMsgs.empty()) {
    delete newMsgs.front();
    newMsgs.pop();
  }

  timeOfLastTransmission_ = MinTime;
  numberOfTransmissions_ = 0;
}

void SimpleClientImp::onNewMessage(NodeNum sourceNode, const char* const message, size_t messageLength) {
  // check source
  int16_t senderId = (int16_t)sourceNode;
  if (replicas_.count(senderId) == 0) return;

  // check length
  if (messageLength > maxLegalMsgSize_) return;
  if (messageLength < sizeof(MessageBase::Header)) return;

  MessageBase::Header* msgHeader = (MessageBase::Header*)message;

  // check type
  if (msgHeader->msgType != REPLY_MSG_TYPE) return;

  std::unique_lock<std::mutex> mlock(lock_);
  {
    if (pendingRequest_.empty()) return;

    // create msg object
    MessageBase::Header* msgBody = (MessageBase::Header*)std::malloc(messageLength);
    memcpy(msgBody, message, messageLength);
    MessageBase* pMsg = new MessageBase(senderId, msgBody, messageLength, true);

    msgQueue_.push(pMsg);  // TODO(GG): handle overflow
  }
  condVar_.notify_one();
}

void SimpleClientImp::onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) {}

void SimpleClientImp::sendRequestToAllOrToPrimary(bool sendToAll, char* data, uint64_t size) {
  if (sendToAll) {
    LOG_DEBUG(logger_, "Send request to all replicas" << KVLOG(clientId_, pendingRequest_[0]->requestSeqNum()));
    for (uint16_t r : replicas_) communication_->sendAsyncMessage(r, data, size);
  } else {
    LOG_DEBUG(logger_, "Send request to primary replica" << KVLOG(clientId_, pendingRequest_[0]->requestSeqNum()));
    pm_->Delay<concord::performance::SlowdownPhase::BftClientBeforeSendPrimary>();
    communication_->sendAsyncMessage(knownPrimaryReplica_, data, size);
  }
}

void SimpleClientImp::sendPendingRequest(bool isBatch, const std::string& cid) {
  ConcordAssert(!pendingRequest_.empty());

  timeOfLastTransmission_ = getMonotonicTime();
  numberOfTransmissions_++;

  const bool resetReplies = (numberOfTransmissions_ % clientPeriodicResetThresh_ == 0);
  const bool readOnly = pendingRequest_.front()->isReadOnly();
  const bool sendToAll = readOnly || !primaryReplicaIsKnown_ ||
                         (numberOfTransmissions_ == clientSendsRequestToAllReplicasFirstThresh_) ||
                         (numberOfTransmissions_ > clientSendsRequestToAllReplicasFirstThresh_ &&
                          (numberOfTransmissions_ % clientSendsRequestToAllReplicasPeriodThresh_ == 0)) ||
                         resetReplies;

  LOG_DEBUG(logger_,
            "Client " << clientId_ << " sends request " << pendingRequest_.front()->requestSeqNum()
                      << " isRO=" << readOnly << ", request size=" << (size_t)pendingRequest_.front()->size()
                      << ", retransmissionMilli=" << (int)limitOfExpectedOperationTime_.upperLimit()
                      << ", numberOfTransmissions=" << numberOfTransmissions_ << ", resetReplies=" << resetReplies
                      << ", sendToAll=" << sendToAll);

  if (resetReplies) {
    LOG_DEBUG(logger_, "Resetting replies" << KVLOG(clientId_, pendingRequest_.front()->requestSeqNum()));
    for (auto& elem : replysCertificate_) elem.second->resetAndFree();
    replysCertificate_.clear();
  }

  if (!isBatch) return sendRequestToAllOrToPrimary(sendToAll, pendingRequest_[0]->body(), pendingRequest_[0]->size());

  uint32_t batchBufSize = 0;
  for (auto const& msg : pendingRequest_) batchBufSize += msg->size();
  ClientBatchRequestMsg* batchMsg = new ClientBatchRequestMsg(clientId_, pendingRequest_, batchBufSize, cid);
  sendRequestToAllOrToPrimary(sendToAll, batchMsg->body(), batchMsg->size());
  delete batchMsg;
}

// SeqNumberGeneratorForClientRequestsImp, generates unique, monotonically increasing, sequence number.
// The SN is a time stamp processed and derived from the system clock.
// The SN is a bitwise or between lastMilli and lastCount.
// Equal time stamp will be bitwise or, with different lastCount values,
// and uniqueness will bre preserved.
class SeqNumberGeneratorForClientRequestsImp : public SeqNumberGeneratorForClientRequests {
  virtual uint64_t generateUniqueSequenceNumberForRequest() override;
  virtual uint64_t generateUniqueSequenceNumberForRequest(
      std::chrono::time_point<std::chrono::system_clock> now) override;

 protected:
  // limited to the size lastMilli shifted.
  const u_int64_t last_count_limit = 0x3FFFFF;
  // lastMilliOfUniqueFetchID_ holds the last SN generated,
  uint64_t lastMilliOfUniqueFetchID_ = 0;
  // lastCount used to preserve uniqueness.
  uint32_t lastCountOfUniqueFetchID_ = 0;
};

uint64_t SeqNumberGeneratorForClientRequestsImp::generateUniqueSequenceNumberForRequest() {
  std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
  return generateUniqueSequenceNumberForRequest(now);
}

uint64_t SeqNumberGeneratorForClientRequestsImp::generateUniqueSequenceNumberForRequest(
    std::chrono::time_point<std::chrono::system_clock> now) {
  uint64_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

  if (milli > lastMilliOfUniqueFetchID_) {
    lastMilliOfUniqueFetchID_ = milli;
    lastCountOfUniqueFetchID_ = 0;
  } else {
    if (lastCountOfUniqueFetchID_ == last_count_limit) {
      LOG_WARN(GL, "Client SeqNum Counter reached max value");
      lastMilliOfUniqueFetchID_++;
      lastCountOfUniqueFetchID_ = 0;
    } else {  // increase last count to preserve uniqueness.
      lastCountOfUniqueFetchID_++;
    }
  }
  // shift lastMilli by 22 (0x3FFFFF) in order to 'bitwise or' with lastCount
  // and preserve uniqueness and monotonicity.
  uint64_t r = (lastMilliOfUniqueFetchID_ << (64 - 42));
  ConcordAssert(lastCountOfUniqueFetchID_ <= 0x3FFFFF);
  r = r | ((uint64_t)lastCountOfUniqueFetchID_);

  return r;
}

}  // namespace impl

SimpleClient* SimpleClient::createSimpleClient(ICommunication* communication,
                                               uint16_t clientId,
                                               uint16_t fVal,
                                               uint16_t cVal,
                                               SimpleClientParams p,
                                               const std::shared_ptr<concord::performance::PerformanceManager>& pm) {
  return new impl::SimpleClientImp(communication, clientId, fVal, cVal, p, pm);
}

SimpleClient* SimpleClient::createSimpleClient(ICommunication* communication,
                                               uint16_t clientId,
                                               uint16_t fVal,
                                               uint16_t cVal,
                                               const std::shared_ptr<concord::performance::PerformanceManager>& pm) {
  SimpleClientParams p;
  return SimpleClient::createSimpleClient(communication, clientId, fVal, cVal, p, pm);
}

SimpleClient::~SimpleClient() = default;

std::unique_ptr<SeqNumberGeneratorForClientRequests>
SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests() {
  return std::make_unique<impl::SeqNumberGeneratorForClientRequestsImp>();
}

}  // namespace bftEngine
