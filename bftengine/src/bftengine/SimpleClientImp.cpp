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

#include "ClientMsgs.hpp"
#include "OpenTracing.hpp"
#include "SimpleClient.hpp"
#include "assertUtils.hpp"
#include "TimeUtils.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "messages/ClientPreProcessRequestMsg.hpp"
#include "messages/ClientBatchRequestMsg.hpp"
#include "messages/MsgsCertificate.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"
#include "Logger.hpp"

using namespace std;
using namespace std::chrono;
using namespace bft::communication;

namespace bftEngine {
namespace impl {
class SimpleClientImp : public SimpleClient, public IReceiver {
 public:
  SimpleClientImp(
      ICommunication* communication, uint16_t clientId, uint16_t fVal, uint16_t cVal, SimpleClientParams& p);
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
                              const std::string& span_context) override;

  OperationResult sendBatch(const std::deque<ClientRequest>& clientRequests,
                            std::deque<ClientReply>& clientReplies) override;

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
  void sendPendingRequest();
  void onMessageFromReplica(MessageBase* msg);
  void onRetransmission();
  void reset();
  bool allRequiredRepliesReceived();
  void sendRequestToAllOrToPrimary(bool sendToAll, char* data, uint64_t size);
  OperationResult isBatchValid(uint64_t requestsSize, uint64_t repliesSize);
  OperationResult isBatchRequestValid(const ClientRequest& req);

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
  std::unordered_map<uint64_t, Certificate> replysCertificate_;

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

  Certificate msgsCertificate(numberOfReplicas_, fVal_, numberOfRequiredReplicas_, clientId_);
  msgsCertificate.addMsg(replyMsg, replyMsg->senderId());
  replysCertificate_.insert(pair<uint64_t, Certificate>(replyMsg->reqSeqNum(), msgsCertificate));
  auto elem = replysCertificate_.find(replyMsg->reqSeqNum());
  if (elem->second.isInconsistent()) elem->second.resetAndFree();
}

void SimpleClientImp::onRetransmission() {
  client_metrics_.retransmissions.Get().Inc();
  sendPendingRequest();
}

// in this version we assume that the set of replicas is 0,1,2,...,numberOfReplicas (TODO(GG): should be changed to
// support full dynamic reconfiguration)
static std::set<ReplicaId> generateSetOfReplicas_helpFunc(const int16_t numberOfReplicas) {
  std::set<ReplicaId> retVal;
  for (int16_t i = 0; i < numberOfReplicas; i++) retVal.insert(i);
  return retVal;
}

SimpleClientImp::SimpleClientImp(
    ICommunication* communication, uint16_t clientId, uint16_t fVal, uint16_t cVal, SimpleClientParams& p)
    : SimpleClient(clientId),
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
      clientPeriodicResetThresh_{p.clientPeriodicResetThresh} {
  ConcordAssert(fVal_ >= 1);

  timeOfLastTransmission_ = MinTime;
  numberOfTransmissions_ = 0;
  primaryReplicaIsKnown_ = false;
  knownPrimaryReplica_ = 0;

  communication_->setReceiver(clientId_, this);
}

SimpleClientImp::~SimpleClientImp() {}

bool SimpleClientImp::allRequiredRepliesReceived() {
  bool completed = false;
  for (auto& elem : replysCertificate_) {
    if (!elem.second.isComplete()) return false;
    completed = true;
  }
  return completed;
}

OperationResult SimpleClientImp::sendRequest(uint8_t flags,
                                             const char* request,
                                             uint32_t lengthOfRequest,
                                             uint64_t reqSeqNum,
                                             uint64_t timeoutMilli,
                                             uint32_t lengthOfReplyBuffer,
                                             char* replyBuffer,
                                             uint32_t& actualReplyLength,
                                             const std::string& cid,
                                             const std::string& span_context) {
  bool isReadOnly = flags & READ_ONLY_REQ;
  bool isPreProcessRequired = flags & PRE_PROCESS_REQ;
  if (isPreProcessRequired) {
    LOG_ERROR(logger_,
              "The 'batch send' to be used for requests intended for pre-processing"
                  << KVLOG(reqSeqNum, clientId_, cid, timeoutMilli));
    reset();
    return INVALID_REQUEST;
  }
  const std::string msgCid = cid.empty() ? std::to_string(reqSeqNum) + "-" + std::to_string(clientId_) : cid;
  // TODO(GG): check params ...
  LOG_DEBUG(logger_,
            "Client " << clientId_ << " - sends request " << reqSeqNum << ", cid=" << msgCid << " (isRO=" << isReadOnly
                      << ", isPreProcess=" << isPreProcessRequired << " , request size=" << lengthOfRequest
                      << ", retransmissionMilli=" << limitOfExpectedOperationTime_.upperLimit()
                      << ", timeout=" << timeoutMilli << ", has span context=" << !span_context.empty());
  ConcordAssert(!(isReadOnly && isPreProcessRequired));

  if (!communication_->isRunning()) communication_->Start();
  if (!isReadOnly && !isSystemReady()) {
    LOG_WARN(logger_,
             "The system is not ready yet to handle requests. Reject reqSeqNum="
                 << reqSeqNum << " clientId=" << clientId_ << " cid=" << cid << " timeout=" << timeoutMilli);
    reset();
    return NOT_READY;
  }

  ConcordAssert(replysCertificate_.empty());
  ConcordAssert(msgQueue_.empty());
  ConcordAssert(pendingRequest_.empty());
  ConcordAssert(timeOfLastTransmission_ == MinTime);
  ConcordAssert(numberOfTransmissions_ == 0);

  static const std::chrono::milliseconds timersRes(timersResolutionMilli_);

  const Time beginTime = getMonotonicTime();

  ClientRequestMsg* reqMsg;
  concordUtils::SpanContext ctx{span_context};
  reqMsg = new ClientRequestMsg(clientId_, flags, reqSeqNum, lengthOfRequest, request, timeoutMilli, msgCid, ctx);
  pendingRequest_.push_back(reqMsg);

  sendPendingRequest();

  // collect metrics and update them
  client_metrics_.retransmissionTimer.Get().Set(limitOfExpectedOperationTime_.upperLimit());
  metrics_.UpdateAggregator();

  // protect against spurious wake-ups
  auto predicate = [this] { return !msgQueue_.empty(); };
  bool requestCommitted = false;
  bool requestTimeout = false;
  while (true) {
    std::queue<MessageBase*> newMsgs;
    bool hasData = false;
    {
      std::unique_lock<std::mutex> mlock(lock_);
      hasData = condVar_.wait_for(mlock, timersRes, predicate);
      if (hasData) msgQueue_.swap(newMsgs);
    }

    if (hasData) {
      while (!newMsgs.empty()) {
        MessageBase* msg = newMsgs.front();
        onMessageFromReplica(msg);
      }
      newMsgs.pop();
    }

    if (allRequiredRepliesReceived()) requestCommitted = true;
    const Time currTime = getMonotonicTime();
    // If client defined timeout for the request expired?
    if (timeoutMilli != INFINITE_TIMEOUT &&
        (uint64_t)duration_cast<milliseconds>(currTime - beginTime).count() > timeoutMilli) {
      requestTimeout = true;
      break;
    }

    if ((uint64_t)duration_cast<milliseconds>(currTime - timeOfLastTransmission_).count() >
        limitOfExpectedOperationTime_.upperLimit()) {
      onRetransmission();
    }
  }

  if (requestCommitted) {
    uint64_t durationMilli = duration_cast<milliseconds>(getMonotonicTime() - beginTime).count();
    limitOfExpectedOperationTime_.add(durationMilli);

    LOG_DEBUG(logger_,
              "Client " << clientId_ << " - request " << reqSeqNum << " has committed (isRO=" << isReadOnly
                        << ", isPreProcess=" << isPreProcessRequired
                        << ", retransmissionMilli=" << (int)limitOfExpectedOperationTime_.upperLimit() << ") ");

    const auto& elem = replysCertificate_.find(reqSeqNum);
    ClientReplyMsg* correctReply = elem->second.bestCorrectMsg();
    primaryReplicaIsKnown_ = true;
    knownPrimaryReplica_ = correctReply->currentPrimaryId();

    if (correctReply->replyLength() <= lengthOfReplyBuffer) {
      memcpy(replyBuffer, correctReply->replyBuf(), correctReply->replyLength());
      actualReplyLength = correctReply->replyLength();
      reset();
      return SUCCESS;
    } else {
      reset();
      return BUFFER_TOO_SMALL;
    }
  } else if (requestTimeout) {
    LOG_DEBUG(logger_, "Client " << clientId_ << " request :" << reqSeqNum << " timeout");

    if (timeoutMilli >= limitOfExpectedOperationTime_.upperLimit()) {
      LOG_DEBUG(logger_, "Client " << clientId_ << " request :" << reqSeqNum << ", primary is set to UNKNOWN");
      primaryReplicaIsKnown_ = false;
      limitOfExpectedOperationTime_.add(timeoutMilli);
    }

    reset();
    return TIMEOUT;
  }

  ConcordAssert(false);
  return SUCCESS;
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
  } else if (!isSystemReady()) {
    LOG_WARN(logger_,
             "The system is not ready yet to handle requests => reject" << KVLOG(clientId_, req.reqSeqNum, req.cid));
    res = NOT_READY;
  }
  if (res != SUCCESS) reset();
  return res;
}

OperationResult SimpleClientImp::isBatchValid(uint64_t requestsSize, uint64_t repliesSize) {
  OperationResult res = SUCCESS;
  if (!requestsSize) {
    LOG_ERROR(logger_, "An empty requests list specified");
    res = INVALID_REQUEST;
  } else if (requestsSize != repliesSize) {
    LOG_ERROR(logger_,
              "The number of requests is not equal to the number of replies" << KVLOG(requestsSize, repliesSize));
    res = INVALID_REQUEST;
  }
  if (res != SUCCESS) reset();
  return res;
}

OperationResult SimpleClientImp::sendBatch(const std::deque<ClientRequest>& clientRequests,
                                           std::deque<ClientReply>& clientReplies) {
  OperationResult res = isBatchValid(clientRequests.size(), clientReplies.size());
  if (res != SUCCESS) return res;
  if (!communication_->isRunning()) communication_->Start();
  bool isPreProcessRequired = false;
  string cid;
  uint64_t maxTimeToWait = 0;
  ClientRequestMsg* reqMsg = nullptr;
  for (auto& req : clientRequests) {
    res = isBatchRequestValid(req);
    if (res != SUCCESS) return res;
    cid = req.cid.empty() ? to_string(req.reqSeqNum) + "-" + to_string(clientId_) : req.cid;
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
                    isPreProcessRequired,
                    req.lengthOfRequest,
                    req.timeoutMilli,
                    limitOfExpectedOperationTime_.upperLimit(),
                    req.span_context.empty()));
    concordUtils::SpanContext ctx{req.span_context};
    reqMsg = new ClientRequestMsg(
        clientId_, req.flags, req.reqSeqNum, req.lengthOfRequest, req.request, req.timeoutMilli, cid, ctx);
    pendingRequest_.push_back(reqMsg);
  }

  ConcordAssert(replysCertificate_.empty());
  ConcordAssert(msgQueue_.empty());
  ConcordAssert(pendingRequest_.empty());
  ConcordAssert(timeOfLastTransmission_ == MinTime);
  ConcordAssert(numberOfTransmissions_ == 0);

  static const std::chrono::milliseconds timersRes(timersResolutionMilli_);
  const Time beginTime = getMonotonicTime();
  sendPendingRequest();

  // collect metrics and update them
  client_metrics_.retransmissionTimer.Get().Set(limitOfExpectedOperationTime_.upperLimit());
  metrics_.UpdateAggregator();

  // protect against spurious wake-ups
  auto predicate = [this] { return !msgQueue_.empty(); };
  bool requestTimeout = false;
  bool requestCommitted = false;
  while (true) {
    std::queue<MessageBase*> newMsgs;
    bool hasData = false;
    {
      std::unique_lock<std::mutex> mlock(lock_);
      hasData = condVar_.wait_for(mlock, timersRes, predicate);
      if (hasData) msgQueue_.swap(newMsgs);
    }
    if (hasData) {
      while (!newMsgs.empty()) {
        MessageBase* msg = newMsgs.front();
        onMessageFromReplica(msg);
      }
      newMsgs.pop();
    }
    if (allRequiredRepliesReceived()) requestCommitted = true;
    const Time currTime = getMonotonicTime();
    // If client-defined timeout for the request expired?
    if (maxTimeToWait != INFINITE_TIMEOUT &&
        (uint64_t)duration_cast<milliseconds>(currTime - beginTime).count() > maxTimeToWait) {
      requestTimeout = true;
      break;
    }
    if ((uint64_t)duration_cast<milliseconds>(currTime - timeOfLastTransmission_).count() >
        limitOfExpectedOperationTime_.upperLimit()) {
      onRetransmission();
    }
  }
  if (requestCommitted) {
    uint64_t durationMilli = duration_cast<milliseconds>(getMonotonicTime() - beginTime).count();
    limitOfExpectedOperationTime_.add(durationMilli);

    for (auto& reply : replysCertificate_) {
      ClientReplyMsg* correctReply = reply.second.bestCorrectMsg();
      const auto reqSeqNum = correctReply->reqSeqNum();
      LOG_DEBUG(logger_, KVLOG(clientId_, reqSeqNum, isPreProcessRequired) << " has committed");

      primaryReplicaIsKnown_ = true;
      knownPrimaryReplica_ = correctReply->currentPrimaryId();

      if (correctReply->replyLength() <= clientReplies[reqSeqNum].lengthOfReplyBuffer) {
        memcpy(clientReplies[reqSeqNum].replyBuffer, correctReply->replyBuf(), correctReply->replyLength());
        clientReplies[reqSeqNum].actualReplyLength = correctReply->replyLength();
      } else {
        reset();
        return BUFFER_TOO_SMALL;
      }
    }
    reset();
    return SUCCESS;
  } else if (requestTimeout) {
    LOG_DEBUG(logger_, "Batch timed out" << KVLOG(clientId_, pendingRequest_[0]->requestSeqNum(), maxTimeToWait));

    if (maxTimeToWait >= limitOfExpectedOperationTime_.upperLimit()) {
      LOG_DEBUG(logger_, KVLOG(clientId_, pendingRequest_[0]->requestSeqNum()) << " primary is set to UNKNOWN");
      primaryReplicaIsKnown_ = false;
      limitOfExpectedOperationTime_.add(maxTimeToWait);
    }
    reset();
    return TIMEOUT;
  }

  ConcordAssert(false);
  return SUCCESS;
}

void SimpleClientImp::reset() {
  for (auto& elem : replysCertificate_) elem.second.resetAndFree();
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
    for (uint16_t r : replicas_) communication_->sendAsyncMessage(r, data, size);
  } else
    communication_->sendAsyncMessage(knownPrimaryReplica_, data, size);
}

void SimpleClientImp::sendPendingRequest() {
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

  if (numberOfTransmissions_ && !(numberOfTransmissions_ % 10))
    LOG_DEBUG(logger_,
              "Client " << clientId_ << " sends request " << pendingRequest_.front()->requestSeqNum()
                        << " isRO=" << readOnly << ", request size=" << (size_t)pendingRequest_.front()->size()
                        << ", retransmissionMilli=" << (int)limitOfExpectedOperationTime_.upperLimit()
                        << ", numberOfTransmissions=" << numberOfTransmissions_ << ", resetReplies=" << resetReplies
                        << ", sendToAll=" << sendToAll);

  if (resetReplies) {
    for (auto& elem : replysCertificate_) elem.second.resetAndFree();
    replysCertificate_.clear();
  }

  if (pendingRequest_.size() == 1)
    return sendRequestToAllOrToPrimary(sendToAll, pendingRequest_[0]->body(), pendingRequest_[0]->size());

  uint32_t batchBufSize = 0;
  for (auto const& msg : pendingRequest_) batchBufSize += msg->size();
  ClientBatchRequestMsg* batchMsg = new ClientBatchRequestMsg(clientId_, pendingRequest_, batchBufSize);
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

SimpleClient* SimpleClient::createSimpleClient(
    ICommunication* communication, uint16_t clientId, uint16_t fVal, uint16_t cVal, SimpleClientParams p) {
  return new impl::SimpleClientImp(communication, clientId, fVal, cVal, p);
}

SimpleClient* SimpleClient::createSimpleClient(ICommunication* communication,
                                               uint16_t clientId,
                                               uint16_t fVal,
                                               uint16_t cVal) {
  SimpleClientParams p;
  return SimpleClient::createSimpleClient(communication, clientId, fVal, cVal, p);
}

SimpleClient::~SimpleClient() = default;

std::unique_ptr<SeqNumberGeneratorForClientRequests>
SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests() {
  return std::make_unique<impl::SeqNumberGeneratorForClientRequestsImp>();
}

}  // namespace bftEngine
