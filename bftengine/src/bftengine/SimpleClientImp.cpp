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
#include "messages/MessageBase.hpp"
#include "messages/ClientRequestMsg.hpp"
#include "messages/ClientReplyMsg.hpp"
#include "messages/ClientPreProcessRequestMsg.hpp"
#include "messages/MsgsCertificate.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"
#include "Logger.hpp"

using namespace std::chrono;
using namespace bft::communication;

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
                              const std::string& span_context) override;

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

  MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate_;

  std::mutex lock_;  // protects _msgQueue and pendingRequest
  std::condition_variable condVar_;

  std::queue<MessageBase*> msgQueue_;
  ClientRequestMsg* pendingRequest_ = nullptr;

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

  LOG_DEBUG(logger_,
            "Client " << clientId_ << " received ClientReplyMsg with seqNum=" << replyMsg->reqSeqNum()
                      << " sender=" << replyMsg->senderId() << "size=" << replyMsg->size()
                      << " primaryId=" << (int)replyMsg->currentPrimaryId() << " hash=" << replyMsg->debugHash());

  if (replyMsg->reqSeqNum() != pendingRequest_->requestSeqNum()) {
    delete msg;
    return;
  }

  replysCertificate_.addMsg(replyMsg, replyMsg->senderId());

  if (replysCertificate_.isInconsistent()) {
    // TODO(GG): print .....
    replysCertificate_.resetAndFree();
  }
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
      replysCertificate_(numberOfReplicas_, fVal, numberOfRequiredReplicas_, clientId),
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

  pendingRequest_ = nullptr;
  timeOfLastTransmission_ = MinTime;
  numberOfTransmissions_ = 0;
  primaryReplicaIsKnown_ = false;
  knownPrimaryReplica_ = 0;

  communication_->setReceiver(clientId_, this);
}

SimpleClientImp::~SimpleClientImp() {
  ConcordAssert(replysCertificate_.isEmpty());
  ConcordAssert(msgQueue_.empty());
  ConcordAssert(pendingRequest_ == nullptr);
  ConcordAssert(timeOfLastTransmission_ == MinTime);
  ConcordAssert(numberOfTransmissions_ == 0);
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
  const std::string msgCid = cid.empty() ? std::to_string(reqSeqNum) + "-" + std::to_string(clientId_) : cid;
  // TODO(GG): check params ...
  LOG_DEBUG(logger_,
            "Client " << clientId_ << " - sends request " << reqSeqNum << " (isRO=" << isReadOnly
                      << ", isPreProcess=" << isPreProcessRequired << " , request size=" << lengthOfRequest
                      << ", retransmissionMilli=" << limitOfExpectedOperationTime_.upperLimit()
                      << ", timeout=" << timeoutMilli << ", has span context=" << !span_context.empty());
  ConcordAssert(!(isReadOnly && isPreProcessRequired));

  if (!communication_->isRunning()) {
    communication_->Start();  // TODO(GG): patch ................ change
  }

  if (!isReadOnly && !isSystemReady()) {
    LOG_WARN(logger_,
             "The system is not ready yet to handle requests. Reject reqSeqNum="
                 << reqSeqNum << " clientId=" << clientId_ << " cid=" << cid << " timeout=" << timeoutMilli);
    reset();
    return NOT_READY;
  }

  ConcordAssert(replysCertificate_.isEmpty());
  ConcordAssert(msgQueue_.empty());
  ConcordAssert(pendingRequest_ == nullptr);
  ConcordAssert(timeOfLastTransmission_ == MinTime);
  ConcordAssert(numberOfTransmissions_ == 0);

  static const std::chrono::milliseconds timersRes(timersResolutionMilli_);

  const Time beginTime = getMonotonicTime();

  ClientRequestMsg* reqMsg;
  concordUtils::SpanContext ctx{span_context};
  if (isPreProcessRequired)
    reqMsg = new preprocessor::ClientPreProcessRequestMsg(
        clientId_, reqSeqNum, lengthOfRequest, request, timeoutMilli, msgCid, ctx);
  else
    reqMsg = new ClientRequestMsg(clientId_, flags, reqSeqNum, lengthOfRequest, request, timeoutMilli, msgCid, ctx);
  pendingRequest_ = reqMsg;

  sendPendingRequest();

  bool requestTimeout = false;
  bool requestCommitted = false;

  // collect metrics and update them
  client_metrics_.retransmissionTimer.Get().Set(limitOfExpectedOperationTime_.upperLimit());
  metrics_.UpdateAggregator();

  // protect against spurious wakeups
  auto predicate = [this] { return !msgQueue_.empty(); };
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
        if (replysCertificate_.isComplete()) {
          delete newMsgs.front();
        } else {
          MessageBase* msg = newMsgs.front();
          onMessageFromReplica(msg);
        }
        newMsgs.pop();
      }

      if (replysCertificate_.isComplete()) {
        requestCommitted = true;
        break;
      }
    }

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
    ConcordAssert(replysCertificate_.isComplete());

    uint64_t durationMilli = duration_cast<milliseconds>(getMonotonicTime() - beginTime).count();
    limitOfExpectedOperationTime_.add(durationMilli);

    LOG_DEBUG(logger_,
              "Client " << clientId_ << " - request " << reqSeqNum
                        << " has committed "
                           "(isRO="
                        << isReadOnly << ", isPreProcess=" << isPreProcessRequired
                        << ", request size=" << lengthOfRequest
                        << ",  retransmissionMilli=" << (int)limitOfExpectedOperationTime_.upperLimit() << ") ");

    ClientReplyMsg* correctReply = replysCertificate_.bestCorrectMsg();

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

void SimpleClientImp::reset() {
  replysCertificate_.resetAndFree();

  std::queue<MessageBase*> newMsgs;
  {
    std::unique_lock<std::mutex> mlock(lock_);
    msgQueue_.swap(newMsgs);

    delete pendingRequest_;
    pendingRequest_ = nullptr;
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
    if (pendingRequest_ == nullptr) return;

    // create msg object
    MessageBase::Header* msgBody = (MessageBase::Header*)std::malloc(messageLength);
    memcpy(msgBody, message, messageLength);
    MessageBase* pMsg = new MessageBase(senderId, msgBody, messageLength, true);

    msgQueue_.push(pMsg);  // TODO(GG): handle overflow
  }
  // no need to notify within the lock
  condVar_.notify_one();
}

void SimpleClientImp::onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) {}

void SimpleClientImp::sendPendingRequest() {
  ConcordAssert(pendingRequest_ != nullptr);

  timeOfLastTransmission_ = getMonotonicTime();
  numberOfTransmissions_++;

  const bool resetReplies = (numberOfTransmissions_ % clientPeriodicResetThresh_ == 0);

  const bool sendToAll = pendingRequest_->isReadOnly() || !primaryReplicaIsKnown_ ||
                         (numberOfTransmissions_ == clientSendsRequestToAllReplicasFirstThresh_) ||
                         (numberOfTransmissions_ > clientSendsRequestToAllReplicasFirstThresh_ &&
                          (numberOfTransmissions_ % clientSendsRequestToAllReplicasPeriodThresh_ == 0)) ||
                         resetReplies;

  if (numberOfTransmissions_ && !(numberOfTransmissions_ % 10))
    LOG_DEBUG(logger_,
              "Client " << clientId_ << " sends request " << pendingRequest_->requestSeqNum() << " isRO="
                        << pendingRequest_->isReadOnly() << ", request size=" << (size_t)pendingRequest_->size()
                        << ", retransmissionMilli=" << (int)limitOfExpectedOperationTime_.upperLimit()
                        << ", numberOfTransmissions=" << numberOfTransmissions_ << ", resetReplies=" << resetReplies
                        << ", sendToAll=" << sendToAll);

  if (resetReplies) {
    replysCertificate_.resetAndFree();
    // TODO(GG): print ....
  }

  std::vector<uint8_t> msg(pendingRequest_->body(), pendingRequest_->body() + pendingRequest_->size());
  if (sendToAll) {
    communication_->multiSendMessage(std::set<NodeNum>(replicas_.begin(), replicas_.end()), std::move(msg));
  } else {
    pm_->Delay<concord::performance::SlowdownPhase::BftClientBeforeSendPrimary>();
    communication_->sendAsyncMessage(knownPrimaryReplica_, std::move(msg));
    // TODO(GG): handle errors (print and/or ....)
  }
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
