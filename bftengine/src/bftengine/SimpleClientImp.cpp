// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
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

#include "ClientMsgs.hpp"
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

namespace bftEngine {
namespace impl {
class SimpleClientImp : public SimpleClient, public IReceiver {
 public:
  SimpleClientImp(
      ICommunication* communication, uint16_t clientId, uint16_t fVal, uint16_t cVal, SimpleClientParams& p);
  ~SimpleClientImp() override;

  int sendRequest(uint8_t flags,
                  const char* request,
                  uint32_t lengthOfRequest,
                  uint64_t reqSeqNum,
                  uint64_t timeoutMilli,
                  uint32_t lengthOfReplyBuffer,
                  char* replyBuffer,
                  uint32_t& actualReplyLength) override;

  int sendRequestToResetSeqNum() override;
  int sendRequestToReadLatestSeqNum(uint64_t timeoutMilli, uint64_t& outLatestReqSeqNum) override;

  // IReceiver methods
  void onNewMessage(const NodeNum sourceNode, const char* const message, const size_t messageLength) override;
  void onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) override;

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
  void sendPendingRequest();
  void onMessageFromReplica(MessageBase* msg);
  void onRetransmission();
  void reset();

 protected:
  static const uint32_t maxLegalMsgSize_ = 64 * 1024;  // TODO(GG): ???
  static const uint16_t timersResolutionMilli_ = 50;

  const uint16_t clientId_;
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
};

void SimpleClientImp::onMessageFromReplica(MessageBase* msg) {
  ClientReplyMsg* replyMsg = static_cast<ClientReplyMsg*>(msg);
  replyMsg->validate(ReplicasInfo());
  Assert(replyMsg != nullptr);
  Assert(replyMsg->type() == REPLY_MSG_TYPE);

  LOG_DEBUG_F(GL,
              "Client %d received ClientReplyMsg with seqNum=%" PRIu64 " sender=%d  size=%d  primaryId=%d hash=%" PRIu64
              "",
              clientId_,
              replyMsg->reqSeqNum(),
              replyMsg->senderId(),
              replyMsg->size(),
              (int)replyMsg->currentPrimaryId(),
              replyMsg->debugHash());

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

void SimpleClientImp::onRetransmission() { sendPendingRequest(); }

// in this version we assume that the set of replicas is 0,1,2,...,numberOfReplicas (TODO(GG): should be changed to
// support full dynamic reconfiguration)
static std::set<ReplicaId> generateSetOfReplicas_helpFunc(const int16_t numberOfReplicas) {
  std::set<ReplicaId> retVal;
  for (int16_t i = 0; i < numberOfReplicas; i++) retVal.insert(i);
  return retVal;
}

SimpleClientImp::SimpleClientImp(
    ICommunication* communication, uint16_t clientId, uint16_t fVal, uint16_t cVal, SimpleClientParams& p)
    : clientId_{clientId},
      fVal_{fVal},
      cVal_{cVal},
      replicas_{generateSetOfReplicas_helpFunc(3 * fVal + 2 * cVal + 1)},
      communication_{communication},
      replysCertificate_(3 * fVal + 2 * cVal + 1, fVal, 2 * fVal + cVal + 1, clientId),
      limitOfExpectedOperationTime_(p.clientInitialRetryTimeoutMilli,
                                    2,
                                    p.clientMaxRetryTimeoutMilli,
                                    p.clientMinRetryTimeoutMilli,
                                    32,
                                    1000,
                                    2,
                                    2),
      clientSendsRequestToAllReplicasFirstThresh_{p.clientSendsRequestToAllReplicasFirstThresh},
      clientSendsRequestToAllReplicasPeriodThresh_{p.clientSendsRequestToAllReplicasPeriodThresh},
      clientPeriodicResetThresh_{p.clientPeriodicResetThresh} {
  Assert(fVal_ >= 1);

  pendingRequest_ = nullptr;
  timeOfLastTransmission_ = MinTime;
  numberOfTransmissions_ = 0;
  primaryReplicaIsKnown_ = false;
  knownPrimaryReplica_ = 0;

  communication_->setReceiver(clientId_, this);
}

SimpleClientImp::~SimpleClientImp() {
  Assert(replysCertificate_.isEmpty());
  Assert(msgQueue_.empty());
  Assert(pendingRequest_ == nullptr);
  Assert(timeOfLastTransmission_ == MinTime);
  Assert(numberOfTransmissions_ == 0);
}

int SimpleClientImp::sendRequest(uint8_t flags,
                                 const char* request,
                                 uint32_t lengthOfRequest,
                                 uint64_t reqSeqNum,
                                 uint64_t timeoutMilli,
                                 uint32_t lengthOfReplyBuffer,
                                 char* replyBuffer,
                                 uint32_t& actualReplyLength) {
  bool isReadOnly = flags & READ_ONLY_REQ;
  bool isPreProcessRequired = flags & PRE_PROCESS_REQ;

  // TODO(GG): check params ...
  LOG_DEBUG(GL,
            "Client " << clientId_ << " - sends request " << reqSeqNum << " (isRO=" << isReadOnly
                      << ", isPreProcess=" << isPreProcessRequired << " , request size=" << lengthOfRequest
                      << ", retransmissionMilli=" << limitOfExpectedOperationTime_.upperLimit() << " ) ");

  if (!communication_->isRunning()) {
    communication_->Start();  // TODO(GG): patch ................ change
  }

  Assert(replysCertificate_.isEmpty());
  Assert(msgQueue_.empty());
  Assert(pendingRequest_ == nullptr);
  Assert(timeOfLastTransmission_ == MinTime);
  Assert(numberOfTransmissions_ == 0);

  static const std::chrono::milliseconds timersRes(timersResolutionMilli_);

  const Time beginTime = getMonotonicTime();

  ClientRequestMsg* reqMsg;
  if (isPreProcessRequired)
    reqMsg = new preprocessor::ClientPreProcessRequestMsg(clientId_, isReadOnly, reqSeqNum, lengthOfRequest, request);
  else
    reqMsg = new ClientRequestMsg(clientId_, isReadOnly, reqSeqNum, lengthOfRequest, request);
  pendingRequest_ = reqMsg;

  sendPendingRequest();

  bool requestTimeout = false;
  bool requestCommitted = false;

  while (true) {
    std::queue<MessageBase*> newMsgs;
    {
      std::unique_lock<std::mutex> mlock(lock_);
      condVar_.wait_for(mlock, timersRes);
      msgQueue_.swap(newMsgs);
    }

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

    const Time currTime = getMonotonicTime();

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
    Assert(replysCertificate_.isComplete());

    uint64_t durationMilli = duration_cast<milliseconds>(getMonotonicTime() - beginTime).count();
    limitOfExpectedOperationTime_.add(durationMilli);

    LOG_DEBUG_F(GL,
                "Client %d - request %" PRIu64
                " has committed "
                "(isRO=%d, isPreProcess=%d, request size=%zu,  retransmissionMilli=%d) ",
                clientId_,
                reqSeqNum,
                (int)isReadOnly,
                isPreProcessRequired,
                (size_t)lengthOfRequest,
                (int)limitOfExpectedOperationTime_.upperLimit());

    ClientReplyMsg* correctReply = replysCertificate_.bestCorrectMsg();

    primaryReplicaIsKnown_ = true;
    knownPrimaryReplica_ = correctReply->currentPrimaryId();

    if (correctReply->replyLength() <= lengthOfReplyBuffer) {
      memcpy(replyBuffer, correctReply->replyBuf(), correctReply->replyLength());
      actualReplyLength = correctReply->replyLength();
      reset();
      return 0;
    } else {
      reset();
      return (-2);
    }
  } else if (requestTimeout) {
    // Logger::printInfo("Client %d - request %" PRIu64 " - timeout");

    if (timeoutMilli >= limitOfExpectedOperationTime_.upperLimit()) {
      primaryReplicaIsKnown_ = false;
      limitOfExpectedOperationTime_.add(timeoutMilli);
    }

    reset();
    return (-1);
  }

  Assert(false);
  return 0;
}

int SimpleClientImp::sendRequestToResetSeqNum() {
  Assert(false);  // not implemented yet
  return 0;
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

int SimpleClientImp::sendRequestToReadLatestSeqNum(uint64_t timeoutMilli, uint64_t& outLatestReqSeqNum) {
  Assert(false);  // not implemented yet
  return 0;
}

void SimpleClientImp::onNewMessage(const NodeNum sourceNode, const char* const message, const size_t messageLength) {
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
    condVar_.notify_one();
  }
}

void SimpleClientImp::onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) {}

void SimpleClientImp::sendPendingRequest() {
  Assert(pendingRequest_ != nullptr);

  timeOfLastTransmission_ = getMonotonicTime();
  numberOfTransmissions_++;

  const bool resetReplies = (numberOfTransmissions_ % clientPeriodicResetThresh_ == 0);

  const bool sendToAll = pendingRequest_->isReadOnly() || !primaryReplicaIsKnown_ ||
                         (numberOfTransmissions_ == clientSendsRequestToAllReplicasFirstThresh_) ||
                         (numberOfTransmissions_ > clientSendsRequestToAllReplicasFirstThresh_ &&
                          (numberOfTransmissions_ % clientSendsRequestToAllReplicasPeriodThresh_ == 0)) ||
                         resetReplies;

  if (numberOfTransmissions_ && !(numberOfTransmissions_ % 10))
    LOG_DEBUG_F(GL,
                "Client %d - sends request %" PRIu64
                " isRO=%d, request size=%zu, "
                "retransmissionMilli=%d, numberOfTransmissions=%d, resetReplies=%d, sendToAll=%d",
                clientId_,
                pendingRequest_->requestSeqNum(),
                (int)pendingRequest_->isReadOnly(),
                (size_t)pendingRequest_->size(),
                (int)limitOfExpectedOperationTime_.upperLimit(),
                (int)numberOfTransmissions_,
                (int)resetReplies,
                (int)sendToAll);

  if (resetReplies) {
    replysCertificate_.resetAndFree();
    // TODO(GG): print ....
  }

  if (sendToAll) {
    for (uint16_t r : replicas_) {
      // int stat =
      communication_->sendAsyncMessage(r, pendingRequest_->body(), pendingRequest_->size());
      // TODO(GG): handle errors (print and/or ....)
    }
  } else {
    // int stat =
    communication_->sendAsyncMessage(knownPrimaryReplica_, pendingRequest_->body(), pendingRequest_->size());
    // TODO(GG): handle errors (print and/or ....)
  }
}

class SeqNumberGeneratorForClientRequestsImp : public SeqNumberGeneratorForClientRequests {
  virtual uint64_t generateUniqueSequenceNumberForRequest() override;

 protected:
  uint64_t lastMilliOfUniqueFetchID_ = 0;
  uint32_t lastCountOfUniqueFetchID_ = 0;
};

uint64_t SeqNumberGeneratorForClientRequestsImp::generateUniqueSequenceNumberForRequest() {
  std::chrono::time_point<std::chrono::system_clock> n = std::chrono::system_clock::now();

  uint64_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(n.time_since_epoch()).count();

  if (milli > lastMilliOfUniqueFetchID_) {
    lastMilliOfUniqueFetchID_ = milli;
    lastCountOfUniqueFetchID_ = 0;
  } else {
    if (lastCountOfUniqueFetchID_ == 0x3FFFFF) {
      LOG_WARN(GL, "Client SeqNum Counter reached max value");
      lastMilliOfUniqueFetchID_++;
      lastCountOfUniqueFetchID_ = 0;
    } else {
      lastCountOfUniqueFetchID_++;
    }
  }

  uint64_t r = (lastMilliOfUniqueFetchID_ << (64 - 42));
  Assert(lastCountOfUniqueFetchID_ <= 0x3FFFFF);
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

SeqNumberGeneratorForClientRequests* SeqNumberGeneratorForClientRequests::createSeqNumberGeneratorForClientRequests() {
  return new impl::SeqNumberGeneratorForClientRequestsImp();
}

}  // namespace bftEngine
