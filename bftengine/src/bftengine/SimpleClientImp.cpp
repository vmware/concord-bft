// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
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
#include "messages/MsgsCertificate.hpp"
#include "DynamicUpperLimitWithSimpleFilter.hpp"
#include "Logger.hpp"

namespace bftEngine {
namespace impl {
class SimpleClientImp : public SimpleClient, public IReceiver {
 public:
  SimpleClientImp(
      ICommunication* communication, uint16_t clientId, uint16_t fVal, uint16_t cVal, SimpleClientParams& p);
  ~SimpleClientImp() override;

  int sendRequest(bool isReadOnly,
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
  static const uint32_t _maxLegalMsgSize = 64 * 1024;  // TODO(GG): ???
  static const uint16_t _timersResolutionMilli = 50;

  const uint16_t _clientId;
  const uint16_t _fVal;
  const uint16_t _cVal;
  const std::set<uint16_t> _replicas;
  ICommunication* const _communication;

  MsgsCertificate<ClientReplyMsg, false, false, true, SimpleClientImp> replysCertificate;

  std::mutex _lock;  // protects _msgQueue and pendingRequest
  std::condition_variable _condVar;

  queue<MessageBase*> _msgQueue;
  ClientRequestMsg* _pendingRequest = nullptr;

  Time _timeOfLastTransmission = MinTime;
  uint16_t _numberOfTransmissions = 0;

  bool _primaryReplicaIsKnown = false;
  uint16_t _knownPrimaryReplica;

  DynamicUpperLimitWithSimpleFilter<uint64_t> _limitOfExpectedOperationTime;

  // configuration params
  uint16_t _clientSendsRequestToAllReplicasFirstThresh;
  uint16_t _clientSendsRequestToAllReplicasPeriodThresh;
  uint16_t _clientPeriodicResetThresh;
};

void SimpleClientImp::onMessageFromReplica(MessageBase* msg) {
  ClientReplyMsg* replyMsg = nullptr;
  if (!ClientReplyMsg::ToActualMsgType(_clientId, msg, replyMsg)) {
    delete msg;
    return;
  }
  Assert(replyMsg != nullptr);
  Assert(replyMsg->type() == REPLY_MSG_TYPE);

  LOG_DEBUG_F(GL,
              "Client %d received ClientReplyMsg with seqNum=%" PRIu64 " sender=%d  size=%d  primaryId=%d hash=%" PRIu64
              "",
              _clientId,
              replyMsg->reqSeqNum(),
              replyMsg->senderId(),
              replyMsg->size(),
              (int)replyMsg->currentPrimaryId(),
              replyMsg->debugHash());

  if (replyMsg->reqSeqNum() != _pendingRequest->requestSeqNum()) {
    delete msg;
    return;
  }

  replysCertificate.addMsg(replyMsg, replyMsg->senderId());

  if (replysCertificate.isInconsistent()) {
    // TODO(GG): print .....
    replysCertificate.resetAndFree();
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
    : _clientId{clientId},
      _fVal{fVal},
      _cVal{cVal},
      _replicas{generateSetOfReplicas_helpFunc(3 * fVal + 2 * cVal + 1)},
      _communication{communication},
      replysCertificate(3 * fVal + 2 * cVal + 1, fVal, 2 * fVal + cVal + 1, clientId),
      _limitOfExpectedOperationTime(p.clientInitialRetryTimeoutMilli, 2, p.clientMaxRetryTimeoutMilli,
                                    p.clientMinRetryTimeoutMilli, 32, 1000, 2, 2),
      _clientSendsRequestToAllReplicasFirstThresh{p.clientSendsRequestToAllReplicasFirstThresh},
      _clientSendsRequestToAllReplicasPeriodThresh{p.clientSendsRequestToAllReplicasPeriodThresh},
      _clientPeriodicResetThresh{p.clientPeriodicResetThresh} {
  Assert(_fVal >= 1);

  _pendingRequest = nullptr;
  _timeOfLastTransmission = MinTime;
  _numberOfTransmissions = 0;
  _primaryReplicaIsKnown = false;
  _knownPrimaryReplica = 0;

  _communication->setReceiver(_clientId, this);
}

SimpleClientImp::~SimpleClientImp() {
  Assert(replysCertificate.isEmpty());
  Assert(_msgQueue.empty());
  Assert(_pendingRequest == nullptr);
  Assert(_timeOfLastTransmission == MinTime);
  Assert(_numberOfTransmissions == 0);
}

int SimpleClientImp::sendRequest(bool isReadOnly,
                                 const char* request,
                                 uint32_t lengthOfRequest,
                                 uint64_t reqSeqNum,
                                 uint64_t timeoutMilli,
                                 uint32_t lengthOfReplyBuffer,
                                 char* replyBuffer,
                                 uint32_t& actualReplyLength) {
  // TODO(GG): check params ...
  LOG_DEBUG(GL,
            "Client " << _clientId << " - sends request " << reqSeqNum << " (isRO=" << isReadOnly
                      << " , request size=" << lengthOfRequest
                      << ", retransmissionMilli=" << _limitOfExpectedOperationTime.upperLimit() << " ) ");

  if (!_communication->isRunning()) {
    _communication->Start();  // TODO(GG): patch ................ change
  }

  Assert(replysCertificate.isEmpty());
  Assert(_msgQueue.empty());
  Assert(_pendingRequest == nullptr);
  Assert(_timeOfLastTransmission == MinTime);
  Assert(_numberOfTransmissions == 0);

  static const std::chrono::milliseconds timersRes(_timersResolutionMilli);

  const Time beginTime = getMonotonicTime();

  ClientRequestMsg* reqMsg = new ClientRequestMsg(_clientId, isReadOnly, reqSeqNum, lengthOfRequest, request);
  _pendingRequest = reqMsg;

  sendPendingRequest();

  bool requestTimeout = false;
  bool requestCommitted = false;

  while (true) {
    queue<MessageBase*> newMsgs;
    {
      std::unique_lock<std::mutex> mlock(_lock);
      _condVar.wait_for(mlock, timersRes);
      _msgQueue.swap(newMsgs);
    }

    while (!newMsgs.empty()) {
      if (replysCertificate.isComplete()) {
        delete newMsgs.front();
      } else {
        MessageBase* msg = newMsgs.front();
        onMessageFromReplica(msg);
      }
      newMsgs.pop();
    }

    if (replysCertificate.isComplete()) {
      requestCommitted = true;
      break;
    }

    const Time currTime = getMonotonicTime();

    // absDifference returns microseconds, so scale up timeoutMilli to match
    if (timeoutMilli != INFINITE_TIMEOUT && (uint64_t)absDifference(beginTime, currTime) > timeoutMilli * 1000) {
      requestTimeout = true;
      break;
    }

    if (((uint64_t)absDifference(_timeOfLastTransmission, currTime)) / 1000 >
        _limitOfExpectedOperationTime.upperLimit()) {
      onRetransmission();
    }
  }

  if (requestCommitted) {
    Assert(replysCertificate.isComplete());

    uint64_t durationMilli = ((uint64_t)absDifference(getMonotonicTime(), beginTime)) / 1000;
    _limitOfExpectedOperationTime.add(durationMilli);

    LOG_DEBUG_F(GL,
                "Client %d - request %" PRIu64
                " has committed "
                "(isRO=%d, request size=%zu,  retransmissionMilli=%d) ",
                _clientId,
                reqSeqNum,
                (int)isReadOnly,
                (size_t)lengthOfRequest,
                (int)_limitOfExpectedOperationTime.upperLimit());

    ClientReplyMsg* correctReply = replysCertificate.bestCorrectMsg();

    _primaryReplicaIsKnown = true;
    _knownPrimaryReplica = correctReply->currentPrimaryId();

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

    if (timeoutMilli >= _limitOfExpectedOperationTime.upperLimit()) {
      _primaryReplicaIsKnown = false;
      _limitOfExpectedOperationTime.add(timeoutMilli);
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
  replysCertificate.resetAndFree();

  queue<MessageBase*> newMsgs;
  {
    std::unique_lock<std::mutex> mlock(_lock);
    _msgQueue.swap(newMsgs);

    delete _pendingRequest;
    _pendingRequest = nullptr;
  }

  while (!newMsgs.empty()) {
    delete newMsgs.front();
    newMsgs.pop();
  }

  _timeOfLastTransmission = MinTime;
  _numberOfTransmissions = 0;
}

int SimpleClientImp::sendRequestToReadLatestSeqNum(uint64_t timeoutMilli, uint64_t& outLatestReqSeqNum) {
  Assert(false);  // not implemented yet
  return 0;
}

void SimpleClientImp::onNewMessage(const NodeNum sourceNode, const char* const message, const size_t messageLength) {
  // check source
  int16_t senderId = (int16_t)sourceNode;
  if (_replicas.count(senderId) == 0) return;

  // check length
  if (messageLength > _maxLegalMsgSize) return;
  if (messageLength < sizeof(MessageBase::Header)) return;

  MessageBase::Header* msgHeader = (MessageBase::Header*)message;

  // check type
  if (msgHeader->msgType != REPLY_MSG_TYPE) return;

  std::unique_lock<std::mutex> mlock(_lock);
  {
    if (_pendingRequest == nullptr) return;

    // create msg object
    MessageBase::Header* msgBody = (MessageBase::Header*)std::malloc(messageLength);
    memcpy(msgBody, message, messageLength);
    MessageBase* pMsg = new MessageBase(senderId, msgBody, messageLength, true);

    _msgQueue.push(pMsg);  // TODO(GG): handle overflow
    _condVar.notify_one();
  }
}

void SimpleClientImp::onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) {}

void SimpleClientImp::sendPendingRequest() {
  Assert(_pendingRequest != nullptr);

  _timeOfLastTransmission = getMonotonicTime();
  _numberOfTransmissions++;

  const bool resetReplies = (_numberOfTransmissions % _clientPeriodicResetThresh == 0);

  const bool sendToAll = _pendingRequest->isReadOnly() || !_primaryReplicaIsKnown ||
                         (_numberOfTransmissions == _clientSendsRequestToAllReplicasFirstThresh) ||
                         (_numberOfTransmissions > _clientSendsRequestToAllReplicasFirstThresh &&
                         (_numberOfTransmissions % _clientSendsRequestToAllReplicasPeriodThresh == 0)) ||
                         resetReplies;

  if (_numberOfTransmissions && !(_numberOfTransmissions % 10))
    LOG_DEBUG_F(GL,
                "Client %d - sends request %" PRIu64
                " isRO=%d, request size=%zu, "
                "retransmissionMilli=%d, numberOfTransmissions=%d, resetReplies=%d, sendToAll=%d",
                _clientId,
                _pendingRequest->requestSeqNum(),
                (int)_pendingRequest->isReadOnly(),
                (size_t)_pendingRequest->size(),
                (int)_limitOfExpectedOperationTime.upperLimit(),
                (int)_numberOfTransmissions,
                (int)resetReplies,
                (int)sendToAll);

  if (resetReplies) {
    replysCertificate.resetAndFree();
    // TODO(GG): print ....
  }

  if (sendToAll) {
    for (uint16_t r : _replicas) {
      // int stat =
      _communication->sendAsyncMessage(r, _pendingRequest->body(), _pendingRequest->size());
      // TODO(GG): handle errors (print and/or ....)
    }
  } else {
    // int stat =
    _communication->sendAsyncMessage(_knownPrimaryReplica, _pendingRequest->body(), _pendingRequest->size());
    // TODO(GG): handle errors (print and/or ....)
  }
}

class SeqNumberGeneratorForClientRequestsImp : public SeqNumberGeneratorForClientRequests {
  virtual uint64_t generateUniqueSequenceNumberForRequest() override;

 protected:
  uint64_t _lastMilliOfUniqueFetchID = 0;
  uint32_t _lastCountOfUniqueFetchID = 0;
};

uint64_t SeqNumberGeneratorForClientRequestsImp::generateUniqueSequenceNumberForRequest() {
  std::chrono::time_point<std::chrono::system_clock> n = std::chrono::system_clock::now();

  uint64_t milli = std::chrono::duration_cast<std::chrono::milliseconds>(n.time_since_epoch()).count();

  if (milli > _lastMilliOfUniqueFetchID) {
    _lastMilliOfUniqueFetchID = milli;
    _lastCountOfUniqueFetchID = 0;
  } else {
    if (_lastCountOfUniqueFetchID == 0x3FFFFF) {
      LOG_WARN(GL, "Client SeqNum Counter reached max value");
      _lastMilliOfUniqueFetchID++;
      _lastCountOfUniqueFetchID = 0;
    } else {
      _lastCountOfUniqueFetchID++;
    }
  }

  uint64_t r = (_lastMilliOfUniqueFetchID << (64 - 42));
  Assert(_lastCountOfUniqueFetchID <= 0x3FFFFF);
  r = r | ((uint64_t)_lastCountOfUniqueFetchID);

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
