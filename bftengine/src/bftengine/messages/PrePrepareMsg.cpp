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

#include <bftengine/ClientMsgs.hpp>
#include "PrePrepareMsg.hpp"
#include "SysConsts.hpp"
#include "Crypto.hpp"
#include "ReplicaConfig.hpp"
#include "ClientRequestMsg.hpp"

namespace bftEngine {
namespace impl {

uint32_t getRequestSizeTemp(const char* request);  // TODO(GG): change - call directly to object

static Digest nullDigest(0x18);

///////////////////////////////////////////////////////////////////////////////
// PrePrepareMsg
///////////////////////////////////////////////////////////////////////////////

MsgSize PrePrepareMsg::maxSizeOfPrePrepareMsg() {
  return ReplicaConfigSingleton::GetInstance().GetMaxExternalMessageSize();
}

MsgSize PrePrepareMsg::maxSizeOfPrePrepareMsgInLocalBuffer() {
  return maxSizeOfPrePrepareMsg() + sizeof(RawHeaderOfObjAndMsg);
}

PrePrepareMsg* PrePrepareMsg::createNullPrePrepareMsg(
    ReplicaId sender, ViewNum v, SeqNum s, CommitPath firstPath, const std::string& spanContext) {
  PrePrepareMsg* p = new PrePrepareMsg(sender, v, s, firstPath, spanContext, true);
  return p;
}

const Digest& PrePrepareMsg::digestOfNullPrePrepareMsg() { return nullDigest; }

void PrePrepareMsg::validate(const ReplicasInfo& repInfo) const {
  Assert(senderId() != repInfo.myId());

  if (size() < sizeof(PrePrepareMsgHeader) ||  // header size
      !repInfo.isIdOfReplica(senderId()))      // sender
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic"));
  // NB: the actual expected sender is verified outside this class (because in some cases, during view-change protocol,
  // this message may sent by a non-primary replica to the primary replica).

  // check flags
  const uint16_t flags = b()->flags;
  const bool isNull = ((flags & 0x1) == 0);
  const bool isReady = (((flags >> 1) & 0x1) == 1);
  const uint16_t firstPath_ = ((flags >> 2) & 0x3);
  const uint16_t reservedBits = (flags >> 4);

  if (b()->seqNum == 0 || isNull ||  // we don't send null requests
      !isReady ||                    // not ready
      firstPath_ >= 3 ||             // invalid first path
      ((firstPath() == CommitPath::FAST_WITH_THRESHOLD) && (repInfo.cVal() == 0)) || reservedBits != 0 ||
      b()->endLocationOfLastRequest > size() || b()->numberOfRequests == 0 ||
      b()->numberOfRequests >= b()->endLocationOfLastRequest || !checkRequests())
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": advanced"));

  // digest
  Digest d;
  const char* requestBuffer = (char*)&(b()->numberOfRequests);
  const uint32_t requestSize = (b()->endLocationOfLastRequest - prePrepareHeaderPrefix);

  DigestUtil::compute(requestBuffer, requestSize, (char*)&d, sizeof(Digest));

  if (d != b()->digestOfRequests) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": digest"));
}

PrePrepareMsg::PrePrepareMsg(ReplicaId sender, ViewNum v, SeqNum s, CommitPath firstPath, bool isNull, size_t size)
    : PrePrepareMsg(sender, v, s, firstPath, "", isNull, size) {}

PrePrepareMsg::PrePrepareMsg(ReplicaId sender,
                             ViewNum v,
                             SeqNum s,
                             CommitPath firstPath,
                             const std::string& spanContext,
                             bool isNull,
                             size_t size)
    : MessageBase(sender,
                  MsgCode::PrePrepare,
                  spanContext.size(),
                  (((size + sizeof(PrePrepareMsgHeader) + spanContext.size()) < maxSizeOfPrePrepareMsg())
                       ? (size + sizeof(PrePrepareMsgHeader) + spanContext.size())
                       : maxSizeOfPrePrepareMsg() - spanContext.size()))

{
  b()->viewNum = v;
  b()->seqNum = s;

  bool ready = isNull;  // if null, then message is ready
  b()->flags = computeFlagsForPrePrepareMsg(isNull, ready, firstPath);

  if (!isNull)  // not null
    b()->digestOfRequests.makeZero();
  else  // null
    b()->digestOfRequests = nullDigest;

  b()->numberOfRequests = 0;
  b()->endLocationOfLastRequest = payloadShift();

  char* position = body() + sizeof(PrePrepareMsgHeader);
  memcpy(position, spanContext.data(), b()->header.spanContextSize);
}

uint32_t PrePrepareMsg::remainingSizeForRequests() const {
  Assert(!isReady());
  Assert(!isNull());
  Assert(b()->endLocationOfLastRequest >= payloadShift());

  return (internalStorageSize() - b()->endLocationOfLastRequest);
}

std::string PrePrepareMsg::spanContext() const {
  return std::string(body() + sizeof(PrePrepareMsgHeader), b()->header.spanContextSize);
}

void PrePrepareMsg::addRequest(const char* pRequest, uint32_t requestSize) {
  Assert(getRequestSizeTemp(pRequest) == requestSize);
  Assert(!isNull());
  Assert(!isReady());
  Assert(remainingSizeForRequests() >= requestSize);

  char* insertPtr = body() + b()->endLocationOfLastRequest;

  memcpy(insertPtr, pRequest, requestSize);

  b()->endLocationOfLastRequest += requestSize;
  b()->numberOfRequests++;
}

void PrePrepareMsg::finishAddingRequests() {
  Assert(!isNull());
  Assert(!isReady());
  Assert(b()->numberOfRequests > 0);
  Assert(b()->endLocationOfLastRequest > payloadShift());
  Assert(b()->digestOfRequests.isZero());

  // check requests (for debug - consider to remove)
  Assert(checkRequests());

  // mark as ready
  b()->flags |= 0x2;
  Assert(isReady());

  // compute and set digest
  Digest d;
  const char* requestBuffer = (char*)&(b()->numberOfRequests);
  const uint32_t requestSize = (b()->endLocationOfLastRequest - prePrepareHeaderPrefix);
  DigestUtil::compute(requestBuffer, requestSize, (char*)&d, sizeof(Digest));
  b()->digestOfRequests = d;

  // size
  setMsgSize(b()->endLocationOfLastRequest);
  shrinkToFit();
}

CommitPath PrePrepareMsg::firstPath() const {
  const uint16_t firstPathNum = ((b()->flags >> 2) & 0x3);
  Assert(firstPathNum <= 2);
  CommitPath retVal = (CommitPath)firstPathNum;  // TODO(GG): check
  return retVal;
}

void PrePrepareMsg::updateView(ViewNum v, CommitPath firstPath) {
  b()->viewNum = v;
  b()->flags = computeFlagsForPrePrepareMsg(isNull(), isReady(), firstPath);
}

int16_t PrePrepareMsg::computeFlagsForPrePrepareMsg(bool isNull, bool isReady, CommitPath firstPath) {
  int16_t retVal = 0;

  Assert(!isNull || isReady);  // isNull --> isReady

  int16_t firstPathNum = (int16_t)firstPath;
  Assert(firstPathNum <= 2);

  retVal |= (firstPathNum << 2);
  retVal |= ((isReady ? 1 : 0) << 1);
  retVal |= (isNull ? 0 : 1);

  return retVal;
}

bool PrePrepareMsg::checkRequests() const {
  uint16_t remainReqs = b()->numberOfRequests;

  if (remainReqs == 0) return (b()->endLocationOfLastRequest == payloadShift());

  uint32_t i = payloadShift();

  if (i >= b()->endLocationOfLastRequest) return false;

  while (true) {
    const char* req = body() + i;
    const uint32_t reqSize = getRequestSizeTemp(req);

    remainReqs--;
    i += reqSize;

    if (remainReqs > 0) {
      if (i >= b()->endLocationOfLastRequest) return false;
    } else {
      return (i == b()->endLocationOfLastRequest);
    }
  }

  Assert(false);
  return false;
}
const std::string PrePrepareMsg::getClientCorrelationIdForMsg(int index) const {
  auto it = RequestsIterator(this);
  int req_num = 0;
  while (!it.end() && req_num < index) {
    it.gotoNext();
    req_num++;
  }
  if (it.end()) return std::string();
  char* requestBody = nullptr;
  it.getCurrent(requestBody);
  return ClientRequestMsg((ClientRequestMsgHeader*)requestBody).getCid();
}

const std::string PrePrepareMsg::getBatchCorrelationIdAsString() const {
  std::string ret;
  auto it = RequestsIterator(this);
  char* requestBody = nullptr;
  while (it.getAndGoToNext(requestBody)) {
    ClientRequestMsg req((ClientRequestMsgHeader*)requestBody);
    ret += req.getCid() + ";";
  }
  return ret;
}

uint32_t PrePrepareMsg::payloadShift() const { return sizeof(PrePrepareMsgHeader) + b()->header.spanContextSize; }

///////////////////////////////////////////////////////////////////////////////
// RequestsIterator
///////////////////////////////////////////////////////////////////////////////

RequestsIterator::RequestsIterator(const PrePrepareMsg* const m) : msg{m}, currLoc{m->payloadShift()} {
  Assert(msg->isReady());
}

void RequestsIterator::restart() { currLoc = msg->payloadShift(); }

bool RequestsIterator::getCurrent(char*& pRequest) const {
  if (end()) return false;

  char* p = msg->body() + currLoc;
  pRequest = p;

  return true;
}

bool RequestsIterator::end() const {
  Assert(currLoc <= msg->b()->endLocationOfLastRequest);

  return (currLoc == msg->b()->endLocationOfLastRequest);
}

void RequestsIterator::gotoNext() {
  Assert(!end());
  char* p = msg->body() + currLoc;
  uint32_t size = getRequestSizeTemp(p);
  currLoc += size;
  Assert(currLoc <= msg->b()->endLocationOfLastRequest);
}

bool RequestsIterator::getAndGoToNext(char*& pRequest) {
  bool atEnd = !getCurrent(pRequest);

  if (atEnd) return false;

  gotoNext();

  return true;
}

}  // namespace impl
}  // namespace bftEngine
