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
#include "OpenTracing.hpp"
#include "PrePrepareMsg.hpp"
#include "SysConsts.hpp"
#include "Crypto.hpp"
#include "ClientRequestMsg.hpp"
#include "SigManager.hpp"

namespace bftEngine {
namespace impl {

uint32_t getRequestSizeTemp(const char* request);  // TODO(GG): change - call directly to object

static Digest nullDigest(0x18);

///////////////////////////////////////////////////////////////////////////////
// PrePrepareMsg
///////////////////////////////////////////////////////////////////////////////

const Digest& PrePrepareMsg::digestOfNullPrePrepareMsg() { return nullDigest; }

void PrePrepareMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(senderId() != repInfo.myId());

  if (size() < sizeof(Header) + spanContextSize() ||  // header size
      !repInfo.isIdOfReplica(senderId()))             // sender
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
      b()->numberOfRequests >= b()->endLocationOfLastRequest || !checkRequests()) {
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": advanced"));
  }

  std::vector<std::string> sodofrequests(b()->numberOfRequests);
  bool clientTransactionSigningEnabled = SigManager::instance()->isClientTransactionSigningEnabled();
  auto it = RequestsIterator(this);
  char* requestBody = nullptr;

  size_t local_id = 0;
  while (it.getAndGoToNext(requestBody)) {
    ClientRequestMsg req((ClientRequestMsgHeader*)requestBody);
    if (clientTransactionSigningEnabled) {
      // Here we validate each of the client requests arriving encapsulated inside the pre-prepare message
      // This might also include validating the request's client signature
      req.validate(repInfo);
    }
    char* sig = req.requestSignature();
    if (sig != nullptr) {
      (sodofrequests[local_id]).append(sig, req.requestSignatureLength());
    } else {
      RequestThreadPool::getThreadPool().async(
          [&sodofrequests, local_id](auto* in, auto il) {
            Digest d;
            DigestUtil::compute(in, il, (char*)&d, sizeof(Digest));
            (sodofrequests[local_id]).append(d.content(), sizeof(Digest));
          },
          req.body(),
          req.size());
    }
    local_id++;
  }
  RequestThreadPool::getThreadPool().finalWaitForAll();

  std::string sig_or_dig;
  for (const auto& sod : sodofrequests) {
    sig_or_dig.append(sod);
  }
  // digest
  Digest d;
  DigestUtil::compute(sig_or_dig.c_str(), sig_or_dig.size(), (char*)&d, sizeof(Digest));

  if (d != b()->digestOfRequests) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": digest"));
}

PrePrepareMsg::PrePrepareMsg(ReplicaId sender, ViewNum v, SeqNum s, CommitPath firstPath, size_t size)
    : PrePrepareMsg(sender, v, s, firstPath, concordUtils::SpanContext{}, size) {}

PrePrepareMsg::PrePrepareMsg(ReplicaId sender,
                             ViewNum v,
                             SeqNum s,
                             CommitPath firstPath,
                             const concordUtils::SpanContext& spanContext,
                             size_t size)
    : PrePrepareMsg::PrePrepareMsg(sender, v, s, firstPath, spanContext, std::to_string(s), size) {}

PrePrepareMsg::PrePrepareMsg(ReplicaId sender,
                             ViewNum v,
                             SeqNum s,
                             CommitPath firstPath,
                             const concordUtils::SpanContext& spanContext,
                             const std::string& batchCid,
                             size_t size)
    : MessageBase(sender,
                  MsgCode::PrePrepare,
                  spanContext.data().size(),
                  (((size + sizeof(Header) + batchCid.size()) < maxMessageSize<PrePrepareMsg>())
                       ? (size + sizeof(Header) + batchCid.size())
                       : maxMessageSize<PrePrepareMsg>() - spanContext.data().size())) {
  bool ready = size == 0;  // if null, then message is ready
  if (!ready) {
    b()->digestOfRequests.makeZero();
  } else {
    b()->digestOfRequests = nullDigest;
  }
  b()->batchCidLength = batchCid.size();
  b()->endLocationOfLastRequest = payloadShift();
  b()->flags = computeFlagsForPrePrepareMsg(ready, ready, firstPath);
  b()->numberOfRequests = 0;
  b()->seqNum = s;
  b()->viewNum = v;

  char* position = body() + sizeof(Header);
  memcpy(position, spanContext.data().data(), b()->header.spanContextSize);
  position += spanContext.data().size();
  memcpy(position, batchCid.data(), b()->batchCidLength);
}

uint32_t PrePrepareMsg::remainingSizeForRequests() const {
  ConcordAssert(!isReady());
  ConcordAssert(!isNull());
  ConcordAssert(b()->endLocationOfLastRequest >= payloadShift());

  return (internalStorageSize() - b()->endLocationOfLastRequest);
}

uint32_t PrePrepareMsg::requestsSize() const { return (b()->endLocationOfLastRequest - prePrepareHeaderPrefix); }

void PrePrepareMsg::addRequest(const char* pRequest, uint32_t requestSize) {
  ConcordAssert(getRequestSizeTemp(pRequest) == requestSize);
  ConcordAssert(!isNull());
  ConcordAssert(!isReady());
  ConcordAssert(remainingSizeForRequests() >= requestSize);

  char* insertPtr = body() + b()->endLocationOfLastRequest;

  memcpy(insertPtr, pRequest, requestSize);

  b()->endLocationOfLastRequest += requestSize;
  b()->numberOfRequests++;
}

void PrePrepareMsg::finishAddingRequests() {
  ConcordAssert(!isNull());
  ConcordAssert(!isReady());
  ConcordAssert(b()->numberOfRequests > 0);
  ConcordAssert(b()->endLocationOfLastRequest > payloadShift());
  ConcordAssert(b()->digestOfRequests.isZero());

  // check requests (for debug - consider to remove)
  ConcordAssert(checkRequests());

  // mark as ready
  b()->flags |= 0x2;
  ConcordAssert(isReady());

  std::vector<std::string> sodofrequests(b()->numberOfRequests);
  auto it = RequestsIterator(this);
  char* requestBody = nullptr;
  size_t local_id = 0;

  while (it.getAndGoToNext(requestBody)) {
    ClientRequestMsg req((ClientRequestMsgHeader*)requestBody);
    char* sig = req.requestSignature();
    if (sig != nullptr) {
      (sodofrequests[local_id]).append(sig, req.requestSignatureLength());
    } else {
      RequestThreadPool::getThreadPool().async(
          [&sodofrequests, local_id](auto* in, auto il) {
            Digest d;
            DigestUtil::compute(in, il, (char*)&d, sizeof(Digest));
            (sodofrequests[local_id]).append(d.content(), sizeof(Digest));
          },
          req.body(),
          req.size());
    }
    local_id++;
  }

  RequestThreadPool::getThreadPool().finalWaitForAll();

  std::string sig_or_dig;
  for (const auto& sod : sodofrequests) {
    sig_or_dig.append(sod);
  }

  // compute and set digest
  Digest d;
  DigestUtil::compute(sig_or_dig.c_str(), sig_or_dig.size(), (char*)&d, sizeof(Digest));
  b()->digestOfRequests = d;

  // size
  setMsgSize(b()->endLocationOfLastRequest);
  shrinkToFit();
}

CommitPath PrePrepareMsg::firstPath() const {
  const uint16_t firstPathNum = ((b()->flags >> 2) & 0x3);
  ConcordAssert(firstPathNum <= 2);
  CommitPath retVal = (CommitPath)firstPathNum;  // TODO(GG): check
  return retVal;
}

void PrePrepareMsg::updateView(ViewNum v, CommitPath firstPath) {
  b()->viewNum = v;
  b()->flags = computeFlagsForPrePrepareMsg(isNull(), isReady(), firstPath);
}

int16_t PrePrepareMsg::computeFlagsForPrePrepareMsg(bool isNull, bool isReady, CommitPath firstPath) {
  int16_t retVal = 0;

  ConcordAssert(!isNull || isReady);  // isNull --> isReady

  int16_t firstPathNum = (int16_t)firstPath;
  ConcordAssert(firstPathNum <= 2);

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

  ConcordAssert(false);
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

uint32_t PrePrepareMsg::payloadShift() const {
  return sizeof(Header) + b()->batchCidLength + b()->header.spanContextSize;
}

///////////////////////////////////////////////////////////////////////////////
// RequestsIterator
///////////////////////////////////////////////////////////////////////////////

RequestsIterator::RequestsIterator(const PrePrepareMsg* const m) : msg{m}, currLoc{m->payloadShift()} {
  ConcordAssert(msg->isReady());
}

void RequestsIterator::restart() { currLoc = msg->payloadShift(); }

bool RequestsIterator::getCurrent(char*& pRequest) const {
  if (end()) return false;

  char* p = msg->body() + currLoc;
  pRequest = p;

  return true;
}

bool RequestsIterator::end() const {
  ConcordAssert(currLoc <= msg->b()->endLocationOfLastRequest);

  return (currLoc == msg->b()->endLocationOfLastRequest);
}

void RequestsIterator::gotoNext() {
  ConcordAssert(!end());
  char* p = msg->body() + currLoc;
  uint32_t size = getRequestSizeTemp(p);
  currLoc += size;
  ConcordAssert(currLoc <= msg->b()->endLocationOfLastRequest);
}

bool RequestsIterator::getAndGoToNext(char*& pRequest) {
  bool atEnd = !getCurrent(pRequest);

  if (atEnd) return false;

  gotoNext();

  return true;
}

std::string PrePrepareMsg::getCid() const {
  return std::string(this->body() + this->payloadShift() - b()->batchCidLength, b()->batchCidLength);
}

}  // namespace impl
}  // namespace bftEngine
