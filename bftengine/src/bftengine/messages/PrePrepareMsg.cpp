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

#include <utility>
#include <bftengine/ClientMsgs.hpp>
#include "OpenTracing.hpp"
#include "PrePrepareMsg.hpp"
#include "SysConsts.hpp"
#include "ClientRequestMsg.hpp"
#include "SigManager.hpp"
#include "RequestThreadPool.hpp"
#include "EpochManager.hpp"

using concord::util::digest::DigestUtil;

namespace bftEngine {
namespace impl {

uint32_t getRequestSizeTemp(const char* request);  // TODO(GG): change - call directly to object

static Digest nullDigest(0x18);

///////////////////////////////////////////////////////////////////////////////
// PrePrepareMsg
///////////////////////////////////////////////////////////////////////////////

const Digest& PrePrepareMsg::digestOfNullPrePrepareMsg() { return nullDigest; }

void PrePrepareMsg::calculateDigestOfRequests(Digest& digest) const {
  std::vector<std::pair<char*, size_t>> sigOrDigestOfRequest(b()->numberOfRequests, std::make_pair(nullptr, 0));
  auto digestBuffer = std::make_unique<char[]>(b()->numberOfRequests * sizeof(Digest));

  std::vector<std::future<void>> tasks;
  auto it = RequestsIterator(this);
  char* requestBody = nullptr;
  size_t local_id = 0;

  // threadpool is initialized once and kept with this function.
  // This function is called in a single thread as the queue
  // by dispatcher will not allow multiple threads together.
  try {
    static auto& threadPool = RequestThreadPool::getThreadPool(RequestThreadPool::PoolLevel::FIRSTLEVEL);

    while (it.getAndGoToNext(requestBody)) {
      ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader*>(requestBody));
      char* sig = req.requestSignature();
      if (sig != nullptr) {
        sigOrDigestOfRequest[local_id].first = sig;
        sigOrDigestOfRequest[local_id].second = req.requestSignatureLength();
      } else {
        tasks.push_back(threadPool.async(
            [&sigOrDigestOfRequest, &digestBuffer, local_id](auto* request, auto requestLength) {
              DigestUtil::compute(
                  request, requestLength, digestBuffer.get() + local_id * sizeof(Digest), sizeof(Digest));
              sigOrDigestOfRequest[local_id].first = digestBuffer.get() + local_id * sizeof(Digest);
              sigOrDigestOfRequest[local_id].second = sizeof(Digest);
            },
            req.body(),
            req.size()));
      }
      local_id++;
    }
    for (const auto& t : tasks) {
      t.wait();
    }

    std::string sigOrDig;
    for (const auto& sod : sigOrDigestOfRequest) {
      sigOrDig.append(sod.first, sod.second);
    }

    // compute and set digest
    DigestUtil::compute(sigOrDig.c_str(), sigOrDig.size(), (char*)&digest, sizeof(Digest));
  } catch (std::out_of_range& ex) {
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": digest threadpool"));
  }
}

void PrePrepareMsg::validate(const ReplicasInfo& repInfo) const {
  ConcordAssert(senderId() != repInfo.myId());

  if (size() < sizeof(Header) + spanContextSize() ||  // header size
      !repInfo.isIdOfReplica(senderId()) || b()->epochNum != EpochManager::instance().getSelfEpochNumber())  // sender
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

  Digest d;
  calculateDigestOfRequests(d);
  if (d != b()->digestOfRequests) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": digest"));

  if (SigManager::instance()->isClientTransactionSigningEnabled()) {
    auto it = RequestsIterator(this);
    char* requestBody = nullptr;
    // Here we validate each of the client requests arriving encapsulated inside the pre-prepare message
    // This might also include validating the request's client signature
    while (it.getAndGoToNext(requestBody)) {
      ClientRequestMsg req((ClientRequestMsgHeader*)requestBody);
      req.validate(repInfo);
    }
  }
}

PrePrepareMsg::PrePrepareMsg(ReplicaId sender, ViewNum v, SeqNum s, CommitPath firstPath, size_t size)
    : PrePrepareMsg(sender, v, s, firstPath, concordUtils::SpanContext{}, size) {}

// Sequence number provided in the PPM at the time of constructor call might change later
// This will result into allotment of next available sequence number. As the sequence number
// is a monotonically increasing series, the next available sequence number will be greater
// then the sequence number allocated and with a probability of 1/(10^digits(seqNum)), the
// next available seq number is having one more digit. As this probability is decreasing
// exponentially for higher digits, the case of seq number with next digit is only taken care,
// and a 0 is prefixed.
PrePrepareMsg::PrePrepareMsg(ReplicaId sender,
                             ViewNum v,
                             SeqNum s,
                             CommitPath firstPath,
                             const concordUtils::SpanContext& spanContext,
                             size_t size)
    : PrePrepareMsg::PrePrepareMsg(
          sender, v, s, firstPath, spanContext, std::string(1, '0') + std::to_string(s), size) {}

PrePrepareMsg::PrePrepareMsg(ReplicaId sender,
                             ViewNum v,
                             SeqNum s,
                             CommitPath firstPath,
                             const concordUtils::SpanContext& spanContext,
                             const std::string& batchCid,
                             size_t size)
    : MessageBase(
          sender,
          MsgCode::PrePrepare,
          spanContext.data().size(),
          (((size + sizeof(Header) + batchCid.size()) + spanContext.data().size() < maxMessageSize<PrePrepareMsg>())
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
  b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  b()->viewNum = v;
  b()->time = 0;

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

  try {
    calculateDigestOfRequests(b()->digestOfRequests);
  } catch (std::runtime_error& ex) {
    ConcordAssert(false);
  }

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
    ClientRequestMsg req(reinterpret_cast<ClientRequestMsgHeader*>(requestBody));
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

void PrePrepareMsg::setCid(SeqNum s) {
  std::string cidStr = std::to_string(s);
  if (cidStr.size() >= b()->batchCidLength) {
    memcpy(this->body() + this->payloadShift() - b()->batchCidLength, cidStr.data(), b()->batchCidLength);
  } else {
    memcpy(this->body() + this->payloadShift() - b()->batchCidLength + 1, cidStr.data(), b()->batchCidLength - 1);
  }
}

}  // namespace impl
}  // namespace bftEngine
