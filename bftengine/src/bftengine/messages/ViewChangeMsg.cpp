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

#include <string.h>
#include <cstring>

#include "assertUtils.hpp"
#include "ViewChangeMsg.hpp"
#include "Crypto.hpp"
#include "ViewsManager.hpp"
#include "Logger.hpp"

namespace bftEngine {
namespace impl {

ViewChangeMsg::ViewChangeMsg(ReplicaId srcReplicaId,
                             ViewNum newView,
                             SeqNum lastStableSeq,
                             const concordUtils::SpanContext& spanContext)
    : MessageBase(srcReplicaId,
                  MsgCode::ViewChange,
                  spanContext.data().size(),
                  ReplicaConfigSingleton::GetInstance().GetMaxExternalMessageSize() - spanContext.data().size()) {
  b()->genReplicaId = srcReplicaId;
  b()->newView = newView;
  b()->lastStable = lastStableSeq;
  b()->numberOfElements = 0;
  b()->locationAfterLast = 0;
  std::memcpy(body() + sizeof(Header), spanContext.data().data(), spanContext.data().size());
}

void ViewChangeMsg::setNewViewNumber(ViewNum newView) {
  ConcordAssert(newView >= b()->newView);
  b()->newView = newView;
}

void ViewChangeMsg::getMsgDigest(Digest& outDigest) const {
  size_t bodySize = b()->locationAfterLast;
  if (bodySize == 0) bodySize = sizeof(Header) + spanContextSize();
  DigestUtil::compute(body(), bodySize, (char*)outDigest.content(), sizeof(Digest));
}

void ViewChangeMsg::addElement(SeqNum seqNum,
                               const Digest& prePrepareDigest,
                               ViewNum originView,
                               bool hasPreparedCertificate,
                               ViewNum certificateView,
                               uint16_t certificateSigLength,
                               const char* certificateSig) {
  ConcordAssert(b()->numberOfElements < kWorkWindowSize);
  ConcordAssert(b()->numberOfElements > 0 || b()->locationAfterLast == 0);
  ConcordAssert(seqNum > b()->lastStable);
  ConcordAssert(seqNum <= b()->lastStable + kWorkWindowSize);

  if (b()->locationAfterLast == 0)  // if this is the first element
  {
    ConcordAssert(b()->numberOfElements == 0);
    b()->locationAfterLast = sizeof(Header) + spanContextSize();
  }

  size_t requiredSpace = b()->locationAfterLast + sizeof(Element);
  if (hasPreparedCertificate) requiredSpace += (sizeof(PreparedCertificate) + certificateSigLength);

  // TODO(GG): we should make sure that this assert will never be violated (by calculating the maximum  size of a
  // ViewChangeMsg message required for the actual configuration)
  ConcordAssertLE((size_t)(requiredSpace + ViewsManager::sigManager_->getMySigLength()), (size_t)internalStorageSize());

  Element* pElement = (Element*)(body() + b()->locationAfterLast);
  pElement->seqNum = seqNum;
  pElement->prePrepareDigest = prePrepareDigest;
  pElement->originView = originView;
  pElement->hasPreparedCertificate = hasPreparedCertificate;

  if (hasPreparedCertificate) {
    PreparedCertificate* pCert = (PreparedCertificate*)(body() + b()->locationAfterLast + sizeof(Element));

    pCert->certificateView = certificateView;
    pCert->certificateSigLength = certificateSigLength;

    char* pSig = (char*)(body() + b()->locationAfterLast + sizeof(Element) + sizeof(PreparedCertificate));
    memcpy(pSig, certificateSig, certificateSigLength);
  }

  b()->locationAfterLast = requiredSpace;
  b()->numberOfElements++;
}

void ViewChangeMsg::finalizeMessage() {
  size_t bodySize = b()->locationAfterLast;
  if (bodySize == 0) bodySize = sizeof(Header) + spanContextSize();

  uint16_t sigSize = ViewsManager::sigManager_->getMySigLength();

  setMsgSize(bodySize + sigSize);
  shrinkToFit();

  ViewsManager::sigManager_->sign(body(), bodySize, body() + bodySize, sigSize);

  bool b = checkElements((uint16_t)sigSize);

  ConcordAssert(b);
}

void ViewChangeMsg::validate(const ReplicasInfo& repInfo) const {
  if (size() < sizeof(Header) + spanContextSize() || !repInfo.isIdOfReplica(idOfGeneratedReplica()) ||
      idOfGeneratedReplica() == repInfo.myId())
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic validations"));

  size_t dataLength = b()->locationAfterLast;
  if (dataLength < sizeof(Header)) dataLength = sizeof(Header);
  uint16_t sigLen = ViewsManager::sigManager_->getSigLength(idOfGeneratedReplica());

  if (size() < (dataLength + sigLen)) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": size"));
  if (!ViewsManager::sigManager_->verifySig(idOfGeneratedReplica(), body(), dataLength, body() + dataLength, sigLen))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": verifySig"));
  if (!checkElements(sigLen))  // check elements in message
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": check elements in message"));
}

bool ViewChangeMsg::checkElements(uint16_t sigSize) const {
  SeqNum lastSeqNumInMsg = lastStable();
  uint16_t numOfActualElements = 0;
  size_t remainingBytes = size() - sigSize - sizeof(Header) - spanContextSize();
  char* currLoc = body() + sizeof(Header) + spanContextSize();

  while ((remainingBytes >= sizeof(Element)) && (numOfActualElements < numberOfElements())) {
    numOfActualElements++;
    Element* pElement = (Element*)currLoc;
    if (pElement->seqNum <= lastSeqNumInMsg) {
      return false;  // elements should be sorted by seq number
    }
    lastSeqNumInMsg = pElement->seqNum;

    if (lastSeqNumInMsg > lastStable() + kWorkWindowSize) return false;

    if (pElement->originView >= newView()) return false;

    remainingBytes -= sizeof(Element);
    currLoc += sizeof(Element);

    if (pElement->hasPreparedCertificate) {
      if (remainingBytes < sizeof(PreparedCertificate)) return false;

      PreparedCertificate* pCert = (PreparedCertificate*)currLoc;

      if (pCert->certificateView > pElement->originView) return false;

      if (pCert->certificateSigLength == 0) return false;

      const size_t s = sizeof(PreparedCertificate) + pCert->certificateSigLength;

      if (remainingBytes < s) return false;

      // we don't check the certificate here (because in many cases it is not needed to make progress)

      remainingBytes -= s;
      currLoc += s;
    }
  }

  if (numOfActualElements != numberOfElements()) return false;

  if (numOfActualElements > 0) {
    const size_t locationAfterLastElement = size() - sigSize - remainingBytes;
    if (this->b()->locationAfterLast != locationAfterLastElement) return false;
  } else {
    if (this->b()->locationAfterLast != 0) return false;
  }

  return true;
}

ViewChangeMsg::ElementsIterator::ElementsIterator(const ViewChangeMsg* const m) : msg{m} {
  if (m == nullptr || m->numberOfElements() == 0) {
    endLoc = 0;
    currLoc = 0;
    nextElementNum = 1;
  } else {
    endLoc = m->b()->locationAfterLast;
    currLoc = sizeof(Header) + m->spanContextSize();
    ConcordAssert(endLoc > currLoc);
    nextElementNum = 1;
  }
}

bool ViewChangeMsg::ElementsIterator::end() {
  if (currLoc >= endLoc) {
    ConcordAssert(msg == nullptr || ((nextElementNum - 1) == msg->numberOfElements()));
    return true;
  }

  return false;
}

bool ViewChangeMsg::ElementsIterator::getCurrent(Element*& pElement) {
  pElement = nullptr;

  if (end()) return false;

  const size_t remainingbytes = (endLoc - currLoc);

  ConcordAssert(remainingbytes >= sizeof(Element));

  pElement = (Element*)(msg->body() + currLoc);

  if (pElement->hasPreparedCertificate) {
    ConcordAssert(remainingbytes >= sizeof(Element) + sizeof(PreparedCertificate));
    PreparedCertificate* pCert = (PreparedCertificate*)(msg->body() + currLoc + sizeof(Element));
    ConcordAssert(remainingbytes >= sizeof(Element) + sizeof(PreparedCertificate) + pCert->certificateSigLength);
  }

  return true;
}

// TODO(GG): consider to optimize the methods of ViewChangeMsg::ElementsIterator

void ViewChangeMsg::ElementsIterator::gotoNext() {
  Element* dummy;
  getAndGoToNext(dummy);
}

bool ViewChangeMsg::ElementsIterator::getAndGoToNext(Element*& pElement) {
  pElement = nullptr;

  if (end()) return false;

  const size_t remainingbytes = (endLoc - currLoc);

  ConcordAssert(remainingbytes >= sizeof(Element));

  Element* p = (Element*)(msg->body() + currLoc);

  currLoc += sizeof(Element);

  if (p->hasPreparedCertificate) {
    ConcordAssert(remainingbytes >= sizeof(Element) + sizeof(PreparedCertificate));

    PreparedCertificate* pCert = (PreparedCertificate*)(msg->body() + currLoc);

    ConcordAssert(remainingbytes >= sizeof(Element) + sizeof(PreparedCertificate) + pCert->certificateSigLength);

    currLoc += (sizeof(PreparedCertificate) + pCert->certificateSigLength);
  }

  pElement = p;

  nextElementNum++;

  return true;
}

bool ViewChangeMsg::ElementsIterator::goToAtLeast(SeqNum lowerBound) {
  Element* currElement = nullptr;
  bool validElement = getCurrent(currElement);

  while (validElement && currElement->seqNum < lowerBound) {
    gotoNext();
    validElement = getCurrent(currElement);
  }

  return validElement;
}

}  // namespace impl
}  // namespace bftEngine
