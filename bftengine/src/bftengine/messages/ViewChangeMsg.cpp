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
                  ReplicaConfig::instance().getmaxExternalMessageSize() - spanContext.data().size()) {
  b()->genReplicaId = srcReplicaId;
  b()->newView = newView;
  b()->lastStable = lastStableSeq;
  b()->numberOfComplaints = 0;
  b()->sizeOfAllComplaints = 0;
  b()->numberOfElements = 0;
  b()->locationAfterLast = 0;
  std::memcpy(body() + sizeof(Header), spanContext.data().data(), spanContext.data().size());
}

void ViewChangeMsg::setNewViewNumber(ViewNum newView) {
  ConcordAssert(newView >= b()->newView);
  b()->newView = newView;
}

void ViewChangeMsg::getMsgDigest(Digest& outDigest) const {
  auto bodySize = getBodySize();
  bodySize += b()->sizeOfAllComplaints;
  DigestUtil::compute(body(), bodySize, (char*)outDigest.content(), sizeof(Digest));
}

uint32_t ViewChangeMsg::getBodySize() const {
  uint32_t bodySize = b()->locationAfterLast;
  if (bodySize == 0) bodySize = sizeof(Header) + spanContextSize();
  return bodySize;
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
  ConcordAssert(b()->numberOfComplaints == 0);   // We first add the elements for each seqNum
  ConcordAssert(b()->sizeOfAllComplaints == 0);  // and only after that we add the complaints

  if (b()->locationAfterLast == 0)  // if this is the first element
  {
    ConcordAssert(b()->numberOfElements == 0);
    b()->locationAfterLast = sizeof(Header) + spanContextSize();
  }

  uint32_t requiredSpace = b()->locationAfterLast + sizeof(Element);
  if (hasPreparedCertificate) requiredSpace += (sizeof(PreparedCertificate) + certificateSigLength);

  // TODO(GG): we should make sure that this assert will never be violated (by calculating the maximum  size of a
  // ViewChangeMsg message required for the actual configuration)
  ConcordAssertLE((size_t)(requiredSpace + SigManager::getInstance()->getMySigLength()), (size_t)internalStorageSize());

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

void ViewChangeMsg::addComplaint(const ReplicaAsksToLeaveViewMsg* const complaint) {
  // We store complaints in a size/value list
  // Visual representation for List Of Complaints:
  // +--------------------+-------------------------+--------------------+-------------------------+---
  // |Size of next element|ReplicaAsksToLeaveViewMsg|Size of next element|ReplicaAsksToLeaveViewMsg|...
  // +--------------------+-------------------------+--------------------+-------------------------+---

  ConcordAssert(b()->numberOfComplaints > 0 || b()->sizeOfAllComplaints == 0);
  auto bodySize = getBodySize();
  auto sigSize = SigManager::getInstance()->getMySigLength();
  bodySize += sigSize + b()->sizeOfAllComplaints;

  auto sizeOfComplaint = complaint->size();

  ConcordAssertLE((size_t)bodySize + sizeof(sizeOfComplaint) + (size_t)sizeOfComplaint, (size_t)internalStorageSize());

  memcpy(body() + bodySize, &sizeOfComplaint, sizeof(sizeOfComplaint));
  memcpy(body() + bodySize + sizeof(sizeOfComplaint), complaint->body(), complaint->size());

  b()->sizeOfAllComplaints += sizeof(sizeOfComplaint) + complaint->size();
  b()->numberOfComplaints++;
}

bool ViewChangeMsg::clearAllComplaints() {
  b()->sizeOfAllComplaints = 0;
  b()->numberOfComplaints = 0;
  if (reallocSize(ReplicaConfig::instance().getmaxExternalMessageSize())) {
    auto bodySize = getBodySize();
    auto sigSize = SigManager::getInstance()->getMySigLength();
    memset(body() + bodySize + sigSize, 0, storageSize_ - (bodySize + sigSize));
    return true;
  } else {
    return false;
  }
}

void ViewChangeMsg::finalizeMessage() {
  auto bodySize = getBodySize();
  auto sigManager = SigManager::getInstance();

  auto sigSize = sigManager->getMySigLength();

  setMsgSize(bodySize + sigSize + b()->sizeOfAllComplaints);
  shrinkToFit();

  // We only sign the part that is concerned with View Change safety,
  // the complaints carry signatures from the issuers.
  // Visual representation:
  // +------------+------------+----------------+-----------------------------+------------------+
  // |VCMsg header|Span Context|List of Elements|Signature for previous fields|List Of Complaints|
  // +------------+------------+----------------+-----------------------------+------------------+
  // |               Message Body               |
  // +------------------------------------------+

  sigManager->sign(body(), bodySize, body() + bodySize, sigSize);

  bool b = checkElements((uint16_t)sigSize) && checkComplaints((uint16_t)sigSize);

  ConcordAssert(b);
}

void ViewChangeMsg::validate(const ReplicasInfo& repInfo) const {
  auto sigManager = SigManager::getInstance();
  if (size() < sizeof(Header) + spanContextSize() || !repInfo.isIdOfReplica(idOfGeneratedReplica()) ||
      idOfGeneratedReplica() == repInfo.myId())
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic validations"));

  auto dataLength = getBodySize();
  uint16_t sigLen = sigManager->getSigLength(idOfGeneratedReplica());

  if (size() < (dataLength + sigLen)) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": size"));
  if (!sigManager->verifySig(idOfGeneratedReplica(), body(), dataLength, body() + dataLength, sigLen))
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": verifySig"));
  if (!checkElements(sigLen))  // check elements in message
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": check elements in message"));
  if (!checkComplaints(sigLen))  // check list of complaints
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": check complaints in message"));
}

bool ViewChangeMsg::checkElements(uint16_t sigSize) const {
  SeqNum lastSeqNumInMsg = lastStable();
  uint16_t numOfActualElements = 0;
  uint32_t remainingBytes = size() - sigSize - sizeof(Header) - spanContextSize();
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

      const uint32_t s = sizeof(PreparedCertificate) + pCert->certificateSigLength;

      if (remainingBytes < s) return false;

      // we don't check the certificate here (because in many cases it is not needed to make progress)

      remainingBytes -= s;
      currLoc += s;
    }
  }

  if (numOfActualElements != numberOfElements()) return false;

  if (numOfActualElements > 0) {
    const uint32_t locationAfterLastElement = size() - sigSize - remainingBytes;
    if (this->b()->locationAfterLast != locationAfterLastElement) return false;
  } else {
    if (this->b()->locationAfterLast != 0) return false;
  }

  return true;
}

bool ViewChangeMsg::checkComplaints(uint16_t sigSize) const {
  uint16_t numOfActualComplaints = 0;
  auto bodySize = getBodySize();
  uint32_t remainingBytes = size() - sigSize - bodySize;
  char* currLoc = body() + bodySize + sigSize;

  while (remainingBytes > sizeOfHeader<ReplicaAsksToLeaveViewMsg>() && (numOfActualComplaints < numberOfComplaints())) {
    MsgSize complaintSize = 0;
    memcpy(&complaintSize, currLoc, sizeof(MsgSize));
    remainingBytes -= sizeof(MsgSize);
    currLoc += sizeof(MsgSize);

    if (complaintSize <= sizeOfHeader<ReplicaAsksToLeaveViewMsg>()) {
      return false;
    }

    numOfActualComplaints++;
    remainingBytes -= complaintSize;
    currLoc += complaintSize;
  }

  if (numOfActualComplaints != numberOfComplaints()) return false;

  if (remainingBytes != 0) return false;

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

  const uint32_t remainingbytes = (endLoc - currLoc);

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

  const uint32_t remainingbytes = (endLoc - currLoc);

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

ViewChangeMsg::ComplaintsIterator::ComplaintsIterator(const ViewChangeMsg* const m) : msg{m} {
  if (m == nullptr || m->numberOfComplaints() == 0) {
    endLoc = 0;
    currLoc = 0;
    nextComplaintNum = 1;
  } else {
    endLoc = m->size();
    auto bodySize = m->getBodySize();
    currLoc = bodySize + SigManager::getInstance()->getMySigLength();
    ConcordAssert(endLoc > currLoc);
    nextComplaintNum = 1;
  }
}

bool ViewChangeMsg::ComplaintsIterator::end() {
  if (currLoc >= endLoc) {
    ConcordAssert(msg == nullptr || ((nextComplaintNum - 1) == msg->numberOfComplaints()));
    return true;
  }

  return false;
}

bool ViewChangeMsg::ComplaintsIterator::getCurrent(char*& pComplaint, MsgSize& size) {
  if (end()) return false;

  memcpy(&size, msg->body() + currLoc, sizeof(MsgSize));
  const uint32_t remainingbytes = (endLoc - currLoc) - sizeof(MsgSize);
  ConcordAssert(remainingbytes >= size);  // Validate method must make sure we never accept such message
  pComplaint = (char*)malloc(size);
  memcpy(pComplaint, msg->body() + currLoc + sizeof(MsgSize), size);

  return true;
}

void ViewChangeMsg::ComplaintsIterator::gotoNext() {
  if (end()) return;

  MsgSize size = 0;
  memcpy(&size, (msg->body() + currLoc), sizeof(MsgSize));
  const uint32_t remainingbytes = (endLoc - currLoc) - sizeof(MsgSize);
  ConcordAssert(remainingbytes >= size);  // Validate method must make sure we never accept such message
  currLoc += sizeof(MsgSize) + size;

  nextComplaintNum++;
}

bool ViewChangeMsg::ComplaintsIterator::getAndGoToNext(char*& pComplaint, MsgSize& size) {
  bool retVal = getCurrent(pComplaint, size);
  gotoNext();
  return retVal;
}

}  // namespace impl
}  // namespace bftEngine
