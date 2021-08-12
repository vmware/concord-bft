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

#include "NewViewMsg.hpp"
#include "assertUtils.hpp"
#include "EpochManager.hpp"

namespace bftEngine {
namespace impl {

NewViewMsg::NewViewMsg(ReplicaId senderId, ViewNum newView, const concordUtils::SpanContext& spanContext)
    : MessageBase(senderId,
                  MsgCode::NewView,
                  spanContext.data().size(),
                  ReplicaConfig::instance().getmaxExternalMessageSize() - spanContext.data().size()) {
  b()->newViewNum = newView;
  b()->epochNum = EpochManager::instance().getSelfEpochNumber();
  b()->elementsCount = 0;
  memcpy(body() + sizeof(Header), spanContext.data().data(), spanContext.data().size());
}

NewViewMsg::NewViewElement* NewViewMsg::elementsArray() const {
  return reinterpret_cast<NewViewElement*>(body() + sizeof(Header) + spanContextSize());
}

void NewViewMsg::addElement(ReplicaId replicaId, Digest& viewChangeDigest) {
  uint16_t currNumOfElements = b()->elementsCount;

  uint16_t requiredSize = sizeof(Header) + ((currNumOfElements + 1) * sizeof(NewViewElement));

  // TODO(GG): we should reject configurations that may violate this assert. TODO(GG): we need something similar for the
  // VC message
  ConcordAssert(requiredSize <=
                ReplicaConfig::instance().getmaxExternalMessageSize());  // not enough space in the message

  auto elements = elementsArray();

  // IDs should be unique and sorted
  ConcordAssert((currNumOfElements == 0) || (replicaId > elements[currNumOfElements - 1].replicaId));

  elements[currNumOfElements].replicaId = replicaId;
  elements[currNumOfElements].viewChangeDigest = viewChangeDigest;

  b()->elementsCount = currNumOfElements + 1;
}

void NewViewMsg::finalizeMessage(const ReplicasInfo& repInfo) {
  const uint16_t numOfElements = b()->elementsCount;

  const uint16_t F = repInfo.fVal();
  const uint16_t C = repInfo.cVal();

  ConcordAssert(numOfElements == (2 * F + 2 * C + 1));

  const uint16_t tSize = sizeof(Header) + (numOfElements * sizeof(NewViewElement)) + spanContextSize();

  setMsgSize(tSize);
  shrinkToFit();
}

void NewViewMsg::validate(const ReplicasInfo& repInfo) const {
  const uint16_t expectedElements = (2 * repInfo.fVal() + 2 * repInfo.cVal() + 1);
  const uint16_t contentSize = sizeof(Header) + expectedElements * sizeof(NewViewElement) + spanContextSize();

  if (size() < contentSize || !repInfo.isIdOfReplica(senderId()) ||  // source replica
      repInfo.myId() == senderId() || repInfo.primaryOfView(newView()) != senderId() ||
      b()->epochNum != EpochManager::instance().getSelfEpochNumber() ||
      b()->elementsCount != expectedElements)  // num of elements
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": basic"));

  auto elements = elementsArray();
  for (uint16_t i = 0; i < expectedElements; i++) {
    if ((i > 0 && elements[i - 1].replicaId >= elements[i].replicaId) || elements[i].viewChangeDigest.isZero())
      throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": IDs should be unique and sorted"));
  }
  // TODO(GG): more?
}

const uint16_t NewViewMsg::elementsCount() const { return b()->elementsCount; }

bool NewViewMsg::includesViewChangeFromReplica(ReplicaId replicaId, const Digest& viewChangeReplica) const {
  const uint16_t numOfElements = b()->elementsCount;

  auto elements = elementsArray();

  for (uint16_t i = 0; i < numOfElements;
       i++)  // IDs are sorted // TODO(GG): consider to improve (e.g., use binary search)
  {
    uint16_t currId = elements[i].replicaId;
    if (currId == replicaId)
      return (viewChangeReplica == elements[i].viewChangeDigest);
    else if (currId > replicaId)
      return false;
  }

  return false;
}

}  // namespace impl
}  // namespace bftEngine
