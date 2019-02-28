// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "NewViewMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

NewViewMsg::NewViewMsg(ReplicaId senderId, ViewNum newView)
    : MessageBase(senderId, MsgCode::NewView, maxExternalMessageSize) {
  b()->newViewNum = newView;
  b()->elementsCount = 0;
}

void NewViewMsg::addElement(ReplicaId replicaId, Digest& viewChangeDigest) {
  uint16_t currNumOfElements = b()->elementsCount;

  uint16_t requiredSize = sizeof(NewViewMsgHeader) +
                          ((currNumOfElements + 1) * sizeof(NewViewElement));

  // TODO(GG): we should reject configurations that may violate this assert.
  // TODO(GG): we need something similar for the VC message
  Assert(requiredSize <=
         maxExternalMessageSize);  // not enough space in the message

  NewViewElement* elementsArray =
      (NewViewElement*)(body() + sizeof(NewViewMsgHeader));

  // IDs should be unique and sorted
  Assert((currNumOfElements == 0) ||
         (replicaId > elementsArray[currNumOfElements - 1].replicaId));

  elementsArray[currNumOfElements].replicaId = replicaId;
  elementsArray[currNumOfElements].viewChangeDigest = viewChangeDigest;

  b()->elementsCount = currNumOfElements + 1;
}

void NewViewMsg::finalizeMessage(const ReplicasInfo& repInfo) {
  const uint16_t numOfElements = b()->elementsCount;

  const uint16_t F = repInfo.fVal();
  const uint16_t C = repInfo.cVal();

  Assert(numOfElements == (2 * F + 2 * C + 1));

  const uint16_t tSize =
      sizeof(NewViewMsgHeader) + (numOfElements * sizeof(NewViewElement));

  setMsgSize(tSize);
  shrinkToFit();
}

bool NewViewMsg::ToActualMsgType(const ReplicasInfo& repInfo,
                                 MessageBase* inMsg,
                                 NewViewMsg*& outMsg) {
  Assert(inMsg->type() == MsgCode::NewView);
  if (inMsg->size() < sizeof(NewViewMsgHeader)) return false;

  const uint16_t F = repInfo.fVal();
  const uint16_t C = repInfo.cVal();
  const uint16_t expectedElements = (2 * F + 2 * C + 1);

  NewViewMsg* t = (NewViewMsg*)inMsg;

  // size
  const uint16_t contentSize =
      sizeof(NewViewMsgHeader) + expectedElements * sizeof(NewViewElement);
  if (t->size() < contentSize) return false;

  // source replica
  const ReplicaId senderId = t->senderId();
  if (!repInfo.isIdOfReplica(senderId)) return false;
  if (repInfo.myId() == senderId) return false;

  const ReplicaId primaryId = repInfo.primaryOfView(t->newView());
  if (primaryId != senderId) return false;

  // num of elements
  if (t->b()->elementsCount != expectedElements) return false;

  // check elements in message
  NewViewElement* elementsArray =
      (NewViewElement*)(t->body() + sizeof(NewViewMsgHeader));
  for (uint16_t i = 0; i < expectedElements; i++) {
    // IDs should be unique and sorted
    if (i > 0 && elementsArray[i - 1].replicaId >= elementsArray[i].replicaId)
      return false;

    if (elementsArray[i].viewChangeDigest.isZero()) return false;
  }

  // TODO(GG): more?

  outMsg = (NewViewMsg*)t;

  return true;
}

bool NewViewMsg::includesViewChangeFromReplica(
    ReplicaId replicaId, const Digest& viewChangeReplica) const {
  const uint16_t numOfElements = b()->elementsCount;

  NewViewElement* elementsArray =
      (NewViewElement*)(body() + sizeof(NewViewMsgHeader));

  for (uint16_t i = 0; i < numOfElements;
       i++)  // IDs are sorted // TODO(GG): consider to improve (e.g., use
             // binary search)
  {
    uint16_t currId = elementsArray[i].replicaId;
    if (currId == replicaId)
      return (viewChangeReplica == elementsArray[i].viewChangeDigest);
    else if (currId > replicaId)
      return false;
  }

  return false;
}

}  // namespace impl
}  // namespace bftEngine
