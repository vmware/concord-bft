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

#pragma once

#include "MessageBase.hpp"
#include "Digest.hpp"

namespace bftEngine {
namespace impl {

class NewViewMsg : public MessageBase {
 public:
  NewViewMsg(ReplicaId senderId, ViewNum newView);

  void addElement(ReplicaId replicaId, Digest& viewChangeDigest);

  ViewNum newView() const { return b()->newViewNum; }

  void finalizeMessage(const ReplicasInfo& repInfo);

  static bool ToActualMsgType(const ReplicasInfo& repInfo,
                              MessageBase* inMsg,
                              NewViewMsg*& outMsg);

  bool includesViewChangeFromReplica(ReplicaId replicaId,
                                     const Digest& viewChangeReplica) const;

 protected:
#pragma pack(push, 1)
  struct NewViewMsgHeader {
    MessageBase::Header header;

    ViewNum newViewNum;  // the new view

    uint16_t elementsCount;  // TODO(GG): remove from header
                             // followed by a sequnce of 2f+2c+1 instances of
                             // NewViewElement
  };

  struct NewViewElement {
    ReplicaId replicaId;
    Digest viewChangeDigest;
  };
#pragma pack(pop)

  static_assert(sizeof(NewViewMsgHeader) == (2 + 8 + 2),
                "NewViewMsgHeader is 12B");
  static_assert(sizeof(NewViewElement) == (2 + DIGEST_SIZE),
                "NewViewElement is 34B");

  NewViewMsgHeader* b() const { return (NewViewMsgHeader*)msgBody_; }
};

}  // namespace impl
}  // namespace bftEngine
