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
#include "ReplicasInfo.hpp"

namespace bftEngine {
namespace impl {

class ViewChangeMsg : public MessageBase {
 public:
#pragma pack(push, 1)
  struct Element {
    SeqNum seqNum;
    Digest prePrepreDigest;
    ViewNum originView;
    bool hasPreparedCertificate;  // TODO(SG): I think sizeof(bool) is
                                  // implementation dependent
    // if (hasPreparedCertificate) then followed by PreparedCertificate
  };

  struct PreparedCertificate {
    ViewNum certificateView;
    uint16_t certificateSigLength;
    // Followed by signature of <certificateView, seqNum, pre-prepre digest>
  };
#pragma pack(pop)
  static_assert(sizeof(Element) == (8 + DIGEST_SIZE + 8 + 1),
                "Element (View Change) is 49B");
  static_assert(sizeof(PreparedCertificate) == (8 + 2),
                "PreparedCertificate is 10B");

  ViewChangeMsg(ReplicaId srcReplicaId, ViewNum newView, SeqNum lastStableSeq);

  void setNewViewNumber(ViewNum newView);

  uint16_t idOfGeneratedReplica() const {
    return b()->genReplicaId;
  }  // TODO(GG): !!!! change meaning/add similar method - this msg may be sent
     // by a different replica (otherwise, the view-change may not completed)

  ViewNum newView() const { return b()->newView; }

  SeqNum lastStable() const { return b()->lastStable; }

  uint16_t numberOfElements() const { return b()->numberOfElements; }

  void getMsgDigest(Digest& outDigest) const;

  void addElement(const ReplicasInfo& repInfo,
                  SeqNum seqNum,
                  const Digest& prePrepreDigest,
                  ViewNum originView,
                  bool hasPreparedCertificate,
                  ViewNum certificateView,
                  uint16_t certificateSigLength,
                  const char* certificateSig);

  void finalizeMessage(const ReplicasInfo& repInfo);

  static bool ToActualMsgType(const ReplicasInfo& repInfo,
                              MessageBase* inMsg,
                              ViewChangeMsg*& outMsg);

  class ElementsIterator {
   public:
    // this ctor assumes that m is a legal ViewChangeMsg message (as defined by
    // checkElements() )
    ElementsIterator(const ViewChangeMsg* const m);

    bool getCurrent(Element*& pElement);

    bool end();

    void gotoNext();

    bool getAndGoToNext(Element*& pElement);

    bool goToAtLeast(SeqNum lowerBound);

   protected:
    const ViewChangeMsg* const msg;
    uint16_t endLoc;
    uint16_t currLoc;
    uint16_t nextElementNum;  // used for debug
  };

 protected:
#pragma pack(push, 1)
  struct ViewChangeMsgHeader {
    MessageBase::Header header;
    ReplicaId
        genReplicaId;  // the replica that originally generated this message
    ViewNum newView;   // the new view
    SeqNum lastStable;
    uint16_t numberOfElements;
    uint16_t locationAfterLast;  // if(numberOfElements > 0) then it holds the
                                 // location after the last element followed by
                                 // a sequnce of Element followed by a signature
                                 // (by genReplicaId)
  };
#pragma pack(pop)
  static_assert(sizeof(ViewChangeMsgHeader) == (2 + 2 + 8 + 8 + 2 + 2),
                "ViewChangeMsgHeader is 24B");

  ViewChangeMsgHeader* b() const { return ((ViewChangeMsgHeader*)msgBody_); }

  bool checkElements(uint16_t sigSize) const;
};

}  // namespace impl
}  // namespace bftEngine
