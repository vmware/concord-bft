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

#pragma once

#include "MessageBase.hpp"
#include "Digest.hpp"
#include "OpenTracing.hpp"
#include "ReplicasInfo.hpp"
#include "ReplicaConfig.hpp"

namespace bftEngine {
namespace impl {

class ViewChangeMsg : public MessageBase {
 public:
#pragma pack(push, 1)
  struct Element {
    SeqNum seqNum;
    Digest prePrepareDigest;
    ViewNum originView;
    uint8_t hasPreparedCertificate;  // if (hasPreparedCertificate) then followed by PreparedCertificate
  };

  struct PreparedCertificate {
    ViewNum certificateView;
    uint16_t certificateSigLength;
    // Followed by signature of <certificateView, seqNum, pre-prepare digest>
  };
#pragma pack(pop)
  static_assert(sizeof(Element) == (8 + DIGEST_SIZE + 8 + 1), "Element (View Change) is 49B");
  static_assert(sizeof(PreparedCertificate) == (8 + 2), "PreparedCertificate is 10B");

  ViewChangeMsg(ReplicaId srcReplicaId,
                ViewNum newView,
                SeqNum lastStableSeq,
                const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  void setNewViewNumber(ViewNum newView);

  uint16_t idOfGeneratedReplica() const {
    return b()->genReplicaId;
  }  // TODO(GG): !!!! change meaning/add similar method - this msg may be sent by a different replica (otherwise, the
     // view-change may not completed)

  ViewNum newView() const { return b()->newView; }

  SeqNum lastStable() const { return b()->lastStable; }

  uint16_t numberOfElements() const { return b()->numberOfElements; }

  void getMsgDigest(Digest& outDigest) const;

  void addElement(SeqNum seqNum,
                  const Digest& prePrepareDigest,
                  ViewNum originView,
                  bool hasPreparedCertificate,
                  ViewNum certificateView,
                  uint16_t certificateSigLength,
                  const char* certificateSig);

  void finalizeMessage();

  void validate(const ReplicasInfo&) const override;

  class ElementsIterator {
   public:
    // this ctor assumes that m is a legal ViewChangeMsg message (as defined by checkElements() )
    ElementsIterator(const ViewChangeMsg* const m);

    bool getCurrent(Element*& pElement);

    bool end();

    void gotoNext();

    bool getAndGoToNext(Element*& pElement);

    bool goToAtLeast(SeqNum lowerBound);

   protected:
    const ViewChangeMsg* const msg;
    size_t endLoc;
    size_t currLoc;
    uint16_t nextElementNum;  // used for debug
  };

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    ReplicaId genReplicaId;  // the replica that originally generated this message
    ViewNum newView;         // the new view
    SeqNum lastStable;
    uint16_t numberOfElements;
    size_t locationAfterLast;  // if(numberOfElements > 0) then it holds the location after the last element
                               // followed by a sequence of Element
                               // followed by a signature (by genReplicaId)
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 2 + 8 + 8 + 2 + 8), "Header is 36B");

  Header* b() const { return ((Header*)msgBody_); }

  bool checkElements(uint16_t sigSize) const;
};

template <>
inline MsgSize maxMessageSize<ViewChangeMsg>() {
  return ReplicaConfigSingleton::GetInstance().GetMaxExternalMessageSize() + MessageBase::SPAN_CONTEXT_MAX_SIZE;
}

}  // namespace impl
}  // namespace bftEngine
