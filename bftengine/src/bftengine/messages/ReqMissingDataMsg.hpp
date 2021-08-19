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
#include "OpenTracing.hpp"

#include <iostream>
#include <sstream>
#include <bitset>

namespace bftEngine {
namespace impl {

class ReqMissingDataMsg : public MessageBase {
 public:
  ReqMissingDataMsg(ReplicaId senderId,
                    ViewNum v,
                    SeqNum s,
                    const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(ReqMissingDataMsg)

  ViewNum viewNumber() const { return b()->viewNum; }

  SeqNum seqNumber() const { return b()->seqNum; }

  bool getPrePrepareIsMissing() const { return b()->flags.bits.prePrepareIsMissing != 0; }
  bool getPartialProofIsMissing() const { return b()->flags.bits.partialProofIsMissing != 0; }
  bool getPartialPrepareIsMissing() const { return b()->flags.bits.partialPrepareIsMissing != 0; }
  bool getPartialCommitIsMissing() const { return b()->flags.bits.partialCommitIsMissing != 0; }
  bool getFullCommitProofIsMissing() const { return b()->flags.bits.fullCommitProofIsMissing != 0; }
  bool getFullPrepareIsMissing() const { return b()->flags.bits.fullPrepareIsMissing != 0; }
  bool getFullCommitIsMissing() const { return b()->flags.bits.fullCommitIsMissing != 0; }
  bool getSlowPathHasStarted() const { return b()->flags.bits.slowPathHasStarted != 0; }

  uint16_t getFlags() const { return b()->flags.flags; }
  std::string getFlagsAsBits() const {
    std::stringstream sst;
    sst << std::bitset<9>(b()->flags.flags);
    return sst.str();
  };

  void resetFlags();

  void setPrePrepareIsMissing() { b()->flags.bits.prePrepareIsMissing = 1; }
  void setPartialProofIsMissing() { b()->flags.bits.partialProofIsMissing = 1; }
  void setPartialPrepareIsMissing() { b()->flags.bits.partialPrepareIsMissing = 1; }
  void setPartialCommitIsMissing() { b()->flags.bits.partialCommitIsMissing = 1; }
  void setFullCommitProofIsMissing() { b()->flags.bits.fullCommitProofIsMissing = 1; }
  void setFullPrepareIsMissing() { b()->flags.bits.fullPrepareIsMissing = 1; }
  void setFullCommitIsMissing() { b()->flags.bits.fullCommitIsMissing = 1; }
  void setSlowPathHasStarted() { b()->flags.bits.slowPathHasStarted = 1; }

  void validate(const ReplicasInfo&) const override;

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct BitFields {
    uint8_t reserved : 1;
    uint8_t prePrepareIsMissing : 1;
    uint8_t partialProofIsMissing : 1;
    uint8_t partialPrepareIsMissing : 1;
    uint8_t partialCommitIsMissing : 1;
    uint8_t fullCommitProofIsMissing : 1;
    uint8_t fullPrepareIsMissing : 1;
    uint8_t fullCommitIsMissing : 1;
    uint8_t slowPathHasStarted : 1;
  };

  union Flags {
    BitFields bits;
    uint16_t flags;
  };

  struct Header : public MessageBase::Header {
    ViewNum viewNum;
    SeqNum seqNum;
    EpochNum epochNum;
    Flags flags;
  };
#pragma pack(pop)
  static_assert(sizeof(Flags) == sizeof(uint16_t));
  static_assert(sizeof(Header) == (6 + 8 + 8 + 8 + 2), "Header is 32B");

  Header* b() const { return (Header*)msgBody_; }
};
}  // namespace impl
}  // namespace bftEngine
