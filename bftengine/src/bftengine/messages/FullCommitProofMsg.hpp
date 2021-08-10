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

namespace bftEngine {
namespace impl {

// TODO(GG): use SignedShareBase
class FullCommitProofMsg : public MessageBase {
 public:
  FullCommitProofMsg(ReplicaId senderId,
                     ViewNum v,
                     SeqNum s,
                     EpochNum e,
                     const char* commitProofSig,
                     uint16_t commitProofSigLength,
                     const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(FullCommitProofMsg)

  ViewNum viewNumber() const { return b()->viewNum; }

  SeqNum seqNumber() const { return b()->seqNum; }

  uint16_t thresholSignatureLength() const { return b()->thresholSignatureLength; }

  const char* thresholSignature() { return body() + sizeof(Header) + spanContextSize(); }

  void validate(const ReplicasInfo&) const override;

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    ViewNum viewNum;
    SeqNum seqNum;
    EpochNum epochNum;
    uint16_t thresholSignatureLength;
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 8 + 8 + 8 + 2), "Header is 32B");

  Header* b() const { return (Header*)msgBody_; }
};

}  // namespace impl
}  // namespace bftEngine
