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
#include <memory>

class IThresholdSigner;

namespace bftEngine {
namespace impl {

// TODO(GG): consider to use SignedShareBase
class PartialCommitProofMsg : public MessageBase {
 public:
  PartialCommitProofMsg(ReplicaId senderId,
                        ViewNum v,
                        SeqNum s,
                        CommitPath commitPath,
                        Digest& digest,
                        std::shared_ptr<IThresholdSigner> thresholdSigner,
                        const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{});

  BFTENGINE_GEN_CONSTRUCT_FROM_BASE_MESSAGE(PartialCommitProofMsg)

  ViewNum viewNumber() const { return b()->viewNum; }

  SeqNum seqNumber() const { return b()->seqNum; }

  CommitPath commitPath() const { return b()->commitPath; }

  uint16_t thresholSignatureLength() const { return b()->thresholSignatureLength; }

  const char* thresholSignature() const { return body() + sizeof(Header) + b()->header.spanContextSize; }

  void validate(const ReplicasInfo&) const override;

  // ToDo--EDJ: Temp add method names used by CollectorOfThresholdSignatures
  uint16_t signatureLen() const { return thresholSignatureLength(); }
  const char* signatureBody() { return thresholSignature(); }

 protected:
  template <typename MessageT>
  friend size_t sizeOfHeader();

#pragma pack(push, 1)
  struct Header {
    MessageBase::Header header;
    ViewNum viewNum;
    SeqNum seqNum;
    EpochNum epochNum;
    CommitPath commitPath;  // TODO(GG): can never be SLOW. Replace with a simple flag
    uint16_t thresholSignatureLength;
    // followed by a partial signature
  };
#pragma pack(pop)
  static_assert(sizeof(Header) == (6 + 8 + 8 + 8 + sizeof(CommitPath) + 2), "Header is 32B+sizeof(CommitPath)");

  Header* b() const { return (Header*)msgBody_; }
};

}  // namespace impl
}  // namespace bftEngine
