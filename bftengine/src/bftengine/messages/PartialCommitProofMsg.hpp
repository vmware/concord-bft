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
                        IThresholdSigner* thresholdSigner,
                        const std::string& span_context = "");

  ViewNum viewNumber() const { return b()->viewNum; }

  SeqNum seqNumber() const { return b()->seqNum; }

  CommitPath commitPath() const { return b()->commitPath; }

  uint16_t thresholSignatureLength() const { return b()->thresholSignatureLength; }

  const char* thresholSignature() const {
    return body() + sizeof(PartialCommitProofMsgHeader) + b()->header.span_context_size;
  }

  std::string spanContext() const override {
    return std::string(body() + sizeof(PartialCommitProofMsgHeader), spanContextSize());
  }

  void validate(const ReplicasInfo&) const override;

 protected:
#pragma pack(push, 1)
  struct PartialCommitProofMsgHeader {
    MessageBase::Header header;
    ViewNum viewNum;
    SeqNum seqNum;
    CommitPath commitPath;  // TODO(GG): can never be SLOW. Replace with a simple flag
    uint16_t thresholSignatureLength;
    // followed by a partial signature
  };
#pragma pack(pop)
  static_assert(sizeof(PartialCommitProofMsgHeader) == (6 + 8 + 8 + sizeof(CommitPath) + 2),
                "PartialCommitProofMsgHeader is 24B+sizeof(CommitPath)");

  PartialCommitProofMsgHeader* b() const { return (PartialCommitProofMsgHeader*)msgBody_; }
};

}  // namespace impl
}  // namespace bftEngine
