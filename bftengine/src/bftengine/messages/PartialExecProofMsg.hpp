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

// TODO(GG): use SignedShareBase
class PartialExecProofMsg : public MessageBase {
 public:
  PartialExecProofMsg(ReplicaId senderId,
                      ViewNum v,
                      SeqNum s,
                      Digest& digest,
                      IThresholdSigner* thresholdSigner,
                      const std::string& spanContext = "");

  ViewNum viewNumber() const { return b()->viewNum; }

  SeqNum seqNumber() const { return b()->seqNum; }

  uint16_t thresholSignatureLength() const { return b()->thresholSignatureLength; }

  const char* thresholSignature() { return body() + sizeof(PartialExecProofMsgHeader) + msgBody_->span_context_size; }

  void validate(const ReplicasInfo&) const override;

  std::string spanContext() const override {
    return std::string(body() + sizeof(PartialExecProofMsgHeader), msgBody_->span_context_size);
  }

 protected:
#pragma pack(push, 1)
  struct PartialExecProofMsgHeader {
    MessageBase::Header header;
    ViewNum viewNum;
    SeqNum seqNum;
    uint16_t thresholSignatureLength;
    // followed by a signature
  };
#pragma pack(pop)
  static_assert(sizeof(PartialExecProofMsgHeader) == (6 + 8 + 8 + 2), "PartialExecProofMsgHeader is 24B");

  PartialExecProofMsgHeader* b() const { return (PartialExecProofMsgHeader*)msgBody_; }
};

}  // namespace impl
}  // namespace bftEngine
