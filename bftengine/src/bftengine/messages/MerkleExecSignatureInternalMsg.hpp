// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "InternalMessage.hpp"
#include "InternalReplicaApi.hpp"

namespace bftEngine::impl {

class MerkleExecSignatureInternalMsg : public InternalMessage {
 public:
  MerkleExecSignatureInternalMsg(InternalReplicaApi* const internalReplicaApi,
                                 ViewNum viewNumber,
                                 SeqNum seqNumber,
                                 uint16_t signatureLength,
                                 const char* signature)
      : replicaApi_(internalReplicaApi) {
    viewNum_ = viewNumber;
    seqNum_ = seqNumber;
    signatureLength_ = signatureLength;
    char* p = (char*)std::malloc(signatureLength);
    memcpy(p, signature, signatureLength);
    signature_ = p;
  }

  ~MerkleExecSignatureInternalMsg() override { std::free((void*)signature_); }

  void handle() override { replicaApi_->onMerkleExecSignature(viewNum_, seqNum_, signatureLength_, signature_); }

 private:
  InternalReplicaApi* const replicaApi_;
  ViewNum viewNum_;
  SeqNum seqNum_;
  uint16_t signatureLength_;
  const char* signature_;
};

}  // namespace bftEngine::impl