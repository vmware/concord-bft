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

#include <string.h>
#include "FullExecProofMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace impl {

FullExecProofMsg::FullExecProofMsg(ReplicaId senderId,
                                   NodeIdType clientId,
                                   ReqId requestId,
                                   uint16_t sigLength,
                                   const char* root,
                                   uint16_t rootLength,
                                   const char* executionProof,
                                   uint16_t proofLength,
                                   const std::string& spanContext)
    : MessageBase(
          senderId, MsgCode::FullExecProof, spanContext.size(), sizeof(Header) + sigLength + rootLength + proofLength) {
  b()->isNotReady = 1;  // message is not ready
  b()->idOfClient = clientId;
  b()->requestId = requestId;
  b()->signatureLength = sigLength;
  b()->merkleRootLength = rootLength;
  b()->executionProofLength = proofLength;

  auto position = body() + sizeof(Header);
  memcpy(position, spanContext.data(), spanContext.size());
  position += spanContext.size();
  memcpy(position, root, rootLength);
  position += rootLength;
  memcpy(position, executionProof, proofLength);
}

FullExecProofMsg::FullExecProofMsg(ReplicaId senderId,
                                   NodeIdType clientId,
                                   const char* sig,
                                   uint16_t sigLength,
                                   const char* root,
                                   uint16_t rootLength,
                                   const char* readProof,
                                   uint16_t proofLength,
                                   const std::string& spanContext)
    : MessageBase(
          senderId, MsgCode::FullExecProof, spanContext.size(), sizeof(Header) + sigLength + rootLength + proofLength) {
  b()->isNotReady = 0;  // message is ready
  b()->idOfClient = clientId;
  b()->requestId = 0;
  b()->signatureLength = sigLength;
  b()->merkleRootLength = rootLength;
  b()->executionProofLength = proofLength;

  auto position = body() + sizeof(Header);
  memcpy(position, spanContext.data(), spanContext.size());
  position += spanContext.size();

  memcpy(position, root, rootLength);
  position += rootLength;

  memcpy(position, readProof, proofLength);
  position += proofLength;

  memcpy(position, sig, sigLength);
}

void FullExecProofMsg::setSignature(const char* sig, uint16_t sigLength) {
  Assert(b()->signatureLength >= sigLength);

  memcpy(
      body() + sizeof(Header) + spanContextSize() + b()->merkleRootLength + b()->executionProofLength, sig, sigLength);

  b()->signatureLength = sigLength;

  b()->isNotReady = 0;  // message is ready now
}

void FullExecProofMsg::validate(const ReplicasInfo&) const {
  if (size() < sizeof(Header) + spanContextSize()) throw std::runtime_error(__PRETTY_FUNCTION__);

  // TODO(GG)
}

}  // namespace impl
}  // namespace bftEngine
