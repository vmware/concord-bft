// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "messages/ClientRequestMsg.hpp"
#include "SimpleClient.hpp"
#include <list>

namespace preprocessor {

// PreProcessResultMsg is a ClientRequestMsg which have been preprocessed. It is inserted in the message queue of
// the primary and is bundled in PrePrepare with other PreProcessResultMsg or ClientRequestMsg.
//
// The message inherits from ClientRequestMsg mainly because it needs to be bundled with ClientRequestMsg in PrePrepare.
// Code reuse is a side benefit, but not the main reason for the inheritance.
//
// The PreExecution implementation before NonRepudiation improvement reused ClientRequestMsg instead of
// PreProcessResultMsg. This was convenient but lead to some 'hacks' which should be considered when working with this
// code.
//
// If ClientRequestMsg is used as 'internal' message in the PreExecution a special flag (PRE_PROCESS_REQ) is set. In
// this case the request field of the message contains the pre-executed result instead of the request itself. As
// PreProcessResultMsg should respect this approach (for now), the result buffer in the constructor below is passed as a
// request to the ClientRequestMsg constructor.
//
// Ideally PrePrepare message should be reworked to bundle ClientRequestMsg and PreProcessResultMsg messages. Then the
// inheritance for this message won't be needed. For the moment (Jun 2021) this change is not implemented though.
class PreProcessResultMsg : public ClientRequestMsg {
 public:
  using ErrorMessage = std::optional<std::string>;

  PreProcessResultMsg(NodeIdType sender,
                      uint64_t reqSeqNum,
                      uint32_t resultLength,
                      const char* result,
                      uint64_t reqTimeoutMilli,
                      const std::string& cid = "",
                      const concordUtils::SpanContext& spanContext = concordUtils::SpanContext{},
                      const char* messageSignature = nullptr,
                      uint32_t messageSignatureLen = 0,
                      const std::string& resultSignatures = "");

  PreProcessResultMsg(bftEngine::ClientRequestMsgHeader* body);

  PreProcessResultMsg(MessageBase* msgBase) : ClientRequestMsg(msgBase) {}

  std::pair<char*, uint32_t> getResultSignaturesBuf();
  ErrorMessage validatePreProcessResultSignatures(ReplicaId myReplicaId, int16_t fVal);
};

struct PreProcessResultSignature {
  std::vector<char> signature;
  NodeIdType sender_replica;
  bftEngine::OperationResult pre_process_result;

  PreProcessResultSignature() = default;

  PreProcessResultSignature(std::vector<char>&& sig, NodeIdType sender, bftEngine::OperationResult result)
      : signature{std::move(sig)}, sender_replica{sender}, pre_process_result{result} {}

  bool operator==(const PreProcessResultSignature& rhs) const {
    return signature == rhs.signature && sender_replica == rhs.sender_replica &&
           pre_process_result == rhs.pre_process_result;
  }

  const bftEngine::OperationResult getPreProcessResult() const { return pre_process_result; }

  static std::string serializeResultSignatureList(const std::list<PreProcessResultSignature>& sigs);
  static std::list<PreProcessResultSignature> deserializeResultSignatureList(const char* buf, size_t len);
};

}  // namespace preprocessor
