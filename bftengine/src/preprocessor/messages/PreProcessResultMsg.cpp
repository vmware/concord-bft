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

#include "PreProcessResultMsg.hpp"
#include "Replica.hpp"  // for HAS_PRE_PROCESSED_FLAG
#include "SigManager.hpp"
#include "PreProcessResultHashCreator.hpp"
#include "endianness.hpp"

namespace preprocessor {

using namespace bftEngine;

PreProcessResultMsg::PreProcessResultMsg(NodeIdType sender,
                                         uint32_t preProcessResult,
                                         uint64_t reqSeqNum,
                                         uint32_t resultLength,
                                         const char* resultBuf,
                                         uint64_t reqTimeoutMilli,
                                         const std::string& cid,
                                         const concordUtils::SpanContext& spanContext,
                                         const char* messageSignature,
                                         uint32_t messageSignatureLen,
                                         const std::string& resultSignatures)
    : ClientRequestMsg(sender,
                       HAS_PRE_PROCESSED_FLAG,
                       reqSeqNum,
                       resultLength,  // check the comment in the header to
                       resultBuf,     // understand why result is passed as request here
                       reqTimeoutMilli,
                       cid,
                       preProcessResult,
                       spanContext,
                       messageSignature,
                       messageSignatureLen,
                       resultSignatures.size()) {
  msgBody_->msgType = MsgCode::PreProcessResult;
  // ClientRequestMsg allocates additional memory for the signatures.
  // Get pointer to it here and assert if the buffer is not big enough.
  auto [pos, max_len] = getExtraBufPtr();
  ConcordAssert(max_len >= resultSignatures.size());
  memcpy(pos, resultSignatures.data(), resultSignatures.size());
}

PreProcessResultMsg::PreProcessResultMsg(ClientRequestMsgHeader* body) : ClientRequestMsg(body) {}

std::pair<char*, uint32_t> PreProcessResultMsg::getResultSignaturesBuf() { return getExtraBufPtr(); }

std::optional<std::string> PreProcessResultMsg::validatePreProcessResultSignatures(ReplicaId myReplicaId,
                                                                                   int16_t fVal) {
  auto sigManager_ = SigManager::instance();
  const auto [buf, buf_len] = getResultSignaturesBuf();
  auto sigs = preprocessor::PreProcessResultSignature::deserializeResultSignatures(buf, buf_len);

  // f+1 signatures are required. Primary always sends exactly f+1 signatures so the signature count should always be
  // f+1. In theory more signatures can be accepted but this makes the non-primary replicas vulnerable to DoS attacks
  // from a malicious primary (e.g. a Primary sends a PreProcessResult message with 100 signatures, which should be
  // validated by the replica).
  std::stringstream err;
  const auto expectedSignatureCount = fVal + 1;
  if (sigs.size() != static_cast<uint16_t>(expectedSignatureCount)) {
    err << "PreProcessResult signatures validation failure - unexpected number of signatures received"
        << KVLOG(expectedSignatureCount, sigs.size(), clientProxyId(), getCid(), requestSeqNum());
    return err.str();
  }

  // At this stage the pre_process_result field has to be the same for all senders.
  auto hash = PreProcessResultHashCreator::create(
      requestBuf(), requestLength(), sigs.begin()->pre_process_result, clientProxyId(), requestSeqNum());

  for (const auto& sig : sigs) {
    bool verificationResult = false;
    if (myReplicaId == sig.sender_replica) {
      std::vector<char> mySignature(sigManager_->getMySigLength(), '\0');
      sigManager_->sign(
          reinterpret_cast<const char*>(hash.data()), hash.size(), mySignature.data(), mySignature.size());
      verificationResult = mySignature == sig.signature;
    } else {
      verificationResult = sigManager_->verifySig(
          sig.sender_replica, (const char*)hash.data(), hash.size(), sig.signature.data(), sig.signature.size());
    }

    if (!verificationResult) {
      err << "PreProcessResult signatures validation failure - invalid signature received from replica"
          << KVLOG(sig.sender_replica, clientProxyId(), getCid(), requestSeqNum());
      return err.str();
    }
  }

  return {};
}

std::string PreProcessResultSignature::serializeResultSignatures(const std::set<PreProcessResultSignature>& signatures,
                                                                 const uint16_t numOfRequiredSignatures) {
  size_t buf_len = 0;
  auto i = 0;
  for (const auto& s : signatures) {
    buf_len += sizeof(s.sender_replica) + sizeof(s.pre_process_result) + sizeof(uint32_t) + s.signature.size();
    if (++i == numOfRequiredSignatures) break;
  }

  std::string output;
  output.reserve(buf_len);
  i = 0;
  for (const auto& s : signatures) {
    output.append(concordUtils::toBigEndianStringBuffer(s.sender_replica));
    output.append(concordUtils::toBigEndianStringBuffer(static_cast<uint32_t>(s.pre_process_result)));
    output.append(concordUtils::toBigEndianStringBuffer<uint32_t>(s.signature.size()));
    output.append(s.signature.begin(), s.signature.end());
    if (++i == numOfRequiredSignatures) break;
  }

  return output;
}

std::set<PreProcessResultSignature> PreProcessResultSignature::deserializeResultSignatures(const char* buf,
                                                                                           size_t len) {
  size_t pos = 0;
  std::set<PreProcessResultSignature> ret;
  while (1) {
    impl::NodeIdType sender_id;
    uint32_t signature_size;
    OperationResult pre_process_result;

    if (sizeof(sender_id) + sizeof(pre_process_result) + sizeof(signature_size) > len - pos) {
      throw std::runtime_error("Deserialization error - remaining buffer length is less than fixed size values size");
    }

    // Read fixed size values
    sender_id = concordUtils::fromBigEndianBuffer<impl::NodeIdType>(buf + pos);
    pos += sizeof(impl::NodeIdType);
    pre_process_result = static_cast<OperationResult>(concordUtils::fromBigEndianBuffer<uint32_t>(buf + pos));
    pos += sizeof(uint32_t);
    signature_size = concordUtils::fromBigEndianBuffer<uint32_t>(buf + pos);
    pos += sizeof(uint32_t);

    if (signature_size > len - pos) {
      throw std::runtime_error("Deserialization error - remaining buffer length is less than a signature size");
    }

    ret.emplace(std::vector<char>(buf + pos, buf + pos + signature_size), sender_id, pre_process_result);
    pos += signature_size;

    if (len - pos == 0) {
      break;
    }
  }

  return ret;
}

}  // namespace preprocessor