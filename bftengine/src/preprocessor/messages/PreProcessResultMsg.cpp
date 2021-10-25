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
#include "endianness.hpp"
#include "sha_hash.hpp"

namespace preprocessor {

PreProcessResultMsg::PreProcessResultMsg(NodeIdType sender,
                                         uint64_t reqSeqNum,
                                         uint32_t resultLength,
                                         const char* result,
                                         uint64_t reqTimeoutMilli,
                                         const std::string& cid,
                                         const concordUtils::SpanContext& spanContext,
                                         const char* messageSignature,
                                         uint32_t messageSignatureLen,
                                         const std::string& resultSignatures)
    : ClientRequestMsg(sender,
                       bftEngine::HAS_PRE_PROCESSED_FLAG,
                       reqSeqNum,
                       resultLength,  // check the comment in the header to
                       result,        // understand why result is passed as request here
                       reqTimeoutMilli,
                       cid,
                       spanContext,
                       messageSignature,
                       messageSignatureLen,
                       resultSignatures.size()) {
  msgBody_->msgType = MsgCode::PreProcessResult;
  // ClientRequestMsg allocates additional memory for the signatures
  // Get pointer to it here and assert that the buffer is big enough
  auto [pos, max_len] = getExtraBufPtr();
  ConcordAssert(max_len >= resultSignatures.size());

  memcpy(pos, resultSignatures.data(), resultSignatures.size());
}

PreProcessResultMsg::PreProcessResultMsg(bftEngine::ClientRequestMsgHeader* body) : ClientRequestMsg(body) {}

std::pair<char*, uint32_t> PreProcessResultMsg::getResultSignaturesBuf() { return getExtraBufPtr(); }

std::optional<std::string> PreProcessResultMsg::validatePreProcessResultSignatures(ReplicaId myReplicaId,
                                                                                   int16_t fVal) {
  auto sigManager_ = SigManager::instance();
  const auto [buf, buf_len] = getResultSignaturesBuf();
  auto sigs = preprocessor::PreProcessResultSignature::deserializeResultSignatureList(buf, buf_len);

  // f+1 signatures are required. Primary always sends exactly f+1 signatures so the signature count should always be
  // f+1. In theory more signatures can be accepted but this makes the non-primary replicas vulnerable to DoS attacks
  // from a malicious primary (e.g. a Primary sends a PreProcessResult message with 100 signatures, which should be
  // validated by the replica).
  const auto expectedSignatureCount = fVal + 1;
  if (sigs.size() != static_cast<uint16_t>(expectedSignatureCount)) {
    std::stringstream err;
    err << "PreProcessResult signatures validation failure - unexpected number of signatures received. Expected "
        << expectedSignatureCount << " got " << sigs.size() << " " << KVLOG(clientProxyId(), getCid(), requestSeqNum());
    return err.str();
  }

  auto length = requestLength() > 256 ? 256 : requestLength();
  if (length == 256) LOG_DEBUG(GL, "DELTA validating hash is trancated");
  auto hash = concord::util::SHA3_256().digest(requestBuf(), length);
  std::unordered_set<bftEngine::impl::NodeIdType> seen_signatures;
  for (const auto& s : sigs) {
    // insert returns std::pair<iterator, bool>. The bool indicates if the element was created or it was already in the
    // set and no insertion was performed. The latter case indicates that we already have got a signature from this
    // replica.
    if (!seen_signatures.insert(s.sender_replica).second) {
      return "PreProcessResult signatures validation failure - got more than one signatures with the same sender id";
    }

    bool verificationResult = false;
    if (myReplicaId == s.sender_replica) {
      std::vector<char> mySignature(sigManager_->getMySigLength(), '\0');
      sigManager_->sign(
          reinterpret_cast<const char*>(hash.data()), hash.size(), mySignature.data(), mySignature.size());
      verificationResult = mySignature == s.signature;
    } else {
      verificationResult = sigManager_->verifySig(
          s.sender_replica, (const char*)hash.data(), hash.size(), s.signature.data(), s.signature.size());
    }

    if (!verificationResult) {
      std::stringstream err;
      err << "PreProcessResult signatures validation failure - invalid signature from replica " << s.sender_replica
          << " " << KVLOG(clientProxyId(), getCid(), requestSeqNum());
      return err.str();
    }
  }

  return {};
}

std::string PreProcessResultSignature::serializeResultSignatureList(
    const std::list<PreProcessResultSignature>& signatures) {
  size_t buf_len = 0;
  for (const auto& s : signatures) {
    buf_len += sizeof(s.sender_replica) + sizeof(uint32_t) + s.signature.size();
  }

  std::string output;
  output.reserve(buf_len);

  for (const auto& s : signatures) {
    output.append(concordUtils::toBigEndianStringBuffer(s.sender_replica));
    output.append(concordUtils::toBigEndianStringBuffer<uint32_t>(s.signature.size()));
    output.append(s.signature.begin(), s.signature.end());
  }

  return output;
}

std::list<PreProcessResultSignature> PreProcessResultSignature::deserializeResultSignatureList(const char* buf,
                                                                                               size_t len) {
  size_t pos = 0;
  std::list<PreProcessResultSignature> ret;
  while (1) {
    bftEngine::impl::NodeIdType sender_id;
    uint32_t signature_size;

    if (sizeof(sender_id) + sizeof(signature_size) > len - pos) {
      throw std::runtime_error(
          "PreProcessResultSignature deserialisation error - remaining buffer length less than fixed size values size");
    }

    // Read fixed size values
    sender_id = concordUtils::fromBigEndianBuffer<bftEngine::impl::NodeIdType>(buf + pos);
    pos += sizeof(bftEngine::impl::NodeIdType);
    signature_size = concordUtils::fromBigEndianBuffer<uint32_t>(buf + pos);
    pos += sizeof(uint32_t);

    if (signature_size > len - pos) {
      throw std::runtime_error(
          "PreProcessResultSignature deserialisation error - remaining buffer length less than signature size");
    }

    ret.emplace_back(std::vector<char>(buf + pos, buf + pos + signature_size), sender_id);
    pos += signature_size;

    if (len - pos == 0) {
      break;
    }
  }

  return ret;
}

}  // namespace preprocessor