// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "reconfiguration/pruning_utils.hpp"
#include "bftengine/ReplicaConfig.hpp"

namespace concord::reconfiguration::pruning {
namespace details {
std::string &operator<<(std::string &buf, const concord::messages::LatestPrunableBlock &block) {
  buf << block.replica << block.block_id;
  buf += std::string(block.signature.begin(), block.signature.end());

  return buf;
}
}  // namespace details
RSAPruningSigner::RSAPruningSigner() : signer_{bftEngine::ReplicaConfig::instance().replicaPrivateKey.c_str()} {}

void RSAPruningSigner::Sign(concord::messages::LatestPrunableBlock &block) const {
  std::ostringstream oss;
  std::string ser;
  oss << block.replica << block.block_id;
  ser = oss.str();
  auto signature = GetSignatureBuffer();
  size_t actual_sign_len{0};
  const auto res =
      signer_.sign(ser.c_str(), ser.length(), signature.data(), signer_.signatureLength(), actual_sign_len);
  if (!res) {
    throw PruningRuntimeException{"RSAPruningSigner failed to sign a LatestPrunableBlock message"};
  } else if (actual_sign_len < signature.length()) {
    signature.resize(actual_sign_len);
  }

  block.signature = signature;
}

std::string RSAPruningSigner::GetSignatureBuffer() const {
  const auto sign_len = signer_.signatureLength();
  return std::string(sign_len, '\0');
}

RSAPruningVerifier::RSAPruningVerifier() {
  auto i = 0u;
  auto &replicas_rsa_pkeys_ = bftEngine::ReplicaConfig::instance().publicKeysOfReplicas;
  for (auto &[idx, pkey] : replicas_rsa_pkeys_) {
    replicas_.push_back(Replica{idx, pkey.c_str()});
    const auto ins_res = replica_ids_.insert(replicas_.back().principal_id);
    if (!ins_res.second) {
      throw PruningConfigurationException{"RSAPruningVerifier found duplicate replica principal_id: " +
                                          std::to_string(replicas_.back().principal_id)};
    }

    const auto &replica = replicas_.back();
    principal_to_replica_idx_[replica.principal_id] = i;
    i++;
  }
}

bool RSAPruningVerifier::Verify(const concord::messages::LatestPrunableBlock &block) const {
  // LatestPrunableBlock can only be sent by replicas and not by client proxies.
  if (replica_ids_.find(block.replica) == std::end(replica_ids_)) {
    return false;
  }
  std::ostringstream oss;
  std::string ser;
  oss << block.replica << block.block_id;
  ser = oss.str();
  return Verify(block.replica, ser, block.signature);
}

bool RSAPruningVerifier::Verify(const concord::messages::PruneRequest &request) const {
  if (request.latest_prunable_block.size() != static_cast<size_t>(replica_ids_.size())) {
    return false;
  }

  // PruneRequest can only be sent by client proxies and not by replicas.
  if (replica_ids_.find(request.sender) != std::end(replica_ids_)) {
    return false;
  }

  // Note RSAPruningVerifier does not handle verification of the operator's
  // signature authorizing this pruning order, as the operator's signature is a
  // dedicated application-level signature rather than one of the Concord-BFT
  // principals' RSA signatures.

  // Verify that *all* replicas have responded with valid responses.
  auto replica_ids_to_verify = replica_ids_;
  for (auto &block : request.latest_prunable_block) {
    if (!Verify(block)) {
      return false;
    }
    auto it = replica_ids_to_verify.find(block.replica);
    if (it == std::end(replica_ids_to_verify)) {
      return false;
    }
    replica_ids_to_verify.erase(it);
  }
  return replica_ids_to_verify.empty();
}

bool RSAPruningVerifier::Verify(std::uint64_t sender, const std::string &ser, const std::string &signature) const {
  auto it = principal_to_replica_idx_.find(sender);
  if (it == std::cend(principal_to_replica_idx_)) {
    return false;
  }

  return GetReplica(it->second).verifier.verify(ser.data(), ser.length(), signature.c_str(), signature.length());
}

const RSAPruningVerifier::Replica &RSAPruningVerifier::GetReplica(ReplicaVector::size_type idx) const {
  return replicas_[idx];
}
}  // namespace concord::reconfiguration::pruning