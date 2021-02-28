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

#pragma once

#include "concord.cmf.hpp"
#include <string>
#include <type_traits>
#include <unordered_set>
#include <bftengine/IStateTransfer.hpp>
#include <db_interfaces.h>
#include <future>
#include "Crypto.hpp"
#include "bftengine/ControlStateManager.hpp"

namespace concord::reconfiguration::pruning {

// This class signs pruning messages via the replica's private key that it gets
// through the configuration. Message contents used to generate the signature
// are generated via the mechanisms provided in pruning_serialization.hpp/cpp .
class RSAPruningSigner {
 public:
  // Construct by passing the configuration for the node the signer is running
  // on.
  RSAPruningSigner(const std::string &key);
  // Sign() methods sign the passed message and store the signature in the
  // 'signature' field of the message. An exception is thrown on error.
  //
  // Note RSAPruningSigner does not handle signing of PruneRequest messages on
  // behalf of the operator, as the operator's signature is a dedicated-purpose
  // application-level signature rather than a Concord-BFT Principal's RSA
  // signature.
  void Sign(concord::messages::LatestPrunableBlock &) const;

 private:
  std::string GetSignatureBuffer() const;

 private:
  bftEngine::impl::RSASigner signer_;
};

// This class verifies pruning messages that were signed by serializing message
// contents via mechanisms provided in pruning_serialization.hpp/cpp . Public
// keys for verification are extracted from the passed configuration.
//
// Idea is to use the principal_id as an ID that identifies senders in pruning
// messages since it is unique across clients and replicas.
class RSAPruningVerifier {
 public:
  // Construct by passing the system configuration.
  RSAPruningVerifier(const std::set<std::pair<uint16_t, const std::string>> &replicasPublicKeys);
  // Verify() methods verify that the message comes from the advertised sender.
  // Methods return true on successful verification and false on unsuccessful.
  // An exception is thrown on error.
  //
  // Note RSAPruningVerifier::Verify(const com::vmware::concord::PruneRequest&)
  // handles verification of the LatestPrunableBlock message(s) contained within
  // the PruneRequest, but does not itself handle verification of the issuing
  // operator's signature of the pruning command, as the operator's signature is
  // a dedicated application-level signature rather than one of the Concord-BFT
  // Principal's RSA signatures.
  bool Verify(const concord::messages::LatestPrunableBlock &) const;
  bool Verify(const concord::messages::PruneRequest &) const;

 private:
  struct Replica {
    std::uint64_t principal_id{0};
    bftEngine::impl::RSAVerifier verifier;
  };

  bool Verify(std::uint64_t sender, const std::string &ser, const std::string &signature) const;

  using ReplicaVector = std::vector<Replica>;

  // Get a replica from the replicas vector by its index.
  const Replica &GetReplica(ReplicaVector::size_type idx) const;

  // A vector of all the replicas in the system.
  ReplicaVector replicas_;
  // We map a principal_id to a replica index in the replicas_ vector to be able
  // to verify a message through the Replica's verifier that is associated with
  // its public key.
  std::unordered_map<std::uint64_t, ReplicaVector::size_type> principal_to_replica_idx_;

  // Contains a set of replica principal_ids for use in verification. Filled in
  // once during construction.
  std::unordered_set<std::uint64_t> replica_ids_;
};
}  // namespace concord::reconfiguration::pruning