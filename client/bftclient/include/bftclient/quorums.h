// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <set>
#include <variant>

#include "bftclient/base_types.h"

namespace bft::client {

// A quorum of 2F + C + 1 matching replies from `destination` must be received for the `send` call
// to complete.
struct LinearizableQuorum {
  std::set<ReplicaId> destinations;
};

// A quorum of F + 1 matching replies from `destination` must be received for the `send` call to complete.
struct ByzantineSafeQuorum {
  std::set<ReplicaId> destinations;
};

// A matching reply from every replica in `destination` must be received for the `send` call to complete.
struct All {
  std::set<ReplicaId> destinations;
};

// A matching reply from `wait_for` number of replicas from `destination` must be received for the
// `send` call to complete.
struct MofN {
  size_t wait_for;
  std::set<ReplicaId> destinations;
};

// Reads and writes support different types of quorums.
typedef std::variant<LinearizableQuorum, ByzantineSafeQuorum, All, MofN> ReadQuorum;
typedef std::variant<LinearizableQuorum, ByzantineSafeQuorum> WriteQuorum;

// Convert a ReadQuorum or a WriteQuorum to an MofN Quorum, given:
//  * All replicas in the cluster
//  * F value
//  * C value
class QuorumConverter {
 public:
  QuorumConverter(const std::set<ReplicaId>& all_replicas,
                  const std::set<ReplicaId>& ro_replicas,
                  uint16_t f_val,
                  uint16_t c_val)
      : all_replicas_(all_replicas),
        ro_replicas_(ro_replicas),
        linearizable_quorum_size_(2 * f_val + c_val + 1),
        bft_safe_quorum_size_(f_val + 1) {}

  // Convert Quorums to a compatible MofN quorum for use by the matcher.
  //
  // This conversion handles quorum validation.
  // The conversion from MofN to MofN only does validation.
  //
  // Throws BftClientException on error.
  MofN toMofN(const LinearizableQuorum& quorum) const;
  MofN toMofN(const ByzantineSafeQuorum& quorum) const;
  MofN toMofN(const All& quorum) const;
  MofN toMofN(const MofN& quorum) const;

 private:
  // Ensure that each replica in `destination` is part of `all_replicas`
  //
  // Throws an InvalidDestinationException if validation fails.
  void validateDestinations(const std::set<ReplicaId>& destinations) const;

  std::set<ReplicaId> all_replicas_;
  std::set<ReplicaId> ro_replicas_;
  size_t linearizable_quorum_size_;
  size_t bft_safe_quorum_size_;
};

}  // namespace bft::client
