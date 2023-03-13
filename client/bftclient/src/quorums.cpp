// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use
// this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the
// subcomponent's license, as noted in the LICENSE file.

#include <algorithm>
#include <iostream>
#include "bftclient/quorums.h"
#include "bftclient/exception.h"

namespace bft::client {

MofN QuorumConverter::toMofN(const LinearizableQuorum& quorum) const {
  MofN new_quorum;
  new_quorum.wait_for = linearizable_quorum_size_;
  if (quorum.destinations.empty()) {
    // If the user doesn't provide destinations, send to all replicas
    std::cout << "Rachit:Linearizable:toMofN Empty quorum" << std::endl;
    new_quorum.destinations = all_replicas_;
  } else if (quorum.destinations.size() < new_quorum.wait_for) {
    throw BadQuorumConfigException(
        "Destination does not contain enough replicas for a linearizable quorum. Destination size: " +
        std::to_string(quorum.destinations.size()) +
        "is less than 2f + c + 1 = " + std::to_string(new_quorum.wait_for));
  } else {
    std::cout << "Rachit:Linearizable:toMofN:validate destinations" << std::endl;
    validateDestinations(quorum.destinations);
    new_quorum.destinations = quorum.destinations;
  }
  return new_quorum;
}

MofN QuorumConverter::toMofN(const ByzantineSafeQuorum& quorum) const {
  MofN new_quorum;
  new_quorum.wait_for = bft_safe_quorum_size_;
  if (quorum.destinations.empty()) {
    // If the user doesn't provide a destination, send to all replicas
    new_quorum.destinations = all_replicas_;
    std::cout << "Rachit:Byzantine:toMofN Empty quorum" << std::endl;
  } else if (quorum.destinations.size() < new_quorum.wait_for) {
    throw BadQuorumConfigException(
        "Destination does not contain enough replicas for a byzantine fault tolerant quorum. Destination size: " +
        std::to_string(quorum.destinations.size()) + "is less than f + 1 = " + std::to_string(new_quorum.wait_for));
  } else {
    std::cout << "Rachit:Byzantine:toMofN validate destinations" << std::endl;
    validateDestinations(quorum.destinations);
    new_quorum.destinations = quorum.destinations;
  }
  return new_quorum;
}

MofN QuorumConverter::toMofN(const All& quorum) const {
  MofN new_quorum;
  if (quorum.destinations.empty()) {
    // If the user doesn't provide a destination, send to all replicas
    new_quorum.wait_for = all_replicas_.size();
    new_quorum.destinations = all_replicas_;
  } else {
    validateDestinations(quorum.destinations);
    new_quorum.wait_for = quorum.destinations.size();
    new_quorum.destinations = quorum.destinations;
  }
  return new_quorum;
}

MofN QuorumConverter::toMofN() const {
  MofN new_quorum;
  new_quorum.wait_for = 1;
  new_quorum.destinations.insert(*(all_replicas_.begin()));
  std::cout << "Rachit:OneNode:toMofN validate destinations" << std::endl;
  return new_quorum;
}

MofN QuorumConverter::toMofN(const MofN& quorum) const {
  if (quorum.wait_for > quorum.destinations.size()) {
    throw BadQuorumConfigException("Invalid MofN config: wait_for: " + std::to_string(quorum.wait_for) +
                                   " > destinations.size(): " + std::to_string(quorum.destinations.size()));
  }
  validateDestinations(quorum.destinations);
  return quorum;
}

void QuorumConverter::validateDestinations(const std::set<ReplicaId>& destinations) const {
  if (destinations.empty()) {
    throw InvalidDestinationException();
  }

  ReplicaId captured;
  if (std::any_of(destinations.begin(), destinations.end(), [this, &captured](auto replica_id) {
        captured = replica_id;
        return all_replicas_.count(replica_id) == 0 && ro_replicas_.count(replica_id) == 0;
      })) {
    throw InvalidDestinationException(captured);
  }
}

}  // namespace bft::client
