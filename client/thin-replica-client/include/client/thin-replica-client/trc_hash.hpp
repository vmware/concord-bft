// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// Hash utility functions for internal use by the Thin Replica Client

#ifndef TRC_HASH_HPP_
#define TRC_HASH_HPP_

#include <list>
#include "openssl_crypto.hpp"
#include "thin_replica.grpc.pb.h"
#include "client/concordclient/trc_queue.hpp"

namespace client::thin_replica_client {

const size_t kThinReplicaHashLength = concord::util::openssl_utils::kExpectedSHA256HashLengthInBytes;

// Compute the Thin Replica Mechanism hash of a given update; the hash is
// returned as a byte string stored in an std::string and should be of length
// kThinReplicaHashLength. Throws an std::invalid_argument in the event the
// update contains any duplicated keys (which is something disallowed by the
// Thin Replica Mechanism).
std::string hashUpdate(const concord::client::concordclient::EventVariant& update);
std::string hashUpdate(const com::vmware::concord::thin_replica::Data& update);

// Compute the Thin Replica Mechanism hash of a given state (i.e. an ordered
// list of updates) from a list of strings storing byte strings for the hashes
// of each of the updates in the state. The input hashes in the list are
// expected to be of length kThinReplicaHashLength bytes, and the complete state
// hash is also returned as a byte string of length kThinReplicaHashLength
// bytes, stored in an std::string. Throws a std::invalid_argument if any of the
// strings in the input list have the wrong length for a Thin Replica mechanism
// hash.
std::string hashState(const std::list<std::string>& state);

}  // namespace client::thin_replica_client

#endif  // TRC_HASH_HPP_
