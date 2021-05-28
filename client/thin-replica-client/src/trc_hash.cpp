// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "client/thin-replica-client/trc_hash.hpp"

#include <map>

#include "boost/detail/endian.hpp"

using com::vmware::concord::thin_replica::Data;
using std::invalid_argument;
using std::list;
using std::map;
using std::string;

namespace client::thin_replica_client {

// Hash functions in this file may be defined in a way assuming char is an 8 bit
// type.
static_assert(CHAR_BIT == 8);

// Helper function for computing the hash of an update from the hashes of the
// keys and values it contains (given those hashes in an std::map). This helper
// factors out logic common to the two overloads of hashUpdate implemented
// below.
//
// Note we store the hashes of the keys and values in an std::map (rather than
// an std::unordered_map) as an intermediate step in the computation of the
// update hash so the map can be used for detecting duplicate keys and
// deterministically ordering the key-value pairs' hashes before they are
// concatenated for the computation of the update hash. The deterministic
// ordering is necessary since two updates with the same set of key-value pairs
// in different orders are considered equivalent so their hashes need to match.
static string hashUpdateFromEntryHashes(uint64_t block_id, const map<string, string>& entry_hashes) {
  string concatenated_entry_hashes;
  concatenated_entry_hashes.reserve(sizeof(block_id) + 2 * entry_hashes.size() * kExpectedSHA256HashLengthInBytes);

#ifdef BOOST_LITTLE_ENDIAN
  concatenated_entry_hashes.append(reinterpret_cast<const char*>(&block_id), sizeof(block_id));
#else  // BOOST_LITTLE_ENDIAN not defined in this case
#ifndef BOOST_BIG_ENDIAN
  static_assert(false,
                "Cannot determine endianness (needed for Thin Replica "
                "mechanism hash function).");
#endif  // BOOST_BIG_ENDIAN defined
  const char* block_id_as_bytes = reinterpret_cast<const char*>(&block_id);
  for (size_t i = 1; i <= sizeof(block_id); ++i) {
    concatenated_entry_hashes.append((block_id_as_bytes + (sizeof(block_id) - i)), 1);
  }
#endif  // if BOOST_LITTLE_ENDIAN defined/else

  for (const auto& kvp_hashes : entry_hashes) {
    concatenated_entry_hashes.append(kvp_hashes.first);
    concatenated_entry_hashes.append(kvp_hashes.second);
  }

  return ComputeSHA256Hash(concatenated_entry_hashes);
}

string hashUpdate(const Update& update) {
  map<string, string> entry_hashes;
  for (const auto& kvp : update.kv_pairs) {
    string key_hash = ComputeSHA256Hash(kvp.first);
    if (entry_hashes.count(key_hash) > 0) {
      throw invalid_argument("hashUpdate called for an update that contains duplicate keys.");
    }
    entry_hashes[key_hash] = ComputeSHA256Hash(kvp.second);
  }

  return hashUpdateFromEntryHashes(update.block_id, entry_hashes);
}

string hashUpdate(const Data& update) {
  map<string, string> entry_hashes;
  for (const auto& kvp : update.data()) {
    string key_hash = ComputeSHA256Hash(kvp.key());
    if (entry_hashes.count(key_hash) > 0) {
      throw invalid_argument("hashUpdate called for an update that contains duplicate keys.");
    }
    entry_hashes[key_hash] = ComputeSHA256Hash(kvp.value());
  }

  return hashUpdateFromEntryHashes(update.block_id(), entry_hashes);
}

string hashState(const list<string>& state) {
  string concatenated_update_hashes;
  concatenated_update_hashes.reserve(state.size() * kThinReplicaHashLength);
  for (const auto& update_hash : state) {
    if (update_hash.length() != kThinReplicaHashLength) {
      throw invalid_argument(
          "hashState called with a list of state hashes containing a hash of "
          "an unexpected length.");
    }
    concatenated_update_hashes.append(update_hash);
  }

  return ComputeSHA256Hash(concatenated_update_hashes);
}

}  // namespace client::thin_replica_client
