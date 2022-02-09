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

#include "client/thin-replica-client/trc_hash.hpp"

#include <map>
#include <set>

#include "boost/detail/endian.hpp"
#include "assertUtils.hpp"

using com::vmware::concord::thin_replica::Data;
using concord::client::concordclient::EventVariant;
using concord::client::concordclient::EventGroup;
using concord::client::concordclient::Update;
using concord::util::openssl_utils::computeSHA256Hash;
using std::invalid_argument;
using std::list;
using std::map;
using std::set;
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
  concatenated_entry_hashes.reserve(sizeof(block_id) + 2 * entry_hashes.size() * kThinReplicaHashLength);

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

  return computeSHA256Hash(concatenated_entry_hashes);
}

static string hashUpdateFromEntryHashes(uint64_t id, const set<string>& entry_hashes) {
  string concatenated_entry_hashes;
  concatenated_entry_hashes.reserve(sizeof(id) + entry_hashes.size() * kThinReplicaHashLength);

#ifdef BOOST_LITTLE_ENDIAN
  concatenated_entry_hashes.append(reinterpret_cast<const char*>(&(id)), sizeof(id));
#else  // BOOST_LITTLE_ENDIAN not defined in this case
#ifndef BOOST_BIG_ENDIAN
  static_assert(false,
                "Cannot determine endianness (needed for Thin Replica "
                "mechanism hash function).");
#endif  // BOOST_BIG_ENDIAN defined
  const char* id_as_bytes = reinterpret_cast<const char*>(&(id));
  for (size_t i = 1; i <= sizeof(id); ++i) {
    concatenated_entry_hashes.append((id_as_bytes + (sizeof(id) - i)), 1);
  }
#endif  // if BOOST_LITTLE_ENDIAN defined/else

  for (const auto& hash : entry_hashes) {
    concatenated_entry_hashes.append(hash);
  }

  return computeSHA256Hash(concatenated_entry_hashes);
}

string hashUpdate(const EventVariant& ev) {
  if (std::holds_alternative<Update>(ev)) {
    auto& legacy_event = std::get<Update>(ev);
    map<string, string> entry_hashes;
    for (const auto& kvp : legacy_event.kv_pairs) {
      string key_hash = computeSHA256Hash(kvp.first);
      if (entry_hashes.count(key_hash) > 0) {
        throw invalid_argument("hashUpdate called for an update that contains duplicate keys.");
      }
      entry_hashes[key_hash] = computeSHA256Hash(kvp.second);
    }

    return hashUpdateFromEntryHashes(legacy_event.block_id, entry_hashes);
  }

  ConcordAssert(std::holds_alternative<EventGroup>(ev));
  auto& event_group = std::get<EventGroup>(ev);
  set<string> entry_hashes;
  for (const auto& event : event_group.events) {
    string event_hash = computeSHA256Hash(event);
    ConcordAssert(entry_hashes.count(event_hash) < 1);
    entry_hashes.emplace(event_hash);
  }

  return hashUpdateFromEntryHashes(event_group.id, entry_hashes);
}

string hashUpdate(const Data& update) {
  if (update.has_events()) {
    map<string, string> entry_hashes;
    for (const auto& kvp : update.events().data()) {
      string key_hash = computeSHA256Hash(kvp.key());
      if (entry_hashes.count(key_hash) > 0) {
        throw invalid_argument("hashUpdate called for an update that contains duplicate keys.");
      }
      entry_hashes[key_hash] = computeSHA256Hash(kvp.value());
    }

    return hashUpdateFromEntryHashes(update.events().block_id(), entry_hashes);
  }
  ConcordAssert(update.has_event_group());
  set<string> entry_hashes;
  for (const auto& event : update.event_group().events()) {
    string event_hash = computeSHA256Hash(event);
    ConcordAssert(entry_hashes.count(event_hash) < 1);
    entry_hashes.emplace(event_hash);
  }

  return hashUpdateFromEntryHashes(update.event_group().id(), entry_hashes);
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

  return computeSHA256Hash(concatenated_update_hashes);
}

}  // namespace client::thin_replica_client
