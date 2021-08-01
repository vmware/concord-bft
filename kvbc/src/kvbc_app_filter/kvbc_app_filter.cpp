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
// Filtered access to the KV Blockchain.

#include "kvbc_app_filter/kvbc_app_filter.h"

#include <boost/detail/endian.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <cassert>
#include <chrono>
#include <exception>
#include <sstream>
#include "Logger.hpp"

#include "concord_kvbc.pb.h"
#include "kv_types.hpp"
#include "kvbc_app_filter/kvbc_key_types.h"
#include "openssl_crypto.hpp"

using namespace std::chrono_literals;

using std::map;
using std::string;
using std::stringstream;

using boost::lockfree::spsc_queue;

using com::vmware::concord::kvbc::ValueWithTrids;
using concord::kvbc::BlockId;
using concord::kvbc::categorization::ImmutableInput;
using concord::kvbc::InvalidBlockRange;
using concord::util::openssl_utils::computeSHA256Hash;
using concord::util::openssl_utils::kExpectedSHA256HashLengthInBytes;

namespace concord {
namespace kvbc {

KvbFilteredUpdate::OrderedKVPairs KvbAppFilter::filterKeyValuePairs(const kvbc::categorization::ImmutableInput &kvs) {
  KvbFilteredUpdate::OrderedKVPairs filtered_kvs;

  for (auto &[prefixed_key, value] : kvs.kv) {
    // Remove the Block ID prefix from the key before using it.
    ConcordAssertGE(prefixed_key.size(), sizeof(kvbc::BlockId));
    auto key = prefixed_key.size() == sizeof(kvbc::BlockId) ? std::string{} : prefixed_key.substr(sizeof(BlockId));

    // If no TRIDs attached then everyone is allowed to view the pair
    // Otherwise, check against the client id
    if (value.tags.size() > 0) {
      bool contains_client_id = false;
      for (const auto &trid : value.tags) {
        if (trid.compare(client_id_) == 0) {
          contains_client_id = true;
          break;
        }
      }
      if (!contains_client_id) {
        continue;
      }
    }

    ValueWithTrids proto;
    if (!proto.ParseFromArray(value.data.c_str(), value.data.length())) {
      continue;
    }

    // We expect a value - this should never trigger
    if (!proto.has_value()) {
      std::stringstream msg;
      msg << "Couldn't decode value with trids " << key;
      throw KvbReadError(msg.str());
    }

    auto val = proto.release_value();
    filtered_kvs.push_back({std::move(key), std::move(*val)});
    delete val;
  }

  return filtered_kvs;
}

KvbFilteredEventGroupUpdate::EventGroup KvbAppFilter::filterEventsInEventGroup(
    EventGroupId event_group_id, const kvbc::categorization::EventGroup &event_group) {
  KvbFilteredEventGroupUpdate::EventGroup filtered_event_group;

  for (auto event : event_group.events) {
    // If no TRIDs attached then everyone is allowed to view the pair
    // Otherwise, check against the client id
    if (event.tags.size() > 0) {
      bool contains_client_id = false;
      for (const auto &trid : event.tags) {
        if (trid.compare(client_id_) == 0) {
          contains_client_id = true;
          break;
        }
      }
      if (!contains_client_id) {
        continue;
      }
    }

    // Event data encodes ValueWithTrids
    // Extract the value, and assign it to event.data
    ValueWithTrids proto;
    if (!proto.ParseFromArray(event.data.c_str(), event.data.length())) {
      continue;
    }

    // We expect a value - this should never trigger
    if (!proto.has_value()) {
      std::stringstream msg;
      msg << "Couldn't decode value with trids for event_group_id" << event_group_id;
      throw KvbReadError(msg.str());
    }

    auto val = proto.release_value();
    event.data.assign(std::move(*val));
    filtered_event_group.events.emplace_back(std::move(event));
    delete val;
  }

  return filtered_event_group;
}

KvbFilteredUpdate KvbAppFilter::filterUpdate(const KvbUpdate &update) {
  auto &[block_id, cid, updates, _] = update;
  return KvbFilteredUpdate{block_id, cid, filterKeyValuePairs(updates)};
}

KvbFilteredEventGroupUpdate KvbAppFilter::filterEventGroupUpdate(const EgUpdate &update) {
  auto &[event_group_id, event_group, _] = update;
  return KvbFilteredEventGroupUpdate{event_group_id, filterEventsInEventGroup(event_group_id, event_group)};
}

string KvbAppFilter::hashUpdate(const KvbFilteredUpdate &update) {
  // Note we store the hashes of the keys and values in an std::map as an
  // intermediate step in the computation of the update hash so the map can be
  // used to deterministically order the key-value pairs' hashes before they are
  // concatenated for the computation of the update hash. The deterministic
  // ordering is necessary since two updates with the same set of key-value
  // pairs in different orders are considered equivalent so their hashes need to
  // match.
  map<string, string> entry_hashes;
  auto &[block_id, _, updates] = update;

  for (const auto &[key, value] : updates) {
    string key_hash = computeSHA256Hash(key.data(), key.length());
    ConcordAssert(entry_hashes.count(key_hash) < 1);
    entry_hashes[key_hash] = computeSHA256Hash(value.data(), value.length());
  }

  string concatenated_entry_hashes;
  concatenated_entry_hashes.reserve(sizeof(BlockId) + 2 * updates.size() * kExpectedSHA256HashLengthInBytes);

#ifdef BOOST_LITTLE_ENDIAN
  concatenated_entry_hashes.append(reinterpret_cast<const char *>(&(block_id)), sizeof(block_id));
#else  // BOOST_LITTLE_ENDIAN not defined in this case
#ifndef BOOST_BIG_ENDIAN
  static_assert(false,
                "Cannot determine endianness (needed for Thin Replica "
                "mechanism hash function).");
#endif  // BOOST_BIG_ENDIAN defined
  const char *block_id_as_bytes = reinterpret_cast<cosnt char *>(&(block_id));
  for (size_t i = 1; i <= sizeof(block_id); ++i) {
    concatenated_entry_hashes.append((block_id_as_bytes + (sizeof(block_id) - i)), 1);
  }
#endif  // if BOOST_LITTLE_ENDIAN defined/else

  for (const auto &kvp_hashes : entry_hashes) {
    concatenated_entry_hashes.append(kvp_hashes.first);
    concatenated_entry_hashes.append(kvp_hashes.second);
  }

  return computeSHA256Hash(concatenated_entry_hashes);
}

string KvbAppFilter::hashEventGroupUpdate(const KvbFilteredEventGroupUpdate &update) {
  // Note we store the hashes of the events in an std::set as an
  // intermediate step in the computation of the update hash so the set can be
  // used to deterministically order the events' hashes before they are
  // concatenated for the computation of the update hash. The deterministic
  // ordering is necessary since two updates with the same set of events
  // in different orders are considered equivalent so their hashes need to
  // match.
  std::set<string> entry_hashes;
  auto &[event_group_id, event_group] = update;

  for (const auto &event : event_group.events) {
    string event_hash = computeSHA256Hash(event.data.data(), event.data.length());
    ConcordAssert(entry_hashes.count(event_hash) < 1);
    entry_hashes.emplace(event_hash);
  }

  string concatenated_entry_hashes;
  concatenated_entry_hashes.reserve(sizeof(EventGroupId) +
                                    event_group.events.size() * kExpectedSHA256HashLengthInBytes);

#ifdef BOOST_LITTLE_ENDIAN
  concatenated_entry_hashes.append(reinterpret_cast<const char *>(&(event_group_id)), sizeof(event_group_id));
#else  // BOOST_LITTLE_ENDIAN not defined in this case
#ifndef BOOST_BIG_ENDIAN
  static_assert(false,
                "Cannot determine endianness (needed for Thin Replica "
                "mechanism hash function).");
#endif  // BOOST_BIG_ENDIAN defined
  const char *event_group_id_as_bytes = reinterpret_cast<const char *>(&(event_group_id));
  for (size_t i = 1; i <= sizeof(event_group_id); ++i) {
    concatenated_entry_hashes.append((event_group_id_as_bytes + (sizeof(event_group_id) - i)), 1);
  }
#endif  // if BOOST_LITTLE_ENDIAN defined/else

  for (const auto &event_hash : entry_hashes) {
    concatenated_entry_hashes.append(event_hash);
  }

  return computeSHA256Hash(concatenated_entry_hashes);
}

void KvbAppFilter::readBlockRange(BlockId block_id_start,
                                  BlockId block_id_end,
                                  spsc_queue<KvbFilteredUpdate> &queue_out,
                                  const std::atomic_bool &stop_execution) {
  if (block_id_start > block_id_end || block_id_end > rostorage_->getLastBlockId()) {
    throw InvalidBlockRange(block_id_start, block_id_end);
  }

  BlockId block_id(block_id_start);

  LOG_DEBUG(logger_, "readBlockRange block " << block_id << " to " << block_id_end);

  for (; block_id <= block_id_end; ++block_id) {
    std::string cid;
    auto events = getBlockEvents(block_id, cid);
    if (!events) {
      std::stringstream msg;
      msg << "Couldn't retrieve block events for block id " << block_id;
      throw KvbReadError(msg.str());
    }
    KvbFilteredUpdate update{block_id, cid, filterKeyValuePairs(*events)};
    while (!stop_execution) {
      if (queue_out.push(update)) {
        break;
      }
    }
    if (stop_execution) {
      LOG_WARN(logger_, "readBlockRange was stopped");
      break;
    }
  }
}

void KvbAppFilter::readEventGroupRange(EventGroupId event_group_id_start,
                                       EventGroupId event_group_id_end,
                                       spsc_queue<KvbFilteredEventGroupUpdate> &queue_out,
                                       const std::atomic_bool &stop_execution) {
  auto last_trid_eg_id_var =
      rostorage_->getLatest(concord::kvbc::categorization::kExecutionEventGroupIdsCategory, client_id_);
  const auto &val = std::get<concord::kvbc::categorization::ImmutableValue>(*last_trid_eg_id_var);
  auto last_trid_eg_id = concordUtils::fromBigEndianBuffer<uint64_t>(val.data.data());

  if (event_group_id_start > event_group_id_end || event_group_id_end > last_trid_eg_id) {
    throw InvalidEventGroupRange(event_group_id_start, event_group_id_end);
  }

  uint64_t event_group_id(event_group_id_start);

  LOG_DEBUG(logger_, "readEventGroupRange event_group_id: " << event_group_id << " to " << event_group_id_end);

  for (; event_group_id <= event_group_id_end; ++event_group_id) {
    std::string cid;
    auto event_group = getEventGroup(event_group_id, cid);
    if (event_group.events.empty()) {
      std::stringstream msg;
      msg << "EventGroup doesn't exist for valid event_group_id: " << event_group_id;
      throw KvbReadError(msg.str());
    }
    KvbFilteredEventGroupUpdate update{event_group_id, filterEventsInEventGroup(event_group_id, event_group)};
    while (!stop_execution) {
      if (queue_out.push(update)) {
        break;
      }
    }
    if (stop_execution) {
      LOG_WARN(logger_, "readEventGroupRange was stopped");
      break;
    }
  }
}

string KvbAppFilter::readBlockHash(BlockId block_id) {
  if (block_id > rostorage_->getLastBlockId()) {
    throw InvalidBlockRange(block_id, block_id);
  }

  std::string cid;
  auto events = getBlockEvents(block_id, cid);
  if (!events) {
    std::stringstream msg;
    msg << "Couldn't retrieve block events for block id " << block_id;
    throw KvbReadError(msg.str());
  }
  KvbFilteredUpdate filtered_update{block_id, cid, filterKeyValuePairs(*events)};
  return hashUpdate(filtered_update);
}

string KvbAppFilter::readEventGroupHash(EventGroupId event_group_id) {
  auto last_trid_eg_id_var =
      rostorage_->getLatest(concord::kvbc::categorization::kExecutionEventGroupIdsCategory, client_id_);
  const auto &val = std::get<concord::kvbc::categorization::ImmutableValue>(*last_trid_eg_id_var);
  auto last_trid_eg_id = concordUtils::fromBigEndianBuffer<uint64_t>(val.data.data());

  if (last_trid_eg_id && event_group_id > last_trid_eg_id) {
    throw InvalidEventGroupRange(event_group_id, event_group_id);
  }
  std::string cid;
  auto event_group = getEventGroup(event_group_id, cid);
  if (event_group.events.empty()) {
    std::stringstream msg;
    msg << "Couldn't retrieve block event groups for event_group_id " << event_group_id;
    throw KvbReadError(msg.str());
  }
  KvbFilteredEventGroupUpdate filtered_update{event_group_id, filterEventsInEventGroup(event_group_id, event_group)};
  return hashEventGroupUpdate(filtered_update);
}

string KvbAppFilter::readBlockRangeHash(BlockId block_id_start, BlockId block_id_end) {
  if (block_id_start > block_id_end || block_id_end > rostorage_->getLastBlockId()) {
    throw InvalidBlockRange(block_id_start, block_id_end);
  }
  BlockId block_id(block_id_start);

  LOG_DEBUG(logger_, "readBlockRangeHash block " << block_id << " to " << block_id_end);

  string concatenated_update_hashes;
  concatenated_update_hashes.reserve((1 + block_id_end - block_id) * kExpectedSHA256HashLengthInBytes);
  for (; block_id <= block_id_end; ++block_id) {
    std::string cid;
    auto events = getBlockEvents(block_id, cid);
    if (!events) {
      std::stringstream msg;
      msg << "Couldn't retrieve block events for block id " << block_id;
      throw KvbReadError(msg.str());
    }
    KvbFilteredUpdate filtered_update{block_id, cid, filterKeyValuePairs(*events)};
    concatenated_update_hashes.append(hashUpdate(filtered_update));
  }
  return computeSHA256Hash(concatenated_update_hashes);
}

string KvbAppFilter::readEventGroupRangeHash(EventGroupId event_group_id_start, EventGroupId event_group_id_end) {
  auto last_trid_eg_id_var =
      rostorage_->getLatest(concord::kvbc::categorization::kExecutionEventGroupIdsCategory, client_id_);
  const auto &val = std::get<concord::kvbc::categorization::ImmutableValue>(*last_trid_eg_id_var);
  auto last_trid_eg_id = concordUtils::fromBigEndianBuffer<uint64_t>(val.data.data());
  if (event_group_id_start > event_group_id_end || event_group_id_end > last_trid_eg_id) {
    throw InvalidEventGroupRange(event_group_id_start, event_group_id_end);
  }

  EventGroupId event_group_id(event_group_id_start);

  LOG_DEBUG(logger_, "readEventGroupRangeHash event_group_id " << event_group_id << " to " << event_group_id_end);

  string concatenated_update_hashes;
  concatenated_update_hashes.reserve((1 + event_group_id_end - event_group_id) * kExpectedSHA256HashLengthInBytes);
  for (; event_group_id <= event_group_id_end; ++event_group_id) {
    std::string cid;
    auto event_group = getEventGroup(event_group_id, cid);
    if (event_group.events.empty()) {
      std::stringstream msg;
      msg << "Couldn't retrieve block event groups for event_group_id: " << event_group_id;
      throw KvbReadError(msg.str());
    }
    KvbFilteredEventGroupUpdate filtered_update{event_group_id, filterEventsInEventGroup(event_group_id, event_group)};
    concatenated_update_hashes.append(hashEventGroupUpdate(filtered_update));
  }
  return computeSHA256Hash(concatenated_update_hashes);
}

std::optional<kvbc::categorization::ImmutableInput> KvbAppFilter::getBlockEvents(kvbc::BlockId block_id,
                                                                                 std::string &cid) {
  const auto updates = rostorage_->getBlockUpdates(block_id);
  if (!updates) {
    LOG_ERROR(logger_, "Couldn't get block updates");
    return {};
  }
  // Not all blocks have events.
  auto immutable = updates.value().categoryUpdates(concord::kvbc::categorization::kExecutionEventsCategory);
  if (!immutable) {
    return kvbc::categorization::ImmutableInput{};
  }
  return std::get<kvbc::categorization::ImmutableInput>(immutable->get());
}

kvbc::categorization::EventGroup KvbAppFilter::getEventGroup(kvbc::EventGroupId event_group_id, std::string &cid) {
  // get global_event_group_id corresponding to trid_event_group_id
  const auto key = client_id_ + concordUtils::toBigEndianStringBuffer(event_group_id);
  const auto global_eg_id_val =
      rostorage_->getLatest(concord::kvbc::categorization::kExecutionTridEventGroupsCategory, key);
  const auto &val = std::get<concord::kvbc::categorization::ImmutableValue>(*global_eg_id_val);
  auto global_event_group_id = concordUtils::fromBigEndianBuffer<uint64_t>(val.data.data());

  // get event group
  const auto event_group = rostorage_->getLatest(concord::kvbc::categorization::kExecutionGlobalEventGroupsCategory,
                                                 concordUtils::toBigEndianStringBuffer(global_event_group_id));
  const auto &event_group_val = std::get<concord::kvbc::categorization::ImmutableValue>(*event_group);
  const std::string serialized_event_group = event_group_val.data.data();
  const std::vector<uint8_t> input_vec(serialized_event_group.begin(), serialized_event_group.end());
  concord::kvbc::categorization::EventGroup event_group_out;
  concord::kvbc::categorization::deserialize(input_vec, event_group_out);
  if (event_group_out.events.empty()) {
    LOG_ERROR(logger_, "Couldn't get event group updates");
    return kvbc::categorization::EventGroup{};
  }
  return event_group_out;
}

}  // namespace kvbc
}  // namespace concord
