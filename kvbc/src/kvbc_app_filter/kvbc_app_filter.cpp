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
#include <optional>
#include <sstream>
#include "Logger.hpp"

#include "concord_kvbc.pb.h"
#include "kv_types.hpp"
#include "kvbc_app_filter/kvbc_key_types.h"
#include "openssl_crypto.hpp"

using namespace std::chrono_literals;

using std::map;
using std::optional;
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

uint64_t KvbAppFilter::getOldestGlobalEventGroupId() const { return getValueFromLatestTable(kGlobalEgIdKeyOldest); }

uint64_t KvbAppFilter::getNewestPublicEventGroupId() const { return getValueFromLatestTable(kPublicEgIdKeyNewest); }

std::optional<kvbc::categorization::EventGroup> KvbAppFilter::getNewestPublicEventGroup() const {
  const auto newest_pub_eg_id = getNewestPublicEventGroupId();
  if (newest_pub_eg_id == 0) {
    // No public event group ID.
    return std::nullopt;
  }
  const auto [global_eg_id, _] = getValueFromTagTable(kPublicEgId, newest_pub_eg_id);
  (void)_;
  return getEventGroup(global_eg_id);
}

optional<BlockId> KvbAppFilter::getOldestEventGroupBlockId() {
  uint64_t global_eg_id_oldest = getOldestGlobalEventGroupId();
  const auto opt = rostorage_->getLatestVersion(concord::kvbc::categorization::kExecutionEventGroupDataCategory,
                                                concordUtils::toBigEndianStringBuffer(global_eg_id_oldest));
  if (not opt.has_value()) {
    return std::nullopt;
  }
  return {opt->version};
}

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
  filtered_event_group.record_time = event_group.record_time;

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

std::optional<KvbFilteredEventGroupUpdate> KvbAppFilter::filterEventGroupUpdate(const EgUpdate &update) {
  auto &[event_group_id, event_group, _] = update;
  KvbFilteredEventGroupUpdate filtered_update{event_group_id, filterEventsInEventGroup(event_group_id, event_group)};
  // Ignore empty event groups
  if (filtered_update.event_group.events.empty()) {
    return std::nullopt;
  }
  return filtered_update;
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
    ConcordAssertLT(entry_hashes.count(key_hash), 1);
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
    string event_hash = computeSHA256Hash(event.data);
    ConcordAssertLT(entry_hashes.count(event_hash), 1);
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

uint64_t KvbAppFilter::getValueFromLatestTable(const std::string &key) const {
  const auto opt = rostorage_->getLatest(kvbc::categorization::kExecutionEventGroupLatestCategory, key);
  if (not opt) {
    LOG_DEBUG(logger_, "External event group ID for key \"" << key << "\" doesn't exist yet");
    // In case there are no public or private event groups for a client, return 0.
    // Note: `0` is an invalid event group id
    return 0;
  }
  auto val = std::get_if<concord::kvbc::categorization::VersionedValue>(&(opt.value()));
  if (not val) {
    std::stringstream msg;
    msg << "Failed to convert stored external event group id for key \"" << key << "\" to versioned value";
    throw std::runtime_error(msg.str());
  }
  return concordUtils::fromBigEndianBuffer<uint64_t>(val->data.data());
}

TagTableValue KvbAppFilter::getValueFromTagTable(const std::string &tag, uint64_t pvt_eg_id) const {
  auto key = tag + kTagTableKeySeparator + concordUtils::toBigEndianStringBuffer(pvt_eg_id);
  const auto opt = rostorage_->getLatest(concord::kvbc::categorization::kExecutionEventGroupTagCategory, key);
  if (not opt) {
    std::stringstream msg;
    msg << "Failed to get event group id from tag table for key " << key;
    LOG_WARN(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  const auto val = std::get_if<concord::kvbc::categorization::ImmutableValue>(&(opt.value()));
  if (not val) {
    std::stringstream msg;
    msg << "Failed to convert stored event group id from tag table for key \"" << key << "\" to immutable value";
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  std::string_view result{val->data};
  auto offset = sizeof(uint64_t);
  uint64_t global_eg_id = concordUtils::fromBigEndianBuffer<uint64_t>(result.substr(0, offset).data());
  uint64_t external_tag_eg_id =
      concordUtils::fromBigEndianBuffer<uint64_t>(result.substr(offset + kTagTableKeySeparator.size()).data());
  // Every tag-table entry must have a valid global event group id
  // If this table was pruned then only valid entries remain which still need to have a proper event group id
  ConcordAssertNE(global_eg_id, 0);
  return {global_eg_id, external_tag_eg_id};
}

// We don't store external event group ids and need to compute them at runtime.

// This function returns the oldest external event group id that the user can request.
// Due to pruning, it depends on the oldest public event group and the oldest private event group available.
uint64_t KvbAppFilter::oldestExternalEventGroupId() const {
  uint64_t public_oldest = getValueFromLatestTable(kPublicEgIdKeyOldest);
  uint64_t private_oldest = getValueFromLatestTable(client_id_ + "_oldest");
  if (!public_oldest && !private_oldest) return 0;

  // If public or private was fully pruned then we have to account for those event groups as well
  if (!public_oldest) return private_oldest + getValueFromLatestTable(kPublicEgIdKeyNewest);
  if (!private_oldest) return public_oldest + getValueFromLatestTable(client_id_ + "_newest");

  // Adding public and private results in an external event group id including two query-able event groups
  // (the oldest private and the oldest public).
  // However, we are only interested in the oldest external and not the second oldest. Hence, we have to subtract 1.
  return public_oldest + private_oldest - 1;
}

// This function returns the newest external event group id that the user can request.
// Note, the newest external event group ids will not be updated by pruning.
uint64_t KvbAppFilter::newestExternalEventGroupId() const {
  uint64_t public_newest = getValueFromLatestTable(kPublicEgIdKeyNewest);
  uint64_t private_newest = getValueFromLatestTable(client_id_ + "_newest");
  return public_newest + private_newest;
}

FindGlobalEgIdResult KvbAppFilter::findGlobalEventGroupId(uint64_t external_event_group_id) const {
  auto external_oldest = oldestExternalEventGroupId();
  auto external_newest = newestExternalEventGroupId();
  ConcordAssertNE(external_oldest, 0);  // Everything pruned or was never created
  ConcordAssertLE(external_oldest, external_event_group_id);
  ConcordAssertGE(external_newest, external_event_group_id);

  uint64_t public_start = getValueFromLatestTable(kPublicEgIdKeyOldest);
  uint64_t public_end = getValueFromLatestTable(kPublicEgIdKeyNewest);
  uint64_t private_start = getValueFromLatestTable(client_id_ + "_oldest");
  uint64_t private_end = getValueFromLatestTable(client_id_ + "_newest");

  if (not private_start and not private_end) {
    // requested external event group id == public event group id
    uint64_t global_id;
    std::tie(global_id, std::ignore) = getValueFromTagTable(kPublicEgId, external_event_group_id);
    return {global_id, true, private_end, external_event_group_id};
  } else if (not public_start and not public_end) {
    // requested external event group id == private event group id
    uint64_t global_id;
    std::tie(global_id, std::ignore) = getValueFromTagTable(client_id_, external_event_group_id);
    return {global_id, false, external_event_group_id, public_end};
  }

  // Binary search in private event groups
  uint64_t window_size;
  auto window_start = private_start;
  auto window_end = private_end;

  // Cursors inside the search window; All point to the same entry
  // client_id_ <separator> current_pvt_eg_id => current_global_eg_id <separator> current_ext_eg_id
  auto [current_pvt_eg_id, current_global_eg_id, current_ext_eg_id] = std::make_tuple(0ull, 0ull, 0ull);

  while (window_start && window_start <= window_end) {
    window_size = window_end - window_start + 1;
    current_pvt_eg_id = window_start + (window_size / 2);
    std::tie(current_global_eg_id, current_ext_eg_id) = getValueFromTagTable(client_id_, current_pvt_eg_id);

    // Either we found it or read the last possible entry in the window
    if (current_ext_eg_id == external_event_group_id || window_size == 1) break;

    // Adjust the window excluding the entry we just checked
    if (external_event_group_id > current_ext_eg_id) {
      window_start = current_pvt_eg_id + 1;
    } else if (external_event_group_id < current_ext_eg_id) {
      window_end = current_pvt_eg_id - 1;
    }
  }

  if (current_ext_eg_id == external_event_group_id) {
    return {current_global_eg_id, false, current_pvt_eg_id, external_event_group_id - current_pvt_eg_id};
  }

  // At this point, we exhausted all private entries => it has to be a public event group
  uint64_t global_eg_id;

  // If all private event groups were pruned then we need to adjust current_* to point to the last private event group
  if (private_start == 0) {
    ConcordAssertNE(private_end, 0);
    current_pvt_eg_id = private_end;
    current_ext_eg_id = private_end + public_start - 1;
  }

  if (external_event_group_id < current_ext_eg_id) {
    auto num_pub_egs = current_ext_eg_id - current_pvt_eg_id;
    auto pub_eg_id = num_pub_egs - (current_ext_eg_id - external_event_group_id - 1);
    std::tie(global_eg_id, std::ignore) = getValueFromTagTable(kPublicEgId, pub_eg_id);
    return {global_eg_id, true, current_pvt_eg_id - 1, pub_eg_id};
  }

  ConcordAssertGT(external_event_group_id, current_ext_eg_id);
  auto num_pub_egs = current_ext_eg_id - current_pvt_eg_id;
  auto pub_eg_id = num_pub_egs + (external_event_group_id - current_ext_eg_id);
  std::tie(global_eg_id, std::ignore) = getValueFromTagTable(kPublicEgId, pub_eg_id);
  return {global_eg_id, true, current_pvt_eg_id, pub_eg_id};
}

void KvbAppFilter::readEventGroups(EventGroupId external_eg_id_start,
                                   const std::function<bool(KvbFilteredEventGroupUpdate &&)> &process_update) {
  ConcordAssertGT(external_eg_id_start, 0);

  uint64_t newest_public_eg_id = getNewestPublicEventGroupId();
  uint64_t newest_private_eg_id = getValueFromLatestTable(client_id_ + "_newest");
  uint64_t oldest_external_eg_id = oldestExternalEventGroupId();
  uint64_t newest_external_eg_id = newestExternalEventGroupId();
  if (not oldest_external_eg_id) {
    std::stringstream msg;
    msg << "Event groups do not exist for client: " << client_id_ << " yet.";
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }

  if (external_eg_id_start < oldest_external_eg_id || external_eg_id_start > newest_external_eg_id) {
    throw InvalidEventGroupRange(external_eg_id_start, oldest_external_eg_id, newest_external_eg_id);
  }

  auto [global_eg_id, is_previous_public, private_eg_id, public_eg_id] = findGlobalEventGroupId(external_eg_id_start);
  uint64_t ext_eg_id = external_eg_id_start;

  uint64_t next_pvt_eg_id = private_eg_id + 1;
  uint64_t next_pub_eg_id = public_eg_id + 1;

  // The next public or private event group might not exist or got pruned
  // In this case, set to max so that the comparison will be lost later
  uint64_t pvt_external_id;
  uint64_t pvt_global_id;
  try {
    std::tie(pvt_global_id, pvt_external_id) = getValueFromTagTable(client_id_, next_pvt_eg_id);
  } catch (const std::exception &e) {
    pvt_global_id = std::numeric_limits<uint64_t>::max();
  }

  uint64_t pub_global_id;
  try {
    std::tie(pub_global_id, std::ignore) = getValueFromTagTable(kPublicEgId, next_pub_eg_id);
  } catch (const std::exception &e) {
    pub_global_id = std::numeric_limits<uint64_t>::max();
  }

  while (ext_eg_id <= newest_external_eg_id) {
    // Get events and filter
    auto event_group = getEventGroup(global_eg_id);
    if (event_group.events.empty()) {
      std::stringstream msg;
      msg << "EventGroup empty/doesn't exist for global event group " << global_eg_id;
      throw KvbReadError(msg.str());
    }
    KvbFilteredEventGroupUpdate update{ext_eg_id, filterEventsInEventGroup(global_eg_id, event_group)};

    // Process update and stop producing more updates if anything goes wrong
    if (not process_update(std::move(update))) break;
    if (ext_eg_id == newest_external_eg_id) break;

    // Update next public or private ids; Only one needs to be udpated
    if (is_previous_public) {
      if (next_pub_eg_id > newest_public_eg_id) {
        pub_global_id = std::numeric_limits<uint64_t>::max();
      } else {
        std::tie(pub_global_id, std::ignore) = getValueFromTagTable(kPublicEgId, next_pub_eg_id);
      }
    } else {
      if (next_pvt_eg_id > newest_private_eg_id) {
        pvt_global_id = std::numeric_limits<uint64_t>::max();
      } else {
        std::tie(pvt_global_id, pvt_external_id) = getValueFromTagTable(client_id_, next_pvt_eg_id);
      }
    }

    // No need to continue if both next counters point into the future
    if (pvt_global_id == std::numeric_limits<uint64_t>::max() && pub_global_id == std::numeric_limits<uint64_t>::max())
      break;

    // The lesser global event group id is the next update for the client
    if (pvt_global_id < pub_global_id) {
      global_eg_id = pvt_global_id;
      is_previous_public = false;
      ConcordAssertEQ(ext_eg_id + 1, pvt_external_id);
      next_pvt_eg_id++;
    } else {
      global_eg_id = pub_global_id;
      is_previous_public = true;
      next_pub_eg_id++;
    }
    ext_eg_id += 1;
  }
  setLastEgIdsRead(ext_eg_id, global_eg_id);
}

void KvbAppFilter::readEventGroupRange(EventGroupId external_eg_id_start,
                                       spsc_queue<KvbFilteredEventGroupUpdate> &queue_out,
                                       const std::atomic_bool &stop_execution) {
  auto process = [&](KvbFilteredEventGroupUpdate &&update) {
    while (!stop_execution) {
      if (queue_out.push(update)) break;
    }
    if (stop_execution) return false;
    return true;
  };
  readEventGroups(external_eg_id_start, process);
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

string KvbAppFilter::readEventGroupHash(EventGroupId external_eg_id) {
  uint64_t oldest_external_eg_id = oldestExternalEventGroupId();
  uint64_t newest_external_eg_id = newestExternalEventGroupId();
  if (not oldest_external_eg_id) {
    std::stringstream msg;
    msg << "Event groups do not exist for client: " << client_id_ << " yet.";
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }

  if (external_eg_id == 0) {
    throw InvalidEventGroupId(external_eg_id);
  }
  if (external_eg_id < oldest_external_eg_id || external_eg_id > newest_external_eg_id) {
    throw InvalidEventGroupRange(external_eg_id, oldest_external_eg_id, newest_external_eg_id);
  }

  auto result = findGlobalEventGroupId(external_eg_id);
  LOG_DEBUG(logger_, "external_eg_id " << external_eg_id << " global_id " << result.global_id);
  auto event_group = getEventGroup(result.global_id);
  if (event_group.events.empty()) {
    std::stringstream msg;
    msg << "Couldn't retrieve block event groups for event_group_id " << result.global_id;
    throw KvbReadError(msg.str());
  }
  KvbFilteredEventGroupUpdate filtered_update{external_eg_id, filterEventsInEventGroup(result.global_id, event_group)};
  setLastEgIdsRead(external_eg_id, result.global_id);
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

string KvbAppFilter::readEventGroupRangeHash(EventGroupId external_eg_id_start) {
  auto external_eg_id_end = newestExternalEventGroupId();
  string concatenated_hashes;
  concatenated_hashes.reserve((1 + external_eg_id_end - external_eg_id_start) * kExpectedSHA256HashLengthInBytes);
  auto process = [&](KvbFilteredEventGroupUpdate &&update) {
    concatenated_hashes.append(hashEventGroupUpdate(update));
    return true;
  };
  readEventGroups(external_eg_id_start, process);
  return computeSHA256Hash(concatenated_hashes);
}

std::optional<kvbc::categorization::ImmutableInput> KvbAppFilter::getBlockEvents(kvbc::BlockId block_id,
                                                                                 std::string &cid) {
  if (auto opt = getOldestEventGroupBlockId()) {
    if (block_id >= opt.value()) {
      throw NoLegacyEvents();
    }
  }
  const auto updates = rostorage_->getBlockUpdates(block_id);
  if (!updates) {
    LOG_ERROR(logger_, "Couldn't get block updates");
    return {};
  }
  // get cid
  const auto &internal_map =
      std::get<kvbc::categorization::VersionedInput>(
          updates.value().categoryUpdates(concord::kvbc::categorization::kConcordInternalCategoryId)->get())
          .kv;
  auto it = internal_map.find(cid_key_);
  if (it != internal_map.end()) {
    cid = it->second.data;
  }
  // Not all blocks have events.
  auto immutable = updates.value().categoryUpdates(concord::kvbc::categorization::kExecutionEventsCategory);
  if (!immutable) {
    return kvbc::categorization::ImmutableInput{};
  }
  return std::get<kvbc::categorization::ImmutableInput>(immutable->get());
}

kvbc::categorization::EventGroup KvbAppFilter::getEventGroup(kvbc::EventGroupId global_event_group_id) const {
  LOG_DEBUG(logger_,
            " Get EventGroup, global_event_group_id: " << global_event_group_id << " for client: " << client_id_);
  // get event group
  const auto opt = rostorage_->getLatest(concord::kvbc::categorization::kExecutionEventGroupDataCategory,
                                         concordUtils::toBigEndianStringBuffer(global_event_group_id));
  if (not opt) {
    stringstream msg;
    msg << "Failed to get global event group " << global_event_group_id;
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  const auto imm_val = std::get_if<concord::kvbc::categorization::ImmutableValue>(&(opt.value()));
  if (not imm_val) {
    stringstream msg;
    msg << "Failed to convert stored global event group " << global_event_group_id;
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  // TODO: Can we avoid copying?
  const std::vector<uint8_t> input_vec(imm_val->data.begin(), imm_val->data.end());
  concord::kvbc::categorization::EventGroup event_group_out;
  concord::kvbc::categorization::deserialize(input_vec, event_group_out);
  if (event_group_out.events.empty()) {
    LOG_ERROR(logger_, "Couldn't get event group updates");
    return kvbc::categorization::EventGroup{};
  }
  return event_group_out;
}

void KvbAppFilter::setLastEgIdsRead(uint64_t last_ext_eg_id_read, uint64_t last_global_eg_id_read) {
  last_ext_and_global_eg_id_read_ = std::make_pair(last_ext_eg_id_read, last_global_eg_id_read);
}

std::pair<uint64_t, uint64_t> KvbAppFilter::getLastEgIdsRead() { return last_ext_and_global_eg_id_read_; }

}  // namespace kvbc
}  // namespace concord
