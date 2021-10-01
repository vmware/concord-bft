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
using concord::kvbc::EventGroupClientState;
using concord::util::openssl_utils::computeSHA256Hash;
using concord::util::openssl_utils::kExpectedSHA256HashLengthInBytes;

namespace concord {
namespace kvbc {

optional<BlockId> KvbAppFilter::getOldestEventGroupBlockId() {
  uint64_t global_eg_id_oldest = getValueFromLatestTable(kGlobalEgIdKeyOldest);
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
  ConcordAssert(!filtered_event_group.events.empty());
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

uint64_t KvbAppFilter::getValueFromLatestTable(const std::string &key) {
  const auto opt = rostorage_->getLatest(kvbc::categorization::kExecutionEventGroupLatestCategory, key);
  if (not opt) {
    LOG_DEBUG(logger_, "Tag-specific event group ID for key \"" << key << "\" doesn't exist yet");
    // In case there are no public or private event groups for a client, return 0.
    // Note: `0` is an invalid event group id
    return 0;
  }
  auto val = std::get_if<concord::kvbc::categorization::VersionedValue>(&(opt.value()));
  if (not val) {
    std::stringstream msg;
    msg << "Failed to convert stored tag-specific event group id for key \"" << key << "\" to versioned value";
    throw std::runtime_error(msg.str());
  }
  return concordUtils::fromBigEndianBuffer<uint64_t>(val->data.data());
}

uint64_t KvbAppFilter::getValueFromTagTable(const std::string &key) {
  const auto opt = rostorage_->getLatest(concord::kvbc::categorization::kExecutionEventGroupTagCategory, key);
  if (not opt) {
    std::stringstream msg;
    msg << "Failed to get global event group id for key " << key;
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  const auto val = std::get_if<concord::kvbc::categorization::ImmutableValue>(&(opt.value()));
  if (not val) {
    std::stringstream msg;
    msg << "Failed to convert stored global event group id for key \"" << key << "\" to immutable value";
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  return concordUtils::fromBigEndianBuffer<uint64_t>(val->data.data());
}

// We don't store tag-specific public event group ids and need to compute them at runtime.

// This function returns the oldest tag-specific public event group id that the user can request.
// Due to pruning, it depends on the oldest public event group and the oldest tag-specific event group available.
uint64_t KvbAppFilter::oldestTagSpecificPublicEventGroupId() {
  uint64_t public_oldest = getValueFromLatestTable(kPublicEgIdKeyOldest);
  uint64_t private_oldest = getValueFromLatestTable(client_id_ + "_oldest");
  if (!public_oldest && !private_oldest) return 0;
  return public_oldest + private_oldest - 1;
}

// This function returns the newest tag-specific public event group id that the user can request.
// Note that newest tag-specific event group ids will not be updated by pruning
uint64_t KvbAppFilter::newestTagSpecificPublicEventGroupId() {
  uint64_t public_newest = getValueFromLatestTable(kPublicEgIdKeyNewest);
  uint64_t private_newest = getValueFromLatestTable(client_id_ + "_newest");
  return public_newest + private_newest;
}

std::optional<uint64_t> KvbAppFilter::getNextEventGroupId(std::shared_ptr<EventGroupClientState> &eg_state) {
  LOG_DEBUG(logger_, "Public_offset: " << eg_state->public_offset << ", private_offset: " << eg_state->private_offset);
  LOG_DEBUG(logger_, "Event group id list batch size: " << eg_state->event_group_id_batch.size());

  // there should be at least one event group to read from storage
  // Note: offset == 0 implies the oldest value for the tag in the latest table is 0, i.e., there is nothing to read for
  // that tag
  ConcordAssert(eg_state->public_offset != 0 || eg_state->private_offset != 0);

  if (eg_state->it != eg_state->event_group_id_batch.end()) {
    return *eg_state->it++;
  }
  uint64_t public_end = getValueFromLatestTable(kPublicEgIdKeyNewest);
  uint64_t private_end = getValueFromLatestTable(client_id_ + "_newest");
  LOG_DEBUG(logger_, ", public_end: " << public_end << ", private_end: " << private_end);
  // we cannot read event group ids from storage if they don't exist
  ConcordAssert(eg_state->public_offset <= public_end + 1 && eg_state->private_offset <= private_end + 1);
  // read global event group IDs from storage into memory
  // no public event groups in storage and all private event group ids have already been read
  if (public_end == 0 && eg_state->private_offset > private_end) return std::nullopt;
  // no private event groups in storage and all public event group ids have already been read
  if (private_end == 0 && eg_state->public_offset > public_end) return std::nullopt;
  // no public and private event groups in storage
  if (public_end == 0 && private_end == 0) return std::nullopt;
  // all public and private event group ids have already been read
  if (eg_state->private_offset > private_end && eg_state->public_offset > public_end) return std::nullopt;

  // reset the vectors to read the new batch from storage
  eg_state->event_group_id_batch.clear();
  // holds a maximum of kBatchSize ordered public event group ids at a time
  std::vector<uint64_t> public_event_group_ids{};
  // holds a maximum of kBatchSize ordered private event group ids at a time
  std::vector<uint64_t> private_event_group_ids{};

  // populate public_event_group_ids
  if (eg_state->public_offset != 0) {
    // start reading from the offset until a complete batch is read or no more event groups exist for the tag
    for (uint64_t i = eg_state->public_offset; i < std::min(eg_state->public_offset + kBatchSize, public_end + 1);
         ++i) {
      // get global_event_group_id corresponding to tag_event_group_id
      // tag + kTagTableKeySeparator + latest_tag_event_group_id concatenation is used as key for kv-updates of type
      // kExecutionEventGroupTagCategory
      uint64_t global_eg_id =
          getValueFromTagTable(kPublicEgId + kTagTableKeySeparator + concordUtils::toBigEndianStringBuffer(i));
      public_event_group_ids.emplace_back(global_eg_id);
    }
  }
  // populate private_event_group_ids
  if (eg_state->private_offset != 0) {
    for (uint64_t i = eg_state->private_offset; i < std::min(eg_state->private_offset + kBatchSize, private_end + 1);
         ++i) {
      uint64_t global_eg_id =
          getValueFromTagTable(client_id_ + kTagTableKeySeparator + concordUtils::toBigEndianStringBuffer(i));
      private_event_group_ids.emplace_back(global_eg_id);
    }
  }
  // populate event_group_id_batch with only public event group ids iff one of the following occurs -
  // 1. No private event groups exist in storage
  // 2. The first global event group id in private_event_group_ids is greater than the last global event group id
  // public_event_group_ids
  // Vice versa is true if event_group_id_batch is populated with only private event group ids
  // Note: each private_event_group_ids and public_event_group_ids hold a sorted list of global event group ids at all
  // times
  if (!public_event_group_ids.empty() &&
      (private_event_group_ids.empty() || (public_event_group_ids.back() <= private_event_group_ids.front()))) {
    for (auto public_event_group_id : public_event_group_ids) {
      eg_state->event_group_id_batch.emplace_back(public_event_group_id);
    }
    ConcordAssertLE(eg_state->event_group_id_batch.size(), kBatchSize);
    // increment the offset
    eg_state->public_offset += eg_state->event_group_id_batch.size();
    LOG_DEBUG(logger_,
              "Updated public_offset: " << eg_state->public_offset
                                        << " public_event_group_ids size: " << public_event_group_ids.size());
  } else if (!private_event_group_ids.empty() &&
             (public_event_group_ids.empty() || (private_event_group_ids.back() <= public_event_group_ids.front()))) {
    for (auto private_event_group_id : private_event_group_ids) {
      eg_state->event_group_id_batch.emplace_back(private_event_group_id);
    }
    ConcordAssertLE(eg_state->event_group_id_batch.size(), kBatchSize);
    // increment the offset
    eg_state->private_offset += eg_state->event_group_id_batch.size();
    LOG_DEBUG(logger_,
              "Updated private_offset: " << eg_state->private_offset
                                         << " private_event_group_ids size: " << private_event_group_ids.size());
  } else {
    LOG_DEBUG(logger_, "Both public and private event groups found in the batch");
    std::merge(public_event_group_ids.begin(),
               public_event_group_ids.end(),
               private_event_group_ids.begin(),
               private_event_group_ids.end(),
               std::back_inserter(eg_state->event_group_id_batch));
    // We cannot return the full merge
    // E.g.: a=[1,2,3,4,5] b=[6,7,8,9] and kBatchSize = 2
    // a_batch=[1,2], b_batch=[6,7], returning [1,2,6,7] would be incorrect,
    // therefore the merged list is truncated to size kBatchSize
    if (eg_state->event_group_id_batch.size() > kBatchSize) {
      eg_state->event_group_id_batch.erase(eg_state->event_group_id_batch.begin() + kBatchSize,
                                           eg_state->event_group_id_batch.end());
    }
    // find and update the offsets
    // We iterate over event_group_id_batch in the reverse order, and update the private/public offset if the event
    // group id in event_group_id_batch is found in private_event_group_ids/public_event_group_ids respectively.
    // In the worst case, we need to read the entire event_group_id_batch (of size kBatchSize) to find the offset
    bool has_pub_offset_updated = false;
    bool has_pvt_offset_updated = false;
    for (auto r_it = eg_state->event_group_id_batch.rbegin(); r_it != eg_state->event_group_id_batch.rend(); r_it++) {
      if (has_pub_offset_updated && has_pvt_offset_updated) break;
      if (!has_pub_offset_updated) {
        if (auto pub_it = std::find(public_event_group_ids.begin(), public_event_group_ids.end(), *r_it);
            pub_it != public_event_group_ids.end()) {
          eg_state->public_offset = ++pub_it - public_event_group_ids.begin();
          has_pub_offset_updated = true;
          LOG_DEBUG(logger_,
                    "Updated public_offset: " << eg_state->public_offset
                                              << " public_event_group_ids size: " << public_event_group_ids.size());
        }
      }
      if (!has_pvt_offset_updated) {
        if (auto pvt_it = std::find(private_event_group_ids.begin(), private_event_group_ids.end(), *r_it);
            pvt_it != private_event_group_ids.end()) {
          eg_state->private_offset = ++pvt_it - private_event_group_ids.begin();
          has_pvt_offset_updated = true;
          LOG_DEBUG(logger_,
                    "Updated private_offset: " << eg_state->private_offset
                                               << " private_event_group_ids size: " << private_event_group_ids.size());
        }
      }
    }
  }
  // let's not return an invalid event group id
  if (eg_state->event_group_id_batch.empty()) return std::nullopt;

  // Reset the iterator so that we read from event_group_id_batch from the beginning
  eg_state->it = eg_state->event_group_id_batch.begin();

  ConcordAssertLE(eg_state->event_group_id_batch.size(), kBatchSize);
  LOG_DEBUG(logger_, "Updated event group id list batch size: " << eg_state->event_group_id_batch.size());
  return *eg_state->it++;
}

void KvbAppFilter::readEventGroupRange(EventGroupId event_group_id_start,
                                       spsc_queue<KvbFilteredEventGroupUpdate> &queue_out,
                                       const std::atomic_bool &stop_execution) {
  uint64_t public_start = getValueFromLatestTable(kPublicEgIdKeyOldest);
  uint64_t private_start = getValueFromLatestTable(client_id_ + "_oldest");
  uint64_t public_end = getValueFromLatestTable(kPublicEgIdKeyNewest);
  uint64_t private_end = getValueFromLatestTable(client_id_ + "_newest");
  LOG_DEBUG(logger_,
            "Reading event group range, public_start: " << public_start << " private_start: " << private_start
                                                        << " public_end: " << public_end
                                                        << " private_end: " << private_end);
  if (!public_start && !private_start) {
    std::stringstream msg;
    msg << "Event groups do not exist for client: " << client_id_ << " yet.";
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  // update the offsets if we now have corresponding public/private event groups in storage
  if (eg_data_state_->public_offset == 0) eg_data_state_->public_offset = public_start;
  if (eg_data_state_->private_offset == 0) eg_data_state_->private_offset = private_start;

  uint64_t event_group_id_end = private_end + public_end;

  if (event_group_id_start == 0) {
    throw InvalidEventGroupId(event_group_id_start);
  }
  if (event_group_id_start > event_group_id_end) {
    throw InvalidEventGroupRange(event_group_id_start, event_group_id_end);
  }

  // populate and read global event group ids from eg_data_state_->event_group_id_batch in batches of size kBatchSize.
  // For every global event group id received, lookup the data table to fetch the event group, filter and push the
  // filtered even group to queue_out
  while (eg_data_state_->curr_trid_event_group_id < event_group_id_end) {
    LOG_DEBUG(logger_,
              "Current tag_event_group_id: " << eg_data_state_->curr_trid_event_group_id
                                             << ", event_group_id_end: " << event_group_id_end);
    auto opt = getNextEventGroupId(eg_data_state_);
    uint64_t global_event_group_id;
    if (opt.has_value()) {
      global_event_group_id = opt.value();
    } else {
      std::stringstream msg;
      msg << "No more event groups in storage";
      LOG_WARN(logger_, msg.str());
      break;
    }
    eg_data_state_->curr_trid_event_group_id++;
    LOG_DEBUG(logger_,
              "Global_event_group_id: " << global_event_group_id
                                        << ", trid_event_group_id: " << eg_data_state_->curr_trid_event_group_id
                                        << ", event_group_id_start: " << event_group_id_start);
    // we are not at the starting point in the event group list yet, let's keep incrementing
    // eg_data_state_->curr_trid_event_group_id until we reach the start
    if (eg_data_state_->curr_trid_event_group_id < event_group_id_start) {
      continue;
    }
    std::string cid;
    auto event_group = getEventGroup(global_event_group_id, cid);
    if (event_group.events.empty()) {
      std::stringstream msg;
      msg << "EventGroup doesn't exist for valid event_group_id: " << global_event_group_id;
      throw KvbReadError(msg.str());
    }
    KvbFilteredEventGroupUpdate update{eg_data_state_->curr_trid_event_group_id,
                                       filterEventsInEventGroup(eg_data_state_->curr_trid_event_group_id, event_group)};
    while (!stop_execution) {
      if (queue_out.push(update)) {
        break;
      }
    }
    if (stop_execution) {
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

string KvbAppFilter::readEventGroupHash(EventGroupId requested_event_group_id) {
  uint64_t public_start = getValueFromLatestTable(kPublicEgIdKeyOldest);
  uint64_t private_start = getValueFromLatestTable(client_id_ + "_oldest");
  uint64_t public_end = getValueFromLatestTable(kPublicEgIdKeyNewest);
  uint64_t private_end = getValueFromLatestTable(client_id_ + "_newest");
  if (!public_start && !private_start) {
    std::stringstream msg;
    msg << "Event groups do not exist for client: " << client_id_ << " yet.";
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  uint64_t global_event_group_id;
  uint64_t event_group_id_end = private_end + public_end;
  if (requested_event_group_id == 0) {
    throw InvalidEventGroupId(requested_event_group_id);
  }
  if (requested_event_group_id > event_group_id_end) {
    throw InvalidEventGroupRange(requested_event_group_id, event_group_id_end);
  }

  // update the offsets if we now have corresponding public/private event groups in storage
  if (eg_hash_state_->public_offset == 0) eg_hash_state_->public_offset = public_start;
  if (eg_hash_state_->private_offset == 0) eg_hash_state_->private_offset = private_start;

  std::optional<uint64_t> opt;
  // populate and read global event group ids from eg_data_state_->event_group_id_batch in batches of size kBatchSize,
  // until requested_event_group_id reached. When global event group id for requested_event_group_id is received, lookup
  // the data table to fetch the event group, filter the event group, and calculate the hash from the filtered event
  // group update.
  while (eg_hash_state_->curr_trid_event_group_id < requested_event_group_id) {
    LOG_DEBUG(logger_,
              "Requested_event_group_id: " << requested_event_group_id
                                           << ", trid_event_group_id: " << eg_hash_state_->curr_trid_event_group_id);
    opt = getNextEventGroupId(eg_hash_state_);
    eg_hash_state_->curr_trid_event_group_id++;
    if (eg_hash_state_->curr_trid_event_group_id == requested_event_group_id) {
      if (opt.has_value()) {
        global_event_group_id = opt.value();
      } else {
        std::stringstream msg;
        msg << "No more event groups in storage";
        throw KvbReadError(msg.str());
      }
    }
  }
  std::string cid;
  LOG_DEBUG(logger_,
            "In readEventGroupHash, requested_event_group_id: " << requested_event_group_id
                                                                << " global_event_group_id: " << global_event_group_id);
  auto event_group = getEventGroup(global_event_group_id, cid);
  if (event_group.events.empty()) {
    std::stringstream msg;
    msg << "Couldn't retrieve block event groups for event_group_id " << global_event_group_id;
    throw KvbReadError(msg.str());
  }
  KvbFilteredEventGroupUpdate filtered_update{requested_event_group_id,
                                              filterEventsInEventGroup(requested_event_group_id, event_group)};
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

string KvbAppFilter::readEventGroupRangeHash(EventGroupId event_group_id_start) {
  uint64_t public_start = getValueFromLatestTable(kPublicEgIdKeyOldest);
  uint64_t public_end = getValueFromLatestTable(kPublicEgIdKeyNewest);
  uint64_t private_start = getValueFromLatestTable(client_id_ + "_oldest");
  uint64_t private_end = getValueFromLatestTable(client_id_ + "_newest");
  if (!public_start && !private_start) {
    std::stringstream msg;
    msg << "Event groups do not exist for client: " << client_id_ << " yet.";
    LOG_ERROR(logger_, msg.str());
    throw std::runtime_error(msg.str());
  }
  uint64_t event_group_id_end = private_end + public_end;
  if (event_group_id_start == 0) {
    throw InvalidEventGroupId(event_group_id_start);
  }
  if (event_group_id_start > event_group_id_end) {
    throw InvalidEventGroupRange(event_group_id_start, event_group_id_end);
  }
  string concatenated_update_hashes;
  // we might reserve more than we need because we can have duplicate entries b/w [event_group_id_start,
  // event_group_id_end]
  concatenated_update_hashes.reserve((1 + event_group_id_end - event_group_id_start) *
                                     kExpectedSHA256HashLengthInBytes);

  // update the offsets if we now have corresponding public/private event groups in storage
  if (eg_hash_state_->public_offset == 0) eg_hash_state_->public_offset = public_start;
  if (eg_hash_state_->private_offset == 0) eg_hash_state_->private_offset = private_start;

  // populate and read global event group ids from eg_data_state_->event_group_id_batch in batches of size kBatchSize.
  // For every global event group id received, lookup the data table to fetch the event group, filter and calculate the
  // concatenate hash from the filtered event group
  while (eg_hash_state_->curr_trid_event_group_id < event_group_id_end) {
    auto opt = getNextEventGroupId(eg_hash_state_);
    uint64_t global_event_group_id;
    if (opt.has_value()) {
      global_event_group_id = opt.value();
    } else {
      std::stringstream msg;
      msg << "No more event groups in storage";
      LOG_WARN(logger_, msg.str());
      break;
    }
    eg_hash_state_->curr_trid_event_group_id++;
    // we are not at the starting point in the event group list yet, let's keep incrementing
    // eg_data_state_->curr_trid_event_group_id until we reach the start
    if (eg_hash_state_->curr_trid_event_group_id < event_group_id_start) continue;

    std::string cid;
    LOG_DEBUG(logger_,
              "Event_group_id_start: " << event_group_id_start << " global_event_group_id: " << global_event_group_id);
    auto event_group = getEventGroup(global_event_group_id, cid);
    if (event_group.events.empty()) {
      std::stringstream msg;
      msg << "EventGroup doesn't exist for valid event_group_id: " << global_event_group_id;
      throw KvbReadError(msg.str());
    }
    KvbFilteredEventGroupUpdate filtered_update{
        eg_hash_state_->curr_trid_event_group_id,
        filterEventsInEventGroup(eg_hash_state_->curr_trid_event_group_id, event_group)};
    concatenated_update_hashes.append(hashEventGroupUpdate(filtered_update));
  }
  return computeSHA256Hash(concatenated_update_hashes);
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
  // Not all blocks have events.
  auto immutable = updates.value().categoryUpdates(concord::kvbc::categorization::kExecutionEventsCategory);
  if (!immutable) {
    return kvbc::categorization::ImmutableInput{};
  }
  return std::get<kvbc::categorization::ImmutableInput>(immutable->get());
}

kvbc::categorization::EventGroup KvbAppFilter::getEventGroup(kvbc::EventGroupId global_event_group_id,
                                                             std::string &cid) {
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

}  // namespace kvbc
}  // namespace concord
