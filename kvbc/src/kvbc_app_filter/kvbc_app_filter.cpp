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
#include "categorization/db_categories.h"
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

KvbFilteredUpdate KvbAppFilter::filterUpdate(const KvbUpdate &update) {
  auto &[block_id, cid, updates, _] = update;
  return KvbFilteredUpdate{block_id, cid, filterKeyValuePairs(updates)};
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
      msg << "Couldn't retrieve block data for block id " << block_id;
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
  // Get the cid
  auto internl_events = updates->categoryUpdates(concord::kvbc::kConcordInternalCategoryId);
  if (internl_events) {
    const auto &kv = std::get<kvbc::categorization::VersionedInput>(internl_events->get()).kv;
    auto it = kv.find(std::string{kvbc::kKvbKeyCorrelationId});
    if (it != kv.end()) cid = it->second.data;
  }
  return std::get<kvbc::categorization::ImmutableInput>(immutable->get());
}

}  // namespace kvbc
}  // namespace concord
