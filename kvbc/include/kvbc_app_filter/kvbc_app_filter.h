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

#ifndef CONCORD_KVBC_APP_FILTER_H_
#define CONCORD_KVBC_APP_FILTER_H_

#include "categorization/updates.h"
#include <atomic>
#include <boost/lockfree/spsc_queue.hpp>
#include <future>
#include <set>
#include "Logger.hpp"

#include "block_update/block_update.hpp"
#include "db_interfaces.h"
#include "categorization/db_categories.h"
#include "kv_types.hpp"
#include "event_group_msgs.cmf.hpp"

namespace concord {
namespace kvbc {

// TODO: Move into concordUtils
typedef size_t KvbStateHash;

typedef BlockUpdate KvbUpdate;

struct KvbFilteredUpdate {
  using OrderedKVPairs = std::vector<std::pair<std::string, std::string>>;
  kvbc::BlockId block_id;
  std::string correlation_id;
  OrderedKVPairs kv_pairs;
};

struct KvbFilteredEventGroup {
  using EventGroup = concord::kvbc::categorization::EventGroup;
  uint64_t event_group_id;
  EventGroup event_group;
};

class KvbReadError : public std::exception {
 public:
  explicit KvbReadError(const std::string &what) : msg(what){};
  virtual const char *what() const noexcept override { return msg.c_str(); }

 private:
  std::string msg;
};

class InvalidBlockRange : public std::exception {
 public:
  InvalidBlockRange(const concord::kvbc::BlockId begin, const concord::kvbc::BlockId end)
      : msg_("Invalid block range") {
    msg_ += " [" + std::to_string(begin) + ", " + std::to_string(end) + "]";
  }
  const char *what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};

class InvalidEventGroupRange : public std::exception {
 public:
  InvalidEventGroupRange(const concord::kvbc::EventGroupId begin, const concord::kvbc::EventGroupId end)
      : msg_("Invalid event group range") {
    msg_ += " [" + std::to_string(begin) + ", " + std::to_string(end) + "]";
  }
  const char *what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};

class KvbAppFilter {
 public:
  KvbAppFilter(const concord::kvbc::IReader *rostorage, const std::string &client_id)
      : logger_(logging::getLogger("concord.storage.KvbFilter")), rostorage_(rostorage), client_id_(client_id) {}

  // Filter the given update
  KvbFilteredUpdate filterUpdate(const KvbUpdate &update);

  KvbFilteredEventGroup::EventGroup filterEventsInEventGroup(kvbc::EventGroupId event_group_id,
                                                             kvbc::categorization::EventGroup &event_group);

  // Compute hash for the given update
  std::string hashUpdate(const KvbFilteredUpdate &update);
  std::string hashEventGroupUpdate(const KvbFilteredEventGroup &update);

  // Return all key-value pairs from the KVB in the block range [earliest block
  // available, given block_id] with the following conditions:
  //   * The key-value pair is part of a block
  // The result is pushed to the given queue. Thereby, the caller is responsible
  // for consuming the elements from the queue. The function will block if the
  // queue is full and therefore, it cannot push a new key-value pair.
  // Note: single producer & single consumer queue.
  void readBlockRange(kvbc::BlockId start,
                      kvbc::BlockId end,
                      boost::lockfree::spsc_queue<KvbFilteredUpdate> &queue_out,
                      const std::atomic_bool &stop_execution);

  void readEventGroupRange(kvbc::EventGroupId event_group_id_start,
                           kvbc::EventGroupId event_group_id_end,
                           boost::lockfree::spsc_queue<KvbFilteredEventGroup> &queue_out,
                           const std::atomic_bool &stop_execution);

  // Compute the state hash of all key-value pairs in the range of [earliest
  // block available, given block_id] based on the given KvbAppFilter::AppType.
  std::string readBlockRangeHash(kvbc::BlockId start, kvbc::BlockId end);

  std::string readEventGroupRangeHash(kvbc::EventGroupId event_group_id_start, EventGroupId event_group_id_end);

  // Compute the hash of a single block based on the given
  // KvbAppFilter::AppType.
  std::string readBlockHash(kvbc::BlockId block_id);

  std::string readEventGroupHash(kvbc::EventGroupId event_group_id);

  std::optional<kvbc::categorization::ImmutableInput> getBlockEvents(kvbc::BlockId block_id, std::string &cid);

  // Filter the given set of key-value pairs and return the result.
  KvbFilteredUpdate::OrderedKVPairs filterKeyValuePairs(const kvbc::categorization::ImmutableInput &kvs);

  kvbc::categorization::EventGroup getEventGroup(kvbc::EventGroupId event_group_id, std::string &cid);

 private:
  logging::Logger logger_;
  const concord::kvbc::IReader *rostorage_;
  const std::string client_id_;
};

}  // namespace kvbc
}  // namespace concord

#endif  // CONCORD_KVBC_APP_FILTER_H_
