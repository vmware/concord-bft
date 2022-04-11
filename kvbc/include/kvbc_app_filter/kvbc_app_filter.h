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
#include <optional>
#include <set>
#include "Logger.hpp"

#include "assertUtils.hpp"
#include "block_update/block_update.hpp"
#include "block_update/event_group_update.hpp"
#include "db_interfaces.h"
#include "categorization/db_categories.h"
#include "kv_types.hpp"
#include "event_group_msgs.cmf.hpp"
#include "endianness.hpp"
#include "kvbc_key_types.h"

namespace concord {
namespace kvbc {

// TODO: Move into concordUtils
typedef size_t KvbStateHash;

typedef BlockUpdate KvbUpdate;
typedef EventGroupUpdate EgUpdate;
// global_event_group_id, external_tag_event_group_id
typedef std::pair<uint64_t, uint64_t> TagTableValue;

struct KvbFilteredUpdate {
  using OrderedKVPairs = std::vector<std::pair<std::string, std::string>>;
  kvbc::BlockId block_id;
  std::string correlation_id;
  OrderedKVPairs kv_pairs;
};

struct KvbFilteredEventGroupUpdate {
  using EventGroup = concord::kvbc::categorization::EventGroup;
  uint64_t event_group_id;
  EventGroup event_group;
};

struct FindGlobalEgIdResult {
  // Global event group id matching the given external id
  uint64_t global_id;
  // Whether the event group is private to the client or public
  bool is_public;
  // Either the matching private/public id or the newest up to this external id
  // Note: The one that is not matching could be pruned (not queryable)
  uint64_t private_id;
  uint64_t public_id;
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

class InvalidEventGroupId : public std::exception {
 public:
  InvalidEventGroupId(const concord::kvbc::EventGroupId eg_id) : msg_("Invalid event group id: ") {
    msg_ += std::to_string(eg_id);
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

class NoLegacyEvents : public std::exception {
 public:
  const char *what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_{"Legacy events requested but event groups found"};
};

class KvbAppFilter {
 public:
  KvbAppFilter(const concord::kvbc::IReader *rostorage, const std::string &client_id)
      : logger_(logging::getLogger("concord.storage.KvbAppFilter")), rostorage_(rostorage), client_id_(client_id) {
    ConcordAssertNE(rostorage_, nullptr);
  }

  // Filter legacy events
  KvbFilteredUpdate filterUpdate(const KvbUpdate &update);

  // Filter event groups
  std::optional<KvbFilteredEventGroupUpdate> filterEventGroupUpdate(const EgUpdate &update);
  KvbFilteredEventGroupUpdate::EventGroup filterEventsInEventGroup(kvbc::EventGroupId event_group_id,
                                                                   const kvbc::categorization::EventGroup &event_group);
  // Compute hash for the given update
  static std::string hashUpdate(const KvbFilteredUpdate &update);
  static std::string hashEventGroupUpdate(const KvbFilteredEventGroupUpdate &update);

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

  void readEventGroups(kvbc::EventGroupId event_group_id_start,
                       const std::function<bool(KvbFilteredEventGroupUpdate &&)> &);
  void readEventGroupRange(kvbc::EventGroupId event_group_id_start,
                           boost::lockfree::spsc_queue<KvbFilteredEventGroupUpdate> &queue_out,
                           const std::atomic_bool &stop_execution);

  // Compute the state hash of all key-value pairs in the range of [earliest
  // block available, given block_id] based on the given KvbAppFilter::AppType.
  std::string readBlockRangeHash(kvbc::BlockId start, kvbc::BlockId end);

  std::string readEventGroupRangeHash(kvbc::EventGroupId event_group_id_start);

  // Compute the hash of a single block based on the given
  // KvbAppFilter::AppType.
  std::string readBlockHash(kvbc::BlockId block_id);

  std::string readEventGroupHash(kvbc::EventGroupId event_group_id);

  std::optional<kvbc::categorization::ImmutableInput> getBlockEvents(kvbc::BlockId block_id, std::string &cid);

  // Filter the given set of key-value pairs and return the result.
  KvbFilteredUpdate::OrderedKVPairs filterKeyValuePairs(const kvbc::categorization::ImmutableInput &kvs);

  uint64_t getValueFromLatestTable(const std::string &key) const;

  TagTableValue getValueFromTagTable(const std::string &key) const;

  uint64_t oldestExternalEventGroupId() const;

  uint64_t newestExternalEventGroupId() const;

  // Given a tag-specific public (external) event group id, return the corresponding global event group id
  // Precondition: We expect that the requested external event group id exists in storage
  FindGlobalEgIdResult findGlobalEventGroupId(uint64_t external_event_group_id) const;

  kvbc::categorization::EventGroup getEventGroup(kvbc::EventGroupId event_group_id) const;

  // Return the oldest global event group id.
  // If no event group can be found then 0 (invalid group id) is returned.
  uint64_t getOldestGlobalEventGroupId() const;

  // Return the ID of the newest public event group.
  // If no event group can be found, then 0 is returned.
  uint64_t getNewestPublicEventGroupId() const;

  // Return the newest public event group, if existing. If non-existent, return std::nullopt.
  // Throws on errors.
  std::optional<kvbc::categorization::EventGroup> getNewestPublicEventGroup() const;

  // Return the block number of the very first global event group.
  // Optional because during start-up there might be no block/event group written yet.
  std::optional<BlockId> getOldestEventGroupBlockId();

  void setLastEgIdsRead(uint64_t last_ext_eg_id_read, uint64_t last_global_eg_id_read);

  // Return the pair {last_ext_eg_id_read, last_global_eg_id_read}
  std::pair<uint64_t, uint64_t> getLastEgIdsRead();

 public:
  static inline const std::string kGlobalEgIdKeyOldest{"_global_eg_id_oldest"};
  static inline const std::string kGlobalEgIdKeyNewest{"_global_eg_id_newest"};
  static inline const std::string kPublicEgIdKeyOldest{"_public_eg_id_oldest"};
  static inline const std::string kPublicEgIdKeyNewest{"_public_eg_id_newest"};
  static inline const std::string kPublicEgId{"_public_eg_id"};

  // tag + kTagTableKeySeparator + latest_tag_event_group_id concatenation is used as key for kv-updates of type
  // kExecutionEventGroupTagCategory
  static inline const std::string kTagTableKeySeparator{"#"};

 private:
  logging::Logger logger_;
  const concord::kvbc::IReader *rostorage_{nullptr};
  const std::string client_id_;
  const std::string cid_key_{kKvbKeyCorrelationId};

  std::pair<uint64_t, uint64_t> last_ext_and_global_eg_id_read_{0, 0};
};

}  // namespace kvbc
}  // namespace concord

#endif  // CONCORD_KVBC_APP_FILTER_H_
