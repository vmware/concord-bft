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

#include "assertUtils.hpp"
#include "block_update/block_update.hpp"
#include "block_update/event_group_update.hpp"
#include "db_interfaces.h"
#include "categorization/db_categories.h"
#include "kv_types.hpp"
#include "event_group_msgs.cmf.hpp"
#include "endianness.hpp"

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

// We need to preserve state per client across calls to getNextEventGroupId method
struct EventGroupClientState {
  EventGroupClientState(const uint64_t pub_oldest,
                        const uint64_t pub_newest,
                        const uint64_t pvt_oldest,
                        const uint64_t pvt_newest)
      : public_offset(pub_oldest), private_offset(pvt_oldest) {
    // Because of pruning, the oldest tag-specific event group might not be available anymore.
    // Therefore, we have to calculate the first available event group ID for this (private) tag.
    // Note: If *_oldest is 0 then all event groups were pruned or never existed.
    curr_trid_event_group_id = 0;

    if (pub_oldest == 0) {
      if (pub_newest) {
        // All public event groups were pruned
        curr_trid_event_group_id += pub_newest;
      }
    } else {
      curr_trid_event_group_id += pub_oldest;
    }

    if (pvt_oldest == 0) {
      if (pvt_newest) {
        // All private event groups were pruned
        curr_trid_event_group_id += pvt_newest;
      }
    } else {
      curr_trid_event_group_id += pvt_oldest;
    }

    // Note: curr_trid_event_group_id has to start at oldest - 1
    if (pub_oldest && pvt_oldest) {
      curr_trid_event_group_id -= 2;
    } else if (pub_oldest || pvt_oldest) {
      curr_trid_event_group_id -= 1;
    }
  }
  // holds a batch of the global event group IDs ordered in the order in which they were generated
  std::vector<uint64_t> event_group_id_batch{};
  // keeps track of the global event group id read from event_group_id_batch
  std::vector<uint64_t>::iterator it = event_group_id_batch.begin();
  // the offset event group id for public/private event groups, everytime event_group_id_batch is populated with
  // public/public event group ids, we need to save the offset that determines the next public/private event group id to
  // be read from the tag table
  uint64_t public_offset;
  uint64_t private_offset;
  // current tag-specific event_group_id
  uint64_t curr_trid_event_group_id;
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
    auto eg_id_pub_oldest = getValueFromLatestTable(kPublicEgIdKeyOldest);
    auto eg_id_pub_newest = getValueFromLatestTable(kPublicEgIdKeyNewest);
    auto eg_id_pvt_oldest = getValueFromLatestTable(client_id + "_oldest");
    auto eg_id_pvt_newest = getValueFromLatestTable(client_id + "_newest");
    eg_hash_state_ =
        std::make_shared<EventGroupClientState>(eg_id_pub_oldest, eg_id_pub_newest, eg_id_pvt_oldest, eg_id_pvt_newest);
    eg_data_state_ =
        std::make_shared<EventGroupClientState>(eg_id_pub_oldest, eg_id_pub_newest, eg_id_pvt_oldest, eg_id_pvt_newest);
  }

  // Filter legacy events
  KvbFilteredUpdate filterUpdate(const KvbUpdate &update);

  // Filter event groups
  KvbFilteredEventGroupUpdate filterEventGroupUpdate(const EgUpdate &update);
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

  uint64_t oldestTagSpecificPublicEventGroupId() const;

  uint64_t newestTagSpecificPublicEventGroupId() const;

  // Generate event group ids in batches from storage
  // We do not want to process in memory, all the event group ids generated in a pruning window
  std::optional<uint64_t> getNextEventGroupId(std::shared_ptr<EventGroupClientState> &eg_state);

  kvbc::categorization::EventGroup getEventGroup(kvbc::EventGroupId event_group_id, std::string &cid);

  // Return the oldest global event group id.
  // If no event group can be found then 0 (invalid group id) is returned.
  uint64_t getOldestEventGroupId() const;

  uint64_t getNewestPublicEventGroupId() const;

  // Return the block number of the very first global event group.
  // Optional because during start-up there might be no block/event group written yet.
  std::optional<BlockId> getOldestEventGroupBlockId();

 private:
  logging::Logger logger_;
  const concord::kvbc::IReader *rostorage_{nullptr};
  const std::string client_id_;
  static inline const std::string kGlobalEgIdKeyOldest{"_global_eg_id_oldest"};
  static inline const std::string kPublicEgIdKeyOldest{"_public_eg_id_oldest"};
  static inline const std::string kPublicEgIdKeyNewest{"_public_eg_id_newest"};
  static inline const std::string kPublicEgId{"_public_eg_id"};

  // tag + kTagTableKeySeparator + latest_tag_event_group_id concatenation is used as key for kv-updates of type
  // kExecutionEventGroupTagCategory
  static inline const std::string kTagTableKeySeparator{"#"};

  // event groups are read in batches from storage, to avoid saving large number of event groups in memory
  // see method definition for getNextEventGroupId()
  static inline const size_t kBatchSize{10};

  // event group hash state for that client
  std::shared_ptr<EventGroupClientState> eg_hash_state_;
  std::shared_ptr<EventGroupClientState> eg_data_state_;
};

}  // namespace kvbc
}  // namespace concord

#endif  // CONCORD_KVBC_APP_FILTER_H_
