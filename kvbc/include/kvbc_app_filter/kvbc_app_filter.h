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
#include "kv_types.hpp"

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

class KvbAppFilter {
 public:
  KvbAppFilter(const concord::kvbc::IReader *rostorage, const std::string &client_id, const std::string &key_prefix)
      : logger_(logging::getLogger("concord.storage.KvbFilter")),
        rostorage_(rostorage),
        client_id_(client_id),
        key_prefix_(key_prefix) {}

  // Filter the given update
  KvbFilteredUpdate filterUpdate(const KvbUpdate &update);

  // Compute hash for the given update
  std::string hashUpdate(const KvbFilteredUpdate &update);

  // Return all key-value pairs from the KVB in the block range [earliest block
  // available, given block_id] with the following conditions:
  //   * The key-value pair is part of a block
  //   * The key starts with the given key_prefix
  // The result is pushed to the given queue. Thereby, the caller is responsible
  // for consuming the elements from the queue. The function will block if the
  // queue is full and therefore, it cannot push a new key-value pair.
  // Note: single producer & single consumer queue.
  void readBlockRange(kvbc::BlockId start,
                      kvbc::BlockId end,
                      boost::lockfree::spsc_queue<KvbFilteredUpdate> &queue_out,
                      const std::atomic_bool &stop_execution);

  // Compute the state hash of all key-value pairs in the range of [earliest
  // block available, given block_id] based on the given KvbAppFilter::AppType.
  std::string readBlockRangeHash(kvbc::BlockId start, kvbc::BlockId end);

  // Compute the hash of a single block based on the given
  // KvbAppFilter::AppType.
  std::string readBlockHash(kvbc::BlockId block_id);

  std::optional<kvbc::categorization::ImmutableInput> getBlockEvents(kvbc::BlockId block_id, std::string &cid);

  // Filter the given set of key-value pairs and return the result.
  KvbFilteredUpdate::OrderedKVPairs filterKeyValuePairs(const kvbc::categorization::ImmutableInput &kvs);

 private:
  logging::Logger logger_;
  const concord::kvbc::IReader *rostorage_;
  const std::string client_id_;
  const std::string key_prefix_;
};

}  // namespace kvbc
}  // namespace concord

#endif  // CONCORD_KVBC_APP_FILTER_H_
