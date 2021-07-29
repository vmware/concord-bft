// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef THIN_REPLICA_CLIENT_UPDATE_HPP_
#define THIN_REPLICA_CLIENT_UPDATE_HPP_

#include <chrono>
#include <map>
#include <string>
#include <variant>
#include <vector>

namespace client::thin_replica_client {

// Types for updates the Thin Replica Client streams from Thin Replica Servers.
struct EventGroup {
  uint64_t id;
  std::vector<std::string> events;
  std::chrono::microseconds record_time;
  // This map follows the W3C specification for trace context.
  // https://www.w3.org/TR/trace-context/#trace-context-http-headers-format
  std::map<std::string, std::string> trace_context;
};

// LegacyEvent
struct Update {
  // Block ID for this update; Block IDs can be expected to be monotonically
  // increasing with each update received in order. It is recommended that
  // applications receiving updates persist at least the Block ID of the most
  // recently received update (though only after any data from the update to be
  // persisted has been persisted), as Block IDs can be used to resume a
  // subscription without having to stream updates preceding that Block ID.
  uint64_t block_id;

  // Actual data for this update, structured as updates to key value pairs.
  std::vector<std::pair<std::string, std::string>> kv_pairs;
  std::string correlation_id_;
  std::string span_context;
};

typedef std::variant<Update, EventGroup> EventVariant;

}  // namespace client::thin_replica_client

#endif  // THIN_REPLICA_CLIENT_UPDATE_HPP_
