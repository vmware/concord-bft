
// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use
// this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license
// terms. Your use of these subcomponents is subject to the terms and conditions of the
// subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <cstdint>
#include <map>
#include <vector>

namespace bft::client {

// A typesafe replica id.
struct ReplicaId {
  uint16_t val;

  bool operator==(const ReplicaId& other) const { return val == other.val; }
  bool operator!=(const ReplicaId& other) const { return val != other.val; }
  bool operator<(const ReplicaId& other) const { return val < other.val; }
};

// A typesafe client id.
struct ClientId {
  uint16_t val;

  bool operator==(const ClientId& other) const { return val == other.val; }
  bool operator!=(const ClientId& other) const { return val != other.val; }
  bool operator<(const ClientId& other) const { return val < other.val; }
};

enum Flags : uint8_t {
  EMPTY_FLAGS_REQ = 0x0,
  READ_ONLY_REQ = 0x1,
  PRE_PROCESS_REQ = 0x2,
  KEY_EXCHANGE_REQ = 0x8,
  RECONFIG_FLAG = 0x20
};

struct ReplicaSpecificInfo {
  ReplicaId from;
  std::vector<uint8_t> data;
};

typedef std::vector<uint8_t> Msg;

// `matched_data` contains any data that must be identical for a quorum of replicas
// `rsi` contains replica specific information that was received for each replying replica.
struct Reply {
  Msg matched_data;
  std::map<ReplicaId, Msg> rsi;
};

}  // namespace bft::client
