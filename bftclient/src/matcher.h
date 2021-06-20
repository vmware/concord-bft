// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <algorithm>
#include <map>
#include <optional>
#include <set>

#include "Logger.hpp"
#include "bftclient/config.h"
#include "msg_receiver.h"

namespace bft::client {

struct MatchConfig {
  // All quorums can be distilled into an MofN quorum.
  MofN quorum;
  uint64_t sequence_number;
  bool include_primary_ = true;  // by default part of the match is the current primary
};

// The parts of data that must match in a reply for quorum to be reached
struct MatchKey {
  ReplyMetadata metadata;
  Msg data;

  bool operator==(const MatchKey& other) const { return metadata == other.metadata && data == other.data; }
  bool operator!=(const MatchKey& other) const { return !(*this == other); }
  bool operator<(const MatchKey& other) const {
    if (metadata < other.metadata) {
      return true;
    }
    if (metadata == other.metadata && data < other.data) {
      return true;
    }
    return false;
  }
};

// A successful match
struct Match {
  Reply reply;
  std::optional<ReplicaId> primary;
};

// Match replies for a given quorum.
class Matcher {
 public:
  Matcher(const MatchConfig& config) : config_(config) {}

  std::optional<Match> onReply(UnmatchedReply&& reply);

  // Return the number of replies from replicas that don't match, excluding RSI. When this number
  // exceeds a threshold, we may want to clear all replies to free memory, if a request is expected to
  // go on for a long time.
  size_t numDifferentReplies() const { return matches_.size(); }

  void clearReplies() { matches_.clear(); }

  std::optional<ReplicaId> getPrimary() {
    if (!config_.include_primary_) return std::nullopt;
    return primary_;
  }

 private:
  // Check the validity of a reply
  bool valid(const UnmatchedReply& reply) const;

  // Is the reply from a source listed in the quorum's destination?
  bool validSource(const ReplicaId& source) const;

  // Check for a quorum based on config_ and matches_
  std::optional<Match> match();

  MatchConfig config_;
  std::optional<ReplicaId> primary_;

  logging::Logger logger_ = logging::getLogger("bftclient.matcher");

  // A map from a MatchKey to ReplicaSpecificInfo
  //
  // We store it as a nested map so that in case a replica returns different ReplicaSpecificInfo.data, we don't count it
  // as 2 replies.
  //
  // In case this occurs, the replica is buggy or malicious, and we should log it and reject any replies from that
  // replica. In the future we can keep track of this across future requests, but for now, we just log it and worry
  // about it for the current match.
  std::map<MatchKey, std::map<ReplicaId, Msg>> matches_;
};

}  // namespace bft::client
