// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
#pragma once

#include <random>
#include <set>
#include <stdint.h>
#include <sstream>

#include "Logger.hpp"
#include "assertUtils.hpp"

namespace bftEngine {
namespace bcst {
namespace impl {

static const uint16_t NO_REPLICA = UINT16_MAX;

// Information about which current source is selected and which replicas are
// preferred, as well as data that helps to select a current source replica.
class SourceSelector {
 public:
  SourceSelector(std::set<uint16_t> allOtherReplicas,
                 uint32_t retransmissionTimeoutMilli,
                 uint32_t sourceReplicaReplacementTimeoutMilli)
      : allOtherReplicas_(std::move(allOtherReplicas)),
        randomGen_(std::random_device()()),
        retransmissionTimeoutMilli_(retransmissionTimeoutMilli),
        sourceReplacementTimeoutMilli_(sourceReplicaReplacementTimeoutMilli) {}

  bool hasSource() const;
  void removeCurrentReplica();
  void setAllReplicasAsPreferred();
  void reset();
  bool isReset() const;
  bool retransmissionTimeoutExpired(uint64_t currTimeMilli) const;

  // Return true if the source should be replaced, false otherwise.
  bool shouldReplaceSource(uint64_t currTimeMilli, bool badDataFromCurrentSource) const;

  // Replace the source.
  void updateSource(uint64_t currTimeMilli);

  // Reset the source selection time without actually changing the source
  void setSourceSelectionTime(uint64_t currTimeMilli);

  // Set the latest time of last sent transmission of FetchResPagesMsg or last received ItemDataMsg
  void setFetchingTimeStamp(logging::Logger &logger, uint64_t currTimeMilli);

  // Create a list of ids of the form "0, 1, 4"
  std::string preferredReplicasToString() const;

  bool hasPreferredReplicas() const { return !preferredReplicas_.empty(); }

  bool noPreferredReplicas() const { return preferredReplicas_.empty(); }

  void addPreferredReplica(uint16_t replicaId) { preferredReplicas_.insert(replicaId); }

  uint16_t numberOfPreferredReplicas() const { return static_cast<uint16_t>(preferredReplicas_.size()); }

  bool isPreferred(uint16_t replicaId) const { return preferredReplicas_.count(replicaId) != 0; }

  uint16_t currentReplica() const { return currentReplica_; }

 private:
  uint64_t timeSinceSourceSelectedMilli(uint64_t currTimeMilli) const;
  void selectSource(uint64_t currTimeMilli);

  std::set<uint16_t> preferredReplicas_;
  uint16_t currentReplica_ = NO_REPLICA;
  uint64_t sourceSelectionTimeMilli_ = 0;
  uint64_t fetchingTimeStamp_ = 0;
  std::set<uint16_t> allOtherReplicas_;
  std::mt19937 randomGen_;
  uint32_t retransmissionTimeoutMilli_;
  uint32_t sourceReplacementTimeoutMilli_;
};
}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
