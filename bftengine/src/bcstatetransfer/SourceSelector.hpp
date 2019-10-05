// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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

#include "assertUtils.hpp"

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl {

static const uint16_t NO_REPLICA = UINT16_MAX;

// Information about which current source is selected and which replicas are
// preferred, as well as data that helps to select a current source replica.
class SourceSelector {
 public:
  std::set<uint16_t> preferredReplicas_;
  uint16_t currentReplica_ = NO_REPLICA;

 private:
  uint64_t sourceSelectionTimeMilli_ = 0;
  std::set<uint16_t> allOtherReplicas_;
  std::mt19937 randomGen_;
  uint32_t retransmissionTimeoutMilli_;
  uint32_t sourceReplacementTimeoutMilli_;


 public:
  SourceSelector(std::set<uint16_t> allOtherReplicas,
                 uint32_t retransmissionTimeoutMilli,
                 uint32_t sourceReplicaReplacementTimeoutMilli)
      : allOtherReplicas_(allOtherReplicas), randomGen_(std::random_device()()),
        retransmissionTimeoutMilli_(retransmissionTimeoutMilli),
        sourceReplacementTimeoutMilli_(sourceReplicaReplacementTimeoutMilli){}

  bool hasSource() const;
  void removeCurrentReplica();
  void setAllReplicasAsPreferred();
  void reset();
  bool isReset() const;
  bool retransmissionTimeoutExpired(uint64_t currTimeMilli) const;

  // Replace the source if necessary.
  //
  // Return true if the source was updated, false otherwise.
  bool updateSource(bool badDataFromCurrentSource, uint64_t currTimeMilli);

  // Reset the source selection time without actually changing the source
  void setSourceSelectionTime(uint64_t currTimeMilli);

  // Create a list of ids of the form "0, 1, 4"
  std::string preferredReplicasToString() const;

 private:
  uint64_t timeSinceSourceSelectedMilli(uint64_t currTimeMilli) const;
  bool shouldReplaceSource(uint64_t currTimeMilli, bool badDataFromCurrentSource) const;
  void selectSource(uint64_t currTimeMilli);
};
}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine
