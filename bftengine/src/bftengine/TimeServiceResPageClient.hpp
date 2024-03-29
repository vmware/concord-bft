// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "ReservedPagesClient.hpp"
#include "TimeService.hpp"

namespace bftEngine::impl {
class TimeServiceResPageClient : private ResPagesClient<TimeServiceResPageClient, 1> {
 public:
  TimeServiceResPageClient();
  ~TimeServiceResPageClient() = default;
  TimeServiceResPageClient(const TimeServiceResPageClient&) = delete;

  ConsensusTime getLastTimestamp() const { return last_timestamp_; }

  // saves the timestamp in reserved pages
  void setLastTimestamp(ConsensusTime timestamp);
  // saves the timestamp in reserved pages from ticks
  void setTimestampFromTicks(ConsensusTickRep ticks);

  // loads the timestamp from reserved pages, to be called on ST completed
  void load();

 private:
  ConsensusTime last_timestamp_ = ConsensusTime::min();
};
}  // namespace bftEngine::impl
