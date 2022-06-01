// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

namespace concord::cron {

// Represents a tick that is passed to the cron table and its entries.
struct Tick {
  // The component ID this tick is for.
  std::uint32_t component_id{0};

  // The BFT sequence number of this tick.
  std::uint64_t bft_sequence_num{0};

  // Time that is the same across replicas after being agreed upon.
  // It is optional as the time service that provides agreement across replicas is itself optional.
  std::optional<std::chrono::milliseconds> ms_since_epoch;

  // a sequnce number, may contain several ticks, this field is used in order to order them.
  std::uint64_t internal_order{};
};

inline bool operator==(const Tick& l, const Tick& r) {
  return (l.component_id == r.component_id && l.bft_sequence_num == r.bft_sequence_num &&
          l.ms_since_epoch == r.ms_since_epoch && l.internal_order == r.internal_order);
}

}  // namespace concord::cron
