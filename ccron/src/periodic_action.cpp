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

#include "ccron/periodic_action.hpp"

#include "ccron_msgs.cmf.hpp"

#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

namespace concord::cron {

static PeriodicActionSchedule schedule;
static std::once_flag init_once;

static const auto periodicCronReservedPageId = 3;

static void saveSchedule(bftEngine::IReservedPages& reserved_pages) {
  auto output = std::vector<std::uint8_t>{};
  serialize(output, schedule);
  // TODO: implement a client that inherits from ResPagesClient in order to genearate the correct reserved page ID
  reserved_pages.saveReservedPage(
      periodicCronReservedPageId, output.size(), reinterpret_cast<const char*>(output.data()));
}

CronEntry periodicAction(std::uint32_t position,
                         const Action& action,
                         const std::chrono::milliseconds& period,
                         bftEngine::IReservedPages& reserved_pages) {
  std::call_once(init_once, [&reserved_pages]() {
    auto input = std::vector<std::uint8_t>(reserved_pages.sizeOfReservedPage());
    if (reserved_pages.loadReservedPage(
            periodicCronReservedPageId, input.size(), reinterpret_cast<char*>(input.data()))) {
      deserialize(input, schedule);
    }
  });

  auto rule = [position](const Tick& tick) {
    // Periodic actions cannot be implemented without time.
    if (!tick.ms_since_epoch.has_value()) {
      return false;
    }
    auto component_it = schedule.components.find(tick.component_id);
    if (component_it == schedule.components.cend()) {
      return true;
    }
    auto& component_table = component_it->second;
    auto next_invocation_it = component_table.find(position);
    // If the position doesn't exist in the persisted schedule (i.e. the table was created with new positions after
    // persistance), assume this is the first invocation and allow it.
    // If the position exists, check the time schedule.
    if (next_invocation_it == component_table.cend() ||
        tick.ms_since_epoch->count() >= next_invocation_it->second.ms_since_epoch) {
      return true;
    }
    return false;
  };

  auto schedule_next = [position, &reserved_pages, period](const Tick& tick) {
    // If schedule next is called, then we know the tick has time.
    auto next = NextInvocation{(*tick.ms_since_epoch + period).count()};
    schedule.components[tick.component_id][position] = next;
    saveSchedule(reserved_pages);
  };

  auto on_remove = [&reserved_pages](std::uint32_t component_id, std::uint32_t position) {
    schedule.components[component_id].erase(position);
    saveSchedule(reserved_pages);
  };

  return CronEntry{position, std::move(rule), action, std::move(schedule_next), std::move(on_remove)};
}

}  // namespace concord::cron