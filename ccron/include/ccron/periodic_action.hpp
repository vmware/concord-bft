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

#include "cron_entry.hpp"

#include "IReservedPages.hpp"

#include <cstdint>
#include <chrono>

namespace concord::cron {

// TODO: Use an ID from a reserved pages client.
static constexpr auto kPeriodicCronReservedPageId = 3;

// Create a cron table entry that represents a periodic `action` that occurs every `period` milliseconds.
// Works in conjunction with the `persistPeriodicSchedule()` entry:
//  -----------------------------------------------
//  | position|rule|action|schedule_next|on_remove|
//  -----------------------------------------------
//  |    1    | R1 |  A1  |     S1      |   RM1   |
//  |    2    | R2 |  A2  |     S2      |   RM2   |
//  |.............................................|
//  |    P    |  ~ |   ~  |     ~       |    ~    |
//  -----------------------------------------------
// Position P is the position of the `persistPeriodicSchedule()` entry that is run `after` all other periodic action
// entries. Rationale is that we want to avoid serializing the schedule to reserved pages multiple times (for every
// periodic action in the table) per tick. Instead, we accumulate the changes in memory and then use the
// `persistPeriodicSchedule()` entry to persist once, at the end.
CronEntry periodicAction(std::uint32_t position,
                         const Action& action,
                         const std::chrono::milliseconds& period,
                         bftEngine::IReservedPages& reserved_pages);

// Create a cron entry that persists the periodic actions schedule. To work as expected, users must ensure that its
// position is bigger than the positions of all other periodic action entries (see above).
CronEntry persistPeriodicSchedule(std::uint32_t position, bftEngine::IReservedPages& reserved_pages);

}  // namespace concord::cron
