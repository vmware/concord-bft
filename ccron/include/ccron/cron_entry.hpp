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

#include "tick.hpp"

#include <functional>
#include <optional>

namespace concord::cron {

using Rule = std::function<bool(const Tick&)>;
using Action = std::function<void(const Tick&)>;
using ScheduleNext = std::function<void(const Tick&)>;
using OnRemove = std::function<void(std::uint32_t component_id, std::uint32_t position)>;

// Represents a cron table entry.
// Note: All callbacks should be `noexcept`. If a callback throws an exception, the behaviour is undefined.
struct CronEntry {
  // Absolute position in the cron table.
  std::uint32_t position{0};

  // Whether to execute the action and schedule a next invocation.
  Rule rule;

  // The action to execute if rule returns true.
  Action action;

  // Schedule (or persist) the next invocation, if applicable.
  std::optional<ScheduleNext> schedule_next;

  // What to do if this entry is removed from the cron table, if applicable.
  std::optional<OnRemove> on_remove;
};

}  // namespace concord::cron
