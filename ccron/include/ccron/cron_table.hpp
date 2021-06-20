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
#include "tick.hpp"

#include <cstdint>
#include <map>

namespace concord::cron {

// A cron table is an ordered list of cron entries per a single component.
class CronTable {
 public:
  CronTable(std::uint32_t component_id);

 public:
  void addEntry(CronEntry entry);

  // Return true if the entry at `position` existed and false otherwise.
  bool removeEntry(std::uint32_t position);

  // Return the number of entries removed.
  std::uint32_t removeAllEntries();

  // Returns a pointer to the cron entry at `position` if existing and nullptr otherwise.
  const CronEntry* at(std::uint32_t position) const;

  std::uint32_t numberOfEntries() const;
  bool empty() const;

  std::uint32_t componentId() const;

  // Evaluate the entries in the cron table when a tick occurs.
  void evaluate(const Tick& tick) const;

 private:
  std::uint32_t component_id_{0};
  std::map<std::uint32_t, CronEntry> entries_;
};

}  // namespace concord::cron
