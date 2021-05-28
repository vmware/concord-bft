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

#include "ccron/cron_table.hpp"

#include <utility>

namespace concord::cron {

CronTable::CronTable(std::uint32_t component_id) : component_id_{component_id} {}

void CronTable::addEntry(CronEntry entry) { entries_.emplace(entry.position, std::move(entry)); }

bool CronTable::removeEntry(std::uint32_t position) {
  auto it = entries_.find(position);
  if (it == entries_.cend()) {
    return false;
  }
  auto& entry = it->second;
  if (entry.on_remove) {
    (*entry.on_remove)(component_id_, position);
  }
  entries_.erase(it);
  return true;
}

std::uint32_t CronTable::removeAllEntries() {
  for (const auto& [position, entry] : entries_) {
    if (entry.on_remove) {
      (*entry.on_remove)(component_id_, position);
    }
  }
  const auto count = entries_.size();
  entries_.clear();
  return count;
}

const CronEntry* CronTable::at(std::uint32_t position) const {
  auto it = entries_.find(position);
  if (it == entries_.cend()) {
    return nullptr;
  }
  return &it->second;
}

std::uint32_t CronTable::numberOfEntries() const { return entries_.size(); }

bool CronTable::empty() const { return (numberOfEntries() == 0); }

std::uint32_t CronTable::componentId() const { return component_id_; }

void CronTable::evaluate(const Tick& tick) const {
  for (const auto& [_, entry] : entries_) {
    (void)_;
    if (tick.component_id == component_id_ && entry.rule(tick)) {
      if (entry.schedule_next) {
        (*entry.schedule_next)(tick);
      }
      entry.action(tick);
    }
  }
}

}  // namespace concord::cron
