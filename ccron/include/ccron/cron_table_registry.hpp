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

#include "cron_table.hpp"

#include <cstdint>
#include <map>

namespace concord::cron {

// A registry that assigns cron tables to components.
class CronTableRegistry {
 public:
  // Accesses the cron table for the given `component_id`. If no cron table exists for it, an empty one is created.
  CronTable& operator[](std::uint32_t component_id) {
    return components_.try_emplace(component_id, CronTable{component_id}).first->second;
  }

  // Disassociates the given `component_id` from its cron table. Does not perform any operations on the cron table
  // itself such as removing entries, etc.
  // Return true if the component had a cron table in the first place or false otherwise.
  bool disassociate(std::uint32_t component_id) { return (components_.erase(component_id) == 1); }

 private:
  std::map<std::uint32_t, CronTable> components_;
};

}  // namespace concord::cron
