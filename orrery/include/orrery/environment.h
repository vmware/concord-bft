// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstddef>
#include <unordered_map>
#include <vector>

#include "assertUtils.hpp"
#include "mailbox.h"
#include "orrery_msgs.cmf.hpp"

namespace concord::orrery {

// An Environment is a static map of components to their executors.
//
// An Environment is meant to be created at startup and remain immutable for the lifetime of the
// process.
//
// Executors communicate by placing envelopes in each other's mailboxes. Since executors can handle
// messages for multiple components, multiple component ids may map to the same executor mailbox.
//
// As there are always a static number of components and executors for a given orrery world, we can
// use a std::array for the mapping.
class Environment {
 public:
  Environment() {
    // The value doesn't matter. `enumSize` is a generated function that ignores the parameter.
    ComponentId dummy = ComponentId::broadcast;
    mailboxes_.resize(enumSize(dummy));
  }

  void add(ComponentId id, const Mailbox& mailbox) {
    ConcordAssertNE(id, ComponentId::broadcast);
    size_t index = static_cast<uint8_t>(id);
    mailboxes_[index] = mailbox;
    executors_.insert({mailbox.executorName(), mailbox});
  }

  Mailbox& mailbox(ComponentId id) {
    ConcordAssertNE(id, ComponentId::broadcast);
    size_t index = static_cast<uint8_t>(id);
    return mailboxes_[index];
  }

  std::unordered_map<std::string, Mailbox>& executors() { return executors_; }

 private:
  // Mailboxes are indexed by ComponentId
  std::vector<Mailbox> mailboxes_;

  // Broadcasts are only sent to distinct mailboxes, as executors distribute them directly to their
  // components. This limits unnecessary traffic over queues.
  std::unordered_map<std::string, Mailbox> executors_;
};

}  // namespace concord::orrery
